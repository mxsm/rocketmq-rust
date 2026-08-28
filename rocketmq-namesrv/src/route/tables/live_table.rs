// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Broker live table with concurrent access
//!
//! Manages broker live status and heartbeat information.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_protocol::protocol::DataVersion;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_transport::api::v1::ChannelId;
use rocketmq_transport::api::v2::SessionId;
use rocketmq_transport::api::v2::SessionStateView;

use crate::config::ExpiryIndexMode;
use crate::route::types::BrokerGeneration;
use crate::route::types::BrokerSession;
use crate::route_info::broker_addr_info::BrokerAddrInfo;

static NEXT_REGISTRATION_EPOCH: AtomicU64 = AtomicU64::new(1);

/// Broker live information
///
/// Contains heartbeat timestamp and data version for a live broker.
pub struct BrokerLiveInfo {
    /// Last heartbeat timestamp (milliseconds since epoch)
    last_update_timestamp: AtomicU64,
    /// Monotonic generation advanced for every accepted heartbeat.
    heartbeat_generation: AtomicU64,
    /// Process-local epoch assigned when a full registration replaces this entry.
    registration_epoch: u64,
    /// Heartbeat timeout in milliseconds (default: 120000ms = 2min)
    pub heartbeat_timeout_millis: u64,
    /// Data version for change detection
    pub data_version: DataVersion,
    /// HA server address (optional)
    pub ha_server_addr: Option<CheetahString>,

    /// Remote socket address of the broker
    pub remote_addr: SocketAddr,

    /// Channel ID for the broker connection
    pub channel_id: ChannelId,

    /// Stable transport session identity, when registered through V2 ingress.
    pub session_id: Option<SessionId>,

    /// Read-only lifecycle observation for the registered V2 session.
    pub session_state: Option<SessionStateView>,

    /// Broker name captured at registration for O(1) expiry cleanup.
    pub broker_name: Option<CheetahString>,

    /// Broker ID captured at registration for O(1) expiry cleanup.
    pub broker_id: Option<u64>,
}

impl BrokerLiveInfo {
    /// Create new broker live info
    ///
    /// # Arguments
    /// * `timestamp` - Current timestamp in milliseconds
    /// * `data_version` - Data version
    pub fn new(timestamp: u64, data_version: DataVersion, remote_addr: SocketAddr, channel_id: ChannelId) -> Self {
        Self {
            last_update_timestamp: AtomicU64::new(timestamp),
            heartbeat_generation: AtomicU64::new(0),
            registration_epoch: NEXT_REGISTRATION_EPOCH.fetch_add(1, Ordering::Relaxed),
            heartbeat_timeout_millis: 120_000, // 2 minutes default
            data_version,
            ha_server_addr: None,
            remote_addr,
            channel_id,
            session_id: None,
            session_state: None,
            broker_name: None,
            broker_id: None,
        }
    }

    /// Create with HA server address
    pub fn with_ha_server(mut self, ha_server_addr: impl Into<CheetahString>) -> Self {
        self.ha_server_addr = Some(ha_server_addr.into());
        self
    }

    /// Set custom heartbeat timeout
    pub fn with_timeout(mut self, timeout_millis: u64) -> Self {
        self.heartbeat_timeout_millis = timeout_millis;
        self
    }

    /// Attach the narrow V2 session facts used for lifecycle cleanup.
    pub(crate) fn with_session(mut self, session: BrokerSession) -> Self {
        self.session_id = session.id;
        self.session_state = session.state;
        self
    }

    /// Attach the immutable broker identity used by delayed cleanup events.
    pub fn with_broker_identity(mut self, broker_name: CheetahString, broker_id: u64) -> Self {
        self.broker_name = Some(broker_name);
        self.broker_id = Some(broker_id);
        self
    }

    /// Check if broker is alive based on current time
    ///
    /// # Arguments
    /// * `current_time` - Current timestamp in milliseconds
    pub fn is_alive(&self, current_time: u64) -> bool {
        current_time.saturating_sub(self.last_update_timestamp()) < self.heartbeat_timeout_millis
    }

    /// Return the last accepted heartbeat timestamp.
    pub fn last_update_timestamp(&self) -> u64 {
        self.last_update_timestamp.load(Ordering::Acquire)
    }

    /// Return the heartbeat generation for stale-event fencing.
    pub fn heartbeat_generation(&self) -> u64 {
        self.heartbeat_generation.load(Ordering::Acquire)
    }

    /// Return the registration epoch for stale-event fencing.
    pub const fn registration_epoch(&self) -> u64 {
        self.registration_epoch
    }

    /// Return the complete generation used by delayed cleanup events.
    pub fn generation(&self) -> BrokerGeneration {
        BrokerGeneration {
            registration_epoch: self.registration_epoch,
            heartbeat_generation: self.heartbeat_generation(),
        }
    }

    /// Atomically update the timestamp without allowing it to move backwards.
    ///
    /// Returns the new heartbeat generation.
    pub fn update_timestamp(&self, timestamp: u64) -> u64 {
        self.last_update_timestamp.fetch_max(timestamp, Ordering::AcqRel);
        self.heartbeat_generation.fetch_add(1, Ordering::AcqRel) + 1
    }
}

impl Clone for BrokerLiveInfo {
    fn clone(&self) -> Self {
        Self {
            last_update_timestamp: AtomicU64::new(self.last_update_timestamp()),
            heartbeat_generation: AtomicU64::new(self.heartbeat_generation()),
            registration_epoch: self.registration_epoch,
            heartbeat_timeout_millis: self.heartbeat_timeout_millis,
            data_version: self.data_version.clone(),
            ha_server_addr: self.ha_server_addr.clone(),
            remote_addr: self.remote_addr,
            channel_id: self.channel_id.clone(),
            session_id: self.session_id,
            session_state: self.session_state.clone(),
            broker_name: self.broker_name.clone(),
            broker_id: self.broker_id,
        }
    }
}

impl fmt::Debug for BrokerLiveInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerLiveInfo")
            .field("last_update_timestamp", &self.last_update_timestamp())
            .field("heartbeat_generation", &self.heartbeat_generation())
            .field("registration_epoch", &self.registration_epoch)
            .field("heartbeat_timeout_millis", &self.heartbeat_timeout_millis)
            .field("data_version", &self.data_version)
            .field("ha_server_addr", &self.ha_server_addr)
            .field("remote_addr", &self.remote_addr)
            .field("channel_id", &self.channel_id)
            .field("session_id", &self.session_id)
            .field(
                "session_closed",
                &self.session_state.as_ref().map(SessionStateView::is_closed),
            )
            .field("broker_name", &self.broker_name)
            .field("broker_id", &self.broker_id)
            .finish()
    }
}

/// Broker live table: BrokerAddr -> BrokerLiveInfo
///
/// This table maintains the live status of brokers including heartbeat
/// timestamps and data versions. Uses DashMap for concurrent access.
///
/// # Performance
/// - Read operations: O(1) average with sharded locking
/// - Write operations: O(1) average, per-entry lock
/// - Heartbeat updates: atomic timestamp update for an existing broker
///
/// # Example
/// ```no_run
/// use std::net::SocketAddr;
/// use std::str::FromStr;
/// use std::sync::Arc;
///
/// use cheetah_string::CheetahString;
/// use rocketmq_namesrv::route::tables::BrokerLiveInfo;
/// use rocketmq_namesrv::route::tables::BrokerLiveTable;
/// use rocketmq_protocol::protocol::DataVersion;
///
/// let table = BrokerLiveTable::new();
/// let remote_addr = SocketAddr::from_str("127.0.0.1:10911").unwrap();
/// let channel_id = CheetahString::from_static_str("test-channel-001");
/// let info = BrokerLiveInfo::new(1000000, DataVersion::default(), remote_addr, channel_id);
/// // Thread-safe operations
/// ```
#[derive(Clone)]
pub struct BrokerLiveTable {
    inner: DashMap<Arc<BrokerAddrInfo>, Arc<BrokerLiveInfo>>,
    by_channel: DashMap<ChannelId, Arc<BrokerAddrInfo>>,
    by_session: DashMap<SessionId, Arc<BrokerAddrInfo>>,
    by_remote_addr: DashMap<SocketAddr, Arc<BrokerAddrInfo>>,
    expiry_index: Arc<ExpiryIndex>,
}

struct ExpiryIndex {
    mode: ExpiryIndexMode,
    state: parking_lot::Mutex<ExpiryIndexState>,
}

#[derive(Default)]
struct ExpiryIndexState {
    by_deadline: BTreeMap<(u64, u64), (Arc<BrokerAddrInfo>, BrokerGeneration)>,
    by_broker: HashMap<Arc<BrokerAddrInfo>, (u64, u64)>,
}

impl ExpiryIndex {
    fn new(mode: ExpiryIndexMode) -> Self {
        Self {
            mode,
            state: parking_lot::Mutex::new(ExpiryIndexState::default()),
        }
    }

    fn schedule(&self, broker: Arc<BrokerAddrInfo>, live_info: &BrokerLiveInfo) {
        if self.mode == ExpiryIndexMode::Off {
            return;
        }
        let deadline = live_info
            .last_update_timestamp()
            .saturating_add(live_info.heartbeat_timeout_millis);
        let generation = live_info.generation();
        let index_key = (deadline, generation.registration_epoch);
        let mut state = self.state.lock();
        if let Some(previous_key) = state.by_broker.insert(Arc::clone(&broker), index_key) {
            state.by_deadline.remove(&previous_key);
        }
        state.by_deadline.insert(index_key, (broker, generation));
    }

    fn remove(&self, broker: &BrokerAddrInfo) {
        if self.mode == ExpiryIndexMode::Off {
            return;
        }
        let mut state = self.state.lock();
        if let Some(index_key) = state.by_broker.remove(broker) {
            state.by_deadline.remove(&index_key);
        }
    }

    fn expired(&self, current_time: u64) -> Vec<(Arc<BrokerAddrInfo>, BrokerGeneration)> {
        if self.mode == ExpiryIndexMode::Off {
            return Vec::new();
        }
        let state = self.state.lock();
        state
            .by_deadline
            .range(..=(current_time, u64::MAX))
            .map(|(_, (broker, generation))| (Arc::clone(broker), *generation))
            .collect()
    }

    fn clear(&self) {
        let mut state = self.state.lock();
        state.by_deadline.clear();
        state.by_broker.clear();
    }
}

impl BrokerLiveTable {
    /// Create a new broker live table
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
            by_channel: DashMap::new(),
            by_session: DashMap::new(),
            by_remote_addr: DashMap::new(),
            expiry_index: Arc::new(ExpiryIndex::new(ExpiryIndexMode::Off)),
        }
    }

    /// Create with estimated capacity
    ///
    /// # Arguments
    /// * `capacity` - Expected number of live brokers
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_expiry_index(capacity, ExpiryIndexMode::Off)
    }

    /// Create with estimated capacity and an optional deadline index.
    pub fn with_capacity_and_expiry_index(capacity: usize, expiry_index_mode: ExpiryIndexMode) -> Self {
        Self {
            inner: DashMap::with_capacity(capacity),
            by_channel: DashMap::with_capacity(capacity),
            by_session: DashMap::with_capacity(capacity),
            by_remote_addr: DashMap::with_capacity(capacity),
            expiry_index: Arc::new(ExpiryIndex::new(expiry_index_mode)),
        }
    }

    /// Register or update broker live status
    ///
    /// # Arguments
    /// * `broker_addr_info` - Broker address info (zero-copy Arc)
    /// * `live_info` - Broker live information
    ///
    /// # Returns
    /// Previous live info if existed
    pub fn register(
        &self,
        broker_addr_info: Arc<BrokerAddrInfo>,
        live_info: BrokerLiveInfo,
    ) -> Option<Arc<BrokerLiveInfo>> {
        let channel_id = live_info.channel_id.clone();
        let session_id = live_info.session_id;
        let remote_addr = live_info.remote_addr;
        let live_info = Arc::new(live_info);
        let previous = self.inner.insert(Arc::clone(&broker_addr_info), Arc::clone(&live_info));
        if let Some(previous) = &previous {
            self.remove_channel_index_if_current(&previous.channel_id, &broker_addr_info);
            if let Some(session_id) = previous.session_id {
                self.remove_session_index_if_current(session_id, &broker_addr_info);
            }
            self.remove_remote_index_if_current(previous.remote_addr, &broker_addr_info);
        }
        self.by_channel.insert(channel_id, Arc::clone(&broker_addr_info));
        if let Some(session_id) = session_id {
            self.by_session.insert(session_id, Arc::clone(&broker_addr_info));
        }
        self.by_remote_addr.insert(remote_addr, Arc::clone(&broker_addr_info));
        self.expiry_index.schedule(broker_addr_info, &live_info);
        previous
    }

    /// Update heartbeat for a broker
    ///
    /// # Arguments
    /// * `broker_addr_info` - Broker address info
    /// * `timestamp` - New heartbeat timestamp
    ///
    /// # Returns
    /// true if broker exists and was updated
    pub fn update_heartbeat(&self, broker_addr_info: &BrokerAddrInfo, timestamp: u64) -> bool {
        if let Some(entry) = self.inner.get(broker_addr_info) {
            entry.update_timestamp(timestamp);
            self.expiry_index.schedule(Arc::clone(entry.key()), entry.value());
            true
        } else {
            false
        }
    }

    /// Get broker live info
    ///
    /// # Arguments
    /// * `broker_addr_info` - Broker address info
    ///
    /// # Returns
    /// Cloned Arc to live info if exists
    pub fn get(&self, broker_addr_info: &BrokerAddrInfo) -> Option<Arc<BrokerLiveInfo>> {
        self.inner.get(broker_addr_info).map(|entry| Arc::clone(entry.value()))
    }

    /// Check if broker is registered
    pub fn contains(&self, broker_addr_info: &BrokerAddrInfo) -> bool {
        self.inner.contains_key(broker_addr_info)
    }

    /// Remove broker
    ///
    /// # Arguments
    /// * `broker_addr_info` - Broker address info
    ///
    /// # Returns
    /// Removed live info if existed
    pub fn remove(&self, broker_addr_info: &BrokerAddrInfo) -> Option<Arc<BrokerLiveInfo>> {
        let (key, live_info) = self.inner.remove(broker_addr_info)?;
        self.expiry_index.remove(&key);
        self.remove_channel_index_if_current(&live_info.channel_id, &key);
        if let Some(session_id) = live_info.session_id {
            self.remove_session_index_if_current(session_id, &key);
        }
        self.remove_remote_index_if_current(live_info.remote_addr, &key);
        Some(live_info)
    }

    /// Get all live brokers
    ///
    /// # Returns
    /// Vector of (broker_addr_info, live_info) pairs
    pub fn get_all(&self) -> Vec<(Arc<BrokerAddrInfo>, Arc<BrokerLiveInfo>)> {
        self.inner
            .iter()
            .map(|entry| (Arc::clone(entry.key()), Arc::clone(entry.value())))
            .collect()
    }

    /// Get expired brokers
    ///
    /// Returns brokers whose last heartbeat exceeds their timeout threshold.
    ///
    /// # Arguments
    /// * `current_time` - Current timestamp in milliseconds
    ///
    /// # Returns
    /// Vector of expired broker address info
    pub fn get_expired_brokers(&self, current_time: u64) -> Vec<Arc<BrokerAddrInfo>> {
        self.inner
            .iter()
            .filter(|entry| {
                !entry.value().is_alive(current_time)
                    || entry
                        .value()
                        .session_state
                        .as_ref()
                        .is_some_and(SessionStateView::is_closed)
            })
            .map(|entry| Arc::clone(entry.key()))
            .collect()
    }

    /// Get expiry candidates from the optional deadline index.
    pub fn get_indexed_expired_brokers(&self, current_time: u64) -> Vec<(Arc<BrokerAddrInfo>, BrokerGeneration)> {
        self.expiry_index.expired(current_time)
    }

    /// Return the configured expiry-index rollout mode.
    pub fn expiry_index_mode(&self) -> ExpiryIndexMode {
        self.expiry_index.mode
    }

    /// Remove expired brokers
    ///
    /// # Arguments
    /// * `current_time` - Current timestamp in milliseconds
    ///
    /// # Returns
    /// Number of brokers removed
    pub fn remove_expired_brokers(&self, current_time: u64) -> usize {
        let expired = self.get_expired_brokers(current_time);
        let count = expired.len();

        for broker in expired {
            self.remove(&broker);
        }

        count
    }

    /// Get number of live brokers
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if table is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clear all data
    pub fn clear(&self) {
        self.inner.clear();
        self.by_channel.clear();
        self.by_session.clear();
        self.by_remote_addr.clear();
        self.expiry_index.clear();
    }

    /// Get brokers with stale data versions
    ///
    /// Useful for detecting brokers that need configuration updates.
    ///
    /// # Arguments
    /// * `expected_version` - Expected data version
    ///
    /// # Returns
    /// Vector of broker address info with stale versions
    pub fn get_stale_version_brokers(&self, expected_version: &DataVersion) -> Vec<Arc<BrokerAddrInfo>> {
        self.inner
            .iter()
            .filter(|entry| &entry.value().data_version != expected_version)
            .map(|entry| Arc::clone(entry.key()))
            .collect()
    }

    /// Get broker live info by broker address.
    ///
    /// # Arguments
    /// * `broker_addr` - Broker address string
    ///
    /// # Returns
    /// Broker live info if found
    pub fn get_broker_by_addr(&self, broker_addr: &str) -> Option<Arc<BrokerLiveInfo>> {
        for entry in self.inner.iter() {
            let key_addr: &str = entry.key().broker_addr.as_ref();
            if key_addr == broker_addr {
                return Some(Arc::clone(entry.value()));
            }
        }
        None
    }

    /// Get broker address info by broker address string
    ///
    /// # Arguments
    /// * `broker_addr` - Broker address string
    ///
    /// # Returns
    /// BrokerAddrInfo if found
    pub fn get_broker_info_by_addr(&self, broker_addr: &str) -> Option<Arc<BrokerAddrInfo>> {
        for entry in self.inner.iter() {
            let key_addr: &str = entry.key().broker_addr.as_ref();
            if key_addr == broker_addr {
                return Some(Arc::clone(entry.key()));
            }
        }
        None
    }

    /// Retrieve broker address information by the compatibility channel ID.
    pub fn get_broker_info_by_channel_id(&self, channel_id: &ChannelId) -> Option<Arc<BrokerAddrInfo>> {
        let broker_addr_info = self.by_channel.get(channel_id)?.clone();
        self.inner
            .get(broker_addr_info.as_ref())
            .filter(|live| &live.channel_id == channel_id)
            .map(|_| broker_addr_info)
    }

    /// Retrieve broker address information by the stable V2 session identity.
    pub fn get_broker_info_by_session_id(&self, session_id: SessionId) -> Option<Arc<BrokerAddrInfo>> {
        let broker_addr_info = self.by_session.get(&session_id)?.clone();
        self.inner
            .get(broker_addr_info.as_ref())
            .filter(|live| live.session_id == Some(session_id))
            .map(|_| broker_addr_info)
    }

    /// Retrieve broker address information and live info by remote socket address.
    pub fn get_broker_info_by_remote_addr(
        &self,
        remote_addr: SocketAddr,
    ) -> Option<(Arc<BrokerAddrInfo>, Arc<BrokerLiveInfo>)> {
        let broker_addr_info = self.by_remote_addr.get(&remote_addr)?.clone();
        let live_info = self
            .inner
            .get(broker_addr_info.as_ref())
            .and_then(|live| (live.remote_addr == remote_addr).then(|| Arc::clone(live.value())))?;
        Some((broker_addr_info, live_info))
    }

    /// Update last update timestamp for a broker.
    ///
    /// # Arguments
    /// * `broker_addr` - Broker address string
    /// * `timestamp` - New timestamp in milliseconds
    pub fn update_last_update_timestamp(
        &self,
        broker_addr: &str,
        timestamp: u64,
        _remote_addr: SocketAddr,
        _channel_id: ChannelId,
    ) {
        for entry in &self.inner {
            let key_addr: &str = entry.key().broker_addr.as_ref();
            if key_addr == broker_addr {
                entry.update_timestamp(timestamp);
                self.expiry_index.schedule(Arc::clone(entry.key()), entry.value());
                return;
            }
        }
    }

    /// Update last update timestamp for a broker by BrokerAddrInfo
    ///
    /// Uses `BrokerAddrInfo(clusterName, brokerAddr)` as the lookup key.
    ///
    /// # Arguments
    /// * `broker_addr_info` - BrokerAddrInfo containing cluster name and broker address
    pub fn update_last_update_timestamp_by_addr_info(&self, broker_addr_info: &BrokerAddrInfo) -> bool {
        self.update_heartbeat(broker_addr_info, current_millis())
    }

    fn remove_channel_index_if_current(&self, channel_id: &ChannelId, broker_addr_info: &BrokerAddrInfo) {
        self.by_channel
            .remove_if(channel_id, |_, indexed| indexed.as_ref() == broker_addr_info);
    }

    fn remove_session_index_if_current(&self, session_id: SessionId, broker_addr_info: &BrokerAddrInfo) {
        self.by_session
            .remove_if(&session_id, |_, indexed| indexed.as_ref() == broker_addr_info);
    }

    fn remove_remote_index_if_current(&self, remote_addr: SocketAddr, broker_addr_info: &BrokerAddrInfo) {
        self.by_remote_addr
            .remove_if(&remote_addr, |_, indexed| indexed.as_ref() == broker_addr_info);
    }
}

impl Default for BrokerLiveTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    fn create_test_broker_addr_info(name: &str, _id: u64) -> Arc<BrokerAddrInfo> {
        Arc::new(BrokerAddrInfo::new("DefaultCluster", format!("{}:10911", name)))
    }

    fn create_test_live_info(timestamp: u64) -> BrokerLiveInfo {
        let remote_addr = SocketAddr::from_str("127.0.0.1:10911").unwrap();
        let channel_id = CheetahString::from_static_str("test-channel-001");
        BrokerLiveInfo::new(timestamp, DataVersion::default(), remote_addr, channel_id)
    }

    #[test]
    fn test_register_and_get() {
        let table = BrokerLiveTable::new();
        let broker_info = create_test_broker_addr_info("broker-a", 0);
        let live_info = create_test_live_info(1000);

        // Register
        let old = table.register(broker_info.clone(), live_info);
        assert!(old.is_none());

        // Get
        let retrieved = table.get(&broker_info).unwrap();
        assert_eq!(retrieved.last_update_timestamp(), 1000);
    }

    #[test]
    fn test_update_heartbeat() {
        let table = BrokerLiveTable::new();
        let broker_info = create_test_broker_addr_info("broker-a", 0);
        let live_info = create_test_live_info(1000);

        table.register(broker_info.clone(), live_info);
        let original = table.get(&broker_info).unwrap();
        let original_epoch = original.registration_epoch();

        // Update heartbeat
        assert!(table.update_heartbeat(&broker_info, 2000));

        // Verify update
        let updated = table.get(&broker_info).unwrap();
        assert!(Arc::ptr_eq(&original, &updated));
        assert_eq!(updated.last_update_timestamp(), 2000);
        assert_eq!(updated.heartbeat_generation(), 1);
        assert_eq!(updated.registration_epoch(), original_epoch);
    }

    #[test]
    fn heartbeat_timestamp_is_monotonic_under_concurrent_updates() {
        let table = Arc::new(BrokerLiveTable::new());
        let broker_info = create_test_broker_addr_info("broker-a", 0);
        table.register(Arc::clone(&broker_info), create_test_live_info(1000));
        let barrier = Arc::new(Barrier::new(17));

        let handles = (0..16)
            .map(|worker| {
                let table = Arc::clone(&table);
                let broker_info = Arc::clone(&broker_info);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for offset in 0..64 {
                        assert!(table.update_heartbeat(&broker_info, 2000 + worker * 64 + offset));
                    }
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }

        let updated = table.get(&broker_info).unwrap();
        assert_eq!(updated.last_update_timestamp(), 3023);
        assert_eq!(updated.heartbeat_generation(), 1024);
    }

    #[test]
    fn heartbeat_for_unknown_broker_is_rejected() {
        let table = BrokerLiveTable::new();
        let broker_info = create_test_broker_addr_info("broker-a", 0);

        assert!(!table.update_heartbeat(&broker_info, 2000));
    }

    #[test]
    fn test_is_alive() {
        let live_info = create_test_live_info(1000);

        // Within timeout
        assert!(live_info.is_alive(1000 + 60_000)); // 1 minute later
        assert!(live_info.is_alive(1000 + 119_999)); // just before timeout
        assert!(live_info.is_alive(999)); // current time moved backwards

        // Exceeded timeout (default 2 minutes)
        assert!(!live_info.is_alive(1000 + 120_000)); // exact timeout boundary
        assert!(!live_info.is_alive(1000 + 150_000)); // 2.5 minutes later
    }

    #[test]
    fn test_custom_timeout() {
        let live_info = create_test_live_info(1000).with_timeout(30_000); // 30 seconds

        assert!(live_info.is_alive(1000 + 20_000)); // 20s later - alive
        assert!(!live_info.is_alive(1000 + 40_000)); // 40s later - dead
    }

    #[test]
    fn test_get_expired_brokers() {
        let table = BrokerLiveTable::new();

        // Register brokers with different timestamps
        let broker1 = create_test_broker_addr_info("broker-1", 0);
        let broker2 = create_test_broker_addr_info("broker-2", 0);

        table.register(broker1.clone(), create_test_live_info(1000));
        table.register(broker2.clone(), create_test_live_info(100_000));

        // Check expired at 150000ms (broker1 should be expired)
        let expired = table.get_expired_brokers(150_000);
        assert_eq!(expired.len(), 1);
    }

    #[test]
    fn expiry_index_reschedules_heartbeat_and_removes_deleted_broker() {
        let table = BrokerLiveTable::with_capacity_and_expiry_index(4, ExpiryIndexMode::Active);
        let broker = create_test_broker_addr_info("broker-indexed", 0);
        table.register(Arc::clone(&broker), create_test_live_info(1000).with_timeout(100));

        assert!(table.get_indexed_expired_brokers(1099).is_empty());
        let initial = table.get_indexed_expired_brokers(1100);
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].1.heartbeat_generation, 0);

        assert!(table.update_heartbeat(&broker, 1080));
        assert!(table.get_indexed_expired_brokers(1100).is_empty());
        let rescheduled = table.get_indexed_expired_brokers(1180);
        assert_eq!(rescheduled.len(), 1);
        assert_eq!(rescheduled[0].1.heartbeat_generation, 1);

        assert!(table.remove(&broker).is_some());
        assert!(table.get_indexed_expired_brokers(u64::MAX).is_empty());
    }

    #[test]
    fn test_remove_expired_brokers() {
        let table = BrokerLiveTable::new();

        let broker1 = create_test_broker_addr_info("broker-1", 0);
        let broker2 = create_test_broker_addr_info("broker-2", 0);

        table.register(broker1.clone(), create_test_live_info(1000));
        table.register(broker2.clone(), create_test_live_info(100_000));

        // Remove expired
        let removed = table.remove_expired_brokers(150_000);
        assert_eq!(removed, 1);
        assert_eq!(table.len(), 1);
        assert!(table.contains(&broker2));
        assert!(!table.contains(&broker1));
    }

    #[test]
    fn test_get_broker_info_by_remote_addr() {
        let table = BrokerLiveTable::new();
        let broker_info = create_test_broker_addr_info("broker-a", 0);
        let remote_addr = SocketAddr::from_str("127.0.0.1:10911").unwrap();
        let live_info = BrokerLiveInfo::new(
            1000,
            DataVersion::default(),
            remote_addr,
            CheetahString::from_static_str("test-channel-001"),
        );

        table.register(broker_info.clone(), live_info);

        let (found_broker_info, found_live_info) = table
            .get_broker_info_by_remote_addr(remote_addr)
            .expect("broker should be found by remote address");
        assert_eq!(found_broker_info, broker_info);
        assert_eq!(found_live_info.remote_addr, remote_addr);
    }

    #[test]
    fn test_get_broker_by_addr_returns_none_for_unknown_addr() {
        let table = BrokerLiveTable::new();
        let broker_info = create_test_broker_addr_info("broker-a", 0);

        table.register(broker_info, create_test_live_info(1000));

        assert!(table.get_broker_by_addr("unknown:10911").is_none());
        assert!(table.get_broker_info_by_addr("unknown:10911").is_none());
    }

    #[test]
    fn test_update_last_update_timestamp_by_addr_info_preserves_live_info_fields() {
        let table = BrokerLiveTable::new();
        let broker_info = create_test_broker_addr_info("broker-a", 0);
        let remote_addr = SocketAddr::from_str("127.0.0.1:10912").unwrap();
        let channel_id = CheetahString::from_static_str("test-channel-002");
        let data_version = DataVersion::with_values(7, 12345, 9);
        let live_info = BrokerLiveInfo::new(1000, data_version.clone(), remote_addr, channel_id.clone())
            .with_timeout(45_000)
            .with_ha_server("ha-server:10912");

        table.register(broker_info.clone(), live_info);
        table.update_last_update_timestamp_by_addr_info(&broker_info);

        let updated = table.get(&broker_info).unwrap();
        assert!(updated.last_update_timestamp() > 1000);
        assert_eq!(updated.heartbeat_timeout_millis, 45_000);
        assert_eq!(updated.data_version, data_version);
        assert_eq!(
            updated.ha_server_addr,
            Some(CheetahString::from_static_str("ha-server:10912"))
        );
        assert_eq!(updated.remote_addr, remote_addr);
        assert_eq!(updated.channel_id, channel_id);
    }

    #[test]
    fn test_with_ha_server() {
        let live_info = create_test_live_info(1000).with_ha_server("ha-server:10912");

        assert_eq!(
            live_info.ha_server_addr,
            Some(CheetahString::from_static_str("ha-server:10912"))
        );
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let table = Arc::new(BrokerLiveTable::new());
        let mut handles = vec![];

        // Spawn multiple threads
        for i in 0..10 {
            let table_clone = table.clone();
            handles.push(thread::spawn(move || {
                let broker_info = create_test_broker_addr_info(&format!("broker-{}", i), 0);
                let live_info = create_test_live_info(1000 + i * 1000);
                table_clone.register(broker_info, live_info);
            }));
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify data
        assert_eq!(table.len(), 10);
    }
}
