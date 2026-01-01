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

use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::DataVersion;

use crate::route_info::broker_addr_info::BrokerAddrInfo;

/// Broker live information
///
/// Contains heartbeat timestamp and data version for a live broker.
#[derive(Debug, Clone)]
pub struct BrokerLiveInfo {
    /// Last heartbeat timestamp (milliseconds since epoch)
    pub last_update_timestamp: u64,
    /// Heartbeat timeout in milliseconds (default: 120000ms = 2min)
    pub heartbeat_timeout_millis: u64,
    /// Data version for change detection
    pub data_version: DataVersion,
    /// HA server address (optional)
    pub ha_server_addr: Option<CheetahString>,
}

impl BrokerLiveInfo {
    /// Create new broker live info
    ///
    /// # Arguments
    /// * `timestamp` - Current timestamp in milliseconds
    /// * `data_version` - Data version
    pub fn new(timestamp: u64, data_version: DataVersion) -> Self {
        Self {
            last_update_timestamp: timestamp,
            heartbeat_timeout_millis: 120_000, // 2 minutes default
            data_version,
            ha_server_addr: None,
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

    /// Check if broker is alive based on current time
    ///
    /// # Arguments
    /// * `current_time` - Current timestamp in milliseconds
    pub fn is_alive(&self, current_time: u64) -> bool {
        current_time.saturating_sub(self.last_update_timestamp) < self.heartbeat_timeout_millis
    }

    /// Update last heartbeat timestamp
    pub fn update_timestamp(&mut self, timestamp: u64) {
        self.last_update_timestamp = timestamp;
    }
}

/// Broker live table: BrokerAddr -> BrokerLiveInfo
///
/// This table maintains the live status of brokers including heartbeat
/// timestamps and data versions. Uses DashMap for concurrent access.
///
/// # Performance
/// - Read operations: O(1) average, lock-free
/// - Write operations: O(1) average, per-entry lock
/// - Heartbeat updates: Lock-free for same broker
///
/// # Example
/// ```no_run
/// use std::sync::Arc;
///
/// use rocketmq_namesrv::route::tables::BrokerLiveInfo;
/// use rocketmq_namesrv::route::tables::BrokerLiveTable;
/// use rocketmq_remoting::protocol::DataVersion;
///
/// let table = BrokerLiveTable::new();
/// let info = BrokerLiveInfo::new(1000000, DataVersion::default());
/// // Thread-safe operations
/// ```
#[derive(Clone)]
pub struct BrokerLiveTable {
    inner: DashMap<Arc<BrokerAddrInfo>, Arc<BrokerLiveInfo>>,
}

impl BrokerLiveTable {
    /// Create a new broker live table
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Create with estimated capacity
    ///
    /// # Arguments
    /// * `capacity` - Expected number of live brokers
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(capacity),
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
        self.inner.insert(broker_addr_info, Arc::new(live_info))
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
        if let Some(mut entry) = self.inner.get_mut(broker_addr_info) {
            let mut new_info = (**entry.value()).clone();
            new_info.update_timestamp(timestamp);
            *entry.value_mut() = Arc::new(new_info);
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
        self.inner.remove(broker_addr_info).map(|(_, v)| v)
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
            .filter(|entry| !entry.value().is_alive(current_time))
            .map(|entry| Arc::clone(entry.key()))
            .collect()
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
            self.inner.remove(&*broker);
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

    /// Get broker live info by broker address (v1 compatibility)
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

    /// Update last update timestamp for a broker (v1 compatibility)
    ///
    /// # Arguments
    /// * `broker_addr` - Broker address string
    /// * `timestamp` - New timestamp in milliseconds
    pub fn update_last_update_timestamp(&self, broker_addr: &str, timestamp: u64) {
        for mut entry in self.inner.iter_mut() {
            let key_addr: &str = entry.key().broker_addr.as_ref();
            if key_addr == broker_addr {
                // Create new BrokerLiveInfo with updated timestamp
                let old_info = entry.value();
                let new_info = BrokerLiveInfo::new(timestamp, old_info.data_version.clone());
                *entry.value_mut() = Arc::new(new_info);
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
    pub fn update_last_update_timestamp_by_addr_info(&self, broker_addr_info: &BrokerAddrInfo) {
        if let Some(mut entry) = self.inner.get_mut(broker_addr_info) {
            let current_time = get_current_millis();
            let old_info = entry.value();
            let new_info = BrokerLiveInfo::new(current_time, old_info.data_version.clone())
                .with_timeout(old_info.heartbeat_timeout_millis)
                .with_ha_server(old_info.ha_server_addr.clone().unwrap_or_default());
            *entry.value_mut() = Arc::new(new_info);
        }
    }
}

impl Default for BrokerLiveTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_broker_addr_info(name: &str, _id: u64) -> Arc<BrokerAddrInfo> {
        Arc::new(BrokerAddrInfo::new("DefaultCluster", format!("{}:10911", name)))
    }

    fn create_test_live_info(timestamp: u64) -> BrokerLiveInfo {
        BrokerLiveInfo::new(timestamp, DataVersion::default())
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
        assert_eq!(retrieved.last_update_timestamp, 1000);
    }

    #[test]
    fn test_update_heartbeat() {
        let table = BrokerLiveTable::new();
        let broker_info = create_test_broker_addr_info("broker-a", 0);
        let live_info = create_test_live_info(1000);

        table.register(broker_info.clone(), live_info);

        // Update heartbeat
        assert!(table.update_heartbeat(&broker_info, 2000));

        // Verify update
        let updated = table.get(&broker_info).unwrap();
        assert_eq!(updated.last_update_timestamp, 2000);
    }

    #[test]
    fn test_is_alive() {
        let live_info = BrokerLiveInfo::new(1000, DataVersion::default());

        // Within timeout
        assert!(live_info.is_alive(1000 + 60_000)); // 1 minute later

        // Exceeded timeout (default 2 minutes)
        assert!(!live_info.is_alive(1000 + 150_000)); // 2.5 minutes later
    }

    #[test]
    fn test_custom_timeout() {
        let live_info = BrokerLiveInfo::new(1000, DataVersion::default()).with_timeout(30_000); // 30 seconds

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
    fn test_with_ha_server() {
        let live_info = BrokerLiveInfo::new(1000, DataVersion::default()).with_ha_server("ha-server:10912");

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
