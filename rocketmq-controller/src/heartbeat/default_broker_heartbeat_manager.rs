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

//! Default implementation of BrokerHeartbeatManager

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::net::channel::Channel;
use tokio::task::JoinHandle;
use tracing::info;
use tracing::warn;

use crate::config::ControllerConfig;
use crate::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
use crate::controller::broker_heartbeat_manager::DEFAULT_BROKER_CHANNEL_EXPIRED_TIME;
use crate::heartbeat::broker_identity_info::BrokerIdentityInfo;
use crate::heartbeat::broker_live_info::BrokerLiveInfo;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;

/// Default implementation of BrokerHeartbeatManager
///
/// This implementation uses:
/// - `DashMap` for concurrent access to broker live information
/// - Tokio for async background scanning
/// - `Vec` for listener list (registered during initialization only)
///
/// # Example
///
/// ```no_run,ignore
/// use std::sync::Arc;
/// use rocketmq_controller::config::ControllerConfig;
/// use rocketmq_controller::heartbeat::default_broker_heartbeat_manager::DefaultBrokerHeartbeatManager;
/// use rocketmq_controller::controller::broker_heartbeat_manager::BrokerHeartbeatManager;
///
/// # async fn example() {
/// let config = Arc::new(ControllerConfig::test_config());
/// let mut manager = DefaultBrokerHeartbeatManager::new(config);
/// manager.initialize();
/// manager.start();
/// // ... use manager ...
/// manager.shutdown();
/// # }
/// ```
pub struct DefaultBrokerHeartbeatManager {
    /// Controller configuration
    config: Arc<ControllerConfig>,

    /// Broker live information table
    /// Key: BrokerIdentityInfo, Value: BrokerLiveInfo
    broker_live_table: Arc<DashMap<BrokerIdentityInfo, BrokerLiveInfo>>,

    /// Registered lifecycle listeners (initialized before start, immutable afterward)
    lifecycle_listeners: Vec<Arc<dyn BrokerLifecycleListener>>,

    /// Background scan task handle
    scan_task_handle: Option<JoinHandle<()>>,

    /// Scan interval in milliseconds
    scan_interval_ms: u64,
}

impl DefaultBrokerHeartbeatManager {
    /// Create a new DefaultBrokerHeartbeatManager
    ///
    /// # Arguments
    ///
    /// * `config` - Controller configuration
    pub fn new(config: Arc<ControllerConfig>) -> Self {
        Self {
            config,
            broker_live_table: Arc::new(DashMap::with_capacity(256)),
            lifecycle_listeners: Vec::new(),
            scan_task_handle: None,
            scan_interval_ms: 2000, // Default: scan every 2 seconds
        }
    }

    /// Set the scan interval
    ///
    /// # Arguments
    ///
    /// * `interval_ms` - Scan interval in milliseconds
    pub fn with_scan_interval_ms(mut self, interval_ms: u64) -> Self {
        self.scan_interval_ms = interval_ms;
        self
    }

    /// Scan for inactive brokers and remove them
    ///
    /// This is called periodically by the background task.
    async fn scan_not_active_broker(
        broker_live_table: Arc<DashMap<BrokerIdentityInfo, BrokerLiveInfo>>,
        listeners: Arc<Vec<Arc<dyn BrokerLifecycleListener>>>,
    ) {
        info!("start scanNotActiveBroker");

        let now_millis = get_current_millis();
        let mut to_remove = Vec::new();

        // Collect brokers to remove
        for entry in broker_live_table.iter() {
            let broker_identity = entry.key();
            let live_info = entry.value();

            let last_update_timestamp = live_info.last_update_timestamp();
            let timeout_millis = live_info.heartbeat_timeout_millis();

            // Check if broker has expired
            if now_millis > last_update_timestamp + timeout_millis {
                to_remove.push((
                    broker_identity.clone(),
                    live_info.broker_name().to_string(),
                    live_info.broker_id(),
                    live_info.heartbeat_timeout_millis(),
                ));
            }
        }

        // Remove expired brokers and notify listeners
        for (identity, _broker_name, broker_id, timeout_millis) in to_remove {
            broker_live_table.remove(&identity);

            warn!(
                "The broker channel expired, brokerInfo {}, expired {}ms",
                identity, timeout_millis
            );

            // Notify all listeners
            for listener in listeners.iter() {
                listener.on_broker_inactive(&identity.cluster_name, &identity.broker_name, broker_id);
            }
        }
    }

    /// Notify listeners that a broker is inactive
    async fn notify_broker_inactive(
        listeners: Arc<Vec<Arc<dyn BrokerLifecycleListener>>>,
        cluster_name: &str,
        broker_name: &str,
        broker_id: i64,
    ) {
        for listener in listeners.iter() {
            listener.on_broker_inactive(cluster_name, broker_name, broker_id);
        }
    }
}

impl BrokerHeartbeatManager for DefaultBrokerHeartbeatManager {
    fn initialize(&mut self) {
        // No special initialization needed for DashMap-based implementation
        info!("DefaultBrokerHeartbeatManager initialized");
    }

    fn on_broker_heartbeat(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_addr: &str,
        broker_id: i64,
        timeout_millis: Option<u64>,
        channel: Channel,
        epoch: Option<i32>,
        max_offset: Option<i64>,
        confirm_offset: Option<i64>,
        election_priority: Option<i32>,
    ) {
        let broker_identity = BrokerIdentityInfo::new(
            cluster_name.to_string(),
            broker_name.to_string(),
            Some(broker_id as u64),
        );

        let real_epoch = epoch.unwrap_or(-1);
        let real_max_offset = max_offset.unwrap_or(-1);
        let real_confirm_offset = confirm_offset.unwrap_or(-1);
        let real_timeout_millis = timeout_millis.unwrap_or(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME);
        let real_election_priority = election_priority.or(Some(i32::MAX));

        // Get current timestamp in milliseconds
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if let Some(mut prev) = self.broker_live_table.get_mut(&broker_identity) {
            // Update existing broker info
            prev.set_last_update_timestamp(now_millis);
            prev.set_heartbeat_timeout_millis(real_timeout_millis);
            prev.set_election_priority(real_election_priority);

            // Only update epoch/offset if they're newer
            if real_epoch > prev.epoch() || (real_epoch == prev.epoch() && real_max_offset > prev.max_offset()) {
                prev.set_epoch(real_epoch);
                prev.set_max_offset(real_max_offset);
                prev.set_confirm_offset(real_confirm_offset);
            }
        } else {
            // Register new broker
            let live_info = BrokerLiveInfo::new(
                broker_name.to_string(),
                broker_addr.to_string(),
                broker_id,
                now_millis,
                real_timeout_millis,
                channel,
                real_epoch,
                real_max_offset,
                real_election_priority,
                Some(real_confirm_offset),
            );

            self.broker_live_table.insert(broker_identity.clone(), live_info);

            info!("new broker registered, {}, brokerId:{}", broker_identity, broker_id);
        }
    }

    fn start(&mut self) {
        let broker_live_table = self.broker_live_table.clone();
        let listeners = Arc::new(self.lifecycle_listeners.clone());
        let scan_interval_ms = self.scan_interval_ms;

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(scan_interval_ms));
            // Skip the first tick (fires immediately)
            interval.tick().await;

            loop {
                interval.tick().await;
                Self::scan_not_active_broker(broker_live_table.clone(), listeners.clone()).await;
            }
        });

        self.scan_task_handle = Some(handle);
        info!("DefaultBrokerHeartbeatManager background scan started");
    }

    fn shutdown(&mut self) {
        if let Some(handle) = self.scan_task_handle.take() {
            handle.abort();
            info!("DefaultBrokerHeartbeatManager background scan stopped");
        }
    }

    fn register_broker_lifecycle_listener(&mut self, listener: Arc<dyn BrokerLifecycleListener>) {
        // Register listeners during initialization (single-threaded)
        self.lifecycle_listeners.push(listener);
    }

    fn on_broker_channel_close(&self, channel: &Channel) {
        let mut broker_identity_to_remove = None;
        let mut broker_info_for_notify = None;

        // Find the broker with this channel
        for entry in self.broker_live_table.iter() {
            if entry.value().channel() == channel {
                let identity = entry.key().clone();
                let live_info = entry.value();

                info!(
                    "Channel inactive, broker {}, addr:{}, id:{}",
                    live_info.broker_name(),
                    live_info.broker_addr(),
                    live_info.broker_id()
                );

                broker_identity_to_remove = Some(identity.clone());
                broker_info_for_notify = Some((
                    identity.cluster_name.to_string(),
                    live_info.broker_name().to_string(),
                    live_info.broker_id(),
                ));
                break;
            }
        }

        // Remove broker and notify listeners
        if let Some(identity) = broker_identity_to_remove {
            self.broker_live_table.remove(&identity);

            if let Some((cluster_name, broker_name, broker_id)) = broker_info_for_notify {
                let listeners = Arc::new(self.lifecycle_listeners.clone());
                tokio::spawn(async move {
                    Self::notify_broker_inactive(listeners, &cluster_name, &broker_name, broker_id).await;
                });
            }
        }
    }

    fn get_broker_live_info(&self, cluster_name: &str, broker_name: &str, broker_id: i64) -> Option<BrokerLiveInfo> {
        let identity = BrokerIdentityInfo::new(
            cluster_name.to_string(),
            broker_name.to_string(),
            Some(broker_id as u64),
        );

        self.broker_live_table.get(&identity).map(|r| r.clone())
    }

    fn is_broker_active(&self, cluster_name: &str, broker_name: &str, broker_id: i64) -> bool {
        let identity = BrokerIdentityInfo::new(
            cluster_name.to_string(),
            broker_name.to_string(),
            Some(broker_id as u64),
        );

        if let Some(info) = self.broker_live_table.get(&identity) {
            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let last = info.last_update_timestamp();
            let timeout_millis = info.heartbeat_timeout_millis();

            return (last + timeout_millis) >= now_millis;
        }

        false
    }

    fn get_active_brokers_num(&self) -> HashMap<String, HashMap<String, u32>> {
        let mut result: HashMap<String, HashMap<String, u32>> = HashMap::new();

        for entry in self.broker_live_table.iter() {
            let identity = entry.key();

            // Check if broker is active
            if self.is_broker_active(
                &identity.cluster_name,
                &identity.broker_name,
                identity.broker_id.unwrap_or(0) as i64,
            ) {
                result
                    .entry(identity.cluster_name.to_string())
                    .or_default()
                    .entry(identity.broker_name.to_string())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }

        result
    }
}

impl Drop for DefaultBrokerHeartbeatManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests are disabled because creating Channel instances requires
    // complex setup with Connection and ResponseTable. Once proper test infrastructure
    // is available, these tests should be enabled.

    #[tokio::test]
    #[ignore]
    async fn test_broker_heartbeat_registration() {
        // TODO: Implement with proper Channel mock/fixture
    }

    #[tokio::test]
    #[ignore]
    async fn test_broker_heartbeat_update() {
        // TODO: Implement with proper Channel mock/fixture
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_active_brokers_num() {
        // TODO: Implement with proper Channel mock/fixture
    }

    #[test]
    fn test_broker_identity_info_creation() {
        let identity = BrokerIdentityInfo::new("cluster1".to_string(), "broker1".to_string(), Some(1));
        assert_eq!(identity.cluster_name, "cluster1");
        assert_eq!(identity.broker_name, "broker1");
        assert_eq!(identity.broker_id, Some(1));
    }

    #[test]
    fn test_default_broker_heartbeat_manager_creation() {
        let config = Arc::new(ControllerConfig::test_config());
        let manager = DefaultBrokerHeartbeatManager::new(config.clone());
        assert_eq!(manager.scan_interval_ms, 2000);
    }
}
