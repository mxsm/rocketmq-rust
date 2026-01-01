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

//! Broker heartbeat management module
//!
//! This module defines the trait for managing broker heartbeats, tracking broker
//! liveness, and notifying listeners when brokers become inactive.

use std::collections::HashMap;
use std::sync::Arc;

use rocketmq_remoting::net::channel::Channel;

use crate::heartbeat::broker_live_info::BrokerLiveInfo;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;

/// Default broker channel expiration time in milliseconds
pub const DEFAULT_BROKER_CHANNEL_EXPIRED_TIME: u64 = 10_000;

/// Broker heartbeat manager trait
///
/// This trait defines the interface for managing broker heartbeats, tracking broker
/// liveness, and notifying listeners of broker state changes.
///
/// # Thread Safety
///
/// All methods in this trait must be thread-safe and can be called concurrently
/// from multiple tasks/threads.
pub trait BrokerHeartbeatManager: Send + Sync {
    /// Initialize the heartbeat manager resources
    ///
    /// This should be called once before calling `start()`.
    fn initialize(&mut self);

    /// Process a broker heartbeat
    ///
    /// Updates the last heartbeat timestamp and broker metadata. If this is the first
    /// heartbeat from a broker, it will be registered.
    ///
    /// # Arguments
    ///
    /// * `cluster_name` - Cluster name
    /// * `broker_name` - Broker name
    /// * `broker_addr` - Broker address
    /// * `broker_id` - Broker ID
    /// * `timeout_millis` - Heartbeat timeout in milliseconds
    /// * `channel` - Network channel
    /// * `epoch` - Broker epoch (for leader election)
    /// * `max_offset` - Maximum message offset
    /// * `confirm_offset` - Confirmed message offset
    /// * `election_priority` - Election priority (lower is higher priority)
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
    );

    /// Start the heartbeat manager
    ///
    /// This starts the background task that periodically scans for inactive brokers.
    fn start(&mut self);

    /// Shutdown the heartbeat manager
    ///
    /// This stops the background scanning task and cleans up resources.
    fn shutdown(&mut self);

    /// Register a broker lifecycle listener
    ///
    /// # Arguments
    ///
    /// * `listener` - The listener to register
    fn register_broker_lifecycle_listener(&mut self, listener: Arc<dyn BrokerLifecycleListener>);

    /// Handle broker channel close event
    ///
    /// This is called when a broker's network channel is closed. It removes the broker
    /// from the live table and notifies listeners.
    ///
    /// # Arguments
    ///
    /// * `channel` - The closed channel
    fn on_broker_channel_close(&self, channel: &Channel);

    /// Get broker live information
    ///
    /// # Arguments
    ///
    /// * `cluster_name` - Cluster name
    /// * `broker_name` - Broker name
    /// * `broker_id` - Broker ID
    ///
    /// # Returns
    ///
    /// The broker live information if the broker is registered, None otherwise
    fn get_broker_live_info(&self, cluster_name: &str, broker_name: &str, broker_id: i64) -> Option<BrokerLiveInfo>;

    /// Check if a broker is active
    ///
    /// A broker is considered active if:
    /// 1. It is registered in the live table
    /// 2. Its last heartbeat timestamp plus timeout is >= current time
    ///
    /// # Arguments
    ///
    /// * `cluster_name` - Cluster name
    /// * `broker_name` - Broker name
    /// * `broker_id` - Broker ID
    ///
    /// # Returns
    ///
    /// true if the broker is active, false otherwise
    fn is_broker_active(&self, cluster_name: &str, broker_name: &str, broker_id: i64) -> bool;

    /// Get the number of active brokers in each cluster and broker set
    ///
    /// # Returns
    ///
    /// A nested map: cluster_name -> broker_name -> active_broker_count
    fn get_active_brokers_num(&self) -> HashMap<String, HashMap<String, u32>>;
}
