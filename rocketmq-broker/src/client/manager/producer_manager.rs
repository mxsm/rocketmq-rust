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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::connection::ConnectionState;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::producer_info::ProducerInfo;
use rocketmq_remoting::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::info;
use tracing::warn;

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::producer_change_listener::ArcProducerChangeListener;
use crate::client::producer_group_event::ProducerGroupEvent;
use crate::types::ProducerGroupName;

/// Timeout for considering a producer channel as expired (120 seconds in milliseconds)
const CHANNEL_EXPIRED_TIMEOUT: u64 = 120_000;
/// Number of retry attempts when getting an available channel
const GET_AVAILABLE_CHANNEL_RETRY_COUNT: u32 = 3;

/// Manages producer client connections and their lifecycle.
///
/// Maintains a two-level mapping from producer groups to channels and client information,
/// with additional indices for efficient lookups and event processing. Automatically expires
/// and removes inactive producer connections.
///
/// All operations are thread-safe through lock-free or internally synchronized data structures.
pub struct ProducerManager {
    /// Group name -> (Channel -> ClientChannelInfo) mapping
    group_channel_table: Arc<DashMap<ProducerGroupName, DashMap<Channel, ClientChannelInfo>>>,
    /// Client ID -> Channel mapping for quick channel lookup by client ID
    client_channel_table: Arc<DashMap<CheetahString, Channel>>,
    /// Channel -> ProducerGroups mapping for fast channel close event processing
    channel_to_groups: Arc<DashMap<Channel, HashSet<ProducerGroupName>>>,
    /// Counter for round-robin channel selection
    positive_atomic_counter: Arc<AtomicI32>,
    /// Listeners notified on producer registration/unregistration events (thread-safe)
    producer_change_listener_vec: Arc<ArcSwap<Vec<ArcProducerChangeListener>>>,
    /// Optional broker statistics manager (set once during initialization)
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    /// Broker configuration for feature toggles
    broker_config: Option<Arc<BrokerConfig>>,
}

impl ProducerManager {
    /// Creates a new producer manager with empty state.
    pub fn new() -> Self {
        Self {
            group_channel_table: Arc::new(DashMap::new()),
            client_channel_table: Arc::new(DashMap::new()),
            channel_to_groups: Arc::new(DashMap::new()),
            positive_atomic_counter: Arc::new(AtomicI32::new(0)),
            producer_change_listener_vec: Arc::new(ArcSwap::from_pointee(Vec::new())),
            broker_stats_manager: None,
            broker_config: None,
        }
    }

    /// Assigns the broker statistics manager.
    ///
    /// This method should be called during initialization before the manager is shared
    /// across threads.
    pub fn set_broker_stats_manager(&mut self, broker_stats_manager: Arc<BrokerStatsManager>) {
        self.broker_stats_manager = Some(broker_stats_manager);
    }

    /// Assigns the broker configuration.
    ///
    /// The configuration controls conditional registration and fast path optimizations.
    /// This method should be called during initialization before the manager is shared
    /// across threads.
    pub fn set_broker_config(&mut self, broker_config: Arc<BrokerConfig>) {
        self.broker_config = Some(broker_config);
    }

    /// Registers a listener for producer registration and unregistration events.
    ///
    /// The listener will be invoked synchronously when producers connect, disconnect,
    /// or when groups are created or removed.
    pub fn append_producer_change_listener(&self, producer_change_listener: ArcProducerChangeListener) {
        self.producer_change_listener_vec.rcu(|listeners| {
            let mut new_listeners = (**listeners).clone();
            new_listeners.push(Arc::clone(&producer_change_listener));
            new_listeners
        });
    }
}

impl ProducerManager {
    /// Returns the number of producer groups currently registered.
    ///
    /// # Returns
    /// The total count of producer groups
    pub fn group_size(&self) -> usize {
        self.group_channel_table.len()
    }

    /// Returns a snapshot of all connected producers organized by group.
    ///
    /// The snapshot reflects the state at the time of the call and may become stale
    /// as producers connect or disconnect.
    pub fn get_producer_table(&self) -> ProducerTableInfo {
        let mut map: HashMap<String, Vec<ProducerInfo>> = HashMap::new();

        // Iterate over all groups
        for group_entry in self.group_channel_table.iter() {
            let (group, channel_map) = (group_entry.key(), group_entry.value());

            // Iterate over all channels in this group
            for channel_entry in channel_map.iter() {
                let client_channel_info = channel_entry.value();

                // Create producer info from client channel info
                let producer_info = ProducerInfo::new(
                    client_channel_info.client_id().to_string(),
                    client_channel_info.channel().remote_address().to_string(),
                    client_channel_info.language(),
                    client_channel_info.version(),
                    client_channel_info.last_update_timestamp() as i64,
                );

                // Add to map, creating a new vector if this is the first entry for this group
                map.entry(group.to_string()).or_default().push(producer_info);
            }
        }

        // Create and return producer table info
        ProducerTableInfo::from(map)
    }

    /// Checks whether a producer group has at least one connected producer.
    ///
    /// # Arguments
    /// * `group` - The producer group name
    ///
    /// # Returns
    /// `true` if the group exists and contains at least one producer, `false` otherwise
    pub fn group_online(&self, group: &str) -> bool {
        self.group_channel_table
            .get(group)
            .map(|channels| !channels.is_empty())
            .unwrap_or(false)
    }

    /// Removes a producer from a group.
    ///
    /// If the removal causes the group to become empty, the group is also removed.
    /// Notifies registered listeners of the unregistration event.
    ///
    /// # Arguments
    /// * `group` - The producer group name
    /// * `client_channel_info` - The client channel information
    /// * `ctx` - Connection handler context containing the channel
    pub fn unregister_producer(
        &self,
        group: &str,
        _client_channel_info: &ClientChannelInfo,
        ctx: &ConnectionHandlerContext,
    ) {
        let mut removed_info: Option<ClientChannelInfo> = None;
        let mut is_group_empty = false;

        // Atomically remove producer from group table
        if let Some(channel_table) = self.group_channel_table.get(group) {
            if let Some((_channel, old)) = channel_table.remove(ctx.channel()) {
                removed_info = Some(old);
                is_group_empty = channel_table.is_empty();
            }
        }

        // Process removal without holding group table locks
        if let Some(old) = removed_info {
            // Remove from clientChannelTable only if the channel matches
            if let Some(entry) = self.client_channel_table.get(old.client_id()) {
                if entry.value() == ctx.channel() {
                    drop(entry); // Release read lock before remove
                    self.client_channel_table.remove(old.client_id());
                }
            }

            info!(
                "unregister a producer[{}] from groupChannelTable, client: {}",
                group,
                old.client_id()
            );

            // Call listener outside of locks
            self.call_producer_change_listener(ProducerGroupEvent::ClientUnregister, group, Some(&old));

            // Update channel_to_groups mapping (if fast path is enabled)
            if self.is_fast_channel_event_enabled() {
                if let Some(mut entry) = self.channel_to_groups.get_mut(ctx.channel()) {
                    entry.remove(group);
                    if entry.is_empty() {
                        drop(entry);
                        self.channel_to_groups.remove(ctx.channel());
                    }
                }
            }

            // Atomically remove group if empty to avoid race conditions
            if is_group_empty {
                // Use remove_if to atomically check and remove
                let removed = self
                    .group_channel_table
                    .remove_if(group, |_, channel_map| channel_map.is_empty());
                if removed.is_some() {
                    info!("unregister a producer group[{}] from groupChannelTable", group);
                    self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, group, None);
                }
            }
        }
    }

    /// Registers a producer or updates its heartbeat timestamp.
    ///
    /// For existing producers, updates the last heartbeat timestamp. For new producers,
    /// adds them to the group and updates internal indices. Registration may be rejected
    /// if conditional registration is enabled and the producer is not already registered.
    ///
    /// # Arguments
    /// * `group` - The producer group name
    /// * `client_channel_info` - The client channel information
    #[allow(clippy::mutable_key_type)]
    pub fn register_producer(&self, group: &ProducerGroupName, client_channel_info: &ClientChannelInfo) {
        // Conditional registration check (capacity protection mechanism)
        if let Some(config) = &self.broker_config {
            if !config.enable_register_producer && config.reject_transaction_message {
                // Check if this is an existing producer (only allow heartbeat updates)
                let channel_table = self.group_channel_table.get(group);
                let need_register = match channel_table {
                    None => false, // Group doesn't exist, don't allow new registration
                    Some(table) => {
                        // Group exists, check if channel is already registered
                        table.contains_key(client_channel_info.channel())
                    }
                };

                if !need_register {
                    // Not an existing producer, reject registration
                    return;
                }
            }
        }

        // Update group_channel_table
        {
            let channel_table = self.group_channel_table.entry(group.clone()).or_default();

            // Check if this channel is already registered
            if let Some(mut existing_info) = channel_table.get_mut(client_channel_info.channel()) {
                // Update timestamp for existing producer
                existing_info.set_last_update_timestamp(current_millis());
                return;
            }

            // New producer - insert into channel table
            channel_table.insert(client_channel_info.channel().clone(), client_channel_info.clone());
        }

        // Update channel_to_groups mapping for fast path (if enabled)
        if self.is_fast_channel_event_enabled() {
            self.channel_to_groups
                .entry(client_channel_info.channel().clone())
                .or_default()
                .insert(group.clone());
        }

        // Update client channel index
        let client_id = client_channel_info.client_id();
        let new_channel = client_channel_info.channel();

        // Check existing channel for this client_id
        let should_update = match self.client_channel_table.get(client_id) {
            Some(existing_channel) if existing_channel.value() == new_channel => {
                // Same channel - no action needed
                false
            }
            Some(existing_channel) => {
                // Different channel with same client_id
                warn!(
                    "Producer client_id[{}] is registering with a different channel. Old channel: {}, New channel: {}",
                    client_id,
                    existing_channel.remote_address(),
                    new_channel.remote_address()
                );
                true
            }
            None => {
                // First time seeing this client_id
                true
            }
        };

        if should_update {
            self.client_channel_table.insert(client_id.clone(), new_channel.clone());
        }

        info!(
            "new producer connected, group: {} channel: {} clientId: {}",
            group,
            client_channel_info.channel().remote_address(),
            client_id
        );
    }

    /// Finds the channel associated with a client identifier.
    ///
    /// # Arguments
    /// * `client_id` - The client identifier
    ///
    /// # Returns
    /// The channel if the client is currently registered, or `None` otherwise
    pub fn find_channel(&self, client_id: &str) -> Option<Channel> {
        self.client_channel_table
            .get(client_id)
            .map(|entry| entry.value().clone())
    }

    /// Selects an available channel from a producer group using round-robin.
    ///
    /// Prefers healthy channels but falls back to degraded channels if no healthy channel
    /// is found after a fixed number of attempts. Skips closed channels.
    ///
    /// # Arguments
    /// * `group` - The producer group name
    ///
    /// # Returns
    /// A channel if the group exists and has at least one non-closed channel, or `None` otherwise
    pub fn get_available_channel(&self, group: Option<&ProducerGroupName>) -> Option<Channel> {
        let group = group?;

        // Collect all channels first, then release the lock
        let channels: Vec<Channel> = {
            let channel_map = self.group_channel_table.get(group)?;
            if channel_map.is_empty() {
                warn!("Channel list is empty. group={}", group);
                return None;
            }
            channel_map.iter().map(|entry| entry.key().clone()).collect()
        };

        let size = channels.len();
        if size == 0 {
            warn!("Channel list is empty. group={}", group);
            return None;
        }

        // Round-robin selection (no locks held)
        let index = self
            .positive_atomic_counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let mut index = index.unsigned_abs() as usize % size;
        let mut last_healthy_channel: Option<Channel> = None;

        // Try to find a healthy channel (retry GET_AVAILABLE_CHANNEL_RETRY_COUNT times)
        for _ in 0..GET_AVAILABLE_CHANNEL_RETRY_COUNT {
            let channel = &channels[index];
            let is_healthy = channel.connection_ref().is_healthy();

            if is_healthy {
                return Some(channel.clone());
            }

            // Track non-closed channels as fallback
            if channel.connection_ref().state() != ConnectionState::Closed {
                last_healthy_channel = Some(channel.clone());
            }

            index = (index + 1) % size;
        }

        last_healthy_channel
    }

    /// Removes producers that have not sent a heartbeat within the timeout period.
    ///
    /// This method should be invoked periodically by a background task. Producers inactive
    /// for more than 120 seconds are removed and their channels are closed. Listeners are
    /// notified of each removal.
    pub fn scan_not_active_channel(&self) {
        let current_time = current_millis();

        // Collect expired channels: (group_name, channel, client_info)
        let mut expired_channels: Vec<(ProducerGroupName, Channel, ClientChannelInfo)> = Vec::new();

        // Phase 1: Identify expired channels
        for group_entry in self.group_channel_table.iter() {
            let (group, channel_map) = (group_entry.key(), group_entry.value());

            for channel_entry in channel_map.iter() {
                let (channel, info) = (channel_entry.key(), channel_entry.value());
                let diff = current_time - info.last_update_timestamp();

                if diff > CHANNEL_EXPIRED_TIMEOUT {
                    expired_channels.push((group.clone(), channel.clone(), info.clone()));
                }
            }
        }

        // Phase 2: Remove expired channels
        let mut empty_groups: std::collections::HashSet<ProducerGroupName> = std::collections::HashSet::new();

        for (group, channel, info) in expired_channels {
            // Remove from channel_map
            if let Some(channel_map) = self.group_channel_table.get(&group) {
                channel_map.remove(&channel);

                warn!(
                    "ProducerManager#scan_not_active_channel: remove expired channel[{}] from ProducerManager \
                     groupChannelTable, producer group name: {}, client_id: {}",
                    channel.remote_address(),
                    group,
                    info.client_id()
                );

                // Check if group is now empty
                if channel_map.is_empty() {
                    empty_groups.insert(group.clone());
                }
            }

            // Remove from clientChannelTable if it matches (outside of group_channel_table lock)
            if let Some(entry) = self.client_channel_table.get(info.client_id()) {
                if *entry.value() == channel {
                    drop(entry); // Release the read lock before remove
                    self.client_channel_table.remove(info.client_id());
                }
            }

            self.call_producer_change_listener(ProducerGroupEvent::ClientUnregister, &group, Some(&info));

            // Update channel_to_groups mapping (if fast path is enabled)
            if self.is_fast_channel_event_enabled() {
                if let Some(mut entry) = self.channel_to_groups.get_mut(&channel) {
                    entry.remove(&group);
                    if entry.is_empty() {
                        drop(entry);
                        self.channel_to_groups.remove(&channel);
                    }
                }
            }

            channel.connection_ref().close();
        }

        // Remove empty groups
        // Use remove_if to avoid TOCTOU race - only remove if still empty
        for group in empty_groups {
            let removed = self
                .group_channel_table
                .remove_if(&group, |_, channel_map| channel_map.is_empty());
            if removed.is_some() {
                warn!(
                    "SCAN: remove expired channel from ProducerManager groupChannelTable, all clear, group={}",
                    group
                );
                self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, &group, None);
            }
        }
    }

    /// Removes all producers associated with a closed channel.
    ///
    /// Invoked by the connection layer when a channel is closed. Selects between fast path
    /// (O(k) where k is the number of groups) and slow path (O(n) where n is total groups)
    /// based on configuration.
    ///
    /// # Arguments
    /// * `remote_addr` - The remote address of the closed channel
    /// * `channel` - The closed channel
    ///
    /// # Returns
    /// `true` if at least one producer was removed, `false` otherwise
    pub fn do_channel_close_event(&self, remote_addr: &str, channel: &Channel) -> bool {
        if self.is_fast_channel_event_enabled() {
            self.do_channel_close_event_fast(remote_addr, channel)
        } else {
            self.do_channel_close_event_slow(remote_addr, channel)
        }
    }

    /// Processes channel close events using the fast path.
    ///
    /// Uses a direct channel-to-groups index to locate affected groups without scanning
    /// the entire group table. Time complexity is O(k) where k is the number of groups
    /// the channel belongs to.
    fn do_channel_close_event_fast(&self, remote_addr: &str, channel: &Channel) -> bool {
        // Get groups associated with this channel from fast lookup table
        let groups = match self.channel_to_groups.get(channel) {
            Some(entry) => entry.value().clone(),
            None => return false, // Channel not in any group
        };

        if groups.is_empty() {
            return false;
        }

        let mut removed = false;
        let mut empty_groups = HashSet::new();

        // Only iterate through groups that contain this channel
        for group in &groups {
            if let Some(channel_map) = self.group_channel_table.get(group) {
                if let Some((_, client_channel_info)) = channel_map.remove(channel) {
                    removed = true;

                    // Remove from clientChannelTable
                    if let Some(entry) = self.client_channel_table.get(client_channel_info.client_id()) {
                        if entry.value() == channel {
                            drop(entry);
                            self.client_channel_table.remove(client_channel_info.client_id());
                        }
                    }

                    info!(
                        "NETTY EVENT (Fast Path): remove channel[{}][{}] from ProducerManager, group: {}",
                        client_channel_info.channel().remote_address(),
                        remote_addr,
                        group
                    );

                    // Notify listener
                    self.call_producer_change_listener(
                        ProducerGroupEvent::ClientUnregister,
                        group,
                        Some(&client_channel_info),
                    );

                    // Check if group is now empty
                    if channel_map.is_empty() {
                        empty_groups.insert(group.clone());
                    }
                }
            }
        }

        // Remove empty groups
        for group in empty_groups {
            if self
                .group_channel_table
                .remove_if(&group, |_, map| map.is_empty())
                .is_some()
            {
                info!(
                    "unregister a producer group[{}] from groupChannelTable (Fast Path)",
                    group
                );
                self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, &group, None);
            }
        }

        // Clean up channel_to_groups mapping
        self.channel_to_groups.remove(channel);

        removed
    }

    /// Processes channel close events using the slow path.
    ///
    /// Scans all producer groups to locate channels matching the closed channel.
    /// Time complexity is O(n) where n is the total number of producer groups.
    /// Used when fast path is disabled.
    fn do_channel_close_event_slow(&self, remote_addr: &str, channel: &Channel) -> bool {
        // Collect affected groups: (group_name, client_channel_info)
        let mut channels_to_remove: Vec<(ProducerGroupName, ClientChannelInfo)> = Vec::new();
        for group_entry in self.group_channel_table.iter() {
            let (group, channel_map) = (group_entry.key(), group_entry.value());
            if let Some(entry) = channel_map.get(channel) {
                channels_to_remove.push((group.clone(), entry.value().clone()));
            }
        }

        if channels_to_remove.is_empty() {
            return false;
        }

        let mut empty_groups: std::collections::HashSet<ProducerGroupName> = std::collections::HashSet::new();

        // Remove channels from their groups
        for (group, client_channel_info) in &channels_to_remove {
            if let Some(channel_map) = self.group_channel_table.get(group) {
                channel_map.remove(channel);

                info!(
                    "Channel Close event: remove channel[{}][{}] from ProducerManager groupChannelTable, producer \
                     group: {}, client_id: {}",
                    client_channel_info.channel().remote_address(),
                    remote_addr,
                    group,
                    client_channel_info.client_id()
                );

                // Check if group is now empty
                if channel_map.is_empty() {
                    empty_groups.insert(group.clone());
                }
            }
        }

        // Remove from clientChannelTable (outside of group_channel_table operations)
        for (_, client_channel_info) in &channels_to_remove {
            if let Some(entry) = self.client_channel_table.get(client_channel_info.client_id()) {
                if entry.value() == channel {
                    drop(entry); // Release read lock before remove
                    self.client_channel_table.remove(client_channel_info.client_id());
                }
            }
        }

        // Notify listeners
        for (group, client_channel_info) in &channels_to_remove {
            self.call_producer_change_listener(ProducerGroupEvent::ClientUnregister, group, Some(client_channel_info));
        }

        // Atomically remove empty groups
        for group in empty_groups {
            let removed = self
                .group_channel_table
                .remove_if(&group, |_, channel_map| channel_map.is_empty());
            if removed.is_some() {
                info!("unregister a producer group[{}] from groupChannelTable", group);
                self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, &group, None);
            }
        }

        true
    }

    /// Checks if fast channel event processing is enabled.
    ///
    /// # Returns
    /// `true` if fast channel event processing is enabled in broker config, `false` otherwise
    fn is_fast_channel_event_enabled(&self) -> bool {
        self.broker_config
            .as_ref()
            .map(|config| config.enable_fast_channel_event_process)
            .unwrap_or(false)
    }

    /// Notifies all registered producer change listeners of an event.
    ///
    /// # Arguments
    /// * `event` - The type of event (register/unregister)
    /// * `group` - The affected producer group
    /// * `client_channel_info` - The affected client channel info (if applicable)
    fn call_producer_change_listener(
        &self,
        event: ProducerGroupEvent,
        group: &str,
        client_channel_info: Option<&ClientChannelInfo>,
    ) {
        let listeners = self.producer_change_listener_vec.load();
        for listener in listeners.iter() {
            listener.handle(event, group, client_channel_info);
        }
    }
}
