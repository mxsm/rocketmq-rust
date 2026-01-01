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
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::TimeUtils::get_current_millis;
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

/// Timeout for considering a producer channel as expired (120 seconds in milliseconds)
const CHANNEL_EXPIRED_TIMEOUT: u64 = 120_000;
/// Number of retry attempts when getting an available channel
const GET_AVAILABLE_CHANNEL_RETRY_COUNT: u32 = 3;

/// Manages producer client connections and their lifecycle.
///
/// This manager maintains:
/// - A two-level mapping: producer_group → channel → ClientChannelInfo
/// - A client_id → channel mapping for quick lookups
/// - Automatic expiration and cleanup of inactive producers
///
/// # Thread Safety
/// All operations are thread-safe using concurrent data structures.
pub struct ProducerManager {
    /// Group name -> (Channel -> ClientChannelInfo) mapping
    group_channel_table: Arc<DashMap<CheetahString, DashMap<Channel, ClientChannelInfo>>>,
    /// Client ID -> Channel mapping for quick channel lookup by client ID
    client_channel_table: Arc<DashMap<CheetahString, Channel>>,
    /// Counter for round-robin channel selection
    positive_atomic_counter: Arc<AtomicI32>,
    /// Listeners notified on producer registration/unregistration events
    producer_change_listener_vec: Vec<ArcProducerChangeListener>,
    /// Optional broker statistics manager (set once during initialization)
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
}

impl ProducerManager {
    /// Creates a new ProducerManager instance.
    ///
    /// Initializes empty producer tables and prepares for producer registration.
    pub fn new() -> Self {
        Self {
            group_channel_table: Arc::new(DashMap::new()),
            client_channel_table: Arc::new(DashMap::new()),
            positive_atomic_counter: Arc::new(AtomicI32::new(0)),
            producer_change_listener_vec: vec![],
            broker_stats_manager: None,
        }
    }

    /// Sets the broker statistics manager for tracking producer metrics.
    pub fn set_broker_stats_manager(&mut self, broker_stats_manager: Arc<BrokerStatsManager>) {
        self.broker_stats_manager = Some(broker_stats_manager);
    }

    /// Appends a producer change listener to be notified of registration events.
    pub fn append_producer_change_listener(&mut self, producer_change_listener: ArcProducerChangeListener) {
        self.producer_change_listener_vec.push(producer_change_listener);
    }
}

impl ProducerManager {
    /// Get a snapshot of all producer clients organized by group.
    ///
    /// Collects information about all currently connected producers and
    /// organizes them by producer group.
    ///
    /// # Returns
    /// A table containing producer information for all connected producers
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

    /// Checks if a producer group has any online (connected) producers.
    ///
    /// # Arguments
    /// * `group` - The producer group name to check
    ///
    /// # Returns
    /// `true` if the group has at least one connected producer, `false` otherwise
    pub fn group_online(&self, group: &str) -> bool {
        self.group_channel_table
            .get(group)
            .map(|channels| !channels.is_empty())
            .unwrap_or(false)
    }

    /// Unregisters a producer client from a specific group.
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

        //Remove from group_channel_table and collect info
        if let Some(channel_table) = self.group_channel_table.get(group) {
            if let Some((_channel, old)) = channel_table.remove(ctx.channel()) {
                removed_info = Some(old);
                // Only check empty after successful removal
                is_group_empty = channel_table.is_empty();
            }
        }
        // Lock released here

        //Handle the removed producer (outside of group_channel_table lock)
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

            //Remove empty group (only if we removed a producer)
            // Re-check if group is still empty to avoid TOCTOU race condition
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

    /// Registers or updates a producer client in a specific group.
    ///
    /// If the producer already exists, updates its last update timestamp.
    /// If it's a new producer, adds it to the group and client tables.
    ///
    /// # Arguments
    /// * `group` - The producer group name
    /// * `client_channel_info` - The client channel information containing client_id, channel, etc.
    ///
    /// # Thread Safety
    /// This method is thread-safe. Uses DashMap's atomic operations to ensure consistency.
    #[allow(clippy::mutable_key_type)]
    pub fn register_producer(&self, group: &CheetahString, client_channel_info: &ClientChannelInfo) {
        // Update group_channel_table
        {
            let channel_table = self.group_channel_table.entry(group.clone()).or_default();

            // Check if this channel is already registered
            if let Some(mut existing_info) = channel_table.get_mut(client_channel_info.channel()) {
                // Update timestamp for existing producer
                existing_info.set_last_update_timestamp(get_current_millis());
                return;
            }

            // New producer - insert into channel table
            channel_table.insert(client_channel_info.channel().clone(), client_channel_info.clone());
        }
        // channel_table lock released here

        //Update clientChannelTable (outside of group_channel_table lock)
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

    /// Finds a channel by client ID.
    ///
    /// # Arguments
    /// * `client_id` - The client identifier
    ///
    /// # Returns
    /// The channel associated with this client ID, if found
    pub fn find_channel(&self, client_id: &str) -> Option<Channel> {
        self.client_channel_table
            .get(client_id)
            .map(|entry| entry.value().clone())
    }

    /// Gets an available (active and writable) channel from a producer group using round-robin
    /// selection.
    ///
    /// # Arguments
    /// * `group` - The producer group name
    ///
    /// # Returns
    /// An available channel from the group, or None if no healthy channel is found
    pub fn get_available_channel(&self, group: Option<&CheetahString>) -> Option<Channel> {
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
        // Lock released here

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

        // Try to find a healthy channel
        for _ in 0..=GET_AVAILABLE_CHANNEL_RETRY_COUNT {
            let channel = &channels[index];
            let is_healthy = channel.connection_ref().is_healthy();

            if is_healthy {
                return Some(channel.clone());
            }

            // Keep updating to last active channel as fallback (matching Java behavior)
            // is_active means state != Closed (similar to Java's channel.isActive())
            if channel.connection_ref().state() != ConnectionState::Closed {
                last_healthy_channel = Some(channel.clone());
            }

            index = (index + 1) % size;
        }

        last_healthy_channel
    }

    /// Scans and removes inactive producer channels that have exceeded the timeout.
    ///
    /// This method should be called periodically to clean up expired producers.
    ///
    /// # Timeout
    /// Producers that haven't sent heartbeat for more than CHANNEL_EXPIRED_TIMEOUT (120 seconds)
    /// will be removed.
    pub fn scan_not_active_channel(&self) {
        let current_time = get_current_millis();

        // Collect all expired channels without holding locks during modification
        // Structure: (group_name, channel, client_info)
        let mut expired_channels: Vec<(CheetahString, Channel, ClientChannelInfo)> = Vec::new();

        // Collect expired channels - only hold read locks here
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
        // All read locks released here

        // Use HashSet to avoid duplicate group removals
        let mut empty_groups: std::collections::HashSet<CheetahString> = std::collections::HashSet::new();

        // Remove expired channels one by one
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

            // Call listener outside of any locks
            self.call_producer_change_listener(ProducerGroupEvent::ClientUnregister, &group, Some(&info));

            // Close the expired channel (matching Java's RemotingHelper.closeChannel)
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

    /// Handles channel close events and removes the associated producer from all groups.
    ///
    /// This method is typically called by the connection layer when a channel is closed.
    ///
    /// # Arguments
    /// * `remote_addr` - The remote address of the closed channel
    /// * `channel` - The closed channel
    ///
    /// # Returns
    /// `true` if at least one producer was removed, `false` otherwise
    pub fn do_channel_close_event(&self, remote_addr: &str, channel: &Channel) -> bool {
        // Collect all groups that contain this channel
        // Structure: (group_name, client_channel_info)
        let mut channels_to_remove: Vec<(CheetahString, ClientChannelInfo)> = Vec::new();

        // Only hold read lock during collection
        for group_entry in self.group_channel_table.iter() {
            let (group, channel_map) = (group_entry.key(), group_entry.value());
            if let Some(entry) = channel_map.get(channel) {
                channels_to_remove.push((group.clone(), entry.value().clone()));
            }
        }
        // Read locks released here

        if channels_to_remove.is_empty() {
            return false;
        }

        let mut empty_groups: std::collections::HashSet<CheetahString> = std::collections::HashSet::new();

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

        //Call listeners (outside of any locks)
        for (group, client_channel_info) in &channels_to_remove {
            self.call_producer_change_listener(ProducerGroupEvent::ClientUnregister, group, Some(client_channel_info));
        }

        // Remove empty groups
        // Use remove_if to avoid TOCTOU race - only remove if still empty
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
        for listener in self.producer_change_listener_vec.iter() {
            listener.handle(event, group, client_channel_info);
        }
    }
}
