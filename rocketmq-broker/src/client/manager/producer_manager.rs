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

use crate::config::broker_config::BrokerConfig;
use arc_swap::ArcSwap;
use arc_swap::ArcSwapOption;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_protocol::protocol::body::producer_info::ProducerInfo;
use rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::BrokerStatsManager;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionState;
use rocketmq_transport::api::v2::SessionId;
use tracing::info;
use tracing::warn;

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::client_channel_info::ClientSessionInfo;
use crate::client::producer_change_listener::ArcProducerChangeListener;
use crate::client::producer_group_event::ProducerGroupEvent;
use crate::client::session_transition_locks::ClientSessionTransitionGuard;
use crate::client::session_transition_locks::ClientSessionTransitionLocks;
use crate::types::ProducerGroupName;

/// Timeout for considering a producer channel as expired (120 seconds in milliseconds)
const CHANNEL_EXPIRED_TIMEOUT: u64 = 120_000;
/// Number of retry attempts when getting an available channel
const GET_AVAILABLE_CHANNEL_RETRY_COUNT: u32 = 3;

struct ProducerSessionRemoval {
    group: ProducerGroupName,
    group_table_removed: bool,
}

#[derive(Default)]
pub(crate) struct ProducerSessionBatch {
    removals: Vec<ProducerSessionRemoval>,
}

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
    /// Group name -> (stable V2 session -> client identity) mapping.
    group_session_table: Arc<DashMap<ProducerGroupName, DashMap<SessionId, ClientSessionInfo>>>,
    /// Latest canonical V2 session for each producer client identity.
    client_session_table: Arc<DashMap<CheetahString, SessionId>>,
    /// Immutable client identity claimed by each live V2 session.
    session_client_table: Arc<DashMap<SessionId, CheetahString>>,
    /// Reverse lookup used to remove all producer groups owned by a V2 session.
    session_to_groups: Arc<DashMap<SessionId, HashSet<ProducerGroupName>>>,
    /// Striped serialization for canonical client-session multi-index transitions.
    session_transition_locks: Arc<ClientSessionTransitionLocks>,
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
    broker_config: Arc<ArcSwapOption<BrokerConfig>>,
}

/// Shared producer-connection mutation capability for Broker housekeeping.
///
/// The handle shares the live manager state but exposes only inactive-channel scanning and
/// channel-close cleanup. It cannot register producers, select channels, or mutate manager
/// configuration.
pub(crate) struct ProducerConnectionHousekeeping {
    manager: ProducerManager,
}

/// Shared producer registration capability for client heartbeat processing.
///
/// This handle shares the live connection tables but does not expose channel selection,
/// configuration mutation, housekeeping scans, or the complete Broker runtime.
pub(crate) struct ProducerClientRegistration {
    manager: ProducerManager,
}

impl Clone for ProducerClientRegistration {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone_shared_state(),
        }
    }
}

impl ProducerClientRegistration {
    pub(crate) fn register_producer(&self, group: &ProducerGroupName, client: &ClientChannelInfo) {
        self.manager.register_producer(group, client);
    }

    pub(crate) fn unregister_producer(&self, group: &str, client: &ClientChannelInfo) {
        self.manager.unregister_producer(group, client);
    }

    pub(crate) fn register_producer_session(&self, group: &ProducerGroupName, client: ClientSessionInfo) {
        self.manager.register_producer_session(group, client);
    }

    pub(crate) fn register_producer_sessions(&self, groups: Vec<ProducerGroupName>, client: ClientSessionInfo) {
        self.manager.register_producer_sessions(groups, client);
    }

    pub(crate) fn prepare_producer_sessions(
        &self,
        transition: &ClientSessionTransitionGuard<'_>,
        groups: Vec<ProducerGroupName>,
        client: ClientSessionInfo,
    ) -> ProducerSessionBatch {
        self.manager
            .register_producer_sessions_locked(transition, groups, client)
    }

    pub(crate) fn complete_producer_sessions(&self, batch: ProducerSessionBatch) {
        self.manager.dispatch_producer_session_removals(batch.removals);
    }

    pub(crate) fn session_transition_locks(&self) -> Arc<ClientSessionTransitionLocks> {
        Arc::clone(&self.manager.session_transition_locks)
    }

    pub(crate) fn client_id_for_session(&self, session_id: SessionId) -> Option<CheetahString> {
        self.manager
            .session_client_table
            .get(&session_id)
            .map(|entry| entry.clone())
    }

    pub(crate) fn unregister_producer_session(&self, group: &str, session_id: SessionId) {
        self.manager.unregister_producer_session(group, session_id);
    }
}

impl Clone for ProducerConnectionHousekeeping {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone_shared_state(),
        }
    }
}

impl ProducerConnectionHousekeeping {
    pub(crate) fn scan_not_active_channel(&self) {
        self.manager.scan_not_active_channel();
    }

    pub(crate) fn do_channel_close_event(&self, remote_addr: &str, channel: &Channel) -> bool {
        self.manager.do_channel_close_event(remote_addr, channel)
    }

    pub(crate) fn do_session_close_event(&self, session_id: SessionId) -> bool {
        self.manager.do_session_close_event(session_id)
    }
}

/// Shared read capability for producer channel selection and connection snapshots.
///
/// This view deliberately omits broker configuration and mutation hooks so transaction checking
/// and administration queries cannot retain the complete producer manager or broker runtime.
#[derive(Clone)]
pub(crate) struct ProducerChannelRegistry {
    group_channel_table: Arc<DashMap<ProducerGroupName, DashMap<Channel, ClientChannelInfo>>>,
    group_session_table: Arc<DashMap<ProducerGroupName, DashMap<SessionId, ClientSessionInfo>>>,
    positive_atomic_counter: Arc<AtomicI32>,
}

impl ProducerChannelRegistry {
    pub(crate) fn get_available_channel(&self, group: Option<&ProducerGroupName>) -> Option<Channel> {
        select_available_channel(&self.group_channel_table, &self.positive_atomic_counter, group)
    }

    pub(crate) fn producer_table(&self) -> ProducerTableInfo {
        producer_table_snapshot(&self.group_channel_table, &self.group_session_table)
    }
}

/// Shared read capability for resolving a producer reply channel by client identifier.
///
/// The registry retains only the live client-channel index, so reply processing cannot access
/// producer registration, housekeeping, configuration, or group-selection operations.
#[derive(Clone)]
pub(crate) struct ProducerReplyChannelRegistry {
    client_channel_table: Arc<DashMap<CheetahString, Channel>>,
}

impl ProducerReplyChannelRegistry {
    pub(crate) fn find_channel(&self, client_id: &str) -> Option<Channel> {
        self.client_channel_table
            .get(client_id)
            .map(|entry| entry.value().clone())
    }
}

fn select_available_channel(
    group_channel_table: &DashMap<ProducerGroupName, DashMap<Channel, ClientChannelInfo>>,
    positive_atomic_counter: &AtomicI32,
    group: Option<&ProducerGroupName>,
) -> Option<Channel> {
    let group = group?;
    let channels = {
        let channel_map = group_channel_table.get(group)?;
        if channel_map.is_empty() {
            warn!("Channel list is empty. group={}", group);
            return None;
        }
        channel_map.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>()
    };
    let size = channels.len();
    let index = positive_atomic_counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    let mut index = index.unsigned_abs() as usize % size;
    let mut last_healthy_channel = None;
    for _ in 0..GET_AVAILABLE_CHANNEL_RETRY_COUNT {
        let channel = &channels[index];
        if channel.connection_ref().is_healthy() {
            return Some(channel.clone());
        }
        if channel.connection_ref().state() != ConnectionState::Closed {
            last_healthy_channel = Some(channel.clone());
        }
        index = (index + 1) % size;
    }
    last_healthy_channel
}

fn producer_table_snapshot(
    group_channel_table: &DashMap<ProducerGroupName, DashMap<Channel, ClientChannelInfo>>,
    group_session_table: &DashMap<ProducerGroupName, DashMap<SessionId, ClientSessionInfo>>,
) -> ProducerTableInfo {
    let mut producers: HashMap<String, Vec<ProducerInfo>> = HashMap::new();
    for group_entry in group_channel_table.iter() {
        for channel_entry in group_entry.value().iter() {
            let client = channel_entry.value();
            let producer = ProducerInfo::new(
                client.client_id().to_string(),
                client.channel().remote_address().to_string(),
                client.language(),
                client.version(),
                client.last_update_timestamp() as i64,
            );
            producers
                .entry(group_entry.key().to_string())
                .or_default()
                .push(producer);
        }
    }
    for group_entry in group_session_table.iter() {
        for session_entry in group_entry.value().iter() {
            let client = session_entry.value();
            let remote_address = client
                .remote_address()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("session:{:?}", client.session_id()));
            let producer = ProducerInfo::new(
                client.client_id().to_string(),
                remote_address,
                client.language(),
                client.version(),
                client.last_update_timestamp() as i64,
            );
            producers
                .entry(group_entry.key().to_string())
                .or_default()
                .push(producer);
        }
    }
    ProducerTableInfo::from(producers)
}

impl ProducerManager {
    pub(crate) fn client_registration(&self) -> ProducerClientRegistration {
        ProducerClientRegistration {
            manager: self.clone_shared_state(),
        }
    }

    pub(crate) fn connection_housekeeping(&self) -> ProducerConnectionHousekeeping {
        ProducerConnectionHousekeeping {
            manager: self.clone_shared_state(),
        }
    }

    pub(crate) fn clone_shared_state(&self) -> Self {
        Self {
            group_channel_table: Arc::clone(&self.group_channel_table),
            group_session_table: Arc::clone(&self.group_session_table),
            client_session_table: Arc::clone(&self.client_session_table),
            session_client_table: Arc::clone(&self.session_client_table),
            session_to_groups: Arc::clone(&self.session_to_groups),
            session_transition_locks: Arc::clone(&self.session_transition_locks),
            client_channel_table: Arc::clone(&self.client_channel_table),
            channel_to_groups: Arc::clone(&self.channel_to_groups),
            positive_atomic_counter: Arc::clone(&self.positive_atomic_counter),
            producer_change_listener_vec: Arc::clone(&self.producer_change_listener_vec),
            broker_stats_manager: self.broker_stats_manager.clone(),
            broker_config: Arc::clone(&self.broker_config),
        }
    }

    pub(crate) fn channel_registry(&self) -> ProducerChannelRegistry {
        ProducerChannelRegistry {
            group_channel_table: Arc::clone(&self.group_channel_table),
            group_session_table: Arc::clone(&self.group_session_table),
            positive_atomic_counter: Arc::clone(&self.positive_atomic_counter),
        }
    }

    pub(crate) fn reply_channel_registry(&self) -> ProducerReplyChannelRegistry {
        ProducerReplyChannelRegistry {
            client_channel_table: Arc::clone(&self.client_channel_table),
        }
    }

    /// Creates a new producer manager with empty state.
    pub fn new() -> Self {
        Self::new_with_session_transition_locks(Arc::new(ClientSessionTransitionLocks::default()))
    }

    pub(crate) fn new_with_session_transition_locks(
        session_transition_locks: Arc<ClientSessionTransitionLocks>,
    ) -> Self {
        Self {
            group_channel_table: Arc::new(DashMap::new()),
            group_session_table: Arc::new(DashMap::new()),
            client_session_table: Arc::new(DashMap::new()),
            session_client_table: Arc::new(DashMap::new()),
            session_to_groups: Arc::new(DashMap::new()),
            session_transition_locks,
            client_channel_table: Arc::new(DashMap::new()),
            channel_to_groups: Arc::new(DashMap::new()),
            positive_atomic_counter: Arc::new(AtomicI32::new(0)),
            producer_change_listener_vec: Arc::new(ArcSwap::from_pointee(Vec::new())),
            broker_stats_manager: None,
            broker_config: Arc::new(ArcSwapOption::empty()),
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
    /// Shared manager views observe subsequent configuration generations.
    pub fn set_broker_config(&self, broker_config: Arc<BrokerConfig>) {
        self.broker_config.store(Some(broker_config));
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
        self.group_channel_table
            .iter()
            .map(|entry| entry.key().clone())
            .chain(self.group_session_table.iter().map(|entry| entry.key().clone()))
            .collect::<HashSet<_>>()
            .len()
    }

    /// Returns connected producer counts grouped by client language and version.
    pub fn connection_count_by_client_attrs(&self) -> Vec<(rocketmq_protocol::protocol::LanguageCode, i32, i64)> {
        let mut counts: HashMap<(rocketmq_protocol::protocol::LanguageCode, i32), i64> = HashMap::new();
        for group_entry in self.group_channel_table.iter() {
            for channel_entry in group_entry.value().iter() {
                let client = channel_entry.value();
                *counts.entry((client.language(), client.version())).or_default() += 1;
            }
        }
        for group_entry in self.group_session_table.iter() {
            for session_entry in group_entry.value().iter() {
                let client = session_entry.value();
                *counts.entry((client.language(), client.version())).or_default() += 1;
            }
        }
        counts
            .into_iter()
            .map(|((language, version), count)| (language, version, count))
            .collect()
    }

    /// Returns a snapshot of all connected producers organized by group.
    ///
    /// The snapshot reflects the state at the time of the call and may become stale
    /// as producers connect or disconnect.
    pub fn get_producer_table(&self) -> ProducerTableInfo {
        producer_table_snapshot(&self.group_channel_table, &self.group_session_table)
    }

    /// Checks whether a producer group has at least one connected producer.
    ///
    /// # Arguments
    /// * `group` - The producer group name
    ///
    /// # Returns
    /// `true` if the group exists and contains at least one producer, `false` otherwise
    pub fn group_online(&self, group: &str) -> bool {
        let has_channel = self
            .group_channel_table
            .get(group)
            .map(|channels| !channels.is_empty())
            .unwrap_or(false);
        has_channel
            || self
                .group_session_table
                .get(group)
                .is_some_and(|sessions| !sessions.is_empty())
    }

    /// Removes a producer from a group.
    ///
    /// If the removal causes the group to become empty, the group is also removed.
    /// Notifies registered listeners of the unregistration event.
    ///
    /// # Arguments
    /// * `group` - The producer group name
    /// * `client_channel_info` - The client channel information
    pub fn unregister_producer(&self, group: &str, client_channel_info: &ClientChannelInfo) {
        let mut removed_info: Option<ClientChannelInfo> = None;
        let mut is_group_empty = false;

        // Atomically remove producer from group table
        if let Some(channel_table) = self.group_channel_table.get(group) {
            if let Some((_channel, old)) = channel_table.remove(client_channel_info.channel()) {
                removed_info = Some(old);
                is_group_empty = channel_table.is_empty();
            }
        }

        // Process removal without holding group table locks
        if let Some(old) = removed_info {
            // Remove from clientChannelTable only if the channel matches
            if let Some(entry) = self.client_channel_table.get(old.client_id()) {
                if entry.value() == client_channel_info.channel() {
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
                if let Some(mut entry) = self.channel_to_groups.get_mut(client_channel_info.channel()) {
                    entry.remove(group);
                    if entry.is_empty() {
                        drop(entry);
                        self.channel_to_groups.remove(client_channel_info.channel());
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
                    if !self.group_online(group) {
                        self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, group, None);
                    }
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
        if let Some(config) = self.broker_config.load_full() {
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

    fn register_producer_session(&self, group: &ProducerGroupName, client: ClientSessionInfo) {
        self.register_producer_sessions(vec![group.clone()], client);
    }

    fn register_producer_sessions(&self, groups: Vec<ProducerGroupName>, client: ClientSessionInfo) {
        let transition = self
            .session_transition_locks
            .lock(client.client_id(), client.session_id());
        let batch = self.register_producer_sessions_locked(&transition, groups, client);
        drop(transition);
        self.dispatch_producer_session_removals(batch.removals);
    }

    fn register_producer_sessions_locked(
        &self,
        transition: &ClientSessionTransitionGuard<'_>,
        groups: Vec<ProducerGroupName>,
        client: ClientSessionInfo,
    ) -> ProducerSessionBatch {
        assert!(
            self.session_transition_locks
                .covers(transition, client.client_id(), client.session_id()),
            "producer session mutation requires the matching transition guard"
        );
        if self
            .session_client_table
            .get(&client.session_id())
            .is_some_and(|current| current.as_str() != client.client_id().as_str())
        {
            warn!(
                "ignore producer heartbeat that changes client identity for live session {:?}",
                client.session_id()
            );
            return ProducerSessionBatch::default();
        }
        let replaced_session = self
            .client_session_table
            .get(client.client_id())
            .map(|entry| *entry.value())
            .filter(|session_id| *session_id != client.session_id());
        if groups.is_empty() {
            let removals = replaced_session
                .map(|session_id| self.unregister_producer_session_all_locked(session_id))
                .unwrap_or_default();
            return ProducerSessionBatch { removals };
        }
        if self
            .broker_config
            .load_full()
            .is_some_and(|config| !config.enable_register_producer && config.reject_transaction_message)
        {
            return ProducerSessionBatch::default();
        }
        let mut unique_groups = HashSet::with_capacity(groups.len());
        for group in groups {
            if !unique_groups.insert(group.clone()) {
                continue;
            }
            let sessions = self.group_session_table.entry(group.clone()).or_default();
            if let Some(mut existing) = sessions.get_mut(&client.session_id()) {
                existing.refresh_from(&client);
            } else {
                sessions.insert(client.session_id(), client.clone());
            }
            drop(sessions);
            self.session_to_groups
                .entry(client.session_id())
                .or_default()
                .insert(group.clone());
        }
        self.client_session_table
            .insert(client.client_id().clone(), client.session_id());
        self.session_client_table
            .insert(client.session_id(), client.client_id().clone());
        let removals = replaced_session
            .map(|session_id| self.unregister_producer_session_all_locked(session_id))
            .unwrap_or_default();
        info!(
            "producer session heartbeat applied, session: {:?} clientId: {}",
            client.session_id(),
            client.client_id()
        );
        ProducerSessionBatch { removals }
    }

    fn unregister_producer_session(&self, group: &str, session_id: SessionId) {
        let Some(client_id) = self.session_client_table.get(&session_id).map(|entry| entry.clone()) else {
            return;
        };
        let transition = self.session_transition_locks.lock(&client_id, session_id);
        let removal = self.unregister_producer_session_locked(group, session_id);
        drop(transition);
        self.dispatch_producer_session_removals(removal.into_iter().collect());
    }

    fn unregister_producer_session_locked(&self, group: &str, session_id: SessionId) -> Option<ProducerSessionRemoval> {
        let sessions = self.group_session_table.get(group)?;
        let (_, client) = sessions.remove(&session_id)?;
        let sessions_empty = sessions.is_empty();
        drop(sessions);
        let group_table_removed = if sessions_empty {
            self.group_session_table
                .remove_if(group, |_, current| current.is_empty())
                .is_some()
        } else {
            false
        };
        let session_has_groups = self.remove_producer_session_group_index(session_id, group);
        if !session_has_groups {
            self.client_session_table
                .remove_if(client.client_id(), |_, current| *current == session_id);
            self.session_client_table
                .remove_if(&session_id, |_, current| current == client.client_id());
        }
        Some(ProducerSessionRemoval {
            group: group.into(),
            group_table_removed,
        })
    }

    fn unregister_producer_session_all_locked(&self, session_id: SessionId) -> Vec<ProducerSessionRemoval> {
        let groups = self
            .session_to_groups
            .get(&session_id)
            .map(|entry| entry.value().clone())
            .unwrap_or_default();
        groups
            .into_iter()
            .filter_map(|group| self.unregister_producer_session_locked(group.as_str(), session_id))
            .collect()
    }

    fn do_session_close_event(&self, session_id: SessionId) -> bool {
        let Some(client_id) = self.session_client_table.get(&session_id).map(|entry| entry.clone()) else {
            return false;
        };
        let transition = self.session_transition_locks.lock(&client_id, session_id);
        let removals = self.unregister_producer_session_all_locked(session_id);
        drop(transition);
        let removed = !removals.is_empty();
        self.dispatch_producer_session_removals(removals);
        removed
    }

    fn dispatch_producer_session_removals(&self, removals: Vec<ProducerSessionRemoval>) {
        for removal in removals {
            if removal.group_table_removed && !self.group_online(&removal.group) {
                self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, &removal.group, None);
            }
        }
    }

    fn remove_producer_session_group_index(&self, session_id: SessionId, group: &str) -> bool {
        let Some(mut groups) = self.session_to_groups.get_mut(&session_id) else {
            return false;
        };
        groups.remove(group);
        let has_groups = !groups.is_empty();
        drop(groups);
        if !has_groups {
            self.session_to_groups
                .remove_if(&session_id, |_, current| current.is_empty());
        }
        has_groups
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
        select_available_channel(&self.group_channel_table, &self.positive_atomic_counter, group)
    }

    /// Removes producers that have not sent a heartbeat within the timeout period.
    ///
    /// This method should be invoked periodically by a background task. Producers inactive
    /// for more than 120 seconds are removed and their channels are closed. Listeners are
    /// notified of each removal.
    pub fn scan_not_active_channel(&self) {
        let current_time = current_millis();

        let expired_sessions = self
            .group_session_table
            .iter()
            .flat_map(|group_entry| {
                group_entry
                    .value()
                    .iter()
                    .filter(|session_entry| {
                        current_time.saturating_sub(session_entry.last_update_timestamp()) > CHANNEL_EXPIRED_TIMEOUT
                    })
                    .map(|session_entry| {
                        (
                            group_entry.key().clone(),
                            *session_entry.key(),
                            session_entry.client_id().clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for (group, session_id, client_id) in expired_sessions {
            let transition = self.session_transition_locks.lock(&client_id, session_id);
            let removed = self.group_session_table.get(&group).and_then(|sessions| {
                sessions
                    .remove_if(&session_id, |_, info| {
                        current_time.saturating_sub(info.last_update_timestamp()) > CHANNEL_EXPIRED_TIMEOUT
                    })
                    .map(|(_, info)| info)
            });
            let removal = removed.map(|client| {
                let group_table_removed = self
                    .group_session_table
                    .remove_if(&group, |_, sessions| sessions.is_empty())
                    .is_some();
                let session_has_groups = self.remove_producer_session_group_index(session_id, &group);
                if !session_has_groups {
                    self.client_session_table
                        .remove_if(client.client_id(), |_, current| *current == session_id);
                    self.session_client_table
                        .remove_if(&session_id, |_, current| current == client.client_id());
                }
                ProducerSessionRemoval {
                    group,
                    group_table_removed,
                }
            });
            drop(transition);
            if let Some(removal) = removal {
                self.dispatch_producer_session_removals(vec![removal]);
            }
        }

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
                if !self.group_online(&group) {
                    self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, &group, None);
                }
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
                if !self.group_online(&group) {
                    self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, &group, None);
                }
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
                if !self.group_online(&group) {
                    self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, &group, None);
                }
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
            .load_full()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;

    use rocketmq_protocol::protocol::LanguageCode;
    use rocketmq_transport::test_support::session_id_for_test;
    use rocketmq_transport::test_support::Connection;
    use tokio::net::TcpStream;

    use super::*;

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        rocketmq_transport::test_support::TestChannelBuilder::new(connection, crate::test_task_group("channel"))
            .addresses(local_addr, local_addr)
            .build()
            .expect("build test channel")
    }

    #[tokio::test]
    async fn channel_registry_observes_later_producer_registration() {
        let manager = ProducerManager::new();
        let registry = manager.channel_registry();
        let group = CheetahString::from_static_str("transaction-producer");
        assert!(registry.get_available_channel(Some(&group)).is_none());
        assert!(registry.producer_table().data().is_empty());

        let channel = create_test_channel().await;
        let client = ClientChannelInfo::new(channel.clone(), "client-id".into(), LanguageCode::default(), 1);
        manager.register_producer(&group, &client);

        assert_eq!(registry.get_available_channel(Some(&group)), Some(channel));
        let snapshot = registry.producer_table();
        let producers = snapshot.data().get(group.as_str()).expect("producer group snapshot");
        assert_eq!(producers.len(), 1);
        assert_eq!(producers[0].client_id(), "client-id");
    }

    #[tokio::test]
    async fn reply_channel_registry_observes_later_producer_registration() {
        let manager = ProducerManager::new();
        let registry = manager.reply_channel_registry();
        assert!(registry.find_channel("reply-client-id").is_none());

        let channel = create_test_channel().await;
        let group = CheetahString::from_static_str("reply-producer");
        let client = ClientChannelInfo::new(channel.clone(), "reply-client-id".into(), LanguageCode::default(), 1);
        manager.register_producer(&group, &client);

        assert_eq!(registry.find_channel("reply-client-id"), Some(channel));
    }

    #[test]
    fn shared_manager_observes_later_broker_config_generations() {
        let manager = ProducerManager::new();
        let shared = manager.clone_shared_state();
        assert!(!shared.is_fast_channel_event_enabled());

        manager.set_broker_config(Arc::new(BrokerConfig {
            enable_fast_channel_event_process: true,
            ..BrokerConfig::default()
        }));

        assert!(shared.is_fast_channel_event_enabled());
    }

    #[test]
    fn narrow_registry_does_not_capture_producer_configuration() {
        let source = include_str!("producer_manager.rs");
        let registry_start = source.find("pub(crate) struct ProducerChannelRegistry").unwrap();
        let registry_end = source[registry_start..]
            .find("impl ProducerManager")
            .map(|offset| registry_start + offset)
            .unwrap();
        let registry_source = &source[registry_start..registry_end];

        assert!(!registry_source.contains("broker_config"));
    }

    #[test]
    fn producer_session_metadata_is_live_in_indexes_metrics_and_snapshots() {
        let manager = ProducerManager::new();
        let group = CheetahString::from_static_str("session-producer");
        let session_id = session_id_for_test(61);
        manager.register_producer_session(
            &group,
            ClientSessionInfo::new(
                session_id,
                "session-client".into(),
                Some("127.0.0.1:10611".into()),
                LanguageCode::RUST,
                7,
            ),
        );

        assert_eq!(
            manager.client_session_table.get("session-client").map(|entry| *entry),
            Some(session_id)
        );
        assert_eq!(
            manager.session_to_groups.get(&session_id).map(|groups| groups.len()),
            Some(1)
        );
        assert_eq!(
            manager.connection_count_by_client_attrs(),
            vec![(LanguageCode::RUST, 7, 1)]
        );
        for snapshot in [
            manager.get_producer_table(),
            manager.channel_registry().producer_table(),
        ] {
            let producers = snapshot
                .data()
                .get(group.as_str())
                .expect("V2 group in producer snapshot");
            assert_eq!(producers.len(), 1);
            assert_eq!(producers[0].client_id(), "session-client");
            assert_eq!(producers[0].remote_ip(), "127.0.0.1:10611");
            assert_eq!(producers[0].version(), 7);
        }

        manager.unregister_producer_session(group.as_str(), session_id);
        assert!(!manager.client_session_table.contains_key("session-client"));
        assert!(!manager.session_to_groups.contains_key(&session_id));
    }

    #[test]
    fn producer_session_reconnect_and_stale_disconnect_preserve_the_new_session() {
        let manager = ProducerManager::new();
        let old_session = session_id_for_test(62);
        let new_session = session_id_for_test(63);
        let group_a = CheetahString::from_static_str("producer-reconnect-a");
        let group_b = CheetahString::from_static_str("producer-reconnect-b");
        for group in [&group_a, &group_b] {
            manager.register_producer_session(
                group,
                ClientSessionInfo::new(old_session, "same-client".into(), None, LanguageCode::RUST, 1),
            );
        }
        assert_eq!(
            manager.session_to_groups.get(&old_session).map(|groups| groups.len()),
            Some(2)
        );

        manager.register_producer_session(
            &group_a,
            ClientSessionInfo::new(new_session, "same-client".into(), None, LanguageCode::RUST, 2),
        );
        assert!(!manager.session_to_groups.contains_key(&old_session));
        assert!(!manager.group_online(group_b.as_str()));
        assert_eq!(
            manager.client_session_table.get("same-client").map(|entry| *entry),
            Some(new_session)
        );

        manager.unregister_producer_session(group_a.as_str(), old_session);
        assert!(manager.group_online(group_a.as_str()));
        let snapshot = manager.get_producer_table();
        let producers = snapshot
            .data()
            .get(group_a.as_str())
            .expect("replacement group snapshot");
        assert_eq!(producers.len(), 1);
        assert_eq!(producers[0].version(), 2);

        manager.unregister_producer_session(group_a.as_str(), new_session);
        assert!(!manager.group_online(group_a.as_str()));
        assert!(!manager.client_session_table.contains_key("same-client"));
    }

    #[test]
    fn concurrent_producer_reconnect_has_one_canonical_session() {
        let manager = ProducerManager::new();
        let first_registration = manager.client_registration();
        let second_registration = first_registration.clone();
        let group = CheetahString::from_static_str("concurrent-producer-group");
        let first_session = session_id_for_test(6_501);
        let second_session = session_id_for_test(6_502);
        let barrier = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            let first_group = group.clone();
            let first_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                first_barrier.wait();
                first_registration.register_producer_session(
                    &first_group,
                    ClientSessionInfo::new(first_session, "concurrent-client".into(), None, LanguageCode::RUST, 1),
                );
            });
            let second_group = group.clone();
            let second_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                second_barrier.wait();
                second_registration.register_producer_session(
                    &second_group,
                    ClientSessionInfo::new(second_session, "concurrent-client".into(), None, LanguageCode::RUST, 2),
                );
            });
        });

        let canonical = *manager
            .client_session_table
            .get("concurrent-client")
            .expect("one canonical producer session");
        let loser = if canonical == first_session {
            second_session
        } else {
            first_session
        };
        let sessions = manager
            .group_session_table
            .get(&group)
            .expect("producer group remains live");
        assert_eq!(sessions.len(), 1);
        assert!(sessions.contains_key(&canonical));
        assert!(!sessions.contains_key(&loser));
        drop(sessions);
        assert!(!manager.session_to_groups.contains_key(&loser));

        manager
            .client_registration()
            .unregister_producer_session(group.as_str(), loser);
        assert_eq!(
            manager
                .client_session_table
                .get("concurrent-client")
                .map(|entry| *entry),
            Some(canonical)
        );
    }

    #[tokio::test]
    async fn producer_group_lives_until_both_channel_and_session_tables_are_empty() {
        let manager = ProducerManager::new();
        let group = CheetahString::from_static_str("mixed-producer-group");
        let channel = create_test_channel().await;
        let legacy = ClientChannelInfo::new(channel, "legacy-client".into(), LanguageCode::RUST, 1);
        manager.register_producer(&group, &legacy);
        let session_id = session_id_for_test(64);
        manager.register_producer_session(
            &group,
            ClientSessionInfo::new(session_id, "session-client".into(), None, LanguageCode::RUST, 2),
        );

        manager.unregister_producer(group.as_str(), &legacy);
        assert!(manager.group_online(group.as_str()));
        assert_eq!(manager.get_producer_table().data()[group.as_str()].len(), 1);
        manager.unregister_producer_session(group.as_str(), session_id);
        assert!(!manager.group_online(group.as_str()));
    }

    #[test]
    fn rejected_producer_session_registration_creates_no_empty_tables() {
        let manager = ProducerManager::new();
        manager.set_broker_config(Arc::new(BrokerConfig {
            enable_register_producer: false,
            reject_transaction_message: true,
            ..BrokerConfig::default()
        }));
        manager.register_producer_session(
            &CheetahString::from_static_str("rejected-session-group"),
            ClientSessionInfo::new(
                session_id_for_test(65),
                "rejected-client".into(),
                None,
                LanguageCode::RUST,
                1,
            ),
        );

        assert!(manager.group_session_table.is_empty());
        assert!(manager.client_session_table.is_empty());
        assert!(manager.session_client_table.is_empty());
        assert!(manager.session_to_groups.is_empty());
        assert_eq!(manager.group_size(), 0);
        assert!(manager.get_producer_table().data().is_empty());
        assert!(manager.connection_count_by_client_attrs().is_empty());
    }

    #[test]
    fn concurrent_producer_heartbeat_batches_preserve_every_winning_group() {
        let manager = ProducerManager::new();
        let first_registration = manager.client_registration();
        let second_registration = first_registration.clone();
        let groups = vec![
            CheetahString::from_static_str("batch-producer-a"),
            CheetahString::from_static_str("batch-producer-b"),
        ];
        let first_session = session_id_for_test(6_601);
        let second_session = session_id_for_test(6_602);
        let barrier = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            let first_groups = groups.clone();
            let first_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                first_barrier.wait();
                first_registration.register_producer_sessions(
                    first_groups,
                    ClientSessionInfo::new(first_session, "batch-client".into(), None, LanguageCode::RUST, 1),
                );
            });
            let second_groups = groups.clone();
            let second_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                second_barrier.wait();
                second_registration.register_producer_sessions(
                    second_groups,
                    ClientSessionInfo::new(second_session, "batch-client".into(), None, LanguageCode::RUST, 2),
                );
            });
        });

        let canonical = *manager
            .client_session_table
            .get("batch-client")
            .expect("canonical batch producer session");
        let loser = if canonical == first_session {
            second_session
        } else {
            first_session
        };
        for group in &groups {
            let sessions = manager.group_session_table.get(group).expect("winning group remains");
            assert_eq!(sessions.len(), 1);
            assert!(sessions.contains_key(&canonical));
            assert!(!sessions.contains_key(&loser));
        }
        assert_eq!(
            manager.session_to_groups.get(&canonical).map(|entry| entry.len()),
            Some(2)
        );
        assert!(!manager.session_to_groups.contains_key(&loser));
        assert!(!manager.session_client_table.contains_key(&loser));
    }

    #[test]
    fn producer_session_close_cleans_all_groups_and_stale_close_preserves_replacement() {
        let manager = ProducerManager::new();
        let registration = manager.client_registration();
        let housekeeping = manager.connection_housekeeping();
        let groups = vec![
            CheetahString::from_static_str("close-producer-a"),
            CheetahString::from_static_str("close-producer-b"),
        ];
        let old_session = session_id_for_test(6_701);
        registration.register_producer_sessions(
            groups.clone(),
            ClientSessionInfo::new(old_session, "close-client".into(), None, LanguageCode::RUST, 1),
        );
        assert!(housekeeping.do_session_close_event(old_session));
        assert!(groups.iter().all(|group| !manager.group_online(group)));
        assert!(!manager.session_to_groups.contains_key(&old_session));

        let new_session = session_id_for_test(6_702);
        registration.register_producer_sessions(
            groups.clone(),
            ClientSessionInfo::new(new_session, "close-client".into(), None, LanguageCode::RUST, 2),
        );
        assert!(!housekeeping.do_session_close_event(old_session));
        assert!(groups.iter().all(|group| manager.group_online(group)));
        assert_eq!(
            manager.client_session_table.get("close-client").map(|entry| *entry),
            Some(new_session)
        );
    }

    #[test]
    fn producer_session_identity_is_immutable() {
        let manager = ProducerManager::new();
        let session_id = session_id_for_test(6_801);
        let group = CheetahString::from_static_str("identity-producer-a");
        manager.register_producer_session(
            &group,
            ClientSessionInfo::new(session_id, "identity-a".into(), None, LanguageCode::RUST, 1),
        );
        manager.register_producer_session(
            &CheetahString::from_static_str("identity-producer-b"),
            ClientSessionInfo::new(session_id, "identity-b".into(), None, LanguageCode::RUST, 2),
        );
        assert_eq!(
            manager.session_client_table.get(&session_id).map(|entry| entry.clone()),
            Some("identity-a".into())
        );
        assert!(!manager.group_session_table.contains_key("identity-producer-b"));

        manager.unregister_producer_session(group.as_str(), session_id);
        assert!(!manager.session_client_table.contains_key(&session_id));
    }
}
