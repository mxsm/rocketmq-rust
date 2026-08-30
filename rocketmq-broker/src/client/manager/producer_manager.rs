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
use std::sync::OnceLock;
use std::sync::Weak;

use crate::config::broker_config::BrokerConfig;
use arc_swap::ArcSwap;
use arc_swap::ArcSwapOption;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_protocol::protocol::body::producer_info::ProducerInfo;
use rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::BrokerStatsManager;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionRegistry;
use tracing::info;
use tracing::warn;

use crate::client::client_session_info::ClientSessionInfo;
use crate::client::client_session_info::ClientSessionRetirement;
use crate::client::client_session_info::ClientSessionTransport;
use crate::client::producer_change_listener::ArcProducerChangeListener;
use crate::client::producer_group_event::ProducerGroupEvent;
use crate::client::session_transition_locks::ClientSessionTransitionGuard;
use crate::client::session_transition_locks::ClientSessionTransitionLocks;
use crate::types::ProducerGroupName;

/// Timeout for considering a producer session expired (120 seconds in milliseconds).
const CHANNEL_EXPIRED_TIMEOUT: u64 = 120_000;

struct ProducerSessionRemoval {
    group: ProducerGroupName,
    group_table_removed: bool,
}

#[derive(Default)]
pub(crate) struct ProducerSessionBatch {
    removals: Vec<ProducerSessionRemoval>,
    retirements: Vec<ClientSessionRetirement>,
}

/// Manages producer client sessions and their lifecycle.
///
/// Maintains a two-level mapping from producer groups to sessions and client information,
/// with additional indices for efficient lookups and event processing. Automatically expires
/// and removes inactive producer sessions.
///
/// All operations are thread-safe through lock-free or internally synchronized data structures.
pub struct ProducerManager {
    /// Group name -> (stable session -> client identity) mapping.
    group_session_table: Arc<DashMap<ProducerGroupName, DashMap<SessionId, ClientSessionInfo>>>,
    /// Latest canonical session for each producer client identity.
    client_session_table: Arc<DashMap<CheetahString, SessionId>>,
    /// Immutable client identity claimed by each live session.
    session_client_table: Arc<DashMap<SessionId, CheetahString>>,
    /// Reverse lookup used to remove all producer groups owned by a session.
    session_to_groups: Arc<DashMap<SessionId, HashSet<ProducerGroupName>>>,
    /// Exact-generation typed transport authority for live producer sessions.
    session_transport_table: Arc<DashMap<SessionId, ClientSessionTransport>>,
    /// Weak composition-root resolver used only while applying a session heartbeat.
    session_registry: Arc<OnceLock<Weak<SessionRegistry>>>,
    /// Striped serialization for canonical client-session multi-index transitions.
    session_transition_locks: Arc<ClientSessionTransitionLocks>,
    /// Counter for round-robin session selection.
    positive_atomic_counter: Arc<AtomicI32>,
    /// Listeners notified on producer registration/unregistration events (thread-safe)
    producer_change_listener_vec: Arc<ArcSwap<Vec<ArcProducerChangeListener>>>,
    /// Optional broker statistics manager (set once during initialization)
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    /// Broker configuration for feature toggles
    broker_config: Arc<ArcSwapOption<BrokerConfig>>,
}

/// Shared producer-session mutation capability for Broker housekeeping.
///
/// The handle shares the live manager state but exposes only inactive-session scanning and
/// session-close cleanup. It cannot register producers, select sessions, or mutate manager
/// configuration.
pub(crate) struct ProducerConnectionHousekeeping {
    manager: ProducerManager,
}

/// Shared producer registration capability for client heartbeat processing.
///
/// This handle shares the live session tables but does not expose session selection,
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

    pub(crate) fn complete_producer_sessions(&self, batch: ProducerSessionBatch) -> Vec<ClientSessionRetirement> {
        self.manager.dispatch_producer_session_removals(batch.removals);
        batch.retirements
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

    pub(crate) fn session_is_active(
        &self,
        transition: &ClientSessionTransitionGuard<'_>,
        client_id: &CheetahString,
        session_id: SessionId,
    ) -> bool {
        assert!(
            self.manager
                .session_transition_locks
                .covers(transition, client_id, session_id),
            "session activity check requires the matching transition guard"
        );
        self.manager
            .session_registry
            .get()
            .and_then(Weak::upgrade)
            .is_some_and(|registry| registry.contains(session_id))
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
    pub(crate) fn scan_inactive_sessions(&self) -> Vec<ClientSessionRetirement> {
        self.manager.scan_inactive_sessions()
    }

    #[cfg(test)]
    pub(crate) fn expire_session_for_test(&self, session_id: SessionId) {
        self.manager.expire_session_for_test(session_id);
    }

    pub(crate) fn do_session_close_event(&self, session_id: SessionId) -> bool {
        self.manager.do_session_close_event(session_id)
    }

    pub(crate) fn release_session_binding(&self, session_id: SessionId) {
        self.manager.session_transition_locks.release_binding(session_id);
    }
}

/// Shared read capability for producer session selection and connection snapshots.
///
/// This view deliberately omits broker configuration and mutation hooks so transaction checking
/// and administration queries cannot retain the complete producer manager or broker runtime.
#[derive(Clone)]
pub(crate) struct ProducerSessionRegistry {
    group_session_table: Arc<DashMap<ProducerGroupName, DashMap<SessionId, ClientSessionInfo>>>,
    session_transport_table: Arc<DashMap<SessionId, ClientSessionTransport>>,
    positive_atomic_counter: Arc<AtomicI32>,
}

impl ProducerSessionRegistry {
    pub(crate) fn get_available_session(&self, group: Option<&ProducerGroupName>) -> Option<ClientSessionTransport> {
        let group = group?;
        let sessions = self.group_session_table.get(group)?;
        let candidates = sessions
            .iter()
            .filter_map(|entry| {
                self.session_transport_table
                    .get(entry.key())
                    .map(|transport| transport.clone())
            })
            .collect::<Vec<_>>();
        drop(sessions);
        if candidates.is_empty() {
            return None;
        }
        let index = self
            .positive_atomic_counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            .unsigned_abs() as usize
            % candidates.len();
        candidates.get(index).cloned()
    }

    pub(crate) fn producer_table(&self) -> ProducerTableInfo {
        producer_table_snapshot(&self.group_session_table)
    }
}

/// Shared read capability for resolving a producer reply session by client identifier.
///
/// The registry retains only the live client-session index, so reply processing cannot access
/// producer registration, housekeeping, configuration, or group-selection operations.
#[derive(Clone)]
pub(crate) struct ProducerReplySessionRegistry {
    client_session_table: Arc<DashMap<CheetahString, SessionId>>,
    session_transport_table: Arc<DashMap<SessionId, ClientSessionTransport>>,
}

impl ProducerReplySessionRegistry {
    pub(crate) fn find_request_sender(&self, client_id: &str) -> Option<rocketmq_transport::api::ServerRequestSender> {
        let session_id = *self.client_session_table.get(client_id)?.value();
        self.session_transport_table
            .get(&session_id)
            .map(|transport| transport.request_sender())
    }
}

fn producer_table_snapshot(
    group_session_table: &DashMap<ProducerGroupName, DashMap<SessionId, ClientSessionInfo>>,
) -> ProducerTableInfo {
    let mut producers: HashMap<String, Vec<ProducerInfo>> = HashMap::new();
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
            group_session_table: Arc::clone(&self.group_session_table),
            client_session_table: Arc::clone(&self.client_session_table),
            session_client_table: Arc::clone(&self.session_client_table),
            session_to_groups: Arc::clone(&self.session_to_groups),
            session_transport_table: Arc::clone(&self.session_transport_table),
            session_registry: Arc::clone(&self.session_registry),
            session_transition_locks: Arc::clone(&self.session_transition_locks),
            positive_atomic_counter: Arc::clone(&self.positive_atomic_counter),
            producer_change_listener_vec: Arc::clone(&self.producer_change_listener_vec),
            broker_stats_manager: self.broker_stats_manager.clone(),
            broker_config: Arc::clone(&self.broker_config),
        }
    }

    pub(crate) fn session_registry(&self) -> ProducerSessionRegistry {
        ProducerSessionRegistry {
            group_session_table: Arc::clone(&self.group_session_table),
            session_transport_table: Arc::clone(&self.session_transport_table),
            positive_atomic_counter: Arc::clone(&self.positive_atomic_counter),
        }
    }

    pub(crate) fn reply_session_registry(&self) -> ProducerReplySessionRegistry {
        ProducerReplySessionRegistry {
            client_session_table: Arc::clone(&self.client_session_table),
            session_transport_table: Arc::clone(&self.session_transport_table),
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
            group_session_table: Arc::new(DashMap::new()),
            client_session_table: Arc::new(DashMap::new()),
            session_client_table: Arc::new(DashMap::new()),
            session_to_groups: Arc::new(DashMap::new()),
            session_transport_table: Arc::new(DashMap::new()),
            session_registry: Arc::new(OnceLock::new()),
            session_transition_locks,
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

    pub(crate) fn install_session_registry(&self, registry: &Arc<SessionRegistry>) -> bool {
        self.session_registry.set(Arc::downgrade(registry)).is_ok()
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
        self.group_session_table.len()
    }

    /// Returns connected producer counts grouped by client language and version.
    pub fn connection_count_by_client_attrs(&self) -> Vec<(rocketmq_protocol::protocol::LanguageCode, i32, i64)> {
        let mut counts: HashMap<(rocketmq_protocol::protocol::LanguageCode, i32), i64> = HashMap::new();
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
        producer_table_snapshot(&self.group_session_table)
    }

    /// Checks whether a producer group has at least one connected producer.
    ///
    /// # Arguments
    /// * `group` - The producer group name
    ///
    /// # Returns
    /// `true` if the group exists and contains at least one producer, `false` otherwise
    pub fn group_online(&self, group: &str) -> bool {
        self.group_session_table
            .get(group)
            .is_some_and(|sessions| !sessions.is_empty())
    }

    #[cfg(test)]
    pub(crate) fn expire_session_for_test(&self, session_id: SessionId) {
        for group in self.group_session_table.iter() {
            if let Some(mut session) = group.value().get_mut(&session_id) {
                session.set_last_update_timestamp_for_test(0);
            }
        }
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
            warn!("ignored producer heartbeat with conflicting session identity");
            return ProducerSessionBatch::default();
        }
        let replaced_session = self
            .client_session_table
            .get(client.client_id())
            .map(|entry| *entry.value())
            .filter(|session_id| *session_id != client.session_id());
        if groups.is_empty() {
            let retirements = replaced_session
                .and_then(|session_id| self.session_retirement(session_id))
                .into_iter()
                .collect();
            let removals = replaced_session
                .map(|session_id| self.unregister_producer_session_all_locked(session_id))
                .unwrap_or_default();
            return ProducerSessionBatch { removals, retirements };
        }
        if self
            .broker_config
            .load_full()
            .is_some_and(|config| !config.enable_register_producer && config.reject_transaction_message)
        {
            return ProducerSessionBatch::default();
        }
        self.session_client_table
            .insert(client.session_id(), client.client_id().clone());
        let transport = self
            .session_registry
            .get()
            .and_then(Weak::upgrade)
            .and_then(|registry| {
                let transport = ClientSessionTransport::resolve(&registry, client.session_id())?;
                registry.contains(client.session_id()).then_some(transport)
            });
        let Some(transport) = transport else {
            self.session_client_table
                .remove_if(&client.session_id(), |_, current| current == client.client_id());
            warn!("producer session is missing exact-generation typed transport capability");
            return ProducerSessionBatch::default();
        };
        self.session_transport_table.insert(client.session_id(), transport);
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
        let retirements = replaced_session
            .and_then(|session_id| self.session_retirement(session_id))
            .into_iter()
            .collect();
        let removals = replaced_session
            .map(|session_id| self.unregister_producer_session_all_locked(session_id))
            .unwrap_or_default();
        info!("producer session heartbeat applied");
        ProducerSessionBatch { removals, retirements }
    }

    fn session_retirement(&self, session_id: SessionId) -> Option<ClientSessionRetirement> {
        let registry = self.session_registry.get()?.clone();
        self.session_transport_table
            .get(&session_id)
            .map(|transport| transport.retirement(registry))
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
            self.session_transport_table.remove(&session_id);
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

    /// Removes producers that have not sent a heartbeat within the timeout period.
    ///
    /// This method should be invoked periodically by a background task. Producers inactive
    /// for more than 120 seconds are removed and their sessions are closed. Listeners are
    /// notified of each removal.
    pub(crate) fn scan_inactive_sessions(&self) -> Vec<ClientSessionRetirement> {
        let current_time = current_millis();
        let mut close_handles = Vec::new();

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
                let close_handle = if !session_has_groups {
                    self.client_session_table
                        .remove_if(client.client_id(), |_, current| *current == session_id);
                    self.session_client_table
                        .remove_if(&session_id, |_, current| current == client.client_id());
                    let close_handle = self
                        .session_transport_table
                        .remove(&session_id)
                        .and_then(|(_, transport)| {
                            self.session_registry
                                .get()
                                .cloned()
                                .map(|registry| transport.retirement(registry))
                        });
                    if close_handle.is_some() {
                        self.session_transition_locks
                            .mark_retiring(&transition, &client_id, session_id);
                    }
                    close_handle
                } else {
                    None
                };
                (
                    ProducerSessionRemoval {
                        group,
                        group_table_removed,
                    },
                    close_handle,
                )
            });
            drop(transition);
            if let Some((removal, close_handle)) = removal {
                if let Some(close_handle) = close_handle {
                    close_handles.push(close_handle);
                }
                self.dispatch_producer_session_removals(vec![removal]);
            }
        }
        close_handles
    }

    /// Notifies all registered producer change listeners of an event.
    ///
    /// # Arguments
    /// * `event` - The type of event (register/unregister)
    /// * `group` - The affected producer group
    /// * `client_session_info` - The affected client session identity (if applicable)
    fn call_producer_change_listener(
        &self,
        event: ProducerGroupEvent,
        group: &str,
        client_session_info: Option<&ClientSessionInfo>,
    ) {
        let listeners = self.producer_change_listener_vec.load();
        for listener in listeners.iter() {
            listener.handle(event, group, client_session_info);
        }
    }
}
