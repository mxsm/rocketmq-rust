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

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_protocol::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::BrokerStatsManager;
use rocketmq_transport::api::v2::ServerPushCommand;
use rocketmq_transport::api::v2::SessionId;
use rocketmq_transport::api::v2::V2SessionRegistry;
use tracing::warn;

use crate::client::client_session_info::ClientSessionInfo;
use crate::client::client_session_info::ClientSessionRetirement;
use crate::client::client_session_info::ClientSessionTransport;
use crate::client::consumer_group_event::ConsumerGroupEvent;
use crate::client::consumer_group_info::ConsumerGroupInfo;
use crate::client::consumer_ids_change_listener::ConsumerConnectionIdentity;
use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
use crate::client::session_transition_locks::ClientSessionTransitionGuard;
use crate::client::session_transition_locks::ClientSessionTransitionLocks;
use crate::long_polling::pull_deferred::PullSessionClientLookup;

/// Type alias for consumer change listener to reduce complexity
type ConsumerListener = Arc<dyn ConsumerIdsChangeListener + Send + Sync + 'static>;

#[cfg(test)]
type EmptyGroupBarrierPair = (Arc<std::sync::Barrier>, Arc<std::sync::Barrier>);

pub(crate) struct ConsumerSessionRegistration {
    pub(crate) group: CheetahString,
    pub(crate) consume_type: ConsumeType,
    pub(crate) message_model: MessageModel,
    pub(crate) consume_from_where: ConsumeFromWhere,
    pub(crate) subscriptions: HashSet<SubscriptionData>,
    pub(crate) notify_consumer_ids_changed: bool,
    pub(crate) update_subscription: bool,
}

fn is_broadcast_mode(message_model: MessageModel) -> bool {
    message_model == MessageModel::Broadcasting
}

struct ConsumerSessionRemoval {
    group: CheetahString,
    callback_drain: Option<ConsumerSessionCallbackDrain>,
}

enum ConsumerSessionCallback {
    GroupUnregister(CheetahString),
    MembersChanged {
        group: CheetahString,
        members: Vec<ConsumerConnectionIdentity>,
    },
    RegisterSubscriptions {
        group: CheetahString,
        subscriptions: HashSet<SubscriptionData>,
    },
}

#[derive(Default)]
struct ConsumerSessionCallbackQueueState {
    draining: bool,
    pending: VecDeque<Vec<ConsumerSessionCallback>>,
}

#[derive(Default)]
struct ConsumerSessionCallbackQueue {
    state: Mutex<ConsumerSessionCallbackQueueState>,
}

struct ConsumerSessionCallbackDrain {
    group: CheetahString,
    queue: Arc<ConsumerSessionCallbackQueue>,
}

#[derive(Default)]
pub(crate) struct ConsumerSessionBatch {
    started_at: Option<Instant>,
    changed_groups: HashSet<CheetahString>,
    callback_drains: Vec<ConsumerSessionCallbackDrain>,
    retirements: Vec<ClientSessionRetirement>,
}

/// Manages consumer client connections and their lifecycle.
///
/// This manager maintains:
/// - Consumer group registrations and subscription relationships
/// - Session heartbeat status and expiration detection
/// - Automatic cleanup of inactive consumers
/// - Topic to consumer group reverse index for fast lookup
///
/// # Thread Safety
/// All operations are thread-safe using concurrent data structures.
pub struct ConsumerManager {
    /// Consumer group name -> ConsumerGroupInfo mapping
    consumer_table: Arc<DashMap<CheetahString, ConsumerGroupInfo>>,
    /// Compensation table for consumers without heartbeat
    consumer_compensation_table: Arc<DashMap<CheetahString, ConsumerGroupInfo>>,
    /// Topic -> Set<Group> reverse index for fast topic-to-group lookup
    topic_group_table: Arc<DashMap<CheetahString, HashSet<CheetahString>>>,
    /// Canonical V2 session -> all consumer groups registered by that session.
    session_to_groups: Arc<DashMap<SessionId, HashSet<CheetahString>>>,
    /// Latest canonical V2 session for each consumer client identity.
    client_session_table: Arc<DashMap<CheetahString, SessionId>>,
    /// Immutable client identity claimed by each live V2 session.
    session_client_table: Arc<DashMap<SessionId, CheetahString>>,
    /// Exact-generation typed transport authority for live consumer sessions.
    session_transport_table: Arc<DashMap<SessionId, ClientSessionTransport>>,
    /// Weak composition-root resolver used only while applying a session heartbeat.
    v2_session_registry: Arc<OnceLock<Weak<V2SessionRegistry>>>,
    /// Consumer-id change notification policy captured for each V2 group registration.
    session_notify_table: Arc<DashMap<(CheetahString, SessionId), bool>>,
    /// Per-group ordered side-effect queues. Group state transitions enqueue before releasing
    /// their group guard, and exactly one lock-free callback drainer preserves that order.
    session_callback_queues: Arc<DashMap<CheetahString, Arc<ConsumerSessionCallbackQueue>>>,
    #[cfg(test)]
    empty_group_barriers: Arc<RwLock<Option<EmptyGroupBarrierPair>>>,
    #[cfg(test)]
    topic_empty_entry_barriers: Arc<RwLock<Option<EmptyGroupBarrierPair>>>,
    /// Striped serialization for canonical client-session multi-index transitions.
    session_transition_locks: Arc<ClientSessionTransitionLocks>,
    /// Listeners notified on consumer registration/unregistration events
    /// Uses Arc<RwLock<Vec>> to support dynamic listener registration at runtime
    consumer_ids_change_listener_list: Arc<RwLock<Vec<ConsumerListener>>>,
    /// Optional broker statistics manager (set once during initialization)
    broker_stats_manager: Option<Weak<BrokerStatsManager>>,
    /// Broker configuration (used for enable_fast_channel_event_process flag)
    broker_config: Option<Arc<BrokerConfig>>,
    /// Timeout for considering a consumer session expired (in milliseconds).
    channel_expired_timeout: u64,
    /// Timeout for subscription data expiration (in milliseconds)
    subscription_expired_timeout: u64,
}
/// Shared consumer-connection mutation capability for Broker housekeeping.
///
/// The handle shares the live manager state but exposes only inactive-session scanning and
/// session-close cleanup. It cannot register consumers, query subscriptions, or mutate manager
/// configuration.
pub(crate) struct ConsumerConnectionHousekeeping {
    manager: ConsumerManager,
}

/// Shared consumer registration capability for client heartbeat processing.
///
/// The handle shares the live consumer tables and listeners but exposes only heartbeat
/// registration and explicit client unregistration.
pub(crate) struct ConsumerClientRegistration {
    manager: ConsumerManager,
}

impl Clone for ConsumerClientRegistration {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone_shared_state(),
        }
    }
}

impl ConsumerClientRegistration {
    pub(crate) fn register_consumer_session(
        &self,
        group: &CheetahString,
        client: ClientSessionInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        subscriptions: HashSet<SubscriptionData>,
        notify_consumer_ids_changed: bool,
    ) -> bool {
        self.manager
            .register_consumer_sessions(
                client,
                vec![ConsumerSessionRegistration {
                    group: group.clone(),
                    consume_type,
                    message_model,
                    consume_from_where,
                    subscriptions,
                    notify_consumer_ids_changed,
                    update_subscription: true,
                }],
            )
            .0
            .contains(group)
    }

    pub(crate) fn register_consumer_session_without_sub(
        &self,
        group: &CheetahString,
        client: ClientSessionInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        notify_consumer_ids_changed: bool,
    ) -> bool {
        self.manager
            .register_consumer_sessions(
                client,
                vec![ConsumerSessionRegistration {
                    group: group.clone(),
                    consume_type,
                    message_model,
                    consume_from_where,
                    subscriptions: HashSet::new(),
                    notify_consumer_ids_changed,
                    update_subscription: false,
                }],
            )
            .0
            .contains(group)
    }

    pub(crate) fn register_consumer_sessions(
        &self,
        client: ClientSessionInfo,
        registrations: Vec<ConsumerSessionRegistration>,
    ) -> HashSet<CheetahString> {
        self.manager.register_consumer_sessions(client, registrations).0
    }

    pub(crate) fn prepare_consumer_sessions(
        &self,
        transition: &ClientSessionTransitionGuard<'_>,
        client: ClientSessionInfo,
        registrations: Vec<ConsumerSessionRegistration>,
    ) -> ConsumerSessionBatch {
        self.manager
            .register_consumer_sessions_locked(transition, client, registrations)
    }

    pub(crate) fn complete_consumer_sessions(
        &self,
        batch: ConsumerSessionBatch,
    ) -> (HashSet<CheetahString>, Vec<ClientSessionRetirement>) {
        self.manager.complete_consumer_session_batch(batch)
    }

    pub(crate) async fn notify_consumer_ids_changed(&self, groups: &HashSet<CheetahString>) {
        for group in groups {
            let Some(group_info) = self.manager.consumer_table.get(group) else {
                continue;
            };
            let sessions = group_info.session_info_snapshot();
            drop(group_info);
            for client in sessions {
                if !self
                    .manager
                    .session_notify_table
                    .get(&(group.clone(), client.session_id()))
                    .is_some_and(|notify| *notify)
                {
                    continue;
                }
                let Some(transport) = self.manager.session_transport_table.get(&client.session_id()) else {
                    continue;
                };
                let sender = transport.push_sender();
                drop(transport);
                let command = ServerPushCommand::NotifyConsumerIdsChanged {
                    header: NotifyConsumerIdsChangedRequestHeader {
                        consumer_group: group.clone(),
                        rpc_request_header: None,
                    },
                    opaque: None,
                };
                if sender.send(command, Duration::from_millis(10)).await.is_err() {
                    warn!("typed consumer membership notification failed");
                }
            }
        }
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

    pub(crate) fn unregister_consumer_session(
        &self,
        group: &str,
        session_id: SessionId,
        notify_consumer_ids_changed: bool,
    ) {
        self.manager
            .unregister_consumer_session(group, session_id, notify_consumer_ids_changed);
    }
}

/// Read-only access to the live consumer table for assignment decisions.
#[derive(Clone)]
pub(crate) struct ConsumerAssignmentView {
    consumer_table: Arc<DashMap<CheetahString, ConsumerGroupInfo>>,
}

/// Live read-only mapping from a canonical V2 session and consumer group to
/// the client identity most recently registered by heartbeat.
#[derive(Clone)]
pub(crate) struct ConsumerSessionRegistry {
    consumer_table: Arc<DashMap<CheetahString, ConsumerGroupInfo>>,
    session_transport_table: Arc<DashMap<SessionId, ClientSessionTransport>>,
}

impl PullSessionClientLookup for ConsumerSessionRegistry {
    fn client_id(&self, session_id: SessionId, consumer_group: &CheetahString) -> Option<CheetahString> {
        self.consumer_table
            .get(consumer_group)
            .and_then(|info| info.session_client_id(session_id))
    }
}

impl ConsumerSessionRegistry {
    pub(crate) fn transport_snapshot(&self, group: &CheetahString) -> Vec<(ClientSessionInfo, ClientSessionTransport)> {
        self.consumer_table.get(group).map_or_else(Vec::new, |info| {
            info.session_info_snapshot()
                .into_iter()
                .filter_map(|client| {
                    self.session_transport_table
                        .get(&client.session_id())
                        .map(|transport| (client, transport.clone()))
                })
                .collect()
        })
    }

    pub(crate) fn find_transport(
        &self,
        group: &CheetahString,
        client_id: &str,
    ) -> Option<(ClientSessionInfo, ClientSessionTransport)> {
        self.transport_snapshot(group)
            .into_iter()
            .find(|(client, _)| client.client_id() == client_id)
    }
}

impl ConsumerAssignmentView {
    pub(crate) fn client_ids(&self, group: &CheetahString) -> Vec<CheetahString> {
        self.consumer_table
            .get(group)
            .map_or_else(Vec::new, |info| info.get_all_client_ids())
    }

    pub(crate) fn client_ids_if_present(&self, group: &CheetahString) -> Option<Vec<CheetahString>> {
        self.consumer_table.get(group).map(|info| info.get_all_client_ids())
    }
}

impl Clone for ConsumerConnectionHousekeeping {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone_shared_state(),
        }
    }
}

impl ConsumerConnectionHousekeeping {
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
}

impl ConsumerManager {
    pub(crate) fn client_registration(&self) -> ConsumerClientRegistration {
        ConsumerClientRegistration {
            manager: self.clone_shared_state(),
        }
    }

    pub(crate) fn assignment_view(&self) -> ConsumerAssignmentView {
        ConsumerAssignmentView {
            consumer_table: Arc::clone(&self.consumer_table),
        }
    }

    pub(crate) fn session_registry(&self) -> ConsumerSessionRegistry {
        ConsumerSessionRegistry {
            consumer_table: Arc::clone(&self.consumer_table),
            session_transport_table: Arc::clone(&self.session_transport_table),
        }
    }

    pub(crate) fn connection_housekeeping(&self) -> ConsumerConnectionHousekeeping {
        ConsumerConnectionHousekeeping {
            manager: self.clone_shared_state(),
        }
    }

    pub(crate) fn clone_shared_state(&self) -> Self {
        Self {
            consumer_table: Arc::clone(&self.consumer_table),
            consumer_compensation_table: Arc::clone(&self.consumer_compensation_table),
            topic_group_table: Arc::clone(&self.topic_group_table),
            session_to_groups: Arc::clone(&self.session_to_groups),
            client_session_table: Arc::clone(&self.client_session_table),
            session_client_table: Arc::clone(&self.session_client_table),
            session_transport_table: Arc::clone(&self.session_transport_table),
            v2_session_registry: Arc::clone(&self.v2_session_registry),
            session_notify_table: Arc::clone(&self.session_notify_table),
            session_callback_queues: Arc::clone(&self.session_callback_queues),
            #[cfg(test)]
            empty_group_barriers: Arc::clone(&self.empty_group_barriers),
            #[cfg(test)]
            topic_empty_entry_barriers: Arc::clone(&self.topic_empty_entry_barriers),
            session_transition_locks: Arc::clone(&self.session_transition_locks),
            consumer_ids_change_listener_list: Arc::clone(&self.consumer_ids_change_listener_list),
            broker_stats_manager: self.broker_stats_manager.clone(),
            broker_config: self.broker_config.clone(),
            channel_expired_timeout: self.channel_expired_timeout,
            subscription_expired_timeout: self.subscription_expired_timeout,
        }
    }

    /// Creates a new ConsumerManager instance.
    ///
    /// # Arguments
    /// * `consumer_ids_change_listener` - Listener for consumer change events
    /// * `expired_timeout` - Timeout for session and subscription expiration (milliseconds)
    pub fn new(consumer_ids_change_listener: ConsumerListener, expired_timeout: u64) -> Self {
        Self::new_with_session_transition_locks(
            consumer_ids_change_listener,
            expired_timeout,
            Arc::new(ClientSessionTransitionLocks::default()),
        )
    }

    pub(crate) fn new_with_session_transition_locks(
        consumer_ids_change_listener: ConsumerListener,
        expired_timeout: u64,
        session_transition_locks: Arc<ClientSessionTransitionLocks>,
    ) -> Self {
        let consumer_ids_change_listener_list = Arc::new(RwLock::new(vec![consumer_ids_change_listener]));
        ConsumerManager {
            // Uses 64 shards for improved concurrency under high load
            consumer_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            consumer_compensation_table: Arc::new(DashMap::with_capacity_and_shard_amount(256, 16)),
            // Topic-Group reverse index for fast topic-to-group lookup
            topic_group_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            session_to_groups: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            client_session_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            session_client_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            session_transport_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            v2_session_registry: Arc::new(OnceLock::new()),
            session_notify_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            session_callback_queues: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            #[cfg(test)]
            empty_group_barriers: Arc::new(RwLock::new(None)),
            #[cfg(test)]
            topic_empty_entry_barriers: Arc::new(RwLock::new(None)),
            session_transition_locks,
            consumer_ids_change_listener_list,
            broker_stats_manager: None,
            broker_config: None,
            channel_expired_timeout: expired_timeout,
            subscription_expired_timeout: expired_timeout,
        }
    }

    /// Creates a new ConsumerManager with broker configuration.
    ///
    /// # Arguments
    /// * `consumer_ids_change_listener` - Listener for consumer change events
    /// * `broker_config` - Broker configuration containing timeout settings
    pub fn new_with_broker_stats(
        consumer_ids_change_listener: ConsumerListener,
        broker_config: Arc<BrokerConfig>,
    ) -> Self {
        Self::new_with_broker_stats_and_session_transition_locks(
            consumer_ids_change_listener,
            broker_config,
            Arc::new(ClientSessionTransitionLocks::default()),
        )
    }

    pub(crate) fn new_with_broker_stats_and_session_transition_locks(
        consumer_ids_change_listener: ConsumerListener,
        broker_config: Arc<BrokerConfig>,
        session_transition_locks: Arc<ClientSessionTransitionLocks>,
    ) -> Self {
        let consumer_ids_change_listener_list = Arc::new(RwLock::new(vec![consumer_ids_change_listener]));
        ConsumerManager {
            // Uses 64 shards for improved concurrency under high load
            consumer_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            consumer_compensation_table: Arc::new(DashMap::with_capacity_and_shard_amount(256, 16)),
            // Topic-Group reverse index for fast topic-to-group lookup
            topic_group_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            session_to_groups: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            client_session_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            session_client_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            session_transport_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            v2_session_registry: Arc::new(OnceLock::new()),
            session_notify_table: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            session_callback_queues: Arc::new(DashMap::with_capacity_and_shard_amount(1024, 64)),
            #[cfg(test)]
            empty_group_barriers: Arc::new(RwLock::new(None)),
            #[cfg(test)]
            topic_empty_entry_barriers: Arc::new(RwLock::new(None)),
            session_transition_locks,
            consumer_ids_change_listener_list,
            broker_stats_manager: None,
            broker_config: Some(broker_config.clone()),
            channel_expired_timeout: broker_config.channel_expired_timeout,
            subscription_expired_timeout: broker_config.subscription_expired_timeout,
        }
    }
}

impl ConsumerManager {
    pub(crate) fn install_v2_session_registry(&self, registry: &Arc<V2SessionRegistry>) -> bool {
        self.v2_session_registry.set(Arc::downgrade(registry)).is_ok()
    }

    pub fn set_broker_stats_manager(&mut self, broker_stats_manager: Weak<BrokerStatsManager>) {
        self.broker_stats_manager = Some(broker_stats_manager);
    }

    /// Returns connected consumer counts grouped by group, language, version, and consume mode.
    pub fn connection_count_by_client_attrs(
        &self,
    ) -> Vec<(
        CheetahString,
        rocketmq_protocol::protocol::LanguageCode,
        i32,
        ConsumeType,
        i64,
    )> {
        let mut counts: HashMap<
            (
                CheetahString,
                rocketmq_protocol::protocol::LanguageCode,
                i32,
                ConsumeType,
            ),
            i64,
        > = HashMap::new();
        for entry in self.consumer_table.iter() {
            let group_info = entry.value();
            let group = group_info.get_group_name().clone();
            let consume_type = group_info.get_consume_type();
            for client in group_info.session_info_snapshot() {
                *counts
                    .entry((group.clone(), client.language(), client.version(), consume_type))
                    .or_default() += 1;
            }
        }
        counts
            .into_iter()
            .map(|((group, language, version, consume_type), count)| (group, language, version, consume_type, count))
            .collect()
    }

    /// Finds subscription data for a topic within a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Subscription data if found
    pub fn find_subscription_data(&self, group: &CheetahString, topic: &CheetahString) -> Option<SubscriptionData> {
        self.find_subscription_data_internal(group, topic, true)
    }

    /// Finds subscription data for a topic within a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    /// * `from_compensation_table` - Whether to check compensation table
    ///
    /// # Returns
    /// Subscription data if found
    pub fn find_subscription_data_internal(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        from_compensation_table: bool,
    ) -> Option<SubscriptionData> {
        if let Some(consumer_group_info) = self.get_consumer_group_info_internal(group, false) {
            if let Some(subscription_data) = consumer_group_info.find_subscription_data(topic) {
                return Some(subscription_data);
            }
        }

        if from_compensation_table {
            if let Some(consumer_group_info) = self.consumer_compensation_table.get(group) {
                return consumer_group_info.find_subscription_data(topic);
            }
        }
        None
    }

    /// Counts the number of subscription data entries for a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    ///
    /// # Returns
    /// Number of subscriptions
    pub fn find_subscription_data_count(&self, group: &CheetahString) -> usize {
        if let Some(consumer_group_info) = self.get_consumer_group_info(group) {
            return consumer_group_info.subscription_count();
        }
        0
    }

    /// Gets consumer group info for a specific group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    ///
    /// # Returns
    /// Consumer group info if found
    pub fn get_consumer_group_info(&self, group: &CheetahString) -> Option<ConsumerGroupInfo> {
        self.get_consumer_group_info_internal(group, false)
    }

    /// Gets consumer group info with optional compensation table fallback.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `from_compensation_table` - Whether to check compensation table
    ///
    /// # Returns
    /// Consumer group info if found
    pub fn get_consumer_group_info_internal(
        &self,
        group: &CheetahString,
        from_compensation_table: bool,
    ) -> Option<ConsumerGroupInfo> {
        if let Some(consumer_group_info) = self.consumer_table.get(group) {
            return Some(consumer_group_info.clone());
        }
        if from_compensation_table {
            if let Some(consumer_group_info) = self.consumer_compensation_table.get(group) {
                return Some(consumer_group_info.clone());
            }
        }
        None
    }

    /// Compensates subscription data for consumers without heartbeat.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    /// * `subscription_data` - Subscription data to compensate
    pub fn compensate_subscribe_data(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        subscription_data: &SubscriptionData,
    ) {
        let consumer_group_info = self
            .consumer_compensation_table
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::with_group_name(group.clone()));
        let mut subscription = subscription_data.clone();
        subscription.topic = topic.clone();
        consumer_group_info.upsert_subscription(subscription);
    }

    /// Compensates basic consumer info (consume type and message model).
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `consume_type` - Consume type (push/pull)
    /// * `message_model` - Message model (clustering/broadcasting)
    pub fn compensate_basic_consumer_info(
        &self,
        group: &CheetahString,
        consume_type: ConsumeType,
        message_model: MessageModel,
    ) {
        let mut consumer_group_info = self
            .consumer_compensation_table
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::with_group_name(group.clone()));
        consumer_group_info.set_consume_type(consume_type);
        consumer_group_info.set_message_model(message_model);
    }

    /// Restores durable POP classification and subscriptions without creating a network session.
    pub(crate) fn restore_pop_consumer_profile(&self, group: &CheetahString, subscriptions: &[SubscriptionData]) {
        self.compensate_basic_consumer_info(group, ConsumeType::ConsumePop, MessageModel::Clustering);
        for subscription in subscriptions {
            self.compensate_subscribe_data(group, &subscription.topic, subscription);
        }
    }

    pub(crate) fn remove_compensated_consumer_profile(&self, group: &CheetahString) {
        self.consumer_compensation_table.remove(group);
    }

    fn register_consumer_sessions(
        &self,
        client: ClientSessionInfo,
        registrations: Vec<ConsumerSessionRegistration>,
    ) -> (HashSet<CheetahString>, Vec<ClientSessionRetirement>) {
        let transition = self
            .session_transition_locks
            .lock(client.client_id(), client.session_id());
        let batch = self.register_consumer_sessions_locked(&transition, client, registrations);
        drop(transition);
        self.complete_consumer_session_batch(batch)
    }

    fn register_consumer_sessions_locked(
        &self,
        transition: &ClientSessionTransitionGuard<'_>,
        client: ClientSessionInfo,
        registrations: Vec<ConsumerSessionRegistration>,
    ) -> ConsumerSessionBatch {
        assert!(
            self.session_transition_locks
                .covers(transition, client.client_id(), client.session_id()),
            "consumer session mutation requires the matching transition guard"
        );
        let start = Instant::now();
        if self
            .session_client_table
            .get(&client.session_id())
            .is_some_and(|current| current.as_str() != client.client_id().as_str())
        {
            warn!("ignored consumer heartbeat with conflicting session identity");
            return ConsumerSessionBatch::default();
        }
        let replaced_session = self
            .client_session_table
            .get(client.client_id())
            .map(|entry| *entry.value())
            .filter(|session_id| *session_id != client.session_id());
        if registrations.is_empty() {
            let retirements = replaced_session
                .and_then(|session_id| self.session_retirement(session_id))
                .into_iter()
                .collect();
            let removals = replaced_session
                .map(|session_id| self.unregister_consumer_session_all_locked(session_id))
                .unwrap_or_default();
            let mut batch = ConsumerSessionBatch {
                started_at: Some(start),
                retirements,
                ..ConsumerSessionBatch::default()
            };
            for removal in removals {
                batch.changed_groups.insert(removal.group);
                if let Some(callback_drain) = removal.callback_drain {
                    batch.callback_drains.push(callback_drain);
                }
            }
            return batch;
        }
        self.session_client_table
            .insert(client.session_id(), client.client_id().clone());
        let transport = self
            .v2_session_registry
            .get()
            .and_then(Weak::upgrade)
            .and_then(|registry| {
                let transport = ClientSessionTransport::resolve(&registry, client.session_id())?;
                registry.contains(client.session_id()).then_some(transport)
            });
        let Some(transport) = transport else {
            self.session_client_table
                .remove_if(&client.session_id(), |_, current| current == client.client_id());
            warn!("consumer session is missing exact-generation typed transport capability");
            return ConsumerSessionBatch::default();
        };
        self.session_transport_table.insert(client.session_id(), transport);
        self.client_session_table
            .insert(client.client_id().clone(), client.session_id());
        let mut changed_groups = HashSet::new();
        let mut callback_drains = Vec::with_capacity(registrations.len());
        let mut unique_groups = HashSet::with_capacity(registrations.len());
        for registration in registrations {
            let ConsumerSessionRegistration {
                group,
                consume_type,
                message_model,
                consume_from_where,
                subscriptions,
                notify_consumer_ids_changed,
                update_subscription,
            } = registration;
            if !unique_groups.insert(group.clone()) {
                continue;
            }
            let mut group_info = self.consumer_table.entry(group.clone()).or_insert_with(|| {
                ConsumerGroupInfo::new(group.clone(), consume_type, message_model, consume_from_where)
            });

            if update_subscription {
                let old_topics = group_info.get_subscribe_topics();
                let new_topics = subscriptions
                    .iter()
                    .map(|subscription| subscription.topic.clone())
                    .collect::<HashSet<_>>();
                for old_topic in old_topics.difference(&new_topics) {
                    if let Some(mut groups) = self.topic_group_table.get_mut(old_topic) {
                        groups.remove(&group);
                        if groups.is_empty() {
                            drop(groups);
                            self.topic_group_table
                                .remove_if(old_topic, |_, current| current.is_empty());
                        }
                    }
                }
                for subscription in &subscriptions {
                    self.topic_group_table
                        .entry(subscription.topic.clone())
                        .or_default()
                        .insert(group.clone());
                }
            }

            let session_changed =
                group_info.update_session(client.clone(), consume_type, message_model, consume_from_where);
            let subscription_changed = update_subscription && group_info.update_subscription(&subscriptions);
            let changed = session_changed || subscription_changed;
            if changed {
                changed_groups.insert(group.clone());
            }
            self.session_notify_table
                .insert((group.clone(), client.session_id()), notify_consumer_ids_changed);
            self.session_to_groups
                .entry(client.session_id())
                .or_default()
                .insert(group.clone());
            let mut callbacks = Vec::with_capacity(2);
            if changed && notify_consumer_ids_changed && !is_broadcast_mode(message_model) {
                callbacks.push(ConsumerSessionCallback::MembersChanged {
                    group: group.clone(),
                    members: group_info.connection_identity_snapshot(),
                });
            }
            if update_subscription {
                callbacks.push(ConsumerSessionCallback::RegisterSubscriptions {
                    group: group.clone(),
                    subscriptions,
                });
            }
            if let Some(callback_drain) = self.enqueue_consumer_group_callbacks(&group, callbacks) {
                callback_drains.push(callback_drain);
            }
            drop(group_info);
        }
        let retirements = replaced_session
            .and_then(|session_id| self.session_retirement(session_id))
            .into_iter()
            .collect();
        let removals = replaced_session
            .map(|session_id| self.unregister_consumer_session_all_locked(session_id))
            .unwrap_or_default();
        for removal in removals {
            changed_groups.insert(removal.group);
            if let Some(callback_drain) = removal.callback_drain {
                callback_drains.push(callback_drain);
            }
        }
        ConsumerSessionBatch {
            started_at: Some(start),
            changed_groups,
            callback_drains,
            retirements,
        }
    }

    fn session_retirement(&self, session_id: SessionId) -> Option<ClientSessionRetirement> {
        let registry = self.v2_session_registry.get()?.clone();
        self.session_transport_table
            .get(&session_id)
            .map(|transport| transport.retirement(registry))
    }

    fn complete_consumer_session_batch(
        &self,
        batch: ConsumerSessionBatch,
    ) -> (HashSet<CheetahString>, Vec<ClientSessionRetirement>) {
        let ConsumerSessionBatch {
            started_at,
            changed_groups,
            callback_drains,
            retirements,
        } = batch;
        for callback_drain in callback_drains {
            self.drain_consumer_session_callbacks(callback_drain);
        }
        if let (Some(started_at), Some(stats)) =
            (started_at, self.broker_stats_manager.as_ref().and_then(Weak::upgrade))
        {
            stats.inc_consumer_register_time(started_at.elapsed().as_millis() as i32);
        }
        (changed_groups, retirements)
    }

    fn unregister_consumer_session(&self, group: &str, session_id: SessionId, notify_consumer_ids_changed: bool) {
        let Some(client_id) = self.session_client_table.get(&session_id).map(|entry| entry.clone()) else {
            return;
        };
        let transition = self.session_transition_locks.lock(&client_id, session_id);
        let removal = self.unregister_consumer_session_locked(group, session_id, notify_consumer_ids_changed);
        drop(transition);
        if let Some(removal) = removal {
            if let Some(callback_drain) = removal.callback_drain {
                self.drain_consumer_session_callbacks(callback_drain);
            }
        }
    }

    fn unregister_consumer_session_locked(
        &self,
        group: &str,
        session_id: SessionId,
        notify_consumer_ids_changed: bool,
    ) -> Option<ConsumerSessionRemoval> {
        let group_info_guard = self.consumer_table.get_mut(group)?;
        let client = group_info_guard.unregister_session(session_id)?;
        let message_model = group_info_guard.get_message_model();
        let group_is_empty = group_info_guard.channels_is_empty();
        let notify_members_changed = self
            .session_notify_table
            .remove(&(group.into(), session_id))
            .map(|(_, notify)| notify)
            .unwrap_or(notify_consumer_ids_changed)
            && !is_broadcast_mode(message_model);
        let members = notify_members_changed.then(|| group_info_guard.connection_identity_snapshot());
        let mut callbacks = Vec::with_capacity(2);
        if let Some(members) = members {
            callbacks.push(ConsumerSessionCallback::MembersChanged {
                group: group.into(),
                members,
            });
        }
        if group_is_empty {
            callbacks.push(ConsumerSessionCallback::GroupUnregister(group.into()));
            // Clear the reverse index while this exact empty group entry is still guarded.
            // A concurrent registration can then either repopulate this entry after the guard
            // is released or create a replacement after the conditional outer removal.
            self.clear_topic_group_table(&group_info_guard);
            #[cfg(test)]
            self.pause_empty_group_removal_for_test();
        }
        let callback_drain = self.enqueue_consumer_group_callbacks(group, callbacks);
        drop(group_info_guard);
        let session_has_groups = self.remove_consumer_session_group_index(session_id, group);
        if !session_has_groups {
            self.client_session_table
                .remove_if(client.client_id(), |_, current| *current == session_id);
            self.session_client_table
                .remove_if(&session_id, |_, current| current == client.client_id());
            self.session_transport_table.remove(&session_id);
        }
        if group_is_empty {
            self.consumer_table
                .remove_if(group, |_, current| current.channels_is_empty());
        }
        Some(ConsumerSessionRemoval {
            group: group.into(),
            callback_drain,
        })
    }

    fn unregister_consumer_session_all_locked(&self, session_id: SessionId) -> Vec<ConsumerSessionRemoval> {
        let groups = self
            .session_to_groups
            .get(&session_id)
            .map(|entry| entry.value().clone())
            .unwrap_or_default();
        groups
            .into_iter()
            .filter_map(|group| self.unregister_consumer_session_locked(group.as_str(), session_id, false))
            .collect()
    }

    fn do_session_close_event(&self, session_id: SessionId) -> bool {
        let Some(client_id) = self.session_client_table.get(&session_id).map(|entry| entry.clone()) else {
            return false;
        };
        let transition = self.session_transition_locks.lock(&client_id, session_id);
        let removals = self.unregister_consumer_session_all_locked(session_id);
        let removed = !removals.is_empty();
        drop(transition);
        for removal in removals {
            if let Some(callback_drain) = removal.callback_drain {
                self.drain_consumer_session_callbacks(callback_drain);
            }
        }
        removed
    }

    fn enqueue_consumer_group_callbacks(
        &self,
        group: impl Into<CheetahString>,
        callbacks: Vec<ConsumerSessionCallback>,
    ) -> Option<ConsumerSessionCallbackDrain> {
        if callbacks.is_empty() {
            return None;
        }
        let group = group.into();
        let queue_entry = self
            .session_callback_queues
            .entry(group.clone())
            .or_insert_with(|| Arc::new(ConsumerSessionCallbackQueue::default()));
        let queue = Arc::clone(queue_entry.value());
        let mut state = queue.state.lock();
        state.pending.push_back(callbacks);
        if state.draining {
            return None;
        }
        state.draining = true;
        drop(state);
        drop(queue_entry);
        Some(ConsumerSessionCallbackDrain { group, queue })
    }

    fn drain_consumer_session_callbacks(&self, drain: ConsumerSessionCallbackDrain) {
        loop {
            let callbacks = {
                let mut state = drain.queue.state.lock();
                match state.pending.pop_front() {
                    Some(callbacks) => callbacks,
                    None => {
                        state.draining = false;
                        drop(state);
                        self.session_callback_queues.remove_if(&drain.group, |_, current| {
                            Arc::ptr_eq(current, &drain.queue) && {
                                let state = current.state.lock();
                                !state.draining && state.pending.is_empty()
                            }
                        });
                        return;
                    }
                }
            };
            for callback in callbacks {
                match callback {
                    ConsumerSessionCallback::GroupUnregister(group) => {
                        if self.consumer_table.get(&group).is_none() {
                            self.call_consumer_ids_change_listener(ConsumerGroupEvent::Unregister, &group, &[]);
                        }
                    }
                    ConsumerSessionCallback::MembersChanged { group, members } => {
                        let listeners = self.consumer_ids_change_listener_list.read().clone();
                        for listener in listeners {
                            listener.handle_connection_change(&group, &members);
                        }
                    }
                    ConsumerSessionCallback::RegisterSubscriptions { group, subscriptions } => self
                        .call_consumer_ids_change_listener(
                            ConsumerGroupEvent::Register,
                            &group,
                            &[&subscriptions as &dyn Any],
                        ),
                }
            }
        }
    }

    fn remove_consumer_session_group_index(&self, session_id: SessionId, group: &str) -> bool {
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

    /// Notifies all registered consumer IDs change listeners of an event.
    ///
    /// # Arguments
    /// * `event` - The type of event
    /// * `group` - The affected consumer group
    /// * `args` - Additional event arguments
    pub fn call_consumer_ids_change_listener(&self, event: ConsumerGroupEvent, group: &str, args: &[&dyn Any]) {
        let listeners = self.consumer_ids_change_listener_list.read().clone();
        for listener in listeners {
            listener.handle(event, group, args);
        }
    }

    /// Dynamically appends a new consumer change listener to the list.
    ///
    /// This enables runtime registration of listeners for plugin-like extensibility.
    ///
    /// # Arguments
    /// * `listener` - The listener to register
    ///
    /// # Example
    /// ```ignore
    /// let custom_listener = Arc::new(MyCustomListener::new());
    /// consumer_manager.append_consumer_ids_change_listener(custom_listener);
    /// ```
    pub fn append_consumer_ids_change_listener(&self, listener: ConsumerListener) {
        self.consumer_ids_change_listener_list.write().push(listener);
    }

    /// Clears topic-group reverse index entries for a removed consumer group.
    ///
    /// This is a helper method called during consumer group cleanup to maintain
    /// consistency of the topic_group_table.
    ///
    /// # Arguments
    /// * `consumer_group_info` - The consumer group being removed
    fn clear_topic_group_table(&self, consumer_group_info: &ConsumerGroupInfo) {
        let group_name = consumer_group_info.get_group_name();

        for topic in consumer_group_info.get_subscribe_topics() {
            if let Some(mut groups) = self.topic_group_table.get_mut(&topic) {
                groups.remove(group_name);
                if groups.is_empty() {
                    drop(groups);
                    #[cfg(test)]
                    self.pause_topic_empty_entry_removal_for_test();
                    self.topic_group_table
                        .remove_if(&topic, |_, current| current.is_empty());
                }
            }
        }
    }

    #[cfg(test)]
    fn pause_empty_group_removal_for_test(&self) {
        if let Some((entered, release)) = self.empty_group_barriers.write().take() {
            entered.wait();
            release.wait();
        }
    }

    #[cfg(test)]
    fn pause_topic_empty_entry_removal_for_test(&self) {
        if let Some((entered, release)) = self.topic_empty_entry_barriers.write().take() {
            entered.wait();
            release.wait();
        }
    }

    /// Queries which consumer groups are consuming a topic.
    ///
    /// # Arguments
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Set of consumer group names consuming this topic
    ///
    /// # Performance
    /// This method uses reverse index lookup (topic_group_table) for efficient retrieval.
    pub fn query_topic_consume_by_who(&self, topic: &CheetahString) -> HashSet<CheetahString> {
        self.topic_group_table
            .get(topic)
            .map(|groups| groups.clone())
            .unwrap_or_default()
    }

    pub fn remove_expire_consumer_group_info(&self) {
        let mut groups_to_remove = Vec::new();

        for mut entry in self.consumer_compensation_table.iter_mut() {
            let group = entry.key().clone();
            let consumer_group_info = entry.value_mut();
            let mut topics_to_remove = Vec::new();
            let subscription_snapshot = consumer_group_info.subscription_snapshot();

            // Find expired subscriptions
            for (topic, subscription_data) in subscription_snapshot {
                let diff = current_millis() as i64 - subscription_data.sub_version;
                if diff > self.subscription_expired_timeout as i64 {
                    topics_to_remove.push(topic);
                }
            }

            // Remove expired subscriptions
            for topic in topics_to_remove {
                consumer_group_info.remove_subscription(topic.as_str());
                if consumer_group_info.subscriptions_is_empty() {
                    groups_to_remove.push(group.clone());
                }
            }
        }

        // Remove empty groups
        for group in groups_to_remove {
            self.consumer_compensation_table.remove(&group);
        }
    }

    /// Scans and removes inactive consumer sessions that have exceeded the timeout.
    ///
    /// This method should be called periodically to clean up expired consumers.
    ///
    /// # Implementation
    /// Minimizes write lock contention by separating read and write operations:
    /// - Collect expired sessions using read-only iteration
    /// - Batch remove expired sessions with minimal write lock duration
    ///
    /// # Timeout
    /// Consumers that haven't sent heartbeat for more than `channel_expired_timeout`
    /// will be removed.
    pub(crate) fn scan_inactive_sessions(&self) -> Vec<ClientSessionRetirement> {
        let current_time = current_millis();
        let mut close_handles = Vec::new();

        let mut expired_sessions: Vec<(CheetahString, ClientSessionInfo)> = Vec::new();

        for entry in self.consumer_table.iter() {
            let group = entry.key().clone();
            let consumer_group_info = entry.value();
            expired_sessions.extend(
                consumer_group_info
                    .session_info_snapshot()
                    .into_iter()
                    .filter(|client| {
                        current_time.saturating_sub(client.last_update_timestamp()) > self.channel_expired_timeout
                    })
                    .map(|client| (group.clone(), client)),
            );
        }

        for (group, candidate) in expired_sessions {
            let transition = self
                .session_transition_locks
                .lock(candidate.client_id(), candidate.session_id());
            let Some(consumer_group_info) = self.consumer_table.get(&group) else {
                continue;
            };
            let Some(client) = consumer_group_info.unregister_session_if_expired(
                candidate.session_id(),
                current_time,
                self.channel_expired_timeout,
            ) else {
                continue;
            };
            warn!("expired consumer session removed");
            let notify_members_changed = self
                .session_notify_table
                .remove(&(group.clone(), client.session_id()))
                .is_some_and(|(_, notify)| notify)
                && !is_broadcast_mode(consumer_group_info.get_message_model());
            let members = notify_members_changed.then(|| consumer_group_info.connection_identity_snapshot());
            let group_is_empty = consumer_group_info.channels_is_empty();
            let mut callbacks = Vec::with_capacity(2);
            if let Some(members) = members {
                callbacks.push(ConsumerSessionCallback::MembersChanged {
                    group: group.clone(),
                    members,
                });
            }
            if group_is_empty {
                callbacks.push(ConsumerSessionCallback::GroupUnregister(group.clone()));
                self.clear_topic_group_table(&consumer_group_info);
            }
            let callback_drain = self.enqueue_consumer_group_callbacks(&group, callbacks);
            drop(consumer_group_info);
            let session_has_groups = self.remove_consumer_session_group_index(client.session_id(), &group);
            if !session_has_groups {
                self.client_session_table
                    .remove_if(client.client_id(), |_, current| *current == client.session_id());
                self.session_client_table
                    .remove_if(&client.session_id(), |_, current| current == client.client_id());
                if let Some((_, transport)) = self.session_transport_table.remove(&client.session_id()) {
                    if let Some(registry) = self.v2_session_registry.get().cloned() {
                        self.session_transition_locks.mark_retiring(
                            &transition,
                            client.client_id(),
                            client.session_id(),
                        );
                        close_handles.push(transport.retirement(registry));
                    }
                }
            }
            if group_is_empty {
                self.consumer_table
                    .remove_if(&group, |_, current| current.channels_is_empty());
            }
            drop(transition);
            if let Some(callback_drain) = callback_drain {
                self.drain_consumer_session_callbacks(callback_drain);
            }
        }

        self.remove_expire_consumer_group_info();
        close_handles
    }

    #[cfg(test)]
    fn expire_session_for_test(&self, session_id: SessionId) {
        for group in self.consumer_table.iter() {
            group.set_session_last_update_timestamp_for_test(session_id, 0);
        }
    }
}
