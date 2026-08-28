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
use std::sync::LazyLock;
use std::sync::Weak;
use std::time::Instant;

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::BrokerStatsManager;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v2::SessionId;
use tracing::info;
use tracing::warn;

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::client_channel_info::ClientSessionInfo;
use crate::client::consumer_group_event::ConsumerGroupEvent;
use crate::client::consumer_group_info::ConsumerGroupInfo;
use crate::client::consumer_ids_change_listener::ConsumerConnectionIdentity;
use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
use crate::client::session_transition_locks::ClientSessionTransitionGuard;
use crate::client::session_transition_locks::ClientSessionTransitionLocks;
use crate::long_polling::pull_deferred::PullSessionClientLookup;

/// Global mapping: Channel ID -> Set<Consumer Group>
///
/// This enables O(1) channel-to-group lookup for fast channel close event processing
/// instead of O(n) traversal of all consumer groups. Only used when
/// `enable_fast_channel_event_process` is enabled in broker configuration.
///
/// # Memory Management
/// Entries are automatically cleaned up during:
/// - Consumer group unregistration
/// - Inactive channel scanning
/// - Channel close events
static CHANNEL_CONSUMER_GROUPS: LazyLock<DashMap<CheetahString, HashSet<CheetahString>>> =
    LazyLock::new(|| DashMap::with_capacity_and_shard_amount(4096, 64));

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
}

/// Manages consumer client connections and their lifecycle.
///
/// This manager maintains:
/// - Consumer group registrations and subscription relationships
/// - Channel heartbeat status and expiration detection
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
    /// Timeout for considering a consumer channel as expired (in milliseconds)
    channel_expired_timeout: u64,
    /// Timeout for subscription data expiration (in milliseconds)
    subscription_expired_timeout: u64,
}

/// Shared consumer-connection mutation capability for Broker housekeeping.
///
/// The handle shares the live manager state but exposes only inactive-channel scanning and
/// channel-close cleanup. It cannot register consumers, query subscriptions, or mutate manager
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
    pub(crate) fn register_consumer(
        &self,
        group: &CheetahString,
        client: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        subscriptions: HashSet<SubscriptionData>,
        notify_consumer_ids_changed: bool,
    ) -> bool {
        self.manager.register_consumer(
            group,
            client,
            consume_type,
            message_model,
            consume_from_where,
            subscriptions,
            notify_consumer_ids_changed,
        )
    }

    pub(crate) fn register_consumer_without_sub(
        &self,
        group: &CheetahString,
        client: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        notify_consumer_ids_changed: bool,
    ) -> bool {
        self.manager.register_consumer_without_sub(
            group,
            client,
            consume_type,
            message_model,
            consume_from_where,
            notify_consumer_ids_changed,
        )
    }

    pub(crate) fn unregister_consumer(
        &self,
        group: &str,
        client: &ClientChannelInfo,
        notify_consumer_ids_changed: bool,
    ) {
        self.manager
            .unregister_consumer(group, client, notify_consumer_ids_changed);
    }

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
            .contains(group)
    }

    pub(crate) fn register_consumer_sessions(
        &self,
        client: ClientSessionInfo,
        registrations: Vec<ConsumerSessionRegistration>,
    ) -> HashSet<CheetahString> {
        self.manager.register_consumer_sessions(client, registrations)
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

    pub(crate) fn complete_consumer_sessions(&self, batch: ConsumerSessionBatch) -> HashSet<CheetahString> {
        self.manager.complete_consumer_session_batch(batch)
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
}

impl PullSessionClientLookup for ConsumerSessionRegistry {
    fn client_id(&self, session_id: SessionId, consumer_group: &CheetahString) -> Option<CheetahString> {
        self.consumer_table
            .get(consumer_group)
            .and_then(|info| info.session_client_id(session_id))
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
    /// * `expired_timeout` - Timeout for channel and subscription expiration (milliseconds)
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
            for (_, client) in group_info.channel_info_snapshot() {
                *counts
                    .entry((group.clone(), client.language(), client.version(), consume_type))
                    .or_default() += 1;
            }
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

    /// Checks if fast channel event processing is enabled.
    ///
    /// When enabled, channel close events use O(1) lookup via CHANNEL_CONSUMER_GROUPS
    /// instead of O(n) traversal of all consumer groups.
    #[inline]
    fn is_fast_channel_event_process_enabled(&self) -> bool {
        self.broker_config
            .as_ref()
            .is_some_and(|config| config.enable_fast_channel_event_process)
    }

    /// Finds a consumer channel by client ID within a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_id` - Client identifier
    ///
    /// # Returns
    /// Client channel info if found
    pub fn find_channel_by_client_id(&self, group: &str, client_id: &str) -> Option<ClientChannelInfo> {
        if let Some(consumer_group_info) = self.consumer_table.get(group) {
            return consumer_group_info.find_channel_by_client_id(client_id);
        }
        None
    }

    /// Finds a consumer channel by channel reference within a consumer group.
    ///
    /// # Arguments  
    /// * `group` - Consumer group name
    /// * `channel` - Channel reference
    ///
    /// # Returns
    /// Client channel info if found
    pub fn find_channel_by_channel(&self, group: &str, channel: &Channel) -> Option<ClientChannelInfo> {
        if let Some(consumer_group_info) = self.consumer_table.get(group) {
            return consumer_group_info.find_channel_by_channel(channel);
        }
        None
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

    /// Restores durable POP classification and subscriptions without creating a network channel.
    pub(crate) fn restore_pop_consumer_profile(&self, group: &CheetahString, subscriptions: &[SubscriptionData]) {
        self.compensate_basic_consumer_info(group, ConsumeType::ConsumePop, MessageModel::Clustering);
        for subscription in subscriptions {
            self.compensate_subscribe_data(group, &subscription.topic, subscription);
        }
    }

    pub(crate) fn remove_compensated_consumer_profile(&self, group: &CheetahString) {
        self.consumer_compensation_table.remove(group);
    }

    /// Registers a consumer in a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_channel_info` - Client channel information
    /// * `consume_type` - Consume type (push/pull)
    /// * `message_model` - Message model (clustering/broadcasting)
    /// * `consume_from_where` - Where to start consuming
    /// * `sub_list` - Set of subscription data
    /// * `is_notify_consumer_ids_changed_enable` - Whether to notify listeners
    ///
    /// # Returns
    /// `true` if registration changed consumer state
    pub fn register_consumer(
        &self,
        group: &CheetahString,
        client_channel_info: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        sub_list: HashSet<SubscriptionData>,
        is_notify_consumer_ids_changed_enable: bool,
    ) -> bool {
        self.register_consumer_ext(
            group,
            client_channel_info,
            consume_type,
            message_model,
            consume_from_where,
            sub_list,
            is_notify_consumer_ids_changed_enable,
            true,
        )
    }

    /// Registers a consumer with extended options.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_channel_info` - Client channel information
    /// * `consume_type` - Consume type (push/pull)
    /// * `message_model` - Message model (clustering/broadcasting)
    /// * `consume_from_where` - Where to start consuming
    /// * `sub_list` - Set of subscription data
    /// * `is_notify_consumer_ids_changed_enable` - Whether to notify listeners
    /// * `update_subscription` - Whether to update subscription data
    ///
    /// # Returns
    /// `true` if registration changed consumer state
    pub fn register_consumer_ext(
        &self,
        group: &CheetahString,
        client_channel_info: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        sub_list: HashSet<SubscriptionData>,
        is_notify_consumer_ids_changed_enable: bool,
        update_subscription: bool,
    ) -> bool {
        let start = Instant::now();
        let mut consumer_group_info = self
            .consumer_table
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::new(group.clone(), consume_type, message_model, consume_from_where));

        // Maintain topic_group_table reverse index: Topic -> Set<Group>
        // When subscription changes, remove group from topics that are no longer subscribed
        if update_subscription {
            // Get old topics before update
            let old_topics = consumer_group_info.get_subscribe_topics();
            let new_topics: HashSet<CheetahString> = sub_list.iter().map(|s| s.topic.clone()).collect();

            // Remove group from topics that are no longer subscribed
            for old_topic in old_topics {
                if !new_topics.contains(&old_topic) {
                    if let Some(mut groups) = self.topic_group_table.get_mut(&old_topic) {
                        groups.remove(group);
                        if groups.is_empty() {
                            drop(groups);
                            self.topic_group_table
                                .remove_if(&old_topic, |_, current| current.is_empty());
                        }
                    }
                }
            }
        }

        // Add group to new topics
        for subscription_data in sub_list.iter() {
            let topic = &subscription_data.topic;
            self.topic_group_table
                .entry(topic.clone())
                .or_default()
                .insert(group.clone());
        }

        let r1 = consumer_group_info.update_channel(
            client_channel_info.clone(),
            consume_type,
            message_model,
            consume_from_where,
        );

        if r1 {
            let topics: HashSet<CheetahString> = sub_list.iter().map(|item| item.topic.clone()).collect();
            self.call_consumer_ids_change_listener(
                ConsumerGroupEvent::ClientRegister,
                group,
                &[&client_channel_info as &dyn Any, &topics as &dyn Any],
            );

            // Maintain Channel -> Groups mapping for fast channel close event processing
            if self.is_fast_channel_event_process_enabled() {
                let channel_id = client_channel_info.channel().channel_id_owned();
                CHANNEL_CONSUMER_GROUPS
                    .entry(channel_id)
                    .or_default()
                    .insert(group.clone());
            }
        }

        let r2 = if update_subscription {
            consumer_group_info.update_subscription(&sub_list)
        } else {
            false
        };

        if (r1 || r2)
            && is_notify_consumer_ids_changed_enable
            && consumer_group_info.get_message_model() != MessageModel::Broadcasting
        {
            let all_channel = consumer_group_info.get_all_channels();
            self.call_consumer_ids_change_listener(ConsumerGroupEvent::Change, group, &[&all_channel as &dyn Any]);
        }

        if let Some(broker_stats_manager) = self.broker_stats_manager.as_ref() {
            if let Some(broker_stats_manager) = broker_stats_manager.upgrade() {
                broker_stats_manager.inc_consumer_register_time(start.elapsed().as_millis() as i32);
            }
        }
        self.call_consumer_ids_change_listener(
            ConsumerGroupEvent::Register,
            group,
            &[&sub_list as &dyn Any, &client_channel_info as &dyn Any],
        );

        r1 || r2
    }

    /// Registers a consumer without subscription data.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_channel_info` - Client channel information
    /// * `consume_type` - Consume type (push/pull)
    /// * `message_model` - Message model (clustering/broadcasting)
    /// * `consume_from_where` - Where to start consuming
    /// * `is_notify_consumer_ids_changed_enable` - Whether to notify listeners
    ///
    /// # Returns
    /// `true` if registration changed consumer state
    pub fn register_consumer_without_sub(
        &self,
        group: &CheetahString,
        client_channel_info: ClientChannelInfo,
        consume_type: ConsumeType,
        message_model: MessageModel,
        consume_from_where: ConsumeFromWhere,
        is_notify_consumer_ids_changed_enable: bool,
    ) -> bool {
        let start = Instant::now();
        let mut consumer_group_info = self
            .consumer_table
            .entry(group.clone())
            .or_insert_with(|| ConsumerGroupInfo::new(group.clone(), consume_type, message_model, consume_from_where));
        let r1 =
            consumer_group_info.update_channel(client_channel_info, consume_type, message_model, consume_from_where);

        if r1 && is_notify_consumer_ids_changed_enable && !is_broadcast_mode(consumer_group_info.get_message_model()) {
            let channels = consumer_group_info.get_all_channels();
            self.call_consumer_ids_change_listener(ConsumerGroupEvent::Change, group, &[&channels as &dyn Any]);
        }

        if let Some(broker_stats_manager) = self.broker_stats_manager.as_ref() {
            if let Some(broker_stats_manager) = broker_stats_manager.upgrade() {
                broker_stats_manager.inc_consumer_register_time(start.elapsed().as_millis() as i32);
            }
        }
        r1
    }

    fn register_consumer_sessions(
        &self,
        client: ClientSessionInfo,
        registrations: Vec<ConsumerSessionRegistration>,
    ) -> HashSet<CheetahString> {
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
            warn!(
                "ignore consumer heartbeat that changes client identity for live session {:?}",
                client.session_id()
            );
            return ConsumerSessionBatch::default();
        }
        let replaced_session = self
            .client_session_table
            .get(client.client_id())
            .map(|entry| *entry.value())
            .filter(|session_id| *session_id != client.session_id());
        if registrations.is_empty() {
            let removals = replaced_session
                .map(|session_id| self.unregister_consumer_session_all_locked(session_id))
                .unwrap_or_default();
            let mut batch = ConsumerSessionBatch {
                started_at: Some(start),
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
        self.client_session_table
            .insert(client.client_id().clone(), client.session_id());
        self.session_client_table
            .insert(client.session_id(), client.client_id().clone());
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
        }
    }

    fn complete_consumer_session_batch(&self, batch: ConsumerSessionBatch) -> HashSet<CheetahString> {
        let ConsumerSessionBatch {
            started_at,
            changed_groups,
            callback_drains,
        } = batch;
        for callback_drain in callback_drains {
            self.drain_consumer_session_callbacks(callback_drain);
        }
        if let (Some(started_at), Some(stats)) =
            (started_at, self.broker_stats_manager.as_ref().and_then(Weak::upgrade))
        {
            stats.inc_consumer_register_time(started_at.elapsed().as_millis() as i32);
        }
        changed_groups
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

    /// Unregisters a consumer from a consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `client_channel_info` - Client channel information
    /// * `is_notify_consumer_ids_changed_enable` - Whether to notify listeners
    pub fn unregister_consumer(
        &self,
        group: &str,
        client_channel_info: &ClientChannelInfo,
        is_notify_consumer_ids_changed_enable: bool,
    ) {
        let Some(consumer_group_info) = self.consumer_table.get_mut(group) else {
            return;
        };
        let removed = consumer_group_info.unregister_channel(client_channel_info);
        if removed {
            self.call_consumer_ids_change_listener(
                ConsumerGroupEvent::ClientUnregister,
                group,
                &[
                    client_channel_info as &dyn Any,
                    &consumer_group_info.get_subscribe_topics() as &dyn Any,
                ],
            );

            // Remove group from channel mapping when unregistering
            if self.is_fast_channel_event_process_enabled() {
                let channel_id = client_channel_info.channel().channel_id_owned();
                if let Some(mut groups) = CHANNEL_CONSUMER_GROUPS.get_mut(&channel_id) {
                    groups.remove(group);
                    if groups.is_empty() {
                        drop(groups);
                        CHANNEL_CONSUMER_GROUPS.remove_if(&channel_id, |_, current| current.is_empty());
                    }
                }
            }
        }
        let message_model = consumer_group_info.get_message_model();
        let channels = consumer_group_info.get_all_channels();
        if consumer_group_info.channels_is_empty() {
            self.clear_topic_group_table(&consumer_group_info);
            #[cfg(test)]
            self.pause_empty_group_removal_for_test();
            drop(consumer_group_info);
            if self
                .consumer_table
                .remove_if(group, |_, current| current.channels_is_empty())
                .is_some()
            {
                info!(
                    "unregister consumer ok, no any connection, and remove consumer group, {}",
                    group
                );
                self.call_consumer_ids_change_listener(ConsumerGroupEvent::Unregister, group, &[]);
            }
        }

        if is_notify_consumer_ids_changed_enable && !is_broadcast_mode(message_model) {
            self.call_consumer_ids_change_listener(ConsumerGroupEvent::Change, group, &[&channels as &dyn Any]);
        }
    }

    /// Removes expired consumer group info from compensation table.
    ///
    /// Cleans up subscriptions that have not been updated for longer than
    /// subscription_expired_timeout.
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

    /// Scans and removes inactive consumer channels that have exceeded the timeout.
    ///
    /// This method should be called periodically to clean up expired consumers.
    ///
    /// # Implementation
    /// Minimizes write lock contention by separating read and write operations:
    /// - Collect expired channels using read-only iteration
    /// - Batch remove expired channels with minimal write lock duration
    ///
    /// # Timeout
    /// Consumers that haven't sent heartbeat for more than channel_expired_timeout
    /// will be removed.
    pub fn scan_not_active_channel(&self) {
        let current_time = current_millis();

        // Collect expired channels without holding write locks
        // Uses iter() instead of iter_mut() to allow concurrent reads
        let mut expired_channels: Vec<(CheetahString, Channel)> = Vec::new();
        let mut expired_sessions: Vec<(CheetahString, ClientSessionInfo)> = Vec::new();
        let mut groups_to_check_empty = Vec::new();

        for entry in self.consumer_table.iter() {
            let group = entry.key().clone();
            let consumer_group_info = entry.value();
            let channel_info_snapshot = consumer_group_info.channel_info_snapshot();

            let mut group_has_expired = false;
            let mut legacy_channel_expired = false;
            // Collect expired channels for this group
            for (channel, client_channel_info) in channel_info_snapshot {
                let diff = current_time as i64 - client_channel_info.last_update_timestamp() as i64;

                if diff > self.channel_expired_timeout as i64 {
                    warn!(
                        "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                        channel.channel_id(),
                        group
                    );

                    expired_channels.push((group.clone(), channel));
                    group_has_expired = true;
                    legacy_channel_expired = true;
                }
            }

            expired_sessions.extend(
                consumer_group_info
                    .session_info_snapshot()
                    .into_iter()
                    .filter(|client| {
                        current_time.saturating_sub(client.last_update_timestamp()) > self.channel_expired_timeout
                    })
                    .map(|client| (group.clone(), client)),
            );

            // Mark groups that might become empty after cleanup
            if group_has_expired {
                groups_to_check_empty.push((group, legacy_channel_expired));
            }
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
            warn!(
                "SCAN: remove expired V2 session from ConsumerManager consumerTable. session={:?}, \
                 consumerGroup={}",
                client.session_id(),
                group
            );
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

        // Batch remove expired channels with minimal write lock duration
        // Process collected channels and notify listeners
        for (group, channel) in expired_channels {
            // Remove channel from group
            if let Some(consumer_group_info) = self.consumer_table.get(&group) {
                consumer_group_info.remove_channel(&channel);

                // Clean up fast channel event process mapping
                if self.is_fast_channel_event_process_enabled() {
                    let channel_id = channel.channel_id_owned();
                    if let Some(mut groups) = CHANNEL_CONSUMER_GROUPS.get_mut(&channel_id) {
                        groups.remove(&group);
                        if groups.is_empty() {
                            drop(groups);
                            CHANNEL_CONSUMER_GROUPS.remove_if(&channel_id, |_, current| current.is_empty());
                        }
                    }
                }

                // Notify listeners about channel unregistration
                if let Some(client_channel_info) = consumer_group_info.find_channel_by_channel(&channel) {
                    self.call_consumer_ids_change_listener(
                        ConsumerGroupEvent::ClientUnregister,
                        &group,
                        &[
                            &client_channel_info as &dyn Any,
                            &consumer_group_info.get_subscribe_topics() as &dyn Any,
                        ],
                    );
                }
            }
        }

        // Handle empty groups and send change notifications.
        for (group, notify_legacy_change) in groups_to_check_empty {
            if let Some(consumer_group_info) = self.consumer_table.get(&group) {
                if consumer_group_info.channels_is_empty() {
                    warn!(
                        "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                        group
                    );
                    self.clear_topic_group_table(&consumer_group_info);
                    #[cfg(test)]
                    self.pause_empty_group_removal_for_test();
                    drop(consumer_group_info);
                    if self
                        .consumer_table
                        .remove_if(&group, |_, current| current.channels_is_empty())
                        .is_some()
                    {
                        self.call_consumer_ids_change_listener(ConsumerGroupEvent::Unregister, group.as_str(), &[]);
                    }
                } else if notify_legacy_change && !is_broadcast_mode(consumer_group_info.get_message_model()) {
                    // Notify remaining channels about the change
                    self.call_consumer_ids_change_listener(
                        ConsumerGroupEvent::Change,
                        &group,
                        &[&consumer_group_info.get_all_channels() as &dyn Any],
                    );
                }
            }
        }

        self.remove_expire_consumer_group_info();
    }

    /// Handles channel close events and removes the associated consumer.
    ///
    /// This method is typically called by the connection layer when a channel is closed.
    ///
    /// # Arguments
    /// * `_remote_addr` - The remote address of the closed channel
    /// * `channel` - The closed channel
    ///
    /// # Returns
    /// `true` if at least one consumer was removed
    pub fn do_channel_close_event(&self, _remote_addr: &str, channel: &Channel) -> bool {
        let mut removed = false;
        let mut remove_list = Vec::new();

        // Fast path: lookup affected groups using channel-to-group mapping
        if self.is_fast_channel_event_process_enabled() {
            let channel_id = channel.channel_id_owned();
            if let Some((_, groups)) = CHANNEL_CONSUMER_GROUPS.remove(&channel_id) {
                // Process only the groups associated with this channel
                for group in groups {
                    if let Some(info) = self.consumer_table.get_mut(&group) {
                        if let Some(client_channel_info) = info.handle_channel_close_event(channel) {
                            self.call_consumer_ids_change_listener(
                                ConsumerGroupEvent::ClientUnregister,
                                &group,
                                &[
                                    &client_channel_info as &dyn Any,
                                    &info.get_subscribe_topics() as &dyn Any,
                                ],
                            );

                            if info.channels_is_empty() {
                                self.clear_topic_group_table(&info);
                                #[cfg(test)]
                                self.pause_empty_group_removal_for_test();
                                remove_list.push(group.clone());
                            } else if !is_broadcast_mode(info.get_message_model()) {
                                self.call_consumer_ids_change_listener(
                                    ConsumerGroupEvent::Change,
                                    &group,
                                    &[&info.get_all_channels() as &dyn Any],
                                );
                            }

                            removed = true;
                        }
                    }
                }

                // Process removal list
                for group in remove_list {
                    if self
                        .consumer_table
                        .remove_if(&group, |_, current| current.channels_is_empty())
                        .is_some()
                    {
                        info!(
                            "unregister consumer ok, no any connection, and remove consumer group, {}",
                            group
                        );
                        self.call_consumer_ids_change_listener(ConsumerGroupEvent::Unregister, group.as_str(), &[]);
                    }
                }

                return removed;
            }
        }

        // Fallback path: scan all consumer groups
        for mut entry in self.consumer_table.iter_mut() {
            let group = entry.key().clone();
            let info = entry.value_mut();
            if let Some(client_channel_info) = info.handle_channel_close_event(channel) {
                self.call_consumer_ids_change_listener(
                    ConsumerGroupEvent::ClientUnregister,
                    &group,
                    &[
                        &client_channel_info as &dyn Any,
                        &info.get_subscribe_topics() as &dyn Any,
                    ],
                );

                if info.channels_is_empty() {
                    self.clear_topic_group_table(info);
                    #[cfg(test)]
                    self.pause_empty_group_removal_for_test();
                    remove_list.push(group.clone());
                } else if !is_broadcast_mode(info.get_message_model()) {
                    // Send Change event only if group still has active channels
                    self.call_consumer_ids_change_listener(
                        ConsumerGroupEvent::Change,
                        &group,
                        &[&info.get_all_channels() as &dyn Any],
                    );
                }

                removed = true;
            }
        }

        for group in remove_list {
            if self
                .consumer_table
                .remove_if(&group, |_, current| current.channels_is_empty())
                .is_some()
            {
                info!(
                    "unregister consumer ok, no any connection, and remove consumer group, {}",
                    group
                );
                self.call_consumer_ids_change_listener(ConsumerGroupEvent::Unregister, group.as_str(), &[]);
            }
        }

        removed
    }
}

/// Checks if the message model is broadcasting mode.
///
/// # Arguments
/// * `message_model` - Message model to check
///
/// # Returns
/// `true` if broadcasting mode
fn is_broadcast_mode(message_model: MessageModel) -> bool {
    message_model == MessageModel::Broadcasting
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::Mutex as StdMutex;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_protocol::protocol::LanguageCode;
    use rocketmq_transport::api::v1::Channel;
    use rocketmq_transport::test_support::session_id_for_test;
    use rocketmq_transport::test_support::Connection;
    use tokio::net::TcpStream;

    use super::ConsumerAssignmentView;
    use super::ConsumerManager;
    use super::ConsumerSessionRegistration;
    use super::ConsumerSessionRegistry;
    use crate::client::client_channel_info::ClientChannelInfo;
    use crate::client::client_channel_info::ClientSessionInfo;
    use crate::client::consumer_group_event::ConsumerGroupEvent;
    use crate::client::consumer_group_info::ConsumerGroupInfo;
    use crate::client::consumer_ids_change_listener::ConsumerConnectionIdentity;
    use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
    use crate::long_polling::pull_deferred::PullSessionClientLookup;

    struct NoopConsumerListener;

    impl ConsumerIdsChangeListener for NoopConsumerListener {
        fn handle(&self, _event: ConsumerGroupEvent, _group: &str, _args: &[&dyn Any]) {}

        fn shutdown(&self) {}
    }

    struct BlockingRecordingListener {
        calls: AtomicUsize,
        first_entered: Barrier,
        release_first: Barrier,
        snapshots: StdMutex<Vec<Vec<ConsumerConnectionIdentity>>>,
    }

    impl BlockingRecordingListener {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                first_entered: Barrier::new(2),
                release_first: Barrier::new(2),
                snapshots: StdMutex::new(Vec::new()),
            }
        }
    }

    impl ConsumerIdsChangeListener for BlockingRecordingListener {
        fn handle(&self, _event: ConsumerGroupEvent, _group: &str, _args: &[&dyn Any]) {}

        fn handle_connection_change(&self, _group: &str, members: &[ConsumerConnectionIdentity]) {
            if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
                self.first_entered.wait();
                self.release_first.wait();
            }
            self.snapshots.lock().expect("snapshot lock").push(members.to_vec());
        }

        fn shutdown(&self) {}
    }

    fn session_registration(group: CheetahString) -> ConsumerSessionRegistration {
        ConsumerSessionRegistration {
            group,
            consume_type: ConsumeType::ConsumePassively,
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            subscriptions: HashSet::new(),
            notify_consumer_ids_changed: false,
            update_subscription: true,
        }
    }

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
    async fn consumer_assignment_view_tracks_live_primary_client_ids() {
        let consumer_table = Arc::new(DashMap::new());
        let view = ConsumerAssignmentView {
            consumer_table: Arc::clone(&consumer_table),
        };
        let group = CheetahString::from_static_str("assignment-group");
        assert!(view.client_ids(&group).is_empty());
        assert_eq!(view.client_ids_if_present(&group), None);

        let group_info = ConsumerGroupInfo::with_group_name(group.clone());
        let client = ClientChannelInfo::new(
            create_test_channel().await,
            "assignment-client".into(),
            LanguageCode::default(),
            1,
        );
        group_info.upsert_channel_info(client.clone());
        consumer_table.insert(group.clone(), group_info.clone());

        assert_eq!(
            view.client_ids(&group),
            vec![CheetahString::from_static_str("assignment-client")]
        );
        assert_eq!(
            view.client_ids_if_present(&group),
            Some(vec![CheetahString::from_static_str("assignment-client")])
        );

        assert!(group_info.unregister_channel(&client));
        assert!(view.client_ids(&group).is_empty());
        assert_eq!(view.client_ids_if_present(&group), Some(Vec::new()));
    }

    #[test]
    fn consumer_session_registry_tracks_live_session_group_identity() {
        let consumer_table = Arc::new(DashMap::new());
        let registry = ConsumerSessionRegistry {
            consumer_table: Arc::clone(&consumer_table),
        };
        let group = CheetahString::from_static_str("session-group");
        let other_group = CheetahString::from_static_str("other-group");
        let session_id = session_id_for_test(41);
        let other_session_id = session_id_for_test(42);
        let mut group_info = ConsumerGroupInfo::new(
            group.clone(),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
        );
        assert!(group_info.update_session(
            ClientSessionInfo::new(session_id, "client-a".into(), None, LanguageCode::RUST, 1),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
        ));
        consumer_table.insert(group.clone(), group_info.clone());

        assert_eq!(
            crate::long_polling::pull_deferred::PullSessionClientLookup::client_id(&registry, session_id, &group),
            Some(CheetahString::from_static_str("client-a"))
        );
        assert_eq!(
            crate::long_polling::pull_deferred::PullSessionClientLookup::client_id(&registry, session_id, &other_group),
            None
        );
        assert_eq!(
            crate::long_polling::pull_deferred::PullSessionClientLookup::client_id(&registry, other_session_id, &group),
            None
        );

        assert!(!group_info.update_session(
            ClientSessionInfo::new(session_id, "client-a".into(), None, LanguageCode::RUST, 2),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
        ));
        assert_eq!(
            crate::long_polling::pull_deferred::PullSessionClientLookup::client_id(&registry, session_id, &group),
            Some(CheetahString::from_static_str("client-a"))
        );
        assert_eq!(group_info.session_info_snapshot()[0].version(), 2);

        assert_eq!(
            group_info
                .unregister_session(session_id)
                .map(|client| client.client_id().clone()),
            Some(CheetahString::from_static_str("client-a"))
        );
        assert_eq!(
            crate::long_polling::pull_deferred::PullSessionClientLookup::client_id(&registry, session_id, &group),
            None
        );
    }

    #[test]
    fn consumer_session_reconnect_and_stale_disconnect_preserve_the_new_session() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 120_000);
        let registration = manager.client_registration();
        let registry = manager.session_registry();
        let old_session = session_id_for_test(51);
        let new_session = session_id_for_test(52);
        let group_a = CheetahString::from_static_str("reconnect-group-a");
        let group_b = CheetahString::from_static_str("reconnect-group-b");
        for group in [&group_a, &group_b] {
            assert!(registration.register_consumer_session(
                group,
                ClientSessionInfo::new(old_session, "reconnect-client".into(), None, LanguageCode::RUST, 1),
                ConsumeType::ConsumePassively,
                MessageModel::Clustering,
                ConsumeFromWhere::ConsumeFromLastOffset,
                HashSet::new(),
                false,
            ));
        }
        assert_eq!(
            manager.session_to_groups.get(&old_session).map(|groups| groups.len()),
            Some(2)
        );

        assert!(registration.register_consumer_session(
            &group_a,
            ClientSessionInfo::new(new_session, "reconnect-client".into(), None, LanguageCode::RUST, 2),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
            HashSet::new(),
            false,
        ));
        assert_eq!(registry.client_id(old_session, &group_a), None);
        assert_eq!(registry.client_id(old_session, &group_b), None);
        assert_eq!(
            registry.client_id(new_session, &group_a),
            Some("reconnect-client".into())
        );
        assert!(!manager.session_to_groups.contains_key(&old_session));
        assert_eq!(
            manager.client_session_table.get("reconnect-client").map(|entry| *entry),
            Some(new_session)
        );

        registration.unregister_consumer_session(group_a.as_str(), old_session, false);
        assert_eq!(
            registry.client_id(new_session, &group_a),
            Some("reconnect-client".into())
        );
        registration.unregister_consumer_session(group_a.as_str(), new_session, false);
        assert!(!manager.session_to_groups.contains_key(&new_session));
        assert!(!manager.client_session_table.contains_key("reconnect-client"));
    }

    #[test]
    fn concurrent_consumer_reconnect_has_one_canonical_session() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 120_000);
        let first_registration = manager.client_registration();
        let second_registration = first_registration.clone();
        let group = CheetahString::from_static_str("concurrent-consumer-group");
        let first_session = session_id_for_test(5_501);
        let second_session = session_id_for_test(5_502);
        let barrier = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            let first_group = group.clone();
            let first_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                first_barrier.wait();
                first_registration.register_consumer_session(
                    &first_group,
                    ClientSessionInfo::new(first_session, "concurrent-client".into(), None, LanguageCode::RUST, 1),
                    ConsumeType::ConsumePassively,
                    MessageModel::Clustering,
                    ConsumeFromWhere::ConsumeFromLastOffset,
                    HashSet::new(),
                    false,
                );
            });
            let second_group = group.clone();
            let second_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                second_barrier.wait();
                second_registration.register_consumer_session(
                    &second_group,
                    ClientSessionInfo::new(second_session, "concurrent-client".into(), None, LanguageCode::RUST, 2),
                    ConsumeType::ConsumePassively,
                    MessageModel::Clustering,
                    ConsumeFromWhere::ConsumeFromLastOffset,
                    HashSet::new(),
                    false,
                );
            });
        });

        let canonical = *manager
            .client_session_table
            .get("concurrent-client")
            .expect("one canonical consumer session");
        let loser = if canonical == first_session {
            second_session
        } else {
            first_session
        };
        let group_info = manager.consumer_table.get(&group).expect("consumer group remains live");
        assert_eq!(group_info.session_info_snapshot().len(), 1);
        assert_eq!(
            group_info.session_client_id(canonical).as_deref(),
            Some("concurrent-client")
        );
        assert_eq!(group_info.session_client_id(loser), None);
        drop(group_info);
        assert!(!manager.session_to_groups.contains_key(&loser));

        manager
            .client_registration()
            .unregister_consumer_session(group.as_str(), loser, false);
        assert_eq!(
            manager.session_registry().client_id(canonical, &group).as_deref(),
            Some("concurrent-client")
        );
    }

    #[test]
    fn consumer_membership_callbacks_are_ordered_per_group_across_clients() {
        let listener = Arc::new(BlockingRecordingListener::new());
        let manager = ConsumerManager::new(listener.clone(), 120_000);
        let first_registration = manager.client_registration();
        let second_registration = first_registration.clone();
        let group = CheetahString::from_static_str("ordered-membership-group");
        let first_group = group.clone();

        std::thread::scope(|scope| {
            scope.spawn(move || {
                first_registration.register_consumer_session(
                    &first_group,
                    ClientSessionInfo::new(
                        session_id_for_test(5_511),
                        "ordered-client-a".into(),
                        None,
                        LanguageCode::RUST,
                        1,
                    ),
                    ConsumeType::ConsumePassively,
                    MessageModel::Clustering,
                    ConsumeFromWhere::ConsumeFromLastOffset,
                    HashSet::new(),
                    true,
                );
            });

            listener.first_entered.wait();
            second_registration.register_consumer_session(
                &group,
                ClientSessionInfo::new(
                    session_id_for_test(5_512),
                    "ordered-client-b".into(),
                    None,
                    LanguageCode::RUST,
                    1,
                ),
                ConsumeType::ConsumePassively,
                MessageModel::Clustering,
                ConsumeFromWhere::ConsumeFromLastOffset,
                HashSet::new(),
                true,
            );
            listener.release_first.wait();
        });

        let snapshots = listener.snapshots.lock().expect("snapshot lock");
        assert_eq!(snapshots.len(), 2);
        let client_ids = |members: &[ConsumerConnectionIdentity]| {
            let mut ids = members
                .iter()
                .map(|member| match member {
                    ConsumerConnectionIdentity::Legacy { client_id }
                    | ConsumerConnectionIdentity::Session { client_id, .. } => client_id.clone(),
                })
                .collect::<Vec<_>>();
            ids.sort();
            ids
        };
        assert_eq!(
            client_ids(&snapshots[0]),
            vec![CheetahString::from_static_str("ordered-client-a")]
        );
        assert_eq!(
            client_ids(&snapshots[1]),
            vec![
                CheetahString::from_static_str("ordered-client-a"),
                CheetahString::from_static_str("ordered-client-b"),
            ]
        );
    }

    #[test]
    fn replacing_an_empty_session_group_preserves_the_new_topic_reverse_index() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 120_000);
        let old_registration = manager.client_registration();
        let new_registration = old_registration.clone();
        let group = CheetahString::from_static_str("topic-index-replacement-group");
        let topic = CheetahString::from_static_str("topic-index-replacement-topic");
        let old_session = session_id_for_test(5_521);
        let new_session = session_id_for_test(5_522);
        let registration_for = |group: CheetahString| ConsumerSessionRegistration {
            group,
            consume_type: ConsumeType::ConsumePassively,
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            subscriptions: HashSet::from([SubscriptionData {
                topic: topic.clone(),
                sub_string: "*".into(),
                ..SubscriptionData::default()
            }]),
            notify_consumer_ids_changed: false,
            update_subscription: true,
        };
        old_registration.register_consumer_sessions(
            ClientSessionInfo::new(old_session, "topic-index-old".into(), None, LanguageCode::RUST, 1),
            vec![registration_for(group.clone())],
        );
        assert!(manager
            .topic_group_table
            .get(&topic)
            .is_some_and(|groups| groups.contains(&group)));

        let empty_entered = Arc::new(Barrier::new(2));
        let release_empty = Arc::new(Barrier::new(2));
        *manager.empty_group_barriers.write() = Some((Arc::clone(&empty_entered), Arc::clone(&release_empty)));
        let new_started = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let old_group = group.clone();
            scope.spawn(move || {
                old_registration.unregister_consumer_session(old_group.as_str(), old_session, false);
            });
            empty_entered.wait();

            let new_group = group.clone();
            let new_started_worker = Arc::clone(&new_started);
            scope.spawn(move || {
                new_started_worker.wait();
                new_registration.register_consumer_sessions(
                    ClientSessionInfo::new(new_session, "topic-index-new".into(), None, LanguageCode::RUST, 2),
                    vec![registration_for(new_group)],
                );
            });
            new_started.wait();
            release_empty.wait();
        });

        assert_eq!(
            manager.session_registry().client_id(new_session, &group).as_deref(),
            Some("topic-index-new")
        );
        assert!(manager
            .topic_group_table
            .get(&topic)
            .is_some_and(|groups| groups.contains(&group)));
    }

    #[tokio::test]
    async fn replacing_an_empty_legacy_group_with_a_session_preserves_the_new_topic_reverse_index() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 120_000);
        let legacy_registration = manager.client_registration();
        let session_registration = legacy_registration.clone();
        let group = CheetahString::from_static_str("legacy-topic-index-replacement-group");
        let topic = CheetahString::from_static_str("legacy-topic-index-replacement-topic");
        let legacy = ClientChannelInfo::new(
            create_test_channel().await,
            "legacy-topic-index-old".into(),
            LanguageCode::RUST,
            1,
        );
        let subscriptions = || {
            HashSet::from([SubscriptionData {
                topic: topic.clone(),
                sub_string: "*".into(),
                ..SubscriptionData::default()
            }])
        };
        legacy_registration.register_consumer(
            &group,
            legacy.clone(),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
            subscriptions(),
            false,
        );

        let empty_entered = Arc::new(Barrier::new(2));
        let release_empty = Arc::new(Barrier::new(2));
        *manager.empty_group_barriers.write() = Some((Arc::clone(&empty_entered), Arc::clone(&release_empty)));
        let new_started = Arc::new(Barrier::new(2));
        let new_session = session_id_for_test(5_523);
        std::thread::scope(|scope| {
            let old_group = group.clone();
            scope.spawn(move || {
                legacy_registration.unregister_consumer(old_group.as_str(), &legacy, false);
            });
            empty_entered.wait();

            let new_group = group.clone();
            let new_started_worker = Arc::clone(&new_started);
            scope.spawn(move || {
                new_started_worker.wait();
                session_registration.register_consumer_session(
                    &new_group,
                    ClientSessionInfo::new(
                        new_session,
                        "legacy-topic-index-new".into(),
                        None,
                        LanguageCode::RUST,
                        2,
                    ),
                    ConsumeType::ConsumePassively,
                    MessageModel::Clustering,
                    ConsumeFromWhere::ConsumeFromLastOffset,
                    subscriptions(),
                    false,
                );
            });
            new_started.wait();
            release_empty.wait();
        });

        assert_eq!(
            manager.session_registry().client_id(new_session, &group).as_deref(),
            Some("legacy-topic-index-new")
        );
        assert!(manager
            .topic_group_table
            .get(&topic)
            .is_some_and(|groups| groups.contains(&group)));
    }

    #[test]
    fn removing_an_empty_topic_entry_does_not_delete_a_concurrent_group_registration() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 120_000);
        let first_registration = manager.client_registration();
        let second_registration = first_registration.clone();
        let first_group = CheetahString::from_static_str("topic-entry-first-group");
        let second_group = CheetahString::from_static_str("topic-entry-second-group");
        let topic = CheetahString::from_static_str("topic-entry-shared-topic");
        let registration_for = |group: CheetahString| ConsumerSessionRegistration {
            group,
            consume_type: ConsumeType::ConsumePassively,
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            subscriptions: HashSet::from([SubscriptionData {
                topic: topic.clone(),
                sub_string: "*".into(),
                ..SubscriptionData::default()
            }]),
            notify_consumer_ids_changed: false,
            update_subscription: true,
        };
        let first_session = session_id_for_test(5_524);
        let second_session = session_id_for_test(5_525);
        first_registration.register_consumer_sessions(
            ClientSessionInfo::new(first_session, "topic-entry-first".into(), None, LanguageCode::RUST, 1),
            vec![registration_for(first_group.clone())],
        );

        let empty_entered = Arc::new(Barrier::new(2));
        let release_empty = Arc::new(Barrier::new(2));
        *manager.topic_empty_entry_barriers.write() = Some((Arc::clone(&empty_entered), Arc::clone(&release_empty)));
        std::thread::scope(|scope| {
            let removed_group = first_group.clone();
            scope.spawn(move || {
                first_registration.unregister_consumer_session(removed_group.as_str(), first_session, false);
            });
            empty_entered.wait();
            second_registration.register_consumer_sessions(
                ClientSessionInfo::new(second_session, "topic-entry-second".into(), None, LanguageCode::RUST, 2),
                vec![registration_for(second_group.clone())],
            );
            release_empty.wait();
        });

        let indexed_groups = manager.query_topic_consume_by_who(&topic);
        assert!(!indexed_groups.contains(&first_group));
        assert!(indexed_groups.contains(&second_group));
        assert_eq!(
            manager
                .session_registry()
                .client_id(second_session, &second_group)
                .as_deref(),
            Some("topic-entry-second")
        );
    }

    #[tokio::test]
    async fn consumer_group_lives_until_both_channel_and_session_tables_are_empty() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 120_000);
        let registration = manager.client_registration();
        let group = CheetahString::from_static_str("mixed-consumer-group");
        let channel = create_test_channel().await;
        let legacy = ClientChannelInfo::new(channel, "legacy-client".into(), LanguageCode::RUST, 1);
        registration.register_consumer(
            &group,
            legacy.clone(),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
            HashSet::new(),
            false,
        );
        let session_id = session_id_for_test(53);
        registration.register_consumer_session(
            &group,
            ClientSessionInfo::new(session_id, "session-client".into(), None, LanguageCode::RUST, 2),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
            HashSet::new(),
            false,
        );

        registration.unregister_consumer(group.as_str(), &legacy, false);
        assert!(manager.get_consumer_group_info(&group).is_some());
        registration.unregister_consumer_session(group.as_str(), session_id, false);
        assert!(manager.get_consumer_group_info(&group).is_none());
    }

    #[test]
    fn consumer_session_expiry_rechecks_timestamp_and_cleans_reverse_indexes() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 1);
        let registration = manager.client_registration();
        let group = CheetahString::from_static_str("expired-session-group");
        let session_id = session_id_for_test(54);
        registration.register_consumer_session(
            &group,
            ClientSessionInfo::new(session_id, "expired-client".into(), None, LanguageCode::RUST, 1),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
            HashSet::new(),
            false,
        );
        manager
            .get_consumer_group_info(&group)
            .expect("registered group")
            .set_session_last_update_timestamp_for_test(session_id, 0);
        manager.scan_not_active_channel();

        assert_eq!(manager.session_registry().client_id(session_id, &group), None);
        assert!(!manager.session_to_groups.contains_key(&session_id));
        assert!(!manager.client_session_table.contains_key("expired-client"));

        let mut group_info = ConsumerGroupInfo::new(
            group,
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
        );
        let scan_timestamp = rocketmq_runtime::common::time_utils::current_millis();
        let mut stale = ClientSessionInfo::new(session_id, "refreshed-client".into(), None, LanguageCode::RUST, 1);
        stale.set_last_update_timestamp_for_test(scan_timestamp.saturating_sub(100));
        group_info.update_session(
            stale,
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
        );
        group_info.update_session(
            ClientSessionInfo::new(session_id, "refreshed-client".into(), None, LanguageCode::RUST, 2),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
        );
        assert!(group_info.remove_expired_sessions(scan_timestamp, 1).is_empty());
        assert_eq!(
            group_info.session_client_id(session_id),
            Some("refreshed-client".into())
        );
    }

    #[test]
    fn concurrent_consumer_heartbeat_batches_preserve_every_winning_group() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 120_000);
        let first_registration = manager.client_registration();
        let second_registration = first_registration.clone();
        let groups = vec![
            CheetahString::from_static_str("batch-consumer-a"),
            CheetahString::from_static_str("batch-consumer-b"),
        ];
        let first_session = session_id_for_test(5_601);
        let second_session = session_id_for_test(5_602);
        let barrier = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            let first_groups = groups.clone();
            let first_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                first_barrier.wait();
                first_registration.register_consumer_sessions(
                    ClientSessionInfo::new(first_session, "batch-client".into(), None, LanguageCode::RUST, 1),
                    first_groups.into_iter().map(session_registration).collect(),
                );
            });
            let second_groups = groups.clone();
            let second_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                second_barrier.wait();
                second_registration.register_consumer_sessions(
                    ClientSessionInfo::new(second_session, "batch-client".into(), None, LanguageCode::RUST, 2),
                    second_groups.into_iter().map(session_registration).collect(),
                );
            });
        });

        let canonical = *manager
            .client_session_table
            .get("batch-client")
            .expect("canonical batch consumer session");
        let loser = if canonical == first_session {
            second_session
        } else {
            first_session
        };
        for group in &groups {
            let group_info = manager.consumer_table.get(group).expect("winning group remains");
            assert_eq!(group_info.session_info_snapshot().len(), 1);
            assert_eq!(group_info.session_client_id(canonical).as_deref(), Some("batch-client"));
            assert_eq!(group_info.session_client_id(loser), None);
        }
        assert_eq!(
            manager.session_to_groups.get(&canonical).map(|entry| entry.len()),
            Some(2)
        );
        assert!(!manager.session_to_groups.contains_key(&loser));
        assert!(!manager.session_client_table.contains_key(&loser));
    }

    #[test]
    fn consumer_session_close_cleans_all_groups_and_stale_close_preserves_replacement() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 120_000);
        let registration = manager.client_registration();
        let housekeeping = manager.connection_housekeeping();
        let groups = [
            CheetahString::from_static_str("close-consumer-a"),
            CheetahString::from_static_str("close-consumer-b"),
        ];
        let old_session = session_id_for_test(5_701);
        registration.register_consumer_sessions(
            ClientSessionInfo::new(old_session, "close-client".into(), None, LanguageCode::RUST, 1),
            groups.iter().cloned().map(session_registration).collect(),
        );
        assert!(housekeeping.do_session_close_event(old_session));
        assert!(groups
            .iter()
            .all(|group| manager.get_consumer_group_info(group).is_none()));
        assert!(!manager.session_to_groups.contains_key(&old_session));

        let new_session = session_id_for_test(5_702);
        registration.register_consumer_sessions(
            ClientSessionInfo::new(new_session, "close-client".into(), None, LanguageCode::RUST, 2),
            groups.iter().cloned().map(session_registration).collect(),
        );
        assert!(!housekeeping.do_session_close_event(old_session));
        assert!(groups
            .iter()
            .all(|group| manager.get_consumer_group_info(group).is_some()));
        assert_eq!(
            manager.client_session_table.get("close-client").map(|entry| *entry),
            Some(new_session)
        );
    }

    #[test]
    fn consumer_session_identity_is_immutable() {
        let manager = ConsumerManager::new(Arc::new(NoopConsumerListener), 120_000);
        let registration = manager.client_registration();
        let session_id = session_id_for_test(5_801);
        let group = CheetahString::from_static_str("identity-consumer-a");
        registration.register_consumer_sessions(
            ClientSessionInfo::new(session_id, "identity-a".into(), None, LanguageCode::RUST, 1),
            vec![session_registration(group.clone())],
        );
        let changed = registration.register_consumer_sessions(
            ClientSessionInfo::new(session_id, "identity-b".into(), None, LanguageCode::RUST, 2),
            vec![session_registration(CheetahString::from_static_str(
                "identity-consumer-b",
            ))],
        );
        assert!(changed.is_empty());
        assert_eq!(
            manager.session_client_table.get(&session_id).map(|entry| entry.clone()),
            Some("identity-a".into())
        );
        assert!(!manager.consumer_table.contains_key("identity-consumer-b"));

        registration.unregister_consumer_session(group.as_str(), session_id, false);
        assert!(!manager.session_client_table.contains_key(&session_id));
    }
}
