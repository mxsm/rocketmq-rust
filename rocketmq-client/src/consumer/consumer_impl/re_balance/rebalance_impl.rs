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
use std::ops::Deref;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::mix_all;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::sync::RwLock;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::types::ConsumerGroupName;
use crate::types::TopicName;

const TIMEOUT_CHECK_TIMES: u32 = 3;
const QUERY_ASSIGNMENT_TIMEOUT: u64 = 3000;

/// Maps each assigned [`MessageQueue`] to its push-mode [`ProcessQueue`].
type ProcessQueueTable = Arc<RwLock<HashMap<MessageQueue, Arc<ProcessQueue>>>>;

/// Maps each assigned [`MessageQueue`] to its pop-mode [`PopProcessQueue`].
type PopProcessQueueTable = Arc<RwLock<HashMap<MessageQueue, Arc<PopProcessQueue>>>>;

/// Maps each subscribed topic to the set of [`MessageQueue`]s known for it.
/// Updated by [`MQClientInstance`] when topic route information changes.
type TopicSubscribeInfoTable = Arc<RwLock<HashMap<TopicName, HashSet<MessageQueue>>>>;

/// Subscription metadata for each topic, including filter expressions and
/// subscription version. Shared with [`MQClientInstance`] for heartbeat generation.
/// Uses [`DashMap`] for lock-free concurrent access without async overhead.
type SubscriptionInnerTable = Arc<DashMap<TopicName, SubscriptionData>>;

/// Topics for which the broker returned a valid queue-assignment result,
/// indicating broker-side rebalance is active for those topics.
type TopicBrokerRebalanceMap = Arc<DashMap<TopicName, TopicName>>;

/// Topics for which queue-assignment failed or timed out, falling back to
/// client-side rebalance via the configured [`AllocateMessageQueueStrategy`].
type TopicClientRebalanceMap = Arc<DashMap<TopicName, TopicName>>;

pub(crate) struct RebalanceImpl<R> {
    /// Maps each assigned [`MessageQueue`] to its [`ProcessQueue`], representing
    /// the local consumption state and flow-control counters for push-mode consumers.
    pub(crate) process_queue_table: ProcessQueueTable,

    /// Maps each assigned [`MessageQueue`] to its [`PopProcessQueue`], used when
    /// the consumer operates in pop-mode (broker-side ack instead of pull).
    pub(crate) pop_process_queue_table: PopProcessQueueTable,

    /// Maps each subscribed topic to the full set of [`MessageQueue`]s that exist
    /// for it on the broker. Updated by [`MQClientInstance`] on route changes.
    pub(crate) topic_subscribe_info_table: TopicSubscribeInfoTable,

    /// Subscription metadata for each topic, including filter type and expression.
    /// Shared with [`MQClientInstance`] to build heartbeat payloads sent to brokers.
    pub(crate) subscription_inner: SubscriptionInnerTable,

    /// Name of the consumer group this rebalance instance belongs to.
    pub(crate) consumer_group: Option<ConsumerGroupName>,

    /// Message consumption model: `Clustering` (load-balanced) or `Broadcasting`
    /// (every consumer receives all messages).
    pub(crate) message_model: Option<MessageModel>,

    /// Strategy that determines how [`MessageQueue`]s are divided among the consumers
    /// in the same group (e.g., average, consistent-hash).
    pub(crate) allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,

    /// Handle to the [`MQClientInstance`] shared by all producers and consumers with
    /// the same `clientId`, used for broker communication and topic route queries.
    pub(crate) client_instance: Option<ArcMut<MQClientInstance>>,

    /// Weak reference to the concrete rebalance implementation (e.g., `RebalancePushImpl`),
    /// enabling abstract callbacks without creating reference cycles.
    pub(crate) sub_rebalance_impl: Option<WeakArcMut<R>>,

    /// Topics for which `queryAssignment` returned a valid broker-side assignment.
    /// Lock-free concurrent map; entries are added on successful assignment and
    /// removed when the topic is no longer subscribed.
    pub(crate) topic_broker_rebalance: TopicBrokerRebalanceMap,

    /// Topics for which `queryAssignment` failed or timed out, causing the client
    /// to fall back to the local [`AllocateMessageQueueStrategy`] instead.
    /// Lock-free concurrent map; entries are added on failure and pruned on
    /// unsubscribe.
    pub(crate) topic_client_rebalance: TopicClientRebalanceMap,
}

impl<R> RebalanceImpl<R>
where
    R: Rebalance,
{
    pub fn new(
        consumer_group: Option<CheetahString>,
        message_model: Option<MessageModel>,
        allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,
        mqclient_instance: Option<ArcMut<MQClientInstance>>,
    ) -> Self {
        RebalanceImpl {
            process_queue_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            pop_process_queue_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            topic_subscribe_info_table: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            subscription_inner: Arc::new(DashMap::with_capacity(64)),
            consumer_group,
            message_model,
            allocate_message_queue_strategy,
            client_instance: mqclient_instance,
            sub_rebalance_impl: None,
            topic_broker_rebalance: Arc::new(DashMap::with_capacity(64)),
            topic_client_rebalance: Arc::new(DashMap::with_capacity(64)),
        }
    }

    #[inline]
    pub fn put_subscription_data(&self, topic: &CheetahString, subscription_data: SubscriptionData) {
        self.subscription_inner.insert(topic.clone(), subscription_data);
    }

    #[inline]
    pub fn remove_subscription_data(&self, topic: &CheetahString) {
        self.subscription_inner.remove(topic);
    }

    pub fn get_mq_client_factory(&self) -> Option<ArcMut<MQClientInstance>> {
        self.client_instance.clone()
    }

    pub fn set_mq_client_factory(&mut self, client_instance: ArcMut<MQClientInstance>) {
        self.client_instance = Some(client_instance);
    }

    fn consumer_group_for_log(&self) -> &str {
        self.consumer_group.as_ref().map_or("<unknown>", |group| group.as_ref())
    }

    fn consumer_group_ref(&self, operation: &str) -> Option<&ConsumerGroupName> {
        match self.consumer_group.as_ref() {
            Some(group) => Some(group),
            None => {
                warn!("{operation} skipped: consumer_group is not initialized");
                None
            }
        }
    }

    fn message_model_value(&self, operation: &str) -> Option<MessageModel> {
        match self.message_model {
            Some(message_model) => Some(message_model),
            None => {
                warn!("{operation} skipped: message_model is not initialized");
                None
            }
        }
    }

    fn upgrade_sub_rebalance(&self, operation: &str) -> Option<ArcMut<R>> {
        match self.sub_rebalance_impl.as_ref().and_then(|weak| weak.upgrade()) {
            Some(rebalance) => Some(rebalance),
            None => {
                warn!("{operation} skipped: sub rebalance implementation is not available");
                None
            }
        }
    }

    #[inline]
    pub async fn do_rebalance(&mut self, is_order: bool) -> bool {
        let mut balanced = true;
        let topics = self
            .subscription_inner
            .iter()
            .map(|e| e.key().clone())
            .collect::<Vec<CheetahString>>();
        for topic in &topics {
            if !self.client_rebalance(topic) && self.try_query_assignment(topic).await {
                if !self.get_rebalance_result_from_broker(topic, is_order).await {
                    balanced = false;
                }
            } else if !self.rebalance_by_topic(topic, is_order).await {
                balanced = false;
            }
        }
        self.truncate_message_queue_not_my_topic().await;
        balanced
    }

    #[inline]
    pub fn client_rebalance(&mut self, topic: &str) -> bool {
        match self.sub_rebalance_impl.as_mut() {
            Some(sub_impl) => match sub_impl.upgrade() {
                Some(mut value) => value.client_rebalance(topic),
                None => {
                    warn!(
                        "Sub rebalance implementation has been dropped, defaulting to client rebalance for topic: {}",
                        topic
                    );
                    true
                }
            },
            None => {
                warn!(
                    "Sub rebalance implementation not set, defaulting to client rebalance for topic: {}",
                    topic
                );
                true
            }
        }
    }

    /// Attempts to query the assignment for a given topic.
    ///
    /// This function checks if the topic is already present in the client or broker rebalance
    /// tables. If not, it queries the assignment from the broker using the allocation strategy.
    ///
    /// # Arguments
    ///
    /// * `topic` - A reference to a `CheetahString` representing the topic to query.
    ///
    /// # Returns
    ///
    /// A `bool` indicating whether the assignment query was successful.
    ///
    /// # Errors
    ///
    /// This function logs errors if the allocation strategy is not set or if the query assignment
    /// fails.
    async fn try_query_assignment(&mut self, topic: &CheetahString) -> bool {
        // Check topic_client_rebalance
        if self.topic_client_rebalance.contains_key(topic) {
            return false;
        }

        // Check topic_broker_rebalance
        if self.topic_broker_rebalance.contains_key(topic) {
            return true;
        }

        // Get strategy name
        let strategy_name = if let Some(strategy) = &self.allocate_message_queue_strategy {
            CheetahString::from_static_str(strategy.get_name())
        } else {
            error!(
                "tryQueryAssignment error: allocateMessageQueueStrategy is None for topic: {}",
                topic
            );
            return false;
        };
        let Some(consumer_group) = self.consumer_group_ref("tryQueryAssignment").cloned() else {
            return false;
        };
        let Some(message_model) = self.message_model_value("tryQueryAssignment") else {
            return false;
        };
        let Some(client_instance) = self.client_instance.as_mut() else {
            error!("tryQueryAssignment error: client_instance is None for topic: {}", topic);
            return false;
        };

        // Retry query assignment
        for retry_times in 1..=TIMEOUT_CHECK_TIMES {
            let timeout = QUERY_ASSIGNMENT_TIMEOUT / (TIMEOUT_CHECK_TIMES as u64) * (retry_times as u64);
            match client_instance
                .query_assignment(topic, &consumer_group, &strategy_name, message_model, timeout)
                .await
            {
                Ok(_) => {
                    self.topic_broker_rebalance.insert(topic.clone(), topic.clone());
                    return true;
                }
                Err(e) => match e {
                    rocketmq_error::RocketMQError::Timeout { .. } => {
                        warn!(
                            "tryQueryAssignment timeout for topic: {}, retry: {}/{}",
                            topic, retry_times, TIMEOUT_CHECK_TIMES
                        );
                    }
                    _ => {
                        error!("tryQueryAssignment error for topic: {}, error: {}", topic, e);
                        self.topic_client_rebalance.insert(topic.clone(), topic.clone());
                        return false;
                    }
                },
            }
        }

        // Insert into topic_client_rebalance after all retries
        warn!(
            "tryQueryAssignment exhausted all retries for topic: {}, falling back to client rebalance",
            topic
        );
        self.topic_client_rebalance.insert(topic.clone(), topic.clone());
        false
    }

    async fn truncate_message_queue_not_my_topic(&self) {
        let topics: HashSet<CheetahString> = self.subscription_inner.iter().map(|e| e.key().clone()).collect();

        // Snapshot queues to remove under the read lock; state modification uses the write lock below.
        let to_remove: Vec<MessageQueue> = {
            let process_queue_table = self.process_queue_table.read().await;
            process_queue_table
                .keys()
                .filter_map(|mq| {
                    if !topics.contains(mq.topic_str()) {
                        Some(mq.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Set dropped and remove under write lock for atomicity
        if !to_remove.is_empty() {
            let mut process_queue_table = self.process_queue_table.write().await;
            for mq in &to_remove {
                if let Some(pq) = process_queue_table.get(mq) {
                    pq.set_dropped(true);
                }
                process_queue_table.remove(mq);
                info!(
                    "doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}",
                    self.consumer_group_for_log(),
                    mq.topic_str()
                );
            }
        }

        // Apply the same cleanup to the pop-mode process queue table.
        let pop_to_remove: Vec<MessageQueue> = {
            let pop_process_queue_table = self.pop_process_queue_table.read().await;
            pop_process_queue_table
                .keys()
                .filter_map(|mq| {
                    if !topics.contains(mq.topic_str()) {
                        Some(mq.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        if !pop_to_remove.is_empty() {
            let mut pop_process_queue_table = self.pop_process_queue_table.write().await;
            for mq in &pop_to_remove {
                if let Some(pq) = pop_process_queue_table.get(mq) {
                    pq.set_dropped(true);
                }
                pop_process_queue_table.remove(mq);
                info!(
                    "doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary pop mq, {}",
                    self.consumer_group_for_log(),
                    mq.topic_str()
                );
            }
        }

        self.topic_client_rebalance.retain(|topic, _| topics.contains(topic));
        self.topic_broker_rebalance.retain(|topic, _| topics.contains(topic));
    }

    /// Retrieves the rebalance result from the broker for a given topic.
    ///
    /// This function queries the broker for message queue assignments for the specified topic.
    /// If the assignments are successfully retrieved, it updates the message queue assignments
    /// and notifies the sub-rebalance implementation of any changes.
    ///
    /// # Arguments
    ///
    /// * `topic` - A reference to a `CheetahString` representing the topic to query.
    /// * `is_order` - A boolean indicating whether the message queues should be ordered.
    ///
    /// # Returns
    ///
    /// A `bool` indicating whether the rebalance result matches the current working message queue.
    ///
    /// # Errors
    ///
    /// This function logs errors if the allocation strategy is not set or if the query assignment
    /// fails.
    async fn get_rebalance_result_from_broker(&mut self, topic: &CheetahString, is_order: bool) -> bool {
        let strategy_name = match self.allocate_message_queue_strategy.as_ref() {
            None => {
                error!("get_rebalance_result_from_broker error: allocate_message_queue_strategy is None.");
                return false;
            }
            Some(strategy) => strategy.get_name(),
        };
        if self.client_instance.is_none() {
            error!("get_rebalance_result_from_broker error: client_instance is None.");
            return false;
        }
        let Some(consumer_group) = self.consumer_group_ref("getRebalanceResultFromBroker").cloned() else {
            return false;
        };
        let Some(message_model) = self.message_model_value("getRebalanceResultFromBroker") else {
            return false;
        };
        let Some(client_instance) = self.client_instance.as_mut() else {
            return false;
        };
        let message_queue_assignments = client_instance
            .query_assignment(
                topic,
                &consumer_group,
                &CheetahString::from_slice(strategy_name),
                message_model,
                QUERY_ASSIGNMENT_TIMEOUT,
            )
            .await;
        let (mq_set, message_queue_assignments) = match message_queue_assignments {
            Ok(assignments) => {
                let Some(assignments_inner) = assignments else {
                    return false;
                };
                let mut mq_set = HashSet::new();
                for assignment in &assignments_inner {
                    if let Some(ref mq) = assignment.message_queue {
                        mq_set.insert(mq.clone());
                    }
                }
                (mq_set, assignments_inner)
            }
            Err(e) => {
                error!(
                    "allocate message queue exception. strategy name: {}, {}.",
                    strategy_name, e
                );
                return false;
            }
        };

        let changed = self
            .update_message_queue_assignment(topic, &message_queue_assignments, is_order)
            .await;
        if changed {
            let Some(mut sub_rebalance_impl) = self.upgrade_sub_rebalance("getRebalanceResultFromBroker") else {
                return false;
            };
            let mq_all = {
                let table = self.topic_subscribe_info_table.read().await;
                table.get(topic.as_str()).cloned().unwrap_or_default()
            };
            sub_rebalance_impl.message_queue_changed(topic, &mq_all, &mq_set).await;
        }
        let set = self.get_working_message_queue(topic).await;
        mq_set.eq(&set)
    }

    /// Updates the message queue assignments for a given topic.
    ///
    /// This function processes the provided message queue assignments, categorizing them into push
    /// and pop assignments. It handles the subscription and unsubscription of retry topics
    /// based on the assignments. It also removes unnecessary message queues and adds new
    /// message queues as needed.
    ///
    /// # Arguments
    ///
    /// * `topic` - A reference to a `CheetahString` representing the topic to update.
    /// * `assignments` - A reference to a `HashSet` of `MessageQueueAssignment` representing the
    ///   new assignments.
    /// * `is_order` - A boolean indicating whether the message queues should be ordered.
    ///
    /// # Returns
    ///
    /// A `bool` indicating whether the message queue assignments were changed.
    async fn update_message_queue_assignment(
        &mut self,
        topic: &CheetahString,
        assignments: &HashSet<MessageQueueAssignment>,
        is_order: bool,
    ) -> bool {
        let Some(consumer_group) = self.consumer_group_ref("updateMessageQueueAssignment").cloned() else {
            return false;
        };
        let mut changed = false;
        let mut mq2push_assignment = HashMap::new();
        let mut mq2pop_assignment = HashMap::new();
        for assignment in assignments {
            if let Some(ref mq) = assignment.message_queue {
                if MessageRequestMode::Pop == assignment.mode {
                    mq2pop_assignment.insert(mq, assignment);
                } else {
                    mq2push_assignment.insert(mq, assignment);
                }
            }
        }

        if !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            if mq2pop_assignment.is_empty() && !mq2push_assignment.is_empty() {
                // Assignment switched from pop-mode to push-mode; subscribe to the pop retry topic.
                let retry_topic =
                    CheetahString::from(KeyBuilder::build_pop_retry_topic(topic, consumer_group.as_ref(), false));
                let subscription_data = FilterAPI::build_subscription_data(
                    &retry_topic,
                    &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                );
                if let Ok(subscription_data) = subscription_data {
                    self.put_subscription_data(&retry_topic, subscription_data);
                }
            } else if !mq2pop_assignment.is_empty() && mq2push_assignment.is_empty() {
                // Assignment switched from push-mode to pop-mode; unsubscribe from the pop retry topic.
                let retry_topic =
                    CheetahString::from(KeyBuilder::build_pop_retry_topic(topic, consumer_group.as_ref(), false));
                self.remove_subscription_data(&retry_topic);
            }
        }
        let mut remove_queue_map = HashMap::with_capacity(64);
        {
            // Mark and collect push-mode queues no longer assigned to this consumer.
            let process_queue_table = self.process_queue_table.read().await;
            for (mq, pq) in process_queue_table.iter() {
                if mq.topic_str() == topic {
                    if !mq2push_assignment.contains_key(mq) {
                        pq.set_dropped(true);
                        remove_queue_map.insert(mq.clone(), pq.clone());
                    } else if pq.is_pull_expired() {
                        if let Some(sub_rebalance) = self.upgrade_sub_rebalance("updateMessageQueueAssignment") {
                            if sub_rebalance.consume_type() == ConsumeType::ConsumePassively {
                                pq.set_dropped(true);
                                remove_queue_map.insert(mq.clone(), pq.clone());
                                error!(
                                    "[BUG]doRebalance, {:?}, try remove unnecessary mq, {}, because pull is pause, so \
                                     try to fixed it",
                                    self.consumer_group,
                                    mq.topic_str()
                                );
                            }
                        }
                    }
                }
            }
        }

        {
            if !remove_queue_map.is_empty() {
                let mut process_queue_table = self.process_queue_table.write().await;
                // Remove unassigned push queues from the process queue table.
                for (mq, pq) in remove_queue_map {
                    if let Some(mut sub_rebalance) = self.upgrade_sub_rebalance("updateMessageQueueAssignment") {
                        if sub_rebalance.remove_unnecessary_message_queue(&mq, &pq).await {
                            process_queue_table.remove(&mq);
                            changed = true;
                            info!(
                                "doRebalance, {:?}, remove unnecessary mq, {}",
                                self.consumer_group,
                                mq.topic_str()
                            );
                        }
                    }
                }
            }
        }

        let mut remove_queue_map = HashMap::with_capacity(64);
        {
            // Drop process queues no longer belong to me
            let pop_process_queue_table = self.pop_process_queue_table.read().await;
            for (mq, pq) in pop_process_queue_table.iter() {
                if mq.topic_str() == topic {
                    if !mq2pop_assignment.contains_key(mq) {
                        pq.set_dropped(true);
                        remove_queue_map.insert(mq.clone(), pq.clone());
                    } else if pq.is_pull_expired() {
                        if let Some(sub_rebalance) = self.upgrade_sub_rebalance("updateMessageQueueAssignment") {
                            if sub_rebalance.consume_type() == ConsumeType::ConsumePassively {
                                pq.set_dropped(true);
                                remove_queue_map.insert(mq.clone(), pq.clone());
                                error!(
                                    "[BUG]doRebalance, {:?}, try remove unnecessary mq, {}, because pull is pause, so \
                                     try to fixed it",
                                    self.consumer_group,
                                    mq.topic_str()
                                );
                            }
                        }
                    }
                }
            }
        }

        {
            if !remove_queue_map.is_empty() {
                let mut pop_process_queue_table = self.pop_process_queue_table.write().await;
                // Remove unassigned pop queues from the pop process queue table.
                for (mq, pq) in remove_queue_map {
                    if let Some(mut sub_rebalance) = self.upgrade_sub_rebalance("updateMessageQueueAssignment") {
                        if sub_rebalance.remove_unnecessary_pop_message_queue(&mq, &pq) {
                            pop_process_queue_table.remove(&mq);
                            changed = true;
                            info!(
                                "doRebalance, {:?}, remove unnecessary pop mq, {}",
                                self.consumer_group,
                                mq.topic_str()
                            );
                        }
                    }
                }
            }
        }

        {
            // Add newly assigned message queues to the push-mode process queue table.
            let mut all_mq_locked = true;
            let mut pull_request_list = Vec::new();
            let Some(mut sub_rebalance_impl) = self.upgrade_sub_rebalance("updateMessageQueueAssignment") else {
                return false;
            };

            // Identify queues that require a broker-side lock before being added.
            let queues_need_lock: Vec<_> = if is_order {
                let process_queue_table = self.process_queue_table.read().await;
                mq2push_assignment
                    .keys()
                    .filter(|mq| !process_queue_table.contains_key(*mq))
                    .cloned()
                    .cloned()
                    .collect()
            } else {
                vec![]
            };

            // Acquire broker-side locks for orderly queues before taking the write lock,
            // preventing network I/O while the write lock is held.
            let mut successfully_locked = HashSet::new();
            if is_order && !queues_need_lock.is_empty() {
                // Snapshot the process queue table under the read lock; the lock is
                // released before broker communication occurs.
                let process_queue_table = {
                    let table = self.process_queue_table.read().await;
                    table.clone()
                };

                for mq in &queues_need_lock {
                    if self.lock_with(mq, &process_queue_table).await {
                        successfully_locked.insert(mq.clone());
                    } else {
                        warn!(
                            "doRebalance, {:?}, lock mq failed before adding, {}",
                            self.consumer_group,
                            mq.topic_str()
                        );
                        all_mq_locked = false;
                    }
                }
            }

            // Insert new queues under the write lock; no network calls are made while the lock is held.
            let process_queue_table_clone = self.process_queue_table.clone();
            let mut process_queue_table = process_queue_table_clone.write().await;
            for (mq, assignment) in mq2push_assignment {
                if !process_queue_table.contains_key(mq) {
                    // Check if lock succeeded (for ordered consumption)
                    if is_order && !successfully_locked.contains(mq) {
                        warn!(
                            "doRebalance, {:?}, skip adding mq because lock failed, {}",
                            self.consumer_group,
                            mq.topic_str()
                        );
                        continue;
                    }

                    sub_rebalance_impl.remove_dirty_offset(mq).await;
                    let pq = Arc::new(sub_rebalance_impl.create_process_queue());
                    pq.set_locked(true);
                    let Ok(next_offset) = sub_rebalance_impl.compute_pull_from_where_with_exception(mq).await else {
                        continue;
                    };
                    if next_offset >= 0 {
                        if process_queue_table.insert(mq.clone(), pq.clone()).is_none() {
                            info!(
                                "doRebalance, {:?}, add a new mq, {}",
                                self.consumer_group,
                                mq.topic_str()
                            );
                            pull_request_list.push(PullRequest::new(
                                consumer_group.clone(),
                                mq.clone(),
                                pq,
                                next_offset,
                            ));
                            changed = true;
                        } else {
                            info!(
                                "doRebalance, {:?}, mq already exists, {}",
                                self.consumer_group,
                                mq.topic_str()
                            );
                        }
                    } else {
                        warn!(
                            "doRebalance, {:?}, add new mq failed, {}",
                            self.consumer_group,
                            mq.topic_str()
                        );
                    }
                }
            }

            if !all_mq_locked {
                if let Some(client_instance) = self.client_instance.as_mut() {
                    client_instance.rebalance_later(500);
                } else {
                    warn!("rebalance_later skipped: client_instance is not initialized");
                }
            }
            sub_rebalance_impl.dispatch_pull_request(pull_request_list, 500).await;
        }

        {
            if let Some(rebalance_impl) = self.upgrade_sub_rebalance("updateMessageQueueAssignment") {
                let mut pop_request_list = Vec::new();
                let mut pop_process_queue_table = self.pop_process_queue_table.write().await;
                for (mq, assignment) in mq2pop_assignment {
                    if !pop_process_queue_table.contains_key(mq) {
                        let pq = rebalance_impl.create_pop_process_queue();
                        let pre = pop_process_queue_table.insert(mq.clone(), Arc::new(pq.clone()));
                        if pre.is_some() {
                            info!("doRebalance, {:?}, mq pop already exists, {}", self.consumer_group, mq);
                        } else {
                            info!("doRebalance, {:?}, add a new pop mq, {}", self.consumer_group, mq);
                            let request = PopRequest::new(
                                topic.clone(),
                                consumer_group.clone(),
                                mq.clone(),
                                pq,
                                rebalance_impl.get_consume_init_mode(),
                            );
                            pop_request_list.push(request);
                            changed = true;
                        }
                    }
                }
                rebalance_impl.dispatch_pop_pull_request(pop_request_list, 500).await;
            }
        }

        changed
    }

    async fn update_process_queue_table_in_rebalance(
        &mut self,
        topic: &str,
        mq_set: &HashSet<MessageQueue>,
        is_order: bool,
    ) -> bool {
        let Some(consumer_group) = self.consumer_group_ref("updateProcessQueueTableInRebalance").cloned() else {
            return false;
        };
        let mut changed = false;
        let mut remove_queue_map = HashMap::new();
        let process_queue_table_cloned = self.process_queue_table.clone();
        {
            let process_queue_table = process_queue_table_cloned.read().await;
            // Mark and collect queues no longer assigned to this consumer.
            for (mq, pq) in process_queue_table.iter() {
                if mq.topic_str() == topic {
                    if !mq_set.contains(mq) {
                        pq.set_dropped(true);
                        remove_queue_map.insert(mq.clone(), pq.clone());
                    } else if pq.is_pull_expired() {
                        if let Some(sub_rebalance) = self.upgrade_sub_rebalance("updateProcessQueueTableInRebalance") {
                            if sub_rebalance.consume_type() == ConsumeType::ConsumePassively {
                                pq.set_dropped(true);
                                remove_queue_map.insert(mq.clone(), pq.clone());
                                error!(
                                    "[BUG]doRebalance, {:?}, try remove unnecessary mq, {}, because pull is pause, so \
                                     try to fixed it",
                                    self.consumer_group,
                                    mq.topic_str()
                                );
                            }
                        }
                    }
                }
            }
        }

        {
            if !remove_queue_map.is_empty() {
                let mut process_queue_table = process_queue_table_cloned.write().await;
                // Remove unassigned queues from the process queue table.
                for (mq, pq) in remove_queue_map {
                    if let Some(mut sub_rebalance) = self.upgrade_sub_rebalance("updateProcessQueueTableInRebalance") {
                        if sub_rebalance.remove_unnecessary_message_queue(&mq, &pq).await {
                            process_queue_table.remove(&mq);
                            changed = true;
                            info!(
                                "doRebalance, {:?}, remove unnecessary mq, {}",
                                self.consumer_group,
                                mq.topic_str()
                            );
                        }
                    }
                }
            }
        }
        // Add newly assigned message queues to the process queue table.
        let mut all_mq_locked = true;
        let mut pull_request_list = Vec::new();
        let Some(mut sub_rebalance_impl) = self.upgrade_sub_rebalance("updateProcessQueueTableInRebalance") else {
            return false;
        };

        // Identify queues that require a broker-side lock before being added.
        let queues_need_lock: Vec<_> = if is_order {
            let process_queue_table = process_queue_table_cloned.read().await;
            mq_set
                .iter()
                .filter(|mq| !process_queue_table.contains_key(*mq))
                .cloned()
                .collect()
        } else {
            vec![]
        };

        // Acquire broker-side locks for orderly queues before taking the write lock,
        // preventing network I/O while the write lock is held.
        let mut successfully_locked = HashSet::new();
        if is_order && !queues_need_lock.is_empty() {
            let process_queue_table = process_queue_table_cloned.read().await;
            for mq in &queues_need_lock {
                if self.lock_with(mq, &process_queue_table).await {
                    successfully_locked.insert(mq.clone());
                } else {
                    warn!(
                        "doRebalance, {:?}, lock mq failed before adding, {}",
                        self.consumer_group,
                        mq.topic_str()
                    );
                    all_mq_locked = false;
                }
            }
        }

        // Insert new queues under the write lock; no network calls are made while the lock is held.
        let mut process_queue_table = process_queue_table_cloned.write().await;
        for mq in mq_set {
            let should_insert = match process_queue_table.get(mq) {
                Some(pq) if pq.is_dropped() => {
                    process_queue_table.remove(mq);
                    true
                }
                Some(_) => false,
                None => true,
            };
            if should_insert {
                // Check if lock succeeded (for ordered consumption)
                if is_order && !successfully_locked.contains(mq) {
                    warn!(
                        "doRebalance, {:?}, skip adding mq because lock failed, {}",
                        self.consumer_group,
                        mq.topic_str()
                    );
                    continue;
                }

                sub_rebalance_impl.remove_dirty_offset(mq).await;
                let pq = Arc::new(sub_rebalance_impl.create_process_queue());
                pq.set_locked(true);
                let next_offset = match sub_rebalance_impl.compute_pull_from_where_with_exception(mq).await {
                    Ok(next_offset) => next_offset,
                    Err(error) => {
                        info!(
                            "doRebalance, {:?}, compute offset failed, {}, error={}",
                            self.consumer_group,
                            mq.topic_str(),
                            error
                        );
                        continue;
                    }
                };
                if next_offset >= 0 {
                    if process_queue_table.insert(mq.clone(), pq.clone()).is_none() {
                        info!(
                            "doRebalance, {:?}, add a new mq, {}",
                            self.consumer_group,
                            mq.topic_str()
                        );
                        pull_request_list.push(PullRequest::new(consumer_group.clone(), mq.clone(), pq, next_offset));
                        changed = true;
                    } else {
                        info!(
                            "doRebalance, {:?}, mq already exists, {}",
                            self.consumer_group,
                            mq.topic_str()
                        );
                    }
                } else {
                    warn!(
                        "doRebalance, {:?}, add new mq failed, {}",
                        self.consumer_group,
                        mq.topic_str()
                    );
                }
            }
        }

        if !all_mq_locked {
            if let Some(client_instance) = self.client_instance.as_mut() {
                client_instance.rebalance_later(500);
            } else {
                warn!("rebalance_later skipped: client_instance is not initialized");
            }
        }
        sub_rebalance_impl.dispatch_pull_request(pull_request_list, 500).await;

        changed
    }

    async fn rebalance_by_topic(&mut self, topic: &CheetahString, is_order: bool) -> bool {
        let Some(message_model) = self.message_model_value("rebalanceByTopic") else {
            return false;
        };
        match message_model {
            MessageModel::Broadcasting => {
                // Snapshot the queue set under the read lock; the lock is released before any
                // async operations to prevent holding it across await points.
                let mq_set_opt: Option<HashSet<MessageQueue>> = {
                    let table = self.topic_subscribe_info_table.read().await;
                    table.get(topic).cloned()
                };
                if let Some(mq_set) = mq_set_opt {
                    let changed = self
                        .update_process_queue_table_in_rebalance(topic, &mq_set, is_order)
                        .await;
                    if changed {
                        if let Some(mut sub_rebalance_impl) = self.upgrade_sub_rebalance("rebalanceByTopic") {
                            sub_rebalance_impl.message_queue_changed(topic, &mq_set, &mq_set).await;
                        }
                    }
                    mq_set.eq(&self.get_working_message_queue(topic).await)
                } else {
                    if let Some(mut sub_rebalance_impl) = self.upgrade_sub_rebalance("rebalanceByTopic") {
                        sub_rebalance_impl
                            .message_queue_changed(topic, &HashSet::new(), &HashSet::new())
                            .await;
                        warn!(
                            "doRebalance, {}, but the topic[{}] not exist.",
                            self.consumer_group_for_log(),
                            topic
                        );
                    }
                    true
                }
            }
            MessageModel::Clustering => {
                let Some(consumer_group) = self.consumer_group_ref("rebalanceByTopic").cloned() else {
                    return false;
                };
                let cid_all = if let Some(client_instance) = self.client_instance.as_mut() {
                    client_instance.find_consumer_id_list(topic, &consumer_group).await
                } else {
                    warn!(
                        "doRebalance, {}, {}, client_instance is not initialized.",
                        consumer_group, topic
                    );
                    return true;
                };
                let topic_sub_cloned = self.topic_subscribe_info_table.clone();
                let topic_subscribe_info_table_inner = topic_sub_cloned.read().await;
                let mq_set = topic_subscribe_info_table_inner.get(topic);
                if mq_set.is_none() && !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                    if let Some(mut sub_rebalance_impl) = self.upgrade_sub_rebalance("rebalanceByTopic") {
                        sub_rebalance_impl
                            .message_queue_changed(topic, &HashSet::new(), &HashSet::new())
                            .await;
                        warn!("doRebalance, {}, but the topic[{}] not exist.", consumer_group, topic);
                    }
                }
                if cid_all.is_none() {
                    warn!(
                        "doRebalance, {}, {}, get consumer id list failed.",
                        consumer_group, topic
                    );
                    return true;
                }
                if let (Some(mq_set), Some(mut ci_all)) = (mq_set, cid_all) {
                    let mut mq_all = mq_set.iter().cloned().collect::<Vec<MessageQueue>>();
                    mq_all.sort();
                    ci_all.sort();

                    let Some(strategy) = self.allocate_message_queue_strategy.as_ref() else {
                        warn!(
                            "doRebalance, {}, {}, allocate_message_queue_strategy is not initialized.",
                            consumer_group, topic
                        );
                        return false;
                    };
                    let strategy_name = strategy.get_name();
                    let Some(client_id) = self.client_instance.as_ref().map(|client| client.client_id.clone()) else {
                        warn!(
                            "doRebalance, {}, {}, client_instance is not initialized.",
                            consumer_group, topic
                        );
                        return true;
                    };
                    let allocate_result = match strategy.allocate(
                        &consumer_group,
                        client_id.as_ref(),
                        mq_all.as_slice(),
                        ci_all.as_slice(),
                    ) {
                        Ok(value) => value,
                        Err(e) => {
                            error!(
                                "allocate message queue exception. strategy name: {}, ex: {}",
                                strategy_name,
                                e.to_string()
                            );
                            return false;
                        }
                    };
                    let allocate_result_set = allocate_result.into_iter().collect::<HashSet<MessageQueue>>();
                    let changed = self
                        .update_process_queue_table_in_rebalance(topic, &allocate_result_set, is_order)
                        .await;
                    let working = self.get_working_message_queue(topic).await;
                    if changed {
                        info!(
                            "client rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, \
                             topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, \
                             rebalanceResultSet={:?}",
                            strategy_name,
                            consumer_group,
                            topic,
                            client_id,
                            mq_set.len(),
                            ci_all.len(),
                            allocate_result_set.len(),
                            allocate_result_set
                        );

                        if let Some(mut sub_rebalance_impl) = self.upgrade_sub_rebalance("rebalanceByTopic") {
                            sub_rebalance_impl
                                .message_queue_changed(topic, mq_set, &allocate_result_set)
                                .await;
                        }
                    }
                    return allocate_result_set.eq(&working);
                }
                true
            }
        }
    }

    pub async fn get_working_message_queue(&self, topic: &str) -> HashSet<MessageQueue> {
        let mut queue_set = HashSet::new();
        let process_queue_table = self.process_queue_table.read().await;
        for (mq, pq) in process_queue_table.iter() {
            if mq.topic_str() == topic && !pq.is_dropped() {
                queue_set.insert(mq.clone());
            }
        }
        let pop_process_queue_table = self.pop_process_queue_table.read().await;
        for (mq, pq) in pop_process_queue_table.iter() {
            if mq.topic_str() == topic && !pq.is_dropped() {
                queue_set.insert(mq.clone());
            }
        }
        queue_set
    }

    pub async fn lock(&self, mq: &MessageQueue) -> bool {
        let process_queue_table_ = self.process_queue_table.clone();
        let process_queue_table = process_queue_table_.read().await;
        let table = process_queue_table.deref();
        self.lock_with(mq, table).await
    }

    pub async fn lock_with(
        &self,
        mq: &MessageQueue,
        process_queue_table: &HashMap<MessageQueue, Arc<ProcessQueue>>,
    ) -> bool {
        let Some(consumer_group) = self.consumer_group_ref("lockWith").cloned() else {
            return false;
        };
        let Some(client) = self.client_instance.as_ref() else {
            warn!("lockWith skipped: client_instance is not initialized, mq={}", mq);
            return false;
        };
        let broker_name = client.get_broker_name_from_message_queue(mq).await;
        let find_broker_result = client
            .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
            .await;
        if let Some(find_broker_result) = find_broker_result {
            let mut request_body = LockBatchRequestBody {
                consumer_group: Some(consumer_group.clone()),
                client_id: Some(client.client_id.clone()),
                ..Default::default()
            };
            request_body.mq_set.insert(mq.clone());
            let Some(mq_client_api_impl) = client.mq_client_api_impl.as_ref() else {
                warn!("lockWith skipped: MQClientAPIImpl is not initialized, mq={}", mq);
                return false;
            };
            let result = mq_client_api_impl
                .lock_batch_mq(find_broker_result.broker_addr.as_str(), request_body, 1_000)
                .await;
            match result {
                Ok(locked_mq) => {
                    for mq in &locked_mq {
                        if let Some(pq) = process_queue_table.get(mq) {
                            pq.set_locked(true);
                            pq.set_last_lock_timestamp(current_millis());
                        }
                    }
                    let lock_ok = locked_mq.contains(mq);
                    info!("message queue lock {}, {:?} {}", lock_ok, consumer_group, mq);
                    lock_ok
                }
                Err(e) => {
                    error!("lockBatchMQ exception {},{}", mq, e);
                    false
                }
            }
        } else {
            false
        }
    }

    pub async fn lock_all(&self) {
        let broker_mqs = self.build_process_queue_table_by_broker_name().await;
        let Some(consumer_group) = self.consumer_group.clone() else {
            warn!("lockAll skipped: consumer_group is not initialized");
            return;
        };
        let Some(client_instance) = self.client_instance.clone() else {
            warn!("lockAll skipped: client_instance is not initialized");
            return;
        };

        let map = broker_mqs
            .into_iter()
            .map(|(broker_name, mqs)| {
                let client_instance = client_instance.clone();
                let process_queue_table = self.process_queue_table.clone();
                let consumer_group = consumer_group.clone();
                async move {
                    if mqs.is_empty() {
                        return;
                    }
                    let find_broker_result = client_instance
                        .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
                        .await;
                    if let Some(find_broker_result) = find_broker_result {
                        let request_body = LockBatchRequestBody {
                            consumer_group: Some(consumer_group.to_owned()),
                            client_id: Some(client_instance.client_id.clone()),
                            mq_set: mqs.clone(),
                            ..Default::default()
                        };
                        let Some(mq_client_api_impl) = client_instance.mq_client_api_impl.as_ref() else {
                            warn!(
                                "lockAll skipped broker {}: MQClientAPIImpl is not initialized",
                                broker_name
                            );
                            return;
                        };
                        let result = mq_client_api_impl
                            .lock_batch_mq(find_broker_result.broker_addr.as_str(), request_body, 1_000)
                            .await;
                        match result {
                            Ok(lock_okmqset) => {
                                let process_queue_table = process_queue_table.read().await;
                                for mq in &mqs {
                                    if let Some(pq) = process_queue_table.get(mq) {
                                        if lock_okmqset.contains(mq) {
                                            if pq.is_locked() {
                                                info!(
                                                    "the message queue locked OK, Group: {:?} {}",
                                                    consumer_group, mq
                                                );
                                            }
                                            pq.set_locked(true);
                                            pq.set_last_lock_timestamp(current_millis());
                                        } else {
                                            pq.set_locked(false);
                                            warn!(
                                                "the message queue locked Failed, Group: {:?} {}",
                                                consumer_group, mq
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("lockBatchMQ exception {}", e);
                            }
                        }
                    }
                }
            })
            .collect::<Vec<_>>();
        futures::future::join_all(map).await;
    }

    pub async fn unlock_all(&self, oneway: bool) {
        let broker_mqs = self.build_process_queue_table_by_broker_name().await;
        let Some(consumer_group) = self.consumer_group.clone() else {
            warn!("unlockAll skipped: consumer_group is not initialized");
            return;
        };
        for (broker_name, mqs) in broker_mqs {
            if mqs.is_empty() {
                continue;
            }
            let Some(client) = self.client_instance.as_ref() else {
                warn!("unlockAll skipped: client_instance is not initialized");
                return;
            };
            let find_broker_result = client
                .find_broker_address_in_subscribe(&broker_name, mix_all::MASTER_ID, true)
                .await;
            if let Some(find_broker_result) = find_broker_result {
                let request_body = UnlockBatchRequestBody {
                    consumer_group: Some(consumer_group.clone()),
                    client_id: Some(client.client_id.clone()),
                    mq_set: mqs.clone(),
                    ..Default::default()
                };
                let Some(mq_client_api_impl) = client.mq_client_api_impl.as_ref() else {
                    warn!(
                        "unlockAll skipped broker {}: MQClientAPIImpl is not initialized",
                        broker_name
                    );
                    continue;
                };
                let result = mq_client_api_impl
                    .unlock_batch_mq(&find_broker_result.broker_addr, request_body, 1_000, oneway)
                    .await;
                match result {
                    Ok(_) => {
                        let process_queue_table = self.process_queue_table.read().await;
                        for mq in &mqs {
                            if let Some(pq) = process_queue_table.get(mq) {
                                pq.set_locked(false);
                                info!("the message queue unlock OK, Group: {:?} {}", consumer_group, mq);
                            }
                        }
                    }
                    Err(e) => {
                        error!("unlockBatchMQ exception {}", e);
                    }
                }
            }
        }
    }

    async fn build_process_queue_table_by_broker_name(
        &self,
    ) -> HashMap<CheetahString /* brokerName */, HashSet<MessageQueue>> {
        // Collect a snapshot under the read lock (no async calls while holding the lock).
        let snapshot: Vec<MessageQueue> = {
            let process_queue_table = self.process_queue_table.read().await;
            process_queue_table
                .iter()
                .filter(|(_, pq)| !pq.is_dropped())
                .map(|(mq, _)| mq.clone())
                .collect()
        };
        // Resolve broker names outside the lock to avoid holding it during async I/O.
        let Some(client) = self.client_instance.as_ref() else {
            warn!("buildProcessQueueTableByBrokerName skipped: client_instance is not initialized");
            return HashMap::new();
        };
        let mut result = HashMap::new();
        for mq in snapshot {
            let broker_name = client.get_broker_name_from_message_queue(&mq).await;
            let entry = result.entry(broker_name).or_insert_with(HashSet::new);
            entry.insert(mq);
        }
        result
    }
}
