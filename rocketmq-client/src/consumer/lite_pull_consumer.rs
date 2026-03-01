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

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::topic_message_queue_change_listener::TopicMessageQueueChangeListener;

/// A consumer that pulls messages from brokers on demand, providing explicit control over
/// fetch timing, offset management, and queue assignment.
///
/// Unlike the push consumer, the caller drives message retrieval through [`poll`] or
/// [`poll_with_timeout`], and may manage offsets manually when auto-commit is disabled.
/// Queue assignment can be controlled either by subscribing to topics (broker-side rebalance)
/// or by calling [`assign`] directly (client-controlled assignment).
///
/// All methods are asynchronous and do not block the calling thread.
///
/// [`poll`]: LitePullConsumerLocal::poll
/// [`poll_with_timeout`]: LitePullConsumerLocal::poll_with_timeout
/// [`assign`]: LitePullConsumerLocal::assign
#[trait_variant::make(LitePullConsumer: Send)]
pub trait LitePullConsumerLocal: Sync {
    /// Starts the consumer and establishes connections to the broker and name server.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Errors
    ///
    /// Returns an error if the consumer is already running, if required configuration is
    /// invalid, or if the connection to the name server cannot be established.
    async fn start(&self) -> rocketmq_error::RocketMQResult<()>;

    /// Shuts down the consumer and releases all associated resources.
    ///
    /// This function does not block the calling thread. After shutdown, the consumer
    /// cannot be restarted.
    async fn shutdown(&self);

    /// Returns whether the consumer is currently in the running state.
    ///
    /// This function does not block the calling thread.
    async fn is_running(&self) -> bool;

    /// Subscribes to the specified topic using the default subscription expression.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic name is invalid or if the subscription cannot be
    /// registered with the broker.
    async fn subscribe(&self, topic: &str) -> rocketmq_error::RocketMQResult<()>;

    /// Subscribes to the specified topic with a tag-based or SQL-based filter expression.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    /// * `sub_expression` - A tag expression (e.g. `"TagA || TagB"`) or SQL-92 predicate. Pass
    ///   `"*"` to receive all messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic name is invalid, the expression cannot be parsed,
    /// or the subscription cannot be registered with the broker.
    async fn subscribe_with_expression(&self, topic: &str, sub_expression: &str) -> rocketmq_error::RocketMQResult<()>;

    /// Subscribes to the specified topic with a filter expression and a queue-change listener.
    ///
    /// The listener is invoked whenever the set of assigned [`MessageQueue`]s changes for
    /// this topic due to rebalance.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    /// * `sub_expression` - A tag expression or SQL-92 predicate. Pass `"*"` for all messages.
    /// * `listener` - A [`MessageQueueListener`] notified on queue assignment changes.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic name is invalid, the expression cannot be parsed,
    /// or the subscription cannot be registered with the broker.
    async fn subscribe_with_listener<MQL>(
        &self,
        topic: &str,
        sub_expression: &str,
        listener: MQL,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        MQL: MessageQueueListener + 'static;

    /// Subscribes to the specified topic using a [`MessageSelector`] for server-side filtering.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    /// * `selector` - The filter selector. Pass `None` to receive all messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic name is invalid, the selector expression is rejected
    /// by the broker, or the subscription cannot be registered.
    async fn subscribe_with_selector(
        &self,
        topic: &str,
        selector: Option<MessageSelector>,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Removes the subscription for the specified topic.
    ///
    /// Messages for this topic will no longer be fetched after the next rebalance cycle.
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to unsubscribe from.
    async fn unsubscribe(&self, topic: &str);

    /// Returns the set of [`MessageQueue`]s currently assigned to this consumer.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Errors
    ///
    /// Returns an error if the consumer is not in the running state.
    async fn assignment(&self) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>>;

    /// Manually assigns the given [`MessageQueue`]s to this consumer, bypassing broker rebalance.
    ///
    /// Any previously assigned queues not present in `message_queues` are removed.
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queues` - The complete set of queues to assign to this consumer.
    async fn assign(&self, message_queues: Vec<MessageQueue>);

    /// Sets the subscription filter expression applied when fetching from manually assigned queues.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic for which the filter expression applies.
    /// * `sub_expression` - A tag expression or SQL-92 predicate used to filter messages.
    async fn set_sub_expression_for_assign(&self, topic: &str, sub_expression: &str);

    /// Populates `sub_expression_map` with the filter selector for each subscribed topic,
    /// providing the subscription metadata required for heartbeat payloads.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `sub_expression_map` - Output map from topic name to its [`MessageSelector`]. Entries are
    ///   inserted for every topic that has an active subscription with a selector.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription metadata cannot be retrieved.
    async fn build_subscriptions_for_heartbeat(
        &self,
        sub_expression_map: &mut HashMap<String, MessageSelector>,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Fetches the next batch of messages without allocating owned copies.
    ///
    /// Returns `ArcMut<MessageExt>` references to messages, providing shared mutable access
    /// without heap allocation or deep cloning. The returned references remain valid until
    /// they are dropped. Messages that need to outlive the poll scope must be cloned explicitly.
    ///
    /// This method uses the default poll timeout configured for the consumer.
    ///
    /// # Performance
    ///
    /// Message contents are not copied. For workloads processing messages without long-term
    /// storage, this eliminates allocation overhead compared to [`poll()`].
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let messages = consumer.poll_zero_copy().await;
    /// for msg in &messages {
    ///     process_message(msg);
    /// }
    ///
    /// // Clone only filtered messages
    /// let messages = consumer.poll_zero_copy().await;
    /// let important: Vec<MessageExt> = messages.into_iter()
    ///     .filter(|msg| is_important(msg))
    ///     .map(|msg| (*msg).clone())
    ///     .collect();
    /// ```
    ///
    /// Returns an empty vector if no messages are available within the default timeout period.
    /// This function does not block the calling thread.
    async fn poll_zero_copy(&self) -> Vec<rocketmq_rust::ArcMut<MessageExt>>;

    /// Fetches the next batch of messages without allocating owned copies, with a specified timeout.
    ///
    /// Behaves identically to [`poll_zero_copy()`], but waits up to `timeout` milliseconds
    /// for messages to become available.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for messages, in milliseconds.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let messages = consumer.poll_with_timeout_zero_copy(1000).await;
    /// ```
    ///
    /// Returns an empty vector if no messages are available before the timeout expires.
    /// This function does not block the calling thread.
    async fn poll_with_timeout_zero_copy(&self, timeout: u64) -> Vec<rocketmq_rust::ArcMut<MessageExt>>;

    /// Fetches the next batch of messages, returning owned copies.
    ///
    /// Each returned message is cloned from the internal message store. The caller
    /// owns the returned messages and may store them beyond the poll scope.
    ///
    /// This method uses the default poll timeout configured for the consumer.
    ///
    /// # Performance
    ///
    /// All messages are deep-cloned, including message body and properties. For a 2KB message,
    /// each poll returning 32 messages allocates approximately 90KB. At 100 polls per second,
    /// this results in approximately 9MB/s of allocations.
    ///
    /// For workloads that do not require owned messages, [`poll_zero_copy()`] avoids
    /// this allocation overhead.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let messages = consumer.poll().await;
    /// my_store.save(messages);
    /// ```
    ///
    /// Returns an empty vector if no messages are available within the timeout period.
    /// This function does not block the calling thread.
    async fn poll(&self) -> Vec<MessageExt>;

    /// Fetches the next batch of messages with a specified timeout, returning owned copies.
    ///
    /// Behaves identically to [`poll()`], but waits up to `timeout` milliseconds for
    /// messages to become available. All messages are deep-cloned.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for messages, in milliseconds.
    ///
    /// Returns an empty vector if no messages are available before the timeout expires.
    /// This function does not block the calling thread.
    async fn poll_with_timeout(&self, timeout: u64) -> Vec<MessageExt>;

    /// Seeks the fetch position of the specified [`MessageQueue`] to the given offset.
    ///
    /// The next [`poll`] invocation will return messages starting from `offset`.
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The queue whose fetch position is to be updated.
    /// * `offset` - The target offset. Must be within the queue's valid range.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue is not currently assigned to this consumer,
    /// or if the specified offset is out of the valid range.
    ///
    /// [`poll`]: LitePullConsumerLocal::poll
    async fn seek(&self, message_queue: &MessageQueue, offset: i64) -> rocketmq_error::RocketMQResult<()>;

    /// Suspends message fetching for the specified [`MessageQueue`]s.
    ///
    /// Paused queues are excluded from subsequent [`poll`] results until [`resume`] is called.
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queues` - The queues to pause.
    ///
    /// [`poll`]: LitePullConsumerLocal::poll
    /// [`resume`]: LitePullConsumerLocal::resume
    async fn pause(&self, message_queues: Vec<MessageQueue>);

    /// Resumes message fetching for the specified [`MessageQueue`]s.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queues` - The queues to resume.
    async fn resume(&self, message_queues: Vec<MessageQueue>);

    /// Returns whether automatic offset commit is enabled.
    ///
    /// This function does not block the calling thread.
    async fn is_auto_commit(&self) -> bool;

    /// Enables or disables automatic offset commit.
    ///
    /// When auto-commit is enabled, offsets are committed periodically without explicit calls
    /// to [`commit`] or [`commit_sync`]. When disabled, the caller is responsible for committing
    /// offsets.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `auto_commit` - `true` to enable automatic offset commit; `false` to disable it.
    ///
    /// [`commit`]: LitePullConsumerLocal::commit
    /// [`commit_sync`]: LitePullConsumerLocal::commit_sync
    async fn set_auto_commit(&self, auto_commit: bool);

    /// Queries the broker for all [`MessageQueue`]s belonging to the specified topic.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to query.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic does not exist, if the name server is unreachable,
    /// or if the consumer is not in the running state.
    async fn fetch_message_queues(&self, topic: &str) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>>;

    /// Queries the broker for the offset corresponding to the given timestamp in a queue.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The queue to query.
    /// * `timestamp` - The Unix timestamp in milliseconds. The broker returns the offset of the
    ///   first message stored at or after this timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue is not found on the broker or the query fails.
    async fn offset_for_timestamp(
        &self,
        message_queue: &MessageQueue,
        timestamp: u64,
    ) -> rocketmq_error::RocketMQResult<i64>;

    /// Commits all consumed offsets and waits for the broker to acknowledge the operation.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Deprecation
    ///
    /// This method is deprecated. The name implies synchronous behavior, but the underlying
    /// implementation relies on a background thread to commit offsets rather than committing
    /// synchronously. Use [`commit`] instead.
    ///
    /// [`commit`]: LitePullConsumerLocal::commit
    async fn commit_sync(&self);

    /// Commits the provided offsets and optionally persists them to the broker.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Deprecation
    ///
    /// This method is deprecated. The name implies synchronous behavior, but the underlying
    /// implementation relies on a background thread to commit offsets rather than committing
    /// synchronously. Use [`commit_with_map`] instead.
    ///
    /// # Arguments
    ///
    /// * `offset_map` - A map from [`MessageQueue`] to the offset to commit.
    /// * `persist` - When `true`, the committed offsets are persisted to the broker immediately.
    ///
    /// [`commit_with_map`]: LitePullConsumerLocal::commit_with_map
    async fn commit_sync_with_map(&self, offset_map: HashMap<MessageQueue, i64>, persist: bool);

    /// Commits all consumed offsets asynchronously.
    ///
    /// This function does not block the calling thread. The commit is performed in the
    /// background; use [`commit_sync`] if acknowledgment is required before proceeding.
    ///
    /// [`commit_sync`]: LitePullConsumerLocal::commit_sync
    async fn commit(&self);

    /// Commits the provided offsets asynchronously, optionally persisting them to the broker.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `offset_map` - A map from [`MessageQueue`] to the offset to commit.
    /// * `persist` - When `true`, the committed offsets are persisted to the broker.
    async fn commit_with_map(&self, offset_map: HashMap<MessageQueue, i64>, persist: bool);

    /// Commits the offsets for the specified subset of assigned queues.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queues` - The queues whose current offsets are to be committed.
    /// * `persist` - When `true`, the committed offsets are persisted to the broker.
    async fn commit_with_set(&self, message_queues: HashSet<MessageQueue>, persist: bool);

    /// Returns the last committed offset for the specified [`MessageQueue`].
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The queue to query.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue is not assigned to this consumer or if the offset
    /// cannot be retrieved from the offset store.
    async fn committed(&self, message_queue: &MessageQueue) -> rocketmq_error::RocketMQResult<i64>;

    /// Registers a listener that is notified when the set of [`MessageQueue`]s for a topic changes.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to monitor for queue changes.
    /// * `listener` - A [`TopicMessageQueueChangeListener`] invoked when the queue set changes.
    ///
    /// # Errors
    ///
    /// Returns an error if a listener is already registered for the given topic, or if the
    /// registration fails due to an internal error.
    async fn register_topic_message_queue_change_listener<TL>(
        &self,
        topic: &str,
        listener: TL,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        TL: TopicMessageQueueChangeListener + 'static;

    /// Updates the name server address used for topic route discovery.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `name_server_address` - The new semicolon-separated name server address list.
    async fn update_name_server_address(&self, name_server_address: &str);

    /// Seeks the fetch position of the specified [`MessageQueue`] to its earliest available offset.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The queue to seek to the beginning.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue is not assigned to this consumer or if the earliest
    /// offset cannot be retrieved from the broker.
    async fn seek_to_begin(&self, message_queue: &MessageQueue) -> rocketmq_error::RocketMQResult<()>;

    /// Seeks the fetch position of the specified [`MessageQueue`] to its latest available offset.
    ///
    /// The next [`poll`] call will return only messages published after this point.
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The queue to seek to the end.
    ///
    /// # Errors
    ///
    /// Returns an error if the queue is not assigned to this consumer or if the latest
    /// offset cannot be retrieved from the broker.
    ///
    /// [`poll`]: LitePullConsumerLocal::poll
    async fn seek_to_end(&self, message_queue: &MessageQueue) -> rocketmq_error::RocketMQResult<()>;

    /// Commits all consumed offsets for all assigned queues.
    ///
    /// This method commits the current consumption offset for every assigned [`MessageQueue`].
    /// Unlike [`commit`], which commits offsets asynchronously in the background, this method
    /// ensures all offsets are persisted to the broker.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Errors
    ///
    /// Returns an error if the consumer is not in the running state or if the offset
    /// persistence fails.
    ///
    /// [`commit`]: LitePullConsumerLocal::commit
    async fn commit_all(&self) -> rocketmq_error::RocketMQResult<()>;

    /// Checks whether a specific [`MessageQueue`] is currently paused.
    ///
    /// A paused queue will not be fetched from during [`poll`] operations until it is resumed.
    ///
    /// This function does not block the calling thread.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The queue to check.
    ///
    /// # Returns
    ///
    /// `true` if the queue is paused, `false` otherwise.
    ///
    /// [`poll`]: LitePullConsumerLocal::poll
    async fn is_paused(&self, message_queue: &MessageQueue) -> bool;
}
