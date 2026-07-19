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
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tokio::sync::OnceCell;
use tokio::sync::RwLock;

use crate::base::client_config::ClientConfig;
use crate::base::query_result::QueryResult;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::validate_lite_pull_consume_from_where;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::DefaultLitePullConsumerImpl;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::LitePullConsumerConfig;
use crate::consumer::default_lite_pull_consumer_builder::DefaultLitePullConsumerBuilder;
use crate::consumer::default_lite_pull_consumer_builder::MIN_AUTOCOMMIT_INTERVAL_MILLIS;
use crate::consumer::lite_pull_consumer::LitePullConsumer;
use crate::consumer::message_queue_listener::ArcMessageQueueListener;
use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::mq_consumer::MQConsumer;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::store::offset_store::OffsetStore;
use crate::consumer::topic_message_queue_change_listener::TopicMessageQueueChangeListener;
use crate::trace::async_trace_dispatcher::AsyncTraceDispatcher;
use crate::trace::hook::consume_message_trace_hook_impl::ConsumeMessageTraceHookImpl;
use crate::trace::trace_dispatcher::TraceDispatcher;
use crate::trace::trace_dispatcher::Type;

/// Default implementation of a lite pull consumer.
///
/// This is the main entry point for creating and using a lite pull consumer. It acts as a facade
/// over the internal implementation ([`DefaultLitePullConsumerImpl`]) and provides:
///
/// - Configuration management via [`DefaultLitePullConsumerBuilder`]
/// - Namespace handling (automatic topic name wrapping/unwrapping)
/// - Optional message trace integration
/// - A clean public API that delegates to the internal implementation
///
/// # Architecture
///
/// ```text
/// ┌────────────────────────────────────┐
/// │   DefaultLitePullConsumer          │  ← Public Facade
/// │  (config, trace, namespace)        │
/// └──────────────┬─────────────────────┘
///                │ delegates
///                ↓
/// ┌────────────────────────────────────┐
/// │ DefaultLitePullConsumerImpl        │  ← Core Logic
/// │  (lifecycle, pull, commit, etc.)   │
/// └────────────────────────────────────┘
/// ```
///
/// # Examples
///
/// ## Basic usage with auto-commit
///
/// ```rust,ignore
/// use rocketmq_client::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
///
/// let consumer = DefaultLitePullConsumer::builder()
///     .consumer_group("my_consumer_group")
///     .name_server_addr("127.0.0.1:9876")
///     .auto_commit(true)
///     .build()?;
///
/// consumer.start().await?;
/// consumer.subscribe("my_topic").await?;
///
/// loop {
///     let messages = consumer.poll_with_timeout(1000).await;
///     for msg in messages {
///         println!("Received: {:?}", msg);
///     }
///     // Offsets are auto-committed
/// }
/// ```
///
/// ## Manual offset control
///
/// ```rust,ignore
/// let consumer = DefaultLitePullConsumer::builder()
///     .consumer_group("my_consumer_group")
///     .name_server_addr("127.0.0.1:9876")
///     .auto_commit(false)  // Disable auto-commit
///     .build()?;
///
/// consumer.start().await?;
/// consumer.subscribe("my_topic").await?;
///
/// loop {
///     let messages = consumer.poll_with_timeout(1000).await;
///     for msg in messages {
///         // Process message
///     }
///     
///     // Manually commit after processing
///     consumer.commit_all().await?;
/// }
/// ```
///
/// ## Manual queue assignment (no rebalance)
///
/// ```rust,ignore
/// use rocketmq_common::common::message::message_queue::MessageQueue;
///
/// let mq1 = MessageQueue::from_parts("my_topic", "broker-a", 0);
/// let mq2 = MessageQueue::from_parts("my_topic", "broker-a", 1);
///
/// consumer.start().await?;
/// consumer.assign(vec![mq1, mq2]).await?;
///
/// let messages = consumer.poll().await;
/// ```
#[derive(Clone)]
pub struct DefaultLitePullConsumer {
    /// Client configuration (network, instance name, etc.)
    client_config: ArcMut<ClientConfig>,

    /// Consumer-specific configuration (pull batch size, flow control, etc.)
    consumer_config: ArcMut<LitePullConsumerConfig>,

    /// Core implementation (lazy-initialized on start using OnceCell)
    default_lite_pull_consumer_impl: Arc<OnceCell<ArcMut<DefaultLitePullConsumerImpl>>>,

    /// Optional RPC hook for request/response interception
    rpc_hook: Option<Arc<dyn RPCHook>>,

    /// Optional trace dispatcher for message tracing (with interior mutability)
    trace_dispatcher: Arc<RwLock<Option<Arc<dyn TraceDispatcher + Send + Sync>>>>,

    /// Whether message trace is enabled
    enable_msg_trace: bool,

    /// Custom trace topic (if specified)
    custom_trace_topic: Option<CheetahString>,

    /// User listener notified after LitePull rebalance updates assigned queues.
    message_queue_listener: Arc<StdRwLock<Option<ArcMessageQueueListener>>>,

    /// User-provided or initialized offset store.
    offset_store: Arc<StdRwLock<Option<Arc<OffsetStore>>>>,
}

impl DefaultLitePullConsumer {
    /// Creates a new consumer with the specified configuration.
    ///
    /// Most users should use [`builder()`](Self::builder) instead.
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<LitePullConsumerConfig>,
        rpc_hook: Option<Arc<dyn RPCHook>>,
        trace_dispatcher: Option<Arc<dyn TraceDispatcher + Send + Sync>>,
        enable_msg_trace: bool,
        custom_trace_topic: Option<CheetahString>,
    ) -> Self {
        Self {
            client_config,
            consumer_config,
            default_lite_pull_consumer_impl: Arc::new(OnceCell::new()),
            rpc_hook,
            trace_dispatcher: Arc::new(RwLock::new(trace_dispatcher)),
            enable_msg_trace,
            custom_trace_topic,
            message_queue_listener: Arc::new(StdRwLock::new(None)),
            offset_store: Arc::new(StdRwLock::new(None)),
        }
    }

    /// Returns a builder for creating a consumer with custom configuration.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let consumer = DefaultLitePullConsumer::builder()
    ///     .consumer_group("my_group")
    ///     .name_server_addr("127.0.0.1:9876")
    ///     .pull_batch_size(32)
    ///     .build()?;
    /// ```
    pub fn builder() -> DefaultLitePullConsumerBuilder {
        DefaultLitePullConsumerBuilder::new()
    }

    /// Starts the consumer.
    pub async fn start(&self) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::start(self).await
    }

    /// Shuts down the consumer.
    pub async fn shutdown(&self) {
        <Self as LitePullConsumer>::shutdown(self).await;
    }

    /// Returns whether the consumer is currently running.
    pub async fn is_running(&self) -> bool {
        <Self as LitePullConsumer>::is_running(self).await
    }

    /// Returns a Java-compatible running snapshot for broker admin diagnostics.
    pub async fn consumer_running_info(&self) -> RocketMQResult<ConsumerRunningInfo> {
        let impl_ = self.try_impl_()?;
        Ok(MQConsumerInner::consumer_running_info(impl_.as_ref()).await)
    }

    /// Subscribes to a topic using the default subscription expression.
    pub async fn subscribe(&self, topic: &str) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::subscribe(self, topic).await
    }

    /// Subscribes to a topic using a tag or SQL expression.
    pub async fn subscribe_with_expression(&self, topic: &str, sub_expression: &str) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::subscribe_with_expression(self, topic, sub_expression).await
    }

    /// Subscribes with a queue-change listener.
    pub async fn subscribe_with_listener<MQL>(
        &self,
        topic: &str,
        sub_expression: &str,
        listener: MQL,
    ) -> RocketMQResult<()>
    where
        MQL: MessageQueueListener + 'static,
    {
        <Self as LitePullConsumer>::subscribe_with_listener(self, topic, sub_expression, listener).await
    }

    /// Subscribes using a server-side selector.
    pub async fn subscribe_with_selector(&self, topic: &str, selector: Option<MessageSelector>) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::subscribe_with_selector(self, topic, selector).await
    }

    /// Removes the subscription for a topic.
    pub async fn unsubscribe(&self, topic: &str) {
        <Self as LitePullConsumer>::unsubscribe(self, topic).await;
    }

    /// Returns the currently assigned queues.
    pub async fn assignment(&self) -> RocketMQResult<HashSet<MessageQueue>> {
        <Self as LitePullConsumer>::assignment(self).await
    }

    /// Assigns queues manually, bypassing broker rebalance.
    pub async fn assign(&self, message_queues: Vec<MessageQueue>) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::assign(self, message_queues).await
    }

    /// Sets the subscription expression used for manually assigned queues.
    pub async fn set_sub_expression_for_assign(&self, topic: &str, sub_expression: &str) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::set_sub_expression_for_assign(self, topic, sub_expression).await
    }

    /// Builds subscription metadata for heartbeat payloads.
    pub async fn build_subscriptions_for_heartbeat(
        &self,
        sub_expression_map: &mut HashMap<String, MessageSelector>,
    ) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::build_subscriptions_for_heartbeat(self, sub_expression_map).await
    }

    /// Returns heartbeat subscriptions.
    pub async fn subscriptions_for_heartbeat(&self) -> HashSet<SubscriptionData> {
        <Self as LitePullConsumer>::subscriptions_for_heartbeat(self).await
    }

    /// Java-compatible getter alias for heartbeat subscriptions.
    pub async fn get_subscriptions_for_heartbeat(&self) -> HashSet<SubscriptionData> {
        self.subscriptions_for_heartbeat().await
    }

    /// Polls messages without cloning message payloads.
    pub async fn poll_zero_copy(&self) -> Vec<Arc<MessageExt>> {
        <Self as LitePullConsumer>::poll_zero_copy(self).await
    }

    /// Polls messages without cloning message payloads with a custom timeout.
    pub async fn poll_with_timeout_zero_copy(&self, timeout: u64) -> Vec<Arc<MessageExt>> {
        <Self as LitePullConsumer>::poll_with_timeout_zero_copy(self, timeout).await
    }

    /// Polls messages and returns owned copies.
    pub async fn poll(&self) -> Vec<MessageExt> {
        <Self as LitePullConsumer>::poll(self).await
    }

    /// Polls messages with a custom timeout and returns owned copies.
    pub async fn poll_with_timeout(&self, timeout: u64) -> Vec<MessageExt> {
        <Self as LitePullConsumer>::poll_with_timeout(self, timeout).await
    }

    /// Returns the configured message model.
    pub async fn message_model(&self) -> MessageModel {
        <Self as LitePullConsumer>::message_model(self).await
    }

    /// Java-compatible getter alias for the configured message model.
    pub async fn get_message_model(&self) -> MessageModel {
        self.message_model().await
    }

    /// Sets the configured message model.
    pub async fn set_message_model(&self, message_model: MessageModel) {
        <Self as LitePullConsumer>::set_message_model(self, message_model).await;
    }

    /// Returns where consumption starts when no offset exists.
    pub async fn consume_from_where(&self) -> ConsumeFromWhere {
        <Self as LitePullConsumer>::consume_from_where(self).await
    }

    /// Java-compatible getter alias for where consumption starts when no offset exists.
    pub async fn get_consume_from_where(&self) -> ConsumeFromWhere {
        self.consume_from_where().await
    }

    /// Sets where consumption starts when no offset exists.
    pub async fn set_consume_from_where(&self, consume_from_where: ConsumeFromWhere) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::set_consume_from_where(self, consume_from_where).await
    }

    /// Returns the consume timestamp.
    pub async fn consume_timestamp(&self) -> Option<CheetahString> {
        <Self as LitePullConsumer>::consume_timestamp(self).await
    }

    /// Java-compatible getter alias for the consume timestamp.
    pub async fn get_consume_timestamp(&self) -> Option<CheetahString> {
        self.consume_timestamp().await
    }

    /// Sets the consume timestamp.
    pub async fn set_consume_timestamp(&self, consume_timestamp: Option<CheetahString>) {
        <Self as LitePullConsumer>::set_consume_timestamp(self, consume_timestamp).await;
    }

    /// Returns the queue allocation strategy.
    pub async fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy + Send + Sync> {
        <Self as LitePullConsumer>::allocate_message_queue_strategy(self).await
    }

    /// Java-compatible getter alias for the queue allocation strategy.
    pub async fn get_allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy + Send + Sync> {
        self.allocate_message_queue_strategy().await
    }

    /// Sets the queue allocation strategy.
    pub async fn set_allocate_message_queue_strategy(
        &self,
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy + Send + Sync>,
    ) {
        <Self as LitePullConsumer>::set_allocate_message_queue_strategy(self, allocate_message_queue_strategy).await;
    }

    /// Returns the message queue listener.
    pub async fn message_queue_listener(&self) -> Option<ArcMessageQueueListener> {
        <Self as LitePullConsumer>::message_queue_listener(self).await
    }

    /// Java-compatible getter alias for the message queue listener.
    pub async fn get_message_queue_listener(&self) -> Option<ArcMessageQueueListener> {
        self.message_queue_listener().await
    }

    /// Sets the message queue listener.
    pub async fn set_message_queue_listener(&self, message_queue_listener: Option<ArcMessageQueueListener>) {
        <Self as LitePullConsumer>::set_message_queue_listener(self, message_queue_listener).await;
    }

    /// Returns the configured offset store.
    pub async fn offset_store(&self) -> Option<Arc<OffsetStore>> {
        <Self as LitePullConsumer>::offset_store(self).await
    }

    /// Java-compatible getter alias for the configured offset store.
    pub async fn get_offset_store(&self) -> Option<Arc<OffsetStore>> {
        self.offset_store().await
    }

    /// Sets the configured offset store.
    pub async fn set_offset_store(&self, offset_store: Option<Arc<OffsetStore>>) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::set_offset_store(self, offset_store).await
    }

    /// Returns the topic metadata check interval in milliseconds.
    pub async fn topic_metadata_check_interval_millis(&self) -> u64 {
        <Self as LitePullConsumer>::topic_metadata_check_interval_millis(self).await
    }

    /// Java-compatible getter alias for the topic metadata check interval.
    pub async fn get_topic_metadata_check_interval_millis(&self) -> u64 {
        self.topic_metadata_check_interval_millis().await
    }

    /// Sets the topic metadata check interval in milliseconds.
    pub async fn set_topic_metadata_check_interval_millis(&self, interval_millis: u64) {
        <Self as LitePullConsumer>::set_topic_metadata_check_interval_millis(self, interval_millis).await;
    }

    /// Returns the default poll timeout in milliseconds.
    pub async fn poll_timeout_millis(&self) -> u64 {
        <Self as LitePullConsumer>::poll_timeout_millis(self).await
    }

    /// Java-compatible getter alias for the default poll timeout.
    pub async fn get_poll_timeout_millis(&self) -> u64 {
        self.poll_timeout_millis().await
    }

    /// Sets the default poll timeout in milliseconds.
    pub async fn set_poll_timeout_millis(&self, timeout_millis: u64) {
        <Self as LitePullConsumer>::set_poll_timeout_millis(self, timeout_millis).await;
    }

    /// Returns the broker suspend max time in milliseconds.
    pub async fn broker_suspend_max_time_millis(&self) -> u64 {
        <Self as LitePullConsumer>::broker_suspend_max_time_millis(self).await
    }

    /// Java-compatible getter alias for the broker suspend max time.
    pub async fn get_broker_suspend_max_time_millis(&self) -> u64 {
        self.broker_suspend_max_time_millis().await
    }

    /// Sets the broker suspend max time in milliseconds.
    pub async fn set_broker_suspend_max_time_millis(&self, timeout_millis: u64) {
        <Self as LitePullConsumer>::set_broker_suspend_max_time_millis(self, timeout_millis).await;
    }

    /// Returns the consumer timeout while suspended.
    pub async fn consumer_timeout_millis_when_suspend(&self) -> u64 {
        <Self as LitePullConsumer>::consumer_timeout_millis_when_suspend(self).await
    }

    /// Java-compatible getter alias for the consumer timeout while suspended.
    pub async fn get_consumer_timeout_millis_when_suspend(&self) -> u64 {
        self.consumer_timeout_millis_when_suspend().await
    }

    /// Sets the consumer timeout while suspended.
    pub async fn set_consumer_timeout_millis_when_suspend(&self, timeout_millis: u64) {
        <Self as LitePullConsumer>::set_consumer_timeout_millis_when_suspend(self, timeout_millis).await;
    }

    /// Returns the pull RPC timeout in milliseconds.
    pub async fn consumer_pull_timeout_millis(&self) -> u64 {
        <Self as LitePullConsumer>::consumer_pull_timeout_millis(self).await
    }

    /// Java-compatible getter alias for the pull RPC timeout.
    pub async fn get_consumer_pull_timeout_millis(&self) -> u64 {
        self.consumer_pull_timeout_millis().await
    }

    /// Sets the pull RPC timeout in milliseconds.
    pub async fn set_consumer_pull_timeout_millis(&self, timeout_millis: u64) {
        <Self as LitePullConsumer>::set_consumer_pull_timeout_millis(self, timeout_millis).await;
    }

    /// Returns the pull batch size.
    pub async fn pull_batch_size(&self) -> i32 {
        <Self as LitePullConsumer>::pull_batch_size(self).await
    }

    /// Java-compatible getter alias for the pull batch size.
    pub async fn get_pull_batch_size(&self) -> i32 {
        self.pull_batch_size().await
    }

    /// Sets the pull batch size.
    pub async fn set_pull_batch_size(&self, pull_batch_size: i32) {
        <Self as LitePullConsumer>::set_pull_batch_size(self, pull_batch_size).await;
    }

    /// Returns the configured pull thread count.
    pub async fn pull_thread_nums(&self) -> usize {
        <Self as LitePullConsumer>::pull_thread_nums(self).await
    }

    /// Java-compatible getter alias for the configured pull thread count.
    pub async fn get_pull_thread_nums(&self) -> usize {
        self.pull_thread_nums().await
    }

    /// Sets the configured pull thread count.
    pub async fn set_pull_thread_nums(&self, pull_thread_nums: usize) {
        <Self as LitePullConsumer>::set_pull_thread_nums(self, pull_thread_nums).await;
    }

    /// Returns the all-queue pull threshold.
    pub async fn pull_threshold_for_all(&self) -> i64 {
        <Self as LitePullConsumer>::pull_threshold_for_all(self).await
    }

    /// Java-compatible getter alias for the all-queue pull threshold.
    pub async fn get_pull_threshold_for_all(&self) -> i64 {
        self.pull_threshold_for_all().await
    }

    /// Sets the all-queue pull threshold.
    pub async fn set_pull_threshold_for_all(&self, pull_threshold_for_all: i64) {
        <Self as LitePullConsumer>::set_pull_threshold_for_all(self, pull_threshold_for_all).await;
    }

    /// Returns the per-queue pull threshold.
    pub async fn pull_threshold_for_queue(&self) -> i64 {
        <Self as LitePullConsumer>::pull_threshold_for_queue(self).await
    }

    /// Java-compatible getter alias for the per-queue pull threshold.
    pub async fn get_pull_threshold_for_queue(&self) -> i64 {
        self.pull_threshold_for_queue().await
    }

    /// Sets the per-queue pull threshold.
    pub async fn set_pull_threshold_for_queue(&self, pull_threshold_for_queue: i64) {
        <Self as LitePullConsumer>::set_pull_threshold_for_queue(self, pull_threshold_for_queue).await;
    }

    /// Returns the per-queue pull size threshold.
    pub async fn pull_threshold_size_for_queue(&self) -> i32 {
        <Self as LitePullConsumer>::pull_threshold_size_for_queue(self).await
    }

    /// Java-compatible getter alias for the per-queue pull size threshold.
    pub async fn get_pull_threshold_size_for_queue(&self) -> i32 {
        self.pull_threshold_size_for_queue().await
    }

    /// Sets the per-queue pull size threshold.
    pub async fn set_pull_threshold_size_for_queue(&self, pull_threshold_size_for_queue: i32) {
        <Self as LitePullConsumer>::set_pull_threshold_size_for_queue(self, pull_threshold_size_for_queue).await;
    }

    /// Returns the max consume span.
    pub async fn consume_max_span(&self) -> i64 {
        <Self as LitePullConsumer>::consume_max_span(self).await
    }

    /// Java-compatible getter alias for the max consume span.
    pub async fn get_consume_max_span(&self) -> i64 {
        self.consume_max_span().await
    }

    /// Sets the max consume span.
    pub async fn set_consume_max_span(&self, consume_max_span: i64) {
        <Self as LitePullConsumer>::set_consume_max_span(self, consume_max_span).await;
    }

    /// Returns whether pulls use the configured broker id.
    pub async fn is_connect_broker_by_user(&self) -> bool {
        <Self as LitePullConsumer>::is_connect_broker_by_user(self).await
    }

    /// Sets whether pulls use the configured broker id.
    pub async fn set_connect_broker_by_user(&self, connect_broker_by_user: bool) {
        <Self as LitePullConsumer>::set_connect_broker_by_user(self, connect_broker_by_user).await;
    }

    /// Returns the configured default broker id.
    pub async fn default_broker_id(&self) -> u64 {
        <Self as LitePullConsumer>::default_broker_id(self).await
    }

    /// Java-compatible getter alias for the configured default broker id.
    pub async fn get_default_broker_id(&self) -> u64 {
        self.default_broker_id().await
    }

    /// Sets the configured default broker id.
    pub async fn set_default_broker_id(&self, broker_id: u64) {
        <Self as LitePullConsumer>::set_default_broker_id(self, broker_id).await;
    }

    /// Fetches message queues for a topic.
    pub async fn fetch_message_queues(&self, topic: &str) -> RocketMQResult<Vec<MessageQueue>> {
        <Self as LitePullConsumer>::fetch_message_queues(self, topic).await
    }

    /// Returns the committed offset for a queue.
    pub async fn committed(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        <Self as LitePullConsumer>::committed(self, message_queue).await
    }

    /// Commits all offsets to the configured offset store.
    pub async fn commit_all(&self) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::commit_all(self).await
    }

    /// Commits explicit offsets.
    pub async fn commit_with_map(&self, offset_map: HashMap<MessageQueue, i64>, persist: bool) {
        <Self as LitePullConsumer>::commit_with_map(self, offset_map, persist).await;
    }

    /// Commits all offsets.
    pub async fn commit(&self) {
        <Self as LitePullConsumer>::commit(self).await;
    }

    /// Commits offsets for selected queues.
    pub async fn commit_with_set(&self, message_queues: HashSet<MessageQueue>, persist: bool) {
        <Self as LitePullConsumer>::commit_with_set(self, message_queues, persist).await;
    }

    /// Java-compatible alias for committing all offsets.
    pub async fn commit_sync(&self) {
        <Self as LitePullConsumer>::commit_sync(self).await;
    }

    /// Java-compatible alias for committing explicit offsets.
    pub async fn commit_sync_with_map(&self, offset_map: HashMap<MessageQueue, i64>, persist: bool) {
        <Self as LitePullConsumer>::commit_sync_with_map(self, offset_map, persist).await;
    }

    /// Returns whether auto commit is enabled.
    pub async fn is_auto_commit(&self) -> bool {
        <Self as LitePullConsumer>::is_auto_commit(self).await
    }

    /// Sets whether auto commit is enabled.
    pub async fn set_auto_commit(&self, auto_commit: bool) {
        <Self as LitePullConsumer>::set_auto_commit(self, auto_commit).await;
    }

    /// Returns whether unit mode is enabled.
    pub async fn is_unit_mode(&self) -> bool {
        <Self as LitePullConsumer>::is_unit_mode(self).await
    }

    /// Sets whether unit mode is enabled.
    pub async fn set_unit_mode(&self, unit_mode: bool) {
        <Self as LitePullConsumer>::set_unit_mode(self, unit_mode).await;
    }

    /// Returns the auto commit interval in milliseconds.
    pub async fn auto_commit_interval_millis(&self) -> u64 {
        <Self as LitePullConsumer>::auto_commit_interval_millis(self).await
    }

    /// Java-compatible getter alias for the auto commit interval.
    pub async fn get_auto_commit_interval_millis(&self) -> u64 {
        self.auto_commit_interval_millis().await
    }

    /// Sets the auto commit interval in milliseconds.
    pub async fn set_auto_commit_interval_millis(&self, interval_millis: u64) {
        <Self as LitePullConsumer>::set_auto_commit_interval_millis(self, interval_millis).await;
    }

    /// Queries the offset for a timestamp.
    pub async fn offset_for_timestamp(&self, message_queue: &MessageQueue, timestamp: u64) -> RocketMQResult<i64> {
        <Self as LitePullConsumer>::offset_for_timestamp(self, message_queue, timestamp).await
    }

    /// Queries the earliest store time for a queue.
    pub async fn earliest_msg_store_time(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        <Self as LitePullConsumer>::earliest_msg_store_time(self, message_queue).await
    }

    /// Queries the broker max offset for a queue.
    pub async fn max_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        <Self as LitePullConsumer>::max_offset(self, message_queue).await
    }

    /// Queries the broker min offset for a queue.
    pub async fn min_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        <Self as LitePullConsumer>::min_offset(self, message_queue).await
    }

    /// Creates a topic through the Java-compatible MQAdmin facade.
    pub async fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        attributes: HashMap<String, String>,
    ) -> RocketMQResult<()> {
        self.create_topic_with_flag(key, new_topic, queue_num, 0, attributes)
            .await
    }

    /// Creates a topic with an explicit topic system flag.
    pub async fn create_topic_with_flag(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> RocketMQResult<()> {
        let new_topic = self.with_namespace(new_topic);
        self.try_impl_()?
            .create_topic(key, new_topic.as_str(), queue_num, topic_sys_flag, attributes)
            .await
    }

    /// Queries messages by key in a time range.
    pub async fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> RocketMQResult<QueryResult> {
        let topic = self.with_namespace(topic);
        self.try_impl_()?
            .query_message(topic.as_str(), key, max_num, begin, end)
            .await
    }

    /// Views a message by offset message id or Java-compatible unique message id.
    pub async fn view_message(&self, topic: &str, msg_id: &str) -> RocketMQResult<MessageExt> {
        let topic = self.with_namespace(topic);
        let impl_ = self.try_impl_()?;
        if message_decoder::decode_message_id(msg_id).is_ok() {
            impl_.view_message(topic.as_str(), msg_id).await
        } else {
            impl_.query_message_by_uniq_key(topic.as_str(), msg_id).await
        }
    }

    /// Pauses fetching from selected queues.
    pub async fn pause(&self, message_queues: Vec<MessageQueue>) {
        <Self as LitePullConsumer>::pause(self, message_queues).await;
    }

    /// Resumes fetching from selected queues.
    pub async fn resume(&self, message_queues: Vec<MessageQueue>) {
        <Self as LitePullConsumer>::resume(self, message_queues).await;
    }

    /// Returns whether a queue is paused.
    pub async fn is_paused(&self, message_queue: &MessageQueue) -> bool {
        <Self as LitePullConsumer>::is_paused(self, message_queue).await
    }

    /// Seeks a queue to an offset.
    pub async fn seek(&self, message_queue: &MessageQueue, offset: i64) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::seek(self, message_queue, offset).await
    }

    /// Seeks a queue to the beginning.
    pub async fn seek_to_begin(&self, message_queue: &MessageQueue) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::seek_to_begin(self, message_queue).await
    }

    /// Seeks a queue to the end.
    pub async fn seek_to_end(&self, message_queue: &MessageQueue) -> RocketMQResult<()> {
        <Self as LitePullConsumer>::seek_to_end(self, message_queue).await
    }

    /// Updates the name server address.
    pub async fn update_name_server_address(&self, name_server_address: &str) {
        <Self as LitePullConsumer>::update_name_server_address(self, name_server_address).await;
    }

    /// Registers a topic queue-change listener.
    pub async fn register_topic_message_queue_change_listener<TL>(
        &self,
        topic: &str,
        listener: TL,
    ) -> RocketMQResult<()>
    where
        TL: TopicMessageQueueChangeListener + 'static,
    {
        <Self as LitePullConsumer>::register_topic_message_queue_change_listener(self, topic, listener).await
    }

    /// Returns the consumer group name.
    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_config.consumer_group
    }

    /// Java-compatible getter alias for the consumer group name.
    pub fn get_consumer_group(&self) -> &CheetahString {
        self.consumer_group()
    }

    /// Sets the consumer group name before the consumer starts.
    ///
    /// Like Java `DefaultLitePullConsumer.setConsumerGroup`, this updates the facade
    /// configuration. If the internal implementation has already been initialized,
    /// the value is synchronized into the impl and rebalance configuration while it
    /// is still in the create state.
    pub fn set_consumer_group(&self, consumer_group: impl Into<CheetahString>) -> RocketMQResult<()> {
        let consumer_group = consumer_group.into();
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_consumer_group(consumer_group.clone())?;
        }
        self.consumer_config.mut_from_ref().consumer_group = consumer_group;
        Ok(())
    }

    /// Returns the namespace (if configured).
    pub fn namespace(&self) -> Option<CheetahString> {
        self.client_config.mut_from_ref().get_namespace()
    }

    /// Returns the custom trace topic, if configured.
    pub fn customized_trace_topic(&self) -> Option<CheetahString> {
        self.custom_trace_topic.clone()
    }

    /// Java-compatible getter alias for the custom trace topic.
    pub fn get_customized_trace_topic(&self) -> Option<CheetahString> {
        self.customized_trace_topic()
    }

    /// Sets the custom trace topic used when message tracing is enabled.
    pub fn set_customized_trace_topic(&mut self, customized_trace_topic: impl Into<CheetahString>) {
        self.custom_trace_topic = Some(customized_trace_topic.into());
    }

    /// Clears the custom trace topic so the default system trace topic is used.
    pub fn clear_customized_trace_topic(&mut self) {
        self.custom_trace_topic = None;
    }

    /// Returns whether message trace is enabled.
    pub fn is_enable_msg_trace(&self) -> bool {
        self.enable_msg_trace
    }

    /// Enables or disables message trace for subsequent startup.
    pub fn set_enable_msg_trace(&mut self, enable_msg_trace: bool) {
        self.enable_msg_trace = enable_msg_trace;
    }

    /// Returns whether outbound remoting connections should use TLS.
    pub fn is_use_tls(&self) -> bool {
        self.client_config.is_use_tls()
    }

    /// Enables or disables TLS on the shared client configuration and trace dispatcher.
    pub async fn set_use_tls(&self, use_tls: bool) {
        self.client_config.mut_from_ref().set_use_tls(use_tls);
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_use_tls(use_tls);
        }
        if let Some(dispatcher) = self.trace_dispatcher.read().await.as_ref().cloned() {
            if let Some(async_dispatcher) = dispatcher.as_any().downcast_ref::<AsyncTraceDispatcher>() {
                async_dispatcher.set_use_tls(use_tls);
            }
        }
    }

    /// Returns the trace dispatcher, if tracing has been configured or initialized.
    pub async fn trace_dispatcher(&self) -> Option<Arc<dyn TraceDispatcher + Send + Sync>> {
        self.trace_dispatcher.read().await.as_ref().cloned()
    }

    /// Java-compatible getter alias for the trace dispatcher.
    pub async fn get_trace_dispatcher(&self) -> Option<Arc<dyn TraceDispatcher + Send + Sync>> {
        self.trace_dispatcher().await
    }

    /// Returns the client configuration.
    pub fn client_config(&self) -> &ArcMut<ClientConfig> {
        &self.client_config
    }

    /// Returns the consumer configuration.
    pub fn consumer_config(&self) -> &ArcMut<LitePullConsumerConfig> {
        &self.consumer_config
    }

    /// Wraps a topic name with the namespace (if configured).
    fn with_namespace(&self, resource: &str) -> CheetahString {
        self.client_config
            .mut_from_ref()
            .with_namespace(CheetahString::from(resource))
    }

    /// Removes namespace from a topic name (if configured).
    fn without_namespace(&self, resource: &str) -> CheetahString {
        match self.client_config.mut_from_ref().get_namespace() {
            Some(namespace) if !namespace.is_empty() => {
                NamespaceUtil::without_namespace_with_namespace(resource, namespace.as_str()).into()
            }
            _ => NamespaceUtil::without_namespace(resource).into(),
        }
    }

    /// Wraps the consumer group with namespace before startup, matching Java LitePull start().
    fn consumer_group_with_namespace(&self) -> CheetahString {
        self.client_config
            .mut_from_ref()
            .with_namespace(self.consumer_config.consumer_group.clone())
    }

    /// Wraps a message queue with namespace.
    fn queue_with_namespace(&self, mq: &MessageQueue) -> MessageQueue {
        self.client_config.mut_from_ref().queue_with_namespace(mq.clone())
    }

    /// Removes namespace from a message queue.
    fn queue_without_namespace(&self, mq: &MessageQueue) -> MessageQueue {
        match self.client_config.mut_from_ref().get_namespace() {
            Some(namespace) if !namespace.is_empty() => {
                let unwrapped_topic = NamespaceUtil::without_namespace_with_namespace(mq.topic(), namespace.as_str());
                MessageQueue::from_parts(unwrapped_topic, mq.broker_name().clone(), mq.queue_id())
            }
            Some(_) | None => mq.clone(),
        }
    }

    /// Initializes the trace dispatcher if message trace is enabled.
    async fn init_trace_dispatcher_internal(&self, impl_: &ArcMut<DefaultLitePullConsumerImpl>) -> RocketMQResult<()> {
        if !self.enable_msg_trace {
            return Ok(());
        }

        let dispatcher_arc = {
            let mut dispatcher_guard = self.trace_dispatcher.write().await;
            if let Some(dispatcher) = dispatcher_guard.as_ref() {
                dispatcher.clone()
            } else {
                let trace_topic = self
                    .custom_trace_topic
                    .as_ref()
                    .map(|t| t.as_str())
                    .unwrap_or(TopicValidator::RMQ_SYS_TRACE_TOPIC);

                let dispatcher = AsyncTraceDispatcher::new(
                    self.consumer_config.consumer_group.as_str(),
                    Type::Consume,
                    self.client_config.trace_msg_batch_num,
                    trace_topic,
                    self.rpc_hook.clone(),
                );
                dispatcher.set_namespace_v2(self.client_config.namespace_v2.clone());
                dispatcher.set_use_tls(self.client_config.use_tls);

                let dispatcher_arc: Arc<dyn TraceDispatcher + Send + Sync> = Arc::new(dispatcher);
                *dispatcher_guard = Some(dispatcher_arc.clone());
                dispatcher_arc
            }
        };

        let hook = Arc::new(ConsumeMessageTraceHookImpl::new(dispatcher_arc));
        impl_.mut_from_ref().register_consume_message_hook(hook);

        Ok(())
    }

    /// Starts the configured trace dispatcher after the consumer implementation is running.
    async fn start_trace_dispatcher(&self) {
        if !self.enable_msg_trace {
            return;
        }

        let dispatcher = self.trace_dispatcher.read().await.as_ref().cloned();
        let Some(dispatcher) = dispatcher else {
            return;
        };

        let name_server_addr = self
            .client_config
            .namesrv_addr
            .as_ref()
            .map(|addr| addr.as_str())
            .unwrap_or_default();
        if let Err(error) = dispatcher.start(name_server_addr, self.client_config.access_channel) {
            tracing::warn!("trace dispatcher start failed: {}", error);
        }
    }

    /// Returns a reference to the internal implementation after it has been initialized.
    fn try_impl_(&self) -> RocketMQResult<&ArcMut<DefaultLitePullConsumerImpl>> {
        self.default_lite_pull_consumer_impl
            .get()
            .ok_or_else(|| RocketMQError::not_initialized("DefaultLitePullConsumer not started. Call start() first."))
    }

    /// Returns the internal implementation, initializing it without starting network services.
    ///
    /// Java's LitePull consumer allows `subscribe`, `assign`, and
    /// `setSubExpressionForAssign` while the implementation is still in
    /// `CREATE_JUST`. Those calls populate local subscription state and only
    /// trigger broker route updates after `start()`.
    async fn get_or_init_impl(&self) -> RocketMQResult<&ArcMut<DefaultLitePullConsumerImpl>> {
        self.default_lite_pull_consumer_impl
            .get_or_try_init(|| async {
                let mut impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
                    self.client_config.clone(),
                    self.consumer_config.clone(),
                ));
                let wrapper = impl_.clone();
                impl_.set_default_lite_pull_consumer_impl(wrapper);
                impl_.set_rpc_hook(self.rpc_hook.clone());
                impl_.set_message_queue_listener(self.current_message_queue_listener());

                self.init_trace_dispatcher_internal(&impl_).await?;

                let consumer_group = self.consumer_group_with_namespace();
                self.set_consumer_group(consumer_group.clone())?;
                impl_.set_consumer_group(consumer_group)?;
                if let Some(offset_store) = self.current_offset_store() {
                    impl_.mut_from_ref().set_offset_store(Some(offset_store))?;
                }

                Ok::<ArcMut<DefaultLitePullConsumerImpl>, rocketmq_error::RocketMQError>(impl_)
            })
            .await
    }

    fn current_message_queue_listener(&self) -> Option<ArcMessageQueueListener> {
        self.message_queue_listener
            .read()
            .ok()
            .and_then(|listener| listener.clone())
    }

    fn set_message_queue_listener_local(&self, listener: Option<ArcMessageQueueListener>) {
        match self.message_queue_listener.write() {
            Ok(mut current) => *current = listener,
            Err(error) => tracing::warn!("LitePull message queue listener lock poisoned: {}", error),
        }
    }

    fn current_offset_store(&self) -> Option<Arc<OffsetStore>> {
        self.offset_store.read().ok().and_then(|store| store.clone())
    }

    fn set_offset_store_local(&self, offset_store: Option<Arc<OffsetStore>>) {
        match self.offset_store.write() {
            Ok(mut current) => *current = offset_store,
            Err(error) => tracing::warn!("LitePull offset store lock poisoned: {}", error),
        }
    }
}

#[allow(unused)]
impl LitePullConsumer for DefaultLitePullConsumer {
    async fn start(&self) -> RocketMQResult<()> {
        let impl_ = self.get_or_init_impl().await?;

        impl_.mut_from_ref().start().await?;
        self.set_offset_store_local(impl_.offset_store());
        self.start_trace_dispatcher().await;
        Ok(())
    }

    async fn shutdown(&self) {
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            let _ = impl_.mut_from_ref().shutdown().await;
        }

        // Shutdown trace dispatcher
        let dispatcher = { self.trace_dispatcher.write().await.take() };
        if let Some(dispatcher) = dispatcher {
            if let Some(async_dispatcher) = dispatcher.as_any().downcast_ref::<AsyncTraceDispatcher>() {
                async_dispatcher.shutdown_async().await;
            } else {
                dispatcher.shutdown();
            }
        }
    }

    async fn is_running(&self) -> bool {
        match self.default_lite_pull_consumer_impl.get() {
            Some(impl_) => impl_.is_running().await,
            None => false,
        }
    }

    async fn subscribe(&self, topic: &str) -> RocketMQResult<()> {
        self.subscribe_with_expression(topic, "*").await
    }

    async fn subscribe_with_expression(&self, topic: &str, sub_expression: &str) -> RocketMQResult<()> {
        let wrapped_topic = self.with_namespace(topic);
        self.get_or_init_impl()
            .await?
            .mut_from_ref()
            .subscribe(wrapped_topic, sub_expression)
            .await
    }

    async fn subscribe_with_listener<MQL>(&self, topic: &str, sub_expression: &str, listener: MQL) -> RocketMQResult<()>
    where
        MQL: MessageQueueListener + 'static,
    {
        let wrapped_topic = self.with_namespace(topic);
        let listener: ArcMessageQueueListener = Arc::new(listener);
        self.get_or_init_impl()
            .await?
            .mut_from_ref()
            .subscribe_with_listener_arc(wrapped_topic, sub_expression, listener.clone())
            .await?;
        self.set_message_queue_listener_local(Some(listener));
        Ok(())
    }

    async fn subscribe_with_selector(&self, topic: &str, selector: Option<MessageSelector>) -> RocketMQResult<()> {
        let wrapped_topic = self.with_namespace(topic);
        self.get_or_init_impl()
            .await?
            .mut_from_ref()
            .subscribe_with_selector(wrapped_topic, selector)
            .await
    }

    async fn unsubscribe(&self, topic: &str) {
        let wrapped_topic = self.with_namespace(topic);
        match self.try_impl_() {
            Ok(impl_) => {
                if let Err(e) = impl_.mut_from_ref().unsubscribe(wrapped_topic).await {
                    tracing::warn!(topic = %topic, error = %e, "unsubscribe failed");
                }
            }
            Err(e) => tracing::warn!(topic = %topic, error = %e, "unsubscribe failed"),
        }
    }

    async fn assignment(&self) -> RocketMQResult<HashSet<MessageQueue>> {
        let assignment = self.try_impl_()?.assignment().await;

        // Remove namespace from all queues
        let mut result = HashSet::with_capacity(assignment.len());
        for mq in assignment {
            result.insert(self.queue_without_namespace(&mq));
        }

        Ok(result)
    }

    async fn assign(&self, message_queues: Vec<MessageQueue>) -> RocketMQResult<()> {
        // Wrap namespace for all queues
        let wrapped_queues: Vec<_> = message_queues.iter().map(|mq| self.queue_with_namespace(mq)).collect();

        self.get_or_init_impl()
            .await?
            .mut_from_ref()
            .assign(wrapped_queues)
            .await
    }

    async fn set_sub_expression_for_assign(&self, topic: &str, sub_expression: &str) -> RocketMQResult<()> {
        let wrapped_topic = self.with_namespace(topic);
        self.get_or_init_impl()
            .await?
            .set_sub_expression_for_assign(wrapped_topic, sub_expression)
            .await
    }

    async fn build_subscriptions_for_heartbeat(
        &self,
        sub_expression_map: &mut HashMap<String, MessageSelector>,
    ) -> RocketMQResult<()> {
        self.try_impl_()?
            .build_subscriptions_for_heartbeat(sub_expression_map)
            .await
    }

    async fn subscriptions_for_heartbeat(&self) -> HashSet<SubscriptionData> {
        self.default_lite_pull_consumer_impl
            .get()
            .map_or_else(HashSet::new, |impl_| impl_.subscriptions_for_heartbeat())
    }

    /// Zero-copy implementation.
    async fn poll_zero_copy(&self) -> Vec<Arc<MessageExt>> {
        match self.try_impl_() {
            Ok(impl_) => match impl_.poll(self.consumer_config.poll_timeout_millis).await {
                Ok(messages) => messages,
                Err(e) => {
                    tracing::error!(error = %e, "poll failed");
                    Vec::new()
                }
            },
            Err(e) => {
                tracing::error!(error = %e, "poll failed");
                Vec::new()
            }
        }
    }

    /// Zero-copy implementation with custom timeout.
    async fn poll_with_timeout_zero_copy(&self, timeout: u64) -> Vec<Arc<MessageExt>> {
        match self.try_impl_() {
            Ok(impl_) => match impl_.poll_with_timeout(timeout).await {
                Ok(messages) => messages,
                Err(e) => {
                    tracing::error!(timeout_ms = timeout, error = %e, "poll failed");
                    Vec::new()
                }
            },
            Err(e) => {
                tracing::error!(timeout_ms = timeout, error = %e, "poll failed");
                Vec::new()
            }
        }
    }

    /// Delegates to zero-copy implementation and creates owned message copies.
    async fn poll(&self) -> Vec<MessageExt> {
        self.poll_zero_copy()
            .await
            .into_iter()
            .map(|arc_mut| (*arc_mut).clone())
            .collect()
    }

    /// Delegates to zero-copy implementation with timeout and creates owned message copies.
    async fn poll_with_timeout(&self, timeout: u64) -> Vec<MessageExt> {
        self.poll_with_timeout_zero_copy(timeout)
            .await
            .into_iter()
            .map(|arc_mut| (*arc_mut).clone())
            .collect()
    }

    async fn message_model(&self) -> MessageModel {
        self.consumer_config.message_model
    }

    async fn set_message_model(&self, message_model: MessageModel) {
        self.consumer_config.mut_from_ref().message_model = message_model;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_message_model(message_model);
        }
    }

    async fn consume_from_where(&self) -> ConsumeFromWhere {
        self.consumer_config.consume_from_where
    }

    async fn set_consume_from_where(&self, consume_from_where: ConsumeFromWhere) -> RocketMQResult<()> {
        validate_lite_pull_consume_from_where(consume_from_where)?;
        self.consumer_config.mut_from_ref().consume_from_where = consume_from_where;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_consume_from_where(consume_from_where)?;
        }
        Ok(())
    }

    async fn consume_timestamp(&self) -> Option<CheetahString> {
        self.consumer_config.consume_timestamp.clone()
    }

    async fn set_consume_timestamp(&self, consume_timestamp: Option<CheetahString>) {
        self.consumer_config.mut_from_ref().consume_timestamp = consume_timestamp.clone();
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_consume_timestamp(consume_timestamp);
        }
    }

    async fn allocate_message_queue_strategy(&self) -> Arc<dyn AllocateMessageQueueStrategy + Send + Sync> {
        self.default_lite_pull_consumer_impl.get().map_or_else(
            || self.consumer_config.allocate_message_queue_strategy.clone(),
            |impl_| impl_.allocate_message_queue_strategy(),
        )
    }

    async fn set_allocate_message_queue_strategy(
        &self,
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy + Send + Sync>,
    ) {
        self.consumer_config.mut_from_ref().allocate_message_queue_strategy = allocate_message_queue_strategy.clone();
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_allocate_message_queue_strategy(allocate_message_queue_strategy);
        }
    }

    async fn message_queue_listener(&self) -> Option<ArcMessageQueueListener> {
        self.default_lite_pull_consumer_impl
            .get()
            .and_then(|impl_| impl_.message_queue_listener())
            .or_else(|| self.current_message_queue_listener())
    }

    async fn set_message_queue_listener(&self, message_queue_listener: Option<ArcMessageQueueListener>) {
        self.set_message_queue_listener_local(message_queue_listener.clone());
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_message_queue_listener(message_queue_listener);
        }
    }

    async fn offset_store(&self) -> Option<Arc<OffsetStore>> {
        self.default_lite_pull_consumer_impl
            .get()
            .and_then(|impl_| impl_.offset_store())
            .or_else(|| self.current_offset_store())
    }

    async fn set_offset_store(&self, offset_store: Option<Arc<OffsetStore>>) -> RocketMQResult<()> {
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.mut_from_ref().set_offset_store(offset_store.clone())?;
        }
        self.set_offset_store_local(offset_store);
        Ok(())
    }

    async fn topic_metadata_check_interval_millis(&self) -> u64 {
        self.consumer_config.topic_metadata_check_interval_millis
    }

    async fn set_topic_metadata_check_interval_millis(&self, interval_millis: u64) {
        self.consumer_config.mut_from_ref().topic_metadata_check_interval_millis = interval_millis;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_topic_metadata_check_interval_millis(interval_millis);
        }
    }

    async fn poll_timeout_millis(&self) -> u64 {
        self.consumer_config.poll_timeout_millis
    }

    async fn set_poll_timeout_millis(&self, timeout_millis: u64) {
        self.consumer_config.mut_from_ref().poll_timeout_millis = timeout_millis;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_poll_timeout_millis(timeout_millis);
        }
    }

    async fn broker_suspend_max_time_millis(&self) -> u64 {
        self.consumer_config.broker_suspend_max_time_millis
    }

    async fn set_broker_suspend_max_time_millis(&self, timeout_millis: u64) {
        self.consumer_config.mut_from_ref().broker_suspend_max_time_millis = timeout_millis;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_broker_suspend_max_time_millis(timeout_millis);
        }
    }

    async fn consumer_timeout_millis_when_suspend(&self) -> u64 {
        self.consumer_config.consumer_timeout_millis_when_suspend
    }

    async fn set_consumer_timeout_millis_when_suspend(&self, timeout_millis: u64) {
        self.consumer_config.mut_from_ref().consumer_timeout_millis_when_suspend = timeout_millis;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_consumer_timeout_millis_when_suspend(timeout_millis);
        }
    }

    async fn consumer_pull_timeout_millis(&self) -> u64 {
        self.consumer_config.consumer_pull_timeout_millis
    }

    async fn set_consumer_pull_timeout_millis(&self, timeout_millis: u64) {
        self.consumer_config.mut_from_ref().consumer_pull_timeout_millis = timeout_millis;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_consumer_pull_timeout_millis(timeout_millis);
        }
    }

    async fn pull_batch_size(&self) -> i32 {
        self.consumer_config.pull_batch_size
    }

    async fn set_pull_batch_size(&self, pull_batch_size: i32) {
        self.consumer_config.mut_from_ref().pull_batch_size = pull_batch_size;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_pull_batch_size(pull_batch_size);
        }
    }

    async fn pull_thread_nums(&self) -> usize {
        self.consumer_config.pull_thread_nums
    }

    async fn set_pull_thread_nums(&self, pull_thread_nums: usize) {
        self.consumer_config.mut_from_ref().pull_thread_nums = pull_thread_nums;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_pull_thread_nums(pull_thread_nums);
        }
    }

    async fn pull_threshold_for_all(&self) -> i64 {
        self.consumer_config.pull_threshold_for_all
    }

    async fn set_pull_threshold_for_all(&self, pull_threshold_for_all: i64) {
        self.consumer_config.mut_from_ref().pull_threshold_for_all = pull_threshold_for_all;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_pull_threshold_for_all(pull_threshold_for_all);
        }
    }

    async fn pull_threshold_for_queue(&self) -> i64 {
        self.consumer_config.pull_threshold_for_queue
    }

    async fn set_pull_threshold_for_queue(&self, pull_threshold_for_queue: i64) {
        self.consumer_config.mut_from_ref().pull_threshold_for_queue = pull_threshold_for_queue;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_pull_threshold_for_queue(pull_threshold_for_queue);
        }
    }

    async fn pull_threshold_size_for_queue(&self) -> i32 {
        self.consumer_config.pull_threshold_size_for_queue
    }

    async fn set_pull_threshold_size_for_queue(&self, pull_threshold_size_for_queue: i32) {
        self.consumer_config.mut_from_ref().pull_threshold_size_for_queue = pull_threshold_size_for_queue;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_pull_threshold_size_for_queue(pull_threshold_size_for_queue);
        }
    }

    async fn consume_max_span(&self) -> i64 {
        self.consumer_config.consume_max_span
    }

    async fn set_consume_max_span(&self, consume_max_span: i64) {
        self.consumer_config.mut_from_ref().consume_max_span = consume_max_span;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_consume_max_span(consume_max_span);
        }
    }

    async fn is_connect_broker_by_user(&self) -> bool {
        self.default_lite_pull_consumer_impl
            .get()
            .map_or(self.consumer_config.connect_broker_by_user, |impl_| {
                impl_.is_connect_broker_by_user()
            })
    }

    async fn set_connect_broker_by_user(&self, connect_broker_by_user: bool) {
        self.consumer_config.mut_from_ref().connect_broker_by_user = connect_broker_by_user;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_connect_broker_by_user(connect_broker_by_user);
        }
    }

    async fn default_broker_id(&self) -> u64 {
        self.default_lite_pull_consumer_impl
            .get()
            .map_or(self.consumer_config.default_broker_id, |impl_| {
                impl_.default_broker_id()
            })
    }

    async fn set_default_broker_id(&self, broker_id: u64) {
        self.consumer_config.mut_from_ref().default_broker_id = broker_id;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_default_broker_id(broker_id);
        }
    }

    async fn fetch_message_queues(&self, topic: &str) -> RocketMQResult<Vec<MessageQueue>> {
        let wrapped_topic = self.with_namespace(topic);
        let queues = self.try_impl_()?.fetch_message_queues(wrapped_topic).await?;

        // Remove namespace from all queues and convert to Vec
        let result: Vec<_> = queues.into_iter().map(|mq| self.queue_without_namespace(&mq)).collect();

        Ok(result)
    }

    async fn committed(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.try_impl_()?.committed(&wrapped_mq).await
    }

    async fn commit_all(&self) -> RocketMQResult<()> {
        self.try_impl_()?.commit_all().await
    }

    async fn commit_with_map(&self, offset_map: HashMap<MessageQueue, i64>, persist: bool) {
        // Wrap namespace for all queues
        let mut wrapped_offsets = HashMap::with_capacity(offset_map.len());
        for (mq, offset) in offset_map {
            wrapped_offsets.insert(self.queue_with_namespace(&mq), offset);
        }

        match self.try_impl_() {
            Ok(impl_) => {
                if let Err(e) = impl_.commit(wrapped_offsets, persist).await {
                    tracing::warn!("commit_with_map failed: {}", e);
                }
            }
            Err(e) => tracing::warn!("commit_with_map failed: {}", e),
        }
    }

    async fn commit(&self) {
        if let Err(e) = self.commit_all().await {
            tracing::warn!("commit failed: {}", e);
        }
    }

    async fn commit_with_set(&self, message_queues: HashSet<MessageQueue>, persist: bool) {
        // Wrap namespace for all queues
        let wrapped_queues: HashSet<_> = message_queues.iter().map(|mq| self.queue_with_namespace(mq)).collect();
        let impl_ = match self.try_impl_() {
            Ok(impl_) => impl_,
            Err(e) => {
                tracing::warn!("commit_with_set failed: {}", e);
                return;
            }
        };

        if let Err(e) = impl_.commit_message_queues(&wrapped_queues, persist).await {
            tracing::warn!("commit_with_set failed: {}", e);
        }
    }

    async fn commit_sync(&self) {
        // Commit all offsets (trait note: misleading name, doesn't actually block)
        if let Err(e) = self.commit_all().await {
            tracing::warn!("commit_sync failed: {}", e);
        }
    }

    async fn commit_sync_with_map(&self, offset_map: HashMap<MessageQueue, i64>, persist: bool) {
        // Commit specific offsets (trait note: misleading name, doesn't actually block)
        self.commit_with_map(offset_map, persist).await;
    }

    async fn is_auto_commit(&self) -> bool {
        self.consumer_config.auto_commit
    }

    async fn set_auto_commit(&self, auto_commit: bool) {
        self.consumer_config.mut_from_ref().auto_commit = auto_commit;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_auto_commit(auto_commit);
        }
    }

    async fn is_unit_mode(&self) -> bool {
        self.consumer_config.unit_mode
    }

    async fn set_unit_mode(&self, unit_mode: bool) {
        self.consumer_config.mut_from_ref().unit_mode = unit_mode;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_unit_mode(unit_mode);
        }
    }

    async fn auto_commit_interval_millis(&self) -> u64 {
        self.consumer_config.auto_commit_interval_millis
    }

    async fn set_auto_commit_interval_millis(&self, interval_millis: u64) {
        if interval_millis < MIN_AUTOCOMMIT_INTERVAL_MILLIS {
            return;
        }
        self.consumer_config.mut_from_ref().auto_commit_interval_millis = interval_millis;
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            impl_.set_auto_commit_interval_millis(interval_millis);
        }
    }

    async fn offset_for_timestamp(&self, message_queue: &MessageQueue, timestamp: u64) -> RocketMQResult<i64> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.try_impl_()?.offset_for_timestamp(&wrapped_mq, timestamp).await
    }

    async fn earliest_msg_store_time(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.try_impl_()?.earliest_msg_store_time(&wrapped_mq).await
    }

    async fn max_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.try_impl_()?.max_offset_public(&wrapped_mq).await
    }

    async fn min_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.try_impl_()?.min_offset_public(&wrapped_mq).await
    }

    async fn pause(&self, message_queues: Vec<MessageQueue>) {
        let wrapped_queues: Vec<_> = message_queues.iter().map(|mq| self.queue_with_namespace(mq)).collect();

        match self.try_impl_() {
            Ok(impl_) => impl_.pause(&wrapped_queues).await,
            Err(e) => tracing::warn!("pause failed: {}", e),
        }
    }

    async fn resume(&self, message_queues: Vec<MessageQueue>) {
        let wrapped_queues: Vec<_> = message_queues.iter().map(|mq| self.queue_with_namespace(mq)).collect();

        match self.try_impl_() {
            Ok(impl_) => impl_.resume(&wrapped_queues).await,
            Err(e) => tracing::warn!("resume failed: {}", e),
        }
    }

    async fn is_paused(&self, message_queue: &MessageQueue) -> bool {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        match self.try_impl_() {
            Ok(impl_) => impl_.is_paused(&wrapped_mq).await,
            Err(e) => {
                tracing::warn!("is_paused failed: {}", e);
                false
            }
        }
    }

    async fn seek(&self, message_queue: &MessageQueue, offset: i64) -> RocketMQResult<()> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.try_impl_()?.seek(&wrapped_mq, offset).await
    }

    async fn seek_to_begin(&self, message_queue: &MessageQueue) -> RocketMQResult<()> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.try_impl_()?.seek_to_begin(&wrapped_mq).await
    }

    async fn seek_to_end(&self, message_queue: &MessageQueue) -> RocketMQResult<()> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.try_impl_()?.seek_to_end(&wrapped_mq).await
    }

    async fn update_name_server_address(&self, name_server_address: &str) {
        self.client_config
            .mut_from_ref()
            .set_namesrv_addr(CheetahString::from_slice(name_server_address));

        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            let addresses: Vec<String> = name_server_address
                .split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            impl_.update_name_server_address(addresses).await;
        }
    }

    async fn register_topic_message_queue_change_listener<TL>(&self, topic: &str, listener: TL) -> RocketMQResult<()>
    where
        TL: TopicMessageQueueChangeListener + 'static,
    {
        let wrapped_topic = self.with_namespace(topic);
        let listener_arc: Arc<dyn TopicMessageQueueChangeListener + Send + Sync> = Arc::new(listener);
        self.try_impl_()?
            .register_topic_message_queue_change_listener(wrapped_topic, listener_arc)
            .await
    }
}

impl MQConsumer for DefaultLitePullConsumer {
    async fn create_topic(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        attributes: HashMap<String, String>,
    ) -> RocketMQResult<()> {
        DefaultLitePullConsumer::create_topic(self, key, new_topic, queue_num, attributes).await
    }

    async fn create_topic_with_flag(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> RocketMQResult<()> {
        DefaultLitePullConsumer::create_topic_with_flag(self, key, new_topic, queue_num, topic_sys_flag, attributes)
            .await
    }

    async fn query_message(
        &mut self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> RocketMQResult<QueryResult> {
        DefaultLitePullConsumer::query_message(self, topic, key, max_num, begin, end).await
    }

    async fn view_message(&mut self, topic: &str, msg_id: &str) -> RocketMQResult<MessageExt> {
        DefaultLitePullConsumer::view_message(self, topic, msg_id).await
    }

    async fn send_message_back(&mut self, msg: MessageExt, delay_level: i32, broker_name: &str) -> RocketMQResult<()> {
        // Lite pull consumer doesn't support send message back
        // This is typically used in push consumer for retry
        Err(crate::mq_client_err!(
            -1,
            "sendMessageBack is not supported in lite pull consumer"
        ))
    }

    async fn fetch_subscribe_message_queues(&mut self, topic: &str) -> RocketMQResult<Vec<MessageQueue>> {
        let queues = self.fetch_message_queues(topic).await?;
        Ok(queues.into_iter().collect())
    }

    async fn search_offset(&mut self, mq: &MessageQueue, timestamp: u64) -> RocketMQResult<i64> {
        <Self as LitePullConsumer>::offset_for_timestamp(self, mq, timestamp).await
    }

    async fn max_offset(&mut self, mq: &MessageQueue) -> RocketMQResult<i64> {
        <Self as LitePullConsumer>::max_offset(self, mq).await
    }

    async fn min_offset(&mut self, mq: &MessageQueue) -> RocketMQResult<i64> {
        <Self as LitePullConsumer>::min_offset(self, mq).await
    }

    async fn earliest_msg_store_time(&mut self, mq: &MessageQueue) -> RocketMQResult<i64> {
        <Self as LitePullConsumer>::earliest_msg_store_time(self, mq).await
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::base::access_channel::AccessChannel;
    use crate::common::acl_client_rpc_hook::AclClientRPCHook;
    use crate::common::session_credentials::SessionCredentials;
    use crate::consumer::mq_consumer_inner::MQConsumerInner;
    use crate::consumer::rebalance_strategy::allocate_message_queue_averagely_by_circle::AllocateMessageQueueAveragelyByCircle;
    use crate::consumer::store::read_offset_type::ReadOffsetType;

    #[derive(Default)]
    struct CapturingTraceDispatcher {
        start_count: AtomicUsize,
        shutdown_count: AtomicUsize,
        append_count: AtomicUsize,
        last_name_srv_addr: StdMutex<Option<String>>,
        last_access_channel: StdMutex<Option<AccessChannel>>,
    }

    impl TraceDispatcher for CapturingTraceDispatcher {
        fn start(&self, name_srv_addr: &str, access_channel: AccessChannel) -> RocketMQResult<()> {
            self.start_count.fetch_add(1, Ordering::SeqCst);
            *self.last_name_srv_addr.lock().expect("name server lock") = Some(name_srv_addr.to_string());
            *self.last_access_channel.lock().expect("access channel lock") = Some(access_channel);
            Ok(())
        }

        fn append(&self, _ctx: &dyn Any) -> bool {
            self.append_count.fetch_add(1, Ordering::SeqCst);
            true
        }

        fn flush(&self) -> RocketMQResult<()> {
            Ok(())
        }

        fn shutdown(&self) {
            self.shutdown_count.fetch_add(1, Ordering::SeqCst);
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_mut_any(&mut self) -> &mut dyn Any {
            self
        }
    }

    struct NoopMessageQueueListener;

    impl MessageQueueListener for NoopMessageQueueListener {
        fn message_queue_changed(
            &self,
            _topic: &str,
            _mq_all: &HashSet<MessageQueue>,
            _mq_assigned: &HashSet<MessageQueue>,
        ) {
        }
    }

    fn new_unstarted_consumer() -> DefaultLitePullConsumer {
        DefaultLitePullConsumer::builder()
            .consumer_group("lite_pull_group")
            .build()
            .expect("builder should create consumer")
    }

    fn new_namespaced_consumer_with_impl() -> (DefaultLitePullConsumer, ArcMut<DefaultLitePullConsumerImpl>) {
        let consumer = DefaultLitePullConsumer::builder()
            .consumer_group("lite_pull_namespace_group")
            .namespace("ns")
            .build()
            .expect("builder should create namespaced consumer");
        let mut impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
            consumer.client_config.clone(),
            consumer.consumer_config.clone(),
        ));
        let wrapper = impl_.clone();
        impl_.set_default_lite_pull_consumer_impl(wrapper);
        if consumer.default_lite_pull_consumer_impl.set(impl_.clone()).is_err() {
            panic!("test consumer impl should be set once");
        }

        (consumer, impl_)
    }

    #[tokio::test]
    async fn assignment_before_start_returns_not_initialized_error() {
        let consumer = new_unstarted_consumer();

        let error = consumer
            .assignment()
            .await
            .expect_err("assignment before start should fail");

        assert!(error.to_string().contains("DefaultLitePullConsumer not started"));
    }

    #[tokio::test]
    async fn consumer_running_info_before_start_returns_not_initialized_error() {
        let consumer = new_unstarted_consumer();

        let error = consumer
            .consumer_running_info()
            .await
            .expect_err("running info before start should fail");

        assert!(error.to_string().contains("DefaultLitePullConsumer not started"));
    }

    #[tokio::test]
    async fn subscribe_before_start_initializes_impl_and_records_subscription_like_java() {
        let consumer = DefaultLitePullConsumer::builder()
            .consumer_group("lite_pull_pre_start_subscribe_group")
            .namespace("ns")
            .build()
            .expect("builder should create consumer");

        consumer
            .subscribe_with_expression("TopicPreStart", "TagA")
            .await
            .expect("LitePull should accept subscribe before start like Java");

        assert!(!consumer.is_running().await);
        let impl_ = consumer
            .try_impl_()
            .expect("pre-start subscribe should initialize the implementation");
        let subscriptions = impl_.subscriptions();
        let subscription = subscriptions
            .iter()
            .find(|subscription| subscription.topic.as_str() == "ns%TopicPreStart")
            .expect("pre-start subscribe should be stored in rebalance subscriptions");

        assert_eq!(subscription.sub_string.as_str(), "TagA");
    }

    #[tokio::test]
    async fn initialization_propagates_rpc_hook_to_impl() {
        let rpc_hook = Arc::new(AclClientRPCHook::new(SessionCredentials::with_keys(
            "access_key",
            "secret_key",
        )));
        let consumer = DefaultLitePullConsumer::builder()
            .consumer_group("lite_pull_rpc_hook_group")
            .rpc_hook(rpc_hook)
            .build()
            .expect("builder should create consumer with RPC hook");

        let impl_ = consumer
            .get_or_init_impl()
            .await
            .expect("impl initialization should keep RPC hook");

        assert!(impl_.has_rpc_hook(), "LitePull impl should retain builder RPC hook");
    }

    #[tokio::test]
    async fn mq_consumer_offset_facade_delegates_to_lite_pull_impl_path() {
        let mut consumer = new_unstarted_consumer();
        let mq = MessageQueue::from_parts("TopicLite", "broker-a", 0);

        let search_error = <DefaultLitePullConsumer as MQConsumer>::search_offset(&mut consumer, &mq, 1)
            .await
            .expect_err("searchOffset should enter LitePull path and fail because the consumer is not started");
        let max_error = <DefaultLitePullConsumer as MQConsumer>::max_offset(&mut consumer, &mq)
            .await
            .expect_err("maxOffset should enter LitePull path and fail because the consumer is not started");
        let min_error = <DefaultLitePullConsumer as MQConsumer>::min_offset(&mut consumer, &mq)
            .await
            .expect_err("minOffset should enter LitePull path and fail because the consumer is not started");
        let earliest_error = <DefaultLitePullConsumer as MQConsumer>::earliest_msg_store_time(&mut consumer, &mq)
            .await
            .expect_err("earliestMsgStoreTime should enter LitePull path and fail because the consumer is not started");

        for error in [search_error, max_error, min_error, earliest_error] {
            let message = error.to_string();
            assert!(message.contains("DefaultLitePullConsumer not started"));
            assert!(!message.contains("not supported by this MQConsumer implementation"));
        }
    }

    #[tokio::test]
    async fn mq_consumer_admin_facade_delegates_to_lite_pull_impl_path() {
        let mut consumer = new_unstarted_consumer();

        let create_error = <DefaultLitePullConsumer as MQConsumer>::create_topic(
            &mut consumer,
            TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
            "TopicLiteAdmin",
            1,
            HashMap::new(),
        )
        .await
        .expect_err("createTopic should enter LitePull path and fail because the consumer is not started");
        let create_with_flag_error = <DefaultLitePullConsumer as MQConsumer>::create_topic_with_flag(
            &mut consumer,
            TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
            "TopicLiteAdmin",
            1,
            0,
            HashMap::new(),
        )
        .await
        .expect_err("createTopicWithFlag should enter LitePull path and fail because the consumer is not started");
        let query_error =
            <DefaultLitePullConsumer as MQConsumer>::query_message(&mut consumer, "TopicLiteAdmin", "key", 32, 0, 1)
                .await
                .expect_err("queryMessage should enter LitePull path and fail because the consumer is not started");
        let view_error =
            <DefaultLitePullConsumer as MQConsumer>::view_message(&mut consumer, "TopicLiteAdmin", "not-a-msg-id")
                .await
                .expect_err("viewMessage should enter LitePull path and fail because the consumer is not started");

        for error in [create_error, create_with_flag_error, query_error, view_error] {
            let message = error.to_string();
            assert!(message.contains("DefaultLitePullConsumer not started"));
            assert!(!message.contains("not supported by this MQConsumer implementation"));
        }
    }

    #[tokio::test]
    async fn consumer_running_info_facade_returns_lite_pull_snapshot() {
        let (consumer, _impl_) = new_namespaced_consumer_with_impl();
        consumer
            .subscribe_with_expression("TopicLiteRunningInfo", "TagA")
            .await
            .expect("subscribe should update running-info subscriptions");
        consumer.set_pull_batch_size(17).await;
        consumer.set_auto_commit(false).await;
        consumer.set_auto_commit_interval_millis(2_000).await;

        let info = consumer
            .consumer_running_info()
            .await
            .expect("running info facade should delegate to impl");

        assert_eq!(
            info.properties
                .get(ConsumerRunningInfo::PROP_CONSUME_TYPE)
                .map(String::as_str),
            Some("CONSUME_ACTIVELY")
        );
        assert_eq!(
            info.properties.get("consumerGroup").map(String::as_str),
            Some("lite_pull_namespace_group")
        );
        assert_eq!(info.properties.get("pullBatchSize").map(String::as_str), Some("17"));
        assert_eq!(info.properties.get("autoCommit").map(String::as_str), Some("false"));
        assert_eq!(
            info.properties.get("autoCommitIntervalMillis").map(String::as_str),
            Some("2000")
        );
        assert!(info
            .subscription_set
            .iter()
            .any(|subscription| subscription.topic.as_str() == "ns%TopicLiteRunningInfo"
                && subscription.sub_string.as_str() == "TagA"));
    }

    #[tokio::test]
    async fn assign_surfaces_impl_errors_like_java() {
        let (consumer, _impl_) = new_namespaced_consumer_with_impl();

        let error = consumer
            .assign(Vec::new())
            .await
            .expect_err("empty manual assignment should be rejected");

        assert!(error.to_string().contains("Message queues can not be null or empty."));
    }

    #[tokio::test]
    async fn offset_metadata_queries_before_start_return_not_initialized_error() {
        let consumer = new_unstarted_consumer();
        let queue = MessageQueue::from_parts("TopicA", "broker-a", 0);

        let earliest_error = consumer
            .earliest_msg_store_time(&queue)
            .await
            .expect_err("earliest store time before start should fail");
        let max_error = consumer
            .max_offset(&queue)
            .await
            .expect_err("max offset before start should fail");
        let min_error = consumer
            .min_offset(&queue)
            .await
            .expect_err("min offset before start should fail");

        assert!(earliest_error
            .to_string()
            .contains("DefaultLitePullConsumer not started"));
        assert!(max_error.to_string().contains("DefaultLitePullConsumer not started"));
        assert!(min_error.to_string().contains("DefaultLitePullConsumer not started"));
    }

    #[tokio::test]
    async fn poll_before_start_returns_empty_vec_without_panic() {
        let consumer = new_unstarted_consumer();

        assert!(consumer.poll_zero_copy().await.is_empty());
        assert!(consumer.poll_with_timeout_zero_copy(1).await.is_empty());
    }

    #[tokio::test]
    async fn lite_pull_facade_namespace_subscribe_wraps_builder_namespace_like_java() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert_eq!(consumer.namespace().as_deref(), Some("ns"));
        consumer
            .subscribe_with_expression("TopicA", "TagA")
            .await
            .expect("subscribe should succeed with injected impl");

        let subscriptions = impl_.subscriptions();
        let subscription = subscriptions
            .iter()
            .find(|subscription| subscription.topic.as_str() == "ns%TopicA")
            .expect("namespaced topic should be subscribed");
        assert_eq!(subscription.sub_string.as_str(), "TagA");
        assert!(!subscriptions
            .iter()
            .any(|subscription| subscription.topic.as_str() == "TopicA"));
    }

    #[tokio::test]
    async fn subscriptions_for_heartbeat_returns_current_rebalance_subscriptions_like_java_getter() {
        let (consumer, _impl_) = new_namespaced_consumer_with_impl();

        assert!(consumer.subscriptions_for_heartbeat().await.is_empty());

        consumer
            .subscribe_with_expression("TopicA", "TagA")
            .await
            .expect("subscribe should succeed with injected impl");

        let subscriptions = consumer.subscriptions_for_heartbeat().await;
        assert_eq!(subscriptions.len(), 1);
        let subscription = subscriptions.iter().next().expect("subscription should exist");
        assert_eq!(subscription.topic.as_str(), "ns%TopicA");
        assert_eq!(subscription.sub_string.as_str(), "TagA");
    }

    #[tokio::test]
    async fn lite_pull_java_getter_aliases_delegate_to_config() {
        let consumer = new_unstarted_consumer();

        assert_eq!(consumer.get_message_model().await, consumer.message_model().await);
        assert_eq!(
            consumer.get_consume_from_where().await,
            consumer.consume_from_where().await
        );
        assert_eq!(
            consumer.get_consume_timestamp().await,
            consumer.consume_timestamp().await
        );
        assert_eq!(
            consumer.get_allocate_message_queue_strategy().await.get_name(),
            consumer.allocate_message_queue_strategy().await.get_name()
        );
        assert_eq!(
            consumer.get_offset_store().await.is_some(),
            consumer.offset_store().await.is_some()
        );
        assert_eq!(
            consumer.get_topic_metadata_check_interval_millis().await,
            consumer.topic_metadata_check_interval_millis().await
        );
        assert_eq!(
            consumer.get_poll_timeout_millis().await,
            consumer.poll_timeout_millis().await
        );
        assert_eq!(
            consumer.get_consumer_timeout_millis_when_suspend().await,
            consumer.consumer_timeout_millis_when_suspend().await
        );
        assert_eq!(
            consumer.get_consumer_pull_timeout_millis().await,
            consumer.consumer_pull_timeout_millis().await
        );
        assert_eq!(consumer.get_pull_batch_size().await, consumer.pull_batch_size().await);
        assert_eq!(
            consumer.get_pull_threshold_for_all().await,
            consumer.pull_threshold_for_all().await
        );
        assert_eq!(
            consumer.get_pull_threshold_for_queue().await,
            consumer.pull_threshold_for_queue().await
        );
        assert_eq!(
            consumer.get_pull_threshold_size_for_queue().await,
            consumer.pull_threshold_size_for_queue().await
        );
        assert_eq!(
            consumer.get_auto_commit_interval_millis().await,
            consumer.auto_commit_interval_millis().await
        );
    }

    #[tokio::test]
    async fn set_sub_expression_for_assign_surfaces_impl_errors_like_java() {
        let (consumer, _impl_) = new_namespaced_consumer_with_impl();

        let error = consumer
            .set_sub_expression_for_assign("TopicA", " ")
            .await
            .expect_err("blank assign filter should be rejected");
        assert!(error.to_string().contains("subExpression can not be null or empty."));

        consumer
            .set_sub_expression_for_assign("TopicA", "TagA")
            .await
            .expect("valid assign filter should be accepted before start");
    }

    #[tokio::test]
    async fn set_auto_commit_updates_initialized_impl_like_java_facade_reference() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert!(consumer.is_auto_commit().await);
        assert!(impl_.is_auto_commit());

        consumer.set_auto_commit(false).await;

        assert!(!consumer.is_auto_commit().await);
        assert!(!impl_.is_auto_commit());

        consumer.set_auto_commit(true).await;

        assert!(consumer.is_auto_commit().await);
        assert!(impl_.is_auto_commit());
    }

    #[tokio::test]
    async fn set_auto_commit_interval_updates_initialized_impl_and_ignores_invalid_like_java() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert_eq!(consumer.auto_commit_interval_millis().await, 5000);
        assert_eq!(impl_.auto_commit_interval_millis(), 5000);

        consumer.set_auto_commit_interval_millis(2000).await;

        assert_eq!(consumer.auto_commit_interval_millis().await, 2000);
        assert_eq!(impl_.auto_commit_interval_millis(), 2000);

        consumer
            .set_auto_commit_interval_millis(MIN_AUTOCOMMIT_INTERVAL_MILLIS - 1)
            .await;

        assert_eq!(consumer.auto_commit_interval_millis().await, 2000);
        assert_eq!(impl_.auto_commit_interval_millis(), 2000);
    }

    #[tokio::test]
    async fn set_unit_mode_updates_initialized_impl_and_heartbeat_source_like_java() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert!(!consumer.is_unit_mode().await);
        assert!(!impl_.is_unit_mode());
        assert!(!MQConsumerInner::is_unit_mode(impl_.as_ref()));

        consumer.set_unit_mode(true).await;

        assert!(consumer.is_unit_mode().await);
        assert!(impl_.is_unit_mode());
        assert!(MQConsumerInner::is_unit_mode(impl_.as_ref()));
    }

    #[test]
    fn set_consumer_group_updates_initialized_impl_and_rebalance_before_start_like_java() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        consumer
            .set_consumer_group("lite_pull_updated_group")
            .expect("consumer group should be mutable before start");

        assert_eq!(consumer.consumer_group().as_str(), "lite_pull_updated_group");
        assert_eq!(impl_.consumer_group_config().as_str(), "lite_pull_updated_group");
        assert_eq!(
            MQConsumerInner::group_name(impl_.as_ref()).as_str(),
            "lite_pull_updated_group"
        );
        assert_eq!(
            impl_.rebalance_consumer_group_name().as_deref(),
            Some("lite_pull_updated_group")
        );
    }

    #[test]
    fn start_namespace_wraps_consumer_group_like_java_lite_pull_start() {
        let (consumer, _impl_) = new_namespaced_consumer_with_impl();

        assert_eq!(
            consumer.consumer_group_with_namespace().as_str(),
            "ns%lite_pull_namespace_group"
        );

        consumer
            .set_consumer_group("ns%lite_pull_namespace_group")
            .expect("pre-wrapped group should be accepted before start");

        assert_eq!(
            consumer.consumer_group_with_namespace().as_str(),
            "ns%lite_pull_namespace_group"
        );
    }

    #[tokio::test]
    async fn set_message_queue_listener_updates_initialized_impl_like_java() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert!(consumer.message_queue_listener().await.is_none());
        assert!(impl_.message_queue_listener().is_none());

        let listener: ArcMessageQueueListener = Arc::new(NoopMessageQueueListener);
        consumer.set_message_queue_listener(Some(listener.clone())).await;

        let facade_listener = consumer
            .message_queue_listener()
            .await
            .expect("facade should return configured listener");
        let impl_listener = impl_
            .message_queue_listener()
            .expect("impl should receive configured listener");
        assert!(Arc::ptr_eq(&facade_listener, &listener));
        assert!(Arc::ptr_eq(&impl_listener, &listener));

        consumer.set_message_queue_listener(None).await;

        assert!(consumer.message_queue_listener().await.is_none());
        assert!(impl_.message_queue_listener().is_none());
    }

    #[tokio::test]
    async fn set_offset_store_updates_initialized_impl_and_rebalance_like_java() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();
        let queue = MessageQueue::from_parts("TopicA", "broker-a", 0);
        let offset_store = Arc::new(OffsetStore::new_test());

        assert!(consumer.offset_store().await.is_none());
        assert!(impl_.offset_store().is_none());
        assert!(!impl_.rebalance_has_offset_store());

        consumer
            .set_offset_store(Some(offset_store.clone()))
            .await
            .expect("offset store should be mutable before start");
        offset_store.update_offset(&queue, 42, false).await;

        let facade_store = consumer
            .offset_store()
            .await
            .expect("facade should expose configured offset store");
        let impl_store = impl_
            .offset_store()
            .expect("impl should receive configured offset store");
        assert_eq!(
            facade_store.read_offset(&queue, ReadOffsetType::ReadFromMemory).await,
            42
        );
        assert_eq!(impl_store.read_offset(&queue, ReadOffsetType::ReadFromMemory).await, 42);
        assert!(impl_.rebalance_has_offset_store());

        consumer
            .set_offset_store(None)
            .await
            .expect("offset store should be clearable before start");

        assert!(consumer.offset_store().await.is_none());
        assert!(impl_.offset_store().is_none());
        assert!(!impl_.rebalance_has_offset_store());
    }

    #[tokio::test]
    async fn set_allocate_message_queue_strategy_updates_initialized_impl_and_rebalance_like_java() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert_eq!(consumer.allocate_message_queue_strategy().await.get_name(), "AVG");
        assert_eq!(impl_.allocate_message_queue_strategy().get_name(), "AVG");
        assert_eq!(impl_.rebalance_allocate_message_queue_strategy_name(), None);

        consumer
            .set_allocate_message_queue_strategy(Arc::new(AllocateMessageQueueAveragelyByCircle))
            .await;

        assert_eq!(
            consumer.allocate_message_queue_strategy().await.get_name(),
            "AVG_BY_CIRCLE"
        );
        assert_eq!(impl_.allocate_message_queue_strategy().get_name(), "AVG_BY_CIRCLE");
        assert_eq!(
            impl_.rebalance_allocate_message_queue_strategy_name(),
            Some("AVG_BY_CIRCLE")
        );
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn core_runtime_config_setters_update_initialized_impl_like_java_facade_reference() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert_eq!(consumer.message_model().await, MessageModel::Clustering);
        assert_eq!(impl_.message_model_config(), MessageModel::Clustering);
        assert_eq!(MQConsumerInner::message_model(impl_.as_ref()), MessageModel::Clustering);

        consumer.set_message_model(MessageModel::Broadcasting).await;

        assert_eq!(consumer.message_model().await, MessageModel::Broadcasting);
        assert_eq!(impl_.message_model_config(), MessageModel::Broadcasting);
        assert_eq!(
            MQConsumerInner::message_model(impl_.as_ref()),
            MessageModel::Broadcasting
        );

        assert_eq!(
            consumer.consume_from_where().await,
            ConsumeFromWhere::ConsumeFromLastOffset
        );
        consumer
            .set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
            .await
            .expect("valid Java LitePull consumeFromWhere should be accepted");

        assert_eq!(
            consumer.consume_from_where().await,
            ConsumeFromWhere::ConsumeFromFirstOffset
        );
        assert_eq!(
            impl_.consume_from_where_config(),
            ConsumeFromWhere::ConsumeFromFirstOffset
        );
        assert_eq!(
            MQConsumerInner::consume_from_where(impl_.as_ref()),
            ConsumeFromWhere::ConsumeFromFirstOffset
        );

        let error = consumer
            .set_consume_from_where(ConsumeFromWhere::ConsumeFromMaxOffset)
            .await
            .expect_err("Java LitePull rejects legacy consumeFromWhere values");
        assert!(error.to_string().contains("Invalid ConsumeFromWhere Value"));
        assert_eq!(
            consumer.consume_from_where().await,
            ConsumeFromWhere::ConsumeFromFirstOffset
        );

        let timestamp = CheetahString::from_static_str("20250102030405");
        consumer.set_consume_timestamp(Some(timestamp.clone())).await;

        assert_eq!(consumer.consume_timestamp().await, Some(timestamp.clone()));
        assert_eq!(impl_.consume_timestamp(), Some(timestamp));

        assert_eq!(consumer.topic_metadata_check_interval_millis().await, 30_000);
        assert_eq!(impl_.topic_metadata_check_interval_millis(), 30_000);

        consumer.set_topic_metadata_check_interval_millis(12_345).await;

        assert_eq!(consumer.topic_metadata_check_interval_millis().await, 12_345);
        assert_eq!(impl_.topic_metadata_check_interval_millis(), 12_345);
    }

    #[tokio::test]
    async fn set_poll_timeout_updates_initialized_impl_like_java_facade_reference() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert_eq!(consumer.poll_timeout_millis().await, 5000);
        assert_eq!(impl_.poll_timeout_millis(), 5000);

        consumer.set_poll_timeout_millis(1234).await;

        assert_eq!(consumer.poll_timeout_millis().await, 1234);
        assert_eq!(impl_.poll_timeout_millis(), 1234);
    }

    #[tokio::test]
    async fn long_polling_timeout_setters_update_initialized_impl_like_java_facade_reference() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert_eq!(consumer.broker_suspend_max_time_millis().await, 20_000);
        assert_eq!(impl_.broker_suspend_max_time_millis(), 20_000);
        assert_eq!(consumer.consumer_timeout_millis_when_suspend().await, 30_000);
        assert_eq!(impl_.consumer_timeout_millis_when_suspend(), 30_000);
        assert_eq!(consumer.consumer_pull_timeout_millis().await, 10_000);
        assert_eq!(impl_.consumer_pull_timeout_millis(), 10_000);

        consumer.set_broker_suspend_max_time_millis(11_111).await;
        consumer.set_consumer_timeout_millis_when_suspend(22_222).await;
        consumer.set_consumer_pull_timeout_millis(12_345).await;

        assert_eq!(consumer.broker_suspend_max_time_millis().await, 11_111);
        assert_eq!(impl_.broker_suspend_max_time_millis(), 11_111);
        assert_eq!(consumer.consumer_timeout_millis_when_suspend().await, 22_222);
        assert_eq!(impl_.consumer_timeout_millis_when_suspend(), 22_222);
        assert_eq!(consumer.consumer_pull_timeout_millis().await, 12_345);
        assert_eq!(impl_.consumer_pull_timeout_millis(), 12_345);
    }

    #[tokio::test]
    async fn pull_flow_control_setters_update_initialized_impl_like_java_facade_reference() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        consumer.set_pull_batch_size(64).await;
        consumer.set_pull_thread_nums(12).await;
        consumer.set_pull_threshold_for_all(20_000).await;
        consumer.set_pull_threshold_for_queue(2_000).await;
        consumer.set_pull_threshold_size_for_queue(256).await;
        consumer.set_consume_max_span(4_000).await;

        assert_eq!(consumer.pull_batch_size().await, 64);
        assert_eq!(impl_.pull_batch_size(), 64);
        assert_eq!(consumer.pull_thread_nums().await, 12);
        assert_eq!(impl_.pull_thread_nums(), 12);
        assert_eq!(consumer.pull_threshold_for_all().await, 20_000);
        assert_eq!(impl_.pull_threshold_for_all(), 20_000);
        assert_eq!(consumer.pull_threshold_for_queue().await, 2_000);
        assert_eq!(impl_.pull_threshold_for_queue(), 2_000);
        assert_eq!(consumer.pull_threshold_size_for_queue().await, 256);
        assert_eq!(impl_.pull_threshold_size_for_queue(), 256);
        assert_eq!(consumer.consume_max_span().await, 4_000);
        assert_eq!(impl_.consume_max_span(), 4_000);
    }

    #[tokio::test]
    async fn broker_selection_setters_update_facade_config_like_java() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert!(!consumer.is_connect_broker_by_user().await);
        assert_eq!(
            consumer.default_broker_id().await,
            rocketmq_common::common::mix_all::MASTER_ID
        );
        assert!(!impl_.is_connect_broker_by_user());
        assert_eq!(impl_.default_broker_id(), rocketmq_common::common::mix_all::MASTER_ID);

        consumer.set_connect_broker_by_user(true).await;
        consumer.set_default_broker_id(3).await;

        assert!(consumer.is_connect_broker_by_user().await);
        assert_eq!(consumer.default_broker_id().await, 3);
        assert!(impl_.is_connect_broker_by_user());
        assert_eq!(impl_.default_broker_id(), 3);
    }

    #[test]
    fn lite_pull_facade_namespace_helpers_use_primary_namespace_like_java() {
        let (consumer, _) = new_namespaced_consumer_with_impl();
        let queue = MessageQueue::from_parts("TopicB", "broker-a", 3);

        assert_eq!(consumer.with_namespace("TopicB").as_str(), "ns%TopicB");
        assert_eq!(consumer.with_namespace("ns%TopicB").as_str(), "ns%TopicB");
        assert_eq!(consumer.without_namespace("ns%TopicB").as_str(), "TopicB");
        assert_eq!(consumer.queue_with_namespace(&queue).topic_str(), "ns%TopicB");
        assert_eq!(
            consumer
                .queue_without_namespace(&MessageQueue::from_parts("ns%TopicB", "broker-a", 3))
                .topic_str(),
            "TopicB"
        );
    }

    #[tokio::test]
    async fn trace_config_accessors_match_java_facade_fields() {
        let mut consumer = new_unstarted_consumer();

        assert!(!consumer.is_enable_msg_trace());
        assert!(consumer.customized_trace_topic().is_none());
        assert!(consumer.trace_dispatcher().await.is_none());

        consumer.set_enable_msg_trace(true);
        consumer.set_customized_trace_topic("TraceTopic");

        assert!(consumer.is_enable_msg_trace());
        assert_eq!(consumer.customized_trace_topic().as_deref(), Some("TraceTopic"));

        consumer.clear_customized_trace_topic();

        assert!(consumer.customized_trace_topic().is_none());
    }

    #[tokio::test]
    async fn lite_pull_use_tls_facade_updates_shared_impl_config_like_java_client_config() {
        let (consumer, impl_) = new_namespaced_consumer_with_impl();

        assert!(!consumer.is_use_tls());
        assert!(!impl_.client_config.load().is_use_tls());

        consumer.set_use_tls(true).await;

        assert!(consumer.is_use_tls());
        assert!(consumer.client_config().is_use_tls());
        assert!(impl_.client_config.load().is_use_tls());
    }

    #[tokio::test]
    async fn lite_pull_use_tls_facade_updates_async_trace_dispatcher() {
        let dispatcher = Arc::new(AsyncTraceDispatcher::new(
            "lite_pull_tls_trace_group",
            Type::Consume,
            1,
            "",
            None,
        ));
        let consumer = DefaultLitePullConsumer::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_tls_trace_group"),
                ..Default::default()
            }),
            None,
            Some(dispatcher.clone()),
            true,
            None,
        );

        assert!(!consumer.is_use_tls());
        assert!(!dispatcher.is_use_tls());

        consumer.set_use_tls(true).await;

        assert!(consumer.is_use_tls());
        assert!(dispatcher.is_use_tls());
    }

    #[tokio::test]
    async fn trace_init_uses_configured_batch_num_like_java() {
        let mut client_config = ClientConfig::default();
        client_config.set_trace_msg_batch_num(6);
        let consumer = DefaultLitePullConsumer::new(
            ArcMut::new(client_config),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_trace_batch_group"),
                ..Default::default()
            }),
            None,
            None,
            true,
            None,
        );
        let mut impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
            consumer.client_config.clone(),
            consumer.consumer_config.clone(),
        ));
        let wrapper = impl_.clone();
        impl_.set_default_lite_pull_consumer_impl(wrapper);

        consumer
            .init_trace_dispatcher_internal(&impl_)
            .await
            .expect("trace init should create dispatcher");
        let dispatcher = consumer
            .trace_dispatcher
            .read()
            .await
            .as_ref()
            .expect("trace dispatcher should be initialized")
            .clone();
        let async_dispatcher = dispatcher
            .as_any()
            .downcast_ref::<AsyncTraceDispatcher>()
            .expect("default trace dispatcher should be AsyncTraceDispatcher");
        assert_eq!(async_dispatcher.batch_num(), 6);
    }

    #[tokio::test]
    async fn trace_init_registers_custom_dispatcher_and_starts_with_client_config() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::default());
        let mut client_config = ClientConfig::default();
        client_config.set_namesrv_addr(CheetahString::from_static_str("127.0.0.1:9876"));
        client_config.set_access_channel(AccessChannel::Cloud);

        let consumer = DefaultLitePullConsumer::new(
            ArcMut::new(client_config),
            ArcMut::new(LitePullConsumerConfig {
                consumer_group: CheetahString::from_static_str("lite_pull_trace_group"),
                ..Default::default()
            }),
            None,
            Some(dispatcher.clone()),
            true,
            None,
        );
        let mut impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
            consumer.client_config.clone(),
            consumer.consumer_config.clone(),
        ));
        let wrapper = impl_.clone();
        impl_.set_default_lite_pull_consumer_impl(wrapper);

        consumer
            .init_trace_dispatcher_internal(&impl_)
            .await
            .expect("trace init should register hook");
        assert_eq!(impl_.consume_message_hook_count(), 1);

        consumer.start_trace_dispatcher().await;
        assert_eq!(dispatcher.start_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            dispatcher
                .last_name_srv_addr
                .lock()
                .expect("name server lock")
                .as_deref(),
            Some("127.0.0.1:9876")
        );
        assert_eq!(
            *dispatcher.last_access_channel.lock().expect("access channel lock"),
            Some(AccessChannel::Cloud)
        );
    }
}
