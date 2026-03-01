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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tokio::sync::OnceCell;
use tokio::sync::RwLock;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::DefaultLitePullConsumerImpl;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::LitePullConsumerConfig;
use crate::consumer::default_lite_pull_consumer_builder::DefaultLitePullConsumerBuilder;
use crate::consumer::lite_pull_consumer::LitePullConsumer;
use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::mq_consumer::MQConsumer;
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
///     .build();
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
///     .build();
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
/// consumer.assign(vec![mq1, mq2]).await;
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
    ///     .build();
    /// ```
    pub fn builder() -> DefaultLitePullConsumerBuilder {
        DefaultLitePullConsumerBuilder::new()
    }

    /// Returns the consumer group name.
    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_config.consumer_group
    }

    /// Returns the namespace (if configured).
    pub fn namespace(&self) -> Option<CheetahString> {
        self.client_config.get_namespace_v2().cloned()
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
        match self.client_config.get_namespace_v2() {
            Some(namespace) => NamespaceUtil::wrap_namespace(namespace.as_str(), resource),
            None => CheetahString::from_string(resource.to_string()),
        }
    }

    /// Removes namespace from a topic name (if configured).
    fn without_namespace(&self, resource: &str) -> CheetahString {
        match self.client_config.get_namespace_v2() {
            Some(namespace) => NamespaceUtil::without_namespace_with_namespace(resource, namespace.as_str()).into(),
            None => CheetahString::from_string(resource.to_string()),
        }
    }

    /// Wraps a message queue with namespace.
    fn queue_with_namespace(&self, mq: &MessageQueue) -> MessageQueue {
        match self.client_config.get_namespace_v2() {
            Some(namespace) => {
                let wrapped_topic = NamespaceUtil::wrap_namespace(namespace.as_str(), mq.topic().clone());
                MessageQueue::from_parts(wrapped_topic, mq.broker_name().clone(), mq.queue_id())
            }
            None => mq.clone(),
        }
    }

    /// Removes namespace from a message queue.
    fn queue_without_namespace(&self, mq: &MessageQueue) -> MessageQueue {
        match self.client_config.get_namespace_v2() {
            Some(namespace) => {
                let unwrapped_topic = NamespaceUtil::without_namespace_with_namespace(mq.topic(), namespace.as_str());
                MessageQueue::from_parts(unwrapped_topic, mq.broker_name().clone(), mq.queue_id())
            }
            None => mq.clone(),
        }
    }

    /// Initializes the trace dispatcher if message trace is enabled.
    async fn init_trace_dispatcher_internal(&self, impl_: &ArcMut<DefaultLitePullConsumerImpl>) -> RocketMQResult<()> {
        if !self.enable_msg_trace {
            return Ok(());
        }

        let mut dispatcher_guard = self.trace_dispatcher.write().await;
        if dispatcher_guard.is_some() {
            return Ok(());
        }

        // Create default async trace dispatcher
        let trace_topic = self
            .custom_trace_topic
            .as_ref()
            .map(|t| t.as_str())
            .unwrap_or(TopicValidator::RMQ_SYS_TRACE_TOPIC);

        let dispatcher = AsyncTraceDispatcher::new(
            self.consumer_config.consumer_group.as_str(),
            Type::Consume,
            trace_topic,
            self.rpc_hook.clone(),
        );

        let dispatcher_arc: Arc<dyn TraceDispatcher + Send + Sync> = Arc::new(dispatcher);
        *dispatcher_guard = Some(dispatcher_arc.clone());

        // Register trace hook to impl
        let hook = Arc::new(ConsumeMessageTraceHookImpl::new(dispatcher_arc));
        impl_.mut_from_ref().register_consume_message_hook(hook);

        Ok(())
    }

    /// Returns a reference to the internal implementation.
    ///
    /// # Panics
    ///
    /// Panics if called before `start()`.
    fn impl_(&self) -> &ArcMut<DefaultLitePullConsumerImpl> {
        self.default_lite_pull_consumer_impl
            .get()
            .expect("Consumer not started. Call start() first.")
    }
}

#[allow(unused)]
impl LitePullConsumer for DefaultLitePullConsumer {
    async fn start(&self) -> RocketMQResult<()> {
        // Initialize impl using OnceCell::get_or_try_init for thread-safe lazy initialization
        let impl_ = self
            .default_lite_pull_consumer_impl
            .get_or_try_init(|| async {
                let impl_ = ArcMut::new(DefaultLitePullConsumerImpl::new(
                    self.client_config.clone(),
                    self.consumer_config.clone(),
                ));

                // Initialize trace if enabled
                self.init_trace_dispatcher_internal(&impl_).await?;

                Ok::<ArcMut<DefaultLitePullConsumerImpl>, rocketmq_error::RocketMQError>(impl_)
            })
            .await?;

        // Start the impl
        impl_.mut_from_ref().start().await
    }

    async fn shutdown(&self) {
        if let Some(impl_) = self.default_lite_pull_consumer_impl.get() {
            let _ = impl_.mut_from_ref().shutdown().await;
        }

        // Shutdown trace dispatcher
        if let Some(dispatcher) = self.trace_dispatcher.write().await.take() {
            dispatcher.shutdown();
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
        self.impl_()
            .mut_from_ref()
            .subscribe(wrapped_topic, sub_expression)
            .await
    }

    async fn subscribe_with_listener<MQL>(&self, topic: &str, sub_expression: &str, listener: MQL) -> RocketMQResult<()>
    where
        MQL: MessageQueueListener + 'static,
    {
        let wrapped_topic = self.with_namespace(topic);
        self.impl_()
            .mut_from_ref()
            .subscribe_with_listener(wrapped_topic, sub_expression, listener)
            .await
    }

    async fn subscribe_with_selector(&self, topic: &str, selector: Option<MessageSelector>) -> RocketMQResult<()> {
        let wrapped_topic = self.with_namespace(topic);
        self.impl_()
            .mut_from_ref()
            .subscribe_with_selector(wrapped_topic, selector)
            .await
    }

    async fn unsubscribe(&self, topic: &str) {
        let wrapped_topic = self.with_namespace(topic);
        if let Err(e) = self.impl_().mut_from_ref().unsubscribe(wrapped_topic).await {
            tracing::warn!("Failed to unsubscribe from topic {}: {}", topic, e);
        }
    }

    async fn assignment(&self) -> RocketMQResult<HashSet<MessageQueue>> {
        let assignment = self.impl_().assignment().await;

        // Remove namespace from all queues
        let mut result = HashSet::with_capacity(assignment.len());
        for mq in assignment {
            result.insert(self.queue_without_namespace(&mq));
        }

        Ok(result)
    }

    async fn assign(&self, message_queues: Vec<MessageQueue>) {
        // Wrap namespace for all queues
        let wrapped_queues: Vec<_> = message_queues.iter().map(|mq| self.queue_with_namespace(mq)).collect();

        self.impl_().mut_from_ref().assign(wrapped_queues).await;
    }

    async fn set_sub_expression_for_assign(&self, topic: &str, sub_expression: &str) {
        let wrapped_topic = self.with_namespace(topic);
        self.impl_()
            .set_sub_expression_for_assign(wrapped_topic, sub_expression)
            .await;
    }

    async fn build_subscriptions_for_heartbeat(
        &self,
        sub_expression_map: &mut HashMap<String, MessageSelector>,
    ) -> RocketMQResult<()> {
        self.impl_().build_subscriptions_for_heartbeat(sub_expression_map).await
    }

    /// Zero-copy implementation.
    async fn poll_zero_copy(&self) -> Vec<ArcMut<MessageExt>> {
        match self.impl_().poll(self.consumer_config.poll_timeout_millis).await {
            Ok(messages) => messages,
            Err(e) => {
                tracing::error!("Poll failed: {}", e);
                Vec::new()
            }
        }
    }

    /// Zero-copy implementation with custom timeout.
    async fn poll_with_timeout_zero_copy(&self, timeout: u64) -> Vec<ArcMut<MessageExt>> {
        match self.impl_().poll_with_timeout(timeout).await {
            Ok(messages) => messages,
            Err(e) => {
                tracing::error!("Poll failed: {}", e);
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

    async fn fetch_message_queues(&self, topic: &str) -> RocketMQResult<Vec<MessageQueue>> {
        let wrapped_topic = self.with_namespace(topic);
        let queues = self.impl_().fetch_message_queues(wrapped_topic).await?;

        // Remove namespace from all queues and convert to Vec
        let result: Vec<_> = queues.into_iter().map(|mq| self.queue_without_namespace(&mq)).collect();

        Ok(result)
    }

    async fn committed(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.impl_().committed(&wrapped_mq).await
    }

    async fn commit_all(&self) -> RocketMQResult<()> {
        self.impl_().commit_all().await
    }

    async fn commit_with_map(&self, offset_map: HashMap<MessageQueue, i64>, persist: bool) {
        // Wrap namespace for all queues
        let mut wrapped_offsets = HashMap::with_capacity(offset_map.len());
        for (mq, offset) in offset_map {
            wrapped_offsets.insert(self.queue_with_namespace(&mq), offset);
        }

        if let Err(e) = self.impl_().commit(wrapped_offsets, persist).await {
            tracing::warn!("commit_with_map failed: {}", e);
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

        // Get current offsets for specified queues
        let mut offset_map = HashMap::new();
        for mq in &wrapped_queues {
            if let Ok(offset) = self.impl_().committed(mq).await {
                offset_map.insert(mq.clone(), offset);
            }
        }

        if let Err(e) = self.impl_().commit(offset_map, persist).await {
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
    }

    async fn offset_for_timestamp(&self, message_queue: &MessageQueue, timestamp: u64) -> RocketMQResult<i64> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.impl_().offset_for_timestamp(&wrapped_mq, timestamp).await
    }

    async fn pause(&self, message_queues: Vec<MessageQueue>) {
        let wrapped_queues: Vec<_> = message_queues.iter().map(|mq| self.queue_with_namespace(mq)).collect();

        self.impl_().pause(&wrapped_queues).await;
    }

    async fn resume(&self, message_queues: Vec<MessageQueue>) {
        let wrapped_queues: Vec<_> = message_queues.iter().map(|mq| self.queue_with_namespace(mq)).collect();

        self.impl_().resume(&wrapped_queues).await;
    }

    async fn is_paused(&self, message_queue: &MessageQueue) -> bool {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.impl_().is_paused(&wrapped_mq).await
    }

    async fn seek(&self, message_queue: &MessageQueue, offset: i64) -> RocketMQResult<()> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.impl_().seek(&wrapped_mq, offset).await
    }

    async fn seek_to_begin(&self, message_queue: &MessageQueue) -> RocketMQResult<()> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.impl_().seek_to_begin(&wrapped_mq).await
    }

    async fn seek_to_end(&self, message_queue: &MessageQueue) -> RocketMQResult<()> {
        let wrapped_mq = self.queue_with_namespace(message_queue);
        self.impl_().seek_to_end(&wrapped_mq).await
    }

    async fn update_name_server_address(&self, name_server_address: &str) {
        self.client_config
            .mut_from_ref()
            .set_namesrv_addr(CheetahString::from_string(name_server_address.to_string()));

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
        self.impl_()
            .register_topic_message_queue_change_listener(wrapped_topic, listener_arc)
            .await
    }
}

impl MQConsumer for DefaultLitePullConsumer {
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
}
