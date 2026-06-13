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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::utils::util_all;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;

use crate::base::client_config::ClientConfig;
use crate::base::query_result::QueryResult;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::default_mq_push_consumer_builder::DefaultMQPushConsumerBuilder;
use crate::consumer::listener::message_listener::MessageListener;
use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use crate::consumer::listener::message_listener_orderly::MessageListenerOrderly;
use crate::consumer::message_queue_listener::ArcMessageQueueListener;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::mq_consumer::MQConsumer;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::mq_push_consumer::MQPushConsumer;
use crate::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely;
use crate::consumer::store::offset_store::OffsetStore;
use crate::hook::consume_message_hook::ConsumeMessageHook;
use crate::trace::async_trace_dispatcher::AsyncTraceDispatcher;
use crate::trace::hook::consume_message_trace_hook_impl::ConsumeMessageTraceHookImpl;
use crate::trace::trace_dispatcher::ArcTraceDispatcher;
use crate::trace::trace_dispatcher::Type;

#[derive(Clone)]
pub struct ConsumerConfig {
    pub(crate) consumer_group: CheetahString,
    pub(crate) topic: CheetahString,
    pub(crate) sub_expression: CheetahString,
    pub(crate) message_model: MessageModel,
    pub(crate) consume_from_where: ConsumeFromWhere,
    pub(crate) consume_timestamp: Option<CheetahString>,
    pub(crate) allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,
    //this field will be removed in a certain version after April 5, 2020
    pub(crate) subscription: ArcMut<HashMap<CheetahString, CheetahString>>,
    pub(crate) message_listener: Option<ArcMut<MessageListener>>,
    pub(crate) message_queue_listener: Option<ArcMessageQueueListener>,
    pub(crate) offset_store: Option<ArcMut<OffsetStore>>,
    pub(crate) consume_thread_min: u32,
    pub(crate) consume_thread_max: u32,
    pub(crate) adjust_thread_pool_nums_threshold: u64,
    pub(crate) consume_concurrently_max_span: u32,
    pub(crate) pull_threshold_for_queue: u32,
    pub(crate) pop_threshold_for_queue: u32,
    pub(crate) pull_threshold_size_for_queue: u32,
    pub(crate) pull_threshold_for_topic: i32,
    pub(crate) pull_threshold_size_for_topic: i32,
    pub(crate) pull_interval: u64,
    pub(crate) consume_message_batch_max_size: u32,
    pub(crate) pull_batch_size: u32,
    pub(crate) pull_batch_size_in_bytes: u32,
    pub(crate) post_subscription_when_pull: bool,
    pub(crate) unit_mode: bool,
    pub(crate) max_reconsume_times: i32,
    pub(crate) suspend_current_queue_time_millis: u64,
    pub(crate) consume_timeout: u64,
    pub(crate) pop_invisible_time: u64,
    pub(crate) pop_batch_nums: u32,
    pub(crate) await_termination_millis_when_shutdown: u64,
    pub(crate) trace_dispatcher: Option<ArcTraceDispatcher>,
    pub(crate) client_rebalance: bool,
    pub(crate) rpc_hook: Option<Arc<dyn RPCHook>>,
}

impl ConsumerConfig {
    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    pub fn message_model(&self) -> MessageModel {
        self.message_model
    }

    pub fn consume_from_where(&self) -> ConsumeFromWhere {
        self.consume_from_where
    }

    pub fn consume_timestamp(&self) -> &Option<CheetahString> {
        &self.consume_timestamp
    }

    pub fn allocate_message_queue_strategy(&self) -> Option<Arc<dyn AllocateMessageQueueStrategy>> {
        self.allocate_message_queue_strategy.clone()
    }

    pub fn subscription(&self) -> &ArcMut<HashMap<CheetahString, CheetahString>> {
        &self.subscription
    }

    /*    pub fn message_listener(&self) -> &Option<ArcMut<MessageListener>> {
        &self.message_listener
    }*/

    /*    pub fn message_queue_listener(&self) -> &Option<ArcMessageQueueListener> {
        &self.message_queue_listener
    }*/

    pub fn offset_store(&self) -> Option<ArcMut<OffsetStore>> {
        self.offset_store.clone()
    }

    pub fn consume_thread_min(&self) -> u32 {
        self.consume_thread_min
    }

    pub fn consume_thread_max(&self) -> u32 {
        self.consume_thread_max
    }

    pub fn adjust_thread_pool_nums_threshold(&self) -> u64 {
        self.adjust_thread_pool_nums_threshold
    }

    pub fn consume_concurrently_max_span(&self) -> u32 {
        self.consume_concurrently_max_span
    }

    pub fn pull_threshold_for_queue(&self) -> u32 {
        self.pull_threshold_for_queue
    }

    pub fn pop_threshold_for_queue(&self) -> u32 {
        self.pop_threshold_for_queue
    }

    pub fn pull_threshold_size_for_queue(&self) -> u32 {
        self.pull_threshold_size_for_queue
    }

    pub fn pull_threshold_for_topic(&self) -> i32 {
        self.pull_threshold_for_topic
    }

    pub fn pull_threshold_size_for_topic(&self) -> i32 {
        self.pull_threshold_size_for_topic
    }

    pub fn pull_interval(&self) -> u64 {
        self.pull_interval
    }

    pub fn consume_message_batch_max_size(&self) -> u32 {
        self.consume_message_batch_max_size
    }

    pub fn pull_batch_size(&self) -> u32 {
        self.pull_batch_size
    }

    pub fn pull_batch_size_in_bytes(&self) -> u32 {
        self.pull_batch_size_in_bytes
    }

    pub fn post_subscription_when_pull(&self) -> bool {
        self.post_subscription_when_pull
    }

    pub fn unit_mode(&self) -> bool {
        self.unit_mode
    }

    pub fn max_reconsume_times(&self) -> i32 {
        self.max_reconsume_times
    }

    pub fn suspend_current_queue_time_millis(&self) -> u64 {
        self.suspend_current_queue_time_millis
    }

    pub fn consume_timeout(&self) -> u64 {
        self.consume_timeout
    }

    pub fn pop_invisible_time(&self) -> u64 {
        self.pop_invisible_time
    }

    pub fn pop_batch_nums(&self) -> u32 {
        self.pop_batch_nums
    }

    pub fn await_termination_millis_when_shutdown(&self) -> u64 {
        self.await_termination_millis_when_shutdown
    }

    pub fn trace_dispatcher(&self) -> &Option<ArcTraceDispatcher> {
        &self.trace_dispatcher
    }

    pub fn client_rebalance(&self) -> bool {
        self.client_rebalance
    }

    pub fn rpc_hook(&self) -> &Option<Arc<dyn RPCHook>> {
        &self.rpc_hook
    }

    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.consumer_group = consumer_group;
    }

    pub fn set_message_model(&mut self, message_model: MessageModel) {
        self.message_model = message_model;
    }

    pub fn set_consume_from_where(&mut self, consume_from_where: ConsumeFromWhere) {
        self.consume_from_where = consume_from_where;
    }

    pub fn set_consume_timestamp(&mut self, consume_timestamp: Option<CheetahString>) {
        self.consume_timestamp = consume_timestamp;
    }

    pub fn set_allocate_message_queue_strategy(
        &mut self,
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    ) {
        self.allocate_message_queue_strategy = Some(allocate_message_queue_strategy);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not
     * use this method.
     */
    pub fn set_subscription(&mut self, subscription: ArcMut<HashMap<CheetahString, CheetahString>>) {
        self.subscription = subscription;
    }

    /*    pub fn set_message_listener(
        &mut self,
        message_listener: Option<ArcMut<MessageListener>>,
    ) {
        self.message_listener = message_listener;
    }*/

    pub fn set_message_queue_listener(&mut self, message_queue_listener: Option<ArcMessageQueueListener>) {
        self.message_queue_listener = message_queue_listener;
    }

    pub fn set_offset_store(&mut self, offset_store: Option<ArcMut<OffsetStore>>) {
        self.offset_store = offset_store;
    }

    pub fn set_consume_thread_min(&mut self, consume_thread_min: u32) {
        self.consume_thread_min = consume_thread_min;
    }

    pub fn set_consume_thread_max(&mut self, consume_thread_max: u32) {
        self.consume_thread_max = consume_thread_max;
    }

    pub fn set_adjust_thread_pool_nums_threshold(&mut self, adjust_thread_pool_nums_threshold: u64) {
        self.adjust_thread_pool_nums_threshold = adjust_thread_pool_nums_threshold;
    }

    pub fn set_consume_concurrently_max_span(&mut self, consume_concurrently_max_span: u32) {
        self.consume_concurrently_max_span = consume_concurrently_max_span;
    }

    pub fn set_pull_threshold_for_queue(&mut self, pull_threshold_for_queue: u32) {
        self.pull_threshold_for_queue = pull_threshold_for_queue;
    }

    pub fn set_pop_threshold_for_queue(&mut self, pop_threshold_for_queue: u32) {
        self.pop_threshold_for_queue = pop_threshold_for_queue;
    }

    pub fn set_pull_threshold_size_for_queue(&mut self, pull_threshold_size_for_queue: u32) {
        self.pull_threshold_size_for_queue = pull_threshold_size_for_queue;
    }

    pub fn set_pull_threshold_for_topic(&mut self, pull_threshold_for_topic: i32) {
        self.pull_threshold_for_topic = pull_threshold_for_topic;
    }

    pub fn set_pull_threshold_size_for_topic(&mut self, pull_threshold_size_for_topic: i32) {
        self.pull_threshold_size_for_topic = pull_threshold_size_for_topic;
    }

    pub fn set_pull_interval(&mut self, pull_interval: u64) {
        self.pull_interval = pull_interval;
    }

    pub fn set_consume_message_batch_max_size(&mut self, consume_message_batch_max_size: u32) {
        self.consume_message_batch_max_size = consume_message_batch_max_size;
    }

    pub fn set_pull_batch_size(&mut self, pull_batch_size: u32) {
        self.pull_batch_size = pull_batch_size;
    }

    pub fn set_pull_batch_size_in_bytes(&mut self, pull_batch_size_in_bytes: u32) {
        self.pull_batch_size_in_bytes = pull_batch_size_in_bytes;
    }

    pub fn set_post_subscription_when_pull(&mut self, post_subscription_when_pull: bool) {
        self.post_subscription_when_pull = post_subscription_when_pull;
    }

    pub fn set_unit_mode(&mut self, unit_mode: bool) {
        self.unit_mode = unit_mode;
    }

    pub fn set_max_reconsume_times(&mut self, max_reconsume_times: i32) {
        self.max_reconsume_times = max_reconsume_times;
    }

    pub fn set_suspend_current_queue_time_millis(&mut self, suspend_current_queue_time_millis: u64) {
        self.suspend_current_queue_time_millis = suspend_current_queue_time_millis;
    }

    pub fn set_consume_timeout(&mut self, consume_timeout: u64) {
        self.consume_timeout = consume_timeout;
    }

    pub fn set_pop_invisible_time(&mut self, pop_invisible_time: u64) {
        self.pop_invisible_time = pop_invisible_time;
    }

    pub fn set_pop_batch_nums(&mut self, pop_batch_nums: u32) {
        self.pop_batch_nums = pop_batch_nums;
    }

    pub fn set_await_termination_millis_when_shutdown(&mut self, await_termination_millis_when_shutdown: u64) {
        self.await_termination_millis_when_shutdown = await_termination_millis_when_shutdown;
    }

    pub fn set_trace_dispatcher(&mut self, trace_dispatcher: Option<ArcTraceDispatcher>) {
        self.trace_dispatcher = trace_dispatcher;
    }

    pub fn set_client_rebalance(&mut self, client_rebalance: bool) {
        self.client_rebalance = client_rebalance;
    }

    pub fn set_rpc_hook(&mut self, rpc_hook: Option<Arc<dyn RPCHook>>) {
        self.rpc_hook = rpc_hook;
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        ConsumerConfig {
            consumer_group: CheetahString::new(),
            topic: CheetahString::new(),
            sub_expression: CheetahString::new(),
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            consume_timestamp: Some(CheetahString::from_string(util_all::time_millis_to_human_string3(
                (current_millis() - (1000 * 60 * 30)) as i64,
            ))),
            allocate_message_queue_strategy: Some(Arc::new(AllocateMessageQueueAveragely)),
            subscription: ArcMut::new(HashMap::new()),
            message_listener: None,
            message_queue_listener: None,
            offset_store: None,

            consume_thread_min: 20,
            consume_thread_max: 64,
            adjust_thread_pool_nums_threshold: 100_000,
            consume_concurrently_max_span: 2000,
            pull_threshold_for_queue: 1000,
            pop_threshold_for_queue: 96,
            pull_threshold_size_for_queue: 100,
            pull_threshold_for_topic: -1,
            pull_threshold_size_for_topic: -1,
            pull_interval: 0,
            consume_message_batch_max_size: 1,
            pull_batch_size: 32,
            pull_batch_size_in_bytes: 256 * 1024,
            post_subscription_when_pull: false,
            unit_mode: false,
            max_reconsume_times: -1,
            suspend_current_queue_time_millis: 1000,
            consume_timeout: 15,
            pop_invisible_time: 60000,
            pop_batch_nums: 32,
            await_termination_millis_when_shutdown: 0,
            trace_dispatcher: None,
            client_rebalance: true,
            rpc_hook: None,
        }
    }
}

pub struct DefaultMQPushConsumer {
    client_config: ClientConfig,
    consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
}

impl MQConsumer for DefaultMQPushConsumer {
    async fn create_topic(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.create_topic_with_flag(key, new_topic, queue_num, 0, attributes)
            .await
    }

    async fn create_topic_with_flag(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let new_topic = self.client_config.with_namespace(CheetahString::from(new_topic));
        self.default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?
            .create_topic(key, new_topic.as_str(), queue_num, topic_sys_flag, attributes)
            .await
    }

    async fn search_offset(&mut self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        let mq = self.client_config.queue_with_namespace(mq.clone());
        self.default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?
            .search_offset(&mq, timestamp)
            .await
    }

    async fn max_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        let mq = self.client_config.queue_with_namespace(mq.clone());
        self.default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?
            .max_offset(&mq)
            .await
    }

    async fn min_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        let mq = self.client_config.queue_with_namespace(mq.clone());
        self.default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?
            .min_offset(&mq)
            .await
    }

    async fn earliest_msg_store_time(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        let mq = self.client_config.queue_with_namespace(mq.clone());
        self.default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?
            .earliest_msg_store_time(&mq)
            .await
    }

    async fn query_message(
        &mut self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        let topic = self.client_config.with_namespace(CheetahString::from(topic));
        self.default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?
            .query_message(topic.as_str(), key, max_num, begin, end)
            .await
    }

    async fn view_message(&mut self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        let topic = self.client_config.with_namespace(CheetahString::from(topic));
        let consumer_impl = self
            .default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?;
        if message_decoder::decode_message_id(msg_id).is_ok() {
            consumer_impl.view_message(topic.as_str(), msg_id).await
        } else {
            consumer_impl.query_message_by_uniq_key(topic.as_str(), msg_id).await
        }
    }

    async fn send_message_back(
        &mut self,
        mut msg: MessageExt,
        delay_level: i32,
        broker_name: &str,
    ) -> rocketmq_error::RocketMQResult<()> {
        let consumer_impl = self
            .default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?;
        let topic = msg.topic().clone();
        msg.set_topic(self.client_config.with_namespace(topic));
        let broker_name = (!broker_name.is_empty()).then(|| CheetahString::from(broker_name));
        consumer_impl
            .send_message_back_with_broker_name(&mut msg, delay_level, broker_name, None)
            .await
    }

    async fn fetch_subscribe_message_queues(
        &mut self,
        topic: &str,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        let topic = self.client_config.with_namespace(CheetahString::from(topic));
        let consumer_impl = self
            .default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?;
        consumer_impl.fetch_subscribe_message_queues(topic.as_str()).await
    }
}

impl MQPushConsumer for DefaultMQPushConsumer {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let consumer_group = NamespaceUtil::wrap_namespace(
            self.client_config.get_namespace().unwrap_or_default().as_str(),
            self.consumer_config.consumer_group.as_str(),
        );
        self.set_consumer_group(consumer_group.as_str());
        let consumer_impl = self
            .default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?;
        consumer_impl.start().await?;

        let trace_dispatcher_to_start =
            Self::prepare_trace_dispatcher(&self.client_config, &mut self.consumer_config, consumer_impl);
        if let Some(dispatcher) = trace_dispatcher_to_start {
            self.start_trace_dispatcher(dispatcher).await;
        }

        Ok(())
    }

    async fn shutdown(&mut self) {
        if let Some(ref mut default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            default_mqpush_consumer_impl
                .shutdown(self.consumer_config.await_termination_millis_when_shutdown)
                .await;
        }
        if let Some(ref trace_dispatcher) = self.consumer_config.trace_dispatcher {
            trace_dispatcher.shutdown();
        }
    }

    fn register_message_listener_concurrently<ML>(&mut self, message_listener: ML)
    where
        ML: MessageListenerConcurrently + Send + Sync + 'static,
    {
        let message_listener = MessageListener {
            message_listener_concurrently: Some(Arc::new(message_listener)),
            message_listener_orderly: None,
        };
        self.consumer_config.message_listener = Some(ArcMut::new(message_listener));
        if let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_mut() {
            default_mqpush_consumer_impl.register_message_listener(self.consumer_config.message_listener.clone());
        } else {
            tracing::warn!("DefaultMQPushConsumerImpl is not initialized; listener stored in consumer config");
        }
    }

    fn register_message_listener_orderly<ML>(&mut self, message_listener: ML)
    where
        ML: MessageListenerOrderly + Send + Sync + 'static,
    {
        let message_listener = MessageListener {
            message_listener_concurrently: None,
            message_listener_orderly: Some(Arc::new(message_listener)),
        };
        self.consumer_config.message_listener = Some(ArcMut::new(message_listener));
        if let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_mut() {
            default_mqpush_consumer_impl.register_message_listener(self.consumer_config.message_listener.clone());
        } else {
            tracing::warn!("DefaultMQPushConsumerImpl is not initialized; listener stored in consumer config");
        }
    }

    async fn subscribe(
        &mut self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let topic = self.client_config.with_namespace(topic);
        let sub_expression = sub_expression.into();

        self.default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?
            .subscribe(topic, sub_expression)
            .await
    }

    async fn subscribe_with_selector(
        &mut self,
        topic: &str,
        selector: Option<MessageSelector>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let consumer_impl = self
            .default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?;
        let topic = self.client_config.with_namespace(CheetahString::from(topic));
        consumer_impl.subscribe_with_selector(topic, selector).await
    }

    async fn unsubscribe(&mut self, topic: &str) {
        if let Some(ref mut default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            default_mqpush_consumer_impl.unsubscribe(topic).await;
        }
    }

    async fn suspend(&self) {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            default_mqpush_consumer_impl.suspend().await;
        }
    }

    async fn resume(&self) {
        if let Some(ref default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl {
            default_mqpush_consumer_impl.resume().await;
        }
    }

    fn update_core_pool_size(&self, core_pool_size: usize) -> rocketmq_error::RocketMQResult<()> {
        let consumer_impl = self
            .default_mqpush_consumer_impl
            .as_ref()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?;
        consumer_impl.update_core_pool_size(core_pool_size);
        Ok(())
    }
}

impl DefaultMQPushConsumer {
    pub fn builder() -> DefaultMQPushConsumerBuilder {
        DefaultMQPushConsumerBuilder::default()
    }

    pub async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQPushConsumer>::start(self).await
    }

    pub async fn shutdown(&mut self) {
        <Self as MQPushConsumer>::shutdown(self).await;
    }

    pub async fn create_topic(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQConsumer>::create_topic(self, key, new_topic, queue_num, attributes).await
    }

    pub async fn create_topic_with_flag(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQConsumer>::create_topic_with_flag(self, key, new_topic, queue_num, topic_sys_flag, attributes).await
    }

    pub async fn search_offset(&mut self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        <Self as MQConsumer>::search_offset(self, mq, timestamp).await
    }

    pub async fn max_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        <Self as MQConsumer>::max_offset(self, mq).await
    }

    pub async fn min_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        <Self as MQConsumer>::min_offset(self, mq).await
    }

    pub async fn earliest_msg_store_time(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        <Self as MQConsumer>::earliest_msg_store_time(self, mq).await
    }

    pub async fn query_message(
        &mut self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        <Self as MQConsumer>::query_message(self, topic, key, max_num, begin, end).await
    }

    pub async fn view_message(&mut self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        <Self as MQConsumer>::view_message(self, topic, msg_id).await
    }

    pub async fn send_message_back(
        &mut self,
        msg: MessageExt,
        delay_level: i32,
        broker_name: &str,
    ) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQConsumer>::send_message_back(self, msg, delay_level, broker_name).await
    }

    pub async fn send_message_back_to_origin_broker(
        &mut self,
        msg: MessageExt,
        delay_level: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQConsumer>::send_message_back_to_origin_broker(self, msg, delay_level).await
    }

    pub async fn fetch_subscribe_message_queues(
        &mut self,
        topic: &str,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        <Self as MQConsumer>::fetch_subscribe_message_queues(self, topic).await
    }

    pub async fn subscribe(
        &mut self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQPushConsumer>::subscribe(self, topic, sub_expression).await
    }

    pub async fn subscribe_with_selector(
        &mut self,
        topic: &str,
        selector: Option<MessageSelector>,
    ) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQPushConsumer>::subscribe_with_selector(self, topic, selector).await
    }

    pub async fn unsubscribe(&mut self, topic: &str) {
        <Self as MQPushConsumer>::unsubscribe(self, topic).await;
    }

    pub async fn suspend(&self) {
        <Self as MQPushConsumer>::suspend(self).await;
    }

    pub async fn resume(&self) {
        <Self as MQPushConsumer>::resume(self).await;
    }

    pub fn update_core_pool_size(&self, core_pool_size: usize) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQPushConsumer>::update_core_pool_size(self, core_pool_size)
    }

    /// Returns a Java-compatible running snapshot for broker admin diagnostics.
    pub async fn consumer_running_info(&self) -> rocketmq_error::RocketMQResult<ConsumerRunningInfo> {
        let consumer_impl = self
            .default_mqpush_consumer_impl
            .as_ref()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?;
        Ok(MQConsumerInner::consumer_running_info(consumer_impl.as_ref()).await)
    }

    fn prepare_trace_dispatcher(
        client_config: &ClientConfig,
        consumer_config: &mut ArcMut<ConsumerConfig>,
        consumer_impl: &mut ArcMut<DefaultMQPushConsumerImpl>,
    ) -> Option<ArcTraceDispatcher> {
        let dispatcher = match consumer_config.trace_dispatcher.clone() {
            Some(dispatcher) => dispatcher,
            None if client_config.enable_trace => {
                let trace_topic = client_config
                    .trace_topic
                    .as_ref()
                    .map(|topic| topic.as_str())
                    .unwrap_or_default();
                let dispatcher = AsyncTraceDispatcher::new(
                    consumer_config.consumer_group.as_str(),
                    Type::Consume,
                    client_config.trace_msg_batch_num,
                    trace_topic,
                    consumer_config.rpc_hook.clone(),
                );
                Arc::new(dispatcher)
            }
            None => return None,
        };

        if let Some(async_dispatcher) = dispatcher.as_any().downcast_ref::<AsyncTraceDispatcher>() {
            async_dispatcher.set_host_consumer(consumer_impl.clone());
            async_dispatcher.set_namespace_v2(client_config.namespace_v2.clone());
            async_dispatcher.set_use_tls(client_config.use_tls);
        }

        consumer_config.trace_dispatcher = Some(dispatcher.clone());
        consumer_impl.register_consume_message_hook(ConsumeMessageTraceHookImpl::new(dispatcher.clone()));
        Some(dispatcher)
    }

    async fn start_trace_dispatcher(&self, dispatcher: ArcTraceDispatcher) {
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

    /// Returns whether this push consumer is currently paused.
    ///
    /// Mirrors Java `DefaultMQPushConsumer.isPause`.
    pub fn is_pause(&self) -> bool {
        self.default_mqpush_consumer_impl
            .as_ref()
            .is_some_and(|consumer_impl| consumer_impl.is_pause())
    }

    /// Returns whether the registered listener consumes messages orderly.
    ///
    /// Mirrors Java `DefaultMQPushConsumer.isConsumeOrderly`.
    pub fn is_consume_orderly(&self) -> bool {
        self.default_mqpush_consumer_impl
            .as_ref()
            .is_some_and(|consumer_impl| consumer_impl.is_consume_orderly())
    }

    /// Registers a hook invoked before and after message consumption.
    ///
    /// Mirrors Java `DefaultMQPushConsumer.registerConsumeMessageHook`.
    pub fn register_consume_message_hook(
        &mut self,
        hook: impl ConsumeMessageHook + 'static,
    ) -> rocketmq_error::RocketMQResult<()> {
        let consumer_impl = self
            .default_mqpush_consumer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQPushConsumerImpl"))?;
        consumer_impl.register_consume_message_hook(hook);
        Ok(())
    }

    pub fn default_mq_push_consumer_impl(&self) -> Option<&ArcMut<DefaultMQPushConsumerImpl>> {
        self.default_mqpush_consumer_impl.as_ref()
    }

    pub fn get_default_mq_push_consumer_impl(&self) -> Option<&ArcMut<DefaultMQPushConsumerImpl>> {
        self.default_mq_push_consumer_impl()
    }

    pub fn message_listener(&self) -> Option<ArcMut<MessageListener>> {
        self.consumer_config.message_listener.clone()
    }

    pub fn get_message_listener(&self) -> Option<ArcMut<MessageListener>> {
        self.message_listener()
    }

    pub fn set_message_listener(&mut self, message_listener: Option<ArcMut<MessageListener>>) {
        self.consumer_config.message_listener = message_listener.clone();
        if let Some(consumer_impl) = self.default_mqpush_consumer_impl.as_mut() {
            consumer_impl.register_message_listener(message_listener);
        }
    }

    pub fn register_message_listener(&mut self, message_listener: Option<ArcMut<MessageListener>>) {
        self.set_message_listener(message_listener);
    }

    pub fn allocate_message_queue_strategy(&self) -> Option<Arc<dyn AllocateMessageQueueStrategy>> {
        self.consumer_config.allocate_message_queue_strategy.clone()
    }

    pub fn get_allocate_message_queue_strategy(&self) -> Option<Arc<dyn AllocateMessageQueueStrategy>> {
        self.allocate_message_queue_strategy()
    }

    pub fn set_allocate_message_queue_strategy(
        &mut self,
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    ) {
        self.consumer_config
            .set_allocate_message_queue_strategy(allocate_message_queue_strategy);
    }

    pub fn consume_concurrently_max_span(&self) -> u32 {
        self.consumer_config.consume_concurrently_max_span
    }

    pub fn get_consume_concurrently_max_span(&self) -> u32 {
        self.consume_concurrently_max_span()
    }

    pub fn set_consume_concurrently_max_span(&mut self, consume_concurrently_max_span: u32) {
        self.consumer_config
            .set_consume_concurrently_max_span(consume_concurrently_max_span);
    }

    pub fn consume_from_where(&self) -> ConsumeFromWhere {
        self.consumer_config.consume_from_where
    }

    pub fn get_consume_from_where(&self) -> ConsumeFromWhere {
        self.consume_from_where()
    }

    pub fn set_consume_from_where(&mut self, consume_from_where: ConsumeFromWhere) {
        self.consumer_config.set_consume_from_where(consume_from_where);
    }

    pub fn consume_message_batch_max_size(&self) -> u32 {
        self.consumer_config.consume_message_batch_max_size
    }

    pub fn get_consume_message_batch_max_size(&self) -> u32 {
        self.consume_message_batch_max_size()
    }

    pub fn set_consume_message_batch_max_size(&mut self, consume_message_batch_max_size: u32) {
        self.consumer_config
            .set_consume_message_batch_max_size(consume_message_batch_max_size);
    }

    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_config.consumer_group
    }

    pub fn get_consumer_group(&self) -> &CheetahString {
        self.consumer_group()
    }

    pub fn get_subscription(&self) -> &ArcMut<HashMap<CheetahString, CheetahString>> {
        &self.consumer_config.subscription
    }

    #[inline]
    pub fn set_consumer_group(&mut self, consumer_group: impl Into<CheetahString>) {
        self.consumer_config.set_consumer_group(consumer_group.into());
    }

    pub fn consume_thread_max(&self) -> u32 {
        self.consumer_config.consume_thread_max
    }

    pub fn get_consume_thread_max(&self) -> u32 {
        self.consume_thread_max()
    }

    pub fn set_consume_thread_max(&mut self, consume_thread_max: u32) {
        self.consumer_config.set_consume_thread_max(consume_thread_max);
    }

    pub fn consume_thread_min(&self) -> u32 {
        self.consumer_config.consume_thread_min
    }

    pub fn get_consume_thread_min(&self) -> u32 {
        self.consume_thread_min()
    }

    pub fn set_consume_thread_min(&mut self, consume_thread_min: u32) {
        self.consumer_config.set_consume_thread_min(consume_thread_min);
    }

    pub fn message_model(&self) -> MessageModel {
        self.consumer_config.message_model
    }

    pub fn get_message_model(&self) -> MessageModel {
        self.message_model()
    }

    pub fn set_message_model(&mut self, message_model: MessageModel) {
        self.consumer_config.set_message_model(message_model);
    }

    pub fn pull_batch_size(&self) -> u32 {
        self.consumer_config.pull_batch_size
    }

    pub fn get_pull_batch_size(&self) -> u32 {
        self.pull_batch_size()
    }

    pub fn set_pull_batch_size(&mut self, pull_batch_size: u32) {
        self.consumer_config.set_pull_batch_size(pull_batch_size);
    }

    pub fn pull_interval(&self) -> u64 {
        self.consumer_config.pull_interval
    }

    pub fn get_pull_interval(&self) -> u64 {
        self.pull_interval()
    }

    pub fn set_pull_interval(&mut self, pull_interval: u64) {
        self.consumer_config.set_pull_interval(pull_interval);
    }

    pub fn pull_threshold_for_queue(&self) -> u32 {
        self.consumer_config.pull_threshold_for_queue
    }

    pub fn get_pull_threshold_for_queue(&self) -> u32 {
        self.pull_threshold_for_queue()
    }

    pub fn set_pull_threshold_for_queue(&mut self, pull_threshold_for_queue: u32) {
        self.consumer_config
            .set_pull_threshold_for_queue(pull_threshold_for_queue);
    }

    pub fn pop_threshold_for_queue(&self) -> u32 {
        self.consumer_config.pop_threshold_for_queue
    }

    pub fn get_pop_threshold_for_queue(&self) -> u32 {
        self.pop_threshold_for_queue()
    }

    pub fn set_pop_threshold_for_queue(&mut self, pop_threshold_for_queue: u32) {
        self.consumer_config
            .set_pop_threshold_for_queue(pop_threshold_for_queue);
    }

    pub fn pull_threshold_for_topic(&self) -> i32 {
        self.consumer_config.pull_threshold_for_topic
    }

    pub fn get_pull_threshold_for_topic(&self) -> i32 {
        self.pull_threshold_for_topic()
    }

    pub fn set_pull_threshold_for_topic(&mut self, pull_threshold_for_topic: i32) {
        self.consumer_config
            .set_pull_threshold_for_topic(pull_threshold_for_topic);
    }

    pub fn pull_threshold_size_for_queue(&self) -> u32 {
        self.consumer_config.pull_threshold_size_for_queue
    }

    pub fn get_pull_threshold_size_for_queue(&self) -> u32 {
        self.pull_threshold_size_for_queue()
    }

    pub fn set_pull_threshold_size_for_queue(&mut self, pull_threshold_size_for_queue: u32) {
        self.consumer_config
            .set_pull_threshold_size_for_queue(pull_threshold_size_for_queue);
    }

    pub fn pull_threshold_size_for_topic(&self) -> i32 {
        self.consumer_config.pull_threshold_size_for_topic
    }

    pub fn get_pull_threshold_size_for_topic(&self) -> i32 {
        self.pull_threshold_size_for_topic()
    }

    pub fn set_pull_threshold_size_for_topic(&mut self, pull_threshold_size_for_topic: i32) {
        self.consumer_config
            .set_pull_threshold_size_for_topic(pull_threshold_size_for_topic);
    }

    pub fn consume_timestamp(&self) -> Option<CheetahString> {
        self.consumer_config.consume_timestamp.clone()
    }

    pub fn get_consume_timestamp(&self) -> Option<CheetahString> {
        self.consume_timestamp()
    }

    pub fn set_consume_timestamp(&mut self, consume_timestamp: impl Into<CheetahString>) {
        self.consumer_config
            .set_consume_timestamp(Some(consume_timestamp.into()));
    }

    pub fn clear_consume_timestamp(&mut self) {
        self.consumer_config.set_consume_timestamp(None);
    }

    pub fn is_post_subscription_when_pull(&self) -> bool {
        self.consumer_config.post_subscription_when_pull
    }

    pub fn set_post_subscription_when_pull(&mut self, post_subscription_when_pull: bool) {
        self.consumer_config
            .set_post_subscription_when_pull(post_subscription_when_pull);
    }

    pub fn is_unit_mode(&self) -> bool {
        self.consumer_config.unit_mode
    }

    pub fn set_unit_mode(&mut self, unit_mode: bool) {
        self.consumer_config.set_unit_mode(unit_mode);
    }

    pub fn adjust_thread_pool_nums_threshold(&self) -> u64 {
        self.consumer_config.adjust_thread_pool_nums_threshold
    }

    pub fn get_adjust_thread_pool_nums_threshold(&self) -> u64 {
        self.adjust_thread_pool_nums_threshold()
    }

    pub fn set_adjust_thread_pool_nums_threshold(&mut self, adjust_thread_pool_nums_threshold: u64) {
        self.consumer_config
            .set_adjust_thread_pool_nums_threshold(adjust_thread_pool_nums_threshold);
    }

    pub fn max_reconsume_times(&self) -> i32 {
        self.consumer_config.max_reconsume_times
    }

    pub fn get_max_reconsume_times(&self) -> i32 {
        self.max_reconsume_times()
    }

    pub fn set_max_reconsume_times(&mut self, max_reconsume_times: i32) {
        self.consumer_config.set_max_reconsume_times(max_reconsume_times);
    }

    pub fn suspend_current_queue_time_millis(&self) -> u64 {
        self.consumer_config.suspend_current_queue_time_millis
    }

    pub fn get_suspend_current_queue_time_millis(&self) -> u64 {
        self.suspend_current_queue_time_millis()
    }

    pub fn set_suspend_current_queue_time_millis(&mut self, suspend_current_queue_time_millis: u64) {
        self.consumer_config
            .set_suspend_current_queue_time_millis(suspend_current_queue_time_millis);
    }

    pub fn consume_timeout(&self) -> u64 {
        self.consumer_config.consume_timeout
    }

    pub fn get_consume_timeout(&self) -> u64 {
        self.consume_timeout()
    }

    pub fn set_consume_timeout(&mut self, consume_timeout: u64) {
        self.consumer_config.set_consume_timeout(consume_timeout);
    }

    pub fn pop_invisible_time(&self) -> u64 {
        self.consumer_config.pop_invisible_time
    }

    pub fn get_pop_invisible_time(&self) -> u64 {
        self.pop_invisible_time()
    }

    pub fn set_pop_invisible_time(&mut self, pop_invisible_time: u64) {
        self.consumer_config.set_pop_invisible_time(pop_invisible_time);
    }

    pub fn await_termination_millis_when_shutdown(&self) -> u64 {
        self.consumer_config.await_termination_millis_when_shutdown
    }

    pub fn get_await_termination_millis_when_shutdown(&self) -> u64 {
        self.await_termination_millis_when_shutdown()
    }

    pub fn set_await_termination_millis_when_shutdown(&mut self, await_termination_millis_when_shutdown: u64) {
        self.consumer_config
            .set_await_termination_millis_when_shutdown(await_termination_millis_when_shutdown);
    }

    pub fn pull_batch_size_in_bytes(&self) -> u32 {
        self.consumer_config.pull_batch_size_in_bytes
    }

    pub fn get_pull_batch_size_in_bytes(&self) -> u32 {
        self.pull_batch_size_in_bytes()
    }

    pub fn set_pull_batch_size_in_bytes(&mut self, pull_batch_size_in_bytes: u32) {
        self.consumer_config
            .set_pull_batch_size_in_bytes(pull_batch_size_in_bytes);
    }

    pub fn trace_dispatcher(&self) -> Option<ArcTraceDispatcher> {
        self.consumer_config.trace_dispatcher.clone()
    }

    pub fn get_trace_dispatcher(&self) -> Option<ArcTraceDispatcher> {
        self.trace_dispatcher()
    }

    pub fn is_use_tls(&self) -> bool {
        self.client_config.is_use_tls()
    }

    pub fn set_use_tls(&mut self, use_tls: bool) {
        self.client_config.set_use_tls(use_tls);
        if let Some(consumer_impl) = self.default_mqpush_consumer_impl.as_mut() {
            consumer_impl.client_config.set_use_tls(use_tls);
            consumer_impl.rebalance_impl.client_config.set_use_tls(use_tls);
        }
        if let Some(trace_dispatcher) = self.consumer_config.trace_dispatcher.as_ref() {
            if let Some(async_dispatcher) = trace_dispatcher.as_any().downcast_ref::<AsyncTraceDispatcher>() {
                async_dispatcher.set_use_tls(use_tls);
            }
        }
    }

    pub fn offset_store(&self) -> Option<ArcMut<OffsetStore>> {
        self.consumer_config.offset_store().or_else(|| {
            self.default_mqpush_consumer_impl
                .as_ref()
                .and_then(|consumer_impl| consumer_impl.offset_store())
        })
    }

    pub fn get_offset_store(&self) -> Option<ArcMut<OffsetStore>> {
        self.offset_store()
    }

    pub fn set_offset_store(&mut self, offset_store: Option<ArcMut<OffsetStore>>) {
        self.consumer_config.set_offset_store(offset_store.clone());
        if let Some(consumer_impl) = self.default_mqpush_consumer_impl.as_mut() {
            consumer_impl.set_offset_store(offset_store);
        }
    }

    pub fn pop_batch_nums(&self) -> u32 {
        self.consumer_config.pop_batch_nums
    }

    pub fn get_pop_batch_nums(&self) -> u32 {
        self.pop_batch_nums()
    }

    pub fn set_pop_batch_nums(&mut self, pop_batch_nums: u32) {
        self.consumer_config.set_pop_batch_nums(pop_batch_nums);
    }

    pub fn is_client_rebalance(&self) -> bool {
        self.consumer_config.client_rebalance
    }

    pub fn set_client_rebalance(&mut self, client_rebalance: bool) {
        self.consumer_config.set_client_rebalance(client_rebalance);
    }

    pub fn message_queue_listener(&self) -> Option<ArcMessageQueueListener> {
        self.consumer_config.message_queue_listener.clone()
    }

    pub fn get_message_queue_listener(&self) -> Option<ArcMessageQueueListener> {
        self.message_queue_listener()
    }

    pub fn set_message_queue_listener(&mut self, message_queue_listener: Option<ArcMessageQueueListener>) {
        self.consumer_config.set_message_queue_listener(message_queue_listener);
    }

    pub fn new(client_config: ClientConfig, consumer_config: ConsumerConfig) -> DefaultMQPushConsumer {
        let consumer_config = ArcMut::new(consumer_config);
        let mut default_mqpush_consumer_impl = ArcMut::new(DefaultMQPushConsumerImpl::new(
            client_config.clone(),
            consumer_config.clone(),
            consumer_config.rpc_hook.clone(),
        ));
        let wrapper = default_mqpush_consumer_impl.clone();
        default_mqpush_consumer_impl.set_default_mqpush_consumer_impl(wrapper);
        DefaultMQPushConsumer {
            client_config,
            consumer_config,
            default_mqpush_consumer_impl: Some(default_mqpush_consumer_impl),
        }
    }

    pub fn set_name_server_addr(&mut self, name_server_addr: CheetahString) {
        self.client_config.namesrv_addr = Some(name_server_addr);
        self.client_config
            .namespace_initialized
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::base::access_channel::AccessChannel;
    use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
    use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
    use crate::consumer::message_queue_listener::MessageQueueListener;
    use crate::consumer::mq_consumer_inner::MQConsumerInner;
    use crate::consumer::store::read_offset_type::ReadOffsetType;
    use crate::hook::consume_message_context::ConsumeMessageContext;
    use crate::hook::consume_message_hook::ConsumeMessageHook;
    use crate::trace::trace_dispatcher::TraceDispatcher;

    #[derive(Default)]
    struct CapturingTraceDispatcher {
        start_count: AtomicUsize,
        shutdown_count: AtomicUsize,
        append_count: AtomicUsize,
        last_name_srv_addr: StdMutex<Option<String>>,
        last_access_channel: StdMutex<Option<AccessChannel>>,
    }

    impl TraceDispatcher for CapturingTraceDispatcher {
        fn start(&self, name_srv_addr: &str, access_channel: AccessChannel) -> rocketmq_error::RocketMQResult<()> {
            self.start_count.fetch_add(1, Ordering::SeqCst);
            *self.last_name_srv_addr.lock().expect("name server lock") = Some(name_srv_addr.to_string());
            *self.last_access_channel.lock().expect("access channel lock") = Some(access_channel);
            Ok(())
        }

        fn append(&self, _ctx: &dyn Any) -> bool {
            self.append_count.fetch_add(1, Ordering::SeqCst);
            true
        }

        fn flush(&self) -> rocketmq_error::RocketMQResult<()> {
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

    struct TestConcurrentListener;

    impl MessageListenerConcurrently for TestConcurrentListener {
        fn consume_message(
            &self,
            _msgs: &[&MessageExt],
            _context: &ConsumeConcurrentlyContext,
        ) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus> {
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        }
    }

    struct TestConsumeHook;

    impl ConsumeMessageHook for TestConsumeHook {
        fn hook_name(&self) -> &'static str {
            "TestConsumeHook"
        }

        fn consume_message_before(&self, _context: &mut ConsumeMessageContext) {}

        fn consume_message_after(&self, _context: &mut ConsumeMessageContext) {}
    }

    struct TestMessageQueueListener;

    impl MessageQueueListener for TestMessageQueueListener {
        fn message_queue_changed(
            &self,
            _topic: &str,
            _mq_all: &HashSet<MessageQueue>,
            _mq_assigned: &HashSet<MessageQueue>,
        ) {
        }
    }

    fn namespaced_push_consumer() -> DefaultMQPushConsumer {
        let mut client_config = ClientConfig::default();
        client_config.set_namespace(CheetahString::from_static_str("ns"));

        DefaultMQPushConsumer::builder()
            .consumer_group("push_namespace_group")
            .client_config(client_config)
            .build()
    }

    #[test]
    fn builder_preserves_default_allocate_message_queue_strategy() {
        let consumer = DefaultMQPushConsumer::builder()
            .consumer_group("builder_default_strategy_group")
            .build();

        let strategy = consumer
            .consumer_config
            .allocate_message_queue_strategy
            .as_ref()
            .expect("default allocation strategy should be preserved");
        assert_eq!("AVG", strategy.get_name());
    }

    #[tokio::test]
    async fn subscribe_without_impl_returns_not_initialized_error() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_missing_impl_group")
            .build();
        consumer.default_mqpush_consumer_impl = None;

        let error = consumer
            .subscribe("topic", "*")
            .await
            .expect_err("subscribe without impl should fail");

        assert!(error.to_string().contains("DefaultMQPushConsumerImpl"));
    }

    #[tokio::test]
    async fn mq_admin_facade_without_impl_returns_not_initialized_error() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_missing_admin_impl_group")
            .build();
        consumer.default_mqpush_consumer_impl = None;
        let mq = MessageQueue::from_parts("TopicA", "broker-a", 0);

        let max_error = MQConsumer::max_offset(&mut consumer, &mq)
            .await
            .expect_err("maxOffset without impl should fail");
        let query_error = MQConsumer::query_message(&mut consumer, "TopicA", "KeyA", 32, 0, 1)
            .await
            .expect_err("queryMessage without impl should fail");
        let view_error = MQConsumer::view_message(&mut consumer, "TopicA", "bad-msg-id")
            .await
            .expect_err("viewMessage without impl should fail");

        assert!(max_error.to_string().contains("DefaultMQPushConsumerImpl"));
        assert!(query_error.to_string().contains("DefaultMQPushConsumerImpl"));
        assert!(view_error.to_string().contains("DefaultMQPushConsumerImpl"));
    }

    #[tokio::test]
    async fn consumer_running_info_without_impl_returns_not_initialized_error() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_missing_running_info_impl_group")
            .build();
        consumer.default_mqpush_consumer_impl = None;

        let error = consumer
            .consumer_running_info()
            .await
            .expect_err("running info without impl should fail");

        assert!(error.to_string().contains("DefaultMQPushConsumerImpl"));
    }

    #[tokio::test]
    async fn consumer_running_info_facade_returns_impl_snapshot_like_java_admin() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_running_info_group")
            .consume_thread_min(7)
            .consume_thread_max(11)
            .pull_batch_size(13)
            .consume_message_batch_max_size(3)
            .build();

        consumer
            .subscribe("TopicRunningInfo", "TagA")
            .await
            .expect("subscribe should update running-info subscriptions");

        let info = consumer
            .consumer_running_info()
            .await
            .expect("running info facade should delegate to impl");

        assert_eq!(
            info.properties
                .get(ConsumerRunningInfo::PROP_CONSUME_TYPE)
                .map(String::as_str),
            Some("CONSUME_PASSIVELY")
        );
        assert_eq!(
            info.properties
                .get(ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE)
                .map(String::as_str),
            Some("7")
        );
        assert_eq!(info.properties.get("consumeThreadMax").map(String::as_str), Some("11"));
        assert_eq!(info.properties.get("pullBatchSize").map(String::as_str), Some("13"));
        assert!(info
            .subscription_set
            .iter()
            .any(|subscription| subscription.topic.as_str() == "TopicRunningInfo"
                && subscription.sub_string.as_str() == "TagA"));
    }

    #[test]
    fn register_listener_without_impl_stores_listener_without_panic() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_missing_impl_group")
            .build();
        consumer.default_mqpush_consumer_impl = None;

        consumer.register_message_listener_concurrently(TestConcurrentListener);

        assert!(consumer.consumer_config.message_listener.is_some());
    }

    #[test]
    fn push_message_listener_facade_syncs_config_and_impl_like_java() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_listener_facade_group")
            .build();
        let listener = ArcMut::new(MessageListener::new(Some(Arc::new(TestConcurrentListener)), None));

        assert!(consumer.default_mq_push_consumer_impl().is_some());
        assert!(consumer.message_listener().is_none());

        consumer.set_message_listener(Some(listener.clone()));

        assert!(consumer
            .message_listener()
            .expect("listener should be set")
            .is_concurrently());
        assert!(consumer
            .consumer_config
            .message_listener
            .as_ref()
            .expect("config listener should be set")
            .is_concurrently());

        consumer.register_message_listener(None);

        assert!(consumer.message_listener().is_none());
        assert!(consumer.consumer_config.message_listener.is_none());
    }

    #[test]
    fn push_use_tls_facade_updates_impl_config_like_java() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_tls_facade_group")
            .build();

        assert!(!consumer.is_use_tls());

        consumer.set_use_tls(true);

        assert!(consumer.is_use_tls());
        let consumer_impl = consumer
            .default_mqpush_consumer_impl
            .as_ref()
            .expect("push consumer impl should exist");
        assert!(consumer_impl.client_config.is_use_tls());
        assert!(consumer_impl.rebalance_impl.client_config.is_use_tls());
    }

    #[test]
    fn push_java_getter_aliases_delegate_to_config() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_java_getter_group")
            .build();
        let offset_store = ArcMut::new(OffsetStore::new_test());

        consumer.set_message_model(MessageModel::Broadcasting);
        consumer.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
        consumer.set_consume_timestamp("20260101000000");
        consumer.set_consume_thread_min(8);
        consumer.set_consume_thread_max(16);
        consumer.set_consume_concurrently_max_span(321);
        consumer.set_consume_message_batch_max_size(7);
        consumer.set_pull_batch_size(11);
        consumer.set_pull_interval(13);
        consumer.set_pull_threshold_for_queue(17);
        consumer.set_pull_threshold_for_topic(19);
        consumer.set_pull_threshold_size_for_queue(23);
        consumer.set_pull_threshold_size_for_topic(29);
        consumer.set_consume_timeout(31);
        consumer.set_offset_store(Some(offset_store));

        assert!(consumer.get_allocate_message_queue_strategy().is_some());
        assert_eq!(consumer.get_message_model(), MessageModel::Broadcasting);
        assert_eq!(
            consumer.get_consume_from_where(),
            ConsumeFromWhere::ConsumeFromFirstOffset
        );
        assert_eq!(
            consumer.get_consume_timestamp(),
            Some(CheetahString::from_static_str("20260101000000"))
        );
        assert_eq!(consumer.get_consume_thread_min(), 8);
        assert_eq!(consumer.get_consume_thread_max(), 16);
        assert_eq!(consumer.get_consume_concurrently_max_span(), 321);
        assert_eq!(consumer.get_consume_message_batch_max_size(), 7);
        assert_eq!(consumer.get_pull_batch_size(), 11);
        assert_eq!(consumer.get_pull_interval(), 13);
        assert_eq!(consumer.get_pull_threshold_for_queue(), 17);
        assert_eq!(consumer.get_pull_threshold_for_topic(), 19);
        assert_eq!(consumer.get_pull_threshold_size_for_queue(), 23);
        assert_eq!(consumer.get_pull_threshold_size_for_topic(), 29);
        assert_eq!(consumer.get_consume_timeout(), 31);
        assert!(consumer.get_offset_store().is_some());
        assert!(consumer.get_subscription().is_empty());
    }

    #[tokio::test]
    async fn push_offset_store_facade_updates_impl_like_java_start_source() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_offset_store_facade_group")
            .build();
        let offset_store = ArcMut::new(OffsetStore::new_test());
        let mq = MessageQueue::from_parts("TopicOffset", "broker-a", 0);

        assert!(consumer.offset_store().is_none());

        consumer.set_offset_store(Some(offset_store.clone()));
        offset_store.update_offset(&mq, 42, false).await;

        assert_eq!(
            consumer
                .offset_store()
                .expect("facade offset store should be present")
                .read_offset(&mq, ReadOffsetType::ReadFromMemory)
                .await,
            42
        );
        assert_eq!(
            consumer
                .default_mqpush_consumer_impl
                .as_ref()
                .expect("push consumer impl should exist")
                .offset_store()
                .expect("impl offset store should be present")
                .read_offset(&mq, ReadOffsetType::ReadFromMemory)
                .await,
            42
        );

        consumer.set_offset_store(None);
        assert!(consumer.offset_store().is_none());
        assert!(consumer
            .default_mqpush_consumer_impl
            .as_ref()
            .expect("push consumer impl should exist")
            .offset_store()
            .is_none());
    }

    #[test]
    fn register_consume_message_hook_facade_updates_impl_like_java() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_hook_group")
            .build();

        consumer
            .register_consume_message_hook(TestConsumeHook)
            .expect("hook registration should succeed");

        assert_eq!(
            consumer
                .default_mqpush_consumer_impl
                .as_ref()
                .expect("push consumer impl should exist")
                .consume_message_hook_count(),
            1
        );
    }

    #[test]
    fn register_consume_message_hook_without_impl_returns_typed_error() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_missing_impl_hook_group")
            .build();
        consumer.default_mqpush_consumer_impl = None;

        let error = consumer
            .register_consume_message_hook(TestConsumeHook)
            .expect_err("hook registration without impl should fail");

        assert!(error.to_string().contains("DefaultMQPushConsumerImpl"));
    }

    #[test]
    fn push_runtime_config_facade_updates_shared_impl_config_like_java() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_runtime_config_group")
            .build();
        let listener: ArcMessageQueueListener = Arc::new(TestMessageQueueListener);

        consumer.set_allocate_message_queue_strategy(Arc::new(AllocateMessageQueueAveragely));
        consumer.set_consume_concurrently_max_span(1234);
        consumer.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
        consumer.set_consume_message_batch_max_size(8);
        consumer.set_consumer_group("push_runtime_config_group_updated");
        consumer.set_consume_thread_max(48);
        consumer.set_consume_thread_min(12);
        consumer.set_message_model(MessageModel::Broadcasting);
        consumer.set_pull_batch_size(64);
        consumer.set_pull_interval(15);
        consumer.set_pull_threshold_for_queue(2048);
        consumer.set_pop_threshold_for_queue(128);
        consumer.set_pull_threshold_for_topic(4096);
        consumer.set_pull_threshold_size_for_queue(256);
        consumer.set_pull_threshold_size_for_topic(512);
        consumer.set_consume_timestamp("20260610101010");
        consumer.set_post_subscription_when_pull(true);
        consumer.set_unit_mode(true);
        consumer.set_adjust_thread_pool_nums_threshold(222_222);
        consumer.set_max_reconsume_times(19);
        consumer.set_suspend_current_queue_time_millis(3333);
        consumer.set_consume_timeout(44);
        consumer.set_pop_invisible_time(55_555);
        consumer.set_await_termination_millis_when_shutdown(66_666);
        consumer.set_pull_batch_size_in_bytes(512 * 1024);
        consumer.set_pop_batch_nums(16);
        consumer.set_client_rebalance(false);
        consumer.set_message_queue_listener(Some(listener));

        assert_eq!(consumer.consume_concurrently_max_span(), 1234);
        assert_eq!(consumer.consume_from_where(), ConsumeFromWhere::ConsumeFromFirstOffset);
        assert_eq!(consumer.consume_message_batch_max_size(), 8);
        assert_eq!(consumer.consumer_group().as_str(), "push_runtime_config_group_updated");
        assert_eq!(consumer.consume_thread_max(), 48);
        assert_eq!(consumer.consume_thread_min(), 12);
        assert_eq!(consumer.message_model(), MessageModel::Broadcasting);
        assert_eq!(consumer.pull_batch_size(), 64);
        assert_eq!(consumer.pull_interval(), 15);
        assert_eq!(consumer.pull_threshold_for_queue(), 2048);
        assert_eq!(consumer.pop_threshold_for_queue(), 128);
        assert_eq!(consumer.pull_threshold_for_topic(), 4096);
        assert_eq!(consumer.pull_threshold_size_for_queue(), 256);
        assert_eq!(consumer.pull_threshold_size_for_topic(), 512);
        assert_eq!(
            consumer.consume_timestamp().as_ref().map(CheetahString::as_str),
            Some("20260610101010")
        );
        assert!(consumer.is_post_subscription_when_pull());
        assert!(consumer.is_unit_mode());
        assert_eq!(consumer.adjust_thread_pool_nums_threshold(), 222_222);
        assert_eq!(consumer.max_reconsume_times(), 19);
        assert_eq!(consumer.suspend_current_queue_time_millis(), 3333);
        assert_eq!(consumer.consume_timeout(), 44);
        assert_eq!(consumer.pop_invisible_time(), 55_555);
        assert_eq!(consumer.await_termination_millis_when_shutdown(), 66_666);
        assert_eq!(consumer.pull_batch_size_in_bytes(), 512 * 1024);
        assert_eq!(consumer.pop_batch_nums(), 16);
        assert!(!consumer.is_client_rebalance());
        assert!(consumer.message_queue_listener().is_some());
        assert!(consumer.allocate_message_queue_strategy().is_some());

        let impl_config = &consumer
            .default_mqpush_consumer_impl
            .as_ref()
            .expect("push consumer impl should exist")
            .consumer_config;

        assert_eq!(impl_config.consume_concurrently_max_span, 1234);
        assert_eq!(impl_config.consume_from_where, ConsumeFromWhere::ConsumeFromFirstOffset);
        assert_eq!(impl_config.consume_message_batch_max_size, 8);
        assert_eq!(impl_config.consumer_group.as_str(), "push_runtime_config_group_updated");
        assert_eq!(impl_config.consume_thread_max, 48);
        assert_eq!(impl_config.consume_thread_min, 12);
        assert_eq!(impl_config.message_model, MessageModel::Broadcasting);
        assert_eq!(impl_config.pull_batch_size, 64);
        assert_eq!(impl_config.pull_interval, 15);
        assert_eq!(impl_config.pull_threshold_for_queue, 2048);
        assert_eq!(impl_config.pop_threshold_for_queue, 128);
        assert_eq!(impl_config.pull_threshold_for_topic, 4096);
        assert_eq!(impl_config.pull_threshold_size_for_queue, 256);
        assert_eq!(impl_config.pull_threshold_size_for_topic, 512);
        assert_eq!(
            impl_config.consume_timestamp.as_ref().map(CheetahString::as_str),
            Some("20260610101010")
        );
        assert!(impl_config.post_subscription_when_pull);
        assert!(impl_config.unit_mode);
        assert_eq!(impl_config.adjust_thread_pool_nums_threshold, 222_222);
        assert_eq!(impl_config.max_reconsume_times, 19);
        assert_eq!(impl_config.suspend_current_queue_time_millis, 3333);
        assert_eq!(impl_config.consume_timeout, 44);
        assert_eq!(impl_config.pop_invisible_time, 55_555);
        assert_eq!(impl_config.await_termination_millis_when_shutdown, 66_666);
        assert_eq!(impl_config.pull_batch_size_in_bytes, 512 * 1024);
        assert_eq!(impl_config.pop_batch_nums, 16);
        assert!(!impl_config.client_rebalance);
        assert!(impl_config.message_queue_listener.is_some());

        consumer.clear_consume_timestamp();
        assert!(consumer.consume_timestamp().is_none());
    }

    #[tokio::test]
    async fn push_pause_state_facade_matches_impl_like_java() {
        let consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_pause_state_group")
            .build();

        assert!(!consumer.is_pause());
        consumer.suspend().await;
        assert!(consumer.is_pause());
        consumer.resume().await;
        assert!(!consumer.is_pause());
    }

    #[test]
    fn is_consume_orderly_facade_reflects_impl_state_like_java() {
        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_orderly_state_group")
            .build();

        assert!(!consumer.is_consume_orderly());

        consumer
            .default_mqpush_consumer_impl
            .as_mut()
            .expect("push consumer impl should exist")
            .set_consume_orderly(true);

        assert!(consumer.is_consume_orderly());
    }

    #[tokio::test]
    async fn push_facade_namespace_subscribe_wraps_topic_like_java() {
        let mut consumer = namespaced_push_consumer();

        consumer
            .subscribe("TopicA", "TagA")
            .await
            .expect("subscribe should succeed before start");

        let subscriptions = consumer
            .default_mqpush_consumer_impl
            .as_ref()
            .expect("push consumer impl should exist")
            .subscriptions();
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
    async fn push_facade_namespace_selector_subscribe_wraps_topic_like_java() {
        let mut consumer = namespaced_push_consumer();

        consumer
            .subscribe_with_selector("TopicB", Some(MessageSelector::by_tag("TagB")))
            .await
            .expect("selector subscribe should succeed before start");

        let subscriptions = consumer
            .default_mqpush_consumer_impl
            .as_ref()
            .expect("push consumer impl should exist")
            .subscriptions();
        let subscription = subscriptions
            .iter()
            .find(|subscription| subscription.topic.as_str() == "ns%TopicB")
            .expect("namespaced selector topic should be subscribed");
        assert_eq!(subscription.sub_string.as_str(), "TagB");
        assert!(!subscriptions
            .iter()
            .any(|subscription| subscription.topic.as_str() == "TopicB"));
    }

    #[tokio::test]
    async fn push_facade_namespace_unsubscribe_passes_topic_through_like_java() {
        let mut consumer = namespaced_push_consumer();
        consumer
            .subscribe("TopicC", "*")
            .await
            .expect("subscribe should succeed before start");

        consumer.unsubscribe("TopicC").await;

        let subscriptions = consumer
            .default_mqpush_consumer_impl
            .as_ref()
            .expect("push consumer impl should exist")
            .subscriptions();
        assert!(subscriptions
            .iter()
            .any(|subscription| subscription.topic.as_str() == "ns%TopicC"));

        consumer.unsubscribe("ns%TopicC").await;

        let subscriptions = consumer
            .default_mqpush_consumer_impl
            .as_ref()
            .expect("push consumer impl should exist")
            .subscriptions();
        assert!(!subscriptions
            .iter()
            .any(|subscription| subscription.topic.as_str() == "ns%TopicC"));
    }

    #[tokio::test]
    async fn custom_trace_dispatcher_registers_hook_and_starts_with_client_config() {
        let dispatcher = Arc::new(CapturingTraceDispatcher::default());
        let trace_dispatcher: ArcTraceDispatcher = dispatcher.clone();
        let mut client_config = ClientConfig::default();
        client_config.set_namesrv_addr(CheetahString::from_static_str("127.0.0.1:9876"));
        client_config.set_access_channel(AccessChannel::Cloud);

        let mut consumer = DefaultMQPushConsumer::builder()
            .consumer_group("push_trace_group")
            .client_config(client_config)
            .trace_dispatcher(Some(trace_dispatcher))
            .build();

        let trace_dispatcher_to_start = {
            let consumer_impl = consumer
                .default_mqpush_consumer_impl
                .as_mut()
                .expect("push consumer impl should exist");
            DefaultMQPushConsumer::prepare_trace_dispatcher(
                &consumer.client_config,
                &mut consumer.consumer_config,
                consumer_impl,
            )
            .expect("custom dispatcher should be prepared")
        };

        assert_eq!(
            consumer
                .default_mqpush_consumer_impl
                .as_ref()
                .expect("push consumer impl should exist")
                .consume_message_hook_count(),
            1
        );

        consumer.start_trace_dispatcher(trace_dispatcher_to_start).await;
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
