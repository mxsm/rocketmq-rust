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
use std::thread;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::utils::util_all;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tokio::runtime::Handle;

use crate::base::client_config::ClientConfig;
use crate::base::mq_admin::MQAdmin;
use crate::base::query_result::QueryResult;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::default_mq_push_consumer_builder::DefaultMQPushConsumerBuilder;
use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use crate::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use crate::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use crate::consumer::listener::message_listener::MessageListener;
use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use crate::consumer::listener::message_listener_orderly::MessageListenerOrderly;
use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::mq_consumer::MQConsumer;
use crate::consumer::mq_push_consumer::MQPushConsumer;
use crate::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely;
use crate::trace::async_trace_dispatcher::AsyncTraceDispatcher;
use crate::trace::hook::consume_message_trace_hook_impl::ConsumeMessageTraceHookImpl;
use crate::trace::trace_dispatcher::TraceDispatcher;
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
    pub(crate) message_queue_listener: Option<Arc<Box<dyn MessageQueueListener>>>,
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
    pub(crate) trace_dispatcher: Option<Arc<Box<dyn TraceDispatcher + Send + Sync>>>,
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

    /*    pub fn message_queue_listener(&self) -> &Option<Arc<Box<dyn MessageQueueListener>>> {
        &self.message_queue_listener
    }*/

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

    pub fn trace_dispatcher(&self) -> &Option<Arc<Box<dyn TraceDispatcher + Send + Sync>>> {
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

    pub fn set_message_queue_listener(&mut self, message_queue_listener: Option<Arc<Box<dyn MessageQueueListener>>>) {
        self.message_queue_listener = message_queue_listener;
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

    pub fn set_trace_dispatcher(&mut self, trace_dispatcher: Option<Arc<Box<dyn TraceDispatcher + Send + Sync>>>) {
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
                (get_current_millis() - (1000 * 60 * 30)) as i64,
            ))),
            allocate_message_queue_strategy: Some(Arc::new(AllocateMessageQueueAveragely)),
            subscription: ArcMut::new(HashMap::new()),
            message_listener: None,
            message_queue_listener: None,

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
    async fn send_message_back(
        &mut self,
        msg: MessageExt,
        delay_level: i32,
        broker_name: &str,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn fetch_subscribe_message_queues(
        &mut self,
        topic: &str,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        todo!()
    }
}

impl MQAdmin for DefaultMQPushConsumer {
    fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        panic!("This method is not implemented for DefaultMQPushConsumer");
    }

    fn create_topic_with_flag(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        panic!("This method is not implemented for DefaultMQPushConsumer");
    }

    fn search_offset(&self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        panic!("This method is not implemented for DefaultMQPushConsumer");
    }

    fn max_offset(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        panic!("This method is not implemented for DefaultMQPushConsumer");
    }

    fn min_offset(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        panic!("This method is not implemented for DefaultMQPushConsumer");
    }

    fn earliest_msg_store_time(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<u64> {
        panic!("This method is not implemented for DefaultMQPushConsumer");
    }

    fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        panic!("This method is not implemented for DefaultMQPushConsumer");
    }

    fn view_message(&self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        panic!("This method is not implemented for DefaultMQPushConsumer");
    }
}

impl MQPushConsumer for DefaultMQPushConsumer {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let consumer_group = NamespaceUtil::wrap_namespace(
            self.client_config.get_namespace().unwrap_or_default().as_str(),
            self.consumer_config.consumer_group.as_str(),
        );
        self.set_consumer_group(consumer_group.as_str());
        self.default_mqpush_consumer_impl.as_mut().unwrap().start().await?;

        if self.client_config.enable_trace {
            let mut dispatcher = AsyncTraceDispatcher::new(
                self.consumer_config.consumer_group.as_str(),
                Type::Consume,
                self.client_config.trace_topic.clone().unwrap().as_str(),
                self.consumer_config.rpc_hook.clone(),
            );
            dispatcher.set_host_consumer(self.default_mqpush_consumer_impl.as_ref().unwrap().clone());
            dispatcher.set_namespace_v2(self.client_config.namespace_v2.clone());
            let dispatcher: Arc<Box<dyn TraceDispatcher + Send + Sync>> = Arc::new(Box::new(dispatcher));
            self.consumer_config.trace_dispatcher = Some(dispatcher.clone());
            let default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_mut().unwrap();
            default_mqpush_consumer_impl
                .register_consume_message_hook(ConsumeMessageTraceHookImpl::new(dispatcher.clone()));
        }

        if let Some(ref rpc_hook) = self.consumer_config.trace_dispatcher {
            unimplemented!("trace hook");
        }

        Ok(())
    }

    async fn shutdown(&mut self) {
        todo!()
    }

    fn register_message_listener_concurrently_fn<MLCFN>(&mut self, message_listener: MLCFN)
    where
        MLCFN: Fn(Vec<MessageExt>, ConsumeConcurrentlyContext) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus>
            + Send
            + Sync,
    {
        todo!()
    }

    fn register_message_listener_concurrently<ML>(&mut self, message_listener: ML)
    where
        ML: MessageListenerConcurrently + Send + Sync + 'static,
    {
        let message_listener = MessageListener {
            message_listener_concurrently: Some((Some(Arc::new(Box::new(message_listener))), None)),
            message_listener_orderly: None,
        };
        self.consumer_config.message_listener = Some(ArcMut::new(message_listener));
        self.default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .register_message_listener(self.consumer_config.message_listener.clone());
    }

    async fn register_message_listener_orderly_fn<MLOFN>(&mut self, message_listener: MLOFN)
    where
        MLOFN: Fn(Vec<MessageExt>, ConsumeOrderlyContext) -> rocketmq_error::RocketMQResult<ConsumeOrderlyStatus>
            + Send
            + Sync,
    {
        todo!()
    }

    fn register_message_listener_orderly<ML>(&mut self, message_listener: ML)
    where
        ML: MessageListenerOrderly + Send + Sync + 'static,
    {
        let message_listener = MessageListener {
            message_listener_concurrently: None,
            message_listener_orderly: Some((Some(Arc::new(Box::new(message_listener))), None)),
        };
        self.consumer_config.message_listener = Some(ArcMut::new(message_listener));
        self.default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .register_message_listener(self.consumer_config.message_listener.clone());
    }

    fn subscribe(&mut self, topic: &str, sub_expression: &str) -> rocketmq_error::RocketMQResult<()> {
        let handle = Handle::current();
        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.clone();
        let topic = topic.to_string();
        let sub_expression = sub_expression.to_string();
        match thread::spawn(move || {
            handle.block_on(async move {
                default_mqpush_consumer_impl
                    .as_mut()
                    .unwrap()
                    .subscribe(topic.into(), sub_expression.into())
                    .await
            })
        })
        .join()
        {
            Ok(value) => value,
            Err(er) => {
                panic!("Error: {er:?}");
            }
        }
    }

    async fn subscribe_with_selector(
        &mut self,
        topic: &str,
        selector: Option<MessageSelector>,
    ) -> rocketmq_error::RocketMQResult<()> {
        todo!()
    }

    async fn unsubscribe(&mut self, topic: &str) {
        todo!()
    }

    async fn suspend(&mut self) {
        todo!()
    }

    async fn resume(&mut self) {
        todo!()
    }
}

impl DefaultMQPushConsumer {
    pub fn builder() -> DefaultMQPushConsumerBuilder {
        DefaultMQPushConsumerBuilder::default()
    }

    #[inline]
    pub fn set_consumer_group(&mut self, consumer_group: impl Into<CheetahString>) {
        self.consumer_config.consumer_group = consumer_group.into();
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

    pub fn set_consume_from_where(&mut self, consume_from_where: ConsumeFromWhere) {
        self.consumer_config.consume_from_where = consume_from_where;
    }
}
