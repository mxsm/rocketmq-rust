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

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::compression::compression_type::CompressionType;
use rocketmq_common::common::compression::compressor::Compressor;
use rocketmq_common::common::compression::compressor_factory::CompressorFactory;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MESSAGE_COMPRESS_LEVEL;
use rocketmq_common::common::mix_all::MESSAGE_COMPRESS_TYPE;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tracing::error;

use crate::base::client_config::ClientConfig;
use crate::base::validators::Validators;
use crate::producer::default_mq_produce_builder::DefaultMQProducerBuilder;
use crate::producer::mq_producer::MQProducer;
use crate::producer::produce_accumulator::ProduceAccumulator;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::send_callback::SendMessageCallback;
use crate::producer::send_result::SendResult;
use crate::producer::transaction_send_result::TransactionSendResult;
use crate::trace::async_trace_dispatcher::AsyncTraceDispatcher;
use crate::trace::hook::end_transaction_trace_hook_impl::EndTransactionTraceHookImpl;
use crate::trace::hook::send_message_trace_hook_impl::SendMessageTraceHookImpl;
use crate::trace::trace_dispatcher::TraceDispatcher;
use crate::trace::trace_dispatcher::Type;

#[derive(Clone)]
pub struct ProducerConfig {
    retry_response_codes: HashSet<i32>,
    /// Producer group conceptually aggregates all producer instances of exactly same role, which
    /// is particularly important when transactional messages are involved.
    ///
    /// For non-transactional messages, it does not matter as long as it's unique per process.
    ///
    /// See [core concepts](https://rocketmq.apache.org/docs/introduction/02concepts) for more discussion.
    producer_group: CheetahString,
    /// Topics that need to be initialized for transaction producer
    topics: Vec<CheetahString>,
    create_topic_key: CheetahString,
    /// Number of queues to create per default topic.
    default_topic_queue_nums: u32,
    /// Timeout for sending messages.
    send_msg_timeout: u32,
    /// Compress message body threshold, namely, message body
    /// larger than 4k will be compressed on default.
    compress_msg_body_over_howmuch: u32,
    /// Maximum number of retry to perform internally before claiming sending failure in
    /// synchronous mode. This may potentially cause message duplication which is up to application
    /// developers to resolve.
    retry_times_when_send_failed: u32,
    /// Maximum number of retry to perform internally before claiming sending failure in
    /// asynchronous mode.This may potentially cause message duplication which is up to application
    /// developers to resolve.
    retry_times_when_send_async_failed: u32,
    /// Indicate whether to retry another broker on sending failure internally.
    retry_another_broker_when_not_store_ok: bool,
    /// Maximum allowed message size in bytes.
    max_message_size: u32,
    trace_dispatcher: Option<Arc<Box<dyn TraceDispatcher + Send + Sync>>>,
    /// Switch flag instance for automatic batch message
    auto_batch: bool,
    /// Instance for batching message automatically
    produce_accumulator: Option<ArcMut<ProduceAccumulator>>,
    /// Indicate whether to block message when asynchronous sending traffic is too heavy.
    enable_backpressure_for_async_mode: bool,
    /// on BackpressureForAsyncMode, limit maximum number of on-going sending async messages
    /// default is 10000
    back_pressure_for_async_send_num: u32,
    /// on BackpressureForAsyncMode, limit maximum message size of on-going sending async messages
    /// default is 100M
    back_pressure_for_async_send_size: u32,
    rpc_hook: Option<Arc<dyn RPCHook>>,
    compress_level: i32,
    compress_type: CompressionType,
    compressor: Option<&'static (dyn Compressor + Send + Sync)>,
}

impl ProducerConfig {
    pub fn retry_response_codes(&self) -> &HashSet<i32> {
        &self.retry_response_codes
    }

    pub fn producer_group(&self) -> &CheetahString {
        &self.producer_group
    }

    pub fn topics(&self) -> &Vec<CheetahString> {
        &self.topics
    }

    pub fn create_topic_key(&self) -> &CheetahString {
        &self.create_topic_key
    }

    pub fn default_topic_queue_nums(&self) -> u32 {
        self.default_topic_queue_nums
    }

    pub fn send_msg_timeout(&self) -> u32 {
        self.send_msg_timeout
    }

    pub fn compress_msg_body_over_howmuch(&self) -> u32 {
        self.compress_msg_body_over_howmuch
    }

    pub fn retry_times_when_send_failed(&self) -> u32 {
        self.retry_times_when_send_failed
    }

    pub fn retry_times_when_send_async_failed(&self) -> u32 {
        self.retry_times_when_send_async_failed
    }

    #[inline]
    pub fn retry_another_broker_when_not_store_ok(&self) -> bool {
        self.retry_another_broker_when_not_store_ok
    }

    pub fn max_message_size(&self) -> u32 {
        self.max_message_size
    }

    pub fn trace_dispatcher(&self) -> Option<&Arc<Box<dyn TraceDispatcher + Send + Sync>>> {
        self.trace_dispatcher.as_ref()
    }

    pub fn auto_batch(&self) -> bool {
        self.auto_batch
    }

    pub fn produce_accumulator(&self) -> Option<&ArcMut<ProduceAccumulator>> {
        self.produce_accumulator.as_ref()
    }

    pub fn enable_backpressure_for_async_mode(&self) -> bool {
        self.enable_backpressure_for_async_mode
    }

    pub fn back_pressure_for_async_send_num(&self) -> u32 {
        self.back_pressure_for_async_send_num
    }

    pub fn back_pressure_for_async_send_size(&self) -> u32 {
        self.back_pressure_for_async_send_size
    }

    pub fn rpc_hook(&self) -> &Option<Arc<dyn RPCHook>> {
        &self.rpc_hook
    }

    pub fn compress_level(&self) -> i32 {
        self.compress_level
    }

    pub fn compress_type(&self) -> CompressionType {
        self.compress_type
    }

    pub fn compressor(&self) -> Option<&'static (dyn Compressor + Send + Sync)> {
        self.compressor
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        let mut retry_response_codes = HashSet::new();
        retry_response_codes.extend(vec![
            ResponseCode::TopicNotExist as i32,
            ResponseCode::ServiceNotAvailable as i32,
            ResponseCode::SystemError as i32,
            ResponseCode::SystemBusy as i32,
            ResponseCode::NoPermission as i32,
            ResponseCode::NoBuyerId as i32,
            ResponseCode::NotInCurrentUnit as i32,
        ]);
        let compression_type = CompressionType::of(
            std::env::var(MESSAGE_COMPRESS_TYPE)
                .unwrap_or("ZLIB".to_string())
                .as_str(),
        );
        Self {
            retry_response_codes,
            producer_group: CheetahString::empty(),
            topics: vec![],
            create_topic_key: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            default_topic_queue_nums: 4,
            send_msg_timeout: 3000,
            compress_msg_body_over_howmuch: 1024 * 4,
            retry_times_when_send_failed: 2,
            retry_times_when_send_async_failed: 2,
            retry_another_broker_when_not_store_ok: false,
            max_message_size: 1024 * 1024 * 4,
            trace_dispatcher: None,
            auto_batch: false,
            produce_accumulator: None,
            enable_backpressure_for_async_mode: false,
            back_pressure_for_async_send_num: 10000,
            back_pressure_for_async_send_size: 100 * 1024 * 1024,
            rpc_hook: None,
            compress_level: std::env::var(MESSAGE_COMPRESS_LEVEL)
                .unwrap_or("5".to_string())
                .parse()
                .unwrap(),
            compress_type: compression_type,
            compressor: Some(CompressorFactory::get_compressor(compression_type)),
        }
    }
}

#[derive(Default, Clone)]
pub struct DefaultMQProducer {
    client_config: ClientConfig,
    producer_config: ProducerConfig,
    pub(crate) default_mqproducer_impl: Option<ArcMut<DefaultMQProducerImpl>>,
}

impl DefaultMQProducer {
    #[inline]
    pub fn builder() -> DefaultMQProducerBuilder {
        DefaultMQProducerBuilder::new()
    }
    pub fn new() -> Self {
        Self::builder().build()
    }

    #[inline]
    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    #[inline]
    pub fn retry_response_codes(&self) -> &HashSet<i32> {
        &self.producer_config.retry_response_codes
    }

    #[inline]
    pub fn producer_group(&self) -> &str {
        &self.producer_config.producer_group
    }

    #[inline]
    pub fn topics(&self) -> &Vec<CheetahString> {
        &self.producer_config.topics
    }

    #[inline]
    pub fn create_topic_key(&self) -> &str {
        &self.producer_config.create_topic_key
    }

    #[inline]
    pub fn default_topic_queue_nums(&self) -> u32 {
        self.producer_config.default_topic_queue_nums
    }

    #[inline]
    pub fn send_msg_timeout(&self) -> u32 {
        self.producer_config.send_msg_timeout
    }

    #[inline]
    pub fn compress_msg_body_over_howmuch(&self) -> u32 {
        self.producer_config.compress_msg_body_over_howmuch
    }

    #[inline]
    pub fn retry_times_when_send_failed(&self) -> u32 {
        self.producer_config.retry_times_when_send_failed
    }

    #[inline]
    pub fn retry_times_when_send_async_failed(&self) -> u32 {
        self.producer_config.retry_times_when_send_async_failed
    }

    #[inline]
    pub fn retry_another_broker_when_not_store_ok(&self) -> bool {
        self.producer_config.retry_another_broker_when_not_store_ok
    }

    #[inline]
    pub fn max_message_size(&self) -> u32 {
        self.producer_config.max_message_size
    }

    #[inline]
    pub fn trace_dispatcher(&self) -> Option<&Arc<Box<dyn TraceDispatcher + Send + Sync>>> {
        self.producer_config.trace_dispatcher()
    }

    #[inline]
    pub fn auto_batch(&self) -> bool {
        self.producer_config.auto_batch
    }

    pub fn produce_accumulator(&self) -> Option<&ArcMut<ProduceAccumulator>> {
        self.producer_config.produce_accumulator()
    }

    pub fn enable_backpressure_for_async_mode(&self) -> bool {
        self.producer_config.enable_backpressure_for_async_mode
    }

    pub fn back_pressure_for_async_send_num(&self) -> u32 {
        self.producer_config.back_pressure_for_async_send_num
    }

    pub fn back_pressure_for_async_send_size(&self) -> u32 {
        self.producer_config.back_pressure_for_async_send_size
    }

    pub fn rpc_hook(&self) -> &Option<Arc<dyn RPCHook>> {
        &self.producer_config.rpc_hook
    }

    pub fn compress_level(&self) -> i32 {
        self.producer_config.compress_level
    }

    pub fn compress_type(&self) -> CompressionType {
        self.producer_config.compress_type
    }

    pub fn compressor(&self) -> Option<&'static (dyn Compressor + Send + Sync)> {
        self.producer_config.compressor
    }

    pub fn set_client_config(&mut self, client_config: ClientConfig) {
        self.client_config = client_config;
    }

    pub fn set_default_mqproducer_impl(&mut self, default_mqproducer_impl: DefaultMQProducerImpl) {
        let wrapper = ArcMut::new(default_mqproducer_impl);
        self.default_mqproducer_impl = Some(ArcMut::clone(&wrapper));
        self.default_mqproducer_impl
            .as_mut()
            .unwrap()
            .set_default_mqproducer_impl_inner(wrapper);
    }

    pub fn set_retry_response_codes(&mut self, retry_response_codes: HashSet<i32>) {
        self.producer_config.retry_response_codes = retry_response_codes;
    }

    #[inline]
    pub fn set_producer_group(&mut self, producer_group: impl Into<CheetahString>) {
        self.producer_config.producer_group = producer_group.into();
    }

    pub fn set_topics(&mut self, topics: Vec<CheetahString>) {
        self.producer_config.topics = topics;
    }

    pub fn set_create_topic_key(&mut self, create_topic_key: impl Into<CheetahString>) {
        self.producer_config.create_topic_key = create_topic_key.into();
    }

    pub fn set_default_topic_queue_nums(&mut self, default_topic_queue_nums: u32) {
        self.producer_config.default_topic_queue_nums = default_topic_queue_nums;
    }

    pub fn set_send_msg_timeout(&mut self, send_msg_timeout: u32) {
        self.producer_config.send_msg_timeout = send_msg_timeout;
    }

    pub fn set_compress_msg_body_over_howmuch(&mut self, compress_msg_body_over_howmuch: u32) {
        self.producer_config.compress_msg_body_over_howmuch = compress_msg_body_over_howmuch;
    }

    pub fn set_retry_times_when_send_failed(&mut self, retry_times_when_send_failed: u32) {
        self.producer_config.retry_times_when_send_failed = retry_times_when_send_failed;
    }

    pub fn set_retry_times_when_send_async_failed(&mut self, retry_times_when_send_async_failed: u32) {
        self.producer_config.retry_times_when_send_async_failed = retry_times_when_send_async_failed;
    }

    pub fn set_retry_another_broker_when_not_store_ok(&mut self, retry_another_broker_when_not_store_ok: bool) {
        self.producer_config.retry_another_broker_when_not_store_ok = retry_another_broker_when_not_store_ok;
    }

    pub fn set_max_message_size(&mut self, max_message_size: u32) {
        self.producer_config.max_message_size = max_message_size;
    }

    pub fn set_trace_dispatcher(&mut self, trace_dispatcher: Arc<Box<dyn TraceDispatcher + Send + Sync>>) {
        self.producer_config.trace_dispatcher = Some(trace_dispatcher);
    }

    pub fn set_auto_batch(&mut self, auto_batch: bool) {
        self.producer_config.auto_batch = auto_batch;
    }

    pub fn set_produce_accumulator(&mut self, produce_accumulator: ProduceAccumulator) {
        self.producer_config.produce_accumulator = Some(ArcMut::new(produce_accumulator));
    }

    pub fn set_enable_backpressure_for_async_mode(&mut self, enable_backpressure_for_async_mode: bool) {
        self.producer_config.enable_backpressure_for_async_mode = enable_backpressure_for_async_mode;
    }

    pub fn set_back_pressure_for_async_send_num(&mut self, back_pressure_for_async_send_num: u32) {
        self.producer_config.back_pressure_for_async_send_num = back_pressure_for_async_send_num;
    }

    pub fn set_back_pressure_for_async_send_size(&mut self, back_pressure_for_async_send_size: u32) {
        self.producer_config.back_pressure_for_async_send_size = back_pressure_for_async_send_size;
    }

    pub fn set_rpc_hook(&mut self, rpc_hook: Arc<dyn RPCHook>) {
        self.producer_config.rpc_hook = Some(rpc_hook);
    }

    pub fn set_compress_level(&mut self, compress_level: i32) {
        self.producer_config.compress_level = compress_level;
    }

    pub fn set_compress_type(&mut self, compress_type: CompressionType) {
        self.producer_config.compress_type = compress_type;
    }

    pub fn set_compressor(&mut self, compressor: Option<&'static (dyn Compressor + Send + Sync)>) {
        self.producer_config.compressor = compressor;
    }

    pub fn producer_config(&self) -> &ProducerConfig {
        &self.producer_config
    }

    #[inline]
    pub fn set_send_latency_fault_enable(&mut self, send_latency_fault_enable: bool) {
        if let Some(ref mut default_mqproducer_impl) = self.default_mqproducer_impl {
            default_mqproducer_impl.set_send_latency_fault_enable(send_latency_fault_enable);
        }
    }

    fn batch<M>(&mut self, messages: Vec<M>) -> rocketmq_error::RocketMQResult<MessageBatch>
    where
        M: MessageTrait + Send + Sync,
    {
        match MessageBatch::generate_from_vec(messages) {
            Ok(mut msg_batch) => {
                for message in &mut msg_batch.messages {
                    Validators::check_message::<Message>(Some(message), &self.producer_config)?;
                    MessageClientIDSetter::set_uniq_id(message);
                    message.set_topic(self.with_namespace(message.get_topic()));
                }
                MessageClientIDSetter::set_uniq_id(&mut msg_batch.final_message);
                msg_batch.set_body(msg_batch.encode());
                msg_batch.set_topic(self.with_namespace(msg_batch.get_topic()));
                Ok(msg_batch)
            }
            Err(err) => {
                error!("Failed to initiate the MessageBatch: {:?}", err);
                Err(mq_client_err!("Failed to initiate the MessageBatch"))
            }
        }
    }

    #[inline]
    pub fn get_auto_batch(&self) -> bool {
        self.producer_config.produce_accumulator.is_some() && self.producer_config.auto_batch
    }
    #[inline]
    fn get_impl_mut(&mut self) -> rocketmq_error::RocketMQResult<&mut ArcMut<DefaultMQProducerImpl>> {
        self.default_mqproducer_impl
            .as_mut()
            .ok_or_else(|| mq_client_err!("DefaultMQProducerImpl is not initialized, call start() first"))
    }

    #[inline]
    fn get_accumulator_mut(&mut self) -> rocketmq_error::RocketMQResult<&mut ArcMut<ProduceAccumulator>> {
        self.producer_config
            .produce_accumulator
            .as_mut()
            .ok_or_else(|| mq_client_err!("ProduceAccumulator is not initialized, auto-batch is enabled"))
    }
    pub async fn send_direct<M>(
        &mut self,
        mut msg: M,
        mq: Option<MessageQueue>,
        send_callback: Option<SendMessageCallback>,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        let producer = self.get_impl_mut()?;

        if send_callback.is_none() {
            if let Some(mq) = mq {
                producer.sync_send_with_message_queue(msg, mq).await
            } else {
                producer.send(&mut msg).await
            }
        } else if mq.is_none() {
            producer.async_send_with_callback(msg, send_callback).await?;
            Ok(None)
        } else {
            producer
                .async_send_with_message_queue_callback(msg, mq.unwrap(), send_callback)
                .await?;
            Ok(None)
        }
    }

    pub async fn send_by_accumulator<M>(
        &mut self,
        mut msg: M,
        mq: Option<MessageQueue>,
        send_callback: Option<SendMessageCallback>,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + std::marker::Sync + 'static,
    {
        if !self.can_batch(&msg) {
            self.send_direct(msg, mq, send_callback).await
        } else {
            Validators::check_message(Some(&msg), self.producer_config())?;
            MessageClientIDSetter::set_uniq_id(&mut msg);
            if send_callback.is_none() {
                let mq_producer = self.clone();
                self.get_accumulator_mut()?.send(msg, mq, mq_producer).await
            } else {
                let mq_producer = self.clone();
                self.get_accumulator_mut()?
                    .send_callback(msg, mq, send_callback, mq_producer)
                    .await?;
                Ok(None)
            }
        }
    }

    fn can_batch<M>(&self, msg: &M) -> bool
    where
        M: MessageTrait,
    {
        // produceAccumulator is full
        if !self
            .producer_config
            .produce_accumulator
            .as_ref()
            .unwrap()
            .try_add_message(msg)
        {
            return false;
        }
        // delay message do not support batch processing
        if msg.get_delay_time_level() > 0
            || msg.get_delay_time_ms() > 0
            || msg.get_delay_time_sec() > 0
            || msg.get_deliver_time_ms() > 0
        {
            return false;
        }
        // retry message do not support batch processing
        if msg.get_topic().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            return false;
        }
        // message which have been assigned to producer group do not support batch processing
        if msg.get_properties().contains_key(MessageConst::PROPERTY_PRODUCER_GROUP) {
            return false;
        }
        true
    }
}

impl DefaultMQProducer {
    #[inline]
    pub fn with_namespace(&mut self, resource: &str) -> CheetahString {
        self.client_config.with_namespace(resource)
    }
}

impl MQProducer for DefaultMQProducer {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let producer_group = self.with_namespace(self.producer_config.producer_group.clone().as_str());
        self.set_producer_group(producer_group);
        let default_mqproducer_impl = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?;
        default_mqproducer_impl.start().await?;
        if let Some(ref mut produce_accumulator) = self.producer_config.produce_accumulator {
            produce_accumulator.start();
        }
        if self.client_config.enable_trace {
            let mut dispatcher = AsyncTraceDispatcher::new(
                self.producer_config.producer_group.as_str(),
                Type::Produce,
                self.client_config.trace_topic.clone().unwrap().as_str(),
                self.producer_config.rpc_hook.clone(),
            );
            dispatcher.set_host_producer(default_mqproducer_impl.clone());
            dispatcher.set_namespace_v2(self.client_config.namespace_v2.clone());
            let dispatcher: Arc<Box<dyn TraceDispatcher + Send + Sync>> = Arc::new(Box::new(dispatcher));
            self.producer_config.trace_dispatcher = Some(dispatcher.clone());
            default_mqproducer_impl
                .register_send_message_hook(Arc::new(SendMessageTraceHookImpl::new(dispatcher.clone())));
            default_mqproducer_impl
                .register_end_transaction_hook(Arc::new(EndTransactionTraceHookImpl::new(dispatcher)))
        }

        if let Some(ref mut trace_dispatcher) = self.producer_config.trace_dispatcher {
            unimplemented!("unimplemented trace dispatcher start");
        }
        Ok(())
    }

    async fn shutdown(&mut self) {
        if let Some(ref mut produce_accumulator) = self.producer_config.produce_accumulator {
            produce_accumulator.shutdown();
        }

        if let Some(ref trace_dispatcher) = self.producer_config.trace_dispatcher {
            trace_dispatcher.shutdown();
        }
    }

    async fn fetch_publish_message_queues(&mut self, topic: &str) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        let topic = self.with_namespace(topic);
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .fetch_publish_message_queues(topic.as_ref())
            .await
    }

    async fn send<M>(&mut self, mut msg: M) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        if self.get_auto_batch() && msg.as_any().downcast_ref::<MessageBatch>().is_none() {
            self.send_by_accumulator(msg, None, None).await
        } else {
            self.send_direct(msg, None, None).await
        }
    }

    async fn send_with_timeout<M>(
        &mut self,
        mut msg: M,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        let topic_build = self.with_namespace(msg.get_topic().as_str());
        msg.set_topic(topic_build);
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_with_timeout(&mut msg, timeout)
            .await
    }

    async fn send_with_callback<M, F>(&mut self, mut msg: M, send_callback: F) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        let send_callback_inner = Arc::new(send_callback);
        let result = if self.get_auto_batch() && msg.as_any().downcast_ref::<MessageBatch>().is_none() {
            self.send_by_accumulator(msg, None, Some(send_callback_inner.clone()))
                .await
        } else {
            self.send_direct(msg, None, Some(send_callback_inner.clone())).await
        };
        if let Err(err) = result {
            send_callback_inner(None, Some(&err));
        }
        Ok(())
    }

    async fn send_with_callback_timeout<F, M>(
        &mut self,
        mut msg: M,
        send_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic().as_str()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .async_send_with_callback_timeout(msg, Some(Arc::new(send_callback)), timeout)
            .await?;
        Ok(())
    }

    async fn send_oneway<M>(&mut self, mut msg: M) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_oneway(msg)
            .await?;
        Ok(())
    }

    async fn send_to_queue<M>(
        &mut self,
        mut msg: M,
        mq: MessageQueue,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        let mq = self.client_config.queue_with_namespace(mq);
        if self.get_auto_batch() && msg.as_any().downcast_ref::<MessageBatch>().is_none() {
            self.send_by_accumulator(msg, Some(mq), None).await
        } else {
            self.send_direct(msg, Some(mq), None).await
        }
    }

    async fn send_to_queue_with_timeout<M>(
        &mut self,
        mut msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        let mq = self.client_config.queue_with_namespace(mq);
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .sync_send_with_message_queue_timeout(msg, mq, timeout)
            .await
    }

    async fn send_to_queue_with_callback<M, F>(
        &mut self,
        mut msg: M,
        mq: MessageQueue,
        send_callback: F,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        let mq = self.client_config.queue_with_namespace(mq);

        if self.get_auto_batch() && msg.as_any().downcast_ref::<MessageBatch>().is_none() {
            self.send_by_accumulator(msg, Some(mq), Some(Arc::new(send_callback)))
                .await
        } else {
            self.send_direct(msg, Some(mq), Some(Arc::new(send_callback))).await
        }?;

        Ok(())
    }

    async fn send_to_queue_with_callback_timeout<M, F>(
        &mut self,
        mut msg: M,
        mq: MessageQueue,
        send_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .async_send_batch_to_queue_with_callback_timeout(msg, mq, Some(Arc::new(send_callback)), timeout)
            .await
    }

    async fn send_oneway_to_queue<M>(&mut self, mut msg: M, mq: MessageQueue) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        let mq = self.client_config.queue_with_namespace(mq);
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_oneway_with_message_queue(msg, mq)
            .await?;
        Ok(())
    }

    async fn send_with_selector<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync + 'static,
        T: std::any::Any + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        let mq = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .invoke_message_queue_selector(
                &msg,
                Arc::new(selector),
                &arg,
                self.producer_config.send_msg_timeout() as u64,
            )
            .await?;
        let mq = self.client_config.queue_with_namespace(mq);
        if self.get_auto_batch() && msg.as_any().downcast_ref::<MessageBatch>().is_none() {
            self.send_by_accumulator(msg, Some(mq), None).await
        } else {
            self.send_direct(msg, Some(mq), None).await
        }
    }

    async fn send_with_selector_timeout<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync + 'static,
        T: std::any::Any + Sync + Send,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_with_selector_timeout(msg, Arc::new(selector), arg, timeout)
            .await
    }

    async fn send_with_selector_callback<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
        send_callback: Option<SendMessageCallback>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync + 'static,
        T: std::any::Any + Sync + Send,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        let mq = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .invoke_message_queue_selector(
                &msg,
                Arc::new(selector),
                &arg,
                self.producer_config.send_msg_timeout() as u64,
            )
            .await?;
        let mq = self.client_config.queue_with_namespace(mq);
        if self.auto_batch() && msg.as_any().downcast_ref::<MessageBatch>().is_none() {
            self.send_by_accumulator(msg, Some(mq), send_callback).await
        } else {
            self.send_direct(msg, Some(mq), send_callback).await
        }?;
        Ok(())
    }

    async fn send_with_selector_callback_timeout<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync + 'static,
        T: std::any::Any + Sync + Send,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_with_selector_callback_timeout(msg, Arc::new(selector), arg, send_callback, timeout)
            .await
    }

    async fn send_oneway_with_selector<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync + 'static,
        T: std::any::Any + Sync + Send,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_oneway_with_selector(msg, Arc::new(selector), arg)
            .await
    }

    async fn send_message_in_transaction<T, M>(
        &mut self,
        msg: M,
        arg: Option<T>,
    ) -> rocketmq_error::RocketMQResult<TransactionSendResult>
    where
        T: std::any::Any + Sync + Send,
        M: MessageTrait + Send + Sync,
    {
        unimplemented!("DefaultMQProducer not support send_message_in_transaction")
    }

    async fn send_batch<M>(&mut self, msgs: Vec<M>) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        let mut batch = self.batch(msgs)?;
        let result = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send(&mut batch)
            .await?;
        Ok(result.expect("SendResult should not be None"))
    }

    async fn send_batch_with_timeout<M>(
        &mut self,
        msgs: Vec<M>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        let mut batch = self.batch(msgs)?;
        let result = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_with_timeout(&mut batch, timeout)
            .await?;
        Ok(result.expect("SendResult should not be None"))
    }

    async fn send_batch_to_queue<M>(
        &mut self,
        msgs: Vec<M>,
        mq: MessageQueue,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        let batch = self.batch(msgs)?;
        let result = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .sync_send_with_message_queue(batch, mq)
            .await?;
        Ok(result.expect("SendResult should not be None"))
    }

    async fn send_batch_to_queue_with_timeout<M>(
        &mut self,
        msgs: Vec<M>,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        let batch = self.batch(msgs)?;
        let result = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .sync_send_with_message_queue_timeout(batch, mq, timeout)
            .await?;
        Ok(result.expect("SendResult should not be None"))
    }

    async fn send_batch_with_callback<M, F>(&mut self, msgs: Vec<M>, f: F) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        let batch = self.batch(msgs)?;
        self.default_mqproducer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQProducerImpl is not initialized"))?
            .async_send_with_callback(batch, Some(Arc::new(f)))
            .await?;
        Ok(())
    }

    async fn send_batch_with_callback_timeout<M, F>(
        &mut self,
        msgs: Vec<M>,
        f: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        let batch = self.batch(msgs)?;
        self.default_mqproducer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQProducerImpl is not initialized"))?
            .async_send_with_callback_timeout(batch, Some(Arc::new(f)), timeout)
            .await?;
        Ok(())
    }

    async fn send_batch_to_queue_with_callback<M, F>(
        &mut self,
        msgs: Vec<M>,
        mq: MessageQueue,
        f: F,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        let batch = self.batch(msgs)?;
        self.default_mqproducer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQProducerImpl is not initialized"))?
            .async_send_with_message_queue_callback(batch, mq, Some(Arc::new(f)))
            .await
    }

    async fn send_batch_to_queue_with_callback_timeout<M, F>(
        &mut self,
        msgs: Vec<M>,
        mq: MessageQueue,
        f: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        let batch = self.batch(msgs)?;
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .async_send_batch_to_queue_with_callback_timeout(batch, mq, Some(Arc::new(f)), timeout)
            .await
    }

    async fn request<M>(
        &mut self,
        mut msg: M,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .request(msg, timeout)
            .await
    }

    async fn request_with_callback<F, M>(
        &mut self,
        mut msg: M,
        request_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQProducerImpl is not initialized"))?
            .request_with_callback(msg, Arc::new(request_callback), timeout)
            .await
    }

    async fn request_with_selector<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync + 'static,
        T: std::any::Any + Sync + Send,
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQProducerImpl is not initialized"))?
            .request_with_selector(msg, selector, arg, timeout)
            .await
    }

    async fn request_with_selector_callback<M, S, T, F>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
        request_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync + 'static,
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        T: std::any::Any + Sync + Send,
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("DefaultMQProducerImpl is not initialized"))?
            .request_with_selector_callback(msg, selector, arg, Arc::new(request_callback), timeout)
            .await
    }

    async fn request_to_queue<M>(
        &mut self,
        mut msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .request_to_queue(msg, mq, timeout)
            .await
    }

    async fn request_to_queue_with_callback<M, F>(
        &mut self,
        mut msg: M,
        mq: MessageQueue,
        request_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.get_topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .request_to_queue_with_callback(msg, mq, Arc::new(request_callback), timeout)
            .await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rocketmq_common::common::message::message_single::Message;
    use rocketmq_error::RocketMQResult;
    #[tokio::test]
    async fn request_with_callback_not_initialized() {
        // Arrange
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };
        let msg = Message {
            topic: "test-topic".into(),
            flag: 0,
            properties: Default::default(),
            body: None,
            compressed_body: None,
            transaction_id: None,
        };
        let callback = |_msg: Option<&dyn MessageTrait>, _err: Option<&dyn std::error::Error>| {
            // no-op
        };
        let result = producer.request_with_callback(msg, callback, 1000).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            RocketMQError::NotInitialized(reason) => {
                assert!(reason.contains("not initialized"), "unexpected error message: {reason}");
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn request_with_selector_not_initialized() {
        // Arrange
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };
        let msg = Message {
            topic: "test-topic".into(),
            flag: 0,
            properties: Default::default(),
            body: None,
            compressed_body: None,
            transaction_id: None,
        };
        let selector = |queues: &[MessageQueue],
                        _msg: &dyn MessageTrait,
                        _arg: &dyn std::any::Any|
         -> Option<MessageQueue> { None };
        let result = producer.request_with_selector(msg, selector, 1, 1000).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            RocketMQError::NotInitialized(reason) => {
                assert!(reason.contains("not initialized"), "unexpected error message: {reason}");
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn request_with_selector_callback_not_initialized() {
        // Arrange
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };
        let msg = Message {
            topic: "test-topic".into(),
            flag: 0,
            properties: Default::default(),
            body: None,
            compressed_body: None,
            transaction_id: None,
        };
        let selector = |queues: &[MessageQueue],
                        _msg: &dyn MessageTrait,
                        _arg: &dyn std::any::Any|
         -> Option<MessageQueue> { None };
        let callback = |_msg: Option<&dyn MessageTrait>, _err: Option<&dyn std::error::Error>| {
            // no-op
        };
        let result = producer
            .request_with_selector_callback(msg, selector, 1, callback, 1000)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            RocketMQError::NotInitialized(reason) => {
                assert!(reason.contains("not initialized"), "unexpected error message: {reason}");
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_batch_with_callback_not_initialized() {
        // Arrange
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };
        let msg = Message {
            topic: "test-topic".into(),
            flag: 0,
            properties: Default::default(),
            body: Some(Bytes::from_static(b"Hello world")),
            compressed_body: None,
            transaction_id: None,
        };
        let callback = |_msg: Option<&SendResult>, _err: Option<&dyn std::error::Error>| {
            // no-op
        };
        let result: RocketMQResult<()> = producer.send_batch_with_callback(vec![msg], callback).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            RocketMQError::NotInitialized(reason) => {
                assert!(reason.contains("not initialized"), "unexpected error message: {reason}");
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_batch_with_callback_timeout_not_initialized() {
        // Arrange
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };
        let msg = Message {
            topic: "test-topic".into(),
            flag: 0,
            properties: Default::default(),
            body: Some(Bytes::from_static(b"Hello world")),
            compressed_body: None,
            transaction_id: None,
        };
        let callback = |_msg: Option<&SendResult>, _err: Option<&dyn std::error::Error>| {
            // no-op
        };
        let result: RocketMQResult<()> = producer
            .send_batch_with_callback_timeout(vec![msg], callback, 1000)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            RocketMQError::NotInitialized(reason) => {
                assert!(reason.contains("not initialized"), "unexpected error message: {reason}");
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_batch_to_queue_with_callback_not_initialized() {
        // Arrange
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };
        let msg = Message {
            topic: "test-topic".into(),
            flag: 0,
            properties: Default::default(),
            body: Some(Bytes::from_static(b"Hello world")),
            compressed_body: None,
            transaction_id: None,
        };
        let callback = |_msg: Option<&SendResult>, _err: Option<&dyn std::error::Error>| {
            // no-op
        };
        let mq = MessageQueue::new();
        let result: RocketMQResult<()> = producer
            .send_batch_to_queue_with_callback(vec![msg], mq, callback)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            RocketMQError::NotInitialized(reason) => {
                assert!(reason.contains("not initialized"), "unexpected error message: {reason}");
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }
}
