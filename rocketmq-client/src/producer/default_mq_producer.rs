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
use std::net::SocketAddr;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::compression::compression_type::CompressionType;
use rocketmq_common::common::compression::compressor::Compressor;
use rocketmq_common::common::compression::compressor_factory::CompressorFactory;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_ext::MessageExt;
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
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tracing::error;

use crate::base::client_config::ClientConfig;
use crate::base::query_result::QueryResult;
use crate::base::validators::Validators;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::latency::mq_fault_strategy::MQFaultStrategy;
use crate::producer::default_mq_produce_builder::DefaultMQProducerBuilder;
use crate::producer::mq_producer::MQProducer;
use crate::producer::produce_accumulator::ProduceAccumulator;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::send_callback::ArcSendCallback;
use crate::producer::send_result::SendResult;
use crate::producer::transaction_send_result::TransactionSendResult;
use crate::trace::async_trace_dispatcher::AsyncTraceDispatcher;
use crate::trace::hook::default_recall_message_trace_hook::DefaultRecallMessageTraceHook;
use crate::trace::hook::end_transaction_trace_hook_impl::EndTransactionTraceHookImpl;
use crate::trace::hook::send_message_trace_hook_impl::SendMessageTraceHookImpl;
use crate::trace::trace_dispatcher::ArcTraceDispatcher;
use crate::trace::trace_dispatcher::Type;

pub(crate) const MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM: u32 = 10;
pub(crate) const MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE: u32 = 1024 * 1024;

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
    /// Timeout for sending messages (milliseconds).
    send_msg_timeout: u32,
    /// Maximum timeout for sending messages per request (milliseconds).
    /// None means no limit.
    send_msg_max_timeout_per_request: Option<u32>,
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
    trace_dispatcher: Option<ArcTraceDispatcher>,
    /// Switch flag instance for automatic batch message
    auto_batch: bool,
    /// Maximum hold time of accumulator in milliseconds.
    /// None means no delay.
    batch_max_delay_ms: Option<u32>,
    /// Maximum accumulation message body size for a single messageAccumulation (bytes).
    /// None means no limit.
    batch_max_bytes: Option<u64>,
    /// Maximum message body size for produceAccumulator (bytes).
    /// None means no limit.
    total_batch_max_bytes: Option<u64>,
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
    callback_executor: Option<tokio::runtime::Handle>,
    async_sender_executor: Option<tokio::runtime::Handle>,
}

struct CompositeRPCHook {
    hooks: Vec<Arc<dyn RPCHook>>,
}

impl CompositeRPCHook {
    fn chain(first: Arc<dyn RPCHook>, second: Arc<dyn RPCHook>) -> Arc<dyn RPCHook> {
        Arc::new(Self {
            hooks: vec![first, second],
        })
    }
}

impl RPCHook for CompositeRPCHook {
    fn do_before_request(
        &self,
        remote_addr: SocketAddr,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        for hook in &self.hooks {
            hook.do_before_request(remote_addr, request)?;
        }
        Ok(())
    }

    fn do_after_response(
        &self,
        remote_addr: SocketAddr,
        request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        for hook in &self.hooks {
            hook.do_after_response(remote_addr, request, response)?;
        }
        Ok(())
    }
}

impl ProducerConfig {
    fn compression_type_from_env() -> CompressionType {
        match std::env::var(MESSAGE_COMPRESS_TYPE) {
            Ok(compression_type) => Self::parse_compression_type_or_zlib(compression_type.as_str()),
            Err(_) => CompressionType::Zlib,
        }
    }

    fn parse_compression_type_or_zlib(compression_type: &str) -> CompressionType {
        match CompressionType::try_of(compression_type) {
            Ok(compression_type) => compression_type,
            Err(error) => {
                tracing::warn!(
                    compression_type,
                    error = %error,
                    "invalid producer compression type, falling back to ZLIB"
                );
                CompressionType::Zlib
            }
        }
    }

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
    #[inline]
    pub fn default_topic_queue_nums(&self) -> u32 {
        self.default_topic_queue_nums
    }

    pub fn send_msg_timeout(&self) -> u32 {
        self.send_msg_timeout
    }

    pub fn send_msg_max_timeout_per_request(&self) -> Option<u32> {
        self.send_msg_max_timeout_per_request
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

    pub fn trace_dispatcher(&self) -> Option<&ArcTraceDispatcher> {
        self.trace_dispatcher.as_ref()
    }

    pub fn auto_batch(&self) -> bool {
        self.auto_batch
    }

    pub fn batch_max_delay_ms(&self) -> Option<u32> {
        self.batch_max_delay_ms
    }

    pub fn batch_max_bytes(&self) -> Option<u64> {
        self.batch_max_bytes
    }

    pub fn total_batch_max_bytes(&self) -> Option<u64> {
        self.total_batch_max_bytes
    }

    pub fn produce_accumulator(&self) -> Option<&ArcMut<ProduceAccumulator>> {
        self.produce_accumulator.as_ref()
    }

    pub fn enable_backpressure_for_async_mode(&self) -> bool {
        self.enable_backpressure_for_async_mode
    }

    pub(crate) fn set_enable_backpressure_for_async_mode(&mut self, enable_backpressure_for_async_mode: bool) {
        self.enable_backpressure_for_async_mode = enable_backpressure_for_async_mode;
    }

    pub fn back_pressure_for_async_send_num(&self) -> u32 {
        self.back_pressure_for_async_send_num
    }

    pub(crate) fn set_back_pressure_for_async_send_num(&mut self, back_pressure_for_async_send_num: u32) {
        self.back_pressure_for_async_send_num =
            back_pressure_for_async_send_num.max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM);
    }

    pub fn back_pressure_for_async_send_size(&self) -> u32 {
        self.back_pressure_for_async_send_size
    }

    pub(crate) fn set_back_pressure_for_async_send_size(&mut self, back_pressure_for_async_send_size: u32) {
        self.back_pressure_for_async_send_size =
            back_pressure_for_async_send_size.max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE);
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

    pub fn callback_executor(&self) -> Option<&tokio::runtime::Handle> {
        self.callback_executor.as_ref()
    }

    pub fn async_sender_executor(&self) -> Option<&tokio::runtime::Handle> {
        self.async_sender_executor.as_ref()
    }

    pub(crate) fn set_callback_executor(&mut self, callback_executor: tokio::runtime::Handle) {
        self.callback_executor = Some(callback_executor);
    }

    pub(crate) fn set_async_sender_executor(&mut self, async_sender_executor: tokio::runtime::Handle) {
        self.async_sender_executor = Some(async_sender_executor);
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
        let compression_type = Self::compression_type_from_env();
        Self {
            retry_response_codes,
            producer_group: CheetahString::empty(),
            topics: vec![],
            create_topic_key: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            default_topic_queue_nums: 4,
            send_msg_timeout: 3000,
            send_msg_max_timeout_per_request: None,
            compress_msg_body_over_howmuch: 1024 * 4,
            retry_times_when_send_failed: 2,
            retry_times_when_send_async_failed: 2,
            retry_another_broker_when_not_store_ok: false,
            max_message_size: 1024 * 1024 * 4,
            trace_dispatcher: None,
            auto_batch: false,
            batch_max_delay_ms: None,
            batch_max_bytes: None,
            total_batch_max_bytes: None,
            produce_accumulator: None,
            enable_backpressure_for_async_mode: false,
            back_pressure_for_async_send_num: 1024,
            back_pressure_for_async_send_size: 100 * 1024 * 1024,
            rpc_hook: None,
            compress_level: std::env::var(MESSAGE_COMPRESS_LEVEL)
                .ok()
                .and_then(|level| level.parse().ok())
                .unwrap_or(5),
            compress_type: compression_type,
            compressor: Some(CompressorFactory::get_compressor(compression_type)),
            callback_executor: None,
            async_sender_executor: None,
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

    pub async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQProducer>::start(self).await
    }

    pub async fn shutdown(&mut self) {
        <Self as MQProducer>::shutdown(self).await;
    }

    pub async fn fetch_publish_message_queues(
        &mut self,
        topic: &str,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        <Self as MQProducer>::fetch_publish_message_queues(self, topic).await
    }

    pub async fn create_topic(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQProducer>::create_topic(self, key, new_topic, queue_num, attributes).await
    }

    pub async fn create_topic_with_flag(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQProducer>::create_topic_with_flag(self, key, new_topic, queue_num, topic_sys_flag, attributes).await
    }

    pub async fn search_offset(&mut self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        <Self as MQProducer>::search_offset(self, mq, timestamp).await
    }

    pub async fn max_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        <Self as MQProducer>::max_offset(self, mq).await
    }

    pub async fn min_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        <Self as MQProducer>::min_offset(self, mq).await
    }

    pub async fn earliest_msg_store_time(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        <Self as MQProducer>::earliest_msg_store_time(self, mq).await
    }

    pub async fn query_message(
        &mut self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        <Self as MQProducer>::query_message(self, topic, key, max_num, begin, end).await
    }

    pub async fn view_message(&mut self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        <Self as MQProducer>::view_message(self, topic, msg_id).await
    }

    pub async fn send<M>(&mut self, msg: M) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send(self, msg).await
    }

    pub async fn send_with_timeout<M>(
        &mut self,
        msg: M,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_with_timeout(self, msg, timeout).await
    }

    pub async fn send_with_callback<M, F>(&mut self, msg: M, send_callback: F) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        <Self as MQProducer>::send_with_callback(self, msg, send_callback).await
    }

    pub async fn send_with_callback_timeout<F, M>(
        &mut self,
        msg: M,
        send_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_with_callback_timeout(self, msg, send_callback, timeout).await
    }

    pub async fn send_oneway<M>(&mut self, msg: M) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_oneway(self, msg).await
    }

    pub async fn send_to_queue<M>(
        &mut self,
        msg: M,
        mq: MessageQueue,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_to_queue(self, msg, mq).await
    }

    pub async fn send_to_queue_with_timeout<M>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_to_queue_with_timeout(self, msg, mq, timeout).await
    }

    pub async fn send_to_queue_with_callback<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        send_callback: F,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        <Self as MQProducer>::send_to_queue_with_callback(self, msg, mq, send_callback).await
    }

    pub async fn send_to_queue_with_callback_timeout<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        send_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        <Self as MQProducer>::send_to_queue_with_callback_timeout(self, msg, mq, send_callback, timeout).await
    }

    pub async fn send_oneway_to_queue<M>(&mut self, msg: M, mq: MessageQueue) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_oneway_to_queue(self, msg, mq).await
    }

    pub async fn send_with_selector<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        <Self as MQProducer>::send_with_selector(self, msg, selector, arg).await
    }

    pub async fn send_with_selector_timeout<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        <Self as MQProducer>::send_with_selector_timeout(self, msg, selector, arg, timeout).await
    }

    pub async fn send_with_selector_callback<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
        send_callback: Option<ArcSendCallback>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        <Self as MQProducer>::send_with_selector_callback(self, msg, selector, arg, send_callback).await
    }

    pub async fn send_with_selector_callback_timeout<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        <Self as MQProducer>::send_with_selector_callback_timeout(self, msg, selector, arg, send_callback, timeout)
            .await
    }

    pub async fn send_oneway_with_selector<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        <Self as MQProducer>::send_oneway_with_selector(self, msg, selector, arg).await
    }

    pub async fn send_message_in_transaction<T, M>(
        &mut self,
        msg: M,
        arg: Option<T>,
    ) -> rocketmq_error::RocketMQResult<TransactionSendResult>
    where
        T: std::any::Any + Sync + Send,
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_message_in_transaction(self, msg, arg).await
    }

    pub async fn send_batch<M>(&mut self, msgs: Vec<M>) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_batch(self, msgs).await
    }

    pub async fn send_batch_with_timeout<M>(
        &mut self,
        msgs: Vec<M>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_batch_with_timeout(self, msgs, timeout).await
    }

    pub async fn send_batch_to_queue<M>(
        &mut self,
        msgs: Vec<M>,
        mq: MessageQueue,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_batch_to_queue(self, msgs, mq).await
    }

    pub async fn send_batch_to_queue_with_timeout<M>(
        &mut self,
        msgs: Vec<M>,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::send_batch_to_queue_with_timeout(self, msgs, mq, timeout).await
    }

    pub async fn send_batch_with_callback<M, F>(&mut self, msgs: Vec<M>, f: F) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        <Self as MQProducer>::send_batch_with_callback(self, msgs, f).await
    }

    pub async fn send_batch_with_callback_timeout<M, F>(
        &mut self,
        msgs: Vec<M>,
        f: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        <Self as MQProducer>::send_batch_with_callback_timeout(self, msgs, f, timeout).await
    }

    pub async fn send_batch_to_queue_with_callback<M, F>(
        &mut self,
        msgs: Vec<M>,
        mq: MessageQueue,
        f: F,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        <Self as MQProducer>::send_batch_to_queue_with_callback(self, msgs, mq, f).await
    }

    pub async fn send_batch_to_queue_with_callback_timeout<M, F>(
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
        <Self as MQProducer>::send_batch_to_queue_with_callback_timeout(self, msgs, mq, f, timeout).await
    }

    pub async fn request<M>(
        &mut self,
        msg: M,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::request(self, msg, timeout).await
    }

    pub async fn request_with_callback<F, M>(
        &mut self,
        msg: M,
        request_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::request_with_callback(self, msg, request_callback, timeout).await
    }

    pub async fn request_with_selector<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::request_with_selector(self, msg, selector, arg, timeout).await
    }

    pub async fn request_with_selector_callback<M, S, T, F>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
        request_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        T: Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::request_with_selector_callback(self, msg, selector, arg, request_callback, timeout).await
    }

    pub async fn request_to_queue<M>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::request_to_queue(self, msg, mq, timeout).await
    }

    pub async fn request_to_queue_with_callback<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        request_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        <Self as MQProducer>::request_to_queue_with_callback(self, msg, mq, request_callback, timeout).await
    }

    pub async fn recall_message(
        &mut self,
        topic: impl Into<CheetahString>,
        recall_handle: impl Into<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<String> {
        <Self as MQProducer>::recall_message(self, topic, recall_handle).await
    }

    #[inline]
    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    #[inline]
    pub fn default_mq_producer_impl(&self) -> Option<&ArcMut<DefaultMQProducerImpl>> {
        self.default_mqproducer_impl.as_ref()
    }

    #[inline]
    pub fn get_default_mq_producer_impl(&self) -> Option<&ArcMut<DefaultMQProducerImpl>> {
        self.default_mq_producer_impl()
    }

    #[inline]
    pub fn retry_response_codes(&self) -> &HashSet<i32> {
        &self.producer_config.retry_response_codes
    }

    #[inline]
    pub fn get_retry_response_codes(&self) -> &HashSet<i32> {
        self.retry_response_codes()
    }

    #[inline]
    pub fn producer_group(&self) -> &str {
        &self.producer_config.producer_group
    }

    #[inline]
    pub fn get_producer_group(&self) -> &str {
        self.producer_group()
    }

    #[inline]
    pub fn topics(&self) -> &Vec<CheetahString> {
        &self.producer_config.topics
    }

    #[inline]
    pub fn get_topics(&self) -> &Vec<CheetahString> {
        self.topics()
    }

    #[inline]
    pub fn create_topic_key(&self) -> &str {
        &self.producer_config.create_topic_key
    }

    #[inline]
    pub fn get_create_topic_key(&self) -> &str {
        self.create_topic_key()
    }

    #[inline]
    pub fn default_topic_queue_nums(&self) -> u32 {
        self.producer_config.default_topic_queue_nums
    }

    #[inline]
    pub fn get_default_topic_queue_nums(&self) -> u32 {
        self.default_topic_queue_nums()
    }

    #[inline]
    pub fn send_msg_timeout(&self) -> u32 {
        self.producer_config.send_msg_timeout
    }

    #[inline]
    pub fn get_send_msg_timeout(&self) -> u32 {
        self.send_msg_timeout()
    }

    #[inline]
    pub fn compress_msg_body_over_howmuch(&self) -> u32 {
        self.producer_config.compress_msg_body_over_howmuch
    }

    #[inline]
    pub fn get_compress_msg_body_over_howmuch(&self) -> u32 {
        self.compress_msg_body_over_howmuch()
    }

    #[inline]
    pub fn retry_times_when_send_failed(&self) -> u32 {
        self.producer_config.retry_times_when_send_failed
    }

    #[inline]
    pub fn get_retry_times_when_send_failed(&self) -> u32 {
        self.retry_times_when_send_failed()
    }

    #[inline]
    pub fn retry_times_when_send_async_failed(&self) -> u32 {
        self.producer_config.retry_times_when_send_async_failed
    }

    #[inline]
    pub fn get_retry_times_when_send_async_failed(&self) -> u32 {
        self.retry_times_when_send_async_failed()
    }

    #[inline]
    pub fn retry_another_broker_when_not_store_ok(&self) -> bool {
        self.producer_config.retry_another_broker_when_not_store_ok
    }

    #[inline]
    pub fn is_retry_another_broker_when_not_store_ok(&self) -> bool {
        self.retry_another_broker_when_not_store_ok()
    }

    #[inline]
    pub fn is_send_message_with_vip_channel(&self) -> bool {
        self.client_config.is_vip_channel_enabled()
    }

    #[inline]
    pub fn is_send_message_with_vipchannel(&self) -> bool {
        self.is_send_message_with_vip_channel()
    }

    #[inline]
    pub fn max_message_size(&self) -> u32 {
        self.producer_config.max_message_size
    }

    #[inline]
    pub fn get_max_message_size(&self) -> u32 {
        self.max_message_size()
    }

    #[inline]
    pub fn trace_dispatcher(&self) -> Option<&ArcTraceDispatcher> {
        self.producer_config.trace_dispatcher()
    }

    #[inline]
    pub fn get_trace_dispatcher(&self) -> Option<&ArcTraceDispatcher> {
        self.trace_dispatcher()
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

    #[inline]
    pub fn is_enable_backpressure_for_async_mode(&self) -> bool {
        self.enable_backpressure_for_async_mode()
    }

    pub fn back_pressure_for_async_send_num(&self) -> u32 {
        self.producer_config.back_pressure_for_async_send_num
    }

    #[inline]
    pub fn get_back_pressure_for_async_send_num(&self) -> u32 {
        self.back_pressure_for_async_send_num()
    }

    pub fn back_pressure_for_async_send_size(&self) -> u32 {
        self.producer_config.back_pressure_for_async_send_size
    }

    #[inline]
    pub fn get_back_pressure_for_async_send_size(&self) -> u32 {
        self.back_pressure_for_async_send_size()
    }

    #[inline]
    pub fn send_msg_max_timeout_per_request(&self) -> Option<u32> {
        self.producer_config.send_msg_max_timeout_per_request
    }

    #[inline]
    pub fn get_send_msg_max_timeout_per_request(&self) -> Option<u32> {
        self.send_msg_max_timeout_per_request()
    }

    #[inline]
    pub fn batch_max_delay_ms(&self) -> Option<u32> {
        self.producer_config.batch_max_delay_ms
    }

    #[inline]
    pub fn get_batch_max_delay_ms(&self) -> Option<u32> {
        self.batch_max_delay_ms()
    }

    #[inline]
    pub fn batch_max_bytes(&self) -> Option<u64> {
        self.producer_config.batch_max_bytes
    }

    #[inline]
    pub fn get_batch_max_bytes(&self) -> Option<u64> {
        self.batch_max_bytes()
    }

    #[inline]
    pub fn total_batch_max_bytes(&self) -> Option<u64> {
        self.producer_config.total_batch_max_bytes
    }

    #[inline]
    pub fn get_total_batch_max_bytes(&self) -> Option<u64> {
        self.total_batch_max_bytes()
    }

    pub fn rpc_hook(&self) -> &Option<Arc<dyn RPCHook>> {
        &self.producer_config.rpc_hook
    }

    pub fn compress_level(&self) -> i32 {
        self.producer_config.compress_level
    }

    #[inline]
    pub fn get_compress_level(&self) -> i32 {
        self.compress_level()
    }

    pub fn compress_type(&self) -> CompressionType {
        self.producer_config.compress_type
    }

    pub fn get_compress_type(&self) -> CompressionType {
        self.compress_type()
    }

    pub fn compressor(&self) -> Option<&'static (dyn Compressor + Send + Sync)> {
        self.producer_config.compressor
    }

    pub fn get_compressor(&self) -> Option<&'static (dyn Compressor + Send + Sync)> {
        self.compressor()
    }

    pub fn callback_executor(&self) -> Option<&tokio::runtime::Handle> {
        self.producer_config.callback_executor()
    }

    pub fn async_sender_executor(&self) -> Option<&tokio::runtime::Handle> {
        self.producer_config.async_sender_executor()
    }

    #[inline]
    pub fn is_use_tls(&self) -> bool {
        self.client_config.is_use_tls()
    }

    pub fn set_use_tls(&mut self, use_tls: bool) {
        self.client_config.set_use_tls(use_tls);
        if let Some(default_mqproducer_impl) = self.default_mqproducer_impl.as_mut() {
            default_mqproducer_impl.set_use_tls(use_tls);
        }
        if let Some(dispatcher) = self.producer_config.trace_dispatcher.as_ref() {
            if let Some(async_dispatcher) = dispatcher.as_any().downcast_ref::<AsyncTraceDispatcher>() {
                async_dispatcher.set_use_tls(use_tls);
            }
        }
    }

    pub fn set_client_config(&mut self, client_config: ClientConfig) {
        self.client_config = client_config;
        if let Some(default_mqproducer_impl) = self.default_mqproducer_impl.as_mut() {
            default_mqproducer_impl.set_use_tls(self.client_config.use_tls);
        }
    }

    pub fn set_default_mqproducer_impl(&mut self, default_mqproducer_impl: DefaultMQProducerImpl) {
        let mut wrapper = ArcMut::new(default_mqproducer_impl);
        let wrapper_weak = ArcMut::downgrade(&wrapper);
        wrapper.set_default_mqproducer_impl_inner(wrapper_weak);
        self.default_mqproducer_impl = Some(wrapper);
    }

    fn sync_producer_config_to_impl(&mut self) {
        if let Some(default_mqproducer_impl) = self.default_mqproducer_impl.as_mut() {
            default_mqproducer_impl.replace_producer_config(self.producer_config.clone());
        }
    }

    pub fn set_retry_response_codes(&mut self, retry_response_codes: HashSet<i32>) {
        self.producer_config.retry_response_codes = retry_response_codes;
        self.sync_producer_config_to_impl();
    }

    pub fn add_retry_response_code(&mut self, response_code: i32) {
        self.producer_config.retry_response_codes.insert(response_code);
        self.sync_producer_config_to_impl();
    }

    #[inline]
    pub fn set_producer_group(&mut self, producer_group: impl Into<CheetahString>) {
        self.producer_config.producer_group = producer_group.into();
        self.sync_producer_config_to_impl();
    }

    pub fn set_topics(&mut self, topics: Vec<CheetahString>) {
        self.producer_config.topics = topics;
        self.sync_producer_config_to_impl();
    }

    pub fn set_create_topic_key(&mut self, create_topic_key: impl Into<CheetahString>) {
        self.producer_config.create_topic_key = create_topic_key.into();
        self.sync_producer_config_to_impl();
    }

    pub fn set_default_topic_queue_nums(&mut self, default_topic_queue_nums: u32) {
        self.producer_config.default_topic_queue_nums = default_topic_queue_nums;
        self.sync_producer_config_to_impl();
    }

    pub fn set_send_msg_timeout(&mut self, send_msg_timeout: u32) {
        self.producer_config.send_msg_timeout = send_msg_timeout;
        self.sync_producer_config_to_impl();
    }

    pub fn set_compress_msg_body_over_howmuch(&mut self, compress_msg_body_over_howmuch: u32) {
        self.producer_config.compress_msg_body_over_howmuch = compress_msg_body_over_howmuch;
        self.sync_producer_config_to_impl();
    }

    pub fn set_retry_times_when_send_failed(&mut self, retry_times_when_send_failed: u32) {
        self.producer_config.retry_times_when_send_failed = retry_times_when_send_failed;
        self.sync_producer_config_to_impl();
    }

    pub fn set_retry_times_when_send_async_failed(&mut self, retry_times_when_send_async_failed: u32) {
        self.producer_config.retry_times_when_send_async_failed = retry_times_when_send_async_failed;
        self.sync_producer_config_to_impl();
    }

    pub fn set_retry_another_broker_when_not_store_ok(&mut self, retry_another_broker_when_not_store_ok: bool) {
        self.producer_config.retry_another_broker_when_not_store_ok = retry_another_broker_when_not_store_ok;
        self.sync_producer_config_to_impl();
    }

    pub fn set_send_message_with_vip_channel(&mut self, send_message_with_vip_channel: bool) {
        self.client_config
            .set_vip_channel_enabled(send_message_with_vip_channel);
        if let Some(default_mqproducer_impl) = self.default_mqproducer_impl.as_mut() {
            default_mqproducer_impl.set_send_message_with_vip_channel(send_message_with_vip_channel);
        }
    }

    pub fn set_send_message_with_vipchannel(&mut self, send_message_with_vip_channel: bool) {
        self.set_send_message_with_vip_channel(send_message_with_vip_channel);
    }

    pub fn set_max_message_size(&mut self, max_message_size: u32) {
        self.producer_config.max_message_size = max_message_size;
        self.sync_producer_config_to_impl();
    }

    pub fn set_trace_dispatcher(&mut self, trace_dispatcher: ArcTraceDispatcher) {
        self.producer_config.trace_dispatcher = Some(trace_dispatcher);
        self.sync_producer_config_to_impl();
    }

    pub fn set_auto_batch(&mut self, auto_batch: bool) {
        self.producer_config.auto_batch = auto_batch;
        self.sync_producer_config_to_impl();
    }

    pub fn set_produce_accumulator(&mut self, produce_accumulator: ProduceAccumulator) {
        let mut produce_accumulator = produce_accumulator;
        self.apply_batch_config_to_accumulator(&mut produce_accumulator);
        self.producer_config.produce_accumulator = Some(ArcMut::new(produce_accumulator));
        self.sync_producer_config_to_impl();
    }

    pub fn set_enable_backpressure_for_async_mode(&mut self, enable_backpressure_for_async_mode: bool) {
        self.producer_config
            .set_enable_backpressure_for_async_mode(enable_backpressure_for_async_mode);
        self.sync_producer_config_to_impl();
    }

    pub fn set_back_pressure_for_async_send_num(&mut self, back_pressure_for_async_send_num: u32) {
        self.producer_config
            .set_back_pressure_for_async_send_num(back_pressure_for_async_send_num);
        self.sync_producer_config_to_impl();
    }

    pub fn set_back_pressure_for_async_send_size(&mut self, back_pressure_for_async_send_size: u32) {
        self.producer_config
            .set_back_pressure_for_async_send_size(back_pressure_for_async_send_size);
        self.sync_producer_config_to_impl();
    }

    pub fn set_rpc_hook(&mut self, rpc_hook: Arc<dyn RPCHook>) {
        self.producer_config.rpc_hook = Some(rpc_hook.clone());
        if let Some(ref mut default_mqproducer_impl) = self.default_mqproducer_impl {
            default_mqproducer_impl.set_rpc_hook(rpc_hook);
        }
    }

    pub fn set_compress_level(&mut self, compress_level: i32) {
        self.producer_config.compress_level = compress_level;
        self.sync_producer_config_to_impl();
    }

    pub fn set_compress_type(&mut self, compress_type: CompressionType) {
        self.producer_config.compress_type = compress_type;
        self.producer_config.compressor = Some(CompressorFactory::get_compressor(compress_type));
        self.sync_producer_config_to_impl();
    }

    pub fn set_compressor(&mut self, compressor: Option<&'static (dyn Compressor + Send + Sync)>) {
        self.producer_config.compressor = compressor;
        self.sync_producer_config_to_impl();
    }

    pub fn set_callback_executor(&mut self, callback_executor: tokio::runtime::Handle) {
        self.producer_config.set_callback_executor(callback_executor.clone());
        if let Some(default_mqproducer_impl) = self.default_mqproducer_impl.as_mut() {
            default_mqproducer_impl.set_callback_executor(callback_executor);
        }
        self.sync_producer_config_to_impl();
    }

    pub fn set_async_sender_executor(&mut self, async_sender_executor: tokio::runtime::Handle) {
        self.producer_config
            .set_async_sender_executor(async_sender_executor.clone());
        if let Some(default_mqproducer_impl) = self.default_mqproducer_impl.as_mut() {
            default_mqproducer_impl.set_async_sender_executor(async_sender_executor);
        }
        self.sync_producer_config_to_impl();
    }

    #[inline]
    pub fn set_send_msg_max_timeout_per_request(&mut self, timeout: u32) {
        self.producer_config.send_msg_max_timeout_per_request = Some(timeout);
        self.sync_producer_config_to_impl();
    }

    #[inline]
    pub fn set_send_msg_max_timeout_per_request_option(&mut self, timeout: Option<u32>) {
        self.producer_config.send_msg_max_timeout_per_request = timeout;
        self.sync_producer_config_to_impl();
    }

    #[inline]
    pub fn set_batch_max_delay_ms(&mut self, delay_ms: u32) {
        self.producer_config.batch_max_delay_ms = Some(delay_ms);
        if let Some(produce_accumulator) = self.producer_config.produce_accumulator.as_mut() {
            if let Err(error) = produce_accumulator.set_batch_max_delay_ms(delay_ms) {
                tracing::warn!("ignore invalid batchMaxDelayMs for ProduceAccumulator: {error}");
            }
        }
        self.sync_producer_config_to_impl();
    }

    #[inline]
    pub fn set_batch_max_delay_ms_option(&mut self, delay_ms: Option<u32>) {
        self.producer_config.batch_max_delay_ms = delay_ms;
        self.sync_producer_config_to_impl();
    }

    #[inline]
    pub fn set_batch_max_bytes(&mut self, bytes: u64) {
        self.producer_config.batch_max_bytes = Some(bytes);
        if let Some(produce_accumulator) = self.producer_config.produce_accumulator.as_mut() {
            if let Err(error) = produce_accumulator.set_batch_max_bytes(bytes) {
                tracing::warn!("ignore invalid batchMaxBytes for ProduceAccumulator: {error}");
            }
        }
        self.sync_producer_config_to_impl();
    }

    #[inline]
    pub fn set_batch_max_bytes_option(&mut self, bytes: Option<u64>) {
        self.producer_config.batch_max_bytes = bytes;
        self.sync_producer_config_to_impl();
    }

    #[inline]
    pub fn set_total_batch_max_bytes(&mut self, bytes: u64) {
        self.producer_config.total_batch_max_bytes = Some(bytes);
        if let Some(produce_accumulator) = self.producer_config.produce_accumulator.as_mut() {
            if let Err(error) = produce_accumulator.set_total_batch_max_bytes(bytes) {
                tracing::warn!("ignore invalid totalBatchMaxBytes for ProduceAccumulator: {error}");
            }
        }
        self.sync_producer_config_to_impl();
    }

    #[inline]
    pub fn set_total_batch_max_bytes_option(&mut self, bytes: Option<u64>) {
        self.producer_config.total_batch_max_bytes = bytes;
        self.sync_producer_config_to_impl();
    }

    pub fn producer_config(&self) -> &ProducerConfig {
        &self.producer_config
    }

    fn apply_batch_config_to_accumulator(&self, produce_accumulator: &mut ProduceAccumulator) {
        if let Some(delay_ms) = self.producer_config.batch_max_delay_ms {
            if let Err(error) = produce_accumulator.set_batch_max_delay_ms(delay_ms) {
                tracing::warn!("ignore invalid batchMaxDelayMs for ProduceAccumulator: {error}");
            }
        }
        if let Some(bytes) = self.producer_config.batch_max_bytes {
            if let Err(error) = produce_accumulator.set_batch_max_bytes(bytes) {
                tracing::warn!("ignore invalid batchMaxBytes for ProduceAccumulator: {error}");
            }
        }
        if let Some(bytes) = self.producer_config.total_batch_max_bytes {
            if let Err(error) = produce_accumulator.set_total_batch_max_bytes(bytes) {
                tracing::warn!("ignore invalid totalBatchMaxBytes for ProduceAccumulator: {error}");
            }
        }
    }

    #[inline]
    pub fn set_send_latency_fault_enable(&mut self, send_latency_fault_enable: bool) {
        self.client_config.set_send_latency_enable(send_latency_fault_enable);
        if let Some(ref mut default_mqproducer_impl) = self.default_mqproducer_impl {
            default_mqproducer_impl.set_send_latency_fault_enable(send_latency_fault_enable);
        }
    }

    #[inline]
    pub fn is_send_latency_fault_enable(&self) -> bool {
        self.default_mqproducer_impl
            .as_ref()
            .map(|producer_impl| producer_impl.is_send_latency_fault_enable())
            .unwrap_or_else(|| self.client_config.is_send_latency_enable())
    }

    #[inline]
    pub fn set_start_detector_enable(&mut self, start_detector_enable: bool) {
        self.client_config.set_start_detector_enable(start_detector_enable);
        if let Some(ref mut default_mqproducer_impl) = self.default_mqproducer_impl {
            default_mqproducer_impl.set_start_detector_enable(start_detector_enable);
        }
    }

    #[inline]
    pub fn is_start_detector_enable(&self) -> bool {
        self.default_mqproducer_impl
            .as_ref()
            .map(|producer_impl| producer_impl.is_start_detector_enable())
            .unwrap_or_else(|| self.client_config.is_start_detector_enable())
    }

    pub fn latency_max(&self) -> &[u64] {
        self.default_mqproducer_impl
            .as_ref()
            .map(|producer_impl| producer_impl.latency_max())
            .unwrap_or(&MQFaultStrategy::DEFAULT_LATENCY_MAX)
    }

    pub fn get_latency_max(&self) -> &[u64] {
        self.latency_max()
    }

    pub fn set_latency_max(&mut self, latency_max: impl Into<Vec<u64>>) {
        let latency_max = latency_max.into();
        if let Some(default_mqproducer_impl) = self.default_mqproducer_impl.as_mut() {
            default_mqproducer_impl.set_latency_max(latency_max);
        }
    }

    pub fn not_available_duration(&self) -> &[u64] {
        self.default_mqproducer_impl
            .as_ref()
            .map(|producer_impl| producer_impl.not_available_duration())
            .unwrap_or(&MQFaultStrategy::DEFAULT_NOT_AVAILABLE_DURATION)
    }

    pub fn get_not_available_duration(&self) -> &[u64] {
        self.not_available_duration()
    }

    pub fn set_not_available_duration(&mut self, not_available_duration: impl Into<Vec<u64>>) {
        let not_available_duration = not_available_duration.into();
        if let Some(default_mqproducer_impl) = self.default_mqproducer_impl.as_mut() {
            default_mqproducer_impl.set_not_available_duration(not_available_duration);
        }
    }

    pub fn init_produce_accumulator(&mut self) {
        let mut produce_accumulator =
            MQClientManager::get_instance().get_or_create_produce_accumulator(self.client_config.clone());
        self.apply_batch_config_to_accumulator(&mut produce_accumulator);
        self.producer_config.produce_accumulator = Some(produce_accumulator);
        self.sync_producer_config_to_impl();
    }

    pub fn set_back_pressure_for_async_send_num_inside_adjust(&mut self, back_pressure_for_async_send_num: u32) {
        self.set_back_pressure_for_async_send_num(back_pressure_for_async_send_num);
    }

    pub fn set_back_pressure_for_async_send_size_inside_adjust(&mut self, back_pressure_for_async_send_size: u32) {
        self.set_back_pressure_for_async_send_size(back_pressure_for_async_send_size);
    }

    pub fn acquire_back_pressure_for_async_send_num_lock(&self) {}

    pub fn release_back_pressure_for_async_send_num_lock(&self) {}

    pub fn acquire_back_pressure_for_async_send_size_lock(&self) {}

    pub fn release_back_pressure_for_async_send_size_lock(&self) {}

    fn batch<M>(&mut self, messages: Vec<M>) -> rocketmq_error::RocketMQResult<MessageBatch>
    where
        M: MessageTrait + Send + Sync,
    {
        let mut msg_batch = match MessageBatch::generate_from_vec(messages) {
            Ok(msg_batch) => msg_batch,
            Err(err) => {
                error!("Failed to initiate the MessageBatch: {:?}", err);
                return Err(err);
            }
        };

        for message in &mut msg_batch.messages {
            Validators::check_message::<Message>(Some(message), &self.producer_config)?;
            MessageClientIDSetter::set_uniq_id(message);
            message.set_topic(self.with_namespace(message.topic()));
        }
        MessageClientIDSetter::set_uniq_id(&mut msg_batch.final_message);
        msg_batch.set_body(msg_batch.encode());
        msg_batch.set_topic(self.with_namespace(msg_batch.topic()));
        Ok(msg_batch)
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
        send_callback: Option<ArcSendCallback>,
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
        } else if let Some(mq) = mq {
            producer
                .async_send_with_message_queue_callback(msg, mq, send_callback)
                .await?;
            Ok(None)
        } else {
            producer.async_send_with_callback(msg, send_callback).await?;
            Ok(None)
        }
    }

    pub async fn send_by_accumulator<M>(
        &mut self,
        mut msg: M,
        mq: Option<MessageQueue>,
        send_callback: Option<ArcSendCallback>,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + std::marker::Sync + 'static,
    {
        Validators::check_message(Some(&msg), self.producer_config())?;
        if !self.can_batch(&msg) {
            self.send_direct(msg, mq, send_callback).await
        } else {
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
        // delay message do not support batch processing
        if msg.delay_time_level() > 0
            || msg.get_delay_time_ms() > 0
            || msg.get_delay_time_sec() > 0
            || msg.get_deliver_time_ms() > 0
        {
            return false;
        }
        // retry message do not support batch processing
        if msg.topic().starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            return false;
        }
        // message which have been assigned to producer group do not support batch processing
        if msg.get_properties().contains_key(MessageConst::PROPERTY_PRODUCER_GROUP) {
            return false;
        }
        // produceAccumulator is full
        let Some(produce_accumulator) = self.producer_config.produce_accumulator.as_ref() else {
            return false;
        };
        if !produce_accumulator.try_add_message(msg) {
            return false;
        }
        true
    }
}

impl DefaultMQProducer {
    #[inline]
    pub fn with_namespace(&mut self, resource: impl Into<CheetahString>) -> CheetahString {
        self.client_config.with_namespace(resource)
    }

    fn prepare_trace_dispatcher(
        client_config: &ClientConfig,
        producer_config: &mut ProducerConfig,
        default_mqproducer_impl: &mut ArcMut<DefaultMQProducerImpl>,
    ) -> Option<ArcTraceDispatcher> {
        let dispatcher = match producer_config.trace_dispatcher.clone() {
            Some(dispatcher) => dispatcher,
            None if client_config.enable_trace => {
                let trace_topic = client_config
                    .trace_topic
                    .as_ref()
                    .map(|topic| topic.as_str())
                    .unwrap_or_default();
                let dispatcher = AsyncTraceDispatcher::new(
                    producer_config.producer_group.as_str(),
                    Type::Produce,
                    client_config.trace_msg_batch_num,
                    trace_topic,
                    producer_config.rpc_hook.clone(),
                );
                Arc::new(dispatcher)
            }
            None => return None,
        };

        if let Some(async_dispatcher) = dispatcher.as_any().downcast_ref::<AsyncTraceDispatcher>() {
            async_dispatcher.set_host_producer(default_mqproducer_impl.clone());
            async_dispatcher.set_namespace_v2(client_config.namespace_v2.clone());
            async_dispatcher.set_use_tls(client_config.use_tls);
        }

        producer_config.trace_dispatcher = Some(dispatcher.clone());
        default_mqproducer_impl.register_send_message_hook(Arc::new(SendMessageTraceHookImpl::new(dispatcher.clone())));
        default_mqproducer_impl
            .register_end_transaction_hook(Arc::new(EndTransactionTraceHookImpl::new(dispatcher.clone())));

        let recall_trace_hook: Arc<dyn RPCHook> = Arc::new(DefaultRecallMessageTraceHook::new(dispatcher.clone()));
        let rpc_hook = match producer_config.rpc_hook.clone() {
            Some(existing_hook) => CompositeRPCHook::chain(existing_hook, recall_trace_hook),
            None => recall_trace_hook,
        };
        producer_config.rpc_hook = Some(rpc_hook.clone());
        default_mqproducer_impl.set_rpc_hook(rpc_hook);

        Some(dispatcher)
    }
}

impl MQProducer for DefaultMQProducer {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let producer_group_clone = self.producer_config.producer_group.clone();
        let producer_group = self.with_namespace(&producer_group_clone);
        self.set_producer_group(producer_group);
        let default_mqproducer_impl = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?;

        let trace_dispatcher_to_start =
            Self::prepare_trace_dispatcher(&self.client_config, &mut self.producer_config, default_mqproducer_impl);

        default_mqproducer_impl.start().await?;
        if let Some(ref mut produce_accumulator) = self.producer_config.produce_accumulator {
            produce_accumulator.start();
        }
        if let Some(dispatcher) = trace_dispatcher_to_start {
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
        Ok(())
    }

    async fn shutdown(&mut self) {
        if let Some(ref mut default_mqproducer_impl) = self.default_mqproducer_impl {
            if let Err(e) = default_mqproducer_impl.shutdown().await {
                error!("DefaultMQProducerImpl shutdown error: {:?}", e);
            }
        }

        if let Some(ref mut produce_accumulator) = self.producer_config.produce_accumulator {
            produce_accumulator.shutdown_async().await;
        }

        if let Some(ref trace_dispatcher) = self.producer_config.trace_dispatcher {
            if let Some(async_dispatcher) = trace_dispatcher.as_any().downcast_ref::<AsyncTraceDispatcher>() {
                async_dispatcher.shutdown_async().await;
            } else {
                trace_dispatcher.shutdown();
            }
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
        let new_topic = self.with_namespace(new_topic);
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .create_topic(key, new_topic.as_str(), queue_num, topic_sys_flag, attributes)
            .await
    }

    async fn search_offset(&mut self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .search_offset(mq, timestamp)
            .await
    }

    async fn max_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .max_offset(mq)
            .await
    }

    async fn min_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .min_offset(mq)
            .await
    }

    async fn earliest_msg_store_time(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .earliest_msg_store_time(mq)
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
        let topic = self.with_namespace(topic);
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .query_message(topic.as_str(), key, max_num, begin, end)
            .await
    }

    async fn view_message(&mut self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        let view_result = {
            let producer_impl = self
                .default_mqproducer_impl
                .as_mut()
                .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?;
            producer_impl.view_message(topic, msg_id).await
        };
        match view_result {
            Ok(message) => Ok(message),
            Err(_) => {
                let topic = self.with_namespace(topic);
                self.default_mqproducer_impl
                    .as_mut()
                    .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
                    .query_message_by_uniq_key(topic.as_str(), msg_id)
                    .await
            }
        }
    }

    async fn send<M>(&mut self, mut msg: M) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.topic()));
        if self.get_auto_batch() && msg.as_any().downcast_ref::<MessageBatch>().is_none() {
            self.send_by_accumulator(msg, None, None).await
        } else {
            let timeout = self.producer_config.send_msg_timeout() as u64;
            self.default_mqproducer_impl
                .as_mut()
                .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
                .send_with_timeout(&mut msg, timeout)
                .await
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
        let topic_build = self.with_namespace(msg.topic().as_str());
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
        msg.set_topic(self.with_namespace(msg.topic()));
        let send_callback_inner = Arc::new(send_callback);
        let result = if self.get_auto_batch() && msg.as_any().downcast_ref::<MessageBatch>().is_none() {
            self.send_by_accumulator(msg, None, Some(send_callback_inner.clone()))
                .await
        } else {
            let timeout = self.producer_config.send_msg_timeout() as u64;
            self.default_mqproducer_impl
                .as_mut()
                .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
                .async_send_with_callback_timeout(msg, Some(send_callback_inner.clone()), timeout)
                .await
                .map(|()| None)
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
        msg.set_topic(self.with_namespace(msg.topic().as_str()));
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
        msg.set_topic(self.with_namespace(msg.topic()));
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
        msg.set_topic(self.with_namespace(msg.topic()));
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
        msg.set_topic(self.with_namespace(msg.topic()));
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
        msg.set_topic(self.with_namespace(msg.topic()));
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
        msg.set_topic(self.with_namespace(msg.topic()));
        let mq = self.client_config.queue_with_namespace(mq);
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
        msg.set_topic(self.with_namespace(msg.topic()));
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
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.topic()));
        let mq = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .invoke_message_queue_selector(&mut msg, selector, &arg, self.producer_config.send_msg_timeout() as u64)
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
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_with_selector_timeout(msg, selector, arg, timeout)
            .await
    }

    async fn send_with_selector_callback<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
        send_callback: Option<ArcSendCallback>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.topic()));
        let mq = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .invoke_message_queue_selector(&mut msg, selector, &arg, self.producer_config.send_msg_timeout() as u64)
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
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        msg.set_topic(self.with_namespace(msg.topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_with_selector_callback_timeout(msg, selector, arg, send_callback, timeout)
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
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        msg.set_topic(self.with_namespace(msg.topic()));
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .send_oneway_with_selector(msg, selector, arg)
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
        Err(crate::mq_client_err!(
            "sendMessageInTransaction not implement, please use TransactionMQProducer class"
        ))
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
        result.ok_or_else(|| crate::mq_client_err!("Synchronous batch send completed without SendResult"))
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
        result.ok_or_else(|| crate::mq_client_err!("Synchronous batch send completed without SendResult"))
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
        let mq = self.client_config.queue_with_namespace(mq);
        let result = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .sync_send_with_message_queue(batch, mq)
            .await?;
        result.ok_or_else(|| crate::mq_client_err!("Synchronous batch send completed without SendResult"))
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
        let mq = self.client_config.queue_with_namespace(mq);
        let result = self
            .default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .sync_send_with_message_queue_timeout(batch, mq, timeout)
            .await?;
        result.ok_or_else(|| crate::mq_client_err!("Synchronous batch send completed without SendResult"))
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
        let mq = self.client_config.queue_with_namespace(mq);
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
        let mq = self.client_config.queue_with_namespace(mq);
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
        msg.set_topic(self.with_namespace(msg.topic()));
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
        msg.set_topic(self.with_namespace(msg.topic()));
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
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.topic()));
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
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        T: Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        msg.set_topic(self.with_namespace(msg.topic()));
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
        msg.set_topic(self.with_namespace(msg.topic()));
        let mq = self.client_config.queue_with_namespace(mq);
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
        msg.set_topic(self.with_namespace(msg.topic()));
        let mq = self.client_config.queue_with_namespace(mq);
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

    async fn recall_message(
        &mut self,
        topic: impl Into<CheetahString>,
        recall_handle: impl Into<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<String> {
        self.default_mqproducer_impl
            .as_mut()
            .ok_or(RocketMQError::not_initialized("DefaultMQProducerImpl not initialized"))?
            .recall_message(topic, recall_handle)
            .await
    }
}

#[cfg(test)]
mod facade_tests {
    use bytes::Bytes;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::common::message::message_single::Message;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_error::RocketMQError;
    use rocketmq_error::RocketMQResult;

    use super::DefaultMQProducer;
    use super::ProducerConfig;
    use crate::base::client_config::ClientConfig;
    use crate::producer::send_result::SendResult;

    fn unstarted_producer() -> DefaultMQProducer {
        DefaultMQProducer {
            client_config: ClientConfig::default(),
            producer_config: ProducerConfig::default(),
            default_mqproducer_impl: None,
        }
    }

    fn message() -> Message {
        Message::builder()
            .topic("test-topic")
            .body(Bytes::from_static(b"facade"))
            .build_unchecked()
    }

    fn queue() -> MessageQueue {
        MessageQueue::from_parts("test-topic", "broker-a", 0)
    }

    fn assert_not_initialized<T>(result: RocketMQResult<T>) {
        match result {
            Err(RocketMQError::NotInitialized(reason)) => {
                assert!(reason.contains("not initialized"), "unexpected error message: {reason}");
            }
            Err(other) => panic!("Unexpected error: {other:?}"),
            Ok(_) => panic!("expected producer facade to require a started implementation"),
        }
    }

    #[tokio::test]
    async fn default_mq_producer_exposes_modern_java_send_facade_methods_without_trait_import() {
        assert_not_initialized(unstarted_producer().send(message()).await);
        assert_not_initialized(unstarted_producer().send_with_timeout(message(), 1000).await);
        assert_not_initialized(
            unstarted_producer()
                .send_to_queue_with_timeout(message(), queue(), 1000)
                .await,
        );
        assert_not_initialized(unstarted_producer().send_oneway_to_queue(message(), queue()).await);

        let selector = |_queues: &[MessageQueue], _msg: &Message, _arg: &i32| -> Option<MessageQueue> { None };
        assert_not_initialized(
            unstarted_producer()
                .send_with_selector_timeout(message(), selector, 1, 1000)
                .await,
        );

        let callback = |_result: Option<&SendResult>, _err: Option<&dyn std::error::Error>| {};
        assert_not_initialized(
            unstarted_producer()
                .send_batch_to_queue_with_callback_timeout(vec![message()], queue(), callback, 1000)
                .await,
        );
    }

    #[tokio::test]
    async fn default_mq_producer_exposes_modern_java_request_facade_methods_without_trait_import() {
        let request_callback = |_msg: Option<&dyn MessageTrait>, _err: Option<&dyn std::error::Error>| {};
        assert_not_initialized(
            unstarted_producer()
                .request_with_callback(message(), request_callback, 1000)
                .await,
        );

        let selector = |_queues: &[MessageQueue], _msg: &Message, _arg: &i32| -> Option<MessageQueue> { None };
        assert_not_initialized(
            unstarted_producer()
                .request_with_selector(message(), selector, 1, 1000)
                .await,
        );

        assert_not_initialized(unstarted_producer().request_to_queue(message(), queue(), 1000).await);

        let request_callback = |_msg: Option<&dyn MessageTrait>, _err: Option<&dyn std::error::Error>| {};
        assert_not_initialized(
            unstarted_producer()
                .request_to_queue_with_callback(message(), queue(), request_callback, 1000)
                .await,
        );
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
        let msg = Message::builder().topic("test-topic").empty_body().build_unchecked();
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
        let msg = Message::builder().topic("test-topic").empty_body().build_unchecked();
        let selector = |_queues: &[MessageQueue], _msg: &Message, _arg: &i32| -> Option<MessageQueue> { None };
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
        let msg = Message::builder().topic("test-topic").empty_body().build_unchecked();
        let selector = |_queues: &[MessageQueue], _msg: &Message, _arg: &i32| -> Option<MessageQueue> { None };
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
        let msg = Message::builder()
            .topic("test-topic")
            .body(Bytes::from_static(b"Hello world"))
            .build_unchecked();
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
    async fn send_batch_delay_millis_returns_java_compatible_batch_error_before_start_check() {
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };
        let msg1 = Message::builder()
            .topic("test-topic")
            .body(Bytes::from_static(b"Hello world"))
            .build_unchecked();
        let msg2 = Message::builder()
            .topic("test-topic")
            .body(Bytes::from_static(b"Delayed"))
            .delay_millis(1000)
            .build_unchecked();

        let result = producer.send_batch(vec![msg1, msg2]).await;

        let err = result.expect_err("delayed batch message should be rejected before send");
        assert!(
            err.to_string()
                .contains("Delayed messages are not supported for batching"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn default_producer_transaction_send_returns_java_compatible_error() {
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };
        let msg = Message::builder()
            .topic("test-topic")
            .body(Bytes::from_static(b"transaction"))
            .build_unchecked();

        let result = producer.send_message_in_transaction(msg, Option::<()>::None).await;

        let err = result.expect_err("DefaultMQProducer should reject transaction sends");
        assert!(
            err.to_string()
                .contains("sendMessageInTransaction not implement, please use TransactionMQProducer class"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn send_batch_with_callback_timeout_not_initialized() {
        // Arrange
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };
        let msg = Message::builder()
            .topic("test-topic")
            .body(Bytes::from_static(b"Hello world"))
            .build_unchecked();
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
        let msg = Message::builder()
            .topic("test-topic")
            .body(Bytes::from_static(b"Hello world"))
            .build_unchecked();
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

    #[tokio::test]
    async fn recall_message_not_initialized() {
        // Arrange
        let mut producer = DefaultMQProducer {
            client_config: Default::default(),
            producer_config: Default::default(),
            default_mqproducer_impl: None,
        };

        // Act
        let result = producer.recall_message("test-topic", "recall-handle-123").await;

        // Assert
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            RocketMQError::NotInitialized(reason) => {
                assert!(reason.contains("not initialized"), "unexpected error message: {reason}");
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn trace_dispatcher_uses_configured_batch_num_like_java() {
        let mut client_config = ClientConfig::default();
        client_config.set_enable_trace(true);
        client_config.set_trace_msg_batch_num(7);
        let mut producer = DefaultMQProducer::builder()
            .client_config(client_config)
            .producer_group("producer_trace_batch_group")
            .build();
        let dispatcher = {
            let default_mqproducer_impl = producer
                .default_mqproducer_impl
                .as_mut()
                .expect("producer impl should exist");
            DefaultMQProducer::prepare_trace_dispatcher(
                &producer.client_config,
                &mut producer.producer_config,
                default_mqproducer_impl,
            )
            .expect("trace dispatcher should be created")
        };

        let async_dispatcher = dispatcher
            .as_any()
            .downcast_ref::<AsyncTraceDispatcher>()
            .expect("default trace dispatcher should be AsyncTraceDispatcher");
        assert_eq!(async_dispatcher.batch_num(), 7);
    }

    #[test]
    fn producer_use_tls_facade_updates_impl_and_trace_dispatcher_like_java_client_config() {
        let mut producer = DefaultMQProducer::builder()
            .producer_group("producer_tls_group")
            .build();
        let dispatcher = Arc::new(AsyncTraceDispatcher::new(
            "producer_tls_group",
            Type::Produce,
            1,
            "",
            None,
        ));
        producer.set_trace_dispatcher(dispatcher.clone());

        assert!(!producer.is_use_tls());
        assert!(!dispatcher.is_use_tls());
        assert!(!producer
            .default_mqproducer_impl
            .as_ref()
            .expect("producer impl should exist")
            .is_use_tls());

        producer.set_use_tls(true);

        assert!(producer.is_use_tls());
        assert!(producer.client_config().is_use_tls());
        assert!(dispatcher.is_use_tls());
        assert!(producer
            .default_mqproducer_impl
            .as_ref()
            .expect("producer impl should exist")
            .is_use_tls());
    }

    #[test]
    fn producer_builder_use_tls_initializes_facade_and_impl_config() {
        let producer = DefaultMQProducer::builder()
            .producer_group("producer_tls_builder_group")
            .use_tls(true)
            .build();

        assert!(producer.is_use_tls());
        assert!(producer
            .default_mqproducer_impl
            .as_ref()
            .expect("producer impl should exist")
            .is_use_tls());
    }

    #[test]
    fn test_builder_with_batch_fields() {
        use crate::producer::default_mq_produce_builder::DefaultMQProducerBuilder;

        let producer = DefaultMQProducerBuilder::new()
            .producer_group("test_group")
            .send_msg_max_timeout_per_request(5000)
            .batch_max_delay_ms(100)
            .batch_max_bytes(2 * 1024 * 1024)
            .total_batch_max_bytes(64 * 1024 * 1024)
            .build();

        assert_eq!(producer.producer_group().to_string(), "test_group");
        assert_eq!(producer.send_msg_max_timeout_per_request(), Some(5000));
        assert_eq!(producer.batch_max_delay_ms(), Some(100));
        assert_eq!(producer.batch_max_bytes(), Some(2 * 1024 * 1024));
        assert_eq!(producer.total_batch_max_bytes(), Some(64 * 1024 * 1024));
    }

    #[test]
    fn test_builder_default_batch_values() {
        use crate::producer::default_mq_produce_builder::DefaultMQProducerBuilder;

        let producer = DefaultMQProducerBuilder::new().producer_group("test_group").build();

        // Verify default values are None for batch fields
        assert_eq!(producer.send_msg_max_timeout_per_request(), None);
        assert_eq!(producer.batch_max_delay_ms(), None);
        assert_eq!(producer.batch_max_bytes(), None);
        assert_eq!(producer.total_batch_max_bytes(), None);
    }

    fn assert_producer_compressor_matches_type(producer: &DefaultMQProducer, compression_type: CompressionType) {
        let body = b"rocketmq producer compressor parity";
        let compressed = producer
            .compressor()
            .expect("producer should have compressor")
            .compress(body, producer.compress_level())
            .expect("producer compressor should compress");
        let decompressed = compression_type
            .try_decompression(&compressed)
            .expect("matching compression type should decompress producer body");

        assert_eq!(decompressed.as_ref(), body);
    }

    #[test]
    fn producer_config_invalid_env_compress_type_falls_back_to_zlib_without_panic() {
        assert_eq!(
            ProducerConfig::parse_compression_type_or_zlib("snappy"),
            CompressionType::Zlib
        );
    }

    #[test]
    fn set_compress_type_updates_compressor_like_java() {
        let mut producer = DefaultMQProducer::default();

        producer.set_compress_type(CompressionType::LZ4);

        assert_eq!(producer.compress_type(), CompressionType::LZ4);
        assert_producer_compressor_matches_type(&producer, CompressionType::LZ4);
    }

    #[test]
    fn builder_compress_type_updates_compressor_like_java() {
        use crate::producer::default_mq_produce_builder::DefaultMQProducerBuilder;

        let producer = DefaultMQProducerBuilder::new()
            .producer_group("test_group")
            .compress_type(CompressionType::Zstd)
            .build();

        assert_eq!(producer.compress_type(), CompressionType::Zstd);
        assert_producer_compressor_matches_type(&producer, CompressionType::Zstd);
    }

    #[test]
    fn produce_accumulator_inherits_preconfigured_batch_settings_like_java_init() {
        let mut producer = DefaultMQProducer::default();
        producer.set_batch_max_delay_ms(25);
        producer.set_batch_max_bytes(64 * 1024);
        producer.set_total_batch_max_bytes(8 * 1024 * 1024);

        producer.set_produce_accumulator(ProduceAccumulator::new("producer-accumulator"));

        let accumulator = producer
            .produce_accumulator()
            .expect("produce accumulator should be configured");
        assert_eq!(accumulator.batch_max_delay_ms(), 25);
        assert_eq!(accumulator.batch_max_bytes(), 64 * 1024);
        assert_eq!(accumulator.total_batch_max_bytes(), 8 * 1024 * 1024);
    }

    #[test]
    fn batch_setting_updates_existing_produce_accumulator_like_java_runtime_setters() {
        let mut producer = DefaultMQProducer::default();
        producer.set_produce_accumulator(ProduceAccumulator::new("producer-accumulator"));

        producer.set_batch_max_delay_ms(20);
        producer.set_batch_max_bytes(128 * 1024);
        producer.set_total_batch_max_bytes(16 * 1024 * 1024);

        let accumulator = producer
            .produce_accumulator()
            .expect("produce accumulator should be configured");
        assert_eq!(accumulator.batch_max_delay_ms(), 20);
        assert_eq!(accumulator.batch_max_bytes(), 128 * 1024);
        assert_eq!(accumulator.total_batch_max_bytes(), 16 * 1024 * 1024);
    }

    #[test]
    fn backpressure_runtime_setters_sync_impl_and_clamp_like_java() {
        let mut producer = DefaultMQProducer::builder()
            .producer_group("backpressure_group")
            .build();

        producer.set_enable_backpressure_for_async_mode(true);
        producer.set_back_pressure_for_async_send_num(1);
        producer.set_back_pressure_for_async_send_size(128);

        assert!(producer.enable_backpressure_for_async_mode());
        assert_eq!(
            producer.back_pressure_for_async_send_num(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM
        );
        assert_eq!(
            producer.get_back_pressure_for_async_send_num(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM
        );
        assert_eq!(
            producer.back_pressure_for_async_send_size(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE
        );
        assert_eq!(
            producer.get_back_pressure_for_async_send_size(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE
        );

        let producer_impl = producer
            .default_mqproducer_impl
            .as_ref()
            .expect("producer impl should be initialized");
        assert!(producer_impl.enable_backpressure_for_async_mode());
        assert_eq!(
            producer_impl.back_pressure_for_async_send_num(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM
        );
        assert_eq!(
            producer_impl.back_pressure_for_async_send_size(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE
        );
    }

    #[test]
    fn runtime_producer_config_setters_sync_impl_like_java_facade_reference() {
        let mut producer = DefaultMQProducer::builder().producer_group("initial_group").build();

        producer.set_producer_group("updated_group");
        producer.set_send_msg_timeout(4321);
        producer.set_compress_msg_body_over_howmuch(8192);
        producer.set_max_message_size(16 * 1024 * 1024);
        producer.set_retry_times_when_send_failed(5);
        producer.set_retry_times_when_send_async_failed(6);
        producer.set_compress_type(CompressionType::LZ4);
        producer.set_compress_level(3);
        producer.add_retry_response_code(12_345);

        assert_eq!(producer.get_send_msg_timeout(), 4321);
        assert_eq!(producer.get_compress_msg_body_over_howmuch(), 8192);
        assert_eq!(producer.get_max_message_size(), 16 * 1024 * 1024);
        assert_eq!(producer.get_retry_times_when_send_failed(), 5);
        assert_eq!(producer.get_retry_times_when_send_async_failed(), 6);
        assert_eq!(producer.get_compress_level(), 3);

        let producer_impl = producer
            .default_mqproducer_impl
            .as_ref()
            .expect("producer impl should be initialized");
        let impl_config = producer_impl.producer_config();

        assert_eq!(impl_config.producer_group(), "updated_group");
        assert_eq!(impl_config.send_msg_timeout(), 4321);
        assert_eq!(impl_config.retry_times_when_send_failed(), 5);
        assert_eq!(impl_config.compress_type(), CompressionType::LZ4);
        assert!(impl_config.compressor().is_some());
        assert!(producer.retry_response_codes().contains(&12_345));
        assert!(impl_config.retry_response_codes().contains(&12_345));
    }

    #[test]
    fn producer_java_facade_accessors_sync_impl_without_panic() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let handle = runtime.handle().clone();
        let mut producer = DefaultMQProducer::builder()
            .producer_group("producer_java_facade_group")
            .build();

        assert!(producer.default_mq_producer_impl().is_some());

        producer.set_callback_executor(handle.clone());
        producer.set_async_sender_executor(handle);
        producer.set_latency_max(vec![10, 20, 30]);
        producer.set_not_available_duration(vec![0, 100, 200]);
        producer.set_back_pressure_for_async_send_num_inside_adjust(1);
        producer.set_back_pressure_for_async_send_size_inside_adjust(128);
        producer.acquire_back_pressure_for_async_send_num_lock();
        producer.release_back_pressure_for_async_send_num_lock();
        producer.acquire_back_pressure_for_async_send_size_lock();
        producer.release_back_pressure_for_async_send_size_lock();

        assert!(producer.callback_executor().is_some());
        assert!(producer.async_sender_executor().is_some());
        assert_eq!(producer.latency_max(), &[10, 20, 30]);
        assert_eq!(producer.not_available_duration(), &[0, 100, 200]);
        assert_eq!(
            producer.back_pressure_for_async_send_num(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM
        );
        assert_eq!(
            producer.back_pressure_for_async_send_size(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE
        );

        let producer_impl = producer
            .default_mq_producer_impl()
            .expect("producer impl should be initialized");
        let impl_config = producer_impl.producer_config();
        assert!(impl_config.callback_executor().is_some());
        assert!(impl_config.async_sender_executor().is_some());
        assert_eq!(producer_impl.latency_max(), &[10, 20, 30]);
        assert_eq!(producer_impl.not_available_duration(), &[0, 100, 200]);
    }

    #[test]
    fn test_producer_config_getters_setters_batch_fields() {
        let mut producer = DefaultMQProducer::default();

        // Test send_msg_max_timeout_per_request
        producer.set_send_msg_max_timeout_per_request(3000);
        assert_eq!(producer.send_msg_max_timeout_per_request(), Some(3000));

        producer.set_send_msg_max_timeout_per_request_option(None);
        assert_eq!(producer.send_msg_max_timeout_per_request(), None);

        // Test batch_max_delay_ms
        producer.set_batch_max_delay_ms(50);
        assert_eq!(producer.batch_max_delay_ms(), Some(50));

        producer.set_batch_max_delay_ms_option(None);
        assert_eq!(producer.batch_max_delay_ms(), None);

        // Test batch_max_bytes
        producer.set_batch_max_bytes(4 * 1024 * 1024);
        assert_eq!(producer.batch_max_bytes(), Some(4 * 1024 * 1024));

        producer.set_batch_max_bytes_option(None);
        assert_eq!(producer.batch_max_bytes(), None);

        // Test total_batch_max_bytes
        producer.set_total_batch_max_bytes(128 * 1024 * 1024);
        assert_eq!(producer.total_batch_max_bytes(), Some(128 * 1024 * 1024));

        producer.set_total_batch_max_bytes_option(None);
        assert_eq!(producer.total_batch_max_bytes(), None);
    }

    #[test]
    fn test_producer_config_default_values_match_java() {
        let config = ProducerConfig::default();

        // Verify default values match Java
        assert_eq!(config.back_pressure_for_async_send_num, 1024); // Java default

        // Verify batch fields default to None
        assert_eq!(config.send_msg_max_timeout_per_request, None);
        assert_eq!(config.batch_max_delay_ms, None);
        assert_eq!(config.batch_max_bytes, None);
        assert_eq!(config.total_batch_max_bytes, None);
    }
}
