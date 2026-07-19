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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_error::RocketMQError;

use crate::base::query_result::QueryResult;
use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::mq_producer::MQProducer;
use crate::producer::send_callback::ArcSendCallback;
use crate::producer::send_result::SendResult;
use crate::producer::transaction_listener::ArcTransactionListener;
use crate::producer::transaction_listener::TransactionListener;
use crate::producer::transaction_mq_produce_builder::TransactionMQProducerBuilder;
use crate::producer::transaction_send_result::TransactionSendResult;

/// Configuration for transaction message producer
///
/// Controls transaction check behavior, aligning with Java's TransactionMQProducer.
#[derive(Clone)]
pub struct TransactionProducerConfig {
    /// Transaction listener for executing and checking local transactions
    pub transaction_listener: Option<ArcTransactionListener>,

    /// Deprecated Java-style transaction check listener facade.
    pub transaction_check_listener: Option<ArcTransactionListener>,

    /// Optional Tokio runtime handle used for broker transaction check execution.
    pub executor_service: Option<tokio::runtime::Handle>,

    /// Minimum size of transaction check thread pool (corresponds to Java checkThreadPoolMinSize)
    ///
    /// Note: When using spawn_blocking with default Tokio Runtime, this serves as a reference.
    /// Default: 1
    pub check_thread_pool_min_size: u32,

    /// Maximum size of transaction check thread pool (corresponds to Java checkThreadPoolMaxSize)
    ///
    /// Note: When using spawn_blocking with default Tokio Runtime, this serves as a reference.
    /// To control actual thread count, configure the Runtime:
    /// ```ignore
    /// tokio::runtime::Builder::new_multi_thread()
    ///     .max_blocking_threads(100)
    ///     .build()
    /// ```
    /// Default: 1
    pub check_thread_pool_max_size: u32,

    /// Maximum capacity of transaction check request queue (corresponds to Java
    /// checkRequestHoldMax)
    ///
    /// This limits queued broker transaction check requests before worker execution.
    /// Default: 2000
    pub check_request_hold_max: u32,
}

impl Default for TransactionProducerConfig {
    fn default() -> Self {
        Self {
            transaction_listener: None,
            transaction_check_listener: None,
            executor_service: None,
            check_thread_pool_min_size: 1,
            check_thread_pool_max_size: 1,
            check_request_hold_max: 2000,
        }
    }
}

#[derive(Default)]
pub struct TransactionMQProducer {
    default_producer: DefaultMQProducer,
    transaction_producer_config: TransactionProducerConfig,
}

impl TransactionMQProducer {
    pub fn builder() -> TransactionMQProducerBuilder {
        TransactionMQProducerBuilder::new()
    }

    pub(crate) fn new(
        transaction_producer_config: TransactionProducerConfig,
        default_producer: DefaultMQProducer,
    ) -> Self {
        Self {
            default_producer,
            transaction_producer_config,
        }
    }

    pub async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        <Self as MQProducer>::start(self).await
    }

    pub async fn shutdown(&mut self) {
        <Self as MQProducer>::shutdown(self).await;
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

    pub fn set_transaction_listener(&mut self, transaction_listener: impl TransactionListener) {
        self.set_transaction_listener_arc(Arc::new(transaction_listener));
    }

    pub fn transaction_listener(&self) -> Option<ArcTransactionListener> {
        self.transaction_producer_config.transaction_listener.clone()
    }

    pub fn get_transaction_listener(&self) -> Option<ArcTransactionListener> {
        self.transaction_listener()
    }

    pub fn transaction_check_listener(&self) -> Option<ArcTransactionListener> {
        self.transaction_producer_config.transaction_check_listener.clone()
    }

    pub fn get_transaction_check_listener(&self) -> Option<ArcTransactionListener> {
        self.transaction_check_listener()
    }

    pub fn set_transaction_check_listener(&mut self, transaction_check_listener: impl TransactionListener) {
        self.set_transaction_check_listener_arc(Arc::new(transaction_check_listener));
    }

    pub fn set_transaction_check_listener_arc(&mut self, transaction_check_listener: ArcTransactionListener) {
        self.transaction_producer_config.transaction_check_listener = Some(transaction_check_listener);
    }

    pub fn set_transaction_listener_arc(&mut self, transaction_listener: ArcTransactionListener) {
        self.transaction_producer_config.transaction_listener = Some(transaction_listener.clone());
        if let Some(default_mqproducer_impl) = self.default_producer.default_mqproducer_impl.as_ref() {
            default_mqproducer_impl.set_transaction_listener(transaction_listener);
        } else {
            tracing::warn!("DefaultMQProducerImpl is not initialized; transaction listener stored in config");
        }
    }

    pub fn check_thread_pool_min_size(&self) -> u32 {
        self.transaction_producer_config.check_thread_pool_min_size
    }

    pub fn get_check_thread_pool_min_size(&self) -> u32 {
        self.check_thread_pool_min_size()
    }

    pub fn set_check_thread_pool_min_size(&mut self, check_thread_pool_min_size: u32) {
        self.transaction_producer_config.check_thread_pool_min_size = check_thread_pool_min_size;
    }

    pub fn check_thread_pool_max_size(&self) -> u32 {
        self.transaction_producer_config.check_thread_pool_max_size
    }

    pub fn get_check_thread_pool_max_size(&self) -> u32 {
        self.check_thread_pool_max_size()
    }

    pub fn set_check_thread_pool_max_size(&mut self, check_thread_pool_max_size: u32) {
        self.transaction_producer_config.check_thread_pool_max_size = check_thread_pool_max_size;
    }

    pub fn check_request_hold_max(&self) -> u32 {
        self.transaction_producer_config.check_request_hold_max
    }

    pub fn get_check_request_hold_max(&self) -> u32 {
        self.check_request_hold_max()
    }

    pub fn set_check_request_hold_max(&mut self, check_request_hold_max: u32) {
        self.transaction_producer_config.check_request_hold_max = check_request_hold_max;
    }

    pub fn executor_service(&self) -> Option<&tokio::runtime::Handle> {
        self.transaction_producer_config.executor_service.as_ref()
    }

    pub fn get_executor_service(&self) -> Option<&tokio::runtime::Handle> {
        self.executor_service()
    }

    pub fn set_executor_service(&mut self, executor_service: tokio::runtime::Handle) {
        self.transaction_producer_config.executor_service = Some(executor_service.clone());
        if let Some(default_mqproducer_impl) = self.default_producer.default_mqproducer_impl.as_ref() {
            default_mqproducer_impl.set_transaction_executor_service(Some(executor_service));
        }
    }

    #[inline]
    pub fn is_use_tls(&self) -> bool {
        self.default_producer.is_use_tls()
    }

    #[inline]
    pub fn set_use_tls(&mut self, use_tls: bool) {
        self.default_producer.set_use_tls(use_tls);
    }
}

impl MQProducer for TransactionMQProducer {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        let transaction_listener = self.transaction_producer_config.transaction_listener.clone();
        let default_mqproducer_impl =
            self.default_producer
                .default_mqproducer_impl
                .as_ref()
                .ok_or(RocketMQError::not_initialized(
                    "DefaultMQProducerImpl is not initialized",
                ))?;
        if let Some(transaction_listener) = transaction_listener {
            default_mqproducer_impl.set_transaction_listener(transaction_listener);
        }
        default_mqproducer_impl
            .set_transaction_executor_service(self.transaction_producer_config.executor_service.clone());
        default_mqproducer_impl.init_transaction_env(
            self.transaction_producer_config.check_thread_pool_min_size,
            self.transaction_producer_config.check_thread_pool_max_size,
            self.transaction_producer_config.check_request_hold_max,
        )?;
        self.default_producer.start().await
    }

    async fn shutdown(&mut self) {
        self.default_producer.shutdown().await;
        if let Some(default_mqproducer_impl) = self.default_producer.default_mqproducer_impl.as_ref() {
            default_mqproducer_impl.destroy_transaction_env().await;
        }
    }

    async fn fetch_publish_message_queues(&mut self, topic: &str) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        self.default_producer.fetch_publish_message_queues(topic).await
    }

    async fn create_topic(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.default_producer
            .create_topic(key, new_topic, queue_num, attributes)
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
        self.default_producer
            .create_topic_with_flag(key, new_topic, queue_num, topic_sys_flag, attributes)
            .await
    }

    async fn search_offset(&mut self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        self.default_producer.search_offset(mq, timestamp).await
    }

    async fn max_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.default_producer.max_offset(mq).await
    }

    async fn min_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.default_producer.min_offset(mq).await
    }

    async fn earliest_msg_store_time(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.default_producer.earliest_msg_store_time(mq).await
    }

    async fn query_message(
        &mut self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        self.default_producer
            .query_message(topic, key, max_num, begin, end)
            .await
    }

    async fn view_message(&mut self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        self.default_producer.view_message(topic, msg_id).await
    }

    async fn send<M>(&mut self, msg: M) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.send(msg).await
    }

    async fn send_with_timeout<M>(&mut self, msg: M, timeout: u64) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.send_with_timeout(msg, timeout).await
    }

    async fn send_with_callback<M, F>(&mut self, msg: M, send_callback: F) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&RocketMQError>) + Send + Sync + 'static,
    {
        self.default_producer.send_with_callback(msg, send_callback).await
    }

    async fn send_with_callback_timeout<F, M>(
        &mut self,
        msg: M,
        send_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Fn(Option<&SendResult>, Option<&RocketMQError>) + Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        self.default_producer
            .send_with_callback_timeout(msg, send_callback, timeout)
            .await
    }

    async fn send_oneway<M>(&mut self, msg: M) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.send_oneway(msg).await
    }

    async fn send_to_queue<M>(&mut self, msg: M, mq: MessageQueue) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.send_to_queue(msg, mq).await
    }

    async fn send_to_queue_with_timeout<M>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.send_to_queue_with_timeout(msg, mq, timeout).await
    }

    async fn send_to_queue_with_callback<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        send_callback: F,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&RocketMQError>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_to_queue_with_callback(msg, mq, send_callback)
            .await
    }

    async fn send_to_queue_with_callback_timeout<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        send_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&RocketMQError>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_to_queue_with_callback_timeout(msg, mq, send_callback, timeout)
            .await
    }

    async fn send_oneway_to_queue<M>(&mut self, msg: M, mq: MessageQueue) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.send_oneway_to_queue(msg, mq).await
    }

    async fn send_with_selector<M, S, T>(
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
        self.default_producer.send_with_selector(msg, selector, arg).await
    }

    async fn send_with_selector_timeout<M, S, T>(
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
        self.default_producer
            .send_with_selector_timeout(msg, selector, arg, timeout)
            .await
    }

    async fn send_with_selector_callback<M, S, T>(
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
        self.default_producer
            .send_with_selector_callback(msg, selector, arg, send_callback)
            .await
    }

    async fn send_with_selector_callback_timeout<M, S, T>(
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
        self.default_producer
            .send_with_selector_callback_timeout(msg, selector, arg, send_callback, timeout)
            .await
    }

    async fn send_oneway_with_selector<M, S, T>(
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
        self.default_producer
            .send_oneway_with_selector(msg, selector, arg)
            .await
    }

    async fn send_message_in_transaction<T, M>(
        &mut self,
        mut msg: M,
        arg: Option<T>,
    ) -> rocketmq_error::RocketMQResult<TransactionSendResult>
    where
        T: std::any::Any + Sync + Send,
        M: MessageTrait + Send + Sync,
    {
        let transaction_listener = self
            .transaction_producer_config
            .transaction_listener
            .clone()
            .ok_or_else(|| crate::mq_client_err!("TransactionListener is null"))?;

        msg.set_topic(self.default_producer.with_namespace(msg.topic()));
        let default_mqproducer_impl = self.default_producer.default_mqproducer_impl.as_ref().ok_or(
            rocketmq_error::RocketMQError::not_initialized("DefaultMQProducerImpl is not initialized"),
        )?;
        default_mqproducer_impl.set_transaction_listener(transaction_listener);
        default_mqproducer_impl
            .send_message_in_transaction(msg, arg.map(|x| Box::new(x) as Box<dyn Any + Sync + Send>))
            .await
    }

    async fn send_batch<M>(&mut self, msgs: Vec<M>) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.send_batch(msgs).await
    }

    async fn send_batch_with_timeout<M>(
        &mut self,
        msgs: Vec<M>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.send_batch_with_timeout(msgs, timeout).await
    }

    async fn send_batch_to_queue<M>(
        &mut self,
        msgs: Vec<M>,
        mq: MessageQueue,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.send_batch_to_queue(msgs, mq).await
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
        self.default_producer
            .send_batch_to_queue_with_timeout(msgs, mq, timeout)
            .await
    }

    async fn send_batch_with_callback<M, F>(&mut self, msgs: Vec<M>, f: F) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&RocketMQError>) + Send + Sync + 'static,
    {
        self.default_producer.send_batch_with_callback(msgs, f).await
    }

    async fn send_batch_with_callback_timeout<M, F>(
        &mut self,
        msgs: Vec<M>,
        f: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&RocketMQError>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_batch_with_callback_timeout(msgs, f, timeout)
            .await
    }

    async fn send_batch_to_queue_with_callback<M, F>(
        &mut self,
        msgs: Vec<M>,
        mq: MessageQueue,
        f: F,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&RocketMQError>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_batch_to_queue_with_callback(msgs, mq, f)
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
        F: Fn(Option<&SendResult>, Option<&RocketMQError>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_batch_to_queue_with_callback_timeout(msgs, mq, f, timeout)
            .await
    }

    async fn request<M>(&mut self, msg: M, timeout: u64) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.request(msg, timeout).await
    }

    async fn request_with_callback<F, M>(
        &mut self,
        msg: M,
        request_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&rocketmq_error::RocketMQError>) + Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        self.default_producer
            .request_with_callback(msg, request_callback, timeout)
            .await
    }

    async fn request_with_selector<M, S, T>(
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
        self.default_producer
            .request_with_selector(msg, selector, arg, timeout)
            .await
    }

    async fn request_with_selector_callback<M, S, T, F>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
        request_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        F: Fn(Option<&dyn MessageTrait>, Option<&rocketmq_error::RocketMQError>) + Send + Sync + 'static,
        T: Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        self.default_producer
            .request_with_selector_callback(msg, selector, arg, request_callback, timeout)
            .await
    }

    async fn request_to_queue<M>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer.request_to_queue(msg, mq, timeout).await
    }

    async fn request_to_queue_with_callback<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        request_callback: F,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&rocketmq_error::RocketMQError>) + Send + Sync + 'static,
        M: MessageTrait + Send + Sync,
    {
        self.default_producer
            .request_to_queue_with_callback(msg, mq, request_callback, timeout)
            .await
    }

    async fn recall_message(
        &mut self,
        topic: impl Into<CheetahString>,
        recall_handle: impl Into<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<String> {
        self.default_producer.recall_message(topic, recall_handle).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_getter_aliases_delegate_to_transaction_config() {
        let mut producer = TransactionMQProducer::default();

        assert!(producer.get_transaction_listener().is_none());
        assert!(producer.get_executor_service().is_none());

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        producer.set_executor_service(runtime.handle().clone());

        assert!(producer.get_executor_service().is_some());
    }

    #[test]
    fn use_tls_facade_and_builder_delegate_to_default_producer_like_java_client_config() {
        let mut producer = TransactionMQProducer::builder()
            .producer_group("transaction_tls_group")
            .use_tls(true)
            .build();

        assert!(producer.is_use_tls());
        assert!(producer.default_producer.is_use_tls());
        assert!(producer
            .default_producer
            .default_mqproducer_impl
            .as_ref()
            .expect("producer impl should exist")
            .is_use_tls());

        producer.set_use_tls(false);

        assert!(!producer.is_use_tls());
        assert!(!producer.default_producer.is_use_tls());
        assert!(!producer
            .default_producer
            .default_mqproducer_impl
            .as_ref()
            .expect("producer impl should exist")
            .is_use_tls());
    }
}
