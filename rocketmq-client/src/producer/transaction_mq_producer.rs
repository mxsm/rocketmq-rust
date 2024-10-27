/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::any::Any;
use std::sync::Arc;

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_runtime::RocketMQRuntime;

use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::mq_producer::MQProducer;
use crate::producer::send_callback::SendMessageCallback;
use crate::producer::send_result::SendResult;
use crate::producer::transaction_listener::TransactionListener;
use crate::producer::transaction_mq_produce_builder::TransactionMQProducerBuilder;
use crate::producer::transaction_send_result::TransactionSendResult;
use crate::Result;

#[derive(Clone, Default)]
pub struct TransactionProducerConfig {
    pub transaction_listener: Option<Arc<Box<dyn TransactionListener>>>,
    pub check_thread_pool_min_size: u32,
    pub check_thread_pool_max_size: u32,
    pub check_request_hold_max: u32,
    pub check_runtime: Option<Arc<RocketMQRuntime>>,
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

    pub fn set_transaction_listener(&mut self, transaction_listener: impl TransactionListener) {
        self.default_producer
            .default_mqproducer_impl
            .as_mut()
            .unwrap()
            .set_transaction_listener(Arc::new(Box::new(transaction_listener)));
    }

    pub fn set_check_runtime(&mut self, check_runtime: RocketMQRuntime) {
        self.default_producer
            .default_mqproducer_impl
            .as_mut()
            .unwrap()
            .set_check_runtime(Arc::new(check_runtime));
    }
}

impl MQProducer for TransactionMQProducer {
    async fn start(&mut self) -> crate::Result<()> {
        let transaction_listener = self
            .transaction_producer_config
            .transaction_listener
            .clone();
        let default_mqproducer_impl = self
            .default_producer
            .default_mqproducer_impl
            .as_mut()
            .unwrap();
        default_mqproducer_impl
            .init_transaction_env(self.transaction_producer_config.check_runtime.take());
        if let Some(transaction_listener) = transaction_listener {
            default_mqproducer_impl.set_transaction_listener(transaction_listener);
        }
        self.default_producer.start().await
    }

    async fn shutdown(&mut self) {
        self.default_producer.shutdown().await
    }

    async fn fetch_publish_message_queues(
        &mut self,
        topic: &str,
    ) -> crate::Result<Vec<MessageQueue>> {
        self.default_producer
            .fetch_publish_message_queues(topic)
            .await
    }

    async fn send<M>(&mut self, msg: M) -> crate::Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync,
    {
        self.default_producer.send(msg).await
    }

    async fn send_with_timeout(&mut self, msg: Message, timeout: u64) -> crate::Result<SendResult> {
        self.default_producer.send_with_timeout(msg, timeout).await
    }

    async fn send_with_callback<M, F>(&mut self, msg: M, send_callback: F) -> crate::Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_with_callback(msg, send_callback)
            .await
    }

    async fn send_with_callback_timeout<F>(
        &mut self,
        msg: Message,
        send_callback: F,
        timeout: u64,
    ) -> crate::Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_with_callback_timeout(msg, send_callback, timeout)
            .await
    }

    async fn send_oneway<M>(&mut self, msg: M) -> crate::Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
    {
        self.default_producer.send_oneway(msg).await
    }

    async fn send_to_queue<M>(&mut self, msg: M, mq: MessageQueue) -> crate::Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync,
    {
        self.default_producer.send_to_queue(msg, mq).await
    }

    async fn send_to_queue_with_timeout<M>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> crate::Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync,
    {
        self.default_producer
            .send_to_queue_with_timeout(msg, mq, timeout)
            .await
    }

    async fn send_to_queue_with_callback<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        send_callback: F,
    ) -> crate::Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
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
    ) -> crate::Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_to_queue_with_callback_timeout(msg, mq, send_callback, timeout)
            .await
    }

    async fn send_oneway_to_queue<M>(&mut self, msg: M, mq: MessageQueue) -> crate::Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
    {
        self.default_producer.send_oneway_to_queue(msg, mq).await
    }

    async fn send_with_selector<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
    ) -> crate::Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue>
            + Send
            + Sync
            + 'static,
        T: std::any::Any + Send + Sync,
    {
        self.default_producer
            .send_with_selector(msg, selector, arg)
            .await
    }

    async fn send_with_selector_timeout<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
        timeout: u64,
    ) -> Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue>
            + Send
            + Sync
            + 'static,
        T: std::any::Any + Sync + Send,
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
        send_callback: Option<SendMessageCallback>,
    ) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue>
            + Send
            + Sync
            + 'static,
        T: std::any::Any + Sync + Send,
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
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue>
            + Send
            + Sync
            + 'static,
        T: std::any::Any + Sync + Send,
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
    ) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue>
            + Send
            + Sync
            + 'static,
        T: std::any::Any + Sync + Send,
    {
        self.default_producer
            .send_oneway_with_selector(msg, selector, arg)
            .await
    }

    async fn send_message_in_transaction<T>(
        &mut self,
        mut msg: Message,
        arg: Option<T>,
    ) -> Result<TransactionSendResult>
    where
        T: std::any::Any + Sync + Send,
    {
        msg.set_topic(
            self.default_producer
                .with_namespace(msg.get_topic())
                .as_str(),
        );
        self.default_producer
            .default_mqproducer_impl
            .as_mut()
            .unwrap()
            .send_message_in_transaction(
                msg,
                arg.map(|x| Box::new(x) as Box<dyn Any + Sync + Send>),
            )
            .await
    }

    async fn send_batch(&mut self, msgs: Vec<Message>) -> Result<SendResult> {
        self.default_producer.send_batch(msgs).await
    }

    async fn send_batch_with_timeout(
        &mut self,
        msgs: Vec<Message>,
        timeout: u64,
    ) -> Result<SendResult> {
        self.default_producer
            .send_batch_with_timeout(msgs, timeout)
            .await
    }

    async fn send_batch_to_queue(
        &mut self,
        msgs: Vec<Message>,
        mq: MessageQueue,
    ) -> Result<SendResult> {
        self.default_producer.send_batch_to_queue(msgs, mq).await
    }

    async fn send_batch_to_queue_with_timeout(
        &mut self,
        msgs: Vec<Message>,
        mq: MessageQueue,
        timeout: u64,
    ) -> Result<SendResult> {
        self.default_producer
            .send_batch_to_queue_with_timeout(msgs, mq, timeout)
            .await
    }

    async fn send_batch_with_callback<F>(&mut self, msgs: Vec<Message>, f: F) -> Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_batch_with_callback(msgs, f)
            .await
    }

    async fn send_batch_with_callback_timeout<F>(
        &mut self,
        msgs: Vec<Message>,
        f: F,
        timeout: u64,
    ) -> Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_batch_with_callback_timeout(msgs, f, timeout)
            .await
    }

    async fn send_batch_to_queue_with_callback<F>(
        &mut self,
        msgs: Vec<Message>,
        mq: MessageQueue,
        f: F,
    ) -> Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_batch_to_queue_with_callback(msgs, mq, f)
            .await
    }

    async fn send_batch_to_queue_with_callback_timeout<F>(
        &mut self,
        msgs: Vec<Message>,
        mq: MessageQueue,
        f: F,
        timeout: u64,
    ) -> Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
    {
        self.default_producer
            .send_batch_to_queue_with_callback_timeout(msgs, mq, f, timeout)
            .await
    }

    async fn request<M>(&mut self, msg: M, timeout: u64) -> Result<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Clone + Send + Sync,
    {
        self.default_producer.request(msg, timeout).await
    }

    async fn request_with_callback<F, M>(
        &mut self,
        msg: M,
        request_callback: F,
        timeout: u64,
    ) -> Result<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Clone + Send + Sync,
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
    ) -> Result<Box<dyn MessageTrait + Send>>
    where
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue>
            + Send
            + Sync
            + 'static,
        T: std::any::Any + Sync + Send,
        M: MessageTrait + Clone + Send + Sync,
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
    ) -> Result<()>
    where
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue>
            + Send
            + Sync
            + 'static,
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        T: std::any::Any + Sync + Send,
        M: MessageTrait + Clone + Send + Sync,
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
    ) -> Result<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Clone + Send + Sync,
    {
        self.default_producer
            .request_to_queue(msg, mq, timeout)
            .await
    }

    async fn request_to_queue_with_callback<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        request_callback: F,
        timeout: u64,
    ) -> Result<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Clone + Send + Sync,
    {
        self.default_producer
            .request_to_queue_with_callback(msg, mq, request_callback, timeout)
            .await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
