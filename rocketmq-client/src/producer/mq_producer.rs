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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;

use crate::producer::send_callback::SendMessageCallback;
use crate::producer::send_result::SendResult;
use crate::producer::transaction_send_result::TransactionSendResult;
use crate::Result;

#[trait_variant::make(MQProducer: Send)]
pub trait MQProducerLocal {
    /// Starts the producer.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn start(&mut self) -> Result<()>;

    /// Shuts down the producer.
    async fn shutdown(&mut self);

    /// Fetches the list of message queues for a given topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - A string slice that holds the name of the topic.
    ///
    /// # Returns
    ///
    /// * `Result<Vec<MessageQueue>>` - A result containing a vector of message queues or an error.
    async fn fetch_publish_message_queues(&mut self, topic: &str) -> Result<Vec<MessageQueue>>;

    /// Sends a message.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
    async fn send<M>(&mut self, msg: M) -> Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a message with a timeout.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
    async fn send_with_timeout(&mut self, msg: Message, timeout: u64) -> Result<SendResult>;

    /// Sends a message with a callback.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `F` - A function type for the callback.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `send_callback` - The callback function to be executed after sending the message.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_with_callback<M, F>(&mut self, msg: M, send_callback: F) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static;

    /// Sends a message with a callback and a timeout.
    ///
    /// # Type Parameters
    ///
    /// * `F` - A function type for the callback.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `send_callback` - The callback function to be executed after sending the message.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_with_callback_timeout<F>(
        &mut self,
        msg: Message,
        send_callback: F,
        timeout: u64,
    ) -> Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static;

    /// Sends a message without waiting for a response.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_oneway<M>(&mut self, msg: M) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a message to a specific message queue.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `mq` - The message queue to which the message will be sent.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
    async fn send_to_queue<M>(&mut self, msg: M, mq: MessageQueue) -> Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a message to a specific message queue with a timeout.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `mq` - The message queue to which the message will be sent.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
    async fn send_to_queue_with_timeout<M>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a message to a specific message queue with a callback.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `F` - A function type for the callback.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `mq` - The message queue to which the message will be sent.
    /// * `send_callback` - The callback function to be executed after sending the message.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_to_queue_with_callback<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        send_callback: F,
    ) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static;

    /// Sends a message to a specific message queue with a callback and a timeout.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `F` - A function type for the callback.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `mq` - The message queue to which the message will be sent.
    /// * `send_callback` - The callback function to be executed after sending the message.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_to_queue_with_callback_timeout<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        send_callback: F,
        timeout: u64,
    ) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static;

    /// Sends a message to a specific message queue without waiting for a response.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `mq` - The message queue to which the message will be sent.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_oneway_to_queue<M>(&mut self, msg: M, mq: MessageQueue) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a message with a selector function to choose the message queue.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `S` - A function type for the selector.
    /// * `T` - A type for the argument passed to the selector.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `selector` - The selector function to choose the message queue.
    /// * `arg` - The argument passed to the selector function.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
    async fn send_with_selector<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
    ) -> Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync,
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue>
            + Send
            + Sync
            + 'static,
        T: std::any::Any + Sync + Send;

    /// Sends a message with a selector function to choose the message queue and a timeout.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `S` - A function type for the selector.
    /// * `T` - A type for the argument passed to the selector.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `selector` - The selector function to choose the message queue.
    /// * `arg` - The argument passed to the selector function.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
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
        T: std::any::Any + Sync + Send;

    /// Sends a message with a selector function to choose the message queue and a callback.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `S` - A function type for the selector.
    /// * `T` - A type for the argument passed to the selector.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `selector` - The selector function to choose the message queue.
    /// * `arg` - The argument passed to the selector function.
    /// * `send_callback` - The callback function to be executed after sending the message.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
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
        T: std::any::Any + Sync + Send;

    /// Sends a message with a selector function to choose the message queue, a callback, and a
    /// timeout.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `S` - A function type for the selector.
    /// * `T` - A type for the argument passed to the selector.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `selector` - The selector function to choose the message queue.
    /// * `arg` - The argument passed to the selector function.
    /// * `send_callback` - The callback function to be executed after sending the message.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
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
        T: std::any::Any + Sync + Send;

    /// Sends a message with a selector function to choose the message queue without waiting for a
    /// response.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `S` - A function type for the selector.
    /// * `T` - A type for the argument passed to the selector.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `selector` - The selector function to choose the message queue.
    /// * `arg` - The argument passed to the selector function.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
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
        T: std::any::Any + Sync + Send;

    /// Sends a message in a transaction.
    ///
    /// # Arguments
    ///
    /// * `msg` - A reference to the message to be sent.
    /// * `arg` - A string slice that holds the argument for the transaction.
    ///
    /// # Returns
    ///
    /// * `Result<TransactionSendResult>` - A result containing the transaction send result or an
    ///   error.
    async fn send_message_in_transaction(
        &self,
        msg: &Message,
        arg: &str,
    ) -> Result<TransactionSendResult>;

    /// Sends a batch of messages.
    ///
    /// # Arguments
    ///
    /// * `msgs` - A vector of messages to be sent.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
    async fn send_batch(&mut self, msgs: Vec<Message>) -> Result<SendResult>;

    /// Sends a batch of messages with a timeout.
    ///
    /// # Arguments
    ///
    /// * `msgs` - A vector of messages to be sent.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
    async fn send_batch_with_timeout(
        &mut self,
        msgs: Vec<Message>,
        timeout: u64,
    ) -> Result<SendResult>;

    /// Sends a batch of messages to a specific message queue.
    ///
    /// # Arguments
    ///
    /// * `msgs` - A vector of messages to be sent.
    /// * `mq` - The message queue to which the messages will be sent.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
    async fn send_batch_to_queue(
        &mut self,
        msgs: Vec<Message>,
        mq: MessageQueue,
    ) -> Result<SendResult>;

    /// Sends a batch of messages to a specific message queue with a timeout.
    ///
    /// # Arguments
    ///
    /// * `msgs` - A vector of messages to be sent.
    /// * `mq` - The message queue to which the messages will be sent.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<SendResult>` - A result containing the send result or an error.
    async fn send_batch_to_queue_with_timeout(
        &mut self,
        msgs: Vec<Message>,
        mq: MessageQueue,
        timeout: u64,
    ) -> Result<SendResult>;

    /// Sends a batch of messages with a callback.
    ///
    /// # Arguments
    ///
    /// * `msgs` - A vector of messages to be sent.
    /// * `f` - The callback function to be executed after sending the messages.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_batch_with_callback<F>(&mut self, msgs: Vec<Message>, f: F) -> Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static;

    /// Sends a batch of messages with a callback and a timeout.
    ///
    /// # Arguments
    ///
    /// * `msgs` - A vector of messages to be sent.
    /// * `f` - The callback function to be executed after sending the messages.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_batch_with_callback_timeout<F>(
        &mut self,
        msgs: Vec<Message>,
        f: F,
        timeout: u64,
    ) -> Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static;

    /// Sends a batch of messages to a specific message queue with a callback.
    ///
    /// # Arguments
    ///
    /// * `msgs` - A vector of messages to be sent.
    /// * `mq` - The message queue to which the messages will be sent.
    /// * `f` - The callback function to be executed after sending the messages.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_batch_to_queue_with_callback<F>(
        &mut self,
        msgs: Vec<Message>,
        mq: MessageQueue,
        f: F,
    ) -> Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static;

    /// Sends a batch of messages to a specific message queue with a callback and a timeout.
    ///
    /// # Arguments
    ///
    /// * `msgs` - A vector of messages to be sent.
    /// * `mq` - The message queue to which the messages will be sent.
    /// * `f` - The callback function to be executed after sending the messages.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn send_batch_to_queue_with_callback_timeout<F>(
        &mut self,
        msgs: Vec<Message>,
        mq: MessageQueue,
        f: F,
        timeout: u64,
    ) -> Result<()>
    where
        F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync + 'static;

    /// Sends a request message.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<Box<dyn MessageTrait + Send>>` - A result containing the response message or an
    ///   error.
    async fn request<M>(&mut self, msg: M, timeout: u64) -> Result<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a request message with a callback.
    ///
    /// # Type Parameters
    ///
    /// * `F` - A function type for the callback.
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `request_callback` - The callback function to be executed after receiving the response.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn request_with_callback<F, M>(
        &mut self,
        msg: M,
        request_callback: F,
        timeout: u64,
    ) -> Result<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a request message with a selector function to choose the message queue.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `S` - A function type for the selector.
    /// * `T` - A type for the argument passed to the selector.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `selector` - The selector function to choose the message queue.
    /// * `arg` - The argument passed to the selector function.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<Box<dyn MessageTrait + Send>>` - A result containing the response message or an
    ///   error.
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
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a request message with a selector function to choose the message queue and a callback.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `S` - A function type for the selector.
    /// * `T` - A type for the argument passed to the selector.
    /// * `F` - A function type for the callback.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `selector` - The selector function to choose the message queue.
    /// * `arg` - The argument passed to the selector function.
    /// * `request_callback` - The callback function to be executed after receiving the response.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
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
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a request message to a specific message queue.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `mq` - The message queue to which the message will be sent.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<Box<dyn MessageTrait + Send>>` - A result containing the response message or an
    ///   error.
    async fn request_to_queue<M>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> Result<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Clone + Send + Sync;

    /// Sends a request message to a specific message queue with a callback.
    ///
    /// # Type Parameters
    ///
    /// * `M` - A type that implements `MessageTrait`, `Clone`, `Send`, and `Sync`.
    /// * `F` - A function type for the callback.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be sent.
    /// * `mq` - The message queue to which the message will be sent.
    /// * `request_callback` - The callback function to be executed after receiving the response.
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - An empty result indicating success or failure.
    async fn request_to_queue_with_callback<M, F>(
        &mut self,
        msg: M,
        mq: MessageQueue,
        request_callback: F,
        timeout: u64,
    ) -> Result<()>
    where
        F: Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync + 'static,
        M: MessageTrait + Clone + Send + Sync;

    /// Returns a reference to the object as a trait object of type `Any`.
    ///
    /// # Returns
    ///
    /// * `&dyn Any` - A reference to the object as a trait object of type `Any`.
    fn as_any(&self) -> &dyn Any;

    /// Returns a mutable reference to the object as a trait object of type `Any`.
    ///
    /// # Returns
    ///
    /// * `&mut dyn Any` - A mutable reference to the object as a trait object of type `Any`.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
