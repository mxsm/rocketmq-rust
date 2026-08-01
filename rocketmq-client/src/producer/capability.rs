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

//! Focused Producer capabilities.
//!
//! Public consumers depend on lifecycle, send, transaction, request/reply,
//! recall, query, or topic-administration behavior without observing the
//! internal producer backend.

use std::any::Any;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageTrait;

use super::producer_backend::ProducerBackend;
use super::request_callback::RequestCallbackFn;
use super::send_callback::ArcSendCallback;
use super::DefaultMQProducer;
use super::SendResult;
use super::TransactionMQProducer;
use super::TransactionSendResult;
use crate::base::query_result::QueryResult;

/// Destination selected before entering the send capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SendDestination {
    /// Use the producer's normal routing strategy.
    Automatic,
    /// Send to one explicit queue.
    Queue(MessageQueue),
}

/// Completion contract for one send request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendMode {
    /// Wait for and return the broker send result.
    AwaitResult,
    /// Submit a one-way send without a broker result.
    Oneway,
}

/// One message send with destination, completion mode, and deadline policy.
pub struct SendRequest<M> {
    pub message: M,
    pub destination: SendDestination,
    pub mode: SendMode,
    /// Timeout in milliseconds, or the configured producer default.
    pub timeout_millis: Option<u64>,
}

/// Batch send with destination and deadline policy.
pub struct BatchSendRequest<M> {
    pub messages: Vec<M>,
    pub destination: SendDestination,
    /// Timeout in milliseconds, or the configured producer default.
    pub timeout_millis: Option<u64>,
}

/// Callback-based send with an optional explicit queue.
pub struct SendCallbackRequest<M> {
    pub message: M,
    pub destination: SendDestination,
    pub timeout_millis: Option<u64>,
    pub callback: ArcSendCallback,
}

/// Selector-based send without duplicating destination overloads.
pub struct SelectedSendRequest<M, S, T> {
    pub message: M,
    pub selector: S,
    pub argument: T,
    pub mode: SendMode,
    pub timeout_millis: Option<u64>,
}

/// Callback-based selector send.
pub struct SelectedSendCallbackRequest<M, S, T> {
    pub message: M,
    pub selector: S,
    pub argument: T,
    pub timeout_millis: Option<u64>,
    pub callback: ArcSendCallback,
}

/// Callback-based batch send with an optional explicit queue.
pub struct BatchSendCallbackRequest<M> {
    pub messages: Vec<M>,
    pub destination: SendDestination,
    pub timeout_millis: Option<u64>,
    pub callback: ArcSendCallback,
}

/// Recall a previously sent message.
pub struct RecallRequest {
    pub topic: CheetahString,
    pub recall_handle: CheetahString,
}

/// Request/reply invocation with an optional explicit queue.
pub struct RequestReplyRequest<M> {
    pub message: M,
    pub destination: SendDestination,
    pub timeout_millis: u64,
}

/// Callback-based request/reply invocation.
pub struct RequestReplyCallbackRequest<M> {
    pub message: M,
    pub destination: SendDestination,
    pub timeout_millis: u64,
    pub callback: RequestCallbackFn,
}

/// Selector-based request/reply invocation.
pub struct SelectedRequestReplyRequest<M, S, T> {
    pub message: M,
    pub selector: S,
    pub argument: T,
    pub timeout_millis: u64,
}

/// Callback-based selector request/reply invocation.
pub struct SelectedRequestReplyCallbackRequest<M, S, T> {
    pub message: M,
    pub selector: S,
    pub argument: T,
    pub timeout_millis: u64,
    pub callback: RequestCallbackFn,
}

/// Transactional send input.
pub struct TransactionSendRequest<M, T> {
    pub message: M,
    pub argument: Option<T>,
}

/// Producer-side metadata query.
pub enum ProducerQueryRequest {
    FetchPublishQueues {
        topic: String,
    },
    SearchOffset {
        queue: MessageQueue,
        timestamp: u64,
    },
    MaxOffset {
        queue: MessageQueue,
    },
    MinOffset {
        queue: MessageQueue,
    },
    EarliestStoreTime {
        queue: MessageQueue,
    },
    QueryMessages {
        topic: String,
        key: String,
        max_count: i32,
        begin_timestamp: u64,
        end_timestamp: u64,
    },
    ViewMessage {
        topic: String,
        message_id: String,
    },
}

/// Closed result set corresponding to [`ProducerQueryRequest`].
pub enum ProducerQueryResponse {
    Queues(Vec<MessageQueue>),
    Offset(i64),
    Timestamp(i64),
    Query(QueryResult),
    Message(Box<MessageExt>),
}

/// Topic creation input shared by both legacy overloads.
pub struct TopicCreateRequest {
    pub key: String,
    pub topic: String,
    pub queue_count: i32,
    pub system_flag: Option<i32>,
    pub attributes: HashMap<String, String>,
}

/// Producer startup and shutdown capability.
#[allow(async_fn_in_trait)]
pub trait ProducerLifecycle {
    /// Starts the producer.
    ///
    /// # Errors
    ///
    /// Returns the existing typed client error when startup fails.
    async fn start_producer(&mut self) -> rocketmq_error::RocketMQResult<()>;

    /// Shuts down producer-owned work.
    async fn shutdown_producer(&mut self);
}

/// Normal send, batch-send, and one-way capability.
#[allow(async_fn_in_trait)]
pub trait MessageSend {
    /// Sends one message using a closed request contract.
    ///
    /// # Errors
    ///
    /// Preserves the error returned by the corresponding legacy operation.
    async fn send_message<M>(&mut self, request: SendRequest<M>) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync;

    /// Sends a batch using a closed request contract.
    ///
    /// # Errors
    ///
    /// Preserves the error returned by the corresponding legacy operation.
    async fn send_message_batch<M>(
        &mut self,
        request: BatchSendRequest<M>,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync;

    /// Sends one message and reports completion through the supplied callback.
    ///
    /// # Errors
    ///
    /// Preserves the corresponding legacy validation and submission errors.
    async fn send_message_with_callback<M>(
        &mut self,
        request: SendCallbackRequest<M>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync;

    /// Sends one message using a queue selector.
    ///
    /// # Errors
    ///
    /// Preserves selector, routing, and send errors from the legacy operation.
    async fn send_selected<M, S, T>(
        &mut self,
        request: SelectedSendRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static;

    /// Sends one selected message and reports completion through a callback.
    ///
    /// # Errors
    ///
    /// Preserves selector, routing, and submission errors.
    async fn send_selected_with_callback<M, S, T>(
        &mut self,
        request: SelectedSendCallbackRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static;

    /// Sends a batch and reports completion through a callback.
    ///
    /// # Errors
    ///
    /// Preserves the corresponding legacy validation and submission errors.
    async fn send_message_batch_with_callback<M>(
        &mut self,
        request: BatchSendCallbackRequest<M>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync;
}

/// Message recall capability.
#[allow(async_fn_in_trait)]
pub trait MessageRecall {
    /// Recalls one message.
    ///
    /// # Errors
    ///
    /// Preserves the existing recall validation or remoting error.
    async fn recall(&mut self, request: RecallRequest) -> rocketmq_error::RocketMQResult<String>;
}

/// Transactional send capability implemented only by transaction producers.
#[allow(async_fn_in_trait)]
pub trait TransactionSend {
    /// Sends a prepared message and executes the configured transaction listener.
    ///
    /// # Errors
    ///
    /// Returns the existing transaction listener, send, or end-transaction error.
    async fn send_transaction<M, T>(
        &mut self,
        request: TransactionSendRequest<M, T>,
    ) -> rocketmq_error::RocketMQResult<TransactionSendResult>
    where
        M: MessageTrait + Send + Sync,
        T: Any + Send + Sync;
}

/// Request/reply capability.
#[allow(async_fn_in_trait)]
pub trait RequestReply {
    /// Sends a request and waits for its correlated reply.
    ///
    /// # Errors
    ///
    /// Preserves the existing timeout, routing, and remoting errors.
    async fn request_reply<M>(
        &mut self,
        request: RequestReplyRequest<M>,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync;

    /// Sends a request and reports its correlated reply through a callback.
    ///
    /// # Errors
    ///
    /// Preserves timeout, routing, and submission errors.
    async fn request_reply_with_callback<M>(
        &mut self,
        request: RequestReplyCallbackRequest<M>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync;

    /// Sends a selector-routed request and waits for its reply.
    ///
    /// # Errors
    ///
    /// Preserves selector, timeout, routing, and remoting errors.
    async fn request_reply_selected<M, S, T>(
        &mut self,
        request: SelectedRequestReplyRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static;

    /// Sends a selector-routed request and reports its reply through a callback.
    ///
    /// # Errors
    ///
    /// Preserves selector, timeout, routing, and submission errors.
    async fn request_reply_selected_with_callback<M, S, T>(
        &mut self,
        request: SelectedRequestReplyCallbackRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static;
}

/// Producer metadata and message-query capability.
#[allow(async_fn_in_trait)]
pub trait MessageQuery {
    /// Executes one producer query.
    ///
    /// # Errors
    ///
    /// Preserves the error behavior of the mapped legacy query.
    async fn query(&mut self, request: ProducerQueryRequest) -> rocketmq_error::RocketMQResult<ProducerQueryResponse>;
}

/// Producer topic-administration capability.
#[allow(async_fn_in_trait)]
pub trait ProducerTopicAdmin {
    /// Creates a topic through the producer's existing admin facade.
    ///
    /// # Errors
    ///
    /// Preserves validation and broker errors from the legacy operation.
    async fn create_topic_request(&mut self, request: TopicCreateRequest) -> rocketmq_error::RocketMQResult<()>;
}

impl ProducerLifecycle for DefaultMQProducer {
    async fn start_producer(&mut self) -> rocketmq_error::RocketMQResult<()> {
        <Self as ProducerBackend>::start(self).await
    }

    async fn shutdown_producer(&mut self) {
        <Self as ProducerBackend>::shutdown(self).await;
    }
}

impl MessageSend for DefaultMQProducer {
    async fn send_message<M>(&mut self, request: SendRequest<M>) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        match (request.destination, request.mode, request.timeout_millis) {
            (SendDestination::Automatic, SendMode::AwaitResult, None) => {
                <Self as ProducerBackend>::send(self, request.message).await
            }
            (SendDestination::Automatic, SendMode::AwaitResult, Some(timeout)) => {
                <Self as ProducerBackend>::send_with_timeout(self, request.message, timeout).await
            }
            (SendDestination::Queue(queue), SendMode::AwaitResult, None) => {
                <Self as ProducerBackend>::send_to_queue(self, request.message, queue).await
            }
            (SendDestination::Queue(queue), SendMode::AwaitResult, Some(timeout)) => {
                <Self as ProducerBackend>::send_to_queue_with_timeout(self, request.message, queue, timeout).await
            }
            (SendDestination::Automatic, SendMode::Oneway, _) => {
                <Self as ProducerBackend>::send_oneway(self, request.message).await?;
                Ok(None)
            }
            (SendDestination::Queue(queue), SendMode::Oneway, _) => {
                <Self as ProducerBackend>::send_oneway_to_queue(self, request.message, queue).await?;
                Ok(None)
            }
        }
    }

    async fn send_message_batch<M>(
        &mut self,
        request: BatchSendRequest<M>,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        match (request.destination, request.timeout_millis) {
            (SendDestination::Automatic, None) => <Self as ProducerBackend>::send_batch(self, request.messages).await,
            (SendDestination::Automatic, Some(timeout)) => {
                <Self as ProducerBackend>::send_batch_with_timeout(self, request.messages, timeout).await
            }
            (SendDestination::Queue(queue), None) => {
                <Self as ProducerBackend>::send_batch_to_queue(self, request.messages, queue).await
            }
            (SendDestination::Queue(queue), Some(timeout)) => {
                <Self as ProducerBackend>::send_batch_to_queue_with_timeout(self, request.messages, queue, timeout)
                    .await
            }
        }
    }

    async fn send_message_with_callback<M>(
        &mut self,
        request: SendCallbackRequest<M>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        let callback = request.callback;
        let callback_fn = move |result: Option<&SendResult>, error: Option<&rocketmq_error::RocketMQError>| {
            if let Some(result) = result {
                callback.on_success(result);
            } else if let Some(error) = error {
                callback.on_exception(error);
            }
        };
        match (request.destination, request.timeout_millis) {
            (SendDestination::Automatic, None) => {
                <Self as ProducerBackend>::send_with_callback(self, request.message, callback_fn).await
            }
            (SendDestination::Automatic, Some(timeout)) => {
                <Self as ProducerBackend>::send_with_callback_timeout(self, request.message, callback_fn, timeout).await
            }
            (SendDestination::Queue(queue), None) => {
                <Self as ProducerBackend>::send_to_queue_with_callback(self, request.message, queue, callback_fn).await
            }
            (SendDestination::Queue(queue), Some(timeout)) => {
                <Self as ProducerBackend>::send_to_queue_with_callback_timeout(
                    self,
                    request.message,
                    queue,
                    callback_fn,
                    timeout,
                )
                .await
            }
        }
    }

    async fn send_selected<M, S, T>(
        &mut self,
        request: SelectedSendRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        match (request.mode, request.timeout_millis) {
            (SendMode::AwaitResult, None) => {
                <Self as ProducerBackend>::send_with_selector(self, request.message, request.selector, request.argument)
                    .await
            }
            (SendMode::AwaitResult, Some(timeout)) => {
                <Self as ProducerBackend>::send_with_selector_timeout(
                    self,
                    request.message,
                    request.selector,
                    request.argument,
                    timeout,
                )
                .await
            }
            (SendMode::Oneway, _) => {
                <Self as ProducerBackend>::send_oneway_with_selector(
                    self,
                    request.message,
                    request.selector,
                    request.argument,
                )
                .await?;
                Ok(None)
            }
        }
    }

    async fn send_selected_with_callback<M, S, T>(
        &mut self,
        request: SelectedSendCallbackRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        match request.timeout_millis {
            Some(timeout) => {
                <Self as ProducerBackend>::send_with_selector_callback_timeout(
                    self,
                    request.message,
                    request.selector,
                    request.argument,
                    Some(request.callback),
                    timeout,
                )
                .await
            }
            None => {
                <Self as ProducerBackend>::send_with_selector_callback(
                    self,
                    request.message,
                    request.selector,
                    request.argument,
                    Some(request.callback),
                )
                .await
            }
        }
    }

    async fn send_message_batch_with_callback<M>(
        &mut self,
        request: BatchSendCallbackRequest<M>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        let callback = request.callback;
        let callback_fn = move |result: Option<&SendResult>, error: Option<&rocketmq_error::RocketMQError>| {
            if let Some(result) = result {
                callback.on_success(result);
            } else if let Some(error) = error {
                callback.on_exception(error);
            }
        };
        match (request.destination, request.timeout_millis) {
            (SendDestination::Automatic, None) => {
                <Self as ProducerBackend>::send_batch_with_callback(self, request.messages, callback_fn).await
            }
            (SendDestination::Automatic, Some(timeout)) => {
                <Self as ProducerBackend>::send_batch_with_callback_timeout(
                    self,
                    request.messages,
                    callback_fn,
                    timeout,
                )
                .await
            }
            (SendDestination::Queue(queue), None) => {
                <Self as ProducerBackend>::send_batch_to_queue_with_callback(self, request.messages, queue, callback_fn)
                    .await
            }
            (SendDestination::Queue(queue), Some(timeout)) => {
                <Self as ProducerBackend>::send_batch_to_queue_with_callback_timeout(
                    self,
                    request.messages,
                    queue,
                    callback_fn,
                    timeout,
                )
                .await
            }
        }
    }
}

impl MessageRecall for DefaultMQProducer {
    async fn recall(&mut self, request: RecallRequest) -> rocketmq_error::RocketMQResult<String> {
        <Self as ProducerBackend>::recall_message(self, request.topic, request.recall_handle).await
    }
}

impl RequestReply for DefaultMQProducer {
    async fn request_reply<M>(
        &mut self,
        request: RequestReplyRequest<M>,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        match request.destination {
            SendDestination::Automatic => {
                <Self as ProducerBackend>::request(self, request.message, request.timeout_millis).await
            }
            SendDestination::Queue(queue) => {
                <Self as ProducerBackend>::request_to_queue(self, request.message, queue, request.timeout_millis).await
            }
        }
    }

    async fn request_reply_with_callback<M>(
        &mut self,
        request: RequestReplyCallbackRequest<M>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        let callback = request.callback;
        let callback_fn = move |response: Option<&dyn MessageTrait>, error: Option<&rocketmq_error::RocketMQError>| {
            callback(response, error);
        };
        match request.destination {
            SendDestination::Automatic => {
                <Self as ProducerBackend>::request_with_callback(
                    self,
                    request.message,
                    callback_fn,
                    request.timeout_millis,
                )
                .await
            }
            SendDestination::Queue(queue) => {
                <Self as ProducerBackend>::request_to_queue_with_callback(
                    self,
                    request.message,
                    queue,
                    callback_fn,
                    request.timeout_millis,
                )
                .await
            }
        }
    }

    async fn request_reply_selected<M, S, T>(
        &mut self,
        request: SelectedRequestReplyRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        <Self as ProducerBackend>::request_with_selector(
            self,
            request.message,
            request.selector,
            request.argument,
            request.timeout_millis,
        )
        .await
    }

    async fn request_reply_selected_with_callback<M, S, T>(
        &mut self,
        request: SelectedRequestReplyCallbackRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        let callback = request.callback;
        <Self as ProducerBackend>::request_with_selector_callback(
            self,
            request.message,
            request.selector,
            request.argument,
            move |response, error| callback(response, error),
            request.timeout_millis,
        )
        .await
    }
}

impl MessageQuery for DefaultMQProducer {
    async fn query(&mut self, request: ProducerQueryRequest) -> rocketmq_error::RocketMQResult<ProducerQueryResponse> {
        let response = match request {
            ProducerQueryRequest::FetchPublishQueues { topic } => ProducerQueryResponse::Queues(
                <Self as ProducerBackend>::fetch_publish_message_queues(self, &topic).await?,
            ),
            ProducerQueryRequest::SearchOffset { queue, timestamp } => {
                ProducerQueryResponse::Offset(<Self as ProducerBackend>::search_offset(self, &queue, timestamp).await?)
            }
            ProducerQueryRequest::MaxOffset { queue } => {
                ProducerQueryResponse::Offset(<Self as ProducerBackend>::max_offset(self, &queue).await?)
            }
            ProducerQueryRequest::MinOffset { queue } => {
                ProducerQueryResponse::Offset(<Self as ProducerBackend>::min_offset(self, &queue).await?)
            }
            ProducerQueryRequest::EarliestStoreTime { queue } => ProducerQueryResponse::Timestamp(
                <Self as ProducerBackend>::earliest_msg_store_time(self, &queue).await?,
            ),
            ProducerQueryRequest::QueryMessages {
                topic,
                key,
                max_count,
                begin_timestamp,
                end_timestamp,
            } => ProducerQueryResponse::Query(
                <Self as ProducerBackend>::query_message(self, &topic, &key, max_count, begin_timestamp, end_timestamp)
                    .await?,
            ),
            ProducerQueryRequest::ViewMessage { topic, message_id } => ProducerQueryResponse::Message(Box::new(
                <Self as ProducerBackend>::view_message(self, &topic, &message_id).await?,
            )),
        };
        Ok(response)
    }
}

impl ProducerTopicAdmin for DefaultMQProducer {
    async fn create_topic_request(&mut self, request: TopicCreateRequest) -> rocketmq_error::RocketMQResult<()> {
        match request.system_flag {
            Some(system_flag) => {
                <Self as ProducerBackend>::create_topic_with_flag(
                    self,
                    &request.key,
                    &request.topic,
                    request.queue_count,
                    system_flag,
                    request.attributes,
                )
                .await
            }
            None => {
                <Self as ProducerBackend>::create_topic(
                    self,
                    &request.key,
                    &request.topic,
                    request.queue_count,
                    request.attributes,
                )
                .await
            }
        }
    }
}

impl ProducerLifecycle for TransactionMQProducer {
    async fn start_producer(&mut self) -> rocketmq_error::RocketMQResult<()> {
        <Self as ProducerBackend>::start(self).await
    }

    async fn shutdown_producer(&mut self) {
        <Self as ProducerBackend>::shutdown(self).await;
    }
}

impl MessageSend for TransactionMQProducer {
    async fn send_message<M>(&mut self, request: SendRequest<M>) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer_mut().send_message(request).await
    }

    async fn send_message_batch<M>(
        &mut self,
        request: BatchSendRequest<M>,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer_mut().send_message_batch(request).await
    }

    async fn send_message_with_callback<M>(
        &mut self,
        request: SendCallbackRequest<M>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer_mut().send_message_with_callback(request).await
    }

    async fn send_selected<M, S, T>(
        &mut self,
        request: SelectedSendRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        self.default_producer_mut().send_selected(request).await
    }

    async fn send_selected_with_callback<M, S, T>(
        &mut self,
        request: SelectedSendCallbackRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        self.default_producer_mut().send_selected_with_callback(request).await
    }

    async fn send_message_batch_with_callback<M>(
        &mut self,
        request: BatchSendCallbackRequest<M>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer_mut()
            .send_message_batch_with_callback(request)
            .await
    }
}

impl MessageRecall for TransactionMQProducer {
    async fn recall(&mut self, request: RecallRequest) -> rocketmq_error::RocketMQResult<String> {
        MessageRecall::recall(self.default_producer_mut(), request).await
    }
}

impl RequestReply for TransactionMQProducer {
    async fn request_reply<M>(
        &mut self,
        request: RequestReplyRequest<M>,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer_mut().request_reply(request).await
    }

    async fn request_reply_with_callback<M>(
        &mut self,
        request: RequestReplyCallbackRequest<M>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        self.default_producer_mut().request_reply_with_callback(request).await
    }

    async fn request_reply_selected<M, S, T>(
        &mut self,
        request: SelectedRequestReplyRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        self.default_producer_mut().request_reply_selected(request).await
    }

    async fn request_reply_selected_with_callback<M, S, T>(
        &mut self,
        request: SelectedRequestReplyCallbackRequest<M, S, T>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        self.default_producer_mut()
            .request_reply_selected_with_callback(request)
            .await
    }
}

impl MessageQuery for TransactionMQProducer {
    async fn query(&mut self, request: ProducerQueryRequest) -> rocketmq_error::RocketMQResult<ProducerQueryResponse> {
        MessageQuery::query(self.default_producer_mut(), request).await
    }
}

impl ProducerTopicAdmin for TransactionMQProducer {
    async fn create_topic_request(&mut self, request: TopicCreateRequest) -> rocketmq_error::RocketMQResult<()> {
        ProducerTopicAdmin::create_topic_request(self.default_producer_mut(), request).await
    }
}

impl TransactionSend for TransactionMQProducer {
    async fn send_transaction<M, T>(
        &mut self,
        request: TransactionSendRequest<M, T>,
    ) -> rocketmq_error::RocketMQResult<TransactionSendResult>
    where
        M: MessageTrait + Send + Sync,
        T: Any + Send + Sync,
    {
        <Self as ProducerBackend>::send_message_in_transaction(self, request.message, request.argument).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_default_capabilities<T>()
    where
        T: ProducerLifecycle + MessageSend + MessageRecall + RequestReply + MessageQuery + ProducerTopicAdmin,
    {
    }

    fn assert_transaction_capabilities<T>()
    where
        T: ProducerLifecycle + MessageSend + TransactionSend + RequestReply + MessageQuery + ProducerTopicAdmin,
    {
    }

    #[test]
    fn concrete_producers_expose_only_their_actual_capabilities() {
        assert_default_capabilities::<DefaultMQProducer>();
        assert_transaction_capabilities::<TransactionMQProducer>();
    }
}
