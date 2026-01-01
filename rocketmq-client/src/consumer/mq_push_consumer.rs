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

use rocketmq_common::common::message::message_ext::MessageExt;

use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use crate::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use crate::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use crate::consumer::listener::message_listener_orderly::MessageListenerOrderly;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::mq_consumer::MQConsumer;

/// The `MQPushConsumer` trait defines the interface for a push consumer in RocketMQ.
/// A push consumer receives messages from the broker and processes them using registered listeners.
#[allow(async_fn_in_trait)]
pub trait MQPushConsumer: MQConsumer {
    /// Starts the push consumer.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or an error.
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()>;

    /// Shuts down the push consumer.
    async fn shutdown(&mut self);

    /// Registers a message listener for concurrent message consumption.
    ///
    /// # Parameters
    ///
    /// * `message_listener` - A closure that processes a batch of messages and returns a status.
    ///
    /// # Type Parameters
    ///
    /// * `MLC` - The type of the message listener closure.
    fn register_message_listener_concurrently_fn<MLCFN>(&mut self, message_listener: MLCFN)
    where
        MLCFN: Fn(Vec<MessageExt>, ConsumeConcurrentlyContext) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus>
            + Send
            + Sync;

    fn register_message_listener_concurrently<ML>(&mut self, message_listener: ML)
    where
        ML: MessageListenerConcurrently + Send + Sync + 'static;

    /// Registers a message listener for orderly message consumption.
    ///
    /// # Parameters
    ///
    /// * `message_listener` - A closure that processes a batch of messages and returns a status.
    ///
    /// # Type Parameters
    ///
    /// * `MLO` - The type of the message listener closure.
    async fn register_message_listener_orderly_fn<MLOFN>(&mut self, message_listener: MLOFN)
    where
        MLOFN: Fn(Vec<MessageExt>, ConsumeOrderlyContext) -> rocketmq_error::RocketMQResult<ConsumeOrderlyStatus>
            + Send
            + Sync;

    fn register_message_listener_orderly<ML>(&mut self, message_listener: ML)
    where
        ML: MessageListenerOrderly + Send + Sync + 'static;

    /// Subscribes to a topic with a subscription expression.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic to subscribe to.
    /// * `sub_expression` - The subscription expression.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or an error.
    fn subscribe(&mut self, topic: &str, sub_expression: &str) -> rocketmq_error::RocketMQResult<()>;

    /// Subscribes to a topic with an optional message selector.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic to subscribe to.
    /// * `selector` - An optional message selector.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or an error.
    async fn subscribe_with_selector(
        &mut self,
        topic: &str,
        selector: Option<MessageSelector>,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Unsubscribes from a topic.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic to unsubscribe from.
    async fn unsubscribe(&mut self, topic: &str);

    /// Suspends the push consumer.
    async fn suspend(&mut self);

    /// Resumes the push consumer.
    async fn resume(&mut self);
}
