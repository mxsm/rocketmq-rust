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

use cheetah_string::CheetahString;

use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use crate::consumer::listener::message_listener_orderly::MessageListenerOrderly;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::mq_consumer::MQConsumer;

/// Defines the push-consumer interface for RocketMQ.
///
/// Implementations receive messages from brokers and dispatch them to the
/// registered concurrent or orderly listener.
#[allow(async_fn_in_trait)]
pub trait MQPushConsumer: MQConsumer {
    /// Asynchronously starts the consumer instance.
    ///
    /// This method does not block the calling thread.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when startup succeeds, or an error when initialization
    /// fails.
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()>;

    /// Asynchronously shuts down the consumer instance.
    ///
    /// This method does not block the calling thread.
    async fn shutdown(&mut self);

    /// Registers a message listener for concurrent message consumption.
    ///
    /// # Parameters
    ///
    /// * `message_listener` - A closure that processes a batch of messages and returns a status.
    ///
    /// # Type Parameters
    ///
    /// * `ML` - The listener type.
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
    /// * `ML` - The listener type.
    fn register_message_listener_orderly<ML>(&mut self, message_listener: ML)
    where
        ML: MessageListenerOrderly + Send + Sync + 'static;

    /// Asynchronously subscribes to a topic with a subscription expression.
    ///
    /// This method does not block the calling thread.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic to subscribe to. Can be `&str`, `String`, or `CheetahString`.
    /// * `sub_expression` - The subscription expression. Can be `&str`, `String`, or
    ///   `CheetahString`.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when the subscription is accepted, or an error when the
    /// subscription request is invalid or cannot be applied.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Using &str
    /// consumer.subscribe("TopicTest", "*").await?;
    ///
    /// // Using String
    /// let topic = String::from("TopicTest");
    /// consumer.subscribe(topic, "*").await?;
    ///
    /// // Using CheetahString
    /// let topic = CheetahString::from_slice("TopicTest");
    /// consumer.subscribe(topic, "*").await?;
    /// ```
    async fn subscribe(
        &mut self,
        topic: impl Into<CheetahString>,
        sub_expression: impl Into<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Asynchronously subscribes to a topic with an optional selector.
    ///
    /// This method does not block the calling thread.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic to subscribe to.
    /// * `selector` - An optional message selector.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when the subscription is accepted, or an error when the
    /// selector is invalid or cannot be applied.
    async fn subscribe_with_selector(
        &mut self,
        topic: &str,
        selector: Option<MessageSelector>,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Asynchronously unsubscribes from a topic.
    ///
    /// This method does not block the calling thread.
    ///
    /// # Parameters
    ///
    /// * `topic` - The topic to unsubscribe from.
    async fn unsubscribe(&mut self, topic: &str);

    /// Asynchronously suspends message consumption.
    ///
    /// This method does not block the calling thread.
    async fn suspend(&self);

    /// Asynchronously resumes message consumption.
    ///
    /// This method does not block the calling thread.
    async fn resume(&self);
}
