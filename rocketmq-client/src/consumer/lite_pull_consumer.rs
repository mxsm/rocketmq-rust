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

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::topic_message_queue_change_listener::TopicMessageQueueChangeListener;

#[trait_variant::make(LitePullConsumer: Send)]
pub trait LitePullConsumerLocal: Sync {
    /// Starts the LitePullConsumer.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or failure.
    async fn start(&self) -> rocketmq_error::RocketMQResult<()>;

    /// Shuts down the LitePullConsumer.
    async fn shutdown(&self);

    /// Checks if the LitePullConsumer is running.
    ///
    /// # Returns
    ///
    /// * `bool` - `true` if the consumer is running, `false` otherwise.
    async fn is_running(&self) -> bool;

    /// Subscribes to a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or failure.
    async fn subscribe(&self, topic: &str) -> rocketmq_error::RocketMQResult<()>;

    /// Subscribes to a topic with a subscription expression.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    /// * `sub_expression` - The subscription expression.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or failure.
    async fn subscribe_with_expression(&self, topic: &str, sub_expression: &str) -> rocketmq_error::RocketMQResult<()>;

    /// Subscribes to a topic with a subscription expression and a message queue listener.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    /// * `sub_expression` - The subscription expression.
    /// * `listener` - The message queue listener.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or failure.
    async fn subscribe_with_listener<MQL>(
        &self,
        topic: &str,
        sub_expression: &str,
        listener: MQL,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        MQL: MessageQueueListener;

    /// Subscribes to a topic with a message selector.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to subscribe to.
    /// * `selector` - The message selector.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or failure.
    async fn subscribe_with_selector(
        &self,
        topic: &str,
        selector: Option<MessageSelector>,
    ) -> rocketmq_error::RocketMQResult<()>;
    /// Unsubscribes from a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic to unsubscribe from.
    async fn unsubscribe(&self, topic: &str);

    /// Retrieves the current assignment of message queues.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<HashSet<MessageQueue>>` - A set of assigned message queues
    ///   or an error.
    async fn assignment(&self) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>>;

    /// Assigns a list of message queues to the consumer.
    ///
    /// # Arguments
    ///
    /// * `message_queues` - A vector of `MessageQueue` instances to assign.
    async fn assign(&self, message_queues: Vec<MessageQueue>);

    /// Sets the subscription expression for an assigned topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic.
    /// * `sub_expression` - The subscription expression.
    async fn set_sub_expression_for_assign(&self, topic: &str, sub_expression: &str);

    /// Polls for messages.
    ///
    /// # Returns
    ///
    /// * `Vec<MessageExt>` - A vector of polled messages.
    async fn poll(&self) -> Vec<MessageExt>;

    /// Polls for messages with a timeout.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The timeout duration in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Vec<MessageExt>` - A vector of polled messages.
    async fn poll_with_timeout(&self, timeout: u64) -> Vec<MessageExt>;

    /// Seeks to a specific offset in a message queue.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The message queue to seek.
    /// * `offset` - The offset to seek to.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or failure.
    async fn seek(&self, message_queue: &MessageQueue, offset: i64) -> rocketmq_error::RocketMQResult<()>;

    /// Pauses message consumption for the specified message queues.
    ///
    /// # Arguments
    ///
    /// * `message_queues` - A vector of `MessageQueue` instances to pause.
    async fn pause(&self, message_queues: Vec<MessageQueue>);

    /// Resumes message consumption for the specified message queues.
    ///
    /// # Arguments
    ///
    /// * `message_queues` - A vector of `MessageQueue` instances to resume.
    async fn resume(&self, message_queues: Vec<MessageQueue>);

    /// Checks if auto-commit is enabled.
    ///
    /// # Returns
    ///
    /// * `bool` - `true` if auto-commit is enabled, `false` otherwise.
    async fn is_auto_commit(&self) -> bool;

    /// Sets the auto-commit mode.
    ///
    /// # Arguments
    ///
    /// * `auto_commit` - `true` to enable auto-commit, `false` to disable it.
    async fn set_auto_commit(&self, auto_commit: bool);

    /// Fetches the message queues for a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<Vec<MessageQueue>>` - A vector of message queues or an
    ///   error.
    async fn fetch_message_queues(&self, topic: &str) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>>;
    /// Retrieves the offset for a given timestamp in a message queue.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The message queue to query.
    /// * `timestamp` - The timestamp to query the offset for.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<i64>` - The offset corresponding to the given timestamp or
    ///   an error.
    async fn offset_for_timestamp(
        &self,
        message_queue: &MessageQueue,
        timestamp: u64,
    ) -> rocketmq_error::RocketMQResult<i64>;

    /// Commits the current offsets synchronously.
    async fn commit_sync(&self);

    /// Commits the provided offsets synchronously.
    ///
    /// # Arguments
    ///
    /// * `offset_map` - A map of message queues to offsets.
    /// * `persist` - Whether to persist the offsets.
    async fn commit_sync_with_map(&self, offset_map: HashMap<MessageQueue, i64>, persist: bool);

    /// Commits the current offsets.
    async fn commit(&self);

    /// Commits the provided offsets.
    ///
    /// # Arguments
    ///
    /// * `offset_map` - A map of message queues to offsets.
    /// * `persist` - Whether to persist the offsets.
    async fn commit_with_map(&self, offset_map: HashMap<MessageQueue, i64>, persist: bool);

    /// Commits the offsets for the provided message queues.
    ///
    /// # Arguments
    ///
    /// * `message_queues` - A set of message queues to commit offsets for.
    /// * `persist` - Whether to persist the offsets.
    async fn commit_with_set(&self, message_queues: HashSet<MessageQueue>, persist: bool);

    /// Retrieves the committed offset for a message queue.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The message queue to query.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<i64>` - The committed offset or an error.
    async fn committed(&self, message_queue: &MessageQueue) -> rocketmq_error::RocketMQResult<i64>;

    /// Registers a listener for changes to the message queues of a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic.
    /// * `listener` - The listener to register.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or failure.
    async fn register_topic_message_queue_change_listener<TL>(
        &self,
        topic: &str,
        listener: TL,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        TL: TopicMessageQueueChangeListener;

    /// Updates the name server address.
    ///
    /// # Arguments
    ///
    /// * `name_server_address` - The new name server address.
    async fn update_name_server_address(&self, name_server_address: &str);

    /// Seeks to the beginning of a message queue.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The message queue to seek.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or failure.
    async fn seek_to_begin(&self, message_queue: &MessageQueue) -> rocketmq_error::RocketMQResult<()>;

    /// Seeks to the end of a message queue.
    ///
    /// # Arguments
    ///
    /// * `message_queue` - The message queue to seek.
    ///
    /// # Returns
    ///
    /// * `rocketmq_error::RocketMQResult<()>` - An empty result indicating success or failure.
    async fn seek_to_end(&self, message_queue: &MessageQueue) -> rocketmq_error::RocketMQResult<()>;
}
