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

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::base::query_result::QueryResult;

/// Trait defining administrative operations for a Message Queue (MQ).
#[allow(dead_code)]
pub trait MQAdmin {
    /// Creates a new topic.
    ///
    /// # Arguments
    /// * `key` - A key used for topic creation.
    /// * `new_topic` - The name of the new topic.
    /// * `queue_num` - The number of queues for the new topic.
    /// * `attributes` - A map of attributes for the new topic.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    fn create_topic(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Creates a new topic with a system flag.
    ///
    /// # Arguments
    /// * `key` - A key used for topic creation.
    /// * `new_topic` - The name of the new topic.
    /// * `queue_num` - The number of queues for the new topic.
    /// * `topic_sys_flag` - The system flag for the new topic.
    /// * `attributes` - A map of attributes for the new topic.
    ///
    /// # Returns
    /// A `Result` indicating success or failure.
    fn create_topic_with_flag(
        &self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Searches for the offset of a message in a queue at a given timestamp.
    ///
    /// # Arguments
    /// * `mq` - The message queue to search.
    /// * `timestamp` - The timestamp to search for.
    ///
    /// # Returns
    /// A `Result` containing the offset if found, or an error.
    fn search_offset(&self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64>;

    /// Retrieves the maximum offset of a message in a queue.
    ///
    /// # Arguments
    /// * `mq` - The message queue to query.
    ///
    /// # Returns
    /// A `Result` containing the maximum offset, or an error.
    fn max_offset(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64>;

    /// Retrieves the minimum offset of a message in a queue.
    ///
    /// # Arguments
    /// * `mq` - The message queue to query.
    ///
    /// # Returns
    /// A `Result` containing the minimum offset, or an error.
    fn min_offset(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64>;

    /// Retrieves the earliest message store time in a queue.
    ///
    /// # Arguments
    /// * `mq` - The message queue to query.
    ///
    /// # Returns
    /// A `Result` containing the earliest message store time, or an error.
    fn earliest_msg_store_time(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<u64>;

    /// Queries messages in a topic by key within a time range.
    ///
    /// # Arguments
    /// * `topic` - The topic to query.
    /// * `key` - The key to search for.
    /// * `max_num` - The maximum number of messages to return.
    /// * `begin` - The start of the time range.
    /// * `end` - The end of the time range.
    ///
    /// # Returns
    /// A `Result` containing a `QueryResult` with the messages, or an error.
    fn query_message(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult>;

    /// Views a message by its ID in a topic.
    ///
    /// # Arguments
    /// * `topic` - The topic containing the message.
    /// * `msg_id` - The ID of the message to view.
    ///
    /// # Returns
    /// A `Result` containing the `MessageExt` if found, or an error.
    fn view_message(&self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt>;
}
