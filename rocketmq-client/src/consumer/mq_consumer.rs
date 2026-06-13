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

use crate::base::query_result::QueryResult;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;

fn unsupported_mq_admin_operation(operation: &'static str) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::illegal_argument(format!(
        "{operation} is not supported by this MQConsumer implementation"
    ))
}

#[allow(async_fn_in_trait)]
pub trait MQConsumer {
    /// Creates a topic through the consumer's admin facade.
    ///
    /// Java's `MQConsumer` extends `MQAdmin`; Rust keeps these admin operations async because all
    /// broker I/O in this client is async.
    async fn create_topic(
        &mut self,
        _key: &str,
        _new_topic: &str,
        _queue_num: i32,
        _attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        Err(unsupported_mq_admin_operation("createTopic"))
    }

    /// Creates a topic with an explicit topic system flag.
    async fn create_topic_with_flag(
        &mut self,
        _key: &str,
        _new_topic: &str,
        _queue_num: i32,
        _topic_sys_flag: i32,
        _attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        Err(unsupported_mq_admin_operation("createTopicWithFlag"))
    }

    /// Searches the offset in a queue by store timestamp.
    async fn search_offset(&mut self, _mq: &MessageQueue, _timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        Err(unsupported_mq_admin_operation("searchOffset"))
    }

    /// Returns the broker max offset for a queue.
    async fn max_offset(&mut self, _mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        Err(unsupported_mq_admin_operation("maxOffset"))
    }

    /// Returns the broker min offset for a queue.
    async fn min_offset(&mut self, _mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        Err(unsupported_mq_admin_operation("minOffset"))
    }

    /// Returns the earliest store time for messages in a queue.
    async fn earliest_msg_store_time(&mut self, _mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        Err(unsupported_mq_admin_operation("earliestMsgStoreTime"))
    }

    /// Queries messages by key in a time range.
    async fn query_message(
        &mut self,
        _topic: &str,
        _key: &str,
        _max_num: i32,
        _begin: u64,
        _end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        Err(unsupported_mq_admin_operation("queryMessage"))
    }

    /// Views a message by message id.
    async fn view_message(&mut self, _topic: &str, _msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        Err(unsupported_mq_admin_operation("viewMessage"))
    }

    async fn send_message_back(
        &mut self,
        msg: MessageExt,
        delay_level: i32,
        broker_name: &str,
    ) -> rocketmq_error::RocketMQResult<()>;

    /// Sends a message back using the broker name carried by the message.
    ///
    /// This is the Rust equivalent of Java's two-argument
    /// `sendMessageBack(MessageExt, int)` overload. Implementations may treat an
    /// empty broker name as Java `null` and fall back to the message store host.
    async fn send_message_back_to_origin_broker(
        &mut self,
        msg: MessageExt,
        delay_level: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let broker_name = msg.broker_name().to_string();
        self.send_message_back(msg, delay_level, &broker_name).await
    }

    async fn fetch_subscribe_message_queues(
        &mut self,
        topic: &str,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CustomConsumer;

    impl MQConsumer for CustomConsumer {
        async fn send_message_back(
            &mut self,
            _msg: MessageExt,
            _delay_level: i32,
            _broker_name: &str,
        ) -> rocketmq_error::RocketMQResult<()> {
            Ok(())
        }

        async fn fetch_subscribe_message_queues(
            &mut self,
            _topic: &str,
        ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn admin_defaults_return_typed_unsupported_error_without_breaking_custom_consumers() {
        let mut consumer = CustomConsumer;
        let mq = MessageQueue::from_parts("topic", "broker-a", 0);

        let error = consumer
            .max_offset(&mq)
            .await
            .expect_err("default MQAdmin facade should be unsupported");

        assert!(error.to_string().contains("maxOffset"));
        assert!(error.to_string().contains("MQConsumer implementation"));
    }
}
