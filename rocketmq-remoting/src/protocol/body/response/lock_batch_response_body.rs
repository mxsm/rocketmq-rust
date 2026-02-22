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

use std::collections::HashSet;

use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct LockBatchResponseBody {
    #[serde(rename = "lockOKMQSet")]
    pub lock_ok_mq_set: HashSet<MessageQueue>,
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json;
    use std::collections::HashSet;

    #[test]
    fn test_lock_batch_response_body_default() {
        let body = LockBatchResponseBody::default();
        assert!(body.lock_ok_mq_set.is_empty());
    }

    #[test]
    fn test_serialization_json_field_name() {
        let mut set = HashSet::new();
        let mq = MessageQueue::from_parts("topic_a", "broker_a", 1);
        set.insert(mq);

        let body = LockBatchResponseBody { lock_ok_mq_set: set };

        let json = serde_json::to_string(&body).unwrap();

        assert!(json.contains("\"lockOKMQSet\""));
    }

    #[test]
    fn test_deserialization_success() {
        let json = r#"{
            "lockOKMQSet": [
                {
                    "topic": "some_test_topic",
                    "brokerName": "TEST_BROKER",
                    "queueId": 1
                }
            ]
        }"#;

        let decoded: LockBatchResponseBody = serde_json::from_str(json).expect("Failed to deserialize");

        assert_eq!(decoded.lock_ok_mq_set.len(), 1);
        let mq = decoded.lock_ok_mq_set.iter().next().unwrap();
        assert_eq!(mq.broker_name(), "TEST_BROKER");
        assert_eq!(mq.queue_id(), 1);
    }

    #[test]
    fn test_hash_set_behavior_with_message_queue() {
        let mut body = LockBatchResponseBody::default();

        let mq1 = MessageQueue::from_parts("topic", "broker", 1);
        let mq2 = MessageQueue::from_parts("topic", "broker", 1);
        let mq3 = MessageQueue::from_parts("topic", "broker", 2);

        body.lock_ok_mq_set.insert(mq1);
        body.lock_ok_mq_set.insert(mq2);
        body.lock_ok_mq_set.insert(mq3);

        assert_eq!(body.lock_ok_mq_set.len(), 2);
    }
}
