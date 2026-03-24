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

use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentResponseBody {
    pub message_queue_assignments: HashSet<MessageQueueAssignment>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_enum::MessageRequestMode;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use std::collections::HashMap;

    #[test]
    fn query_assignment_response_body_default_is_empty() {
        let body = QueryAssignmentResponseBody::default();
        assert!(body.message_queue_assignments.is_empty());
    }

    #[test]
    fn query_assignment_response_body_serialization() {
        let mut assignments = HashSet::new();

        let mq = MessageQueue::new();

        let mut attachments = HashMap::new();
        attachments.insert(CheetahString::from("key"), CheetahString::from("value"));

        let assignment = MessageQueueAssignment {
            message_queue: Some(mq),
            mode: MessageRequestMode::Pull,
            attachments: Some(attachments),
        };
        assignments.insert(assignment);

        let body = QueryAssignmentResponseBody {
            message_queue_assignments: assignments,
        };

        let json = serde_json::to_string(&body).unwrap();

        assert!(json.contains("\"messageQueueAssignments\""));
        assert!(json.contains("\"messageQueue\""));
        assert!(json.contains("\"topic\""));
    }

    #[test]
    fn query_assignment_response_body_deserialization() {
        let json = r#"{
            "messageQueueAssignments": [
                {
                    "messageQueue": {
                        "topic": "test-topic",
                        "brokerName": "broker-b",
                        "queueId": 2
                    },
                    "mode": "PULL",
                    "attachments": {"version": "1.0"}
                }
            ]
        }"#;

        let deserialized: QueryAssignmentResponseBody =
            serde_json::from_str(json).expect("Failed to deserialize QueryAssignmentResponseBody");

        assert_eq!(deserialized.message_queue_assignments.len(), 1);

        let assignment = deserialized.message_queue_assignments.iter().next().unwrap();
        let mq = assignment
            .message_queue
            .as_ref()
            .expect("MessageQueue should be present");

        assert_eq!(mq.topic(), "test-topic");
        assert_eq!(mq.queue_id(), 2);

        let attachments = assignment.attachments.as_ref().unwrap();
        assert_eq!(attachments.get(&CheetahString::from("version")).unwrap(), "1.0");
    }

    #[test]
    fn test_hashset_equality_and_uniqueness() {
        let mut assignments = HashSet::new();

        let mq_gen = || MessageQueue::new();

        let a1 = MessageQueueAssignment {
            message_queue: Some(mq_gen()),
            mode: MessageRequestMode::Pop,
            attachments: None,
        };

        let a2 = MessageQueueAssignment {
            message_queue: Some(mq_gen()),
            mode: MessageRequestMode::Pop,
            attachments: None,
        };

        assignments.insert(a1);
        assignments.insert(a2);

        assert_eq!(assignments.len(), 1);
    }
}
