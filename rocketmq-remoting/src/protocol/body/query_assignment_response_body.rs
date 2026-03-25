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
    use crate::protocol::RemotingDeserializable;
    use crate::protocol::RemotingSerializable;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_enum::MessageRequestMode;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use std::collections::HashMap;

    fn create_message_queue(topic: &str, broker: &str, queue_id: i32) -> MessageQueue {
        let mut mq = MessageQueue::new();
        mq.set_topic(CheetahString::from(topic));
        mq.set_broker_name(CheetahString::from(broker));
        mq.set_queue_id(queue_id);
        mq
    }

    fn create_assignment(
        topic: &str,
        broker: &str,
        queue_id: i32,
        mode: MessageRequestMode,
        attachments: Option<HashMap<CheetahString, CheetahString>>,
    ) -> MessageQueueAssignment {
        MessageQueueAssignment {
            message_queue: Some(create_message_queue(topic, broker, queue_id)),
            mode,
            attachments,
        }
    }

    fn create_body_with_assignments(assignments: Vec<MessageQueueAssignment>) -> QueryAssignmentResponseBody {
        QueryAssignmentResponseBody {
            message_queue_assignments: assignments.into_iter().collect(),
        }
    }

    // ==================== Basic Structure Tests ====================

    #[test]
    fn query_assignment_response_body_default_is_empty() {
        let body = QueryAssignmentResponseBody::default();
        assert!(body.message_queue_assignments.is_empty());
    }

    #[test]
    fn query_assignment_response_body_with_empty_hashset() {
        let body = QueryAssignmentResponseBody {
            message_queue_assignments: HashSet::new(),
        };
        assert!(body.message_queue_assignments.is_empty());
    }

    #[test]
    fn query_assignment_response_body_clone_preserves_data() {
        let body = create_body_with_assignments(vec![create_assignment(
            "test-topic",
            "broker-a",
            0,
            MessageRequestMode::Pull,
            None,
        )]);
        let cloned = body.clone();
        assert_eq!(
            body.message_queue_assignments.len(),
            cloned.message_queue_assignments.len()
        );
    }

    #[test]
    fn query_assignment_response_body_clone_is_independent() {
        let body = create_body_with_assignments(vec![create_assignment(
            "topic",
            "broker",
            0,
            MessageRequestMode::Pull,
            None,
        )]);
        let mut cloned = body.clone();
        cloned.message_queue_assignments.insert(create_assignment(
            "other",
            "broker",
            99,
            MessageRequestMode::Pop,
            None,
        ));
        assert_eq!(body.message_queue_assignments.len(), 1);
        assert_eq!(cloned.message_queue_assignments.len(), 2);
    }

    #[test]
    fn query_assignment_response_body_debug_format() {
        let body = create_body_with_assignments(vec![create_assignment(
            "test-topic",
            "broker-a",
            0,
            MessageRequestMode::Pull,
            None,
        )]);
        let debug_str = format!("{:?}", body);
        assert!(debug_str.contains("QueryAssignmentResponseBody"));
        assert!(debug_str.contains("message_queue_assignments"));
    }

    // ==================== HashSet Behavior Tests ====================

    #[test]
    fn query_assignment_response_body_hashset_deduplication() {
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

    #[test]
    fn query_assignment_response_body_hashset_ordering_not_guaranteed() {
        let assignments = vec![
            create_assignment("topic", "broker", 0, MessageRequestMode::Pull, None),
            create_assignment("topic", "broker", 1, MessageRequestMode::Pull, None),
            create_assignment("topic", "broker", 2, MessageRequestMode::Pull, None),
        ];
        let body = create_body_with_assignments(assignments);

        let queue_ids: Vec<i32> = body
            .message_queue_assignments
            .iter()
            .filter_map(|a| a.message_queue.as_ref().map(|mq| mq.queue_id()))
            .collect();

        assert_eq!(queue_ids.len(), 3);
        assert!(queue_ids.contains(&0));
        assert!(queue_ids.contains(&1));
        assert!(queue_ids.contains(&2));
    }

    #[test]
    fn query_assignment_response_body_hashset_equality() {
        let body1 = create_body_with_assignments(vec![create_assignment(
            "topic",
            "broker",
            0,
            MessageRequestMode::Pull,
            None,
        )]);
        let body2 = create_body_with_assignments(vec![create_assignment(
            "topic",
            "broker",
            0,
            MessageRequestMode::Pull,
            None,
        )]);
        assert_eq!(body1.message_queue_assignments, body2.message_queue_assignments);
    }

    // ==================== Serialization Tests ====================

    #[test]
    fn query_assignment_response_body_serialization_camelcase() {
        let body = create_body_with_assignments(vec![create_assignment(
            "topic",
            "broker",
            0,
            MessageRequestMode::Pull,
            None,
        )]);
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"messageQueueAssignments\""));
        assert!(!json.contains("\"message_queue_assignments\""));
    }

    #[test]
    fn query_assignment_response_body_serialization_with_modes() {
        let assignments = vec![
            create_assignment("topic", "broker", 0, MessageRequestMode::Pull, None),
            create_assignment("topic", "broker", 1, MessageRequestMode::Pop, None),
        ];
        let body = create_body_with_assignments(assignments);
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"PULL\""));
        assert!(json.contains("\"POP\""));
    }

    #[test]
    fn query_assignment_response_body_serialization_empty() {
        let body = QueryAssignmentResponseBody::default();
        let json = serde_json::to_string(&body).unwrap();
        assert_eq!(json, "{\"messageQueueAssignments\":[]}");
    }

    #[test]
    fn query_assignment_response_body_roundtrip_consistency() {
        let assignments = vec![create_assignment(
            "test-topic",
            "broker-a",
            5,
            MessageRequestMode::Pull,
            None,
        )];
        let body = create_body_with_assignments(assignments);
        let json = serde_json::to_string(&body).unwrap();
        let recovered: QueryAssignmentResponseBody = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(
            body.message_queue_assignments.len(),
            recovered.message_queue_assignments.len()
        );
    }

    #[test]
    fn query_assignment_response_body_encode_decode() {
        let body = create_body_with_assignments(vec![create_assignment(
            "test-topic",
            "broker-a",
            3,
            MessageRequestMode::Pull,
            None,
        )]);

        let encoded = body.encode().expect("encode should succeed");
        assert!(!encoded.is_empty());

        let decoded = QueryAssignmentResponseBody::decode(&encoded).expect("decode should succeed");
        assert_eq!(
            body.message_queue_assignments.len(),
            decoded.message_queue_assignments.len()
        );
    }

    #[test]
    fn query_assignment_response_body_encode_produces_valid_json() {
        let body = create_body_with_assignments(vec![create_assignment(
            "topic",
            "broker",
            0,
            MessageRequestMode::Pull,
            None,
        )]);

        let encoded = body.encode().expect("encode should succeed");
        let json_str = String::from_utf8(encoded).expect("encoded bytes should be valid UTF-8");

        assert!(json_str.contains("messageQueueAssignments"));
        let _: serde_json::Value = serde_json::from_str(&json_str).expect("encoded JSON should be valid");
    }

    // ==================== Deserialization Tests ====================

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

        let assignment = deserialized
            .message_queue_assignments
            .iter()
            .next()
            .expect("Should have at least one assignment");
        let mq = assignment
            .message_queue
            .as_ref()
            .expect("MessageQueue should be present");

        assert_eq!(mq.topic(), "test-topic");
        assert_eq!(mq.queue_id(), 2);

        let attachments = assignment.attachments.as_ref().expect("Attachments should be present");
        assert_eq!(
            attachments
                .get(&CheetahString::from("version"))
                .expect("Version key should exist"),
            "1.0"
        );
    }

    #[test]
    fn query_assignment_response_body_deserialization_missing_optional_fields() {
        let json = r#"{
            "messageQueueAssignments": [
                {
                    "messageQueue": {
                        "topic": "topic",
                        "brokerName": "broker",
                        "queueId": 1
                    },
                    "mode": "PULL"
                }
            ]
        }"#;
        let body: QueryAssignmentResponseBody = serde_json::from_str(json).expect("Failed to deserialize");
        assert_eq!(body.message_queue_assignments.len(), 1);
        let assignment = body
            .message_queue_assignments
            .iter()
            .next()
            .expect("Should have at least one assignment");
        assert!(assignment.attachments.is_none());
    }

    #[test]
    fn query_assignment_response_body_deserialization_extra_fields_ignored() {
        let json = r#"{
            "messageQueueAssignments": [
                {
                    "messageQueue": {
                        "topic": "topic",
                        "brokerName": "broker",
                        "queueId": 1
                    },
                    "mode": "PULL",
                    "attachments": null,
                    "extraField": "should be ignored",
                    "anotherField": 123
                }
            ]
        }"#;
        let body: QueryAssignmentResponseBody = serde_json::from_str(json).expect("Failed to deserialize");
        assert_eq!(body.message_queue_assignments.len(), 1);
        let assignment = body
            .message_queue_assignments
            .iter()
            .next()
            .expect("Should have at least one assignment");
        let mq = assignment
            .message_queue
            .as_ref()
            .expect("MessageQueue should be present");
        assert_eq!(mq.topic(), "topic");
        assert_eq!(mq.queue_id(), 1);
    }

    #[test]
    fn query_assignment_response_body_deserialization_malformed_json() {
        let json = r#"{ invalid json }"#;
        let result: Result<QueryAssignmentResponseBody, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn query_assignment_response_body_deserialization_wrong_data_types() {
        let json = r#"{
            "messageQueueAssignments": "not an array"
        }"#;
        let result: Result<QueryAssignmentResponseBody, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn query_assignment_response_body_nested_structure() {
        let json = r#"{
            "messageQueueAssignments": [
                {
                    "messageQueue": {
                        "topic": "my-topic",
                        "brokerName": "my-broker",
                        "queueId": 3
                    },
                    "mode": "PULL",
                    "attachments": null
                }
            ]
        }"#;
        let body: QueryAssignmentResponseBody = serde_json::from_str(json).expect("Failed to deserialize");
        assert_eq!(body.message_queue_assignments.len(), 1);
        let assignment = body
            .message_queue_assignments
            .iter()
            .next()
            .expect("Should have at least one assignment");
        let mq = assignment
            .message_queue
            .as_ref()
            .expect("MessageQueue should be present");
        assert_eq!(mq.topic(), "my-topic");
        assert_eq!(mq.broker_name(), "my-broker");
        assert_eq!(mq.queue_id(), 3);
    }

    // ==================== Edge Cases Tests ====================

    #[test]
    fn query_assignment_response_body_with_none_message_queue() {
        let assignment = MessageQueueAssignment {
            message_queue: None,
            mode: MessageRequestMode::Pull,
            attachments: None,
        };
        let body = create_body_with_assignments(vec![assignment]);
        assert_eq!(body.message_queue_assignments.len(), 1);
        let json = serde_json::to_string(&body).unwrap();
        let recovered: QueryAssignmentResponseBody = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(recovered.message_queue_assignments.len(), 1);
        let assignment = recovered
            .message_queue_assignments
            .iter()
            .next()
            .expect("Should have at least one assignment");
        assert!(assignment.message_queue.is_none());
    }

    #[test]
    fn query_assignment_response_body_empty_attachments_vs_none() {
        let empty_attachments = HashMap::new();
        let body_with_empty = create_body_with_assignments(vec![create_assignment(
            "topic",
            "broker",
            0,
            MessageRequestMode::Pull,
            Some(empty_attachments),
        )]);

        let body_with_none = create_body_with_assignments(vec![create_assignment(
            "topic",
            "broker",
            0,
            MessageRequestMode::Pull,
            None,
        )]);

        let json_empty = serde_json::to_string(&body_with_empty).unwrap();
        let json_none = serde_json::to_string(&body_with_none).unwrap();

        assert_ne!(json_empty, json_none);
    }

    #[test]
    fn query_assignment_response_body_with_attachments() {
        let mut attachments = HashMap::new();
        attachments.insert(CheetahString::from("key1"), CheetahString::from("value1"));
        attachments.insert(CheetahString::from("key2"), CheetahString::from("value2"));
        let assignments = vec![create_assignment(
            "topic",
            "broker",
            0,
            MessageRequestMode::Pull,
            Some(attachments),
        )];
        let body = create_body_with_assignments(assignments);
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"key1\""));
        assert!(json.contains("\"value1\""));
        let recovered: QueryAssignmentResponseBody = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(recovered.message_queue_assignments.len(), 1);
        let assignment = recovered
            .message_queue_assignments
            .iter()
            .next()
            .expect("Should have at least one assignment");
        let recovered_attachments = assignment.attachments.as_ref().expect("Attachments should be present");
        assert_eq!(
            recovered_attachments
                .get(&CheetahString::from("key1"))
                .expect("key1 should exist"),
            "value1"
        );
    }

    #[test]
    fn query_assignment_response_body_large_dataset() {
        let mut assignments = Vec::new();
        for i in 0..100 {
            assignments.push(create_assignment(
                &format!("topic{}", i % 5),
                &format!("broker{}", i % 3),
                i,
                if i % 2 == 0 {
                    MessageRequestMode::Pull
                } else {
                    MessageRequestMode::Pop
                },
                None,
            ));
        }
        let body = create_body_with_assignments(assignments);
        assert_eq!(body.message_queue_assignments.len(), 100);
        let json = serde_json::to_string(&body).unwrap();
        let recovered: QueryAssignmentResponseBody = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(recovered.message_queue_assignments.len(), 100);
    }

    // ==================== Real-World Scenario Tests ====================

    #[test]
    fn query_assignment_response_body_typical_real_world_response() {
        let mut assignments = Vec::new();

        for queue_id in 0..8 {
            assignments.push(create_assignment(
                "TopicOrder",
                "broker-a",
                queue_id,
                MessageRequestMode::Pull,
                None,
            ));
            assignments.push(create_assignment(
                "TopicOrder",
                "broker-b",
                queue_id,
                MessageRequestMode::Pull,
                None,
            ));
        }

        let body = create_body_with_assignments(assignments);
        assert_eq!(body.message_queue_assignments.len(), 16);

        let json = serde_json::to_string(&body).unwrap();
        let recovered: QueryAssignmentResponseBody = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(recovered.message_queue_assignments.len(), 16);

        let broker_a_count = recovered
            .message_queue_assignments
            .iter()
            .filter(|a| {
                a.message_queue
                    .as_ref()
                    .map(|mq| mq.broker_name() == "broker-a")
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(broker_a_count, 8);
    }

    #[test]
    fn query_assignment_response_body_single_queue_assignment() {
        let assignments = vec![create_assignment(
            "my-topic",
            "my-broker",
            7,
            MessageRequestMode::Pop,
            None,
        )];
        let body = create_body_with_assignments(assignments);
        assert_eq!(body.message_queue_assignments.len(), 1);
        let assignment = body
            .message_queue_assignments
            .iter()
            .next()
            .expect("Should have at least one assignment");
        assert_eq!(
            assignment
                .message_queue
                .as_ref()
                .expect("MessageQueue should be present")
                .topic(),
            "my-topic"
        );
    }

    #[test]
    fn query_assignment_response_body_empty_response() {
        let body = QueryAssignmentResponseBody::default();
        assert!(body.message_queue_assignments.is_empty());
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"messageQueueAssignments\":[]"));

        let recovered: QueryAssignmentResponseBody = serde_json::from_str(&json).expect("Failed to deserialize");
        assert!(recovered.message_queue_assignments.is_empty());
    }

    #[test]
    fn query_assignment_response_body_multiple_queues_single_broker() {
        let assignments = vec![
            create_assignment("topic", "broker-a", 0, MessageRequestMode::Pull, None),
            create_assignment("topic", "broker-a", 1, MessageRequestMode::Pull, None),
            create_assignment("topic", "broker-a", 2, MessageRequestMode::Pull, None),
        ];
        let body = create_body_with_assignments(assignments);
        assert_eq!(body.message_queue_assignments.len(), 3);
        assert!(body.message_queue_assignments.iter().all(|a| a
            .message_queue
            .as_ref()
            .expect("MessageQueue should be present")
            .broker_name()
            == "broker-a"));
    }

    #[test]
    fn query_assignment_response_body_with_multiple_assignments() {
        let assignments = vec![
            create_assignment("topic", "broker", 0, MessageRequestMode::Pull, None),
            create_assignment("topic", "broker", 1, MessageRequestMode::Pull, None),
            create_assignment("topic", "broker", 2, MessageRequestMode::Pop, None),
        ];
        let body = create_body_with_assignments(assignments);
        assert_eq!(body.message_queue_assignments.len(), 3);
    }
}
