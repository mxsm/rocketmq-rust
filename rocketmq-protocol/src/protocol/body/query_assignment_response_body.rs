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

use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentResponseBody {
    pub message_queue_assignments: HashSet<MessageQueueAssignment>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_model::common::message::message_enum::MessageRequestMode;
    use rocketmq_model::message::MessageQueue;

    use super::*;
    use crate::protocol::RemotingDeserializable;
    use crate::protocol::RemotingSerializable;

    #[test]
    fn remoting_round_trip_preserves_the_nested_assignment_contract() {
        let mut queue = MessageQueue::new();
        queue.set_topic(CheetahString::from_static_str("topic-a"));
        queue.set_broker_name(CheetahString::from_static_str("broker-a"));
        queue.set_queue_id(3);

        let mut attachments = HashMap::new();
        attachments.insert(
            CheetahString::from_static_str("key"),
            CheetahString::from_static_str("value"),
        );
        let assignment = MessageQueueAssignment {
            message_queue: Some(queue),
            mode: MessageRequestMode::Pop,
            attachments: Some(attachments),
        };
        let body = QueryAssignmentResponseBody {
            message_queue_assignments: [assignment].into_iter().collect(),
        };

        let encoded = body.encode().expect("encode query assignment response");
        let value: serde_json::Value = serde_json::from_slice(&encoded).expect("encoded response should be JSON");
        let assignments = value
            .get("messageQueueAssignments")
            .and_then(serde_json::Value::as_array)
            .expect("encoded assignments should be an array");
        assert_eq!(assignments.len(), 1);
        assert!(value.get("message_queue_assignments").is_none());

        let decoded = QueryAssignmentResponseBody::decode(&encoded).expect("decode query assignment response");
        assert_eq!(decoded.message_queue_assignments.len(), 1);
        let assignment = decoded
            .message_queue_assignments
            .iter()
            .next()
            .expect("one assignment should survive the round trip");
        let queue = assignment
            .message_queue
            .as_ref()
            .expect("the message queue should be preserved");
        assert_eq!(queue.topic(), "topic-a");
        assert_eq!(queue.broker_name(), "broker-a");
        assert_eq!(queue.queue_id(), 3);
        assert_eq!(assignment.mode, MessageRequestMode::Pop);
        assert_eq!(
            assignment
                .attachments
                .as_ref()
                .and_then(|attachments| attachments.get("key")),
            Some(&CheetahString::from_static_str("value"))
        );
    }
}
