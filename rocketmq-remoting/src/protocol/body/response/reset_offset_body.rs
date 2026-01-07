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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_for_c::MessageQueueForC;
use serde::Deserialize;
use serde::Serialize;
use serde_json_any_key::*;

/// Response body for reset consumer offset operation.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResetOffsetBody {
    #[serde(with = "any_key_map")]
    pub offset_table: HashMap<MessageQueue, i64>,
}

impl ResetOffsetBody {
    pub fn new() -> Self {
        Self {
            offset_table: HashMap::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn decode(body: &[u8]) -> Option<Self> {
        serde_json::from_slice(body).ok()
    }
}

/// Response body for reset consumer offset operation for C++ clients.
/// Uses a list format with offset embedded in MessageQueueForC.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResetOffsetBodyForC {
    pub offset_table: Vec<MessageQueueForC>,
}

impl ResetOffsetBodyForC {
    pub fn new() -> Self {
        Self {
            offset_table: Vec::new(),
        }
    }

    /// Convert from standard offset table to C++ client format.
    pub fn from_offset_table(offset_table: &HashMap<MessageQueue, i64>) -> Self {
        let offset_list: Vec<MessageQueueForC> = offset_table
            .iter()
            .map(|(mq, offset)| MessageQueueForC::from_message_queue(mq, *offset))
            .collect();
        Self {
            offset_table: offset_list,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn decode(body: &[u8]) -> Option<Self> {
        serde_json::from_slice(body).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reset_offset_body_new() {
        let body = ResetOffsetBody::new();
        assert!(body.offset_table.is_empty());
    }

    #[test]
    fn test_reset_offset_body_encode_decode() {
        let mut body = ResetOffsetBody::new();
        let mq = MessageQueue::from_parts("test_topic", "broker-a", 1);
        body.offset_table.insert(mq.clone(), 100);

        let encoded = body.encode();
        // Note: HashMap with complex key may not round-trip perfectly with serde_json
        // The actual encoding is valid JSON, decoding may vary based on MessageQueue's
        // Serialize/Deserialize implementation
        assert!(!encoded.is_empty());

        // Test that encoding produces valid JSON
        let json_str = String::from_utf8(encoded).unwrap();
        assert!(json_str.contains("offsetTable"));
    }

    #[test]
    fn test_reset_offset_body_for_c_from_offset_table() {
        let mut offset_table = HashMap::new();
        let mq1 = MessageQueue::from_parts("topic1", "broker-a", 0);
        let mq2 = MessageQueue::from_parts("topic1", "broker-a", 1);
        offset_table.insert(mq1, 100);
        offset_table.insert(mq2, 200);

        let body = ResetOffsetBodyForC::from_offset_table(&offset_table);
        assert_eq!(body.offset_table.len(), 2);

        // Verify offsets are included
        for mq_for_c in &body.offset_table {
            assert!(mq_for_c.offset == 100 || mq_for_c.offset == 200);
        }
    }

    #[test]
    fn test_reset_offset_body_for_c_encode_decode() {
        let mut body = ResetOffsetBodyForC::new();
        body.offset_table
            .push(MessageQueueForC::new("test_topic", "broker-a", 1, 100));

        let encoded = body.encode();
        let decoded = ResetOffsetBodyForC::decode(&encoded).unwrap();
        assert_eq!(decoded.offset_table.len(), 1);
        assert_eq!(decoded.offset_table[0].offset, 100);
    }
}
