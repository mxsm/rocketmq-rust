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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Deserialize;
use serde::Serialize;
use serde_json_any_key::*;

/// Response body for get consumer status operation.
/// Maps client ID to their message queue offset table.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerStatusBody {
    /// Maps MessageQueue to offset (used for single client response)
    #[serde(default, with = "any_key_map")]
    pub message_queue_table: HashMap<MessageQueue, i64>,

    /// Maps client ID to their MessageQueue offset table (used for aggregated response)
    #[serde(default)]
    pub consumer_table: HashMap<CheetahString, HashMap<MessageQueue, i64>>,
}

impl GetConsumerStatusBody {
    pub fn new() -> Self {
        Self {
            message_queue_table: HashMap::new(),
            consumer_table: HashMap::new(),
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
    fn get_consumer_status_body_new_and_default() {
        let body = GetConsumerStatusBody::new();
        assert!(body.message_queue_table.is_empty());
        assert!(body.consumer_table.is_empty());

        let body = GetConsumerStatusBody::default();
        assert!(body.message_queue_table.is_empty());
        assert!(body.consumer_table.is_empty());
    }

    #[test]
    fn get_consumer_status_body_encode_and_decode() {
        let mut body = GetConsumerStatusBody::new();
        let mq = MessageQueue::from_parts(CheetahString::from("topic"), CheetahString::from("broker"), 1);
        body.message_queue_table.insert(mq.clone(), 123);

        let encoded = body.encode();
        assert!(!encoded.is_empty());
        let decoded: GetConsumerStatusBody =
            GetConsumerStatusBody::decode(&encoded).expect("Failed to deserialize GetConsumerStatusBody");
        assert_eq!(decoded.message_queue_table.get(&mq), Some(&123));
    }

    #[test]
    fn get_consumer_status_body_serialization_and_deserialization() {
        let mut body = GetConsumerStatusBody::new();
        let mq = MessageQueue::from_parts(CheetahString::from("topic"), CheetahString::from("broker"), 1);
        body.message_queue_table.insert(mq.clone(), 123);

        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("messageQueueTable"));
        assert!(json.contains("consumerTable"));
        let deserialized: GetConsumerStatusBody = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.message_queue_table.get(&mq), Some(&123));
    }

    #[test]
    fn get_consumer_status_body_debug() {
        let body = GetConsumerStatusBody::new();
        let debug = format!("{:?}", body);
        assert!(debug.contains("GetConsumerStatusBody"));
        assert!(debug.contains("message_queue_table"));
        assert!(debug.contains("consumer_table"));
    }
}
