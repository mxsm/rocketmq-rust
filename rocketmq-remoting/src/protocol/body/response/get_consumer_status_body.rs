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

use crate::protocol::admin::consume_stats::append_message_queue_object_key;
use crate::protocol::admin::consume_stats::normalize_nonstandard_offset_table_keys;
use crate::protocol::RemotingDeserializable;

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
        self.encode_java_compatible().unwrap_or_default()
    }

    pub fn encode_java_compatible(&self) -> rocketmq_error::RocketMQResult<Vec<u8>> {
        Ok(self.to_java_compatible_json()?.into_bytes())
    }

    pub fn to_java_compatible_json(&self) -> rocketmq_error::RocketMQResult<String> {
        let mut body = String::new();
        body.push_str("{\"messageQueueTable\":{");
        append_offset_map(&mut body, &self.message_queue_table)?;
        body.push_str("},\"consumerTable\":{");

        for (index, (client_id, offsets)) in self.consumer_table.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(&serde_json::to_string(client_id.as_str())?);
            body.push_str(":{");
            append_offset_map(&mut body, offsets)?;
            body.push('}');
        }

        body.push_str("}}");
        Ok(body)
    }

    pub fn decode(body: &[u8]) -> Option<Self> {
        match <Self as RemotingDeserializable>::decode(body) {
            Ok(decoded) => Some(decoded),
            Err(_) => {
                let raw = std::str::from_utf8(body).ok()?;
                let normalized = normalize_nonstandard_message_queue_table_keys(raw);
                if normalized == raw {
                    return None;
                }
                <Self as RemotingDeserializable>::decode_str(&normalized).ok()
            }
        }
    }
}

fn append_offset_map(output: &mut String, offsets: &HashMap<MessageQueue, i64>) -> rocketmq_error::RocketMQResult<()> {
    for (index, (queue, offset)) in offsets.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        append_message_queue_object_key(output, queue)?;
        output.push(':');
        output.push_str(&offset.to_string());
    }
    Ok(())
}

fn normalize_nonstandard_message_queue_table_keys(input: &str) -> String {
    let marker = "\"messageQueueTable\"";
    if !input.contains(marker) {
        return input.to_string();
    }

    let offset_marker = "\"offsetTable\"";
    let rewritten = input.replacen(marker, offset_marker, 1);
    let normalized = normalize_nonstandard_offset_table_keys(&rewritten);
    if normalized == rewritten {
        return input.to_string();
    }
    normalized.replacen(offset_marker, marker, 1)
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
    fn get_consumer_status_body_encode_uses_java_object_keys() {
        let mut body = GetConsumerStatusBody::new();
        let mq = MessageQueue::from_parts(CheetahString::from("topic"), CheetahString::from("broker"), 1);
        body.message_queue_table.insert(mq.clone(), 123);
        let mut consumer_offsets = HashMap::new();
        consumer_offsets.insert(mq, 456);
        body.consumer_table
            .insert(CheetahString::from("client-a"), consumer_offsets);

        let encoded = String::from_utf8(body.encode()).expect("get consumer status body should be utf8");

        assert!(encoded.contains(r#""messageQueueTable":{{"topic":"topic","brokerName":"broker","queueId":1}:123}"#));
        assert!(encoded.contains(r#""client-a":{{"topic":"topic","brokerName":"broker","queueId":1}:456}"#));
        assert!(!encoded.contains(r#""{\"topic\""#));
    }

    #[test]
    fn get_consumer_status_body_decode_accepts_java_message_queue_table_keys() {
        let body =
            r#"{"messageQueueTable":{{"topic":"topic","brokerName":"broker","queueId":1}:123},"consumerTable":{}}"#;

        let decoded = GetConsumerStatusBody::decode(body.as_bytes()).expect("decode get consumer status body");

        let mq = MessageQueue::from_parts(CheetahString::from("topic"), CheetahString::from("broker"), 1);
        assert_eq!(decoded.message_queue_table.get(&mq), Some(&123));
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
