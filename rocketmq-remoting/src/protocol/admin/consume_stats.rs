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
use serde::Deserialize;
use serde::Serialize;
use serde_json_any_key::*;

use crate::protocol::admin::offset_wrapper::OffsetWrapper;
use crate::protocol::RemotingDeserializable;

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeStats {
    #[serde(with = "any_key_map")]
    pub offset_table: HashMap<MessageQueue, OffsetWrapper>,
    pub consume_tps: f64,
}

impl ConsumeStats {
    pub fn new() -> Self {
        Self {
            offset_table: HashMap::new(),
            consume_tps: 0.0,
        }
    }

    pub fn compute_total_diff(&self) -> i64 {
        self.offset_table
            .values()
            .map(|value| value.get_broker_offset() - value.get_consumer_offset())
            .sum()
    }

    pub fn compute_inflight_total_diff(&self) -> i64 {
        self.offset_table
            .values()
            .map(|value| value.get_pull_offset() - value.get_consumer_offset())
            .sum()
    }

    pub fn get_offset_table(&self) -> &HashMap<MessageQueue, OffsetWrapper> {
        &self.offset_table
    }

    pub fn get_offset_table_mut(&mut self) -> &mut HashMap<MessageQueue, OffsetWrapper> {
        &mut self.offset_table
    }

    pub fn set_offset_table(&mut self, offset_table: HashMap<MessageQueue, OffsetWrapper>) {
        self.offset_table = offset_table;
    }

    pub fn get_consume_tps(&self) -> f64 {
        self.consume_tps
    }

    pub fn set_consume_tps(&mut self, consume_tps: f64) {
        self.consume_tps = consume_tps;
    }

    pub fn decode(body: &[u8]) -> rocketmq_error::RocketMQResult<Self> {
        match <Self as RemotingDeserializable>::decode(body) {
            Ok(stats) => Ok(stats),
            Err(error) => {
                let Ok(raw_body) = std::str::from_utf8(body) else {
                    return Err(error);
                };
                let normalized_body = normalize_nonstandard_offset_table_keys(raw_body);
                if normalized_body == raw_body {
                    return Err(error);
                }
                <Self as RemotingDeserializable>::decode_str(&normalized_body)
            }
        }
    }

    pub fn encode_java_compatible(&self) -> rocketmq_error::RocketMQResult<Vec<u8>> {
        Ok(self.to_java_compatible_json()?.into_bytes())
    }

    pub fn to_java_compatible_json(&self) -> rocketmq_error::RocketMQResult<String> {
        let mut body = String::new();
        body.push_str("{\"offsetTable\":{");

        for (index, (queue, offset)) in self.offset_table.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            append_message_queue_object_key(&mut body, queue)?;
            body.push(':');
            body.push_str(&serde_json::to_string(offset)?);
        }

        body.push_str("},\"consumeTps\":");
        body.push_str(&serde_json::to_string(&self.consume_tps)?);
        body.push('}');
        Ok(body)
    }
}

pub(crate) fn normalize_nonstandard_offset_table_keys(input: &str) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;

    while index < chars.len() {
        if chars[index] != '"' {
            output.push(chars[index]);
            index += 1;
            continue;
        }

        let string_end = consume_string_end(&chars, index);
        output.extend(chars[index..=string_end].iter());
        let key = chars[index + 1..string_end].iter().collect::<String>();
        index = string_end + 1;

        if key != "offsetTable" {
            continue;
        }

        while index < chars.len() && chars[index].is_whitespace() {
            output.push(chars[index]);
            index += 1;
        }
        if index >= chars.len() || chars[index] != ':' {
            continue;
        }
        output.push(':');
        index += 1;

        while index < chars.len() && chars[index].is_whitespace() {
            output.push(chars[index]);
            index += 1;
        }
        if index >= chars.len() || chars[index] != '{' {
            continue;
        }

        output.push('{');
        index += 1;
        let (normalized_map, next_index) = normalize_object_key_map_body(&chars, index);
        output.push_str(&normalized_map);
        index = next_index;
    }
    output
}

pub(crate) fn append_message_queue_object_key(
    output: &mut String,
    queue: &MessageQueue,
) -> rocketmq_error::RocketMQResult<()> {
    output.push('{');
    output.push_str("\"topic\":");
    output.push_str(&serde_json::to_string(queue.topic_str())?);
    output.push_str(",\"brokerName\":");
    output.push_str(&serde_json::to_string(queue.broker_name().as_str())?);
    output.push_str(",\"queueId\":");
    output.push_str(&queue.queue_id().to_string());
    output.push('}');
    Ok(())
}

fn normalize_object_key_map_body(chars: &[char], mut index: usize) -> (String, usize) {
    let mut output = String::new();
    let mut expecting_key = true;
    let mut nested_value_depth = 0usize;
    let length = chars.len();

    while index < length {
        let current = chars[index];
        if expecting_key {
            match current {
                '}' => {
                    output.push('}');
                    return (output, index + 1);
                }
                '{' => {
                    let object_end = consume_balanced_object_end(chars, index);
                    let raw_key = chars[index..=object_end].iter().collect::<String>();
                    output.push_str(&serde_json::to_string(&raw_key).expect("stringify object key"));
                    index = object_end + 1;
                    expecting_key = false;
                }
                '"' => {
                    let string_end = consume_string_end(chars, index);
                    output.extend(chars[index..=string_end].iter());
                    index = string_end + 1;
                    expecting_key = false;
                }
                _ => {
                    output.push(current);
                    index += 1;
                }
            }
            continue;
        }

        match current {
            '"' => {
                let string_end = consume_string_end(chars, index);
                output.extend(chars[index..=string_end].iter());
                index = string_end + 1;
            }
            '{' | '[' => {
                nested_value_depth += 1;
                output.push(current);
                index += 1;
            }
            '}' => {
                if nested_value_depth == 0 {
                    output.push('}');
                    return (output, index + 1);
                }
                nested_value_depth -= 1;
                output.push('}');
                index += 1;
            }
            ']' => {
                nested_value_depth = nested_value_depth.saturating_sub(1);
                output.push(']');
                index += 1;
            }
            ',' if nested_value_depth == 0 => {
                output.push(',');
                index += 1;
                expecting_key = true;
            }
            _ => {
                output.push(current);
                index += 1;
            }
        }
    }

    (output, index)
}

fn consume_string_end(chars: &[char], start: usize) -> usize {
    let mut index = start + 1;
    let mut escaped = false;
    while index < chars.len() {
        if escaped {
            escaped = false;
        } else if chars[index] == '\\' {
            escaped = true;
        } else if chars[index] == '"' {
            return index;
        }
        index += 1;
    }
    chars.len().saturating_sub(1)
}

fn consume_balanced_object_end(chars: &[char], start: usize) -> usize {
    let mut depth = 0usize;
    let mut index = start;
    let mut in_string = false;
    let mut escaped = false;

    while index < chars.len() {
        let current = chars[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if current == '\\' {
                escaped = true;
            } else if current == '"' {
                in_string = false;
            }
        } else {
            match current {
                '"' => in_string = true,
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return index;
                    }
                }
                _ => {}
            }
        }
        index += 1;
    }

    chars.len().saturating_sub(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_offset_wrapper(broker: i64, consumer: i64, pull: i64) -> OffsetWrapper {
        let mut wrapper = OffsetWrapper::new();
        wrapper.set_broker_offset(broker);
        wrapper.set_consumer_offset(consumer);
        wrapper.set_pull_offset(pull);
        wrapper
    }

    fn create_mq(topic: &str, queue_id: i32) -> MessageQueue {
        let json = format!(
            r#"{{"topic": "{}", "brokerName": "broker_1", "queueId": {}}}"#,
            topic, queue_id
        );
        serde_json::from_str(&json).expect("Failed to create MessageQueue for test")
    }

    #[test]
    fn test_compute_total_diff() {
        let mut stats = ConsumeStats::new();
        let mut map = HashMap::new();

        // queue 1
        map.insert(create_mq("topic_a", 0), create_offset_wrapper(500, 400, 450));
        // queue 2
        map.insert(create_mq("topic_a", 1), create_offset_wrapper(200, 150, 180));

        stats.set_offset_table(map);

        assert_eq!(stats.compute_total_diff(), 150);
    }

    #[test]
    fn test_compute_inflight_total_diff() {
        let mut stats = ConsumeStats::new();
        let mut map = HashMap::new();
        map.insert(create_mq("topic_b", 0), create_offset_wrapper(500, 400, 450));
        map.insert(create_mq("topic_b", 1), create_offset_wrapper(200, 150, 180));

        stats.set_offset_table(map);

        assert_eq!(stats.compute_inflight_total_diff(), 80);
    }

    #[test]
    fn test_getters_and_setters() {
        let mut stats = ConsumeStats::new();
        stats.set_consume_tps(5.67);

        assert_eq!(stats.get_consume_tps(), 5.67);
        assert!(stats.get_offset_table().is_empty());

        stats
            .get_offset_table_mut()
            .insert(create_mq("test", 0), OffsetWrapper::new());
        assert_eq!(stats.get_offset_table().len(), 1);
    }

    #[test]
    fn test_empty_stats_diff() {
        let stats = ConsumeStats::new();

        assert_eq!(stats.compute_total_diff(), 0);
        assert_eq!(stats.compute_inflight_total_diff(), 0);
    }

    #[test]
    fn test_serialization_json_any_key() {
        let mut stats = ConsumeStats::new();
        let mq = create_mq("order", 2);
        stats.get_offset_table_mut().insert(mq, create_offset_wrapper(10, 5, 8));
        stats.set_consume_tps(1.0);

        let serialized = serde_json::to_string(&stats).expect("Serialization failed");

        assert!(serialized.contains("offsetTable"));
        assert!(serialized.contains("consumeTps"));

        let deserialized: ConsumeStats = serde_json::from_str(&serialized).expect("Deserialization failed");
        assert_eq!(deserialized.get_offset_table().len(), 1);
        assert_eq!(deserialized.compute_total_diff(), 5);
    }

    #[test]
    fn decode_accepts_nonstandard_offset_table_object_keys() {
        let body = r#"{"offsetTable":{{"topic":"TopicTest","brokerName":"broker-a","queueId":1}:{"brokerOffset":120,"consumerOffset":20,"pullOffset":80,"lastTimestamp":1700000000000}},"consumeTps":1.5}"#;

        let decoded = ConsumeStats::decode(body.as_bytes()).expect("consume stats should decode");

        assert_eq!(decoded.get_offset_table().len(), 1);
        assert_eq!(decoded.get_consume_tps(), 1.5);
        let (queue, offset) = decoded.get_offset_table().iter().next().expect("one queue");
        assert_eq!(queue.topic_str(), "TopicTest");
        assert_eq!(queue.broker_name(), "broker-a");
        assert_eq!(queue.queue_id(), 1);
        assert_eq!(offset.get_broker_offset(), 120);
        assert_eq!(offset.get_consumer_offset(), 20);
        assert_eq!(offset.get_pull_offset(), 80);
        assert_eq!(offset.get_last_timestamp(), 1_700_000_000_000);
    }

    #[test]
    fn normalize_nonstandard_offset_table_keys_stringifies_object_keys() {
        let input = r#"{"offsetTable":{{"topic":"TopicTest","brokerName":"broker-a","queueId":1}:{"brokerOffset":120}},"consumeTps":1.5}"#;
        let normalized = normalize_nonstandard_offset_table_keys(input);

        assert_eq!(
            normalized,
            r#"{"offsetTable":{"{\"topic\":\"TopicTest\",\"brokerName\":\"broker-a\",\"queueId\":1}":{"brokerOffset":120}},"consumeTps":1.5}"#
        );
    }

    #[test]
    fn java_compatible_encode_uses_object_keys_for_offset_table() {
        let mut stats = ConsumeStats::new();
        stats
            .get_offset_table_mut()
            .insert(create_mq("TopicTest", 1), create_offset_wrapper(120, 20, 80));
        stats.set_consume_tps(1.5);

        let encoded = String::from_utf8(stats.encode_java_compatible().expect("encode consume stats"))
            .expect("utf8 consume stats");

        assert!(encoded.contains(r#""offsetTable":{{"topic":"TopicTest","brokerName":"broker_1","queueId":1}:"#));
        assert!(!encoded.contains(r#""{\"topic\""#));

        let decoded = ConsumeStats::decode(encoded.as_bytes()).expect("decode java-compatible consume stats");
        assert_eq!(decoded.get_offset_table().len(), 1);
        assert_eq!(decoded.compute_total_diff(), 100);
    }
}
