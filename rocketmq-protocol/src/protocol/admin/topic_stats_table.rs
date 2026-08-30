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

use rocketmq_model::message::MessageQueue;
use serde::Deserialize;
use serde::Serialize;
use serde_json_any_key::*;

use crate::protocol::admin::consume_stats::normalize_nonstandard_offset_table_keys;
use crate::protocol::admin::topic_offset::TopicOffset;
use crate::protocol::RemotingDeserializable;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopicStatsTable {
    topic_put_tps: f64,
    #[serde(with = "any_key_map")]
    offset_table: HashMap<MessageQueue, TopicOffset>,
}

impl TopicStatsTable {
    pub fn new() -> Self {
        Self {
            topic_put_tps: 0.0,
            offset_table: HashMap::new(),
        }
    }

    pub fn get_topic_put_tps(&self) -> f64 {
        self.topic_put_tps
    }

    pub fn set_topic_put_tps(&mut self, topic_put_tps: f64) {
        self.topic_put_tps = topic_put_tps;
    }

    pub fn get_offset_table(&self) -> &HashMap<MessageQueue, TopicOffset> {
        &self.offset_table
    }

    pub fn get_offset_table_mut(&mut self) -> &mut HashMap<MessageQueue, TopicOffset> {
        &mut self.offset_table
    }

    pub fn into_offset_table(self) -> HashMap<MessageQueue, TopicOffset> {
        self.offset_table
    }

    pub fn set_offset_table(&mut self, offset_table: HashMap<MessageQueue, TopicOffset>) {
        self.offset_table = offset_table;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_custom_mq(topic: &str, queue_id: i32) -> MessageQueue {
        let json = format!(
            r#"{{"topic": "{}", "brokerName": "default", "queueId": {}}}"#,
            topic, queue_id
        );
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn methods_and_serde_preserve_topic_stats() {
        let mut table = TopicStatsTable::new();
        assert_eq!(table.get_topic_put_tps(), 0.0);
        assert!(table.get_offset_table().is_empty());

        table.set_topic_put_tps(2.5);
        let mut map = HashMap::new();
        let mq = create_custom_mq("order_topic", 5);
        let mut offset = TopicOffset::new();
        offset.set_min_offset(100);
        offset.set_max_offset(200);
        map.insert(mq.clone(), offset);
        table.set_offset_table(map);
        table
            .get_offset_table_mut()
            .get_mut(&mq)
            .expect("queue offset")
            .set_last_update_timestamp(11_111_111);

        let serialized = serde_json::to_string(&table).expect("Serialization failed");
        assert!(serialized.contains("offsetTable"));

        let deserialized: TopicStatsTable = serde_json::from_str(&serialized).expect("Deserialization failed");
        assert_eq!(deserialized.get_topic_put_tps(), 2.5);
        let offsets = deserialized.into_offset_table();
        let offset = offsets.get(&mq).expect("queue offset should round-trip");
        assert_eq!(offset.get_min_offset(), 100);
        assert_eq!(offset.get_max_offset(), 200);
        assert_eq!(offset.get_last_update_timestamp(), 11_111_111);
    }

    #[test]
    fn test_decode_java_fastjson_message_queue_keys() {
        let body = br#"{"offsetTable":{{"topic":"TBW102","brokerName":"broker-a","queueId":0}:{"minOffset":0,"maxOffset":0,"lastUpdateTimestamp":0}},"topicPutTps":1.5}"#;

        let table = TopicStatsTable::decode(body).expect("decode Java fastjson topic stats");

        let queue = MessageQueue::from_parts("TBW102", "broker-a", 0);
        let offset = table
            .get_offset_table()
            .get(&queue)
            .expect("queue offset should decode");
        assert_eq!(offset.get_min_offset(), 0);
        assert_eq!(offset.get_max_offset(), 0);
        assert_eq!(table.get_topic_put_tps(), 1.5);
    }
}
