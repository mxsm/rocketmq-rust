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

use crate::protocol::admin::topic_offset::TopicOffset;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopicStatsTable {
    #[serde(with = "any_key_map")]
    offset_table: HashMap<MessageQueue, TopicOffset>,
}

impl TopicStatsTable {
    pub fn new() -> Self {
        Self {
            offset_table: HashMap::new(),
        }
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
    fn test_topic_stats_table_initialization() {
        let table = TopicStatsTable::new();
        assert!(table.get_offset_table().is_empty());
    }

    #[test]
    fn test_message_queue_default_new() {
        let mq = MessageQueue::new();
        let json = serde_json::to_value(&mq).unwrap();
        assert_eq!(json["queueId"], 0);
        assert_eq!(json["topic"], "");
    }

    #[test]
    fn test_set_and_get_offsets() {
        let mut table = TopicStatsTable::new();
        let mut map = HashMap::new();

        let mq = create_custom_mq("test_topic", 5);
        let mut offset = TopicOffset::new();
        offset.set_min_offset(100);
        offset.set_max_offset(200);

        map.insert(mq.clone(), offset);
        table.set_offset_table(map);
        let result_table = table.get_offset_table();
        assert_eq!(result_table.len(), 1);

        let retrieved_offset = result_table.get(&mq).expect("MQ should exist in map");
        assert_eq!(retrieved_offset.get_min_offset(), 100);
        assert_eq!(retrieved_offset.get_max_offset(), 200);
    }

    #[test]
    fn test_serialization_cycle_with_any_key() {
        let mut table = TopicStatsTable::new();
        let mut map = HashMap::new();

        let mq = create_custom_mq("order_topic", 1);
        let mut offset = TopicOffset::new();
        offset.set_last_update_timestamp(11111111);

        map.insert(mq, offset);
        table.set_offset_table(map);

        let serialized = serde_json::to_string(&table).expect("Serialization failed");

        assert!(serialized.contains("offsetTable"));

        let deserialized: TopicStatsTable = serde_json::from_str(&serialized).expect("Deserialization failed");
        assert_eq!(deserialized.get_offset_table().len(), 1);

        let offset_val = deserialized.get_offset_table().values().next().unwrap().clone();
        assert_eq!(offset_val.get_last_update_timestamp(), 11111111);
    }

    #[test]
    fn test_topic_offset_display_format() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(5);
        offset.set_max_offset(15);

        let output = format!("{}", offset);
        let expected = "TopicOffset{min_offset=5, max_offset=15, last_update_timestamp=0}";
        assert_eq!(output, expected);
    }
}
