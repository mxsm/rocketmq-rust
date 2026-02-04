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

    #[test]
    fn offset_table_accessors_work_with_borrow_and_move() {
        let queue_0 = MessageQueue::from_parts("TopicA", "BrokerA", 0);
        let queue_1 = MessageQueue::from_parts("TopicA", "BrokerA", 1);

        let mut offset_0 = TopicOffset::new();
        offset_0.set_min_offset(10);
        offset_0.set_max_offset(20);
        offset_0.set_last_update_timestamp(1000);

        let mut offset_1 = TopicOffset::new();
        offset_1.set_min_offset(30);
        offset_1.set_max_offset(40);
        offset_1.set_last_update_timestamp(2000);

        let mut table = TopicStatsTable::new();
        table.set_offset_table(HashMap::from([(queue_0.clone(), offset_0)]));

        let offset_table = table.get_offset_table();
        assert_eq!(offset_table.len(), 1);
        assert_eq!(offset_table.get(&queue_0).unwrap().get_max_offset(), 20);

        table.get_offset_table_mut().insert(queue_1.clone(), offset_1);
        assert_eq!(table.get_offset_table().len(), 2);
        assert_eq!(
            table
                .get_offset_table()
                .get(&queue_1)
                .unwrap()
                .get_last_update_timestamp(),
            2000
        );

        let moved = table.into_offset_table();
        assert_eq!(moved.len(), 2);
        assert!(moved.contains_key(&queue_0));
        assert!(moved.contains_key(&queue_1));
    }
}
