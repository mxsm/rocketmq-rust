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
use std::sync::Arc;

use cheetah_string::CheetahString;
use tracing::info;

pub struct QueueOffsetOperator {
    topic_queue_table: Arc<parking_lot::Mutex<HashMap<CheetahString, i64>>>,
    batch_topic_queue_table: Arc<parking_lot::Mutex<HashMap<CheetahString, i64>>>,
    lmq_topic_queue_table: Arc<parking_lot::Mutex<HashMap<CheetahString, i64>>>,
}

impl Default for QueueOffsetOperator {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl QueueOffsetOperator {
    #[inline]
    pub fn new() -> Self {
        QueueOffsetOperator {
            topic_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            batch_topic_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            lmq_topic_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        }
    }

    #[inline]
    pub fn get_queue_offset(&self, topic_queue_key: CheetahString) -> i64 {
        let topic_queue_table = self.topic_queue_table.lock();
        let mut table = topic_queue_table;
        *table.entry(topic_queue_key).or_insert(0)
    }

    #[inline]
    pub fn get_topic_queue_next_offset(&self, topic_queue_key: &CheetahString) -> Option<i64> {
        let topic_queue_table = self.topic_queue_table.lock();
        let table = topic_queue_table;
        table.get(topic_queue_key).cloned()
    }

    #[inline]
    pub fn increase_queue_offset(&self, topic_queue_key: CheetahString, message_num: i16) {
        let topic_queue_table = self.topic_queue_table.lock();
        let mut table = topic_queue_table;
        let entry = table.entry(topic_queue_key).or_insert(0);
        *entry += message_num as i64;
    }

    #[inline]
    pub fn update_queue_offset(&self, topic_queue_key: &CheetahString, offset: i64) {
        let topic_queue_table = self.topic_queue_table.lock();
        let mut table = topic_queue_table;
        table.insert(topic_queue_key.clone(), offset);
    }

    #[inline]
    pub fn get_batch_queue_offset(&self, topic_queue_key: &CheetahString) -> i64 {
        let batch_topic_queue_table = self.batch_topic_queue_table.lock();
        let mut table = batch_topic_queue_table;
        *table.entry(topic_queue_key.clone()).or_insert(0)
    }

    #[inline]
    pub fn increase_batch_queue_offset(&self, topic_queue_key: &CheetahString, message_num: i16) {
        let batch_topic_queue_table = self.batch_topic_queue_table.lock();
        let mut table = batch_topic_queue_table;
        let entry = table.entry(topic_queue_key.clone()).or_insert(0);
        *entry += message_num as i64;
    }

    #[inline]
    pub fn get_lmq_offset(&self, topic_queue_key: &CheetahString) -> i64 {
        let lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        let mut table = lmq_topic_queue_table;
        *table.entry(topic_queue_key.clone()).or_insert(0)
    }

    #[inline]
    pub fn get_lmq_topic_queue_next_offset(&self, topic_queue_key: &CheetahString) -> Option<i64> {
        let lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        let table = lmq_topic_queue_table;
        table.get(topic_queue_key).cloned()
    }

    #[inline]
    pub fn increase_lmq_offset(&self, queue_key: &CheetahString, message_num: i16) {
        let lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        let mut table = lmq_topic_queue_table;
        let entry = table.entry(queue_key.clone()).or_insert(0);
        *entry += message_num as i64;
    }

    #[inline]
    pub fn current_queue_offset(&self, topic_queue_key: &CheetahString) -> i64 {
        let topic_queue_table = self.topic_queue_table.lock();
        let table = topic_queue_table;
        let current_queue_offset = table.get(topic_queue_key);
        current_queue_offset.map_or(0, |offset| *offset)
    }

    #[inline]
    pub fn remove(&self, topic: &CheetahString, queue_id: i32) {
        let topic_queue_key = CheetahString::from(format!("{topic}-{queue_id}"));
        // Beware of thread-safety
        let mut topic_queue_table = self.topic_queue_table.lock();
        topic_queue_table.remove(&topic_queue_key);
        let mut batch_topic_queue_table = self.batch_topic_queue_table.lock();
        batch_topic_queue_table.remove(&topic_queue_key);
        let mut lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        lmq_topic_queue_table.remove(&topic_queue_key);

        info!(
            "removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}",
            topic, queue_id
        );
    }

    #[inline]
    pub fn set_topic_queue_table(&self, topic_queue_table: HashMap<CheetahString, i64>) {
        *self.topic_queue_table.lock() = topic_queue_table;
    }

    #[inline]
    pub fn set_lmq_topic_queue_table(&self, lmq_topic_queue_table: HashMap<CheetahString, i64>) {
        let mut table = HashMap::new();
        for (key, value) in lmq_topic_queue_table.iter() {
            if key.contains("lmq") {
                table.insert(key.clone(), *value);
            }
        }
        *self.lmq_topic_queue_table.lock() = table;
    }

    #[inline]
    pub fn set_batch_topic_queue_table(&self, batch_topic_queue_table: HashMap<CheetahString, i64>) {
        *self.batch_topic_queue_table.lock() = batch_topic_queue_table;
    }

    #[inline]
    pub fn get_topic_queue_table(&self) -> HashMap<CheetahString, i64> {
        self.topic_queue_table.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn queue_offset_operator_initializes_empty_tables() {
        let operator = QueueOffsetOperator::new();

        assert!(operator.topic_queue_table.lock().is_empty());
        assert!(operator.batch_topic_queue_table.lock().is_empty());
        assert!(operator.lmq_topic_queue_table.lock().is_empty());
    }

    #[test]
    fn get_queue_offset_returns_zero_for_new_key() {
        let operator = QueueOffsetOperator::new();
        assert_eq!(operator.get_queue_offset("new_key".into()), 0);
    }

    #[test]
    fn get_queue_offset_returns_existing_value() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("existing_key".into(), 5);
        assert_eq!(operator.get_queue_offset("existing_key".into()), 5);
    }

    #[test]
    fn increase_queue_offset_increases_existing_value() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("key".into(), 5);
        operator.increase_queue_offset("key".into(), 3);
        assert_eq!(operator.get_queue_offset("key".into()), 8);
    }

    #[test]
    fn update_queue_offset_updates_existing_value() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("key".into(), 5);
        operator.update_queue_offset(&CheetahString::from_static_str("key"), 10);
        assert_eq!(operator.get_queue_offset("key".into()), 10);
    }

    #[test]
    fn remove_clears_key_from_all_tables() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("topic-1".into(), 5);
        operator.increase_batch_queue_offset(&CheetahString::from_static_str("topic-1"), 5);
        operator.increase_lmq_offset(&CheetahString::from_static_str("topic-1"), 5);

        operator.remove(&CheetahString::from_static_str("topic"), 1);

        assert_eq!(operator.get_queue_offset("topic-1".into()), 0);
        assert_eq!(
            operator.get_batch_queue_offset(&CheetahString::from_static_str("topic-1")),
            0
        );
        assert_eq!(operator.get_lmq_offset(&CheetahString::from_static_str("topic-1")), 0);
    }

    #[test]
    fn set_topic_queue_table_replaces_existing_table() {
        let operator = QueueOffsetOperator::new();
        let mut new_table = HashMap::new();
        new_table.insert("new_key".into(), 10);

        operator.set_topic_queue_table(new_table);

        assert_eq!(operator.get_queue_offset("new_key".into()), 10);
    }
}
