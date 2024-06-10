/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::sync::Arc;

use tracing::info;

pub struct QueueOffsetOperator {
    topic_queue_table: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
    batch_topic_queue_table: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
    lmq_topic_queue_table: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
}

impl Default for QueueOffsetOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueOffsetOperator {
    pub fn new() -> Self {
        QueueOffsetOperator {
            topic_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            batch_topic_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            lmq_topic_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        }
    }

    pub fn get_queue_offset(&self, topic_queue_key: &str) -> i64 {
        let topic_queue_table = self.topic_queue_table.lock();
        let mut table = topic_queue_table;
        *table.entry(topic_queue_key.to_string()).or_insert(0)
    }

    pub fn get_topic_queue_next_offset(&self, topic_queue_key: &str) -> Option<i64> {
        let topic_queue_table = self.topic_queue_table.lock();
        let table = topic_queue_table;
        table.get(topic_queue_key).cloned()
    }

    pub fn increase_queue_offset(&self, topic_queue_key: &str, message_num: i16) {
        let topic_queue_table = self.topic_queue_table.lock();
        let mut table = topic_queue_table;
        let entry = table.entry(topic_queue_key.to_string()).or_insert(0);
        *entry += message_num as i64;
    }

    pub fn update_queue_offset(&self, topic_queue_key: &str, offset: i64) {
        let topic_queue_table = self.topic_queue_table.lock();
        let mut table = topic_queue_table;
        table.insert(topic_queue_key.to_string(), offset);
    }

    pub fn get_batch_queue_offset(&self, topic_queue_key: &str) -> i64 {
        let batch_topic_queue_table = self.batch_topic_queue_table.lock();
        let mut table = batch_topic_queue_table;
        *table.entry(topic_queue_key.to_string()).or_insert(0)
    }

    pub fn increase_batch_queue_offset(&self, topic_queue_key: &str, message_num: i16) {
        let batch_topic_queue_table = self.batch_topic_queue_table.lock();
        let mut table = batch_topic_queue_table;
        let entry = table.entry(topic_queue_key.to_string()).or_insert(0);
        *entry += message_num as i64;
    }

    pub fn get_lmq_offset(&self, topic_queue_key: &str) -> i64 {
        let lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        let mut table = lmq_topic_queue_table;
        *table.entry(topic_queue_key.to_string()).or_insert(0)
    }

    pub fn get_lmq_topic_queue_next_offset(&self, topic_queue_key: &str) -> Option<i64> {
        let lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        let table = lmq_topic_queue_table;
        table.get(topic_queue_key).cloned()
    }

    pub fn increase_lmq_offset(&self, queue_key: &str, message_num: i16) {
        let lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        let mut table = lmq_topic_queue_table;
        let entry = table.entry(queue_key.to_string()).or_insert(0);
        *entry += message_num as i64;
    }

    pub fn current_queue_offset(&self, topic_queue_key: &str) -> i64 {
        let topic_queue_table = self.topic_queue_table.lock();
        let table = topic_queue_table;
        let current_queue_offset = table.get(topic_queue_key);
        current_queue_offset.map_or(0, |offset| *offset)
    }

    pub fn remove(&self, topic: &str, queue_id: i32) {
        let topic_queue_key = format!("{}-{}", topic, queue_id);
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

    pub fn set_topic_queue_table(&self, topic_queue_table: HashMap<String, i64>) {
        *self.topic_queue_table.lock() = topic_queue_table;
    }

    pub fn set_lmq_topic_queue_table(&self, lmq_topic_queue_table: HashMap<String, i64>) {
        let mut table = HashMap::new();
        for (key, value) in lmq_topic_queue_table.iter() {
            if key.contains("lmq") {
                table.insert(key.clone(), *value);
            }
        }
        *self.lmq_topic_queue_table.lock() = table;
    }

    pub fn set_batch_topic_queue_table(&self, batch_topic_queue_table: HashMap<String, i64>) {
        *self.batch_topic_queue_table.lock() = batch_topic_queue_table;
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
        assert_eq!(operator.get_queue_offset("new_key"), 0);
    }

    #[test]
    fn get_queue_offset_returns_existing_value() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("existing_key", 5);
        assert_eq!(operator.get_queue_offset("existing_key"), 5);
    }

    #[test]
    fn increase_queue_offset_increases_existing_value() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("key", 5);
        operator.increase_queue_offset("key", 3);
        assert_eq!(operator.get_queue_offset("key"), 8);
    }

    #[test]
    fn update_queue_offset_updates_existing_value() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("key", 5);
        operator.update_queue_offset("key", 10);
        assert_eq!(operator.get_queue_offset("key"), 10);
    }

    #[test]
    fn remove_clears_key_from_all_tables() {
        let operator = QueueOffsetOperator::new();
        operator.increase_queue_offset("topic-1", 5);
        operator.increase_batch_queue_offset("topic-1", 5);
        operator.increase_lmq_offset("topic-1", 5);

        operator.remove("topic", 1);

        assert_eq!(operator.get_queue_offset("topic-1"), 0);
        assert_eq!(operator.get_batch_queue_offset("topic-1"), 0);
        assert_eq!(operator.get_lmq_offset("topic-1"), 0);
    }

    #[test]
    fn set_topic_queue_table_replaces_existing_table() {
        let operator = QueueOffsetOperator::new();
        let mut new_table = HashMap::new();
        new_table.insert("new_key".to_string(), 10);

        operator.set_topic_queue_table(new_table);

        assert_eq!(operator.get_queue_offset("new_key"), 10);
    }
}
