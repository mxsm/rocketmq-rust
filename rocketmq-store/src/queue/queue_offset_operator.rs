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
use dashmap::DashMap;
use rocketmq_common::common::mix_all::is_lmq;
use tracing::info;

use crate::queue::multi_dispatch_utils::lmq_queue_key;

pub struct QueueOffsetOperator {
    topic_queue_table: Arc<DashMap<CheetahString, i64>>,
    batch_topic_queue_table: Arc<DashMap<CheetahString, i64>>,
    lmq_topic_queue_table: Arc<DashMap<CheetahString, i64>>,
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
            topic_queue_table: Arc::new(DashMap::new()),
            batch_topic_queue_table: Arc::new(DashMap::new()),
            lmq_topic_queue_table: Arc::new(DashMap::new()),
        }
    }

    #[inline]
    pub fn get_queue_offset(&self, topic_queue_key: CheetahString) -> i64 {
        *self.topic_queue_table.entry(topic_queue_key).or_insert(0)
    }

    #[inline]
    pub fn get_topic_queue_next_offset(&self, topic_queue_key: &CheetahString) -> Option<i64> {
        self.topic_queue_table.get(topic_queue_key).map(|offset| *offset)
    }

    #[inline]
    pub fn increase_queue_offset(&self, topic_queue_key: CheetahString, message_num: i16) {
        self.topic_queue_table
            .entry(topic_queue_key)
            .and_modify(|offset| *offset += message_num as i64)
            .or_insert(message_num as i64);
    }

    #[inline]
    pub fn update_queue_offset(&self, topic_queue_key: &CheetahString, offset: i64) {
        self.topic_queue_table.insert(topic_queue_key.clone(), offset);
    }

    #[inline]
    pub fn get_batch_queue_offset(&self, topic_queue_key: &CheetahString) -> i64 {
        *self.batch_topic_queue_table.entry(topic_queue_key.clone()).or_insert(0)
    }

    #[inline]
    pub fn increase_batch_queue_offset(&self, topic_queue_key: &CheetahString, message_num: i16) {
        self.batch_topic_queue_table
            .entry(topic_queue_key.clone())
            .and_modify(|offset| *offset += message_num as i64)
            .or_insert(message_num as i64);
    }

    #[inline]
    pub fn get_lmq_offset(&self, topic_queue_key: &CheetahString) -> i64 {
        *self.lmq_topic_queue_table.entry(topic_queue_key.clone()).or_insert(0)
    }

    #[inline]
    pub fn get_lmq_topic_queue_next_offset(&self, topic_queue_key: &CheetahString) -> Option<i64> {
        self.lmq_topic_queue_table.get(topic_queue_key).map(|offset| *offset)
    }

    #[inline]
    pub fn increase_lmq_offset(&self, queue_key: &CheetahString, message_num: i16) {
        self.lmq_topic_queue_table
            .entry(queue_key.clone())
            .and_modify(|offset| *offset += message_num as i64)
            .or_insert(message_num as i64);
    }

    #[inline]
    pub fn current_queue_offset(&self, topic_queue_key: &CheetahString) -> i64 {
        self.topic_queue_table.get(topic_queue_key).map_or(0, |offset| *offset)
    }

    #[inline]
    pub fn remove(&self, topic: &CheetahString, queue_id: i32) {
        let topic_queue_key = CheetahString::from(format!("{topic}-{queue_id}"));
        self.topic_queue_table.remove(&topic_queue_key);
        self.batch_topic_queue_table.remove(&topic_queue_key);
        self.lmq_topic_queue_table.remove(&topic_queue_key);

        info!(
            "removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}",
            topic, queue_id
        );
    }

    #[inline]
    pub fn set_topic_queue_table(&self, topic_queue_table: HashMap<CheetahString, i64>) {
        self.topic_queue_table.clear();
        for (key, value) in topic_queue_table {
            self.topic_queue_table.insert(key, value);
        }
    }

    #[inline]
    pub fn set_lmq_topic_queue_table(&self, lmq_topic_queue_table: HashMap<CheetahString, i64>) {
        self.lmq_topic_queue_table.clear();
        for (key, value) in lmq_topic_queue_table {
            if is_lmq(Some(key.as_str())) {
                self.lmq_topic_queue_table.insert(key, value);
            }
        }
    }

    #[inline]
    pub fn get_lmq_num(&self) -> i32 {
        self.lmq_topic_queue_table.len() as i32
    }

    #[inline]
    pub fn is_lmq_exist(&self, lmq_topic: &str) -> bool {
        let queue_key = CheetahString::from_string(lmq_queue_key(lmq_topic));
        self.lmq_topic_queue_table.contains_key(&queue_key)
    }

    #[inline]
    pub fn set_batch_topic_queue_table(&self, batch_topic_queue_table: HashMap<CheetahString, i64>) {
        self.batch_topic_queue_table.clear();
        for (key, value) in batch_topic_queue_table {
            self.batch_topic_queue_table.insert(key, value);
        }
    }

    #[inline]
    pub fn get_topic_queue_table(&self) -> HashMap<CheetahString, i64> {
        self.topic_queue_table
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn queue_offset_operator_initializes_empty_tables() {
        let operator = QueueOffsetOperator::new();

        assert!(operator.topic_queue_table.is_empty());
        assert!(operator.batch_topic_queue_table.is_empty());
        assert!(operator.lmq_topic_queue_table.is_empty());
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
        operator.increase_queue_offset("old_key".into(), 5);

        let mut new_table = HashMap::new();
        new_table.insert("new_key".into(), 10);
        operator.set_topic_queue_table(new_table);

        let old_key = CheetahString::from_static_str("old_key");
        assert_eq!(operator.get_topic_queue_next_offset(&old_key), None);
        assert_eq!(operator.get_queue_offset("new_key".into()), 10);
    }

    #[test]
    fn set_lmq_topic_queue_table_keeps_prefixed_lmq_entries() {
        let operator = QueueOffsetOperator::new();
        let mut table = HashMap::new();
        table.insert(CheetahString::from_static_str("%LMQ%group-0"), 7);
        table.insert(CheetahString::from_static_str("normal-topic-0"), 3);

        operator.set_lmq_topic_queue_table(table);

        assert_eq!(operator.lmq_topic_queue_table.len(), 1);
        assert_eq!(
            operator
                .lmq_topic_queue_table
                .get(&CheetahString::from_static_str("%LMQ%group-0"))
                .map(|offset| *offset),
            Some(7)
        );
        assert_eq!(operator.get_lmq_num(), 1);
        assert!(operator.is_lmq_exist("%LMQ%group"));
        assert!(!operator.is_lmq_exist("normal-topic"));
    }

    #[test]
    fn concurrent_increase_queue_offset_keeps_offsets_consistent() {
        let operator = Arc::new(QueueOffsetOperator::new());
        let mut handles = Vec::new();

        for thread_id in 0..8 {
            let operator = operator.clone();
            handles.push(std::thread::spawn(move || {
                for iteration in 0..1_000 {
                    let key = CheetahString::from_string(format!("topic-{}", thread_id % 4));
                    let batch_key = CheetahString::from_string(format!("batch-topic-{}", iteration % 4));
                    operator.increase_queue_offset(key, 1);
                    operator.increase_batch_queue_offset(&batch_key, 1);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("offset worker should finish");
        }

        let total_normal: i64 = operator.get_topic_queue_table().values().sum();
        assert_eq!(total_normal, 8_000);

        let total_batch: i64 = operator
            .batch_topic_queue_table
            .iter()
            .map(|entry| *entry.value())
            .sum();
        assert_eq!(total_batch, 8_000);
    }
}
