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

use std::{collections::HashMap, sync::Arc};

pub struct QueueOffsetOperator {
    topic_queue_table: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
    batch_topic_queue_table: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
    lmq_topic_queue_table: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
}

impl QueueOffsetOperator {
    pub fn new() -> Self {
        QueueOffsetOperator {
            topic_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            batch_topic_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            lmq_topic_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        }
    }

    fn get_queue_offset(&self, topic_queue_key: &str) -> i64 {
        let topic_queue_table = self.topic_queue_table.lock();
        let mut table = topic_queue_table;
        *table.entry(topic_queue_key.to_string()).or_insert(0)
    }

    fn get_topic_queue_next_offset(&self, topic_queue_key: &str) -> Option<i64> {
        let topic_queue_table = self.topic_queue_table.lock();
        let table = topic_queue_table;
        table.get(topic_queue_key).cloned()
    }

    fn increase_queue_offset(&self, topic_queue_key: &str, message_num: i16) {
        let topic_queue_table = self.topic_queue_table.lock();
        let mut table = topic_queue_table;
        let entry = table.entry(topic_queue_key.to_string()).or_insert(0);
        *entry += message_num as i64;
    }

    fn update_queue_offset(&self, topic_queue_key: &str, offset: i64) {
        let topic_queue_table = self.topic_queue_table.lock();
        let mut table = topic_queue_table;
        table.insert(topic_queue_key.to_string(), offset);
    }

    fn get_batch_queue_offset(&self, topic_queue_key: &str) -> i64 {
        let batch_topic_queue_table = self.batch_topic_queue_table.lock();
        let mut table = batch_topic_queue_table;
        *table.entry(topic_queue_key.to_string()).or_insert(0)
    }

    fn increase_batch_queue_offset(&self, topic_queue_key: &str, message_num: i16) {
        let batch_topic_queue_table = self.batch_topic_queue_table.lock();
        let mut table = batch_topic_queue_table;
        let entry = table.entry(topic_queue_key.to_string()).or_insert(0);
        *entry += message_num as i64;
    }

    fn get_lmq_offset(&self, topic_queue_key: &str) -> i64 {
        let lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        let mut table = lmq_topic_queue_table;
        *table.entry(topic_queue_key.to_string()).or_insert(0)
    }

    fn get_lmq_topic_queue_next_offset(&self, topic_queue_key: &str) -> Option<i64> {
        let lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        let table = lmq_topic_queue_table;
        table.get(topic_queue_key).cloned()
    }

    fn increase_lmq_offset(&self, queue_key: &str, message_num: i16) {
        let lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        let mut table = lmq_topic_queue_table;
        let entry = table.entry(queue_key.to_string()).or_insert(0);
        *entry += message_num as i64;
    }

    fn current_queue_offset(&self, topic_queue_key: &str) -> i64 {
        let topic_queue_table = self.topic_queue_table.lock();
        let table = topic_queue_table;
        let current_queue_offset = table.get(topic_queue_key);
        current_queue_offset.map_or(0, |offset| *offset)
    }

    fn remove(&self, topic: &str, queue_id: i32) {
        let topic_queue_key = format!("{}-{}", topic, queue_id);
        // Beware of thread-safety
        let mut topic_queue_table = self.topic_queue_table.lock();
        topic_queue_table.remove(&topic_queue_key);
        let mut batch_topic_queue_table = self.batch_topic_queue_table.lock();
        batch_topic_queue_table.remove(&topic_queue_key);
        let mut lmq_topic_queue_table = self.lmq_topic_queue_table.lock();
        lmq_topic_queue_table.remove(&topic_queue_key);

        println!(
            "removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}",
            topic, queue_id
        );
    }

    fn set_topic_queue_table(&self, topic_queue_table: HashMap<String, i64>) {
        *self.topic_queue_table.lock() = topic_queue_table;
    }

    fn set_lmq_topic_queue_table(&self, lmq_topic_queue_table: HashMap<String, i64>) {
        let mut table = HashMap::new();
        for (key, value) in lmq_topic_queue_table.iter() {
            if key.contains("lmq") {
                table.insert(key.clone(), *value);
            }
        }
        *self.lmq_topic_queue_table.lock() = table;
    }

    fn set_batch_topic_queue_table(&self, batch_topic_queue_table: HashMap<String, i64>) {
        *self.batch_topic_queue_table.lock() = batch_topic_queue_table;
    }
}
