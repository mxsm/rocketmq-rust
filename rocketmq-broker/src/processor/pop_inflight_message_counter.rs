//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use tracing::info;

type PopInflightMessageCounterMap =
    Arc<Mutex<HashMap<CheetahString, HashMap<i32, Arc<AtomicI64>>>>>;

pub(crate) struct PopInflightMessageCounter {
    should_start_time: Arc<AtomicU64>,
    topic_in_flight_message_num: PopInflightMessageCounterMap,
}

impl PopInflightMessageCounter {
    const TOPIC_GROUP_SEPARATOR: &'static str = "@";

    pub fn new(should_start_time: Arc<AtomicU64>) -> Self {
        PopInflightMessageCounter {
            should_start_time,
            topic_in_flight_message_num: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn increment_in_flight_message_num(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        num: i64,
    ) {
        if num <= 0 {
            return;
        }
        let key = Self::build_key(topic, group);
        let mut map = self.topic_in_flight_message_num.lock();
        map.entry(key)
            .or_default()
            .entry(queue_id)
            .or_insert_with(|| Arc::new(AtomicI64::new(0)))
            .fetch_add(num, Ordering::SeqCst);
    }

    pub fn decrement_in_flight_message_num(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        pop_time: i64,
        queue_id: i32,
        delta: i64,
    ) {
        if pop_time < self.should_start_time.load(Ordering::SeqCst) as i64 {
            return;
        }
        self.decrement_in_flight_message_num_internal(topic, group, queue_id, delta);
    }

    pub fn decrement_in_flight_message_num_checkpoint(&self, check_point: &PopCheckPoint) {
        if check_point.pop_time < self.should_start_time.load(Ordering::SeqCst) as i64 {
            return;
        }
        self.decrement_in_flight_message_num_internal(
            &check_point.topic,
            &check_point.cid,
            check_point.queue_id,
            1,
        );
    }

    fn decrement_in_flight_message_num_internal(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        delta: i64,
    ) {
        let key = Self::build_key(topic, group);
        let mut map = self.topic_in_flight_message_num.lock();
        if let Some(queue_num) = map.get_mut(&key) {
            if let Some(counter) = queue_num.get(&queue_id) {
                if counter.fetch_add(-delta, Ordering::SeqCst) <= 0 {
                    queue_num.remove(&queue_id);
                }
            }
            if queue_num.is_empty() {
                map.remove(&key);
            }
        }
    }

    pub fn clear_in_flight_message_num_by_group_name(&self, group: &CheetahString) {
        let keys: Vec<CheetahString> = {
            let map = self.topic_in_flight_message_num.lock();
            map.keys().cloned().collect()
        };
        for key in keys {
            if key.contains(group.as_str()) {
                if let Some((topic, group_name)) = Self::split_key(&key) {
                    if &group_name == group {
                        let mut map = self.topic_in_flight_message_num.lock();
                        map.remove(&key);
                        info!(
                            "PopInflightMessageCounter#clearInFlightMessageNumByGroupName: clean \
                             by group, topic={}, group={}",
                            topic, group_name
                        );
                    }
                }
            }
        }
    }

    pub fn clear_in_flight_message_num_by_topic_name(&self, topic: &CheetahString) {
        let keys: Vec<CheetahString> = {
            let map = self.topic_in_flight_message_num.lock();
            map.keys().cloned().collect()
        };
        for key in keys {
            if key.contains(topic.as_str()) {
                if let Some((topic_name, group)) = Self::split_key(&key) {
                    if topic_name.as_str() == topic {
                        let mut map = self.topic_in_flight_message_num.lock();
                        map.remove(&key);
                        info!(
                            "PopInflightMessageCounter#clearInFlightMessageNumByTopicName: clean \
                             by topic, topic={}, group={}",
                            topic_name, group
                        );
                    }
                }
            }
        }
    }

    pub fn clear_in_flight_message_num(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
    ) {
        let key = Self::build_key(topic, group);
        let mut map = self.topic_in_flight_message_num.lock();
        if let Some(queue_num) = map.get_mut(&key) {
            queue_num.remove(&queue_id);
            if queue_num.is_empty() {
                map.remove(&key);
            }
        }
    }

    pub fn get_group_pop_in_flight_message_num(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
    ) -> i64 {
        let key = Self::build_key(topic, group);
        let map = self.topic_in_flight_message_num.lock();
        if let Some(queue_counter) = map.get(&key) {
            if let Some(counter) = queue_counter.get(&queue_id) {
                return counter.load(Ordering::SeqCst).max(0);
            }
        }
        0
    }

    fn split_key(key: &CheetahString) -> Option<(CheetahString, CheetahString)> {
        let parts: Vec<&str> = key.split(Self::TOPIC_GROUP_SEPARATOR).collect();
        if parts.len() == 2 {
            Some((parts[0].into(), parts[1].into()))
        } else {
            None
        }
    }

    fn build_key(topic: &CheetahString, group: &CheetahString) -> CheetahString {
        format!("{}{}{}", topic, Self::TOPIC_GROUP_SEPARATOR, group).into()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use cheetah_string::CheetahString;

    use super::*;

    fn setup_counter() -> PopInflightMessageCounter {
        PopInflightMessageCounter::new(Arc::new(AtomicU64::new(0)))
    }

    #[test]
    fn increment_in_flight_message_num_increments_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");
        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        assert_eq!(
            counter.get_group_pop_in_flight_message_num(&topic, &group, 1),
            5
        );
    }

    #[test]
    fn decrement_in_flight_message_num_decrements_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");
        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.decrement_in_flight_message_num(&topic, &group, 0, 1, 3);
        assert_eq!(
            counter.get_group_pop_in_flight_message_num(&topic, &group, 1),
            2
        );
    }

    #[test]
    fn clear_in_flight_message_num_by_group_name_clears_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");
        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.clear_in_flight_message_num_by_group_name(&group);
        assert_eq!(
            counter.get_group_pop_in_flight_message_num(&topic, &group, 1),
            0
        );
    }

    #[test]
    fn clear_in_flight_message_num_by_topic_name_clears_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");
        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.clear_in_flight_message_num_by_topic_name(&topic);
        assert_eq!(
            counter.get_group_pop_in_flight_message_num(&topic, &group, 1),
            0
        );
    }

    #[test]
    fn clear_in_flight_message_num_clears_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");
        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.clear_in_flight_message_num(&topic, &group, 1);
        assert_eq!(
            counter.get_group_pop_in_flight_message_num(&topic, &group, 1),
            0
        );
    }

    #[test]
    fn decrement_in_flight_message_num_checkpoint_decrements_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");
        let checkpoint = PopCheckPoint {
            start_offset: 0,
            topic: topic.clone(),
            cid: group.clone(),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            queue_id: 1,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            re_put_times: None,
        };
        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.decrement_in_flight_message_num_checkpoint(&checkpoint);
        assert_eq!(
            counter.get_group_pop_in_flight_message_num(&topic, &group, 1),
            4
        );
    }
}
