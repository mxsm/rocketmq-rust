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
use parking_lot::Mutex;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;

use crate::offset::manager::consumer_order_info_manager::OrderInfo;

const TOPIC_GROUP_SEPARATOR: &str = "@";

#[derive(Default)]
pub(crate) struct MemoryConsumerOrderInfoManager {
    table: Mutex<HashMap<CheetahString, HashMap<i32, OrderInfo>>>,
}

impl MemoryConsumerOrderInfoManager {
    pub(crate) fn check_block(
        &self,
        attempt_id: &CheetahString,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        invisible_time: u64,
    ) -> bool {
        self.table
            .lock()
            .get_mut(&build_key(topic, group))
            .and_then(|queues| queues.get_mut(&queue_id))
            .is_some_and(|order_info| order_info.need_block(attempt_id.as_str(), invisible_time))
    }

    pub(crate) fn clear_block(&self, topic: &CheetahString, group: &CheetahString, queue_id: i32) {
        if let Some(queues) = self.table.lock().get_mut(&build_key(topic, group)) {
            queues.remove(&queue_id);
        }
    }

    pub(crate) fn update(
        &self,
        attempt_id: CheetahString,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        pop_time: u64,
        invisible_time: u64,
        msg_queue_offset_list: Vec<u64>,
        order_info_builder: &mut String,
    ) {
        let key = build_key(topic, group);
        let mut table = self.table.lock();
        let queues = table.entry(key).or_default();
        let previous = queues.get(&queue_id).cloned();

        let mut order_info = OrderInfo {
            pop_time,
            invisible_time: Some(invisible_time),
            offset_list: OrderInfo::build_offset_list(msg_queue_offset_list),
            offset_next_visible_time: HashMap::new(),
            offset_consumed_count: HashMap::new(),
            last_consume_timestamp: current_millis(),
            commit_offset_bit: 0,
            attempt_id: attempt_id.to_string(),
        };

        if let Some(previous) = previous {
            order_info.merge_offset_consumed_count(
                previous.attempt_id.as_str(),
                previous.offset_list,
                previous.offset_consumed_count,
            );
        }
        queues.insert(queue_id, order_info.clone());

        let mut min_consumed_times = i32::MAX;
        for (offset, consumed_times) in &order_info.offset_consumed_count {
            ExtraInfoUtil::build_queue_offset_order_count_info(
                order_info_builder,
                topic.as_str(),
                queue_id as i64,
                *offset as i64,
                *consumed_times,
            );
            min_consumed_times = min_consumed_times.min(*consumed_times);
        }

        if order_info.offset_consumed_count.len() != order_info.offset_list.len() {
            min_consumed_times = 0;
        }
        if min_consumed_times == i32::MAX {
            min_consumed_times = 0;
        }

        ExtraInfoUtil::build_queue_id_order_count_info(
            order_info_builder,
            topic.as_str(),
            queue_id,
            min_consumed_times,
        );
    }

    pub(crate) fn order_info_count(&self) -> usize {
        self.table.lock().len()
    }
}

fn build_key(topic: &CheetahString, group: &CheetahString) -> CheetahString {
    CheetahString::from_string(format!("{topic}{TOPIC_GROUP_SEPARATOR}{group}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_creates_order_info_and_queue_level_count() {
        let manager = MemoryConsumerOrderInfoManager::default();
        let topic = CheetahString::from_static_str("%LMQ%$parent$child-a");
        let group = CheetahString::from_static_str("group-a");
        let mut builder = String::new();

        manager.update(
            CheetahString::from_static_str("attempt-1"),
            &topic,
            &group,
            0,
            1,
            30_000,
            vec![10, 11],
            &mut builder,
        );

        assert!(builder.ends_with("0 0 0"));
        assert_eq!(manager.order_info_count(), 1);
    }

    #[test]
    fn different_attempt_is_blocked_before_invisible_timeout() {
        let manager = MemoryConsumerOrderInfoManager::default();
        let topic = CheetahString::from_static_str("%LMQ%$parent$child-a");
        let group = CheetahString::from_static_str("group-a");
        let mut builder = String::new();
        manager.update(
            CheetahString::from_static_str("attempt-1"),
            &topic,
            &group,
            0,
            current_millis(),
            30_000,
            vec![10],
            &mut builder,
        );

        assert!(manager.check_block(&CheetahString::from_static_str("attempt-2"), &topic, &group, 0, 30_000,));
        assert!(!manager.check_block(&CheetahString::from_static_str("attempt-1"), &topic, &group, 0, 30_000,));
    }
}
