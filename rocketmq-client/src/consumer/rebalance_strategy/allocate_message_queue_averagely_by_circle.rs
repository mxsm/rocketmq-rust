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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::rebalance_strategy::check;

pub struct AllocateMessageQueueAveragelyByCircle;

impl AllocateMessageQueueStrategy for AllocateMessageQueueAveragelyByCircle {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        let mut result = Vec::new();
        if !check(consumer_group, current_cid, mq_all, cid_all)? {
            return Ok(result);
        }
        let index = cid_all.iter().position(|cid| cid == current_cid).unwrap_or(0);
        for (i, item) in mq_all.iter().enumerate().skip(index) {
            if i % cid_all.len() == index {
                result.push(item.clone());
            }
        }
        Ok(result)
    }

    #[inline]
    fn get_name(&self) -> &'static str {
        "AVG_BY_CIRCLE"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::*;

    #[test]
    fn allocate_returns_empty_when_check_fails() {
        let strategy = AllocateMessageQueueAveragelyByCircle;
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all = vec![MessageQueue::from_parts("topic", "broker", 0)];
        let cid_all = vec![CheetahString::from("consumer1")];

        let result = strategy
            .allocate(&consumer_group, &current_cid, &mq_all, &cid_all)
            .unwrap();
        assert!(!result.is_empty());
    }

    #[test]
    fn allocate_returns_correct_queues_for_single_consumer() {
        let strategy = AllocateMessageQueueAveragelyByCircle;
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all = vec![
            MessageQueue::from_parts("topic", "broker", 0),
            MessageQueue::from_parts("topic", "broker", 1),
        ];
        let cid_all = vec![CheetahString::from("consumer1")];

        let result = strategy
            .allocate(&consumer_group, &current_cid, &mq_all, &cid_all)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get_queue_id(), 0);
        assert_eq!(result[1].get_queue_id(), 1);
    }

    #[test]
    fn allocate_returns_correct_queues_for_multiple_consumers() {
        let strategy = AllocateMessageQueueAveragelyByCircle;
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer2");
        let mq_all = vec![
            MessageQueue::from_parts("topic", "broker", 0),
            MessageQueue::from_parts("topic", "broker", 1),
            MessageQueue::from_parts("topic", "broker", 2),
        ];
        let cid_all = vec![
            CheetahString::from("consumer1"),
            CheetahString::from("consumer2"),
            CheetahString::from("consumer3"),
        ];

        let result = strategy
            .allocate(&consumer_group, &current_cid, &mq_all, &cid_all)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get_queue_id(), 1);
    }

    #[test]
    fn get_name_returns_correct_name() {
        let strategy = AllocateMessageQueueAveragelyByCircle;
        assert_eq!(strategy.get_name(), "AVG_BY_CIRCLE");
    }

    #[test]
    fn test_allocate_message_queue_averagely_by_circle() {
        let consumer_id_list = create_consumer_id_list(4);
        let message_queue_list = create_message_queue_list(10);
        let allocate_queues = AllocateMessageQueueAveragelyByCircle
            .allocate(
                &CheetahString::from(""),
                &CheetahString::from("CID_PREFIX"),
                &message_queue_list,
                &consumer_id_list,
            )
            .unwrap();
        assert_eq!(0, allocate_queues.len());

        let mut consumer_allocate_queue = HashMap::new();
        for consumer_id in &consumer_id_list {
            let queues = AllocateMessageQueueAveragelyByCircle
                .allocate(
                    &CheetahString::from(""),
                    consumer_id,
                    &message_queue_list,
                    &consumer_id_list,
                )
                .unwrap();
            let queue_ids: Vec<i32> = queues.iter().map(|q| q.get_queue_id()).collect();
            consumer_allocate_queue.insert(consumer_id.clone(), queue_ids);
        }
        assert_eq!(vec![0, 4, 8], consumer_allocate_queue["CID_PREFIX0"]);
        assert_eq!(vec![1, 5, 9], consumer_allocate_queue["CID_PREFIX1"]);
        assert_eq!(vec![2, 6], consumer_allocate_queue["CID_PREFIX2"]);
        assert_eq!(vec![3, 7], consumer_allocate_queue["CID_PREFIX3"]);
    }
    fn create_consumer_id_list(size: usize) -> Vec<CheetahString> {
        (0..size).map(|i| format!("CID_PREFIX{}", i).into()).collect()
    }

    fn create_message_queue_list(size: usize) -> Vec<MessageQueue> {
        (0..size)
            .map(|i| MessageQueue::from_parts("topic", "broker", i as i32))
            .collect()
    }
}
