use std::collections::HashSet;

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

pub struct AllocateMessageQueueByMachineRoom {
    consumer_idcs: HashSet<CheetahString>,
}

impl AllocateMessageQueueByMachineRoom {
    #[inline]
    pub fn new(consumer_idcs: HashSet<CheetahString>) -> Self {
        Self { consumer_idcs }
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByMachineRoom {
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

        let current_index = cid_all.iter().position(|cid| cid == current_cid).unwrap_or(0);

        let premq_all: Vec<MessageQueue> = mq_all
            .iter()
            .filter(|mq| {
                let parts: Vec<&str> = mq.get_broker_name().split('@').collect();
                parts.len() == 2 && self.consumer_idcs.contains(parts[0])
            })
            .cloned()
            .collect();

        let mod_size = premq_all.len() / cid_all.len();
        let rem = premq_all.len() % cid_all.len();

        let start_index = mod_size * current_index;
        let end_index = start_index + mod_size;

        result.extend(
            premq_all
                .iter()
                .skip(start_index)
                .take(end_index - start_index)
                .cloned(),
        );

        if rem > current_index {
            let extra_index = current_index + mod_size * cid_all.len();
            if extra_index < premq_all.len() {
                result.push(premq_all[extra_index].clone());
            }
        }

        Ok(result)
    }

    #[inline]
    fn get_name(&self) -> &'static str {
        "MACHINE_ROOM"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::hashset;

    use super::*;

    #[test]
    fn test_allocate_message_queue_by_machine_room() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer0");
        let mq_all = create_message_queue_list(10);
        let cid_all = create_consumer_id_list(2);
        let consumer_idcs: HashSet<CheetahString> = hashset!(CheetahString::from("room1"));
        let strategy = AllocateMessageQueueByMachineRoom::new(consumer_idcs);

        let mut consumer_allocate_queue = HashMap::new();
        for consumer_id in &cid_all {
            let queues = strategy
                .allocate(&consumer_group, consumer_id, &mq_all, &cid_all)
                .unwrap();
            let queue_ids: Vec<i32> = queues.into_iter().map(|mq| mq.get_queue_id()).collect();
            consumer_allocate_queue.insert(consumer_id.clone(), queue_ids);
        }

        assert_eq!(
            consumer_allocate_queue.get("CID_PREFIX0").unwrap().as_slice(),
            &[0, 1, 4]
        );
        assert_eq!(consumer_allocate_queue.get("CID_PREFIX1").unwrap().as_slice(), &[2, 3]);
    }

    #[test]
    fn allocate_returns_empty_when_check_fails() {
        let consumer_idcs: HashSet<CheetahString> = hashset!(CheetahString::from("room1"));
        let strategy = AllocateMessageQueueByMachineRoom::new(consumer_idcs);
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all = vec![MessageQueue::from_parts("topic", "broker", 0)];
        let cid_all = vec![CheetahString::from("consumer2")];

        let result = strategy
            .allocate(&consumer_group, &current_cid, &mq_all, &cid_all)
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn allocate_returns_correct_queues_for_single_consumer() {
        let consumer_idcs: HashSet<CheetahString> = hashset!(CheetahString::from("room1"));
        let strategy = AllocateMessageQueueByMachineRoom::new(consumer_idcs);
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer1");
        let mq_all = vec![
            MessageQueue::from_parts("topic", "room1@broker-a", 0),
            MessageQueue::from_parts("topic", "room1@broker-a", 1),
            MessageQueue::from_parts("topic", "room2@broker-b", 2),
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
        let consumer_idcs: HashSet<CheetahString> = hashset!(CheetahString::from("room1"));
        let strategy = AllocateMessageQueueByMachineRoom::new(consumer_idcs);
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer2");
        let mq_all = vec![
            MessageQueue::from_parts("topic", "room1@broker-a", 0),
            MessageQueue::from_parts("topic", "room1@broker-a", 1),
            MessageQueue::from_parts("topic", "room1@broker-a", 2),
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
    fn allocate_returns_empty_for_mismatching_rooms() {
        let consumer_idcs: HashSet<CheetahString> = hashset!(CheetahString::from("room1"));
        let strategy = AllocateMessageQueueByMachineRoom::new(consumer_idcs);
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("consumer2");
        let mq_all = vec![
            MessageQueue::from_parts("topic", "room2@broker-a", 0),
            MessageQueue::from_parts("topic", "room2@broker-a", 1),
            MessageQueue::from_parts("topic", "room2@broker-a", 2),
        ];
        let cid_all = vec![
            CheetahString::from("consumer1"),
            CheetahString::from("consumer2"),
            CheetahString::from("consumer3"),
        ];

        let result = strategy
            .allocate(&consumer_group, &current_cid, &mq_all, &cid_all)
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn get_name_returns_correct_name() {
        let strategy = AllocateMessageQueueByMachineRoom::new(Default::default());
        assert_eq!(strategy.get_name(), "MACHINE_ROOM");
    }

    fn create_consumer_id_list(size: usize) -> Vec<CheetahString> {
        (0..size).map(|i| format!("CID_PREFIX{}", i).into()).collect()
    }

    pub fn create_message_queue_list(size: usize) -> Vec<MessageQueue> {
        let mut message_queue_list = Vec::with_capacity(size);

        for i in 0..size {
            let mq = if i < size / 2 {
                MessageQueue::from_parts("topic", "room1@broker-a", i as i32)
            } else {
                MessageQueue::from_parts("topic", "room2@broker-b", i as i32)
            };
            message_queue_list.push(mq);
        }

        message_queue_list
    }
}
