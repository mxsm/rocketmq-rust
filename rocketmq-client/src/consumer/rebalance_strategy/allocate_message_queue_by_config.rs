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

pub struct AllocateMessageQueueByConfig {
    message_queue_list: Vec<MessageQueue>,
}

impl AllocateMessageQueueByConfig {
    #[inline]
    pub fn new(message_queue_list: Vec<MessageQueue>) -> Self {
        Self { message_queue_list }
    }
}

impl AllocateMessageQueueStrategy for AllocateMessageQueueByConfig {
    fn allocate(
        &self,
        consumer_group: &CheetahString,
        current_cid: &CheetahString,
        mq_all: &[MessageQueue],
        cid_all: &[CheetahString],
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        Ok(self.message_queue_list.clone())
    }

    #[inline]
    fn get_name(&self) -> &'static str {
        "CONFIG"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::*;

    #[test]
    fn test_allocate_message_queue_by_config() {
        let consumer_group = CheetahString::from("test_group");
        let current_cid = CheetahString::from("CID_PREFIX1");
        let mq_all = create_message_queue_list(4);
        let cid_all = create_consumer_id_list(2);
        let strategy = AllocateMessageQueueByConfig::new(mq_all.clone());

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
            &[0, 1, 2, 3]
        );
        assert_eq!(
            consumer_allocate_queue.get("CID_PREFIX1").unwrap().as_slice(),
            &[0, 1, 2, 3]
        );
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
