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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;

use crate::producer::message_queue_selector::MessageQueueSelector;

#[derive(Debug, Clone, Default)]
pub struct SelectMessageQueueByMachineRoom {
    consumer_idcs: HashSet<CheetahString>,
}

impl SelectMessageQueueByMachineRoom {
    pub fn new(consumer_idcs: HashSet<CheetahString>) -> Self {
        Self { consumer_idcs }
    }

    #[inline]
    pub fn consumer_idcs(&self) -> &HashSet<CheetahString> {
        &self.consumer_idcs
    }

    #[inline]
    pub fn set_consumer_idcs(&mut self, consumer_idcs: HashSet<CheetahString>) {
        self.consumer_idcs = consumer_idcs;
    }

    fn queue_matches_machine_room(&self, mq: &MessageQueue) -> bool {
        let Some((machine_room, _broker)) = mq.broker_name().split_once('@') else {
            return false;
        };
        self.consumer_idcs.contains(machine_room)
    }
}

impl<M, A> MessageQueueSelector<M, A> for SelectMessageQueueByMachineRoom
where
    M: MessageTrait,
{
    fn select(&self, mqs: &[MessageQueue], _msg: &M, _arg: &A) -> Option<MessageQueue> {
        if self.consumer_idcs.is_empty() {
            return None;
        }
        mqs.iter().find(|mq| self.queue_matches_machine_room(mq)).cloned()
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::message::message_single::Message;
    use rocketmq_common::hashset;

    use super::*;

    #[test]
    fn select_message_queue_by_machine_room_matches_room_prefix() {
        let selector = SelectMessageQueueByMachineRoom::new(hashset!(CheetahString::from("room-a")));
        let queues = vec![
            MessageQueue::from_parts("topic", "room-b@broker", 0),
            MessageQueue::from_parts("topic", "room-a@broker", 1),
        ];
        let msg = Message::builder().topic("topic").build().unwrap();

        let selected = selector.select(&queues, &msg, &());

        assert_eq!(selected, Some(queues[1].clone()));
    }

    #[test]
    fn select_message_queue_by_machine_room_returns_none_without_match() {
        let selector = SelectMessageQueueByMachineRoom::new(hashset!(CheetahString::from("room-a")));
        let queues = vec![MessageQueue::from_parts("topic", "broker", 0)];
        let msg = Message::builder().topic("topic").build().unwrap();

        assert_eq!(selector.select(&queues, &msg, &()), None);
    }
}
