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

use rocketmq_common::common::message::message_queue::MessageQueue;

pub struct ConsumeConcurrentlyContext {
    pub(crate) message_queue: MessageQueue,
    pub(crate) delay_level_when_next_consume: i32,
    pub(crate) ack_index: i32,
}

impl ConsumeConcurrentlyContext {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self {
            message_queue,
            delay_level_when_next_consume: 0,
            ack_index: i32::MAX,
        }
    }

    pub fn get_delay_level_when_next_consume(&self) -> i32 {
        self.delay_level_when_next_consume
    }

    pub fn set_delay_level_when_next_consume(&mut self, delay_level_when_next_consume: i32) {
        self.delay_level_when_next_consume = delay_level_when_next_consume;
    }

    pub fn get_message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    pub fn get_ack_index(&self) -> i32 {
        self.ack_index
    }

    pub fn set_ack_index(&mut self, ack_index: i32) {
        self.ack_index = ack_index;
    }
}
