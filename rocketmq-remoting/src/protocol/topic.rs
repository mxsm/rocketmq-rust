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
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
pub struct OffsetMovedEvent {
    pub consumer_group: String,
    pub message_queue: MessageQueue,
    pub offset_request: i64,
    pub offset_new: i64,
}

impl OffsetMovedEvent {
    pub fn get_consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub fn set_consumer_group(&mut self, consumer_group: String) {
        self.consumer_group = consumer_group;
    }

    pub fn get_message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    pub fn set_message_queue(&mut self, message_queue: MessageQueue) {
        self.message_queue = message_queue;
    }

    pub fn get_offset_request(&self) -> i64 {
        self.offset_request
    }

    pub fn set_offset_request(&mut self, offset_request: i64) {
        self.offset_request = offset_request;
    }

    pub fn get_offset_new(&self) -> i64 {
        self.offset_new
    }

    pub fn set_offset_new(&mut self, offset_new: i64) {
        self.offset_new = offset_new;
    }
}

impl std::fmt::Display for OffsetMovedEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OffsetMovedEvent [consumer_group={}, message_queue={:?}, offset_request={}, offset_new={}]",
            self.consumer_group, self.message_queue, self.offset_request, self.offset_new
        )
    }
}
