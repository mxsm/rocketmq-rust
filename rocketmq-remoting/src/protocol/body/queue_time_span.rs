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
use rocketmq_common::UtilAll::time_millis_to_human_string2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueueTimeSpan {
    pub message_queue: Option<MessageQueue>,
    pub min_time_stamp: i64,
    pub max_time_stamp: i64,
    pub consume_time_stamp: i64,
    pub delay_time: i64,
}

impl QueueTimeSpan {
    pub fn get_message_queue(&self) -> Option<MessageQueue> {
        self.message_queue.clone()
    }

    pub fn set_message_queue(&mut self, message_queue: MessageQueue) {
        self.message_queue = Some(message_queue);
    }

    pub fn get_min_time_stamp(&self) -> i64 {
        self.min_time_stamp
    }

    pub fn set_min_time_stamp(&mut self, min_time_stamp: i64) {
        self.min_time_stamp = min_time_stamp;
    }

    pub fn get_max_time_stamp(&self) -> i64 {
        self.max_time_stamp
    }

    pub fn set_max_time_stamp(&mut self, max_time_stamp: i64) {
        self.max_time_stamp = max_time_stamp;
    }

    pub fn get_consume_time_stamp(&self) -> i64 {
        self.consume_time_stamp
    }

    pub fn set_consume_time_stamp(&mut self, consume_time_stamp: i64) {
        self.consume_time_stamp = consume_time_stamp;
    }

    pub fn get_delay_time(&self) -> i64 {
        self.delay_time
    }

    pub fn set_delay_time(&mut self, delay_time: i64) {
        self.delay_time = delay_time;
    }

    pub fn get_min_time_stamp_str(&self) -> String {
        time_millis_to_human_string2(self.min_time_stamp)
    }

    pub fn get_max_time_stamp_str(&self) -> String {
        time_millis_to_human_string2(self.max_time_stamp)
    }

    pub fn get_consume_time_stamp_str(&self) -> String {
        time_millis_to_human_string2(self.consume_time_stamp)
    }
}
