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

pub struct ConsumeOrderlyContext {
    message_queue: MessageQueue,
    auto_commit: bool,
    suspend_current_queue_time_millis: i64,
}

impl ConsumeOrderlyContext {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self {
            message_queue,
            auto_commit: true,
            suspend_current_queue_time_millis: -1,
        }
    }

    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn set_auto_commit(&mut self, auto_commit: bool) {
        self.auto_commit = auto_commit;
    }

    pub fn get_message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    pub fn get_suspend_current_queue_time_millis(&self) -> i64 {
        self.suspend_current_queue_time_millis
    }

    pub fn set_suspend_current_queue_time_millis(&mut self, suspend_current_queue_time_millis: i64) {
        self.suspend_current_queue_time_millis = suspend_current_queue_time_millis;
    }
}
