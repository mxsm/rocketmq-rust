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

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::consumer_impl::message_request::MessageRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;

#[derive(Clone)]
pub struct PullRequest {
    pub consumer_group: CheetahString,
    pub message_queue: MessageQueue,
    pub process_queue: Arc<ProcessQueue>,
    pub next_offset: i64,
    pub previously_locked: bool,
}

impl PullRequest {
    pub fn new(
        consumer_group: CheetahString,
        message_queue: MessageQueue,
        process_queue: Arc<ProcessQueue>,
        next_offset: i64,
    ) -> Self {
        PullRequest {
            consumer_group,
            message_queue,
            process_queue,
            next_offset,
            previously_locked: false,
        }
    }

    pub fn is_previously_locked(&self) -> bool {
        self.previously_locked
    }

    pub fn set_previously_locked(&mut self, previously_locked: bool) {
        self.previously_locked = previously_locked;
    }

    pub fn get_consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.consumer_group = consumer_group;
    }

    pub fn get_message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    pub fn set_message_queue(&mut self, message_queue: MessageQueue) {
        self.message_queue = message_queue;
    }

    pub fn get_next_offset(&self) -> i64 {
        self.next_offset
    }

    pub fn set_next_offset(&mut self, next_offset: i64) {
        self.next_offset = next_offset;
    }

    pub fn get_process_queue(&self) -> &Arc<ProcessQueue> {
        &self.process_queue
    }

    pub fn set_process_queue(&mut self, process_queue: Arc<ProcessQueue>) {
        self.process_queue = process_queue;
    }
}

impl MessageRequest for PullRequest {
    fn get_message_request_mode(&self) -> MessageRequestMode {
        MessageRequestMode::Pull
    }
}

impl Hash for PullRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.consumer_group.hash(state);
        self.message_queue.hash(state);
    }
}

impl PartialEq for PullRequest {
    fn eq(&self, other: &Self) -> bool {
        self.consumer_group == other.consumer_group && self.message_queue == other.message_queue
    }
}

impl Eq for PullRequest {}

impl std::fmt::Display for PullRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PullRequest [consumer_group={}, message_queue={:?}, next_offset={}]",
            self.consumer_group, self.message_queue, self.next_offset
        )
    }
}
