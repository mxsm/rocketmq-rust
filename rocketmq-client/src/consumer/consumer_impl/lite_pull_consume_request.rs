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

use std::sync::Arc;

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_rust::ArcMut;

use crate::consumer::consumer_impl::process_queue::ProcessQueue;

/// Request to consume messages from a specific queue.
///
/// This structure is passed from pull tasks to the poll() API via an unbounded channel.
#[derive(Clone)]
pub struct LitePullConsumeRequest {
    /// Messages to be consumed.
    pub(crate) messages: Vec<ArcMut<MessageExt>>,

    /// The message queue these messages belong to.
    pub(crate) message_queue: MessageQueue,

    /// The process queue managing this queue's state.
    pub(crate) process_queue: Arc<ProcessQueue>,
}

impl LitePullConsumeRequest {
    /// Creates a new consume request.
    pub fn new(
        messages: Vec<ArcMut<MessageExt>>,
        message_queue: MessageQueue,
        process_queue: Arc<ProcessQueue>,
    ) -> Self {
        Self {
            messages,
            message_queue,
            process_queue,
        }
    }

    /// Returns the messages in this request.
    pub fn messages(&self) -> &[ArcMut<MessageExt>] {
        &self.messages
    }

    /// Returns the message queue for this request.
    pub fn message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    /// Returns the process queue for this request.
    pub fn process_queue(&self) -> &Arc<ProcessQueue> {
        &self.process_queue
    }

    /// Consumes the request and returns its components.
    pub fn into_parts(self) -> (Vec<ArcMut<MessageExt>>, MessageQueue, Arc<ProcessQueue>) {
        (self.messages, self.message_queue, self.process_queue)
    }
}
