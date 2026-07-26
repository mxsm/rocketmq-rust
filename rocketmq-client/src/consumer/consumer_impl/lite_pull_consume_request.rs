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

use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageTrait;

use crate::consumer::consumer_impl::process_queue::ProcessQueue;

/// Request to consume messages from a specific queue.
///
/// This structure is passed from pull tasks to the poll API through a
/// resource-budgeted queue.
#[derive(Clone)]
pub struct LitePullConsumeRequest {
    /// Messages to be consumed.
    pub(crate) messages: Vec<Arc<MessageExt>>,

    /// The message queue these messages belong to.
    pub(crate) message_queue: MessageQueue,

    /// The process queue managing this queue's state.
    pub(crate) process_queue: Arc<ProcessQueue>,
}

impl LitePullConsumeRequest {
    /// Creates a new consume request.
    pub fn new(messages: Vec<Arc<MessageExt>>, message_queue: MessageQueue, process_queue: Arc<ProcessQueue>) -> Self {
        Self {
            messages,
            message_queue,
            process_queue,
        }
    }

    /// Returns the messages in this request.
    pub fn messages(&self) -> &[Arc<MessageExt>] {
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
    pub fn into_parts(self) -> (Vec<Arc<MessageExt>>, MessageQueue, Arc<ProcessQueue>) {
        (self.messages, self.message_queue, self.process_queue)
    }

    /// Estimated bytes retained while this request waits in the poll queue.
    pub fn retained_bytes(&self) -> usize {
        let message_bytes = self.messages.iter().fold(0usize, |total, message| {
            let properties = message.properties();
            let property_bytes = properties
                .capacity()
                .saturating_mul(std::mem::size_of::<(
                    cheetah_string::CheetahString,
                    cheetah_string::CheetahString,
                )>())
                .saturating_add(
                    properties
                        .iter()
                        .map(|(key, value)| key.len().saturating_add(value.len()))
                        .sum::<usize>(),
                );
            total
                .saturating_add(std::mem::size_of::<MessageExt>())
                .saturating_add(message.get_body().map_or(0, |body| body.len()))
                .saturating_add(message.topic().len())
                .saturating_add(message.broker_name().len())
                .saturating_add(message.msg_id().len())
                .saturating_add(message.message_inner().transaction_id().map_or(0, str::len))
                .saturating_add(property_bytes)
        });
        std::mem::size_of::<Self>()
            .saturating_add(
                self.messages
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Arc<MessageExt>>()),
            )
            .saturating_add(self.message_queue.topic().len())
            .saturating_add(self.message_queue.broker_name().len())
            .saturating_add(message_bytes)
    }
}
