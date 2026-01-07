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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;

pub type MessageQueueSelectorFn =
    Arc<dyn Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync>;

/// A trait for selecting a message queue.
///
/// This trait defines a method for selecting a message queue from a list of available queues
/// based on the provided message and an additional argument.
pub trait MessageQueueSelector: Send + Sync {
    /// Selects a message queue from the provided list.
    ///
    /// # Arguments
    /// * `mqs` - A reference to a vector of `MessageQueue` from which to select.
    /// * `msg` - A reference to the `Message` for which the queue is being selected.
    /// * `arg` - An additional argument that can be used in the selection process.
    ///
    /// # Returns
    /// The selected `MessageQueue`.
    fn select(&self, mqs: &[MessageQueue], msg: &Message, arg: &dyn std::any::Any) -> MessageQueue;
}
