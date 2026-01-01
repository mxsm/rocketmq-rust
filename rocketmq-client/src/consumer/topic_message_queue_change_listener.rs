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

use rocketmq_common::common::message::message_queue::MessageQueue;

/// A trait for listening to changes in the message queues of a specific topic.
pub trait TopicMessageQueueChangeListener: Send + Sync {
    /// Called when the message queues for a topic have changed.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic whose message queues have changed.
    /// * `message_queues` - A set of `MessageQueue` instances representing the new state of the
    ///   message queues.
    fn on_changed(&self, topic: &str, message_queues: HashSet<MessageQueue>);
}
