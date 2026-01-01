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

/// A trait for filtering message queues.
///
/// This trait defines a method for filtering message queues based on custom criteria.
pub trait QueueFilter: Send + Sync + 'static {
    /// Filters a message queue.
    ///
    /// This method determines whether the specified message queue meets the filter criteria.
    ///
    /// # Arguments
    /// * `mq` - A reference to the `MessageQueue` to be filtered.
    ///
    /// # Returns
    /// A boolean value indicating whether the message queue meets the filter criteria.
    fn filter(&self, mq: &MessageQueue) -> bool;
}

/// Implements the `QueueFilter` trait for any type `F` that implements the `Fn(&MessageQueue) ->
/// bool` function signature, and also satisfies the `Send`, `Sync`, and `'static` bounds.
///
/// This allows any closure or function that matches the required signature to be used as a
/// `QueueFilter`.
impl<F> QueueFilter for F
where
    F: Fn(&MessageQueue) -> bool + Send + Sync + 'static,
{
    /// Filters a message queue using the provided closure or function.
    ///
    /// # Arguments
    /// * `mq` - A reference to the `MessageQueue` to be filtered.
    ///
    /// # Returns
    /// A boolean value indicating whether the message queue meets the filter criteria.
    fn filter(&self, mq: &MessageQueue) -> bool {
        self(mq)
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::*;

    #[test]
    fn queue_filter_closure_returns_true() {
        let filter = |mq: &MessageQueue| mq.get_topic() == "test_topic";
        let mq = MessageQueue::from_parts("test_topic", "broker", 1);
        assert!(filter.filter(&mq));
    }

    #[test]
    fn queue_filter_closure_returns_false() {
        let filter = |mq: &MessageQueue| mq.get_topic() == "test_topic";
        let mq = MessageQueue::from_parts("other_topic", "broker", 1);
        assert!(!filter.filter(&mq));
    }

    #[test]
    fn queue_filter_function_returns_true() {
        fn filter_fn(mq: &MessageQueue) -> bool {
            mq.get_queue_id() == 1
        }
        let mq = MessageQueue::from_parts("test_topic", "broker", 1);
        assert!(filter_fn.filter(&mq));
    }

    #[test]
    fn queue_filter_function_returns_false() {
        fn filter_fn(mq: &MessageQueue) -> bool {
            mq.get_queue_id() == 1
        }
        let mq = MessageQueue::from_parts("test_topic", "broker", 2);
        assert!(!filter_fn.filter(&mq));
    }
}
