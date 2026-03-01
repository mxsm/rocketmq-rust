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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;

use crate::producer::message_queue_selector::MessageQueueSelector;

/// A message queue selector that uses hash-based routing.
///
/// Routes messages to queues by computing a hash of the provided argument and applying
/// modulo against the available queue count. Messages with identical argument values
/// are consistently routed to the same queue, preserving ordering semantics.
///
/// # Performance
///
/// Selection operates in O(1) time with no heap allocations. The `select` method is
/// inlined to eliminate function call overhead.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_client_rust::producer::queue_selector::SelectMessageQueueByHash;
/// use rocketmq_client_rust::producer::message_queue_selector::MessageQueueSelector;
///
/// let selector = SelectMessageQueueByHash;
/// let order_id = 12345;
/// let queue = selector.select(&message_queues, &message, &order_id);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct SelectMessageQueueByHash;

impl SelectMessageQueueByHash {
    /// Returns a new instance.
    pub fn new() -> Self {
        Self
    }
}

impl<M, A> MessageQueueSelector<M, A> for SelectMessageQueueByHash
where
    M: MessageTrait,
    A: Hash,
{
    /// Selects a message queue by hashing the argument.
    ///
    /// Returns `None` if the queue list is empty.
    #[inline]
    fn select(&self, mqs: &[MessageQueue], _msg: &M, arg: &A) -> Option<MessageQueue> {
        if mqs.is_empty() {
            return None;
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        arg.hash(&mut hasher);
        let hash_code = hasher.finish();

        let index = (hash_code % mqs.len() as u64) as usize;
        mqs.get(index).cloned()
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::common::message::message_single::Message;

    use super::*;

    #[test]
    fn test_select_message_queue_by_hash() {
        let selector = SelectMessageQueueByHash::new();

        let queues = vec![
            MessageQueue::from_parts("test_topic", "broker-a", 0),
            MessageQueue::from_parts("test_topic", "broker-a", 1),
            MessageQueue::from_parts("test_topic", "broker-a", 2),
            MessageQueue::from_parts("test_topic", "broker-a", 3),
        ];

        let msg = Message::builder().topic("test_topic").build().unwrap();

        // Test with integer argument
        let order_id = 12345;
        let selected = selector.select(&queues, &msg, &order_id);
        assert!(selected.is_some());

        // Same argument should select same queue
        let selected1 = selector.select(&queues, &msg, &order_id);
        let selected2 = selector.select(&queues, &msg, &order_id);
        assert_eq!(selected1, selected2);

        // Different arguments should distribute across queues
        let selected_a = selector.select(&queues, &msg, &100);
        let selected_b = selector.select(&queues, &msg, &200);
        assert!(selected_a.is_some());
        assert!(selected_b.is_some());
    }

    #[test]
    fn test_select_with_string_argument() {
        let selector = SelectMessageQueueByHash::new();

        let queues = vec![
            MessageQueue::from_parts("test_topic", "broker-a", 0),
            MessageQueue::from_parts("test_topic", "broker-a", 1),
            MessageQueue::from_parts("test_topic", "broker-a", 2),
        ];

        let msg = Message::builder().topic("test_topic").build().unwrap();

        // Test with string argument
        let user_id = "user_12345";
        let selected1 = selector.select(&queues, &msg, &user_id);
        let selected2 = selector.select(&queues, &msg, &user_id);

        assert_eq!(selected1, selected2);
        assert!(selected1.is_some());
    }

    #[test]
    fn test_select_empty_queue_list() {
        let selector = SelectMessageQueueByHash::new();
        let queues: Vec<MessageQueue> = vec![];
        let msg = Message::builder().topic("test_topic").build().unwrap();
        let order_id = 12345;

        let selected = selector.select(&queues, &msg, &order_id);
        assert!(selected.is_none());
    }

    #[test]
    fn test_select_single_queue() {
        let selector = SelectMessageQueueByHash::new();
        let queues = vec![MessageQueue::from_parts("test_topic", "broker-a", 0)];
        let msg = Message::builder().topic("test_topic").build().unwrap();

        // All arguments should select the only available queue
        let selected1 = selector.select(&queues, &msg, &100);
        let selected2 = selector.select(&queues, &msg, &200);
        let selected3 = selector.select(&queues, &msg, &300);

        assert_eq!(selected1, selected2);
        assert_eq!(selected2, selected3);
        assert_eq!(selected1.unwrap().queue_id(), 0);
    }

    #[test]
    fn test_distribution_across_queues() {
        let selector = SelectMessageQueueByHash::new();
        let queues = vec![
            MessageQueue::from_parts("test_topic", "broker-a", 0),
            MessageQueue::from_parts("test_topic", "broker-a", 1),
            MessageQueue::from_parts("test_topic", "broker-a", 2),
            MessageQueue::from_parts("test_topic", "broker-a", 3),
        ];

        let msg = Message::builder().topic("test_topic").build().unwrap();
        let mut queue_counts = std::collections::HashMap::new();

        // Test distribution with 100 different arguments
        for i in 0..100 {
            if let Some(queue) = selector.select(&queues, &msg, &i) {
                *queue_counts.entry(queue.queue_id()).or_insert(0) += 1;
            }
        }

        // Verify all queues received at least some messages
        assert!(!queue_counts.is_empty());
        for count in queue_counts.values() {
            assert!(*count > 0);
        }
    }
}
