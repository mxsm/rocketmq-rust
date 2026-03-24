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

use rand::RngExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;

use crate::producer::message_queue_selector::MessageQueueSelector;

/// A message queue selector that uses random selection.
///
/// Each invocation randomly selects a queue from the available list using uniform
/// distribution. This provides load balancing without ordering guarantees.
///
/// # Performance
///
/// Selection operates in O(1) time using thread-local RNG. The `select` method is
/// inlined to eliminate function call overhead.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_client_rust::producer::queue_selector::SelectMessageQueueByRandom;
/// use rocketmq_client_rust::producer::message_queue_selector::MessageQueueSelector;
///
/// let selector = SelectMessageQueueByRandom;
/// let queue = selector.select(&message_queues, &message, &());
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct SelectMessageQueueByRandom;

impl SelectMessageQueueByRandom {
    /// Returns a new instance.
    pub fn new() -> Self {
        Self
    }
}

impl<M, A> MessageQueueSelector<M, A> for SelectMessageQueueByRandom
where
    M: MessageTrait,
{
    /// Selects a message queue randomly.
    ///
    /// Returns `None` if the queue list is empty.
    #[inline]
    fn select(&self, mqs: &[MessageQueue], _msg: &M, _arg: &A) -> Option<MessageQueue> {
        if mqs.is_empty() {
            return None;
        }

        let mut rng = rand::rng();
        let index = rng.random_range(0..mqs.len());
        mqs.get(index).cloned()
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::common::message::message_single::Message;

    use super::*;

    #[test]
    fn test_select_message_queue_by_random() {
        let selector = SelectMessageQueueByRandom::new();

        let queues = vec![
            MessageQueue::from_parts("test_topic", "broker-a", 0),
            MessageQueue::from_parts("test_topic", "broker-a", 1),
            MessageQueue::from_parts("test_topic", "broker-a", 2),
            MessageQueue::from_parts("test_topic", "broker-a", 3),
        ];

        let msg = Message::builder().topic("test_topic").build().unwrap();

        let selected = selector.select(&queues, &msg, &());
        assert!(selected.is_some());
        assert!(queues.contains(&selected.unwrap()));
    }

    #[test]
    fn test_select_empty_queue_list() {
        let selector = SelectMessageQueueByRandom::new();
        let queues: Vec<MessageQueue> = vec![];
        let msg = Message::builder().topic("test_topic").build().unwrap();

        let selected = selector.select(&queues, &msg, &());
        assert!(selected.is_none());
    }

    #[test]
    fn test_select_single_queue() {
        let selector = SelectMessageQueueByRandom::new();
        let queues = vec![MessageQueue::from_parts("test_topic", "broker-a", 0)];
        let msg = Message::builder().topic("test_topic").build().unwrap();

        let selected = selector.select(&queues, &msg, &());
        assert_eq!(selected.unwrap().queue_id(), 0);
    }

    #[test]
    fn test_random_distribution() {
        let selector = SelectMessageQueueByRandom::new();
        let queues = vec![
            MessageQueue::from_parts("test_topic", "broker-a", 0),
            MessageQueue::from_parts("test_topic", "broker-a", 1),
            MessageQueue::from_parts("test_topic", "broker-a", 2),
            MessageQueue::from_parts("test_topic", "broker-a", 3),
        ];

        let msg = Message::builder().topic("test_topic").build().unwrap();
        let mut queue_counts = std::collections::HashMap::new();

        for _ in 0..1000 {
            if let Some(queue) = selector.select(&queues, &msg, &()) {
                *queue_counts.entry(queue.queue_id()).or_insert(0) += 1;
            }
        }

        assert_eq!(queue_counts.len(), 4);
        for &count in queue_counts.values() {
            assert!(count > 100);
            assert!(count < 400);
        }
    }

    #[test]
    fn test_randomness() {
        let selector = SelectMessageQueueByRandom::new();
        let queues = vec![
            MessageQueue::from_parts("test_topic", "broker-a", 0),
            MessageQueue::from_parts("test_topic", "broker-a", 1),
            MessageQueue::from_parts("test_topic", "broker-a", 2),
            MessageQueue::from_parts("test_topic", "broker-a", 3),
        ];

        let msg = Message::builder().topic("test_topic").build().unwrap();

        let mut selections = Vec::new();
        for _ in 0..10 {
            if let Some(queue) = selector.select(&queues, &msg, &()) {
                selections.push(queue.queue_id());
            }
        }

        let unique_selections: std::collections::HashSet<_> = selections.iter().collect();
        assert!(unique_selections.len() > 1);
    }
}
