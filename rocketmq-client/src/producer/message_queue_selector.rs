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

use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageTrait;

/// A trait for selecting a message queue from a list of available queues.
///
/// This trait provides a zero-cost abstraction for queue selection logic using compile-time
/// monomorphization. Implement this trait for your custom selector logic, or use closures
/// directly as they automatically implement this trait.
///
/// # Type Parameters
///
/// * `M` - Message type that implements `MessageTrait`
/// * `A` - Argument type for custom selection logic
///
/// # Performance
///
/// This trait uses compile-time generics, allowing the compiler to:
/// - Fully inline the selector function
/// - Eliminate dynamic dispatch overhead
/// - Perform aggressive optimizations
///
/// # Example
///
/// ```
/// use rocketmq_client_rust::MessageQueueSelector;
/// use rocketmq_model::common::message::message_queue::MessageQueue;
/// use rocketmq_model::common::message::message_single::Message;
///
/// // Closures automatically implement MessageQueueSelector. Order IDs are signed, so guard
/// // against an empty queue list before indexing and use `rem_euclid` so a negative order ID
/// // still maps into the valid queue range instead of producing a negative remainder.
/// let selector = |mqs: &[MessageQueue], _msg: &Message, order_id: &i64| {
///     if mqs.is_empty() {
///         return None;
///     }
///     let index = order_id.rem_euclid(mqs.len() as i64) as usize;
///     mqs.get(index).cloned()
/// };
///
/// let msg = Message::builder()
///     .topic("TopicTest")
///     .body_slice(b"body")
///     .build_unchecked();
///
/// assert_eq!(selector.select(&[], &msg, &7), None);
///
/// let mqs = vec![
///     MessageQueue::from_parts("TopicTest", "broker-a", 0),
///     MessageQueue::from_parts("TopicTest", "broker-a", 1),
///     MessageQueue::from_parts("TopicTest", "broker-a", 2),
/// ];
/// assert_eq!(selector.select(&mqs, &msg, &1), Some(mqs[1].clone()));
/// assert_eq!(selector.select(&mqs, &msg, &-1), Some(mqs[2].clone()));
///
/// let first = selector.select(&mqs, &msg, &i64::MIN);
/// let second = selector.select(&mqs, &msg, &i64::MIN);
/// assert_eq!(first, second);
/// assert!(first.is_some_and(|mq| mqs.contains(&mq)));
/// ```
pub trait MessageQueueSelector<M: MessageTrait, A>: Send + Sync {
    /// Selects a message queue from the provided list.
    ///
    /// # Arguments
    ///
    /// * `mqs` - Available message queues to select from
    /// * `msg` - The message to be sent
    /// * `arg` - Custom argument for selection logic
    ///
    /// # Returns
    ///
    /// Selected `MessageQueue`, or `None` if no suitable queue is found
    fn select(&self, mqs: &[MessageQueue], msg: &M, arg: &A) -> Option<MessageQueue>;
}

/// Implement MessageQueueSelector for all compatible closures and functions.
///
/// This allows closures to be used directly as selectors without explicit trait implementation.
impl<F, M, A> MessageQueueSelector<M, A> for F
where
    F: Fn(&[MessageQueue], &M, &A) -> Option<MessageQueue> + Send + Sync,
    M: MessageTrait,
{
    fn select(&self, mqs: &[MessageQueue], msg: &M, arg: &A) -> Option<MessageQueue> {
        self(mqs, msg, arg)
    }
}

/// Type-erased message queue selector function for storage and cross-boundary passing.
///
/// This type uses dynamic dispatch (`Arc<dyn Fn>`) and is suitable for scenarios where:
/// - The selector needs to be stored in a struct field
/// - The selector crosses async boundaries
/// - The selector type cannot be determined at compile time
///
/// # Performance Note
///
/// This type incurs runtime overhead due to:
/// - Dynamic dispatch (~5-10ns per call)
/// - Arc reference counting
/// - Type erasure with `dyn Any`
///
/// For best performance, prefer using generic parameters with the `MessageQueueSelector` trait
/// in function signatures instead of this type alias.
///
/// # Example
///
/// ```
/// use rocketmq_client_rust::MessageQueueSelectorFn;
/// use rocketmq_model::common::message::message_queue::MessageQueue;
/// use rocketmq_model::common::message::message_single::Message;
/// use std::sync::Arc;
///
/// let selector: MessageQueueSelectorFn = Arc::new(|mqs: &[MessageQueue], _msg, arg| {
///     if mqs.is_empty() {
///         return None;
///     }
///     let order_id = arg.downcast_ref::<i64>()?;
///     let index = order_id.rem_euclid(mqs.len() as i64) as usize;
///     mqs.get(index).cloned()
/// });
///
/// let msg = Message::builder()
///     .topic("TopicTest")
///     .body_slice(b"body")
///     .build_unchecked();
///
/// assert_eq!(selector(&[], &msg, &7i64), None);
///
/// let mqs = vec![
///     MessageQueue::from_parts("TopicTest", "broker-a", 0),
///     MessageQueue::from_parts("TopicTest", "broker-a", 1),
/// ];
/// assert_eq!(selector(&mqs, &msg, &1i64), Some(mqs[1].clone()));
///
/// // An argument that cannot downcast to `i64` still returns `None`.
/// assert_eq!(selector(&mqs, &msg, &"not-an-i64"), None);
/// ```
pub type MessageQueueSelectorFn =
    Arc<dyn Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync>;
