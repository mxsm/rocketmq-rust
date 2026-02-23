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
use rocketmq_common::common::message::MessageTrait;

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
/// ```no_run
/// use rocketmq_client_rust::producer::message_queue_selector::MessageQueueSelector;
/// use rocketmq_common::common::message::message_queue::MessageQueue;
/// use rocketmq_common::common::message::message_single::Message;
///
/// // Closures automatically implement MessageQueueSelector
/// let selector = |mqs: &[MessageQueue], _msg: &Message, order_id: &i64| {
///     let index = (*order_id % mqs.len() as i64) as usize;
///     mqs.get(index).cloned()
/// };
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
/// ```no_run
/// use rocketmq_client_rust::producer::message_queue_selector::MessageQueueSelectorFn;
/// use std::sync::Arc;
///
/// let selector: MessageQueueSelectorFn = Arc::new(|mqs, _msg, arg| {
///     let order_id = arg.downcast_ref::<i64>()?;
///     let index = (*order_id % mqs.len() as i64) as usize;
///     mqs.get(index).cloned()
/// });
/// ```
pub type MessageQueueSelectorFn =
    Arc<dyn Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync>;
