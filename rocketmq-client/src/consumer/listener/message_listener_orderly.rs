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

use crate::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use crate::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;

/// A trait for processing messages in order.
///
/// This trait ensures messages from the same queue are consumed sequentially.
/// Implement this trait for your custom sequential message processing logic, or use
/// closures directly as they automatically implement this trait.
///
/// # Difference from MessageListenerConcurrently
///
/// - **Orderly**: Messages from the same queue are processed sequentially
/// - **Concurrently**: Messages from different queues can be processed in parallel
///
/// # Context Mutability
///
/// The `context` parameter is mutable (`&mut`) to allow consumers to:
/// - Control auto-commit behavior via `set_auto_commit()`
/// - Set queue suspension time via `set_suspend_current_queue_time_millis()`
///
/// # Performance
///
/// When using closures or custom implementations directly (without type erasure),
/// this trait provides zero-cost abstraction through:
/// - Compile-time monomorphization
/// - Full function inlining
/// - No dynamic dispatch overhead
///
/// # Example
///
/// ```rust
/// use rocketmq_client_rust::consumer::listener::ConsumeOrderlyContext;
/// use rocketmq_client_rust::consumer::listener::ConsumeOrderlyStatus;
/// use rocketmq_client_rust::consumer::listener::MessageListenerOrderly;
/// use rocketmq_common::common::message::message_ext::MessageExt;
///
/// // Closures automatically implement MessageListenerOrderly
/// let listener = |msgs: &[&MessageExt],
///                 context: &mut ConsumeOrderlyContext|
///  -> rocketmq_error::RocketMQResult<ConsumeOrderlyStatus> {
///     for msg in msgs {
///         println!("Processing message in order: {:?}", msg.msg_id());
///     }
///     // Control commit behavior
///     context.set_auto_commit(true);
///     Ok(ConsumeOrderlyStatus::Success)
/// };
///
/// // Struct-based implementation
/// struct MyOrderlyListener;
///
/// impl MessageListenerOrderly for MyOrderlyListener {
///     fn consume_message(
///         &self,
///         msgs: &[&MessageExt],
///         context: &mut ConsumeOrderlyContext,
///     ) -> rocketmq_error::RocketMQResult<ConsumeOrderlyStatus> {
///         // Process messages sequentially
///         context.set_auto_commit(false);
///         Ok(ConsumeOrderlyStatus::Success)
///     }
/// }
/// ```
pub trait MessageListenerOrderly: Send + Sync {
    /// Processes a batch of messages in order.
    ///
    /// # Arguments
    ///
    /// * `msgs` - Slice of message references. Zero-cost borrowing without cloning.
    /// * `context` - Mutable context for orderly consumption. Allows control over commit behavior
    ///   and queue suspension.
    ///
    /// # Returns
    ///
    /// * `Ok(ConsumeOrderlyStatus::Success)` - Messages processed successfully, commit offset
    /// * `Ok(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment)` - Suspend consumption temporarily,
    ///   retry later
    /// * `Ok(ConsumeOrderlyStatus::Rollback)` - Rollback transaction
    /// * `Ok(ConsumeOrderlyStatus::Commit)` - Commit transaction explicitly
    /// * `Err(error)` - Processing failed with an error
    ///
    /// # Notes
    ///
    /// - Messages from the same queue are NEVER processed concurrently
    /// - Messages from different queues MAY be processed in parallel
    /// - The context allows you to control auto-commit and suspension behavior
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> rocketmq_error::RocketMQResult<ConsumeOrderlyStatus>;
}

/// Implement MessageListenerOrderly for all compatible closures and functions.
///
/// This blanket implementation allows closures to be used directly as message listeners
/// without explicit trait implementation, providing a convenient and zero-cost API.
///
/// # Note on Mutability
///
/// The closure signature requires `&mut ConsumeOrderlyContext` to match the trait
/// definition. This allows the closure to modify context state (auto-commit, suspension).
impl<F> MessageListenerOrderly for F
where
    F: Fn(&[&MessageExt], &mut ConsumeOrderlyContext) -> rocketmq_error::RocketMQResult<ConsumeOrderlyStatus>
        + Send
        + Sync,
{
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> rocketmq_error::RocketMQResult<ConsumeOrderlyStatus> {
        self(msgs, context)
    }
}

/// Type-erased message listener for storage and cross-boundary passing.
///
/// This type uses dynamic dispatch (`Arc<dyn Trait>`) and is suitable for scenarios where:
/// - The listener needs to be stored in a struct field
/// - The listener crosses async boundaries
/// - The listener type cannot be determined at compile time
///
/// # Performance Note
///
/// This type incurs runtime overhead due to:
/// - Dynamic dispatch (~5-10ns per call)
/// - Arc reference counting
///
/// For best performance, prefer using generic parameters with the `MessageListenerOrderly`
/// trait in function signatures instead of this type alias.
///
/// # Example
///
/// ```rust
/// use rocketmq_client_rust::consumer::listener::ArcMessageListenerOrderly;
/// use rocketmq_client_rust::consumer::listener::ConsumeOrderlyStatus;
/// use rocketmq_client_rust::consumer::listener::MessageListenerOrderly;
/// use std::sync::Arc;
///
/// struct Consumer {
///     listener: Option<ArcMessageListenerOrderly>,
/// }
///
/// impl Consumer {
///     fn register_listener<L: MessageListenerOrderly + 'static>(&mut self, listener: L) {
///         self.listener = Some(Arc::new(listener));
///     }
/// }
/// ```
pub type ArcMessageListenerOrderly = Arc<dyn MessageListenerOrderly>;
