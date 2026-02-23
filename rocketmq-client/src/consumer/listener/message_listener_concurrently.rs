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

use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;

/// A trait for processing messages concurrently.
///
/// This trait provides a zero-cost abstraction for concurrent message consumption.
/// Implement this trait for your custom message processing logic, or use closures
/// directly as they automatically implement this trait.
///
/// # Type Parameters
///
/// Note: This trait does not use generic type parameters. The message type is
/// fixed to `MessageExt` and the context to `ConsumeConcurrentlyContext` for
/// compatibility with RocketMQ protocol.
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
/// use rocketmq_client_rust::consumer::listener::ConsumeConcurrentlyContext;
/// use rocketmq_client_rust::consumer::listener::ConsumeConcurrentlyStatus;
/// use rocketmq_client_rust::consumer::listener::MessageListenerConcurrently;
/// use rocketmq_common::common::message::message_ext::MessageExt;
///
/// // Closures automatically implement MessageListenerConcurrently
/// let listener = |msgs: &[&MessageExt],
///                 _context: &ConsumeConcurrentlyContext|
///  -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus> {
///     for msg in msgs {
///         println!("Processing message: {:?}", msg.msg_id());
///     }
///     Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
/// };
///
/// // Struct-based implementation
/// struct MyListener;
///
/// impl MessageListenerConcurrently for MyListener {
///     fn consume_message(
///         &self,
///         msgs: &[&MessageExt],
///         context: &ConsumeConcurrentlyContext,
///     ) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus> {
///         // Process messages
///         Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
///     }
/// }
/// ```
pub trait MessageListenerConcurrently: Send + Sync {
    /// Processes a batch of messages concurrently.
    ///
    /// # Arguments
    ///
    /// * `msgs` - Slice of message references. Zero-cost borrowing without cloning.
    /// * `context` - Context for concurrent consumption containing metadata like message queue,
    ///   delay level, etc.
    ///
    /// # Returns
    ///
    /// * `Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)` - Messages processed successfully
    /// * `Ok(ConsumeConcurrentlyStatus::ReconsumeLater)` - Messages should be retried later
    /// * `Err(error)` - Processing failed with an error
    ///
    /// # Notes
    ///
    /// - This method may be called concurrently for different message batches
    /// - Messages from the same queue are never processed concurrently
    /// - The default batch size is 1, configurable via consumer settings
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        context: &ConsumeConcurrentlyContext,
    ) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus>;
}

/// Implement MessageListenerConcurrently for all compatible closures and functions.
///
/// This blanket implementation allows closures to be used directly as message listeners
/// without explicit trait implementation, providing a convenient and zero-cost API.
impl<F> MessageListenerConcurrently for F
where
    F: Fn(&[&MessageExt], &ConsumeConcurrentlyContext) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus>
        + Send
        + Sync,
{
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        context: &ConsumeConcurrentlyContext,
    ) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus> {
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
/// For best performance, prefer using generic parameters with the `MessageListenerConcurrently`
/// trait in function signatures instead of this type alias.
///
/// # Example
///
/// ```rust
/// use rocketmq_client_rust::consumer::listener::ArcMessageListenerConcurrently;
/// use rocketmq_client_rust::consumer::listener::ConsumeConcurrentlyStatus;
/// use rocketmq_client_rust::consumer::listener::MessageListenerConcurrently;
/// use std::sync::Arc;
///
/// struct Consumer {
///     listener: Option<ArcMessageListenerConcurrently>,
/// }
///
/// impl Consumer {
///     fn register_listener<L: MessageListenerConcurrently + 'static>(&mut self, listener: L) {
///         self.listener = Some(Arc::new(listener));
///     }
/// }
/// ```
pub type ArcMessageListenerConcurrently = Arc<dyn MessageListenerConcurrently>;
