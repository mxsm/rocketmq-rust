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

use crate::hook::consume_message_context::ConsumeMessageContext;

/// Type alias for a thread-safe hook reference.
pub type ConsumeMessageHookArc = Arc<dyn ConsumeMessageHook>;

/// Hook for message consumption lifecycle events.
///
/// Implementations are invoked synchronously before and after each batch of messages
/// is consumed by a push consumer. Multiple hooks may be registered and are executed
/// in registration order.
///
/// # Examples
///
/// ```ignore
/// use rocketmq_client::hook::consume_message_hook::ConsumeMessageHook;
/// use rocketmq_client::hook::consume_message_context::ConsumeMessageContext;
///
/// struct LoggingHook;
///
/// impl ConsumeMessageHook for LoggingHook {
///     fn hook_name(&self) -> &'static str {
///         "LoggingHook"
///     }
///
///     fn consume_message_before(&self, context: &ConsumeMessageContext) {
///         println!("Before consuming {} messages from {}",
///                  context.msg_list.len(), context.consumer_group);
///     }
///
///     fn consume_message_after(&self, context: &ConsumeMessageContext) {
///         println!("After consuming, success: {}", context.success);
///     }
/// }
/// ```
pub trait ConsumeMessageHook: Send + Sync {
    /// Returns the unique name of this hook.
    fn hook_name(&self) -> &'static str;

    /// Invoked before message consumption begins.
    fn consume_message_before(&self, context: &ConsumeMessageContext);

    /// Invoked after message consumption completes.
    ///
    /// The context reflects the final consumption status, including whether
    /// the messages were consumed successfully.
    fn consume_message_after(&self, context: &ConsumeMessageContext);
}
