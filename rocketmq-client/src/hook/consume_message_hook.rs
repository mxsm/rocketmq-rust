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

/// A shared, thread-safe reference to a [`ConsumeMessageHook`] implementation.
pub type ConsumeMessageHookArc = Arc<dyn ConsumeMessageHook + Send + Sync>;

/// Defines lifecycle callbacks invoked around the message consumption process.
///
/// Implementations are registered with a push consumer and called synchronously on the
/// consumer's processing thread before and after each batch of messages is consumed.
/// Multiple hooks may be registered; they are invoked in registration order.
///
/// Both callbacks receive an optional mutable reference to a [`ConsumeMessageContext`]
/// that carries metadata about the current consumption attempt, including the consumer
/// group, topic, message list, and the final consumption result.
pub trait ConsumeMessageHook {
    /// Returns the name that uniquely identifies this hook implementation.
    fn hook_name(&self) -> &str;

    /// Invoked immediately before message consumption begins.
    ///
    /// # Arguments
    ///
    /// * `context` - Mutable reference to the consumption context, or `None` if no context is
    ///   available for the current invocation.
    fn consume_message_before(&self, context: Option<&mut ConsumeMessageContext>);

    /// Invoked immediately after message consumption completes.
    ///
    /// The [`ConsumeMessageContext`] reflects the final consumption status at this point,
    /// including whether the messages were consumed successfully or encountered an error.
    ///
    /// # Arguments
    ///
    /// * `context` - Mutable reference to the consumption context, or `None` if no context is
    ///   available for the current invocation.
    fn consume_message_after(&self, context: Option<&mut ConsumeMessageContext>);
}
