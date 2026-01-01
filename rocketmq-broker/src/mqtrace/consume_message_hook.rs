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

use crate::mqtrace::consume_message_context::ConsumeMessageContext;

/// Trait for hooks in the message consumption process.
///
/// This trait defines a mechanism for intercepting and possibly modifying the behavior
/// of message consumption in a message queue system. Implementors can provide custom logic
/// before and after the message consumption process.
///
/// # Requirements
///
/// Implementors must be thread-safe (`Sync + Send`) and support static lifetimes (`'static`).
pub trait ConsumeMessageHook: Sync + Send + 'static {
    /// Returns the name of the hook.
    ///
    /// This method should provide a unique name for the hook, which can be used for logging,
    /// debugging, or identifying the hook within a collection of hooks.
    ///
    /// # Returns
    /// A string slice (`&str`) representing the name of the hook.
    fn hook_name(&self) -> &str;

    /// Hook method called before a message is consumed.
    ///
    /// This method is invoked before the actual consumption of a message, allowing for
    /// pre-processing, logging, or other preparatory actions based on the message context.
    ///
    /// # Arguments
    /// * `context` - A mutable reference to the `ConsumeMessageContext`, providing access to the
    ///   message and its metadata for possible inspection or modification.
    fn consume_message_before<'a>(&self, context: &mut ConsumeMessageContext<'a>);

    /// Hook method called after a message is consumed.
    ///
    /// This method is invoked after a message has been consumed, allowing for post-processing,
    /// logging, or other follow-up actions based on the message context and the outcome of its
    /// consumption.
    ///
    /// # Arguments
    /// * `context` - A mutable reference to the `ConsumeMessageContext`, providing access to the
    ///   message and its metadata for possible inspection or modification.
    fn consume_message_after<'a>(&self, context: &mut ConsumeMessageContext<'a>);
}
