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

use crate::mqtrace::send_message_context::SendMessageContext;

/// The `SendMessageHook` trait defines a common interface for sending messages.
/// It is designed to be thread-safe and have a static lifetime.
///
/// This trait is composed of three methods:
/// - `hook_name`: Returns a string slice that represents the name of the hook.
/// - `send_message_before`: Called before a message is sent. It takes a reference to a
///   `SendMessageContext`.
/// - `send_message_after`: Called after a message is sent. It also takes a reference to a
///   `SendMessageContext`.
pub trait SendMessageHook: Send + Sync + 'static {
    /// Returns the name of the hook.
    ///
    /// # Returns
    ///
    /// A string slice that represents the name of the hook.
    fn hook_name(&self) -> &str;

    /// Called before a message is sent.
    ///
    /// # Parameters
    ///
    /// * `context`: A reference to a `SendMessageContext`.
    fn send_message_before(&self, context: &SendMessageContext);

    /// Called after a message is sent.
    ///
    /// # Parameters
    ///
    /// * `context`: A reference to a `SendMessageContext`.
    fn send_message_after(&self, context: &SendMessageContext);
}
