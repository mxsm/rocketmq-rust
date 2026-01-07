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

use rocketmq_common::common::message::message_ext::MessageExt;

/// Trait defining the listener for transactional message checks.
/// This trait provides a method for resolving discarded messages.
#[trait_variant::make(TransactionalMessageCheckListener: Send)]
pub trait TransactionalMessageCheckListenerInner: std::any::Any {
    /// Attempts to resolve a discarded message, typically called when a transaction
    /// message needs cleanup or final disposition.
    ///
    /// # Arguments
    ///
    /// * `msg_ext` - The message to be resolved, containing transaction metadata
    ///
    /// # Returns
    ///
    /// A Result indicating whether the resolution was successful, with an error
    /// type appropriate for resolution failures.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The message cannot be resolved
    /// - The broker fails to process the resolution
    /// - The message is in an invalid state
    async fn resolve_discard_msg(&mut self, msg_ext: MessageExt);

    /// Sends a check message to verify the state of a transactional message.
    ///
    /// # Arguments
    ///
    /// * `msg_ext` - The transactional message to be checked, containing metadata such as
    ///   transaction ID and state.
    ///
    /// # Returns
    ///
    /// A `RocketMQResult<()>` indicating the success or failure of the operation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The message cannot be sent
    /// - The broker encounters an issue during processing
    async fn send_check_message(&self, msg_ext: MessageExt) -> rocketmq_error::RocketMQResult<()>;

    /// Resolves a half message, typically used for transactional messages
    /// that are in an intermediate state and require further processing.
    ///
    /// # Arguments
    ///
    /// * `msg_ext` - The half message to be resolved, containing metadata such as transaction ID
    ///   and state.
    ///
    /// # Returns
    ///
    /// A `RocketMQResult<()>` indicating the success or failure of the resolution.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The message cannot be resolved
    /// - The broker encounters an issue during processing
    async fn resolve_half_msg(&self, msg_ext: MessageExt) -> rocketmq_error::RocketMQResult<()>;

    fn as_any(&self) -> &dyn std::any::Any;

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}
