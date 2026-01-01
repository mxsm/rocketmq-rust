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

use rocketmq_common::common::message::MessageTrait;

use crate::base::message_result::PutMessageResult;

/// Trait for hook executed before putting a message.
pub trait PutMessageHook {
    /// Returns the name of the hook.
    fn hook_name(&self) -> &'static str;

    /// Execute before putting a message.
    /// For example, message verification or special message transformation.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to be put
    ///
    /// # Returns
    ///
    /// The result of putting the message
    fn execute_before_put_message(&self, msg: &mut dyn MessageTrait) -> Option<PutMessageResult>;
}

/// Alias for `Arc<dyn PutMessageHook>`.
pub type BoxedPutMessageHook = Box<dyn PutMessageHook + Send + Sync + 'static>;
