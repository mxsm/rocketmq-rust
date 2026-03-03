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

use crate::hook::check_forbidden_context::CheckForbiddenContext;

/// Hook for checking forbidden message sending operations.
///
/// This trait allows custom validation logic to be executed before messages are sent.
/// Implementations can inspect the message context and reject operations that violate
/// business rules or security policies.
///
/// All implementations must be `Send + Sync` to support concurrent message sending
/// in multi-threaded producer environments.
///
/// # Examples
///
/// ```ignore
/// use rocketmq_client::hook::check_forbidden_hook::CheckForbiddenHook;
/// use rocketmq_client::hook::check_forbidden_context::CheckForbiddenContext;
/// use rocketmq_error::RocketMQResult;
///
/// struct RegionValidator;
///
/// impl CheckForbiddenHook for RegionValidator {
///     fn hook_name(&self) -> &str {
///         "RegionValidator"
///     }
///
///     fn check_forbidden(&self, context: &CheckForbiddenContext) -> RocketMQResult<()> {
///         if let Some(broker_addr) = &context.broker_addr {
///             if broker_addr.contains("forbidden-region") {
///                 return Err("Cannot send to forbidden region".into());
///             }
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait CheckForbiddenHook: Send + Sync {
    /// Returns the unique name of this hook.
    ///
    /// The name is used for logging and diagnostic purposes.
    fn hook_name(&self) -> &str;

    /// Validates whether a message send operation should be allowed.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation should be rejected based on the context.
    fn check_forbidden(&self, context: &CheckForbiddenContext<'_>) -> rocketmq_error::RocketMQResult<()>;
}
