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

use std::error::Error;

use crate::consumer::ack_result::AckResult;

/// Trait representing an acknowledgment callback.
/// This trait defines two methods: `on_success` and `on_exception`.
pub trait AckCallback {
    /// Called when the acknowledgment is successful.
    ///
    /// # Arguments
    ///
    /// * `ack_result` - The result of the acknowledgment.
    fn on_success(&self, ack_result: AckResult);

    /// Called when there is an exception during acknowledgment.
    ///
    /// # Arguments
    ///
    /// * `e` - The error that occurred.
    fn on_exception(&self, e: Box<dyn Error>);
}

/// Type alias for a function that acts as an acknowledgment callback.
/// The function takes two optional arguments: an `AckResult` and an error.
///
/// This type alias is used to define a callback function that can be passed
/// around and invoked when an acknowledgment operation completes.
///
/// The function must be `Send` and `Sync` to ensure it can be safely used
/// across threads.
pub type AckCallbackFn = Box<dyn Fn(AckResult) -> Result<(), Box<dyn Error>> + Send + Sync>;
