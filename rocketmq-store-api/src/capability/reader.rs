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

use std::future::Future;

use crate::StoreError;

/// Message read capability with implementation-owned request and output values.
///
/// A consumer generic over this capability cannot access lifecycle or
/// administration operations that were not injected:
///
/// ```compile_fail
/// use rocketmq_store_api::MessageReader;
///
/// fn read_only<R: MessageReader>(reader: &mut R) {
///     reader.execute_admin(());
/// }
/// ```
pub trait MessageReader: Send + Sync {
    /// Value type used for request.
    type Request: Send;
    /// Value type used for output.
    type Output: Send;

    /// Reads a bounded message window.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] with [`crate::StoreOperation::Read`] when the
    /// requested data cannot be read safely.
    fn read(&self, request: Self::Request) -> impl Future<Output = Result<Self::Output, StoreError>> + Send;
}
