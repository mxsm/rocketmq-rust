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

/// Replication control capability with implementation-owned command and state values.
pub trait ReplicationControl: Send + Sync {
    /// Value type used for command.
    type Command: Send;
    /// Value type used for state.
    type State: Send;

    /// Returns current replication state.
    fn replication_state(&self) -> Self::State;

    /// Applies one replication command.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] with [`crate::StoreOperation::Replicate`] when
    /// the replication operation fails.
    fn apply_replication(
        &mut self,
        command: Self::Command,
    ) -> impl Future<Output = Result<Self::State, StoreError>> + Send;
}
