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

/// Storage lifecycle capability.
pub trait StoreLifecycle: Send + Sync {
    /// Loads existing state.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] with [`crate::StoreOperation::Load`] when state
    /// cannot be loaded safely.
    fn load(&mut self) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Starts the store within its existing lifecycle owner.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] with [`crate::StoreOperation::Start`] when
    /// startup cannot establish a usable store.
    fn start(&mut self) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Stops the store and completes its owned shutdown sequence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] with [`crate::StoreOperation::Shutdown`] when
    /// final progress or shutdown fails.
    fn shutdown(&mut self) -> impl Future<Output = Result<(), StoreError>> + Send;
}
