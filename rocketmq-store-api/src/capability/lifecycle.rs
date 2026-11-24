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

use crate::StoreError;

/// Storage lifecycle capability.
#[trait_variant::make(Send)]
pub trait StoreLifecycle: Sync {
    /// Loads existing state.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] with [`crate::StoreOperation::Load`] when state
    /// cannot be loaded safely.
    async fn load(&mut self) -> Result<bool, StoreError>;

    /// Starts the store within its existing lifecycle owner.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] with [`crate::StoreOperation::Start`] when
    /// startup cannot establish a usable store.
    async fn start(&mut self) -> Result<(), StoreError>;

    /// Stops the store and completes its owned shutdown sequence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] with [`crate::StoreOperation::Shutdown`] when
    /// final progress or shutdown fails.
    async fn shutdown(&mut self) -> Result<(), StoreError>;
}
