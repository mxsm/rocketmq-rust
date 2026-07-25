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

use std::error::Error as StdError;
use std::future::Future;

/// Storage lifecycle capability.
pub trait StoreLifecycle: Send + Sync {
    type Error: StdError + Send + Sync + 'static;

    /// Loads existing state.
    ///
    /// # Errors
    ///
    /// Returns a typed error when state cannot be loaded safely.
    fn load(&mut self) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Starts the store within its existing lifecycle owner.
    ///
    /// # Errors
    ///
    /// Returns a typed error when startup cannot establish a usable store.
    fn start(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Stops the store and completes its owned shutdown sequence.
    ///
    /// # Errors
    ///
    /// Returns a typed error when final progress or shutdown fails.
    fn shutdown(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
