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

/// Message append capability generic over a consumer-owned input.
pub trait MessageAppender<M: Send>: Send {
    type Receipt: Send;
    type Error: StdError + Send + Sync + 'static;

    /// Appends one input and returns the implementation-owned receipt projection.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the append cannot produce an outcome.
    fn append_message(&mut self, message: M) -> impl Future<Output = Result<Self::Receipt, Self::Error>> + Send;
}
