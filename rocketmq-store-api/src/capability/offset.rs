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

/// Logical offset lookup capability.
pub trait OffsetIndex: Send + Sync {
    /// Value type used for query.
    type Query: Send + Sync;
    /// Value type used for output.
    type Output;

    /// Queries the current logical offset projection.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] with [`crate::StoreOperation::QueryOffset`] when
    /// index state is unavailable or inconsistent.
    fn query_offset(&self, query: &Self::Query) -> Result<Self::Output, StoreError>;
}
