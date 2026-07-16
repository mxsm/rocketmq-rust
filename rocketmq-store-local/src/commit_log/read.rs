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

use rocketmq_store_api::StoreError;

/// Narrow read port for the single Local CommitLog owned by a composed backend.
///
/// Derived stores use this port to read the primary log without owning another
/// message log or depending on the legacy Store facade.
pub trait LocalWalPort: Send + Sync {
    type Selection;

    /// Reads one exact message range from the primary log.
    ///
    /// # Errors
    ///
    /// Returns a backend-neutral error when the Local WAL cannot safely serve
    /// the requested range.
    fn read_message(&self, offset: i64, size: i32) -> Result<Option<Self::Selection>, StoreError>;

    /// Reads the message range beginning at a physical offset.
    ///
    /// # Errors
    ///
    /// Returns a backend-neutral error when the Local WAL cannot safely serve
    /// the requested offset.
    fn read_from(&self, offset: i64) -> Result<Option<Self::Selection>, StoreError>;

    /// Borrows the selected message bytes while the selection keeps its Local
    /// mapped-file lease alive.
    fn selection_bytes<'a>(&self, selection: &'a Self::Selection) -> &'a [u8];

    /// Applies the Local offset-correction policy used by legacy pull results.
    fn correct_queue_offset(&self, old_offset: i64, new_offset: i64) -> i64;
}
