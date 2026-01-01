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

use crate::base::swappable::Swappable;

/// Trait defining the lifecycle of a file-based queue, including operations for loading,
/// recovery, flushing, and destruction.
pub trait FileQueueLifeCycle: Swappable {
    /// Loads the queue from persistent storage.
    ///
    /// # Returns
    /// `true` if the queue was successfully loaded, `false` otherwise.
    fn load(&mut self) -> bool;

    /// Recovers the queue state from persistent storage.
    fn recover(&mut self);

    /// Performs a self-check to ensure the queue's integrity.
    fn check_self(&self);

    /// Flushes the queue's data to persistent storage.
    ///
    /// # Arguments
    /// * `flush_least_pages` - The minimum number of pages to flush.
    ///
    /// # Returns
    /// `true` if any data was flushed, `false` otherwise.
    fn flush(&self, flush_least_pages: i32) -> bool;

    /// Destroys the queue, cleaning up resources.
    fn destroy(&mut self);

    /// Truncates dirty logic files beyond a specified commit log position.
    ///
    /// # Arguments
    /// * `max_commit_log_pos` - The maximum commit log position to retain.
    fn truncate_dirty_logic_files(&mut self, max_commit_log_pos: i64);

    /// Deletes expired files based on a minimum commit log position.
    ///
    /// # Arguments
    /// * `min_commit_log_pos` - The minimum commit log position to consider.
    ///
    /// # Returns
    /// The number of files deleted.
    fn delete_expired_file(&self, min_commit_log_pos: i64) -> i32;

    /// Rolls over to the next file in the queue, based on the provided offset.
    ///
    /// # Arguments
    /// * `next_begin_offset` - The offset to start the next file at.
    ///
    /// # Returns
    /// The offset at which the next file begins.
    fn roll_next_file(&self, next_begin_offset: i64) -> i64;

    /// Checks if the first file in the queue is available for operations.
    ///
    /// # Returns
    /// `true` if the first file is available, `false` otherwise.
    fn is_first_file_available(&self) -> bool;

    /// Checks if the first file in the queue exists on the storage medium.
    ///
    /// # Returns
    /// `true` if the first file exists, `false` otherwise.
    fn is_first_file_exist(&self) -> bool;
}
