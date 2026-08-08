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

    /// Recovers the queue and reports whether every destructive repair completed.
    ///
    /// The compatibility default invokes the legacy hook but fails closed because an
    /// implementation without an explicit outcome cannot prove that repair identities remain
    /// retryable.
    fn recover_with_outcome(&mut self) -> bool {
        self.recover();
        false
    }

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

    /// Attempts destruction and reports whether every tracked mapped file was removed.
    ///
    /// The default invokes the legacy cleanup hook but fails closed because implementations
    /// without retry-aware ownership cannot prove that every namespace entry was removed.
    fn destroy_with_outcome(&mut self) -> bool {
        self.destroy();
        false
    }

    /// Truncates dirty logic files beyond a specified commit log position.
    ///
    /// # Arguments
    /// * `max_commit_log_pos` - The maximum commit log position to retain.
    fn truncate_dirty_logic_files(&mut self, max_commit_log_pos: i64);

    /// Truncates dirty logic files and reports whether every namespace operation completed.
    fn truncate_dirty_logic_files_with_outcome(&mut self, max_commit_log_pos: i64) -> bool {
        self.truncate_dirty_logic_files(max_commit_log_pos);
        false
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct LegacyQueue {
        destroy_called: bool,
    }

    impl Swappable for LegacyQueue {
        fn swap_map(&self, _reserve_num: i32, _force_swap_interval_ms: i64, _normal_swap_interval_ms: i64) {}

        fn clean_swapped_map(&self, _force_clean_swap_interval_ms: i64) {}
    }

    impl FileQueueLifeCycle for LegacyQueue {
        fn load(&mut self) -> bool {
            true
        }

        fn recover(&mut self) {}

        fn recover_with_outcome(&mut self) -> bool {
            true
        }

        fn check_self(&self) {}

        fn flush(&self, _flush_least_pages: i32) -> bool {
            false
        }

        fn destroy(&mut self) {
            self.destroy_called = true;
        }

        fn truncate_dirty_logic_files(&mut self, _max_commit_log_pos: i64) {}

        fn truncate_dirty_logic_files_with_outcome(&mut self, _max_commit_log_pos: i64) -> bool {
            true
        }

        fn delete_expired_file(&self, _min_commit_log_pos: i64) -> i32 {
            0
        }

        fn roll_next_file(&self, next_begin_offset: i64) -> i64 {
            next_begin_offset
        }

        fn is_first_file_available(&self) -> bool {
            false
        }

        fn is_first_file_exist(&self) -> bool {
            false
        }
    }

    #[test]
    fn legacy_destroy_default_cannot_claim_retry_aware_success() {
        let mut queue = LegacyQueue::default();

        assert!(!queue.destroy_with_outcome());
        assert!(queue.destroy_called);
    }
}
