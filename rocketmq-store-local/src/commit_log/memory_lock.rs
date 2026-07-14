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

use crate::base::memory_lock_manager::MemoryLockCategory;

const DEFAULT_ACTIVE_MEMORY_LOCK_WINDOW_BYTES: usize = 128 * 1024 * 1024;

/// CommitLog memory-lock policy selected by the Store configuration adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogMemoryLockMode {
    /// Do not lock the active CommitLog file.
    Off,
    /// Lock a bounded window beginning at the current write position.
    ActiveWindow,
    /// Lock the entire active CommitLog file.
    ActiveFile,
}

/// Pure address-independent target for an active CommitLog memory lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitLogMemoryLockTarget {
    /// Memory-lock accounting category.
    pub category: MemoryLockCategory,
    /// Byte offset from the beginning of the active CommitLog file.
    pub offset: u64,
    /// Number of bytes to lock.
    pub len: usize,
}

/// Plans the memory-lock target for an active CommitLog file.
pub fn plan_commit_log_memory_lock_target(
    mode: CommitLogMemoryLockMode,
    active_window_bytes: usize,
    wrote_position: u64,
    file_size: u64,
) -> Option<CommitLogMemoryLockTarget> {
    if file_size == 0 {
        return None;
    }

    match mode {
        CommitLogMemoryLockMode::Off => None,
        CommitLogMemoryLockMode::ActiveWindow => {
            if wrote_position >= file_size {
                return None;
            }
            let requested_len = if active_window_bytes == 0 {
                DEFAULT_ACTIVE_MEMORY_LOCK_WINDOW_BYTES
            } else {
                active_window_bytes
            };
            let remaining = file_size.saturating_sub(wrote_position);
            let len = requested_len.min(usize::try_from(remaining).unwrap_or(usize::MAX));
            (len > 0).then_some(CommitLogMemoryLockTarget {
                category: MemoryLockCategory::CommitLogActiveWindow,
                offset: wrote_position,
                len,
            })
        }
        CommitLogMemoryLockMode::ActiveFile => {
            let len = usize::try_from(file_size).ok()?;
            (len > 0).then_some(CommitLogMemoryLockTarget {
                category: MemoryLockCategory::CommitLogActiveFile,
                offset: 0,
                len,
            })
        }
    }
}
