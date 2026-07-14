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

use rocketmq_store_local::base::memory_lock_manager::MemoryLockCategory;
use rocketmq_store_local::commit_log::memory_lock::plan_commit_log_memory_lock_target;
use rocketmq_store_local::commit_log::memory_lock::CommitLogMemoryLockMode;
use rocketmq_store_local::commit_log::memory_lock::CommitLogMemoryLockTarget;

#[test]
fn off_and_empty_files_never_produce_targets() {
    for mode in [
        CommitLogMemoryLockMode::Off,
        CommitLogMemoryLockMode::ActiveWindow,
        CommitLogMemoryLockMode::ActiveFile,
    ] {
        assert_eq!(plan_commit_log_memory_lock_target(mode, 1024, 0, 0), None);
    }
    assert_eq!(
        plan_commit_log_memory_lock_target(CommitLogMemoryLockMode::Off, 1024, 0, 4096),
        None
    );
}

#[test]
fn active_window_preserves_default_bounds_and_write_position_edges() {
    assert_eq!(
        plan_commit_log_memory_lock_target(CommitLogMemoryLockMode::ActiveWindow, 0, 64, 1024 * 1024 * 1024),
        Some(CommitLogMemoryLockTarget {
            category: MemoryLockCategory::CommitLogActiveWindow,
            offset: 64,
            len: 128 * 1024 * 1024,
        })
    );
    assert_eq!(
        plan_commit_log_memory_lock_target(CommitLogMemoryLockMode::ActiveWindow, 4096, 900, 1024),
        Some(CommitLogMemoryLockTarget {
            category: MemoryLockCategory::CommitLogActiveWindow,
            offset: 900,
            len: 124,
        })
    );
    for wrote_position in [1024, 1025, u64::MAX] {
        assert_eq!(
            plan_commit_log_memory_lock_target(CommitLogMemoryLockMode::ActiveWindow, 4096, wrote_position, 1024,),
            None
        );
    }
}

#[test]
fn active_file_preserves_checked_platform_conversion() {
    assert_eq!(
        plan_commit_log_memory_lock_target(CommitLogMemoryLockMode::ActiveFile, 1, 37, 4096),
        Some(CommitLogMemoryLockTarget {
            category: MemoryLockCategory::CommitLogActiveFile,
            offset: 0,
            len: 4096,
        })
    );

    #[cfg(target_pointer_width = "32")]
    assert_eq!(
        plan_commit_log_memory_lock_target(CommitLogMemoryLockMode::ActiveFile, 1, 0, u64::MAX),
        None
    );
    #[cfg(target_pointer_width = "64")]
    assert_eq!(
        plan_commit_log_memory_lock_target(CommitLogMemoryLockMode::ActiveFile, 1, 0, u64::MAX),
        Some(CommitLogMemoryLockTarget {
            category: MemoryLockCategory::CommitLogActiveFile,
            offset: 0,
            len: usize::MAX,
        })
    );
}
