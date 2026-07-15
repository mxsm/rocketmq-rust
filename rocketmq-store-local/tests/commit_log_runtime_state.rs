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

use std::ptr::NonNull;

use rocketmq_error::RocketMQResult;
use rocketmq_store_local::base::memory_lock_manager::MemoryLockCategory;
use rocketmq_store_local::commit_log::memory_lock::CommitLogMemoryLockTarget;
use rocketmq_store_local::commit_log::runtime_state::CommitLogActiveMemoryLock;
use rocketmq_store_local::commit_log::runtime_state::CommitLogPutMessageLockStats;

fn test_handle(
    state: &CommitLogActiveMemoryLock,
    category: MemoryLockCategory,
) -> rocketmq_store_local::base::memory_lock_manager::MemoryLockHandle {
    state
        .manager()
        .lock_region_with(category, NonNull::<u8>::dangling().as_ptr(), 1, |_, _| {
            Ok::<_, rocketmq_error::RocketMQError>(())
        })
        .expect("test lock")
        .expect("non-empty test handle")
}

#[test]
fn put_message_lock_stats_accumulate_totals_and_maxima() {
    let stats = CommitLogPutMessageLockStats::default();
    stats.record(7, 11);
    stats.record(13, 5);

    let snapshot = stats.snapshot();
    assert_eq!(snapshot.acquire_total, 2);
    assert_eq!(snapshot.wait_total_millis, 20);
    assert_eq!(snapshot.wait_max_millis, 13);
    assert_eq!(snapshot.hold_total_millis, 16);
    assert_eq!(snapshot.hold_max_millis, 11);
}

#[test]
fn active_window_reuses_only_offsets_inside_the_current_region() {
    let mut state = CommitLogActiveMemoryLock::new(true, 1024);
    let target = CommitLogMemoryLockTarget {
        category: MemoryLockCategory::CommitLogActiveWindow,
        offset: 16,
        len: 32,
    };
    state.set_current(
        100,
        target,
        test_handle(&state, MemoryLockCategory::CommitLogActiveWindow),
    );

    assert!(state.is_current(100, target));
    assert!(state.is_current(100, CommitLogMemoryLockTarget { offset: 47, ..target }));
    assert!(!state.is_current(100, CommitLogMemoryLockTarget { offset: 48, ..target }));
    assert!(!state.is_current(101, target));
}

#[test]
fn active_file_requires_exact_region_and_take_clear_removes_identity() -> RocketMQResult<()> {
    let mut state = CommitLogActiveMemoryLock::new(true, 1024);
    let target = CommitLogMemoryLockTarget {
        category: MemoryLockCategory::CommitLogActiveFile,
        offset: 0,
        len: 64,
    };
    state.set_current(
        200,
        target,
        test_handle(&state, MemoryLockCategory::CommitLogActiveFile),
    );

    assert!(state.is_current(200, target));
    assert!(!state.is_current(200, CommitLogMemoryLockTarget { len: 63, ..target }));
    let handle = state.take_handle().expect("active handle");
    state.manager().unlock_region_with(handle, |_, _| Ok(()))?;
    state.clear();
    assert!(!state.is_current(200, target));
    assert!(state.take_handle().is_none());
    Ok(())
}
