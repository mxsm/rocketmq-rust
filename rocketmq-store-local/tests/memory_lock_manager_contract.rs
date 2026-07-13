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

use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_store_local::base::memory_lock_manager::MemoryLockCategory;
use rocketmq_store_local::base::memory_lock_manager::MemoryLockHandle;
use rocketmq_store_local::base::memory_lock_manager::MemoryLockManager;

fn successful_lock(
    manager: &MemoryLockManager,
    category: MemoryLockCategory,
    len: usize,
) -> RocketMQResult<Option<MemoryLockHandle>> {
    manager.lock_region_with(category, std::ptr::NonNull::<u8>::dangling().as_ptr(), len, |_, _| {
        Ok(())
    })
}

#[test]
fn memory_lock_category_labels_are_stable() {
    assert_eq!(MemoryLockCategory::TransientStorePool.as_str(), "transient_store_pool");
    assert_eq!(
        MemoryLockCategory::CommitLogActiveWindow.as_str(),
        "commitlog_active_window"
    );
    assert_eq!(
        MemoryLockCategory::CommitLogActiveFile.as_str(),
        "commitlog_active_file"
    );
    assert_eq!(
        MemoryLockCategory::ConsumeQueueHotWindow.as_str(),
        "consumequeue_hot_window"
    );
    assert_eq!(MemoryLockCategory::IndexHotWindow.as_str(), "index_hot_window");
}

#[test]
fn strict_manager_preserves_lock_error_and_failure_counters() {
    let manager = MemoryLockManager::new(false, 4096);

    let error = manager
        .lock_region_with(
            MemoryLockCategory::CommitLogActiveFile,
            std::ptr::null(),
            4096,
            |_, _| {
                Err(RocketMQError::StorageLockFailed {
                    path: "injected strict failure".to_string(),
                })
            },
        )
        .expect_err("strict manager must return the injected error");

    assert!(matches!(
        error,
        RocketMQError::StorageLockFailed { path } if path == "injected strict failure"
    ));
    assert_eq!(manager.lock_attempt_count(), 1);
    assert_eq!(manager.locked_buffer_count(), 0);
    assert_eq!(manager.lock_failed_buffer_count(), 1);
    assert_eq!(manager.lock_failed_bytes(), 4096);
    assert_eq!(manager.locked_bytes(), 0);
}

#[test]
fn warn_only_manager_skips_exhausted_budget_without_calling_locker() {
    let manager = MemoryLockManager::warn_only_with_budget(1024);
    let mut called = false;

    let handle = manager
        .lock_region_with(
            MemoryLockCategory::CommitLogActiveWindow,
            std::ptr::null(),
            2048,
            |_, _| {
                called = true;
                Ok(())
            },
        )
        .expect("warn-only budget exhaustion is non-fatal");

    assert!(handle.is_none());
    assert!(!called);
    assert_eq!(manager.lock_attempt_count(), 1);
    assert_eq!(manager.lock_skipped_buffer_count(), 1);
    assert_eq!(manager.lock_skipped_bytes(), 2048);
    assert_eq!(manager.locked_bytes(), 0);
}

#[test]
fn strict_manager_reports_the_legacy_budget_error_text() {
    let manager = MemoryLockManager::new(false, 1024);

    let error = manager
        .lock_region_with(
            MemoryLockCategory::CommitLogActiveWindow,
            std::ptr::null(),
            2048,
            |_, _| panic!("budget rejection must happen before the locker"),
        )
        .expect_err("strict budget exhaustion must fail");

    assert!(matches!(
        error,
        RocketMQError::StorageLockFailed { path }
            if path == "memory lock budget exhausted: requested=2048 budget=1024"
    ));
}

#[test]
fn concurrent_reservations_do_not_exceed_budget_and_unlock_to_zero() {
    let manager = Arc::new(MemoryLockManager::new(false, 4096));
    let workers = (0..8)
        .map(|_| {
            let manager = Arc::clone(&manager);
            std::thread::spawn(move || successful_lock(&manager, MemoryLockCategory::CommitLogActiveWindow, 1024))
        })
        .collect::<Vec<_>>();

    let outcomes = workers
        .into_iter()
        .map(|worker| worker.join().expect("worker must not panic"))
        .collect::<Vec<_>>();
    let errors = outcomes.iter().filter(|outcome| outcome.is_err()).count();
    let handles = outcomes
        .into_iter()
        .filter_map(|outcome| outcome.ok().flatten())
        .collect::<Vec<_>>();

    assert_eq!(handles.len(), 4);
    assert_eq!(errors, 4);
    assert_eq!(manager.lock_attempt_count(), 8);
    assert_eq!(manager.locked_buffer_count(), 4);
    assert_eq!(manager.locked_bytes(), 4096);
    for handle in handles {
        manager
            .unlock_region_with(handle, |_, _| Ok(()))
            .expect("injected unlock succeeds");
    }
    assert_eq!(manager.locked_bytes(), 0);
}
