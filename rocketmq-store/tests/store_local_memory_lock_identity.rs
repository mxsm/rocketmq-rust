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

use rocketmq_error::RocketMQResult;
use rocketmq_store::base::memory_lock_manager::MemoryLockCategory as LegacyCategory;
use rocketmq_store::base::memory_lock_manager::MemoryLockHandle as LegacyHandle;
use rocketmq_store::base::memory_lock_manager::MemoryLockManager as LegacyManager;
use rocketmq_store_local::base::memory_lock_manager::MemoryLockCategory as CanonicalCategory;
use rocketmq_store_local::base::memory_lock_manager::MemoryLockHandle as CanonicalHandle;
use rocketmq_store_local::base::memory_lock_manager::MemoryLockManager as CanonicalManager;

fn manager_to_canonical(value: LegacyManager) -> CanonicalManager {
    value
}

fn manager_to_legacy(value: CanonicalManager) -> LegacyManager {
    value
}

fn category_to_canonical(value: LegacyCategory) -> CanonicalCategory {
    value
}

fn handle_to_canonical(value: LegacyHandle) -> CanonicalHandle {
    value
}

fn handle_to_legacy(value: CanonicalHandle) -> LegacyHandle {
    value
}

#[test]
fn legacy_memory_lock_types_are_the_canonical_local_types() {
    let manager = manager_to_legacy(manager_to_canonical(LegacyManager::warn_only_with_budget(4096)));
    let category = category_to_canonical(LegacyCategory::CommitLogActiveWindow);
    let handle = manager
        .lock_region_with(category, std::ptr::NonNull::<u8>::dangling().as_ptr(), 4096, |_, _| {
            Ok(())
        })
        .expect("injected lock succeeds")
        .expect("successful lock returns a handle");
    let handle = handle_to_legacy(handle_to_canonical(handle));

    assert_eq!(handle.category(), LegacyCategory::CommitLogActiveWindow);
    assert_eq!(handle.len(), 4096);
    manager
        .unlock_region_with(handle, |_, _| Ok(()))
        .expect("injected unlock succeeds");
    assert_eq!(manager.locked_bytes(), 0);
}

#[test]
fn legacy_memory_lock_syscalls_are_direct_local_reexports() {
    let legacy_mlock: fn(*const u8, usize) -> RocketMQResult<()> = rocketmq_store::utils::ffi::mlock;
    let canonical_mlock: fn(*const u8, usize) -> RocketMQResult<()> = rocketmq_store_local::utils::ffi::mlock;
    let legacy_munlock: fn(*const u8, usize) -> RocketMQResult<()> = rocketmq_store::utils::ffi::munlock;
    let canonical_munlock: fn(*const u8, usize) -> RocketMQResult<()> = rocketmq_store_local::utils::ffi::munlock;

    assert_eq!(legacy_mlock as usize, canonical_mlock as usize);
    assert_eq!(legacy_munlock as usize, canonical_munlock as usize);
}
