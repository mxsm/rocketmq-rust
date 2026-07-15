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

//! Runtime-neutral state owned by the Local CommitLog boundary.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::base::memory_lock_manager::MemoryLockCategory;
use crate::base::memory_lock_manager::MemoryLockHandle;
use crate::base::memory_lock_manager::MemoryLockManager;
use crate::commit_log::memory_lock::CommitLogMemoryLockTarget;

/// Snapshot of CommitLog put-message lock timing counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommitLogPutMessageLockRuntimeInfo {
    /// Number of completed lock acquisitions.
    pub acquire_total: u64,
    /// Cumulative time spent waiting for the lock.
    pub wait_total_millis: u64,
    /// Longest observed lock wait.
    pub wait_max_millis: u64,
    /// Cumulative time spent holding the lock.
    pub hold_total_millis: u64,
    /// Longest observed lock hold.
    pub hold_max_millis: u64,
}

/// Atomic owner for CommitLog put-message lock timing counters.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct CommitLogPutMessageLockStats {
    acquire_total: AtomicU64,
    wait_total_millis: AtomicU64,
    wait_max_millis: AtomicU64,
    hold_total_millis: AtomicU64,
    hold_max_millis: AtomicU64,
}

impl CommitLogPutMessageLockStats {
    /// Records one completed lock acquisition.
    #[doc(hidden)]
    pub fn record(&self, wait_millis: u64, hold_millis: u64) {
        self.acquire_total.fetch_add(1, Ordering::Relaxed);
        self.wait_total_millis.fetch_add(wait_millis, Ordering::Relaxed);
        self.hold_total_millis.fetch_add(hold_millis, Ordering::Relaxed);
        Self::update_max(&self.wait_max_millis, wait_millis);
        Self::update_max(&self.hold_max_millis, hold_millis);
    }

    /// Returns a consistent per-counter snapshot using relaxed diagnostic loads.
    #[doc(hidden)]
    pub fn snapshot(&self) -> CommitLogPutMessageLockRuntimeInfo {
        CommitLogPutMessageLockRuntimeInfo {
            acquire_total: self.acquire_total.load(Ordering::Relaxed),
            wait_total_millis: self.wait_total_millis.load(Ordering::Relaxed),
            wait_max_millis: self.wait_max_millis.load(Ordering::Relaxed),
            hold_total_millis: self.hold_total_millis.load(Ordering::Relaxed),
            hold_max_millis: self.hold_max_millis.load(Ordering::Relaxed),
        }
    }

    fn update_max(target: &AtomicU64, value: u64) {
        let mut current = target.load(Ordering::Relaxed);
        while value > current {
            match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

/// Current active CommitLog memory-lock identity and manager state.
#[doc(hidden)]
#[derive(Debug)]
pub struct CommitLogActiveMemoryLock {
    manager: MemoryLockManager,
    handle: Option<MemoryLockHandle>,
    file_from_offset: Option<u64>,
    region_offset: u64,
    region_len: usize,
}

impl CommitLogActiveMemoryLock {
    /// Creates an empty active-lock state with the configured manager policy.
    #[doc(hidden)]
    pub fn new(warn_only: bool, budget_bytes: u64) -> Self {
        Self {
            manager: MemoryLockManager::new(warn_only, budget_bytes),
            handle: None,
            file_from_offset: None,
            region_offset: 0,
            region_len: 0,
        }
    }

    /// Returns whether `target` is already covered by the current locked region.
    #[doc(hidden)]
    pub fn is_current(&self, file_from_offset: u64, target: CommitLogMemoryLockTarget) -> bool {
        let Some(handle) = self.handle else {
            return false;
        };
        if self.file_from_offset != Some(file_from_offset) || handle.category() != target.category {
            return false;
        }

        if target.category == MemoryLockCategory::CommitLogActiveWindow {
            let region_end = self.region_offset.saturating_add(self.region_len as u64);
            target.offset >= self.region_offset && target.offset < region_end
        } else {
            self.region_offset == target.offset && self.region_len == target.len
        }
    }

    /// Borrows the canonical Local memory-lock manager for the platform adapter.
    #[doc(hidden)]
    pub fn manager(&self) -> &MemoryLockManager {
        &self.manager
    }

    /// Replaces the current locked-region identity after a successful platform lock.
    #[doc(hidden)]
    pub fn set_current(&mut self, file_from_offset: u64, target: CommitLogMemoryLockTarget, handle: MemoryLockHandle) {
        self.handle = Some(handle);
        self.file_from_offset = Some(file_from_offset);
        self.region_offset = target.offset;
        self.region_len = target.len;
    }

    /// Takes the current handle so the platform adapter can unlock it exactly once.
    #[doc(hidden)]
    pub fn take_handle(&mut self) -> Option<MemoryLockHandle> {
        self.handle.take()
    }

    /// Clears the current locked-region identity.
    #[doc(hidden)]
    pub fn clear(&mut self) {
        self.handle = None;
        self.file_from_offset = None;
        self.region_offset = 0;
        self.region_len = 0;
    }
}
