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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::base::memory_lock_manager::MemoryLockCategory;
use crate::base::memory_lock_manager::MemoryLockHandle;
use crate::base::memory_lock_manager::MemoryLockManager;
use crate::commit_log::load::LoadStatistics;
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

/// Canonical runtime-neutral state held by the Local CommitLog core.
#[doc(hidden)]
#[derive(Debug)]
pub struct CommitLogRuntimeState {
    confirm_offset: AtomicI64,
    put_message_lock_stats: CommitLogPutMessageLockStats,
    begin_time_in_lock: Arc<AtomicU64>,
    active_memory_lock: Mutex<CommitLogActiveMemoryLock>,
    active_memory_lock_present: AtomicBool,
    last_load_statistics: Mutex<LoadStatistics>,
}

impl CommitLogRuntimeState {
    /// Creates the legacy initial CommitLog runtime state.
    #[doc(hidden)]
    pub fn new(memory_lock_warn_only: bool, memory_lock_budget_bytes: u64) -> Self {
        Self {
            confirm_offset: AtomicI64::new(-1),
            put_message_lock_stats: CommitLogPutMessageLockStats::default(),
            begin_time_in_lock: Arc::new(AtomicU64::new(0)),
            active_memory_lock: Mutex::new(CommitLogActiveMemoryLock::new(
                memory_lock_warn_only,
                memory_lock_budget_bytes,
            )),
            active_memory_lock_present: AtomicBool::new(false),
            last_load_statistics: Mutex::new(LoadStatistics::default()),
        }
    }

    /// Returns the stored confirm offset without applying HA/config policy.
    #[doc(hidden)]
    pub fn confirm_offset(&self) -> i64 {
        self.confirm_offset.load(Ordering::SeqCst)
    }

    /// Replaces the stored confirm offset.
    #[doc(hidden)]
    pub fn set_confirm_offset(&mut self, confirm_offset: i64) {
        self.publish_confirm_offset(confirm_offset);
    }

    /// Publishes the stored confirm offset through the shared HA boundary.
    #[doc(hidden)]
    pub fn publish_confirm_offset(&self, confirm_offset: i64) {
        self.confirm_offset.store(confirm_offset, Ordering::SeqCst);
    }

    /// Returns the current put-message lock timing snapshot.
    #[doc(hidden)]
    pub fn put_message_lock_runtime_info(&self) -> CommitLogPutMessageLockRuntimeInfo {
        self.put_message_lock_stats.snapshot()
    }

    /// Records one completed put-message lock acquisition.
    #[doc(hidden)]
    pub fn record_put_message_lock(&self, wait_millis: u64, hold_millis: u64) {
        self.put_message_lock_stats.record(wait_millis, hold_millis);
    }

    /// Stores the timestamp at which the current put-message lock hold began.
    #[doc(hidden)]
    pub fn set_begin_time_in_lock(&self, begin_time: u64) {
        self.begin_time_in_lock.store(begin_time, Ordering::Release);
    }

    /// Clears the current put-message lock start timestamp.
    #[doc(hidden)]
    pub fn clear_begin_time_in_lock(&self) {
        self.begin_time_in_lock.store(0, Ordering::Release);
    }

    /// Returns the shared legacy lock-start timestamp handle.
    #[doc(hidden)]
    pub fn begin_time_in_lock(&self) -> &Arc<AtomicU64> {
        &self.begin_time_in_lock
    }

    /// Returns the active-lock state and its lock-free presence hint.
    #[doc(hidden)]
    pub fn active_memory_lock_parts(&self) -> (&Mutex<CommitLogActiveMemoryLock>, &AtomicBool) {
        (&self.active_memory_lock, &self.active_memory_lock_present)
    }

    /// Replaces the most recent CommitLog load statistics.
    #[doc(hidden)]
    pub fn set_load_statistics(&self, statistics: LoadStatistics) {
        *self.last_load_statistics.lock() = statistics;
    }

    /// Returns a snapshot of the most recent CommitLog load statistics.
    #[doc(hidden)]
    pub fn load_statistics(&self) -> LoadStatistics {
        self.last_load_statistics.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::CommitLogRuntimeState;

    #[test]
    fn shared_confirm_offset_publication_preserves_decreases() {
        let state = CommitLogRuntimeState::new(true, 0);

        state.publish_confirm_offset(128);
        assert_eq!(state.confirm_offset(), 128);

        state.publish_confirm_offset(64);
        assert_eq!(state.confirm_offset(), 64);
    }
}
