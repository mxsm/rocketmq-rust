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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use rocketmq_error::RocketMQResult;
use tracing::warn;

use crate::utils::ffi::mlock;
use crate::utils::ffi::munlock;

#[cfg(test)]
const TRANSIENT_STORE_POOL_CATEGORY: &str = MemoryLockCategory::TransientStorePool.as_str();
#[cfg(any(test, feature = "observability"))]
const MEMORY_LOCK_BUDGET_EXHAUSTED_REASON: &str = "budget_exhausted";
#[cfg(any(test, feature = "observability"))]
const MEMORY_LOCK_UNKNOWN_ERRNO: i32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryLockCategory {
    TransientStorePool,
    CommitLogActiveWindow,
    CommitLogActiveFile,
    ConsumeQueueHotWindow,
    IndexHotWindow,
}

impl MemoryLockCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransientStorePool => "transient_store_pool",
            Self::CommitLogActiveWindow => "commitlog_active_window",
            Self::CommitLogActiveFile => "commitlog_active_file",
            Self::ConsumeQueueHotWindow => "consumequeue_hot_window",
            Self::IndexHotWindow => "index_hot_window",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryLockHandle {
    addr: usize,
    len: usize,
    category: MemoryLockCategory,
}

impl MemoryLockHandle {
    fn new(addr: *const u8, len: usize, category: MemoryLockCategory) -> Self {
        Self {
            addr: addr as usize,
            len,
            category,
        }
    }

    pub fn addr(self) -> *const u8 {
        self.addr as *const u8
    }

    pub fn len(self) -> usize {
        self.len
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    pub fn category(self) -> MemoryLockCategory {
        self.category
    }
}

#[derive(Debug)]
pub struct MemoryLockManager {
    warn_only: bool,
    budget_bytes: AtomicU64,
    lock_attempts: AtomicUsize,
    locked_buffers: AtomicUsize,
    lock_failed_buffers: AtomicUsize,
    lock_skipped_buffers: AtomicUsize,
    locked_bytes: AtomicU64,
    lock_failed_bytes: AtomicU64,
    lock_skipped_bytes: AtomicU64,
}

impl MemoryLockManager {
    pub fn new(warn_only: bool, budget_bytes: u64) -> Self {
        Self {
            warn_only,
            budget_bytes: AtomicU64::new(budget_bytes),
            lock_attempts: AtomicUsize::new(0),
            locked_buffers: AtomicUsize::new(0),
            lock_failed_buffers: AtomicUsize::new(0),
            lock_skipped_buffers: AtomicUsize::new(0),
            locked_bytes: AtomicU64::new(0),
            lock_failed_bytes: AtomicU64::new(0),
            lock_skipped_bytes: AtomicU64::new(0),
        }
    }

    pub fn warn_only() -> Self {
        Self::warn_only_with_budget(0)
    }

    pub fn warn_only_with_budget(budget_bytes: u64) -> Self {
        Self::new(true, budget_bytes)
    }

    pub fn lock_buffer(&self, addr: *const u8, len: usize) -> RocketMQResult<()> {
        self.lock_buffer_with(addr, len, mlock)
    }

    /// Runs the lock operation through an injected compatibility callback.
    ///
    /// This hidden compatibility seam is public only so the Store facade can keep deterministic
    /// tests while the mapped-file owner is migrated in a later slice.
    ///
    /// # Errors
    ///
    /// Returns the injected lock error when strict locking is enabled or budget reservation fails.
    #[doc(hidden)]
    pub fn lock_buffer_with<F>(&self, addr: *const u8, len: usize, mut locker: F) -> RocketMQResult<()>
    where
        F: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        let _ = self.lock_region_with(MemoryLockCategory::TransientStorePool, addr, len, &mut locker)?;
        Ok(())
    }

    pub fn lock_region(
        &self,
        category: MemoryLockCategory,
        addr: *const u8,
        len: usize,
    ) -> RocketMQResult<Option<MemoryLockHandle>> {
        self.lock_region_with(category, addr, len, mlock)
    }

    /// Runs a categorized lock through an injected compatibility callback.
    ///
    /// This hidden compatibility seam is public only so the Store facade can keep deterministic
    /// tests while the mapped-file owner is migrated in a later slice.
    ///
    /// # Errors
    ///
    /// Returns the injected lock error when strict locking is enabled or budget reservation fails.
    #[doc(hidden)]
    pub fn lock_region_with<F>(
        &self,
        category: MemoryLockCategory,
        addr: *const u8,
        len: usize,
        mut locker: F,
    ) -> RocketMQResult<Option<MemoryLockHandle>>
    where
        F: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        self.lock_attempts.fetch_add(1, Ordering::Relaxed);
        emit_memory_lock_attempt_observability(category);

        let len_bytes = len as u64;
        if !self.reserve_lock_budget(len_bytes) {
            self.lock_skipped_buffers.fetch_add(1, Ordering::Relaxed);
            self.lock_skipped_bytes.fetch_add(len_bytes, Ordering::Relaxed);
            emit_memory_lock_skip_observability(category, self.locked_bytes.load(Ordering::Relaxed));
            if self.warn_only {
                warn!(
                    "Skipped {} memory lock of {} bytes because lock budget {} bytes is exhausted",
                    category.as_str(),
                    len_bytes,
                    self.budget_bytes.load(Ordering::Relaxed)
                );
                return Ok(None);
            }
            return Err(rocketmq_error::RocketMQError::StorageLockFailed {
                path: format!(
                    "memory lock budget exhausted: requested={} budget={}",
                    len_bytes,
                    self.budget_bytes.load(Ordering::Relaxed)
                ),
            });
        }

        match locker(addr, len) {
            Ok(()) => {
                self.locked_buffers.fetch_add(1, Ordering::Relaxed);
                if self.budget_bytes.load(Ordering::Relaxed) == 0 {
                    self.locked_bytes.fetch_add(len_bytes, Ordering::Relaxed);
                }
                emit_memory_lock_success_observability(category, self.locked_bytes.load(Ordering::Relaxed));
                Ok(Some(MemoryLockHandle::new(addr, len, category)))
            }
            Err(error) => {
                self.release_reserved_budget(len_bytes);
                self.lock_failed_buffers.fetch_add(1, Ordering::Relaxed);
                self.lock_failed_bytes.fetch_add(len_bytes, Ordering::Relaxed);
                emit_memory_lock_failure_observability(category, self.locked_bytes.load(Ordering::Relaxed));
                if self.warn_only {
                    warn!(
                        "Failed to lock {} memory region of {} bytes, continuing without mlock: {}",
                        category.as_str(),
                        len,
                        error
                    );
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn unlock_region(&self, handle: MemoryLockHandle) -> RocketMQResult<()> {
        self.unlock_region_with(handle, munlock)
    }

    /// Runs the unlock operation through an injected compatibility callback.
    ///
    /// This hidden compatibility seam is public only so the Store facade can keep deterministic
    /// tests while the mapped-file owner is migrated in a later slice.
    ///
    /// # Errors
    ///
    /// Returns the injected unlock error when strict locking is enabled.
    #[doc(hidden)]
    pub fn unlock_region_with<F>(&self, handle: MemoryLockHandle, mut unlocker: F) -> RocketMQResult<()>
    where
        F: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        match unlocker(handle.addr(), handle.len()) {
            Ok(()) => {
                self.release_locked_bytes(handle.len() as u64);
                emit_memory_lock_locked_bytes_observability(
                    handle.category(),
                    self.locked_bytes.load(Ordering::Relaxed),
                );
                Ok(())
            }
            Err(error) => {
                emit_memory_unlock_failure_observability(handle.category(), self.locked_bytes.load(Ordering::Relaxed));
                if self.warn_only {
                    warn!(
                        "Failed to unlock {} memory region of {} bytes, continuing: {}",
                        handle.category().as_str(),
                        handle.len(),
                        error
                    );
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    fn reserve_lock_budget(&self, len: u64) -> bool {
        let budget = self.budget_bytes.load(Ordering::Relaxed);
        if budget == 0 {
            return true;
        }
        let mut current = self.locked_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(len) else {
                return false;
            };
            if next > budget {
                return false;
            }
            match self
                .locked_bytes
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn release_reserved_budget(&self, len: u64) {
        if self.budget_bytes.load(Ordering::Relaxed) != 0 {
            self.locked_bytes.fetch_sub(len, Ordering::AcqRel);
        }
    }

    fn release_locked_bytes(&self, len: u64) {
        let _ = self
            .locked_bytes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(len))
            });
    }

    pub fn lock_attempt_count(&self) -> usize {
        self.lock_attempts.load(Ordering::Relaxed)
    }

    pub fn locked_buffer_count(&self) -> usize {
        self.locked_buffers.load(Ordering::Relaxed)
    }

    pub fn lock_failed_buffer_count(&self) -> usize {
        self.lock_failed_buffers.load(Ordering::Relaxed)
    }

    pub fn lock_skipped_buffer_count(&self) -> usize {
        self.lock_skipped_buffers.load(Ordering::Relaxed)
    }

    pub fn locked_bytes(&self) -> u64 {
        self.locked_bytes.load(Ordering::Relaxed)
    }

    pub fn lock_failed_bytes(&self) -> u64 {
        self.lock_failed_bytes.load(Ordering::Relaxed)
    }

    pub fn lock_skipped_bytes(&self) -> u64 {
        self.lock_skipped_bytes.load(Ordering::Relaxed)
    }
}

#[cfg(any(test, feature = "observability"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryLockAttemptObservabilityEvent {
    category: &'static str,
    count: u64,
}

#[cfg(any(test, feature = "observability"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryLockSuccessObservabilityEvent {
    category: &'static str,
    count: u64,
    locked_bytes: u64,
}

#[cfg(any(test, feature = "observability"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryLockSkipObservabilityEvent {
    category: &'static str,
    reason: &'static str,
    count: u64,
    locked_bytes: u64,
}

#[cfg(any(test, feature = "observability"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryLockFailureObservabilityEvent {
    category: &'static str,
    errno: i32,
    count: u64,
    locked_bytes: u64,
}

#[cfg(any(test, feature = "observability"))]
fn memory_lock_attempt_observability_event(category: MemoryLockCategory) -> MemoryLockAttemptObservabilityEvent {
    MemoryLockAttemptObservabilityEvent {
        category: category.as_str(),
        count: 1,
    }
}

#[cfg(any(test, feature = "observability"))]
fn memory_lock_success_observability_event(
    category: MemoryLockCategory,
    locked_bytes: u64,
) -> MemoryLockSuccessObservabilityEvent {
    MemoryLockSuccessObservabilityEvent {
        category: category.as_str(),
        count: 1,
        locked_bytes,
    }
}

#[cfg(any(test, feature = "observability"))]
fn memory_lock_skip_observability_event(
    category: MemoryLockCategory,
    locked_bytes: u64,
) -> MemoryLockSkipObservabilityEvent {
    MemoryLockSkipObservabilityEvent {
        category: category.as_str(),
        reason: MEMORY_LOCK_BUDGET_EXHAUSTED_REASON,
        count: 1,
        locked_bytes,
    }
}

#[cfg(any(test, feature = "observability"))]
fn memory_lock_failure_observability_event(
    category: MemoryLockCategory,
    locked_bytes: u64,
) -> MemoryLockFailureObservabilityEvent {
    MemoryLockFailureObservabilityEvent {
        category: category.as_str(),
        errno: MEMORY_LOCK_UNKNOWN_ERRNO,
        count: 1,
        locked_bytes,
    }
}

fn emit_memory_lock_attempt_observability(category: MemoryLockCategory) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_attempt_observability_event(category);
        rocketmq_observability::metrics::store::record_linux_mlock_attempt(event.category, event.count);
    }

    #[cfg(not(feature = "observability"))]
    let _ = category;
}

fn emit_memory_lock_success_observability(category: MemoryLockCategory, locked_bytes: u64) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_success_observability_event(category, locked_bytes);
        rocketmq_observability::metrics::store::record_linux_mlock_success(event.category, event.count);
        rocketmq_observability::metrics::store::record_linux_locked_bytes(event.category, event.locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = (category, locked_bytes);
}

fn emit_memory_lock_skip_observability(category: MemoryLockCategory, locked_bytes: u64) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_skip_observability_event(category, locked_bytes);
        rocketmq_observability::metrics::store::record_linux_mlock_skipped(event.category, event.reason, event.count);
        rocketmq_observability::metrics::store::record_linux_locked_bytes(event.category, event.locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = (category, locked_bytes);
}

fn emit_memory_lock_failure_observability(category: MemoryLockCategory, locked_bytes: u64) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_failure_observability_event(category, locked_bytes);
        rocketmq_observability::metrics::store::record_linux_mlock_failure(event.category, event.errno, event.count);
        rocketmq_observability::metrics::store::record_linux_locked_bytes(event.category, event.locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = (category, locked_bytes);
}

fn emit_memory_lock_locked_bytes_observability(category: MemoryLockCategory, locked_bytes: u64) {
    #[cfg(feature = "observability")]
    rocketmq_observability::metrics::store::record_linux_locked_bytes(category.as_str(), locked_bytes);

    #[cfg(not(feature = "observability"))]
    let _ = (category, locked_bytes);
}

fn emit_memory_unlock_failure_observability(category: MemoryLockCategory, locked_bytes: u64) {
    #[cfg(feature = "observability")]
    {
        rocketmq_observability::metrics::store::record_linux_munlock_failure(
            category.as_str(),
            MEMORY_LOCK_UNKNOWN_ERRNO,
            1,
        );
        rocketmq_observability::metrics::store::record_linux_locked_bytes(category.as_str(), locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = (category, locked_bytes);
}

impl Default for MemoryLockManager {
    fn default() -> Self {
        Self::warn_only()
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;

    use super::*;

    #[test]
    fn warn_only_manager_records_failure_without_returning_error() {
        let manager = MemoryLockManager::warn_only();

        let result = manager.lock_buffer_with(std::ptr::null(), 4096, |_, _| {
            Err(RocketMQError::StorageLockFailed {
                path: "test mlock failure".to_string(),
            })
        });

        assert!(result.is_ok());
        assert_eq!(manager.locked_buffer_count(), 0);
        assert_eq!(manager.lock_failed_buffer_count(), 1);
    }

    #[test]
    fn warn_only_manager_skips_lock_when_budget_is_exhausted() {
        let manager = MemoryLockManager::warn_only_with_budget(4096);
        let mut called = false;

        let result = manager.lock_buffer_with(std::ptr::null(), 8192, |_, _| {
            called = true;
            Ok(())
        });

        assert!(result.is_ok());
        assert!(!called);
        assert_eq!(manager.lock_attempt_count(), 1);
        assert_eq!(manager.locked_buffer_count(), 0);
        assert_eq!(manager.lock_skipped_buffer_count(), 1);
        assert_eq!(manager.lock_skipped_bytes(), 8192);
        assert_eq!(manager.locked_bytes(), 0);
    }

    #[test]
    fn memory_lock_success_observability_event_uses_transient_pool_category() {
        let event = memory_lock_success_observability_event(MemoryLockCategory::TransientStorePool, 4096);

        assert_eq!(event.category, TRANSIENT_STORE_POOL_CATEGORY);
        assert_eq!(event.count, 1);
        assert_eq!(event.locked_bytes, 4096);
    }

    #[test]
    fn memory_lock_observability_events_use_requested_category() {
        let attempt = memory_lock_attempt_observability_event(MemoryLockCategory::CommitLogActiveWindow);
        let success = memory_lock_success_observability_event(MemoryLockCategory::CommitLogActiveWindow, 4096);
        let skipped = memory_lock_skip_observability_event(MemoryLockCategory::CommitLogActiveWindow, 2048);
        let failure = memory_lock_failure_observability_event(MemoryLockCategory::CommitLogActiveFile, 1024);

        assert_eq!(attempt.category, "commitlog_active_window");
        assert_eq!(success.category, "commitlog_active_window");
        assert_eq!(success.locked_bytes, 4096);
        assert_eq!(skipped.category, "commitlog_active_window");
        assert_eq!(skipped.reason, MEMORY_LOCK_BUDGET_EXHAUSTED_REASON);
        assert_eq!(failure.category, "commitlog_active_file");
        assert_eq!(failure.errno, MEMORY_LOCK_UNKNOWN_ERRNO);
    }

    #[test]
    fn lock_region_handle_releases_reserved_budget_on_unlock() {
        let manager = MemoryLockManager::warn_only_with_budget(8192);
        let addr = std::ptr::NonNull::<u8>::dangling().as_ptr();

        let handle = manager
            .lock_region_with(MemoryLockCategory::CommitLogActiveWindow, addr, 4096, |_, _| Ok(()))
            .expect("lock should not fail")
            .expect("successful lock should return handle");

        assert_eq!(handle.category(), MemoryLockCategory::CommitLogActiveWindow);
        assert_eq!(handle.len(), 4096);
        assert_eq!(manager.locked_bytes(), 4096);

        let mut unlocked = false;
        manager
            .unlock_region_with(handle, |unlock_addr, unlock_len| {
                unlocked = true;
                assert_eq!(unlock_addr, addr);
                assert_eq!(unlock_len, 4096);
                Ok(())
            })
            .expect("unlock should not fail");

        assert!(unlocked);
        assert_eq!(manager.locked_bytes(), 0);
    }

    #[test]
    fn memory_lock_skip_observability_event_uses_budget_reason() {
        let event = memory_lock_skip_observability_event(MemoryLockCategory::TransientStorePool, 2048);

        assert_eq!(event.category, TRANSIENT_STORE_POOL_CATEGORY);
        assert_eq!(event.reason, MEMORY_LOCK_BUDGET_EXHAUSTED_REASON);
        assert_eq!(event.count, 1);
        assert_eq!(event.locked_bytes, 2048);
    }

    #[test]
    fn memory_lock_failure_observability_event_uses_unknown_errno_until_syscall_exposes_it() {
        let event = memory_lock_failure_observability_event(MemoryLockCategory::TransientStorePool, 1024);

        assert_eq!(event.category, TRANSIENT_STORE_POOL_CATEGORY);
        assert_eq!(event.errno, MEMORY_LOCK_UNKNOWN_ERRNO);
        assert_eq!(event.count, 1);
        assert_eq!(event.locked_bytes, 1024);
    }
}
