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

use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use rocketmq_error::RocketMQResult;
use tracing::warn;

use crate::mapped_file::lifecycle::MappedFileLease;
use crate::utils::ffi::lock_memory_region;
use crate::utils::ffi::unlock_memory_region;

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

trait LockedMemoryRegion: Send + Sync {
    fn as_slice(&self) -> &[u8];
}

impl<T> LockedMemoryRegion for T
where
    T: AsRef<[u8]> + Send + Sync,
{
    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }
}

#[must_use = "an armed memory-lock handle must be unlocked or retained for a later retry"]
pub struct MemoryLockHandle {
    mapped_file_lease: Option<MappedFileLease>,
    region: Box<dyn LockedMemoryRegion>,
    category: MemoryLockCategory,
    locked: bool,
}

impl MemoryLockHandle {
    fn new(region: Box<dyn LockedMemoryRegion>, category: MemoryLockCategory) -> Self {
        Self {
            mapped_file_lease: None,
            region,
            category,
            locked: true,
        }
    }

    fn region(&self) -> &[u8] {
        self.region.as_slice()
    }

    pub fn len(&self) -> usize {
        self.region().len()
    }

    pub fn is_empty(&self) -> bool {
        self.region().is_empty()
    }

    pub fn category(&self) -> MemoryLockCategory {
        self.category
    }

    pub(crate) fn attach_mapped_file_lease(&mut self, lease: MappedFileLease) {
        debug_assert!(self.mapped_file_lease.is_none());
        self.mapped_file_lease = Some(lease);
    }

    fn release_mapped_file_lease(&mut self) {
        drop(self.mapped_file_lease.take());
    }
}

impl fmt::Debug for MemoryLockHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryLockHandle")
            .field("len", &self.len())
            .field("category", &self.category)
            .finish_non_exhaustive()
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
    #[cfg(feature = "observability")]
    store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
}

impl MemoryLockManager {
    pub fn new(warn_only: bool, budget_bytes: u64) -> Self {
        #[cfg(feature = "observability")]
        {
            Self::new_with_store_metrics(
                warn_only,
                budget_bytes,
                rocketmq_observability::metrics::store::StoreMetricsRecorder::noop(),
            )
        }

        #[cfg(not(feature = "observability"))]
        {
            Self::new_without_store_metrics(warn_only, budget_bytes)
        }
    }

    /// Binds memory-lock observations to the owning Store telemetry instance.
    #[cfg(feature = "observability")]
    #[doc(hidden)]
    pub fn new_with_store_metrics(
        warn_only: bool,
        budget_bytes: u64,
        store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) -> Self {
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
            store_metrics,
        }
    }

    #[cfg(not(feature = "observability"))]
    fn new_without_store_metrics(warn_only: bool, budget_bytes: u64) -> Self {
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

    pub fn lock_buffer(&self, memory: &[u8]) -> RocketMQResult<()> {
        self.lock_buffer_with(memory, lock_memory_region)
    }

    /// Runs the lock operation through an injected compatibility callback.
    ///
    /// The Local production `TransientStorePool` owner is the sole production caller. This seam
    /// remains crate-private for deterministic Local tests during the storage-boundary migration.
    ///
    /// # Errors
    ///
    /// Returns the injected lock error when strict locking is enabled or budget reservation fails.
    pub(crate) fn lock_buffer_with<F>(&self, memory: &[u8], mut locker: F) -> RocketMQResult<()>
    where
        F: FnMut(&[u8]) -> RocketMQResult<()>,
    {
        let len = memory.len();
        self.lock_attempts.fetch_add(1, Ordering::Relaxed);
        emit_memory_lock_attempt_observability(self, MemoryLockCategory::TransientStorePool);

        let len_bytes = len as u64;
        if !self.reserve_lock_budget(len_bytes) {
            self.lock_skipped_buffers.fetch_add(1, Ordering::Relaxed);
            self.lock_skipped_bytes.fetch_add(len_bytes, Ordering::Relaxed);
            emit_memory_lock_skip_observability(
                self,
                MemoryLockCategory::TransientStorePool,
                self.locked_bytes.load(Ordering::Relaxed),
            );
            if self.warn_only {
                warn!(
                    "Skipped {} memory lock of {} bytes because lock budget {} bytes is exhausted",
                    MemoryLockCategory::TransientStorePool.as_str(),
                    len_bytes,
                    self.budget_bytes.load(Ordering::Relaxed)
                );
                return Ok(());
            }
            return Err(rocketmq_error::RocketMQError::StorageLockFailed {
                path: format!(
                    "memory lock budget exhausted: requested={} budget={}",
                    len_bytes,
                    self.budget_bytes.load(Ordering::Relaxed)
                ),
            });
        }

        match locker(memory) {
            Ok(()) => {
                self.locked_buffers.fetch_add(1, Ordering::Relaxed);
                if self.budget_bytes.load(Ordering::Relaxed) == 0 {
                    self.locked_bytes.fetch_add(len_bytes, Ordering::Relaxed);
                }
                emit_memory_lock_success_observability(
                    self,
                    MemoryLockCategory::TransientStorePool,
                    self.locked_bytes.load(Ordering::Relaxed),
                );
                Ok(())
            }
            Err(error) => {
                self.release_reserved_budget(len_bytes);
                self.lock_failed_buffers.fetch_add(1, Ordering::Relaxed);
                self.lock_failed_bytes.fetch_add(len_bytes, Ordering::Relaxed);
                emit_memory_lock_failure_observability(
                    self,
                    MemoryLockCategory::TransientStorePool,
                    self.locked_bytes.load(Ordering::Relaxed),
                );
                if self.warn_only {
                    warn!(
                        "Failed to lock {} memory region of {} bytes, continuing without mlock: {}",
                        MemoryLockCategory::TransientStorePool.as_str(),
                        len,
                        error
                    );
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    pub fn lock_region<R>(&self, category: MemoryLockCategory, region: R) -> RocketMQResult<Option<MemoryLockHandle>>
    where
        R: AsRef<[u8]> + Send + Sync + 'static,
    {
        self.lock_region_with(category, region, lock_memory_region)
    }

    /// Runs a categorized lock through an injected compatibility callback.
    ///
    /// During the storage-boundary migration, this hidden compatibility seam remains public for
    /// Store production adapters that implement `DefaultMappedFile` range locking and the
    /// CommitLog active-lock lifecycle, as well as deterministic Store/Local tests.
    ///
    /// # Errors
    ///
    /// Returns the injected lock error when strict locking is enabled or budget reservation fails.
    #[doc(hidden)]
    pub fn lock_region_with<R, F>(
        &self,
        category: MemoryLockCategory,
        region: R,
        mut locker: F,
    ) -> RocketMQResult<Option<MemoryLockHandle>>
    where
        R: AsRef<[u8]> + Send + Sync + 'static,
        F: FnMut(&[u8]) -> RocketMQResult<()>,
    {
        let region: Box<dyn LockedMemoryRegion> = Box::new(region);
        let len = region.as_slice().len();
        self.lock_attempts.fetch_add(1, Ordering::Relaxed);
        emit_memory_lock_attempt_observability(self, category);

        let len_bytes = len as u64;
        if !self.reserve_lock_budget(len_bytes) {
            self.lock_skipped_buffers.fetch_add(1, Ordering::Relaxed);
            self.lock_skipped_bytes.fetch_add(len_bytes, Ordering::Relaxed);
            emit_memory_lock_skip_observability(self, category, self.locked_bytes.load(Ordering::Relaxed));
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

        match locker(region.as_slice()) {
            Ok(()) => {
                self.locked_buffers.fetch_add(1, Ordering::Relaxed);
                if self.budget_bytes.load(Ordering::Relaxed) == 0 {
                    self.locked_bytes.fetch_add(len_bytes, Ordering::Relaxed);
                }
                emit_memory_lock_success_observability(self, category, self.locked_bytes.load(Ordering::Relaxed));
                Ok(Some(MemoryLockHandle::new(region, category)))
            }
            Err(error) => {
                self.release_reserved_budget(len_bytes);
                self.lock_failed_buffers.fetch_add(1, Ordering::Relaxed);
                self.lock_failed_bytes.fetch_add(len_bytes, Ordering::Relaxed);
                emit_memory_lock_failure_observability(self, category, self.locked_bytes.load(Ordering::Relaxed));
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

    pub fn unlock_region(&self, handle: &mut MemoryLockHandle) -> RocketMQResult<()> {
        self.unlock_region_with(handle, unlock_memory_region)
    }

    /// Runs the unlock operation through an injected compatibility callback.
    ///
    /// During the storage-boundary migration, this hidden compatibility seam remains public for
    /// Store production adapters that implement `DefaultMappedFile` range unlocking and the
    /// CommitLog active-lock lifecycle, as well as deterministic Store/Local tests.
    ///
    /// # Errors
    ///
    /// A failed unlock leaves `handle` armed and its owned region live so the caller can retry.
    /// Unlock failures are returned even in warn-only mode because discarding an armed handle
    /// would lose both the region owner and the lock-accounting identity. Repeating the operation
    /// after a successful unlock is an idempotent no-op.
    #[doc(hidden)]
    pub fn unlock_region_with<F>(&self, handle: &mut MemoryLockHandle, mut unlocker: F) -> RocketMQResult<()>
    where
        F: FnMut(&[u8]) -> RocketMQResult<()>,
    {
        if !handle.locked {
            return Ok(());
        }

        match unlocker(handle.region()) {
            Ok(()) => {
                handle.locked = false;
                self.release_locked_bytes(handle.len() as u64);
                emit_memory_lock_locked_bytes_observability(
                    self,
                    handle.category(),
                    self.locked_bytes.load(Ordering::Relaxed),
                );
                handle.release_mapped_file_lease();
                Ok(())
            }
            Err(error) => {
                emit_memory_unlock_failure_observability(
                    self,
                    handle.category(),
                    self.locked_bytes.load(Ordering::Relaxed),
                );
                if self.warn_only {
                    warn!(
                        "Failed to unlock {} memory region of {} bytes; retaining handle for retry: {}",
                        handle.category().as_str(),
                        handle.len(),
                        error
                    );
                }
                Err(error)
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

fn emit_memory_lock_attempt_observability(manager: &MemoryLockManager, category: MemoryLockCategory) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_attempt_observability_event(category);
        manager
            .store_metrics
            .record_linux_mlock_attempt(event.category, event.count);
    }

    #[cfg(not(feature = "observability"))]
    let _ = (manager, category);
}

fn emit_memory_lock_success_observability(
    manager: &MemoryLockManager,
    category: MemoryLockCategory,
    locked_bytes: u64,
) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_success_observability_event(category, locked_bytes);
        manager
            .store_metrics
            .record_linux_mlock_success(event.category, event.count);
        manager
            .store_metrics
            .record_linux_locked_bytes(event.category, event.locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = (manager, category, locked_bytes);
}

fn emit_memory_lock_skip_observability(manager: &MemoryLockManager, category: MemoryLockCategory, locked_bytes: u64) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_skip_observability_event(category, locked_bytes);
        manager
            .store_metrics
            .record_linux_mlock_skipped(event.category, event.reason, event.count);
        manager
            .store_metrics
            .record_linux_locked_bytes(event.category, event.locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = (manager, category, locked_bytes);
}

fn emit_memory_lock_failure_observability(
    manager: &MemoryLockManager,
    category: MemoryLockCategory,
    locked_bytes: u64,
) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_failure_observability_event(category, locked_bytes);
        manager
            .store_metrics
            .record_linux_mlock_failure(event.category, event.errno, event.count);
        manager
            .store_metrics
            .record_linux_locked_bytes(event.category, event.locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = (manager, category, locked_bytes);
}

fn emit_memory_lock_locked_bytes_observability(
    manager: &MemoryLockManager,
    category: MemoryLockCategory,
    locked_bytes: u64,
) {
    #[cfg(feature = "observability")]
    manager
        .store_metrics
        .record_linux_locked_bytes(category.as_str(), locked_bytes);

    #[cfg(not(feature = "observability"))]
    let _ = (manager, category, locked_bytes);
}

fn emit_memory_unlock_failure_observability(
    manager: &MemoryLockManager,
    category: MemoryLockCategory,
    locked_bytes: u64,
) {
    #[cfg(feature = "observability")]
    {
        manager
            .store_metrics
            .record_linux_munlock_failure(category.as_str(), MEMORY_LOCK_UNKNOWN_ERRNO, 1);
        manager
            .store_metrics
            .record_linux_locked_bytes(category.as_str(), locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = (manager, category, locked_bytes);
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

        let memory = vec![0u8; 4096];
        let result = manager.lock_buffer_with(&memory, |_| {
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

        let memory = vec![0u8; 8192];
        let result = manager.lock_buffer_with(&memory, |_| {
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
        let region = vec![0u8; 4096];
        let addr = region.as_ptr();

        let mut handle = manager
            .lock_region_with(MemoryLockCategory::CommitLogActiveWindow, region, |_| Ok(()))
            .expect("lock should not fail")
            .expect("successful lock should return handle");

        assert_eq!(handle.category(), MemoryLockCategory::CommitLogActiveWindow);
        assert_eq!(handle.len(), 4096);
        assert!(handle.mapped_file_lease.is_none());
        assert_eq!(manager.locked_bytes(), 4096);

        let mut unlocked = false;
        manager
            .unlock_region_with(&mut handle, |memory| {
                unlocked = true;
                assert_eq!(memory.as_ptr(), addr);
                assert_eq!(memory.len(), 4096);
                Ok(())
            })
            .expect("unlock should not fail");

        assert!(unlocked);
        assert_eq!(manager.locked_bytes(), 0);
    }

    #[test]
    fn inline_region_owner_is_fixed_before_lock_and_unlock() {
        let manager = MemoryLockManager::warn_only_with_budget(32);
        let mut locked_addr = std::ptr::null();

        let mut handle = manager
            .lock_region_with(MemoryLockCategory::CommitLogActiveWindow, [0u8; 32], |memory| {
                locked_addr = memory.as_ptr();
                Ok(())
            })
            .expect("lock should not fail")
            .expect("successful lock should return handle");

        manager
            .unlock_region_with(&mut handle, |memory| {
                assert_eq!(memory.as_ptr(), locked_addr);
                assert_eq!(memory.len(), 32);
                Ok(())
            })
            .expect("unlock should not fail");
    }

    #[test]
    fn failed_unlock_retains_owner_and_budget_for_retry() {
        struct TrackedRegion {
            memory: Box<[u8]>,
            drops: std::sync::Arc<AtomicUsize>,
        }

        impl AsRef<[u8]> for TrackedRegion {
            fn as_ref(&self) -> &[u8] {
                &self.memory
            }
        }

        impl Drop for TrackedRegion {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }

        let manager = MemoryLockManager::warn_only_with_budget(64);
        let drops = std::sync::Arc::new(AtomicUsize::new(0));
        let region = TrackedRegion {
            memory: vec![0u8; 64].into_boxed_slice(),
            drops: std::sync::Arc::clone(&drops),
        };
        let locked_addr = region.as_ref().as_ptr();
        let mut handle = manager
            .lock_region_with(MemoryLockCategory::CommitLogActiveWindow, region, |_| Ok(()))
            .expect("lock should not fail")
            .expect("successful lock should return handle");

        let error = manager
            .unlock_region_with(&mut handle, |_| {
                Err(rocketmq_error::RocketMQError::StorageLockFailed {
                    path: "retryable unlock failure".to_string(),
                })
            })
            .expect_err("warn-only unlock failure must remain observable");

        assert!(matches!(
            error,
            rocketmq_error::RocketMQError::StorageLockFailed { path }
                if path == "retryable unlock failure"
        ));
        assert_eq!(manager.locked_bytes(), 64);
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        manager
            .unlock_region_with(&mut handle, |memory| {
                assert_eq!(memory.as_ptr(), locked_addr);
                Ok(())
            })
            .expect("retry should unlock the retained region");
        assert_eq!(manager.locked_bytes(), 0);
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        manager
            .unlock_region_with(&mut handle, |_| panic!("disarmed handle must be an idempotent no-op"))
            .expect("repeated unlock after success should be a no-op");
        drop(handle);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
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
