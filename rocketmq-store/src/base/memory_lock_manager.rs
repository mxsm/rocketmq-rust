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

#[cfg(any(test, feature = "observability"))]
const TRANSIENT_STORE_POOL_CATEGORY: &str = "transient_store_pool";
#[cfg(any(test, feature = "observability"))]
const MEMORY_LOCK_BUDGET_EXHAUSTED_REASON: &str = "budget_exhausted";
#[cfg(any(test, feature = "observability"))]
const MEMORY_LOCK_UNKNOWN_ERRNO: i32 = 0;

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
    pub fn warn_only() -> Self {
        Self::warn_only_with_budget(0)
    }

    pub fn warn_only_with_budget(budget_bytes: u64) -> Self {
        Self {
            warn_only: true,
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

    pub fn lock_buffer(&self, addr: *const u8, len: usize) -> RocketMQResult<()> {
        self.lock_buffer_with(addr, len, mlock)
    }

    pub(crate) fn lock_buffer_with<F>(&self, addr: *const u8, len: usize, mut locker: F) -> RocketMQResult<()>
    where
        F: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        self.lock_attempts.fetch_add(1, Ordering::Relaxed);
        emit_memory_lock_attempt_observability();

        let len_bytes = len as u64;
        if !self.reserve_lock_budget(len_bytes) {
            self.lock_skipped_buffers.fetch_add(1, Ordering::Relaxed);
            self.lock_skipped_bytes.fetch_add(len_bytes, Ordering::Relaxed);
            emit_memory_lock_skip_observability(self.locked_bytes.load(Ordering::Relaxed));
            if self.warn_only {
                warn!(
                    "Skipped memory lock of {} bytes because lock budget {} bytes is exhausted",
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

        match locker(addr, len) {
            Ok(()) => {
                self.locked_buffers.fetch_add(1, Ordering::Relaxed);
                if self.budget_bytes.load(Ordering::Relaxed) == 0 {
                    self.locked_bytes.fetch_add(len_bytes, Ordering::Relaxed);
                }
                emit_memory_lock_success_observability(self.locked_bytes.load(Ordering::Relaxed));
                Ok(())
            }
            Err(error) => {
                self.release_reserved_budget(len_bytes);
                self.lock_failed_buffers.fetch_add(1, Ordering::Relaxed);
                self.lock_failed_bytes.fetch_add(len_bytes, Ordering::Relaxed);
                emit_memory_lock_failure_observability(self.locked_bytes.load(Ordering::Relaxed));
                if self.warn_only {
                    warn!(
                        "Failed to lock memory buffer of {} bytes, continuing without mlock: {}",
                        len, error
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
fn memory_lock_attempt_observability_event() -> MemoryLockAttemptObservabilityEvent {
    MemoryLockAttemptObservabilityEvent {
        category: TRANSIENT_STORE_POOL_CATEGORY,
        count: 1,
    }
}

#[cfg(any(test, feature = "observability"))]
fn memory_lock_success_observability_event(locked_bytes: u64) -> MemoryLockSuccessObservabilityEvent {
    MemoryLockSuccessObservabilityEvent {
        category: TRANSIENT_STORE_POOL_CATEGORY,
        count: 1,
        locked_bytes,
    }
}

#[cfg(any(test, feature = "observability"))]
fn memory_lock_skip_observability_event(locked_bytes: u64) -> MemoryLockSkipObservabilityEvent {
    MemoryLockSkipObservabilityEvent {
        category: TRANSIENT_STORE_POOL_CATEGORY,
        reason: MEMORY_LOCK_BUDGET_EXHAUSTED_REASON,
        count: 1,
        locked_bytes,
    }
}

#[cfg(any(test, feature = "observability"))]
fn memory_lock_failure_observability_event(locked_bytes: u64) -> MemoryLockFailureObservabilityEvent {
    MemoryLockFailureObservabilityEvent {
        category: TRANSIENT_STORE_POOL_CATEGORY,
        errno: MEMORY_LOCK_UNKNOWN_ERRNO,
        count: 1,
        locked_bytes,
    }
}

fn emit_memory_lock_attempt_observability() {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_attempt_observability_event();
        rocketmq_observability::metrics::store::record_linux_mlock_attempt(event.category, event.count);
    }
}

fn emit_memory_lock_success_observability(locked_bytes: u64) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_success_observability_event(locked_bytes);
        rocketmq_observability::metrics::store::record_linux_mlock_success(event.category, event.count);
        rocketmq_observability::metrics::store::record_linux_locked_bytes(event.category, event.locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = locked_bytes;
}

fn emit_memory_lock_skip_observability(locked_bytes: u64) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_skip_observability_event(locked_bytes);
        rocketmq_observability::metrics::store::record_linux_mlock_skipped(event.category, event.reason, event.count);
        rocketmq_observability::metrics::store::record_linux_locked_bytes(event.category, event.locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = locked_bytes;
}

fn emit_memory_lock_failure_observability(locked_bytes: u64) {
    #[cfg(feature = "observability")]
    {
        let event = memory_lock_failure_observability_event(locked_bytes);
        rocketmq_observability::metrics::store::record_linux_mlock_failure(event.category, event.errno, event.count);
        rocketmq_observability::metrics::store::record_linux_locked_bytes(event.category, event.locked_bytes);
    }

    #[cfg(not(feature = "observability"))]
    let _ = locked_bytes;
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
        let event = memory_lock_success_observability_event(4096);

        assert_eq!(event.category, TRANSIENT_STORE_POOL_CATEGORY);
        assert_eq!(event.count, 1);
        assert_eq!(event.locked_bytes, 4096);
    }

    #[test]
    fn memory_lock_skip_observability_event_uses_budget_reason() {
        let event = memory_lock_skip_observability_event(2048);

        assert_eq!(event.category, TRANSIENT_STORE_POOL_CATEGORY);
        assert_eq!(event.reason, MEMORY_LOCK_BUDGET_EXHAUSTED_REASON);
        assert_eq!(event.count, 1);
        assert_eq!(event.locked_bytes, 2048);
    }

    #[test]
    fn memory_lock_failure_observability_event_uses_unknown_errno_until_syscall_exposes_it() {
        let event = memory_lock_failure_observability_event(1024);

        assert_eq!(event.category, TRANSIENT_STORE_POOL_CATEGORY);
        assert_eq!(event.errno, MEMORY_LOCK_UNKNOWN_ERRNO);
        assert_eq!(event.count, 1);
        assert_eq!(event.locked_bytes, 1024);
    }
}
