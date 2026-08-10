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

use std::collections::VecDeque;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Condvar;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tracing::warn;

use crate::base::memory_lock_manager::MemoryLockManager;
use crate::utils::ffi::lock_memory_region;
use crate::utils::ffi::unlock_memory_region;

type BufferUnlocker = dyn Fn(&[u8]) -> RocketMQResult<()> + Send + Sync;

struct TransientStorePoolInner {
    pool_size: usize,
    file_size: usize,
    state: Mutex<TransientStorePoolState>,
    leases_returned: Condvar,
    is_real_commit: Mutex<bool>,
    memory_lock_manager: Arc<MemoryLockManager>,
    late_unlocker: Arc<BufferUnlocker>,
}

struct TransientStorePoolState {
    accepting: bool,
    available_buffers: VecDeque<Vec<u8>>,
    outstanding_leases: usize,
}

/// Pool of locked transient write buffers shared by mapped-file allocations.
#[derive(Clone)]
pub struct TransientStorePool {
    inner: Arc<TransientStorePoolInner>,
}

/// Exclusive ownership of one transient write buffer.
///
/// Dropping the lease returns its buffer while the pool is accepting. Once shutdown starts, a
/// late lease instead unlocks and drops its buffer through the retained pool inner.
pub struct PoolLease {
    pool: Arc<TransientStorePoolInner>,
    buffer: Option<Vec<u8>>,
}

impl PoolLease {
    /// Returns the buffer immediately instead of waiting for `Drop`.
    pub fn return_now(mut self) {
        self.return_buffer();
    }

    /// Returns the size of the leased buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.as_ref().map_or(0, Vec::len)
    }

    /// Returns whether the leased buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn return_buffer(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        self.pool.return_leased_buffer(buffer);
    }
}

impl Deref for PoolLease {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer.as_deref().unwrap_or_default()
    }
}

impl DerefMut for PoolLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_deref_mut().unwrap_or_default()
    }
}

impl Drop for PoolLease {
    fn drop(&mut self) {
        self.return_buffer();
    }
}

/// Outcome of stopping new transient-buffer leases and draining available buffers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransientStorePoolShutdownReport {
    drained_buffers: usize,
    outstanding_leases: usize,
    timed_out: bool,
}

impl TransientStorePoolShutdownReport {
    /// Number of available buffers removed and unlocked during shutdown.
    #[inline]
    pub const fn drained_buffers(self) -> usize {
        self.drained_buffers
    }

    /// Number of leases still held when the configured wait ended.
    #[inline]
    pub const fn outstanding_leases(self) -> usize {
        self.outstanding_leases
    }

    /// Whether the wait ended while leases were still outstanding.
    #[inline]
    pub const fn timed_out(self) -> bool {
        self.timed_out
    }
}

impl TransientStorePool {
    pub fn new(pool_size: usize, file_size: usize) -> Self {
        Self::new_with_memory_lock_budget(pool_size, file_size, 0)
    }

    pub fn new_with_memory_lock_budget(pool_size: usize, file_size: usize, memory_lock_budget_bytes: u64) -> Self {
        Self::with_manager(
            pool_size,
            file_size,
            Arc::new(MemoryLockManager::warn_only_with_budget(memory_lock_budget_bytes)),
            Arc::new(unlock_memory_region),
        )
    }

    /// Creates a pool whose memory-lock observations use the owning Store recorder.
    #[cfg(feature = "observability")]
    #[doc(hidden)]
    pub fn new_with_memory_lock_budget_and_store_metrics(
        pool_size: usize,
        file_size: usize,
        memory_lock_budget_bytes: u64,
        store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
    ) -> Self {
        Self::with_manager(
            pool_size,
            file_size,
            Arc::new(MemoryLockManager::new_with_store_metrics(
                true,
                memory_lock_budget_bytes,
                store_metrics,
            )),
            Arc::new(unlock_memory_region),
        )
    }

    fn with_manager(
        pool_size: usize,
        file_size: usize,
        memory_lock_manager: Arc<MemoryLockManager>,
        late_unlocker: Arc<BufferUnlocker>,
    ) -> Self {
        Self {
            inner: Arc::new(TransientStorePoolInner {
                pool_size,
                file_size,
                state: Mutex::new(TransientStorePoolState {
                    accepting: true,
                    available_buffers: VecDeque::with_capacity(pool_size),
                    outstanding_leases: 0,
                }),
                leases_returned: Condvar::new(),
                is_real_commit: Mutex::new(true),
                memory_lock_manager,
                late_unlocker,
            }),
        }
    }

    #[cfg(test)]
    fn new_for_test<F>(pool_size: usize, file_size: usize, late_unlocker: F) -> Self
    where
        F: Fn(&[u8]) -> RocketMQResult<()> + Send + Sync + 'static,
    {
        Self::with_manager(
            pool_size,
            file_size,
            Arc::new(MemoryLockManager::warn_only()),
            Arc::new(late_unlocker),
        )
    }

    pub fn init(&self) -> RocketMQResult<()> {
        self.init_with_locker(lock_memory_region)
    }

    pub(crate) fn init_with_locker<F>(&self, mut locker: F) -> RocketMQResult<()>
    where
        F: FnMut(&[u8]) -> RocketMQResult<()>,
    {
        let mut state = self.inner.state.lock();
        if !state.accepting {
            return Err(RocketMQError::IllegalArgument(
                "transient store pool is already shut down".to_owned(),
            ));
        }
        for _ in 0..self.inner.pool_size {
            let buffer = vec![0u8; self.inner.file_size];
            self.inner.memory_lock_manager.lock_buffer_with(&buffer, &mut locker)?;
            state.available_buffers.push_back(buffer);
        }
        Ok(())
    }

    pub fn destroy(&self) -> RocketMQResult<()> {
        self.shutdown(Duration::ZERO).map(|_| ())
    }

    /// Stops new leases, drains currently available buffers, and waits up to `wait_timeout` for
    /// outstanding leases to return.
    pub fn shutdown(&self, wait_timeout: Duration) -> RocketMQResult<TransientStorePoolShutdownReport> {
        self.shutdown_with_unlocker(wait_timeout, unlock_memory_region)
    }

    #[cfg(test)]
    fn destroy_with_unlocker<F>(&self, unlocker: F) -> RocketMQResult<()>
    where
        F: FnMut(&[u8]) -> RocketMQResult<()>,
    {
        self.shutdown_with_unlocker(Duration::ZERO, unlocker).map(|_| ())
    }

    fn shutdown_with_unlocker<F>(
        &self,
        wait_timeout: Duration,
        mut unlocker: F,
    ) -> RocketMQResult<TransientStorePoolShutdownReport>
    where
        F: FnMut(&[u8]) -> RocketMQResult<()>,
    {
        let available_buffers = {
            let mut state = self.inner.state.lock();
            state.accepting = false;
            state.available_buffers.drain(..).collect::<Vec<_>>()
        };
        let drained_buffers = available_buffers.len();
        let mut first_error = None;
        for available_buffer in available_buffers {
            if let Err(error) = unlocker(&available_buffer) {
                first_error = Some(error);
                break;
            }
        }

        let started = Instant::now();
        let mut state = self.inner.state.lock();
        while state.outstanding_leases != 0 {
            let Some(remaining) = wait_timeout.checked_sub(started.elapsed()) else {
                break;
            };
            if remaining.is_zero() || self.inner.leases_returned.wait_for(&mut state, remaining).timed_out() {
                break;
            }
        }
        let outstanding_leases = state.outstanding_leases;
        drop(state);

        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(TransientStorePoolShutdownReport {
            drained_buffers,
            outstanding_leases,
            timed_out: outstanding_leases != 0,
        })
    }

    /// Compatibility buffer return. New code must retain `PoolLease` instead.
    pub fn return_buffer(&self, buffer: Vec<u8>) {
        let should_unlock = {
            let mut state = self.inner.state.lock();
            if state.accepting {
                state.available_buffers.push_front(buffer);
                return;
            }
            true
        };
        if should_unlock {
            self.inner.unlock_late_buffer(buffer);
        }
    }

    /// Compatibility raw borrow. Production mapped-file code must use `borrow_lease`.
    pub fn borrow_buffer(&self) -> Option<Vec<u8>> {
        let mut state = self.inner.state.lock();
        if !state.accepting {
            return None;
        }
        let buffer = state.available_buffers.pop_front();
        if state.available_buffers.len() < self.inner.pool_size / 10 * 4 {
            warn!(
                "TransientStorePool only remain {} sheets.",
                state.available_buffers.len()
            );
        }
        buffer
    }

    /// Borrows one buffer whose `Drop` path is bound to this pool's lifecycle.
    pub fn borrow_lease(&self) -> Option<PoolLease> {
        let mut state = self.inner.state.lock();
        if !state.accepting {
            return None;
        }
        let buffer = state.available_buffers.pop_front()?;
        state.outstanding_leases += 1;
        if state.available_buffers.len() < self.inner.pool_size / 10 * 4 {
            warn!(
                "TransientStorePool only remain {} sheets.",
                state.available_buffers.len()
            );
        }
        Some(PoolLease {
            pool: Arc::clone(&self.inner),
            buffer: Some(buffer),
        })
    }

    pub fn available_buffer_nums(&self) -> usize {
        self.inner.state.lock().available_buffers.len()
    }

    /// Number of buffers currently owned by live leases.
    pub fn outstanding_lease_count(&self) -> usize {
        self.inner.state.lock().outstanding_leases
    }

    pub fn locked_buffer_count(&self) -> usize {
        self.inner.memory_lock_manager.locked_buffer_count()
    }

    pub fn lock_attempt_count(&self) -> usize {
        self.inner.memory_lock_manager.lock_attempt_count()
    }

    pub fn lock_failed_buffer_count(&self) -> usize {
        self.inner.memory_lock_manager.lock_failed_buffer_count()
    }

    pub fn lock_skipped_buffer_count(&self) -> usize {
        self.inner.memory_lock_manager.lock_skipped_buffer_count()
    }

    pub fn locked_bytes(&self) -> u64 {
        self.inner.memory_lock_manager.locked_bytes()
    }

    pub fn lock_failed_bytes(&self) -> u64 {
        self.inner.memory_lock_manager.lock_failed_bytes()
    }

    pub fn lock_skipped_bytes(&self) -> u64 {
        self.inner.memory_lock_manager.lock_skipped_bytes()
    }

    pub fn is_real_commit(&self) -> bool {
        let is_real_commit = self.inner.is_real_commit.lock();
        *is_real_commit
    }

    pub fn set_real_commit(&self, real_commit: bool) {
        let mut is_real_commit = self.inner.is_real_commit.lock();
        *is_real_commit = real_commit;
    }
}

impl TransientStorePoolInner {
    fn return_leased_buffer(&self, buffer: Vec<u8>) {
        let should_unlock = {
            let mut state = self.state.lock();
            debug_assert!(state.outstanding_leases > 0, "pool lease count cannot underflow");
            state.outstanding_leases = state.outstanding_leases.saturating_sub(1);
            if state.accepting {
                state.available_buffers.push_front(buffer);
                self.leases_returned.notify_all();
                return;
            }
            self.leases_returned.notify_all();
            true
        };
        if should_unlock {
            self.unlock_late_buffer(buffer);
        }
    }

    fn unlock_late_buffer(&self, buffer: Vec<u8>) {
        if let Err(error) = (self.late_unlocker)(&buffer) {
            warn!(error = %error, "failed to unlock a transient buffer returned after pool shutdown");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_error::RocketMQError;

    use super::*;

    #[test]
    fn init_keeps_buffers_when_memory_lock_fails_warn_only() {
        let pool = TransientStorePool::new(2, 4096);

        let result = pool.init_with_locker(|_| {
            Err(RocketMQError::StorageLockFailed {
                path: "test mlock failure".to_string(),
            })
        });

        assert!(result.is_ok());
        assert_eq!(pool.available_buffer_nums(), 2);
        assert_eq!(pool.locked_buffer_count(), 0);
        assert_eq!(pool.lock_failed_buffer_count(), 2);
    }

    #[test]
    fn init_records_one_lock_attempt_per_buffer() {
        let pool = TransientStorePool::new(1, 1);

        let result = pool.init();

        assert!(result.is_ok());
        assert_eq!(pool.lock_attempt_count(), 1);
        let _ = pool.destroy();
    }

    #[test]
    fn init_applies_configured_memory_lock_budget() {
        let pool = TransientStorePool::new_with_memory_lock_budget(2, 4096, 4096);

        let result = pool.init_with_locker(|_| Ok(()));

        assert!(result.is_ok());
        assert_eq!(pool.lock_attempt_count(), 2);
        assert_eq!(pool.locked_buffer_count(), 1);
        assert_eq!(pool.lock_skipped_buffer_count(), 1);
        assert_eq!(pool.locked_bytes(), 4096);
        assert_eq!(pool.lock_skipped_bytes(), 4096);
    }

    #[test]
    fn repeated_init_appends_buffers_and_accumulates_lock_statistics() {
        let pool = TransientStorePool::new(2, 16);

        pool.init_with_locker(|_| Ok(())).expect("first init succeeds");
        pool.init_with_locker(|_| Ok(())).expect("second init succeeds");

        assert_eq!(pool.available_buffer_nums(), 4);
        assert_eq!(pool.lock_attempt_count(), 4);
        assert_eq!(pool.locked_buffer_count(), 4);
        assert_eq!(pool.locked_bytes(), 64);
    }

    #[test]
    fn destroy_unlocks_failed_lock_buffers_without_updating_manager_statistics() {
        let pool = TransientStorePool::new(2, 32);
        pool.init_with_locker(|_| {
            Err(RocketMQError::StorageLockFailed {
                path: "injected lock failure".to_string(),
            })
        })
        .expect("warn-only init keeps failed buffers");
        let mut unlock_calls = 0;

        pool.destroy_with_unlocker(|memory| {
            unlock_calls += 1;
            assert_eq!(memory.len(), 32);
            Ok(())
        })
        .expect("injected unlock succeeds");

        assert_eq!(unlock_calls, 2);
        assert_eq!(pool.available_buffer_nums(), 0);
        assert_eq!(pool.locked_buffer_count(), 0);
        assert_eq!(pool.lock_failed_buffer_count(), 2);
        assert_eq!(pool.lock_failed_bytes(), 64);
    }

    #[test]
    fn destroy_unlocks_budget_skipped_buffer_and_keeps_locked_statistics() {
        let pool = TransientStorePool::new_with_memory_lock_budget(2, 4096, 4096);
        pool.init_with_locker(|_| Ok(())).expect("warn-only init succeeds");
        let mut unlock_calls = 0;

        pool.destroy_with_unlocker(|memory| {
            unlock_calls += 1;
            assert_eq!(memory.len(), 4096);
            Ok(())
        })
        .expect("injected unlock succeeds");

        assert_eq!(unlock_calls, 2);
        assert_eq!(pool.available_buffer_nums(), 0);
        assert_eq!(pool.locked_buffer_count(), 1);
        assert_eq!(pool.locked_bytes(), 4096);
        assert_eq!(pool.lock_skipped_buffer_count(), 1);
        assert_eq!(pool.lock_skipped_bytes(), 4096);
    }

    #[test]
    fn destroy_first_error_stops_syscalls_but_drain_removes_remaining_buffers() {
        let pool = TransientStorePool::new(3, 8);
        for value in 0..3 {
            pool.return_buffer(vec![value; 8]);
        }
        let mut unlock_calls = 0;

        let error = pool
            .destroy_with_unlocker(|_| {
                unlock_calls += 1;
                Err(RocketMQError::StorageLockFailed {
                    path: "injected unlock failure".to_string(),
                })
            })
            .expect_err("first unlock error is returned");

        assert!(matches!(
            error,
            RocketMQError::StorageLockFailed { path } if path == "injected unlock failure"
        ));
        assert_eq!(unlock_calls, 1);
        assert_eq!(pool.available_buffer_nums(), 0);
    }

    #[test]
    fn destroy_ignores_a_borrowed_buffer_and_unlocks_only_available_buffers() {
        let pool = TransientStorePool::new(2, 8);
        pool.return_buffer(vec![1; 8]);
        pool.return_buffer(vec![2; 8]);
        let borrowed = pool.borrow_buffer().expect("one buffer is borrowed");
        let mut unlock_calls = 0;

        pool.destroy_with_unlocker(|_| {
            unlock_calls += 1;
            Ok(())
        })
        .expect("available buffer unlock succeeds");

        assert_eq!(borrowed, vec![2; 8]);
        assert_eq!(unlock_calls, 1);
        assert_eq!(pool.available_buffer_nums(), 0);
    }

    #[test]
    fn pool_lease_drop_returns_the_buffer_and_clears_outstanding() {
        let pool = TransientStorePool::new(1, 8);
        pool.init_with_locker(|_| Ok(())).expect("pool initializes");

        let lease = pool.borrow_lease().expect("one lease is available");
        assert_eq!(pool.available_buffer_nums(), 0);
        assert_eq!(pool.outstanding_lease_count(), 1);

        drop(lease);

        assert_eq!(pool.available_buffer_nums(), 1);
        assert_eq!(pool.outstanding_lease_count(), 0);
    }

    #[test]
    fn pool_lease_return_now_returns_exactly_once() {
        let pool = TransientStorePool::new(1, 8);
        pool.init_with_locker(|_| Ok(())).expect("pool initializes");

        pool.borrow_lease().expect("one lease is available").return_now();

        assert_eq!(pool.available_buffer_nums(), 1);
        assert_eq!(pool.outstanding_lease_count(), 0);
    }

    #[test]
    fn shutdown_rejects_new_borrows_and_late_drop_does_not_requeue() {
        let unlocks = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&unlocks);
        let pool = TransientStorePool::new_for_test(1, 8, move |_| {
            observed.fetch_add(1, Ordering::AcqRel);
            Ok(())
        });
        pool.init_with_locker(|_| Ok(())).expect("pool initializes");
        let lease = pool.borrow_lease().expect("one lease is available");

        let report = pool
            .shutdown_with_unlocker(Duration::ZERO, |_| Ok(()))
            .expect("shutdown reports the outstanding lease");

        assert_eq!(report.drained_buffers(), 0);
        assert_eq!(report.outstanding_leases(), 1);
        assert!(report.timed_out());
        assert!(pool.borrow_lease().is_none());

        drop(lease);

        assert_eq!(pool.available_buffer_nums(), 0);
        assert_eq!(pool.outstanding_lease_count(), 0);
        assert_eq!(unlocks.load(Ordering::Acquire), 1);
    }

    #[test]
    fn shutdown_waits_for_a_lease_returned_by_another_thread() {
        let pool = TransientStorePool::new_for_test(1, 8, |_| Ok(()));
        pool.init_with_locker(|_| Ok(())).expect("pool initializes");
        let lease = pool.borrow_lease().expect("one lease is available");
        let worker = std::thread::spawn(move || drop(lease));

        let report = pool
            .shutdown_with_unlocker(Duration::from_secs(1), |_| Ok(()))
            .expect("shutdown waits for the lease");

        worker.join().expect("lease-return worker");
        assert_eq!(report.outstanding_leases(), 0);
        assert!(!report.timed_out());
        assert_eq!(pool.available_buffer_nums(), 0);
    }

    #[test]
    fn init_after_shutdown_fails_closed() {
        let pool = TransientStorePool::new(1, 8);
        pool.shutdown_with_unlocker(Duration::ZERO, |_| Ok(()))
            .expect("empty pool shuts down");

        let error = pool
            .init_with_locker(|_| Ok(()))
            .expect_err("shutdown pool cannot be reinitialized");

        assert!(matches!(error, RocketMQError::IllegalArgument(message) if message.contains("shut down")));
    }
}
