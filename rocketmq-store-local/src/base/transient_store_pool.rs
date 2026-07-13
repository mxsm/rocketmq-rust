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
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_error::RocketMQResult;
use tracing::warn;

use crate::base::memory_lock_manager::MemoryLockManager;
use crate::utils::ffi::mlock;
use crate::utils::ffi::munlock;

#[derive(Clone)]
pub struct TransientStorePool {
    pool_size: usize,
    file_size: usize,
    available_buffers: Arc<parking_lot::Mutex<VecDeque<Vec<u8>>>>,
    is_real_commit: Arc<parking_lot::Mutex<bool>>,
    memory_lock_manager: Arc<MemoryLockManager>,
}

impl TransientStorePool {
    pub fn new(pool_size: usize, file_size: usize) -> Self {
        Self::new_with_memory_lock_budget(pool_size, file_size, 0)
    }

    pub fn new_with_memory_lock_budget(pool_size: usize, file_size: usize, memory_lock_budget_bytes: u64) -> Self {
        let available_buffers = Arc::new(Mutex::new(VecDeque::with_capacity(pool_size)));
        let is_real_commit = Arc::new(Mutex::new(true));
        TransientStorePool {
            pool_size,
            file_size,
            available_buffers,
            is_real_commit,
            memory_lock_manager: Arc::new(MemoryLockManager::warn_only_with_budget(memory_lock_budget_bytes)),
        }
    }

    pub fn init(&self) -> RocketMQResult<()> {
        self.init_with_locker(mlock)
    }

    pub(crate) fn init_with_locker<F>(&self, mut locker: F) -> RocketMQResult<()>
    where
        F: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        let mut available_buffers = self.available_buffers.lock();
        for _ in 0..self.pool_size {
            let buffer = vec![0u8; self.file_size];
            self.memory_lock_manager
                .lock_buffer_with(buffer.as_ptr(), self.file_size, &mut locker)?;
            available_buffers.push_back(buffer);
        }
        Ok(())
    }

    pub fn destroy(&self) -> RocketMQResult<()> {
        self.destroy_with_unlocker(munlock)
    }

    fn destroy_with_unlocker<F>(&self, mut unlocker: F) -> RocketMQResult<()>
    where
        F: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        let mut available_buffers = self.available_buffers.lock();
        for available_buffer in available_buffers.drain(0..) {
            unlocker(available_buffer.as_ptr(), self.file_size)?;
        }
        Ok(())
    }

    pub fn return_buffer(&self, buffer: Vec<u8>) {
        let mut available_buffers = self.available_buffers.lock();
        available_buffers.push_front(buffer);
    }

    pub fn borrow_buffer(&self) -> Option<Vec<u8>> {
        let mut available_buffers = self.available_buffers.lock();
        let buffer = available_buffers.pop_front();
        if available_buffers.len() < self.pool_size / 10 * 4 {
            warn!("TransientStorePool only remain {} sheets.", available_buffers.len());
        }
        buffer
    }

    pub fn available_buffer_nums(&self) -> usize {
        let available_buffers = self.available_buffers.lock();
        available_buffers.len()
    }

    pub fn locked_buffer_count(&self) -> usize {
        self.memory_lock_manager.locked_buffer_count()
    }

    pub fn lock_attempt_count(&self) -> usize {
        self.memory_lock_manager.lock_attempt_count()
    }

    pub fn lock_failed_buffer_count(&self) -> usize {
        self.memory_lock_manager.lock_failed_buffer_count()
    }

    pub fn lock_skipped_buffer_count(&self) -> usize {
        self.memory_lock_manager.lock_skipped_buffer_count()
    }

    pub fn locked_bytes(&self) -> u64 {
        self.memory_lock_manager.locked_bytes()
    }

    pub fn lock_failed_bytes(&self) -> u64 {
        self.memory_lock_manager.lock_failed_bytes()
    }

    pub fn lock_skipped_bytes(&self) -> u64 {
        self.memory_lock_manager.lock_skipped_bytes()
    }

    pub fn is_real_commit(&self) -> bool {
        let is_real_commit = self.is_real_commit.lock();
        *is_real_commit
    }

    pub fn set_real_commit(&self, real_commit: bool) {
        let mut is_real_commit = self.is_real_commit.lock();
        *is_real_commit = real_commit;
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::RocketMQError;

    use super::*;

    #[test]
    fn init_keeps_buffers_when_memory_lock_fails_warn_only() {
        let pool = TransientStorePool::new(2, 4096);

        let result = pool.init_with_locker(|_, _| {
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

        let result = pool.init_with_locker(|_, _| Ok(()));

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

        pool.init_with_locker(|_, _| Ok(())).expect("first init succeeds");
        pool.init_with_locker(|_, _| Ok(())).expect("second init succeeds");

        assert_eq!(pool.available_buffer_nums(), 4);
        assert_eq!(pool.lock_attempt_count(), 4);
        assert_eq!(pool.locked_buffer_count(), 4);
        assert_eq!(pool.locked_bytes(), 64);
    }

    #[test]
    fn destroy_unlocks_failed_lock_buffers_without_updating_manager_statistics() {
        let pool = TransientStorePool::new(2, 32);
        pool.init_with_locker(|_, _| {
            Err(RocketMQError::StorageLockFailed {
                path: "injected lock failure".to_string(),
            })
        })
        .expect("warn-only init keeps failed buffers");
        let mut unlock_calls = 0;

        pool.destroy_with_unlocker(|_, len| {
            unlock_calls += 1;
            assert_eq!(len, 32);
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
        pool.init_with_locker(|_, _| Ok(())).expect("warn-only init succeeds");
        let mut unlock_calls = 0;

        pool.destroy_with_unlocker(|_, len| {
            unlock_calls += 1;
            assert_eq!(len, 4096);
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
            .destroy_with_unlocker(|_, _| {
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

        pool.destroy_with_unlocker(|_, _| {
            unlock_calls += 1;
            Ok(())
        })
        .expect("available buffer unlock succeeds");

        assert_eq!(borrowed, vec![2; 8]);
        assert_eq!(unlock_calls, 1);
        assert_eq!(pool.available_buffer_nums(), 0);
    }
}
