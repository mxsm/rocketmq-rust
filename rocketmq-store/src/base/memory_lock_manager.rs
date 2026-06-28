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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use rocketmq_error::RocketMQResult;
use tracing::warn;

use crate::utils::ffi::mlock;

#[derive(Debug)]
pub struct MemoryLockManager {
    warn_only: bool,
    locked_buffers: AtomicUsize,
    lock_failed_buffers: AtomicUsize,
}

impl MemoryLockManager {
    pub fn warn_only() -> Self {
        Self {
            warn_only: true,
            locked_buffers: AtomicUsize::new(0),
            lock_failed_buffers: AtomicUsize::new(0),
        }
    }

    pub fn lock_buffer(&self, addr: *const u8, len: usize) -> RocketMQResult<()> {
        self.lock_buffer_with(addr, len, mlock)
    }

    pub(crate) fn lock_buffer_with<F>(&self, addr: *const u8, len: usize, mut locker: F) -> RocketMQResult<()>
    where
        F: FnMut(*const u8, usize) -> RocketMQResult<()>,
    {
        match locker(addr, len) {
            Ok(()) => {
                self.locked_buffers.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                self.lock_failed_buffers.fetch_add(1, Ordering::Relaxed);
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

    pub fn locked_buffer_count(&self) -> usize {
        self.locked_buffers.load(Ordering::Relaxed)
    }

    pub fn lock_failed_buffer_count(&self) -> usize {
        self.lock_failed_buffers.load(Ordering::Relaxed)
    }
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
}
