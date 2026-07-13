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

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

#[inline]
/// Locks a process-local memory range into physical memory.
///
/// Callers must ensure that `addr..addr + len` identifies an active mapped region or buffer and
/// remains valid for the duration of this call.
///
/// # Errors
///
/// Returns [`RocketMQError::StorageLockFailed`] when the operating system rejects the lock.
pub fn mlock(addr: *const u8, len: usize) -> RocketMQResult<()> {
    #[cfg(unix)]
    {
        use std::ffi::c_void;

        // SAFETY: mlock does not transfer ownership or dereference the range in Rust; the OS
        // validates the supplied address and length and reports an invalid range as an error.
        let result = unsafe { libc::mlock(addr as *const c_void, len) };
        if result != 0 {
            return Err(RocketMQError::StorageLockFailed {
                path: "memory lock (mlock)".to_string(),
            });
        }
        Ok(())
    }

    #[cfg(windows)]
    {
        use windows::Win32::System::Memory::VirtualLock;

        // SAFETY: VirtualLock does not transfer ownership; Windows validates the process-local
        // address range and returns an error for an invalid or unavailable range.
        let result = unsafe { VirtualLock(addr as _, len) };
        result.map_err(|e| RocketMQError::StorageLockFailed {
            path: format!("memory lock (VirtualLock): {}", e),
        })?;
        Ok(())
    }
}

#[inline]
/// Unlocks a process-local memory range previously submitted for locking.
///
/// Callers must ensure that `addr..addr + len` identifies an active mapped region or buffer and
/// remains valid for the duration of this call.
///
/// # Errors
///
/// Returns [`RocketMQError::StorageLockFailed`] when the operating system rejects the unlock.
pub fn munlock(addr: *const u8, len: usize) -> RocketMQResult<()> {
    #[cfg(unix)]
    {
        use std::ffi::c_void;

        // SAFETY: munlock does not transfer ownership or dereference the range in Rust; the OS
        // validates the supplied address and length and reports an invalid range as an error.
        let result = unsafe { libc::munlock(addr as *const c_void, len) };
        if result != 0 {
            return Err(RocketMQError::StorageLockFailed {
                path: "memory unlock (munlock)".to_string(),
            });
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        use windows::Win32::System::Memory::VirtualUnlock;

        // SAFETY: VirtualUnlock does not transfer ownership; Windows validates the process-local
        // address range and returns an error for an invalid or unlocked range.
        let result = unsafe { VirtualUnlock(addr as _, len) };
        result.map_err(|e| RocketMQError::StorageLockFailed {
            path: format!("memory unlock (VirtualUnlock): {}", e),
        })?;
        Ok(())
    }
}
