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

pub const MADV_NORMAL: i32 = 0;
pub const MADV_RANDOM: i32 = 1;
pub const MADV_SEQUENTIAL: i32 = 2;
pub const MADV_WILLNEED: i32 = 3;
pub const MADV_DONTNEED: i32 = 4;

#[inline]
pub fn get_page_size() -> usize {
    page_size::get()
}

/// Advises the operating system how a valid mapped range will be used.
pub fn madvise(addr: *const u8, len: usize, advice: i32) -> i32 {
    #[cfg(unix)]
    {
        use std::ffi::c_void;

        // SAFETY: this call does not transfer ownership. The caller supplies a live mapped range;
        // the operating system validates the address, length, and advice value.
        unsafe { libc::madvise(addr as *mut c_void, len, advice) }
    }
    #[cfg(windows)]
    {
        let _ = (addr, len, advice);
        0
    }
}

/// Requests best-effort prefetching for a valid process-local memory range.
pub fn prefetch_virtual_memory(addr: *const u8, len: usize) -> RocketMQResult<bool> {
    if len == 0 {
        return Ok(false);
    }

    #[cfg(windows)]
    {
        use std::ffi::c_void;

        use windows::Win32::System::Memory::PrefetchVirtualMemory;
        use windows::Win32::System::Memory::WIN32_MEMORY_RANGE_ENTRY;
        use windows::Win32::System::Threading::GetCurrentProcess;

        let range = WIN32_MEMORY_RANGE_ENTRY {
            VirtualAddress: addr as *mut c_void,
            NumberOfBytes: len,
        };
        // SAFETY: `addr..addr + len` is required to be a live process-local range. The API does
        // not retain the range or transfer ownership.
        unsafe { PrefetchVirtualMemory(GetCurrentProcess(), &[range], 0) }.map_err(|error| {
            RocketMQError::StorageReadFailed {
                path: "PrefetchVirtualMemory".to_string(),
                reason: error.to_string(),
            }
        })?;
        Ok(true)
    }

    #[cfg(not(windows))]
    {
        let _ = (addr, len);
        Ok(false)
    }
}

/// Reports residency for each page in a valid mapped range.
pub fn mincore(addr: *const u8, len: usize, vec: *mut u8) -> i32 {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::c_void;

        use libc::c_uchar;

        // SAFETY: the caller provides a live mapped range and an output vector large enough for
        // one residency byte per page. The operating system validates both pointers.
        unsafe { libc::mincore(addr as *mut c_void, len, vec as *mut c_uchar) }
    }
    #[cfg(target_os = "macos")]
    {
        use std::ffi::c_void;

        use libc::c_char;

        // SAFETY: the caller provides a live mapped range and an output vector large enough for
        // one residency byte per page. The operating system validates both pointers.
        unsafe { libc::mincore(addr as *mut c_void, len, vec as *mut c_char) }
    }

    #[cfg(windows)]
    {
        let _ = (addr, len, vec);
        0
    }
}

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
