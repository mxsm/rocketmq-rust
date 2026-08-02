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

use std::io;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

/// Operating-system access pattern for a live mapped-memory region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryAdvice {
    /// No special access pattern is expected.
    Normal,
    /// Pages are expected to be accessed in a non-sequential order.
    Random,
    /// Pages are expected to be accessed sequentially.
    Sequential,
    /// Pages are expected to be accessed soon.
    WillNeed,
}

impl MemoryAdvice {
    #[cfg(unix)]
    const fn as_raw(self) -> i32 {
        match self {
            Self::Normal => libc::MADV_NORMAL,
            Self::Random => libc::MADV_RANDOM,
            Self::Sequential => libc::MADV_SEQUENTIAL,
            Self::WillNeed => libc::MADV_WILLNEED,
        }
    }
}

#[inline]
pub fn get_page_size() -> usize {
    page_size::get()
}

/// Advises the operating system how a live mapped region will be used.
///
/// Empty regions are accepted as a no-op.
///
/// # Errors
///
/// Returns the operating-system error when the advice cannot be applied.
pub(crate) fn advise_memory(memory: &[u8], advice: MemoryAdvice) -> io::Result<()> {
    if memory.is_empty() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        // SAFETY: `memory` proves that the complete input range is live for this call, and the
        // enum maps only to platform-defined advice values. The OS does not retain the range.
        let result = unsafe { sys_madvise(memory.as_ptr(), memory.len(), advice.as_raw()) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
    }

    #[cfg(not(unix))]
    {
        let _ = advice;
    }

    Ok(())
}

/// Requests best-effort prefetching for a live process-local memory region.
///
/// Returns `Ok(false)` when the operation is unsupported or the region is empty.
///
/// # Errors
///
/// Returns [`RocketMQError::StorageReadFailed`] when Windows rejects the request.
pub(crate) fn prefetch_memory(memory: &[u8]) -> RocketMQResult<bool> {
    if memory.is_empty() {
        return Ok(false);
    }

    #[cfg(windows)]
    {
        // SAFETY: the slice proves the range is live for the duration of the call. Windows does
        // not retain the range after PrefetchVirtualMemory returns.
        unsafe { sys_prefetch_virtual_memory(memory.as_ptr(), memory.len()) }?;
        Ok(true)
    }

    #[cfg(not(windows))]
    {
        let _ = memory;
        Ok(false)
    }
}

/// Reports one residency byte for every page covered by a live mapped region.
///
/// The implementation validates page alignment and owns the correctly sized output buffer. Empty
/// regions return an empty vector without invoking the operating system.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] for an unaligned region, invalid page size, or page-count
/// overflow, or the operating-system error when the query fails.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn memory_residency(memory: &[u8]) -> io::Result<Vec<u8>> {
    if memory.is_empty() {
        return Ok(Vec::new());
    }

    let page_size = get_page_size();
    let page_count = residency_page_count(memory.len(), page_size)?;
    if (memory.as_ptr() as usize) % page_size != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "memory residency range must start on a page boundary",
        ));
    }

    let mut residency = vec![0u8; page_count];
    // SAFETY: `memory` is live and page-aligned, and `residency` owns exactly one writable byte
    // per covered page as required by mincore. Neither range is retained by the OS.
    let result = unsafe { sys_mincore(memory.as_ptr(), memory.len(), residency.as_mut_ptr(), residency.len()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(residency)
}

#[cfg(any(target_os = "linux", target_os = "macos", test))]
fn residency_page_count(len: usize, page_size: usize) -> io::Result<usize> {
    if page_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "operating-system page size must be non-zero",
        ));
    }
    if len == 0 {
        return Ok(0);
    }
    len.checked_add(page_size - 1)
        .map(|rounded| rounded / page_size)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "memory residency page count overflowed"))
}

type MemoryLocker = fn(&[u8]) -> RocketMQResult<()>;

/// A scoped physical-memory lock tied to the borrowed region's lifetime.
///
/// Dropping the guard unlocks the region exactly once. Use [`Self::unlock`] when the caller needs
/// to observe an unlock failure.
#[derive(Debug)]
#[must_use = "dropping the guard immediately unlocks the memory region"]
pub struct MemoryLockGuard<'a> {
    memory: &'a [u8],
    unlocker: MemoryLocker,
    locked: bool,
}

impl MemoryLockGuard<'_> {
    /// Returns whether this guard represents a non-empty locked region.
    pub const fn is_locked(&self) -> bool {
        self.locked
    }

    /// Unlocks the region immediately and consumes the guard.
    ///
    /// # Errors
    ///
    /// Returns [`RocketMQError::StorageLockFailed`] when the operating system rejects the unlock.
    pub fn unlock(mut self) -> RocketMQResult<()> {
        if !self.locked {
            return Ok(());
        }
        self.locked = false;
        (self.unlocker)(self.memory)
    }
}

impl Drop for MemoryLockGuard<'_> {
    fn drop(&mut self) {
        if self.locked {
            self.locked = false;
            let _ = (self.unlocker)(self.memory);
        }
    }
}

/// Locks a live process-local region for the lifetime of the returned guard.
///
/// Empty regions produce an unarmed guard without invoking the operating system.
///
/// # Errors
///
/// Returns [`RocketMQError::StorageLockFailed`] when the operating system rejects the lock.
pub fn lock_memory(memory: &[u8]) -> RocketMQResult<MemoryLockGuard<'_>> {
    lock_memory_with(memory, lock_memory_region, unlock_memory_region)
}

fn lock_memory_with<'a>(
    memory: &'a [u8],
    locker: MemoryLocker,
    unlocker: MemoryLocker,
) -> RocketMQResult<MemoryLockGuard<'a>> {
    if memory.is_empty() {
        return Ok(MemoryLockGuard {
            memory,
            unlocker,
            locked: false,
        });
    }
    locker(memory)?;
    Ok(MemoryLockGuard {
        memory,
        unlocker,
        locked: true,
    })
}

pub(crate) fn lock_memory_region(memory: &[u8]) -> RocketMQResult<()> {
    if memory.is_empty() {
        return Ok(());
    }
    // SAFETY: the slice proves that the complete input range is live for this call. The OS does
    // not retain a Rust reference or access the range after the call returns.
    unsafe { sys_mlock(memory.as_ptr(), memory.len()) }
}

pub(crate) fn unlock_memory_region(memory: &[u8]) -> RocketMQResult<()> {
    if memory.is_empty() {
        return Ok(());
    }
    // SAFETY: the slice proves that the complete input range is live for this call. The OS does
    // not retain a Rust reference or access the range after the call returns.
    unsafe { sys_munlock(memory.as_ptr(), memory.len()) }
}

/// Invokes the platform memory-advice operation.
///
/// # Safety
///
/// `addr..addr + len` must identify one live process-local mapping for the duration of the call.
/// `advice` must be a platform-defined value, and no concurrent operation may unmap the range.
#[cfg(unix)]
unsafe fn sys_madvise(addr: *const u8, len: usize, advice: i32) -> i32 {
    use std::ffi::c_void;

    // SAFETY: the caller upholds the complete mapping, range, and concurrency contract.
    unsafe { libc::madvise(addr.cast_mut().cast::<c_void>(), len, advice) }
}

/// Invokes the platform residency query.
///
/// # Safety
///
/// `addr..addr + len` must be a live page-aligned mapping. `output` must reference at least
/// `output_len` writable bytes, where `output_len == ceil(len / page_size)`. The mapping and output
/// must remain live and non-overlapping for the duration of the call.
#[cfg(any(target_os = "linux", target_os = "macos"))]
unsafe fn sys_mincore(addr: *const u8, len: usize, output: *mut u8, output_len: usize) -> i32 {
    let page_size = get_page_size();
    debug_assert_eq!(
        Some(output_len),
        len.checked_add(page_size - 1).map(|rounded| rounded / page_size)
    );
    use std::ffi::c_void;

    #[cfg(target_os = "linux")]
    {
        // SAFETY: the caller upholds the mapping and exact writable-output capacity contract.
        unsafe { libc::mincore(addr.cast_mut().cast::<c_void>(), len, output.cast::<libc::c_uchar>()) }
    }
    #[cfg(target_os = "macos")]
    {
        // SAFETY: the caller upholds the mapping and exact writable-output capacity contract.
        unsafe { libc::mincore(addr.cast_mut().cast::<c_void>(), len, output.cast::<libc::c_char>()) }
    }
}

/// Invokes the platform physical-memory lock operation.
///
/// # Safety
///
/// `addr..addr + len` must identify a live process-local memory region for the duration of the
/// call. No concurrent operation may unmap or replace that region during the call.
unsafe fn sys_mlock(addr: *const u8, len: usize) -> RocketMQResult<()> {
    #[cfg(unix)]
    {
        use std::ffi::c_void;

        // SAFETY: the caller upholds the live-range and concurrent-unmapping contract.
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

        // SAFETY: the caller upholds the live-range and concurrent-unmapping contract.
        let result = unsafe { VirtualLock(addr as _, len) };
        result.map_err(|e| RocketMQError::StorageLockFailed {
            path: format!("memory lock (VirtualLock): {}", e),
        })?;
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (addr, len);
        Err(RocketMQError::StorageLockFailed {
            path: "memory lock is unsupported on this target".to_string(),
        })
    }
}

/// Invokes the platform physical-memory unlock operation.
///
/// # Safety
///
/// `addr..addr + len` must identify the same live process-local region previously submitted for
/// locking. No concurrent operation may unmap or replace that region during the call.
unsafe fn sys_munlock(addr: *const u8, len: usize) -> RocketMQResult<()> {
    #[cfg(unix)]
    {
        use std::ffi::c_void;

        // SAFETY: the caller upholds the live, previously locked range contract.
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

        // SAFETY: the caller upholds the live, previously locked range contract.
        let result = unsafe { VirtualUnlock(addr as _, len) };
        result.map_err(|e| RocketMQError::StorageLockFailed {
            path: format!("memory unlock (VirtualUnlock): {}", e),
        })?;
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (addr, len);
        Err(RocketMQError::StorageLockFailed {
            path: "memory unlock is unsupported on this target".to_string(),
        })
    }
}

/// Invokes Windows best-effort virtual-memory prefetching.
///
/// # Safety
///
/// `addr..addr + len` must identify a live process-local memory region for the duration of the
/// call. No concurrent operation may unmap or replace that region during the call.
#[cfg(windows)]
unsafe fn sys_prefetch_virtual_memory(addr: *const u8, len: usize) -> RocketMQResult<()> {
    use std::ffi::c_void;

    use windows::Win32::System::Memory::PrefetchVirtualMemory;
    use windows::Win32::System::Memory::WIN32_MEMORY_RANGE_ENTRY;
    use windows::Win32::System::Threading::GetCurrentProcess;

    let range = WIN32_MEMORY_RANGE_ENTRY {
        VirtualAddress: addr as *mut c_void,
        NumberOfBytes: len,
    };
    // SAFETY: the caller upholds the live-range contract and Windows does not retain the entry.
    unsafe { PrefetchVirtualMemory(GetCurrentProcess(), &[range], 0) }.map_err(|error| {
        RocketMQError::StorageReadFailed {
            path: "PrefetchVirtualMemory".to_string(),
            reason: error.to_string(),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    static UNLOCK_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn successful_test_lock(_memory: &[u8]) -> RocketMQResult<()> {
        Ok(())
    }

    fn recording_test_unlock(_memory: &[u8]) -> RocketMQResult<()> {
        UNLOCK_CALLS.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[test]
    fn residency_page_count_handles_zero_boundaries_and_overflow() {
        assert_eq!(residency_page_count(0, 4096).unwrap(), 0);
        assert_eq!(residency_page_count(1, 4096).unwrap(), 1);
        assert_eq!(residency_page_count(4096, 4096).unwrap(), 1);
        assert_eq!(residency_page_count(4097, 4096).unwrap(), 2);
        assert_eq!(
            residency_page_count(1, 0).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(
            residency_page_count(usize::MAX, 4096).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn residency_owns_the_correct_output_capacity_for_a_live_mapping() {
        let page_size = get_page_size();
        let mapping = memmap2::MmapMut::map_anon(page_size * 2).unwrap();

        assert_eq!(memory_residency(&mapping[..1]).unwrap().len(), 1);
        assert_eq!(memory_residency(&mapping).unwrap().len(), 2);
        assert_eq!(
            memory_residency(&mapping[1..]).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn memory_lock_guard_unlocks_exactly_once() {
        UNLOCK_CALLS.store(0, Ordering::Relaxed);
        let memory = [0u8; 8];

        {
            let guard = lock_memory_with(&memory, successful_test_lock, recording_test_unlock).unwrap();
            assert!(guard.is_locked());
        }
        assert_eq!(UNLOCK_CALLS.load(Ordering::Relaxed), 1);

        lock_memory_with(&memory, successful_test_lock, recording_test_unlock)
            .unwrap()
            .unlock()
            .unwrap();
        assert_eq!(UNLOCK_CALLS.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn zero_length_lock_is_a_noop() {
        UNLOCK_CALLS.store(0, Ordering::Relaxed);
        let guard = lock_memory_with(&[], successful_test_lock, recording_test_unlock).unwrap();

        assert!(!guard.is_locked());
        drop(guard);
        assert_eq!(UNLOCK_CALLS.load(Ordering::Relaxed), 0);
    }
}
