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

#[cfg(windows)]
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

pub use rocketmq_store_local::utils::ffi::mlock;
pub use rocketmq_store_local::utils::ffi::munlock;

pub const MADV_NORMAL: i32 = 0;
pub const MADV_RANDOM: i32 = 1;
pub const MADV_WILLNEED: i32 = 3;
pub const MADV_DONTNEED: i32 = 4;

#[inline]
pub fn get_page_size() -> usize {
    page_size::get()
}

pub fn madvise(addr: *const u8, len: usize, advice: i32) -> i32 {
    #[cfg(unix)]
    {
        use std::ffi::c_void;
        unsafe { libc::madvise(addr as *mut c_void, len, advice) }
    }
    #[cfg(windows)]
    {
        // Windows does not have madvise, so we just return 0
        0
    }
}

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
        let _ = addr;
        let _ = len;
        Ok(false)
    }
}

pub fn mincore(addr: *const u8, len: usize, vec: *const u8) -> i32 {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::c_void;

        use libc::c_uchar;

        unsafe { libc::mincore(addr as *mut c_void, len, vec as *mut c_uchar) }
    }
    #[cfg(target_os = "macos")]
    {
        use std::ffi::c_void;

        use libc::c_char;

        unsafe { libc::mincore(addr as *mut c_void, len, vec as *mut c_char) }
    }

    #[cfg(target_os = "windows")]
    {
        // Windows does not have mincore, so we just return 0
        0
    }
}
