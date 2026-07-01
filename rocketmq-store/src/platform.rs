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

use std::fs::File;

use crate::utils::ffi::get_page_size;

pub const PREALLOCATE_UNSUPPORTED_ERRNO: i32 = 95;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorePlatformCapability {
    pub os_name: &'static str,
    pub page_size: usize,
    pub memory_lock_limit_bytes: Option<u64>,
    pub file_preallocate_supported: bool,
    pub optimization: StorePlatformOptimizationCapability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorePlatformIoHintBranch {
    LinuxMmapAdvice,
    WindowsPrefetch,
    Unsupported,
}

impl StorePlatformIoHintBranch {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LinuxMmapAdvice => "linux_mmap_advice",
            Self::WindowsPrefetch => "windows_prefetch",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorePlatformOptimizationCapability {
    pub io_hint_branch: StorePlatformIoHintBranch,
    pub mmap_advice_supported: bool,
    pub file_prefetch_supported: bool,
    pub lazy_mmap_supported: bool,
    pub hint_failure_affects_correctness: bool,
}

impl StorePlatformOptimizationCapability {
    pub fn for_os_name(os_name: &str) -> Self {
        match os_name {
            "linux" => Self {
                io_hint_branch: StorePlatformIoHintBranch::LinuxMmapAdvice,
                mmap_advice_supported: true,
                file_prefetch_supported: false,
                lazy_mmap_supported: true,
                hint_failure_affects_correctness: false,
            },
            "windows" => Self {
                io_hint_branch: StorePlatformIoHintBranch::WindowsPrefetch,
                mmap_advice_supported: false,
                file_prefetch_supported: true,
                lazy_mmap_supported: true,
                hint_failure_affects_correctness: false,
            },
            _ => Self {
                io_hint_branch: StorePlatformIoHintBranch::Unsupported,
                mmap_advice_supported: false,
                file_prefetch_supported: false,
                lazy_mmap_supported: false,
                hint_failure_affects_correctness: false,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilePreallocateOutcome {
    Allocated,
    Unsupported { errno: i32 },
    Failed { errno: i32 },
}

impl FilePreallocateOutcome {
    pub fn is_degraded(self) -> bool {
        matches!(self, Self::Unsupported { .. })
    }
}

pub fn current_store_platform_capability() -> StorePlatformCapability {
    StorePlatformCapability {
        os_name: std::env::consts::OS,
        page_size: get_page_size().max(1),
        memory_lock_limit_bytes: memory_lock_limit_bytes(),
        file_preallocate_supported: cfg!(target_os = "linux"),
        optimization: StorePlatformOptimizationCapability::for_os_name(std::env::consts::OS),
    }
}

pub fn classify_file_preallocate_result(result: i32, errno: i32) -> FilePreallocateOutcome {
    if result == 0 {
        FilePreallocateOutcome::Allocated
    } else if is_unsupported_preallocate_errno(errno) {
        FilePreallocateOutcome::Unsupported { errno }
    } else {
        FilePreallocateOutcome::Failed { errno }
    }
}

pub fn preallocate_file(file: &File, len: u64) -> FilePreallocateOutcome {
    if len == 0 {
        return FilePreallocateOutcome::Allocated;
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;

        if len > i64::MAX as u64 {
            return FilePreallocateOutcome::Failed { errno: libc::EINVAL };
        }

        let result = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len as libc::off_t) };
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        classify_file_preallocate_result(result, errno)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        FilePreallocateOutcome::Unsupported {
            errno: PREALLOCATE_UNSUPPORTED_ERRNO,
        }
    }
}

#[cfg(unix)]
fn is_unsupported_preallocate_errno(errno: i32) -> bool {
    errno == PREALLOCATE_UNSUPPORTED_ERRNO || errno == libc::ENOSYS || errno == libc::EINVAL
}

#[cfg(not(unix))]
fn is_unsupported_preallocate_errno(errno: i32) -> bool {
    errno == PREALLOCATE_UNSUPPORTED_ERRNO
}

#[cfg(unix)]
fn memory_lock_limit_bytes() -> Option<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let result = unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) };
    if result != 0 {
        return None;
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        Some(u64::MAX)
    } else {
        Some(limit.rlim_cur)
    }
}

#[cfg(not(unix))]
fn memory_lock_limit_bytes() -> Option<u64> {
    Some(0)
}

#[cfg(test)]
mod tests {
    use super::StorePlatformIoHintBranch;
    use super::StorePlatformOptimizationCapability;

    #[test]
    fn classifies_linux_platform_optimization_capability() {
        let capability = StorePlatformOptimizationCapability::for_os_name("linux");

        assert_eq!(capability.io_hint_branch, StorePlatformIoHintBranch::LinuxMmapAdvice);
        assert_eq!(capability.io_hint_branch.as_str(), "linux_mmap_advice");
        assert!(capability.mmap_advice_supported);
        assert!(!capability.file_prefetch_supported);
        assert!(capability.lazy_mmap_supported);
        assert!(!capability.hint_failure_affects_correctness);
    }

    #[test]
    fn classifies_windows_platform_optimization_capability() {
        let capability = StorePlatformOptimizationCapability::for_os_name("windows");

        assert_eq!(capability.io_hint_branch, StorePlatformIoHintBranch::WindowsPrefetch);
        assert_eq!(capability.io_hint_branch.as_str(), "windows_prefetch");
        assert!(!capability.mmap_advice_supported);
        assert!(capability.file_prefetch_supported);
        assert!(capability.lazy_mmap_supported);
        assert!(!capability.hint_failure_affects_correctness);
    }

    #[test]
    fn classifies_unsupported_platform_optimization_capability() {
        let capability = StorePlatformOptimizationCapability::for_os_name("freebsd");

        assert_eq!(capability.io_hint_branch, StorePlatformIoHintBranch::Unsupported);
        assert_eq!(capability.io_hint_branch.as_str(), "unsupported");
        assert!(!capability.mmap_advice_supported);
        assert!(!capability.file_prefetch_supported);
        assert!(!capability.lazy_mmap_supported);
        assert!(!capability.hint_failure_affects_correctness);
    }
}
