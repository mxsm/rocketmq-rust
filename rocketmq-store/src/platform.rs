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

use crate::utils::ffi::get_page_size;

pub use rocketmq_store_local::mapped_file::file::classify_file_preallocate_result;
pub use rocketmq_store_local::mapped_file::file::preallocate_file;
pub use rocketmq_store_local::mapped_file::file::FilePreallocateOutcome;
pub use rocketmq_store_local::mapped_file::file::PREALLOCATE_UNSUPPORTED_ERRNO;

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

pub fn current_store_platform_capability() -> StorePlatformCapability {
    StorePlatformCapability {
        os_name: std::env::consts::OS,
        page_size: get_page_size().max(1),
        memory_lock_limit_bytes: memory_lock_limit_bytes(),
        file_preallocate_supported: cfg!(target_os = "linux"),
        optimization: StorePlatformOptimizationCapability::for_os_name(std::env::consts::OS),
    }
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
