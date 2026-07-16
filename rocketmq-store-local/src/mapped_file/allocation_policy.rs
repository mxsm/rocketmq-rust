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

//! Runtime-neutral allocation and warm-up policy for mapped files.

use crate::config::FlushDiskType;

/// Mapped-file warm-up values copied from the message-store configuration.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MappedFileWarmupConfig {
    enabled: bool,
    flush_disk_type: FlushDiskType,
    minimum_file_size: usize,
    flush_least_pages: usize,
}

impl MappedFileWarmupConfig {
    /// Creates a disabled warm-up configuration.
    #[doc(hidden)]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            flush_disk_type: FlushDiskType::AsyncFlush,
            minimum_file_size: usize::MAX,
            flush_least_pages: 0,
        }
    }

    /// Creates a warm-up configuration from boundary-owned values.
    #[doc(hidden)]
    pub fn new(
        enabled: bool,
        flush_disk_type: FlushDiskType,
        minimum_file_size: usize,
        flush_least_pages: usize,
    ) -> Self {
        Self {
            enabled,
            flush_disk_type,
            minimum_file_size,
            flush_least_pages,
        }
    }

    /// Returns whether a mapped file should be warmed before publication.
    #[doc(hidden)]
    pub fn should_warm(self, file_size: u64) -> bool {
        self.enabled && file_size as usize >= self.minimum_file_size
    }

    /// Returns the flush mode used while warming a mapped file.
    #[doc(hidden)]
    pub fn flush_disk_type(self) -> FlushDiskType {
        self.flush_disk_type
    }

    /// Returns the least number of pages between warm-up flushes.
    #[doc(hidden)]
    pub fn flush_least_pages(self) -> usize {
        self.flush_least_pages
    }
}

/// Capacity values observed from a transient-store pool and allocation queue.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappedFileAllocationPoolSnapshot {
    available_buffers: usize,
    queued_requests: usize,
}

impl MappedFileAllocationPoolSnapshot {
    /// Creates a point-in-time capacity snapshot.
    #[doc(hidden)]
    pub fn new(available_buffers: usize, queued_requests: usize) -> Self {
        Self {
            available_buffers,
            queued_requests,
        }
    }

    fn remaining_capacity(self) -> usize {
        self.available_buffers.saturating_sub(self.queued_requests)
    }
}

/// Resolves how many allocation requests may be submitted from a pool snapshot.
#[doc(hidden)]
pub fn mapped_file_allocation_capacity(
    default_capacity: usize,
    transient_store_pool_enabled: bool,
    fast_fail_if_no_buffer: bool,
    pool_snapshot: Option<MappedFileAllocationPoolSnapshot>,
) -> usize {
    if transient_store_pool_enabled && fast_fail_if_no_buffer {
        pool_snapshot.map_or(default_capacity, MappedFileAllocationPoolSnapshot::remaining_capacity)
    } else {
        default_capacity
    }
}
