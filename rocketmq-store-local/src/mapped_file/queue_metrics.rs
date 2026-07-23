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

//! Local mapped-file queue statistics and position queries.

use std::sync::Arc;

use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::LazyMmapStats;
use crate::mapped_file::MappedFile;

/// Aggregate mapped-file warm-up statistics for one queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MappedFileWarmupStats {
    pub operations: u64,
    pub bytes: u64,
    pub total_millis: u64,
    pub last_millis: u64,
}

/// Aggregate I/O counters from one mapped-file queue generation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MappedFileIoStats {
    /// Successful append operations recorded by mapped files.
    pub write_operations: u64,
    /// Encoded bytes appended to mapped files.
    pub bytes_written: u64,
    /// Successful mapped-memory flush operations.
    pub flush_operations: u64,
    /// Read operations served by mapped files.
    pub read_operations: u64,
    /// Bytes returned by mapped-file reads.
    pub bytes_read: u64,
}

/// Aggregates I/O metrics from the current mapped-file snapshot.
#[doc(hidden)]
pub fn mapped_file_queue_io_stats(files: &[Arc<DefaultMappedFile>]) -> MappedFileIoStats {
    let mut stats = MappedFileIoStats::default();
    for mapped_file in files {
        let Some(metrics) = mapped_file.get_metrics() else {
            continue;
        };
        stats.write_operations = stats.write_operations.saturating_add(metrics.total_writes());
        stats.bytes_written = stats.bytes_written.saturating_add(metrics.total_bytes_written());
        stats.flush_operations = stats.flush_operations.saturating_add(metrics.total_flushes());
        stats.read_operations = stats.read_operations.saturating_add(metrics.total_reads());
        stats.bytes_read = stats.bytes_read.saturating_add(metrics.total_bytes_read());
    }
    stats
}

/// Aggregates warm-up metrics from the current mapped-file snapshot.
#[doc(hidden)]
pub fn mapped_file_queue_warmup_stats(files: &[Arc<DefaultMappedFile>]) -> MappedFileWarmupStats {
    let mut stats = MappedFileWarmupStats::default();
    for mapped_file in files {
        let Some(metrics) = mapped_file.get_metrics() else {
            continue;
        };
        let operations = metrics.warm_operations();
        if operations == 0 {
            continue;
        }
        stats.operations = stats.operations.saturating_add(operations);
        stats.bytes = stats.bytes.saturating_add(metrics.warm_bytes());
        stats.total_millis = stats.total_millis.saturating_add(metrics.total_warm_millis());
        stats.last_millis = metrics.last_warm_millis();
    }
    stats
}

/// Aggregates lazy-mmap metrics from the current mapped-file snapshot.
#[doc(hidden)]
pub fn mapped_file_queue_lazy_mmap_stats(files: &[Arc<DefaultMappedFile>]) -> LazyMmapStats {
    let mut stats = LazyMmapStats::default();
    for mapped_file in files {
        stats.saturating_add_assign(mapped_file.lazy_mmap_stats());
    }
    stats
}

/// Returns the readable end offset of the last mapped file.
#[doc(hidden)]
pub fn mapped_file_queue_max_offset(last: Option<&Arc<DefaultMappedFile>>) -> i64 {
    last.map_or(0, |file| {
        file.get_file_from_offset() as i64 + file.get_read_position() as i64
    })
}

/// Returns the written end offset of the last mapped file.
#[doc(hidden)]
pub fn mapped_file_queue_max_wrote_position(last: Option<&Arc<DefaultMappedFile>>) -> i64 {
    last.map_or(0, |file| {
        file.get_file_from_offset() as i64 + file.get_wrote_position() as i64
    })
}

/// Reports whether the queue must roll before appending a message of the requested size.
#[doc(hidden)]
pub fn mapped_file_queue_should_roll(last: Option<&Arc<DefaultMappedFile>>, message_size: i32) -> bool {
    let Some(last) = last else {
        return true;
    };
    last.is_full() || last.get_wrote_position() + message_size > last.get_file_size() as i32
}

/// Returns the first mapped-file offset or the legacy empty sentinel.
#[doc(hidden)]
pub fn mapped_file_queue_min_offset(first: Option<&Arc<DefaultMappedFile>>) -> i64 {
    first.map_or(-1, |file| file.get_file_from_offset() as i64)
}

/// Returns mapped memory attributed to available files.
#[doc(hidden)]
pub fn mapped_file_queue_available_memory_size(files: &[Arc<DefaultMappedFile>], mapped_file_size: u64) -> i64 {
    files.iter().filter(|file| file.is_available()).count() as i64 * mapped_file_size as i64
}

/// Returns the written distance beyond the flushed watermark.
#[doc(hidden)]
pub fn mapped_file_queue_fall_behind(last: Option<&Arc<DefaultMappedFile>>, flushed_where: i64) -> i64 {
    if flushed_where == 0 {
        return 0;
    }
    last.map_or(0, |file| {
        file.get_file_from_offset() as i64 + file.get_wrote_position() as i64 - flushed_where
    })
}

/// Returns total configured bytes for a mapped-file count and segment size.
#[doc(hidden)]
pub fn mapped_file_queue_total_size(mapped_file_count: usize, mapped_file_size: u64) -> i64 {
    mapped_file_count as i64 * mapped_file_size as i64
}
