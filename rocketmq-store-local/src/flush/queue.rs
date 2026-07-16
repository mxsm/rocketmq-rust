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

//! Canonical mapped-file queue flush and commit drivers.

/// Appended and durable watermarks produced by one canonical queue flush.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlushProgress {
    pub appended: i64,
    pub durable_before: i64,
    pub durable: i64,
    pub store_timestamp: u64,
}

/// Result of flushing the mapped-file segment selected by the Store adapter.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentFlushProgress {
    file_from_offset: u64,
    flushed_position: i32,
    store_timestamp: u64,
}

impl SegmentFlushProgress {
    /// Captures the concrete segment side effect without exposing its type to Local.
    #[doc(hidden)]
    pub const fn new(file_from_offset: u64, flushed_position: i32, store_timestamp: u64) -> Self {
        Self {
            file_from_offset,
            flushed_position,
            store_timestamp,
        }
    }
}

/// Drives one queue flush around a concrete mapped-file I/O adapter.
#[doc(hidden)]
pub fn try_flush_mapped_file_queue<E>(
    appended: i64,
    durable_before: i64,
    store_timestamp_before: u64,
    flush_least_pages: i32,
    flush_segment: impl FnOnce(i64, bool) -> Result<Option<SegmentFlushProgress>, E>,
) -> Result<FlushProgress, E> {
    let Some(segment) = flush_segment(durable_before, durable_before == 0)? else {
        return Ok(FlushProgress {
            appended,
            durable_before,
            durable: durable_before,
            store_timestamp: store_timestamp_before,
        });
    };

    let durable = (segment.file_from_offset + segment.flushed_position as u64) as i64;
    let store_timestamp = if flush_least_pages == 0 {
        segment.store_timestamp
    } else {
        store_timestamp_before
    };
    Ok(FlushProgress {
        appended,
        durable_before,
        durable,
        store_timestamp,
    })
}

/// Result of committing the transient buffer of the selected segment.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentCommitProgress {
    file_from_offset: u64,
    committed_position: i32,
}

impl SegmentCommitProgress {
    /// Captures the concrete segment side effect without exposing its type to Local.
    #[doc(hidden)]
    pub const fn new(file_from_offset: u64, committed_position: i32) -> Self {
        Self {
            file_from_offset,
            committed_position,
        }
    }
}

/// Canonical result of one mapped-file queue commit attempt.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueCommitProgress {
    committed_before: i64,
    committed: i64,
}

impl QueueCommitProgress {
    /// Returns the committed watermark after the attempt.
    #[doc(hidden)]
    pub const fn committed(self) -> i64 {
        self.committed
    }

    /// Preserves the legacy boolean contract: `true` means no watermark advance.
    #[doc(hidden)]
    pub const fn legacy_commit_result(self) -> bool {
        self.committed == self.committed_before
    }
}

/// Drives one transient-buffer commit around a concrete mapped-file adapter.
#[doc(hidden)]
pub fn commit_mapped_file_queue(
    committed_before: i64,
    commit_segment: impl FnOnce(i64, bool) -> Option<SegmentCommitProgress>,
) -> QueueCommitProgress {
    let committed = commit_segment(committed_before, committed_before == 0)
        .map(|segment| (segment.file_from_offset + segment.committed_position as u64) as i64)
        .unwrap_or(committed_before);

    QueueCommitProgress {
        committed_before,
        committed,
    }
}
