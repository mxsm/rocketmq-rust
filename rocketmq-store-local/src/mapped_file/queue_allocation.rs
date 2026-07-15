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

//! Runtime-neutral segment roll and preallocation decisions for mapped-file queues.

/// Last-file values needed to decide whether a queue should preallocate.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappedFileQueueLastFile {
    file_from_offset: u64,
    wrote_position: i32,
    full: bool,
}

impl MappedFileQueueLastFile {
    /// Creates a last-file decision snapshot.
    #[doc(hidden)]
    pub fn new(file_from_offset: u64, wrote_position: i32, full: bool) -> Self {
        Self {
            file_from_offset,
            wrote_position,
            full,
        }
    }
}

/// Last-file values observed after preallocation when deciding whether to roll.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappedFileQueueRollFile {
    file_from_offset: u64,
    full: bool,
}

impl MappedFileQueueRollFile {
    /// Creates a post-preallocation roll snapshot.
    #[doc(hidden)]
    pub fn new(file_from_offset: u64, full: bool) -> Self {
        Self { file_from_offset, full }
    }
}

/// Plans background preallocation without performing I/O.
#[doc(hidden)]
pub fn plan_mapped_file_queue_preallocation(segment_size: u64, last_file: MappedFileQueueLastFile) -> Option<u64> {
    let usage_ratio = last_file.wrote_position as f64 / segment_size as f64;
    (usage_ratio >= 0.8 && !last_file.full).then_some(last_file.file_from_offset + segment_size)
}

/// Plans current-request segment creation without performing I/O.
#[doc(hidden)]
pub fn plan_mapped_file_queue_creation(
    start_offset: u64,
    segment_size: u64,
    last_file: Option<MappedFileQueueRollFile>,
    need_create: bool,
) -> Option<u64> {
    match last_file {
        None => {
            let start_offset = start_offset as i64;
            let segment_size = segment_size as i64;
            Some((start_offset - start_offset % segment_size) as u64)
        }
        Some(last) if last.full => Some((last.file_from_offset as i64 + segment_size as i64) as u64),
        Some(_) => None,
    }
    .filter(|_| need_create)
}
