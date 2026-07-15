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

//! Runtime-neutral dirty-tail truncation and reset planning for mapped-file queues.

/// Action applied to one mapped file while truncating a dirty queue tail.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappedFileQueueTruncateAction {
    Retain,
    Truncate(i32),
    Remove,
}

/// Selects the dirty-tail action for one mapped-file segment.
#[doc(hidden)]
pub fn mapped_file_queue_truncate_action(
    offset: i64,
    mapped_file_size: u64,
    file_from_offset: u64,
) -> MappedFileQueueTruncateAction {
    let file_tail_offset = file_from_offset + mapped_file_size;
    if file_tail_offset as i64 <= offset {
        MappedFileQueueTruncateAction::Retain
    } else if offset >= file_from_offset as i64 {
        MappedFileQueueTruncateAction::Truncate((offset % mapped_file_size as i64) as i32)
    } else {
        MappedFileQueueTruncateAction::Remove
    }
}

/// Last-file values captured before loading the reset collection snapshot.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MappedFileQueueResetLastFile {
    file_from_offset: u64,
    wrote_position: i32,
}

impl MappedFileQueueResetLastFile {
    /// Creates a last-file reset snapshot.
    #[doc(hidden)]
    pub fn new(file_from_offset: u64, wrote_position: i32) -> Self {
        Self {
            file_from_offset,
            wrote_position,
        }
    }
}

/// Reset operations to apply to the Store-owned mapped-file collection.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedFileQueueResetPlan {
    target: Option<(usize, i32)>,
    remove_indices: Vec<usize>,
}

impl MappedFileQueueResetPlan {
    /// Returns the retained target file index and its reset position.
    #[doc(hidden)]
    pub fn target(&self) -> Option<(usize, i32)> {
        self.target
    }

    /// Returns removal indices in the same newest-to-oldest order as the legacy scan.
    #[doc(hidden)]
    pub fn remove_indices(&self) -> &[usize] {
        &self.remove_indices
    }
}

/// Plans a recovery reset while preserving the legacy two-snapshot observation model.
#[doc(hidden)]
pub fn plan_mapped_file_queue_reset<T, O, S>(
    offset: i64,
    mapped_file_size: u64,
    last_file: Option<MappedFileQueueResetLastFile>,
    files: &[T],
    file_from_offset: O,
    file_size: S,
) -> Option<MappedFileQueueResetPlan>
where
    O: Fn(&T) -> u64,
    S: Fn(&T) -> u64,
{
    if let Some(last_file) = last_file {
        let last_offset = last_file.file_from_offset as i64 + last_file.wrote_position as i64;
        let diff = last_offset - offset;
        let max_diff = (mapped_file_size * 2) as i64;
        if diff > max_diff {
            return None;
        }
    }

    let mut remove_indices = Vec::new();
    for index in (0..files.len()).rev() {
        let file = &files[index];
        if offset >= file_from_offset(file) as i64 {
            let target_position = (offset % file_size(file) as i64) as i32;
            return Some(MappedFileQueueResetPlan {
                target: Some((index, target_position)),
                remove_indices,
            });
        }
        remove_indices.push(index);
    }

    Some(MappedFileQueueResetPlan {
        target: None,
        remove_indices,
    })
}
