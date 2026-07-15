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

//! Runtime-neutral index algorithms for mapped-file queues.

/// Selection returned by an offset lookup.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappedFileQueueIndex {
    /// Return the first file captured by the caller.
    First,
    /// Return an entry from the caller's current collection snapshot.
    Indexed(usize),
}

/// Visits every adjacent pair whose offsets are not separated by one segment.
#[doc(hidden)]
pub fn for_each_discontinuous_pair<T>(
    files: &[T],
    segment_size: u64,
    file_from_offset: impl Fn(&T) -> u64,
    mut visit: impl FnMut(usize, usize),
) {
    for current in 1..files.len() {
        let previous = current - 1;
        if file_from_offset(&files[current]) - file_from_offset(&files[previous]) != segment_size {
            visit(previous, current);
        }
    }
}

/// Returns the contiguous slice window intersecting `[from, to)`.
#[doc(hidden)]
pub fn overlapping_file_range<T>(
    files: &[T],
    from: i64,
    to: i64,
    file_bounds: impl Fn(&T) -> (i64, i64),
) -> std::ops::Range<usize> {
    let mut start = None;
    let mut end = 0;

    for (index, file) in files.iter().enumerate() {
        let (file_from, file_to) = file_bounds(file);
        if file_to <= from {
            continue;
        }
        if file_from >= to {
            break;
        }
        start.get_or_insert(index);
        end = index + 1;
    }

    start.map_or(0..0, |start| start..end)
}

/// Returns the first file whose last-modified timestamp reaches the target.
///
/// If no timestamp reaches the target, the last file is returned.
#[doc(hidden)]
pub fn file_index_by_timestamp<T>(
    files: &[T],
    timestamp: i64,
    last_modified_timestamp: impl Fn(&T) -> i64,
) -> Option<usize> {
    files
        .iter()
        .position(|file| last_modified_timestamp(file) >= timestamp)
        .or_else(|| files.len().checked_sub(1))
}

/// Returns the file containing `offset`, using direct indexing before a linear fallback.
#[doc(hidden)]
pub fn file_index_by_offset<T>(
    files: &[T],
    segment_size: u64,
    offset: i64,
    return_first_on_not_found: bool,
    first_file_offset: u64,
    last_file_offset: u64,
    file_from_offset: impl Fn(&T) -> u64,
) -> Option<MappedFileQueueIndex> {
    let first_offset = first_file_offset as i64;
    let last_offset = last_file_offset as i64 + segment_size as i64;

    if offset < first_offset || offset >= last_offset {
        return return_first_on_not_found.then_some(MappedFileQueueIndex::First);
    }

    let index = (offset as usize / segment_size as usize) - (first_offset as usize / segment_size as usize);
    if let Some(file) = files.get(index) {
        let file_offset = file_from_offset(file) as i64;
        if offset >= file_offset && offset < file_offset + segment_size as i64 {
            return Some(MappedFileQueueIndex::Indexed(index));
        }
    }

    files
        .iter()
        .position(|file| {
            let file_offset = file_from_offset(file) as i64;
            offset >= file_offset && offset < file_offset + segment_size as i64
        })
        .map(MappedFileQueueIndex::Indexed)
        .or_else(|| return_first_on_not_found.then_some(MappedFileQueueIndex::First))
}
