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

//! Raw byte-range and progress operations for one mapped-file segment.

use super::kernel::MappedFileProgress;

/// Owns mapped-file byte-range rules and atomic progress without owning a mapping.
///
/// Callers lend the currently active mapped bytes to individual operations. This
/// keeps mapping/reference/platform lifecycles outside the raw state owner.
pub struct MappedFileRawCore {
    progress: MappedFileProgress,
}

impl MappedFileRawCore {
    /// Creates empty raw state for a segment with the configured size.
    #[inline]
    pub fn new(file_size: u64) -> Self {
        Self {
            progress: MappedFileProgress::new(file_size),
        }
    }

    #[inline]
    pub fn file_size(&self) -> u64 {
        self.progress.file_size()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.progress.is_full()
    }

    #[inline]
    pub fn wrote_position(&self) -> i32 {
        self.progress.wrote_position()
    }

    #[inline]
    pub fn wrote_position_relaxed(&self) -> i32 {
        self.progress.wrote_position_relaxed()
    }

    #[inline]
    pub fn set_wrote_position(&self, position: i32) {
        self.progress.set_wrote_position(position);
    }

    #[inline]
    pub fn advance_wrote_position(&self, delta: i32) {
        self.progress.advance_wrote_position(delta);
    }

    #[inline]
    pub fn committed_position(&self) -> i32 {
        self.progress.committed_position()
    }

    #[inline]
    pub fn set_committed_position(&self, position: i32) {
        self.progress.set_committed_position(position);
    }

    #[inline]
    pub fn set_committed_position_release(&self, position: i32) {
        self.progress.set_committed_position_release(position);
    }

    #[inline]
    pub fn flushed_position(&self) -> i32 {
        self.progress.flushed_position()
    }

    #[inline]
    pub fn set_flushed_position(&self, position: i32) {
        self.progress.set_flushed_position(position);
    }

    /// Returns readable progress when writes go directly to the mapped bytes.
    #[inline]
    pub fn normal_read_position(&self) -> i32 {
        self.progress.wrote_position()
    }

    /// Returns readable progress when writes must first be committed from a transient buffer.
    #[inline]
    pub fn transient_read_position(&self) -> i32 {
        self.progress.committed_position()
    }

    /// Returns a source slice for a copied read without checking readable progress.
    ///
    /// The provider is invoked only after the configured file-range check succeeds. A provider
    /// that returns a slice shorter than the configured file size is rejected without panicking.
    #[inline]
    pub fn copied_read_slice<'a, F>(&self, mapped: F, pos: usize, size: usize) -> Option<&'a [u8]>
    where
        F: FnOnce() -> &'a [u8],
    {
        if pos + size > self.file_size() as usize {
            return None;
        }
        mapped().get(pos..pos + size)
    }

    /// Returns a source slice for a copied read bounded by the supplied readable position.
    #[inline]
    pub fn copied_readable_slice<'a, F>(
        &self,
        mapped: F,
        pos: usize,
        size: usize,
        readable_position: i32,
    ) -> Option<&'a [u8]>
    where
        F: FnOnce() -> &'a [u8],
    {
        let end_position = pos + size;
        if (readable_position as usize) < end_position || end_position > self.file_size() as usize {
            return None;
        }
        mapped().get(pos..end_position)
    }

    /// Appends bytes at the current write position and advances progress on success.
    pub fn append_bytes_with_position_update<'a, F>(&self, mapped: F, data: &[u8], offset: usize, length: usize) -> bool
    where
        F: FnOnce() -> &'a mut [u8],
    {
        let current_pos = self.wrote_position() as usize;
        if current_pos + length <= self.file_size() as usize {
            if let Some(target) = mapped().get_mut(current_pos..current_pos + length) {
                if let Some(data_slice) = data.get(offset..offset + length) {
                    target.copy_from_slice(data_slice);
                    self.advance_wrote_position(length as i32);
                    return true;
                }
            }
        }
        false
    }

    /// Appends bytes at the current relaxed write position without changing progress.
    pub fn append_bytes_without_position_update<'a, F>(
        &self,
        mapped: F,
        data: &[u8],
        offset: usize,
        length: usize,
    ) -> bool
    where
        F: FnOnce() -> &'a mut [u8],
    {
        let current_pos = self.wrote_position_relaxed() as usize;
        if current_pos + length <= self.file_size() as usize {
            if let Some(target) = mapped().get_mut(current_pos..current_pos + length) {
                if let Some(data_slice) = data.get(offset..offset + length) {
                    target.copy_from_slice(data_slice);
                    return true;
                }
            }
        }
        false
    }

    /// Borrows the direct-write range beginning at the current write position.
    pub fn direct_write_range<'a, F>(&self, mapped: F, required_space: usize) -> Option<(&'a mut [u8], usize)>
    where
        F: FnOnce() -> &'a mut [u8],
    {
        let current_pos = self.wrote_position() as usize;
        if current_pos + required_space > self.file_size() as usize {
            return None;
        }
        mapped()
            .get_mut(current_pos..current_pos + required_space)
            .map(|buffer| (buffer, current_pos))
    }

    /// Commits a non-empty direct write when it fits in the segment.
    pub fn commit_direct_write(&self, bytes_written: usize) -> bool {
        if bytes_written == 0 {
            return false;
        }
        let current_pos = self.wrote_position() as usize;
        if current_pos + bytes_written > self.file_size() as usize {
            return false;
        }
        self.advance_wrote_position(bytes_written as i32);
        true
    }

    /// Writes one segment without changing progress.
    pub fn write_bytes_segment<'a, F>(&self, mapped: F, data: &[u8], start: usize, offset: usize, length: usize) -> bool
    where
        F: FnOnce() -> &'a mut [u8],
    {
        if start + length <= self.file_size() as usize {
            if let Some(target) = mapped().get_mut(start..start + length) {
                if data.len() == length {
                    target.copy_from_slice(data);
                    return true;
                }
                if let Some(data_slice) = data.get(offset..offset + length) {
                    target.copy_from_slice(data_slice);
                    return true;
                }
            }
        }
        false
    }

    /// Writes a non-empty slice at an absolute index without changing progress.
    pub fn put_slice<'a, F>(&self, mapped: F, data: &[u8], index: usize) -> bool
    where
        F: FnOnce() -> &'a mut [u8],
    {
        let length = data.len();
        let end_index = index + length;
        if length > 0 && end_index <= self.file_size() as usize {
            if let Some(target) = mapped().get_mut(index..end_index) {
                target.copy_from_slice(data);
                return true;
            }
        }
        false
    }

    /// Returns a readable raw slice, preserving the legacy exact-end rejection.
    #[inline]
    pub fn raw_slice<'a, F>(&self, mapped: F, pos: usize, size: usize) -> Option<&'a [u8]>
    where
        F: FnOnce() -> &'a [u8],
    {
        if pos >= self.file_size() as usize || pos + size >= self.file_size() as usize {
            return None;
        }
        mapped().get(pos..pos + size)
    }

    /// Returns a raw slice when its end is within the supplied readable position.
    #[inline]
    pub fn readable_slice<'a, F>(&self, mapped: F, pos: usize, size: usize, readable_position: i32) -> Option<&'a [u8]>
    where
        F: FnOnce() -> &'a [u8],
    {
        let end_position = pos + size;
        if end_position > readable_position as usize {
            return None;
        }
        mapped().get(pos..end_position)
    }

    /// Checks a byte range against a supplied readable position.
    #[inline]
    pub fn is_readable_byte_range(&self, pos: usize, size: usize, readable_position: i32) -> bool {
        pos + size <= readable_position as usize
    }

    /// Preserves the legacy signed range predicate used by mapped-buffer selection.
    #[inline]
    pub fn is_readable_range(&self, pos: i32, size: i32, readable_position: i32) -> bool {
        pos + size <= readable_position
    }

    /// Returns the legacy readable tail size for mapped-buffer selection.
    #[inline]
    pub fn readable_tail_size(&self, pos: i32, readable_position: i32) -> Option<i32> {
        if pos < readable_position && pos >= 0 {
            Some(readable_position - pos)
        } else {
            None
        }
    }

    /// Checks whether a range fits in the configured file size.
    #[inline]
    pub fn is_file_range(&self, pos: usize, size: usize) -> bool {
        pos + size <= self.file_size() as usize
    }

    #[inline]
    pub fn record_append(&self, wrote_bytes: i32, store_timestamp: u64) {
        self.progress.record_append(wrote_bytes, store_timestamp);
    }

    #[inline]
    pub fn store_timestamp(&self) -> u64 {
        self.progress.store_timestamp()
    }

    #[inline]
    pub fn set_store_timestamp(&self, timestamp: u64) {
        self.progress.set_store_timestamp(timestamp);
    }

    #[inline]
    pub fn is_able_to_flush(&self, readable_position: i32, flush_least_pages: i32) -> bool {
        self.progress.is_able_to_flush(readable_position, flush_least_pages)
    }

    #[inline]
    pub fn record_flush_success(&self, position: i32) {
        self.progress.record_flush_success(position);
    }

    #[inline]
    pub fn record_flush_time(&self) {
        self.progress.record_flush_time();
    }

    #[inline]
    pub fn last_flush_time(&self) -> u64 {
        self.progress.last_flush_time()
    }

    /// Advances committed progress to wrote progress when the threshold is met.
    #[inline]
    pub fn commit(&self, commit_least_pages: i32) -> i32 {
        if self.progress.is_able_to_commit(commit_least_pages) {
            self.progress.commit_wrote_position();
        }
        self.progress.committed_position()
    }

    #[inline]
    pub fn is_able_to_commit(&self, commit_least_pages: i32) -> bool {
        self.progress.is_able_to_commit(commit_least_pages)
    }

    #[inline]
    pub fn start_timestamp(&self) -> i64 {
        self.progress.start_timestamp()
    }

    #[inline]
    pub fn set_start_timestamp(&self, timestamp: i64) {
        self.progress.set_start_timestamp(timestamp);
    }

    #[inline]
    pub fn stop_timestamp(&self) -> i64 {
        self.progress.stop_timestamp()
    }

    #[inline]
    pub fn set_stop_timestamp(&self, timestamp: i64) {
        self.progress.set_stop_timestamp(timestamp);
    }

    #[inline]
    pub fn lock_region_range(&self, offset: u64, requested_len: usize) -> Option<(usize, usize)> {
        self.progress.lock_region_range(offset, requested_len)
    }

    #[inline]
    pub fn is_valid_cache_range(&self, position: i64, size: usize) -> bool {
        self.progress.is_valid_cache_range(position, size)
    }

    /// Validates a non-empty flush range.
    pub fn prepare_flush_range(&self, start: usize, end: usize) -> Option<(usize, usize)> {
        if start >= end || end > self.file_size() as usize {
            return None;
        }
        Some((start, end - start))
    }

    /// Records the committed end of a validated transient-store flush range.
    pub fn record_transient_flush_range(&self, end: usize) {
        self.set_committed_position_release(end as i32);
    }
}
