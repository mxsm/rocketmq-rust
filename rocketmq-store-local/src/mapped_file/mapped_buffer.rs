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

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use memmap2::MmapMut;
use parking_lot::RecursiveRwLock;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use super::mapped_file_error::MappedFileViolation;
use super::MappedFileFailure;

/// Safe abstraction over a memory-mapped file region.
///
/// Provides bounds-checked access to a portion of a memory-mapped file,
/// with proper synchronization for concurrent access. All operations
/// verify bounds at runtime to prevent undefined behavior.
///
/// # Thread Safety
///
/// - Multiple readers can access the buffer concurrently
/// - Writes require exclusive access via RwLock
/// - Safe to share across threads via Arc
///
/// # Performance
///
/// - Read operations acquire shared lock (no contention with other readers)
/// - Write operations acquire exclusive lock (blocks all other operations)
/// - Read results own a copy and never borrow the mutable mmap region
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_store::MappedBuffer;
///
/// let buffer = MappedBuffer::from_mmap(mmap, 0, 1024)?;
///
/// // Write data
/// buffer.write(0, b"Hello, World!")?;
///
/// // Owning copied read
/// let data = buffer.read_copy(0..13)?;
/// assert_eq!(&data[..], b"Hello, World!");
/// ```
#[derive(Debug, Clone)]
pub struct MappedBuffer {
    /// Reader preference permits nested read callbacks even when a writer is waiting.
    mmap: Arc<RecursiveRwLock<MmapMut>>,

    /// Starting offset of this buffer within the mmap
    offset: usize,

    /// Length of this buffer
    len: usize,
}

impl MappedBuffer {
    /// Creates a buffer that exclusively takes ownership of a mutable mapping.
    ///
    /// # Errors
    ///
    /// Returns a mapped-file request error when the requested region is outside the mapping.
    pub fn from_mmap(mmap: MmapMut, offset: usize, len: usize) -> Option<Self> {
        Self::from_mmap_checked(mmap, offset, len).ok()
    }

    /// Creates a buffer that exclusively takes ownership of a mutable mapping.
    ///
    /// Cloned buffers and regions continue to share the mapping through this abstraction, while
    /// callers cannot retain a separate mutable mapping handle that bypasses range checks.
    ///
    /// # Errors
    ///
    /// Returns [`MappedFileFailure::OutOfBounds`] when `offset..offset + len` is not fully contained
    /// in the mapping.
    fn from_mmap_checked(mmap: MmapMut, offset: usize, len: usize) -> Result<Self, MappedFileViolation> {
        Self::from_shared_mmap(Arc::new(RecursiveRwLock::new(mmap)), offset, len)
    }

    fn from_shared_mmap(
        mmap: Arc<RecursiveRwLock<MmapMut>>,
        offset: usize,
        len: usize,
    ) -> Result<Self, MappedFileViolation> {
        // Validate bounds
        let mmap_guard = mmap.read();
        let mmap_len = mmap_guard.len();
        drop(mmap_guard);

        if offset.checked_add(len).is_none_or(|end| end > mmap_len) {
            return Err(MappedFileViolation::out_of_bounds(offset, len, mmap_len as u64));
        }

        Ok(Self { mmap, offset, len })
    }

    /// Creates a child buffer over a checked range relative to this buffer.
    ///
    /// # Errors
    ///
    /// Returns [`MappedFileFailure::OutOfBounds`] for reversed or out-of-range bounds.
    fn region_checked(&self, range: Range<usize>) -> Result<Self, MappedFileViolation> {
        let absolute = self.checked_range(range)?;
        Ok(Self {
            mmap: Arc::clone(&self.mmap),
            offset: absolute.start,
            len: absolute.len(),
        })
    }

    /// Creates a child buffer over a checked range relative to this buffer.
    ///
    /// # Errors
    ///
    /// Returns a mapped-file request error for reversed or out-of-range bounds.
    pub fn region(&self, range: Range<usize>) -> Option<Self> {
        self.region_checked(range).ok()
    }

    /// Runs a read operation against a checked range while keeping the lock guard private.
    ///
    /// The callback executes while a recursive mapping read lock is held. It may invoke other
    /// read-only operations on the same mapping, but must not invoke a mutating operation.
    ///
    /// # Errors
    ///
    /// Returns [`MappedFileFailure::OutOfBounds`] for reversed or out-of-range bounds.
    fn with_read_range_checked<T>(
        &self,
        range: Range<usize>,
        read: impl FnOnce(&[u8]) -> T,
    ) -> Result<T, MappedFileViolation> {
        let absolute = self.checked_range(range)?;
        let mmap = self.mmap.read();
        Ok(read(&mmap[absolute]))
    }

    /// Runs a read operation against a checked range while keeping the lock guard private.
    ///
    /// # Errors
    ///
    /// Returns a mapped-file request error for reversed or out-of-range bounds.
    pub fn with_read_range<T>(&self, range: Range<usize>, read: impl FnOnce(&[u8]) -> T) -> Option<T> {
        self.with_read_range_checked(range, read).ok()
    }

    /// Runs a mutation against a checked range while keeping the lock guard private.
    ///
    /// The callback executes while the mapping write lock is held and must not call another method
    /// on a buffer backed by the same mapping.
    ///
    /// # Errors
    ///
    /// Returns [`MappedFileFailure::OutOfBounds`] for reversed or out-of-range bounds.
    fn with_write_range_checked<T>(
        &self,
        range: Range<usize>,
        write: impl FnOnce(&mut [u8]) -> T,
    ) -> Result<T, MappedFileViolation> {
        let absolute = self.checked_range(range)?;
        let mut mmap = self.mmap.write();
        Ok(write(&mut mmap[absolute]))
    }

    /// Runs a mutation against a checked range while keeping the lock guard private.
    ///
    /// # Errors
    ///
    /// Returns a mapped-file request error for reversed or out-of-range bounds.
    pub fn with_write_range<T>(&self, range: Range<usize>, write: impl FnOnce(&mut [u8]) -> T) -> Option<T> {
        self.with_write_range_checked(range, write).ok()
    }

    /// Writes data at the specified offset within this buffer.
    ///
    /// # Arguments
    ///
    /// * `offset` - Offset within this buffer (relative to buffer start)
    /// * `data` - Data to write
    ///
    /// # Returns
    ///
    /// `Ok(())` on success
    ///
    /// # Errors
    ///
    /// Returns `MappedFileFailure::OutOfBounds` if write would exceed buffer bounds
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// buffer.write(0, b"Hello")?;
    /// buffer.write(5, b" World")?;
    /// ```
    fn write_checked(&self, offset: usize, data: &[u8]) -> Result<(), MappedFileViolation> {
        let Some(end) = offset.checked_add(data.len()) else {
            return Err(MappedFileViolation::out_of_bounds(
                self.offset.saturating_add(offset),
                data.len(),
                (self.offset + self.len) as u64,
            ));
        };

        self.with_write_range_checked(offset..end, |destination| destination.copy_from_slice(data))
    }

    /// Writes data at the specified offset within this buffer.
    ///
    /// # Errors
    ///
    /// Returns a mapped-file request error when the write exceeds the buffer bounds.
    pub fn write(&self, offset: usize, data: &[u8]) -> bool {
        self.write_checked(offset, data).is_ok()
    }

    /// Reads data from the specified range into owning bytes.
    ///
    /// # Arguments
    ///
    /// * `range` - Range to read (relative to buffer start)
    ///
    /// # Returns
    ///
    /// A `Bytes` copy of the requested data
    ///
    /// # Errors
    ///
    /// Returns `MappedFileFailure::OutOfBounds` if range exceeds buffer bounds
    ///
    /// # Performance
    ///
    /// This method performs one allocation and copy. Use the mapped-file
    /// selection and transfer APIs when an owning lease or file range is
    /// required.
    fn read_copy_checked(&self, range: Range<usize>) -> Result<Bytes, MappedFileViolation> {
        self.with_read_range_checked(range, Bytes::copy_from_slice)
    }

    /// Reads data from the specified range into owning bytes.
    ///
    /// # Errors
    ///
    /// Returns a mapped-file request error when the range exceeds the buffer bounds.
    pub fn read_copy(&self, range: Range<usize>) -> Option<Bytes> {
        self.read_copy_checked(range).ok()
    }

    /// Batch writes multiple data slices with single lock acquisition.
    ///
    /// # Arguments
    ///
    /// * `writes` - Iterator of (offset, data) pairs
    ///
    /// # Returns
    ///
    /// Total bytes written
    ///
    /// # Errors
    ///
    /// Returns error on first out-of-bounds write
    ///
    /// # Performance
    ///
    /// Much faster than individual writes for batch operations.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let writes = vec![
    ///     (0, b"Header" as &[u8]),
    ///     (6, b"Body"),
    ///     (10, b"Footer"),
    /// ];
    /// let total = buffer.batch_write(writes)?;
    /// ```
    fn batch_write_checked<'a, I>(&self, writes: I) -> Result<usize, MappedFileViolation>
    where
        I: IntoIterator<Item = (usize, &'a [u8])>,
    {
        let writes = writes
            .into_iter()
            .map(|(offset, data)| {
                let end = offset.checked_add(data.len()).ok_or_else(|| {
                    MappedFileViolation::out_of_bounds(
                        self.offset.saturating_add(offset),
                        data.len(),
                        (self.offset + self.len) as u64,
                    )
                })?;
                let absolute = self.checked_range(offset..end)?;
                Ok((absolute, data))
            })
            .collect::<Result<Vec<_>, MappedFileViolation>>()?;
        let mut mmap = self.mmap.write();
        let mut total_written = 0;

        for (absolute, data) in writes {
            mmap[absolute].copy_from_slice(data);
            total_written += data.len();
        }

        Ok(total_written)
    }

    /// Batch writes multiple data slices with one lock acquisition.
    ///
    /// # Errors
    ///
    /// Returns a mapped-file request error for the first out-of-bounds write.
    pub fn batch_write<'a, I>(&self, writes: I) -> Option<usize>
    where
        I: IntoIterator<Item = (usize, &'a [u8])>,
    {
        self.batch_write_checked(writes).ok()
    }

    /// Returns the length of this buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether this buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the starting offset of this buffer within the mmap.
    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Flushes this buffer's region to disk (calls msync).
    ///
    /// # Returns
    ///
    /// `Ok(())` on success
    ///
    /// # Errors
    ///
    /// Returns `MappedFileFailure::FlushFailed` if msync fails
    fn flush_checked(&self) -> Result<(), MappedFileFailure> {
        if self.len == 0 {
            return Ok(());
        }
        let mmap = self.mmap.read();
        mmap.flush_range(self.offset, self.len)
            .map_err(MappedFileFailure::FlushFailed)
    }

    /// Flushes this buffer's region to disk.
    ///
    /// # Errors
    ///
    /// Returns a mapped-file I/O error when the operating-system flush fails.
    pub fn flush(&self) -> Result<(), StoreError> {
        self.flush_checked()
            .map_err(|error| error.into_store_error(StoreOperation::Flush))
    }

    /// Flushes a specific range to disk.
    ///
    /// # Arguments
    ///
    /// * `range` - Range to flush (relative to buffer start)
    ///
    /// # Returns
    ///
    /// `Ok(())` on success
    ///
    /// # Errors
    ///
    /// Returns error if range is invalid or flush fails
    fn flush_range_checked(&self, range: Range<usize>) -> Result<(), MappedFileFailure> {
        let absolute = self.checked_range(range)?;
        if absolute.is_empty() {
            return Ok(());
        }
        let mmap = self.mmap.read();

        mmap.flush_range(absolute.start, absolute.len())
            .map_err(MappedFileFailure::FlushFailed)
    }

    /// Flushes a specific range to disk.
    ///
    /// # Errors
    ///
    /// Returns `Ok(None)` for an invalid range and a mapped-file I/O error when the
    /// operating-system flush fails.
    pub fn flush_range(&self, range: Range<usize>) -> Result<Option<()>, StoreError> {
        match self.flush_range_checked(range) {
            Ok(()) => Ok(Some(())),
            Err(error) if error.is_contract() => Ok(None),
            Err(error) => Err(error.into_store_error(StoreOperation::Flush)),
        }
    }

    fn checked_range(&self, range: Range<usize>) -> Result<Range<usize>, MappedFileViolation> {
        if range.start > range.end || range.end > self.len {
            return Err(MappedFileViolation::out_of_bounds(
                self.offset.saturating_add(range.start),
                range.end.saturating_sub(range.start),
                (self.offset + self.len) as u64,
            ));
        }

        let start = self.offset + range.start;
        let end = self.offset + range.end;
        Ok(start..end)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as IoWrite;

    use tempfile::NamedTempFile;

    use super::*;

    fn create_test_mmap(size: usize) -> MmapMut {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&vec![0u8; size]).unwrap();
        file.flush().unwrap();

        let file = file.reopen().unwrap();
        // SAFETY: The reopened file remains alive while creating the mapping and is not resized
        // while the returned mapping is in use.
        unsafe { MmapMut::map_mut(&file).unwrap() }
    }

    #[test]
    fn test_new_valid_bounds() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 0, 512);
        assert!(buffer.is_some());
    }

    #[test]
    fn test_new_invalid_bounds() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 512, 1024);
        assert!(buffer.is_none());
    }

    #[test]
    fn test_write_read() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 0, 1024).unwrap();

        assert!(buffer.write(0, b"Hello, World!"));
        let data = buffer.read_copy(0..13).unwrap();

        assert_eq!(&data[..], b"Hello, World!");
    }

    #[test]
    fn test_write_out_of_bounds() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 0, 100).unwrap();

        let result = buffer.write(90, &[0u8; 20]);
        assert!(!result);
    }

    #[test]
    fn test_batch_write() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 0, 1024).unwrap();

        let writes = vec![(0, b"Header" as &[u8]), (6, b"Body"), (10, b"Footer")];

        let total = buffer.batch_write(writes).unwrap();
        assert_eq!(total, 16);

        let data = buffer.read_copy(0..16).unwrap();
        assert_eq!(&data[0..6], b"Header");
        assert_eq!(&data[6..10], b"Body");
        assert_eq!(&data[10..16], b"Footer");
    }

    #[test]
    fn copied_reads_cover_alignment_boundaries_and_large_ranges() {
        let mmap = create_test_mmap(256 * 1024);
        let buffer = MappedBuffer::from_mmap(mmap, 0, 256 * 1024).unwrap();
        let expected = (0..256 * 1024).map(|index| (index % 251) as u8).collect::<Vec<_>>();
        assert!(buffer.write(0, &expected));

        for range in [0..1, 0..64, 1..65, 63..8193, 64..65_600, 1_023..200_000] {
            let data = buffer.read_copy(range.clone()).unwrap();
            assert_eq!(&data[..], &expected[range]);
        }

        let source_range = buffer
            .with_read_range(0..buffer.len(), |source| {
                let range = source.as_ptr_range();
                range.start as usize..range.end as usize
            })
            .unwrap();
        let copied = buffer.read_copy(64..65_600).unwrap();
        assert!(
            !source_range.contains(&(copied.as_ptr() as usize)),
            "owning copied bytes must not alias the mutable mmap"
        );
    }

    #[test]
    fn scoped_ranges_reject_reversed_bounds_and_regions_stay_relative() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 128, 512).unwrap();

        assert!(buffer.read_copy(Range { start: 10, end: 9 }).is_none());
        assert!(buffer.with_write_range(Range { start: 12, end: 11 }, |_| ()).is_none());
        assert!(matches!(buffer.flush_range(Range { start: 20, end: 19 }), Ok(None)));

        let region = buffer.region(64..128).unwrap();
        assert_eq!(region.offset(), 192);
        assert_eq!(region.len(), 64);
        assert!(region.write(0, b"scoped"));
        assert_eq!(&buffer.read_copy(64..70).unwrap()[..], b"scoped");
        assert!(buffer.region(500..513).is_none());
    }

    #[test]
    fn read_callbacks_can_reenter_read_only_operations_and_empty_flushes_are_noops() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 128, 256).unwrap();
        assert!(buffer.write(0, b"nested-read"));

        let nested = buffer
            .with_read_range(0..11, |_| buffer.read_copy(0..11))
            .unwrap()
            .unwrap();
        assert_eq!(&nested[..], b"nested-read");
        buffer.flush_range(0..0).unwrap();
        buffer.region(16..16).unwrap().flush().unwrap();
    }

    #[test]
    fn read_callbacks_can_reenter_while_a_writer_is_waiting() {
        let buffer = MappedBuffer::from_mmap(create_test_mmap(1024), 128, 256).unwrap();
        assert!(buffer.write(0, b"nested-read"));
        let region = buffer.region(0..11).unwrap();

        std::thread::scope(|scope| {
            buffer
                .with_read_range(0..11, |outer| {
                    let writer = scope.spawn(|| assert!(buffer.write(0, b"after-write")));
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                    // An upgradable reader is refused once the writer has reserved the lock.
                    while buffer.mmap.try_upgradable_read().is_some() {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "writer did not wait for the reader"
                        );
                        std::thread::yield_now();
                    }

                    // Fail before a blocking recursive read if writer preference is introduced.
                    assert!(buffer.mmap.try_read().is_some());
                    assert_eq!(&buffer.read_copy(0..11).unwrap()[..], outer);
                    assert_eq!(region.with_read_range(0..11, |nested| nested == outer), Some(true));
                    buffer.flush().unwrap();
                    assert_eq!(region.flush_range(0..11).unwrap(), Some(()));
                    writer
                })
                .unwrap()
                .join()
                .unwrap();
        });

        assert_eq!(&buffer.read_copy(0..11).unwrap()[..], b"after-write");
    }
}
