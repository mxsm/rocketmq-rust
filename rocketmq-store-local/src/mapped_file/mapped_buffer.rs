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
use parking_lot::RwLock;

use super::MappedFileError;
use super::MappedFileResult;

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
    /// Shared reference to the memory-mapped file
    mmap: Arc<RwLock<MmapMut>>,

    /// Starting offset of this buffer within the mmap
    offset: usize,

    /// Length of this buffer
    len: usize,
}

impl MappedBuffer {
    /// Creates a buffer that exclusively takes ownership of a mutable mapping.
    ///
    /// Cloned buffers and regions continue to share the mapping through this abstraction, while
    /// callers cannot retain a separate mutable mapping handle that bypasses range checks.
    ///
    /// # Errors
    ///
    /// Returns [`MappedFileError::OutOfBounds`] when `offset..offset + len` is not fully contained
    /// in the mapping.
    pub fn from_mmap(mmap: MmapMut, offset: usize, len: usize) -> MappedFileResult<Self> {
        Self::from_shared_mmap(Arc::new(RwLock::new(mmap)), offset, len)
    }

    /// Creates a new `MappedBuffer` over the specified region.
    ///
    /// # Arguments
    ///
    /// * `mmap` - Shared mmap reference
    /// * `offset` - Starting offset within the mmap
    /// * `len` - Length of the buffer region
    ///
    /// # Returns
    ///
    /// A new buffer or an error if the region is out of bounds
    ///
    /// # Errors
    ///
    /// Returns `MappedFileError::OutOfBounds` if `offset + len` exceeds mmap size
    #[deprecated(note = "use from_mmap and region so mutable mapping ownership stays encapsulated; removal in 2.0.0")]
    pub fn new(mmap: Arc<RwLock<MmapMut>>, offset: usize, len: usize) -> MappedFileResult<Self> {
        Self::from_shared_mmap(mmap, offset, len)
    }

    fn from_shared_mmap(mmap: Arc<RwLock<MmapMut>>, offset: usize, len: usize) -> MappedFileResult<Self> {
        // Validate bounds
        let mmap_guard = mmap.read();
        let mmap_len = mmap_guard.len();
        drop(mmap_guard);

        if offset.checked_add(len).is_none_or(|end| end > mmap_len) {
            return Err(MappedFileError::out_of_bounds(offset, len, mmap_len as u64));
        }

        Ok(Self { mmap, offset, len })
    }

    /// Creates a child buffer over a checked range relative to this buffer.
    ///
    /// # Errors
    ///
    /// Returns [`MappedFileError::OutOfBounds`] for reversed or out-of-range bounds.
    pub fn region(&self, range: Range<usize>) -> MappedFileResult<Self> {
        let absolute = self.checked_range(range)?;
        Ok(Self {
            mmap: Arc::clone(&self.mmap),
            offset: absolute.start,
            len: absolute.len(),
        })
    }

    /// Runs a read operation against a checked range while keeping the lock guard private.
    ///
    /// The callback executes while a recursive mapping read lock is held. It may invoke other
    /// read-only operations on the same mapping, but must not invoke a mutating operation.
    ///
    /// # Errors
    ///
    /// Returns [`MappedFileError::OutOfBounds`] for reversed or out-of-range bounds.
    pub fn with_read_range<T>(&self, range: Range<usize>, read: impl FnOnce(&[u8]) -> T) -> MappedFileResult<T> {
        let absolute = self.checked_range(range)?;
        let mmap = self.mmap.read_recursive();
        Ok(read(&mmap[absolute]))
    }

    /// Runs a mutation against a checked range while keeping the lock guard private.
    ///
    /// The callback executes while the mapping write lock is held and must not call another method
    /// on a buffer backed by the same mapping.
    ///
    /// # Errors
    ///
    /// Returns [`MappedFileError::OutOfBounds`] for reversed or out-of-range bounds.
    pub fn with_write_range<T>(&self, range: Range<usize>, write: impl FnOnce(&mut [u8]) -> T) -> MappedFileResult<T> {
        let absolute = self.checked_range(range)?;
        let mut mmap = self.mmap.write();
        Ok(write(&mut mmap[absolute]))
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
    /// Returns `MappedFileError::OutOfBounds` if write would exceed buffer bounds
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// buffer.write(0, b"Hello")?;
    /// buffer.write(5, b" World")?;
    /// ```
    pub fn write(&self, offset: usize, data: &[u8]) -> MappedFileResult<()> {
        let Some(end) = offset.checked_add(data.len()) else {
            return Err(MappedFileError::out_of_bounds(
                self.offset.saturating_add(offset),
                data.len(),
                (self.offset + self.len) as u64,
            ));
        };

        self.with_write_range(offset..end, |destination| destination.copy_from_slice(data))
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
    /// Returns `MappedFileError::OutOfBounds` if range exceeds buffer bounds
    ///
    /// # Performance
    ///
    /// This method performs one allocation and copy. Use the mapped-file
    /// selection and transfer APIs when an owning lease or file range is
    /// required.
    pub fn read_copy(&self, range: Range<usize>) -> MappedFileResult<Bytes> {
        self.with_read_range(range, Bytes::copy_from_slice)
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
    pub fn batch_write<'a, I>(&self, writes: I) -> MappedFileResult<usize>
    where
        I: IntoIterator<Item = (usize, &'a [u8])>,
    {
        let writes = writes
            .into_iter()
            .map(|(offset, data)| {
                let end = offset.checked_add(data.len()).ok_or_else(|| {
                    MappedFileError::out_of_bounds(
                        self.offset.saturating_add(offset),
                        data.len(),
                        (self.offset + self.len) as u64,
                    )
                })?;
                let absolute = self.checked_range(offset..end)?;
                Ok((absolute, data))
            })
            .collect::<MappedFileResult<Vec<_>>>()?;
        let mut mmap = self.mmap.write();
        let mut total_written = 0;

        for (absolute, data) in writes {
            mmap[absolute].copy_from_slice(data);
            total_written += data.len();
        }

        Ok(total_written)
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
    /// Returns `MappedFileError::FlushFailed` if msync fails
    pub fn flush(&self) -> MappedFileResult<()> {
        if self.len == 0 {
            return Ok(());
        }
        let mmap = self.mmap.read_recursive();
        mmap.flush_range(self.offset, self.len)
            .map_err(MappedFileError::FlushFailed)
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
    pub fn flush_range(&self, range: Range<usize>) -> MappedFileResult<()> {
        let absolute = self.checked_range(range)?;
        if absolute.is_empty() {
            return Ok(());
        }
        let mmap = self.mmap.read_recursive();

        mmap.flush_range(absolute.start, absolute.len())
            .map_err(MappedFileError::FlushFailed)
    }

    fn checked_range(&self, range: Range<usize>) -> MappedFileResult<Range<usize>> {
        if range.start > range.end || range.end > self.len {
            return Err(MappedFileError::out_of_bounds(
                self.offset.saturating_add(range.start),
                range.end.saturating_sub(range.start),
                (self.offset + self.len) as u64,
            ));
        }

        let start = self.offset + range.start;
        let end = self.offset + range.end;
        Ok(start..end)
    }

    /// Returns a clone of the underlying mmap Arc for legacy integrations.
    ///
    /// Direct access bypasses this buffer's region invariant. New code should use the scoped,
    /// range-checked operations instead.
    #[deprecated(note = "use read_copy, with_read_range, write, with_write_range, or flush_range; removal in 2.0.0")]
    pub fn get_mmap(&self) -> Arc<RwLock<MmapMut>> {
        Arc::clone(&self.mmap)
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
        assert!(buffer.is_ok());
    }

    #[test]
    fn test_new_invalid_bounds() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 512, 1024);
        assert!(buffer.is_err());
    }

    #[test]
    fn test_write_read() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 0, 1024).unwrap();

        buffer.write(0, b"Hello, World!").unwrap();
        let data = buffer.read_copy(0..13).unwrap();

        assert_eq!(&data[..], b"Hello, World!");
    }

    #[test]
    fn test_write_out_of_bounds() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 0, 100).unwrap();

        let result = buffer.write(90, &[0u8; 20]);
        assert!(result.is_err());
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
        buffer.write(0, &expected).unwrap();

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

        assert!(buffer.read_copy(Range { start: 10, end: 9 }).is_err());
        assert!(buffer.with_write_range(Range { start: 12, end: 11 }, |_| ()).is_err());
        assert!(buffer.flush_range(Range { start: 20, end: 19 }).is_err());

        let region = buffer.region(64..128).unwrap();
        assert_eq!(region.offset(), 192);
        assert_eq!(region.len(), 64);
        region.write(0, b"scoped").unwrap();
        assert_eq!(&buffer.read_copy(64..70).unwrap()[..], b"scoped");
        assert!(buffer.region(500..513).is_err());
    }

    #[test]
    fn read_callbacks_can_reenter_read_only_operations_and_empty_flushes_are_noops() {
        let mmap = create_test_mmap(1024);
        let buffer = MappedBuffer::from_mmap(mmap, 128, 256).unwrap();
        buffer.write(0, b"nested-read").unwrap();

        let nested = buffer
            .with_read_range(0..11, |_| buffer.read_copy(0..11))
            .unwrap()
            .unwrap();
        assert_eq!(&nested[..], b"nested-read");
        buffer.flush_range(0..0).unwrap();
        buffer.region(16..16).unwrap().flush().unwrap();
    }
}
