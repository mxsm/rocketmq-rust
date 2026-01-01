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

use std::fs::File;
use std::io;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;

use crate::base::append_message_callback::AppendMessageCallback;
use crate::base::compaction_append_msg_callback::CompactionAppendMsgCallback;
use crate::base::message_result::AppendMessageResult;
use crate::base::put_message_context::PutMessageContext;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::flush_disk_type::FlushDiskType;

pub mod default_mapped_file_impl;
pub(crate) mod reference_resource;
mod reference_resource_counter;

// New modules for storage optimization
mod builder;
mod flush_strategy;
mod mapped_buffer;
mod mapped_file_error;
mod metrics;

// io_uring implementation
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub mod io_uring_impl;

/*
// Factory for creating MappedFile instances
pub mod factory;
*/

// Re-export commonly used types
pub use builder::MappedFileBuilder;
/*pub use factory::MappedFileConfig;
pub use factory::MappedFileFactory;
pub use factory::MappedFileType;*/
pub use flush_strategy::FlushStrategy;
// Re-export io_uring implementation
/*#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use io_uring_impl::IoUringMappedFile;*/
pub use mapped_buffer::MappedBuffer;
pub use mapped_file_error::MappedFileError;
pub use mapped_file_error::MappedFileResult;
pub use metrics::MappedFileMetrics;

pub trait MappedFile {
    /// Returns the file name of the mapped file.
    ///
    /// # Returns
    /// A `String` representing the name of the file.
    fn get_file_name(&self) -> &CheetahString;

    /// Renames the mapped file to the specified file name.
    ///
    /// # Arguments
    /// * `file_name` - The new name for the file.
    ///
    /// # Returns
    /// `true` if the file was successfully renamed, `false` otherwise.
    fn rename_to(&mut self, file_name: &str) -> bool;

    /// Retrieves the size of the mapped file in bytes.
    ///
    /// # Returns
    /// The size of the file in bytes as a `u64`.
    fn get_file_size(&self) -> u64;

    /// Checks if the mapped file is full.
    ///
    /// # Returns
    /// `true` if the file is full, `false` otherwise.
    fn is_full(&self) -> bool;

    /// Determines if the mapped file is available for operations.
    ///
    /// # Returns
    /// `true` if the file is available, `false` otherwise.
    fn is_available(&self) -> bool;

    /// Appends a single message to the mapped file.
    ///
    /// This method is responsible for appending a single message to the mapped file using the
    /// provided message callback for processing. It is typically used for appending individual
    /// messages to the store.
    ///
    /// # Type Parameters
    /// - `AMC`: A type that implements the `AppendMessageCallback` trait, used for processing the
    ///   message.
    ///
    /// # Arguments
    /// * `message`: A mutable reference to a `MessageExtBrokerInner` representing the message to be
    ///   appended.
    /// * `message_callback`: A reference to the implementor of `AppendMessageCallback` used for
    ///   message processing.
    /// * `put_message_context`: A reference to the `PutMessageContext` providing context for the
    ///   message being appended.
    ///
    /// # Returns
    /// An `AppendMessageResult` indicating the result of the append operation.
    fn append_message<AMC: AppendMessageCallback>(
        &self,
        message: &mut MessageExtBrokerInner,
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;

    /// Appends a batch of messages to the mapped file.
    ///
    /// This method allows for appending a batch of messages to the mapped file, which can improve
    /// throughput by reducing the number of individual write operations. The method uses a
    /// provided message callback for processing the batch of messages.
    ///
    /// # Type Parameters
    /// - `AMC`: A type that implements the `AppendMessageCallback` trait, used for processing the
    ///   messages.
    ///
    /// # Arguments
    /// * `message`: A mutable reference to a `MessageExtBatch` representing the batch of messages
    ///   to be appended.
    /// * `message_callback`: A reference to the implementor of `AppendMessageCallback` used for
    ///   message processing.
    /// * `put_message_context`: A mutable reference to the `PutMessageContext` providing context
    ///   for the messages being appended.
    /// * `enabled_append_prop_crc`: A boolean flag indicating whether to enable CRC checks for
    ///   message properties.
    ///
    /// # Returns
    /// An `AppendMessageResult` indicating the result of the append operation.
    fn append_messages<AMC: AppendMessageCallback>(
        &self,
        message: &mut MessageExtBatch,
        message_callback: &AMC,
        put_message_context: &mut PutMessageContext,
        enabled_append_prop_crc: bool,
    ) -> AppendMessageResult;

    /// Appends a message for compaction to the mapped file.
    ///
    /// This method is specifically designed for appending messages that are subject to compaction.
    /// It allows for the efficient storage of messages that may be compacted or merged based on
    /// certain criteria.
    ///
    /// # Arguments
    /// * `byte_buffer_msg`: A mutable reference to a `Bytes` buffer containing the message to be
    ///   compacted and appended.
    /// * `cb`: A reference to an implementor of the `CompactionAppendMsgCallback` trait used for
    ///   processing the message.
    ///
    /// # Returns
    /// An `AppendMessageResult` indicating the result of the append operation.
    fn append_message_compaction(
        &mut self,
        byte_buffer_msg: &mut bytes::Bytes,
        cb: &dyn CompactionAppendMsgCallback,
    ) -> AppendMessageResult;

    /// Appends a byte array to the mapped file.
    ///
    /// This method appends a given byte array to the mapped file. It is a convenience method that
    /// internally calls `append_message_offset_length` with the offset set to 0 and length set
    /// to the byte array's length.
    ///
    /// # Arguments
    /// * `data` - A reference to the byte array to be appended.
    ///
    /// # Returns
    /// `true` if the append operation was successful, `false` otherwise.
    fn append_message_bytes(&self, data: &[u8]) -> bool {
        self.append_message_offset_length(data, 0, data.len())
    }

    /// Appends a byte array to the mapped file with specified offset and length.
    ///
    /// This method allows for more controlled appending of bytes to the mapped file by specifying
    /// an offset and length. This can be useful for appending parts of a byte array or for
    /// advanced manipulation of file contents.
    ///
    /// # Arguments
    /// * `data` - A reference to the byte array to be appended.
    /// * `offset` - The starting offset in the byte array from where bytes should be appended.
    /// * `length` - The number of bytes to append starting from the offset.
    ///
    /// # Returns
    /// `true` if the append operation was successful, `false` otherwise.
    fn append_message_offset_length(&self, data: &[u8], offset: usize, length: usize) -> bool;

    /// Selects a portion of the mapped file based on the specified size.
    ///
    /// This method provides a way to access a specific portion of the mapped file, identified by
    /// the starting position (`pos`) and the size (`size`). It is useful for reading or modifying
    /// sections of the file without needing to load the entire file into memory.
    ///
    /// # Arguments
    /// * `pos` - The starting position from which the buffer is selected.
    /// * `size` - The size of the buffer to select.
    ///
    /// # Returns
    /// An `Option<SelectMappedBufferResult>` containing the selected buffer if available, or `None`
    /// if the specified range is not valid or the file is not available.
    fn select_mapped_buffer(&self, pos: i32, size: i32) -> Option<SelectMappedBufferResult>;

    /// Selects a buffer from the mapped file starting from the specified position.
    ///
    /// Similar to `select_mapped_buffer_size`, but selects the buffer starting from `pos` to the
    /// end of the file. This method is useful for operations that require processing the
    /// remainder of the file from a given position.
    ///
    /// # Arguments
    /// * `pos` - The starting position from which the buffer is selected.
    ///
    /// # Returns
    /// An `Option<SelectMappedBufferResult>` containing the selected buffer if available, or `None`
    /// if the starting position is not valid or the file is not available.
    fn select_mapped_buffer_with_position(&self, pos: i32) -> Option<SelectMappedBufferResult>;

    /// Retrieves a byte slice from the mapped file.
    ///
    /// This method returns a byte slice starting from the specified position and of the specified
    /// size. It is useful for reading parts of the file without loading the entire file into
    /// memory.
    ///
    /// # Arguments
    /// * `pos` - The starting position from where bytes should be read.
    /// * `size` - The number of bytes to read from the starting position.
    ///
    /// # Returns
    /// An `Option<bytes::Bytes>` containing the requested byte slice if available, or `None` if the
    /// requested slice goes beyond the file boundaries or the file is not available.
    fn get_bytes(&self, pos: usize, size: usize) -> Option<bytes::Bytes>;

    /// Retrieves a byte slice from the mapped file with readable bounds checking.
    ///
    /// This method returns a byte slice starting from the specified position and of the specified
    /// size. It performs bounds checking to ensure the requested slice is within the file
    /// boundaries.
    ///
    /// # Arguments
    ///
    /// * `pos` - The starting position from where bytes should be read.
    /// * `size` - The number of bytes to read from the starting position.
    ///
    /// # Returns
    ///
    /// An `Option<bytes::Bytes>` containing the requested byte slice if available, or `None` if the
    /// requested slice goes beyond the file boundaries or the file is not available.
    fn get_bytes_readable_checked(&self, pos: usize, size: usize) -> Option<bytes::Bytes>;

    /// Appends a byte array to the mapped file without updating the write position.
    ///
    /// This method appends the given byte array to the mapped file without updating the internal
    /// write position. It is useful for scenarios where the write position should remain unchanged.
    ///
    /// # Arguments
    /// * `data` - A reference to the byte array to be appended.
    ///
    /// # Returns
    /// `true` if the append operation was successful, `false` otherwise.
    fn append_message_bytes_no_position_update(&self, data: &bytes::Bytes) -> bool {
        self.append_message_no_position_update(data.as_ref(), 0, data.len())
    }

    /// Appends a byte array to the mapped file without updating the write position.
    ///
    /// This method appends the given byte array to the mapped file without updating the internal
    /// write position. It is useful for scenarios where the write position should remain unchanged.
    ///
    /// # Arguments
    /// * `data` - A reference to the byte array to be appended.
    ///
    /// # Returns
    /// `true` if the append operation was successful, `false` otherwise.
    fn append_message_bytes_no_position_update_ref(&self, data: &[u8]) -> bool {
        self.append_message_no_position_update(data, 0, data.len())
    }

    /// Appends a byte array to the mapped file without updating the write position.
    ///
    /// This method appends a specified portion of the given byte array to the mapped file without
    /// updating the internal write position. It allows for more controlled appending by specifying
    /// an offset and length.
    ///
    /// # Arguments
    /// * `data` - A reference to the byte array to be appended.
    /// * `offset` - The starting offset in the byte array from where bytes should be appended.
    /// * `length` - The number of bytes to append starting from the offset.
    ///
    /// # Returns
    /// `true` if the append operation was successful, `false` otherwise.
    fn append_message_no_position_update(&self, data: &[u8], offset: usize, length: usize) -> bool;

    /// **Phase 3 Optimization**: Gets a direct mutable buffer slice for zero-copy message encoding.
    ///
    /// This method returns a mutable byte slice directly pointing to the mmap region, allowing
    /// message encoding to occur directly in the target buffer without intermediate copies.
    ///
    /// # Performance Benefits
    /// - **Eliminates memory copy**: No copy from pre_encode_buffer to mmap
    /// - **CPU reduction**: 20-30% less CPU usage during message append
    /// - **Throughput increase**: 15-25% higher throughput
    ///
    /// # Arguments
    /// * `required_space` - The minimum number of bytes required in the buffer
    ///
    /// # Returns
    /// `Option<(&mut [u8], usize)>` - A tuple of (mutable buffer, start position) if space is
    /// available, or `None` if insufficient space remains
    ///
    /// # Safety
    /// The returned buffer is safe to write to, but the caller must ensure:
    /// 1. Data written does not exceed the returned buffer length
    /// 2. Position updates are done via `commit_direct_write` after writing
    ///
    /// # Example
    /// ```ignore
    /// if let Some((buffer, pos)) = mapped_file.get_direct_write_buffer(msg_len) {
    ///     // Encode message directly into buffer
    ///     encoder.encode_to_buffer(buffer, message);
    ///     
    ///     // Commit the write
    ///     mapped_file.commit_direct_write(msg_len);
    /// }
    /// ```
    fn get_direct_write_buffer(&self, required_space: usize) -> Option<(&mut [u8], usize)> {
        // Default implementation returns None (zero-copy not supported)
        None
    }

    /// **Phase 3 Optimization**: Commits a direct write operation, updating the write position.
    ///
    /// This method should be called after writing data via `get_direct_write_buffer` to update
    /// the internal write position.
    ///
    /// # Arguments
    /// * `bytes_written` - Number of bytes that were written to the direct buffer
    ///
    /// # Returns
    /// `true` if the commit was successful, `false` otherwise
    ///
    /// # Example
    /// ```ignore
    /// if let Some((buffer, pos)) = mapped_file.get_direct_write_buffer(100) {
    ///     buffer[..100].copy_from_slice(&data);
    ///     mapped_file.commit_direct_write(100);
    /// }
    /// ```
    fn commit_direct_write(&self, bytes_written: usize) -> bool {
        // Default implementation does nothing
        false
    }

    /// Writes a segment of bytes to the mapped file.
    ///
    /// This method writes a specified portion of the given byte array to the mapped file, starting
    /// at the specified position and for the specified length.
    ///
    /// # Arguments
    /// * `data` - A reference to the byte array containing the data to be written.
    /// * `start` - The starting position in the mapped file where the data should be written.
    /// * `offset` - The offset within the byte array from where the data should be read.
    /// * `length` - The number of bytes to write from the byte array.
    ///
    /// # Returns
    /// `true` if the write operation was successful, `false` otherwise.
    fn write_bytes_segment(&self, data: &[u8], start: usize, offset: usize, length: usize) -> bool;

    /// Puts a slice of bytes into the mapped file at the specified index.
    ///
    /// This method writes the given byte slice to the mapped file starting at the specified index.
    ///
    /// # Arguments
    /// * `data` - A reference to the byte slice to be written.
    /// * `index` - The starting index in the mapped file where the data should be written.
    ///
    /// # Returns
    /// `true` if the write operation was successful, `false` otherwise.
    fn put_slice(&self, data: &[u8], index: usize) -> bool;

    /// Retrieves the file offset based on the current write position.
    ///
    /// This method calculates and returns the file offset corresponding to the current write
    /// position. It is useful for determining the physical file location that corresponds to a
    /// logical position in the file.
    ///
    /// # Returns
    /// The file offset as a `u64`.
    fn get_file_from_offset(&self) -> u64;

    /// Flushes the mapped file to disk.
    ///
    /// This method flushes the contents of the mapped file to disk based on the specified least
    /// number of pages. It ensures that data written to the mapped file is safely stored on
    /// disk, which is crucial for preventing data loss.
    ///
    /// # Arguments
    /// * `flush_least_pages` - The minimum number of pages to flush. If set to 0, all pages are
    ///   flushed.
    ///
    /// # Returns
    /// The number of pages flushed as an `i32`.
    fn flush(&self, flush_least_pages: i32) -> i32;

    /// Commits the data to the mapped file.
    ///
    /// This method ensures that the changes made to the mapped file are committed to storage,
    /// based on the specified minimum number of pages. It is crucial for ensuring data integrity
    /// and durability.
    ///
    /// # Arguments
    /// * `commit_least_pages` - The minimum number of pages to commit. This allows for flexibility
    ///   in how data is committed, enabling optimizations based on the underlying storage system.
    ///
    /// # Returns
    /// The number of pages actually committed.
    fn commit(&self, commit_least_pages: i32) -> i32;

    /// Retrieves the entire mapped byte buffer.
    ///
    /// This method provides access to the entire byte buffer of the mapped file. It is useful for
    /// operations that need to work with the complete contents of the file.
    ///
    /// # Returns
    /// A `bytes::Bytes` instance containing the byte buffer of the entire mapped file.
    fn get_mapped_byte_buffer(&self) -> &[u8];

    /// Creates a slice of the mapped byte buffer.
    ///
    /// This method is intended for creating a slice of the entire mapped byte buffer, potentially
    /// for performance optimizations or specific byte manipulation operations.
    ///
    /// # Returns
    /// A `bytes::Bytes` instance representing a slice of the mapped byte buffer.
    fn slice_byte_buffer(&self) -> &[u8];

    /// Returns the timestamp when the store was created.
    ///
    /// # Returns
    /// A `i64` representing the timestamp of the store creation.
    fn get_store_timestamp(&self) -> u64;

    /// Returns the timestamp of the last modification to the store.
    ///
    /// # Returns
    /// A `i64` representing the timestamp of the last modification.
    fn get_last_modified_timestamp(&self) -> u64;

    /// Retrieves data from the store starting at the specified position and of the specified size.
    ///
    /// # Arguments
    /// * `pos` - The starting position from where the data should be read.
    /// * `size` - The number of bytes to read from the starting position.
    ///
    /// # Returns
    /// An `Option<bytes::Bytes>` containing the requested data slice if available, or `None` if the
    /// requested slice goes beyond the store boundaries or the store is not available.
    fn get_data(&self, pos: usize, size: usize) -> Option<bytes::Bytes>;

    /// Retrieves a slice of the mapped file.
    ///
    /// This method returns a byte slice starting from the specified position and of the specified
    /// size. It is useful for reading parts of the file without loading the entire file into
    /// memory.
    ///
    /// # Arguments
    /// * `pos` - The starting position from where bytes should be read.
    /// * `size` - The number of bytes to read from the starting position.
    ///
    /// # Returns
    /// An `Option<&[u8]>` containing the requested byte slice if available, or `None` if the
    /// requested slice goes beyond the file boundaries or the file is not available.
    fn get_slice(&self, pos: usize, size: usize) -> Option<&[u8]>;

    /// Destroys the store after a specified interval.
    ///
    /// # Arguments
    /// * `interval_forcibly` - The time interval after which the store should be forcibly
    ///   destroyed.
    ///
    /// # Returns
    /// `true` if the store was successfully destroyed, `false` otherwise.
    fn destroy(&self, interval_forcibly: u64) -> bool;

    /// Initiates a shutdown of the store after a specified interval.
    ///
    /// # Arguments
    /// * `interval_forcibly` - The time interval after which the store should be forcibly shut
    ///   down.
    fn shutdown(&self, interval_forcibly: u64);

    /// Releases any resources held by the store.
    fn release(&self);

    /// Holds the mapped file to prevent it from being swapped out.
    ///
    /// # Returns
    /// `true` if the file is successfully held, preventing it from being swapped out; `false`
    /// otherwise.
    fn hold(&self) -> bool;

    /// Checks if this is the first file created in the queue.
    ///
    /// # Returns
    /// `true` if this is the first file created in the queue; `false` otherwise.
    fn is_first_create_in_queue(&self) -> bool;

    /// Sets the flag indicating whether this is the first file created in the queue.
    ///
    /// # Arguments
    /// * `first_create_in_queue` - A boolean value indicating whether this is the first file
    ///   created in the queue.
    fn set_first_create_in_queue(&mut self, first_create_in_queue: bool);

    /// Retrieves the position up to which the file has been flushed.
    ///
    /// # Returns
    /// The position up to which the file has been flushed as an `i32`.
    fn get_flushed_position(&self) -> i32;

    /// Sets the position up to which the file has been flushed.
    ///
    /// # Arguments
    /// * `flushed_position` - The position up to which the file has been flushed.
    fn set_flushed_position(&self, flushed_position: i32);

    /// Retrieves the position up to which the file has been written.
    ///
    /// # Returns
    /// The position up to which the file has been written as an `i32`.
    fn get_wrote_position(&self) -> i32;

    /// Sets the position up to which the file has been written.
    ///
    /// # Arguments
    /// * `wrote_position` - The position up to which the file has been written.
    fn set_wrote_position(&self, wrote_position: i32);

    /// Retrieves the current read position in the mapped file.
    ///
    /// # Returns
    /// The current read position as an `i32`.
    fn get_read_position(&self) -> i32;

    /// Sets the committed position in the mapped file.
    ///
    /// This method is used to update the position up to which changes have been committed in the
    /// mapped file.
    ///
    /// # Arguments
    /// * `committed_position` - The position to set as the committed position.
    fn set_committed_position(&self, committed_position: i32);

    /// Retrieves the committed position in the mapped file.
    ///
    /// # Returns
    /// The committed position as an `i32`.
    fn get_committed_position(&self) -> i32;

    /// Locks the mapped file into memory.
    ///
    /// This method prevents the mapped file from being paged out to swap space, ensuring it remains
    /// in physical memory.
    fn mlock(&self);

    /// Unlocks the mapped file from memory.
    ///
    /// This method reverses the effect of `mlock`, allowing the mapped file to be paged out to swap
    /// space if necessary.
    fn munlock(&self);

    /// Warms up the mapped file by accessing a specified number of pages.
    ///
    /// This method is used to improve the performance of accessing the mapped file by pre-loading a
    /// specified number of pages into memory.
    ///
    /// # Arguments
    /// * `flush_disk_type` - The strategy used for flushing data to disk.
    /// * `pages` - The number of pages to access for warming up the file.
    fn warm_mapped_file(&self, flush_disk_type: FlushDiskType, pages: usize);

    /// Attempts to swap the current mapped file with a new one.
    ///
    /// # Returns
    /// `true` if the swap was successful, `false` otherwise.
    fn swap_map(&self) -> bool;

    /// Cleans up the swapped map files.
    ///
    /// This method is responsible for cleaning up the swapped map files. It can be forced to clean
    /// up even if certain conditions (like time since last swap) are not met.
    ///
    /// # Arguments
    /// * `force` - A boolean indicating whether the cleanup should be forced regardless of
    ///   conditions.
    fn clean_swaped_map(&self, force: bool);

    /// Retrieves the timestamp of the most recent swap operation.
    ///
    /// # Returns
    /// A `i64` representing the timestamp (in milliseconds since the epoch) when the last swap
    /// operation occurred.
    fn get_recent_swap_map_time(&self) -> i64;

    /// Gets the access count of the mapped byte buffer since the last swap.
    ///
    /// This method returns the number of times the mapped byte buffer has been accessed since the
    /// last swap operation. This can be useful for understanding the usage pattern of the
    /// mapped file.
    ///
    /// # Returns
    /// An `i64` representing the number of accesses to the mapped byte buffer since the last swap.
    fn get_mapped_byte_buffer_access_count_since_last_swap(&self) -> i64;

    /// Retrieves a reference to the underlying file.
    ///
    /// This method provides access to the `File` instance associated with the mapped file. It can
    /// be used for operations that require direct access to the file, such as file metadata
    /// retrieval.
    ///
    /// # Returns
    /// A reference to the `File` instance associated with the mapped file.
    fn get_file(&self) -> &File;

    /// Marks the mapped file for deletion.
    ///
    /// This method renames the mapped file in such a way that it is marked for deletion. The actual
    /// deletion may happen immediately or be deferred until the file is no longer in use.
    fn rename_to_delete(&self);

    /// Moves the current mapped file's context to its parent directory.
    ///
    /// This operation may involve updating internal paths or states to reflect the change in
    /// location. It's primarily used when organizing files or handling hierarchical storage
    /// structures.
    ///
    /// # Returns
    /// An `io::Result<()>` indicating the success or failure of the operation. A successful
    /// operation returns `Ok(())`, while a failure returns an `Err` with more details.
    fn move_to_parent(&self) -> io::Result<()>;

    /// Retrieves the timestamp of the last flush operation.
    ///
    /// This method returns the time at which the last flush operation was completed for the mapped
    /// file. The timestamp is represented as an `u64`, measuring milliseconds since the Unix
    /// epoch.
    ///
    /// # Returns
    /// An `u64` representing the timestamp of the last flush operation.
    fn get_last_flush_time(&self) -> u64;

    /// Checks if a specific portion of the mapped file is loaded into memory.
    ///
    /// This method is used to determine if a specified range of the mapped file has been loaded
    /// into memory, which can be useful for optimizing access patterns or preloading data.
    ///
    /// # Arguments
    /// * `position` - The starting position of the range to check, as an `i64`.
    /// * `size` - The size of the range to check, in bytes, as a `usize`.
    ///
    /// # Returns
    /// `true` if the specified range is loaded into memory; `false` otherwise.
    fn is_loaded(&self, position: i64, size: usize) -> bool;

    fn init(
        &self,
        file_name: &CheetahString,
        file_size: usize,
        transient_store_pool: &TransientStorePool,
    ) -> io::Result<()>;
}

pub trait MappedFileRefactor {
    fn append_message<AMC: AppendMessageCallback>(
        &self,
        message: &MessageExtBrokerInner,
        message_callback: AMC,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;

    fn append_messages<AMC: AppendMessageCallback>(
        &self,
        message: &MessageExtBatch,
        message_callback: AMC,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;

    fn append_message_with_callback(
        &self,
        byte_buffer_msg: &[u8],
        cb: &dyn CompactionAppendMsgCallback,
    ) -> AppendMessageResult;

    fn iterator(&self, pos: usize) -> Box<dyn Iterator<Item = SelectMappedBufferResult>>;
}
