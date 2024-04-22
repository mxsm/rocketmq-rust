/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{fs::File, io};

use rocketmq_common::common::message::{
    message_batch::MessageExtBatch, message_single::MessageExtBrokerInner,
};

use crate::{
    base::{
        append_message_callback::AppendMessageCallback,
        compaction_append_msg_callback::CompactionAppendMsgCallback,
        message_result::AppendMessageResult, put_message_context::PutMessageContext,
        select_result::SelectMappedBufferResult, transient_store_pool::TransientStorePool,
    },
    config::flush_disk_type::FlushDiskType,
};

pub(crate) mod default_impl;
pub mod default_impl_refactor;

pub trait MappedFileBak {
    /// Returns the file name of the `MappedFile`.
    fn get_file_name(&self) -> &str;

    /// Change the file name of the `MappedFile`.
    fn rename_to(&mut self, file_name: &str) -> bool;

    /// Returns the file size of the `MappedFile`.
    fn get_file_size(&self) -> usize;

    /// Returns the `FileChannel` behind the `MappedFile`.
    fn get_file_channel(&self) -> io::Result<&File>;

    /// Returns true if this `MappedFile` is full and no new messages can be added.
    fn is_full(&self) -> bool;

    /// Returns true if this `MappedFile` is available.
    /// The mapped file will be not available if it's shutdown or destroyed.
    fn is_available(&self) -> bool;

    /// Appends a message object to the current `MappedFile` with a specific callback.
    fn append_message(
        &mut self,
        message: &MessageExtBrokerInner,
        message_callback: &dyn AppendMessageCallback,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;

    /// Appends a batch message object to the current `MappedFile` with a specific callback.
    fn append_messages(
        &mut self,
        message: &MessageExtBatch,
        message_callback: &dyn AppendMessageCallback,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult;

    fn append_message_compaction(
        &mut self,
        byte_buffer_msg: &mut bytes::Bytes,
        cb: &dyn CompactionAppendMsgCallback,
    ) -> AppendMessageResult;

    /// Appends a raw message data represents by a byte array to the current `MappedFile`.
    fn append_message_byte_array(&mut self, data: &[u8]) -> bool;

    /// Appends a raw message data represents by a byte buffer to the current `MappedFile`.
    fn append_message_bytes(&mut self, data: &mut bytes::Bytes) -> bool;

    /// Appends a raw message data represents by a byte array to the current `MappedFile`,
    /// starting at the given offset in the array.
    fn append_message_offset_length(&mut self, data: &[u8], offset: usize, length: usize) -> bool;

    /// Returns the global offset of the current `MappedFile`, it's a long value of the file name.
    fn get_file_from_offset(&self) -> i64;

    /// Flushes the data in cache to disk immediately.
    fn flush(&mut self, flush_least_pages: usize) -> usize;

    /// Flushes the data in the secondary cache to page cache or disk immediately.
    fn commit(&mut self, commit_least_pages: usize) -> usize;

    /// Selects a slice of the mapped byte buffer's sub-region behind the mapped file, starting at
    /// the given position.
    fn select_mapped_buffer_size(&self, pos: usize, size: usize) -> SelectMappedBufferResult;

    /// Selects a slice of the mapped byte buffer's sub-region behind the mapped file, starting at
    /// the given position.
    fn select_mapped_buffer(&self, pos: usize) -> SelectMappedBufferResult;

    /// Returns the mapped byte buffer behind the mapped file.
    fn get_mapped_byte_buffer(&self) -> bytes::Bytes;

    /// Returns a slice of the mapped byte buffer behind the mapped file.
    fn slice_byte_buffer(&self) -> bytes::Bytes;

    /// Returns the store timestamp of the last message.
    fn get_store_timestamp(&self) -> i64;

    /// Returns the last modified timestamp of the file.
    fn get_last_modified_timestamp(&self) -> i64;

    /// Get data from a certain pos offset with size byte
    fn get_data(&self, pos: usize, size: usize, byte_buffer: &mut bytes::Bytes) -> bool;

    /// Destroys the file and delete it from the file system.
    fn destroy(&self, interval_forcibly: i64) -> bool;

    /// Shutdowns the file and mark it unavailable.
    fn shutdown(&self, interval_forcibly: i64);

    /// Decreases the reference count by `1` and clean up the mapped file if the reference count
    /// reaches at `0`.
    fn release(&self);

    /// Increases the reference count by `1`.
    fn hold(&self) -> bool;

    /// Returns true if the current file is first mapped file of some consume queue.
    fn is_first_create_in_queue(&self) -> bool;

    /// Sets the flag whether the current file is first mapped file of some consume queue.
    fn set_first_create_in_queue(&mut self, first_create_in_queue: bool);

    /// Returns the flushed position of this mapped file.
    fn get_flushed_position(&self) -> usize;

    /// Sets the flushed position of this mapped file.
    fn set_flushed_position(&mut self, flushed_position: usize);

    /// Returns the wrote position of this mapped file.
    fn get_wrote_position(&self) -> usize;

    /// Sets the wrote position of this mapped file.
    fn set_wrote_position(&mut self, wrote_position: usize);

    /// Returns the current max readable position of this mapped file.
    fn get_read_position(&self) -> usize;

    /// Sets the committed position of this mapped file.
    fn set_committed_position(&mut self, committed_position: usize);

    /// Lock the mapped byte buffer
    fn mlock(&self);

    /// Unlock the mapped byte buffer
    fn munlock(&self);

    /// Warm up the mapped byte buffer
    fn warm_mapped_file(&self, flush_disk_type: FlushDiskType, pages: usize);

    /// Swap map
    fn swap_map(&self) -> bool;

    /// Clean pageTable
    fn clean_swaped_map(&self, force: bool);

    /// Get recent swap map time
    fn get_recent_swap_map_time(&self) -> i64;

    /// Get recent MappedByteBuffer access count since last swap
    fn get_mapped_byte_buffer_access_count_since_last_swap(&self) -> i64;

    /// Get the underlying file
    fn get_file(&self) -> &File;

    /// Rename file to add ".delete" suffix
    fn rename_to_delete(&self);

    /// Move the file to the parent directory
    fn move_to_parent(&self) -> io::Result<()>;

    /// Get the last flush time
    fn get_last_flush_time(&self) -> i64;

    /// Init mapped file
    fn init(
        &mut self,
        file_name: &str,
        file_size: usize,
        transient_store_pool: &TransientStorePool,
    ) -> io::Result<()>;

    fn iterator(&self, pos: usize) -> Box<dyn Iterator<Item = SelectMappedBufferResult>>;

    /// Check mapped file is loaded to memory with given position and size
    fn is_loaded(&self, position: i64, size: usize) -> bool;
}
