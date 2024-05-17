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

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
};

use bytes::{Bytes, BytesMut};
use memmap2::MmapMut;
use rocketmq_common::common::message::{
    message_batch::MessageExtBatch, message_single::MessageExtBrokerInner,
};
use tracing::error;

use crate::{
    base::{
        append_message_callback::AppendMessageCallback,
        compaction_append_msg_callback::CompactionAppendMsgCallback,
        message_result::AppendMessageResult, message_status_enum::AppendMessageStatus,
        put_message_context::PutMessageContext, select_result::SelectMappedBufferResult,
    },
    config::flush_disk_type::FlushDiskType,
    log_file::mapped_file::MappedFile,
};

pub struct LocalMappedFile {
    //file information
    file_name: String,
    file_size: u64,
    file: File,
    //file data information
    wrote_position: AtomicI32,
    committed_position: AtomicI32,
    flushed_position: AtomicI32,

    //file name, for example: file name is:00000000000000000000, file_from_offset is 0
    file_from_offset: u64,

    mmapped_file: parking_lot::RwLock<MmapMut>,

    first_create_in_queue: bool,
}

impl LocalMappedFile {
    pub fn new(file_name: String, file_size: u64) -> Self {
        let path_buf = PathBuf::from(file_name.clone());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path_buf)
            .unwrap();
        file.set_len(file_size).unwrap();

        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        Self {
            file_name,
            file_size,
            file,
            wrote_position: AtomicI32::new(0),
            committed_position: AtomicI32::new(0),
            flushed_position: AtomicI32::new(0),
            file_from_offset: path_buf
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string()
                .parse::<u64>()
                .unwrap(),
            mmapped_file: parking_lot::RwLock::new(mmap),
            first_create_in_queue: false,
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
    pub fn wrote_position(&self) -> i32 {
        self.wrote_position.load(Ordering::Relaxed)
    }
    pub fn committed_position(&self) -> i32 {
        self.committed_position.load(Ordering::Relaxed)
    }
    pub fn flushed_position(&self) -> i32 {
        self.flushed_position.load(Ordering::Relaxed)
    }

    pub fn file_from_offset(&self) -> u64 {
        self.file_from_offset
    }

    pub fn get_file_from_offset(&self) -> u64 {
        self.file_from_offset
    }

    pub fn set_first_create_in_queue(&mut self, first_create_in_queue: bool) {
        self.first_create_in_queue = first_create_in_queue;
    }

    pub fn get_file_name(&self) -> String {
        PathBuf::from(&self.file_name)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string()
    }

    ///Returns the current max readable position of this mapped file.
    pub fn get_read_position(&self) -> i32 {
        self.committed_position.load(Ordering::Relaxed)
    }

    pub fn set_wrote_position(&self, wrote_position: i32) {
        self.wrote_position.store(wrote_position, Ordering::SeqCst);
    }
    pub fn set_committed_position(&self, committed_position: i32) {
        self.committed_position
            .store(committed_position, Ordering::SeqCst);
    }
    pub fn set_flushed_position(&self, flushed_position: i32) {
        self.flushed_position
            .store(flushed_position, Ordering::SeqCst);
    }
}

impl LocalMappedFile {
    pub fn append_data(&self, data: BytesMut, sync: bool) -> bool {
        let current_pos = self.wrote_position.load(Ordering::SeqCst) as usize;
        if current_pos + data.len() > self.file_size as usize {
            return false;
        }
        let mut write_success = if (&mut self.mmapped_file.write()[current_pos..])
            .write_all(data.as_ref())
            .is_ok()
        {
            self.wrote_position
                .store((current_pos + data.len()) as i32, Ordering::SeqCst);
            true
        } else {
            false
        };

        write_success &= if sync {
            self.mmapped_file.write().flush().is_ok()
        } else {
            self.mmapped_file.write().flush_async().is_ok()
        };

        write_success
    }

    pub fn append_message<AMC: AppendMessageCallback>(
        &self,
        message: MessageExtBrokerInner,
        message_callback: &AMC,
        put_message_context: &mut PutMessageContext,
    ) -> AppendMessageResult {
        let mut message = message;
        let current_pos = self.wrote_position.load(Ordering::Relaxed) as u64;
        if current_pos >= self.file_size {
            error!(
                "MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}",
                current_pos, self.file_size
            );
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..Default::default()
            };
        }
        let append_message_result = message_callback.do_append(
            self.file_from_offset() as i64,
            self,
            (self.file_size - current_pos) as i32,
            &mut message,
            put_message_context,
        );
        self.append_data(message.encoded_buff.take().unwrap(), false);
        append_message_result
    }

    pub fn get_bytes(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        if pos + size > self.file_size as usize {
            return None;
        }
        Some(Bytes::copy_from_slice(
            &self.mmapped_file.read()[pos..pos + size],
        ))
    }

    pub fn is_full(&self) -> bool {
        false
    }
}

impl MappedFile for LocalMappedFile {
    fn get_file_name(&self) -> String {
        todo!()
    }

    fn rename_to(&mut self, file_name: &str) -> bool {
        todo!()
    }

    fn get_file_size(&self) -> u64 {
        todo!()
    }

    fn is_full(&self) -> bool {
        todo!()
    }

    fn is_available(&self) -> bool {
        todo!()
    }

    fn append_message<AMC: AppendMessageCallback>(
        &self,
        message: MessageExtBrokerInner,
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        todo!()
    }

    fn append_messages<AMC: AppendMessageCallback>(
        &mut self,
        message: &MessageExtBatch,
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        todo!()
    }

    fn append_message_compaction(
        &mut self,
        byte_buffer_msg: &mut Bytes,
        cb: &dyn CompactionAppendMsgCallback,
    ) -> AppendMessageResult {
        todo!()
    }

    fn get_bytes(&self, pos: usize, size: usize) -> Option<Bytes> {
        todo!()
    }

    fn append_message_offset_length(&self, data: &Bytes, offset: usize, length: usize) -> bool {
        todo!()
    }

    fn get_file_from_offset(&self) -> u64 {
        todo!()
    }

    fn flush(&mut self, flush_least_pages: i32) -> i32 {
        todo!()
    }

    fn commit(&mut self, commit_least_pages: usize) -> usize {
        todo!()
    }

    fn select_mapped_buffer_size(&self, pos: usize, size: usize) -> SelectMappedBufferResult {
        todo!()
    }

    fn select_mapped_buffer(self: Arc<Self>, pos: i32) -> Option<SelectMappedBufferResult> {
        todo!()
    }

    fn get_mapped_byte_buffer(&self) -> Bytes {
        todo!()
    }

    fn slice_byte_buffer(&self) -> Bytes {
        todo!()
    }

    fn get_store_timestamp(&self) -> i64 {
        todo!()
    }

    fn get_last_modified_timestamp(&self) -> i64 {
        todo!()
    }

    fn get_data(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        todo!()
    }

    fn destroy(&self, interval_forcibly: i64) -> bool {
        todo!()
    }

    fn shutdown(&self, interval_forcibly: i64) {
        todo!()
    }

    fn release(&self) {
        todo!()
    }

    fn hold(&self) -> bool {
        todo!()
    }

    fn is_first_create_in_queue(&self) -> bool {
        todo!()
    }

    fn set_first_create_in_queue(&mut self, first_create_in_queue: bool) {
        todo!()
    }

    fn get_flushed_position(&self) -> i32 {
        todo!()
    }

    fn set_flushed_position(&self, flushed_position: i32) {
        todo!()
    }

    fn get_wrote_position(&self) -> i32 {
        todo!()
    }

    fn set_wrote_position(&self, wrote_position: i32) {
        todo!()
    }

    fn get_read_position(&self) -> i32 {
        todo!()
    }

    fn set_committed_position(&self, committed_position: i32) {
        todo!()
    }

    fn get_committed_position(&self) -> i32 {
        todo!()
    }

    fn mlock(&self) {
        todo!()
    }

    fn munlock(&self) {
        todo!()
    }

    fn warm_mapped_file(&self, flush_disk_type: FlushDiskType, pages: usize) {
        todo!()
    }

    fn swap_map(&self) -> bool {
        todo!()
    }

    fn clean_swaped_map(&self, force: bool) {
        todo!()
    }

    fn get_recent_swap_map_time(&self) -> i64 {
        todo!()
    }

    fn get_mapped_byte_buffer_access_count_since_last_swap(&self) -> i64 {
        todo!()
    }

    fn get_file(&self) -> &File {
        todo!()
    }

    fn rename_to_delete(&self) {
        todo!()
    }

    fn move_to_parent(&self) -> std::io::Result<()> {
        todo!()
    }

    fn get_last_flush_time(&self) -> i64 {
        todo!()
    }

    fn is_loaded(&self, position: i64, size: usize) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    // use super::*;

    #[test]
    pub fn test_local_mapped_file() {
        // let mut file = LocalMappedFile::new(
        //     "C:\\Users\\ljbmx\\Desktop\\EventMesh\\0000".to_string(),
        //     1024,
        // );
        // let data = Bytes::from("ttt");
        // assert!(file.append_data(data, true));
    }
}
