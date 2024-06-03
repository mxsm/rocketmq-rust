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
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering},
        Arc,
    },
};

use bytes::{Bytes, BytesMut};
use memmap2::MmapMut;
use rocketmq_common::{
    common::message::{message_batch::MessageExtBatch, message_single::MessageExtBrokerInner},
    UtilAll::ensure_dir_ok,
};
use tracing::{debug, error, info, warn};

use crate::{
    base::{
        append_message_callback::AppendMessageCallback,
        compaction_append_msg_callback::CompactionAppendMsgCallback,
        message_result::AppendMessageResult, message_status_enum::AppendMessageStatus,
        put_message_context::PutMessageContext, select_result::SelectMappedBufferResult,
        transient_store_pool::TransientStorePool,
    },
    config::flush_disk_type::FlushDiskType,
    log_file::mapped_file::MappedFile,
};

pub const OS_PAGE_SIZE: u64 = 1024 * 4;

static TOTAL_MAPPED_VIRTUAL_MEMORY: AtomicI64 = AtomicI64::new(0);
static TOTAL_MAPPED_FILES: AtomicI32 = AtomicI32::new(0);

pub struct DefaultMappedFile {
    reference_resource: ReferenceResource,
    file: File,
    //  file_channel: FileChannel,
    mmapped_file: parking_lot::Mutex<MmapMut>,
    transient_store_pool: Option<TransientStorePool>,
    file_name: String,
    file_from_offset: u64,
    mapped_byte_buffer: Option<bytes::Bytes>,
    wrote_position: AtomicI32,
    committed_position: AtomicI32,
    flushed_position: AtomicI32,
    file_size: u64,
    store_timestamp: AtomicI64,
    first_create_in_queue: bool,
    last_flush_time: u64,
    //  mapped_byte_buffer_wait_to_clean: Option<MappedByteBuffer>,
    swap_map_time: u64,
    mapped_byte_buffer_access_count_since_last_swap: AtomicI64,
    start_timestamp: u64,
    stop_timestamp: u64,
}

impl PartialEq for DefaultMappedFile {
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self as *const Self, other as *const Self)
    }
}

impl Default for DefaultMappedFile {
    fn default() -> Self {
        Self::new(String::new(), 0)
    }
}

impl DefaultMappedFile {
    pub fn new(file_name: String, file_size: u64) -> Self {
        let file_from_offset = Self::get_file_from_offset(&file_name);
        let path_buf = PathBuf::from(file_name.clone());
        ensure_dir_ok(path_buf.parent().unwrap().to_str().unwrap());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path_buf)
            .unwrap();
        file.set_len(file_size).unwrap();

        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        Self {
            reference_resource: ReferenceResource {
                ref_count: AtomicI64::new(1),
                available: AtomicBool::new(true),
                cleanup_over: AtomicBool::new(false),
                first_shutdown_timestamp: AtomicI64::new(0),
            },
            file,
            mmapped_file: parking_lot::Mutex::new(mmap),
            file_name,
            file_from_offset,
            mapped_byte_buffer: None,
            wrote_position: Default::default(),
            committed_position: Default::default(),
            flushed_position: Default::default(),
            file_size,
            store_timestamp: Default::default(),
            first_create_in_queue: false,
            last_flush_time: 0,
            swap_map_time: 0,
            mapped_byte_buffer_access_count_since_last_swap: Default::default(),
            start_timestamp: 0,
            transient_store_pool: None,
            stop_timestamp: 0,
        }
    }

    fn get_file_from_offset(file_name: &String) -> u64 {
        let file_from_offset = PathBuf::from(file_name.to_owned())
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        file_from_offset
    }

    fn build_file(file_name: &String, file_size: u64) -> File {
        let path = PathBuf::from(file_name.clone());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.set_len(file_size)
            .unwrap_or_else(|_| panic!("failed to set file size: {}", file_name));
        file
    }

    pub fn new_with_transient_store_pool(
        file_name: String,
        file_size: u64,
        transient_store_pool: TransientStorePool,
    ) -> Self {
        let file_from_offset = Self::get_file_from_offset(&file_name);
        let path_buf = PathBuf::from(file_name.clone());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path_buf)
            .unwrap();
        file.set_len(file_size).unwrap();

        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        Self {
            reference_resource: ReferenceResource {
                ref_count: AtomicI64::new(1),
                available: AtomicBool::new(true),
                cleanup_over: AtomicBool::new(false),
                first_shutdown_timestamp: AtomicI64::new(0),
            },
            file,
            file_name,
            file_from_offset,
            mapped_byte_buffer: None,
            wrote_position: Default::default(),
            committed_position: Default::default(),
            flushed_position: Default::default(),
            file_size,
            store_timestamp: Default::default(),
            first_create_in_queue: false,
            last_flush_time: 0,
            swap_map_time: 0,
            mapped_byte_buffer_access_count_since_last_swap: Default::default(),
            start_timestamp: 0,
            transient_store_pool: Some(transient_store_pool),
            stop_timestamp: 0,
            mmapped_file: parking_lot::Mutex::new(mmap),
        }
    }
}

#[allow(unused_variables)]
impl MappedFile for DefaultMappedFile {
    fn get_file_name(&self) -> String {
        self.file_name.clone()
    }

    fn rename_to(&mut self, file_name: &str) -> bool {
        todo!()
    }

    fn get_file_size(&self) -> u64 {
        self.file_size
    }

    fn is_full(&self) -> bool {
        self.file_size == self.wrote_position.load(Ordering::Relaxed) as u64
    }

    fn is_available(&self) -> bool {
        self.reference_resource.available.load(Ordering::Relaxed)
    }

    fn append_message<AMC: AppendMessageCallback>(
        &self,
        message: &mut MessageExtBrokerInner,
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        let current_pos = self.wrote_position.load(Ordering::Acquire) as u64;
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
        let result = message_callback.do_append(
            self.file_from_offset as i64,
            self,
            (self.file_size - current_pos) as i32,
            message,
            put_message_context,
        );
        self.store_timestamp
            .store(message.store_timestamp(), Ordering::Release);
        result
    }

    fn append_messages<AMC: AppendMessageCallback>(
        &mut self,
        message: &MessageExtBatch,
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        unimplemented!()
    }

    fn append_message_compaction(
        &mut self,
        byte_buffer_msg: &mut Bytes,
        cb: &dyn CompactionAppendMsgCallback,
    ) -> AppendMessageResult {
        todo!()
    }

    fn get_bytes(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        if pos + size > self.file_size as usize {
            return None;
        }
        Some(Bytes::copy_from_slice(
            &self.mmapped_file.lock()[pos..pos + size],
        ))
    }

    fn append_message_offset_length(&self, data: &Bytes, offset: usize, length: usize) -> bool {
        let current_pos = self.wrote_position.load(Ordering::Relaxed) as usize;
        if current_pos + length <= self.file_size as usize {
            match (&mut self.mmapped_file.lock()[current_pos..]).write_all(data.as_ref()) {
                Ok(_) => {
                    self.wrote_position
                        .fetch_add(length as i32, Ordering::SeqCst);
                    true
                }
                Err(_) => {
                    error!("append_message_offset_length write_all error");
                    false
                }
            }
        } else {
            false
        }
    }

    fn get_file_from_offset(&self) -> u64 {
        self.file_from_offset
    }

    fn flush(&self, flush_least_pages: i32) -> i32 {
        if self.is_able_to_flush(flush_least_pages) {
            if self.reference_resource.hold() {
                let value = self.get_read_position();
                if self.transient_store_pool.is_none() {
                    self.mmapped_file
                        .lock()
                        .flush()
                        .expect("Error occurred when force data to disk.");
                } else {
                    unimplemented!()
                }
                self.flushed_position.store(value, Ordering::SeqCst);
            } else {
                warn!(
                    "in flush, hold failed, flush offset = {}",
                    self.flushed_position.load(Ordering::Relaxed)
                );
                self.flushed_position
                    .store(self.get_read_position(), Ordering::SeqCst);
            }
        }
        self.get_flushed_position()
    }

    fn commit(&self, commit_least_pages: i32) -> i32 {
        0
    }

    fn select_mapped_buffer_size(
        self: Arc<Self>,
        pos: i32,
        size: i32,
    ) -> Option<SelectMappedBufferResult> {
        let read_position = self.get_read_position();
        if pos + size <= read_position {
            if self.hold() {
                self.mapped_byte_buffer_access_count_since_last_swap
                    .fetch_add(1, Ordering::SeqCst);
                Some(SelectMappedBufferResult {
                    start_offset: self.file_from_offset + pos as u64,
                    size,
                    mapped_file: Some(self),
                    is_in_cache: false,
                })
            } else {
                None
            }
        } else {
            warn!(
                "selectMappedBuffer request pos invalid, request pos: {}, size:{}, \
                 fileFromOffset: {}",
                pos, size, self.file_from_offset
            );
            None
        }
    }

    fn select_mapped_buffer(self: Arc<Self>, pos: i32) -> Option<SelectMappedBufferResult> {
        let read_position = self.get_read_position();
        if pos < read_position && read_position > 0 && self.hold() {
            Some(SelectMappedBufferResult {
                start_offset: self.get_file_from_offset() + pos as u64,
                size: read_position - pos,
                mapped_file: Some(self),
                is_in_cache: false,
            })
        } else {
            None
        }
    }

    fn get_mapped_byte_buffer(&self) -> Bytes {
        todo!()
    }

    fn slice_byte_buffer(&self) -> Bytes {
        todo!()
    }

    fn get_store_timestamp(&self) -> i64 {
        self.store_timestamp.load(Ordering::Relaxed)
    }

    fn get_last_modified_timestamp(&self) -> i64 {
        todo!()
    }

    fn get_data(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        let read_position = self.get_read_position();
        let read_end_position = pos + size;
        if read_end_position <= read_position as usize {
            if self.hold() {
                let buffer = BytesMut::from(&self.mmapped_file.lock()[pos..read_end_position]);
                Some(buffer.freeze())
            } else {
                debug!(
                    "matched, but hold failed, request pos: {}, fileFromOffset: {}",
                    pos, self.file_from_offset
                );
                None
            }
        } else {
            warn!(
                "selectMappedBuffer request pos invalid, request pos: {}, size:{}, \
                 fileFromOffset: {}",
                pos, size, self.file_from_offset
            );
            None
        }
    }

    fn destroy(&self, interval_forcibly: i64) -> bool {
        true
    }

    fn shutdown(&self, interval_forcibly: i64) {
        if self.reference_resource.available.load(Ordering::Relaxed) {
            self.reference_resource
                .available
                .store(false, Ordering::Relaxed);
            self.reference_resource.first_shutdown_timestamp.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
                Ordering::Relaxed,
            );
            self.release();
        } else if self.reference_resource.get_ref_count() > 0
            && (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
                - self
                    .reference_resource
                    .first_shutdown_timestamp
                    .load(Ordering::Relaxed))
                >= interval_forcibly
        {
            self.reference_resource.ref_count.store(
                -1000 - self.reference_resource.get_ref_count(),
                Ordering::Relaxed,
            );
            self.release();
        }
    }

    fn release(&self) {
        let value = self
            .reference_resource
            .ref_count
            .fetch_sub(1, Ordering::SeqCst)
            - 1;
        if value > 0 {
            return;
        }
        self.reference_resource
            .cleanup_over
            .store(self.cleanup(value), Ordering::SeqCst);
    }

    fn hold(&self) -> bool {
        self.reference_resource.hold()
    }

    fn is_first_create_in_queue(&self) -> bool {
        self.first_create_in_queue
    }

    fn set_first_create_in_queue(&mut self, first_create_in_queue: bool) {
        self.first_create_in_queue = first_create_in_queue
    }

    fn get_flushed_position(&self) -> i32 {
        self.flushed_position.load(Ordering::Relaxed)
    }

    fn set_flushed_position(&self, flushed_position: i32) {
        self.flushed_position
            .store(flushed_position, Ordering::SeqCst)
    }

    fn get_wrote_position(&self) -> i32 {
        self.wrote_position
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_wrote_position(&self, wrote_position: i32) {
        self.wrote_position.store(wrote_position, Ordering::SeqCst)
    }

    fn get_read_position(&self) -> i32 {
        match self.transient_store_pool {
            None => self.wrote_position.load(Ordering::Acquire),
            Some(_) => {
                //need to optimize
                self.wrote_position.load(Ordering::Acquire)
            }
        }
    }

    fn set_committed_position(&self, committed_position: i32) {
        self.committed_position
            .store(committed_position, Ordering::SeqCst)
    }

    fn get_committed_position(&self) -> i32 {
        self.committed_position.load(Ordering::Relaxed)
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

    /*    fn init(
        &mut self,
        file_name: &str,
        file_size: usize,
        transient_store_pool: &TransientStorePool,
    ) -> std::io::Result<()> {
        todo!()
    }*/

    fn is_loaded(&self, position: i64, size: usize) -> bool {
        todo!()
    }
}

#[allow(unused_variables)]
impl DefaultMappedFile {
    /*    fn append_message_inner(
        &mut self,
        message: &mut MessageExtBrokerInner,
        append_message_callback: &mut dyn AppendMessageCallback,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        //write pointer position
        let current_write_pos = self
            .wrote_position
            .load(std::sync::atomic::Ordering::Relaxed);
        if current_write_pos >= self.file_size as i32 {
            error!(
                "MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}",
                current_write_pos, self.file_size
            );
            return AppendMessageResult {
                status: AppendMessageStatus::UnknownError,
                ..AppendMessageResult::default()
            };
        }
        //do append to the Mapped file(Default is local file)
        /*        append_message_callback.do_append(
            self.file_from_offset, // file start logic address offset
            &mut ByteBuffer::new(&mut self.mmapped_file, current_write_pos as i64),
            (self.file_size - current_write_pos as u64) as i32,
            message,
            put_message_context,
        )*/
        AppendMessageResult::default()
    }

    fn append_messages_inner(
        &mut self,
        message: &MessageExtBatch,
        message_callback: &dyn AppendMessageCallback,
        put_message_context: &PutMessageContext,
    ) -> AppendMessageResult {
        unimplemented!()
    }*/

    fn is_able_to_flush(&self, flush_least_pages: i32) -> bool {
        if self.is_full() {
            return true;
        }
        let flush = self.flushed_position.load(Ordering::Relaxed);
        let write = self.get_read_position();
        if flush_least_pages > 0 {
            return (write - flush) / OS_PAGE_SIZE as i32 >= flush_least_pages;
        }
        write > flush
    }

    fn cleanup(&self, current_ref: i64) -> bool {
        if self.is_available() {
            error!(
                "this file[REF:{}] {} have not shutdown, stop unmapping.",
                self.file_name, current_ref
            );
            return false;
        }

        if self.reference_resource.is_cleanup_over() {
            error!(
                "this file[REF:{}]  {} have cleanup, do not do it again.",
                self.file_name, current_ref
            );
            return true;
        }
        TOTAL_MAPPED_VIRTUAL_MEMORY.fetch_sub(self.file_size as i64, Ordering::Relaxed);
        TOTAL_MAPPED_FILES.fetch_sub(1, Ordering::Relaxed);
        info!("unmap file[REF:{}] {} OK", current_ref, self.file_name);
        true
    }
}

pub struct ReferenceResource {
    ref_count: AtomicI64,
    available: AtomicBool,
    cleanup_over: AtomicBool,
    first_shutdown_timestamp: AtomicI64,
}

impl ReferenceResource {
    pub fn new() -> Self {
        Self {
            ref_count: AtomicI64::new(1),
            available: AtomicBool::new(true),
            cleanup_over: AtomicBool::new(false),
            first_shutdown_timestamp: AtomicI64::new(0),
        }
    }

    pub fn hold(&self) -> bool {
        if self.is_available() {
            if self.ref_count.fetch_add(1, Ordering::Relaxed) + 1 > 0 {
                return true;
            } else {
                self.ref_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        false
    }

    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }

    pub fn get_ref_count(&self) -> i64 {
        self.ref_count.load(Ordering::Relaxed)
    }

    pub fn is_cleanup_over(&self) -> bool {
        self.ref_count.load(Ordering::Relaxed) <= 0 && self.cleanup_over.load(Ordering::Relaxed)
    }
}
