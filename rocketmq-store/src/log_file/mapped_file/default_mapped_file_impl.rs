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

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use memmap2::MmapMut;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::UtilAll::ensure_dir_ok;
use rocketmq_rust::SyncUnsafeCellWrapper;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::append_message_callback::AppendMessageCallback;
use crate::base::compaction_append_msg_callback::CompactionAppendMsgCallback;
use crate::base::message_result::AppendMessageResult;
use crate::base::message_status_enum::AppendMessageStatus;
use crate::base::put_message_context::PutMessageContext;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::transient_store_pool::TransientStorePool;
use crate::config::flush_disk_type::FlushDiskType;
use crate::log_file::mapped_file::reference_resource::ReferenceResource;
use crate::log_file::mapped_file::reference_resource_counter::ReferenceResourceCounter;
use crate::log_file::mapped_file::MappedFile;

pub const OS_PAGE_SIZE: u64 = 1024 * 4;

static TOTAL_MAPPED_VIRTUAL_MEMORY: AtomicI64 = AtomicI64::new(0);
static TOTAL_MAPPED_FILES: AtomicI32 = AtomicI32::new(0);

pub struct DefaultMappedFile {
    reference_resource: ReferenceResourceCounter,
    file: File,
    mmapped_file: SyncUnsafeCellWrapper<MmapMut>,
    mapped_bytes: Bytes,
    transient_store_pool: Option<TransientStorePool>,
    file_name: CheetahString,
    file_from_offset: u64,
    mapped_byte_buffer: Option<bytes::Bytes>,
    wrote_position: AtomicI32,
    committed_position: AtomicI32,
    flushed_position: AtomicI32,
    file_size: u64,
    store_timestamp: AtomicU64,
    first_create_in_queue: bool,
    last_flush_time: AtomicU64,
    swap_map_time: u64,
    mapped_byte_buffer_access_count_since_last_swap: AtomicI64,
    start_timestamp: u64,
    stop_timestamp: u64,
}

impl AsRef<DefaultMappedFile> for DefaultMappedFile {
    #[inline]
    fn as_ref(&self) -> &DefaultMappedFile {
        self
    }
}

impl AsMut<DefaultMappedFile> for DefaultMappedFile {
    #[inline]
    fn as_mut(&mut self) -> &mut DefaultMappedFile {
        self
    }
}

impl PartialEq for DefaultMappedFile {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        ptr::eq(self as *const Self, other as *const Self)
    }
}

impl Default for DefaultMappedFile {
    #[inline]
    fn default() -> Self {
        Self::new(CheetahString::new(), 0)
    }
}

impl DefaultMappedFile {
    #[inline]
    pub fn new(file_name: CheetahString, file_size: u64) -> Self {
        let path_buf = PathBuf::from(file_name.as_str());
        if path_buf.parent().is_none() {
            panic!("file path is invalid: {}", file_name);
        }
        let dir = path_buf.parent().unwrap().to_str();
        if dir.is_none() {
            panic!("file path is invalid: {}", file_name);
        }
        ensure_dir_ok(dir.unwrap());
        let file_from_offset = Self::parse_file_from_offset(&path_buf);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path_buf)
            .expect("Create file failed");
        file.set_len(file_size).unwrap();

        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let mapped_slice: &'static [u8] =
            unsafe { std::mem::transmute::<&[u8], &'static [u8]>(mmap.as_ref()) };
        let mapped_bytes = Bytes::from_static(mapped_slice);

        Self {
            reference_resource: ReferenceResourceCounter::new(),
            file,
            mmapped_file: SyncUnsafeCellWrapper::new(mmap),
            file_name,
            file_from_offset,
            mapped_byte_buffer: None,
            wrote_position: Default::default(),
            committed_position: Default::default(),
            flushed_position: Default::default(),
            file_size,
            store_timestamp: Default::default(),
            first_create_in_queue: false,
            last_flush_time: AtomicU64::new(0),
            swap_map_time: 0,
            mapped_byte_buffer_access_count_since_last_swap: Default::default(),
            start_timestamp: 0,
            transient_store_pool: None,
            stop_timestamp: 0,
            mapped_bytes,
        }
    }

    /// Extracts the file offset from the given file name.
    ///
    /// This function takes a `CheetahString` representing the file name,
    /// parses it to extract the offset, and returns it as a `u64`.
    ///
    /// # Arguments
    ///
    /// * `file_name` - A `CheetahString` representing the name of the file.
    ///
    /// # Returns
    ///
    /// A `u64` representing the file offset extracted from the file name.
    ///
    /// # Panics
    ///
    /// This function will panic if the file name cannot be parsed to a valid `u64` offset.
    #[inline]
    pub fn parse_file_from_offset(file_name: &Path) -> u64 {
        file_name
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .expect("File name parse to offset is invalid")
    }

    /// Creates and initializes a new file with the specified name and size.
    ///
    /// This function takes a `CheetahString` representing the file name and a `u64`
    /// representing the file size. It creates a new file at the specified path,
    /// truncates it to the specified size, and returns the file handle.
    ///
    /// # Arguments
    ///
    /// * `file_name` - A `CheetahString` representing the name of the file.
    /// * `file_size` - A `u64` representing the size of the file to be created.
    ///
    /// # Returns
    ///
    /// A `File` handle to the newly created file.
    ///
    /// # Panics
    ///
    /// This function will panic if the file cannot be opened or if the file size
    /// cannot be set.
    fn build_file(file_name: &CheetahString, file_size: u64) -> File {
        let path = PathBuf::from(file_name.as_str());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("Open file failed");
        file.set_len(file_size)
            .unwrap_or_else(|_| panic!("failed to set file size: {}", file_name));
        file
    }

    pub fn new_with_transient_store_pool(
        file_name: CheetahString,
        file_size: u64,
        transient_store_pool: TransientStorePool,
    ) -> Self {
        let path_buf = PathBuf::from(file_name.as_str());
        let file_from_offset = Self::parse_file_from_offset(&path_buf);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path_buf)
            .expect("Open file failed");
        file.set_len(file_size).expect("Set file size failed");

        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let mapped_slice: &'static [u8] =
            unsafe { std::mem::transmute::<&[u8], &'static [u8]>(mmap.as_ref()) };
        let mapped_bytes = Bytes::from_static(mapped_slice);
        Self {
            reference_resource: ReferenceResourceCounter::new(),
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
            last_flush_time: AtomicU64::new(0),
            swap_map_time: 0,
            mapped_byte_buffer_access_count_since_last_swap: Default::default(),
            start_timestamp: 0,
            transient_store_pool: Some(transient_store_pool),
            stop_timestamp: 0,
            mmapped_file: SyncUnsafeCellWrapper::new(mmap),
            mapped_bytes,
        }
    }
}

#[allow(unused_variables)]
impl MappedFile for DefaultMappedFile {
    #[inline]
    fn get_file_name(&self) -> &CheetahString {
        &self.file_name
    }

    fn rename_to(&mut self, file_name: &str) -> bool {
        let new_file = Path::new(file_name);
        match std::fs::rename(self.file_name.as_str(), new_file) {
            Ok(_) => {
                self.file_name = CheetahString::from(file_name);
                match fs::File::open(new_file) {
                    Ok(new_file) => {
                        self.file = new_file;
                    }
                    Err(_) => return false,
                }
                true
            }
            Err(_) => false,
        }
    }

    #[inline]
    fn get_file_size(&self) -> u64 {
        self.file_size
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.file_size == self.wrote_position.load(Ordering::Acquire) as u64
    }

    #[inline]
    fn is_available(&self) -> bool {
        self.reference_resource.is_available()
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
            self.file_from_offset as i64, // file name parsed as offset
            self,
            (self.file_size - current_pos) as i32, // remaining space
            message,
            put_message_context,
        );
        self.wrote_position
            .fetch_add(result.wrote_bytes, Ordering::AcqRel);
        self.store_timestamp
            .store(result.store_timestamp as u64, Ordering::Release);
        result
    }

    fn append_messages<AMC: AppendMessageCallback>(
        &self,
        message: &mut MessageExtBatch,
        message_callback: &AMC,
        put_message_context: &mut PutMessageContext,
        enabled_append_prop_crc: bool,
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
        let result = message_callback.do_append_batch(
            self.file_from_offset as i64,
            self,
            (self.file_size - current_pos) as i32,
            message,
            put_message_context,
            enabled_append_prop_crc,
        );
        self.wrote_position
            .fetch_add(result.wrote_bytes, Ordering::AcqRel);
        self.store_timestamp
            .store(result.store_timestamp as u64, Ordering::Release);
        result
    }

    #[inline]
    fn append_message_compaction(
        &mut self,
        byte_buffer_msg: &mut Bytes,
        cb: &dyn CompactionAppendMsgCallback,
    ) -> AppendMessageResult {
        unimplemented!("append_message_compaction not implemented")
    }

    #[inline]
    fn get_bytes(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        // not check can read position, so maybe read invalid data in the file
        if pos + size > self.file_size as usize {
            return None;
        }
        Some(Bytes::copy_from_slice(
            &self.get_mapped_file()[pos..pos + size],
        ))
    }

    #[inline]
    fn get_bytes_readable_checked(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        //check can read position
        let max_readable_position = self.get_read_position() as usize;
        let end_position = pos + size;
        if max_readable_position < end_position || end_position > self.file_size as usize {
            return None;
        }
        Some(Bytes::copy_from_slice(
            &self.get_mapped_file()[pos..end_position],
        ))
    }

    fn append_message_offset_length(&self, data: &[u8], offset: usize, length: usize) -> bool {
        let current_pos = self.wrote_position.load(Ordering::Acquire) as usize;
        if current_pos + length <= self.file_size as usize {
            let mut mapped_file =
                &mut self.get_mapped_file_mut()[current_pos..current_pos + length];
            if let Some(data_slice) = data.get(offset..offset + length) {
                if mapped_file.write_all(data_slice).is_ok() {
                    self.wrote_position
                        .fetch_add(length as i32, Ordering::AcqRel);
                    return true;
                } else {
                    error!("append_message_offset_length write_all error");
                }
            } else {
                error!("Invalid data slice");
            }
        }
        false
    }

    fn append_message_no_position_update(&self, data: &[u8], offset: usize, length: usize) -> bool {
        let current_pos = self.wrote_position.load(Ordering::Relaxed) as usize;

        if current_pos + length <= self.file_size as usize {
            let mut mapped_file =
                &mut self.get_mapped_file_mut()[current_pos..current_pos + length];
            if let Some(data_slice) = data.get(offset..offset + length) {
                if mapped_file.write_all(data_slice).is_ok() {
                    return true;
                } else {
                    error!("append_message_offset_length write_all error");
                }
            } else {
                error!("Invalid data slice");
            }
        }
        false
    }

    fn write_bytes_segment(&self, data: &[u8], start: usize, offset: usize, length: usize) -> bool {
        if start + length <= self.file_size as usize {
            let mut mapped_file = &mut self.get_mapped_file_mut()[start..start + length];
            if data.len() == length {
                if mapped_file.write_all(data).is_ok() {
                    return true;
                } else {
                    error!("append_message_offset_length write_all error");
                }
            } else if let Some(data_slice) = data.get(offset..offset + length) {
                if mapped_file.write_all(data_slice).is_ok() {
                    return true;
                } else {
                    error!("append_message_offset_length write_all error");
                }
            } else {
                error!("Invalid data slice");
            }
        }
        false
    }

    fn put_slice(&self, data: &[u8], index: usize) -> bool {
        let length = data.len();
        let end_index = index + length;
        if length > 0 && end_index <= self.file_size as usize {
            let mut mapped_file = &mut self.get_mapped_file_mut()[index..end_index];
            if mapped_file.write_all(data).is_ok() {
                return true;
            } else {
                error!("append_message_offset_length write_all error");
            }
        }
        false
    }

    #[inline]
    fn get_file_from_offset(&self) -> u64 {
        self.file_from_offset
    }

    fn flush(&self, flush_least_pages: i32) -> i32 {
        if self.is_able_to_flush(flush_least_pages) {
            if MappedFile::hold(self) {
                let value = self.get_read_position();
                self.mapped_byte_buffer_access_count_since_last_swap
                    .fetch_add(1, Ordering::AcqRel);

                if self.transient_store_pool.is_none() {
                    if let Err(e) = self.mmapped_file.flush() {
                        error!("Error occurred when force data to disk: {:?}", e);
                    } else {
                        self.last_flush_time
                            .store(get_current_millis(), Ordering::Relaxed);
                    }
                    MappedFile::release(self);
                } else {
                    unimplemented!(
                        // need to implement
                        "flush transient store pool"
                    );
                }
                self.flushed_position.store(value, Ordering::Release);
            } else {
                warn!(
                    "in flush, hold failed, flush offset = {}",
                    self.flushed_position.load(Ordering::Relaxed)
                );
                self.flushed_position
                    .store(self.get_read_position(), Ordering::Release);
            }
        }
        self.get_flushed_position()
    }

    #[inline]
    fn commit(&self, commit_least_pages: i32) -> i32 {
        unimplemented!(
            // need to implement
            "commit"
        )
    }

    fn select_mapped_buffer(&self, pos: i32, size: i32) -> Option<SelectMappedBufferResult> {
        let read_position = self.get_read_position();
        if pos + size <= read_position {
            if MappedFile::hold(self) {
                self.mapped_byte_buffer_access_count_since_last_swap
                    .fetch_add(1, Ordering::AcqRel);
                Some(SelectMappedBufferResult {
                    start_offset: self.file_from_offset + pos as u64,
                    size,
                    mapped_file: None,
                    is_in_cache: true,
                    ..Default::default()
                })
            } else {
                warn!(
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

    #[inline]
    fn get_mapped_byte_buffer(&self) -> &[u8] {
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);
        self.mmapped_file.as_ref()
    }

    #[inline]
    fn slice_byte_buffer(&self) -> &[u8] {
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);
        self.mmapped_file.as_ref()
    }

    #[inline]
    fn get_store_timestamp(&self) -> u64 {
        self.store_timestamp.load(Ordering::Relaxed)
    }

    #[inline]
    fn get_last_modified_timestamp(&self) -> u64 {
        self.file
            .metadata()
            .unwrap()
            .modified()
            .unwrap()
            .elapsed()
            .unwrap()
            .as_millis() as u64
    }

    fn get_data(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        let read_position = self.get_read_position();
        let read_end_position = pos + size;
        if read_end_position <= read_position as usize {
            if MappedFile::hold(self) {
                let buffer = BytesMut::from(&self.mmapped_file.as_ref()[pos..read_end_position]);
                MappedFile::release(self);
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

    #[inline]
    fn destroy(&self, interval_forcibly: u64) -> bool {
        MappedFile::shutdown(self, interval_forcibly);
        if self.is_cleanup_over() {
            if let Err(e) = fs::remove_file(self.file_name.as_str()) {
                error!("delete file failed: {:?}", e);
                false
            } else {
                info!("delete file success: {}", self.file_name);
                true
            }
        } else {
            warn!("destroy mapped file failed, cleanup over failed");
            false
        }
    }

    #[inline]
    fn shutdown(&self, interval_forcibly: u64) {
        self.reference_resource.shutdown(interval_forcibly);
    }

    #[inline]
    fn release(&self) {
        self.reference_resource.release();
    }

    #[inline]
    fn hold(&self) -> bool {
        self.reference_resource.hold()
    }

    #[inline]
    fn is_first_create_in_queue(&self) -> bool {
        self.first_create_in_queue
    }

    #[inline]
    fn set_first_create_in_queue(&mut self, first_create_in_queue: bool) {
        self.first_create_in_queue = first_create_in_queue
    }

    #[inline]
    fn get_flushed_position(&self) -> i32 {
        self.flushed_position.load(Ordering::Acquire)
    }

    #[inline]
    fn set_flushed_position(&self, flushed_position: i32) {
        self.flushed_position
            .store(flushed_position, Ordering::SeqCst)
    }

    #[inline]
    fn get_wrote_position(&self) -> i32 {
        self.wrote_position
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[inline]
    fn set_wrote_position(&self, wrote_position: i32) {
        self.wrote_position.store(wrote_position, Ordering::SeqCst)
    }

    /// Return The max position which have valid data
    ///
    /// # Returns
    ///
    /// An `i32` representing the current read position.
    #[inline]
    fn get_read_position(&self) -> i32 {
        match self.transient_store_pool {
            None => self.wrote_position.load(Ordering::Acquire),
            Some(_) => {
                //need to optimize
                self.wrote_position.load(Ordering::Acquire)
            }
        }
    }

    #[inline]
    fn set_committed_position(&self, committed_position: i32) {
        self.committed_position
            .store(committed_position, Ordering::SeqCst)
    }

    #[inline]
    fn get_committed_position(&self) -> i32 {
        self.committed_position.load(Ordering::Acquire)
    }

    #[inline]
    fn mlock(&self) {
        todo!()
    }

    #[inline]
    fn munlock(&self) {
        todo!()
    }

    #[inline]
    fn warm_mapped_file(&self, flush_disk_type: FlushDiskType, pages: usize) {
        todo!()
    }

    #[inline]
    fn swap_map(&self) -> bool {
        todo!()
    }

    #[inline]
    fn clean_swaped_map(&self, force: bool) {
        todo!()
    }

    #[inline]
    fn get_recent_swap_map_time(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_mapped_byte_buffer_access_count_since_last_swap(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_file(&self) -> &File {
        &self.file
    }

    #[inline]
    fn rename_to_delete(&self) {
        todo!()
    }

    #[inline]
    fn move_to_parent(&self) -> std::io::Result<()> {
        todo!()
    }

    #[inline]
    fn get_last_flush_time(&self) -> u64 {
        self.last_flush_time.load(Ordering::Relaxed)
    }

    #[inline]
    #[cfg(target_os = "linux")]
    fn is_loaded(&self, position: i64, size: usize) -> bool {
        // use libc::c_void;
        // use libc::mincore;
        // use libc::EINVAL;
        // let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        // let page_count = (length + page_size - 1) / page_size;

        // let mut vec = vec![0u8; page_count];
        // let ret = unsafe { mincore(address as *mut c_void, length, vec.as_mut_ptr()) };

        // if ret == -1 {
        //     return false;
        // }

        // !vec.iter().any(|&byte| byte & 1 == 0)
        true
    }

    #[inline]
    #[cfg(target_os = "windows")]
    fn is_loaded(&self, position: i64, size: usize) -> bool {
        /*use windows::Win32::Foundation::{BOOL, HANDLE};
        use windows::Win32::System::Memory::{VirtualQuery, MEMORY_BASIC_INFORMATION, MEM_COMMIT};

        let address = self.mmapped_file.as_ptr().wrapping_add(position as usize);
        let mut info: MEMORY_BASIC_INFORMATION = unsafe { std::mem::zeroed() };
        let mut offset = 0;

        while offset < length {
            let result = unsafe {
                VirtualQuery(
                    address.add(offset) as *const _,
                    &mut info,
                    std::mem::size_of::<MEMORY_BASIC_INFORMATION>(),
                )
            };

            if result == 0 {
                return Err(std::io::Error::last_os_error());
            }

            if info.State != MEM_COMMIT {
                return Ok(false);
            }

            offset += info.RegionSize;
        }*/

        true
    }

    #[inline]
    #[cfg(target_os = "macos")]
    fn is_loaded(&self, position: i64, size: usize) -> bool {
        /*use windows::Win32::Foundation::{BOOL, HANDLE};
        use windows::Win32::System::Memory::{VirtualQuery, MEMORY_BASIC_INFORMATION, MEM_COMMIT};

        let address = self.mmapped_file.as_ptr().wrapping_add(position as usize);
        let mut info: MEMORY_BASIC_INFORMATION = unsafe { std::mem::zeroed() };
        let mut offset = 0;

        while offset < length {
            let result = unsafe {
                VirtualQuery(
                    address.add(offset) as *const _,
                    &mut info,
                    std::mem::size_of::<MEMORY_BASIC_INFORMATION>(),
                )
            };

            if result == 0 {
                return Err(std::io::Error::last_os_error());
            }

            if info.State != MEM_COMMIT {
                return Ok(false);
            }

            offset += info.RegionSize;
        }*/

        true
    }

    fn select_mapped_buffer_with_position(&self, pos: i32) -> Option<SelectMappedBufferResult> {
        let read_position = self.get_read_position();
        if pos < read_position && pos >= 0 && MappedFile::hold(self) {
            self.mapped_byte_buffer_access_count_since_last_swap
                .fetch_add(1, Ordering::AcqRel);
            Some(SelectMappedBufferResult {
                start_offset: self.get_file_from_offset() + pos as u64,
                size: read_position - pos,
                mapped_file: None,
                is_in_cache: true,
                ..Default::default()
            })
        } else {
            None
        }
    }

    fn init(
        &self,
        file_name: &CheetahString,
        file_size: usize,
        transient_store_pool: &TransientStorePool,
    ) -> std::io::Result<()> {
        unimplemented!("init")
    }

    fn get_slice(&self, pos: usize, size: usize) -> Option<&[u8]> {
        if pos >= self.file_size as usize || pos + size >= self.file_size as usize {
            return None;
        }
        Some(&self.get_mapped_file()[pos..pos + size])
    }
}

#[allow(unused_variables)]
impl DefaultMappedFile {
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub fn get_mapped_file_mut(&self) -> &mut MmapMut {
        self.mmapped_file.mut_from_ref()
    }

    #[inline]
    pub fn get_mapped_file(&self) -> &MmapMut {
        self.mmapped_file.as_ref()
    }

    #[inline]
    fn is_able_to_flush(&self, flush_least_pages: i32) -> bool {
        if self.is_full() {
            return true;
        }
        let flush = self.flushed_position.load(Ordering::Acquire);
        let write = self.get_read_position();
        if flush_least_pages > 0 {
            return (write - flush) / OS_PAGE_SIZE as i32 >= flush_least_pages;
        }
        write > flush
    }

    #[inline]
    fn cleanup(&self, current_ref: i64) -> bool {
        if MappedFile::is_available(self) {
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

impl ReferenceResource for DefaultMappedFile {
    fn hold(&self) -> bool {
        self.reference_resource.hold()
    }

    fn is_available(&self) -> bool {
        self.reference_resource.is_available()
    }

    fn shutdown(&self, interval_forcibly: u64) {
        self.reference_resource.shutdown(interval_forcibly)
    }

    fn release(&self) {
        self.reference_resource.release()
    }

    fn get_ref_count(&self) -> i64 {
        self.reference_resource.get_ref_count()
    }

    fn cleanup(&self, current_ref: i64) -> bool {
        self.cleanup(current_ref)
    }

    fn is_cleanup_over(&self) -> bool {
        self.reference_resource.is_cleanup_over()
    }
}
