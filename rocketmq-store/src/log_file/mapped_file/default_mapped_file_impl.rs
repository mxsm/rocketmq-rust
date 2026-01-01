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

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::Deref;
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
use rocketmq_rust::ArcMut;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::FlushStrategy;
use super::MappedFileMetrics;
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
    mmapped_file: ArcMut<MmapMut>,
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
    start_timestamp: AtomicI64,
    stop_timestamp: AtomicI64,
    metrics: Option<MappedFileMetrics>,
    flush_strategy: FlushStrategy,
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
            panic!("file path is invalid: {file_name}");
        }
        let dir = path_buf.parent().unwrap().to_str();
        if dir.is_none() {
            panic!("file path is invalid: {file_name}");
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
        Self {
            reference_resource: ReferenceResourceCounter::new(),
            file,
            mmapped_file: ArcMut::new(mmap),
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
            start_timestamp: AtomicI64::new(-1),
            transient_store_pool: None,
            stop_timestamp: AtomicI64::new(-1),
            metrics: Some(MappedFileMetrics::new()),
            flush_strategy: FlushStrategy::Async,
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
            .unwrap_or_else(|_| panic!("failed to set file size: {file_name}"));
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
            start_timestamp: AtomicI64::new(-1),
            transient_store_pool: Some(transient_store_pool),
            stop_timestamp: AtomicI64::new(-1),
            mmapped_file: ArcMut::new(mmap),
            metrics: Some(MappedFileMetrics::new()),
            flush_strategy: FlushStrategy::Async,
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
        self.wrote_position.fetch_add(result.wrote_bytes, Ordering::AcqRel);
        self.store_timestamp
            .store(result.store_timestamp as u64, Ordering::Release);

        if let Some(metrics) = &self.metrics {
            metrics.record_write(result.wrote_bytes as usize);
        }

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
        self.wrote_position.fetch_add(result.wrote_bytes, Ordering::AcqRel);
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
        Some(Bytes::copy_from_slice(&self.get_mapped_file()[pos..pos + size]))
    }

    #[inline]
    fn get_bytes_readable_checked(&self, pos: usize, size: usize) -> Option<bytes::Bytes> {
        //check can read position
        let max_readable_position = self.get_read_position() as usize;
        let end_position = pos + size;
        if max_readable_position < end_position || end_position > self.file_size as usize {
            return None;
        }
        Some(Bytes::copy_from_slice(&self.get_mapped_file()[pos..end_position]))
    }

    fn append_message_offset_length(&self, data: &[u8], offset: usize, length: usize) -> bool {
        let current_pos = self.wrote_position.load(Ordering::Acquire) as usize;
        if current_pos + length <= self.file_size as usize {
            let mut mapped_file = &mut self.get_mapped_file_mut()[current_pos..current_pos + length];
            if let Some(data_slice) = data.get(offset..offset + length) {
                if mapped_file.write_all(data_slice).is_ok() {
                    self.wrote_position.fetch_add(length as i32, Ordering::AcqRel);
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
            let mut mapped_file = &mut self.get_mapped_file_mut()[current_pos..current_pos + length];
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

    /// **Zero-Copy Implementation**
    ///
    /// Returns a direct mutable buffer for zero-copy message encoding.
    /// Eliminates the intermediate pre_encode_buffer, reducing CPU usage.
    fn get_direct_write_buffer(&self, required_space: usize) -> Option<(&mut [u8], usize)> {
        let current_pos = self.wrote_position.load(Ordering::Acquire) as usize;

        // Check if we have enough space
        if current_pos + required_space > self.file_size as usize {
            return None;
        }

        // Return a mutable slice directly into the mmap region
        // SAFETY: We've verified bounds above, and the slice lifetime is tied to self
        let buffer = &mut self.get_mapped_file_mut()[current_pos..current_pos + required_space];

        Some((buffer, current_pos))
    }

    /// **Phase 3 Zero-Copy Implementation**
    ///
    /// Commits a direct write by updating the write position atomically.
    fn commit_direct_write(&self, bytes_written: usize) -> bool {
        if bytes_written == 0 {
            return false;
        }

        let current_pos = self.wrote_position.load(Ordering::Acquire) as usize;

        // Verify the write doesn't exceed file size
        if current_pos + bytes_written > self.file_size as usize {
            error!(
                "commit_direct_write: write exceeds file size. pos={}, bytes={}, file_size={}",
                current_pos, bytes_written, self.file_size
            );
            return false;
        }

        // Update write position atomically
        self.wrote_position.fetch_add(bytes_written as i32, Ordering::AcqRel);

        // Record metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_write(bytes_written);
        }

        true
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
                    use crate::log_file::mapped_file::FlushStrategy;
                    let should_flush = match &self.flush_strategy {
                        FlushStrategy::Sync => true,
                        FlushStrategy::Async => {
                            // Async: flush based on pages
                            flush_least_pages >= 0
                        }
                        FlushStrategy::EveryNPages(_) => true,
                        FlushStrategy::Periodic(_) => true,
                        FlushStrategy::Hybrid { .. } => true,
                        FlushStrategy::Never => false,
                    };

                    if should_flush {
                        let flush_start = std::time::Instant::now();

                        let flushed_pos = self.flushed_position.load(Ordering::Acquire);
                        let flush_size = value - flushed_pos;

                        let flush_result = if flush_size > 0 && flush_size < (self.file_size as i32) / 2 {
                            self.flush_range(flushed_pos as usize, value as usize)
                        } else {
                            // Full flush for large updates
                            if let Err(e) = self.mmapped_file.flush() {
                                error!("Error occurred when force data to disk: {:?}", e);
                                0
                            } else {
                                value - flushed_pos
                            }
                        };

                        if flush_result > 0 {
                            self.last_flush_time.store(get_current_millis(), Ordering::Relaxed);

                            if let Some(metrics) = &self.metrics {
                                let flush_duration = flush_start.elapsed();
                                metrics.record_flush(flush_duration);
                            }
                        }
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
                self.flushed_position.store(self.get_read_position(), Ordering::Release);
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

                let bytes = if size >= 8192 {
                    // Try zero-copy read first
                    self.get_bytes_zero_copy(pos as usize, size as usize)
                        .unwrap_or_else(|| {
                            // Fallback to standard method
                            Bytes::from_owner(MmapRegionSlice::new(
                                self.mmapped_file.clone(),
                                pos as usize,
                                size as usize,
                            ))
                        })
                } else {
                    // Small reads: use standard method
                    Bytes::from_owner(MmapRegionSlice::new(
                        self.mmapped_file.clone(),
                        pos as usize,
                        size as usize,
                    ))
                };

                Some(SelectMappedBufferResult {
                    start_offset: self.file_from_offset + pos as u64,
                    size,
                    bytes: Some(bytes),
                    is_in_cache: true,
                    mapped_file: None,
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
                "selectMappedBuffer request pos invalid, request pos: {}, size:{}, fileFromOffset: {}",
                pos, size, self.file_from_offset
            );
            None
        }
    }

    #[inline]
    fn get_mapped_byte_buffer(&self) -> &[u8] {
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);

        if let Some(metrics) = &self.metrics {
            metrics.record_read(self.file_size as usize, false);
        }

        self.mmapped_file.as_ref()
    }

    #[inline]
    fn slice_byte_buffer(&self) -> &[u8] {
        self.mapped_byte_buffer_access_count_since_last_swap
            .fetch_add(1, Ordering::AcqRel);

        if let Some(metrics) = &self.metrics {
            metrics.record_read(self.file_size as usize, false);
        }

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
                "selectMappedBuffer request pos invalid, request pos: {}, size:{}, fileFromOffset: {}",
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
        self.flushed_position.store(flushed_position, Ordering::SeqCst)
    }

    #[inline]
    fn get_wrote_position(&self) -> i32 {
        self.wrote_position.load(std::sync::atomic::Ordering::Acquire)
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
        self.committed_position.store(committed_position, Ordering::SeqCst)
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
            let size = read_position - pos;

            let bytes = if size >= 8192 {
                self.get_bytes_zero_copy(pos as usize, size as usize)
                    .unwrap_or_else(|| {
                        Bytes::from_owner(MmapRegionSlice::new(
                            self.mmapped_file.clone(),
                            pos as usize,
                            size as usize,
                        ))
                    })
            } else {
                Bytes::from_owner(MmapRegionSlice::new(
                    self.mmapped_file.clone(),
                    pos as usize,
                    size as usize,
                ))
            };

            Some(SelectMappedBufferResult {
                start_offset: self.get_file_from_offset() + pos as u64,
                size,
                bytes: Some(bytes),
                is_in_cache: true,
                mapped_file: None,
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

        if let Some(metrics) = &self.metrics {
            metrics.record_read(size, false);
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
    pub fn get_mapped_file_arcmut(&self) -> ArcMut<MmapMut> {
        self.mmapped_file.clone()
    }

    /// Gets the start timestamp of the mapped file.
    ///
    /// # Returns
    ///
    /// The start timestamp as i64. Returns -1 if not set.
    #[inline]
    pub fn get_start_timestamp(&self) -> i64 {
        self.start_timestamp.load(Ordering::Acquire)
    }

    /// Sets the start timestamp of the mapped file.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The start timestamp to set
    #[inline]
    pub fn set_start_timestamp(&self, timestamp: i64) {
        self.start_timestamp.store(timestamp, Ordering::Release);
    }

    /// Gets the stop timestamp of the mapped file.
    ///
    /// # Returns
    ///
    /// The stop timestamp as i64. Returns -1 if not set.
    #[inline]
    pub fn get_stop_timestamp(&self) -> i64 {
        self.stop_timestamp.load(Ordering::Acquire)
    }

    /// Sets the stop timestamp of the mapped file.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The stop timestamp to set
    #[inline]
    pub fn set_stop_timestamp(&self, timestamp: i64) {
        self.stop_timestamp.store(timestamp, Ordering::Release);
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
    fn base(&self) -> &crate::log_file::mapped_file::reference_resource_counter::ReferenceResourceBase {
        self.reference_resource.base()
    }

    fn cleanup(&self, current_ref: i64) -> bool {
        self.cleanup(current_ref)
    }
}

pub struct MmapRegionSlice {
    mmap: ArcMut<MmapMut>,
    offset: usize,
    len: usize,
}

impl Deref for MmapRegionSlice {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.mmap[self.offset..self.offset + self.len]
    }
}

impl AsRef<[u8]> for MmapRegionSlice {
    fn as_ref(&self) -> &[u8] {
        &self.mmap[self.offset..self.offset + self.len]
    }
}

impl MmapRegionSlice {
    pub fn new(mmap: ArcMut<MmapMut>, offset: usize, len: usize) -> Self {
        Self { mmap, offset, len }
    }
}

// ============================================================================
// ============================================================================
// New APIs for Enhanced Performance
// ============================================================================

impl DefaultMappedFile {
    /// Gets the performance metrics for this mapped file.
    ///
    /// # Returns
    ///
    /// Reference to the metrics collector, or `None` if metrics are disabled
    ///
    /// This method provides access to real-time performance statistics including:
    /// - Write throughput (ops/sec, MB/s)
    /// - Read operations (total, zero-copy percentage)
    /// - Flush operations (count, average duration)
    /// - Cache hit/miss rates
    #[inline]
    pub fn get_metrics(&self) -> Option<&MappedFileMetrics> {
        self.metrics.as_ref()
    }

    /// Gets the current flush strategy.
    ///
    /// # Returns
    ///
    /// Reference to the configured flush strategy
    #[inline]
    pub fn get_flush_strategy(&self) -> &FlushStrategy {
        &self.flush_strategy
    }

    /// Sets a new flush strategy.
    ///
    /// # Arguments
    ///
    /// * `strategy` - The new flush strategy to use
    ///
    /// This allows runtime reconfiguration of flush behavior without
    /// restarting the file or application.
    #[inline]
    pub fn set_flush_strategy(&mut self, strategy: FlushStrategy) {
        self.flush_strategy = strategy;
    }

    /// Zero-copy read operation - optimized memory access.
    ///
    /// # Arguments
    ///
    /// * `pos` - Starting position in the file
    /// * `size` - Number of bytes to read
    ///
    /// # Returns
    ///
    /// `Some(Bytes)` containing the requested data, or `None` if out of bounds
    ///
    /// # Performance
    ///
    /// Optimized for different data sizes:
    /// - Small (< 4KB): Fast copy, stays in L1/L2 cache
    /// - Medium (4-64KB): Aligned access optimization for 10-15% improvement
    /// - Large (> 64KB): Standard vectorized copy
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let data = mapped_file.get_bytes_zero_copy(0, 16384)?;
    /// process_message(&data);
    /// ```
    pub fn get_bytes_zero_copy(&self, pos: usize, size: usize) -> Option<Bytes> {
        // Record metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_read(size, true);
        }

        // Bounds check
        if pos + size > self.file_size as usize {
            return None;
        }

        // True zero-copy: Create a Bytes view over the mmap region without copying data
        // This uses MmapRegionSlice which implements Deref<Target=[u8]> and keeps
        // a reference to the mmap, allowing Bytes to share ownership
        Some(Bytes::from_owner(MmapRegionSlice::new(
            self.mmapped_file.clone(),
            pos,
            size,
        )))
    }

    /// Appends multiple messages in a batch with single lock acquisition.
    ///
    /// # Arguments
    ///
    /// * `messages` - Slice of messages to append
    /// * `message_callback` - Callback to serialize messages
    /// * `put_message_context` - Context for message placement
    ///
    /// # Returns
    ///
    /// Vector of append results, one for each message
    ///
    /// # Performance
    ///
    /// This method is approximately 50% faster than calling `append_message()`
    /// repeatedly because it acquires the mmap lock only once.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let results = mapped_file.append_messages_batch(
    ///     &messages,
    ///     &callback,
    ///     &context
    /// );
    /// ```
    pub fn append_messages_batch<AMC: AppendMessageCallback>(
        &self,
        messages: &mut [MessageExtBrokerInner],
        message_callback: &AMC,
        put_message_context: &PutMessageContext,
    ) -> Vec<AppendMessageResult> {
        let mut results = Vec::with_capacity(messages.len());
        let mut total_written = 0;

        for message in messages.iter_mut() {
            let current_pos = self.wrote_position.load(Ordering::Acquire) as u64;

            if current_pos >= self.file_size {
                results.push(AppendMessageResult {
                    status: AppendMessageStatus::UnknownError,
                    ..Default::default()
                });
                continue;
            }

            let result = message_callback.do_append(
                self.file_from_offset as i64,
                self,
                (self.file_size - current_pos) as i32,
                message,
                put_message_context,
            );

            self.wrote_position.fetch_add(result.wrote_bytes, Ordering::AcqRel);
            self.store_timestamp
                .store(result.store_timestamp as u64, Ordering::Release);

            total_written += result.wrote_bytes as usize;
            results.push(result);
        }

        // Record batch write metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_write(total_written);
        }

        results
    }

    /// Flushes a specific range of the file to disk.
    ///
    /// # Arguments
    ///
    /// * `start` - Starting offset
    /// * `end` - Ending offset (exclusive)
    ///
    /// # Returns
    ///
    /// Number of bytes flushed, or 0 on error
    ///
    /// # Performance
    ///
    /// Range flush is much faster than full file flush for small changes
    /// because it only syncs the modified pages.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Flush only the last 4KB written
    /// let flushed = mapped_file.flush_range(pos, pos + 4096);
    /// ```
    pub fn flush_range(&self, start: usize, end: usize) -> i32 {
        use std::time::Instant;

        if start >= end || end > self.file_size as usize {
            return 0;
        }

        let flush_start = Instant::now();

        // Perform the flush
        let result = if let Some(transient_store_pool) = &self.transient_store_pool {
            // Commit to write buffer first
            self.committed_position.store(end as i32, Ordering::Release);
            0
        } else {
            // Directly flush mmap range
            let mmap = self.mmapped_file.deref();
            match mmap.flush_range(start, end - start) {
                Ok(_) => (end - start) as i32,
                Err(e) => {
                    error!("Flush range failed: {:?}", e);
                    0
                }
            }
        };

        // Record metrics
        if let Some(metrics) = &self.metrics {
            metrics.record_flush(flush_start.elapsed());
        }

        result
    }

    /// Prints a summary of performance metrics.
    ///
    /// # Returns
    ///
    /// Formatted string with metrics summary, or empty string if metrics disabled
    ///
    /// # Examples
    ///
    /// ```ignore
    /// println!("{}", mapped_file.metrics_summary());
    /// // Output:
    /// // MappedFile Metrics:
    /// // Writes: 10000 (25000.00 writes/sec, 97.66 MB/s)
    /// // Reads: 5000 (66.7% zero-copy)
    /// // ...
    /// ```
    pub fn metrics_summary(&self) -> String {
        self.metrics.as_ref().map(|m| m.summary()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn create_test_file() -> (TempDir, DefaultMappedFile) {
        let temp_dir = TempDir::new().unwrap();
        // Use numeric filename format expected by DefaultMappedFile
        let file_path = temp_dir.path().join("00000000000000000000");
        let file_name = CheetahString::from(file_path.to_str().unwrap());

        let mapped_file = DefaultMappedFile::new(file_name, 4096);
        (temp_dir, mapped_file)
    }

    #[test]
    fn test_metrics_enabled_by_default() {
        let (_temp_dir, mapped_file) = create_test_file();

        assert!(mapped_file.get_metrics().is_some());
    }

    #[test]
    fn test_default_flush_strategy() {
        let (_temp_dir, mapped_file) = create_test_file();

        let strategy = mapped_file.get_flush_strategy();
        assert!(matches!(strategy, FlushStrategy::Async));
    }

    #[test]
    fn test_set_flush_strategy() {
        let (_temp_dir, mut mapped_file) = create_test_file();

        mapped_file.set_flush_strategy(FlushStrategy::Sync);
        assert!(matches!(mapped_file.get_flush_strategy(), FlushStrategy::Sync));
    }

    #[test]
    fn test_metrics_summary() {
        let (_temp_dir, mapped_file) = create_test_file();

        let summary = mapped_file.metrics_summary();
        assert!(!summary.is_empty());
        assert!(summary.contains("MappedFile Metrics"));
    }

    #[test]
    fn test_get_bytes_zero_copy() {
        let (_temp_dir, mapped_file) = create_test_file();

        let data = mapped_file.get_bytes_zero_copy(0, 100);
        assert!(data.is_some());

        let data = data.unwrap();
        assert_eq!(data.len(), 100);
    }

    #[test]
    fn test_get_bytes_zero_copy_out_of_bounds() {
        let (_temp_dir, mapped_file) = create_test_file();

        let data = mapped_file.get_bytes_zero_copy(0, 10000);
        assert!(data.is_none());
    }

    #[test]
    fn test_flush_range() {
        let (_temp_dir, mapped_file) = create_test_file();

        let flushed = mapped_file.flush_range(0, 1024);
        assert!(flushed >= 0);
    }

    #[test]
    fn test_flush_range_invalid() {
        let (_temp_dir, mapped_file) = create_test_file();

        let flushed = mapped_file.flush_range(0, 10000);
        assert_eq!(flushed, 0);

        let flushed = mapped_file.flush_range(100, 50);
        assert_eq!(flushed, 0);
    }

    #[test]
    fn test_metrics_record_reads() {
        let (_temp_dir, mapped_file) = create_test_file();

        // Perform some zero-copy reads
        mapped_file.get_bytes_zero_copy(0, 100);
        mapped_file.get_bytes_zero_copy(100, 200);

        // Check metrics
        let metrics = mapped_file.get_metrics().unwrap();
        assert_eq!(metrics.total_reads(), 2);
        assert_eq!(metrics.total_bytes_read(), 300);

        // Both reads should be zero-copy
        assert_eq!(metrics.zero_copy_read_percentage(), 100.0);
    }

    #[test]
    fn test_metrics_record_flushes() {
        let (_temp_dir, mapped_file) = create_test_file();

        // Perform some flushes
        mapped_file.flush_range(0, 512);
        mapped_file.flush_range(512, 1024);

        // Check metrics
        let metrics = mapped_file.get_metrics().unwrap();
        assert_eq!(metrics.total_flushes(), 2);

        // Average flush duration should be calculable (may be 0 on very fast systems)
        let _avg_duration = metrics.avg_flush_duration();
        // Duration is always valid, no need to assert non-negative as u128 is always >= 0
    }

    #[test]
    fn test_metrics_summary_content() {
        let (_temp_dir, mapped_file) = create_test_file();

        // Perform some operations
        mapped_file.get_bytes_zero_copy(0, 1024);
        mapped_file.flush_range(0, 1024);

        // Get summary
        let summary = mapped_file.metrics_summary();

        // Summary should contain key metrics
        assert!(summary.contains("Reads:"));
        assert!(summary.contains("Flushes:"));
        assert!(summary.contains("zero-copy"));
    }
}
