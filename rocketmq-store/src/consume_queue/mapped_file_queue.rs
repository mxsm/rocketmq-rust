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
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::UtilAll::offset_to_file_name;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::allocate_mapped_file_service::AllocateMappedFileService;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;
use crate::queue::single_consume_queue::CQ_STORE_UNIT_SIZE;

pub struct MappedFileQueue {
    pub(crate) store_path: String,

    pub(crate) mapped_file_size: u64,

    /// Lock-free concurrent access using ArcSwap (equivalent to Java's CopyOnWriteArrayList)
    pub(crate) mapped_files: ArcSwap<Vec<Arc<DefaultMappedFile>>>,

    pub(crate) allocate_mapped_file_service: Option<AllocateMappedFileService>,

    pub(crate) flushed_where: Arc<AtomicU64>,

    pub(crate) committed_where: Arc<AtomicU64>,

    pub(crate) store_timestamp: Arc<AtomicU64>,

    /// Commit lock for thread safety (matches Java's synchronized)
    commit_lock: Arc<Mutex<()>>,
}

impl Default for MappedFileQueue {
    fn default() -> Self {
        Self {
            store_path: String::new(),
            mapped_file_size: 0,
            mapped_files: ArcSwap::from_pointee(Vec::new()),
            allocate_mapped_file_service: None,
            flushed_where: Arc::new(AtomicU64::new(0)),
            committed_where: Arc::new(AtomicU64::new(0)),
            store_timestamp: Arc::new(AtomicU64::new(0)),
            commit_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl MappedFileQueue {
    #[inline]
    pub fn new(
        store_path: String,
        mapped_file_size: u64,
        allocate_mapped_file_service: Option<AllocateMappedFileService>,
    ) -> MappedFileQueue {
        MappedFileQueue {
            store_path,
            mapped_file_size,
            mapped_files: ArcSwap::from_pointee(Vec::new()),
            allocate_mapped_file_service,
            flushed_where: Arc::new(AtomicU64::new(0)),
            committed_where: Arc::new(AtomicU64::new(0)),
            store_timestamp: Arc::new(AtomicU64::new(0)),
            commit_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl MappedFileQueue {
    #[inline]
    pub fn load(&mut self) -> bool {
        //list dir files
        let dir = Path::new(&self.store_path);
        if let Ok(ls) = fs::read_dir(dir) {
            let files: Vec<_> = ls.filter_map(Result::ok).map(|entry| entry.path()).collect();
            return self.do_load(files);
        }
        true
    }

    /// Commit data to file system cache
    ///
    ///
    /// # Arguments
    /// * `commit_least_pages` - Minimum pages to commit
    ///
    /// # Returns
    /// Returns true if commit succeeded
    #[inline]
    pub fn commit(&self, commit_least_pages: i32) -> bool {
        let _lock = self.commit_lock.lock();

        let mut result = true;
        let committed_where = self.get_committed_where();
        if let Some(mapped_file) = self.find_mapped_file_by_offset(committed_where, committed_where == 0) {
            let offset = mapped_file.commit(commit_least_pages);
            let whered = mapped_file.get_file_from_offset() + offset as u64;
            result = whered == self.get_committed_where() as u64;
            self.set_committed_where(whered as i64);
        }
        result
    }

    #[inline]
    pub fn get_committed_where(&self) -> i64 {
        self.committed_where.load(Ordering::Acquire) as i64
    }

    pub fn check_self(&self) {
        let mapped_files = self.mapped_files.load();
        if !mapped_files.is_empty() {
            let mut iter = mapped_files.iter();
            let mut pre = iter.next();

            for cur in iter {
                if let Some(pre_file) = pre {
                    if cur.get_file_from_offset() - pre_file.get_file_from_offset() != self.mapped_file_size {
                        error!(
                            "[BUG] The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't \
                             match. pre file {}, cur file {}",
                            pre_file.get_file_name(),
                            cur.get_file_name()
                        );
                    }
                }
                pre = Some(cur);
            }
        }
    }

    pub fn do_load(&mut self, mut files: Vec<std::path::PathBuf>) -> bool {
        // Ascending order sorting
        files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        for (i, file) in files.iter().enumerate() {
            let metadata = match file.metadata() {
                Ok(meta) => meta,
                Err(e) => {
                    error!("Failed to get metadata for file {:?}: {}", file, e);
                    return false;
                }
            };

            if metadata.is_dir() {
                continue;
            }

            if metadata.len() == 0 && i == files.len() - 1 {
                match fs::remove_file(file) {
                    Ok(_) => warn!("{} size is 0, auto deleted.", file.display()),
                    Err(e) => warn!("Failed to delete file {}: {}", file.display(), e),
                }
                continue;
            }

            if metadata.len() != self.mapped_file_size {
                warn!(
                    "{} length not matched message store config value, please check it manually",
                    file.display()
                );
                return false;
            }

            let mapped_file = DefaultMappedFile::new(
                CheetahString::from_string(file.to_string_lossy().to_string()),
                self.mapped_file_size,
            );
            // Set wrote, flushed, committed positions for mapped_file
            mapped_file.set_wrote_position(self.mapped_file_size as i32);
            mapped_file.set_flushed_position(self.mapped_file_size as i32);
            mapped_file.set_committed_position(self.mapped_file_size as i32);

            // Write: copy-on-write update
            let mut files = (**self.mapped_files.load()).clone();
            files.push(Arc::new(mapped_file));
            self.mapped_files.store(Arc::new(files));
        }
        true
    }

    #[inline]
    pub fn get_last_mapped_file(&self) -> Option<Arc<DefaultMappedFile>> {
        self.mapped_files.load().last().cloned()
    }

    #[inline]
    pub fn get_first_mapped_file(&self) -> Option<Arc<DefaultMappedFile>> {
        self.mapped_files.load().first().cloned()
    }

    #[inline]
    pub fn get_last_mapped_file_mut_start_offset(
        &mut self,
        start_offset: u64,
        need_create: bool,
    ) -> Option<Arc<DefaultMappedFile>> {
        let mut create_offset = -1i64;
        let file_size = self.mapped_file_size as i64;
        let mapped_file_last = self.get_last_mapped_file();

        if let Some(ref current_file) = mapped_file_last {
            let usage_ratio = current_file.get_wrote_position() as f64 / self.mapped_file_size as f64;
            if usage_ratio >= 0.8 && !current_file.is_full() {
                // Pre-allocate next file in background
                let next_offset = current_file.get_file_from_offset() + self.mapped_file_size;
                self.trigger_pre_allocation(next_offset);
            }
        }

        match mapped_file_last {
            None => {
                create_offset = start_offset as i64 - (start_offset as i64 % file_size);
            }
            Some(ref value) => {
                if value.is_full() {
                    create_offset = value.get_file_from_offset() as i64 + file_size
                }
            }
        }
        if create_offset != -1 && need_create {
            return self.try_create_mapped_file(create_offset as u64);
        }
        mapped_file_last
    }

    #[inline]
    fn trigger_pre_allocation(&self, next_offset: u64) {
        if let Some(ref service) = self.allocate_mapped_file_service {
            let next_file_path = PathBuf::from(self.store_path.clone()).join(offset_to_file_name(next_offset));

            // Submit async request (non-blocking)
            let _drop = service.submit_request(next_file_path.to_string_lossy().to_string(), self.mapped_file_size);
            std::mem::drop(_drop);
        }
    }

    #[inline]
    pub fn try_create_mapped_file(&mut self, create_offset: u64) -> Option<Arc<DefaultMappedFile>> {
        let next_file_path = PathBuf::from(self.store_path.clone()).join(offset_to_file_name(create_offset));
        let next_next_file_path =
            PathBuf::from(self.store_path.clone()).join(offset_to_file_name(create_offset + self.mapped_file_size));
        self.do_create_mapped_file(next_file_path, next_next_file_path)
    }

    /// Create mapped file with refactored logic to avoid deadlock
    ///
    /// # Arguments
    /// * `next_file_path` - Path for the file to create
    /// * `next_next_file_path` - Path for pre-allocation (N+2 file)
    ///
    /// # Returns
    /// The created mapped file wrapped in Arc
    #[inline]
    fn do_create_mapped_file(
        &mut self,
        next_file_path: PathBuf,
        next_next_file_path: PathBuf,
    ) -> Option<Arc<DefaultMappedFile>> {
        let is_first = self.mapped_files.load().is_empty();

        let mut arc_file = self.create_mapped_file_internal(next_file_path.clone(), next_next_file_path)?;

        if is_first {
            if let Some(file) = Arc::get_mut(&mut arc_file) {
                file.set_first_create_in_queue(true);
            }
        }

        // Write: copy-on-write update
        let mut files = (**self.mapped_files.load()).clone();
        files.push(arc_file.clone());
        self.mapped_files.store(Arc::new(files));

        Some(arc_file)
    }

    /// Internal method to create mapped file (lock-free)
    fn create_mapped_file_internal(
        &self,
        file_path: PathBuf,
        next_file_path: PathBuf,
    ) -> Option<Arc<DefaultMappedFile>> {
        let file_path_str = file_path.to_string_lossy().to_string();

        // Try async allocation if service available
        if let Some(ref service) = self.allocate_mapped_file_service {
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                match rt.block_on(async {
                    service
                        .allocate_mapped_file(file_path_str.clone(), self.mapped_file_size)
                        .await
                }) {
                    Ok(pre_allocated) => {
                        // Trigger pre-allocation of N+2 file
                        std::mem::drop(
                            service.submit_request(next_file_path.to_string_lossy().to_string(), self.mapped_file_size),
                        );
                        // Return Arc directly
                        return Some(pre_allocated);
                    }
                    Err(e) => {
                        warn!("Pre-allocation failed: {}, using sync creation", e);
                    }
                }
            }
        }

        // Fallback: synchronous creation
        Some(Arc::new(DefaultMappedFile::new(
            CheetahString::from_string(file_path_str),
            self.mapped_file_size,
        )))
    }

    #[inline]
    pub fn get_mapped_files(&self) -> &ArcSwap<Vec<Arc<DefaultMappedFile>>> {
        &self.mapped_files
    }

    #[inline]
    pub fn get_mapped_files_size(&self) -> usize {
        self.mapped_files.load().len()
    }

    #[inline]
    pub fn set_flushed_where(&self, flushed_where: i64) {
        self.flushed_where.store(flushed_where as u64, Ordering::SeqCst);
    }

    #[inline]
    pub fn set_committed_where(&self, committed_where: i64) {
        self.committed_where.store(committed_where as u64, Ordering::SeqCst);
    }

    /// Truncate dirty files beyond the specified offset
    ///
    /// # Arguments
    /// * `offset` - The offset beyond which files are considered dirty
    #[inline]
    pub fn truncate_dirty_files(&mut self, offset: i64) {
        let mut will_remove_files = Vec::new();

        for mapped_file in self.mapped_files.load().iter() {
            let file_tail_offset = mapped_file.get_file_from_offset() + self.mapped_file_size;
            if file_tail_offset as i64 > offset {
                if offset >= mapped_file.get_file_from_offset() as i64 {
                    mapped_file.set_wrote_position((offset % self.mapped_file_size as i64) as i32);
                    mapped_file.set_committed_position((offset % self.mapped_file_size as i64) as i32);
                    mapped_file.set_flushed_position((offset % self.mapped_file_size as i64) as i32);
                } else {
                    mapped_file.destroy(1000);
                    will_remove_files.push(mapped_file.clone());
                }
            }
        }

        //Actually delete the files that were marked for removal
        self.delete_expired_file(will_remove_files);
    }

    #[inline]
    pub fn get_max_offset(&self) -> i64 {
        match self.get_last_mapped_file() {
            None => 0,
            Some(file) => file.get_file_from_offset() as i64 + file.get_read_position() as i64,
        }
    }

    #[inline]
    pub fn delete_last_mapped_file(&mut self) {
        if let Some(last_mapped_file) = self.get_last_mapped_file() {
            last_mapped_file.destroy(1000);

            // Write: copy-on-write update
            let mut files = (**self.mapped_files.load()).clone();
            files.retain(|mf| mf.as_ref() != last_mapped_file.as_ref());
            self.mapped_files.store(Arc::new(files));

            info!(
                "on recover, destroy a logic mapped file {}",
                last_mapped_file.get_file_name()
            );
        }
    }

    #[inline]
    pub(crate) fn delete_expired_file(&mut self, files: Vec<Arc<DefaultMappedFile>>) {
        let mut files = files;
        let current_files = self.mapped_files.load();
        if !files.is_empty() {
            files.retain(|mf| current_files.contains(mf));

            // Write: copy-on-write update
            let mut new_files = (**current_files).clone();
            new_files.retain(|mf| !files.contains(mf));
            self.mapped_files.store(Arc::new(new_files));
        }
    }

    /// Delete expired files by time
    ///
    /// This is a critical method for disk space management.
    ///
    /// # Arguments
    /// * `expired_time` - Maximum time a file can exist (milliseconds)
    /// * `delete_files_interval` - Interval between deleting files (milliseconds)
    /// * `interval_forcibly` - Force deletion interval (milliseconds)
    /// * `clean_immediately` - Whether to clean immediately
    /// * `delete_file_batch_max` - Maximum files to delete in one batch
    ///
    /// # Returns
    /// Number of files deleted
    pub fn delete_expired_file_by_time(
        &mut self,
        expired_time: i64,
        delete_files_interval: i32,
        interval_forcibly: i64,
        clean_immediately: bool,
        delete_file_batch_max: i32,
    ) -> i32 {
        let mfs = (**self.mapped_files.load()).clone();
        let mfs_length = mfs.len().saturating_sub(1);
        let mut delete_count = 0;
        let mut files = Vec::new();

        // Check before deleting
        self.check_self();

        for (i, mapped_file) in mfs.iter().enumerate().take(mfs_length) {
            let live_max_timestamp = mapped_file.get_last_modified_timestamp() as i64 + expired_time;

            if get_current_millis() as i64 >= live_max_timestamp || clean_immediately {
                if mapped_file.destroy(interval_forcibly as u64) {
                    files.push(mapped_file.clone());
                    delete_count += 1;

                    if files.len() >= delete_file_batch_max as usize {
                        break;
                    }

                    if delete_files_interval > 0 && (i + 1) < mfs_length {
                        std::thread::sleep(std::time::Duration::from_millis(delete_files_interval as u64));
                    }
                } else {
                    break;
                }
            } else {
                // Avoid deleting files in the middle
                break;
            }
        }

        self.delete_expired_file(files);
        delete_count
    }

    /// Delete expired files by offset
    ///
    /// This method deletes files based on consumption progress.
    ///
    /// # Arguments
    /// * `offset` - The minimum physical offset still needed
    /// * `unit_size` - Size of each unit (for ConsumeQueue, typically CQ_STORE_UNIT_SIZE)
    ///
    /// # Returns
    /// Number of files deleted
    pub fn delete_expired_file_by_offset(&mut self, offset: i64, unit_size: i32) -> i32 {
        let mfs = (**self.mapped_files.load()).clone();
        let mut files = Vec::new();
        let mut delete_count = 0;
        let mfs_length = mfs.len().saturating_sub(1);

        for mapped_file in mfs.iter().take(mfs_length) {
            let mut destroy = false;

            if let Some(result) =
                mapped_file.select_mapped_buffer((self.mapped_file_size - unit_size as u64) as i32, unit_size)
            {
                if let Some(ref buffer) = result.bytes {
                    if buffer.len() >= 8 {
                        let max_offset_in_logic_queue = i64::from_be_bytes(buffer[0..8].try_into().unwrap_or([0u8; 8]));
                        destroy = max_offset_in_logic_queue < offset;

                        if destroy {
                            info!(
                                "physic min offset {}, logics in current mappedFile max offset {}, delete it",
                                offset, max_offset_in_logic_queue
                            );
                        }
                    }
                }
            } else if !mapped_file.is_available() {
                warn!("Found a hanged consume queue file, attempting to delete it.");
                destroy = true;
            } else {
                warn!("this being not executed forever.");
                break;
            }

            if destroy && mapped_file.destroy(1000 * 60) {
                files.push(mapped_file.clone());
                delete_count += 1;
            } else {
                break;
            }
        }

        self.delete_expired_file(files);
        delete_count
    }

    /// Reset offset to specified position
    ///
    /// Used in recovery scenarios.
    ///
    /// # Arguments
    /// * `offset` - Target offset to reset to
    ///
    /// # Returns
    /// true if reset succeeded, false if offset is too far back
    pub fn reset_offset(&mut self, offset: i64) -> bool {
        // Check if offset is reasonable
        if let Some(mapped_file_last) = self.get_last_mapped_file() {
            let last_offset =
                mapped_file_last.get_file_from_offset() as i64 + mapped_file_last.get_wrote_position() as i64;
            let diff = last_offset - offset;
            let max_diff = (self.mapped_file_size * 2) as i64;

            if diff > max_diff {
                return false;
            }
        }

        // Load current files
        let current_files = self.mapped_files.load();
        let mut to_removes = Vec::new();

        // Iterate backwards
        for i in (0..current_files.len()).rev() {
            let mapped_file = &current_files[i];

            if offset >= mapped_file.get_file_from_offset() as i64 {
                let where_pos = (offset % mapped_file.get_file_size() as i64) as i32;
                mapped_file.set_flushed_position(where_pos);
                mapped_file.set_wrote_position(where_pos);
                mapped_file.set_committed_position(where_pos);
                break;
            } else {
                to_removes.push(i);
            }
        }

        // Remove files beyond the offset (copy-on-write update)
        if !to_removes.is_empty() {
            let mut new_files = (**current_files).clone();
            for &idx in to_removes.iter().rev() {
                new_files.remove(idx);
            }
            self.mapped_files.store(Arc::new(new_files));
        }

        true
    }

    /// Check if mapped files list is empty
    #[inline]
    pub fn is_mapped_files_empty(&self) -> bool {
        self.mapped_files.load().is_empty()
    }

    /// Check if list is empty or current file is full
    #[inline]
    pub fn is_empty_or_current_file_full(&self) -> bool {
        match self.get_last_mapped_file() {
            None => true,
            Some(file) => file.is_full(),
        }
    }

    /// Check if should roll to next file
    ///
    /// # Arguments
    /// * `msg_size` - Size of message to write
    ///
    /// # Returns
    /// true if should create new file
    #[inline]
    pub fn should_roll(&self, msg_size: i32) -> bool {
        if self.is_empty_or_current_file_full() {
            return true;
        }

        if let Some(mapped_file_last) = self.get_last_mapped_file() {
            if mapped_file_last.get_wrote_position() + msg_size > mapped_file_last.get_file_size() as i32 {
                return true;
            }
        }

        false
    }

    /// Get minimum offset in the queue
    ///
    /// Returns -1 if no mapped files exist
    #[inline]
    pub fn get_min_offset(&self) -> i64 {
        if !self.mapped_files.load().is_empty() {
            if let Some(first) = self.get_first_mapped_file() {
                return first.get_file_from_offset() as i64;
            }
        }
        -1
    }

    /// Get total mapped memory size
    ///
    /// Only counts available (not destroyed) mapped files
    #[inline]
    pub fn get_mapped_memory_size(&self) -> i64 {
        let files = self.mapped_files.load();
        let mut size = 0i64;
        for mapped_file in files.iter() {
            if mapped_file.is_available() {
                size += self.mapped_file_size as i64;
            }
        }
        size
    }

    /// Get how much data has fallen behind
    ///
    /// Returns the difference between max wrote position and flushed position
    #[inline]
    pub fn how_much_fall_behind(&self) -> i64 {
        if self.mapped_files.load().is_empty() {
            return 0;
        }

        let committed = self.get_flushed_where();
        if committed != 0 {
            if let Some(mapped_file) = self.get_last_mapped_file() {
                return (mapped_file.get_file_from_offset() as i64 + mapped_file.get_wrote_position() as i64)
                    - committed;
            }
        }

        0
    }

    /// Gracefully shutdown all mapped files
    ///
    /// # Arguments
    /// * `interval_forcibly` - Force shutdown interval (milliseconds)
    pub fn shutdown(&self, interval_forcibly: u64) {
        for mapped_file in self.mapped_files.load().iter() {
            mapped_file.shutdown(interval_forcibly);
        }
    }

    /// Create a snapshot of current mapped files
    ///
    /// Returns a safe copy of all mapped files that can be iterated
    /// without holding the lock.
    ///
    /// # Returns
    /// Vector of Arc-wrapped mapped files
    ///
    /// # Example
    /// ```ignore
    /// let snapshot = queue.snapshot();
    /// for file in snapshot {
    ///     // Process each file without holding locks
    /// }
    /// ```
    #[inline]
    pub fn snapshot(&self) -> Vec<Arc<DefaultMappedFile>> {
        (**self.mapped_files.load()).clone()
    }

    /// Create an iterator over mapped files
    ///
    /// # Returns
    /// Iterator that yields Arc<DefaultMappedFile>
    ///
    /// # Example
    /// ```ignore
    /// for file in queue.iter() {
    ///     println!("File: {}", file.get_file_name());
    /// }
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = Arc<DefaultMappedFile>> {
        self.snapshot().into_iter()
    }

    /// Create a reversed iterator over mapped files
    ///
    /// Iterates from newest to oldest file.
    ///
    /// # Returns
    /// Reversed iterator that yields Arc<DefaultMappedFile>
    ///
    /// # Example
    /// ```ignore
    /// for file in queue.iter_reversed() {
    ///     println!("File (reversed): {}", file.get_file_name());
    /// }
    /// ```
    pub fn iter_reversed(&self) -> impl Iterator<Item = Arc<DefaultMappedFile>> {
        let mut snapshot = self.snapshot();
        snapshot.reverse();
        snapshot.into_iter()
    }

    /// Get mapped files within a specific offset range
    ///
    /// Returns files whose range [fileFromOffset, fileFromOffset + fileSize)
    /// intersects with the requested range [from, to).
    ///
    /// # Arguments
    /// * `from` - Start offset (inclusive)
    /// * `to` - End offset (exclusive)
    ///
    /// # Returns
    /// Vector of mapped files in the range
    ///
    /// # Example
    /// ```ignore
    /// let files = queue.range(1024, 4096);
    /// for file in files {
    ///     // Process files that overlap with [1024, 4096)
    /// }
    /// ```
    pub fn range(&self, from: i64, to: i64) -> Vec<Arc<DefaultMappedFile>> {
        let mut result = Vec::new();
        let files = self.mapped_files.load();

        for mapped_file in files.iter() {
            let file_from = mapped_file.get_file_from_offset() as i64;
            let file_to = file_from + mapped_file.get_file_size() as i64;

            // File ends before range starts - skip
            if file_to <= from {
                continue;
            }

            // File starts after range ends - break (files are sorted)
            if file_from >= to {
                break;
            }

            // File overlaps with range - include it
            result.push(mapped_file.clone());
        }

        result
    }

    /// Get total file size (all mapped files combined)
    ///
    /// # Returns
    /// Total size in bytes
    #[inline]
    pub fn get_total_file_size(&self) -> i64 {
        let files = self.mapped_files.load();
        (files.len() as i64) * (self.mapped_file_size as i64)
    }

    /// Get the number of mapped files
    ///
    /// # Returns
    /// Count of mapped files in the queue
    #[inline]
    pub fn get_mapped_file_count(&self) -> usize {
        self.mapped_files.load().len()
    }

    /// Get store path
    ///
    /// # Returns
    /// The storage directory path
    #[inline]
    pub fn get_store_path(&self) -> &str {
        &self.store_path
    }

    /// Get mapped file size
    ///
    /// # Returns
    /// Size of each mapped file in bytes
    #[inline]
    pub fn get_mapped_file_size_config(&self) -> u64 {
        self.mapped_file_size
    }

    /// Retry deleting the first file
    ///
    /// Attempts to delete the first file if it's not available.
    /// This is used for cleanup when a file is destroyed but still in the list.
    ///
    /// # Arguments
    /// * `interval_forcibly` - Force deletion timeout (milliseconds)
    ///
    /// # Returns
    /// true if deletion succeeded, false otherwise
    pub fn retry_delete_first_file(&mut self, interval_forcibly: i64) -> bool {
        if let Some(first) = self.get_first_mapped_file() {
            if !first.is_available() {
                warn!(
                    "The mappedFile was destroyed once, but still alive: {}",
                    first.get_file_name()
                );

                let result = first.destroy(interval_forcibly as u64);
                if result {
                    info!("The mappedFile re-delete OK: {}", first.get_file_name());
                    let files = vec![first];
                    self.delete_expired_file(files);
                } else {
                    warn!("The mappedFile re-delete failed: {}", first.get_file_name());
                }

                return result;
            }
        }

        false
    }

    /// Swap mapped byte buffers to reduce memory pressure
    ///
    /// This method implements memory-mapped buffer swapping for old files
    /// that are no longer frequently accessed. It helps reduce memory usage
    /// by unmapping buffers that haven't been accessed recently.
    ///
    /// # Arguments
    /// * `reserve_num` - Number of recent files to keep in memory (minimum 3)
    /// * `force_swap_interval_ms` - Force swap after this interval (milliseconds)
    /// * `normal_swap_interval_ms` - Normal swap interval if file was accessed
    ///
    /// # Note
    /// - Only files older than `reserve_num` are candidates for swapping
    /// - Files are swapped if they exceed the swap interval
    ///
    /// # Example
    /// ```ignore
    /// // Swap buffers for files older than 10 minutes
    /// queue.swap_map(3, 1000 * 60 * 10, 1000 * 60 * 5);
    /// ```
    pub fn swap_map(&self, reserve_num: i32, force_swap_interval_ms: i64, normal_swap_interval_ms: i64) {
        let files = self.mapped_files.load();

        if files.is_empty() {
            return;
        }

        // Ensure we reserve at least 3 files
        let reserve_num = if reserve_num < 3 { 3 } else { reserve_num };

        let files_len = files.len() as i32;

        // Process files, skipping the most recent reserved files
        // Iterates from (length-reserve-1) down to 0
        for i in (0..=(files_len - reserve_num - 1)).rev() {
            if i < 0 {
                break;
            }

            let mapped_file = &files[i as usize];
            let current_time = get_current_millis() as i64;
            let recent_swap_time = mapped_file.get_recent_swap_map_time();

            // Force swap if interval exceeded
            if current_time - recent_swap_time > force_swap_interval_ms {
                mapped_file.swap_map();
                continue;
            }

            // Normal swap if accessed and interval exceeded
            if current_time - recent_swap_time > normal_swap_interval_ms
                && mapped_file.get_mapped_byte_buffer_access_count_since_last_swap() > 0
            {
                mapped_file.swap_map();
            }
        }
    }

    /// Clean swapped mapped byte buffers
    ///
    /// This method cleans up swapped buffers that have been swapped for
    /// a long time and are unlikely to be accessed again. This helps
    /// reclaim system resources.
    ///
    /// # Arguments
    /// * `force_clean_swap_interval_ms` - Clean buffers swapped longer than this
    ///
    /// # Note
    /// - Keeps the 3 most recent files from cleaning
    /// - Only cleans buffers that have been swapped for a long time
    ///
    /// # Example
    /// ```ignore
    /// // Clean buffers swapped for more than 1 hour
    /// queue.clean_swapped_map(1000 * 60 * 60);
    /// ```
    pub fn clean_swapped_map(&self, force_clean_swap_interval_ms: i64) {
        let files = self.mapped_files.load();

        if files.is_empty() {
            return;
        }

        let reserve_num = 3;
        let files_len = files.len() as i32;

        // Process files, skipping the most recent reserved files
        // Iterates from (length-reserve-1) down to 0
        for i in (0..=(files_len - reserve_num - 1)).rev() {
            if i < 0 {
                break;
            }

            let mapped_file = &files[i as usize];
            let current_time = get_current_millis() as i64;
            let recent_swap_time = mapped_file.get_recent_swap_map_time();

            if current_time - recent_swap_time > force_clean_swap_interval_ms {
                // Note: clean_swapped_map method needs to be implemented in MappedFile
                // For now, we can call swap_map again which has similar effect
                let _ = mapped_file.swap_map();
            }
        }
    }

    /// Get mapped file by timestamp
    ///
    /// Finds the mapped file that contains data for the given timestamp.
    /// This is a simple lookup based on file modification time.
    ///
    /// # Arguments
    /// * `timestamp` - The timestamp to search for (milliseconds)
    ///
    /// # Returns
    /// The mapped file containing data for that timestamp, or the last file
    pub fn get_mapped_file_by_time(&self, timestamp: i64) -> Option<Arc<DefaultMappedFile>> {
        let files = self.snapshot();

        if files.is_empty() {
            return None;
        }

        for mapped_file in files.iter() {
            if mapped_file.get_last_modified_timestamp() as i64 >= timestamp {
                return Some(mapped_file.clone());
            }
        }

        // Return last file if no match
        files.last().cloned()
    }

    // ============ Additional Helper Methods ============

    /// Copy mapped files to array
    ///
    /// Internal method to create a snapshot of files, optionally reserving
    /// some files from the copy.
    ///
    /// # Arguments
    /// * `reserved_mapped_files` - Number of files to exclude from the end
    ///
    /// # Returns
    /// Vector of mapped files, or None if insufficient files
    fn copy_mapped_files(&self, reserved_mapped_files: usize) -> Option<Vec<Arc<DefaultMappedFile>>> {
        let files = self.mapped_files.load();

        if files.len() <= reserved_mapped_files {
            return None;
        }

        Some((**files).clone())
    }

    /// Stream access to mapped files
    ///
    /// Returns an iterator that can be used with standard iterator combinators.
    #[inline]
    pub fn stream(&self) -> impl Iterator<Item = Arc<DefaultMappedFile>> {
        self.iter()
    }

    /// Destroy all mapped files and cleanup resources
    ///
    /// This method destroys all mapped files and removes the storage directory.
    /// It should only be called during shutdown or cleanup operations.
    #[inline]
    pub fn destroy(&mut self) {
        for mapped_file in self.mapped_files.load().iter() {
            mapped_file.destroy(1000 * 3);
        }
        self.mapped_files.store(Arc::new(Vec::new()));
        self.set_flushed_where(0);
        let path = PathBuf::from(&self.store_path);
        if path.is_dir() {
            let _ = fs::remove_dir_all(path);
        }
    }

    /// Find mapped file by offset
    ///
    /// Locates the mapped file containing the specified offset.
    /// Uses direct indexing for O(1) lookup when possible.
    ///
    /// # Arguments
    /// * `offset` - The offset to search for
    /// * `return_first_on_not_found` - Return first file if offset not found
    ///
    /// # Returns
    /// The mapped file containing the offset, or None
    pub fn find_mapped_file_by_offset(
        &self,
        offset: i64,
        return_first_on_not_found: bool,
    ) -> Option<Arc<DefaultMappedFile>> {
        let first_mapped_file = self.get_first_mapped_file();
        let last_mapped_file = self.get_last_mapped_file();

        match (first_mapped_file, last_mapped_file) {
            (Some(first), Some(last)) => {
                let first_offset = first.get_file_from_offset() as i64;
                let last_offset = last.get_file_from_offset() as i64 + self.mapped_file_size as i64;

                if offset < first_offset || offset >= last_offset {
                    return if return_first_on_not_found { Some(first) } else { None };
                }

                // Try direct index calculation (O(1) access)
                let index = (offset as usize / self.mapped_file_size as usize)
                    - (first_offset as usize / self.mapped_file_size as usize);

                let files = self.mapped_files.load();
                if let Some(file) = files.get(index).cloned() {
                    let file_offset = file.get_file_from_offset() as i64;
                    // Must check both lower and upper bounds
                    if offset >= file_offset && offset < file_offset + self.mapped_file_size as i64 {
                        return Some(file);
                    }
                }

                // Fall back to linear search if direct indexing fails
                for file in files.iter() {
                    let file_offset = file.get_file_from_offset() as i64;
                    if offset >= file_offset && offset < file_offset + self.mapped_file_size as i64 {
                        return Some(file.clone());
                    }
                }

                // Not found in any file
                if return_first_on_not_found {
                    Some(first)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    #[inline]
    pub fn get_flushed_where(&self) -> i64 {
        self.flushed_where.load(Ordering::Acquire) as i64
    }

    #[inline]
    pub fn set_store_timestamp(&self, store_timestamp: u64) {
        self.store_timestamp.store(store_timestamp, Ordering::Release);
    }

    #[inline]
    pub fn get_store_timestamp(&self) -> u64 {
        self.store_timestamp.load(Ordering::Acquire)
    }

    #[inline]
    pub fn flush(&self, flush_least_pages: i32) -> bool {
        let mut result = true;
        let flushed_where = self.get_flushed_where();
        if let Some(mapped_file) = self.find_mapped_file_by_offset(flushed_where, flushed_where == 0) {
            let tmp_time_stamp = mapped_file.get_store_timestamp();
            let offset = mapped_file.flush(flush_least_pages);
            let whered = mapped_file.get_file_from_offset() + offset as u64;
            result = whered == self.get_flushed_where() as u64;
            self.set_flushed_where(whered as i64);
            if flush_least_pages == 0 {
                self.set_store_timestamp(tmp_time_stamp);
            }
        }
        result
    }

    #[inline]
    pub fn remain_how_many_data_to_commit(&self) -> i64 {
        self.get_max_wrote_position() - self.get_committed_where()
    }

    #[inline]
    pub fn remain_how_many_data_to_flush(&self) -> i64 {
        self.get_max_offset() - self.get_flushed_where()
    }
    #[inline]
    fn get_max_wrote_position(&self) -> i64 {
        let mapped_file = self.get_last_mapped_file();
        match mapped_file {
            None => 0,
            Some(file) => file.get_file_from_offset() as i64 + file.get_wrote_position() as i64,
        }
    }

    /// Gets a consume queue mapped file by timestamp.
    ///
    /// This method finds the appropriate mapped file based on the given timestamp
    /// and boundary type. It ensures each mapped file in the consume queue has
    /// accurate start and stop timestamps based on the commit log.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp to search for
    /// * `commit_log` - Reference to the commit log for looking up message store times
    /// * `boundary_type` - The boundary type (Lower or Upper)
    ///
    /// # Returns
    ///
    /// The mapped file that matches the criteria, or None if not found
    pub fn get_consume_queue_mapped_file_by_time(
        &self,
        timestamp: i64,
        commit_log: &CommitLog,
        boundary_type: BoundaryType,
    ) -> Option<Arc<DefaultMappedFile>> {
        let mapped_files = self.mapped_files.load();
        if mapped_files.is_empty() {
            return None;
        }

        let mfs: Vec<Arc<DefaultMappedFile>> = mapped_files.iter().cloned().collect();

        let mfs_len = mfs.len();

        for i in (0..mfs_len).rev() {
            let mapped_file = &mfs[i];

            if mapped_file.get_start_timestamp() < 0 {
                if let Some(select_result) = mapped_file.select_mapped_buffer(0, CQ_STORE_UNIT_SIZE) {
                    if let Some(ref buffer) = select_result.bytes {
                        if buffer.len() >= 12 {
                            let physical_offset = i64::from_be_bytes(buffer[0..8].try_into().unwrap());
                            let message_size = i32::from_be_bytes(buffer[8..12].try_into().unwrap());
                            let message_store_time = commit_log.pickup_store_timestamp(physical_offset, message_size);
                            if message_store_time > 0 {
                                mapped_file.set_start_timestamp(message_store_time);
                            }
                        }
                    }
                }
            }

            if i < mfs_len - 1 && mapped_file.get_stop_timestamp() < 0 {
                let last_unit_offset = self.mapped_file_size as i32 - CQ_STORE_UNIT_SIZE;
                if let Some(select_result) = mapped_file.select_mapped_buffer(last_unit_offset, CQ_STORE_UNIT_SIZE) {
                    if let Some(ref buffer) = select_result.bytes {
                        if buffer.len() >= 12 {
                            let physical_offset = i64::from_be_bytes(buffer[0..8].try_into().unwrap());
                            let message_size = i32::from_be_bytes(buffer[8..12].try_into().unwrap());
                            let message_store_time = commit_log.pickup_store_timestamp(physical_offset, message_size);
                            if message_store_time > 0 {
                                mapped_file.set_stop_timestamp(message_store_time);
                            }
                        }
                    }
                }
            }
        }

        match boundary_type {
            BoundaryType::Lower => {
                for (i, mapped_file) in mfs.iter().enumerate() {
                    if i < mfs_len - 1 {
                        if mapped_file.get_stop_timestamp() >= timestamp {
                            return Some(mapped_file.clone());
                        }
                    } else {
                        return Some(mapped_file.clone());
                    }
                }
            }
            BoundaryType::Upper => {
                for mapped_file in mfs.iter().rev() {
                    if mapped_file.get_start_timestamp() <= timestamp {
                        return Some(mapped_file.clone());
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_empty_dir() {
        let mut queue = MappedFileQueue {
            store_path: String::from("/path/to/empty/dir"),
            ..MappedFileQueue::default()
        };
        assert!(queue.load());
        assert!(queue.mapped_files.load().is_empty());
    }

    #[test]
    fn test_load_with_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file1_path = temp_dir.path().join("1111");
        let file2_path = temp_dir.path().join("2222");
        fs::File::create(&file1_path).unwrap();
        fs::File::create(&file2_path).unwrap();

        let mut queue = MappedFileQueue {
            store_path: temp_dir.path().to_string_lossy().into_owned(),
            ..MappedFileQueue::default()
        };
        assert!(queue.load());
        assert_eq!(queue.mapped_files.load().len(), 1);
    }

    #[test]
    fn test_load_with_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("1111");
        fs::File::create(&file_path).unwrap();

        let mut queue = MappedFileQueue {
            store_path: temp_dir.path().to_string_lossy().into_owned(),
            ..MappedFileQueue::default()
        };
        assert!(queue.load());
        assert!(queue.mapped_files.load().is_empty());
    }

    #[test]
    fn test_load_with_invalid_file_size() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("invalid_file.txt");
        fs::write(&file_path, "Some data").unwrap();

        let mut queue = MappedFileQueue {
            store_path: temp_dir.path().to_string_lossy().into_owned(),
            ..MappedFileQueue::default()
        };
        assert!(!queue.load());
        assert!(queue.mapped_files.load().is_empty());
    }

    #[test]
    fn test_load_with_correct_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("1111");
        fs::write(&file_path, vec![0u8; 1024]).unwrap();

        let mut queue = MappedFileQueue {
            store_path: temp_dir.path().to_string_lossy().into_owned(),
            mapped_file_size: 1024,
            ..MappedFileQueue::default()
        };
        assert!(queue.load());
        assert_eq!(queue.mapped_files.load().len(), 1);
    }
}
