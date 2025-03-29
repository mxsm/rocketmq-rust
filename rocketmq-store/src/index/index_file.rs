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
use std::mem;
use std::sync::Arc;

use bytes::Buf;
use cheetah_string::CheetahString;
use rocketmq_common::common::hasher::string_hasher::JavaStringHasher;
use tracing::info;
use tracing::warn;

use crate::index::index_header::IndexHeader;
use crate::index::index_header::INDEX_HEADER_SIZE;
use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

const HASH_SLOT_SIZE: usize = 4;
const INDEX_SIZE: usize = 20;
const INVALID_INDEX: i32 = 0;

/// Each index's store unit. Format:
/// ```text
/// ┌───────────────┬───────────────────────────────┬───────────────┬───────────────┐
/// │ Key HashCode  │        Physical Offset        │   Time Diff   │ Next Index Pos│
/// │   (4 Bytes)   │          (8 Bytes)            │   (4 Bytes)   │   (4 Bytes)   │
/// ├───────────────┴───────────────────────────────┴───────────────┴───────────────┤
/// │                                 Index Store Unit                              │
/// │                                                                               │
/// ```
/// Each index's store unit. Size:
/// Key HashCode(4) + Physical Offset(8) + Time Diff(4) + Next Index Pos(4) = 20 Bytes
pub struct IndexFile {
    hash_slot_num: usize,
    index_num: usize,
    file_total_size: usize,
    mapped_file: Arc<DefaultMappedFile>,
    index_header: IndexHeader,
}

impl PartialEq for IndexFile {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self as *const IndexFile, other as *const IndexFile)
    }
}

impl IndexFile {
    pub fn new(
        file_name: &str,
        hash_slot_num: usize,
        index_num: usize,
        end_phy_offset: i64,
        end_timestamp: i64,
    ) -> IndexFile {
        let file_total_size =
            INDEX_HEADER_SIZE + (hash_slot_num * HASH_SLOT_SIZE) + (index_num * INDEX_SIZE);
        let mapped_file = Arc::new(DefaultMappedFile::new(
            CheetahString::from_slice(file_name),
            file_total_size as u64,
        ));

        let index_header = IndexHeader::new(mapped_file.clone());
        let index_file = IndexFile {
            hash_slot_num,
            index_num,
            file_total_size,
            mapped_file,
            index_header,
        };

        if end_phy_offset > 0 {
            index_file.index_header.set_begin_phy_offset(end_phy_offset);
            index_file.index_header.set_end_phy_offset(end_phy_offset);
        }

        if end_timestamp > 0 {
            index_file.index_header.set_begin_timestamp(end_timestamp);
            index_file.index_header.set_end_timestamp(end_timestamp);
        }
        index_file
    }

    #[inline]
    pub fn get_file_name(&self) -> &CheetahString {
        self.mapped_file.get_file_name()
    }

    #[inline]
    pub fn get_file_size(&self) -> usize {
        self.file_total_size
    }

    #[inline]
    pub fn load(&self) {
        self.index_header.load();
    }

    #[inline]
    pub fn shutdown(&self) {
        self.flush();
    }

    pub fn flush(&self) {
        let begin_time = std::time::Instant::now();
        if self.mapped_file.hold() {
            self.index_header.update_byte_buffer();
            self.mapped_file.flush(0);
            self.mapped_file.release();
            info!(
                "flush index file elapsed time(ms) {}",
                begin_time.elapsed().as_millis()
            );
        }
    }

    #[inline]
    pub fn is_write_full(&self) -> bool {
        self.index_header.get_index_count() >= self.index_num as i32
    }

    #[inline]
    pub fn destroy(&self, interval_forcibly: u64) -> bool {
        self.mapped_file.destroy(interval_forcibly)
    }

    pub fn put_key(&self, key: &str, phy_offset: i64, store_timestamp: i64) -> bool {
        if self.index_header.get_index_count() < self.index_num as i32 {
            let hash_code = self.index_key_hash_method(key);
            let slot_pos = hash_code as usize % self.hash_slot_num;
            // Calculate the absolute position of the slot
            let abs_slot_pos = INDEX_HEADER_SIZE + slot_pos * HASH_SLOT_SIZE;

            let mapped_file = self.mapped_file.get_mapped_file_mut();

            let mut slot_value = mapped_file
                .get(abs_slot_pos..abs_slot_pos + 4)
                .unwrap()
                .get_i32();

            if slot_value <= INVALID_INDEX || slot_value > self.index_header.get_index_count() {
                slot_value = INVALID_INDEX;
            }

            let mut time_diff = store_timestamp - self.index_header.get_begin_timestamp();
            time_diff /= 1000;
            if self.index_header.get_begin_timestamp() <= 0 {
                time_diff = 0;
            } else if time_diff > i32::MAX as i64 {
                time_diff = i32::MAX as i64;
            } else if time_diff < 0 {
                time_diff = 0;
            }

            let abs_index_pos = INDEX_HEADER_SIZE
                + self.hash_slot_num * HASH_SLOT_SIZE
                + self.index_header.get_index_count() as usize * INDEX_SIZE;

            self.mapped_file.write_bytes_segment(
                &hash_code.to_be_bytes(),
                abs_index_pos,
                0,
                mem::size_of::<i32>(),
            );
            self.mapped_file.write_bytes_segment(
                &phy_offset.to_be_bytes(),
                abs_index_pos + 4,
                0,
                mem::size_of::<i64>(),
            );
            self.mapped_file.write_bytes_segment(
                &(time_diff as i32).to_be_bytes(),
                abs_index_pos + 4 + 8,
                0,
                mem::size_of::<i32>(),
            );
            self.mapped_file.write_bytes_segment(
                &slot_value.to_be_bytes(),
                abs_index_pos + 4 + 8 + 4,
                0,
                mem::size_of::<i32>(),
            );
            self.mapped_file.write_bytes_segment(
                &self.index_header.get_index_count().to_be_bytes(),
                abs_slot_pos,
                0,
                mem::size_of::<i32>(),
            );

            if self.index_header.get_index_count() <= 1 {
                self.index_header.set_begin_phy_offset(phy_offset);
                self.index_header.set_begin_timestamp(store_timestamp);
            }

            if slot_value == INVALID_INDEX {
                self.index_header.inc_hash_slot_count();
            }
            self.index_header.inc_index_count();
            self.index_header.set_end_phy_offset(phy_offset);
            self.index_header.set_end_timestamp(store_timestamp);
            true
        } else {
            warn!(
                "Over index file capacity: index count = {}; index max num = {}",
                self.index_header.get_index_count(),
                self.index_num
            );
            false
        }
    }

    pub fn index_key_hash_method(&self, key: &str) -> i32 {
        let hash_code = JavaStringHasher::hash_str(key);
        if hash_code == i32::MIN {
            0
        } else {
            hash_code.abs()
        }
    }

    #[inline]
    pub fn get_begin_timestamp(&self) -> i64 {
        self.index_header.get_begin_timestamp()
    }

    #[inline]
    pub fn get_end_timestamp(&self) -> i64 {
        self.index_header.get_end_timestamp()
    }

    #[inline]
    pub fn get_end_phy_offset(&self) -> i64 {
        self.index_header.get_end_phy_offset()
    }

    pub fn is_time_matched(&self, begin: i64, end: i64) -> bool {
        let begin_timestamp = self.index_header.get_begin_timestamp();
        let end_timestamp = self.index_header.get_end_timestamp();
        begin < begin_timestamp && end > end_timestamp
            || begin >= begin_timestamp && begin <= end_timestamp
            || end >= begin_timestamp && end <= end_timestamp
    }

    pub fn select_phy_offset(
        &self,
        phy_offsets: &mut Vec<i64>,
        key: &str,
        max_num: usize,
        begin: i64,
        end: i64,
    ) {
        if !self.mapped_file.hold() {
            return;
        }

        let key_hash = self.index_key_hash_method(key);
        let slot_pos = key_hash as usize % self.hash_slot_num;
        let abs_slot_pos = INDEX_HEADER_SIZE + slot_pos * HASH_SLOT_SIZE;

        let mut buffer = self
            .mapped_file
            .get_data(abs_slot_pos, abs_slot_pos + 4)
            .unwrap();
        let slot_value = buffer.get_i32();
        if slot_value <= INVALID_INDEX
            || slot_value > self.index_header.get_index_count()
            || self.index_header.get_index_count() <= 1
        {
            return;
        }

        let mut next_index_to_read = slot_value;
        while phy_offsets.len() < max_num {
            let abs_index_pos = INDEX_HEADER_SIZE
                + self.hash_slot_num * HASH_SLOT_SIZE
                + next_index_to_read as usize * INDEX_SIZE;

            let key_hash_read = buffer.slice(abs_index_pos..abs_index_pos + 4).get_i32();
            let phy_offset_read = buffer
                .slice(abs_index_pos + 4..abs_index_pos + 12)
                .get_i64();
            let time_diff = buffer
                .slice(abs_index_pos + 12..abs_index_pos + 16)
                .get_i32();
            let prev_index_read = buffer
                .slice(abs_index_pos + 16..abs_index_pos + 20)
                .get_i32();

            if time_diff < 0 {
                break;
            }

            let time_read = self.index_header.get_begin_timestamp() + time_diff as i64 * 1000;
            if key_hash == key_hash_read && (time_read >= begin && time_read <= end) {
                phy_offsets.push(phy_offset_read);
            }

            if prev_index_read <= INVALID_INDEX
                || prev_index_read > self.index_header.get_index_count()
                || prev_index_read == next_index_to_read
                || time_read < begin
            {
                break;
            }

            next_index_to_read = prev_index_read;
        }
    }
}
