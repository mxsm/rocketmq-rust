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

use std::mem;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Buf;

use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

pub const INDEX_HEADER_SIZE: usize = 40;
const BEGIN_TIMESTAMP_INDEX: usize = 0;
const END_TIMESTAMP_INDEX: usize = 8;
const BEGIN_PHY_OFFSET_INDEX: usize = 16;
const END_PHY_OFFSET_INDEX: usize = 24;
const HASH_SLOT_COUNT_INDEX: usize = 32;
const INDEX_COUNT_INDEX: usize = 36;

/// Index File Header. Format
/// ```text
/// ┌───────────────────────────────┬───────────────────────────────┬───────────────────────────────┬───────────────────────────────┬───────────────────┬───────────────────┐
/// │        Begin Timestamp        │          End Timestamp        │     Begin Physical Offset     │       End Physical Offset     │  Hash Slot Count  │    Index Count    │
/// │           (8 Bytes)           │            (8 Bytes)          │           (8 Bytes)           │           (8 Bytes)           │      (4 Bytes)    │      (4 Bytes)    │
/// ├───────────────────────────────┴───────────────────────────────┴───────────────────────────────┴───────────────────────────────┴───────────────────┴───────────────────┤
/// │                                                                      Index File Header                                                                                │
/// │
/// ```
///
/// Index File Header. Size:
/// Begin Timestamp(8) + End Timestamp(8) + Begin Physical Offset(8) + End Physical Offset(8) + Hash
/// Slot Count(4) + Index Count(4) = 40 Bytes
pub struct IndexHeader {
    mapped_file: Arc<DefaultMappedFile>,
    begin_timestamp: AtomicI64,
    end_timestamp: AtomicI64,
    begin_phy_offset: AtomicI64,
    end_phy_offset: AtomicI64,
    hash_slot_count: AtomicI32,
    index_count: AtomicI32,
}

impl IndexHeader {
    pub fn new(mapped_file: Arc<DefaultMappedFile>) -> Self {
        Self {
            mapped_file,
            begin_timestamp: AtomicI64::new(0),
            end_timestamp: AtomicI64::new(0),
            begin_phy_offset: AtomicI64::new(0),
            end_phy_offset: AtomicI64::new(0),
            hash_slot_count: AtomicI32::new(0),
            index_count: AtomicI32::new(1),
        }
    }

    pub fn load(&self) {
        let mut buffer = self.mapped_file.get_bytes(0, INDEX_HEADER_SIZE).unwrap();
        self.begin_timestamp.store(buffer.get_i64(), Ordering::Relaxed);
        self.end_timestamp.store(buffer.get_i64(), Ordering::Relaxed);
        self.begin_phy_offset.store(buffer.get_i64(), Ordering::Relaxed);
        self.end_phy_offset.store(buffer.get_i64(), Ordering::Relaxed);
        self.hash_slot_count.store(buffer.get_i32(), Ordering::Relaxed);
        self.index_count.store(buffer.get_i32(), Ordering::Relaxed);
        if self.index_count.load(Ordering::Relaxed) <= 0 {
            self.index_count.store(1, Ordering::Relaxed);
        }
    }

    pub fn update_byte_buffer(&self) {
        self.mapped_file.put_slice(
            &self.begin_timestamp.load(Ordering::Acquire).to_be_bytes(),
            BEGIN_TIMESTAMP_INDEX,
        );

        self.mapped_file.put_slice(
            &self.end_timestamp.load(Ordering::Acquire).to_be_bytes(),
            END_TIMESTAMP_INDEX,
        );
        self.mapped_file.put_slice(
            &self.begin_phy_offset.load(Ordering::Acquire).to_be_bytes(),
            BEGIN_PHY_OFFSET_INDEX,
        );
        self.mapped_file.put_slice(
            &self.end_phy_offset.load(Ordering::Acquire).to_be_bytes(),
            END_PHY_OFFSET_INDEX,
        );
        self.mapped_file.put_slice(
            &self.hash_slot_count.load(Ordering::Acquire).to_be_bytes(),
            HASH_SLOT_COUNT_INDEX,
        );
        self.mapped_file.put_slice(
            &self.index_count.load(Ordering::Acquire).to_be_bytes(),
            INDEX_COUNT_INDEX,
        );
    }

    #[inline]
    pub fn get_begin_timestamp(&self) -> i64 {
        self.begin_timestamp.load(Ordering::Acquire)
    }

    pub fn set_begin_timestamp(&self, begin_timestamp: i64) {
        self.begin_timestamp.store(begin_timestamp, Ordering::Release);
        self.mapped_file.write_bytes_segment(
            begin_timestamp.to_be_bytes().as_ref(),
            BEGIN_TIMESTAMP_INDEX,
            0,
            mem::size_of::<i64>(),
        );
    }

    #[inline]
    pub fn get_end_timestamp(&self) -> i64 {
        self.end_timestamp.load(Ordering::Acquire)
    }

    pub fn set_end_timestamp(&self, end_timestamp: i64) {
        self.end_timestamp.store(end_timestamp, Ordering::Release);
        self.mapped_file.write_bytes_segment(
            end_timestamp.to_be_bytes().as_ref(),
            END_TIMESTAMP_INDEX,
            0,
            mem::size_of::<i64>(),
        );
    }

    #[inline]
    pub fn get_begin_phy_offset(&self) -> i64 {
        self.begin_phy_offset.load(Ordering::Acquire)
    }

    pub fn set_begin_phy_offset(&self, begin_phy_offset: i64) {
        self.begin_phy_offset.store(begin_phy_offset, Ordering::Release);
        self.mapped_file.write_bytes_segment(
            begin_phy_offset.to_be_bytes().as_ref(),
            BEGIN_PHY_OFFSET_INDEX,
            0,
            mem::size_of::<i64>(),
        );
    }

    #[inline]
    pub fn get_end_phy_offset(&self) -> i64 {
        self.end_phy_offset.load(Ordering::Acquire)
    }

    pub fn set_end_phy_offset(&self, end_phy_offset: i64) {
        self.end_phy_offset.store(end_phy_offset, Ordering::SeqCst);
        self.mapped_file.write_bytes_segment(
            end_phy_offset.to_be_bytes().as_ref(),
            END_PHY_OFFSET_INDEX,
            0,
            mem::size_of::<i64>(),
        );
    }

    #[inline]
    pub fn get_hash_slot_count(&self) -> i32 {
        self.hash_slot_count.load(Ordering::SeqCst)
    }

    pub fn inc_hash_slot_count(&self) {
        let result = self.hash_slot_count.fetch_add(1, Ordering::AcqRel) + 1;
        self.mapped_file.write_bytes_segment(
            result.to_be_bytes().as_ref(),
            HASH_SLOT_COUNT_INDEX,
            0,
            mem::size_of::<i32>(),
        );
    }

    #[inline]
    pub fn get_index_count(&self) -> i32 {
        self.index_count.load(Ordering::Acquire)
    }

    pub fn inc_index_count(&self) {
        let count = self.index_count.fetch_add(1, Ordering::AcqRel) + 1;
        self.mapped_file.write_bytes_segment(
            count.to_be_bytes().as_ref(),
            INDEX_COUNT_INDEX,
            0,
            mem::size_of::<i32>(),
        );
    }
}
