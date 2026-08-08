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

use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_store_local::index::codec::IndexHeaderRecord;
pub use rocketmq_store_local::index::codec::INDEX_HEADER_SIZE;
use rocketmq_store_local::index::file::IndexHeaderUpdate;

use crate::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use crate::log_file::mapped_file::MappedFile;

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
        let buffer = self.mapped_file.get_bytes(0, INDEX_HEADER_SIZE).unwrap();
        let record = IndexHeaderRecord::decode(&buffer).expect("mapped index header must contain 40 bytes");
        self.store_record(record);
    }

    pub fn update_byte_buffer(&self) {
        let _ = self.try_update_byte_buffer();
    }

    pub(crate) fn try_update_byte_buffer(&self) -> bool {
        self.mapped_file.put_slice(&self.record().encode(), 0)
    }

    pub(crate) fn try_apply_updates(&self, updates: &[IndexHeaderUpdate]) -> bool {
        let mut record = self.record();
        for update in updates {
            match *update {
                IndexHeaderUpdate::SetBeginPhyOffset(offset) => record.begin_phy_offset = offset,
                IndexHeaderUpdate::SetBeginTimestamp(timestamp) => record.begin_timestamp = timestamp,
                IndexHeaderUpdate::IncrementHashSlotCount => {
                    let Some(count) = record.hash_slot_count.checked_add(1) else {
                        return false;
                    };
                    record.hash_slot_count = count;
                }
                IndexHeaderUpdate::IncrementIndexCount => {
                    let Some(count) = record.index_count.checked_add(1) else {
                        return false;
                    };
                    record.index_count = count;
                }
                IndexHeaderUpdate::SetEndPhyOffset(offset) => record.end_phy_offset = offset,
                IndexHeaderUpdate::SetEndTimestamp(timestamp) => record.end_timestamp = timestamp,
            }
        }
        if !self.mapped_file.put_slice(&record.encode(), 0) {
            return false;
        }
        self.store_record(record);
        true
    }

    fn record(&self) -> IndexHeaderRecord {
        IndexHeaderRecord {
            begin_timestamp: self.begin_timestamp.load(Ordering::Acquire),
            end_timestamp: self.end_timestamp.load(Ordering::Acquire),
            begin_phy_offset: self.begin_phy_offset.load(Ordering::Acquire),
            end_phy_offset: self.end_phy_offset.load(Ordering::Acquire),
            hash_slot_count: self.hash_slot_count.load(Ordering::Acquire),
            index_count: self.index_count.load(Ordering::Acquire),
        }
    }

    fn store_record(&self, record: IndexHeaderRecord) {
        self.begin_timestamp.store(record.begin_timestamp, Ordering::Release);
        self.end_timestamp.store(record.end_timestamp, Ordering::Release);
        self.begin_phy_offset.store(record.begin_phy_offset, Ordering::Release);
        self.end_phy_offset.store(record.end_phy_offset, Ordering::SeqCst);
        self.hash_slot_count.store(record.hash_slot_count, Ordering::SeqCst);
        self.index_count.store(record.index_count, Ordering::Release);
    }

    #[inline]
    pub fn get_begin_timestamp(&self) -> i64 {
        self.begin_timestamp.load(Ordering::Acquire)
    }

    pub fn set_begin_timestamp(&self, begin_timestamp: i64) {
        let _ = self.try_apply_updates(&[IndexHeaderUpdate::SetBeginTimestamp(begin_timestamp)]);
    }

    #[inline]
    pub fn get_end_timestamp(&self) -> i64 {
        self.end_timestamp.load(Ordering::Acquire)
    }

    pub fn set_end_timestamp(&self, end_timestamp: i64) {
        let _ = self.try_apply_updates(&[IndexHeaderUpdate::SetEndTimestamp(end_timestamp)]);
    }

    #[inline]
    pub fn get_begin_phy_offset(&self) -> i64 {
        self.begin_phy_offset.load(Ordering::Acquire)
    }

    pub fn set_begin_phy_offset(&self, begin_phy_offset: i64) {
        let _ = self.try_apply_updates(&[IndexHeaderUpdate::SetBeginPhyOffset(begin_phy_offset)]);
    }

    #[inline]
    pub fn get_end_phy_offset(&self) -> i64 {
        self.end_phy_offset.load(Ordering::Acquire)
    }

    pub fn set_end_phy_offset(&self, end_phy_offset: i64) {
        let _ = self.try_apply_updates(&[IndexHeaderUpdate::SetEndPhyOffset(end_phy_offset)]);
    }

    #[inline]
    pub fn get_hash_slot_count(&self) -> i32 {
        self.hash_slot_count.load(Ordering::SeqCst)
    }

    pub fn inc_hash_slot_count(&self) {
        let _ = self.try_apply_updates(&[IndexHeaderUpdate::IncrementHashSlotCount]);
    }

    #[inline]
    pub fn get_index_count(&self) -> i32 {
        self.index_count.load(Ordering::Acquire)
    }

    pub fn inc_index_count(&self) {
        let _ = self.try_apply_updates(&[IndexHeaderUpdate::IncrementIndexCount]);
    }
}
