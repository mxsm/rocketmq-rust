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
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use std::cell::UnsafeCell;

const INDEX_HEADER_SIZE: usize = 40;
const BEGIN_TIMESTAMP_INDEX: usize = 0;
const END_TIMESTAMP_INDEX: usize = 8;
const BEGIN_PHY_OFFSET_INDEX: usize = 16;
const END_PHY_OFFSET_INDEX: usize = 24;
const HASH_SLOT_COUNT_INDEX: usize = 32;
const INDEX_COUNT_INDEX: usize = 36;

#[derive(Debug)]
pub struct IndexHeader {
    byte_buffer: Arc<UnsafeCell<Vec<u8>>>,
    begin_timestamp: AtomicI64,
    end_timestamp: AtomicI64,
    begin_phy_offset: AtomicI64,
    end_phy_offset: AtomicI64,
    hash_slot_count: AtomicI32,
    index_count: AtomicI32,
}

impl IndexHeader {
    pub fn new(byte_buffer: Vec<u8>) -> Self {
        Self {
            byte_buffer: Arc::new(UnsafeCell::new(byte_buffer)),
            begin_timestamp: AtomicI64::new(0),
            end_timestamp: AtomicI64::new(0),
            begin_phy_offset: AtomicI64::new(0),
            end_phy_offset: AtomicI64::new(0),
            hash_slot_count: AtomicI32::new(0),
            index_count: AtomicI32::new(1),
        }
    }

    pub fn load(&self) {
        unsafe {
            let buffer = &*self.byte_buffer.get();
            self.begin_timestamp.store(i64::from_be_bytes(buffer[BEGIN_TIMESTAMP_INDEX..BEGIN_TIMESTAMP_INDEX + 8].try_into().unwrap()), Ordering::SeqCst);
            self.end_timestamp.store(i64::from_be_bytes(buffer[END_TIMESTAMP_INDEX..END_TIMESTAMP_INDEX + 8].try_into().unwrap()), Ordering::SeqCst);
            self.begin_phy_offset.store(i64::from_be_bytes(buffer[BEGIN_PHY_OFFSET_INDEX..BEGIN_PHY_OFFSET_INDEX + 8].try_into().unwrap()), Ordering::SeqCst);
            self.end_phy_offset.store(i64::from_be_bytes(buffer[END_PHY_OFFSET_INDEX..END_PHY_OFFSET_INDEX + 8].try_into().unwrap()), Ordering::SeqCst);
            self.hash_slot_count.store(i32::from_be_bytes(buffer[HASH_SLOT_COUNT_INDEX..HASH_SLOT_COUNT_INDEX + 4].try_into().unwrap()), Ordering::SeqCst);
            self.index_count.store(i32::from_be_bytes(buffer[INDEX_COUNT_INDEX..INDEX_COUNT_INDEX + 4].try_into().unwrap()), Ordering::SeqCst);
            if self.index_count.load(Ordering::SeqCst) <= 0 {
                self.index_count.store(1, Ordering::SeqCst);
            }
        }
    }

    pub fn update_byte_buffer(&self) {
        unsafe {
            let buffer = &mut *self.byte_buffer.get();
            buffer[BEGIN_TIMESTAMP_INDEX..BEGIN_TIMESTAMP_INDEX + 8].copy_from_slice(&self.begin_timestamp.load(Ordering::SeqCst).to_be_bytes());
            buffer[END_TIMESTAMP_INDEX..END_TIMESTAMP_INDEX + 8].copy_from_slice(&self.end_timestamp.load(Ordering::SeqCst).to_be_bytes());
            buffer[BEGIN_PHY_OFFSET_INDEX..BEGIN_PHY_OFFSET_INDEX + 8].copy_from_slice(&self.begin_phy_offset.load(Ordering::SeqCst).to_be_bytes());
            buffer[END_PHY_OFFSET_INDEX..END_PHY_OFFSET_INDEX + 8].copy_from_slice(&self.end_phy_offset.load(Ordering::SeqCst).to_be_bytes());
            buffer[HASH_SLOT_COUNT_INDEX..HASH_SLOT_COUNT_INDEX + 4].copy_from_slice(&self.hash_slot_count.load(Ordering::SeqCst).to_be_bytes());
            buffer[INDEX_COUNT_INDEX..INDEX_COUNT_INDEX + 4].copy_from_slice(&self.index_count.load(Ordering::SeqCst).to_be_bytes());
        }
    }

    pub fn update_byte_buffer_type(&self) {
        unsafe {
            let buffer = &mut *self.byte_buffer.get();
            buffer[BEGIN_TIMESTAMP_INDEX..BEGIN_TIMESTAMP_INDEX + 8].copy_from_slice(&self.begin_timestamp.load(Ordering::SeqCst).to_be_bytes());
            buffer[END_TIMESTAMP_INDEX..END_TIMESTAMP_INDEX + 8].copy_from_slice(&self.end_timestamp.load(Ordering::SeqCst).to_be_bytes());
            buffer[BEGIN_PHY_OFFSET_INDEX..BEGIN_PHY_OFFSET_INDEX + 8].copy_from_slice(&self.begin_phy_offset.load(Ordering::SeqCst).to_be_bytes());
            buffer[END_PHY_OFFSET_INDEX..END_PHY_OFFSET_INDEX + 8].copy_from_slice(&self.end_phy_offset.load(Ordering::SeqCst).to_be_bytes());
            buffer[HASH_SLOT_COUNT_INDEX..HASH_SLOT_COUNT_INDEX + 4].copy_from_slice(&self.hash_slot_count.load(Ordering::SeqCst).to_be_bytes());
            buffer[INDEX_COUNT_INDEX..INDEX_COUNT_INDEX + 4].copy_from_slice(&self.index_count.load(Ordering::SeqCst).to_be_bytes());
        }
    }

    pub fn get_begin_timestamp(&self) -> i64 {
        self.begin_timestamp.load(Ordering::SeqCst)
    }

    pub fn set_begin_timestamp(&self, begin_timestamp: i64) {
        self.begin_timestamp.store(begin_timestamp, Ordering::SeqCst);
        self.update_byte_buffer();
    }

    pub fn get_end_timestamp(&self) -> i64 {
        self.end_timestamp.load(Ordering::SeqCst)
    }

    pub fn set_end_timestamp(&self, end_timestamp: i64) {
        self.end_timestamp.store(end_timestamp, Ordering::SeqCst);
        self.update_byte_buffer();
    }

    pub fn get_begin_phy_offset(&self) -> i64 {
        self.begin_phy_offset.load(Ordering::SeqCst)
    }

    pub fn set_begin_phy_offset(&self, begin_phy_offset: i64) {
        self.begin_phy_offset.store(begin_phy_offset, Ordering::SeqCst);
        self.update_byte_buffer();
    }

    pub fn get_end_phy_offset(&self) -> i64 {
        self.end_phy_offset.load(Ordering::SeqCst)
    }

    pub fn set_end_phy_offset(&self, end_phy_offset: i64) {
        self.end_phy_offset.store(end_phy_offset, Ordering::SeqCst);
        self.update_byte_buffer();
    }

    pub fn get_hash_slot_count(&self) -> i32 {
        self.hash_slot_count.load(Ordering::SeqCst)
    }

    pub fn inc_hash_slot_count(&self) {
        self.hash_slot_count.fetch_add(1, Ordering::SeqCst);
        self.update_byte_buffer();
    }

    pub fn get_index_count(&self) -> i32 {
        self.index_count.load(Ordering::SeqCst)
    }

    pub fn inc_index_count(&self) {
        self.index_count.fetch_add(1, Ordering::SeqCst);
        self.update_byte_buffer();
    }
}
