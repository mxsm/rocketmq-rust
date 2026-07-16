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

use rocketmq_store_local::index::codec::hash_slot_position;
use rocketmq_store_local::index::codec::index_entry_position;
use rocketmq_store_local::index::codec::index_file_total_size;
use rocketmq_store_local::index::codec::IndexEntry;
use rocketmq_store_local::index::codec::IndexHeaderRecord;
use rocketmq_store_local::index::codec::IndexLayoutError;
use rocketmq_store_local::index::codec::IndexSlot;
use rocketmq_store_local::index::codec::INDEX_ENTRY_SIZE;
use rocketmq_store_local::index::codec::INDEX_HASH_SLOT_SIZE;
use rocketmq_store_local::index::codec::INDEX_HEADER_SIZE;

#[test]
fn header_encoding_matches_the_java_40_byte_golden() {
    let record = IndexHeaderRecord {
        begin_timestamp: 0x0102_0304_0506_0708,
        end_timestamp: 0x1112_1314_1516_1718,
        begin_phy_offset: 0x2122_2324_2526_2728,
        end_phy_offset: 0x3132_3334_3536_3738,
        hash_slot_count: 0x4142_4344,
        index_count: 0x5152_5354,
    };

    assert_eq!(INDEX_HEADER_SIZE, 40);
    assert_eq!(
        record.encode(),
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x21, 0x22,
            0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x41, 0x42, 0x43, 0x44,
            0x51, 0x52, 0x53, 0x54,
        ]
    );
    assert_eq!(IndexHeaderRecord::decode(&record.encode()), Some(record));
    assert_eq!(IndexHeaderRecord::decode(&record.encode()[..39]), None);
}

#[test]
fn header_decode_preserves_the_legacy_minimum_index_count() {
    let mut bytes = IndexHeaderRecord::default().encode();
    assert_eq!(IndexHeaderRecord::decode(&bytes).unwrap().index_count, 1);

    bytes[36..40].copy_from_slice(&(-7_i32).to_be_bytes());
    assert_eq!(IndexHeaderRecord::decode(&bytes).unwrap().index_count, 1);
}

#[test]
fn slot_and_entry_codecs_preserve_signed_values_and_bounds() {
    let slot = IndexSlot(-7);
    assert_eq!(INDEX_HASH_SLOT_SIZE, 4);
    assert_eq!(slot.encode(), [0xff, 0xff, 0xff, 0xf9]);
    assert_eq!(IndexSlot::decode(&slot.encode()), Some(slot));
    assert_eq!(IndexSlot::decode(&slot.encode()[..3]), None);

    let entry = IndexEntry::new(0x0102_0304, 0x1112_1314_1516_1718, -2, 0x2122_2324);
    assert_eq!(INDEX_ENTRY_SIZE, 20);
    assert_eq!(
        entry.encode(),
        [
            0x01, 0x02, 0x03, 0x04, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0xff, 0xff, 0xff, 0xfe, 0x21, 0x22,
            0x23, 0x24,
        ]
    );
    assert_eq!(IndexEntry::decode(&entry.encode()), Some(entry));
    assert_eq!(IndexEntry::decode(&entry.encode()[..19]), None);
}

#[test]
fn checked_layout_matches_the_persisted_sections() {
    assert_eq!(hash_slot_position(0), Some(40));
    assert_eq!(hash_slot_position(3), Some(52));
    assert_eq!(index_entry_position(5, 0), Some(60));
    assert_eq!(index_entry_position(5, 2), Some(100));
    assert_eq!(index_file_total_size(5, 3), Ok(120));
}

#[test]
fn checked_layout_rejects_zero_dimensions_and_overflow() {
    assert_eq!(index_file_total_size(0, 1), Err(IndexLayoutError::ZeroHashSlots));
    assert_eq!(index_file_total_size(1, 0), Err(IndexLayoutError::ZeroIndexEntries));
    assert_eq!(
        index_file_total_size(usize::MAX, 1),
        Err(IndexLayoutError::HashSlotSectionOverflow)
    );
    assert_eq!(
        index_file_total_size(1, usize::MAX),
        Err(IndexLayoutError::IndexEntrySectionOverflow)
    );
    assert_eq!(
        index_file_total_size(1, usize::MAX / INDEX_ENTRY_SIZE),
        Err(IndexLayoutError::TotalSizeOverflow)
    );
    assert_eq!(hash_slot_position(usize::MAX), None);
    assert_eq!(index_entry_position(usize::MAX, 1), None);
}
