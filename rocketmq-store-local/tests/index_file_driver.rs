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

use std::cell::RefCell;

use rocketmq_store_local::index::codec::index_entry_position;
use rocketmq_store_local::index::codec::index_file_total_size;
use rocketmq_store_local::index::codec::IndexEntry;
use rocketmq_store_local::index::codec::IndexSlot;
use rocketmq_store_local::index::file::drive_index_put;
use rocketmq_store_local::index::file::is_index_time_matched;
use rocketmq_store_local::index::file::normalize_index_key_hash;
use rocketmq_store_local::index::file::query_index_offsets;
use rocketmq_store_local::index::file::IndexFileSnapshot;
use rocketmq_store_local::index::file::IndexHeaderUpdate;
use rocketmq_store_local::index::file::IndexPutOutcome;
use rocketmq_store_local::index::file::IndexQueryOutcome;

#[test]
fn put_driver_writes_entry_and_slot_before_the_legacy_header_sequence() {
    let snapshot = IndexFileSnapshot::new(2, 8, 1, 0);
    let storage = RefCell::new(vec![0_u8; index_file_total_size(2, 8).unwrap()]);
    let updates = RefCell::new(Vec::new());

    let outcome = drive_index_put(
        snapshot,
        3,
        42,
        1_000_000,
        |position| read_array::<4>(&storage.borrow(), position),
        |position, bytes| write_bytes(&mut storage.borrow_mut(), position, bytes),
        |update| updates.borrow_mut().push(update),
    );

    assert_eq!(outcome, IndexPutOutcome::Written);
    assert_eq!(IndexSlot::decode(&storage.borrow()[44..48]), Some(IndexSlot(1)));
    let entry_position = index_entry_position(2, 1).unwrap();
    assert_eq!(
        IndexEntry::decode(&storage.borrow()[entry_position..entry_position + 20]),
        Some(IndexEntry::new(3, 42, 0, 0))
    );
    assert_eq!(
        *updates.borrow(),
        [
            IndexHeaderUpdate::SetBeginPhyOffset(42),
            IndexHeaderUpdate::SetBeginTimestamp(1_000_000),
            IndexHeaderUpdate::IncrementHashSlotCount,
            IndexHeaderUpdate::IncrementIndexCount,
            IndexHeaderUpdate::SetEndPhyOffset(42),
            IndexHeaderUpdate::SetEndTimestamp(1_000_000),
        ]
    );
}

#[test]
fn put_driver_normalizes_chain_head_and_clamps_time_diff() {
    let snapshot = IndexFileSnapshot::new(1, 8, 3, 1_000_000);
    let storage = RefCell::new(vec![0_u8; index_file_total_size(1, 8).unwrap()]);
    write_bytes(&mut storage.borrow_mut(), 40, &IndexSlot(99).encode());
    let updates = RefCell::new(Vec::new());

    let outcome = drive_index_put(
        snapshot,
        7,
        91,
        i64::MAX,
        |position| read_array::<4>(&storage.borrow(), position),
        |position, bytes| write_bytes(&mut storage.borrow_mut(), position, bytes),
        |update| updates.borrow_mut().push(update),
    );

    assert_eq!(outcome, IndexPutOutcome::Written);
    let entry_position = index_entry_position(1, 3).unwrap();
    assert_eq!(
        IndexEntry::decode(&storage.borrow()[entry_position..entry_position + 20]),
        Some(IndexEntry::new(7, 91, i32::MAX, 0))
    );
    assert_eq!(updates.borrow()[0], IndexHeaderUpdate::IncrementHashSlotCount);
}

#[test]
fn put_driver_stops_before_io_when_full_or_invalid() {
    let mut writes = 0;
    let outcome = drive_index_put(
        IndexFileSnapshot::new(1, 3, 3, 1),
        1,
        2,
        3,
        |_| panic!("full file must not read a slot"),
        |_, _| writes += 1,
        |_| panic!("full file must not update the header"),
    );
    assert_eq!(outcome, IndexPutOutcome::Full);
    assert_eq!(writes, 0);

    let outcome = drive_index_put(
        IndexFileSnapshot::new(0, 3, 1, 1),
        1,
        2,
        3,
        |_| None,
        |_, _| writes += 1,
        |_| {},
    );
    assert_eq!(outcome, IndexPutOutcome::LayoutOverflow);
    assert_eq!(writes, 0);
}

#[test]
fn query_driver_walks_collision_chain_and_honors_time_and_result_limits() {
    let hash_slot_num = 1;
    let storage = RefCell::new(vec![0_u8; index_file_total_size(hash_slot_num, 8).unwrap()]);
    write_bytes(&mut storage.borrow_mut(), 40, &IndexSlot(3).encode());
    write_entry(&storage, hash_slot_num, 1, IndexEntry::new(7, 100, 0, 0));
    write_entry(&storage, hash_slot_num, 2, IndexEntry::new(8, 200, 1, 1));
    write_entry(&storage, hash_slot_num, 3, IndexEntry::new(7, 300, 2, 2));
    let snapshot = IndexFileSnapshot::new(hash_slot_num, 8, 4, 1_000_000);

    let mut offsets = Vec::new();
    let outcome = query_index_offsets(
        snapshot,
        7,
        10,
        999_000,
        1_003_000,
        &mut offsets,
        |position| read_array::<4>(&storage.borrow(), position),
        |position| read_array::<20>(&storage.borrow(), position),
    );
    assert_eq!(outcome, IndexQueryOutcome::Completed);
    assert_eq!(offsets, [300, 100]);

    let mut limited = vec![999];
    let outcome = query_index_offsets(
        snapshot,
        7,
        2,
        999_000,
        1_003_000,
        &mut limited,
        |position| read_array::<4>(&storage.borrow(), position),
        |position| read_array::<20>(&storage.borrow(), position),
    );
    assert_eq!(outcome, IndexQueryOutcome::Completed);
    assert_eq!(limited, [999, 300]);
}

#[test]
fn query_driver_reports_unavailable_storage_without_losing_prior_results() {
    let storage = RefCell::new(vec![0_u8; index_file_total_size(1, 4).unwrap()]);
    write_bytes(&mut storage.borrow_mut(), 40, &IndexSlot(2).encode());
    write_entry(&storage, 1, 2, IndexEntry::new(7, 200, 0, 1));
    let mut offsets = Vec::new();

    let outcome = query_index_offsets(
        IndexFileSnapshot::new(1, 4, 3, 1_000),
        7,
        10,
        0,
        10_000,
        &mut offsets,
        |position| read_array::<4>(&storage.borrow(), position),
        |position| {
            (position == index_entry_position(1, 2).unwrap()).then(|| read_array::<20>(&storage.borrow(), position))?
        },
    );

    assert_eq!(outcome, IndexQueryOutcome::EntryUnavailable);
    assert_eq!(offsets, [200]);
}

#[test]
fn hash_and_time_helpers_preserve_legacy_edge_semantics() {
    assert_eq!(normalize_index_key_hash(i32::MIN), 0);
    assert_eq!(normalize_index_key_hash(-7), 7);
    assert_eq!(normalize_index_key_hash(7), 7);

    assert!(is_index_time_matched(10, 20, 0, 30));
    assert!(is_index_time_matched(10, 20, 15, 30));
    assert!(is_index_time_matched(10, 20, 0, 15));
    assert!(!is_index_time_matched(10, 20, 0, 9));
    assert!(!is_index_time_matched(10, 20, 21, 30));
}

fn read_array<const N: usize>(storage: &[u8], position: usize) -> Option<[u8; N]> {
    storage.get(position..position.checked_add(N)?)?.try_into().ok()
}

fn write_bytes(storage: &mut [u8], position: usize, bytes: &[u8]) {
    storage[position..position + bytes.len()].copy_from_slice(bytes);
}

fn write_entry(storage: &RefCell<Vec<u8>>, hash_slot_num: usize, index: usize, entry: IndexEntry) {
    let position = index_entry_position(hash_slot_num, index).unwrap();
    write_bytes(&mut storage.borrow_mut(), position, &entry.encode());
}
