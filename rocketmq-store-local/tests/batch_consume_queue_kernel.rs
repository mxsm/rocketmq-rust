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

use rocketmq_store_local::consume_queue::batch::*;
use rocketmq_store_local::consume_queue::single::ConsumeQueueTimeBoundary;

fn record(physical_offset: i64, store_time: i64, base_offset: i64, batch_size: i16) -> BatchConsumeQueueRecord {
    BatchConsumeQueueRecord::new(
        physical_offset,
        32,
        7,
        store_time,
        base_offset,
        batch_size,
        INVALID_COMPACTED_OFFSET,
    )
}

#[test]
fn batch_record_round_trip_keeps_the_exact_46_byte_layout() {
    let record = record(0x0102_0304_0506_0708, 0x1112_1314_1516_1718, 9, 3);
    let encoded = record.encode();

    assert_eq!(encoded.len(), BATCH_CQ_STORE_UNIT_SIZE as usize);
    assert_eq!(&encoded[..8], &record.physical_offset.to_be_bytes());
    assert_eq!(&encoded[20..28], &record.store_time.to_be_bytes());
    assert_eq!(BatchConsumeQueueRecord::decode(&encoded), Some(record));
    assert_eq!(BatchConsumeQueueRecord::decode(&encoded[..45]), None);
}

#[test]
fn batch_record_validation_and_end_offsets_match_the_persisted_fields() {
    let valid = record(100, 1_000, 20, 4);
    let mut invalid = valid;
    invalid.message_base_offset = -1;

    assert!(valid.is_valid());
    assert!(!invalid.is_valid());
    assert_eq!(valid.physical_end_offset(), 132);
    assert_eq!(valid.queue_end_offset(), 24);
}

#[test]
fn batch_record_search_returns_the_last_base_not_greater_than_the_target() {
    let files = [vec![record(100, 1_000, 0, 3), record(132, 2_000, 3, 2)]];
    let found = find_batch_record_in_files(
        files.len(),
        4,
        |file| files[file].len() as i32 * BATCH_CQ_STORE_UNIT_SIZE,
        |file, offset| files[file].get((offset / BATCH_CQ_STORE_UNIT_SIZE) as usize).copied(),
    );

    assert_eq!(found.map(|(_, value)| value.message_base_offset), Some(3));
}

#[test]
fn batch_recovery_and_truncate_select_the_same_valid_prefix() {
    let records = [record(100, 1_000, 0, 3), record(132, 2_000, 3, 2)];
    let read = |offset| records.get((offset / BATCH_CQ_STORE_UNIT_SIZE) as usize).copied();

    assert_eq!(
        scan_batch_recovery(3 * BATCH_CQ_STORE_UNIT_SIZE, read),
        BatchRecoveryScan {
            valid_bytes: 2 * BATCH_CQ_STORE_UNIT_SIZE,
            max_physical_offset: Some(164),
        }
    );
    assert_eq!(
        plan_batch_truncate(2 * BATCH_CQ_STORE_UNIT_SIZE, 132, read),
        BatchTruncatePlan::Retain {
            valid_bytes: BATCH_CQ_STORE_UNIT_SIZE,
            max_physical_offset: Some(132),
        }
    );
    assert_eq!(
        plan_batch_truncate(2 * BATCH_CQ_STORE_UNIT_SIZE, 100, read),
        BatchTruncatePlan::DeleteFile
    );
    assert_eq!(
        plan_batch_truncate(BATCH_CQ_STORE_UNIT_SIZE + 1, i64::MAX, read),
        BatchTruncatePlan::RetryFile
    );
}

#[test]
fn batch_minimum_offset_correction_advances_over_expired_batches() {
    let files = [vec![record(100, 1_000, 0, 3), record(200, 2_000, 3, 3)]];
    let selected = correct_batch_min_offsets(
        files.len(),
        150,
        0,
        0,
        |file| files[file].len() as i32 * BATCH_CQ_STORE_UNIT_SIZE,
        |_| 1_000,
        |file, offset| files[file].get((offset / BATCH_CQ_STORE_UNIT_SIZE) as usize).copied(),
    );

    assert_eq!(selected.queue_offset, 3);
    assert_eq!(selected.logic_offset, 1_000 + i64::from(BATCH_CQ_STORE_UNIT_SIZE));
}

#[test]
fn batch_timestamp_search_preserves_lower_and_upper_boundaries() {
    let files = [vec![record(100, 1_000, 0, 3), record(132, 2_000, 3, 2)]];
    let search = |boundary| {
        search_batch_offset_by_time(
            files.len(),
            1_500,
            boundary,
            |file| files[file].len() as i32 * BATCH_CQ_STORE_UNIT_SIZE,
            |file, offset| files[file].get((offset / BATCH_CQ_STORE_UNIT_SIZE) as usize).copied(),
        )
    };

    assert_eq!(search(ConsumeQueueTimeBoundary::Lower), 3);
    assert_eq!(search(ConsumeQueueTimeBoundary::Upper), 3);
}
