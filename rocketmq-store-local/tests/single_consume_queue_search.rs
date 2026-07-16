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

use rocketmq_store_local::consume_queue::record::ConsumeQueueRecord;
use rocketmq_store_local::consume_queue::single::find_min_offset_record;
use rocketmq_store_local::consume_queue::single::plan_truncate_records;
use rocketmq_store_local::consume_queue::single::scan_recovery_records;
use rocketmq_store_local::consume_queue::single::search_queue_offset_by_time;
use rocketmq_store_local::consume_queue::single::ConsumeQueueTimeBoundary;
use rocketmq_store_local::consume_queue::single::ConsumeQueueTruncatePlan;

fn records() -> Vec<u8> {
    [
        ConsumeQueueRecord::new(100, 1, 0),
        ConsumeQueueRecord::new(200, 1, 0),
        ConsumeQueueRecord::new(300, 1, 0),
    ]
    .into_iter()
    .flat_map(ConsumeQueueRecord::encode)
    .collect()
}

fn timestamp(physical_offset: i64, _size: i32) -> i64 {
    match physical_offset {
        100 | 200 => 1_000,
        300 => 3_000,
        _ => -1,
    }
}

#[test]
fn exact_duplicate_timestamp_honors_lower_and_upper_boundaries() {
    let records = records();
    let search = |boundary| search_queue_offset_by_time(&records, 0, 0, 0, 1_000, boundary, timestamp, |_| {});

    assert_eq!(search(ConsumeQueueTimeBoundary::Lower), 0);
    assert_eq!(search(ConsumeQueueTimeBoundary::Upper), 1);
}

#[test]
fn missing_timestamp_returns_the_enclosing_queue_boundaries() {
    let records = records();

    assert_eq!(
        search_queue_offset_by_time(
            &records,
            0,
            0,
            0,
            2_000,
            ConsumeQueueTimeBoundary::Lower,
            timestamp,
            |_| {},
        ),
        2
    );
    assert_eq!(
        search_queue_offset_by_time(
            &records,
            0,
            0,
            0,
            2_000,
            ConsumeQueueTimeBoundary::Upper,
            timestamp,
            |_| {},
        ),
        1
    );
}

#[test]
fn timestamps_outside_the_file_keep_legacy_edge_offsets() {
    let records = records();

    assert_eq!(
        search_queue_offset_by_time(
            &records,
            0,
            0,
            0,
            500,
            ConsumeQueueTimeBoundary::Lower,
            timestamp,
            |_| {},
        ),
        0
    );
    assert_eq!(
        search_queue_offset_by_time(
            &records,
            0,
            0,
            0,
            4_000,
            ConsumeQueueTimeBoundary::Lower,
            timestamp,
            |_| {},
        ),
        3
    );
    assert_eq!(
        search_queue_offset_by_time(
            &records,
            0,
            0,
            0,
            4_000,
            ConsumeQueueTimeBoundary::Upper,
            timestamp,
            |_| {},
        ),
        2
    );
}

#[test]
fn failed_midpoint_lookup_reports_the_same_physical_offset() {
    let records = records();
    let mut failed_offset = None;

    let result = search_queue_offset_by_time(
        &records,
        0,
        0,
        0,
        2_000,
        ConsumeQueueTimeBoundary::Lower,
        |physical_offset, _| match physical_offset {
            100 => 1_000,
            200 => -1,
            300 => 3_000,
            _ => -1,
        },
        |physical_offset| failed_offset = Some(physical_offset),
    );

    assert_eq!(result, 0);
    assert_eq!(failed_offset, Some(200));
}

#[test]
fn empty_short_and_min_logic_excluded_inputs_fail_closed() {
    let records = records();

    assert_eq!(
        search_queue_offset_by_time(
            &records[..19],
            0,
            0,
            0,
            1_000,
            ConsumeQueueTimeBoundary::Lower,
            timestamp,
            |_| {},
        ),
        0
    );
    assert_eq!(
        search_queue_offset_by_time(
            &records,
            0,
            100,
            0,
            1_000,
            ConsumeQueueTimeBoundary::Lower,
            timestamp,
            |_| {},
        ),
        0
    );
}

#[test]
fn recovery_scan_returns_the_complete_written_prefix_and_last_progress() {
    let records = [
        ConsumeQueueRecord::pre_blank(),
        ConsumeQueueRecord::new(100, 20, -7),
        ConsumeQueueRecord::new(120, 10, 9),
    ];

    let scan = scan_recovery_records(
        60,
        |relative_offset| records.get((relative_offset / 20) as usize).copied(),
        |tags_code| tags_code < 0,
    );

    assert!(scan.fills_file(60));
    assert_eq!(scan.valid_bytes, 60);
    assert_eq!(scan.max_physical_offset, Some(130));
    assert_eq!(scan.max_extension_address, Some(-7));
    assert_eq!(scan.stopped_record, None);
}

#[test]
fn recovery_scan_stops_before_invalid_or_missing_records() {
    let records = [ConsumeQueueRecord::new(100, 20, 1), ConsumeQueueRecord::new(-1, 0, 2)];
    let invalid = scan_recovery_records(
        60,
        |relative_offset| records.get((relative_offset / 20) as usize).copied(),
        |_| false,
    );
    let missing = scan_recovery_records(
        40,
        |relative_offset| records.first().copied().filter(|_| relative_offset == 0),
        |_| false,
    );

    assert_eq!(invalid.valid_bytes, 20);
    assert_eq!(invalid.max_physical_offset, Some(120));
    assert_eq!(invalid.stopped_record, Some(records[1]));
    assert_eq!(missing.valid_bytes, 20);
    assert_eq!(missing.stopped_record, None);
}

#[test]
fn min_offset_scan_selects_the_first_written_record_at_the_boundary() {
    let records = [
        ConsumeQueueRecord::new(100, 20, 1),
        ConsumeQueueRecord::new(120, 20, -9),
        ConsumeQueueRecord::new(140, 20, 3),
    ];

    let selected = find_min_offset_record(60, 120, |relative_offset| {
        records.get((relative_offset / 20) as usize).copied()
    });

    assert_eq!(selected.map(|value| value.relative_offset), Some(20));
    assert_eq!(selected.map(|value| value.record), Some(records[1]));
    assert_eq!(find_min_offset_record(19, 0, |_| Some(records[0])), None);
}

#[test]
fn truncate_plan_deletes_from_the_first_boundary_or_retains_the_prefix() {
    let records = [
        ConsumeQueueRecord::new(100, 20, 1),
        ConsumeQueueRecord::new(120, 20, -9),
        ConsumeQueueRecord::new(140, 20, 3),
    ];
    let read = |relative_offset| records.get((relative_offset / 20) as usize).copied();

    assert_eq!(
        plan_truncate_records(60, 100, read, |tags_code| tags_code < 0),
        ConsumeQueueTruncatePlan::DeleteFile
    );
    assert_eq!(
        plan_truncate_records(60, 130, read, |tags_code| tags_code < 0),
        ConsumeQueueTruncatePlan::Retain {
            valid_bytes: 40,
            max_physical_offset: Some(140),
            max_extension_address: Some(-9),
        }
    );
}

#[test]
fn truncate_plan_preserves_the_first_record_special_case() {
    let records = [ConsumeQueueRecord::new(-1, 0, 7), ConsumeQueueRecord::new(-1, 0, 8)];

    assert_eq!(
        plan_truncate_records(
            40,
            100,
            |relative_offset| records.get((relative_offset / 20) as usize).copied(),
            |_| false,
        ),
        ConsumeQueueTruncatePlan::Retain {
            valid_bytes: 20,
            max_physical_offset: Some(-1),
            max_extension_address: None,
        }
    );
    assert_eq!(
        plan_truncate_records(20, 100, |_| None, |_| false),
        ConsumeQueueTruncatePlan::RetryFile
    );
}
