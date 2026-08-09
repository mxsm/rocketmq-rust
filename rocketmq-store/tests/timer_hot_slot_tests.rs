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

use rocketmq_store_local::timer::slot_drain_file::SlotDrainFileBuilder;
use rocketmq_store_local::timer::slot_drain_file::SlotDrainLocator;
use tempfile::tempdir;

#[test]
fn ten_thousand_same_second_locators_are_read_once_with_64_and_192_budgets() {
    assert_linear_drain(10_000, 64);
    assert_linear_drain(10_000, 192);
}

#[test]
fn one_hundred_thousand_same_second_locators_remain_linear() {
    assert_linear_drain(100_000, 192);
}

#[test]
#[ignore = "manual million-entry capacity evidence before canary"]
fn one_million_same_second_locators_remain_linear() {
    assert_linear_drain(1_000_000, 192);
}

#[test]
#[ignore = "manual ten-million-entry capacity evidence before canary"]
fn ten_million_same_second_locators_remain_linear() {
    assert_linear_drain(10_000_000, 192);
}

fn assert_linear_drain(records: usize, batch_size: usize) {
    let directory = tempdir().unwrap();
    let path = directory.path().join("slot-drain");
    let mut builder = SlotDrainFileBuilder::create(&path, 1_000, 9).unwrap();
    for index in (0..records).rev() {
        builder
            .push_reverse(SlotDrainLocator {
                timer_log_position: index as i64 * 40,
                commit_log_offset: index as i64 * 100,
                size: 64,
                magic: if index.is_multiple_of(2) { 1 } else { 4 },
                queue_offset: index as i64,
                generation: 9,
            })
            .unwrap();
    }
    let drain = builder.finish().unwrap();
    let mut cursor = 0usize;
    let mut records_read = 0usize;
    while cursor < records {
        let batch = drain.read_batch(cursor, batch_size).unwrap();
        assert!(!batch.is_empty());
        assert!(batch.len() <= batch_size);
        for (offset, locator) in batch.iter().enumerate() {
            assert_eq!(locator.queue_offset, (cursor + offset) as i64);
        }
        cursor += batch.len();
        records_read += batch.len();
    }
    assert_eq!(records_read, records);
    drain.remove().unwrap();
}
