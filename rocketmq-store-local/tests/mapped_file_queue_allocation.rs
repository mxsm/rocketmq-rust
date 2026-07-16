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

use rocketmq_store_local::mapped_file::queue_allocation::plan_mapped_file_queue_creation;
use rocketmq_store_local::mapped_file::queue_allocation::plan_mapped_file_queue_preallocation;
use rocketmq_store_local::mapped_file::queue_allocation::MappedFileQueueLastFile;
use rocketmq_store_local::mapped_file::queue_allocation::MappedFileQueueRollFile;

#[test]
fn allocation_plan_aligns_an_empty_queue_to_the_segment_start() {
    assert_eq!(plan_mapped_file_queue_creation(250, 100, None, true), Some(200));
}

#[test]
fn allocation_plan_preallocates_at_eighty_percent_without_rolling() {
    let below = MappedFileQueueLastFile::new(100, 79, false);
    let threshold = MappedFileQueueLastFile::new(100, 80, false);

    assert_eq!(plan_mapped_file_queue_preallocation(100, below), None);
    assert_eq!(plan_mapped_file_queue_preallocation(100, threshold), Some(200));
}

#[test]
fn allocation_plan_rolls_a_full_segment_without_preallocation() {
    let full = MappedFileQueueLastFile::new(100, 100, true);
    let roll = MappedFileQueueRollFile::new(100, true);

    assert_eq!(plan_mapped_file_queue_preallocation(100, full), None);
    assert_eq!(plan_mapped_file_queue_creation(0, 100, Some(roll), true), Some(200));
}

#[test]
fn allocation_plan_keeps_preallocation_when_creation_is_disabled() {
    let active = MappedFileQueueLastFile::new(100, 90, false);
    let full = MappedFileQueueRollFile::new(100, true);

    assert_eq!(plan_mapped_file_queue_preallocation(100, active), Some(200));
    assert_eq!(plan_mapped_file_queue_creation(0, 100, Some(full), false), None);
}

#[test]
#[should_panic]
fn allocation_plan_preserves_zero_segment_size_failure_for_an_empty_queue() {
    let _ = plan_mapped_file_queue_creation(0, 0, None, true);
}
