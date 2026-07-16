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

use rocketmq_store_local::mapped_file::queue_maintenance::mapped_file_queue_truncate_action;
use rocketmq_store_local::mapped_file::queue_maintenance::plan_mapped_file_queue_reset;
use rocketmq_store_local::mapped_file::queue_maintenance::MappedFileQueueResetLastFile;
use rocketmq_store_local::mapped_file::queue_maintenance::MappedFileQueueTruncateAction;

#[derive(Clone, Copy)]
struct FileState {
    file_from_offset: u64,
    file_size: u64,
}

#[test]
fn truncate_plan_retains_completed_segments_and_truncates_the_target() {
    assert_eq!(
        mapped_file_queue_truncate_action(1024, 1024, 0),
        MappedFileQueueTruncateAction::Retain
    );
    assert_eq!(
        mapped_file_queue_truncate_action(1536, 1024, 1024),
        MappedFileQueueTruncateAction::Truncate(512)
    );
    assert_eq!(
        mapped_file_queue_truncate_action(1536, 1024, 2048),
        MappedFileQueueTruncateAction::Remove
    );
}

#[test]
fn reset_plan_rejects_offsets_more_than_two_segments_behind() {
    let files = [FileState {
        file_from_offset: 4096,
        file_size: 1024,
    }];
    let last = MappedFileQueueResetLastFile::new(4096, 1024);

    assert_eq!(
        plan_mapped_file_queue_reset(
            2047,
            1024,
            Some(last),
            &files,
            |file| file.file_from_offset,
            |file| file.file_size,
        ),
        None
    );
}

#[test]
fn reset_plan_preserves_target_position_and_newest_to_oldest_removals() {
    let files = [
        FileState {
            file_from_offset: 0,
            file_size: 1024,
        },
        FileState {
            file_from_offset: 1024,
            file_size: 1024,
        },
        FileState {
            file_from_offset: 2048,
            file_size: 1024,
        },
    ];
    let last = MappedFileQueueResetLastFile::new(2048, 0);

    let plan = plan_mapped_file_queue_reset(
        1536,
        1024,
        Some(last),
        &files,
        |file| file.file_from_offset,
        |file| file.file_size,
    )
    .expect("reset within two segments");

    assert_eq!(plan.target(), Some((1, 512)));
    assert_eq!(plan.remove_indices(), &[2]);
}

#[test]
fn reset_plan_removes_every_file_when_offset_precedes_the_queue() {
    let files = [
        FileState {
            file_from_offset: 1024,
            file_size: 1024,
        },
        FileState {
            file_from_offset: 2048,
            file_size: 1024,
        },
        FileState {
            file_from_offset: 3072,
            file_size: 1024,
        },
    ];

    let plan = plan_mapped_file_queue_reset(
        -1,
        1024,
        None,
        &files,
        |file| file.file_from_offset,
        |file| file.file_size,
    )
    .expect("missing last-file snapshot does not reject reset");

    assert_eq!(plan.target(), None);
    assert_eq!(plan.remove_indices(), &[2, 1, 0]);
}
