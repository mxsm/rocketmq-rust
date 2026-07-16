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

use rocketmq_store_local::mapped_file::queue_index::file_index_by_offset;
use rocketmq_store_local::mapped_file::queue_index::file_index_by_timestamp;
use rocketmq_store_local::mapped_file::queue_index::for_each_discontinuous_pair;
use rocketmq_store_local::mapped_file::queue_index::overlapping_file_range;
use rocketmq_store_local::mapped_file::queue_index::MappedFileQueueIndex;

#[derive(Clone, Copy)]
struct FileView {
    offset: u64,
    size: u64,
    modified: i64,
}

fn files() -> [FileView; 3] {
    [
        FileView {
            offset: 100,
            size: 100,
            modified: 10,
        },
        FileView {
            offset: 200,
            size: 100,
            modified: 20,
        },
        FileView {
            offset: 300,
            size: 100,
            modified: 30,
        },
    ]
}

#[test]
fn queue_index_reports_every_discontinuous_pair() {
    let files = [0_u64, 100, 250, 350, 500];
    let mut pairs = Vec::new();

    for_each_discontinuous_pair(
        &files,
        100,
        |offset| *offset,
        |previous, current| {
            pairs.push((previous, current));
        },
    );

    assert_eq!(pairs, [(1, 2), (3, 4)]);
}

#[test]
fn queue_index_selects_overlapping_window() {
    let files = files();

    assert_eq!(
        overlapping_file_range(&files, 150, 350, |file| {
            (file.offset as i64, (file.offset + file.size) as i64)
        }),
        0..3
    );
    assert_eq!(
        overlapping_file_range(&files, 400, 500, |file| {
            (file.offset as i64, (file.offset + file.size) as i64)
        }),
        0..0
    );
}

#[test]
fn queue_index_selects_timestamp_or_last_file() {
    let files = files();

    assert_eq!(file_index_by_timestamp(&files, 11, |file| file.modified), Some(1));
    assert_eq!(file_index_by_timestamp(&files, 31, |file| file.modified), Some(2));
    assert_eq!(file_index_by_timestamp::<FileView>(&[], 0, |file| file.modified), None);
}

#[test]
fn queue_index_finds_offset_with_fallback_and_first_policy() {
    let files = files();
    let gapped = [files[0], files[2]];

    assert_eq!(
        file_index_by_offset(&files, 100, 250, false, 100, 300, |file| file.offset),
        Some(MappedFileQueueIndex::Indexed(1))
    );
    assert_eq!(
        file_index_by_offset(&gapped, 100, 350, false, 100, 300, |file| file.offset),
        Some(MappedFileQueueIndex::Indexed(1))
    );
    assert_eq!(
        file_index_by_offset(&gapped, 100, 250, false, 100, 300, |file| file.offset),
        None
    );
    assert_eq!(
        file_index_by_offset(&files, 100, 99, true, 100, 300, |file| file.offset),
        Some(MappedFileQueueIndex::First)
    );
    assert_eq!(
        file_index_by_offset(&files, 100, 400, false, 100, 300, |file| file.offset),
        None
    );
}
