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

use rocketmq_store_local::flush::queue::commit_mapped_file_queue;
use rocketmq_store_local::flush::queue::try_flush_mapped_file_queue;
use rocketmq_store_local::flush::queue::SegmentCommitProgress;
use rocketmq_store_local::flush::queue::SegmentFlushProgress;

#[test]
fn empty_flush_preserves_watermarks_and_timestamp() {
    let progress = try_flush_mapped_file_queue::<()>(128, 64, 7, 0, |offset, first| {
        assert_eq!(offset, 64);
        assert!(!first);
        Ok(None)
    })
    .unwrap();

    assert_eq!(progress.appended, 128);
    assert_eq!(progress.durable_before, 64);
    assert_eq!(progress.durable, 64);
    assert_eq!(progress.store_timestamp, 7);
}

#[test]
fn thorough_flush_advances_watermark_and_timestamp() {
    let progress = try_flush_mapped_file_queue::<()>(128, 0, 7, 0, |offset, first| {
        assert_eq!(offset, 0);
        assert!(first);
        Ok(Some(SegmentFlushProgress::new(64, 32, 11)))
    })
    .unwrap();

    assert_eq!(progress.durable, 96);
    assert_eq!(progress.store_timestamp, 11);
}

#[test]
fn partial_flush_preserves_previous_timestamp() {
    let progress =
        try_flush_mapped_file_queue::<()>(128, 64, 7, 4, |_, _| Ok(Some(SegmentFlushProgress::new(64, 32, 11))))
            .unwrap();

    assert_eq!(progress.durable, 96);
    assert_eq!(progress.store_timestamp, 7);
}

#[test]
fn flush_error_is_returned_without_synthesizing_progress() {
    let error = try_flush_mapped_file_queue(128, 64, 7, 0, |_, _| {
        Err::<Option<SegmentFlushProgress>, _>("injected I/O failure")
    })
    .unwrap_err();

    assert_eq!(error, "injected I/O failure");
}

#[test]
fn commit_preserves_legacy_no_progress_boolean() {
    let advanced = commit_mapped_file_queue(64, |offset, first| {
        assert_eq!(offset, 64);
        assert!(!first);
        Some(SegmentCommitProgress::new(64, 32))
    });
    let unchanged = commit_mapped_file_queue(96, |_, _| None);

    assert_eq!(advanced.committed(), 96);
    assert!(!advanced.legacy_commit_result());
    assert_eq!(unchanged.committed(), 96);
    assert!(unchanged.legacy_commit_result());
}
