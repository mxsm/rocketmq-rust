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

use bytes::Bytes;
use rocketmq_store_local::commit_log::record::is_blank_message;
use rocketmq_store_local::commit_log::record::CommitLogFrameCursor;
use rocketmq_store_local::commit_log::record::CommitLogFrameSource;
use rocketmq_store_local::commit_log::record::BLANK_MAGIC_CODE;
use rocketmq_store_local::commit_log::record::MESSAGE_MAGIC_CODE;

const PARSE_BATCH_SIZE: usize = 64 * 1024;

#[derive(Clone)]
struct BytesSource {
    bytes: Bytes,
}

impl BytesSource {
    fn new(bytes: impl Into<Bytes>) -> Self {
        Self { bytes: bytes.into() }
    }
}

impl CommitLogFrameSource for BytesSource {
    fn source_len(&self) -> usize {
        self.bytes.len()
    }

    fn read(&self, offset: usize, len: usize) -> Option<Bytes> {
        let end = offset.checked_add(len)?;
        self.bytes.get(offset..end).map(Bytes::copy_from_slice)
    }
}

fn frame(size: usize, magic: i32) -> Bytes {
    assert!(size >= 8);
    let mut bytes = vec![0xA5; size];
    bytes[..4].copy_from_slice(&(size as i32).to_be_bytes());
    bytes[4..8].copy_from_slice(&magic.to_be_bytes());
    Bytes::from(bytes)
}

fn collect(source: BytesSource) -> (Vec<(Bytes, usize, usize)>, usize) {
    let mut cursor = CommitLogFrameCursor::new(source);
    let mut frames = Vec::new();
    while let Some(next) = cursor.next_message() {
        frames.push(next);
    }
    (frames, cursor.current_offset())
}

#[test]
fn empty_and_short_sources_stop_without_advancing() {
    for bytes in [Bytes::new(), Bytes::from_static(&[0; 7])] {
        let (frames, offset) = collect(BytesSource::new(bytes));
        assert!(frames.is_empty());
        assert_eq!(offset, 0);
    }
}

#[test]
fn one_and_multiple_frames_preserve_bytes_sizes_and_absolute_offsets() {
    let first = frame(8, MESSAGE_MAGIC_CODE);
    let second = frame(17, MESSAGE_MAGIC_CODE);
    let source = BytesSource::new(Bytes::from([first.as_ref(), second.as_ref()].concat()));

    let (actual, offset) = collect(source);

    assert_eq!(actual, vec![(first, 0, 8), (second, 8, 17)]);
    assert_eq!(offset, 25);
}

#[test]
fn non_positive_frame_size_stops_at_the_marker() {
    for declared_size in [0_i32, -1_i32] {
        let mut bytes = vec![0_u8; 8];
        bytes[..4].copy_from_slice(&declared_size.to_be_bytes());
        let (frames, offset) = collect(BytesSource::new(Bytes::from(bytes)));
        assert!(frames.is_empty());
        assert_eq!(offset, 0);
    }
}

#[test]
fn declared_frame_beyond_dirty_tail_stops_after_last_complete_frame() {
    let complete = frame(12, MESSAGE_MAGIC_CODE);
    let mut partial = frame(24, MESSAGE_MAGIC_CODE).to_vec();
    partial.truncate(11);
    let source = BytesSource::new(Bytes::from([complete.as_ref(), partial.as_slice()].concat()));

    let (actual, offset) = collect(source);

    assert_eq!(actual, vec![(complete, 0, 12)]);
    assert_eq!(offset, 12);
}

#[test]
fn exact_64k_oversized_and_cross_batch_frames_are_returned_intact() {
    let exact = frame(PARSE_BATCH_SIZE, MESSAGE_MAGIC_CODE);
    let oversized = frame(PARSE_BATCH_SIZE + 1, MESSAGE_MAGIC_CODE);
    let prefix = frame(PARSE_BATCH_SIZE - 16, MESSAGE_MAGIC_CODE);
    let crossing = frame(32, MESSAGE_MAGIC_CODE);

    let (exact_frames, exact_offset) = collect(BytesSource::new(exact.clone()));
    assert_eq!(exact_frames, vec![(exact, 0, PARSE_BATCH_SIZE)]);
    assert_eq!(exact_offset, PARSE_BATCH_SIZE);

    let (oversized_frames, oversized_offset) = collect(BytesSource::new(oversized.clone()));
    assert_eq!(oversized_frames, vec![(oversized, 0, PARSE_BATCH_SIZE + 1)]);
    assert_eq!(oversized_offset, PARSE_BATCH_SIZE + 1);

    let (crossing_frames, crossing_offset) = collect(BytesSource::new(Bytes::from(
        [prefix.as_ref(), crossing.as_ref()].concat(),
    )));
    assert_eq!(
        crossing_frames,
        vec![
            (prefix, 0, PARSE_BATCH_SIZE - 16),
            (crossing, PARSE_BATCH_SIZE - 16, 32),
        ]
    );
    assert_eq!(crossing_offset, PARSE_BATCH_SIZE + 16);
}

#[test]
fn blank_marker_recognition_is_bounded_and_exact() {
    let blank = frame(8, BLANK_MAGIC_CODE);
    let message = frame(8, MESSAGE_MAGIC_CODE);

    assert!(is_blank_message(&blank));
    assert!(!is_blank_message(&Bytes::from_static(&[0; 7])));
    assert!(!is_blank_message(&message));
}

#[test]
fn partial_tail_does_not_advance_past_the_last_complete_frame() {
    let complete = frame(16, MESSAGE_MAGIC_CODE);
    let source = BytesSource::new(Bytes::from([complete.as_ref(), &[1, 2, 3, 4, 5][..]].concat()));

    let (actual, offset) = collect(source);

    assert_eq!(actual, vec![(complete, 0, 16)]);
    assert_eq!(offset, 16);
}
