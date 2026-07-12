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
use rocketmq_store::consume_queue::mapped_file_queue::MappedFileQueue;
use rocketmq_store::log_file::commit_log;
use rocketmq_store::log_file::commit_log_recovery;
use rocketmq_store::log_file::commit_log_recovery::BatchMessageIterator;
use rocketmq_store::log_file::mapped_file::MappedFile;
use rocketmq_store_local::commit_log::record;
use tempfile::TempDir;

const PARSE_BATCH_SIZE: usize = 64 * 1024;

fn frame(size: usize, magic: i32) -> Bytes {
    assert!(size >= 8);
    let mut bytes = vec![0x5A; size];
    bytes[..4].copy_from_slice(&(size as i32).to_be_bytes());
    bytes[4..8].copy_from_slice(&magic.to_be_bytes());
    Bytes::from(bytes)
}

fn with_iterator(bytes: &[u8], check: impl FnOnce(&mut BatchMessageIterator<'_>)) {
    let temp_dir = TempDir::new().expect("create temp directory");
    let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), bytes.len() as u64, None);
    let mapped_file = queue
        .get_last_mapped_file_mut_start_offset(0, true)
        .expect("create mapped file");
    assert!(mapped_file.append_message_bytes(bytes));
    let mut iterator = BatchMessageIterator::new(&mapped_file);
    check(&mut iterator);
}

#[test]
fn legacy_wrapper_preserves_oversized_frame_and_dirty_tail_offsets() {
    let oversized = frame(PARSE_BATCH_SIZE + 17, record::MESSAGE_MAGIC_CODE);
    let mut dirty_tail = frame(32, record::MESSAGE_MAGIC_CODE).to_vec();
    dirty_tail.truncate(12);
    let bytes = [oversized.as_ref(), dirty_tail.as_slice()].concat();
    with_iterator(&bytes, |iterator| {
        assert_eq!(iterator.next_message(), Some((oversized, 0, PARSE_BATCH_SIZE + 17)));
        assert_eq!(iterator.next_message(), None);
        assert_eq!(iterator.current_offset(), PARSE_BATCH_SIZE + 17);
    });
}

#[test]
fn legacy_wrapper_preserves_cross_batch_refill_behavior() {
    let prefix = frame(PARSE_BATCH_SIZE - 16, record::MESSAGE_MAGIC_CODE);
    let crossing = frame(48, record::MESSAGE_MAGIC_CODE);
    let bytes = [prefix.as_ref(), crossing.as_ref()].concat();
    with_iterator(&bytes, |iterator| {
        assert_eq!(iterator.next_message(), Some((prefix, 0, PARSE_BATCH_SIZE - 16)));
        assert_eq!(iterator.next_message(), Some((crossing, PARSE_BATCH_SIZE - 16, 48)));
        assert_eq!(iterator.next_message(), None);
        assert_eq!(iterator.current_offset(), PARSE_BATCH_SIZE + 32);
    });
}

#[test]
fn legacy_constant_and_blank_paths_preserve_identity_and_signatures() {
    let legacy_blank: fn(&Bytes) -> bool = commit_log_recovery::is_blank_message;
    let canonical_blank: fn(&Bytes) -> bool = record::is_blank_message;
    let blank = frame(8, record::BLANK_MAGIC_CODE);

    assert_eq!(commit_log::MESSAGE_MAGIC_CODE, record::MESSAGE_MAGIC_CODE);
    assert_eq!(commit_log::BLANK_MAGIC_CODE, record::BLANK_MAGIC_CODE);
    assert_eq!(
        record::MESSAGE_MAGIC_CODE,
        rocketmq_common::common::message::MESSAGE_MAGIC_CODE_V1
    );
    assert_eq!(
        record::MESSAGE_MAGIC_CODE,
        rocketmq_remoting::protocol::body::message_codec::MESSAGE_MAGIC_CODE
    );
    assert_eq!(
        record::MESSAGE_MAGIC_CODE_V2,
        rocketmq_common::common::message::MESSAGE_MAGIC_CODE_V2
    );
    assert_eq!(
        record::MESSAGE_MAGIC_CODE_V2,
        rocketmq_remoting::protocol::body::message_codec::MESSAGE_MAGIC_CODE_V2
    );
    assert_eq!(legacy_blank as usize, canonical_blank as usize);
    assert!(legacy_blank(&blank));
}
