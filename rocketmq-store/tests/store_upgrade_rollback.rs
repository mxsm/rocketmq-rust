// Copyright 2026 The RocketMQ Rust Authors
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
use cheetah_string::CheetahString;
use rocketmq_store::inspect_commit_log_record;
use rocketmq_store::CommitLogRecordBodyMode;
use rocketmq_store::CommitLogRecordChecksum;
use rocketmq_store::CommitLogRecordOutcome;
use rocketmq_store::QueryMessageRequest;

struct FixtureChecksum;

impl CommitLogRecordChecksum for FixtureChecksum {
    fn checksum(&self, bytes: &[u8]) -> u32 {
        rocketmq_model::utils::crc32_utils::crc32(bytes)
    }
}

#[test]
fn v0_9_local_file_fixture_remains_readable_without_rewrite() {
    let bytes = include_bytes!("fixtures/upgrade/v0.9.0/local-file/commitlog/00000000000000000000");
    let before = bytes.to_vec();
    let mut position = 0usize;
    let mut records = Vec::new();
    while position + 4 <= bytes.len() {
        let size = i32::from_be_bytes(bytes[position..position + 4].try_into().expect("size prefix"));
        if size <= 0 {
            break;
        }
        let size = usize::try_from(size).expect("positive size");
        if position + size > bytes.len() {
            break;
        }
        let frame = Bytes::copy_from_slice(&bytes[position..position + size]);
        match inspect_commit_log_record(&frame, CommitLogRecordBodyMode::ReadAndVerify, &FixtureChecksum) {
            CommitLogRecordOutcome::Message(record) => records.push(record),
            CommitLogRecordOutcome::Blank { .. } => break,
            status => panic!("decode v0.9.0 frame failed: {status:?}"),
        }
        position += size;
    }

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].topic.as_ref(), b"V1UpgradeTopic");
    assert_eq!(records[0].queue_offset, 0);
    assert_eq!(records[1].queue_offset, 1);
    assert_eq!(records[0].body.as_deref(), Some(b"v0.9.0-message-0".as_slice()));
    assert_eq!(records[1].body.as_deref(), Some(b"v0.9.0-message-1".as_slice()));
    assert_eq!(
        bytes.as_slice(),
        before.as_slice(),
        "read compatibility must not rewrite the fixture"
    );
}

#[test]
fn legacy_and_cursor_query_requests_can_coexist() {
    let topic = CheetahString::from_static_str("UpgradeTopic");
    let key = CheetahString::from_static_str("order-7");
    let legacy = QueryMessageRequest::legacy(&topic, &key, 32, 1, 2);
    let cursor = QueryMessageRequest {
        index_type: Some(CheetahString::from_static_str("K")),
        last_key: Some(CheetahString::from_static_str(
            "1700000000000@UpgradeTopic@K@order-7@uniq-7@1024",
        )),
        ..legacy.clone()
    };

    assert_eq!(legacy.last_key, None);
    assert_eq!(legacy.legacy_backend_key(), key);
    assert_eq!(cursor.index_type.as_deref(), Some("K"));
    assert!(cursor.last_key.is_some());
}
