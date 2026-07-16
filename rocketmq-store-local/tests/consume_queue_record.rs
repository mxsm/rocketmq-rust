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
use rocketmq_store_local::consume_queue::record::CQ_STORE_UNIT_SIZE;
use rocketmq_store_local::consume_queue::record::MSG_TAG_OFFSET_INDEX;

#[test]
fn record_encoding_matches_the_java_20_byte_golden() {
    let record = ConsumeQueueRecord::new(0x0102_0304_0506_0708, 0x1122_3344, 0x5152_5354_5556_5758);

    assert_eq!(CQ_STORE_UNIT_SIZE, 20);
    assert_eq!(MSG_TAG_OFFSET_INDEX, 12);
    assert_eq!(
        record.encode(),
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x22, 0x33, 0x44, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56,
            0x57, 0x58,
        ]
    );
}

#[test]
fn signed_record_round_trip_and_written_semantics_are_stable() {
    let record = ConsumeQueueRecord::new(-7, -3, i64::MIN);

    assert_eq!(ConsumeQueueRecord::decode(&record.encode()), Some(record));
    assert!(!record.is_written());
    assert!(ConsumeQueueRecord::pre_blank().is_written());
    assert_eq!(ConsumeQueueRecord::new(40, 2, 0).physical_end_offset(), 42);
}

#[test]
fn decode_at_is_bounded_and_does_not_require_an_exact_outer_slice() {
    let record = ConsumeQueueRecord::new(42, 8, 9);
    let mut bytes = vec![0xAA; 3];
    bytes.extend_from_slice(&record.encode());
    bytes.push(0xBB);

    assert_eq!(ConsumeQueueRecord::decode_at(&bytes, 3), Some(record));
    assert_eq!(ConsumeQueueRecord::decode_at(&bytes, 5), None);
    assert_eq!(ConsumeQueueRecord::decode(&bytes[..19]), None);
    assert_eq!(ConsumeQueueRecord::decode_at(&bytes, usize::MAX), None);
}

#[test]
fn pre_blank_record_keeps_the_existing_storage_sentinel() {
    assert_eq!(
        ConsumeQueueRecord::pre_blank().encode(),
        [0, 0, 0, 0, 0, 0, 0, 0, 0x7f, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0,]
    );
}
