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

use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_rocksdb::timer::codec::RecallLookupKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;

#[test]
fn timeline_v1_golden_preserves_exact_millisecond_order() {
    let key = |due_time_ms, lane, generation| {
        TimelineKeyV1 {
            due_time_ms,
            lane,
            timer_id: TimerId::new(0x0102_0304),
            generation: TimerGeneration::new(generation),
        }
        .encode()
    };
    assert!(key(7_999, 0, 0) < key(8_000, 0, 0));
    assert!(key(8_000, 0, 0) < key(8_000, 1, 0));
    assert!(key(8_000, 1, 0) < key(8_000, 1, 1));
    assert!(key(8_000, 1, 1) < key(8_001, 0, 0));
    assert_eq!(
        TimelineKeyV1::decode(&key(8_000, 1, 1)).expect("decode").due_time_ms,
        8_000
    );
}

#[test]
fn timeline_value_rejects_unknown_version_length_and_crc() {
    let record = TimelineRecordV1 {
        payload: TimerPayloadStoreLocator::try_new(20_675, 2, 3, 5, 7, 11).expect("locator"),
        source_cq_offset: TimerSourceCqOffset::new(13),
        source_physical_offset: 17,
        source_size: 19,
        state_version: 23,
        owner_engine: TimerEngineId::ExtendedTimeline,
        shadow_only: false,
    };
    let encoded = record.encode();
    assert_eq!(TimelineRecordV1::decode(&encoded).expect("decode"), record);
    assert!(TimelineRecordV1::decode(&encoded[..encoded.len() - 1]).is_err());
    let mut unknown_version = encoded;
    unknown_version[1] = 2;
    assert!(TimelineRecordV1::decode(&unknown_version).is_err());
    let mut damaged = record.encode();
    damaged[24] ^= 1;
    assert!(TimelineRecordV1::decode(&damaged).is_err());
}

#[test]
fn recall_lookup_is_length_prefixed_and_collision_free() {
    let first = RecallLookupKeyV1 {
        engine: TimerEngineId::ExtendedTimeline,
        topic: "orders+east".to_string(),
        unique_key: "42".to_string(),
    };
    let second = RecallLookupKeyV1 {
        engine: TimerEngineId::ExtendedTimeline,
        topic: "orders".to_string(),
        unique_key: "east+42".to_string(),
    };
    let first_bytes = first.encode().expect("first key");
    let second_bytes = second.encode().expect("second key");
    assert_ne!(first_bytes, second_bytes);
    assert_eq!(RecallLookupKeyV1::decode(&first_bytes).expect("decode"), first);
}
