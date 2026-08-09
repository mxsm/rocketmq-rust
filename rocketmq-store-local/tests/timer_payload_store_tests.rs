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

use std::io::Write;

use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_local::timer::partition_manifest::TimerPayloadPartitionKey;
use rocketmq_store_local::timer::payload_record::TimerPayloadRecordV1;
use rocketmq_store_local::timer::payload_store::TimerPayloadStore;
use rocketmq_store_local::timer::payload_store::TimerPayloadStoreConfig;

const DAY_MS: i64 = 86_400_000;

fn config(root: &std::path::Path) -> TimerPayloadStoreConfig {
    TimerPayloadStoreConfig {
        root: root.join("payload"),
        segment_bytes: 256,
        max_open_handles: 2,
        batch_bytes: 2_048,
        max_record_bytes: 256,
        max_partition_live_bytes: 4_096,
    }
}

fn record(id: u128, due_day: i32, lane: u16, frame: &[u8]) -> TimerPayloadRecordV1 {
    TimerPayloadRecordV1 {
        due_time_ms: i64::from(due_day) * DAY_MS + 123,
        lane,
        timer_id: TimerId::new(id),
        generation: TimerGeneration::new(0),
        source_cq_offset: TimerSourceCqOffset::new(id as i64),
        source_physical_offset: id as i64 * 1_000,
        real_queue_id: lane.into(),
        real_topic: format!("orders-{lane}"),
        frame: frame.to_vec(),
    }
}

#[test]
fn payload_record_crc_and_unknown_damage_fail_closed() {
    let expected = record(1, 20_000, 0, b"complete-commitlog-frame");
    let mut encoded = expected.encode().expect("encode");
    assert_eq!(TimerPayloadRecordV1::decode(&encoded).expect("decode"), expected);
    encoded[30] ^= 1;
    assert!(TimerPayloadRecordV1::decode(&encoded).is_err());
}

#[test]
fn payload_store_supports_180_366_and_400_day_partitions_with_bounded_handles() {
    let directory = tempfile::tempdir().expect("root");
    let store = TimerPayloadStore::new(config(directory.path())).expect("store");
    store.load().expect("load");
    let base_day = 20_000;
    let records = [
        record(1, base_day + 180, 0, &[1; 32]),
        record(2, base_day + 366, 1, &[2; 32]),
        record(3, base_day + 400, 2, &[3; 32]),
    ];
    let locators = store.append_batch(&records).expect("append");
    assert_eq!(locators.len(), records.len());
    for (locator, expected) in locators.into_iter().zip(records) {
        assert_eq!(store.read(locator).expect("read"), expected);
    }
    assert!(store.open_handle_count() <= 2);
}

#[test]
fn payload_store_rolls_segments_repairs_torn_tail_and_survives_source_cleanup() {
    let directory = tempfile::tempdir().expect("root");
    let store_config = config(directory.path());
    let source_path = directory.path().join("ordinary-commitlog-segment");
    std::fs::write(&source_path, b"source-can-expire").expect("source");
    let store = TimerPayloadStore::new(store_config.clone()).expect("store");
    store.load().expect("load");
    let due_day = 20_500;
    let first = record(1, due_day, 0, &[1; 72]);
    let second = record(2, due_day, 0, &[2; 72]);
    let locators = store.append_batch(&[first.clone(), second.clone()]).expect("append");
    assert_ne!(locators[0].segment_id(), locators[1].segment_id());
    std::fs::remove_file(&source_path).expect("ordinary source cleanup");
    assert_eq!(store.read(locators[0]).expect("payload remains"), first);
    drop(store);

    let active = store_config
        .root
        .join(format!("day-{due_day:010}"))
        .join("lane-00000")
        .join(format!("{:020}", locators[1].segment_id()));
    std::fs::OpenOptions::new()
        .append(true)
        .open(&active)
        .expect("active segment")
        .write_all(b"torn")
        .expect("torn tail");

    let recovered = TimerPayloadStore::new(store_config).expect("recovered store");
    recovered.load().expect("tail recovery");
    assert_eq!(recovered.read(locators[1]).expect("second payload"), second);
    assert_eq!(
        active.metadata().expect("metadata").len(),
        locators[1].offset() + u64::from(locators[1].length())
    );
}

#[test]
fn whole_partition_gc_requires_every_safety_fence() {
    let directory = tempfile::tempdir().expect("root");
    let store = TimerPayloadStore::new(config(directory.path())).expect("store");
    store.load().expect("load");
    let due_day = 20_600;
    let key = TimerPayloadPartitionKey {
        due_day_utc: due_day,
        lane: 4,
    };
    store.append_batch(&[record(1, due_day, 4, &[4; 16])]).expect("append");
    store.seal_partition(key).expect("seal");
    store.mark_gc_eligible(key).expect("eligible");
    assert!(!store.gc_partition(key, true, false, true).expect("snapshot blocks GC"));
    assert!(store.gc_partition(key, true, true, true).expect("safe GC"));
    assert!(store.partition_manifest(key).is_none());
}
