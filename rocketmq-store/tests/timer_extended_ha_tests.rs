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

#![cfg(feature = "extended_timeline")]

use rocketmq_store_api::StoreContractViolation;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSnapshotFile;
use rocketmq_store_api::TimerSnapshotManifest;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::TimerTimelineIndexKind;
use rocketmq_store_api::TIMER_SNAPSHOT_SCHEMA_VERSION;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;

fn manifest() -> TimerSnapshotManifest {
    let mut manifest = TimerSnapshotManifest {
        schema_version: TIMER_SNAPSHOT_SCHEMA_VERSION,
        generation: 11,
        source_cq_cursor: 42,
        source_physical_cursor: 4_096,
        due_time_cursor_ms: 1_800_000_000_000,
        completion_physical_cursor: 8_192,
        timeline_sequence: 17,
        timeline_index_kind: TimerTimelineIndexKind::RocksDb,
        native_manifest_generation: None,
        native_durable_end: None,
        native_manifest_checksum: None,
        native_files: Vec::new(),
        role_epoch: 9,
        activation_epoch: 7,
        format_fingerprint: 0xA55A,
        timeline_checkpoint_uri: "file:///controlled/timer-snapshot/timeline".to_owned(),
        payload_files: vec![TimerSnapshotFile {
            relative_path: "payload/day-0000000001/lane-00000/00000000000000000000".to_owned(),
            length: 512,
            sha256: "ab".repeat(32),
        }],
        checksum: String::new(),
    };
    manifest.seal().expect("seal manifest");
    manifest
}

#[test]
fn snapshot_manifest_fences_both_incremental_replay_streams() {
    let manifest = manifest();
    manifest.validate().expect("valid manifest");
    assert_eq!(manifest.source_physical_cursor, 4_096);
    assert_eq!(manifest.completion_physical_cursor, 8_192);

    let mut damaged = manifest;
    damaged.completion_physical_cursor += 1;
    assert_eq!(
        damaged.validate(),
        Err(StoreContractViolation::TimerSnapshotChecksumMismatch)
    );
}

#[test]
fn snapshot_gc_pin_survives_restart_until_installation_is_confirmed() {
    let root = tempfile::tempdir().expect("temporary Timeline root");
    let entry_key = TimelineKeyV1 {
        due_time_ms: 100,
        lane: 0,
        timer_id: TimerId::new(1),
        generation: TimerGeneration::new(1),
    };
    let pin_fence = TimelineKeyV1 {
        due_time_ms: 50,
        lane: 0,
        timer_id: TimerId::new(0),
        generation: TimerGeneration::new(0),
    };
    let requested_fence = TimelineKeyV1 {
        due_time_ms: 200,
        lane: 0,
        timer_id: TimerId::new(0),
        generation: TimerGeneration::new(0),
    };

    let timeline = RocksDbTimelineIndex::open(root.path()).expect("open Timeline");
    timeline
        .put_batch(
            &[TimelineIndexEntry {
                key: entry_key,
                record: TimelineRecordV1 {
                    payload: TimerPayloadStoreLocator::try_new(0, 0, 0, 0, 64, 1).expect("payload locator"),
                    source_cq_offset: TimerSourceCqOffset::new(0),
                    source_physical_offset: 0,
                    source_size: 64,
                    state_version: 0,
                    owner_engine: TimerEngineId::ExtendedTimeline,
                    shadow_only: false,
                },
            }],
            None,
        )
        .expect("append Timeline entry");
    timeline
        .pin_snapshot_generation(pin_fence, 7)
        .expect("persist snapshot pin");
    timeline.close();
    drop(timeline);

    let reopened = RocksDbTimelineIndex::open(root.path()).expect("reopen Timeline");
    assert_eq!(
        reopened.gc(requested_fence, 16).expect("pinned GC"),
        0,
        "a restarted store must restore the snapshot fence before GC"
    );
    assert!(reopened.get(entry_key).expect("read pinned entry").is_some());
    reopened
        .release_snapshot(rocketmq_store_rocksdb::timer::timeline_index::TimelineSnapshotPin {
            generation: 7,
            gc_fence: pin_fence,
        })
        .expect("release replicated snapshot pin");
    assert_eq!(reopened.gc(requested_fence, 16).expect("unpinned GC"), 1);
}
