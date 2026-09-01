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

use std::sync::Arc;

use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSnapshotManifest;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::TimerTimelineCursor;
use rocketmq_store_api::TimerTimelineIndexKind;
use rocketmq_store_api::TIMER_SNAPSHOT_SCHEMA_VERSION;
use rocketmq_store_local::timer::segmented_timeline::NativeSnapshotPin;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimeline;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimelineConfig;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentKey;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointKind;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointV1;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::native_overlay::RocksDbNativeTimelineOverlay;
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;

use crate::timer::timeline::segmented_index::SegmentedCommitCoordinator;
use crate::timer::timeline::segmented_index::SegmentedCommitCrashPoint;

/// Public names for the deterministic commit boundaries exercised by integration tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentedCommitTestCrashPoint {
    BeforeNativeAppend,
    AfterNativeFsyncBeforeOverlay,
    AfterAtomicOverlayCheckpoint,
    AfterPublish,
}

/// Durable state observed before and after idempotent replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentedCommitCrashProbe {
    pub native_before_replay: bool,
    pub overlay_before_replay: bool,
    pub checkpoint_before_replay: bool,
    pub orphan_records_before_replay: usize,
    pub native_records_after_replay: u64,
    pub overlay_after_replay: bool,
    pub checkpoint_after_replay: bool,
    pub orphan_records_after_replay: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentedSnapshotProbe {
    pub joint_binding_valid: bool,
    pub rocks_sequence_nonzero: bool,
    pub damaged_native_binding_rejected: bool,
    pub damaged_artifact_rejected: bool,
}

/// Runs one deterministic crash/restart/replay scenario using real native files and RocksDB.
pub fn run_segmented_commit_crash_probe(crash: SegmentedCommitTestCrashPoint) -> SegmentedCommitCrashProbe {
    let root = tempfile::tempdir().expect("segmented commit root");
    let native = Arc::new(
        SegmentedTimeline::open(root.path(), SegmentedTimelineConfig::default())
            .expect("open native Timeline")
            .expect("valid native Timeline configuration"),
    );
    let timeline = Arc::new(RocksDbTimelineIndex::open(root.path()).expect("open overlay"));
    let coordinator = SegmentedCommitCoordinator::new(Arc::clone(&native), Arc::clone(&timeline));
    let entry = entry();
    let checkpoint = checkpoint();
    let result = coordinator.commit_with_crash(&[entry], state_batch(entry), checkpoint, map_crash(crash));
    assert!(result.is_err(), "probe requires an injected crash");

    let overlay = RocksDbNativeTimelineOverlay::new(timeline.store());
    let native_before_replay = native
        .get(native_key(entry.key))
        .expect("native before replay")
        .is_some();
    let overlay_before_replay = overlay
        .get(entry.key.timer_id, entry.key.generation)
        .expect("overlay before replay")
        .is_some();
    let checkpoint_before_replay = timeline
        .checkpoint(TimelineCheckpointKind::MaterializedSource, 0)
        .expect("checkpoint before replay")
        .is_some();
    let orphan_records_before_replay = coordinator.recover(16).expect("recover before replay").orphan_records;

    coordinator
        .commit(&[entry], state_batch(entry), checkpoint)
        .expect("idempotent replay");
    let recovered = coordinator.recover(16).expect("recover after replay");
    SegmentedCommitCrashProbe {
        native_before_replay,
        overlay_before_replay,
        checkpoint_before_replay,
        orphan_records_before_replay,
        native_records_after_replay: native.manifest().active_runs.iter().map(|run| run.record_count).sum(),
        overlay_after_replay: overlay
            .get(entry.key.timer_id, entry.key.generation)
            .expect("overlay after replay")
            .is_some(),
        checkpoint_after_replay: timeline
            .checkpoint(TimelineCheckpointKind::MaterializedSource, 0)
            .expect("checkpoint after replay")
            .is_some(),
        orphan_records_after_replay: recovered.orphan_records,
    }
}

/// Verifies that one snapshot identity binds both native manifest durability and RocksDB sequence.
pub fn run_segmented_snapshot_probe() -> SegmentedSnapshotProbe {
    let root = tempfile::tempdir().expect("segmented snapshot root");
    let native = Arc::new(
        SegmentedTimeline::open(root.path(), SegmentedTimelineConfig::default())
            .expect("open native Timeline")
            .expect("valid native Timeline configuration"),
    );
    let timeline = Arc::new(RocksDbTimelineIndex::open(root.path()).expect("open overlay"));
    SegmentedCommitCoordinator::new(Arc::clone(&native), Arc::clone(&timeline))
        .commit(&[entry()], state_batch(entry()), checkpoint())
        .expect("commit");
    let pin = native
        .pin_snapshot(23)
        .expect("pin native")
        .expect("snapshot generation is non-zero");
    let native_snapshot_root = root.path().join("native-snapshot");
    let native_files = native
        .create_snapshot_files(&native_snapshot_root, pin)
        .expect("copy native snapshot files");
    let rocks_sequence = timeline.store().latest_sequence_number().expect("RocksDB sequence");
    let mut manifest = TimerSnapshotManifest {
        schema_version: TIMER_SNAPSHOT_SCHEMA_VERSION,
        generation: pin.snapshot_generation,
        source_cq_cursor: 1,
        source_physical_cursor: 1_152,
        due_time_cursor_ms: 1,
        completion_physical_cursor: 1,
        timeline_sequence: rocks_sequence,
        timeline_index_kind: TimerTimelineIndexKind::Segmented,
        native_manifest_generation: Some(pin.manifest_generation),
        native_durable_end: Some(pin.durable_end),
        native_manifest_checksum: Some(pin.manifest_checksum),
        native_files,
        role_epoch: 1,
        activation_epoch: 1,
        format_fingerprint: 1,
        timeline_checkpoint_uri: "file:///segmented-snapshot/timeline".to_owned(),
        payload_files: Vec::new(),
        checksum: String::new(),
    };
    manifest.seal().expect("seal manifest");
    let joint_binding_valid =
        manifest.validate_artifact_files(&native_snapshot_root).is_ok() && native.validate_snapshot_pin(pin).is_ok();
    let damaged_path = native_snapshot_root.join(
        manifest
            .native_files
            .last()
            .expect("native snapshot contains a run")
            .relative_path
            .replace('/', std::path::MAIN_SEPARATOR_STR),
    );
    let mut damaged_bytes = std::fs::read(&damaged_path).expect("read snapshot run");
    damaged_bytes[0] ^= 0x01;
    std::fs::write(&damaged_path, damaged_bytes).expect("damage snapshot run");
    let damaged_artifact_rejected = manifest.validate_artifact_files(&native_snapshot_root).is_err();

    let damaged_checksum = pin.manifest_checksum.wrapping_add(1).max(1);
    manifest.native_manifest_checksum = Some(damaged_checksum);
    manifest.seal().expect("reseal damaged identity");
    let damaged_native_binding_rejected = manifest.validate().is_ok()
        && native
            .validate_snapshot_pin(NativeSnapshotPin {
                manifest_checksum: damaged_checksum,
                ..pin
            })
            .is_err();
    SegmentedSnapshotProbe {
        joint_binding_valid,
        rocks_sequence_nonzero: rocks_sequence > 0,
        damaged_native_binding_rejected,
        damaged_artifact_rejected,
    }
}

fn map_crash(crash: SegmentedCommitTestCrashPoint) -> SegmentedCommitCrashPoint {
    match crash {
        SegmentedCommitTestCrashPoint::BeforeNativeAppend => SegmentedCommitCrashPoint::BeforeNativeAppend,
        SegmentedCommitTestCrashPoint::AfterNativeFsyncBeforeOverlay => {
            SegmentedCommitCrashPoint::AfterNativeFsyncBeforeOverlay
        }
        SegmentedCommitTestCrashPoint::AfterAtomicOverlayCheckpoint => {
            SegmentedCommitCrashPoint::AfterOverlayAndCheckpointBeforePublish
        }
        SegmentedCommitTestCrashPoint::AfterPublish => SegmentedCommitCrashPoint::AfterPublish,
    }
}

fn entry() -> TimelineIndexEntry {
    TimelineIndexEntry {
        key: TimelineKeyV1 {
            due_time_ms: 1_800_000_000_000,
            lane: 2,
            timer_id: TimerId::new(7),
            generation: TimerGeneration::new(3),
        },
        record: TimelineRecordV1 {
            payload: TimerPayloadStoreLocator::try_new(20_833, 2, 0, 0, 128, 9).expect("payload"),
            source_cq_offset: TimerSourceCqOffset::new(0),
            source_physical_offset: 1_024,
            source_size: 128,
            state_version: 0,
            owner_engine: TimerEngineId::ExtendedTimeline,
            shadow_only: false,
        },
    }
}

fn state_batch(entry: TimelineIndexEntry) -> RocksDbWriteBatch {
    let mut batch = RocksDbWriteBatch::with_capacity(1);
    RocksDbTimelineStateIndex::append_state(
        &mut batch,
        entry.key.timer_id,
        entry.key.generation,
        &TimelineStateRecordV1 {
            state: TimelineState::Pending,
            state_version: 0,
            route: PersistedTimerRoute::try_new(
                TimerEngineId::ExtendedTimeline,
                1,
                1,
                entry.key.generation,
                "stable-token",
            )
            .expect("route"),
            admission_epoch: TimerEngineEpoch::new(1),
            owner_epoch: TimerEngineEpoch::new(1),
            claim_seq: 0,
            due_time_ms: entry.key.due_time_ms,
            lane: entry.key.lane,
            terminal_at_ms: 0,
            shadow_only: false,
        },
    )
    .expect("state");
    batch
}

fn checkpoint() -> TimelineCheckpointV1 {
    TimelineCheckpointV1 {
        materialized_source_offset: TimerSourceCqOffset::new(0),
        due_cursor: TimerTimelineCursor::default(),
        completion_cursor: TimerTimelineCursor::default(),
        format_fingerprint: 1,
        generation: 1,
    }
}

fn native_key(key: TimelineKeyV1) -> TimelineSegmentKey {
    TimelineSegmentKey {
        due_time_ms: key.due_time_ms,
        lane: key.lane,
        timer_id: key.timer_id,
        generation: key.generation,
    }
}
