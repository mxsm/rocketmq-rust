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

use crate::timer::timeline::index_migration::TimelineIndexMigrationManager;
use crate::timer::timeline::index_migration::TimelineIndexMigrationPhase;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerIndexMigrationProbe {
    pub interrupted_owner_is_rocks: bool,
    pub resumed_from_checkpoint: bool,
    pub compared_records: u64,
    pub bulk_overlay_complete: bool,
    pub cutover_owner_is_segmented: bool,
    pub rollback_standby_received_increment: bool,
    pub rollback_restored_rocks: bool,
}

/// Runs a complete checkpoint export, restart, mirrored increment, cutover, and rollback.
pub fn run_timer_index_migration_probe() -> TimerIndexMigrationProbe {
    let root = tempfile::tempdir().expect("migration root");
    let rocks = Arc::new(RocksDbTimelineIndex::open(root.path()).expect("rocks Timeline"));
    let native =
        Arc::new(SegmentedTimeline::open(root.path(), SegmentedTimelineConfig::default()).expect("native Timeline"));
    seed(&rocks, 0..12);

    let manager = TimelineIndexMigrationManager::new(root.path(), Arc::clone(&rocks), Arc::clone(&native));
    manager.begin().expect("begin migration");
    let first = manager.build_page(3, 3 * 128).expect("first page");
    let interrupted_owner_is_rocks = manager.owner().expect("owner") == TimerTimelineIndexKind::RocksDb;
    drop(manager);

    let manager = TimelineIndexMigrationManager::new(root.path(), Arc::clone(&rocks), Arc::clone(&native));
    let resumed_from_checkpoint = manager.state().expect("resume state").continuation.is_some() && first.scanned == 3;
    mirror_increment(&manager, &rocks, entry(50), 50);
    loop {
        if manager.build_page(4, 4 * 128).expect("build page").complete {
            break;
        }
    }
    let native_overlay = RocksDbNativeTimelineOverlay::new(rocks.store());
    let bulk_overlay_complete = native_overlay
        .get(TimerId::new(1), TimerGeneration::new(1))
        .expect("bulk locator")
        .is_some()
        && native_overlay.checkpoint().expect("native checkpoint").is_some();
    let comparison = manager.compare(5).expect("differential comparison");
    assert_eq!(manager.compare(5).expect("repeat shadow comparison"), comparison);
    assert_eq!(
        manager.state().expect("shadow state").phase,
        TimelineIndexMigrationPhase::Shadowing
    );

    let pin = native.pin_snapshot(17).expect("native snapshot pin");
    let native_files = native
        .create_snapshot_files(&root.path().join("migration-snapshot-native"), pin)
        .expect("copy native snapshot files");
    let mut manifest = TimerSnapshotManifest {
        schema_version: TIMER_SNAPSHOT_SCHEMA_VERSION,
        generation: pin.snapshot_generation,
        source_cq_cursor: 51,
        source_physical_cursor: 51_000,
        due_time_cursor_ms: 1,
        completion_physical_cursor: 1,
        timeline_sequence: rocks.store().latest_sequence_number().expect("sequence").max(1),
        timeline_index_kind: TimerTimelineIndexKind::Segmented,
        native_manifest_generation: Some(pin.manifest_generation),
        native_durable_end: Some(pin.durable_end),
        native_manifest_checksum: Some(pin.manifest_checksum),
        native_files,
        role_epoch: 1,
        activation_epoch: 1,
        format_fingerprint: 1,
        timeline_checkpoint_uri: "file:///timer-migration/checkpoint".to_owned(),
        payload_files: Vec::new(),
        checksum: String::new(),
    };
    manifest.seal().expect("seal snapshot");
    manager.cutover(&manifest).expect("cutover");
    let cutover_owner_is_segmented = manager.owner().expect("segmented owner") == TimerTimelineIndexKind::Segmented;

    let standby_entry = entry(51);
    mirror_increment(&manager, &rocks, standby_entry, 51);
    let rollback_standby_received_increment = rocks.get(standby_entry.key).expect("standby get").is_some()
        && native.get(native_key(standby_entry.key)).expect("native get").is_some();
    manager.rollback().expect("rollback");
    let rollback_restored_rocks = manager.owner().expect("rollback owner") == TimerTimelineIndexKind::RocksDb;

    TimerIndexMigrationProbe {
        interrupted_owner_is_rocks,
        resumed_from_checkpoint,
        compared_records: comparison.records,
        bulk_overlay_complete,
        cutover_owner_is_segmented,
        rollback_standby_received_increment,
        rollback_restored_rocks,
    }
}

fn seed(rocks: &RocksDbTimelineIndex, ids: impl Iterator<Item = u128>) {
    let entries = ids.map(entry).collect::<Vec<_>>();
    let mut batch = RocksDbWriteBatch::with_capacity(entries.len().saturating_mul(2).saturating_add(1));
    for entry in &entries {
        RocksDbTimelineIndex::append_entry(&mut batch, entry).expect("append entry");
        append_state(&mut batch, *entry);
    }
    RocksDbTimelineIndex::append_checkpoint(
        &mut batch,
        TimelineCheckpointKind::MaterializedSource,
        0,
        checkpoint(11, 1),
    );
    rocks.write_batch(&batch).expect("seed Timeline");
}

fn mirror_increment(
    manager: &TimelineIndexMigrationManager,
    rocks: &RocksDbTimelineIndex,
    entry: TimelineIndexEntry,
    source_offset: i64,
) {
    let mut batch = RocksDbWriteBatch::with_capacity(2);
    if manager.keeps_rocks_timeline().expect("standby policy") {
        RocksDbTimelineIndex::append_entry(&mut batch, &entry).expect("append standby entry");
    }
    append_state(&mut batch, entry);
    manager
        .commit_derived(&[entry], batch, checkpoint(source_offset, source_offset as u64 + 2))
        .expect("mirror derived increment");
    assert!(rocks
        .state_index()
        .get(entry.key.timer_id, entry.key.generation)
        .expect("state get")
        .is_some());
}

fn append_state(batch: &mut RocksDbWriteBatch, entry: TimelineIndexEntry) {
    RocksDbTimelineStateIndex::append_state(
        batch,
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
                format!("migration-{}", entry.key.timer_id.get()),
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
    .expect("append state");
}

fn entry(id: u128) -> TimelineIndexEntry {
    let lane = u16::try_from(id % 4).expect("lane");
    TimelineIndexEntry {
        key: TimelineKeyV1 {
            due_time_ms: 1_800_000_000_000 + i64::try_from(id % 3).expect("deadline"),
            lane,
            timer_id: TimerId::new(id + 1),
            generation: TimerGeneration::new(1),
        },
        record: TimelineRecordV1 {
            payload: TimerPayloadStoreLocator::try_new(20_833, lane, id as u64 + 1, 0, 128, id as u32 + 7)
                .expect("payload locator"),
            source_cq_offset: TimerSourceCqOffset::new(id as i64),
            source_physical_offset: id as i64 * 1_024,
            source_size: 128,
            state_version: 0,
            owner_engine: TimerEngineId::ExtendedTimeline,
            shadow_only: false,
        },
    }
}

fn checkpoint(source_offset: i64, generation: u64) -> TimelineCheckpointV1 {
    TimelineCheckpointV1 {
        materialized_source_offset: TimerSourceCqOffset::new(source_offset),
        due_cursor: TimerTimelineCursor::default(),
        completion_cursor: TimerTimelineCursor::default(),
        format_fingerprint: 1,
        generation,
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
