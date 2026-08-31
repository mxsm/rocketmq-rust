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

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerSnapshotManifest;
use rocketmq_store_api::TimerTimelineIndexKind;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimeline;
use rocketmq_store_local::timer::storage_format::crc32c;
use rocketmq_store_local::timer::timeline_segment::TimelinePartitionKey;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentKey;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentRecord;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointKind;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointV1;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::native_overlay::NativeDurabilityV1;
use rocketmq_store_rocksdb::timer::native_overlay::NativeMaterializedMarkerV1;
use rocketmq_store_rocksdb::timer::native_overlay::NativeOverlayCheckpointV1;
use rocketmq_store_rocksdb::timer::native_overlay::NativeTimelineLocatorV1;
use rocketmq_store_rocksdb::timer::native_overlay::RocksDbNativeTimelineOverlay;
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;
use rocketmq_store_rocksdb::timer::NATIVE_META_CF;
use thiserror::Error;

use super::segmented_index::entry_to_native;
use super::segmented_index::SegmentedCommitCoordinator;
use super::segmented_index::SegmentedCommitError;

const MIGRATION_STATE_KEY: &[u8] = b"timer-index-migration-v1";
const MIGRATION_STATE_VERSION: u16 = 1;
const MIGRATION_STATE_SIZE: usize = 93;
const MIGRATION_DIRECTORY: &str = "timer-extended/index-migration-v1";

/// Durable owner and online migration phase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TimelineIndexMigrationPhase {
    /// RocksDB is the only owner and no migration is active.
    #[default]
    RocksOwner = 0,
    /// A fixed RocksDB checkpoint is being exported while derived increments are mirrored.
    Building = 1,
    /// Bulk export finished and differential comparison may be repeated.
    Shadowing = 2,
    /// Segmented is owner while RocksDB remains an up-to-date rollback standby.
    SegmentedOwnerRollback = 3,
    /// Segmented is owner and RocksDB Timeline standby writes have stopped.
    SegmentedOwnerFinal = 4,
}

impl TimelineIndexMigrationPhase {
    const fn decode(value: u8) -> Result<Self, IndexMigrationError> {
        match value {
            0 => Ok(Self::RocksOwner),
            1 => Ok(Self::Building),
            2 => Ok(Self::Shadowing),
            3 => Ok(Self::SegmentedOwnerRollback),
            4 => Ok(Self::SegmentedOwnerFinal),
            _ => Err(IndexMigrationError::CorruptState),
        }
    }

    pub(crate) const fn owner(self) -> TimerTimelineIndexKind {
        match self {
            Self::RocksOwner | Self::Building | Self::Shadowing => TimerTimelineIndexKind::RocksDb,
            Self::SegmentedOwnerRollback | Self::SegmentedOwnerFinal => TimerTimelineIndexKind::Segmented,
        }
    }

    const fn mirrors_native(self) -> bool {
        !matches!(self, Self::RocksOwner)
    }

    const fn keeps_rocks_timeline(self) -> bool {
        !matches!(self, Self::SegmentedOwnerFinal)
    }
}

/// Persisted migration progress. The continuation belongs to the immutable RocksDB checkpoint,
/// never to an expired CommitLog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimelineIndexMigrationState {
    pub(crate) phase: TimelineIndexMigrationPhase,
    pub(crate) source_checkpoint: i64,
    pub(crate) rocks_sequence: u64,
    pub(crate) continuation: Option<TimelineKeyV1>,
    pub(crate) engine_epoch: TimerEngineEpoch,
    pub(crate) exported_records: u64,
    pub(crate) rolling_hash: u64,
    pub(crate) bulk_complete: bool,
    pub(crate) cutover_snapshot_generation: u64,
    pub(crate) standby_active: bool,
}

impl Default for TimelineIndexMigrationState {
    fn default() -> Self {
        Self {
            phase: TimelineIndexMigrationPhase::RocksOwner,
            source_checkpoint: -1,
            rocks_sequence: 0,
            continuation: None,
            engine_epoch: TimerEngineEpoch::new(1),
            exported_records: 0,
            rolling_hash: FNV_OFFSET,
            bulk_complete: false,
            cutover_snapshot_generation: 0,
            standby_active: false,
        }
    }
}

/// One bounded bulk-export result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MigrationBuildResult {
    pub(crate) scanned: usize,
    pub(crate) exported: usize,
    pub(crate) complete: bool,
}

/// Backend-neutral comparison summary used as the cutover oracle.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MigrationComparison {
    pub(crate) records: u64,
    pub(crate) rolling_hash: u64,
    pub(crate) partitions: BTreeMap<TimelinePartitionKey, (u64, u64)>,
    pub(crate) first_key: Option<TimelineKeyV1>,
    pub(crate) last_key: Option<TimelineKeyV1>,
}

/// Coordinates snapshot export, derived-write mirroring, owner cutover, and rollback.
///
/// Every mutating method shares `operation_lock`; callers may attach the same manager to the
/// materializer so checkpoint capture cannot race a derived commit.
pub(crate) struct TimelineIndexMigrationManager {
    root: PathBuf,
    rocks: Arc<RocksDbTimelineIndex>,
    native: Arc<SegmentedTimeline>,
    coordinator: SegmentedCommitCoordinator,
    operation_lock: Mutex<()>,
}

impl TimelineIndexMigrationManager {
    pub(crate) fn new(
        store_root: impl AsRef<Path>,
        rocks: Arc<RocksDbTimelineIndex>,
        native: Arc<SegmentedTimeline>,
    ) -> Self {
        Self {
            root: store_root.as_ref().join(MIGRATION_DIRECTORY),
            coordinator: SegmentedCommitCoordinator::new(Arc::clone(&native), Arc::clone(&rocks)),
            rocks,
            native,
            operation_lock: Mutex::new(()),
        }
    }

    pub(crate) fn state(&self) -> Result<TimelineIndexMigrationState, IndexMigrationError> {
        self.load_state()
    }

    pub(crate) fn owner(&self) -> Result<TimerTimelineIndexKind, IndexMigrationError> {
        Ok(self.load_state()?.phase.owner())
    }

    /// Captures the only supported migration source: a RocksDB checkpoint plus its source cursor.
    pub(crate) fn begin(&self) -> Result<TimelineIndexMigrationState, IndexMigrationError> {
        let _guard = self.operation_lock.lock();
        let current = self.load_state()?;
        if current.phase != TimelineIndexMigrationPhase::RocksOwner {
            return Err(IndexMigrationError::InvalidTransition);
        }
        let checkpoint = self
            .rocks
            .checkpoint(TimelineCheckpointKind::MaterializedSource, 0)?
            .unwrap_or(TimelineCheckpointV1 {
                materialized_source_offset: rocketmq_store_api::TimerSourceCqOffset::new(-1),
                due_cursor: rocketmq_store_api::TimerTimelineCursor::default(),
                completion_cursor: rocketmq_store_api::TimerTimelineCursor::default(),
                format_fingerprint: 1,
                generation: 0,
            });
        let path_sequence = self.rocks.store().latest_sequence_number()?.max(1);
        std::fs::create_dir_all(&self.root)?;
        let checkpoint_path = self.checkpoint_path(path_sequence);
        if checkpoint_path.exists() {
            return Err(IndexMigrationError::CheckpointExists(path_sequence));
        }
        self.rocks.store().create_checkpoint_blocking(checkpoint_path.clone())?;
        let checkpoint_index = RocksDbTimelineIndex::open_database_path(&checkpoint_path)?;
        let rocks_sequence = checkpoint_index.store().latest_sequence_number()?.max(1);
        checkpoint_index.close();
        drop(checkpoint_index);
        if rocks_sequence != path_sequence {
            let exact_path = self.checkpoint_path(rocks_sequence);
            if exact_path.exists() {
                return Err(IndexMigrationError::CheckpointExists(rocks_sequence));
            }
            std::fs::rename(checkpoint_path, exact_path)?;
        }
        let state = TimelineIndexMigrationState {
            phase: TimelineIndexMigrationPhase::Building,
            source_checkpoint: checkpoint.materialized_source_offset.get(),
            rocks_sequence,
            continuation: None,
            engine_epoch: current.engine_epoch,
            exported_records: 0,
            rolling_hash: FNV_OFFSET,
            bulk_complete: false,
            cutover_snapshot_generation: 0,
            standby_active: false,
        };
        self.persist_state(state)?;
        Ok(state)
    }

    /// Exports one bounded page from the fixed RocksDB checkpoint and skips terminal generations.
    pub(crate) fn build_page(
        &self,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<MigrationBuildResult, IndexMigrationError> {
        if max_records == 0 || max_bytes < TimelineSegmentRecord::encoded_size() {
            return Err(IndexMigrationError::InvalidBudget);
        }
        let _guard = self.operation_lock.lock();
        let mut state = self.load_state()?;
        if state.phase != TimelineIndexMigrationPhase::Building || state.bulk_complete {
            return Err(IndexMigrationError::InvalidTransition);
        }
        let snapshot = RocksDbTimelineIndex::open_database_path(self.checkpoint_path(state.rocks_sequence))?;
        let page = snapshot.range_scan(i64::MIN, i64::MAX, state.continuation, max_records, max_bytes)?;
        let states = snapshot.state_index().get_many(
            &page
                .entries
                .iter()
                .map(|entry| (entry.key.timer_id, entry.key.generation))
                .collect::<Vec<_>>(),
        )?;
        let mut exported = Vec::new();
        for (entry, current) in page.entries.iter().copied().zip(states) {
            let current = current.ok_or(IndexMigrationError::MissingState(entry.key))?;
            if !current.state.is_terminal() {
                exported.push(entry);
            }
        }
        let native_receipt = if exported.is_empty() {
            None
        } else {
            Some(
                self.native.append_batch(
                    &exported
                        .iter()
                        .copied()
                        .map(entry_to_native)
                        .collect::<Result<Vec<_>, _>>()?,
                )?,
            )
        };
        for entry in &exported {
            state.rolling_hash = hash_entry(state.rolling_hash, *entry);
        }
        state.exported_records = state
            .exported_records
            .saturating_add(u64::try_from(exported.len()).unwrap_or(u64::MAX));
        state.continuation = page.entries.last().map(|entry| entry.key).or(state.continuation);
        state.bulk_complete = page.continuation.is_none();
        let mut durable_batch = RocksDbWriteBatch::with_capacity(exported.len().saturating_mul(2).saturating_add(2));
        if let Some(receipt) = native_receipt {
            let durability = NativeDurabilityV1 {
                manifest_generation: receipt.manifest_generation,
                durable_end: receipt.durable_end,
                record_hash: receipt.record_hash,
                manifest_checksum: receipt.manifest_checksum,
            };
            for entry in &exported {
                RocksDbNativeTimelineOverlay::append_locator(
                    &mut durable_batch,
                    NativeTimelineLocatorV1 {
                        entry: *entry,
                        durability,
                    },
                )?;
                RocksDbNativeTimelineOverlay::append_materialized_marker(
                    &mut durable_batch,
                    entry.record.source_cq_offset,
                    NativeMaterializedMarkerV1 {
                        timer_id: entry.key.timer_id,
                        generation: entry.key.generation,
                        durability,
                    },
                )?;
            }
            let overlay = RocksDbNativeTimelineOverlay::new(self.rocks.store());
            let previous_generation = overlay.checkpoint()?.map_or(0, |checkpoint| checkpoint.generation);
            let source = self
                .rocks
                .checkpoint(TimelineCheckpointKind::MaterializedSource, 0)?
                .map_or(state.source_checkpoint, |checkpoint| {
                    checkpoint.materialized_source_offset.get()
                });
            RocksDbNativeTimelineOverlay::append_checkpoint(
                &mut durable_batch,
                NativeOverlayCheckpointV1 {
                    manifest_generation: receipt.manifest_generation,
                    durable_end: receipt.durable_end,
                    manifest_checksum: receipt.manifest_checksum,
                    materialized_source_offset: rocketmq_store_api::TimerSourceCqOffset::new(source),
                    generation: previous_generation.saturating_add(1).max(1),
                },
            );
        }
        durable_batch.put_cf(NATIVE_META_CF, MIGRATION_STATE_KEY, encode_state(state));
        self.rocks.write_batch(&durable_batch)?;
        Ok(MigrationBuildResult {
            scanned: page.entries.len(),
            exported: exported.len(),
            complete: state.bulk_complete,
        })
    }

    /// Returns whether materialization should keep a RocksDB Timeline standby entry.
    pub(crate) fn keeps_rocks_timeline(&self) -> Result<bool, IndexMigrationError> {
        Ok(self.load_state()?.phase.keeps_rocks_timeline())
    }

    /// Reads one formal record from the active migration set without consulting CommitLog.
    pub(crate) fn get(&self, key: TimelineKeyV1) -> Result<Option<TimelineRecordV1>, IndexMigrationError> {
        let state = self.load_state()?;
        if state.phase.mirrors_native() {
            let native = self.native.get(TimelineSegmentKey {
                due_time_ms: key.due_time_ms,
                lane: key.lane,
                timer_id: key.timer_id,
                generation: key.generation,
            })?;
            if let Some(native) = native {
                return Ok(Some(native_entry(native).record));
            }
        }
        Ok(self.rocks.get(key)?)
    }

    /// Commits a materializer-derived batch according to the durable migration phase.
    /// Producer request threads never invoke this path.
    pub(crate) fn commit_derived(
        &self,
        entries: &[TimelineIndexEntry],
        mut overlay_batch: RocksDbWriteBatch,
        source_checkpoint: TimelineCheckpointV1,
    ) -> Result<(), IndexMigrationError> {
        let _guard = self.operation_lock.lock();
        let state = self.load_state()?;
        if !state.phase.mirrors_native() || entries.is_empty() {
            RocksDbTimelineIndex::append_checkpoint(
                &mut overlay_batch,
                TimelineCheckpointKind::MaterializedSource,
                0,
                source_checkpoint,
            );
            self.rocks.write_batch(&overlay_batch)?;
            return Ok(());
        }
        self.coordinator.commit(entries, overlay_batch, source_checkpoint)?;
        Ok(())
    }

    /// Compares all current nonterminal records, including post-snapshot mirrored increments.
    pub(crate) fn compare(&self, page_records: usize) -> Result<MigrationComparison, IndexMigrationError> {
        if page_records == 0 {
            return Err(IndexMigrationError::InvalidBudget);
        }
        let _guard = self.operation_lock.lock();
        let state = self.load_state()?;
        if !matches!(
            state.phase,
            TimelineIndexMigrationPhase::Building | TimelineIndexMigrationPhase::Shadowing
        ) || !state.bulk_complete
        {
            return Err(IndexMigrationError::InvalidTransition);
        }
        let rocks = self.summarize_rocks(page_records)?;
        let native = self.summarize_native(page_records)?;
        if rocks != native {
            return Err(IndexMigrationError::Difference {
                rocks_records: rocks.records,
                native_records: native.records,
                rocks_hash: rocks.rolling_hash,
                native_hash: native.rolling_hash,
            });
        }
        if state.phase == TimelineIndexMigrationPhase::Building {
            let mut verified = state;
            verified.phase = TimelineIndexMigrationPhase::Shadowing;
            self.persist_state(verified)?;
        }
        Ok(rocks)
    }

    /// Atomically changes owner only after a validated joint snapshot binds native and RocksDB.
    pub(crate) fn cutover(&self, snapshot: &TimerSnapshotManifest) -> Result<TimerEngineEpoch, IndexMigrationError> {
        let _guard = self.operation_lock.lock();
        snapshot.validate()?;
        let mut state = self.load_state()?;
        if state.phase != TimelineIndexMigrationPhase::Shadowing
            || snapshot.timeline_index_kind != TimerTimelineIndexKind::Segmented
            || snapshot.timeline_sequence < state.rocks_sequence
        {
            return Err(IndexMigrationError::InvalidCutoverSnapshot);
        }
        self.validate_native_snapshot(snapshot)?;
        state.phase = TimelineIndexMigrationPhase::SegmentedOwnerRollback;
        state.engine_epoch = next_epoch(state.engine_epoch)?;
        state.cutover_snapshot_generation = snapshot.generation;
        state.standby_active = true;
        self.persist_state(state)?;
        Ok(state.engine_epoch)
    }

    pub(crate) fn rollback(&self) -> Result<TimerEngineEpoch, IndexMigrationError> {
        let _guard = self.operation_lock.lock();
        let mut state = self.load_state()?;
        if state.phase != TimelineIndexMigrationPhase::SegmentedOwnerRollback || !state.standby_active {
            return Err(IndexMigrationError::RollbackUnavailable);
        }
        state.phase = TimelineIndexMigrationPhase::RocksOwner;
        state.engine_epoch = next_epoch(state.engine_epoch)?;
        state.standby_active = false;
        self.persist_state(state)?;
        Ok(state.engine_epoch)
    }

    pub(crate) fn finish_rollback_window(&self) -> Result<(), IndexMigrationError> {
        let _guard = self.operation_lock.lock();
        let mut state = self.load_state()?;
        if state.phase != TimelineIndexMigrationPhase::SegmentedOwnerRollback || !state.standby_active {
            return Err(IndexMigrationError::InvalidTransition);
        }
        state.phase = TimelineIndexMigrationPhase::SegmentedOwnerFinal;
        state.standby_active = false;
        self.persist_state(state)
    }

    /// Aborts only before cutover. Delivery authority therefore remains RocksDB throughout.
    pub(crate) fn abort(&self) -> Result<(), IndexMigrationError> {
        let _guard = self.operation_lock.lock();
        let state = self.load_state()?;
        if !matches!(
            state.phase,
            TimelineIndexMigrationPhase::Building | TimelineIndexMigrationPhase::Shadowing
        ) {
            return Err(IndexMigrationError::InvalidTransition);
        }
        let mut partitions = self
            .native
            .manifest()
            .active_runs
            .iter()
            .map(|run| run.partition)
            .collect::<Vec<_>>();
        partitions.sort_unstable();
        partitions.dedup();
        for partition in partitions {
            self.native.delete_partition(partition)?;
        }
        self.persist_state(TimelineIndexMigrationState {
            engine_epoch: state.engine_epoch,
            ..TimelineIndexMigrationState::default()
        })
    }

    fn summarize_rocks(&self, page_records: usize) -> Result<MigrationComparison, IndexMigrationError> {
        let mut summary = MigrationComparison {
            rolling_hash: FNV_OFFSET,
            ..MigrationComparison::default()
        };
        let mut continuation = None;
        loop {
            let page = self.rocks.range_scan(
                i64::MIN,
                i64::MAX,
                continuation,
                page_records,
                page_records.saturating_mul(128),
            )?;
            let states = self.rocks.state_index().get_many(
                &page
                    .entries
                    .iter()
                    .map(|entry| (entry.key.timer_id, entry.key.generation))
                    .collect::<Vec<_>>(),
            )?;
            for (entry, state) in page.entries.iter().copied().zip(states) {
                let state = state.ok_or(IndexMigrationError::MissingState(entry.key))?;
                if !state.state.is_terminal() {
                    summary.add(entry)?;
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                summary.finalize();
                return Ok(summary);
            }
        }
    }

    fn summarize_native(&self, page_records: usize) -> Result<MigrationComparison, IndexMigrationError> {
        let mut summary = MigrationComparison {
            rolling_hash: FNV_OFFSET,
            ..MigrationComparison::default()
        };
        let state_index = RocksDbTimelineStateIndex::new(self.rocks.store());
        let mut continuation = None;
        loop {
            let page = self.native.scan_due(
                None,
                i64::MAX,
                page_records,
                page_records.saturating_mul(TimelineSegmentRecord::encoded_size()),
                continuation,
            )?;
            let states = state_index.get_many(
                &page
                    .records
                    .iter()
                    .map(|record| (record.key.timer_id, record.key.generation))
                    .collect::<Vec<_>>(),
            )?;
            for (record, state) in page.records.iter().copied().zip(states) {
                let key = native_key(record.key);
                let state = state.ok_or(IndexMigrationError::MissingState(key))?;
                if !state.state.is_terminal() {
                    summary.add(native_entry(record))?;
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                summary.finalize();
                return Ok(summary);
            }
        }
    }

    fn validate_native_snapshot(&self, snapshot: &TimerSnapshotManifest) -> Result<(), IndexMigrationError> {
        let pin = rocketmq_store_local::timer::segmented_timeline::NativeSnapshotPin {
            snapshot_generation: snapshot.generation,
            manifest_generation: snapshot
                .native_manifest_generation
                .ok_or(IndexMigrationError::InvalidCutoverSnapshot)?,
            durable_end: snapshot
                .native_durable_end
                .ok_or(IndexMigrationError::InvalidCutoverSnapshot)?,
            manifest_checksum: snapshot
                .native_manifest_checksum
                .ok_or(IndexMigrationError::InvalidCutoverSnapshot)?,
        };
        self.native.validate_snapshot_pin(pin)?;
        Ok(())
    }

    fn checkpoint_path(&self, sequence: u64) -> PathBuf {
        self.root.join(format!("rocks-snapshot-{sequence:020}"))
    }

    fn load_state(&self) -> Result<TimelineIndexMigrationState, IndexMigrationError> {
        self.rocks
            .store()
            .get_cf(NATIVE_META_CF, MIGRATION_STATE_KEY)?
            .map(|bytes| decode_state(&bytes))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    fn persist_state(&self, state: TimelineIndexMigrationState) -> Result<(), IndexMigrationError> {
        let mut batch = RocksDbWriteBatch::with_capacity(1);
        batch.put_cf(NATIVE_META_CF, MIGRATION_STATE_KEY, encode_state(state));
        self.rocks.write_batch(&batch)?;
        Ok(())
    }
}

impl MigrationComparison {
    fn add(&mut self, entry: TimelineIndexEntry) -> Result<(), IndexMigrationError> {
        let partition = TimelinePartitionKey::from_deadline(entry.key.due_time_ms, entry.key.lane)?;
        let hash = hash_entry(FNV_OFFSET, entry);
        let partition_summary = self.partitions.entry(partition).or_default();
        partition_summary.0 = partition_summary.0.saturating_add(1);
        partition_summary.1 = mix_hash(partition_summary.1, hash);
        self.records = self.records.saturating_add(1);
        self.first_key = Some(self.first_key.map_or(entry.key, |key| key.min(entry.key)));
        self.last_key = Some(self.last_key.map_or(entry.key, |key| key.max(entry.key)));
        Ok(())
    }

    fn finalize(&mut self) {
        let mut hash = FNV_OFFSET;
        for (partition, (count, partition_hash)) in &self.partitions {
            for byte in partition
                .due_day_utc
                .to_be_bytes()
                .into_iter()
                .chain([partition.due_hour_utc])
                .chain(partition.lane.to_be_bytes())
                .chain(count.to_be_bytes())
                .chain(partition_hash.to_be_bytes())
            {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(FNV_PRIME);
            }
        }
        self.rolling_hash = hash;
    }
}

fn encode_state(state: TimelineIndexMigrationState) -> [u8; MIGRATION_STATE_SIZE] {
    let mut output = [0u8; MIGRATION_STATE_SIZE];
    output[0..2].copy_from_slice(&MIGRATION_STATE_VERSION.to_be_bytes());
    output[2] = state.phase as u8;
    output[3..11].copy_from_slice(&state.source_checkpoint.to_be_bytes());
    output[11..19].copy_from_slice(&state.rocks_sequence.to_be_bytes());
    output[19] = u8::from(state.continuation.is_some());
    if let Some(continuation) = state.continuation {
        output[20..55].copy_from_slice(&continuation.encode());
    }
    output[55..63].copy_from_slice(&state.engine_epoch.get().to_be_bytes());
    output[63..71].copy_from_slice(&state.exported_records.to_be_bytes());
    output[71..79].copy_from_slice(&state.rolling_hash.to_be_bytes());
    output[79] = u8::from(state.bulk_complete);
    output[80..88].copy_from_slice(&state.cutover_snapshot_generation.to_be_bytes());
    output[88] = u8::from(state.standby_active);
    let checksum = crc32c(&output[..MIGRATION_STATE_SIZE - 4]);
    output[MIGRATION_STATE_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
    output
}

fn decode_state(bytes: &[u8]) -> Result<TimelineIndexMigrationState, IndexMigrationError> {
    if bytes.len() != MIGRATION_STATE_SIZE
        || read_u16(bytes, 0)? != MIGRATION_STATE_VERSION
        || crc32c(&bytes[..MIGRATION_STATE_SIZE - 4]) != read_u32(bytes, MIGRATION_STATE_SIZE - 4)?
        || bytes[19] > 1
        || bytes[79] > 1
        || bytes[88] > 1
    {
        return Err(IndexMigrationError::CorruptState);
    }
    let continuation = (bytes[19] == 1)
        .then(|| TimelineKeyV1::decode(&bytes[20..55]))
        .transpose()?;
    let engine_epoch = TimerEngineEpoch::new(read_u64(bytes, 55)?);
    if engine_epoch.get() == 0 {
        return Err(IndexMigrationError::CorruptState);
    }
    Ok(TimelineIndexMigrationState {
        phase: TimelineIndexMigrationPhase::decode(bytes[2])?,
        source_checkpoint: read_i64(bytes, 3)?,
        rocks_sequence: read_u64(bytes, 11)?,
        continuation,
        engine_epoch,
        exported_records: read_u64(bytes, 63)?,
        rolling_hash: read_u64(bytes, 71)?,
        bulk_complete: bytes[79] == 1,
        cutover_snapshot_generation: read_u64(bytes, 80)?,
        standby_active: bytes[88] == 1,
    })
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn hash_entry(mut hash: u64, entry: TimelineIndexEntry) -> u64 {
    for byte in entry.key.encode().into_iter().chain(entry.record.encode()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

const fn mix_hash(accumulator: u64, value: u64) -> u64 {
    accumulator.rotate_left(7) ^ value.wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

fn native_key(key: TimelineSegmentKey) -> TimelineKeyV1 {
    TimelineKeyV1 {
        due_time_ms: key.due_time_ms,
        lane: key.lane,
        timer_id: key.timer_id,
        generation: key.generation,
    }
}

fn native_entry(record: TimelineSegmentRecord) -> TimelineIndexEntry {
    TimelineIndexEntry {
        key: native_key(record.key),
        record: rocketmq_store_rocksdb::timer::codec::TimelineRecordV1 {
            payload: record.payload,
            source_cq_offset: record.source_cq_offset,
            source_physical_offset: record.source_physical_offset,
            source_size: record.source_size,
            state_version: record.state_version,
            owner_engine: record.owner_engine,
            shadow_only: record.shadow_only,
        },
    }
}

fn next_epoch(epoch: TimerEngineEpoch) -> Result<TimerEngineEpoch, IndexMigrationError> {
    epoch
        .get()
        .checked_add(1)
        .map(TimerEngineEpoch::new)
        .ok_or(IndexMigrationError::EpochExhausted)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, IndexMigrationError> {
    bytes
        .get(offset..offset.saturating_add(2))
        .and_then(|value| value.try_into().ok())
        .map(u16::from_be_bytes)
        .ok_or(IndexMigrationError::CorruptState)
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, IndexMigrationError> {
    bytes
        .get(offset..offset.saturating_add(4))
        .and_then(|value| value.try_into().ok())
        .map(u32::from_be_bytes)
        .ok_or(IndexMigrationError::CorruptState)
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IndexMigrationError> {
    bytes
        .get(offset..offset.saturating_add(8))
        .and_then(|value| value.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or(IndexMigrationError::CorruptState)
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64, IndexMigrationError> {
    bytes
        .get(offset..offset.saturating_add(8))
        .and_then(|value| value.try_into().ok())
        .map(i64::from_be_bytes)
        .ok_or(IndexMigrationError::CorruptState)
}

#[derive(Debug, Error)]
pub(crate) enum IndexMigrationError {
    #[error("Timeline index migration state is corrupt")]
    CorruptState,
    #[error("invalid Timeline index migration transition")]
    InvalidTransition,
    #[error("Timeline index migration budget cannot fit one record")]
    InvalidBudget,
    #[error("RocksDB migration checkpoint {0} already exists")]
    CheckpointExists(u64),
    #[error("Timeline migration source is missing state for {0:?}")]
    MissingState(TimelineKeyV1),
    #[error(
        "Timeline migration differs: rocks records={rocks_records} hash={rocks_hash:#x}, native records={native_records} hash={native_hash:#x}"
    )]
    Difference {
        rocks_records: u64,
        native_records: u64,
        rocks_hash: u64,
        native_hash: u64,
    },
    #[error("cutover snapshot does not bind the verified native and RocksDB generations")]
    InvalidCutoverSnapshot,
    #[error("RocksDB rollback standby is unavailable")]
    RollbackUnavailable,
    #[error("Timer index owner epoch is exhausted")]
    EpochExhausted,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Rocks(#[from] rocketmq_error::RocketMQError),
    #[error(transparent)]
    Native(#[from] rocketmq_store_local::timer::segmented_timeline::SegmentedTimelineError),
    #[error(transparent)]
    Segment(#[from] rocketmq_store_local::timer::timeline_segment::TimelineSegmentError),
    #[error(transparent)]
    Commit(#[from] SegmentedCommitError),
    #[error(transparent)]
    Snapshot(#[from] rocketmq_store_api::StoreContractViolation),
}
