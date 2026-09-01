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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadLocator;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::TimerTimelineCursor;
use rocketmq_store_local::timer::segmented_timeline::NativeSnapshotPin;
use rocketmq_store_local::timer::segmented_timeline::NativeWriteReceipt;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimeline;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimelineContinuation;
use rocketmq_store_local::timer::timeline_segment::TimelinePartitionKey;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentKey;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentRecord;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
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
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineSnapshotPin;
use thiserror::Error;

use crate::timer::engine::WorkBudget;
use crate::timer::error::TimerEngineError;
use crate::timer::index::TimerIndex;
use crate::timer::index::TimerIndexBackendCursor;
use crate::timer::index::TimerIndexCheckpoint;
use crate::timer::index::TimerIndexCursor;
use crate::timer::index::TimerIndexPage;
use crate::timer::index::TimerRecordState;
use crate::timer::index::TimerSnapshotPin;
use crate::timer::request::DueTimerRecord;
use crate::timer::request::TimerSourceRecord;

/// Deterministic failure boundary used by recovery tests. Production always uses `None`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum SegmentedCommitCrashPoint {
    #[default]
    None,
    BeforeNativeAppend,
    AfterNativeFsyncBeforeOverlay,
    AfterOverlayAndCheckpointBeforePublish,
    AfterPublish,
}

/// Result of reconciling the native generation with the RocksDB overlay.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SegmentedRecoveryState {
    pub(crate) manifest_generation: u64,
    pub(crate) durable_end: u64,
    pub(crate) checkpoint_generation: u64,
    pub(crate) orphan_records: usize,
}

/// Enforces the only valid steady-state cross-media ordering.
pub(crate) struct SegmentedCommitCoordinator {
    native: Arc<SegmentedTimeline>,
    timeline: Arc<RocksDbTimelineIndex>,
    overlay: RocksDbNativeTimelineOverlay,
}

impl SegmentedCommitCoordinator {
    pub(crate) fn new(native: Arc<SegmentedTimeline>, timeline: Arc<RocksDbTimelineIndex>) -> Self {
        Self {
            overlay: RocksDbNativeTimelineOverlay::new(timeline.store()),
            native,
            timeline,
        }
    }

    /// Commits native data, overlay state/locators, and the contiguous source checkpoint in that
    /// order. The source and native overlay checkpoints share the same sync-WAL WriteBatch.
    pub(crate) fn commit(
        &self,
        entries: &[TimelineIndexEntry],
        overlay_batch: RocksDbWriteBatch,
        source_checkpoint: TimelineCheckpointV1,
    ) -> Result<NativeWriteReceipt, SegmentedCommitError> {
        self.commit_with_crash(
            entries,
            overlay_batch,
            source_checkpoint,
            SegmentedCommitCrashPoint::None,
        )
    }

    pub(crate) fn commit_with_crash(
        &self,
        entries: &[TimelineIndexEntry],
        mut overlay_batch: RocksDbWriteBatch,
        source_checkpoint: TimelineCheckpointV1,
        crash_point: SegmentedCommitCrashPoint,
    ) -> Result<NativeWriteReceipt, SegmentedCommitError> {
        if entries.is_empty() {
            return Err(SegmentedCommitError::EmptyBatch);
        }
        if crash_point == SegmentedCommitCrashPoint::BeforeNativeAppend {
            return Err(SegmentedCommitError::InjectedCrash(crash_point));
        }
        let native_records = entries
            .iter()
            .copied()
            .map(entry_to_native)
            .collect::<Result<Vec<_>, _>>()?;
        let receipt = self.native.append_batch(&native_records)?;
        if crash_point == SegmentedCommitCrashPoint::AfterNativeFsyncBeforeOverlay {
            return Err(SegmentedCommitError::InjectedCrash(crash_point));
        }
        let durability = durability(receipt);
        for entry in entries {
            RocksDbNativeTimelineOverlay::append_locator(
                &mut overlay_batch,
                NativeTimelineLocatorV1 {
                    entry: *entry,
                    durability,
                },
            )?;
            RocksDbNativeTimelineOverlay::append_materialized_marker(
                &mut overlay_batch,
                entry.record.source_cq_offset,
                NativeMaterializedMarkerV1 {
                    timer_id: entry.key.timer_id,
                    generation: entry.key.generation,
                    durability,
                },
            )?;
        }
        RocksDbNativeTimelineOverlay::append_checkpoint(
            &mut overlay_batch,
            NativeOverlayCheckpointV1 {
                manifest_generation: receipt.manifest_generation,
                durable_end: receipt.durable_end,
                manifest_checksum: receipt.manifest_checksum,
                materialized_source_offset: source_checkpoint.materialized_source_offset,
                generation: source_checkpoint.generation,
            },
        );
        RocksDbTimelineIndex::append_checkpoint(
            &mut overlay_batch,
            TimelineCheckpointKind::MaterializedSource,
            0,
            source_checkpoint,
        );
        self.timeline.write_batch(&overlay_batch)?;
        if crash_point == SegmentedCommitCrashPoint::AfterOverlayAndCheckpointBeforePublish {
            return Err(SegmentedCommitError::InjectedCrash(crash_point));
        }
        if crash_point == SegmentedCommitCrashPoint::AfterPublish {
            return Err(SegmentedCommitError::InjectedCrash(crash_point));
        }
        Ok(receipt)
    }

    /// Fails closed if RocksDB is ahead of native durability and counts native-only records that
    /// can be idempotently adopted by materializer replay.
    pub(crate) fn recover(&self, max_records: usize) -> Result<SegmentedRecoveryState, SegmentedCommitError> {
        let manifest = self.native.manifest();
        let checkpoint = self.overlay.checkpoint()?;
        if let Some(checkpoint) = checkpoint {
            self.native.validate_overlay_checkpoint(
                checkpoint.manifest_generation,
                checkpoint.durable_end,
                checkpoint.manifest_checksum,
            )?;
        }
        let mut orphan_records = 0usize;
        let mut continuation = None;
        'pages: loop {
            let page = self.native.scan_due(
                None,
                i64::MAX,
                max_records.max(1),
                max_records.max(1).saturating_mul(TimelineSegmentRecord::encoded_size()),
                continuation,
            )?;
            for record in &page.records {
                if self.overlay.materialized_marker(record.source_cq_offset)?.is_none() {
                    orphan_records = orphan_records.saturating_add(1);
                    if orphan_records >= max_records {
                        break 'pages;
                    }
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        Ok(SegmentedRecoveryState {
            manifest_generation: manifest.generation,
            durable_end: manifest.durable_end,
            checkpoint_generation: checkpoint.map_or(0, |checkpoint| checkpoint.generation),
            orphan_records,
        })
    }
}

/// Backend-neutral adapter for conformance tests and migration shadow comparisons.
pub(crate) struct SegmentedTimelineIndex {
    native: Arc<SegmentedTimeline>,
    timeline: Arc<RocksDbTimelineIndex>,
    state: Arc<RocksDbTimelineStateIndex>,
    coordinator: Arc<SegmentedCommitCoordinator>,
    storage_io: BlockingExecutor,
    pins: Mutex<HashMap<u64, (NativeSnapshotPin, TimelineSnapshotPin)>>,
}

impl SegmentedTimelineIndex {
    pub(crate) fn new(
        native: Arc<SegmentedTimeline>,
        timeline: Arc<RocksDbTimelineIndex>,
        storage_io: BlockingExecutor,
    ) -> Self {
        Self {
            state: Arc::new(timeline.state_index()),
            coordinator: Arc::new(SegmentedCommitCoordinator::new(
                Arc::clone(&native),
                Arc::clone(&timeline),
            )),
            native,
            timeline,
            storage_io,
            pins: Mutex::new(HashMap::new()),
        }
    }
}

impl TimerIndex for SegmentedTimelineIndex {
    async fn put_batch(&self, records: Vec<TimerSourceRecord>, budget: WorkBudget) -> Result<usize, TimerEngineError> {
        let mut selected = Vec::new();
        let mut retained_bytes = 0usize;
        for record in records {
            let next_bytes = retained_bytes.saturating_add(record.estimated_bytes);
            if !budget.allows(selected.len().saturating_add(1), next_bytes) {
                break;
            }
            retained_bytes = next_bytes;
            selected.push(record);
        }
        if selected.is_empty() {
            return Ok(0);
        }
        let coordinator = Arc::clone(&self.coordinator);
        self.storage_io
            .spawn_io("timer.segmented.put_batch", move || {
                let entries = selected.iter().map(source_to_entry).collect::<Result<Vec<_>, _>>()?;
                let mut batch = RocksDbWriteBatch::with_capacity(entries.len().saturating_mul(2));
                for (source, entry) in selected.iter().zip(&entries) {
                    RocksDbTimelineStateIndex::append_state(
                        &mut batch,
                        entry.key.timer_id,
                        entry.key.generation,
                        &TimelineStateRecordV1 {
                            state: TimelineState::Pending,
                            state_version: 0,
                            route: source.route.clone(),
                            admission_epoch: TimerEngineEpoch::new(1),
                            owner_epoch: TimerEngineEpoch::new(1),
                            claim_seq: 0,
                            due_time_ms: entry.key.due_time_ms,
                            lane: entry.key.lane,
                            terminal_at_ms: 0,
                            shadow_only: false,
                        },
                    )
                    .map_err(storage_error)?;
                }
                let last = entries.last().ok_or(TimerEngineError::InvalidBudget)?;
                coordinator
                    .commit(
                        &entries,
                        batch,
                        TimelineCheckpointV1 {
                            materialized_source_offset: last.record.source_cq_offset,
                            due_cursor: TimerTimelineCursor::default(),
                            completion_cursor: TimerTimelineCursor::default(),
                            format_fingerprint: 1,
                            generation: 1,
                        },
                    )
                    .map_err(commit_error)?;
                Ok(entries.len())
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))?
    }

    async fn scan_due(
        &self,
        from: Option<TimerIndexCursor>,
        due_exclusive_ms: i64,
        budget: WorkBudget,
    ) -> Result<TimerIndexPage, TimerEngineError> {
        let native = Arc::clone(&self.native);
        let state = Arc::clone(&self.state);
        self.storage_io
            .spawn_io("timer.segmented.scan_due", move || {
                let from_key = from.as_ref().map(index_cursor_key);
                let native_cursor = from.as_ref().and_then(index_cursor_native);
                let page = native
                    .scan_due(
                        from_key,
                        due_exclusive_ms,
                        budget.max_messages,
                        budget
                            .max_messages
                            .saturating_mul(TimelineSegmentRecord::encoded_size()),
                        native_cursor,
                    )
                    .map_err(local_error)?;
                let keys = page
                    .records
                    .iter()
                    .map(|record| (record.key.timer_id, record.key.generation))
                    .collect::<Vec<_>>();
                let states = state.get_many(&keys).map_err(storage_error)?;
                let mut records = Vec::new();
                let mut retained_bytes = 0usize;
                let mut last_consumed = from_key;
                let mut budget_exhausted = false;
                for (native_record, state) in page.records.into_iter().zip(states) {
                    let state = state.ok_or_else(|| {
                        TimerEngineError::Storage(std::io::Error::other("native Timeline record has no overlay state"))
                    })?;
                    if state.state.is_terminal() {
                        last_consumed = Some(native_record.key);
                        continue;
                    }
                    let due = native_to_due(native_record)?;
                    let next_bytes = retained_bytes.saturating_add(due.source.estimated_bytes);
                    if !budget.allows(records.len().saturating_add(1), next_bytes) {
                        budget_exhausted = true;
                        break;
                    }
                    retained_bytes = next_bytes;
                    last_consumed = Some(native_record.key);
                    records.push(due);
                }
                let continuation = if budget_exhausted {
                    last_consumed.map(ordered_cursor)
                } else {
                    page.continuation.map(native_cursor_to_index)
                };
                Ok(TimerIndexPage { records, continuation })
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))?
    }

    async fn set_state(
        &self,
        timer_id: TimerId,
        generation: u64,
        state: TimerRecordState,
    ) -> Result<(), TimerEngineError> {
        let state_index = Arc::clone(&self.state);
        self.storage_io
            .spawn_io("timer.segmented.set_state", move || {
                let generation = TimerGeneration::new(generation);
                let Some(mut current) = state_index.get(timer_id, generation).map_err(storage_error)? else {
                    return Ok(());
                };
                current.state = map_state(state);
                current.state_version = current.state_version.saturating_add(1);
                state_index.put(timer_id, generation, &current).map_err(storage_error)
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))?
    }

    async fn checkpoint(&self, checkpoint: TimerIndexCheckpoint) -> Result<(), TimerEngineError> {
        let timeline = Arc::clone(&self.timeline);
        self.storage_io
            .spawn_io("timer.segmented.checkpoint", move || {
                timeline
                    .put_batch(
                        &[],
                        Some((
                            TimelineCheckpointKind::Due,
                            0,
                            TimelineCheckpointV1 {
                                materialized_source_offset: TimerSourceCqOffset::new(-1),
                                due_cursor: checkpoint.cursor,
                                completion_cursor: TimerTimelineCursor::default(),
                                format_fingerprint: 1,
                                generation: checkpoint.epoch.get(),
                            },
                        )),
                    )
                    .map(|_| ())
                    .map_err(storage_error)
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))?
    }

    async fn load_checkpoint(&self) -> Result<Option<TimerIndexCheckpoint>, TimerEngineError> {
        let timeline = Arc::clone(&self.timeline);
        self.storage_io
            .spawn_io("timer.segmented.load_checkpoint", move || {
                timeline
                    .checkpoint(TimelineCheckpointKind::Due, 0)
                    .map(|checkpoint| {
                        checkpoint.map(|checkpoint| TimerIndexCheckpoint {
                            cursor: checkpoint.due_cursor,
                            epoch: TimerEngineEpoch::new(checkpoint.generation),
                        })
                    })
                    .map_err(storage_error)
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))?
    }

    async fn pin_snapshot(&self, gc_fence: TimerTimelineCursor) -> Result<TimerSnapshotPin, TimerEngineError> {
        let generation = self.native.manifest().generation.saturating_add(1).max(1);
        let native = self.native.pin_snapshot(generation).map_err(local_error)?;
        let rocks = self
            .timeline
            .pin_snapshot_generation(
                TimelineKeyV1 {
                    due_time_ms: gc_fence.due_time_ms(),
                    lane: 0,
                    timer_id: TimerId::new(0),
                    generation: TimerGeneration::new(0),
                },
                generation,
            )
            .map_err(storage_error)?;
        self.pins.lock().insert(generation, (native, rocks));
        Ok(TimerSnapshotPin { generation, gc_fence })
    }

    async fn release_snapshot(&self, pin: TimerSnapshotPin) -> Result<(), TimerEngineError> {
        let Some((native, rocks)) = self.pins.lock().remove(&pin.generation) else {
            return Err(TimerEngineError::Storage(std::io::Error::other(
                "unknown segmented snapshot pin",
            )));
        };
        self.native.release_snapshot(native).map_err(local_error)?;
        self.timeline.release_snapshot(rocks).map_err(storage_error)
    }

    async fn gc(&self, fence: TimerTimelineCursor, budget: WorkBudget) -> Result<usize, TimerEngineError> {
        let native = Arc::clone(&self.native);
        let state = Arc::clone(&self.state);
        self.storage_io
            .spawn_io("timer.segmented.gc", move || {
                let page = native
                    .scan_due(
                        None,
                        fence.due_time_ms(),
                        budget.max_messages,
                        budget
                            .max_messages
                            .saturating_mul(TimelineSegmentRecord::encoded_size()),
                        None,
                    )
                    .map_err(local_error)?;
                let keys = page
                    .records
                    .iter()
                    .map(|record| (record.key.timer_id, record.key.generation))
                    .collect::<Vec<_>>();
                let states = state.get_many(&keys).map_err(storage_error)?;
                Ok(states
                    .into_iter()
                    .filter(|state| state.as_ref().is_some_and(|state| state.state.is_terminal()))
                    .count())
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error)))?
    }
}

fn source_to_entry(source: &TimerSourceRecord) -> Result<TimelineIndexEntry, TimerEngineError> {
    let lane = 0u16;
    Ok(TimelineIndexEntry {
        key: TimelineKeyV1 {
            due_time_ms: source.due_time_ms,
            lane,
            timer_id: source.id,
            generation: source.route.generation(),
        },
        record: TimelineRecordV1 {
            payload: TimerPayloadStoreLocator::try_new(
                i32::try_from(source.due_time_ms.div_euclid(86_400_000))
                    .map_err(|_| TimerEngineError::InvalidBudget)?,
                lane,
                0,
                u64::try_from(source.payload.commit_log_offset()).map_err(|_| TimerEngineError::InvalidBudget)?,
                source.payload.size(),
                1,
            )
            .map_err(|_| TimerEngineError::InvalidBudget)?,
            source_cq_offset: source.source_offset,
            source_physical_offset: source.payload.commit_log_offset(),
            source_size: source.payload.size(),
            state_version: 0,
            owner_engine: source.route.engine_id(),
            shadow_only: false,
        },
    })
}

pub(crate) fn entry_to_native(entry: TimelineIndexEntry) -> Result<TimelineSegmentRecord, SegmentedCommitError> {
    Ok(TimelineSegmentRecord {
        key: TimelineSegmentKey {
            due_time_ms: entry.key.due_time_ms,
            lane: entry.key.lane,
            timer_id: entry.key.timer_id,
            generation: entry.key.generation,
        },
        payload: entry.record.payload,
        source_cq_offset: entry.record.source_cq_offset,
        source_physical_offset: entry.record.source_physical_offset,
        source_size: entry.record.source_size,
        state_version: entry.record.state_version,
        owner_engine: entry.record.owner_engine,
        shadow_only: entry.record.shadow_only,
    })
}

fn native_to_due(record: TimelineSegmentRecord) -> Result<DueTimerRecord, TimerEngineError> {
    let route = PersistedTimerRoute::try_new(
        record.owner_engine,
        1,
        1,
        record.key.generation,
        format!("native:{}:{}", record.key.timer_id.get(), record.key.generation.get()),
    )
    .map_err(|_| TimerEngineError::InvalidBudget)?;
    Ok(DueTimerRecord {
        source: TimerSourceRecord {
            id: record.key.timer_id,
            source_offset: record.source_cq_offset,
            due_time_ms: record.key.due_time_ms,
            payload: TimerPayloadLocator::try_new(record.source_physical_offset, record.source_size)
                .map_err(|_| TimerEngineError::InvalidBudget)?,
            route,
            estimated_bytes: record.source_size as usize,
        },
        cursor: TimerTimelineCursor::new(record.key.due_time_ms, record.source_cq_offset.get() as u64),
        shard: u32::from(record.key.lane),
    })
}

fn index_cursor_key(cursor: &TimerIndexCursor) -> TimelineSegmentKey {
    TimelineSegmentKey {
        due_time_ms: cursor.due_time_ms,
        lane: cursor.lane,
        timer_id: cursor.timer_id,
        generation: TimerGeneration::new(cursor.generation),
    }
}

fn index_cursor_native(cursor: &TimerIndexCursor) -> Option<SegmentedTimelineContinuation> {
    match &cursor.backend {
        TimerIndexBackendCursor::OrderedKey => None,
        TimerIndexBackendCursor::Segmented {
            manifest_generation,
            due_day_utc,
            due_hour_utc,
            run_positions,
        } => Some(SegmentedTimelineContinuation {
            manifest_generation: *manifest_generation,
            partition: TimelinePartitionKey {
                due_day_utc: *due_day_utc,
                due_hour_utc: *due_hour_utc,
                lane: cursor.lane,
            },
            run_positions: run_positions.clone(),
            last_key: Some(index_cursor_key(cursor)),
        }),
    }
}

fn native_cursor_to_index(cursor: SegmentedTimelineContinuation) -> TimerIndexCursor {
    let key = cursor.last_key.unwrap_or(TimelineSegmentKey {
        due_time_ms: i64::MIN,
        lane: cursor.partition.lane,
        timer_id: TimerId::new(0),
        generation: TimerGeneration::new(0),
    });
    TimerIndexCursor {
        due_time_ms: key.due_time_ms,
        lane: key.lane,
        timer_id: key.timer_id,
        generation: key.generation.get(),
        backend: TimerIndexBackendCursor::Segmented {
            manifest_generation: cursor.manifest_generation,
            due_day_utc: cursor.partition.due_day_utc,
            due_hour_utc: cursor.partition.due_hour_utc,
            run_positions: cursor.run_positions,
        },
    }
}

fn ordered_cursor(key: TimelineSegmentKey) -> TimerIndexCursor {
    TimerIndexCursor::ordered_key(key.due_time_ms, key.lane, key.timer_id, key.generation.get())
}

const fn durability(receipt: NativeWriteReceipt) -> NativeDurabilityV1 {
    NativeDurabilityV1 {
        manifest_generation: receipt.manifest_generation,
        durable_end: receipt.durable_end,
        record_hash: receipt.record_hash,
        manifest_checksum: receipt.manifest_checksum,
    }
}

const fn map_state(state: TimerRecordState) -> TimelineState {
    match state {
        TimerRecordState::Pending => TimelineState::Pending,
        TimerRecordState::Delivering => TimelineState::Delivering,
        TimerRecordState::Delivered => TimelineState::Delivered,
        TimerRecordState::Cancelled => TimelineState::Cancelled,
        TimerRecordState::Quarantined => TimelineState::Quarantined,
    }
}

fn storage_error(error: rocketmq_error::RocketMQError) -> TimerEngineError {
    TimerEngineError::Storage(std::io::Error::other(error))
}

fn local_error(error: crate::store_error::StoreError) -> TimerEngineError {
    TimerEngineError::Storage(std::io::Error::other(error))
}

fn commit_error(error: SegmentedCommitError) -> TimerEngineError {
    TimerEngineError::Storage(std::io::Error::other(error))
}

/// Native/overlay commit or recovery failure.
#[derive(Debug, Error)]
pub(crate) enum SegmentedCommitError {
    #[error(transparent)]
    Store(#[from] crate::store_error::StoreError),
    #[error(transparent)]
    Rocks(#[from] rocketmq_error::RocketMQError),
    #[error("segmented commit batch is empty")]
    EmptyBatch,
    #[error("injected segmented commit crash at {0:?}")]
    InjectedCrash(SegmentedCommitCrashPoint),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_storage_source<T>(error: TimerEngineError)
    where
        T: std::error::Error + Send + Sync + 'static,
    {
        let TimerEngineError::Storage(error) = error else {
            panic!("expected timer storage error");
        };
        assert!(error.get_ref().is_some_and(|source| source.is::<T>()));
    }

    #[test]
    fn local_error_preserves_segmented_timeline_error_as_source() {
        assert_storage_source::<crate::store_error::StoreError>(local_error(crate::store_error::StoreError::new(
            &rocketmq_error::STORAGE_WRITE_FAILED,
            crate::store_error::StoreOperation::Append,
        )));
    }

    #[test]
    fn commit_error_preserves_segmented_commit_error_as_source() {
        assert_storage_source::<SegmentedCommitError>(commit_error(SegmentedCommitError::EmptyBatch));
    }
}
