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
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadLocator;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::TimerTimelineCursor;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointKind;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointV1;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineSnapshotPin;

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

/// Backend-neutral adapter over the dedicated RocksDB Timeline database.
pub(crate) struct RocksDbTimerIndex {
    index: Arc<RocksDbTimelineIndex>,
    state: Arc<RocksDbTimelineStateIndex>,
    storage_io: BlockingExecutor,
    pins: Mutex<HashMap<u64, TimelineSnapshotPin>>,
}

impl RocksDbTimerIndex {
    pub(crate) fn new(index: Arc<RocksDbTimelineIndex>, storage_io: BlockingExecutor) -> Self {
        Self {
            state: Arc::new(index.state_index()),
            index,
            storage_io,
            pins: Mutex::new(HashMap::new()),
        }
    }
}

impl TimerIndex for RocksDbTimerIndex {
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
        let index = Arc::clone(&self.index);
        self.storage_io
            .spawn_io("timer.timeline.put_batch", move || {
                let entries = selected
                    .iter()
                    .map(source_to_entry)
                    .collect::<Result<Vec<_>, TimerEngineError>>()?;
                let mut batch = rocketmq_store_rocksdb::batch::RocksDbWriteBatch::with_capacity(entries.len() * 2);
                for (source, entry) in selected.iter().zip(&entries) {
                    RocksDbTimelineIndex::append_entry(&mut batch, entry).map_err(storage_error)?;
                    RocksDbTimelineStateIndex::append_state(
                        &mut batch,
                        entry.key.timer_id,
                        entry.key.generation,
                        &TimelineStateRecordV1 {
                            state: TimelineState::Pending,
                            state_version: 0,
                            route: source.route.clone(),
                            admission_epoch: rocketmq_store_api::TimerEngineEpoch::new(1),
                            owner_epoch: rocketmq_store_api::TimerEngineEpoch::new(1),
                            claim_seq: 0,
                            due_time_ms: entry.key.due_time_ms,
                            lane: entry.key.lane,
                            terminal_at_ms: 0,
                            shadow_only: false,
                        },
                    )
                    .map_err(storage_error)?;
                }
                index.write_batch(&batch).map_err(storage_error)?;
                Ok(entries.len())
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error.to_string())))?
    }

    async fn scan_due(
        &self,
        from: Option<TimerIndexCursor>,
        due_exclusive_ms: i64,
        budget: WorkBudget,
    ) -> Result<TimerIndexPage, TimerEngineError> {
        let index = Arc::clone(&self.index);
        self.storage_io
            .spawn_io("timer.timeline.scan_due", move || {
                let continuation = from.map(index_cursor_to_key);
                let start = continuation.map_or(i64::MIN, |cursor| cursor.due_time_ms);
                let encoded_budget = budget.max_bytes.saturating_add(
                    budget
                        .max_messages
                        .saturating_mul(TimelineKeyV1::encoded_size() + TimelineRecordV1::encoded_size()),
                );
                let page = index
                    .range_scan(
                        start,
                        due_exclusive_ms,
                        continuation,
                        budget.max_messages,
                        encoded_budget,
                    )
                    .map_err(storage_error)?;
                let mut records = Vec::with_capacity(page.entries.len());
                let mut retained_bytes = 0usize;
                let mut last_key = None;
                let mut budget_exhausted = false;
                for entry in page.entries {
                    let state = index
                        .state_index()
                        .get(entry.key.timer_id, entry.key.generation)
                        .map_err(storage_error)?;
                    if state.is_some_and(|state| {
                        matches!(
                            state.state,
                            TimelineState::Delivered | TimelineState::Cancelled | TimelineState::Quarantined
                        )
                    }) {
                        last_key = Some(entry.key);
                        continue;
                    }
                    let record = entry_to_due_record(entry)?;
                    let next_bytes = retained_bytes.saturating_add(record.source.estimated_bytes);
                    if !budget.allows(records.len().saturating_add(1), next_bytes) {
                        budget_exhausted = true;
                        break;
                    }
                    retained_bytes = next_bytes;
                    last_key = Some(entry.key);
                    records.push(record);
                }
                Ok(TimerIndexPage {
                    records,
                    continuation: (if budget_exhausted { last_key } else { page.continuation })
                        .map(key_to_index_cursor),
                })
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error.to_string())))?
    }

    async fn set_state(
        &self,
        timer_id: TimerId,
        generation: u64,
        state: TimerRecordState,
    ) -> Result<(), TimerEngineError> {
        let state_index = Arc::clone(&self.state);
        self.storage_io
            .spawn_io("timer.timeline.set_state", move || {
                let generation = TimerGeneration::new(generation);
                let Some(mut current) = state_index.get(timer_id, generation).map_err(storage_error)? else {
                    return Ok(());
                };
                current.state = map_state(state);
                current.state_version = current.state_version.saturating_add(1);
                state_index.put(timer_id, generation, &current).map_err(storage_error)
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error.to_string())))?
    }

    async fn checkpoint(&self, checkpoint: TimerIndexCheckpoint) -> Result<(), TimerEngineError> {
        let index = Arc::clone(&self.index);
        self.storage_io
            .spawn_io("timer.timeline.checkpoint", move || {
                index
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
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error.to_string())))?
    }

    async fn load_checkpoint(&self) -> Result<Option<TimerIndexCheckpoint>, TimerEngineError> {
        let index = Arc::clone(&self.index);
        self.storage_io
            .spawn_io("timer.timeline.load_checkpoint", move || {
                index
                    .checkpoint(TimelineCheckpointKind::Due, 0)
                    .map(|checkpoint| {
                        checkpoint.map(|checkpoint| TimerIndexCheckpoint {
                            cursor: checkpoint.due_cursor,
                            epoch: rocketmq_store_api::TimerEngineEpoch::new(checkpoint.generation),
                        })
                    })
                    .map_err(storage_error)
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error.to_string())))?
    }

    async fn pin_snapshot(&self, gc_fence: TimerTimelineCursor) -> Result<TimerSnapshotPin, TimerEngineError> {
        let index = Arc::clone(&self.index);
        let pin = self
            .storage_io
            .spawn_io("timer.timeline.pin_snapshot", move || {
                index
                    .pin_snapshot(TimelineKeyV1 {
                        due_time_ms: gc_fence.due_time_ms(),
                        lane: 0,
                        timer_id: TimerId::new(0),
                        generation: TimerGeneration::new(0),
                    })
                    .map_err(storage_error)
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error.to_string())))??;
        self.pins.lock().insert(pin.generation, pin);
        Ok(TimerSnapshotPin {
            generation: pin.generation,
            gc_fence,
        })
    }

    async fn release_snapshot(&self, pin: TimerSnapshotPin) -> Result<(), TimerEngineError> {
        let Some(native) = self.pins.lock().remove(&pin.generation) else {
            return Err(TimerEngineError::Storage(std::io::Error::other(
                "unknown Timeline snapshot pin",
            )));
        };
        let index = Arc::clone(&self.index);
        self.storage_io
            .spawn_io("timer.timeline.release_snapshot", move || {
                index.release_snapshot(native).map_err(storage_error)
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error.to_string())))?
    }

    async fn gc(&self, fence: TimerTimelineCursor, budget: WorkBudget) -> Result<usize, TimerEngineError> {
        let index = Arc::clone(&self.index);
        self.storage_io
            .spawn_io("timer.timeline.gc", move || {
                let candidates = index
                    .gc_candidates(
                        TimelineKeyV1 {
                            due_time_ms: fence.due_time_ms(),
                            lane: 0,
                            timer_id: TimerId::new(0),
                            generation: TimerGeneration::new(0),
                        },
                        budget.max_messages,
                    )
                    .map_err(storage_error)?;
                let state = index.state_index();
                let mut batch = rocketmq_store_rocksdb::batch::RocksDbWriteBatch::with_capacity(candidates.len());
                let mut removed = 0usize;
                for entry in candidates {
                    let terminal = state
                        .get(entry.key.timer_id, entry.key.generation)
                        .map_err(storage_error)?
                        .is_some_and(|state| {
                            matches!(
                                state.state,
                                TimelineState::Delivered | TimelineState::Cancelled | TimelineState::Quarantined
                            )
                        });
                    if terminal {
                        RocksDbTimelineIndex::append_delete_entry(&mut batch, entry.key);
                        removed = removed.saturating_add(1);
                    }
                }
                index.write_batch(&batch).map_err(storage_error)?;
                Ok(removed)
            })
            .await
            .map_err(|error| TimerEngineError::Storage(std::io::Error::other(error.to_string())))?
    }
}

fn source_to_entry(source: &TimerSourceRecord) -> Result<TimelineIndexEntry, TimerEngineError> {
    let lane = 0u16;
    let payload = TimerPayloadStoreLocator::try_new(
        i32::try_from(source.due_time_ms.div_euclid(86_400_000)).map_err(|_| TimerEngineError::InvalidBudget)?,
        lane,
        0,
        u64::try_from(source.payload.commit_log_offset()).map_err(|_| TimerEngineError::InvalidBudget)?,
        source.payload.size(),
        0,
    )
    .map_err(|_| TimerEngineError::InvalidBudget)?;
    Ok(TimelineIndexEntry {
        key: TimelineKeyV1 {
            due_time_ms: source.due_time_ms,
            lane,
            timer_id: source.id,
            generation: source.route.generation(),
        },
        record: TimelineRecordV1 {
            payload,
            source_cq_offset: source.source_offset,
            source_physical_offset: source.payload.commit_log_offset(),
            source_size: source.payload.size(),
            state_version: 1,
            owner_engine: source.route.engine_id(),
            shadow_only: false,
        },
    })
}

fn entry_to_due_record(entry: TimelineIndexEntry) -> Result<DueTimerRecord, TimerEngineError> {
    let route = rocketmq_store_api::PersistedTimerRoute::try_new(
        entry.record.owner_engine,
        1,
        1,
        entry.key.generation,
        format!("timeline:{}:{}", entry.key.timer_id.get(), entry.key.generation.get()),
    )
    .map_err(|_| TimerEngineError::InvalidBudget)?;
    Ok(DueTimerRecord {
        source: TimerSourceRecord {
            id: entry.key.timer_id,
            source_offset: entry.record.source_cq_offset,
            due_time_ms: entry.key.due_time_ms,
            payload: TimerPayloadLocator::try_new(entry.record.source_physical_offset, entry.record.source_size)
                .map_err(|_| TimerEngineError::InvalidBudget)?,
            route,
            estimated_bytes: entry.record.source_size as usize,
        },
        cursor: TimerTimelineCursor::new(entry.key.due_time_ms, entry.record.source_cq_offset.get() as u64),
        shard: u32::from(entry.key.lane),
    })
}

const fn key_to_index_cursor(key: TimelineKeyV1) -> TimerIndexCursor {
    TimerIndexCursor::ordered_key(key.due_time_ms, key.lane, key.timer_id, key.generation.get())
}

fn index_cursor_to_key(cursor: TimerIndexCursor) -> TimelineKeyV1 {
    debug_assert!(matches!(cursor.backend, TimerIndexBackendCursor::OrderedKey));
    TimelineKeyV1 {
        due_time_ms: cursor.due_time_ms,
        lane: cursor.lane,
        timer_id: cursor.timer_id,
        generation: TimerGeneration::new(cursor.generation),
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
    TimerEngineError::Storage(std::io::Error::other(error.to_string()))
}
