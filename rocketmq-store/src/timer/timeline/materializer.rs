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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::TimerTimelineCursor;
use rocketmq_store_local::timer::payload_record::TimerPayloadRecordV1;
use rocketmq_store_local::timer::payload_store::TimerPayloadStore;
use rocketmq_store_local::timer::payload_store::TimerPayloadStoreConfig;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointKind;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointV1;
use rocketmq_store_rocksdb::timer::codec::RecallLookupKeyV1;
use rocketmq_store_rocksdb::timer::codec::RecallLookupValueV1;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::ShadowObservationKind;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;
use thiserror::Error;

use crate::config::timer_store_config::TimerStoreConfig;
use crate::log_file::commit_log::CommitLogReadHandle;
use crate::queue::local_file_consume_queue_store::ConsumeQueueLookupHandle;
use crate::timer::delivery::delivery_shard;
use crate::timer::timer_message_store::TIMER_TOPIC;

use super::shadow::ShadowExpectedRecord;
use super::shadow::ShadowReconciliationSnapshot;
use super::ShadowReconciler;
use super::TimelineIndexMigrationManager;
use super::TimelineReadyOutbox;
use super::TimelineRecallService;

const SOURCE_CHECKPOINT_LANE: u16 = 0;
const SHADOW_DUE_CHECKPOINT_LANE: u16 = u16::MAX;

/// CQ-owned, payload-first Java-compatible shadow materializer.
///
/// A successful iteration has one durable order: payload files and manifests are
/// synced first, then source markers, shadow indexes, observations, and the
/// contiguous CQ checkpoint are committed in one sync-WAL RocksDB batch.
pub(crate) struct ShadowTimelineMaterializer {
    config: TimerStoreConfig,
    consume_queues: ConsumeQueueLookupHandle,
    commit_log: CommitLogReadHandle,
    payload_store: Arc<TimerPayloadStore>,
    timeline: Arc<RocksDbTimelineIndex>,
    first_unmaterialized_physical_offset: Arc<AtomicI64>,
    format_fingerprint: u64,
    reconciler: Arc<ShadowReconciler>,
    snapshot_barrier: Arc<RwLock<()>>,
    run_lock: Mutex<()>,
    mode: TimelineMaterializationMode,
    index_migration: Option<Arc<TimelineIndexMigrationManager>>,
    materialized_records: AtomicU64,
    materialized_bytes: AtomicU64,
    materialization_failures: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimelineMaterializationMode {
    Shadow,
    Formal { admission_epoch: TimerEngineEpoch },
}

impl ShadowTimelineMaterializer {
    /// Opens the independent payload and Timeline stores and restores the cleanup fence.
    pub(crate) fn open(
        store_root: impl AsRef<Path>,
        config: TimerStoreConfig,
        consume_queues: ConsumeQueueLookupHandle,
        commit_log: CommitLogReadHandle,
        first_unmaterialized_physical_offset: Arc<AtomicI64>,
    ) -> Result<Self, TimelineMaterializerError> {
        Self::open_with_mode(
            store_root,
            config,
            consume_queues,
            commit_log,
            first_unmaterialized_physical_offset,
            TimelineMaterializationMode::Shadow,
        )
    }

    /// Opens a production materializer whose new records are owned by Extended Timeline.
    pub(crate) fn open_formal(
        store_root: impl AsRef<Path>,
        config: TimerStoreConfig,
        consume_queues: ConsumeQueueLookupHandle,
        commit_log: CommitLogReadHandle,
        first_unmaterialized_physical_offset: Arc<AtomicI64>,
        admission_epoch: TimerEngineEpoch,
    ) -> Result<Self, TimelineMaterializerError> {
        if admission_epoch.get() == 0 {
            return Err(TimelineMaterializerError::InvalidAdmissionEpoch);
        }
        Self::open_with_mode(
            store_root,
            config,
            consume_queues,
            commit_log,
            first_unmaterialized_physical_offset,
            TimelineMaterializationMode::Formal { admission_epoch },
        )
    }

    fn open_with_mode(
        store_root: impl AsRef<Path>,
        config: TimerStoreConfig,
        consume_queues: ConsumeQueueLookupHandle,
        commit_log: CommitLogReadHandle,
        first_unmaterialized_physical_offset: Arc<AtomicI64>,
        mode: TimelineMaterializationMode,
    ) -> Result<Self, TimelineMaterializerError> {
        config.validate()?;
        let mut payload_config = TimerPayloadStoreConfig::for_store_root(store_root.as_ref());
        payload_config.segment_bytes = config.payload_segment_bytes;
        payload_config.max_open_handles = config.payload_open_handles;
        payload_config.batch_bytes = config.payload_batch_bytes;
        payload_config.max_record_bytes = config.payload_record_bytes;
        payload_config.max_partition_live_bytes = config.payload_partition_live_bytes;
        let payload_store =
            Arc::new(TimerPayloadStore::new(payload_config)?.ok_or(TimelineMaterializerError::InvalidPayloadConfig)?);
        payload_store.load()?;
        let timeline = Arc::new(RocksDbTimelineIndex::open(store_root)?);
        let reconciler = Arc::new(ShadowReconciler::new(
            Arc::clone(&timeline),
            Arc::clone(&payload_store),
            config.shadow_diff_sample_limit,
        ));
        let format_fingerprint = config_fingerprint(&config);
        let materializer = Self {
            config,
            consume_queues,
            commit_log,
            payload_store,
            timeline,
            first_unmaterialized_physical_offset,
            format_fingerprint,
            reconciler,
            snapshot_barrier: Arc::new(RwLock::new(())),
            run_lock: Mutex::new(()),
            mode,
            index_migration: None,
            materialized_records: AtomicU64::new(0),
            materialized_bytes: AtomicU64::new(0),
            materialization_failures: AtomicU64::new(0),
        };
        materializer.restore_cleanup_fence()?;
        Ok(materializer)
    }

    pub(crate) fn timeline(&self) -> Arc<RocksDbTimelineIndex> {
        Arc::clone(&self.timeline)
    }

    pub(crate) fn payload_store(&self) -> Arc<TimerPayloadStore> {
        Arc::clone(&self.payload_store)
    }

    pub(crate) fn reconciler(&self) -> Arc<ShadowReconciler> {
        Arc::clone(&self.reconciler)
    }

    pub(crate) fn snapshot_barrier(&self) -> Arc<RwLock<()>> {
        Arc::clone(&self.snapshot_barrier)
    }

    /// Routes formal derived commits through the managed online index migration adapter.
    pub(crate) fn with_index_migration(mut self, migration: Arc<TimelineIndexMigrationManager>) -> Self {
        self.index_migration = Some(migration);
        self
    }

    pub(crate) fn snapshot_source_cursors(&self) -> Result<(i64, i64, u64), TimelineMaterializerError> {
        let checkpoint = self.source_checkpoint()?;
        Ok((
            checkpoint.materialized_source_offset.get().saturating_add(1),
            self.first_unmaterialized_physical_offset.load(Ordering::Acquire).max(0),
            checkpoint.format_fingerprint,
        ))
    }

    /// Returns bounded operational counters without scanning Timeline keys.
    pub(crate) fn metrics(&self) -> ExtendedTimelineShadowMetrics {
        let checkpoint = self.source_checkpoint().ok();
        let materialized_offset = checkpoint
            .map(|checkpoint| checkpoint.materialized_source_offset.get())
            .unwrap_or(-1);
        let source_max_offset = self
            .consume_queues
            .find_or_create_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
            .map(|queue| queue.read().get_max_offset_in_queue())
            .unwrap_or_default();
        let payload = self.payload_store.metrics();
        let rocks = self.timeline.store().metrics();
        let ticker = self.timeline.store().ticker_metrics();
        ExtendedTimelineShadowMetrics {
            materialization_lag: source_max_offset.saturating_sub(materialized_offset.saturating_add(1)) as u64,
            materialized_records: self.materialized_records.load(Ordering::Relaxed),
            materialized_bytes: self.materialized_bytes.load(Ordering::Relaxed),
            materialization_failures: self.materialization_failures.load(Ordering::Relaxed),
            payload_live_bytes: payload.live_bytes,
            payload_records: payload.record_count,
            payload_partitions: payload.partition_count,
            payload_open_handles: payload.open_handle_count,
            timeline_bytes_written: ticker.bytes_written,
            timeline_bytes_read: ticker.bytes_read,
            timeline_errors: rocks.error_count,
            reconciliation: self.reconciler.snapshot(),
        }
    }

    /// Recomputes the fail-closed cleanup fence after ConsumeQueue recovery.
    pub(crate) fn refresh_cleanup_fence(&self) -> Result<(), TimelineMaterializerError> {
        self.restore_cleanup_fence()
    }

    /// Closes the dedicated Timeline database after all owned workers stop.
    pub(crate) fn close(&self) {
        self.timeline.close();
    }

    /// Materializes one bounded, physically ordered Timer CQ batch.
    pub(crate) fn run_once(&self) -> Result<usize, TimelineMaterializerError> {
        self.run_once_with_budget(
            self.config.materialize_batch_messages,
            self.config.materialize_batch_bytes,
        )
    }

    /// Materializes one batch bounded by both the engine and storage limits.
    pub(crate) fn run_once_with_budget(
        &self,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<usize, TimelineMaterializerError> {
        let _run_guard = self.run_lock.lock();
        let _snapshot_guard = self.snapshot_barrier.read();
        let result = self.run_once_inner(
            max_messages.min(self.config.materialize_batch_messages),
            max_bytes.min(self.config.materialize_batch_bytes),
        );
        if result.is_err() {
            self.materialization_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn run_once_inner(&self, max_messages: usize, max_bytes: usize) -> Result<usize, TimelineMaterializerError> {
        let checkpoint = self.source_checkpoint()?;
        let due_checkpoint_lane = match self.mode {
            TimelineMaterializationMode::Shadow => SHADOW_DUE_CHECKPOINT_LANE,
            TimelineMaterializationMode::Formal { .. } => 0,
        };
        let due_cursor = self
            .timeline
            .checkpoint(TimelineCheckpointKind::Due, due_checkpoint_lane)?
            .map(|checkpoint| checkpoint.due_cursor.due_time_ms())
            .unwrap_or_default();
        let start_offset = checkpoint.materialized_source_offset.get().saturating_add(1);
        let Some(queue) = self
            .consume_queues
            .find_or_create_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
        else {
            return Err(TimelineMaterializerError::SourceUnavailable);
        };
        let sources = {
            let queue = queue.read();
            let Some(iter) =
                queue.iterate_from_with_count(start_offset, i32::try_from(max_messages).unwrap_or(i32::MAX))
            else {
                self.publish_cleanup_fence(None);
                return Ok(0);
            };
            iter.map(|unit| SourceIdentity {
                cq_offset: unit.queue_offset,
                physical_offset: unit.pos,
                size: unit.size,
            })
            .collect::<Vec<_>>()
        };
        if sources.is_empty() {
            self.publish_cleanup_fence(None);
            return Ok(0);
        }

        let mut prepared = Vec::new();
        let mut retained_bytes = 0usize;
        for source in sources {
            let size = usize::try_from(source.size).map_err(|_| TimelineMaterializerError::InvalidSourceSize)?;
            if prepared.is_empty() && size > max_bytes {
                return Ok(0);
            }
            if !prepared.is_empty() && retained_bytes.saturating_add(size) > max_bytes {
                break;
            }
            let raw_frame = self.read_frame(source)?;
            let message = decode_frame(&raw_frame)?;
            let mut decoded = decode_source(
                source,
                raw_frame,
                &message,
                self.config.lane_count,
                self.mode,
                self.format_fingerprint,
            )?;
            decoded.late = !decoded.cancelled
                && decoded.key.due_time_ms <= due_cursor.saturating_add(self.config.safety_overlap_ms);
            let generation = decoded.key.generation;
            let existing = match self.mode {
                TimelineMaterializationMode::Shadow => {
                    self.timeline
                        .get_shadow(source.cq_offset, source.physical_offset, generation)?
                }
                TimelineMaterializationMode::Formal { .. } => match self.index_migration.as_ref() {
                    Some(migration) => migration.get(decoded.key)?,
                    None => self.timeline.get(decoded.key)?,
                },
            };
            if let Some(existing) = existing {
                validate_existing_source(
                    existing,
                    source,
                    matches!(self.mode, TimelineMaterializationMode::Shadow),
                )?;
                prepared.push(PreparedSource::Existing(decoded));
                retained_bytes = retained_bytes.saturating_add(size);
                continue;
            }
            retained_bytes = retained_bytes.saturating_add(size);
            prepared.push(PreparedSource::New(decoded));
        }
        if prepared.is_empty() {
            return Ok(0);
        }

        let payload_records = prepared
            .iter()
            .filter_map(|source| match source {
                PreparedSource::Existing(_) => None,
                PreparedSource::New(source) => Some(source.payload.clone()),
            })
            .collect::<Vec<_>>();
        let locators = self
            .payload_store
            .append_batch(&payload_records)?
            .ok_or(TimelineMaterializerError::InvalidPayloadRecord)?;
        let mut locator_iter = locators.into_iter();
        let mut batch = RocksDbWriteBatch::with_capacity(prepared.len().saturating_mul(3).saturating_add(1));
        let mut formal_entries = Vec::new();
        let mut usage_deltas = BTreeMap::<Vec<u8>, (u64, u64)>::new();
        for prepared_source in &prepared {
            let decoded = prepared_source.decoded();
            if decoded.late && matches!(self.mode, TimelineMaterializationMode::Shadow) {
                RocksDbTimelineIndex::append_shadow_observation(
                    &mut batch,
                    decoded.source.cq_offset,
                    decoded.source.physical_offset,
                    decoded.key.generation,
                    ShadowObservationKind::Due,
                    decoded.key.due_time_ms.to_be_bytes(),
                )?;
            }
            let PreparedSource::New(source) = prepared_source else {
                continue;
            };
            let locator = locator_iter
                .next()
                .ok_or(TimelineMaterializerError::LocatorCountMismatch)?;
            let entry = TimelineIndexEntry {
                key: source.key,
                record: TimelineRecordV1 {
                    payload: locator,
                    source_cq_offset: TimerSourceCqOffset::new(source.source.cq_offset),
                    source_physical_offset: source.source.physical_offset,
                    source_size: u32::try_from(source.source.size)
                        .map_err(|_| TimelineMaterializerError::InvalidSourceSize)?,
                    state_version: 0,
                    owner_engine: source.route.engine_id(),
                    shadow_only: matches!(self.mode, TimelineMaterializationMode::Shadow),
                },
            };
            let keep_rocks_timeline = self
                .index_migration
                .as_ref()
                .map(|migration| migration.keeps_rocks_timeline())
                .transpose()?
                .unwrap_or(true);
            if matches!(self.mode, TimelineMaterializationMode::Shadow) || keep_rocks_timeline {
                RocksDbTimelineIndex::append_entry(&mut batch, &entry)?;
            }
            match self.mode {
                TimelineMaterializationMode::Shadow => {
                    RocksDbTimelineIndex::append_shadow_observation(
                        &mut batch,
                        source.source.cq_offset,
                        source.source.physical_offset,
                        source.key.generation,
                        ShadowObservationKind::Materialized,
                        source.key.due_time_ms.to_be_bytes(),
                    )?;
                    if source.cancelled {
                        RocksDbTimelineIndex::delete_shadow_due_candidate(&mut batch, source.key);
                        RocksDbTimelineIndex::append_shadow_observation(
                            &mut batch,
                            source.source.cq_offset,
                            source.source.physical_offset,
                            source.key.generation,
                            ShadowObservationKind::Cancelled,
                            source.key.due_time_ms.to_be_bytes(),
                        )?;
                    }
                }
                TimelineMaterializationMode::Formal { admission_epoch } => {
                    formal_entries.push(entry);
                    if source.cancelled {
                        return Err(TimelineMaterializerError::FormalRecallTombstone);
                    }
                    RocksDbTimelineStateIndex::append_state(
                        &mut batch,
                        source.key.timer_id,
                        source.key.generation,
                        &TimelineStateRecordV1 {
                            state: TimelineState::Pending,
                            state_version: 0,
                            route: source.route.clone(),
                            admission_epoch,
                            owner_epoch: admission_epoch,
                            claim_seq: 0,
                            due_time_ms: source.key.due_time_ms,
                            lane: source.key.lane,
                            terminal_at_ms: 0,
                            shadow_only: false,
                        },
                    )?;
                    if source.late {
                        TimelineReadyOutbox::append_late_ready(&mut batch, source.key, 0);
                    }
                    if let Some(unique_key) = source.unique_key.as_ref() {
                        TimelineRecallService::append_lookup(
                            &mut batch,
                            &RecallLookupKeyV1 {
                                engine: TimerEngineId::ExtendedTimeline,
                                topic: source.payload.real_topic.clone(),
                                unique_key: unique_key.clone(),
                            },
                            RecallLookupValueV1 {
                                timer_id: source.key.timer_id,
                                generation: source.key.generation,
                                due_time_ms: source.key.due_time_ms,
                                lane: source.key.lane,
                                state_version: 0,
                            },
                        )?;
                    }
                    let encoded_bytes = u64::try_from(
                        source
                            .payload
                            .encoded_len()
                            .ok_or(TimelineMaterializerError::InvalidPayloadRecord)?,
                    )
                    .unwrap_or(u64::MAX);
                    let keys = super::usage_summary_keys(&source.payload.real_topic, source.key.due_time_ms);
                    for key in [keys.global, keys.topic, keys.tenant, keys.bucket] {
                        let delta = usage_deltas.entry(key).or_default();
                        delta.0 = delta.0.saturating_add(1);
                        delta.1 = delta.1.saturating_add(encoded_bytes);
                    }
                }
            }
        }
        if locator_iter.next().is_some() {
            return Err(TimelineMaterializerError::LocatorCountMismatch);
        }
        for (key, (count_delta, bytes_delta)) in usage_deltas {
            let (count, bytes) = self.timeline.bucket_summary(&key)?.unwrap_or_default();
            RocksDbTimelineIndex::append_bucket_summary(
                &mut batch,
                key,
                count.saturating_add(count_delta),
                bytes.saturating_add(bytes_delta),
            );
        }

        let last_source = prepared
            .last()
            .map(PreparedSource::identity)
            .ok_or(TimelineMaterializerError::EmptyBatch)?;
        let next_checkpoint = TimelineCheckpointV1 {
            materialized_source_offset: TimerSourceCqOffset::new(last_source.cq_offset),
            due_cursor: checkpoint.due_cursor,
            completion_cursor: checkpoint.completion_cursor,
            format_fingerprint: self.format_fingerprint,
            generation: checkpoint.generation.saturating_add(1),
        };
        if let (TimelineMaterializationMode::Formal { .. }, Some(migration)) =
            (self.mode, self.index_migration.as_ref())
        {
            migration.commit_derived(&formal_entries, batch, next_checkpoint)?;
        } else {
            RocksDbTimelineIndex::append_checkpoint(
                &mut batch,
                TimelineCheckpointKind::MaterializedSource,
                SOURCE_CHECKPOINT_LANE,
                next_checkpoint,
            );
            self.timeline.write_batch(&batch)?;
        }
        if matches!(self.mode, TimelineMaterializationMode::Shadow) {
            for source in &prepared {
                let source = source.decoded();
                self.reconciler.reconcile_materialized(source.expected()?);
                if source.late {
                    self.reconciler.reconcile_due(source.expected()?);
                }
            }
        }
        self.materialized_records.fetch_add(
            u64::try_from(payload_records.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.materialized_bytes.fetch_add(
            u64::try_from(payload_records.iter().map(|record| record.frame.len()).sum::<usize>()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.publish_cleanup_fence(Some(last_source.cq_offset.saturating_add(1)));
        Ok(prepared.len())
    }

    fn source_checkpoint(&self) -> Result<TimelineCheckpointV1, TimelineMaterializerError> {
        let checkpoint = self
            .timeline
            .checkpoint(TimelineCheckpointKind::MaterializedSource, SOURCE_CHECKPOINT_LANE)?
            .unwrap_or(TimelineCheckpointV1 {
                materialized_source_offset: TimerSourceCqOffset::new(-1),
                due_cursor: TimerTimelineCursor::default(),
                completion_cursor: TimerTimelineCursor::default(),
                format_fingerprint: self.format_fingerprint,
                generation: 0,
            });
        if checkpoint.format_fingerprint != self.format_fingerprint {
            return Err(TimelineMaterializerError::FormatFingerprintMismatch);
        }
        Ok(checkpoint)
    }

    fn read_frame(&self, source: SourceIdentity) -> Result<Vec<u8>, TimelineMaterializerError> {
        if source.size <= 0 {
            return Err(TimelineMaterializerError::InvalidSourceSize);
        }
        self.commit_log
            .get_message(source.physical_offset, source.size)
            .and_then(|result| result.get_bytes())
            .map(|bytes| bytes.to_vec())
            .ok_or(TimelineMaterializerError::SourcePayloadMissing(source.physical_offset))
    }

    fn restore_cleanup_fence(&self) -> Result<(), TimelineMaterializerError> {
        let next = self
            .source_checkpoint()?
            .materialized_source_offset
            .get()
            .saturating_add(1);
        self.publish_cleanup_fence(Some(next));
        Ok(())
    }

    fn publish_cleanup_fence(&self, next_cq_offset: Option<i64>) {
        let physical = next_cq_offset
            .and_then(|offset| {
                self.consume_queues
                    .find_or_create_consume_queue(&CheetahString::from_static_str(TIMER_TOPIC), 0)
                    .and_then(|queue| queue.read().get(offset))
                    .map(|unit| unit.pos)
            })
            .unwrap_or_else(|| self.commit_log.get_max_offset());
        self.first_unmaterialized_physical_offset
            .store(physical.max(0), Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug)]
struct SourceIdentity {
    cq_offset: i64,
    physical_offset: i64,
    size: i32,
}

enum PreparedSource {
    Existing(DecodedSource),
    New(DecodedSource),
}

impl PreparedSource {
    fn identity(&self) -> SourceIdentity {
        match self {
            Self::Existing(source) => source.source,
            Self::New(source) => source.source,
        }
    }

    fn decoded(&self) -> &DecodedSource {
        match self {
            Self::Existing(source) | Self::New(source) => source,
        }
    }
}

struct DecodedSource {
    source: SourceIdentity,
    key: TimelineKeyV1,
    payload: TimerPayloadRecordV1,
    cancelled: bool,
    late: bool,
    route: PersistedTimerRoute,
    unique_key: Option<String>,
}

impl DecodedSource {
    fn expected(&self) -> Result<ShadowExpectedRecord, TimelineMaterializerError> {
        Ok(ShadowExpectedRecord {
            source_cq_offset: self.source.cq_offset,
            source_physical_offset: self.source.physical_offset,
            source_size: u32::try_from(self.source.size).map_err(|_| TimelineMaterializerError::InvalidSourceSize)?,
            timer_id: self.key.timer_id,
            generation: self.key.generation,
            due_time_ms: self.key.due_time_ms,
            cancelled: self.cancelled,
        })
    }
}

fn decode_source(
    source: SourceIdentity,
    frame: Vec<u8>,
    message: &MessageExt,
    lane_count: usize,
    mode: TimelineMaterializationMode,
    fallback_policy_fingerprint: u64,
) -> Result<DecodedSource, TimelineMaterializerError> {
    if source.cq_offset < 0 || source.physical_offset < 0 {
        return Err(TimelineMaterializerError::InvalidSourceIdentity);
    }
    let due_time_ms = parse_i64_property(message, MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS)
        .or_else(|| parse_i64_property(message, MessageConst::PROPERTY_TIMER_DELIVER_MS))
        .or_else(|| parse_i64_property(message, MessageConst::PROPERTY_TIMER_OUT_MS))
        .ok_or(TimelineMaterializerError::MissingProperty(
            MessageConst::PROPERTY_TIMER_OUT_MS,
        ))?;
    let real_topic = property(message, MessageConst::PROPERTY_REAL_TOPIC).ok_or(
        TimelineMaterializerError::MissingProperty(MessageConst::PROPERTY_REAL_TOPIC),
    )?;
    let real_queue_id = parse_i32_property(message, MessageConst::PROPERTY_REAL_QUEUE_ID).ok_or(
        TimelineMaterializerError::MissingProperty(MessageConst::PROPERTY_REAL_QUEUE_ID),
    )?;
    let generation =
        TimerGeneration::new(parse_u64_property(message, MessageConst::PROPERTY_TIMER_GENERATION).unwrap_or_default());
    let route = decode_route(
        message,
        mode,
        source,
        generation,
        due_time_ms,
        fallback_policy_fingerprint,
    )?;
    let lane = u16::try_from(delivery_shard(&real_topic, real_queue_id, lane_count))
        .map_err(|_| TimelineMaterializerError::LaneOverflow)?;
    let timer_id =
        TimerId::new(((u128::from(source.cq_offset as u64)) << 64) | u128::from(source.physical_offset as u64));
    let key = TimelineKeyV1 {
        due_time_ms,
        lane,
        timer_id,
        generation,
    };
    Ok(DecodedSource {
        source,
        key,
        cancelled: property(message, MessageConst::PROPERTY_TIMER_DEL_UNIQKEY).is_some(),
        late: false,
        route,
        unique_key: property(message, MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
        payload: TimerPayloadRecordV1 {
            due_time_ms,
            lane,
            timer_id,
            generation,
            source_cq_offset: TimerSourceCqOffset::new(source.cq_offset),
            source_physical_offset: source.physical_offset,
            real_queue_id,
            real_topic,
            frame,
        },
    })
}

fn decode_route(
    message: &MessageExt,
    mode: TimelineMaterializationMode,
    source: SourceIdentity,
    generation: TimerGeneration,
    due_time_ms: i64,
    fallback_policy_fingerprint: u64,
) -> Result<PersistedTimerRoute, TimelineMaterializerError> {
    let expected_engine = match mode {
        TimelineMaterializationMode::Shadow => TimerEngineId::JavaCompat,
        TimelineMaterializationMode::Formal { .. } => TimerEngineId::ExtendedTimeline,
    };
    let encoded_engine = property(message, MessageConst::TIMER_ENGINE_TYPE);
    let engine = match encoded_engine {
        Some(engine) => TimerEngineId::parse(&engine).map_err(|_| TimelineMaterializerError::RouteMismatch)?,
        None if matches!(mode, TimelineMaterializationMode::Shadow) => expected_engine,
        None => return Err(TimelineMaterializerError::RouteMismatch),
    };
    if engine != expected_engine {
        return Err(TimelineMaterializerError::RouteMismatch);
    }
    let format_version = parse_u64_property(message, MessageConst::PROPERTY_TIMER_FORMAT_VERSION)
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or(1);
    let policy_fingerprint = parse_u64_property(message, MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT)
        .filter(|value| *value != 0)
        .unwrap_or(fallback_policy_fingerprint);
    let token = property(message, MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN).unwrap_or_else(|| {
        format!(
            "{}:{format_version}:{}:{}:{}:{}",
            expected_engine.as_str(),
            source.cq_offset,
            source.physical_offset,
            due_time_ms,
            generation.get()
        )
    });
    if matches!(mode, TimelineMaterializationMode::Formal { .. })
        && (format_version != rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION
            || property(message, MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN).is_none()
            || parse_u64_property(message, MessageConst::PROPERTY_TIMER_POLICY_FINGERPRINT).is_none())
    {
        return Err(TimelineMaterializerError::RouteMismatch);
    }
    PersistedTimerRoute::try_new(engine, format_version, policy_fingerprint, generation, token)
        .map_err(|_| TimelineMaterializerError::RouteMismatch)
}

/// Runtime metrics for the non-delivering Extended Timeline shadow.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ExtendedTimelineShadowMetrics {
    pub(crate) materialization_lag: u64,
    pub(crate) materialized_records: u64,
    pub(crate) materialized_bytes: u64,
    pub(crate) materialization_failures: u64,
    pub(crate) payload_live_bytes: u64,
    pub(crate) payload_records: u64,
    pub(crate) payload_partitions: usize,
    pub(crate) payload_open_handles: usize,
    pub(crate) timeline_bytes_written: u64,
    pub(crate) timeline_bytes_read: u64,
    pub(crate) timeline_errors: u64,
    pub(crate) reconciliation: ShadowReconciliationSnapshot,
}

fn decode_frame(frame: &[u8]) -> Result<MessageExt, TimelineMaterializerError> {
    MessageDecoder::decode(&mut Bytes::copy_from_slice(frame), true, false, false, false, false)
        .ok_or(TimelineMaterializerError::InvalidCommitLogFrame)
}

fn property(message: &MessageExt, name: &'static str) -> Option<String> {
    message
        .property(&CheetahString::from_static_str(name))
        .map(|value| value.to_string())
}

fn parse_i64_property(message: &MessageExt, name: &'static str) -> Option<i64> {
    property(message, name)?.parse().ok()
}

fn parse_i32_property(message: &MessageExt, name: &'static str) -> Option<i32> {
    property(message, name)?.parse().ok()
}

fn parse_u64_property(message: &MessageExt, name: &'static str) -> Option<u64> {
    property(message, name)?.parse().ok()
}

fn validate_existing_source(
    existing: TimelineRecordV1,
    source: SourceIdentity,
    expected_shadow: bool,
) -> Result<(), TimelineMaterializerError> {
    if existing.source_cq_offset.get() != source.cq_offset
        || existing.source_physical_offset != source.physical_offset
        || existing.source_size
            != u32::try_from(source.size).map_err(|_| TimelineMaterializerError::InvalidSourceSize)?
        || existing.shadow_only != expected_shadow
    {
        return Err(TimelineMaterializerError::ReplayIdentityMismatch);
    }
    Ok(())
}

fn config_fingerprint(config: &TimerStoreConfig) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for value in [
        config.lane_count as u64,
        config.payload_segment_bytes,
        config.horizon_days.into(),
        config.safety_overlap_ms as u64,
    ] {
        for byte in value.to_be_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash.max(1)
}

/// Fail-closed materialization error. The caller must leave the cleanup fence in place.
#[derive(Debug, Error)]
pub(crate) enum TimelineMaterializerError {
    #[error("invalid Extended Timeline configuration: {0}")]
    Config(#[from] crate::config::timer_store_config::TimerStoreConfigError),
    #[error("local store failure: {0}")]
    Store(#[from] rocketmq_store_api::StoreError),
    #[error("Timeline store failure: {0}")]
    Timeline(#[from] rocketmq_error::RocketMQError),
    #[error("Timeline index migration failure: {0}")]
    IndexMigration(#[from] super::index_migration::IndexMigrationError),
    #[error("Timer ConsumeQueue is unavailable")]
    SourceUnavailable,
    #[error("Timer source payload is missing at CommitLog offset {0}")]
    SourcePayloadMissing(i64),
    #[error("Timer source size is invalid")]
    InvalidSourceSize,
    #[error("Timer source offsets must be non-negative")]
    InvalidSourceIdentity,
    #[error("Timer source CommitLog frame is invalid")]
    InvalidCommitLogFrame,
    #[error("derived Timer payload configuration is invalid")]
    InvalidPayloadConfig,
    #[error("derived Timer payload record is invalid")]
    InvalidPayloadRecord,
    #[error("Timer source is missing required property {0}")]
    MissingProperty(&'static str),
    #[error("Timer lane cannot fit the persisted format")]
    LaneOverflow,
    #[error("payload locator count does not match newly materialized records")]
    LocatorCountMismatch,
    #[error("empty materialization batch")]
    EmptyBatch,
    #[error("replayed source identity disagrees with its durable marker")]
    ReplayIdentityMismatch,
    #[error("Extended Timeline configuration fingerprint changed without migration")]
    FormatFingerprintMismatch,
    #[error("formal Extended Timeline admission epoch must be non-zero")]
    InvalidAdmissionEpoch,
    #[error("timer source route does not match the materializer owner")]
    RouteMismatch,
    #[error("formal Extended Recall must use the state transition API, not a Timer topic tombstone")]
    FormalRecallTombstone,
}
