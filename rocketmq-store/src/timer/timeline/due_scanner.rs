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

use parking_lot::Mutex;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::TimerTimelineCursor;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimeline;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimelineContinuation;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentKey;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentRecord;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointKind;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointV1;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
use rocketmq_store_rocksdb::timer::state_index::StateTransitionResult;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::ShadowObservationKind;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;
use thiserror::Error;

use super::shadow::ShadowExpectedRecord;
use super::ShadowReconciler;
use super::TimelineReadyOutbox;
use crate::config::timer_store_config::TimerStoreConfig;
use crate::timer::clock::TimerClockSafety;
use crate::timer::clock::TimerClockState;

const FORMAL_DUE_CHECKPOINT_LANE: u16 = 0;
const SHADOW_DUE_CHECKPOINT_LANE: u16 = u16::MAX;

#[derive(Clone, Debug)]
enum FormalDueContinuation {
    Rocks(TimelineKeyV1),
    Segmented(SegmentedTimelineContinuation),
}

/// Where the first page of a due scan starts.
#[derive(Clone, Copy, Debug)]
enum DueScanStart {
    /// Every key whose due time is at or after this time.
    AtOrAfter(i64),
    /// Every key ordered after this full key, which a previous scan already consumed.
    After(TimelineKeyV1),
}

impl DueScanStart {
    fn due_time_ms(self) -> i64 {
        match self {
            Self::AtOrAfter(due_time_ms) => due_time_ms,
            Self::After(key) => key.due_time_ms,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct FormalDuePage {
    entries: Vec<TimelineIndexEntry>,
    scan_continuation: Option<FormalDueContinuation>,
    retained_bytes: usize,
}

/// Synchronous, bounded read contract used by the blocking Due Scanner. Mutable state, ready,
/// receipts, and checkpoints remain in the RocksDB overlay for both implementations.
trait TimelineDueIndex: Send + Sync {
    fn range_scan(
        &self,
        start: DueScanStart,
        end_due_exclusive_ms: i64,
        scan_continuation: Option<FormalDueContinuation>,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<FormalDuePage, TimelineDueScannerError>;
}

struct RocksTimelineDueIndex {
    timeline: Arc<RocksDbTimelineIndex>,
}

impl TimelineDueIndex for RocksTimelineDueIndex {
    fn range_scan(
        &self,
        start: DueScanStart,
        end_due_exclusive_ms: i64,
        scan_continuation: Option<FormalDueContinuation>,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<FormalDuePage, TimelineDueScannerError> {
        let rocks_resume_key = match scan_continuation {
            Some(FormalDueContinuation::Rocks(resume_key)) => Some(resume_key),
            Some(FormalDueContinuation::Segmented(_)) => {
                return Err(TimelineDueScannerError::ContinuationIndexKindMismatch)
            }
            None => match start {
                DueScanStart::After(key) => Some(key),
                DueScanStart::AtOrAfter(_) => None,
            },
        };
        let page = self.timeline.range_scan(
            start.due_time_ms(),
            end_due_exclusive_ms,
            rocks_resume_key,
            max_messages,
            max_bytes,
        )?;
        Ok(FormalDuePage {
            entries: page.entries,
            scan_continuation: page.continuation.map(FormalDueContinuation::Rocks),
            retained_bytes: page.retained_bytes,
        })
    }
}

struct SegmentedTimelineDueIndex {
    timeline: Arc<SegmentedTimeline>,
}

impl TimelineDueIndex for SegmentedTimelineDueIndex {
    fn range_scan(
        &self,
        start: DueScanStart,
        end_due_exclusive_ms: i64,
        scan_continuation: Option<FormalDueContinuation>,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<FormalDuePage, TimelineDueScannerError> {
        let segmented_continuation = match scan_continuation {
            Some(FormalDueContinuation::Segmented(segmented_continuation)) => Some(segmented_continuation),
            Some(FormalDueContinuation::Rocks(_)) => {
                return Err(TimelineDueScannerError::ContinuationIndexKindMismatch)
            }
            None => None,
        };
        let from_exclusive = match start {
            DueScanStart::AtOrAfter(due_time_ms) => TimelineSegmentKey {
                due_time_ms: due_time_ms.max(0),
                lane: 0,
                timer_id: rocketmq_store_api::TimerId::new(0),
                generation: rocketmq_store_api::TimerGeneration::new(0),
            },
            DueScanStart::After(key) => TimelineSegmentKey {
                due_time_ms: key.due_time_ms,
                lane: key.lane,
                timer_id: key.timer_id,
                generation: key.generation,
            },
        };
        let Some(page) = self.timeline.scan_due(
            Some(from_exclusive),
            end_due_exclusive_ms,
            max_messages,
            max_bytes.max(max_messages.saturating_mul(TimelineSegmentRecord::encoded_size())),
            segmented_continuation,
        )?
        else {
            return Ok(FormalDuePage {
                entries: Vec::new(),
                scan_continuation: None,
                retained_bytes: 0,
            });
        };
        let retained_bytes = page.records.len().saturating_mul(TimelineSegmentRecord::encoded_size());
        Ok(FormalDuePage {
            entries: page.records.into_iter().map(native_entry).collect(),
            scan_continuation: page.continuation.map(FormalDueContinuation::Segmented),
            retained_bytes,
        })
    }
}

/// Bounded, overlap-safe Timeline scanner.
///
/// Each scan kind resumes after the last key it scanned. Only the first scan after start-up
/// begins `safetyOverlapMs` before the persisted due cursor, so entries that are already Ready
/// or Delivered no longer refill every page. A source materialized behind the scanner reaches it
/// through the late-ready outbox instead.
pub(crate) struct TimelineDueScanner {
    config: TimerStoreConfig,
    timeline: Arc<RocksDbTimelineIndex>,
    formal_index: Arc<dyn TimelineDueIndex>,
    state: RocksDbTimelineStateIndex,
    shadow_reconciler: Option<Arc<ShadowReconciler>>,
    clock: Option<Arc<TimerClockSafety>>,
    formal_resume_after: Mutex<Option<TimelineKeyV1>>,
    shadow_resume_after: Mutex<Option<TimelineKeyV1>>,
}

impl TimelineDueScanner {
    pub(crate) fn new(config: TimerStoreConfig, timeline: Arc<RocksDbTimelineIndex>) -> Self {
        Self {
            config,
            state: timeline.state_index(),
            formal_index: Arc::new(RocksTimelineDueIndex {
                timeline: Arc::clone(&timeline),
            }),
            timeline,
            shadow_reconciler: None,
            clock: None,
            formal_resume_after: Mutex::new(None),
            shadow_resume_after: Mutex::new(None),
        }
    }

    pub(crate) fn new_with_clock(
        config: TimerStoreConfig,
        timeline: Arc<RocksDbTimelineIndex>,
        clock: Arc<TimerClockSafety>,
    ) -> Self {
        Self {
            config,
            state: timeline.state_index(),
            formal_index: Arc::new(RocksTimelineDueIndex {
                timeline: Arc::clone(&timeline),
            }),
            timeline,
            shadow_reconciler: None,
            clock: Some(clock),
            formal_resume_after: Mutex::new(None),
            shadow_resume_after: Mutex::new(None),
        }
    }

    pub(crate) fn new_shadow(
        config: TimerStoreConfig,
        timeline: Arc<RocksDbTimelineIndex>,
        reconciler: Arc<ShadowReconciler>,
    ) -> Self {
        Self {
            config,
            state: timeline.state_index(),
            formal_index: Arc::new(RocksTimelineDueIndex {
                timeline: Arc::clone(&timeline),
            }),
            timeline,
            shadow_reconciler: Some(reconciler),
            clock: None,
            formal_resume_after: Mutex::new(None),
            shadow_resume_after: Mutex::new(None),
        }
    }

    /// Creates a Due Scanner whose ordered records come from native runs and whose state/outbox
    /// operations remain in the existing RocksDB overlay.
    pub(crate) fn new_segmented(
        config: TimerStoreConfig,
        timeline: Arc<RocksDbTimelineIndex>,
        native: Arc<SegmentedTimeline>,
    ) -> Self {
        Self {
            config,
            state: timeline.state_index(),
            timeline,
            formal_index: Arc::new(SegmentedTimelineDueIndex { timeline: native }),
            shadow_reconciler: None,
            clock: None,
            formal_resume_after: Mutex::new(None),
            shadow_resume_after: Mutex::new(None),
        }
    }

    /// Records due observations for Java-compatible shadow entries without creating ready work.
    pub(crate) fn scan_shadow_until(&self, now_ms: i64) -> Result<DueScanResult, TimelineDueScannerError> {
        self.scan_shadow_until_with_budget(now_ms, self.config.due_scan_messages, self.config.due_scan_bytes)
    }

    pub(crate) fn scan_shadow_until_with_budget(
        &self,
        now_ms: i64,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<DueScanResult, TimelineDueScannerError> {
        let checkpoint = self.checkpoint(SHADOW_DUE_CHECKPOINT_LANE)?;
        let mut resume_after = self.shadow_resume_after.lock();
        let start = self.scan_start(checkpoint, *resume_after);
        let end_ms = now_ms.saturating_add(1);
        let mut continuation = match start {
            DueScanStart::After(key) => Some(key),
            DueScanStart::AtOrAfter(_) => None,
        };
        let mut result = DueScanResult::default();
        // A resumed scan is ahead of a clock that moved back: nothing in its range is due yet.
        if end_ms <= start.due_time_ms() {
            return Ok(result);
        }
        loop {
            let remaining_messages = max_messages.saturating_sub(result.observed);
            let remaining_bytes = max_bytes.saturating_sub(result.bytes);
            if remaining_messages == 0 || remaining_bytes == 0 {
                break;
            }
            let page = self.timeline.range_scan_shadow(
                start.due_time_ms(),
                end_ms,
                continuation,
                remaining_messages,
                remaining_bytes,
            )?;
            if page.entries.is_empty() {
                break;
            }
            let mut batch = RocksDbWriteBatch::with_capacity(page.entries.len().saturating_add(1));
            for entry in &page.entries {
                if !entry.record.shadow_only || entry.record.owner_engine != TimerEngineId::JavaCompat {
                    return Err(TimelineDueScannerError::ShadowNamespaceViolation);
                }
                RocksDbTimelineIndex::append_shadow_observation(
                    &mut batch,
                    entry.record.source_cq_offset.get(),
                    entry.record.source_physical_offset,
                    entry.key.generation,
                    ShadowObservationKind::Due,
                    now_ms.to_be_bytes(),
                )?;
            }
            let last = page
                .entries
                .last()
                .map(|entry| entry.key)
                .ok_or(TimelineDueScannerError::EmptyPage)?;
            if advances_checkpoint(checkpoint, last) {
                RocksDbTimelineIndex::append_checkpoint(
                    &mut batch,
                    TimelineCheckpointKind::Due,
                    SHADOW_DUE_CHECKPOINT_LANE,
                    checkpoint_after(
                        checkpoint,
                        last,
                        u64::try_from(result.pages.saturating_add(1)).unwrap_or(u64::MAX),
                    ),
                );
            }
            self.timeline.write_batch(&batch)?;
            *resume_after = Some(last);
            if let Some(reconciler) = self.shadow_reconciler.as_ref() {
                for entry in &page.entries {
                    reconciler.reconcile_due(ShadowExpectedRecord {
                        source_cq_offset: entry.record.source_cq_offset.get(),
                        source_physical_offset: entry.record.source_physical_offset,
                        source_size: entry.record.source_size,
                        timer_id: entry.key.timer_id,
                        generation: entry.key.generation,
                        due_time_ms: entry.key.due_time_ms,
                        cancelled: false,
                    });
                }
            }
            result.observed = result.observed.saturating_add(page.entries.len());
            result.bytes = result.bytes.saturating_add(page.retained_bytes);
            result.pages = result.pages.saturating_add(1);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        Ok(result)
    }

    /// Promotes formal, Extended-owned PENDING records to a durable ready outbox.
    pub(crate) fn scan_formal_until(&self, now_ms: i64) -> Result<DueScanResult, TimelineDueScannerError> {
        self.scan_formal_until_with_budget(now_ms, self.config.due_scan_messages, self.config.due_scan_bytes)
    }

    pub(crate) fn scan_formal_until_with_budget(
        &self,
        now_ms: i64,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<DueScanResult, TimelineDueScannerError> {
        let now_ms = if let Some(clock) = self.clock.as_ref() {
            let observation = clock.observe();
            if observation.state == TimerClockState::Unsafe {
                return Err(TimelineDueScannerError::ClockUnsafe);
            }
            observation.wall_time_ms.min(now_ms)
        } else {
            now_ms
        };
        let mut result = self.drain_late_ready(max_messages, max_bytes)?;
        let checkpoint = self.checkpoint(FORMAL_DUE_CHECKPOINT_LANE)?;
        let mut resume_after = self.formal_resume_after.lock();
        let start = self.scan_start(checkpoint, *resume_after);
        let end_ms = now_ms.saturating_add(1);
        if end_ms <= start.due_time_ms() {
            return Ok(result);
        }
        let mut scan_continuation = None;
        loop {
            let remaining_messages = max_messages.saturating_sub(result.observed);
            let remaining_bytes = max_bytes.saturating_sub(result.bytes);
            if remaining_messages == 0 || remaining_bytes == 0 {
                break;
            }
            let page =
                self.formal_index
                    .range_scan(start, end_ms, scan_continuation, remaining_messages, remaining_bytes)?;
            if page.entries.is_empty() {
                break;
            }
            for entry in &page.entries {
                if entry.record.shadow_only || entry.record.owner_engine != TimerEngineId::ExtendedTimeline {
                    continue;
                }
                let Some(current) = self.state.get(entry.key.timer_id, entry.key.generation)? else {
                    return Err(TimelineDueScannerError::MissingFormalState);
                };
                if current.shadow_only || current.route.engine_id() != TimerEngineId::ExtendedTimeline {
                    return Err(TimelineDueScannerError::FormalNamespaceViolation);
                }
                if current.state != TimelineState::Pending {
                    continue;
                }
                let mut side_effects = RocksDbWriteBatch::with_capacity(1);
                TimelineReadyOutbox::append_ready(
                    &mut side_effects,
                    entry.key,
                    current.state_version.saturating_add(1),
                );
                if matches!(
                    self.state.compare_and_set(
                        entry.key.timer_id,
                        entry.key.generation,
                        TimelineState::Pending,
                        current.state_version,
                        TimelineState::Ready,
                        side_effects,
                    )?,
                    StateTransitionResult::Applied(_)
                ) {
                    result.ready = result.ready.saturating_add(1);
                }
            }
            let last = page
                .entries
                .last()
                .map(|entry| entry.key)
                .ok_or(TimelineDueScannerError::EmptyPage)?;
            if advances_checkpoint(checkpoint, last) {
                self.timeline.put_batch(
                    &[],
                    Some((
                        TimelineCheckpointKind::Due,
                        FORMAL_DUE_CHECKPOINT_LANE,
                        checkpoint_after(
                            checkpoint,
                            last,
                            u64::try_from(result.pages.saturating_add(1)).unwrap_or(u64::MAX),
                        ),
                    )),
                )?;
            }
            *resume_after = Some(last);
            result.observed = result.observed.saturating_add(page.entries.len());
            result.bytes = result.bytes.saturating_add(page.retained_bytes);
            result.pages = result.pages.saturating_add(1);
            scan_continuation = page.scan_continuation;
            if scan_continuation.is_none() {
                break;
            }
        }
        Ok(result)
    }

    fn drain_late_ready(
        &self,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<DueScanResult, TimelineDueScannerError> {
        let outbox = TimelineReadyOutbox::new(Arc::clone(&self.timeline));
        let mut result = DueScanResult::default();
        for lane in 0..self.config.lane_count {
            let lane = u16::try_from(lane).map_err(|_| TimelineDueScannerError::LaneOverflow)?;
            let remaining = max_messages.saturating_sub(result.observed);
            let remaining_bytes = max_bytes.saturating_sub(result.bytes);
            let key_bytes = TimelineKeyV1::encoded_size();
            if remaining == 0 || remaining_bytes < key_bytes {
                break;
            }
            let byte_limited = remaining_bytes / key_bytes;
            for key in outbox.scan_late_ready(lane, remaining.min(byte_limited))? {
                result.observed = result.observed.saturating_add(1);
                result.bytes = result.bytes.saturating_add(key_bytes);
                let Some(current) = self.state.get(key.timer_id, key.generation)? else {
                    return Err(TimelineDueScannerError::MissingFormalState);
                };
                match current.state {
                    TimelineState::Pending => {
                        let mut side_effects = RocksDbWriteBatch::with_capacity(2);
                        TimelineReadyOutbox::append_ready(
                            &mut side_effects,
                            key,
                            current.state_version.saturating_add(1),
                        );
                        TimelineReadyOutbox::delete_late_ready(&mut side_effects, key);
                        if matches!(
                            self.state.compare_and_set(
                                key.timer_id,
                                key.generation,
                                TimelineState::Pending,
                                current.state_version,
                                TimelineState::Ready,
                                side_effects,
                            )?,
                            StateTransitionResult::Applied(_)
                        ) {
                            result.ready = result.ready.saturating_add(1);
                        }
                    }
                    TimelineState::Ready | TimelineState::Cancelled | TimelineState::Delivered => {
                        let mut batch = RocksDbWriteBatch::with_capacity(1);
                        TimelineReadyOutbox::delete_late_ready(&mut batch, key);
                        self.timeline.write_batch(&batch)?;
                    }
                    TimelineState::SourceOnly
                    | TimelineState::Delivering
                    | TimelineState::Committing
                    | TimelineState::Quarantined => {}
                }
            }
        }
        Ok(result)
    }

    /// Resumes after the last scanned key. Without one, starts the safety overlap before the
    /// persisted cursor, so a restart re-reads what a crashed run may not have promoted.
    fn scan_start(&self, checkpoint: TimelineCheckpointV1, resume_after: Option<TimelineKeyV1>) -> DueScanStart {
        resume_after.map_or_else(
            || {
                DueScanStart::AtOrAfter(
                    checkpoint
                        .due_cursor
                        .due_time_ms()
                        .saturating_sub(self.config.safety_overlap_ms),
                )
            },
            DueScanStart::After,
        )
    }

    fn checkpoint(&self, lane: u16) -> Result<TimelineCheckpointV1, TimelineDueScannerError> {
        Ok(self
            .timeline
            .checkpoint(TimelineCheckpointKind::Due, lane)?
            .unwrap_or(TimelineCheckpointV1 {
                materialized_source_offset: TimerSourceCqOffset::new(-1),
                due_cursor: TimerTimelineCursor::default(),
                completion_cursor: TimerTimelineCursor::default(),
                format_fingerprint: 1,
                generation: 0,
            }))
    }
}

/// The first scan after start-up re-reads the safety overlap, and its pages must not move the
/// persisted cursor back.
fn advances_checkpoint(base: TimelineCheckpointV1, key: TimelineKeyV1) -> bool {
    key.due_time_ms >= base.due_cursor.due_time_ms()
}

fn checkpoint_after(base: TimelineCheckpointV1, key: TimelineKeyV1, page_generation: u64) -> TimelineCheckpointV1 {
    TimelineCheckpointV1 {
        materialized_source_offset: base.materialized_source_offset,
        due_cursor: TimerTimelineCursor::new(key.due_time_ms, key.timer_id.get() as u64),
        completion_cursor: base.completion_cursor,
        format_fingerprint: base.format_fingerprint,
        generation: base.generation.saturating_add(page_generation),
    }
}

/// Work completed by one bounded scanner call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DueScanResult {
    pub(crate) pages: usize,
    pub(crate) observed: usize,
    pub(crate) ready: usize,
    pub(crate) bytes: usize,
}

#[derive(Debug, Error)]
pub(crate) enum TimelineDueScannerError {
    #[error("shadow Timeline contains a claimable or non-Java record")]
    ShadowNamespaceViolation,
    #[error("formal Timeline contains a shadow or non-Extended state")]
    FormalNamespaceViolation,
    #[error("formal Timeline record has no durable state")]
    MissingFormalState,
    #[error("Timeline range scan returned an empty page with continuation")]
    EmptyPage,
    #[error("configured Timeline lane does not fit the persisted format")]
    LaneOverflow,
    #[error("CLOCK_UNSAFE prevents formal due promotion")]
    ClockUnsafe,
    #[error("Timeline scan continuation does not match the selected index kind")]
    ContinuationIndexKindMismatch,
    #[error("Timeline storage failure: {0}")]
    Store(#[from] rocketmq_store_api::StoreError),
}

fn native_entry(record: TimelineSegmentRecord) -> TimelineIndexEntry {
    TimelineIndexEntry {
        key: TimelineKeyV1 {
            due_time_ms: record.key.due_time_ms,
            lane: record.key.lane,
            timer_id: record.key.timer_id,
            generation: record.key.generation,
        },
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

#[cfg(test)]
mod tests {
    use rocketmq_store_api::PersistedTimerRoute;
    use rocketmq_store_api::TimerEngineEpoch;
    use rocketmq_store_api::TimerGeneration;
    use rocketmq_store_api::TimerId;
    use rocketmq_store_api::TimerPayloadStoreLocator;
    use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;
    use rocketmq_store_local::timer::segmented_timeline::SegmentedTimelineConfig;
    use rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1;
    use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;
    use tempfile::tempdir;

    use super::*;

    fn formal_entry(due_time_ms: i64, timer_id: u128) -> TimelineIndexEntry {
        TimelineIndexEntry {
            key: TimelineKeyV1 {
                due_time_ms,
                lane: 3,
                timer_id: TimerId::new(timer_id),
                generation: TimerGeneration::new(1),
            },
            record: rocketmq_store_rocksdb::timer::codec::TimelineRecordV1 {
                payload: TimerPayloadStoreLocator::try_new(1, 3, 0, 0, 10, 11).expect("locator"),
                source_cq_offset: TimerSourceCqOffset::new(timer_id as i64),
                source_physical_offset: timer_id as i64 * 100,
                source_size: 10,
                state_version: 0,
                owner_engine: TimerEngineId::ExtendedTimeline,
                shadow_only: false,
            },
        }
    }

    fn state_record() -> TimelineStateRecordV1 {
        TimelineStateRecordV1 {
            state: TimelineState::Pending,
            state_version: 0,
            route: PersistedTimerRoute::try_new(
                TimerEngineId::ExtendedTimeline,
                EXTENDED_TIMELINE_FORMAT_VERSION,
                1,
                TimerGeneration::new(1),
                "token",
            )
            .expect("route"),
            admission_epoch: TimerEngineEpoch::new(1),
            owner_epoch: TimerEngineEpoch::new(1),
            claim_seq: 0,
            due_time_ms: 1_000,
            lane: 0,
            terminal_at_ms: 0,
            shadow_only: false,
        }
    }

    fn put_formal(timeline: &RocksDbTimelineIndex, entry: TimelineIndexEntry, late: bool) {
        let mut batch = RocksDbWriteBatch::with_capacity(3);
        RocksDbTimelineIndex::append_entry(&mut batch, &entry).expect("entry");
        RocksDbTimelineStateIndex::append_state(&mut batch, entry.key.timer_id, entry.key.generation, &state_record())
            .expect("state");
        if late {
            TimelineReadyOutbox::append_late_ready(&mut batch, entry.key, 0);
        }
        timeline.write_batch(&batch).expect("write");
    }

    fn shadow_entry(due_time_ms: i64) -> TimelineIndexEntry {
        let mut entry = formal_entry(due_time_ms, 3);
        entry.record.owner_engine = TimerEngineId::JavaCompat;
        entry.record.shadow_only = true;
        entry
    }

    #[test]
    fn due_scanner_persists_ready_before_advancing() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("open")
                .expect("valid Timeline configuration"),
        );
        let entry = formal_entry(8_000, 1);
        put_formal(&timeline, entry, false);
        let scanner = TimelineDueScanner::new(TimerStoreConfig::default(), Arc::clone(&timeline));

        let result = scanner.scan_formal_until(8_000).expect("scan");
        assert_eq!(result.ready, 1);
        let state = RocksDbTimelineStateIndex::new(timeline.store())
            .get(entry.key.timer_id, entry.key.generation)
            .expect("read state")
            .expect("state");
        assert_eq!(state.state, TimelineState::Ready);
        assert_eq!(
            TimelineReadyOutbox::new(Arc::clone(&timeline))
                .scan_ready(3, 10)
                .expect("ready"),
            vec![entry.key]
        );
    }

    #[test]
    fn late_ready_is_drained_even_after_due_cursor_passed() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("open")
                .expect("valid Timeline configuration"),
        );
        let entry = formal_entry(8_000, 2);
        put_formal(&timeline, entry, true);
        timeline
            .put_batch(
                &[],
                Some((
                    TimelineCheckpointKind::Due,
                    FORMAL_DUE_CHECKPOINT_LANE,
                    TimelineCheckpointV1 {
                        materialized_source_offset: TimerSourceCqOffset::new(-1),
                        due_cursor: TimerTimelineCursor::new(9_000, 0),
                        completion_cursor: TimerTimelineCursor::default(),
                        format_fingerprint: 1,
                        generation: 1,
                    },
                )),
            )
            .expect("checkpoint");
        let scanner = TimelineDueScanner::new(TimerStoreConfig::default(), Arc::clone(&timeline));

        let result = scanner.scan_formal_until(9_000).expect("scan");
        assert_eq!(result.ready, 1);
        assert!(TimelineReadyOutbox::new(timeline)
            .scan_late_ready(3, 10)
            .expect("late")
            .is_empty());
    }

    #[test]
    fn shadow_due_observation_never_creates_claimable_ready_work() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("open")
                .expect("valid Timeline configuration"),
        );
        let entry = shadow_entry(8_000);
        timeline.put_batch(&[entry], None).expect("shadow entry");
        let scanner = TimelineDueScanner::new(TimerStoreConfig::default(), Arc::clone(&timeline));

        let result = scanner.scan_shadow_until(8_000).expect("scan");
        assert_eq!(result.observed, 1);
        assert_eq!(result.ready, 0);
        assert!(timeline
            .shadow_observation(
                entry.record.source_cq_offset.get(),
                entry.record.source_physical_offset,
                entry.key.generation,
                ShadowObservationKind::Due,
            )
            .expect("observation")
            .is_some());
        assert!(TimelineReadyOutbox::new(timeline)
            .scan_ready(entry.key.lane, 10)
            .expect("ready")
            .is_empty());
    }

    #[test]
    fn segmented_due_index_uses_the_same_state_and_ready_overlay() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("open overlay")
                .expect("valid overlay configuration"),
        );
        let native = Arc::new(
            SegmentedTimeline::open(dir.path(), SegmentedTimelineConfig::default())
                .expect("open native")
                .expect("valid native configuration"),
        );
        let mut entry = formal_entry(8_000, 9);
        entry.record.payload = TimerPayloadStoreLocator::try_new(0, entry.key.lane, 1, 0, 10, 11).expect("locator");
        native
            .append_batch(&[TimelineSegmentRecord {
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
                shadow_only: false,
            }])
            .expect("native entry")
            .expect("valid native entry");
        let mut state_batch = RocksDbWriteBatch::with_capacity(1);
        RocksDbTimelineStateIndex::append_state(
            &mut state_batch,
            entry.key.timer_id,
            entry.key.generation,
            &state_record(),
        )
        .expect("state");
        timeline.write_batch(&state_batch).expect("write state");

        let scanner = TimelineDueScanner::new_segmented(TimerStoreConfig::default(), Arc::clone(&timeline), native);
        assert_eq!(scanner.scan_formal_until(8_000).expect("scan").ready, 1);
        assert_eq!(
            timeline
                .state_index()
                .get(entry.key.timer_id, entry.key.generation)
                .expect("state")
                .expect("present")
                .state,
            TimelineState::Ready
        );
    }

    /// Five entries due within one safety overlap; the first three were promoted by an earlier
    /// run, which on their own fill a page of two.
    fn entries_behind_processed_ones() -> Vec<(TimelineIndexEntry, TimelineState)> {
        (0..5)
            .map(|index| {
                let mut entry = formal_entry(8_000 + index, 10 + index as u128);
                entry.record.payload =
                    TimerPayloadStoreLocator::try_new(0, entry.key.lane, 1, 0, 10, 11).expect("locator");
                let state = if index < 3 {
                    TimelineState::Ready
                } else {
                    TimelineState::Pending
                };
                (entry, state)
            })
            .collect()
    }

    fn put_states(timeline: &RocksDbTimelineIndex, entries: &[(TimelineIndexEntry, TimelineState)]) {
        let mut batch = RocksDbWriteBatch::with_capacity(entries.len());
        for (entry, state) in entries {
            RocksDbTimelineStateIndex::append_state(
                &mut batch,
                entry.key.timer_id,
                entry.key.generation,
                &TimelineStateRecordV1 {
                    state: *state,
                    ..state_record()
                },
            )
            .expect("state");
        }
        timeline.write_batch(&batch).expect("write states");
    }

    fn promote_with_pages_of_two(scanner: &TimelineDueScanner) -> usize {
        (0..3)
            .map(|_| {
                scanner
                    .scan_formal_until_with_budget(9_000, 2, TimerStoreConfig::default().due_scan_bytes)
                    .expect("scan")
                    .ready
            })
            .sum()
    }

    fn assert_all_ready(timeline: &RocksDbTimelineIndex, entries: &[(TimelineIndexEntry, TimelineState)]) {
        for (entry, _) in entries {
            let state = timeline
                .state_index()
                .get(entry.key.timer_id, entry.key.generation)
                .expect("state")
                .expect("present")
                .state;
            assert_eq!(state, TimelineState::Ready, "{:?}", entry.key);
        }
    }

    #[test]
    fn formal_scan_resumes_after_processed_entries_that_fill_a_page() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("open")
                .expect("valid Timeline configuration"),
        );
        let entries = entries_behind_processed_ones();
        let mut batch = RocksDbWriteBatch::with_capacity(entries.len());
        for (entry, _) in &entries {
            RocksDbTimelineIndex::append_entry(&mut batch, entry).expect("entry");
        }
        timeline.write_batch(&batch).expect("write entries");
        put_states(&timeline, &entries);
        let scanner = TimelineDueScanner::new(TimerStoreConfig::default(), Arc::clone(&timeline));

        assert_eq!(promote_with_pages_of_two(&scanner), 2);
        assert_all_ready(&timeline, &entries);
    }

    #[test]
    fn segmented_formal_scan_resumes_after_processed_entries_that_fill_a_page() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("open overlay")
                .expect("valid overlay configuration"),
        );
        let native = Arc::new(
            SegmentedTimeline::open(dir.path(), SegmentedTimelineConfig::default())
                .expect("open native")
                .expect("valid native configuration"),
        );
        let entries = entries_behind_processed_ones();
        let records: Vec<_> = entries
            .iter()
            .map(|(entry, _)| TimelineSegmentRecord {
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
                shadow_only: false,
            })
            .collect();
        native
            .append_batch(&records)
            .expect("native entries")
            .expect("valid native entries");
        put_states(&timeline, &entries);
        let scanner = TimelineDueScanner::new_segmented(TimerStoreConfig::default(), Arc::clone(&timeline), native);

        assert_eq!(promote_with_pages_of_two(&scanner), 2);
        assert_all_ready(&timeline, &entries);
    }

    #[test]
    fn shadow_scan_resumes_after_the_last_observed_entry() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("open")
                .expect("valid Timeline configuration"),
        );
        let entries: Vec<_> = (0..5)
            .map(|index| {
                let mut entry = formal_entry(8_000 + index, 20 + index as u128);
                entry.record.owner_engine = TimerEngineId::JavaCompat;
                entry.record.shadow_only = true;
                entry
            })
            .collect();
        timeline.put_batch(&entries, None).expect("shadow entries");
        let scanner = TimelineDueScanner::new(TimerStoreConfig::default(), Arc::clone(&timeline));

        for _ in 0..3 {
            scanner
                .scan_shadow_until_with_budget(9_000, 2, TimerStoreConfig::default().due_scan_bytes)
                .expect("scan");
        }

        for entry in &entries {
            assert!(
                timeline
                    .shadow_observation(
                        entry.record.source_cq_offset.get(),
                        entry.record.source_physical_offset,
                        entry.key.generation,
                        ShadowObservationKind::Due,
                    )
                    .expect("observation")
                    .is_some(),
                "{:?}",
                entry.key
            );
        }
    }
}
