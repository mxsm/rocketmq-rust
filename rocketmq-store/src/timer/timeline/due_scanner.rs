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

#[derive(Clone, Debug, Default)]
struct FormalDuePage {
    entries: Vec<TimelineIndexEntry>,
    continuation: Option<FormalDueContinuation>,
}

/// Synchronous, bounded read contract used by the blocking Due Scanner. Mutable state, ready,
/// receipts, and checkpoints remain in the RocksDB overlay for both implementations.
trait TimelineDueIndex: Send + Sync {
    fn range_scan(
        &self,
        start_due_ms: i64,
        end_due_exclusive_ms: i64,
        continuation: Option<FormalDueContinuation>,
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
        start_due_ms: i64,
        end_due_exclusive_ms: i64,
        continuation: Option<FormalDueContinuation>,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<FormalDuePage, TimelineDueScannerError> {
        let continuation = match continuation {
            Some(FormalDueContinuation::Rocks(key)) => Some(key),
            Some(FormalDueContinuation::Segmented(_)) => return Err(TimelineDueScannerError::CursorBackendMismatch),
            None => None,
        };
        let page = self.timeline.range_scan(
            start_due_ms,
            end_due_exclusive_ms,
            continuation,
            max_messages,
            max_bytes,
        )?;
        Ok(FormalDuePage {
            entries: page.entries,
            continuation: page.continuation.map(FormalDueContinuation::Rocks),
        })
    }
}

struct SegmentedTimelineDueIndex {
    timeline: Arc<SegmentedTimeline>,
}

impl TimelineDueIndex for SegmentedTimelineDueIndex {
    fn range_scan(
        &self,
        start_due_ms: i64,
        end_due_exclusive_ms: i64,
        continuation: Option<FormalDueContinuation>,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<FormalDuePage, TimelineDueScannerError> {
        let continuation = match continuation {
            Some(FormalDueContinuation::Segmented(cursor)) => Some(cursor),
            Some(FormalDueContinuation::Rocks(_)) => return Err(TimelineDueScannerError::CursorBackendMismatch),
            None => None,
        };
        let page = self.timeline.scan_due(
            Some(TimelineSegmentKey {
                due_time_ms: start_due_ms.max(0),
                lane: 0,
                timer_id: rocketmq_store_api::TimerId::new(0),
                generation: rocketmq_store_api::TimerGeneration::new(0),
            }),
            end_due_exclusive_ms,
            max_messages,
            max_bytes.max(max_messages.saturating_mul(TimelineSegmentRecord::encoded_size())),
            continuation,
        )?;
        Ok(FormalDuePage {
            entries: page.records.into_iter().map(native_entry).collect(),
            continuation: page.continuation.map(FormalDueContinuation::Segmented),
        })
    }
}

/// Bounded, overlap-safe Timeline scanner.
pub(crate) struct TimelineDueScanner {
    config: TimerStoreConfig,
    timeline: Arc<RocksDbTimelineIndex>,
    formal_index: Arc<dyn TimelineDueIndex>,
    state: RocksDbTimelineStateIndex,
    shadow_reconciler: Option<Arc<ShadowReconciler>>,
    clock: Option<Arc<TimerClockSafety>>,
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
        }
    }

    /// Records due observations for Java-compatible shadow entries without creating ready work.
    pub(crate) fn scan_shadow_until(&self, now_ms: i64) -> Result<DueScanResult, TimelineDueScannerError> {
        let checkpoint = self.checkpoint(SHADOW_DUE_CHECKPOINT_LANE)?;
        let start_ms = checkpoint
            .due_cursor
            .due_time_ms()
            .saturating_sub(self.config.safety_overlap_ms);
        let mut continuation = None;
        let mut result = DueScanResult::default();
        loop {
            let page = self.timeline.range_scan_shadow(
                start_ms,
                now_ms.saturating_add(1),
                continuation,
                self.config.due_scan_messages,
                self.config.due_scan_bytes,
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
            self.timeline.write_batch(&batch)?;
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
        let now_ms = if let Some(clock) = self.clock.as_ref() {
            let observation = clock.observe();
            if observation.state == TimerClockState::Unsafe {
                return Err(TimelineDueScannerError::ClockUnsafe);
            }
            observation.wall_time_ms.min(now_ms)
        } else {
            now_ms
        };
        let mut result = self.drain_late_ready()?;
        let checkpoint = self.checkpoint(FORMAL_DUE_CHECKPOINT_LANE)?;
        let start_ms = checkpoint
            .due_cursor
            .due_time_ms()
            .saturating_sub(self.config.safety_overlap_ms);
        let mut continuation = None;
        loop {
            let page = self.formal_index.range_scan(
                start_ms,
                now_ms.saturating_add(1),
                continuation,
                self.config.due_scan_messages,
                self.config.due_scan_bytes,
            )?;
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
            result.observed = result.observed.saturating_add(page.entries.len());
            result.pages = result.pages.saturating_add(1);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        Ok(result)
    }

    fn drain_late_ready(&self) -> Result<DueScanResult, TimelineDueScannerError> {
        let outbox = TimelineReadyOutbox::new(Arc::clone(&self.timeline));
        let mut result = DueScanResult::default();
        for lane in 0..self.config.lane_count {
            let lane = u16::try_from(lane).map_err(|_| TimelineDueScannerError::LaneOverflow)?;
            for key in outbox.scan_late_ready(lane, self.config.due_scan_messages)? {
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
}

#[derive(Debug, Error)]
pub(crate) enum TimelineDueScannerError {
    #[error("Timeline store failure: {0}")]
    Timeline(#[from] rocketmq_error::RocketMQError),
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
    #[error("Timeline continuation belongs to a different index backend")]
    CursorBackendMismatch,
    #[error("native Timeline failure: {0}")]
    Native(#[from] rocketmq_store_local::timer::segmented_timeline::SegmentedTimelineError),
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
        let timeline = Arc::new(RocksDbTimelineIndex::open(dir.path()).expect("open"));
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
        let timeline = Arc::new(RocksDbTimelineIndex::open(dir.path()).expect("open"));
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
        let timeline = Arc::new(RocksDbTimelineIndex::open(dir.path()).expect("open"));
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
        let timeline = Arc::new(RocksDbTimelineIndex::open(dir.path()).expect("open overlay"));
        let native =
            Arc::new(SegmentedTimeline::open(dir.path(), SegmentedTimelineConfig::default()).expect("open native"));
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
            .expect("native entry");
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
}
