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

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_store_api::DerivedCheckpoint;
use rocketmq_store_api::DerivedCursor;
use rocketmq_store_api::DerivedEngine;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::state_index::StateTransitionResult;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::CHECKPOINT_CF;
use thiserror::Error;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::log_file::commit_log::CommitLogReadHandle;
use crate::log_file::commit_log::BLANK_MAGIC_CODE;
use crate::timer::timer_message_store::TIMER_TOPIC;

use super::TimelineCompletionReceiptV1;
use super::TimelineReadyOutbox;
use super::TimelineReceiptStore;

const COMPLETION_CURSOR_KEY: &[u8] = b"timer-completion-physical-v1";
const FRAME_HEADER_SIZE: usize = 8;
/// Entries the feed holds before it gives up its range to the CommitLog scan. Other frames
/// collapse into runs, so this bounds the final facts waiting for durability, not traffic.
const FEED_CAPACITY: usize = 65_536;

/// Completion facts recorded on the Reput dispatch path.
///
/// Reput hands this dispatcher every CommitLog frame in offset order. Once armed, the feed keeps
/// the frames it has seen since `start` without a gap: each Extended final delivery as its own
/// entry with the identity parsed from the dispatch properties, and every run of other frames
/// as one entry. The reconciler applies these entries instead of reading and decoding the
/// CommitLog again. A range the feed cannot vouch for (frames before the first one it saw, a
/// gap, a rewind, an overflow) stays behind `start`, and the reconciler scans it from the
/// CommitLog.
#[derive(Debug, Default)]
pub(crate) struct TimelineCompletionFeed {
    state: Mutex<FeedState>,
}

#[derive(Debug, Default)]
struct FeedState {
    armed: bool,
    /// Offset of the first frame the entries cover; `None` until a frame arrives after arming
    /// or after a reset.
    start: Option<i64>,
    /// End of the last frame the entries cover.
    end: i64,
    /// Whether the last `Frames` entry may still grow. A dispatch batch ends on a frame boundary
    /// and closes it, so the reconciler cursor can stop there.
    run_open: bool,
    entries: VecDeque<FeedEntry>,
}

#[derive(Clone, Debug)]
enum FeedEntry {
    /// Frames without a final fact, ending at `end`.
    Frames { end: i64 },
    /// One final delivery frame.
    Final {
        offset: i64,
        size: u32,
        fact: FinalTimerFact,
    },
    /// One frame routed to the Extended engine whose identity properties are incomplete.
    Malformed { end: i64 },
}

impl FeedEntry {
    fn end(&self) -> i64 {
        match self {
            Self::Frames { end } | Self::Malformed { end } => *end,
            Self::Final { offset, size, .. } => offset.saturating_add(i64::from(*size)),
        }
    }

    fn is_fact(&self) -> bool {
        !matches!(self, Self::Frames { .. })
    }
}

impl TimelineCompletionFeed {
    /// Starts recording. Before this, the feed records nothing, so a Store without a formal
    /// Extended engine keeps no entries.
    fn arm(&self) {
        self.state.lock().armed = true;
    }

    /// The offset where the feed's coverage starts when that is after `cursor`, or the safe
    /// limit when the feed covers nothing: the CommitLog has to be scanned up to there.
    fn uncovered_limit(&self, cursor: i64, safe_limit: i64) -> Option<i64> {
        match self.state.lock().start {
            Some(start) if start <= cursor => None,
            Some(start) => Some(start.min(safe_limit)),
            None => Some(safe_limit),
        }
    }

    /// Removes and returns the entries after `cursor` that end at or before `safe_limit`, taking
    /// at most `max_records` final facts and, after the first one, at most `max_bytes` of them.
    fn take_through(&self, cursor: i64, safe_limit: i64, max_records: usize, max_bytes: usize) -> Vec<FeedEntry> {
        let mut state = self.state.lock();
        if state.start.is_none_or(|start| start > cursor) {
            return Vec::new();
        }
        let mut taken = Vec::new();
        let (mut records, mut bytes) = (0usize, 0usize);
        while let Some(front) = state.entries.front() {
            let end = front.end();
            if end > safe_limit {
                break;
            }
            if end > cursor && front.is_fact() {
                let size = match front {
                    FeedEntry::Final { size, .. } => *size as usize,
                    FeedEntry::Frames { .. } | FeedEntry::Malformed { .. } => 0,
                };
                if records >= max_records || (records > 0 && bytes.saturating_add(size) > max_bytes) {
                    break;
                }
                records += 1;
                bytes = bytes.saturating_add(size);
            }
            if let Some(entry) = state.entries.pop_front() {
                if end > cursor {
                    taken.push(entry);
                }
            }
            state.start = Some(end);
        }
        taken
    }

    /// Whether every final fact after `cursor` that ends at or before `safe_limit` has been
    /// taken, with every frame up to `safe_limit` dispatched.
    fn holds_no_fact_through(&self, cursor: i64, safe_limit: i64) -> bool {
        let state = self.state.lock();
        state.start.is_some_and(|start| start <= cursor)
            && state.end >= safe_limit
            && !state
                .entries
                .iter()
                .take_while(|entry| entry.end() <= safe_limit)
                .any(|entry| entry.is_fact() && entry.end() > cursor)
    }
}

impl FeedState {
    fn record(&mut self, request: &DispatchRequest) {
        if !self.armed {
            return;
        }
        // Without parsed properties the feed cannot tell whether the frame is a final fact, so
        // it leaves the frame to the CommitLog scan.
        if !request.success
            || request.commit_log_offset < 0
            || request.msg_size <= 0
            || request.properties_map.is_none()
        {
            self.reset();
            return;
        }
        let offset = request.commit_log_offset;
        if self.start.is_none() || offset != self.end {
            // A first frame, a gap or a rewind: coverage starts over at this frame.
            self.restart_at(offset);
        }
        let end = offset.saturating_add(i64::from(request.msg_size));
        match FinalTimerFact::from_dispatch(request) {
            Ok(Some(fact)) => self.push_fact(FeedEntry::Final {
                offset,
                size: request.msg_size.unsigned_abs(),
                fact,
            }),
            Ok(None) => self.extend_frames(end),
            Err(_) => self.push_fact(FeedEntry::Malformed { end }),
        }
        self.end = end;
        if self.entries.len() > FEED_CAPACITY {
            self.reset();
        }
    }

    fn record_blank(&mut self, blank_start_offset: i64, next_file_offset: i64) {
        if !self.armed {
            return;
        }
        if self.start.is_none() || blank_start_offset != self.end {
            self.restart_at(next_file_offset);
            return;
        }
        self.extend_frames(next_file_offset);
        self.end = next_file_offset;
    }

    fn push_fact(&mut self, entry: FeedEntry) {
        self.entries.push_back(entry);
        self.run_open = false;
    }

    fn extend_frames(&mut self, end: i64) {
        if self.run_open {
            if let Some(FeedEntry::Frames { end: run_end }) = self.entries.back_mut() {
                *run_end = end;
                return;
            }
        }
        self.entries.push_back(FeedEntry::Frames { end });
        self.run_open = true;
    }

    fn restart_at(&mut self, offset: i64) {
        self.entries.clear();
        self.start = Some(offset);
        self.end = offset;
        self.run_open = false;
    }

    fn reset(&mut self) {
        self.entries.clear();
        self.start = None;
        self.run_open = false;
    }
}

impl CommitLogDispatcher for TimelineCompletionFeed {
    fn supports_parallel_dispatch(&self) -> bool {
        true
    }

    fn dispatch(&self, request: &mut DispatchRequest) {
        self.state.lock().record(request);
    }

    fn dispatch_batch(&self, requests: &mut [DispatchRequest]) {
        let mut state = self.state.lock();
        for request in requests.iter() {
            state.record(request);
        }
        state.run_open = false;
    }

    fn dispatch_commit_log_blank(&self, blank_start_offset: i64, next_file_offset: i64) {
        self.state.lock().record_blank(blank_start_offset, next_file_offset);
    }
}

/// Sequentially derives durable completion receipts from the replicated final CommitLog.
pub(crate) struct TimelineCompletionReconciler {
    timeline: Arc<RocksDbTimelineIndex>,
    commit_log: CommitLogReadHandle,
    feed: Arc<TimelineCompletionFeed>,
    run_lock: Mutex<()>,
}

impl TimelineCompletionReconciler {
    pub(crate) fn new(
        timeline: Arc<RocksDbTimelineIndex>,
        commit_log: CommitLogReadHandle,
        feed: Arc<TimelineCompletionFeed>,
    ) -> Self {
        feed.arm();
        Self {
            timeline,
            commit_log,
            feed,
            run_lock: Mutex::new(()),
        }
    }

    pub(crate) fn completion_physical_cursor(&self) -> Result<i64, TimelineCompletionError> {
        let value = self
            .timeline
            .store()
            .get_cf(StoreOperation::Load, CHECKPOINT_CF, COMPLETION_CURSOR_KEY)?;
        let Some(value) = value else {
            return Ok(self.commit_log.get_min_offset().max(0));
        };
        let checkpoint = DerivedCheckpoint::decode(&value, DerivedEngine::TimerCompletion)?;
        i64::try_from(checkpoint.cursor().next_offset()).map_err(|_| TimelineCompletionError::CursorOverflow)
    }

    /// Whether every final fact in the durable CommitLog prefix has been reconciled.
    pub(crate) fn is_caught_up(&self) -> Result<bool, TimelineCompletionError> {
        let cursor = self.completion_physical_cursor()?;
        let safe_limit = self.safe_replay_limit();
        Ok(cursor >= safe_limit || self.feed.holds_no_fact_through(cursor, safe_limit))
    }

    /// Reconciles one bounded part of the durable CommitLog prefix: first any range the feed
    /// does not cover, read back from the CommitLog, then the feed's entries. `max_records` and
    /// `max_bytes` bound the frames scanned plus the final facts applied; frames without a final
    /// fact cost nothing once the feed holds them.
    pub(crate) fn run_once(
        &self,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<CompletionReconcileResult, TimelineCompletionError> {
        if max_records == 0 || max_bytes == 0 {
            return Err(TimelineCompletionError::InvalidBudget);
        }
        let _run_guard = self.run_lock.lock();
        let mut cursor = self.completion_physical_cursor()?;
        let safe_limit = self.safe_replay_limit();
        let mut result = CompletionReconcileResult::default();
        if let Some(scan_limit) = self.feed.uncovered_limit(cursor, safe_limit) {
            cursor = self.scan(cursor, scan_limit, max_records, max_bytes, &mut result)?;
        }
        if result.records < max_records && result.bytes < max_bytes {
            let entries = self.feed.take_through(
                cursor,
                safe_limit,
                max_records - result.records,
                max_bytes - result.bytes,
            );
            cursor = self.apply_feed_entries(cursor, entries, &mut result)?;
        }
        result.completion_physical_cursor = cursor;
        Ok(result)
    }

    fn apply_feed_entries(
        &self,
        mut cursor: i64,
        entries: Vec<FeedEntry>,
        result: &mut CompletionReconcileResult,
    ) -> Result<i64, TimelineCompletionError> {
        let mut persisted = cursor;
        for entry in entries {
            let end = entry.end();
            match entry {
                FeedEntry::Frames { .. } => {}
                FeedEntry::Final { offset, size, fact } => {
                    self.reconcile_fact(offset, size, &fact, end)?;
                    persisted = end;
                    result.records = result.records.saturating_add(1);
                    result.bytes = result.bytes.saturating_add(size as usize);
                }
                FeedEntry::Malformed { .. } => {
                    // The feed has moved past this frame; leaving the cursor in front of it makes
                    // the next run scan it and fail the same way.
                    if cursor != persisted {
                        self.persist_cursor(cursor)?;
                    }
                    return Err(TimelineCompletionError::MalformedFinalFact);
                }
            }
            cursor = end;
        }
        if cursor != persisted {
            self.persist_cursor(cursor)?;
        }
        Ok(cursor)
    }

    /// Reads frames from the CommitLog between `cursor` and `scan_limit`, for a range the feed
    /// does not cover. Only frames that end at or before `scan_limit` are reconciled.
    fn scan(
        &self,
        mut cursor: i64,
        scan_limit: i64,
        max_records: usize,
        max_bytes: usize,
        result: &mut CompletionReconcileResult,
    ) -> Result<i64, TimelineCompletionError> {
        let mut persisted = cursor;
        let outcome = self.scan_frames(&mut cursor, &mut persisted, scan_limit, max_records, max_bytes, result);
        // Frames without a final fact only move the cursor; it is written once per scan.
        if cursor != persisted {
            self.persist_cursor(cursor)?;
        }
        outcome.map(|()| cursor)
    }

    fn scan_frames(
        &self,
        cursor: &mut i64,
        persisted: &mut i64,
        scan_limit: i64,
        max_records: usize,
        max_bytes: usize,
        result: &mut CompletionReconcileResult,
    ) -> Result<(), TimelineCompletionError> {
        while *cursor < scan_limit && result.records < max_records && result.bytes < max_bytes {
            let Some(selection) = self.commit_log.get_data(*cursor) else {
                break;
            };
            let bytes = selection.get_buffer();
            if bytes.len() < FRAME_HEADER_SIZE {
                break;
            }
            let mut declared_bytes = [0_u8; 4];
            declared_bytes.copy_from_slice(&bytes[0..4]);
            let declared = i32::from_be_bytes(declared_bytes);
            let mut magic_bytes = [0_u8; 4];
            magic_bytes.copy_from_slice(&bytes[4..8]);
            let magic = i32::from_be_bytes(magic_bytes);
            if declared <= 0 {
                return Err(TimelineCompletionError::InvalidFrame(*cursor));
            }
            let frame_size = usize::try_from(declared).map_err(|_| TimelineCompletionError::InvalidFrame(*cursor))?;
            let next_cursor = cursor
                .checked_add(i64::from(declared))
                .ok_or(TimelineCompletionError::CursorOverflow)?;
            if next_cursor > scan_limit {
                break;
            }
            if magic == BLANK_MAGIC_CODE {
                *cursor = next_cursor;
                continue;
            }
            if frame_size > max_bytes.saturating_sub(result.bytes) && result.records > 0 {
                break;
            }
            let frame = self
                .commit_log
                .get_message(*cursor, declared)
                .and_then(|selection| selection.get_bytes())
                .ok_or(TimelineCompletionError::FrameUnavailable(*cursor))?;
            if let Some(fact) = FinalTimerFact::from_message(&decode_message(frame)?)? {
                let record_size = u32::try_from(frame_size).map_err(|_| TimelineCompletionError::CursorOverflow)?;
                self.reconcile_fact(*cursor, record_size, &fact, next_cursor)?;
                *persisted = next_cursor;
            }
            *cursor = next_cursor;
            result.records = result.records.saturating_add(1);
            result.bytes = result.bytes.saturating_add(frame_size);
        }
        Ok(())
    }

    fn safe_replay_limit(&self) -> i64 {
        self.commit_log
            .get_flushed_where()
            .max(self.commit_log.get_confirm_offset())
            .min(self.commit_log.get_max_offset())
            .max(0)
    }

    /// Records the receipt of one final delivery and moves the cursor to `next_cursor` in the
    /// same write.
    fn reconcile_fact(
        &self,
        physical_offset: i64,
        record_size: u32,
        final_fact: &FinalTimerFact,
        next_cursor: i64,
    ) -> Result<(), TimelineCompletionError> {
        let receipt = TimelineCompletionReceiptV1 {
            timer_id: final_fact.timer_id,
            generation: final_fact.generation,
            owner_epoch: final_fact.owner_epoch,
            due_time_ms: final_fact.due_time_ms,
            lane: final_fact.lane,
            final_physical_offset: physical_offset,
            final_record_size: record_size,
        };
        let mut side_effects = RocksDbWriteBatch::with_capacity(4);
        TimelineReceiptStore::append(&mut side_effects, &final_fact.delivery_token, receipt)?;
        let key = TimelineKeyV1 {
            due_time_ms: final_fact.due_time_ms,
            lane: final_fact.lane,
            timer_id: final_fact.timer_id,
            generation: final_fact.generation,
        };
        TimelineReadyOutbox::delete_ready(&mut side_effects, key);
        TimelineReadyOutbox::delete_late_ready(&mut side_effects, key);
        append_cursor(&mut side_effects, next_cursor)?;

        match self.timeline.state_index().complete_from_receipt(
            final_fact.timer_id,
            final_fact.generation,
            &final_fact.delivery_token,
            final_fact.owner_epoch,
            side_effects,
        )? {
            StateTransitionResult::Applied(_) => Ok(()),
            StateTransitionResult::Conflict(current) if current.state == TimelineState::Delivered => {
                let mut duplicate = RocksDbWriteBatch::with_capacity(4);
                TimelineReceiptStore::append(&mut duplicate, &final_fact.delivery_token, receipt)?;
                TimelineReadyOutbox::delete_ready(&mut duplicate, key);
                TimelineReadyOutbox::delete_late_ready(&mut duplicate, key);
                append_cursor(&mut duplicate, next_cursor)?;
                self.timeline.write_batch(&duplicate)?;
                Ok(())
            }
            StateTransitionResult::Missing => {
                // Snapshot/bootstrap may observe the replicated final fact before its state
                // catch-up stream. Persisting the receipt makes later reconciliation idempotent.
                let mut pending_state = RocksDbWriteBatch::with_capacity(2);
                TimelineReceiptStore::append(&mut pending_state, &final_fact.delivery_token, receipt)?;
                append_cursor(&mut pending_state, next_cursor)?;
                self.timeline.write_batch(&pending_state)?;
                Ok(())
            }
            StateTransitionResult::Conflict(_) => Err(TimelineCompletionError::StateConflict {
                timer_id: final_fact.timer_id.get(),
                generation: final_fact.generation.get(),
            }),
        }
    }

    fn persist_cursor(&self, next_cursor: i64) -> Result<(), TimelineCompletionError> {
        let mut batch = RocksDbWriteBatch::with_capacity(1);
        append_cursor(&mut batch, next_cursor)?;
        self.timeline.write_batch(&batch)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct FinalTimerFact {
    timer_id: TimerId,
    generation: TimerGeneration,
    owner_epoch: TimerEngineEpoch,
    due_time_ms: i64,
    lane: u16,
    delivery_token: String,
}

impl FinalTimerFact {
    fn from_message(message: &MessageExt) -> Result<Option<Self>, TimelineCompletionError> {
        Self::from_parts(message.topic().as_str(), |key| {
            message
                .property(&CheetahString::from_static_str(key))
                .map(|value| value.to_string())
        })
    }

    fn from_dispatch(request: &DispatchRequest) -> Result<Option<Self>, TimelineCompletionError> {
        Self::from_parts(request.topic.as_str(), |key| {
            request
                .properties_map
                .as_ref()?
                .get(&CheetahString::from_static_str(key))
                .map(|value| value.to_string())
        })
    }

    fn from_parts(
        topic: &str,
        property: impl Fn(&'static str) -> Option<String>,
    ) -> Result<Option<Self>, TimelineCompletionError> {
        // Extended source records carry the engine route as well, but they are not final
        // delivery facts.  Completion is derived only after the message has left the
        // internal Timer topic and has been appended to its real topic.
        if topic == TIMER_TOPIC {
            return Ok(None);
        }
        let engine = property(MessageConst::TIMER_ENGINE_TYPE);
        if engine.as_deref() != Some(TimerEngineId::ExtendedTimeline.as_str()) {
            return Ok(None);
        }
        let timer_id = property(MessageConst::PROPERTY_TIMER_ID)
            .and_then(|value| value.parse::<u128>().ok())
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let generation = property(MessageConst::PROPERTY_TIMER_GENERATION)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let owner_epoch = property(MessageConst::PROPERTY_TIMER_OWNER_EPOCH)
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let due_time_ms = property(MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS)
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value >= 0)
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let lane = property(MessageConst::PROPERTY_TIMER_LANE)
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let delivery_token = property(MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN)
            .filter(|value| !value.is_empty())
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        Ok(Some(Self {
            timer_id: TimerId::new(timer_id),
            generation: TimerGeneration::new(generation),
            owner_epoch: TimerEngineEpoch::new(owner_epoch),
            due_time_ms,
            lane,
            delivery_token,
        }))
    }
}

fn append_cursor(batch: &mut RocksDbWriteBatch, next_cursor: i64) -> Result<(), TimelineCompletionError> {
    let next_offset = u64::try_from(next_cursor).map_err(|_| TimelineCompletionError::CursorOverflow)?;
    let checkpoint = DerivedCheckpoint::new(DerivedEngine::TimerCompletion, DerivedCursor::restore(0, next_offset));
    batch.put_cf(CHECKPOINT_CF, COMPLETION_CURSOR_KEY, checkpoint.encode());
    Ok(())
}

/// Decodes a frame's topic and properties; the body is not needed for a final fact.
fn decode_message(frame: Bytes) -> Result<MessageExt, TimelineCompletionError> {
    MessageDecoder::decode(&mut frame.clone(), false, false, false, false, false)
        .ok_or(TimelineCompletionError::MalformedFrame)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CompletionReconcileResult {
    pub(crate) records: usize,
    pub(crate) bytes: usize,
    pub(crate) completion_physical_cursor: i64,
}

#[derive(Debug, Error)]
pub(crate) enum TimelineCompletionError {
    #[error(transparent)]
    Store(#[from] rocketmq_store_api::StoreError),
    #[error("completion checkpoint decode failed: {0}")]
    Checkpoint(#[from] rocketmq_store_api::StoreContractViolation),
    #[error("completion replay budget must be non-zero")]
    InvalidBudget,
    #[error("completion cursor overflow")]
    CursorOverflow,
    #[error("invalid CommitLog frame at {0}")]
    InvalidFrame(i64),
    #[error("CommitLog frame at {0} is unavailable")]
    FrameUnavailable(i64),
    #[error("CommitLog frame cannot be decoded")]
    MalformedFrame,
    #[error("Extended final fact is missing required identity properties")]
    MalformedFinalFact,
    #[error("final fact conflicts with timer {timer_id} generation {generation} state")]
    StateConflict { timer_id: u128, generation: u64 },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_model::common::message::MessageConst;
    use rocketmq_model::common::message::MessageTrait;
    use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
    use rocketmq_store_api::TimerEngineId;
    use rocketmq_store_api::TimerStoreMode;

    use super::FeedEntry;
    use super::TimelineCompletionFeed;
    use crate::base::backend_ops::BackendOps;
    use crate::base::commit_log_dispatcher::CommitLogDispatcher;
    use crate::base::dispatch_request::DispatchRequest;
    use crate::config::store_runtime_config::StoreRuntimeConfig;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;
    use crate::timer::timer_message_store::TIMER_TOPIC;
    use crate::MessageStoreConfig;

    const REAL_TOPIC: &str = "completion-feed-real-topic";

    fn final_fact_properties(timer_id: u128) -> Vec<(&'static str, String)> {
        vec![
            (
                MessageConst::TIMER_ENGINE_TYPE,
                TimerEngineId::ExtendedTimeline.as_str().to_owned(),
            ),
            (MessageConst::PROPERTY_TIMER_ID, timer_id.to_string()),
            (MessageConst::PROPERTY_TIMER_GENERATION, "0".to_owned()),
            (MessageConst::PROPERTY_TIMER_OWNER_EPOCH, "7".to_owned()),
            (MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS, "1000".to_owned()),
            (MessageConst::PROPERTY_TIMER_LANE, "0".to_owned()),
            (
                MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
                format!("R:1:completion-feed:{timer_id}"),
            ),
        ]
    }

    fn request(offset: i64, size: i32, properties: Vec<(&'static str, String)>) -> DispatchRequest {
        DispatchRequest {
            success: true,
            topic: CheetahString::from_static_str(REAL_TOPIC),
            commit_log_offset: offset,
            msg_size: size,
            properties_map: Some(
                properties
                    .into_iter()
                    .map(|(key, value)| (CheetahString::from_static_str(key), CheetahString::from_string(value)))
                    .collect::<HashMap<_, _>>(),
            ),
            ..DispatchRequest::default()
        }
    }

    fn armed_feed() -> TimelineCompletionFeed {
        let feed = TimelineCompletionFeed::default();
        feed.arm();
        feed
    }

    #[test]
    fn feed_keeps_final_facts_and_collapses_other_frames() {
        let feed = armed_feed();
        let mut batch: Vec<_> = (0..1_000).map(|index| request(index * 100, 100, Vec::new())).collect();
        batch.push(request(100_000, 200, final_fact_properties(9)));
        batch.extend((0..500).map(|index| request(100_200 + index * 100, 100, Vec::new())));
        feed.dispatch_batch(&mut batch);

        assert_eq!(feed.state.lock().entries.len(), 3);
        assert!(feed.holds_no_fact_through(0, 100_000));
        assert!(!feed.holds_no_fact_through(0, 150_200));

        let taken = feed.take_through(0, 150_200, 1, 1);
        assert_eq!(taken.len(), 3);
        assert!(matches!(
            &taken[1],
            FeedEntry::Final { offset: 100_000, size: 200, fact } if fact.timer_id.get() == 9
        ));
        assert_eq!(taken[2].end(), 150_200);
        assert!(feed.holds_no_fact_through(150_200, 150_200));
    }

    #[test]
    fn feed_does_not_vouch_for_frames_it_did_not_see_in_order() {
        let feed = TimelineCompletionFeed::default();
        feed.dispatch_batch(&mut [request(0, 100, Vec::new())]);
        assert_eq!(feed.uncovered_limit(0, 100), Some(100));

        feed.arm();
        feed.dispatch_batch(&mut [request(100, 100, Vec::new()), request(200, 100, Vec::new())]);
        assert_eq!(feed.uncovered_limit(0, 300), Some(100));
        assert_eq!(feed.uncovered_limit(100, 300), None);

        // A gap starts coverage over, and so does a rewind.
        feed.dispatch_batch(&mut [request(500, 100, Vec::new())]);
        assert_eq!(feed.uncovered_limit(300, 600), Some(500));
        feed.dispatch_batch(&mut [request(400, 100, final_fact_properties(3))]);
        assert_eq!(feed.uncovered_limit(300, 600), Some(400));
        assert!(feed.take_through(300, 600, 8, 1_024).is_empty());

        // A CommitLog BLANK extends coverage to the next file only when it follows the last frame.
        feed.dispatch_commit_log_blank(500, 4_096);
        let taken = feed.take_through(400, 4_096, 8, 1_024);
        assert_eq!(taken.iter().map(FeedEntry::end).collect::<Vec<_>>(), vec![500, 4_096]);
        feed.dispatch_commit_log_blank(5_000, 8_192);
        assert_eq!(feed.uncovered_limit(4_096, 8_192), Some(8_192));
    }

    fn plain_message(index: usize) -> MessageExtBrokerInner {
        let mut message = MessageExtBrokerInner::default();
        message.set_topic(CheetahString::from_static_str(REAL_TOPIC));
        message.message_ext_inner.queue_id = 0;
        message.set_body(Bytes::from(format!("plain-{index}")));
        message.properties_string = message_properties_to_string(message.get_properties());
        message
    }

    fn final_message(timer_id: u128) -> MessageExtBrokerInner {
        let mut message = plain_message(0);
        for (key, value) in final_fact_properties(timer_id) {
            message.put_property(CheetahString::from_static_str(key), CheetahString::from_string(value));
        }
        message.properties_string = message_properties_to_string(message.get_properties());
        message
    }

    #[tokio::test]
    async fn one_run_passes_ordinary_traffic_without_spending_its_budget() {
        let root = tempfile::tempdir().expect("store root");
        let config = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            read_uncommitted: true,
            timer_wheel_enable: false,
            timer_store_mode: TimerStoreMode::ExtendedTimeline,
            timer_extended_shadow_enable: false,
            timer_extended_admission_enable: true,
            timer_extended_activation_epoch: 7,
            timer_extended_admission_horizon_days: 3,
            mapped_file_size_commit_log: 64 * 1024,
            ..MessageStoreConfig::default()
        };
        let topics = Arc::new(DashMap::new());
        topics.insert(
            CheetahString::from_static_str(TIMER_TOPIC),
            Arc::new(TopicConfig::default()),
        );
        topics.insert(
            CheetahString::from_static_str(REAL_TOPIC),
            Arc::new(TopicConfig::default()),
        );
        let mut store = LocalFileMessageStore::new(
            Arc::new(config),
            rocketmq_store_local::commit_log::append::micro_batch::MicroBatchPolicy::disabled(1)
                .expect("valid test policy"),
            Arc::new(StoreRuntimeConfig::default()),
            topics,
            None,
            false,
            crate::runtime::test_service_context("timer-completion-feed"),
        )
        .expect("create Extended Timeline Store")
        .expect("Extended Timeline configuration is valid");
        store.wire_owned_root_dependencies().expect("wire Extended Timeline");
        store.init().await.expect("initialize Store");
        assert!(store.load().await);
        let completion = store
            .extended_timeline_completion_for_test()
            .expect("formal Extended Timeline completion");

        for index in 0..600 {
            assert!(store.put_message(plain_message(index)).await.is_ok());
        }
        assert!(store
            .timer_message_write_handle_for_test()
            .put_message(final_message(11))
            .await
            .is_ok());
        for index in 0..600 {
            assert!(store.put_message(plain_message(index)).await.is_ok());
        }
        let max_offset = store.get_max_phy_offset();
        for _ in 0..64 {
            store.reput_once().await;
            if store.dispatch_behind_bytes() == 0 {
                break;
            }
        }
        assert_eq!(store.dispatch_behind_bytes(), 0);

        // Eight frames would cover a fraction of the CommitLog. The feed carries one run past
        // every ordinary frame and spends the budget only on the final delivery.
        let result = completion.run_once(8, 64 * 1024).expect("reconcile completions");
        assert_eq!(result.records, 1);
        assert_eq!(result.completion_physical_cursor, max_offset);
        assert_eq!(completion.completion_physical_cursor().unwrap(), max_offset);
        assert!(completion.is_caught_up().unwrap());
    }
}
