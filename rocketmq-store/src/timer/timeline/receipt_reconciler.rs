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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
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

/// Loss-tolerant dispatcher notification. Correctness comes from active CommitLog replay.
#[derive(Debug, Default)]
pub(crate) struct TimelineCompletionWake {
    observed_end_offset: AtomicI64,
}

impl TimelineCompletionWake {
    pub(crate) fn observed_end_offset(&self) -> i64 {
        self.observed_end_offset.load(Ordering::Acquire)
    }
}

impl CommitLogDispatcher for TimelineCompletionWake {
    fn supports_parallel_dispatch(&self) -> bool {
        true
    }

    fn dispatch(&self, request: &mut DispatchRequest) {
        if !request.success || request.commit_log_offset < 0 || request.msg_size <= 0 {
            return;
        }
        let end = request.commit_log_offset.saturating_add(i64::from(request.msg_size));
        self.observed_end_offset.fetch_max(end, Ordering::Release);
    }
}

/// Sequentially derives durable completion receipts from the replicated final CommitLog.
pub(crate) struct TimelineCompletionReconciler {
    timeline: Arc<RocksDbTimelineIndex>,
    commit_log: CommitLogReadHandle,
    wake: Arc<TimelineCompletionWake>,
    run_lock: Mutex<()>,
}

impl TimelineCompletionReconciler {
    pub(crate) fn new(
        timeline: Arc<RocksDbTimelineIndex>,
        commit_log: CommitLogReadHandle,
        wake: Arc<TimelineCompletionWake>,
    ) -> Self {
        Self {
            timeline,
            commit_log,
            wake,
            run_lock: Mutex::new(()),
        }
    }

    pub(crate) fn completion_physical_cursor(&self) -> Result<i64, TimelineCompletionError> {
        let value = self.timeline.store().get_cf(CHECKPOINT_CF, COMPLETION_CURSOR_KEY)?;
        let Some(value) = value else {
            return Ok(self.commit_log.get_min_offset().max(0));
        };
        let checkpoint = DerivedCheckpoint::decode(&value, DerivedEngine::TimerCompletion)?;
        i64::try_from(checkpoint.cursor().next_offset()).map_err(|_| TimelineCompletionError::CursorOverflow)
    }

    pub(crate) fn is_caught_up(&self) -> Result<bool, TimelineCompletionError> {
        Ok(self.completion_physical_cursor()? >= self.safe_replay_limit())
    }

    /// Replays one bounded prefix. Dispatcher notifications only reduce idle polling latency.
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
        let durable_offset = self.safe_replay_limit();
        // Dispatcher notifications are deliberately only wake-up hints.  They may be
        // coalesced or lost during restart, so bounding the durable replay by the last
        // observed notification could strand a committed completion forever.
        let _wake_hint = self.wake.observed_end_offset();
        let scan_limit = durable_offset;
        let mut result = CompletionReconcileResult::default();
        while cursor < scan_limit && result.records < max_records && result.bytes < max_bytes {
            let Some(selection) = self.commit_log.get_data(cursor) else {
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
                return Err(TimelineCompletionError::InvalidFrame(cursor));
            }
            let frame_size = usize::try_from(declared).map_err(|_| TimelineCompletionError::InvalidFrame(cursor))?;
            let next_cursor = cursor
                .checked_add(i64::from(declared))
                .ok_or(TimelineCompletionError::CursorOverflow)?;
            if magic == BLANK_MAGIC_CODE {
                self.persist_cursor(next_cursor)?;
                cursor = next_cursor;
                continue;
            }
            if frame_size > max_bytes.saturating_sub(result.bytes) && result.records > 0 {
                break;
            }
            let frame = self
                .commit_log
                .get_message(cursor, declared)
                .and_then(|selection| selection.get_bytes())
                .ok_or(TimelineCompletionError::FrameUnavailable(cursor))?;
            let message = decode_message(frame)?;
            self.reconcile_message(
                cursor,
                u32::try_from(frame_size).map_err(|_| TimelineCompletionError::CursorOverflow)?,
                &message,
                next_cursor,
            )?;
            cursor = next_cursor;
            result.records = result.records.saturating_add(1);
            result.bytes = result.bytes.saturating_add(frame_size);
        }
        result.completion_physical_cursor = cursor;
        Ok(result)
    }

    fn safe_replay_limit(&self) -> i64 {
        self.commit_log
            .get_flushed_where()
            .max(self.commit_log.get_confirm_offset())
            .min(self.commit_log.get_max_offset())
            .max(0)
    }

    fn reconcile_message(
        &self,
        physical_offset: i64,
        record_size: u32,
        message: &MessageExt,
        next_cursor: i64,
    ) -> Result<(), TimelineCompletionError> {
        let Some(final_fact) = FinalTimerFact::from_message(message)? else {
            self.persist_cursor(next_cursor)?;
            return Ok(());
        };
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
        // Extended source records carry the engine route as well, but they are not final
        // delivery facts.  Completion is derived only after the message has left the
        // internal Timer topic and has been appended to its real topic.
        if message.topic() == TIMER_TOPIC {
            return Ok(None);
        }
        let engine = property(message, MessageConst::TIMER_ENGINE_TYPE);
        if engine.as_deref() != Some(TimerEngineId::ExtendedTimeline.as_str()) {
            return Ok(None);
        }
        let timer_id = property(message, MessageConst::PROPERTY_TIMER_ID)
            .and_then(|value| value.parse::<u128>().ok())
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let generation = property(message, MessageConst::PROPERTY_TIMER_GENERATION)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let owner_epoch = property(message, MessageConst::PROPERTY_TIMER_OWNER_EPOCH)
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let due_time_ms = property(message, MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS)
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value >= 0)
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let lane = property(message, MessageConst::PROPERTY_TIMER_LANE)
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or(TimelineCompletionError::MalformedFinalFact)?;
        let delivery_token = property(message, MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN)
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

fn decode_message(frame: Bytes) -> Result<MessageExt, TimelineCompletionError> {
    MessageDecoder::decode(&mut frame.clone(), true, false, false, false, false)
        .ok_or(TimelineCompletionError::MalformedFrame)
}

fn property(message: &MessageExt, key: &'static str) -> Option<String> {
    message
        .property(&CheetahString::from_static_str(key))
        .map(|value| value.to_string())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CompletionReconcileResult {
    pub(crate) records: usize,
    pub(crate) bytes: usize,
    pub(crate) completion_physical_cursor: i64,
}

#[derive(Debug, Error)]
pub(crate) enum TimelineCompletionError {
    #[error("Timeline store failure: {0}")]
    Timeline(#[from] rocketmq_error::RocketMQError),
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
