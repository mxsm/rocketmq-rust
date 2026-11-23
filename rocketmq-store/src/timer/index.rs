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

use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerTimelineCursor;

use crate::timer::engine::WorkBudget;
use crate::timer::error::TimerEngineError;
use crate::timer::request::DueTimerRecord;
use crate::timer::request::TimerSourceRecord;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimerRecordState {
    Pending,
    Delivering,
    Delivered,
    Cancelled,
    Quarantined,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TimerIndexPage {
    pub(crate) records: Vec<DueTimerRecord>,
    pub(crate) continuation: Option<TimerIndexCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TimerIndexCursor {
    pub(crate) due_time_ms: i64,
    pub(crate) lane: u16,
    pub(crate) timer_id: TimerId,
    pub(crate) generation: u64,
    /// In-memory state for resuming `scan_due()` pagination after this key.
    pub(crate) resume_state: TimerIndexResumeState,
}

impl TimerIndexCursor {
    pub(crate) const fn ordered_key(due_time_ms: i64, lane: u16, timer_id: TimerId, generation: u64) -> Self {
        Self {
            due_time_ms,
            lane,
            timer_id,
            generation,
            resume_state: TimerIndexResumeState::OrderedKey,
        }
    }
}

/// How a paginated `scan_due()` resumes from a [`TimerIndexCursor`].
///
/// This is in-memory pagination state only and is never persisted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TimerIndexResumeState {
    /// Resume from the outer cursor's ordered key alone.
    OrderedKey,
    /// Resume a segmented timeline scan. The outer cursor key is supplemented
    /// with the manifest generation, the UTC day/hour partition, and the
    /// per-run consumed positions needed to rebuild the native continuation.
    Segmented {
        manifest_generation: u64,
        due_day_utc: i32,
        due_hour_utc: u8,
        run_positions: Vec<(u64, u64)>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerIndexCheckpoint {
    pub(crate) cursor: TimerTimelineCursor,
    pub(crate) epoch: TimerEngineEpoch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerSnapshotPin {
    pub(crate) generation: u64,
    pub(crate) gc_fence: TimerTimelineCursor,
}

#[trait_variant::make(Send)]
pub(crate) trait TimerIndex: Sync {
    async fn put_batch(&self, records: Vec<TimerSourceRecord>, budget: WorkBudget) -> Result<usize, TimerEngineError>;

    async fn scan_due(
        &self,
        from: Option<TimerIndexCursor>,
        due_exclusive_ms: i64,
        budget: WorkBudget,
    ) -> Result<TimerIndexPage, TimerEngineError>;

    async fn set_state(
        &self,
        timer_id: TimerId,
        generation: u64,
        state: TimerRecordState,
    ) -> Result<(), TimerEngineError>;

    async fn checkpoint(&self, checkpoint: TimerIndexCheckpoint) -> Result<(), TimerEngineError>;

    async fn load_checkpoint(&self) -> Result<Option<TimerIndexCheckpoint>, TimerEngineError>;

    async fn pin_snapshot(&self, gc_fence: TimerTimelineCursor) -> Result<TimerSnapshotPin, TimerEngineError>;

    async fn release_snapshot(&self, pin: TimerSnapshotPin) -> Result<(), TimerEngineError>;

    async fn gc(&self, fence: TimerTimelineCursor, budget: WorkBudget) -> Result<usize, TimerEngineError>;
}
