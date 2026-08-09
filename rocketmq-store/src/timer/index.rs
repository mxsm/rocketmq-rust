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

use std::future::Future;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TimerIndexCursor {
    pub(crate) due_time_ms: i64,
    pub(crate) lane: u16,
    pub(crate) timer_id: TimerId,
    pub(crate) generation: u64,
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

pub(crate) trait TimerIndex: Send + Sync {
    fn put_batch(
        &self,
        records: Vec<TimerSourceRecord>,
        budget: WorkBudget,
    ) -> impl Future<Output = Result<usize, TimerEngineError>> + Send;

    fn scan_due(
        &self,
        from: Option<TimerIndexCursor>,
        due_exclusive_ms: i64,
        budget: WorkBudget,
    ) -> impl Future<Output = Result<TimerIndexPage, TimerEngineError>> + Send;

    fn set_state(
        &self,
        timer_id: TimerId,
        state: TimerRecordState,
    ) -> impl Future<Output = Result<(), TimerEngineError>> + Send;

    fn checkpoint(&self, checkpoint: TimerIndexCheckpoint)
        -> impl Future<Output = Result<(), TimerEngineError>> + Send;

    fn pin_snapshot(
        &self,
        gc_fence: TimerTimelineCursor,
    ) -> impl Future<Output = Result<TimerSnapshotPin, TimerEngineError>> + Send;

    fn release_snapshot(&self, pin: TimerSnapshotPin) -> impl Future<Output = Result<(), TimerEngineError>> + Send;

    fn gc(
        &self,
        fence: TimerTimelineCursor,
        budget: WorkBudget,
    ) -> impl Future<Output = Result<usize, TimerEngineError>> + Send;
}
