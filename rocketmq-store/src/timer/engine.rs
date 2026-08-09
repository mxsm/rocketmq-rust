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
use std::time::Instant;

use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerStoreMode;

use crate::timer::error::TimerEngineError;
use crate::timer::request::EngineBatchProgress;

#[derive(Clone, Copy, Debug)]
pub(crate) struct WorkBudget {
    pub(crate) max_messages: usize,
    pub(crate) max_bytes: usize,
    pub(crate) deadline: Instant,
}

impl WorkBudget {
    pub(crate) fn try_new(max_messages: usize, max_bytes: usize, deadline: Instant) -> Result<Self, TimerEngineError> {
        if max_messages == 0 || max_bytes == 0 {
            return Err(TimerEngineError::InvalidBudget);
        }
        Ok(Self {
            max_messages,
            max_bytes,
            deadline,
        })
    }

    pub(crate) fn is_exhausted(self, messages: usize, bytes: usize) -> bool {
        messages >= self.max_messages || bytes >= self.max_bytes || Instant::now() >= self.deadline
    }

    pub(crate) fn allows(self, messages: usize, bytes: usize) -> bool {
        messages <= self.max_messages && bytes <= self.max_bytes && Instant::now() < self.deadline
    }
}

pub(crate) trait TimerEngine: Send + Sync {
    fn engine_id(&self) -> TimerEngineId;

    fn load(&self) -> impl Future<Output = Result<(), TimerEngineError>> + Send;

    fn enqueue_source(
        &self,
        budget: WorkBudget,
    ) -> impl Future<Output = Result<EngineBatchProgress, TimerEngineError>> + Send;

    fn roll_due(
        &self,
        epoch: TimerEngineEpoch,
        budget: WorkBudget,
    ) -> impl Future<Output = Result<EngineBatchProgress, TimerEngineError>> + Send;

    fn complete(
        &self,
        timer_id: TimerId,
        epoch: TimerEngineEpoch,
    ) -> impl Future<Output = Result<(), TimerEngineError>> + Send;

    fn cancel(&self, timer_id: TimerId) -> impl Future<Output = Result<(), TimerEngineError>> + Send;

    fn checkpoint(&self) -> impl Future<Output = Result<bool, TimerEngineError>> + Send;

    fn on_role_change(
        &self,
        active: bool,
        epoch: TimerEngineEpoch,
    ) -> impl Future<Output = Result<(), TimerEngineError>> + Send;

    fn shutdown(&self) -> impl Future<Output = Result<(), TimerEngineError>> + Send;
}

pub(crate) fn select_engine_owner(
    mode: TimerStoreMode,
    persisted_owner: Option<TimerEngineId>,
    requested_delay_ms: u64,
    java_compat_horizon_ms: u64,
    extended_available: bool,
) -> Result<TimerEngineId, TimerEngineError> {
    if let Some(owner) = persisted_owner {
        return Ok(owner);
    }
    if requested_delay_ms <= java_compat_horizon_ms && mode == TimerStoreMode::JavaCompat {
        return Ok(TimerEngineId::JavaCompat);
    }
    if mode == TimerStoreMode::ExtendedTimeline && extended_available {
        return Ok(TimerEngineId::ExtendedTimeline);
    }
    Err(TimerEngineError::UnsupportedMode(match mode {
        TimerStoreMode::JavaCompat => "requested delay exceeds the Java-compatible horizon",
        TimerStoreMode::ExtendedTimeline => "extended timeline capability is unavailable",
    }))
}
