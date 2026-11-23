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
    pub(crate) fn try_new(max_messages: usize, max_bytes: usize, deadline: Instant) -> Option<Self> {
        if max_messages == 0 || max_bytes == 0 {
            return None;
        }
        Some(Self {
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

#[trait_variant::make(Send)]
pub(crate) trait TimerEngine: Sync {
    fn engine_id(&self) -> TimerEngineId;

    async fn load(&self) -> Result<(), TimerEngineError>;

    async fn enqueue_source(&self, budget: WorkBudget) -> Result<EngineBatchProgress, TimerEngineError>;

    async fn roll_due(
        &self,
        epoch: TimerEngineEpoch,
        budget: WorkBudget,
    ) -> Result<EngineBatchProgress, TimerEngineError>;

    async fn complete(&self, timer_id: TimerId, epoch: TimerEngineEpoch) -> Result<(), TimerEngineError>;

    async fn cancel(&self, timer_id: TimerId) -> Result<(), TimerEngineError>;

    async fn checkpoint(&self) -> Result<bool, TimerEngineError>;

    async fn on_role_change(&self, active: bool, epoch: TimerEngineEpoch) -> Result<(), TimerEngineError>;

    async fn shutdown(&self) -> Result<(), TimerEngineError>;
}

pub(crate) fn select_engine_owner(
    mode: TimerStoreMode,
    persisted_owner: Option<TimerEngineId>,
    requested_delay_ms: u64,
    java_compat_horizon_ms: u64,
    extended_available: bool,
) -> Option<TimerEngineId> {
    if let Some(owner) = persisted_owner {
        return Some(owner);
    }
    if requested_delay_ms <= java_compat_horizon_ms && mode == TimerStoreMode::JavaCompat {
        return Some(TimerEngineId::JavaCompat);
    }
    if mode == TimerStoreMode::ExtendedTimeline && extended_available {
        return Some(TimerEngineId::ExtendedTimeline);
    }
    None
}
