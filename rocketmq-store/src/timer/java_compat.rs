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

mod due;
mod enqueue;
mod recovery;

use std::sync::Arc;

use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerId;

use crate::timer::engine::TimerEngine;
use crate::timer::engine::WorkBudget;
use crate::timer::error::TimerEngineError;
use crate::timer::request::EngineBatchProgress;
use crate::timer::timer_message_store::TimerMessageStore;

#[derive(Clone)]
pub(crate) struct JavaCompatEngine {
    store: Arc<TimerMessageStore>,
}

impl JavaCompatEngine {
    pub(crate) fn new(store: Arc<TimerMessageStore>) -> Self {
        Self { store }
    }
}

impl TimerEngine for JavaCompatEngine {
    fn engine_id(&self) -> TimerEngineId {
        TimerEngineId::JavaCompat
    }

    async fn load(&self) -> Result<(), TimerEngineError> {
        Ok(())
    }

    async fn enqueue_source(&self, budget: WorkBudget) -> Result<EngineBatchProgress, TimerEngineError> {
        enqueue::process(&self.store, budget).await
    }

    async fn roll_due(
        &self,
        epoch: TimerEngineEpoch,
        budget: WorkBudget,
    ) -> Result<EngineBatchProgress, TimerEngineError> {
        due::process(&self.store, epoch, budget).await
    }

    async fn complete(&self, _timer_id: TimerId, epoch: TimerEngineEpoch) -> Result<(), TimerEngineError> {
        if !self.store.is_current_delivery_epoch(epoch.get()) {
            return Ok(());
        }
        self.checkpoint().await.map(|_| ())
    }

    async fn cancel(&self, _timer_id: TimerId) -> Result<(), TimerEngineError> {
        Ok(())
    }

    async fn checkpoint(&self) -> Result<bool, TimerEngineError> {
        Ok(self.store.commit_pipeline_progress()?)
    }

    async fn on_role_change(&self, active: bool, _epoch: TimerEngineEpoch) -> Result<(), TimerEngineError> {
        self.store.set_should_running_dequeue(active);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), TimerEngineError> {
        self.checkpoint().await.map(|_| ())
    }
}
