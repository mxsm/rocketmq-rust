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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerId;

use super::ShadowTimelineMaterializer;
use super::TimelineDueScanner;
use crate::runtime::StoreRuntimeScope;
use crate::timer::engine::TimerEngine;
use crate::timer::engine::WorkBudget;
use crate::timer::error::TimerEngineError;
use crate::timer::request::EngineBatchProgress;
use crate::timer::role::TimerRoleState;

/// Rust-native Extended Timeline implementation of the common timer engine contract.
///
/// All filesystem and RocksDB work is dispatched through the Store-owned blocking executor.
/// Shadow instances only materialize and observe records; formal instances additionally require
/// a current durable role epoch before they may publish ready work.
#[derive(Clone)]
pub(crate) struct ExtendedTimelineEngine {
    runtime_scope: StoreRuntimeScope,
    materializer: Arc<ShadowTimelineMaterializer>,
    due_scanner: Arc<TimelineDueScanner>,
    role: Arc<TimerRoleState>,
    formal: bool,
    loaded: Arc<AtomicBool>,
}

impl ExtendedTimelineEngine {
    pub(crate) fn new(
        runtime_scope: StoreRuntimeScope,
        materializer: Arc<ShadowTimelineMaterializer>,
        due_scanner: Arc<TimelineDueScanner>,
        role: Arc<TimerRoleState>,
        formal: bool,
    ) -> Self {
        Self {
            runtime_scope,
            materializer,
            due_scanner,
            role,
            formal,
            // Construction follows durable role recovery and successful Timeline open.
            loaded: Arc::new(AtomicBool::new(true)),
        }
    }

    fn is_loaded(&self) -> bool {
        self.loaded.load(Ordering::Acquire)
    }

    fn is_current_epoch(&self, epoch: TimerEngineEpoch) -> bool {
        if !self.formal {
            return false;
        }
        self.role.is_current_delivery_epoch(epoch.get())
    }

    async fn run_io<T, F>(&self, name: &'static str, operation: F) -> Result<T, TimerEngineError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, TimerEngineError> + Send + 'static,
    {
        self.runtime_scope
            .spawn_io(name, operation)
            .await
            .map_err(TimerEngineError::Runtime)?
    }
}

impl TimerEngine for ExtendedTimelineEngine {
    fn engine_id(&self) -> TimerEngineId {
        TimerEngineId::ExtendedTimeline
    }

    async fn load(&self) -> Result<(), TimerEngineError> {
        let materializer = Arc::clone(&self.materializer);
        self.run_io("timer-extended-engine-load", move || {
            materializer
                .refresh_cleanup_fence()
                .map_err(|error| TimerEngineError::Materializer(Box::new(error)))
        })
        .await?;
        self.loaded.store(true, Ordering::Release);
        Ok(())
    }

    async fn enqueue_source(&self, budget: WorkBudget) -> Result<EngineBatchProgress, TimerEngineError> {
        if !self.is_loaded() || budget.is_exhausted(0, 0) {
            return Ok(EngineBatchProgress {
                durable: false,
                ..EngineBatchProgress::empty()
            });
        }
        let materializer = Arc::clone(&self.materializer);
        let messages = self
            .run_io("timer-extended-engine-source", move || {
                materializer
                    .run_once_with_budget(budget.max_messages, budget.max_bytes)
                    .map_err(|error| TimerEngineError::Materializer(Box::new(error)))
            })
            .await?;
        Ok(EngineBatchProgress {
            messages,
            bytes: 0,
            continuation: None,
            durable: true,
        })
    }

    async fn roll_due(
        &self,
        epoch: TimerEngineEpoch,
        budget: WorkBudget,
    ) -> Result<EngineBatchProgress, TimerEngineError> {
        if !self.is_loaded() || budget.is_exhausted(0, 0) {
            return Ok(EngineBatchProgress {
                durable: false,
                ..EngineBatchProgress::empty()
            });
        }
        if self.formal && !self.is_current_epoch(epoch) {
            return Ok(EngineBatchProgress {
                durable: false,
                ..EngineBatchProgress::empty()
            });
        }
        let scanner = Arc::clone(&self.due_scanner);
        let formal = self.formal;
        let result = self
            .run_io("timer-extended-engine-due", move || {
                let now_ms = rocketmq_runtime::common::time_utils::current_millis() as i64;
                let result = if formal {
                    scanner.scan_formal_until_with_budget(now_ms, budget.max_messages, budget.max_bytes)
                } else {
                    scanner.scan_shadow_until_with_budget(now_ms, budget.max_messages, budget.max_bytes)
                };
                result.map_err(|error| TimerEngineError::DueScanner(Box::new(error)))
            })
            .await?;
        Ok(EngineBatchProgress {
            messages: result.observed,
            bytes: result.bytes,
            continuation: None,
            durable: true,
        })
    }

    async fn complete(&self, _timer_id: TimerId, epoch: TimerEngineEpoch) -> Result<(), TimerEngineError> {
        if !self.is_loaded() || !self.is_current_epoch(epoch) {
            return Ok(());
        }
        self.checkpoint().await.map(|_| ())
    }

    async fn cancel(&self, _timer_id: TimerId) -> Result<(), TimerEngineError> {
        Ok(())
    }

    async fn checkpoint(&self) -> Result<bool, TimerEngineError> {
        if !self.is_loaded() {
            return Ok(false);
        }
        let materializer = Arc::clone(&self.materializer);
        self.run_io("timer-extended-engine-checkpoint", move || {
            materializer
                .refresh_cleanup_fence()
                .map(|_| true)
                .map_err(|error| TimerEngineError::Materializer(Box::new(error)))
        })
        .await
    }

    async fn on_role_change(&self, active: bool, epoch: TimerEngineEpoch) -> Result<(), TimerEngineError> {
        if !self.is_loaded() {
            return Ok(());
        }
        if !self.formal {
            return Ok(());
        }
        let role = Arc::clone(&self.role);
        self.run_io("timer-extended-engine-role", move || {
            role.transition_with_term(active, epoch.get())
                .map(|_| ())
                .map_err(TimerEngineError::Storage)
        })
        .await
    }

    async fn shutdown(&self) -> Result<(), TimerEngineError> {
        self.loaded.store(false, Ordering::Release);
        self.role.fence_in_memory();
        self.materializer.close();
        Ok(())
    }
}
