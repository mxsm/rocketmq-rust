// Copyright 2023 The RocketMQ Rust Authors
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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_runtime::ShutdownReport;

pub(crate) mod pop_lite_long_polling_service;
pub(crate) mod pop_long_polling_service;
pub(crate) mod pull_request_hold_service;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LegacyServiceResourceSnapshot {
    pub(crate) table_entries: usize,
    pub(crate) tracked_waiters: u64,
    pub(crate) request_budget_count: usize,
    pub(crate) request_budget_bytes: usize,
    pub(crate) waking_clients: usize,
    pub(crate) active_executions: u64,
    pub(crate) task_count: usize,
    pub(crate) wake_task_count: usize,
    pub(crate) shutdown_wake_failures: u64,
}

impl LegacyServiceResourceSnapshot {
    pub(crate) fn is_zero(self) -> bool {
        self == Self::default()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LegacyServiceFinalization {
    pub(crate) observed_after_session_drain: LegacyServiceResourceSnapshot,
    pub(crate) terminal: LegacyServiceResourceSnapshot,
}

#[derive(Clone, Debug)]
pub(crate) struct LegacyServiceShutdownReport {
    pub(crate) name: &'static str,
    pub(crate) producer: Option<ShutdownReport>,
    pub(crate) executions: Option<ShutdownReport>,
    pub(crate) observed_after_session_drain: LegacyServiceResourceSnapshot,
    pub(crate) resources: LegacyServiceResourceSnapshot,
}

impl LegacyServiceShutdownReport {
    pub(crate) fn is_healthy(&self) -> bool {
        self.resources.is_zero()
            && self.observed_after_session_drain.is_zero()
            && self.producer.as_ref().is_some_and(ShutdownReport::is_healthy)
            && self.executions.as_ref().is_some_and(ShutdownReport::is_healthy)
    }

    pub(crate) fn has_timed_out(&self) -> bool {
        self.producer.as_ref().is_some_and(shutdown_report_has_timed_out)
            || self.executions.as_ref().is_some_and(shutdown_report_has_timed_out)
    }
}

fn shutdown_report_has_timed_out(report: &ShutdownReport) -> bool {
    report.timed_out > 0 || report.children.iter().any(shutdown_report_has_timed_out)
}

#[derive(Default)]
pub(crate) struct LegacyExecutionTracker {
    active: AtomicU64,
}

impl LegacyExecutionTracker {
    pub(crate) fn enter(self: &Arc<Self>) -> LegacyExecutionGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        LegacyExecutionGuard {
            tracker: Arc::clone(self),
        }
    }

    pub(crate) fn active(&self) -> u64 {
        self.active.load(Ordering::Acquire)
    }
}

pub(crate) struct LegacyExecutionGuard {
    tracker: Arc<LegacyExecutionTracker>,
}

impl Drop for LegacyExecutionGuard {
    fn drop(&mut self) {
        self.tracker.active.fetch_sub(1, Ordering::AcqRel);
    }
}
