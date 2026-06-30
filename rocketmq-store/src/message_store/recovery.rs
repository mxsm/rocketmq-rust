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

use crate::config::message_store_config::RecoveryMode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryExit {
    Normal,
    Abnormal,
}

impl RecoveryExit {
    pub const fn from_last_exit_ok(last_exit_ok: bool) -> Self {
        if last_exit_ok {
            Self::Normal
        } else {
            Self::Abnormal
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Abnormal => "abnormal",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryPhase {
    ConsumeQueue,
    CommitLog,
    TopicQueueTable,
}

impl RecoveryPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConsumeQueue => "consume_queue",
            Self::CommitLog => "commit_log",
            Self::TopicQueueTable => "topic_queue_table",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPlan {
    pub mode: RecoveryMode,
    pub exit: RecoveryExit,
    pub recover_concurrently: bool,
    pub max_recovery_commit_log_files: usize,
    pub dispatch_recovery_offset: Option<i64>,
}

impl RecoveryPlan {
    pub const fn new(
        mode: RecoveryMode,
        exit: RecoveryExit,
        recover_concurrently: bool,
        max_recovery_commit_log_files: usize,
    ) -> Self {
        Self {
            mode,
            exit,
            recover_concurrently,
            max_recovery_commit_log_files,
            dispatch_recovery_offset: None,
        }
    }

    pub fn set_dispatch_recovery_offset(&mut self, dispatch_recovery_offset: i64) {
        self.dispatch_recovery_offset = Some(dispatch_recovery_offset);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPhaseReport {
    pub phase: RecoveryPhase,
    pub duration_ms: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryReport {
    pub plan: RecoveryPlan,
    pub phases: Vec<RecoveryPhaseReport>,
    pub total_duration_ms: u128,
    pub fallback_reason: Option<String>,
}

impl RecoveryReport {
    pub fn new(plan: RecoveryPlan) -> Self {
        Self {
            plan,
            phases: Vec::with_capacity(3),
            total_duration_ms: 0,
            fallback_reason: None,
        }
    }

    pub fn record_phase(&mut self, phase: RecoveryPhase, duration_ms: u128) {
        self.total_duration_ms = self.total_duration_ms.saturating_add(duration_ms);
        self.phases.push(RecoveryPhaseReport { phase, duration_ms });
    }

    pub fn set_fallback_reason(&mut self, fallback_reason: impl Into<String>) {
        self.fallback_reason = Some(fallback_reason.into());
    }

    pub fn phase_duration_ms(&self, phase: RecoveryPhase) -> Option<u128> {
        self.phases
            .iter()
            .find(|report| report.phase == phase)
            .map(|report| report.duration_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_exit_reflects_abort_marker_result() {
        assert_eq!(RecoveryExit::from_last_exit_ok(true), RecoveryExit::Normal);
        assert_eq!(RecoveryExit::from_last_exit_ok(false), RecoveryExit::Abnormal);
        assert_eq!(RecoveryExit::Normal.as_str(), "normal");
        assert_eq!(RecoveryExit::Abnormal.as_str(), "abnormal");
    }

    #[test]
    fn recovery_report_accumulates_phase_durations() {
        let mut plan = RecoveryPlan::new(RecoveryMode::Strict, RecoveryExit::Abnormal, false, 0);
        plan.set_dispatch_recovery_offset(1024);
        let mut report = RecoveryReport::new(plan);

        report.record_phase(RecoveryPhase::ConsumeQueue, 10);
        report.record_phase(RecoveryPhase::CommitLog, 20);
        report.record_phase(RecoveryPhase::TopicQueueTable, 5);

        assert_eq!(report.plan.dispatch_recovery_offset, Some(1024));
        assert_eq!(report.total_duration_ms, 35);
        assert_eq!(report.phase_duration_ms(RecoveryPhase::CommitLog), Some(20));
        assert_eq!(report.phase_duration_ms(RecoveryPhase::TopicQueueTable), Some(5));
    }
}
