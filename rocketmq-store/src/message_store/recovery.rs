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

use std::future::Future;
use std::time::Instant;

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecoveryIndexRepairPolicy {
    #[default]
    Synchronous,
    Background,
    Disabled,
}

impl RecoveryIndexRepairPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Synchronous => "synchronous",
            Self::Background => "background",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecoveryFallbackPolicy {
    #[default]
    Fail,
    Strict,
    DegradedStart,
    BackgroundContinue,
}

impl RecoveryFallbackPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fail => "fail",
            Self::Strict => "strict",
            Self::DegradedStart => "degraded_start",
            Self::BackgroundContinue => "background_continue",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryCrcPolicy {
    pub check_message_crc: bool,
    pub check_property_crc: bool,
}

impl RecoveryCrcPolicy {
    pub const fn new(check_message_crc: bool, check_property_crc: bool) -> Self {
        Self {
            check_message_crc,
            check_property_crc,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryScanRange {
    pub start_file_index: Option<usize>,
    pub start_offset: Option<i64>,
    pub end_offset: Option<i64>,
    pub file_count_limit: Option<usize>,
}

impl RecoveryScanRange {
    pub const fn with_file_count_limit(file_count_limit: usize) -> Self {
        Self {
            start_file_index: None,
            start_offset: None,
            end_offset: None,
            file_count_limit: if file_count_limit == 0 {
                None
            } else {
                Some(file_count_limit)
            },
        }
    }

    pub fn set_offsets(&mut self, start_offset: i64, end_offset: i64) {
        self.start_offset = Some(start_offset);
        self.end_offset = Some(end_offset);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryOffsets {
    pub dispatch_recovery_offset: Option<i64>,
    pub commit_log_min_offset: Option<i64>,
    pub commit_log_max_offset: Option<i64>,
    pub confirm_offset: Option<i64>,
    pub max_consume_queue_physical_offset: Option<i64>,
    pub index_safe_offset: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConsumeQueueRecoveryConcurrency {
    pub local_file_enabled: bool,
    pub local_file_parallelism: usize,
}

impl ConsumeQueueRecoveryConcurrency {
    pub const fn new(local_file_enabled: bool, local_file_parallelism: usize) -> Self {
        Self {
            local_file_enabled,
            local_file_parallelism,
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
    pub scan_range: RecoveryScanRange,
    pub crc_policy: RecoveryCrcPolicy,
    pub index_repair_policy: RecoveryIndexRepairPolicy,
    pub fallback_policy: RecoveryFallbackPolicy,
    pub offsets: RecoveryOffsets,
    pub consume_queue_recovery_concurrency: ConsumeQueueRecoveryConcurrency,
}

impl RecoveryPlan {
    pub fn new(
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
            scan_range: RecoveryScanRange::with_file_count_limit(max_recovery_commit_log_files),
            crc_policy: RecoveryCrcPolicy::default(),
            index_repair_policy: RecoveryIndexRepairPolicy::default(),
            fallback_policy: RecoveryFallbackPolicy::default(),
            offsets: RecoveryOffsets::default(),
            consume_queue_recovery_concurrency: ConsumeQueueRecoveryConcurrency::default(),
        }
    }

    pub fn set_dispatch_recovery_offset(&mut self, dispatch_recovery_offset: i64) {
        self.dispatch_recovery_offset = Some(dispatch_recovery_offset);
        self.offsets.dispatch_recovery_offset = Some(dispatch_recovery_offset);
        self.scan_range.start_offset = Some(dispatch_recovery_offset);
    }

    pub fn set_commit_log_offsets(&mut self, min_offset: i64, max_offset: i64, confirm_offset: i64) {
        self.offsets.commit_log_min_offset = Some(min_offset);
        self.offsets.commit_log_max_offset = Some(max_offset);
        self.offsets.confirm_offset = Some(confirm_offset);
        self.scan_range.end_offset = Some(max_offset);
    }

    pub fn set_max_consume_queue_physical_offset(&mut self, max_offset: i64) {
        self.offsets.max_consume_queue_physical_offset = Some(max_offset);
    }

    pub fn set_index_safe_offset(&mut self, index_safe_offset: i64) {
        self.offsets.index_safe_offset = Some(index_safe_offset);
    }

    pub fn set_consume_queue_recovery_concurrency(&mut self, concurrency: ConsumeQueueRecoveryConcurrency) {
        self.consume_queue_recovery_concurrency = concurrency;
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecoveryPhaseStatus {
    #[default]
    Success,
    Fallback,
    Failed,
}

impl RecoveryPhaseStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Fallback => "fallback",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReportStats {
    pub scanned_bytes: u64,
    pub recovered_messages: u64,
    pub invalid_messages: u64,
    pub truncated_files: u64,
    pub index_files_removed: u64,
    pub index_files_rebuilt: u64,
}

impl RecoveryReportStats {
    pub fn accumulate(&mut self, other: Self) {
        self.scanned_bytes = self.scanned_bytes.saturating_add(other.scanned_bytes);
        self.recovered_messages = self.recovered_messages.saturating_add(other.recovered_messages);
        self.invalid_messages = self.invalid_messages.saturating_add(other.invalid_messages);
        self.truncated_files = self.truncated_files.saturating_add(other.truncated_files);
        self.index_files_removed = self.index_files_removed.saturating_add(other.index_files_removed);
        self.index_files_rebuilt = self.index_files_rebuilt.saturating_add(other.index_files_rebuilt);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPhaseReport {
    pub phase: RecoveryPhase,
    pub duration_ms: u128,
    pub status: RecoveryPhaseStatus,
    pub stats: RecoveryReportStats,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryReport {
    pub plan: RecoveryPlan,
    pub phases: Vec<RecoveryPhaseReport>,
    pub stats: RecoveryReportStats,
    pub total_duration_ms: u128,
    pub fallback_reason: Option<String>,
}

impl RecoveryReport {
    pub fn new(plan: RecoveryPlan) -> Self {
        Self {
            plan,
            phases: Vec::with_capacity(3),
            stats: RecoveryReportStats::default(),
            total_duration_ms: 0,
            fallback_reason: None,
        }
    }

    pub fn record_phase(&mut self, phase: RecoveryPhase, duration_ms: u128) {
        self.record_phase_with_stats(phase, duration_ms, RecoveryReportStats::default());
    }

    pub fn record_phase_with_stats(&mut self, phase: RecoveryPhase, duration_ms: u128, stats: RecoveryReportStats) {
        self.record_phase_with_status(phase, duration_ms, RecoveryPhaseStatus::Success, stats);
    }

    pub fn record_phase_with_status(
        &mut self,
        phase: RecoveryPhase,
        duration_ms: u128,
        status: RecoveryPhaseStatus,
        stats: RecoveryReportStats,
    ) {
        self.total_duration_ms = self.total_duration_ms.saturating_add(duration_ms);
        self.stats.accumulate(stats);
        self.phases.push(RecoveryPhaseReport {
            phase,
            duration_ms,
            status,
            stats,
        });
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

#[derive(Debug)]
pub struct RecoveryExecutor {
    report: RecoveryReport,
}

impl RecoveryExecutor {
    pub fn new(plan: RecoveryPlan) -> Self {
        Self {
            report: RecoveryReport::new(plan),
        }
    }

    pub fn plan(&self) -> &RecoveryPlan {
        &self.report.plan
    }

    pub fn plan_mut(&mut self) -> &mut RecoveryPlan {
        &mut self.report.plan
    }

    pub fn report(&self) -> &RecoveryReport {
        &self.report
    }

    pub async fn run_phase<F>(&mut self, phase: RecoveryPhase, future: F) -> u128
    where
        F: Future<Output = ()>,
    {
        self.run_phase_with_status(
            phase,
            RecoveryPhaseStatus::Success,
            RecoveryReportStats::default(),
            future,
        )
        .await
    }

    pub async fn run_phase_with_status<F>(
        &mut self,
        phase: RecoveryPhase,
        status: RecoveryPhaseStatus,
        stats: RecoveryReportStats,
        future: F,
    ) -> u128
    where
        F: Future<Output = ()>,
    {
        let start = Instant::now();
        future.await;
        let duration_ms = Instant::now().saturating_duration_since(start).as_millis();
        self.report.record_phase_with_status(phase, duration_ms, status, stats);
        duration_ms
    }

    pub fn finish(self) -> RecoveryReport {
        self.report
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
        assert_eq!(report.phases[0].status, RecoveryPhaseStatus::Success);
        assert_eq!(report.stats, RecoveryReportStats::default());
    }

    #[test]
    fn recovery_plan_defaults_preserve_strict_compatible_policy() {
        let plan = RecoveryPlan::new(RecoveryMode::Strict, RecoveryExit::Normal, false, 7);

        assert_eq!(plan.mode, RecoveryMode::Strict);
        assert_eq!(plan.exit, RecoveryExit::Normal);
        assert!(!plan.recover_concurrently);
        assert_eq!(plan.max_recovery_commit_log_files, 7);
        assert_eq!(plan.scan_range.file_count_limit, Some(7));
        assert_eq!(plan.scan_range.start_offset, None);
        assert_eq!(plan.scan_range.end_offset, None);
        assert_eq!(plan.crc_policy, RecoveryCrcPolicy::default());
        assert_eq!(plan.index_repair_policy, RecoveryIndexRepairPolicy::Synchronous);
        assert_eq!(plan.fallback_policy, RecoveryFallbackPolicy::Fail);
        assert_eq!(
            plan.consume_queue_recovery_concurrency,
            ConsumeQueueRecoveryConcurrency::default()
        );
    }

    #[test]
    fn recovery_plan_tracks_offsets_and_policies() {
        let mut plan = RecoveryPlan::new(RecoveryMode::Balanced, RecoveryExit::Abnormal, true, 0);
        plan.crc_policy = RecoveryCrcPolicy::new(true, true);
        plan.index_repair_policy = RecoveryIndexRepairPolicy::Background;
        plan.fallback_policy = RecoveryFallbackPolicy::Strict;

        plan.set_dispatch_recovery_offset(1024);
        plan.set_commit_log_offsets(512, 4096, 3072);
        plan.set_max_consume_queue_physical_offset(2048);
        plan.set_index_safe_offset(1536);

        assert_eq!(plan.dispatch_recovery_offset, Some(1024));
        assert_eq!(plan.offsets.dispatch_recovery_offset, Some(1024));
        assert_eq!(plan.offsets.commit_log_min_offset, Some(512));
        assert_eq!(plan.offsets.commit_log_max_offset, Some(4096));
        assert_eq!(plan.offsets.confirm_offset, Some(3072));
        assert_eq!(plan.offsets.max_consume_queue_physical_offset, Some(2048));
        assert_eq!(plan.offsets.index_safe_offset, Some(1536));
        assert_eq!(plan.scan_range.start_offset, Some(1024));
        assert_eq!(plan.scan_range.end_offset, Some(4096));
        assert_eq!(plan.scan_range.file_count_limit, None);
        assert_eq!(plan.crc_policy, RecoveryCrcPolicy::new(true, true));
        assert_eq!(plan.index_repair_policy.as_str(), "background");
        assert_eq!(plan.fallback_policy.as_str(), "strict");
    }

    #[test]
    fn recovery_plan_tracks_consume_queue_recovery_concurrency() {
        let mut plan = RecoveryPlan::new(RecoveryMode::Balanced, RecoveryExit::Abnormal, false, 0);

        plan.set_consume_queue_recovery_concurrency(ConsumeQueueRecoveryConcurrency::new(true, 4));

        assert_eq!(
            plan.consume_queue_recovery_concurrency,
            ConsumeQueueRecoveryConcurrency::new(true, 4)
        );
    }

    #[test]
    fn recovery_report_accumulates_phase_stats() {
        let plan = RecoveryPlan::new(RecoveryMode::Fast, RecoveryExit::Abnormal, true, 3);
        let mut report = RecoveryReport::new(plan);

        report.record_phase_with_stats(
            RecoveryPhase::CommitLog,
            12,
            RecoveryReportStats {
                scanned_bytes: 4096,
                recovered_messages: 8,
                invalid_messages: 1,
                truncated_files: 2,
                index_files_removed: 0,
                index_files_rebuilt: 0,
            },
        );
        report.record_phase_with_status(
            RecoveryPhase::TopicQueueTable,
            3,
            RecoveryPhaseStatus::Fallback,
            RecoveryReportStats {
                scanned_bytes: 1024,
                recovered_messages: 2,
                invalid_messages: 0,
                truncated_files: 0,
                index_files_removed: 1,
                index_files_rebuilt: 4,
            },
        );
        report.set_fallback_reason("index repair deferred");

        assert_eq!(report.total_duration_ms, 15);
        assert_eq!(report.stats.scanned_bytes, 5120);
        assert_eq!(report.stats.recovered_messages, 10);
        assert_eq!(report.stats.invalid_messages, 1);
        assert_eq!(report.stats.truncated_files, 2);
        assert_eq!(report.stats.index_files_removed, 1);
        assert_eq!(report.stats.index_files_rebuilt, 4);
        assert_eq!(report.phases[1].status, RecoveryPhaseStatus::Fallback);
        assert_eq!(report.phases[1].status.as_str(), "fallback");
        assert_eq!(report.fallback_reason.as_deref(), Some("index repair deferred"));
    }

    #[tokio::test]
    async fn recovery_executor_wraps_phase_execution() {
        let plan = RecoveryPlan::new(RecoveryMode::Strict, RecoveryExit::Normal, false, 3);
        let mut executor = RecoveryExecutor::new(plan);
        let mut ran = false;

        let duration_ms = executor
            .run_phase(RecoveryPhase::ConsumeQueue, async {
                ran = true;
            })
            .await;
        executor.plan_mut().set_dispatch_recovery_offset(256);

        assert!(ran);
        assert_eq!(duration_ms, executor.report().phases[0].duration_ms);
        assert_eq!(
            executor.report().phase_duration_ms(RecoveryPhase::ConsumeQueue),
            Some(duration_ms)
        );
        assert_eq!(executor.plan().dispatch_recovery_offset, Some(256));

        let report = executor.finish();
        assert_eq!(report.phases.len(), 1);
        assert_eq!(report.phases[0].phase, RecoveryPhase::ConsumeQueue);
        assert_eq!(report.phases[0].status, RecoveryPhaseStatus::Success);
    }

    #[tokio::test]
    async fn recovery_executor_records_phase_status_and_stats() {
        let plan = RecoveryPlan::new(RecoveryMode::Balanced, RecoveryExit::Abnormal, true, 2);
        let mut executor = RecoveryExecutor::new(plan);

        executor
            .run_phase_with_status(
                RecoveryPhase::CommitLog,
                RecoveryPhaseStatus::Fallback,
                RecoveryReportStats {
                    scanned_bytes: 64,
                    recovered_messages: 1,
                    invalid_messages: 0,
                    truncated_files: 0,
                    index_files_removed: 0,
                    index_files_rebuilt: 0,
                },
                async {},
            )
            .await;

        let report = executor.finish();
        assert_eq!(report.phases[0].status, RecoveryPhaseStatus::Fallback);
        assert_eq!(report.stats.scanned_bytes, 64);
        assert_eq!(report.stats.recovered_messages, 1);
    }
}
