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

//! Redacted schema conversion without business I/O.

use super::*;

pub(super) fn task_section_v2(
    root: &TaskGroup,
    options: RuntimeDiagnosticsViewOptionsV2,
) -> (RuntimeTaskSectionV2, bool) {
    let diagnostics = root.diagnostics(options.long_running_threshold);
    let summary_count = diagnostics.task_kinds.len();
    // Totals cover every kind, so a truncated summary list still reports the
    // long-running and maximum-elapsed values for the whole population, and the
    // truncation is stated rather than folded into the numbers.
    let long_running = diagnostics
        .task_kinds
        .iter()
        .fold(0usize, |total, summary| total.saturating_add(summary.long_running));
    let max_elapsed = diagnostics
        .task_kinds
        .iter()
        .map(|summary| summary.max_elapsed)
        .max()
        .unwrap_or(Duration::ZERO);
    let section = RuntimeTaskSectionV2 {
        scope: RuntimeDiagnosticsScope::Subtree,
        task_group_count: diagnostics.group_count,
        task_count: diagnostics.task_count,
        local_task_count: root.local_diagnostics(options.long_running_threshold).task_count,
        long_running,
        max_elapsed_millis: duration_millis(max_elapsed),
        truncated: summary_count > options.max_task_kind_summaries,
        task_kinds: diagnostics
            .task_kinds
            .into_iter()
            .take(options.max_task_kind_summaries)
            .map(|summary| RuntimeTaskKindSummaryV1 {
                kind: runtime_task_kind(summary.kind),
                active: summary.active,
                long_running: summary.long_running,
                max_elapsed_millis: duration_millis(summary.max_elapsed),
            })
            .collect(),
    };
    (section, summary_count > options.max_task_kind_summaries)
}

pub(super) fn blocking_section_v2(
    blocking_lanes: Vec<BlockingExecutorSnapshot>,
    options: RuntimeDiagnosticsViewOptionsV2,
) -> (RuntimeBlockingSectionV2, bool) {
    let lane_count = blocking_lanes.len();
    let limit = options.max_blocking_lane_summaries.min(BlockingLane::ALL.len());
    let mut queued = 0usize;
    let mut running = 0usize;
    let mut timed_out_still_running = 0usize;
    let mut blocking_still_running = 0usize;
    let lanes = blocking_lanes
        .into_iter()
        .take(limit)
        .map(|snapshot| {
            queued = queued.saturating_add(snapshot.queued);
            running = running.saturating_add(snapshot.running);
            timed_out_still_running = timed_out_still_running.saturating_add(snapshot.timed_out_still_running);
            blocking_still_running = blocking_still_running.saturating_add(snapshot.blocking_still_running);
            sanitize_blocking_lane(snapshot)
        })
        .collect();
    (
        RuntimeBlockingSectionV2 {
            scope: RuntimeDiagnosticsScope::ProcessShared,
            queued,
            running,
            timed_out_still_running,
            blocking_still_running,
            lanes,
            truncated: lane_count > limit,
        },
        lane_count > limit,
    )
}

pub(super) fn schedule_section_v2(
    schedule: &[ScheduledTaskSnapshot],
    options: RuntimeDiagnosticsViewOptionsV2,
) -> RuntimeScheduleSectionV2 {
    let scanned = schedule.len();
    let mut section = RuntimeScheduleSectionV2 {
        scope: RuntimeDiagnosticsScope::Local,
        tasks_scanned: scanned,
        tasks_emitted: 0,
        active_runs: 0,
        runs: 0,
        skips: 0,
        overlaps: 0,
        failures: 0,
        max_elapsed_millis: 0,
        truncated: scanned > options.max_schedule_tasks,
    };
    for snapshot in schedule.iter().take(options.max_schedule_tasks) {
        section.tasks_emitted = section.tasks_emitted.saturating_add(1);
        section.active_runs = section.active_runs.saturating_add(snapshot.active_runs);
        section.runs = section.runs.saturating_add(snapshot.runs);
        section.skips = section.skips.saturating_add(snapshot.skips);
        section.overlaps = section.overlaps.saturating_add(snapshot.overlaps);
        section.failures = section.failures.saturating_add(snapshot.failures);
        section.max_elapsed_millis = section.max_elapsed_millis.max(snapshot.max_elapsed_ms);
    }
    section
}

pub(super) fn metadata_section_v2(
    snapshot: &MetadataIoSnapshot,
    options: RuntimeDiagnosticsViewOptionsV2,
) -> RuntimeMetadataSectionV2 {
    let scanned = snapshot.resources.len();
    let waiters = snapshot
        .resources
        .iter()
        .take(options.max_metadata_resources)
        .fold(0usize, |total, resource| total.saturating_add(resource.waiter_count));
    RuntimeMetadataSectionV2 {
        scope: RuntimeDiagnosticsScope::Local,
        accepting: snapshot.accepting,
        retained_operations: snapshot.pending_operations,
        retained_bytes: snapshot.pending_bytes,
        waiters,
        resources_scanned: scanned,
        resources_emitted: scanned.min(options.max_metadata_resources),
        truncated: scanned > options.max_metadata_resources,
    }
}

pub(super) fn shutdown_section_v2(report: &ShutdownReport) -> RuntimeShutdownSectionV2 {
    RuntimeShutdownSectionV2 {
        scope: RuntimeDiagnosticsScope::Local,
        elapsed_millis: duration_millis(report.elapsed),
        cancelled: report.cancelled,
        failed: report.failed,
        timed_out: report.timed_out,
        leaked: report.leaked,
    }
}

pub(super) const fn runtime_detail_scope(scope: TaskDetailScope) -> RuntimeDiagnosticsScope {
    match scope {
        TaskDetailScope::Local => RuntimeDiagnosticsScope::Local,
        TaskDetailScope::Subtree => RuntimeDiagnosticsScope::Subtree,
    }
}

pub(super) fn sanitize_blocking_lane(snapshot: BlockingExecutorSnapshot) -> RuntimeBlockingLaneSummaryV1 {
    let lane = match snapshot.lane {
        BlockingLane::StorageIo => RuntimeBlockingLaneV1::StorageIo,
        BlockingLane::MetadataIo => RuntimeBlockingLaneV1::MetadataIo,
        BlockingLane::CpuCrypto => RuntimeBlockingLaneV1::CpuCrypto,
    };
    let mut task_kinds = [
        (BlockingKind::ShortIo, 0usize, Duration::ZERO),
        (BlockingKind::CpuBound, 0usize, Duration::ZERO),
        (BlockingKind::LongRunning, 0usize, Duration::ZERO),
    ];
    for task in snapshot.tasks {
        let index = match task.kind {
            BlockingKind::ShortIo => 0,
            BlockingKind::CpuBound => 1,
            BlockingKind::LongRunning => 2,
        };
        task_kinds[index].1 = task_kinds[index].1.saturating_add(1);
        task_kinds[index].2 = task_kinds[index].2.max(task.elapsed);
    }

    RuntimeBlockingLaneSummaryV1 {
        lane,
        max_concurrency: Some(snapshot.max_concurrency),
        max_queue_depth: Some(snapshot.max_queue_depth),
        queued: snapshot.queued,
        running: snapshot.running,
        timed_out_still_running: snapshot.timed_out_still_running,
        blocking_still_running: snapshot.blocking_still_running,
        task_kinds: task_kinds
            .into_iter()
            .filter_map(|(kind, active, max_elapsed)| {
                (active > 0).then_some(RuntimeBlockingKindSummaryV1 {
                    kind: runtime_blocking_kind(kind),
                    active,
                    max_elapsed_millis: duration_millis(max_elapsed),
                })
            })
            .collect(),
    }
}

pub(super) const fn runtime_lifecycle_state(state: TaskGroupLifecycleState) -> RuntimeLifecycleStateV1 {
    match state {
        TaskGroupLifecycleState::Open => RuntimeLifecycleStateV1::Open,
        TaskGroupLifecycleState::Closing => RuntimeLifecycleStateV1::Closing,
        TaskGroupLifecycleState::Closed => RuntimeLifecycleStateV1::Closed,
        TaskGroupLifecycleState::ShutdownCompleted => RuntimeLifecycleStateV1::ShutdownCompleted,
        TaskGroupLifecycleState::Poisoned => RuntimeLifecycleStateV1::Poisoned,
    }
}

pub(super) const fn runtime_task_kind(kind: TaskKind) -> RuntimeTaskKindV1 {
    match kind {
        TaskKind::Service => RuntimeTaskKindV1::Service,
        TaskKind::Worker => RuntimeTaskKindV1::Worker,
        TaskKind::ScheduledDriver => RuntimeTaskKindV1::ScheduledDriver,
        TaskKind::ScheduledRun => RuntimeTaskKindV1::ScheduledRun,
        TaskKind::BlockingReaper => RuntimeTaskKindV1::BlockingReaper,
        TaskKind::Shutdown => RuntimeTaskKindV1::Shutdown,
        TaskKind::Other => RuntimeTaskKindV1::Other,
    }
}

pub(super) const fn runtime_blocking_kind(kind: BlockingKind) -> RuntimeBlockingKindV1 {
    match kind {
        BlockingKind::ShortIo => RuntimeBlockingKindV1::ShortIo,
        BlockingKind::CpuBound => RuntimeBlockingKindV1::CpuBound,
        BlockingKind::LongRunning => RuntimeBlockingKindV1::LongRunning,
    }
}

pub(super) fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
