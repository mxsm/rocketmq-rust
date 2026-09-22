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

mod conversion;

use conversion::*;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

use crate::blocking::BlockingExecutorSnapshot;
use crate::blocking::BlockingKind;
use crate::blocking::BlockingLane;
use crate::metadata_io::MetadataIoSnapshot;
use crate::scheduled::ScheduledTaskSnapshot;
use crate::shutdown_report::ShutdownReport;
use crate::task_group::TaskDetailScope;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupId;
use crate::task_group::TaskGroupLifecycleState;
use crate::task_group::TaskKind;

static NEXT_RUNTIME_DIAGNOSTICS_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
/// Represents runtime diagnostics.
pub struct RuntimeDiagnostics {
    runtime_id: Arc<str>,
}

#[derive(Debug, Clone, Serialize)]
/// Represents runtime diagnostics snapshot.
pub struct RuntimeDiagnosticsSnapshot {
    /// The runtime identifier.
    pub runtime_id: String,
    /// The root name value.
    pub root_name: String,
    /// The group identifier.
    pub group_id: TaskGroupId,
    /// The parent group identifier.
    pub parent_group_id: Option<TaskGroupId>,
    /// The lifecycle state value.
    pub lifecycle_state: TaskGroupLifecycleState,
    /// The number of task entries.
    pub task_count: usize,
    /// The number of child entries.
    pub child_count: usize,
    /// The number of active task entries.
    pub active_tasks: usize,
    /// The number of active child registry slots.
    pub registry_slots: usize,
    /// The blocking lanes value.
    pub blocking_lanes: Vec<BlockingExecutorSnapshot>,
}

/// A stable component identifier used by the sanitized diagnostics view.
///
/// The enum deliberately avoids caller-provided labels so diagnostics cannot
/// disclose deployment names or other high-cardinality runtime data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeComponent {
    /// A RocketMQ broker process.
    Broker,
    /// A RocketMQ NameServer process.
    NameServer,
    /// A RocketMQ controller process.
    Controller,
    /// A RocketMQ proxy process.
    Proxy,
    /// A RocketMQ MCP server process.
    Mcp,
    /// An AI SRE control-plane process.
    SreControlPlane,
    /// An AI SRE connector process.
    SreConnector,
    /// A component without a dedicated bounded identifier.
    Other,
}

/// Describes the lifecycle state of the root task group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLifecycleStateV1 {
    /// The task group accepts new tasks and child groups.
    Open,
    /// Shutdown has started and new work is rejected.
    Closing,
    /// The task group is closed and its owned work is draining.
    Closed,
    /// Shutdown completed and all bounded cleanup attempts have finished.
    ShutdownCompleted,
    /// The lifecycle state could not be mapped to a supported value.
    Poisoned,
}

/// Identifies a bounded category of runtime-owned task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTaskKindV1 {
    /// A long-lived service task.
    Service,
    /// A background worker task.
    Worker,
    /// A task that drives a scheduled workload.
    ScheduledDriver,
    /// One execution of a scheduled workload.
    ScheduledRun,
    /// A task that reaps completed blocking work.
    BlockingReaper,
    /// A task that coordinates shutdown.
    Shutdown,
    /// A task kind without a dedicated bounded identifier.
    Other,
}

/// Identifies a bounded blocking-executor lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeBlockingLaneV1 {
    /// Blocking storage input and output.
    StorageIo,
    /// Blocking metadata input and output.
    MetadataIo,
    /// CPU-intensive cryptographic work.
    CpuCrypto,
}

/// Identifies a bounded category of blocking work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeBlockingKindV1 {
    /// Short blocking input or output.
    ShortIo,
    /// CPU-bound work.
    CpuBound,
    /// Blocking work expected to run for an extended duration.
    LongRunning,
}

/// Bounds applied while creating [`RuntimeDiagnosticsViewV1`].
#[derive(Debug, Clone, Copy)]
pub struct RuntimeDiagnosticsViewOptions {
    /// Elapsed time after which an active task is classified as long-running.
    pub long_running_threshold: Duration,
    /// Maximum number of task-kind summaries included in the view.
    pub max_task_kind_summaries: usize,
    /// Maximum number of blocking-lane summaries included in the view.
    pub max_blocking_lane_summaries: usize,
}

impl Default for RuntimeDiagnosticsViewOptions {
    fn default() -> Self {
        Self {
            long_running_threshold: Duration::from_secs(30),
            max_task_kind_summaries: TaskKind::COUNT,
            max_blocking_lane_summaries: BlockingLane::ALL.len(),
        }
    }
}

/// Versioned, bounded, and sanitized runtime diagnostics safe for authenticated
/// operational APIs.
///
/// Unlike [`RuntimeDiagnosticsSnapshot`], this view never exposes runtime IDs,
/// task IDs, task names, task-group names, executor names, arguments, or
/// configuration objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeDiagnosticsViewV1 {
    /// Schema identifier for this serialized diagnostics view.
    pub schema_version: String,
    /// UTC timestamp at which the snapshot was observed.
    pub observed_at: DateTime<Utc>,
    /// Bounded component category that owns the runtime.
    pub component: RuntimeComponent,
    /// Current lifecycle state of the root task group.
    pub lifecycle_state: RuntimeLifecycleStateV1,
    /// Number of task groups represented by the snapshot.
    pub task_group_count: usize,
    /// Number of active tasks represented by the snapshot.
    pub task_count: usize,
    /// Bounded aggregate summaries grouped by task kind.
    pub task_kinds: Vec<RuntimeTaskKindSummaryV1>,
    /// Bounded aggregate summaries for blocking-executor lanes.
    pub blocking_lanes: Vec<RuntimeBlockingLaneSummaryV1>,
    /// Whether one or more summaries were omitted by configured bounds.
    pub truncated: bool,
}

impl RuntimeDiagnosticsViewV1 {
    /// Stable schema identifier emitted by [`RuntimeDiagnostics::view_v1`].
    pub const SCHEMA_VERSION: &'static str = "rocketmq.runtime-diagnostics.v1";
}

/// Aggregated task counts and elapsed-time information for one task kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTaskKindSummaryV1 {
    /// Bounded task category.
    pub kind: RuntimeTaskKindV1,
    /// Number of active tasks in the category.
    pub active: usize,
    /// Number of active tasks exceeding the long-running threshold.
    pub long_running: usize,
    /// Maximum elapsed time among active tasks, in milliseconds.
    pub max_elapsed_millis: u64,
}

/// Aggregated state for one blocking-executor lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeBlockingLaneSummaryV1 {
    /// Bounded blocking-executor lane.
    pub lane: RuntimeBlockingLaneV1,
    /// Configured concurrency limit when the executor exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<usize>,
    /// Configured queue-depth limit when the executor exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_queue_depth: Option<usize>,
    /// Number of queued blocking tasks.
    pub queued: usize,
    /// Number of running blocking tasks.
    pub running: usize,
    /// Number of timed-out tasks whose blocking work is still running.
    pub timed_out_still_running: usize,
    /// Number of blocking tasks that remain active during observation.
    pub blocking_still_running: usize,
    /// Bounded aggregate summaries grouped by blocking-work kind.
    pub task_kinds: Vec<RuntimeBlockingKindSummaryV1>,
}

/// Aggregated task counts and elapsed-time information for one blocking-work kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeBlockingKindSummaryV1 {
    /// Bounded blocking-work category.
    pub kind: RuntimeBlockingKindV1,
    /// Number of active blocking tasks in the category.
    pub active: usize,
    /// Maximum elapsed time among active tasks, in milliseconds.
    pub max_elapsed_millis: u64,
}

impl RuntimeDiagnostics {
    pub(crate) fn new() -> Self {
        let runtime_id = NEXT_RUNTIME_DIAGNOSTICS_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            runtime_id: Arc::from(format!("rocketmq-runtime-{runtime_id}")),
        }
    }

    /// Returns the runtime id.
    pub fn runtime_id(&self) -> &str {
        &self.runtime_id
    }

    /// Returns the snapshot.
    pub fn snapshot(
        &self,
        root: &TaskGroup,
        blocking_lanes: Vec<BlockingExecutorSnapshot>,
    ) -> RuntimeDiagnosticsSnapshot {
        RuntimeDiagnosticsSnapshot {
            runtime_id: self.runtime_id.to_string(),
            root_name: root.name().to_string(),
            group_id: root.id(),
            parent_group_id: root.parent_id(),
            lifecycle_state: root.lifecycle_state(),
            task_count: root.task_count(),
            child_count: root.component_count(),
            active_tasks: root.task_count(),
            registry_slots: root.component_count(),
            blocking_lanes,
        }
    }

    /// Creates a bounded diagnostics view using the default sanitization limits.
    pub fn view_v1(
        &self,
        component: RuntimeComponent,
        root: &TaskGroup,
        blocking_lanes: Vec<BlockingExecutorSnapshot>,
    ) -> RuntimeDiagnosticsViewV1 {
        self.view_v1_with_options(
            component,
            root,
            blocking_lanes,
            RuntimeDiagnosticsViewOptions::default(),
        )
    }

    /// Creates a bounded diagnostics view using explicit sanitization limits.
    pub fn view_v1_with_options(
        &self,
        component: RuntimeComponent,
        root: &TaskGroup,
        blocking_lanes: Vec<BlockingExecutorSnapshot>,
        options: RuntimeDiagnosticsViewOptions,
    ) -> RuntimeDiagnosticsViewV1 {
        let task_diagnostics = root.diagnostics(options.long_running_threshold);
        let task_kind_count = task_diagnostics.task_kinds.len();
        let task_kinds = task_diagnostics
            .task_kinds
            .into_iter()
            .take(options.max_task_kind_summaries)
            .map(|summary| RuntimeTaskKindSummaryV1 {
                kind: runtime_task_kind(summary.kind),
                active: summary.active,
                long_running: summary.long_running,
                max_elapsed_millis: duration_millis(summary.max_elapsed),
            })
            .collect();

        let blocking_lane_count = blocking_lanes.len();
        let blocking_lanes = blocking_lanes
            .into_iter()
            .take(options.max_blocking_lane_summaries.min(BlockingLane::ALL.len()))
            .map(sanitize_blocking_lane)
            .collect();

        RuntimeDiagnosticsViewV1 {
            schema_version: RuntimeDiagnosticsViewV1::SCHEMA_VERSION.to_string(),
            observed_at: Utc::now(),
            component,
            lifecycle_state: runtime_lifecycle_state(root.lifecycle_state()),
            task_group_count: task_diagnostics.group_count,
            task_count: task_diagnostics.task_count,
            task_kinds,
            blocking_lanes,
            truncated: task_kind_count > options.max_task_kind_summaries
                || blocking_lane_count > options.max_blocking_lane_summaries.min(3),
        }
    }
}

/// Names the population a V2 diagnostics value covers.
///
/// The scope is part of the payload, so a reader never has to infer it from how
/// the view was produced, and a partial value cannot be read as a total.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDiagnosticsScope {
    /// Only the calling group's own state.
    Local,
    /// The calling group and every descendant group.
    Subtree,
    /// State owned once per process and shared by every component.
    ProcessShared,
}

/// Bounds applied while creating [`RuntimeDiagnosticsViewV2`].
///
/// The detail budgets default to zero, so a routine sample reads only bounded
/// aggregates and a task list is always an explicit request.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeDiagnosticsViewOptionsV2 {
    /// Elapsed time after which an active task is classified as long-running.
    pub long_running_threshold: Duration,
    /// Maximum number of task-kind summaries included in the view.
    pub max_task_kind_summaries: usize,
    /// Maximum number of blocking-lane summaries included in the view.
    pub max_blocking_lane_summaries: usize,
    /// Maximum number of schedule summaries included in the view.
    pub max_schedule_tasks: usize,
    /// Maximum number of metadata resources summarized in the view.
    pub max_metadata_resources: usize,
    /// Maximum number of task details emitted when a detail list is requested.
    pub max_detail_entries: usize,
    /// Maximum number of tasks examined when a detail list is requested.
    pub detail_scan_budget: usize,
}

impl Default for RuntimeDiagnosticsViewOptionsV2 {
    fn default() -> Self {
        Self {
            long_running_threshold: Duration::from_secs(30),
            max_task_kind_summaries: TaskKind::COUNT,
            max_blocking_lane_summaries: BlockingLane::ALL.len(),
            max_schedule_tasks: 16,
            max_metadata_resources: 32,
            max_detail_entries: 0,
            detail_scan_budget: 0,
        }
    }
}

/// Component-owned inputs for the V2 view.
///
/// A section whose input is absent is reported as absent rather than as zero, so
/// a caller that does not own scheduled work, a metadata actor, or a shutdown
/// report cannot make the view claim those values are empty.
#[derive(Debug, Clone, Default)]
pub struct RuntimeDiagnosticsInputs {
    /// Snapshots of the caller's scheduled tasks.
    pub schedule: Vec<ScheduledTaskSnapshot>,
    /// The snapshot of the caller's metadata actor.
    pub metadata: Option<MetadataIoSnapshot>,
    /// The most recent shutdown report for the caller's group.
    pub shutdown: Option<ShutdownReport>,
}

/// Bounded task counts for one group population.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTaskSectionV2 {
    /// The population these counts cover.
    pub scope: RuntimeDiagnosticsScope,
    /// Number of task groups represented.
    pub task_group_count: usize,
    /// Number of active tasks represented.
    pub task_count: usize,
    /// Number of active tasks in the calling group alone.
    ///
    /// The aggregate `task_count` is scoped to the subtree, so this value lets a
    /// reader separate the calling group's own work from its descendants without
    /// requesting a detail list.
    pub local_task_count: usize,
    /// Number of active tasks exceeding the long-running threshold.
    pub long_running: usize,
    /// Maximum elapsed time among active tasks, in milliseconds.
    pub max_elapsed_millis: u64,
    /// Bounded aggregate summaries grouped by task kind.
    pub task_kinds: Vec<RuntimeTaskKindSummaryV1>,
    /// Whether summaries were omitted by configured bounds.
    pub truncated: bool,
}

/// Bounded blocking-executor state, including work that outlived its wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeBlockingSectionV2 {
    /// The population these counts cover.
    pub scope: RuntimeDiagnosticsScope,
    /// Number of queued blocking tasks.
    pub queued: usize,
    /// Number of running blocking tasks.
    pub running: usize,
    /// Number of waits that expired while the blocking closure keeps running.
    pub timed_out_still_running: usize,
    /// Number of blocking tasks that were still running during observation.
    pub blocking_still_running: usize,
    /// Bounded per-lane summaries.
    pub lanes: Vec<RuntimeBlockingLaneSummaryV1>,
    /// Whether summaries were omitted by configured bounds.
    pub truncated: bool,
}

/// Bounded scheduled-work aggregates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeScheduleSectionV2 {
    /// The population these counts cover.
    pub scope: RuntimeDiagnosticsScope,
    /// Number of scheduled tasks examined.
    pub tasks_scanned: usize,
    /// Number of scheduled tasks included in the aggregates.
    pub tasks_emitted: usize,
    /// Number of runs that are still active.
    pub active_runs: u64,
    /// Number of runs started.
    pub runs: u64,
    /// Number of skipped ticks.
    pub skips: u64,
    /// Number of overlapping runs that were coalesced or refused.
    pub overlaps: u64,
    /// Number of failed runs.
    pub failures: u64,
    /// Maximum run duration, in milliseconds.
    pub max_elapsed_millis: u64,
    /// Whether summaries were omitted by configured bounds.
    pub truncated: bool,
}

/// Bounded retained-metadata state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeMetadataSectionV2 {
    /// The population these counts cover.
    pub scope: RuntimeDiagnosticsScope,
    /// Whether the metadata actor still accepts work.
    pub accepting: bool,
    /// Number of retained operations.
    pub retained_operations: usize,
    /// Number of retained payload bytes.
    pub retained_bytes: usize,
    /// Number of durability waiters.
    pub waiters: usize,
    /// Number of metadata resources examined.
    pub resources_scanned: usize,
    /// Number of metadata resources included in the counts.
    pub resources_emitted: usize,
    /// Whether resources were omitted by configured bounds.
    pub truncated: bool,
}

/// The most recent shutdown result for one group population.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeShutdownSectionV2 {
    /// The population this result covers.
    pub scope: RuntimeDiagnosticsScope,
    /// Shutdown duration in milliseconds.
    pub elapsed_millis: u64,
    /// Number of tasks that were cancelled.
    pub cancelled: usize,
    /// Number of tasks that finished with an explicit failure.
    pub failed: usize,
    /// Number of tasks that did not finish inside the deadline.
    pub timed_out: usize,
    /// Number of tasks that were still tracked after shutdown.
    pub leaked: usize,
}

/// One bounded task detail.
///
/// Carries a bounded category, the population it was observed in, and its
/// elapsed time. Task names, group names, and identifiers are deliberately
/// excluded because they are caller-provided labels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTaskDetailV2 {
    /// Bounded task category.
    pub kind: RuntimeTaskKindV1,
    /// The population the task was observed in.
    pub scope: RuntimeDiagnosticsScope,
    /// Elapsed time since the task started, in milliseconds.
    pub elapsed_millis: u64,
}

/// Versioned, bounded, and sanitized runtime diagnostics with explicit scopes.
///
/// Unlike [`RuntimeDiagnosticsViewV1`], every section states the population it
/// covers, the payload separates a section that has no input from a section that
/// is empty, and a bounded detail list reports how much of the tree it examined
/// instead of presenting a partial list as the whole runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeDiagnosticsViewV2 {
    /// Schema identifier for this serialized diagnostics view.
    pub schema_version: String,
    /// UTC timestamp at which the snapshot was observed.
    pub observed_at: DateTime<Utc>,
    /// Bounded component category that owns the runtime.
    pub component: RuntimeComponent,
    /// Current lifecycle state of the root task group.
    pub lifecycle_state: RuntimeLifecycleStateV1,
    /// Bounded task counts and their scope.
    pub tasks: RuntimeTaskSectionV2,
    /// Bounded blocking state and its scope.
    pub blocking: RuntimeBlockingSectionV2,
    /// Bounded scheduled-work aggregates, absent when the caller owns none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<RuntimeScheduleSectionV2>,
    /// Bounded retained-metadata state, absent when the caller owns no actor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<RuntimeMetadataSectionV2>,
    /// The most recent shutdown result, absent until shutdown was attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shutdown: Option<RuntimeShutdownSectionV2>,
    /// Bounded task details, empty unless a detail list was requested.
    pub details: Vec<RuntimeTaskDetailV2>,
    /// The scan budget applied while collecting details.
    pub detail_scan_budget: usize,
    /// Number of tasks examined while collecting details.
    pub details_scanned: usize,
    /// Whether any section, including the detail list, was cut short.
    pub truncated: bool,
}

impl RuntimeDiagnosticsViewV2 {
    /// Stable schema identifier emitted by [`RuntimeDiagnostics::view_v2`].
    pub const SCHEMA_VERSION: &'static str = "rocketmq.runtime-diagnostics.v2";
}

impl RuntimeDiagnostics {
    /// Creates a bounded, explicitly scoped diagnostics view.
    pub fn view_v2(
        &self,
        component: RuntimeComponent,
        root: &TaskGroup,
        blocking_lanes: Vec<BlockingExecutorSnapshot>,
        inputs: RuntimeDiagnosticsInputs,
    ) -> RuntimeDiagnosticsViewV2 {
        self.view_v2_with_options(
            component,
            root,
            blocking_lanes,
            inputs,
            RuntimeDiagnosticsViewOptionsV2::default(),
        )
    }

    /// Creates a bounded, explicitly scoped diagnostics view using explicit
    /// budget limits.
    pub fn view_v2_with_options(
        &self,
        component: RuntimeComponent,
        root: &TaskGroup,
        blocking_lanes: Vec<BlockingExecutorSnapshot>,
        inputs: RuntimeDiagnosticsInputs,
        options: RuntimeDiagnosticsViewOptionsV2,
    ) -> RuntimeDiagnosticsViewV2 {
        let (tasks, tasks_truncated) = task_section_v2(root, options);
        let (blocking, blocking_truncated) = blocking_section_v2(blocking_lanes, options);
        let schedule = (!inputs.schedule.is_empty()).then(|| schedule_section_v2(&inputs.schedule, options));
        let metadata = inputs
            .metadata
            .as_ref()
            .map(|snapshot| metadata_section_v2(snapshot, options));
        let shutdown = inputs.shutdown.as_ref().map(shutdown_section_v2);
        // A zero budget means no detail list was requested, so a routine sample
        // neither pays for the scan nor reports truncation for a list nobody
        // asked for.
        let details = if options.max_detail_entries == 0 && options.detail_scan_budget == 0 {
            crate::task_group::TaskDetailScan::default()
        } else {
            root.bounded_task_details(options.detail_scan_budget, options.max_detail_entries)
        };
        let details_truncated = details.truncated;

        let schedule_truncated = schedule.as_ref().is_some_and(|section| section.truncated);
        let metadata_truncated = metadata.as_ref().is_some_and(|section| section.truncated);

        RuntimeDiagnosticsViewV2 {
            schema_version: RuntimeDiagnosticsViewV2::SCHEMA_VERSION.to_string(),
            observed_at: Utc::now(),
            component,
            lifecycle_state: runtime_lifecycle_state(root.lifecycle_state()),
            tasks,
            blocking,
            schedule,
            metadata,
            shutdown,
            details: details
                .details
                .into_iter()
                .map(|detail| RuntimeTaskDetailV2 {
                    kind: runtime_task_kind(detail.kind),
                    scope: runtime_detail_scope(detail.scope),
                    elapsed_millis: duration_millis(detail.elapsed),
                })
                .collect(),
            detail_scan_budget: options.detail_scan_budget,
            details_scanned: details.scanned,
            truncated: tasks_truncated
                || blocking_truncated
                || schedule_truncated
                || metadata_truncated
                || details_truncated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeContext;

    #[tokio::test]
    async fn lane_identity_survives_partial_and_reordered_inputs_in_both_schemas() {
        let context = RuntimeContext::from_current("lane-identity");
        let diagnostics = RuntimeDiagnostics::new();
        let snapshot = |lane: BlockingLane| BlockingExecutorSnapshot {
            name: "private-lane".to_owned(),
            lane,
            max_concurrency: 8,
            max_queue_depth: 32,
            global_capacity: 8,
            global_running: 0,
            global_available: 8,
            lane_reserved: 1,
            lane_running: 0,
            lane_borrowed: 0,
            queued: lane.index() + 10,
            running: 0,
            timed_out_still_running: 0,
            blocking_still_running: 0,
            rejected: 0,
            oldest_queue_wait: Duration::ZERO,
            tasks: Vec::new(),
        };
        for lanes in [
            vec![BlockingLane::MetadataIo],
            vec![
                BlockingLane::CpuCrypto,
                BlockingLane::StorageIo,
                BlockingLane::MetadataIo,
            ],
            vec![BlockingLane::MetadataIo, BlockingLane::CpuCrypto],
            vec![],
        ] {
            let snapshots: Vec<_> = lanes.iter().copied().map(snapshot).collect();
            let v1 = diagnostics.view_v1(RuntimeComponent::Broker, context.root_group(), snapshots.clone());
            let v2 = diagnostics.view_v2(
                RuntimeComponent::Broker,
                context.root_group(),
                snapshots,
                RuntimeDiagnosticsInputs::default(),
            );
            assert_eq!(v1.blocking_lanes, v2.blocking.lanes);
            assert_eq!(v1.blocking_lanes.len(), lanes.len());
            for (summary, lane) in v1.blocking_lanes.iter().zip(lanes) {
                let expected = match lane {
                    BlockingLane::StorageIo => "storage_io",
                    BlockingLane::MetadataIo => "metadata_io",
                    BlockingLane::CpuCrypto => "cpu_crypto",
                };
                assert_eq!(serde_json::to_value(summary.lane).unwrap(), expected);
                assert_eq!(summary.queued, lane.index() + 10);
            }
        }
    }

    #[tokio::test]
    async fn every_task_kind_keeps_its_own_population_and_wire_label() {
        let context = RuntimeContext::from_current("kind-population");
        let root = context.root_group();
        let labels = [
            "service",
            "worker",
            "scheduled_driver",
            "scheduled_run",
            "blocking_reaper",
            "shutdown",
            "other",
        ];
        for (kind, count) in TaskKind::ALL.into_iter().zip(1..) {
            for _ in 0..count {
                let cancellation = root.cancellation_token();
                root.spawn("pending", kind, async move { cancellation.cancelled().await })
                    .unwrap();
            }
        }
        let view = RuntimeDiagnostics::new().view_v1(RuntimeComponent::Broker, root, Vec::new());
        assert_eq!(view.task_kinds.len(), labels.len());
        for ((summary, label), count) in view.task_kinds.iter().zip(labels).zip(1..) {
            assert_eq!(serde_json::to_value(summary.kind).unwrap(), label);
            assert_eq!(summary.active, count);
        }
        assert!(root.shutdown(Duration::from_secs(1)).await.is_healthy());
    }

    #[tokio::test]
    async fn sanitized_view_does_not_expose_runtime_or_task_names() {
        let context = RuntimeContext::from_current("sensitive-root-name");
        let child = context.service_context("sensitive-child-name");
        child
            .spawn("sensitive-task-name", TaskKind::Worker, std::future::pending())
            .expect("task should spawn");
        let diagnostics = RuntimeDiagnostics::new();

        let view = diagnostics.view_v1(RuntimeComponent::Mcp, context.root_group(), Vec::new());
        let json = serde_json::to_string(&view).expect("view should serialize");

        assert!(!json.contains("sensitive-root-name"));
        assert!(!json.contains("sensitive-child-name"));
        assert!(!json.contains("sensitive-task-name"));
        assert!(!json.contains("rocketmq-runtime-"));
        assert!(json.contains("\"lifecycle_state\":\"open\""));
        assert!(json.contains("\"kind\":\"worker\""));
        assert!(view.task_group_count >= 2);
        assert_eq!(view.task_count, 1);
    }

    #[tokio::test]
    async fn zero_bounds_produce_an_explicit_truncated_view() {
        let context = RuntimeContext::from_current("runtime-view-bounds");
        context
            .root_group()
            .spawn("worker", TaskKind::Worker, std::future::pending())
            .expect("task should spawn");
        let diagnostics = RuntimeDiagnostics::new();
        let view = diagnostics.view_v1_with_options(
            RuntimeComponent::Other,
            context.root_group(),
            Vec::new(),
            RuntimeDiagnosticsViewOptions {
                max_task_kind_summaries: 0,
                max_blocking_lane_summaries: 0,
                ..RuntimeDiagnosticsViewOptions::default()
            },
        );

        assert!(view.truncated);
        assert!(view.task_kinds.is_empty());
        assert!(view.blocking_lanes.is_empty());
    }

    #[tokio::test]
    async fn sanitized_view_exposes_only_bounded_blocking_capacity_and_counts() {
        let context = RuntimeContext::from_current("runtime-blocking-capacity");
        let diagnostics = RuntimeDiagnostics::new();
        let view = diagnostics.view_v1(
            RuntimeComponent::Broker,
            context.root_group(),
            vec![BlockingExecutorSnapshot {
                name: "sensitive-lane-name".to_owned(),
                lane: crate::BlockingLane::StorageIo,
                max_concurrency: 8,
                max_queue_depth: 32,
                global_capacity: 8,
                global_running: 2,
                global_available: 6,
                lane_reserved: 8,
                lane_running: 2,
                lane_borrowed: 0,
                queued: 3,
                running: 2,
                timed_out_still_running: 1,
                blocking_still_running: 0,
                rejected: 0,
                oldest_queue_wait: Duration::ZERO,
                tasks: Vec::new(),
            }],
        );

        assert_eq!(view.blocking_lanes.len(), 1);
        assert_eq!(view.blocking_lanes[0].max_concurrency, Some(8));
        assert_eq!(view.blocking_lanes[0].max_queue_depth, Some(32));
        let encoded = serde_json::to_string(&view).expect("view should serialize");
        assert!(!encoded.contains("sensitive-lane-name"));
        let decoded: RuntimeDiagnosticsViewV1 =
            serde_json::from_str(&encoded).expect("versioned view should deserialize");
        assert_eq!(decoded, view);

        let mut legacy = serde_json::to_value(&view).expect("view should encode");
        let lane = legacy
            .get_mut("blocking_lanes")
            .and_then(serde_json::Value::as_array_mut)
            .and_then(|lanes| lanes.first_mut())
            .and_then(serde_json::Value::as_object_mut)
            .expect("blocking lane");
        lane.remove("max_concurrency");
        lane.remove("max_queue_depth");
        let legacy: RuntimeDiagnosticsViewV1 =
            serde_json::from_value(legacy).expect("older v1 view should remain readable");
        assert_eq!(legacy.blocking_lanes[0].max_concurrency, None);
        assert_eq!(legacy.blocking_lanes[0].max_queue_depth, None);
    }

    fn scheduled_snapshot(name: &str, runs: u64, skips: u64, max_elapsed_ms: u64) -> ScheduledTaskSnapshot {
        ScheduledTaskSnapshot {
            name: name.to_string(),
            mode: crate::scheduled::ScheduleMode::FixedDelay,
            running: true,
            active_runs: 1,
            runs,
            skips,
            overlaps: 0,
            failures: 0,
            last_drift_ms: 0,
            last_elapsed_ms: max_elapsed_ms,
            max_elapsed_ms,
        }
    }

    fn metadata_snapshot(waiter_count: usize) -> MetadataIoSnapshot {
        MetadataIoSnapshot {
            accepting: true,
            pending_operations: 2,
            pending_bytes: 4_096,
            max_pending_operations: 1_024,
            max_pending_bytes: 65_536,
            resources: vec![crate::metadata_io::MetadataIoResourceSnapshot {
                resource: "sensitive-resource-name".into(),
                target: None,
                durable_generation: None,
                in_flight_generation: None,
                queued_generation: None,
                waiter_count,
            }],
        }
    }

    #[tokio::test]
    async fn v2_sections_state_their_scope_and_absent_inputs_stay_absent() {
        let context = RuntimeContext::from_current("runtime-v2-scope");
        context
            .root_group()
            .spawn("local-worker", TaskKind::Worker, std::future::pending())
            .expect("task should spawn");
        context
            .service_context("runtime-v2-child")
            .spawn("child-worker", TaskKind::Worker, std::future::pending())
            .expect("task should spawn");
        let diagnostics = RuntimeDiagnostics::new();

        let view = diagnostics.view_v2(
            RuntimeComponent::Broker,
            context.root_group(),
            Vec::new(),
            RuntimeDiagnosticsInputs::default(),
        );

        assert_eq!(view.schema_version, "rocketmq.runtime-diagnostics.v2");
        assert_eq!(view.tasks.scope, RuntimeDiagnosticsScope::Subtree);
        assert_eq!(view.tasks.task_count, 2);
        assert_eq!(view.tasks.local_task_count, 1);
        assert_eq!(view.blocking.scope, RuntimeDiagnosticsScope::ProcessShared);
        assert!(view.schedule.is_none());
        assert!(view.metadata.is_none());
        assert!(view.shutdown.is_none());
        assert!(view.details.is_empty());
        // No detail list was requested, so a routine sample reports no truncation.
        assert!(!view.truncated);
    }

    #[tokio::test]
    async fn v2_reports_component_owned_sections_with_their_scope() {
        let context = RuntimeContext::from_current("runtime-v2-sections");
        let diagnostics = RuntimeDiagnostics::new();

        let view = diagnostics.view_v2(
            RuntimeComponent::Broker,
            context.root_group(),
            Vec::new(),
            RuntimeDiagnosticsInputs {
                schedule: vec![scheduled_snapshot("sensitive-schedule", 4, 3, 250)],
                metadata: Some(metadata_snapshot(2)),
                shutdown: Some(ShutdownReport {
                    cancelled: 1,
                    failed: 1,
                    timed_out: 1,
                    leaked: 1,
                    ..ShutdownReport::new("sensitive-group", Duration::from_millis(120))
                }),
            },
        );

        let schedule = view.schedule.clone().expect("a supplied schedule section is present");
        assert_eq!(schedule.scope, RuntimeDiagnosticsScope::Local);
        assert_eq!(schedule.tasks_scanned, 1);
        assert_eq!(schedule.tasks_emitted, 1);
        assert_eq!(schedule.runs, 4);
        assert_eq!(schedule.skips, 3);
        assert_eq!(schedule.max_elapsed_millis, 250);
        assert!(!schedule.truncated);

        let metadata = view.metadata.clone().expect("a supplied metadata section is present");
        assert_eq!(metadata.scope, RuntimeDiagnosticsScope::Local);
        assert_eq!(metadata.retained_operations, 2);
        assert_eq!(metadata.retained_bytes, 4_096);
        assert_eq!(metadata.waiters, 2);
        assert!(metadata.accepting);

        let shutdown = view.shutdown.clone().expect("a supplied shutdown section is present");
        assert_eq!(shutdown.scope, RuntimeDiagnosticsScope::Local);
        assert_eq!(shutdown.elapsed_millis, 120);
        assert_eq!(shutdown.failed, 1);
        assert_eq!(shutdown.timed_out, 1);
        assert_eq!(shutdown.leaked, 1);

        let json = serde_json::to_string(&view).expect("view should serialize");
        assert!(!json.contains("sensitive"));
    }

    #[tokio::test]
    async fn v2_truncation_reports_what_was_scanned_and_emitted() {
        let context = RuntimeContext::from_current("runtime-v2-truncation");
        let diagnostics = RuntimeDiagnostics::new();
        let schedule = vec![
            scheduled_snapshot("first", 1, 0, 10),
            scheduled_snapshot("second", 1, 0, 20),
            scheduled_snapshot("third", 1, 0, 30),
        ];

        let view = diagnostics.view_v2_with_options(
            RuntimeComponent::Other,
            context.root_group(),
            Vec::new(),
            RuntimeDiagnosticsInputs {
                schedule,
                ..RuntimeDiagnosticsInputs::default()
            },
            RuntimeDiagnosticsViewOptionsV2 {
                max_schedule_tasks: 1,
                ..RuntimeDiagnosticsViewOptionsV2::default()
            },
        );

        let section = view.schedule.expect("a supplied schedule section is present");
        assert!(section.truncated);
        assert_eq!(section.tasks_scanned, 3);
        assert_eq!(section.tasks_emitted, 1);
        assert_eq!(section.runs, 1);
        assert!(view.truncated);
    }

    #[tokio::test]
    async fn v2_detail_lists_stay_bounded_and_redacted() {
        let context = RuntimeContext::from_current("sensitive-runtime-name");
        let child = context.service_context("sensitive-child-name");
        for index in 0..4 {
            child
                .spawn(
                    format!("sensitive-task-{index}"),
                    TaskKind::Worker,
                    std::future::pending(),
                )
                .expect("task should spawn");
        }
        let diagnostics = RuntimeDiagnostics::new();

        let view = diagnostics.view_v2_with_options(
            RuntimeComponent::Mcp,
            context.root_group(),
            Vec::new(),
            RuntimeDiagnosticsInputs::default(),
            RuntimeDiagnosticsViewOptionsV2 {
                max_detail_entries: 2,
                detail_scan_budget: 4,
                ..RuntimeDiagnosticsViewOptionsV2::default()
            },
        );

        assert_eq!(view.details.len(), 2);
        assert_eq!(view.details_scanned, 4);
        assert_eq!(view.detail_scan_budget, 4);
        assert!(view.truncated);
        assert!(
            view.details
                .iter()
                .all(|detail| detail.scope == RuntimeDiagnosticsScope::Subtree),
            "the tasks belong to a descendant group"
        );

        let json = serde_json::to_string(&view).expect("view should serialize");
        assert!(!json.contains("sensitive"));
        assert!(!json.contains("rocketmq-runtime-"));
        let decoded: RuntimeDiagnosticsViewV2 = serde_json::from_str(&json).expect("versioned view should deserialize");
        assert_eq!(decoded, view);
    }

    #[tokio::test]
    async fn v2_detail_scan_budget_bounds_the_scan_and_not_only_the_output() {
        let context = RuntimeContext::from_current("runtime-v2-scan-budget");
        for index in 0..4 {
            context
                .root_group()
                .spawn(format!("worker-{index}"), TaskKind::Worker, std::future::pending())
                .expect("task should spawn");
        }
        let diagnostics = RuntimeDiagnostics::new();

        let view = diagnostics.view_v2_with_options(
            RuntimeComponent::Other,
            context.root_group(),
            Vec::new(),
            RuntimeDiagnosticsInputs::default(),
            RuntimeDiagnosticsViewOptionsV2 {
                max_detail_entries: 8,
                detail_scan_budget: 2,
                ..RuntimeDiagnosticsViewOptionsV2::default()
            },
        );

        assert_eq!(view.details_scanned, 2);
        assert_eq!(view.details.len(), 2);
        assert!(view.truncated, "the scan stopped before every task was examined");
        assert!(view
            .details
            .iter()
            .all(|detail| detail.scope == RuntimeDiagnosticsScope::Local));
    }

    #[tokio::test]
    async fn v2_view_is_a_snapshot_that_late_completions_cannot_rewrite() {
        let context = RuntimeContext::from_current("runtime-v2-frozen");
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        context
            .root_group()
            .spawn("finishes-later", TaskKind::Worker, async move {
                let _ = wait.await;
            })
            .expect("task should spawn");
        let diagnostics = RuntimeDiagnostics::new();

        let view = diagnostics.view_v2(
            RuntimeComponent::Other,
            context.root_group(),
            Vec::new(),
            RuntimeDiagnosticsInputs::default(),
        );
        assert_eq!(view.tasks.task_count, 1);

        release.send(()).expect("the task should still be waiting");
        for _ in 0..10_000 {
            if context.root_group().task_count() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert_eq!(context.root_group().task_count(), 0, "the task really finished");
        assert_eq!(
            view.tasks.task_count, 1,
            "the earlier view keeps what it observed instead of following late completions"
        );
    }
}
