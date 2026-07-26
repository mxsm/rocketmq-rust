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
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use serde::Serialize;

use crate::blocking::BlockingExecutorSnapshot;
use crate::blocking::BlockingKind;
use crate::handle::RuntimeHandle;
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
    /// The blocking lanes value.
    pub blocking_lanes: Vec<BlockingExecutorSnapshot>,
}

/// A stable component identifier used by the sanitized diagnostics view.
///
/// The enum deliberately avoids caller-provided labels so diagnostics cannot
/// disclose deployment names or other high-cardinality runtime data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeComponent {
    Broker,
    NameServer,
    Controller,
    Proxy,
    Mcp,
    SreControlPlane,
    SreConnector,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLifecycleStateV1 {
    Open,
    Closing,
    Closed,
    ShutdownCompleted,
    Poisoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTaskKindV1 {
    Service,
    Worker,
    ScheduledDriver,
    ScheduledRun,
    BlockingReaper,
    Shutdown,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeBlockingLaneV1 {
    StorageIo,
    MetadataIo,
    CpuCrypto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeBlockingKindV1 {
    ShortIo,
    CpuBound,
    LongRunning,
}

/// Bounds applied while creating [`RuntimeDiagnosticsViewV1`].
#[derive(Debug, Clone, Copy)]
pub struct RuntimeDiagnosticsViewOptions {
    pub long_running_threshold: Duration,
    pub max_task_kind_summaries: usize,
    pub max_blocking_lane_summaries: usize,
}

impl Default for RuntimeDiagnosticsViewOptions {
    fn default() -> Self {
        Self {
            long_running_threshold: Duration::from_secs(30),
            max_task_kind_summaries: 7,
            max_blocking_lane_summaries: 3,
        }
    }
}

/// Versioned, bounded, and sanitized runtime diagnostics safe for authenticated
/// operational APIs.
///
/// Unlike [`RuntimeDiagnosticsSnapshot`], this view never exposes runtime IDs,
/// task IDs, task names, task-group names, executor names, arguments, or
/// configuration objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeDiagnosticsViewV1 {
    pub schema_version: String,
    pub observed_at: DateTime<Utc>,
    pub component: RuntimeComponent,
    pub lifecycle_state: RuntimeLifecycleStateV1,
    pub task_group_count: usize,
    pub task_count: usize,
    pub task_kinds: Vec<RuntimeTaskKindSummaryV1>,
    pub blocking_lanes: Vec<RuntimeBlockingLaneSummaryV1>,
    pub truncated: bool,
}

impl RuntimeDiagnosticsViewV1 {
    pub const SCHEMA_VERSION: &'static str = "rocketmq.runtime-diagnostics.v1";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeTaskKindSummaryV1 {
    pub kind: RuntimeTaskKindV1,
    pub active: usize,
    pub long_running: usize,
    pub max_elapsed_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeBlockingLaneSummaryV1 {
    pub lane: RuntimeBlockingLaneV1,
    pub queued: usize,
    pub running: usize,
    pub timed_out_still_running: usize,
    pub blocking_still_running: usize,
    pub task_kinds: Vec<RuntimeBlockingKindSummaryV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeBlockingKindSummaryV1 {
    pub kind: RuntimeBlockingKindV1,
    pub active: usize,
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
            child_count: root.child_count(),
            blocking_lanes,
        }
    }

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
            .enumerate()
            .take(options.max_blocking_lane_summaries.min(3))
            .map(|(index, snapshot)| sanitize_blocking_lane(index, snapshot))
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

fn sanitize_blocking_lane(index: usize, snapshot: BlockingExecutorSnapshot) -> RuntimeBlockingLaneSummaryV1 {
    let lane = match index {
        0 => RuntimeBlockingLaneV1::StorageIo,
        1 => RuntimeBlockingLaneV1::MetadataIo,
        _ => RuntimeBlockingLaneV1::CpuCrypto,
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

const fn runtime_lifecycle_state(state: TaskGroupLifecycleState) -> RuntimeLifecycleStateV1 {
    match state {
        TaskGroupLifecycleState::Open => RuntimeLifecycleStateV1::Open,
        TaskGroupLifecycleState::Closing => RuntimeLifecycleStateV1::Closing,
        TaskGroupLifecycleState::Closed => RuntimeLifecycleStateV1::Closed,
        TaskGroupLifecycleState::ShutdownCompleted => RuntimeLifecycleStateV1::ShutdownCompleted,
        TaskGroupLifecycleState::Poisoned => RuntimeLifecycleStateV1::Poisoned,
    }
}

const fn runtime_task_kind(kind: TaskKind) -> RuntimeTaskKindV1 {
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

const fn runtime_blocking_kind(kind: BlockingKind) -> RuntimeBlockingKindV1 {
    match kind {
        BlockingKind::ShortIo => RuntimeBlockingKindV1::ShortIo,
        BlockingKind::CpuBound => RuntimeBlockingKindV1::CpuBound,
        BlockingKind::LongRunning => RuntimeBlockingKindV1::LongRunning,
    }
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeContext;

    #[tokio::test]
    async fn sanitized_view_does_not_expose_runtime_or_task_names() {
        let context = RuntimeContext::from_current("sensitive-root-name");
        let child = context.root_group().child("sensitive-child-name");
        child
            .spawn("sensitive-task-name", TaskKind::Worker, std::future::pending())
            .expect("task should spawn");
        let diagnostics = RuntimeDiagnostics::new(context.runtime().clone());

        let view = diagnostics.view_v1(RuntimeComponent::Mcp, context.root_group(), Vec::new());
        let json = serde_json::to_string(&view).expect("view should serialize");

        assert!(!json.contains("sensitive-root-name"));
        assert!(!json.contains("sensitive-child-name"));
        assert!(!json.contains("sensitive-task-name"));
        assert!(!json.contains("rocketmq-runtime-"));
        assert!(!json.contains("max_concurrency"));
        assert!(!json.contains("max_queue_depth"));
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
        let diagnostics = RuntimeDiagnostics::new(context.runtime().clone());
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
}
