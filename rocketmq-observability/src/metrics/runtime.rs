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

pub use crate::semantic::metrics::RUNTIME_BLOCKING_QUEUED;
pub use crate::semantic::metrics::RUNTIME_BLOCKING_RUNNING;
pub use crate::semantic::metrics::RUNTIME_BLOCKING_TIMEOUTS;
pub use crate::semantic::metrics::RUNTIME_LIFECYCLE_TRANSITIONS_TOTAL;
pub use crate::semantic::metrics::RUNTIME_LONG_RUNNING_TASKS;
pub use crate::semantic::metrics::RUNTIME_TASKS;
pub use crate::semantic::metrics::RUNTIME_TASK_GROUPS;

#[cfg(any(feature = "otel-metrics", test))]
use rocketmq_runtime::RuntimeBlockingLaneV1;
use rocketmq_runtime::RuntimeComponent;
use rocketmq_runtime::RuntimeDiagnosticsViewV1;
#[cfg(any(feature = "otel-metrics", test))]
use rocketmq_runtime::RuntimeTaskKindV1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeLifecycleState {
    Starting,
    Ready,
    Stopping,
    Stopped,
    Failed,
}

impl RuntimeLifecycleState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeLifecycleReason {
    Startup,
    ShutdownRequest,
    ShutdownComplete,
    Timeout,
    Internal,
}

impl RuntimeLifecycleReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::ShutdownRequest => "shutdown_request",
            Self::ShutdownComplete => "shutdown_complete",
            Self::Timeout => "timeout",
            Self::Internal => "internal",
        }
    }
}

/// Instance-owned runtime diagnostics and lifecycle metric recorder.
#[derive(Clone)]
pub struct RuntimeMetricsRecorder {
    component: RuntimeComponent,
    #[cfg(feature = "otel-metrics")]
    telemetry: crate::TelemetryRecorder,
    #[cfg(feature = "otel-metrics")]
    metrics: Option<RuntimeMetrics>,
}

impl RuntimeMetricsRecorder {
    /// Creates a no-op recorder that still emits the bounded lifecycle log contract.
    #[must_use]
    pub fn noop(component: RuntimeComponent) -> Self {
        Self::from_handle(&crate::TelemetryHandle::noop(), component)
    }

    /// Creates a recorder bound to the component's fixed injected meter.
    #[must_use]
    pub fn from_handle(handle: &crate::TelemetryHandle, component: RuntimeComponent) -> Self {
        #[cfg(feature = "otel-metrics")]
        {
            let telemetry = handle.child(crate::handle::RUNTIME_METER_SCOPE);
            let metrics = telemetry.meter().map(|meter| RuntimeMetrics::new(&meter));
            Self {
                component,
                telemetry,
                metrics,
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        {
            let _ = handle;
            Self { component }
        }
    }

    /// Records one caller-owned diagnostics snapshot.
    ///
    /// This method does not create a polling task. Services decide when to
    /// sample from lifecycle-owned work or an authenticated diagnostics request.
    pub fn record_snapshot(&self, view: &RuntimeDiagnosticsViewV1) {
        if view.component != self.component {
            return;
        }
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.record(view);
            }
        }
    }

    /// Records one bounded lifecycle transition and its structured log event.
    pub fn record_lifecycle(&self, state: RuntimeLifecycleState, reason: RuntimeLifecycleReason) {
        let result = if state == RuntimeLifecycleState::Failed {
            "failure"
        } else {
            "success"
        };
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.record_lifecycle(self.component, state, result, reason);
            }
        }

        tracing::info!(
            event = crate::semantic::events::RUNTIME_LIFECYCLE,
            component = component_name(self.component),
            state = state.as_str(),
            result,
            reason = reason.as_str(),
            "runtime lifecycle transition"
        );
    }
}

/// Compatibility helper for callers that have not yet injected a recorder.
///
/// This path never reads global telemetry state. It records no metrics; callers
/// should retain a [`RuntimeMetricsRecorder`] when metrics are required.
pub fn record_snapshot(view: &RuntimeDiagnosticsViewV1) {
    RuntimeMetricsRecorder::noop(view.component).record_snapshot(view);
}

/// Compatibility helper for lifecycle logging without global metric state.
pub fn record_lifecycle(component: RuntimeComponent, state: RuntimeLifecycleState, reason: RuntimeLifecycleReason) {
    RuntimeMetricsRecorder::noop(component).record_lifecycle(state, reason);
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
struct RuntimeMetrics {
    tasks: opentelemetry::metrics::Gauge<u64>,
    task_groups: opentelemetry::metrics::Gauge<u64>,
    long_running_tasks: opentelemetry::metrics::Gauge<u64>,
    blocking_queued: opentelemetry::metrics::Gauge<u64>,
    blocking_running: opentelemetry::metrics::Gauge<u64>,
    blocking_timeouts: opentelemetry::metrics::Gauge<u64>,
    lifecycle_transitions_total: opentelemetry::metrics::Counter<u64>,
}

#[cfg(feature = "otel-metrics")]
impl RuntimeMetrics {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            tasks: meter
                .u64_gauge(RUNTIME_TASKS)
                .with_description("Active runtime tasks grouped by bounded task kind")
                .with_unit("{task}")
                .build(),
            task_groups: meter
                .u64_gauge(RUNTIME_TASK_GROUPS)
                .with_description("Active runtime task groups")
                .with_unit("{group}")
                .build(),
            long_running_tasks: meter
                .u64_gauge(RUNTIME_LONG_RUNNING_TASKS)
                .with_description("Tasks exceeding the diagnostics long-running threshold")
                .with_unit("{task}")
                .build(),
            blocking_queued: meter
                .u64_gauge(RUNTIME_BLOCKING_QUEUED)
                .with_description("Blocking executor tasks waiting for a permit")
                .with_unit("{task}")
                .build(),
            blocking_running: meter
                .u64_gauge(RUNTIME_BLOCKING_RUNNING)
                .with_description("Blocking executor tasks currently running")
                .with_unit("{task}")
                .build(),
            blocking_timeouts: meter
                .u64_gauge(RUNTIME_BLOCKING_TIMEOUTS)
                .with_description("Blocking executor tasks still running after timeout")
                .with_unit("{task}")
                .build(),
            lifecycle_transitions_total: meter
                .u64_counter(RUNTIME_LIFECYCLE_TRANSITIONS_TOTAL)
                .with_description("Runtime startup, readiness, shutdown, and failure transitions")
                .with_unit("{transition}")
                .build(),
        }
    }

    fn record_lifecycle(
        &self,
        component: RuntimeComponent,
        state: RuntimeLifecycleState,
        result: &'static str,
        reason: RuntimeLifecycleReason,
    ) {
        self.lifecycle_transitions_total.add(
            1,
            &[
                opentelemetry::KeyValue::new(crate::semantic::labels::COMPONENT, component_name(component)),
                opentelemetry::KeyValue::new(crate::semantic::labels::STATE, state.as_str()),
                opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result),
                opentelemetry::KeyValue::new(crate::semantic::labels::REASON, reason.as_str()),
            ],
        );
    }

    fn record(&self, view: &RuntimeDiagnosticsViewV1) {
        let component = component_name(view.component);
        self.task_groups.record(
            usize_to_u64(view.task_group_count),
            &[opentelemetry::KeyValue::new(
                crate::semantic::labels::COMPONENT,
                component,
            )],
        );
        for kind in RUNTIME_TASK_KINDS {
            let task = view.task_kinds.iter().find(|summary| summary.kind == kind);
            let attributes = [
                opentelemetry::KeyValue::new(crate::semantic::labels::COMPONENT, component),
                opentelemetry::KeyValue::new(crate::semantic::labels::TASK_TYPE, task_kind_name(kind)),
            ];
            self.tasks
                .record(usize_to_u64(task.map_or(0, |summary| summary.active)), &attributes);
            self.long_running_tasks.record(
                usize_to_u64(task.map_or(0, |summary| summary.long_running)),
                &attributes,
            );
        }
        for lane_kind in RUNTIME_BLOCKING_LANES {
            let lane = view.blocking_lanes.iter().find(|summary| summary.lane == lane_kind);
            let attributes = [
                opentelemetry::KeyValue::new(crate::semantic::labels::COMPONENT, component),
                opentelemetry::KeyValue::new(crate::semantic::labels::BLOCKING_LANE, lane_name(lane_kind)),
            ];
            self.blocking_queued
                .record(usize_to_u64(lane.map_or(0, |summary| summary.queued)), &attributes);
            self.blocking_running
                .record(usize_to_u64(lane.map_or(0, |summary| summary.running)), &attributes);
            self.blocking_timeouts.record(
                usize_to_u64(lane.map_or(0, |summary| summary.timed_out_still_running)),
                &attributes,
            );
        }
    }
}

#[cfg(feature = "otel-metrics")]
const RUNTIME_TASK_KINDS: [RuntimeTaskKindV1; 7] = [
    RuntimeTaskKindV1::Service,
    RuntimeTaskKindV1::Worker,
    RuntimeTaskKindV1::ScheduledDriver,
    RuntimeTaskKindV1::ScheduledRun,
    RuntimeTaskKindV1::BlockingReaper,
    RuntimeTaskKindV1::Shutdown,
    RuntimeTaskKindV1::Other,
];

#[cfg(feature = "otel-metrics")]
const RUNTIME_BLOCKING_LANES: [RuntimeBlockingLaneV1; 3] = [
    RuntimeBlockingLaneV1::StorageIo,
    RuntimeBlockingLaneV1::MetadataIo,
    RuntimeBlockingLaneV1::CpuCrypto,
];

const fn component_name(component: RuntimeComponent) -> &'static str {
    match component {
        RuntimeComponent::Broker => "broker",
        RuntimeComponent::NameServer => "name_server",
        RuntimeComponent::Controller => "controller",
        RuntimeComponent::Proxy => "proxy",
        RuntimeComponent::Mcp => "mcp",
        RuntimeComponent::SreControlPlane => "sre_control_plane",
        RuntimeComponent::SreConnector => "sre_connector",
        RuntimeComponent::Other => "other",
    }
}

#[cfg(any(feature = "otel-metrics", test))]
const fn task_kind_name(kind: RuntimeTaskKindV1) -> &'static str {
    match kind {
        RuntimeTaskKindV1::Service => "service",
        RuntimeTaskKindV1::Worker => "worker",
        RuntimeTaskKindV1::ScheduledDriver => "scheduled_driver",
        RuntimeTaskKindV1::ScheduledRun => "scheduled_run",
        RuntimeTaskKindV1::BlockingReaper => "blocking_reaper",
        RuntimeTaskKindV1::Shutdown => "shutdown",
        RuntimeTaskKindV1::Other => "other",
    }
}

#[cfg(any(feature = "otel-metrics", test))]
const fn lane_name(lane: RuntimeBlockingLaneV1) -> &'static str {
    match lane {
        RuntimeBlockingLaneV1::StorageIo => "storage_io",
        RuntimeBlockingLaneV1::MetadataIo => "metadata_io",
        RuntimeBlockingLaneV1::CpuCrypto => "cpu_crypto",
    }
}

#[cfg(feature = "otel-metrics")]
fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_labels_are_bounded_enums() {
        RuntimeMetricsRecorder::noop(RuntimeComponent::Mcp)
            .record_lifecycle(RuntimeLifecycleState::Ready, RuntimeLifecycleReason::Startup);
        assert_eq!(component_name(RuntimeComponent::Other), "other");
        assert_eq!(task_kind_name(RuntimeTaskKindV1::ScheduledRun), "scheduled_run");
        assert_eq!(lane_name(RuntimeBlockingLaneV1::MetadataIo), "metadata_io");
    }

    #[test]
    fn source_has_no_process_global_meter_access() {
        let source = include_str!("runtime.rs");

        assert!(!source.contains("global::meter"));
        assert!(!source.contains("static RUNTIME_METRICS"));
    }
}
