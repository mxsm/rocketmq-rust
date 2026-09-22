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
pub use crate::semantic::metrics::RUNTIME_BUSINESS_DRAINS_TOTAL;
pub use crate::semantic::metrics::RUNTIME_LIFECYCLE_TRANSITIONS_TOTAL;
pub use crate::semantic::metrics::RUNTIME_LONG_RUNNING_TASKS;
pub use crate::semantic::metrics::RUNTIME_OPERATION_OUTCOMES_TOTAL;
pub use crate::semantic::metrics::RUNTIME_TASKS;
pub use crate::semantic::metrics::RUNTIME_TASK_GROUPS;

#[cfg(any(feature = "otel-metrics", test))]
use rocketmq_runtime::RuntimeBlockingLaneV1;
use rocketmq_runtime::RuntimeComponent;
use rocketmq_runtime::RuntimeDiagnosticsViewV1;
use rocketmq_runtime::RuntimeDiagnosticsViewV2;
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

/// Business drain evidence recorded before telemetry finalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeBusinessDrainOutcome {
    Drained,
    Failed,
    DeadlineExceeded,
}

impl RuntimeBusinessDrainOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Drained => "drained",
            Self::Failed => "failed",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
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

impl std::fmt::Debug for RuntimeMetricsRecorder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeMetricsRecorder")
            .field("component", &self.component)
            .finish_non_exhaustive()
    }
}

impl rocketmq_runtime::OperationOutcomeObserver for RuntimeMetricsRecorder {
    fn on_outcome(&self, kind: rocketmq_runtime::TaskKind, outcome: rocketmq_runtime::OperationOutcome) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                use rocketmq_runtime::TaskKind;
                let kind = match kind {
                    TaskKind::Service => "service",
                    TaskKind::Worker => "worker",
                    TaskKind::ScheduledDriver => "scheduled_driver",
                    TaskKind::ScheduledRun => "scheduled_run",
                    TaskKind::BlockingReaper => "blocking_reaper",
                    TaskKind::Shutdown => "shutdown",
                    TaskKind::Other => "other",
                };
                metrics.operation_outcomes_total.add(
                    1,
                    &[
                        opentelemetry::KeyValue::new(
                            crate::semantic::labels::COMPONENT,
                            component_name(self.component),
                        ),
                        opentelemetry::KeyValue::new(crate::semantic::labels::TASK_TYPE, kind),
                        opentelemetry::KeyValue::new(crate::semantic::labels::OUTCOME, outcome.as_str()),
                    ],
                );
            }
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (kind, outcome);
    }
}

impl rocketmq_runtime::ServiceLifecycleObserver for RuntimeMetricsRecorder {
    fn on_transition(&self, transition: rocketmq_runtime::ServiceLifecycleTransition) {
        use rocketmq_runtime::ServiceLifecycleState;
        let (state, reason) = match transition.to {
            ServiceLifecycleState::Starting => (RuntimeLifecycleState::Starting, RuntimeLifecycleReason::Startup),
            ServiceLifecycleState::Ready => (RuntimeLifecycleState::Ready, RuntimeLifecycleReason::Startup),
            ServiceLifecycleState::Draining => {
                (RuntimeLifecycleState::Stopping, RuntimeLifecycleReason::ShutdownRequest)
            }
            ServiceLifecycleState::Stopped => {
                (RuntimeLifecycleState::Stopped, RuntimeLifecycleReason::ShutdownComplete)
            }
            ServiceLifecycleState::Failed => (RuntimeLifecycleState::Failed, RuntimeLifecycleReason::Internal),
        };
        self.record_lifecycle(state, reason);
    }
}

impl RuntimeMetricsRecorder {
    /// Records business drain completion without declaring telemetry or the process stopped.
    ///
    /// The caller emits this once at the business shutdown boundary, before
    /// consuming its telemetry guard. Deadline failure never extends that deadline.
    pub fn record_business_drain(&self, outcome: RuntimeBusinessDrainOutcome) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.business_drains_total.add(
                    1,
                    &[
                        opentelemetry::KeyValue::new(
                            crate::semantic::labels::COMPONENT,
                            component_name(self.component),
                        ),
                        opentelemetry::KeyValue::new(crate::semantic::labels::OUTCOME, outcome.as_str()),
                    ],
                );
            }
        }
        tracing::info!(
            event = crate::semantic::events::RUNTIME_BUSINESS_DRAIN,
            component = component_name(self.component),
            outcome = outcome.as_str(),
            "runtime business drain completed"
        );
    }
    pub(crate) fn is_enabled(&self) -> bool {
        #[cfg(feature = "otel-metrics")]
        {
            self.telemetry.is_active() && self.metrics.is_some()
        }
        #[cfg(not(feature = "otel-metrics"))]
        {
            false
        }
    }

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
        #[cfg(feature = "otel-metrics")]
        {
            if view.component != self.component {
                return;
            }
            if self.telemetry.is_active() {
                if let Some(metrics) = &self.metrics {
                    metrics.record(view);
                }
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = view;
    }

    /// Records task and blocking aggregates from an already collected V2 view.
    ///
    /// Optional schedule, metadata and shutdown sections remain diagnostics
    /// data. This method performs no second runtime scan.
    pub fn record_snapshot_v2(&self, view: &RuntimeDiagnosticsViewV2) {
        #[cfg(feature = "otel-metrics")]
        if view.component == self.component && self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.record_parts(
                    view.component,
                    view.tasks.task_group_count,
                    &view.tasks.task_kinds,
                    &view.blocking.lanes,
                );
            }
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = view;
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
    operation_outcomes_total: opentelemetry::metrics::Counter<u64>,
    business_drains_total: opentelemetry::metrics::Counter<u64>,
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
            operation_outcomes_total: meter
                .u64_counter(RUNTIME_OPERATION_OUTCOMES_TOTAL)
                .with_description(
                    "Accepted operation task outcomes after future destruction; completion is not business success",
                )
                .with_unit("{task}")
                .build(),
            business_drains_total: meter
                .u64_counter(RUNTIME_BUSINESS_DRAINS_TOTAL)
                .with_description("Business shutdown outcomes before telemetry finalization")
                .with_unit("{drain}")
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
        self.record_parts(
            view.component,
            view.task_group_count,
            &view.task_kinds,
            &view.blocking_lanes,
        );
    }

    fn record_parts(
        &self,
        component: RuntimeComponent,
        task_group_count: usize,
        task_kinds: &[rocketmq_runtime::RuntimeTaskKindSummaryV1],
        blocking_lanes: &[rocketmq_runtime::RuntimeBlockingLaneSummaryV1],
    ) {
        let component = component_name(component);
        self.task_groups.record(
            usize_to_u64(task_group_count),
            &[opentelemetry::KeyValue::new(
                crate::semantic::labels::COMPONENT,
                component,
            )],
        );
        for kind in RUNTIME_TASK_KINDS {
            let task = task_kinds.iter().find(|summary| summary.kind == kind);
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
            let lane = blocking_lanes.iter().find(|summary| summary.lane == lane_kind);
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

    #[cfg(feature = "prometheus")]
    #[tokio::test]
    async fn real_lifecycle_and_operation_events_are_exported_before_finalization() {
        use prometheus::Encoder;
        use rocketmq_runtime::{
            OperationContext, RuntimeContext, ServiceLifecycle, ServiceLifecycleConfig, ShutdownReason, TaskKind,
        };
        use std::sync::Arc;
        use std::time::Duration;

        let mut config = crate::ObservabilityConfig {
            enabled: true,
            ..Default::default()
        };
        config.metrics.enabled = true;
        let (provider, registry) = crate::exporter::prometheus::init_prometheus_metrics(&config)
            .unwrap()
            .into_parts();
        let handle = crate::TelemetryHandle::active(&config, Some(&provider));
        let recorder = Arc::new(RuntimeMetricsRecorder::from_handle(&handle, RuntimeComponent::Broker));
        let lifecycle = ServiceLifecycle::new(ServiceLifecycleConfig {
            service_name: Arc::from("private-service"),
            probe_bind_addr: None,
            shutdown_timeout: Duration::from_secs(1),
            liveness_stale_after: Duration::from_secs(3),
        });
        lifecycle.set_observer(recorder.clone()).unwrap();
        lifecycle.mark_ready().unwrap();
        lifecycle.mark_ready().unwrap();
        let runtime = RuntimeContext::from_current("event-export");
        let owner = runtime.service_context("event-owner");
        let operation = OperationContext::without_deadline(TaskKind::Worker);
        operation.set_outcome_observer(recorder.clone()).unwrap();
        owner
            .task_group()
            .spawn_operation(&operation, "private-task", async {})
            .unwrap();
        while owner.task_group().task_count() != 0 {
            tokio::task::yield_now().await;
        }
        lifecycle.request_shutdown(ShutdownReason::Internal);
        lifecycle.request_shutdown(ShutdownReason::Signal);
        assert!(runtime.shutdown_tasks(Duration::from_secs(1)).await.is_healthy());
        recorder.record_business_drain(RuntimeBusinessDrainOutcome::Drained);
        let mut output = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&registry.gather(), &mut output)
            .unwrap();
        let output = String::from_utf8(output).unwrap();
        let samples: Vec<_> = output.lines().filter(|line| !line.starts_with('#')).collect();
        let transitions: Vec<_> = samples
            .iter()
            .filter(|line| line.starts_with(RUNTIME_LIFECYCLE_TRANSITIONS_TOTAL))
            .collect();
        assert_eq!(transitions.len(), 3, "{output}");
        assert!(transitions.iter().all(|line| line.ends_with(" 1")), "{output}");
        for state in ["starting", "ready", "stopping"] {
            assert!(
                transitions
                    .iter()
                    .any(|line| line.contains(&format!("state=\"{state}\""))),
                "{output}"
            );
        }
        assert!(
            samples
                .iter()
                .any(|line| line.starts_with(RUNTIME_OPERATION_OUTCOMES_TOTAL)
                    && line.contains("outcome=\"completed\"")
                    && line.ends_with(" 1")),
            "{output}"
        );
        assert!(
            samples
                .iter()
                .any(|line| line.starts_with(RUNTIME_BUSINESS_DRAINS_TOTAL)
                    && line.contains("outcome=\"drained\"")
                    && line.ends_with(" 1")),
            "{output}"
        );
        assert!(!output.contains("private-task"));
        assert!(!output.contains("private-service"));
        provider.shutdown().unwrap();
        lifecycle.mark_stopped();
        assert_eq!(lifecycle.state(), rocketmq_runtime::ServiceLifecycleState::Stopped);
    }

    #[test]
    fn lifecycle_labels_are_bounded_enums() {
        RuntimeMetricsRecorder::noop(RuntimeComponent::Mcp)
            .record_lifecycle(RuntimeLifecycleState::Ready, RuntimeLifecycleReason::Startup);
        assert_eq!(component_name(RuntimeComponent::Other), "other");
        assert_eq!(task_kind_name(RuntimeTaskKindV1::ScheduledRun), "scheduled_run");
        assert_eq!(lane_name(RuntimeBlockingLaneV1::MetadataIo), "metadata_io");
    }

    #[cfg(feature = "prometheus")]
    #[tokio::test]
    async fn startup_and_critical_failures_export_one_terminal_transition() {
        use rocketmq_runtime::{
            CriticalFailureRecovery, CriticalFailureState, RuntimeContext, ServiceLifecycle, ServiceLifecycleConfig,
            ShutdownReason,
        };
        use std::sync::Arc;
        use std::time::Duration;
        for critical in [false, true] {
            let mut config = crate::ObservabilityConfig {
                enabled: true,
                ..Default::default()
            };
            config.metrics.enabled = true;
            let (provider, registry) = crate::exporter::prometheus::init_prometheus_metrics(&config)
                .unwrap()
                .into_parts();
            let handle = crate::TelemetryHandle::active(&config, Some(&provider));
            let recorder = Arc::new(RuntimeMetricsRecorder::from_handle(&handle, RuntimeComponent::Broker));
            let lifecycle = ServiceLifecycle::new(ServiceLifecycleConfig {
                service_name: Arc::from("failure-probe"),
                probe_bind_addr: None,
                shutdown_timeout: Duration::from_secs(1),
                liveness_stale_after: Duration::from_secs(3),
            });
            lifecycle.set_observer(recorder.clone()).unwrap();
            if critical {
                lifecycle.mark_ready().unwrap();
                let runtime = RuntimeContext::from_current("critical-export");
                let failures = CriticalFailureState::new();
                let mut notifications = failures.subscribe(1).unwrap();
                runtime
                    .service_context("failed-service")
                    .spawn_critical_service("critical-task", failures, async {
                        panic!("injected critical failure");
                    })
                    .unwrap();
                let failure = notifications.recv().await.unwrap();
                lifecycle.handle_critical_failure(&failure, CriticalFailureRecovery::FailAndRequestShutdown);
                runtime.shutdown_tasks(Duration::from_secs(1)).await;
            } else {
                lifecycle.mark_failed();
            }
            lifecycle.mark_failed();
            lifecycle.request_shutdown(ShutdownReason::Internal);
            lifecycle.request_shutdown(ShutdownReason::Signal);
            recorder.record_business_drain(RuntimeBusinessDrainOutcome::Failed);
            let families = registry.gather();
            let transitions = families
                .iter()
                .find(|family| family.name() == RUNTIME_LIFECYCLE_TRANSITIONS_TOTAL)
                .unwrap();
            let failed = transitions
                .get_metric()
                .iter()
                .filter(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "state" && label.value() == "failed")
                })
                .collect::<Vec<_>>();
            assert_eq!(failed.len(), 1);
            assert_eq!(failed[0].get_counter().get_value(), 1.0);
            provider.shutdown().unwrap();
            lifecycle.mark_stopped();
            assert_eq!(lifecycle.state(), rocketmq_runtime::ServiceLifecycleState::Failed);
        }
    }

    #[test]
    fn source_has_no_process_global_meter_access() {
        let source = include_str!("runtime.rs");
        let global_meter = ["global", "::meter"].concat();
        let static_metrics = ["static RUNTIME", "_METRICS"].concat();

        assert!(!source.contains(&global_meter));
        assert!(!source.contains(&static_metrics));
    }
}
