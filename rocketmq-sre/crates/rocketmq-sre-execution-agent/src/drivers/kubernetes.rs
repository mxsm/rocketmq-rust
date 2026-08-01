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

use rocketmq_admin_core::core::proxy::ProxyDrainState;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use serde::Serialize;

use super::DriverFuture;

/// Sanitized Deployment state required by the one-replica scale action.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProxyScaleState {
    pub desired_replicas: u32,
    pub ready_replicas: u32,
    pub unavailable_replicas: u32,
    pub quota_available: bool,
    pub capacity_available: bool,
    pub pdb_healthy: bool,
    pub last_operation_id: Option<String>,
}
/// Closed scale-out request. The target is always exactly `expected + 1`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyScaleOutOneWrite {
    pub namespace: String,
    pub workload: String,
    pub expected_replicas: u32,
    pub target_replicas: u32,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Closed restoration request for a prior one-replica scale-out.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyScaleRestore {
    pub namespace: String,
    pub workload: String,
    pub original_replicas: u32,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Exact Kubernetes operations available to `proxy.scale_out_one.v1`.
///
/// Implementations must compare `expected_replicas` with the live workload
/// resource version before scaling. Restoration is valid only for the same
/// execution step and only from the recorded `original + 1` state.
pub trait ProxyScaleClient: Send + Sync {
    fn proxy_scale_state<'a>(&'a self, namespace: &'a str, workload: &'a str) -> DriverFuture<'a, ProxyScaleState>;

    fn scale_out_one<'a>(&'a self, request: &'a ProxyScaleOutOneWrite) -> DriverFuture<'a, ()>;

    fn restore_proxy_replicas<'a>(&'a self, request: &'a ProxyScaleRestore) -> DriverFuture<'a, ()>;
}

/// Sanitized pod, drain, and verification state for one Proxy restart.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProxyRestartState {
    pub drain_supported: bool,
    pub pod_uid: String,
    pub pod_ready: bool,
    pub remaining_replicas_healthy: bool,
    pub replacement_ready: bool,
    pub synthetic_path_healthy: bool,
    pub slo_healthy: bool,
    pub active_operation_id: Option<String>,
    pub last_operation_id: Option<String>,
    pub drain: Option<ProxyDrainState>,
}

/// Closed drain-and-restart request for exactly one expected pod UID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyRestartOneWrite {
    pub namespace: String,
    pub pod: String,
    pub expected_uid: String,
    pub drain_timeout_seconds: u32,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Closed cancellation/restoration request for an interrupted restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyRestartRestore {
    pub namespace: String,
    pub pod: String,
    pub expected_uid: String,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Result of the bounded restart compensation primitive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyRestartRestoreOutcome {
    IngressRestored,
    ManualTakeoverRequired,
}

/// Exact Kubernetes/Proxy orchestration available to `proxy.restart_one.v1`.
///
/// `restart_one_drained` must authenticate the Proxy drain endpoint, begin
/// drain, stop admission/readiness/routing, wait until every pending counter is
/// exactly zero, and only then issue one typed pod restart. Timeout before the
/// restart must cancel drain and restore ingress. It must never force-delete a
/// pod or restart more than the expected UID.
pub trait ProxyRestartClient: Send + Sync {
    fn proxy_restart_state<'a>(&'a self, namespace: &'a str, pod: &'a str) -> DriverFuture<'a, ProxyRestartState>;

    fn restart_one_drained<'a>(&'a self, request: &'a ProxyRestartOneWrite) -> DriverFuture<'a, ()>;

    fn cancel_restart_and_restore<'a>(
        &'a self,
        request: &'a ProxyRestartRestore,
    ) -> DriverFuture<'a, ProxyRestartRestoreOutcome>;
}

/// Sanitized Deployment state for the digest-only one-canary rollout.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProxyImageCanaryState {
    pub generation: u64,
    pub observed_generation: u64,
    pub image_digest: String,
    pub ready_canary_replicas: u32,
    pub old_replicas_unchanged: bool,
    pub pdb_healthy: bool,
    pub slo_healthy: bool,
    pub last_operation_id: Option<String>,
}

/// Closed digest-only rollout request for exactly one Proxy canary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyImageCanaryWrite {
    pub namespace: String,
    pub workload: String,
    pub container: String,
    pub expected_generation: u64,
    pub image_digest: String,
    pub canary_replicas: u32,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Closed restoration request bound to the rollout's durable before snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyImageCanaryRestore {
    pub namespace: String,
    pub workload: String,
    pub container: String,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Exact Kubernetes operations available to
/// `proxy.rollout_image_canary.v1`.
///
/// Implementations must patch only the named container image to a digest,
/// stage exactly one canary, retain the previous digest durably, and compare
/// the live Deployment generation. Generic patches, tags, environment
/// changes, and secret mutations are outside this boundary.
pub trait ProxyImageCanaryClient: Send + Sync {
    fn proxy_image_canary_state<'a>(
        &'a self,
        namespace: &'a str,
        workload: &'a str,
        container: &'a str,
    ) -> DriverFuture<'a, ProxyImageCanaryState>;

    fn rollout_proxy_image_canary<'a>(&'a self, request: &'a ProxyImageCanaryWrite) -> DriverFuture<'a, ()>;

    fn restore_proxy_image<'a>(&'a self, request: &'a ProxyImageCanaryRestore) -> DriverFuture<'a, ()>;
}

/// Sanitized state for one allowlisted OpenTelemetry Collector restart.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TelemetryCollectorRestartState {
    pub pod_uid: String,
    pub pod_ready: bool,
    pub deployment_ready: bool,
    pub replacement_ready: bool,
    pub exporter_connected: bool,
    pub queue_healthy: bool,
    pub data_plane_unaffected: bool,
    pub active_pod: String,
    pub last_operation_id: Option<String>,
    pub last_execution_id: Option<String>,
    pub last_plan_step_id: Option<String>,
}

/// Closed restart request for exactly one allowlisted Collector pod UID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelemetryCollectorRestartOneWrite {
    pub namespace: String,
    pub pod: String,
    pub expected_uid: String,
    pub pipeline: String,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Exact Kubernetes operation available to
/// `telemetry.collector.restart_one.v1`.
///
/// Implementations must bind the request to one allowlisted Deployment and
/// current pod UID, change only the pod-template restart annotations, and
/// observe a Ready replacement. Force deletion, configuration changes,
/// exporter credentials, and multi-workload mutations are outside this
/// boundary.
pub trait TelemetryCollectorRestartClient: Send + Sync {
    fn telemetry_collector_restart_state<'a>(
        &'a self,
        namespace: &'a str,
        pod: &'a str,
        pipeline: &'a str,
    ) -> DriverFuture<'a, TelemetryCollectorRestartState>;

    fn restart_one_telemetry_collector<'a>(
        &'a self,
        request: &'a TelemetryCollectorRestartOneWrite,
    ) -> DriverFuture<'a, ()>;
}
