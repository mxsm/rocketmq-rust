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

use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use serde::Serialize;

use super::AgentActionHandler;
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

/// Typed Kubernetes scale/restart/rollout adapter.
///
/// Implementations use concrete Kubernetes API types and never accept an
/// arbitrary JSON Patch document.
pub trait KubernetesDriver: AgentActionHandler {}
