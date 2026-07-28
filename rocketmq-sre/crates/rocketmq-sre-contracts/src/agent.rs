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

use std::collections::BTreeMap;

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::ActionPlanId;
use crate::AgentStepRequest;
use crate::AgentStepResult;
use crate::ClusterId;
use crate::ExecutionAction;
use crate::ExecutionId;
use crate::FenceAck;
use crate::PlanStepId;
use crate::ReconcileGrant;
use crate::TenantId;

pub const EXECUTION_AGENT_SCHEMA_VERSION: &str = "rocketmq-sre.execution-agent.v1";

/// Narrow read-side request used for precheck and live-state reconciliation.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentReadRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
    pub action: ExecutionAction,
    pub descriptor_version: String,
    pub target: String,
    pub parameters: Value,
}

/// Sanitized live-state result. It never contains target configuration dumps.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentReadResult {
    pub schema_version: String,
    pub action: ExecutionAction,
    pub target: String,
    pub precondition_hash: String,
    pub ready: bool,
    pub reason_codes: Vec<String>,
    #[serde(default)]
    pub resource_conditions: BTreeMap<String, bool>,
    pub observed_at: DateTime<Utc>,
}

/// Exclusive authorization path used for one Agent dispatch.
#[derive(Clone, Copy, Debug, Default, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentDispatchAuthorization {
    #[default]
    HumanApproved,
    Autonomous,
}

/// Exact typed dispatch request accepted only from Executor workload identity.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDispatchRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<ActionPlanId>,
    #[serde(default)]
    pub authorization: AgentDispatchAuthorization,
    pub request: AgentStepRequest,
}

/// Dispatch response including whether an idempotent prior result was reused.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDispatchResponse {
    pub schema_version: String,
    pub result: AgentStepResult,
    pub replayed: bool,
}

/// Pending-epoch request to obtain an Agent-signed `FenceAck`.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvanceFenceRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub reconcile_grant: ReconcileGrant,
}

/// Successful fence advancement response.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdvanceFenceResponse {
    pub schema_version: String,
    pub fence_ack: FenceAck,
}

/// Read-only old-effect reconciliation request.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileEffectRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub reconcile_grant: ReconcileGrant,
    pub idempotency_key: String,
}

/// Closed reconciliation outcome; `unknown` never causes a redispatch.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileEffectState {
    Applied,
    NotApplied,
    Failed,
    Unknown,
}

/// Durable reconciliation response.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileEffectResponse {
    pub schema_version: String,
    pub state: ReconcileEffectState,
    pub outcome_code: String,
    pub sanitized_summary: String,
    pub observed_at: DateTime<Utc>,
}

/// Bounded service capabilities used by readiness and deployment smoke tests.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAgentCapabilities {
    pub schema_version: String,
    pub registered_actions: Vec<ExecutionAction>,
    pub raw_admin_request_supported: bool,
    pub arbitrary_json_patch_supported: bool,
    pub shell_supported: bool,
    pub durable_fencing: bool,
}
