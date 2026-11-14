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

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    ClusterId, InvestigationBudget, InvestigationBudgetCost, InvestigationControlEpoch, InvestigationId,
    InvestigationPackRef, InvestigationResource, InvestigationRevision, InvestigationRun, InvestigationRunId,
    InvestigationRunScope, InvestigationRunStatus, InvestigationStopReason, TenantId,
};

/// Bounded discovery metadata; list pages do not repeat step or Evidence payloads.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunSummary {
    pub id: InvestigationRunId,
    pub initial_scope: InvestigationRunScope,
    pub effective_scope: InvestigationRunScope,
    pub effective_packs: Vec<InvestigationPackRef>,
    pub status: InvestigationRunStatus,
    pub revision: InvestigationRevision,
    pub control_epoch: InvestigationControlEpoch,
    pub deadline: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<InvestigationStopReason>,
}

impl From<&InvestigationRun> for InvestigationRunSummary {
    fn from(run: &InvestigationRun) -> Self {
        Self {
            id: run.id,
            initial_scope: run.initial_scope.clone(),
            effective_scope: run.effective_scope.clone(),
            effective_packs: run.effective_packs.clone(),
            status: run.status,
            revision: run.revision,
            control_epoch: run.control_epoch,
            deadline: run.deadline,
            created_at: run.created_at,
            updated_at: run.updated_at,
            stop_reason: run.stop_reason,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationRunCommandKind {
    Pause,
    Resume,
    Cancel,
    Steer,
}

/// A sanitized reason for a current caller's read-only affordance projection.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationRunControlBlockReason {
    ControlPermissionRequired,
    Terminal,
    StateNotEligible,
    DeadlineExceeded,
    BudgetExhausted,
    CounterExhausted,
    PackUnavailable,
    ScopeUnavailable,
    UnsupportedResource,
}

#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunCommandAvailability {
    pub kind: InvestigationRunCommandKind,
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<InvestigationRunControlBlockReason>,
}

/// Current caller policy, never persisted into an investigation or run snapshot.
/// Availability is advisory; mutations authenticate and validate their body again.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunCreationOptions {
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<InvestigationRunControlBlockReason>,
    pub parent_resource: Option<InvestigationResource>,
    pub compiled_packs: Vec<InvestigationPackRef>,
    pub default_budget: InvestigationBudget,
    pub maximum_budget: InvestigationBudget,
    pub default_duration_seconds: u32,
    pub max_duration_seconds: u32,
    pub max_window_seconds: u32,
}

#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunListPage {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub investigation_id: InvestigationId,
    pub observed_at: DateTime<Utc>,
    pub items: Vec<InvestigationRunSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub creation: InvestigationRunCreationOptions,
}

/// Calls remain charged by admission even when their actual result reports zero.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunBudgetUsage {
    pub settled: InvestigationBudgetCost,
    pub reserved: InvestigationBudgetCost,
}

/// The complete snapshot and affordances refer to the same revision and epoch.
/// An event or command receipt must trigger a refresh, not overwrite this snapshot.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunControls {
    pub schema_version: String,
    pub run: InvestigationRun,
    pub observed_at: DateTime<Utc>,
    pub commands: Vec<InvestigationRunCommandAvailability>,
    /// Current control authority to retry a retained original request, even when
    /// its new-command kind is no longer available. Exact body/actor binding and
    /// new-versus-replay admission are still checked by the command endpoint.
    /// Older responses without this field fail closed.
    #[serde(default)]
    pub receipt_replay_allowed: bool,
    pub budget_usage: InvestigationRunBudgetUsage,
}
