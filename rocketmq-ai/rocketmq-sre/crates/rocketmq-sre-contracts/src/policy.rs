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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ActionPlanId;
use crate::ClusterId;
use crate::PolicyDecisionId;
use crate::TenantId;

/// Deterministic effect produced by the versioned Rust policy evaluator.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyEffect {
    Allow,
    Deny,
    RequireApproval,
}

/// Immutable policy evaluation bound to one exact plan and input digest.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyDecision {
    pub id: PolicyDecisionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub policy_version: String,
    pub input_hash: String,
    pub effect: PolicyEffect,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    pub evaluated_by: String,
    pub evaluated_at: DateTime<Utc>,
}
