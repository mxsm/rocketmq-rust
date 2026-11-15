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

use crate::ClusterId;
use crate::CorrelationId;
use crate::EvidenceId;
use crate::ExecutionAction;
use crate::ExecutionId;
use crate::ExecutionObservationReason;
use crate::ExecutionStepId;
use crate::PlanStepId;
use crate::TenantId;
use crate::VerificationWindowProof;

pub const EXECUTION_VERIFICATION_SCHEMA_VERSION: &str = "rocketmq-sre.execution-verification.v1";

/// Bounded request for independently evaluated technical SLI conditions.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSliQuery {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub correlation_id: CorrelationId,
    pub conditions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<ExecutionSliBinding>,
}

/// Exact execution selector; it does not create a metric source or authority.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSliBinding {
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
    pub action: ExecutionAction,
    pub descriptor_version: String,
    pub target: String,
    pub parameters_digest: String,
}

/// Actual source clock and scope, kept separate from report evaluation time.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSliSourceFacts {
    pub binding: Option<ExecutionSliBinding>,
    pub source_observed_at: Option<DateTime<Utc>>,
    pub collected_at: DateTime<Utc>,
    pub exact_target_conditions: bool,
    pub reason_codes: Vec<ExecutionObservationReason>,
}

/// Deterministic technical-SLI projection returned to the Executor.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionSliObservation {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub correlation_id: CorrelationId,
    pub conditions: BTreeMap<String, bool>,
    pub complete: bool,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
    pub observed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_facts: Option<ExecutionSliSourceFacts>,
}

/// Deterministic verification outcome.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationOutcome {
    Succeeded,
    Failed,
    Inconclusive,
}

/// Bounded verification result with before/after evidence.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationResult {
    pub step_id: ExecutionStepId,
    pub outcome: VerificationOutcome,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    #[serde(default)]
    pub pre_evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub during_evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub post_evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub satisfied_conditions: Vec<String>,
    #[serde(default)]
    pub failed_conditions: Vec<String>,
    pub stable_window_seconds: u64,
    /// Old saved results omit this field unchanged. Present invalid data must
    /// be reported invalid rather than treated as old unqualified history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_proof: Option<VerificationWindowProof>,
}
