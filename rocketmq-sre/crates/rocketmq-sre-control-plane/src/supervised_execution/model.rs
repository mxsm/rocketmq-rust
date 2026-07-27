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
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::ApprovalGrant;
use rocketmq_sre_contracts::ApprovalRecord;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ExecutionState;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::ManualRunbookDraft;
use rocketmq_sre_contracts::ModelInvocationId;
use rocketmq_sre_contracts::PolicyDecision;
use rocketmq_sre_contracts::ResourceQuarantine;
use rocketmq_sre_contracts::TenantId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreatePlanRequest {
    pub(crate) cluster_id: ClusterId,
    pub(crate) incident_id: IncidentId,
    pub(crate) diagnosis_revision_id: DiagnosisRevisionId,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) steps: Vec<CandidatePlanStep>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidatePlanStep {
    pub(crate) action_id: String,
    pub(crate) descriptor_version: String,
    pub(crate) resource: String,
    pub(crate) parameters: Value,
    pub(crate) evidence_ids: Vec<EvidenceId>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CreatePlanResponse {
    ActionPlan {
        plan: Box<ActionPlan>,
        risk: ActionRisk,
        policy_decision: PolicyDecision,
    },
    ManualRunbook {
        runbook: ManualRunbookDraft,
    },
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ActionPlanView {
    pub(crate) plan: ActionPlan,
    pub(crate) risk: ActionRisk,
    pub(crate) latest_policy_decision: Option<PolicyDecision>,
    pub(crate) latest_approval: Option<ApprovalRecord>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApprovalDecisionRequest {
    pub(crate) plan_hash: String,
    pub(crate) precondition_hash: String,
    pub(crate) reason: String,
    pub(crate) validity_seconds: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ApprovalDecisionResponse {
    pub(crate) plan: ActionPlan,
    pub(crate) approval: ApprovalRecord,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) grant: Option<ApprovalGrant>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SubmitExecutionRequest {
    pub(crate) plan_id: ActionPlanId,
    pub(crate) plan_hash: String,
    pub(crate) precondition_hash: String,
    pub(crate) idempotency_key: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ExecutionSubmissionView {
    pub(crate) execution: ExecutionRequest,
    pub(crate) state: ExecutionState,
    pub(crate) submitted_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AuditPage {
    pub(crate) schema_version: &'static str,
    pub(crate) correlation_id: CorrelationId,
    pub(crate) items: Vec<rocketmq_sre_contracts::AuditEvent>,
    pub(crate) partial: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct QuarantineListQuery {
    pub(crate) cluster_id: ClusterId,
    pub(crate) include_cleared: Option<bool>,
    pub(crate) limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct QuarantinePage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<ResourceQuarantine>,
    pub(crate) partial: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClearQuarantineRequest {
    pub(crate) reason: String,
    pub(crate) evidence_ids: Vec<EvidenceId>,
}

#[derive(Clone, Debug)]
pub(super) struct DiagnosisPlanContext {
    pub(super) tenant_id: TenantId,
    pub(super) cluster_id: ClusterId,
    pub(super) incident_id: IncidentId,
    pub(super) diagnosis_revision_id: DiagnosisRevisionId,
    pub(super) status: String,
    pub(super) evidence_ids: Vec<EvidenceId>,
    pub(super) primary_model_invocation_id: Option<ModelInvocationId>,
    pub(super) execution_eligible: bool,
    pub(super) partial: bool,
}

#[derive(Clone, Debug)]
pub(super) struct PersistedPlanProjection {
    pub(super) plan: ActionPlan,
    pub(super) risk: ActionRisk,
}

#[derive(Clone, Debug)]
pub(super) struct StoredExecutionProjection {
    pub(super) request: ExecutionRequest,
    pub(super) state: ExecutionState,
    pub(super) submitted_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct EvidenceBinding {
    pub(super) evidence_id: EvidenceId,
    pub(super) content_hash: String,
    pub(super) resource: String,
    pub(super) observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct PolicyInputDigest<'a> {
    pub(super) plan_hash: &'a str,
    pub(super) subject: &'a str,
    pub(super) roles: &'a std::collections::BTreeSet<String>,
    pub(super) tenant_id: TenantId,
    pub(super) cluster_id: ClusterId,
    pub(super) diagnosis_confirmed: bool,
    pub(super) diagnosis_execution_eligible: bool,
    pub(super) evidence_current: bool,
    pub(super) resource_quarantined: bool,
    pub(super) resource_busy: bool,
    pub(super) maintenance_window_open: bool,
    pub(super) rollback_available: bool,
    pub(super) risks: &'a [ActionRisk],
}

#[derive(Clone, Debug)]
pub(super) struct NewExecutionProjection {
    pub(super) id: ExecutionId,
    pub(super) tenant_id: TenantId,
    pub(super) cluster_id: ClusterId,
    pub(super) correlation_id: CorrelationId,
    pub(super) resource_key: String,
    pub(super) action_id: String,
    pub(super) request: ExecutionRequest,
}
