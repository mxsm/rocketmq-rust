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
use serde_json::Value;

use crate::ActionPlanId;
use crate::AgentReadResult;
use crate::ClusterId;
use crate::DiagnosisRevisionId;
use crate::EvidenceId;
use crate::IncidentId;
use crate::PlanStepId;
use crate::TenantId;

mod age;

pub use age::action_evidence_valid_until;

pub const ACTION_READINESS_SCHEMA_VERSION: &str = "rocketmq-sre.action-readiness.v1";
pub const ACTION_PREPARATION_SCHEMA_VERSION: &str = "rocketmq-sre.action-preparation.v1";

/// A local, read-only assessment of one closed action candidate.
///
/// Missing candidate inputs may produce blocked facts while diagnosis-only
/// operations remain observable. Selecting a Plan requires both Plan selectors.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReadinessRequest {
    pub cluster_id: ClusterId,
    pub diagnosis_revision_id: DiagnosisRevisionId,
    pub action_id: String,
    pub descriptor_version: String,
    pub resource: String,
    pub parameters: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precondition_evidence_id: Option<EvidenceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<ActionPlanId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_step_id: Option<PlanStepId>,
}

/// Immutable evidence identity; a newer same-resource record cannot replace it.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReadinessEvidenceBinding {
    pub evidence_id: EvidenceId,
    pub content_hash: String,
}

/// Exact candidate and caller observed by the server; its digest grants no authority.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReadinessBinding {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub diagnosis_revision_id: DiagnosisRevisionId,
    pub latest_diagnosis_revision_id: Option<DiagnosisRevisionId>,
    pub action_id: String,
    pub descriptor_version: String,
    /// Exact execution-target literal, without rewriting existing Plan aliases.
    pub resource: String,
    /// Present only after exact schema and forbidden-argument validation.
    pub parameters: Option<Value>,
    pub parameters_digest: Option<String>,
    pub target_scope_digest: Option<String>,
    pub diagnostic_evidence: Vec<ActionReadinessEvidenceBinding>,
    pub precondition_evidence: Option<ActionReadinessEvidenceBinding>,
    pub precondition_hash: Option<String>,
    pub policy_version: String,
    pub actor_subject: String,
    pub plan_id: Option<ActionPlanId>,
    pub plan_step_id: Option<PlanStepId>,
    pub plan_hash: Option<String>,
    pub binding_digest: String,
}

/// Closed explanation codes. Internal source text must not be placed in this view.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionReadinessReason {
    ActionUnknown,
    DescriptorVersionMismatch,
    ManualOnly,
    PlanOnly,
    ParametersInvalid,
    ResourceRequired,
    ResourceInvalid,
    OperatorRequired,
    DiagnosisNotConfirmed,
    DiagnosisAlreadyConfirmed,
    DiagnosisNotCurrent,
    IncidentInactive,
    DiagnosisIncomplete,
    PrimaryModelLineageInvalid,
    ExecutorNotConfigured,
    HandlerNotObserved,
    HandlerReadObserved,
    RuntimeEnablementNotObserved,
    TargetSupportNotObserved,
    TargetScopeUnverified,
    TargetScopeChanged,
    TargetPreconditionNotReady,
    TargetUnsupported,
    EvidenceMissing,
    EvidenceStale,
    EvidenceInvalid,
    EvidenceScopeMismatch,
    PreparationBindingMissing,
    PreparationBindingMismatch,
    PreparationOriginInvalid,
    PreparationExpired,
    PolicyDenied,
    MaintenanceWindowClosed,
    ResourceQuarantined,
    ResourceBusy,
    CompensationUnavailable,
    CompensationNotObserved,
    PlanNotCreated,
    ApprovalRequired,
    ApprovalExpired,
    ApprovalBindingMismatch,
    SelfApprovalForbidden,
    ActionTargetQualificationUnavailable,
    ObservationAvailable,
    PlanExpired,
    PlanBindingMismatch,
    PlanCompleted,
    PlanImmutable,
    ReadinessExpired,
    SourceUnavailable,
}

/// Whether a particular proof is present, missing, or inapplicable.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionReadinessState {
    Satisfied,
    Blocked,
    Unknown,
    NotApplicable,
}

/// The actual kind of source used for a stage; registration is not qualification.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionReadinessSource {
    DescriptorCatalog,
    ExecutorConfiguration,
    TargetScopeConfiguration,
    StoredPreparation,
    CurrentDiagnosis,
    PolicyEvaluation,
    StoredApproval,
    QualificationUnavailable,
    NotObserved,
}

/// A bounded fact derived from actual catalog, runtime or immutable record data.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReadinessFact {
    pub state: ActionReadinessState,
    pub reason_codes: Vec<ActionReadinessReason>,
    pub source: ActionReadinessSource,
    pub evidence_ids: Vec<EvidenceId>,
    pub observed_at: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
}

/// Fixed stage names prevent omitted or duplicated readiness gates.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReadinessStages {
    pub descriptor: ActionReadinessFact,
    pub handler: ActionReadinessFact,
    pub enabled: ActionReadinessFact,
    pub target: ActionReadinessFact,
    pub evidence: ActionReadinessFact,
    pub policy: ActionReadinessFact,
    pub approval: ActionReadinessFact,
    pub qualification: ActionReadinessFact,
    pub compensation: ActionReadinessFact,
}

/// A diagnosis-basis fact, separate from action approval.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReadinessDiagnosis {
    pub is_current: bool,
    pub is_confirmed: bool,
    pub is_complete: bool,
    pub primary_model_lineage_valid: bool,
    pub reason_codes: Vec<ActionReadinessReason>,
}

/// Fresh server availability of one preparation operation; never a bearer grant.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReadinessOperation {
    pub allowed: bool,
    pub reason_codes: Vec<ActionReadinessReason>,
}

/// Draft creation and readonly preparation do not require dispatch qualification.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReadinessOperations {
    pub confirm_diagnosis: ActionReadinessOperation,
    pub prepare_precondition: ActionReadinessOperation,
    pub create_plan: ActionReadinessOperation,
}

/// Summary of execution gates. Satisfied still requires actual dispatch authorization.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionExecutionReadiness {
    Blocked,
    Unknown,
    AwaitingApproval,
    Satisfied,
}

/// Ephemeral, non-authorizing projection for one exact action candidate.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionReadinessView {
    pub schema_version: String,
    pub binding: ActionReadinessBinding,
    pub diagnosis: ActionReadinessDiagnosis,
    pub stages: ActionReadinessStages,
    pub execution_readiness: ActionExecutionReadiness,
    pub operations: ActionReadinessOperations,
    pub observed_at: DateTime<Utc>,
    /// Null cannot enable an operation; refreshing does not renew immutable proof.
    pub valid_until: Option<DateTime<Utc>>,
}

/// CP record of the exact readonly request which produced a prepared observation.
///
/// This is not an Agent signature or action consent. The immutable Evidence hash
/// binds it together with the unchanged Agent response.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionPreparationBinding {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub incident_id: IncidentId,
    pub diagnosis_revision_id: DiagnosisRevisionId,
    pub action_id: String,
    pub descriptor_version: String,
    pub resource: String,
    pub parameters_digest: String,
    pub target_scope_digest: String,
    pub diagnostic_evidence: Vec<ActionReadinessEvidenceBinding>,
    pub policy_version: String,
    pub actor_subject: String,
    pub prepared_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
}

/// Versioned CP-owned evidence content; the Agent's wire DTO remains unchanged.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionPreparationEvidence {
    pub schema_version: String,
    pub evidence_id: EvidenceId,
    pub candidate_binding: ActionPreparationBinding,
    pub agent_result: AgentReadResult,
    /// CP origin attestation under a dedicated purpose, never dispatch authority.
    pub origin_signature: String,
}
