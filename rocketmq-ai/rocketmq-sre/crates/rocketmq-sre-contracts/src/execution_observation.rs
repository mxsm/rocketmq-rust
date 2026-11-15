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
use crate::CorrelationId;
use crate::DiagnosisRevisionId;
use crate::EffectState;
use crate::EvidenceId;
use crate::ExecutionAction;
use crate::ExecutionId;
use crate::ExecutionState;
use crate::ExecutionStepId;
use crate::IncidentId;
use crate::PlanStepId;
use crate::TenantId;
use crate::VerificationOutcome;
use crate::VerificationSpec;

pub const EXECUTION_OBSERVATION_SCHEMA_VERSION: &str = "rocketmq-sre.execution-observation.v1";
pub const MAX_EXECUTION_OBSERVATION_STEPS: usize = 16;
pub const MAX_EXECUTION_OBSERVATION_ATTEMPTS: usize = 64;
pub const MAX_EXECUTION_OBSERVATION_VERIFICATIONS: usize = 64;
pub const MAX_EXECUTION_OBSERVATION_TIMELINE: usize = 256;
pub const MAX_PLAN_EXECUTION_RECEIPTS: usize = 20;

/// Original accepted intent selector. Reading it never retries execution.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLookupRequest {
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub precondition_hash: String,
    pub idempotency_key: String,
}

/// A missing receipt at this read snapshot does not prove no acceptance/effect.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionLookupStatus {
    Accepted,
    NotObserved,
}

/// Safe immutable accepted identity, with a separately observed journal state.
///
/// This omits the signed request, Grants, nonce, parameters and Agent details.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReceiptReference {
    pub execution_id: ExecutionId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub precondition_hash: String,
    pub incident_id: IncidentId,
    pub diagnosis_revision_id: DiagnosisRevisionId,
    pub correlation_id: CorrelationId,
    pub requested_by: String,
    pub accepted_at: DateTime<Utc>,
    pub state: ExecutionState,
}

/// Readonly receipt lookup bound to the current authenticated requester.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLookupView {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub actor_subject: String,
    pub request: ExecutionLookupRequest,
    pub status: ExecutionLookupStatus,
    pub receipt: Option<ExecutionReceiptReference>,
    pub observed_at: DateTime<Utc>,
    /// Snapshot expiry never changes the immutable accepted identity.
    pub valid_until: DateTime<Utc>,
}

/// Bounded receipt discovery for one authorized immutable Plan/hash.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanExecutionPage {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub items: Vec<ExecutionReceiptReference>,
    pub partial: bool,
    pub observed_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
}

/// Closed observation status; none of these values grants execution authority.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionObservationState {
    NotObserved,
    Unknown,
    Observed,
    Satisfied,
    NotSatisfied,
}

/// Source of a fact, independent of the fact's observed truth value.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionObservationSource {
    AcceptedRequest,
    StepJournal,
    VerificationJournal,
    ReconciliationJournal,
    QuarantineJournal,
    IndependentBusinessObservation,
    NotObserved,
}

/// Safe explanation codes; arbitrary stored/remote text is not a public reason.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionObservationReason {
    AcceptedReceipt,
    ReceiptNotObserved,
    IntentRecorded,
    ResultRecorded,
    EffectUnknown,
    NoEffectResult,
    ReconciledApplied,
    ReconciledNotApplied,
    ReconciledFailed,
    ReconciliationRequired,
    VerificationNotObserved,
    LegacyWindowProofMissing,
    WindowProofInvalid,
    RetainedProofUnavailable,
    PolicyNotConfigured,
    TargetVersionNotObserved,
    ExactSliSourceMissing,
    SourceMissing,
    SourcePartial,
    SourceStale,
    SourceFuture,
    SourceBeforeWindow,
    SourceOutOfOrder,
    SourceRepeated,
    ObservationGap,
    EvidenceHashInvalid,
    EvidenceScopeMismatch,
    ReadDeadlineExceeded,
    ObservationBudgetExhausted,
    ObservationClockInvalid,
    ObservationCancelled,
    KnownConditionFailed,
    StableWindowIncomplete,
    PolicyWindowSatisfied,
    CompensationNotObserved,
    CompensationResultRecorded,
    CompensationUnknown,
    CompensationFailed,
    ManualTakeoverRequired,
    ManualTakeoverNotRecorded,
    IndependentBusinessObservationMissing,
    PartialHistory,
    RecordedStateChange,
    RecordedVerification,
    UnrecognizedRecordedEvent,
}

/// One typed backend fact. Browser time alone cannot satisfy it.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionObservationFact {
    pub state: ExecutionObservationState,
    pub source: ExecutionObservationSource,
    pub reason_codes: Vec<ExecutionObservationReason>,
}

/// Quality of a saved verification proof, separate from its recorded outcome.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionWindowProofState {
    NotObserved,
    LegacyMissing,
    Invalid,
    Unavailable,
    Inconclusive,
    ValidHistorical,
}

/// Engineering fixtures must not be presented as physical production proof.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationProofOrigin {
    RecordedOnly,
    SyntheticFixture,
    ProductionRead,
}

/// Per-attempt verification metadata without raw source payloads.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionVerificationObservation {
    pub step_id: ExecutionStepId,
    pub attempt: u16,
    pub compensation: bool,
    pub recorded_outcome: VerificationOutcome,
    pub proof_state: ExecutionWindowProofState,
    pub provenance: VerificationProofOrigin,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub required_stable_window_seconds: u64,
    pub observed_stable_seconds: Option<u64>,
    pub max_wait_seconds: Option<u64>,
    pub sample_count: Option<u32>,
    pub max_source_age_seconds: Option<u64>,
    pub max_sample_gap_seconds: Option<u64>,
    pub source_first_at: Option<DateTime<Utc>>,
    pub source_last_at: Option<DateTime<Utc>>,
    pub resource_observed_at: Option<DateTime<Utc>>,
    pub technical_sli_observed_at: Option<DateTime<Utc>>,
    pub collected_at: Option<DateTime<Utc>>,
    pub policy_digest: Option<String>,
    /// Codes are intersected with the recognized exact compiled declaration.
    pub satisfied_conditions: Vec<String>,
    pub failed_conditions: Vec<String>,
    pub missing_conditions: Vec<String>,
    /// A declaration is not a verified physical target version.
    pub declared_target_version: Option<String>,
    pub target_version_verified: bool,
    pub evidence_ids: Vec<EvidenceId>,
    pub reason_codes: Vec<ExecutionObservationReason>,
}

/// Actual append-only attempt identity and bounded recorded effect result.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAttemptObservation {
    pub step_id: ExecutionStepId,
    pub attempt: u16,
    pub compensation: bool,
    pub intended_at: DateTime<Utc>,
    pub result_at: Option<DateTime<Utc>>,
    pub effect_state: Option<EffectState>,
    pub fact: ExecutionObservationFact,
    pub lease_id: String,
    /// Decimal strings retain u64 precision in browser clients.
    pub lease_epoch: String,
}

/// Exact immutable Plan step with observed attempts, not inferred completion.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionStepObservation {
    pub plan_step_id: PlanStepId,
    pub sequence: u16,
    pub action: ExecutionAction,
    pub descriptor_version: String,
    pub target: String,
    /// Present only for a recognized exact compiled declaration.
    pub declared_verification: Option<VerificationSpec>,
    pub attempts: Vec<ExecutionAttemptObservation>,
    pub verifications: Vec<ExecutionVerificationObservation>,
    pub effect: ExecutionObservationFact,
    pub compensation: ExecutionObservationFact,
    pub partial: bool,
}

/// A recorded event type. It is never expanded into guessed earlier phases.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionObservationEventKind {
    Accepted,
    StateChanged,
    IntentPersisted,
    ResultRecorded,
    VerificationCaptured,
    VerificationCompleted,
    ManualTakeoverRequired,
    QuarantineCreated,
    QuarantineCleared,
    Other,
}

/// Safe per-execution event in durable sequence order.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionObservationEvent {
    pub sequence: String,
    pub kind: ExecutionObservationEventKind,
    pub occurred_at: DateTime<Utc>,
    pub step_id: Option<ExecutionStepId>,
    pub state: Option<ExecutionState>,
    pub reason: ExecutionObservationReason,
}

/// Safe current read projection; recorded history is distinct from recovery.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionObservationView {
    pub schema_version: String,
    pub binding: ExecutionReceiptReference,
    pub observed_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub steps: Vec<ExecutionStepObservation>,
    pub timeline: Vec<ExecutionObservationEvent>,
    pub timeline_partial: bool,
    pub history: ExecutionObservationFact,
    pub reconciliation: ExecutionObservationFact,
    pub manual_takeover: ExecutionObservationFact,
    /// Typed server fact. Execution/verification success alone cannot set it.
    pub business_recovery: ExecutionObservationFact,
}
