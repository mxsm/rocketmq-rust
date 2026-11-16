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
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use rocketmq_sre_contracts::TenantId;
use serde::Deserialize;
use serde::Serialize;

pub(super) const FRAGMENT_SCHEMA: &str = "rocketmq-sre.execution-drill-fragment.v1";
pub(super) const REPORT_SCHEMA: &str = "rocketmq-sre.execution-drill-report.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rejection {
    Arguments,
    InputUnavailable,
    LimitExceeded,
    InvalidRecord,
    DuplicateCase,
    OutputExists,
    OutputUnavailable,
}

impl Rejection {
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Arguments => "invalid_drill_arguments",
            Self::InputUnavailable => "drill_input_unavailable",
            Self::LimitExceeded => "drill_limit_exceeded",
            Self::InvalidRecord => "invalid_drill_record",
            Self::DuplicateCase => "duplicate_drill_case",
            Self::OutputExists => "drill_output_exists",
            Self::OutputUnavailable => "drill_output_unavailable",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fragment {
    pub schema_version: String,
    pub run_id: String,
    pub production_qualified: bool,
    pub runtime_admission_authority: bool,
    pub cases: Vec<Case>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Report {
    pub schema_version: &'static str,
    pub run_id: String,
    pub production_qualified: bool,
    pub runtime_admission_authority: bool,
    pub physical_truth_validated: bool,
    pub coverage: Vec<CoverageSummary>,
    pub cases: Vec<Case>,
}

#[derive(Debug, Serialize)]
pub(super) struct CoverageSummary {
    pub action: ExecutionAction,
    pub descriptor_version: &'static str,
    pub evidence_class: EvidenceClass,
    pub passed_case_claims: usize,
    pub failed_case_claims: usize,
    pub blocked_case_claims: usize,
    pub not_run_case_claims: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Case {
    pub action: ExecutionAction,
    pub descriptor_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub target_id: String,
    pub target_artifact_identity: Option<ArtifactIdentity>,
    pub observed_software_version: Option<SoftwareVersion>,
    pub evidence_class: EvidenceClass,
    pub execution_path: ExecutionPath,
    pub scenario: Scenario,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub outcome: Outcome,
    pub observed_state: ObservedState,
    pub guard: Option<Guard>,
    pub guard_reached: Option<bool>,
    pub original_identity_preserved: Option<bool>,
    pub cleanup_confirmed: Option<bool>,
    pub accepted_identity: Option<AcceptedIdentity>,
    pub counters: Counters,
    pub artifact_references: Vec<String>,
    pub limitations: Vec<Limitation>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ArtifactIdentity {
    pub kind: ArtifactKind,
    pub opaque_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ArtifactKind {
    ControlledChildBinary,
    DeployedImage,
    DeterministicFixture,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SoftwareVersion {
    pub value: String,
    pub source: VersionSource,
    pub observation_reference: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum VersionSource {
    TargetProtocol,
    TargetVersionEndpoint,
    ControlledBinarySelfReport,
    DeployedBinarySelfReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AcceptedIdentity {
    pub execution_id: ExecutionId,
    pub correlation_id: CorrelationId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub plan_step_id: PlanStepId,
    pub original_selector_digest: String,
    pub lease_epoch: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum EvidenceClass {
    DeterministicContract,
    ProtocolFault,
    IsolatedDriver,
    NormalAdmission,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExecutionPath {
    DeterministicHarness,
    InjectedExecutorAgent,
    DirectHandlerAdapter,
    NormalControlPlane,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Scenario {
    GuardedPositiveControl,
    ExpiredApproval,
    ExpiredFence,
    AcceptedDuplicate,
    TargetDrift,
    ResponseLoss,
    TargetDisconnected,
    VerificationUnknown,
    VerificationFailed,
    CompensationFailed,
    FrozenAdmission,
    OwnerCancelled,
    NormalUnqualifiedDenied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Outcome {
    Passed,
    Failed,
    Blocked,
    NotRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ObservedState {
    NotObserved,
    Rejected,
    RecordedEffect,
    UnknownEffect,
    Quarantined,
    Restored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Guard {
    ApprovalExpired,
    FenceExpired,
    ScopeFrozen,
    SelectorChanged,
    GenerationChanged,
    SourceUnknown,
    SourceStale,
    TargetVersionUnknown,
    QualificationUnknown,
    OwnerCancelled,
    QualifiedConditionFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Limitation {
    OperatorDeclaredRecordScope,
    SoftwareVersionUnknown,
    PhysicalAttestationUnavailable,
    ExactSliUnavailable,
    IndependentBusinessObservationUnavailable,
    InjectedAuthority,
    GenericKubernetesMechanicsOnly,
    CombinedProductPathUnavailable,
    KubernetesUnavailable,
    CredentialBackendUnavailable,
    ObservationUnavailable,
    TargetCleanupFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Counters {
    pub accepted_identities: Option<u64>,
    pub forward_effects: Option<u64>,
    pub compensation_effects: Option<u64>,
    pub stale_effects: Option<u64>,
    pub new_idempotency_keys: Option<u64>,
    pub quarantines: Option<u64>,
}
