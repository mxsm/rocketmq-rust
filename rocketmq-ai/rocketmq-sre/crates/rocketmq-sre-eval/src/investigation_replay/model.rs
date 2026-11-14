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
use rocketmq_sre_contracts::*;
use serde::{Deserialize, Serialize};

pub const INVESTIGATION_REPLAY_SCHEMA_VERSION: &str = "rocketmq-sre.investigation-replay.v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayArm {
    SinglePack,
    MultiPack,
}

/// A captured sample is provenance, never production qualification or a human verdict.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplayProvenance {
    SyntheticFixture { fixture_ref: String },
    RedactedCapture { source_ref: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayBinding {
    pub requirement: InvestigationRequirementRef,
    pub source: String,
    pub resource: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplayReadOutcome {
    SavedEvidence { snapshot: Box<EvidenceSnapshot> },
    RetryableUnavailable {},
    Rejected {},
    OutcomeUnknown {},
}

/// Exhausting the supplied sequence repeats its final outcome. Every callback
/// invocation is counted and still requires a fresh production admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayRead {
    pub source: String,
    pub resource: String,
    pub outcomes: Vec<ReplayReadOutcome>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayModel {
    Disabled,
    NoEligibleProvider,
    InvokedFailure,
    OutcomeUnknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayCheckpoint {
    None,
    AfterFirstCollection,
}

/// Explicit synthetic control input, not a model-selected plan or an operator grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayRefinement {
    None,
    ReassessSameFactsOnce,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationReplayCase {
    pub schema_version: String,
    pub case_id: String,
    pub provenance: ReplayProvenance,
    pub scope: InvestigationRunScope,
    pub packs: Vec<InvestigationPackRef>,
    pub baseline_pack: InvestigationPackRef,
    pub as_of: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub budget: InvestigationBudget,
    pub bindings: Vec<ReplayBinding>,
    pub reads: Vec<ReplayRead>,
    pub model: ReplayModel,
    pub checkpoint: ReplayCheckpoint,
    pub refinement: ReplayRefinement,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayReadObservation {
    pub source: String,
    pub resource: String,
    pub callbacks: u32,
}

/// Serializable observation deliberately excludes random run/step/revision IDs.
/// Evidence IDs, exact versions, scope and actual rule conclusions remain bound.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayObservation {
    pub scope: InvestigationRunScope,
    pub as_of: DateTime<Utc>,
    pub packs: Vec<InvestigationPackRef>,
    pub status: InvestigationRunStatus,
    pub stop_reason: Option<InvestigationStopReason>,
    pub conclusion: Option<HypothesisAssessmentConclusion>,
    pub evidence_ids: Vec<EvidenceId>,
    pub findings: Vec<InvestigationRunFinding>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationReplayArm {
    pub arm: ReplayArm,
    pub observation: ReplayObservation,
    pub observation_digest: String,
    pub read_observations: Vec<ReplayReadObservation>,
    pub read_callbacks: u32,
    pub model_callbacks: u32,
    pub model_invocations: u32,
    pub accepted_output_bytes: u64,
    /// Only known StepResult.actual_cost values. Unknown attempts have no
    /// invented measurement; their full reservation is reported separately.
    pub known_reported_cost: InvestigationBudgetCost,
    /// Production admission accounting charges known read/model attempts even
    /// if a known result reports a smaller cost. It is not a remote measurement.
    pub settled: InvestigationBudgetCost,
    pub reserved: InvestigationBudgetCost,
    /// A typed JSON checkpoint was hydrated and validated; this is NOT a PG or
    /// process-restart measurement.
    pub serialized_checkpoint_restored: bool,
    pub snapshot: InvestigationRun,
    pub revisions: Vec<SavedInvestigationRunRevision>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationReplayPair {
    pub schema_version: String,
    pub case_id: String,
    pub case_digest: String,
    pub provenance: ReplayProvenance,
    pub baseline: InvestigationReplayArm,
    pub multi_pack: InvestigationReplayArm,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayVerdict {
    Correct,
    Wrong,
    ReasonableInconclusive,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdjudicationProvenance {
    SyntheticFixture {
        fixture_ref: String,
    },
    ExternallySupplied {
        adjudicator_ref: String,
        source_ref: String,
    },
}

/// Supplied AFTER replay. The runner never reads a verdict or expected conclusion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuppliedReplayAdjudication {
    pub case_id: String,
    pub case_digest: String,
    pub arm: ReplayArm,
    pub observation: ReplayObservation,
    pub observation_digest: String,
    pub verdict: ReplayVerdict,
    pub rationale: String,
    pub provenance: AdjudicationProvenance,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplayAdjudication {
    Unadjudicated {},
    Supplied {
        verdict: ReplayVerdict,
        rationale: String,
        provenance: AdjudicationProvenance,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdjudicatedReplayPair {
    pub case_id: String,
    pub baseline: ReplayAdjudication,
    pub multi_pack: ReplayAdjudication,
}

/// Closed, sanitized failures: source payloads, SQL and reviewer notes are never
/// formatted into errors.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum InvestigationReplayRejection {
    InvalidCase,
    InputTooLarge,
    ScopeMismatch,
    UnknownPack,
    InvalidEvidence,
    AmbiguousFixture,
    ContractRejected,
    RulesRejected,
    InvalidCheckpoint,
    InvalidAdjudication,
}

impl std::fmt::Display for InvestigationReplayRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Investigation replay was rejected")
    }
}
impl std::fmt::Debug for InvestigationReplayRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for InvestigationReplayRejection {}

impl From<InvestigationRunFailure> for InvestigationReplayRejection {
    fn from(_: InvestigationRunFailure) -> Self {
        Self::ContractRejected
    }
}
