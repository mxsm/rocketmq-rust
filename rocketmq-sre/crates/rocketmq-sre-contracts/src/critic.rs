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
use crate::ContractError;
use crate::CriticReviewId;
use crate::DiagnosisRevisionId;
use crate::EvidenceId;
use crate::ModelInvocationId;

/// Critic review availability and validation state.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriticReviewStatus {
    Pending,
    Valid,
    Invalid,
    Unavailable,
    Conflict,
}

/// Bounded deterministic Critic conclusion.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriticConclusion {
    Accept,
    NeedsRevision,
    Reject,
}

/// Closed set of checks that a model Critic may report.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriticFindingCode {
    EvidenceReferenceInvalid,
    CounterEvidenceMissing,
    ParameterOutOfRange,
    MissingPrecondition,
    ImpactScopeExceeded,
    RollbackUnavailable,
}

/// One structured finding returned by the Critic.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CriticFinding {
    pub code: CriticFindingCode,
    pub message: String,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
}

/// Fixed, bounded model output accepted from the heterogeneous Critic.
///
/// It cannot replace descriptors, policy decisions, plan fields, or execution
/// parameters. Those remain locally validated Control Plane inputs.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CriticAssessment {
    pub conclusion: CriticConclusion,
    #[serde(default)]
    pub cited_evidence_ids: Vec<EvidenceId>,
    #[serde(default)]
    pub counter_evidence_ids: Vec<EvidenceId>,
    pub parameter_ranges_valid: bool,
    #[serde(default)]
    pub missing_preconditions: Vec<String>,
    pub impact_scope_valid: bool,
    pub rollback_available: bool,
    #[serde(default)]
    pub findings: Vec<CriticFinding>,
    pub rationale: String,
}

impl CriticAssessment {
    /// Validates model-controlled fields against bounded local provenance.
    ///
    /// # Errors
    ///
    /// Rejects unknown Evidence references, duplicate or oversized collections,
    /// empty findings, and unbounded rationale or precondition strings.
    pub fn validate(&self, allowed_evidence_ids: &[EvidenceId]) -> Result<(), ContractError> {
        const MAX_ITEMS: usize = 32;
        let allowed = allowed_evidence_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let cited = self
            .cited_evidence_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let counter = self
            .counter_evidence_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if self.cited_evidence_ids.len() > MAX_ITEMS
            || self.counter_evidence_ids.len() > MAX_ITEMS
            || self.findings.len() > MAX_ITEMS
            || self.missing_preconditions.len() > MAX_ITEMS
            || cited.len() != self.cited_evidence_ids.len()
            || counter.len() != self.counter_evidence_ids.len()
            || !cited.is_subset(&allowed)
            || !counter.is_subset(&allowed)
            || self.rationale.trim().is_empty()
            || self.rationale.chars().count() > 4_000
            || self
                .missing_preconditions
                .iter()
                .any(|item| item.trim().is_empty() || item.chars().count() > 256)
            || self.findings.iter().any(|finding| {
                finding.message.trim().is_empty()
                    || finding.message.chars().count() > 1_000
                    || finding.evidence_ids.len() > MAX_ITEMS
                    || !finding.evidence_ids.iter().all(|id| allowed.contains(id))
            })
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "Critic assessment violates bounded schema or Evidence provenance".to_owned(),
            });
        }
        Ok(())
    }
}

/// Operator-facing state of the optional or required Critic gate.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriticGateState {
    UnreviewedNotRequired,
    PendingRequired,
    Accepted,
    NeedsRevision,
    Rejected,
    Invalid,
    Unavailable,
    Conflict,
}

/// Immutable heterogeneous model review bound to actual invocation identity.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CriticReview {
    pub id: CriticReviewId,
    pub plan_id: ActionPlanId,
    pub plan_hash: String,
    pub diagnosis_revision_id: DiagnosisRevisionId,
    pub primary_invocation_id: ModelInvocationId,
    pub critic_invocation_id: Option<ModelInvocationId>,
    pub primary_model_family: String,
    pub critic_model_family: Option<String>,
    pub critic_provider: Option<String>,
    pub critic_profile: Option<String>,
    pub critic_model_revision: Option<String>,
    pub endpoint_instance: Option<String>,
    #[serde(default)]
    pub fallback_chain: Vec<String>,
    pub prompt_version: String,
    pub schema_version: String,
    pub payload_hash: String,
    pub status: CriticReviewStatus,
    pub conclusion: CriticConclusion,
    pub assessment: Option<CriticAssessment>,
    #[serde(default)]
    pub findings: Vec<CriticFinding>,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assessment(evidence_id: EvidenceId) -> CriticAssessment {
        CriticAssessment {
            conclusion: CriticConclusion::Accept,
            cited_evidence_ids: vec![evidence_id],
            counter_evidence_ids: Vec::new(),
            parameter_ranges_valid: true,
            missing_preconditions: Vec::new(),
            impact_scope_valid: true,
            rollback_available: true,
            findings: Vec::new(),
            rationale: "All fixed checks passed.".to_owned(),
        }
    }

    #[test]
    fn assessment_rejects_fabricated_evidence_references() {
        let allowed = EvidenceId::new();
        let fabricated = EvidenceId::new();

        assert!(assessment(allowed).validate(&[allowed]).is_ok());
        assert!(assessment(fabricated).validate(&[allowed]).is_err());
    }

    #[test]
    fn assessment_has_no_plan_or_policy_override_fields() {
        let value = serde_json::to_value(assessment(EvidenceId::new())).expect("assessment");
        let object = value.as_object().expect("assessment object");

        for forbidden in ["plan", "descriptor", "policy", "parameters", "action", "execution"] {
            assert!(!object.contains_key(forbidden));
        }
    }
}
