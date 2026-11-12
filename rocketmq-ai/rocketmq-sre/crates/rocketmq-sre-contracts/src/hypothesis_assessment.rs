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

use crate::investigation_run_error::{unique_run_ids, valid_run_text};
use crate::{
    DiagnosisRevisionId, EvidenceId, HypothesisAssessmentId, INVESTIGATION_RUN_SCHEMA_VERSION,
    InvestigationControlEpoch, InvestigationDiagnosisRevision, InvestigationDiagnosisStatus,
    InvestigationRequirementRef, InvestigationRevision, InvestigationRun, InvestigationRunContext,
    InvestigationRunFailure, InvestigationRunId, InvestigationRunScope, InvestigationRunStatus,
};

/// Explicit stop reasons do not imply a root cause was established.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvestigationStopReason {
    EvidenceInsufficient,
    BudgetExhausted,
    DeadlineExceeded,
    NoProgress,
    ModelUnavailable,
    SourceUnavailable,
}

/// Advisory conclusions reference saved Evidence, never an inferred timestamp relation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HypothesisAssessmentConclusion {
    RootCause {
        statement: String,
        supporting_evidence_ids: Vec<EvidenceId>,
        counter_evidence_ids: Vec<EvidenceId>,
    },
    Healthy {
        supporting_evidence_ids: Vec<EvidenceId>,
    },
    Inconclusive {
        reason: InvestigationStopReason,
        considered_evidence_ids: Vec<EvidenceId>,
        missing_requirements: Vec<InvestigationRequirementRef>,
    },
}

/// Evidence-bound advisory assessment on an existing immutable diagnosis revision.
///
/// No execution-eligibility field exists: this object cannot grant execution.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HypothesisAssessment {
    pub schema_version: String,
    pub id: HypothesisAssessmentId,
    pub run_id: InvestigationRunId,
    pub scope: InvestigationRunScope,
    pub control_epoch: InvestigationControlEpoch,
    pub diagnosis_revision_id: DiagnosisRevisionId,
    pub conclusion: HypothesisAssessmentConclusion,
    pub summary: String,
    pub created_at: DateTime<Utc>,
}

impl HypothesisAssessment {
    pub(crate) fn references(&self) -> Vec<EvidenceId> {
        match &self.conclusion {
            HypothesisAssessmentConclusion::RootCause {
                supporting_evidence_ids,
                counter_evidence_ids,
                ..
            } => supporting_evidence_ids
                .iter()
                .chain(counter_evidence_ids)
                .copied()
                .collect(),
            HypothesisAssessmentConclusion::Healthy {
                supporting_evidence_ids,
            } => supporting_evidence_ids.clone(),
            HypothesisAssessmentConclusion::Inconclusive {
                considered_evidence_ids,
                ..
            } => considered_evidence_ids.clone(),
        }
    }

    pub(crate) fn validate_shape(&self) -> Result<(), InvestigationRunFailure> {
        self.scope.validate()?;
        if self.schema_version != INVESTIGATION_RUN_SCHEMA_VERSION
            || self.id.as_uuid().is_nil()
            || self.run_id.as_uuid().is_nil()
            || self.diagnosis_revision_id.as_uuid().is_nil()
            || !valid_run_text(&self.summary, 4096)
            || !unique_run_ids(&self.references(), 128)
            || self.references().iter().any(|id| id.as_uuid().is_nil())
        {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        match &self.conclusion {
            HypothesisAssessmentConclusion::RootCause {
                statement,
                supporting_evidence_ids,
                ..
            } => {
                if !valid_run_text(statement, 2048) || supporting_evidence_ids.is_empty() {
                    return Err(InvestigationRunFailure::InvalidAssessment);
                }
            }
            HypothesisAssessmentConclusion::Healthy {
                supporting_evidence_ids,
            } => {
                if supporting_evidence_ids.is_empty() {
                    return Err(InvestigationRunFailure::InvalidAssessment);
                }
            }
            HypothesisAssessmentConclusion::Inconclusive {
                missing_requirements, ..
            } => {
                if !unique_run_ids(missing_requirements, 32) {
                    return Err(InvestigationRunFailure::InvalidAssessment);
                }
                for requirement in missing_requirements {
                    requirement.pack.validate()?;
                    if !crate::investigation_run_error::valid_run_key(&requirement.requirement_id) {
                        return Err(InvestigationRunFailure::InvalidAssessment);
                    }
                }
            }
        }
        Ok(())
    }

    /// Checks saved revision membership, current epoch, scope and trusted Evidence bindings.
    ///
    /// # Errors
    /// Rejects cross-case revisions, invisible or unsaved references, invented
    /// support, and incompatible resource/window bindings. The service must first
    /// authorize the saved revision's tenant/cluster parent and actual metadata.
    pub fn validate_for(
        &self,
        run: &InvestigationRun,
        context: &InvestigationRunContext,
        revision: &InvestigationDiagnosisRevision,
    ) -> Result<(), InvestigationRunFailure> {
        context.authorize(&run.initial_scope)?;
        run.validate()?;
        self.validate_shape()?;
        match &self.conclusion {
            HypothesisAssessmentConclusion::RootCause { .. }
                if revision.partial || revision.status != InvestigationDiagnosisStatus::Fault =>
            {
                return Err(InvestigationRunFailure::InvalidAssessment);
            }
            HypothesisAssessmentConclusion::Healthy { .. }
                if revision.partial || revision.status != InvestigationDiagnosisStatus::Healthy =>
            {
                return Err(InvestigationRunFailure::InvalidAssessment);
            }
            _ => {}
        }
        if self.run_id != run.id
            || self.scope != run.effective_scope
            || self.control_epoch != run.control_epoch
            || revision.id != self.diagnosis_revision_id
            || revision.investigation_id != run.initial_scope.investigation_id
            || !run
                .effective_packs
                .iter()
                .any(|pack| pack.pack_id == revision.pack_id && pack.pack_version == revision.pack_version)
        {
            return Err(InvestigationRunFailure::ScopeMismatch);
        }
        let ids = self.references();
        context.evidence_visible(&ids, &self.scope)?;
        if ids
            .iter()
            .any(|id| !revision.evidence_ids.contains(id) || !run.evidence_ids.contains(id))
        {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        if let HypothesisAssessmentConclusion::Inconclusive {
            missing_requirements, ..
        } = &self.conclusion
            && missing_requirements
                .iter()
                .any(|item| !context.allowed_requirements.contains(item) || !run.effective_packs.contains(&item.pack))
        {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        Ok(())
    }
}

impl InvestigationRun {
    /// Finishes a drained run with either a supported assessment or an explicit no-conclusion result.
    ///
    /// # Errors
    /// Rejects stale control epochs, in-flight work, unauthorized saved revisions,
    /// repeated finalization and invalid Evidence. Inconclusive may settle after
    /// deadline; a new conclusive assessment may not be admitted after deadline.
    pub fn finish_with_assessment(
        &mut self,
        context: &InvestigationRunContext,
        expected_revision: InvestigationRevision,
        assessment: &HypothesisAssessment,
        revision: &InvestigationDiagnosisRevision,
        at: DateTime<Utc>,
    ) -> Result<(), InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        self.expect_revision(expected_revision)?;
        if !matches!(
            self.status,
            InvestigationRunStatus::Running | InvestigationRunStatus::NeedsEvidence
        ) || self.in_flight_count() != 0
            || self.assessments.len() >= 16
            || assessment.created_at < self.created_at
            || assessment.created_at > at
            || self.assessments.iter().any(|item| item.id == assessment.id)
        {
            return Err(InvestigationRunFailure::InvalidStateTransition);
        }
        assessment.validate_for(self, context, revision)?;
        let status = if matches!(
            assessment.conclusion,
            HypothesisAssessmentConclusion::Inconclusive { .. }
        ) {
            InvestigationRunStatus::Inconclusive
        } else {
            self.check_live(at)?;
            InvestigationRunStatus::Completed
        };
        let mut next = self.clone();
        next.bump(at)?;
        next.assessments.push(assessment.clone());
        next.status = status;
        next.validate()?;
        *self = next;
        Ok(())
    }

    /// Records a drained stop without pretending a worker or remote read was withdrawn.
    ///
    /// # Errors
    /// Only running work with no in-flight attempts can become needs-evidence or failed.
    pub fn stop_without_assessment(
        &mut self,
        context: &InvestigationRunContext,
        expected_revision: InvestigationRevision,
        status: InvestigationRunStatus,
        at: DateTime<Utc>,
    ) -> Result<(), InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        self.expect_revision(expected_revision)?;
        if self.status != InvestigationRunStatus::Running
            || self.in_flight_count() != 0
            || !matches!(
                status,
                InvestigationRunStatus::NeedsEvidence | InvestigationRunStatus::Failed
            )
        {
            return Err(InvestigationRunFailure::InvalidStateTransition);
        }
        let mut next = self.clone();
        next.bump(at)?;
        next.status = status;
        next.validate()?;
        *self = next;
        Ok(())
    }
}
