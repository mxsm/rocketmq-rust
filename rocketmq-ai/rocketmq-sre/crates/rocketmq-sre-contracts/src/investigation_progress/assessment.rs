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

use crate::{
    HypothesisAssessment, HypothesisAssessmentConclusion, InvestigationRevision, InvestigationRun,
    InvestigationRunContext, InvestigationRunFailure, InvestigationRunStatus,
};

use super::*;

impl InvestigationRun {
    /// Latest result per selected pack in the current scope and control epoch.
    #[must_use]
    pub fn current_pack_result(&self, pack: &InvestigationPackRef) -> Option<&InvestigationPackResult> {
        self.pack_results.iter().rev().find(|result| {
            &result.pack == pack && result.scope == self.effective_scope && result.control_epoch == self.control_epoch
        })
    }

    /// Validates the conservative all-selected-pack gate. It does not establish
    /// that the revision rows exist; the service must verify their actual rows.
    ///
    /// # Errors
    /// Rejects missing, partial, stale, contradictory or ambiguous conclusions.
    pub fn validate_planned_conclusion(
        &self,
        assessment: &HypothesisAssessment,
        basis: &SavedInvestigationRunRevision,
    ) -> Result<(), InvestigationRunFailure> {
        if matches!(
            assessment.conclusion,
            HypothesisAssessmentConclusion::Inconclusive { .. }
        ) {
            return Ok(());
        }
        let mut faults = 0;
        for pack in &self.effective_packs {
            let result = self
                .current_pack_result(pack)
                .ok_or(InvestigationRunFailure::InvalidAssessment)?;
            if result.partial
                || result.required_evidence_quality != DiagnosticEvidenceQuality::Available
                || !result.counter_evidence_ids.is_empty()
                || result.evidence_ids.iter().any(|id| !self.evidence_ids.contains(id))
            {
                return Err(InvestigationRunFailure::InvalidAssessment);
            }
            match result.status {
                InvestigationDiagnosisStatus::Healthy => {}
                InvestigationDiagnosisStatus::Fault if result.fault_finding_count == 1 => faults += 1,
                _ => return Err(InvestigationRunFailure::InvalidAssessment),
            }
        }
        match &assessment.conclusion {
            HypothesisAssessmentConclusion::Healthy {
                supporting_evidence_ids,
            } if faults == 0 && basis.status == InvestigationDiagnosisStatus::Healthy => {
                if supporting_evidence_ids.iter().any(|id| {
                    !basis
                        .findings
                        .iter()
                        .any(|finding| finding.supporting_evidence_ids.contains(id))
                }) {
                    return Err(InvestigationRunFailure::InvalidAssessment);
                }
            }
            HypothesisAssessmentConclusion::RootCause {
                statement,
                supporting_evidence_ids,
                counter_evidence_ids,
            } if faults == 1
                && basis.status == InvestigationDiagnosisStatus::Fault
                && counter_evidence_ids.is_empty() =>
            {
                let findings: Vec<_> = basis
                    .findings
                    .iter()
                    .filter(|finding| finding.outcome == InvestigationDiagnosisStatus::Fault)
                    .collect();
                if findings.len() != 1
                    || &findings[0].root_cause != statement
                    || !findings[0].counter_evidence_ids.is_empty()
                    || supporting_evidence_ids
                        .iter()
                        .any(|id| !findings[0].supporting_evidence_ids.contains(id))
                {
                    return Err(InvestigationRunFailure::InvalidAssessment);
                }
            }
            _ => return Err(InvestigationRunFailure::InvalidAssessment),
        }
        Ok(())
    }

    /// Finishes against an actual run-owned revision, preserving the legacy
    /// chat-revision wrapper and its serialized payload compatibility.
    ///
    /// # Errors
    /// Rejects changed epochs, missing saved bindings, unsupported strong
    /// conclusions and any attempt to reset the shared budget/deadline.
    pub fn finish_planned_assessment(
        &mut self,
        context: &InvestigationRunContext,
        expected_revision: InvestigationRevision,
        assessment: &HypothesisAssessment,
        revision: &SavedInvestigationRunRevision,
        at: DateTime<Utc>,
    ) -> Result<(), InvestigationRunFailure> {
        context.authorize(&self.initial_scope)?;
        self.validate()?;
        self.expect_revision(expected_revision)?;
        assessment.validate_shape()?;
        revision.validate()?;
        if !matches!(
            self.status,
            InvestigationRunStatus::Running | InvestigationRunStatus::NeedsEvidence
        ) || self.in_flight_count() != 0
            || self.assessments.len() >= 16
            || assessment.run_id != self.id
            || assessment.scope != self.effective_scope
            || assessment.control_epoch != self.control_epoch
            || assessment.diagnosis_revision_id.is_some()
            || assessment.run_revision_id != Some(revision.id)
            || revision.run_id != self.id
            || revision.scope != self.effective_scope
            || revision.control_epoch != self.control_epoch
            || self.current_pack_result(&revision.pack) != Some(&revision.pack_result()?)
            || assessment.created_at < revision.created_at
            || assessment.created_at > at
            || self.assessments.iter().any(|item| item.id == assessment.id)
        {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        let references = assessment.references();
        context.evidence_visible(&references, &assessment.scope)?;
        let inconclusive = matches!(
            assessment.conclusion,
            HypothesisAssessmentConclusion::Inconclusive { .. }
        );
        if references.iter().any(|id| {
            !self.evidence_ids.contains(id)
                || if inconclusive {
                    !self
                        .effective_packs
                        .iter()
                        .filter_map(|pack| self.current_pack_result(pack))
                        .any(|result| result.evidence_ids.contains(id))
                } else {
                    !revision.evidence_ids.contains(id)
                }
        }) {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        if let HypothesisAssessmentConclusion::Inconclusive {
            missing_requirements, ..
        } = &assessment.conclusion
            && missing_requirements.iter().any(|requirement| {
                !self.effective_packs.contains(&requirement.pack) || !context.allowed_requirements.contains(requirement)
            })
        {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        self.validate_planned_conclusion(assessment, revision)?;
        let mut next = self.clone();
        if let HypothesisAssessmentConclusion::Inconclusive { reason, .. } = assessment.conclusion {
            next.status = InvestigationRunStatus::Inconclusive;
            next.stop_reason = Some(reason);
        } else {
            self.check_live(at)?;
            next.status = InvestigationRunStatus::Completed;
            next.stop_reason = None;
        }
        next.bump(at)?;
        next.assessments.push(assessment.clone());
        next.validate()?;
        *self = next;
        Ok(())
    }
}
