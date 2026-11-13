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

use crate::investigation_run_error::{unique_run_ids, valid_run_key, valid_run_text};
use crate::{InvestigationDiagnosisStatus, InvestigationRunFailure};

use super::*;

impl SavedInvestigationRunRevision {
    /// Validates bounded deterministic findings and their exact saved references.
    ///
    /// # Errors
    /// Rejects invented references, invalid qualification claims and malformed scope.
    pub fn validate(&self) -> Result<(), InvestigationRunFailure> {
        self.scope.validate()?;
        self.pack.validate()?;
        if self.schema_version != INVESTIGATION_RUN_REVISION_SCHEMA_VERSION
            || self.id.as_uuid().is_nil()
            || self.run_id.as_uuid().is_nil()
            || self.step_id.as_uuid().is_nil()
            || self.attempt_id.as_uuid().is_nil()
            || !unique_run_ids(&self.evidence_ids, 256)
            || self.evidence_ids.iter().any(|id| id.as_uuid().is_nil())
            || self.requirements.len() > 64
            || self.findings.len() > 64
        {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        if !valid_fingerprint(&self.input_fingerprint) {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        let mut keys = std::collections::BTreeSet::new();
        for requirement in &self.requirements {
            if !valid_run_key(&requirement.id)
                || !keys.insert(&requirement.id)
                || !valid_run_text(&requirement.source, 64)
                || !valid_run_text(&requirement.resource_prefix, 512)
                || !valid_run_key(&requirement.reason_code)
                || requirement.fields.len() > 128
                || requirement.missing_fields.len() > 128
                || !unique_run_ids(&requirement.evidence_ids, 256)
                || requirement
                    .evidence_ids
                    .iter()
                    .any(|id| !self.evidence_ids.contains(id))
                || requirement.fields.iter().any(|field| !valid_run_text(&field.path, 256))
                || requirement
                    .missing_fields
                    .iter()
                    .any(|field| !valid_run_text(field, 256))
                || (requirement.quality == DiagnosticEvidenceQuality::Available
                    && (requirement.fields.is_empty()
                        || requirement.evidence_ids.is_empty()
                        || !requirement.missing_fields.is_empty()
                        || requirement.observed_at.is_none()))
            {
                return Err(InvestigationRunFailure::InvalidAssessment);
            }
        }
        for finding in &self.findings {
            if !valid_run_key(&finding.reason_code)
                || !valid_run_text(&finding.root_cause, 2048)
                || !unique_run_ids(&finding.supporting_evidence_ids, 128)
                || !unique_run_ids(&finding.counter_evidence_ids, 128)
                || finding
                    .supporting_evidence_ids
                    .iter()
                    .chain(&finding.counter_evidence_ids)
                    .any(|id| !self.evidence_ids.contains(id))
                || (matches!(
                    finding.outcome,
                    InvestigationDiagnosisStatus::Healthy | InvestigationDiagnosisStatus::Fault
                ) && finding.supporting_evidence_ids.is_empty())
                || finding.outcome == InvestigationDiagnosisStatus::Unsupported
            {
                return Err(InvestigationRunFailure::InvalidAssessment);
            }
        }
        if (self.status == InvestigationDiagnosisStatus::Fault
            && !self
                .findings
                .iter()
                .any(|finding| finding.outcome == InvestigationDiagnosisStatus::Fault))
            || (self.status == InvestigationDiagnosisStatus::Healthy
                && (self.findings.is_empty()
                    || self
                        .findings
                        .iter()
                        .any(|finding| finding.outcome != InvestigationDiagnosisStatus::Healthy)))
        {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
        Ok(())
    }
}

pub(super) fn valid_fingerprint(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

pub(crate) fn validate_history(
    run: &crate::InvestigationRun,
    observed: &std::collections::BTreeSet<crate::EvidenceId>,
) -> Result<(), InvestigationRunFailure> {
    if run.pack_results.len() > 64 {
        return Err(InvestigationRunFailure::InvalidInput);
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut attempts = std::collections::BTreeSet::new();
    for result in &run.pack_results {
        result.scope.validate()?;
        result.pack.validate()?;
        let step = run
            .steps
            .iter()
            .find(|step| step.id == result.step_id)
            .ok_or(InvestigationRunFailure::InvalidAssessment)?;
        let attempt = step
            .attempts
            .iter()
            .find(|attempt| attempt.id == result.attempt_id)
            .ok_or(InvestigationRunFailure::InvalidAssessment)?;
        let observation = attempt
            .result
            .as_ref()
            .ok_or(InvestigationRunFailure::InvalidAssessment)?;
        if result.run_revision_id.as_uuid().is_nil()
            || !ids.insert(result.run_revision_id)
            || !attempts.insert(result.attempt_id)
            || !run.initial_packs.contains(&result.pack)
            || !result.scope.narrows(&run.initial_scope)
            || result.pack != step.spec.pack
            || result.scope != step.spec.scope
            || result.control_epoch != attempt.control_epoch
            || observation.outcome != crate::InvestigationStepOutcome::Succeeded
            || !matches!(step.spec.work, crate::InvestigationStepKind::EvaluateRules { .. })
            || result.created_at < observation.finished_at
            || result.created_at > run.updated_at
            || !valid_fingerprint(&result.input_fingerprint)
            || !valid_fingerprint(&result.revision_digest)
            || !unique_run_ids(&result.evidence_ids, 256)
            || result
                .evidence_ids
                .iter()
                .any(|id| !observed.contains(id) || !observation.evidence_ids.contains(id))
            || !unique_run_ids(&result.counter_evidence_ids, 256)
            || result
                .counter_evidence_ids
                .iter()
                .any(|id| !result.evidence_ids.contains(id))
            || result.fault_finding_count > 64
            || (result.status == InvestigationDiagnosisStatus::Healthy && result.fault_finding_count != 0)
            || (result.status == InvestigationDiagnosisStatus::Fault && result.fault_finding_count == 0)
        {
            return Err(InvestigationRunFailure::InvalidAssessment);
        }
    }
    for assessment in &run.assessments {
        if let Some(id) = assessment.run_revision_id {
            let basis = run
                .pack_results
                .iter()
                .find(|result| result.run_revision_id == id)
                .ok_or(InvestigationRunFailure::InvalidAssessment)?;
            if basis.scope != assessment.scope
                || basis.control_epoch != assessment.control_epoch
                || assessment.created_at < basis.created_at
            {
                return Err(InvestigationRunFailure::InvalidAssessment);
            }
            match &assessment.conclusion {
                crate::HypothesisAssessmentConclusion::RootCause { .. }
                    if basis.status != InvestigationDiagnosisStatus::Fault || basis.partial =>
                {
                    return Err(InvestigationRunFailure::InvalidAssessment);
                }
                crate::HypothesisAssessmentConclusion::Healthy { .. }
                    if basis.status != InvestigationDiagnosisStatus::Healthy || basis.partial =>
                {
                    return Err(InvestigationRunFailure::InvalidAssessment);
                }
                _ => {}
            }
            if !matches!(
                assessment.conclusion,
                crate::HypothesisAssessmentConclusion::Inconclusive { .. }
            ) && assessment
                .references()
                .iter()
                .any(|id| !basis.evidence_ids.contains(id))
            {
                return Err(InvestigationRunFailure::InvalidAssessment);
            }
        }
    }
    if run.status == crate::InvestigationRunStatus::Completed
        && run
            .assessments
            .last()
            .is_some_and(|assessment| assessment.run_revision_id.is_some())
    {
        let mut faults = 0;
        for pack in &run.effective_packs {
            let result = run
                .current_pack_result(pack)
                .ok_or(InvestigationRunFailure::InvalidAssessment)?;
            if result.partial
                || result.required_evidence_quality != DiagnosticEvidenceQuality::Available
                || !result.counter_evidence_ids.is_empty()
                || result.evidence_ids.iter().any(|id| !run.evidence_ids.contains(id))
            {
                return Err(InvestigationRunFailure::InvalidAssessment);
            }
            match result.status {
                InvestigationDiagnosisStatus::Healthy => {}
                InvestigationDiagnosisStatus::Fault if result.fault_finding_count == 1 => faults += 1,
                _ => return Err(InvestigationRunFailure::InvalidAssessment),
            }
        }
        match run.assessments.last().map(|assessment| &assessment.conclusion) {
            Some(crate::HypothesisAssessmentConclusion::Healthy { .. }) if faults == 0 => {}
            Some(crate::HypothesisAssessmentConclusion::RootCause { .. }) if faults == 1 => {}
            _ => return Err(InvestigationRunFailure::InvalidAssessment),
        }
    }
    Ok(())
}
