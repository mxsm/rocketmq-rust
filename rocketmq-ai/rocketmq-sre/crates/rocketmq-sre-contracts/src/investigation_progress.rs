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

use crate::{
    DiagnosticEvidenceQuality, DiagnosticRequirementQuality, EvidenceId, InvestigationControlEpoch,
    InvestigationDiagnosisStatus, InvestigationPackRef, InvestigationRunId, InvestigationRunRevisionId,
    InvestigationRunScope, InvestigationStepId, StepAttemptId,
};

mod assessment;
mod recording;
mod validation;
pub(crate) use validation::validate_history;

pub const INVESTIGATION_RUN_REVISION_SCHEMA_VERSION: &str = "rocketmq-sre.investigation-run-revision.v1";

/// Deterministic rule output. Model prose cannot replace these findings or
/// promote missing/counter evidence into a root-cause statement.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationRunFinding {
    pub reason_code: String,
    pub root_cause: String,
    pub outcome: InvestigationDiagnosisStatus,
    pub supporting_evidence_ids: Vec<EvidenceId>,
    pub counter_evidence_ids: Vec<EvidenceId>,
}

/// Run-owned immutable diagnosis, without synthetic conversation/turn/answer IDs.
/// It must be inserted in the fenced evaluation-settlement transaction.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedInvestigationRunRevision {
    pub schema_version: String,
    pub id: InvestigationRunRevisionId,
    pub run_id: InvestigationRunId,
    pub scope: InvestigationRunScope,
    pub control_epoch: InvestigationControlEpoch,
    pub pack: InvestigationPackRef,
    pub step_id: InvestigationStepId,
    pub attempt_id: StepAttemptId,
    pub status: InvestigationDiagnosisStatus,
    pub partial: bool,
    pub input_fingerprint: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub requirements: Vec<DiagnosticRequirementQuality>,
    pub findings: Vec<InvestigationRunFinding>,
    pub created_at: DateTime<Utc>,
}

/// Compact immutable projection derived from an actual saved run revision.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationPackResult {
    pub run_revision_id: InvestigationRunRevisionId,
    pub revision_digest: String,
    pub pack: InvestigationPackRef,
    pub scope: InvestigationRunScope,
    pub control_epoch: InvestigationControlEpoch,
    pub step_id: InvestigationStepId,
    pub attempt_id: StepAttemptId,
    pub status: InvestigationDiagnosisStatus,
    pub partial: bool,
    pub input_fingerprint: String,
    pub evidence_ids: Vec<EvidenceId>,
    pub required_evidence_quality: DiagnosticEvidenceQuality,
    pub fault_finding_count: u16,
    pub counter_evidence_ids: Vec<EvidenceId>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestigationProgressAcceptance {
    Recorded,
    Replay,
}

impl SavedInvestigationRunRevision {
    /// Creates the compact projection after structural validation. This does
    /// not attest that the row was persisted or that a caller is authorized.
    ///
    /// # Errors
    /// Rejects malformed or unbound report/reference metadata.
    pub fn pack_result(&self) -> Result<InvestigationPackResult, crate::InvestigationRunFailure> {
        self.validate()?;
        let required: Vec<_> = self
            .requirements
            .iter()
            .filter(|requirement| requirement.required)
            .collect();
        let quality = if required.is_empty() {
            DiagnosticEvidenceQuality::Unknown
        } else if required
            .iter()
            .all(|item| item.quality == DiagnosticEvidenceQuality::Available)
        {
            DiagnosticEvidenceQuality::Available
        } else if required
            .iter()
            .any(|item| item.quality == DiagnosticEvidenceQuality::Missing)
        {
            DiagnosticEvidenceQuality::Missing
        } else if required
            .iter()
            .any(|item| item.quality == DiagnosticEvidenceQuality::Stale)
        {
            DiagnosticEvidenceQuality::Stale
        } else if required
            .iter()
            .any(|item| item.quality == DiagnosticEvidenceQuality::Unsupported)
        {
            DiagnosticEvidenceQuality::Unsupported
        } else if required
            .iter()
            .any(|item| item.quality == DiagnosticEvidenceQuality::Partial)
        {
            DiagnosticEvidenceQuality::Partial
        } else {
            DiagnosticEvidenceQuality::Unknown
        };
        let mut counters: Vec<_> = self
            .findings
            .iter()
            .flat_map(|finding| finding.counter_evidence_ids.iter().copied())
            .collect();
        counters.sort();
        counters.dedup();
        Ok(InvestigationPackResult {
            run_revision_id: self.id,
            revision_digest: crate::canonical_sha256(self)
                .map_err(|_| crate::InvestigationRunFailure::InvalidAssessment)?,
            pack: self.pack.clone(),
            scope: self.scope.clone(),
            control_epoch: self.control_epoch,
            step_id: self.step_id,
            attempt_id: self.attempt_id,
            status: self.status,
            partial: self.partial,
            input_fingerprint: self.input_fingerprint.clone(),
            evidence_ids: self.evidence_ids.clone(),
            required_evidence_quality: quality,
            fault_finding_count: self
                .findings
                .iter()
                .filter(|finding| finding.outcome == InvestigationDiagnosisStatus::Fault)
                .count() as u16,
            counter_evidence_ids: counters,
            created_at: self.created_at,
        })
    }
}
