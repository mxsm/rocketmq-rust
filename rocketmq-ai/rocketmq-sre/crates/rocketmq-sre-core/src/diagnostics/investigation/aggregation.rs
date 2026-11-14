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

use rocketmq_sre_contracts::{
    DiagnosticEvidenceQuality, DiagnosticRequirementQuality, HypothesisAssessmentConclusion,
    InvestigationDiagnosisStatus, InvestigationRequirementRef, InvestigationRunFailure, InvestigationRunRevisionId,
    InvestigationStopReason, SavedInvestigationRunRevision,
};

use super::super::DiagnosticPackRegistry;
use super::{InvestigationPlanningInput, investigation_requirements};

/// A read-only synthesis decision. The service must still verify immutable rows,
/// construct the assessment and apply its current authenticated ownership fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvestigationAggregateDecision {
    Stop {
        reason: InvestigationStopReason,
    },
    Assess {
        basis_revision_id: InvestigationRunRevisionId,
        conclusion: HypothesisAssessmentConclusion,
        /// Mandatory fallback if the Run's conservative all-pack contract
        /// rejects a candidate strong conclusion. It retains real gaps/context.
        inconclusive: HypothesisAssessmentConclusion,
    },
}

/// Rechecks current compiled quality for every selected pack and every Evidence
/// actually used by a finding, including optional support/counter citations.
/// This is shared by the owned runtime and fixed-evidence paired replay.
///
/// # Errors
/// Rejects missing or changed saved-row projections, malformed scopes, exact
/// versions and unbound inputs. This function performs no I/O or authorization.
pub fn aggregate_investigation(
    registry: &DiagnosticPackRegistry,
    input: &InvestigationPlanningInput<'_>,
    revisions: &[SavedInvestigationRunRevision],
    stop_reason: Option<InvestigationStopReason>,
) -> Result<InvestigationAggregateDecision, InvestigationRunFailure> {
    if revisions.len() > 64 {
        return Err(InvestigationRunFailure::InvalidInput);
    }
    let run = input.run;
    run.validate()?;
    super::planning::validate_inputs(registry, input)?;
    let mut saved = Vec::new();
    let mut qualified = stop_reason.is_none();
    let mut missing = Vec::new();
    for pack in &run.effective_packs {
        let current = investigation_requirements(registry, pack, input)?;
        missing.extend(
            current
                .iter()
                .filter(|requirement| {
                    requirement.required && requirement.quality != DiagnosticEvidenceQuality::Available
                })
                .map(|requirement| InvestigationRequirementRef {
                    pack: pack.clone(),
                    requirement_id: requirement.id.clone(),
                }),
        );
        let Some(result) = run.current_pack_result(pack) else {
            qualified = false;
            continue;
        };
        let revision = revisions
            .iter()
            .find(|revision| revision.id == result.run_revision_id)
            .ok_or(InvestigationRunFailure::ResultConflict)?;
        if revision.pack_result()? != *result {
            return Err(InvestigationRunFailure::ResultConflict);
        }
        qualified &= investigation_conclusion_evidence_available(revision, &current);
        for requirement in current.iter().filter(|requirement| {
            requirement.quality != DiagnosticEvidenceQuality::Available
                && requirement.evidence_ids.iter().any(|id| {
                    revision.findings.iter().any(|finding| {
                        finding.supporting_evidence_ids.contains(id) || finding.counter_evidence_ids.contains(id)
                    })
                })
        }) {
            let reference = InvestigationRequirementRef {
                pack: pack.clone(),
                requirement_id: requirement.id.clone(),
            };
            if !missing.contains(&reference) {
                missing.push(reference);
            }
        }
        saved.push(revision);
    }
    let reason = stop_reason.unwrap_or(InvestigationStopReason::EvidenceInsufficient);
    if saved.is_empty() {
        return Ok(InvestigationAggregateDecision::Stop { reason });
    }
    let faults = saved
        .iter()
        .filter(|revision| revision.status == InvestigationDiagnosisStatus::Fault)
        .collect::<Vec<_>>();
    let basis = if faults.len() == 1 { *faults[0] } else { saved[0] };
    let mut considered = saved
        .iter()
        .flat_map(|revision| &revision.evidence_ids)
        .filter(|id| run.evidence_ids.contains(id))
        .copied()
        .collect::<Vec<_>>();
    considered.sort();
    considered.dedup();
    let inconclusive = HypothesisAssessmentConclusion::Inconclusive {
        reason,
        considered_evidence_ids: considered,
        missing_requirements: missing,
    };
    let mut conclusion = inconclusive.clone();
    if qualified && saved.len() == run.effective_packs.len() {
        if faults.len() == 1 {
            let findings = basis
                .findings
                .iter()
                .filter(|finding| finding.outcome == InvestigationDiagnosisStatus::Fault)
                .collect::<Vec<_>>();
            if findings.len() == 1 && !findings[0].supporting_evidence_ids.is_empty() {
                conclusion = HypothesisAssessmentConclusion::RootCause {
                    statement: findings[0].root_cause.clone(),
                    supporting_evidence_ids: findings[0].supporting_evidence_ids.clone(),
                    counter_evidence_ids: findings[0].counter_evidence_ids.clone(),
                };
            }
        } else if faults.is_empty()
            && saved
                .iter()
                .all(|revision| revision.status == InvestigationDiagnosisStatus::Healthy)
        {
            let mut supporting = basis
                .findings
                .iter()
                .flat_map(|finding| finding.supporting_evidence_ids.iter().copied())
                .collect::<Vec<_>>();
            supporting.sort();
            supporting.dedup();
            if !supporting.is_empty() {
                conclusion = HypothesisAssessmentConclusion::Healthy {
                    supporting_evidence_ids: supporting,
                };
            }
        }
    }
    Ok(InvestigationAggregateDecision::Assess {
        basis_revision_id: basis.id,
        conclusion,
        inconclusive,
    })
}

/// Checks current required quality and actual finding citations, without
/// requiring an unrelated optional source to exist.
#[must_use]
pub fn investigation_conclusion_evidence_available(
    revision: &SavedInvestigationRunRevision,
    current: &[DiagnosticRequirementQuality],
) -> bool {
    let required = current
        .iter()
        .filter(|requirement| requirement.required)
        .collect::<Vec<_>>();
    let available = current
        .iter()
        .filter(|requirement| requirement.quality == DiagnosticEvidenceQuality::Available)
        .flat_map(|requirement| requirement.evidence_ids.iter())
        .collect::<std::collections::BTreeSet<_>>();
    !required.is_empty()
        && required.iter().all(|requirement| {
            requirement.quality == DiagnosticEvidenceQuality::Available
                && requirement
                    .evidence_ids
                    .iter()
                    .all(|id| revision.evidence_ids.contains(id))
        })
        && revision
            .findings
            .iter()
            .flat_map(|finding| {
                finding
                    .supporting_evidence_ids
                    .iter()
                    .chain(&finding.counter_evidence_ids)
            })
            .all(|id| available.contains(id))
}
