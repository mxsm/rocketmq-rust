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

//! Pure planning over a persisted run and service-authorized canonical reads.

use rocketmq_sre_contracts::{
    DiagnosticRequirementQuality, EvidenceSnapshot, InvestigationBudgetCost, InvestigationPackRef,
    InvestigationRequirementRef, InvestigationRun, InvestigationRunFailure, InvestigationRunScope, InvestigationStepId,
    InvestigationStepSpec, InvestigationStopReason,
};

use super::{DiagnosticPackRegistry, DiagnosticReport, FindingOutcome};

mod aggregation;
mod planning;
mod quality;
pub use aggregation::{
    InvestigationAggregateDecision, aggregate_investigation, investigation_conclusion_evidence_available,
};

/// Trusted service resolution of a fixed compiled requirement. No URL, query
/// expression, credentials or free-form tool name is accepted by the planner.
/// Include historical admitted collection bindings so removing a consuming pack
/// cannot reset shared-read accounting or redispatch completed work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestigationReadBinding {
    pub requirement: InvestigationRequirementRef,
    pub source: String,
    pub resource: String,
    pub scope: InvestigationRunScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvestigationExplanationPolicy {
    RuleOnly,
    /// An unavailable model remains rule-only; it is not repeatedly retried.
    ExplainOnce,
}

/// Bounds supplied by the service and enforced again before actual I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvestigationReservationPolicy {
    pub collection_output_bytes: u64,
    pub evaluation_output_bytes: u64,
    pub explanation_output_bytes: u64,
}

/// This input is intentionally not deserializable from a browser or model.
/// Canonical bindings and saved Evidence require fresh service authorization.
/// Time is explicit, keeping Core independent of clocks and runtime crates.
pub struct InvestigationPlanningInput<'a> {
    pub run: &'a InvestigationRun,
    pub bindings: &'a [InvestigationReadBinding],
    pub evidence: &'a [EvidenceSnapshot],
    pub as_of_unix_ms: i64,
    pub explanation: InvestigationExplanationPolicy,
    pub reservations: InvestigationReservationPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestigationPlannedCollection {
    pub spec: InvestigationStepSpec,
    pub source: String,
    pub resource: String,
    /// Every consumer shares one exact source/resource/scope/window read.
    pub consumers: Vec<InvestigationRequirementRef>,
    pub retry_step_id: Option<InvestigationStepId>,
    pub reservation: InvestigationBudgetCost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestigationPlannedEvaluation {
    pub spec: InvestigationStepSpec,
    pub input_fingerprint: String,
    pub requirements: Vec<DiagnosticRequirementQuality>,
    pub reservation: InvestigationBudgetCost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestigationPlannedExplanation {
    pub spec: InvestigationStepSpec,
    pub reservation: InvestigationBudgetCost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvestigationPlanDecision {
    Collect(InvestigationPlannedCollection),
    Evaluate(InvestigationPlannedEvaluation),
    Explain(InvestigationPlannedExplanation),
    /// Rules and persisted per-pack results are ready for conservative synthesis.
    Finalize {
        stop_reason: Option<InvestigationStopReason>,
    },
    /// Paused, queued, draining, terminal, or waiting for an admitted attempt.
    Wait,
    Stop {
        reason: InvestigationStopReason,
    },
}

/// Plans one bounded action across all explicitly selected compiled packs.
///
/// # Errors
/// Rejects malformed runs, ambiguous/unbound canonical inputs, unknown exact
/// pack versions and counter overflow. It never performs I/O or grants authority.
pub fn plan_investigation(
    registry: &DiagnosticPackRegistry,
    input: InvestigationPlanningInput<'_>,
) -> Result<InvestigationPlanDecision, InvestigationRunFailure> {
    planning::plan(registry, input)
}

/// Rechecks the same compiled field, exact-resource, window and age contracts
/// immediately before finalization, including after a model wait.
///
/// # Errors
/// Rejects unselected versions and malformed/unbound planner inputs.
pub fn investigation_requirements(
    registry: &DiagnosticPackRegistry,
    pack: &InvestigationPackRef,
    input: &InvestigationPlanningInput<'_>,
) -> Result<Vec<DiagnosticRequirementQuality>, InvestigationRunFailure> {
    planning::validate_inputs(registry, input)?;
    if !input.run.effective_packs.contains(pack) {
        return Err(InvestigationRunFailure::PackNotAllowed);
    }
    Ok(quality::requirements(quality::exact_pack(registry, pack)?, pack, input))
}

/// Copies only actual deterministic findings and their explicit citations.
#[must_use]
pub fn investigation_findings(report: &DiagnosticReport) -> Vec<rocketmq_sre_contracts::InvestigationRunFinding> {
    report
        .findings
        .iter()
        .map(|finding| rocketmq_sre_contracts::InvestigationRunFinding {
            reason_code: finding.reason_code.clone(),
            root_cause: finding.root_cause.clone(),
            outcome: match finding.outcome {
                FindingOutcome::Healthy => rocketmq_sre_contracts::InvestigationDiagnosisStatus::Healthy,
                FindingOutcome::Fault => rocketmq_sre_contracts::InvestigationDiagnosisStatus::Fault,
                FindingOutcome::Inconclusive => rocketmq_sre_contracts::InvestigationDiagnosisStatus::Inconclusive,
            },
            supporting_evidence_ids: finding
                .supporting_evidence
                .iter()
                .map(|reference| reference.evidence_id)
                .collect(),
            counter_evidence_ids: finding
                .counter_evidence
                .iter()
                .map(|reference| reference.evidence_id)
                .collect(),
        })
        .collect()
}
