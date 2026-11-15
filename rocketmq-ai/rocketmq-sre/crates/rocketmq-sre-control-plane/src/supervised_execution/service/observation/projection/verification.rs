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

use crate::supervised_execution::repository::{ObservationJournal, VerificationRecord};
use rocketmq_sre_contracts::{
    ExecutionObservationReason as Reason, ExecutionVerificationObservation, ExecutionWindowProofState as ProofState,
    PlanStep, StepIntent, VerificationOutcome, VerificationProofOrigin, VerificationResult, VerificationSpec,
};
use std::collections::BTreeSet;

mod proof;

pub(super) fn project(
    journal: &ObservationJournal,
    step: &PlanStep,
    declared: Option<&VerificationSpec>,
) -> (Vec<ExecutionVerificationObservation>, bool) {
    let mut partial = false;
    let allowed: BTreeSet<_> = declared
        .into_iter()
        .flat_map(|spec| {
            spec.resource_conditions
                .iter()
                .map(|name| format!("resource:{name}"))
                .chain(spec.technical_slis.iter().map(|name| format!("sli:{name}")))
        })
        .collect();
    let mut output = Vec::new();
    for row in &journal.verifications {
        let intent = journal
            .attempts
            .iter()
            .find(|record| record.kind == "intent" && record.step_id == row.step_id && record.attempt == row.attempt)
            .and_then(|record| record.snapshot.as_ref())
            .and_then(|value| serde_json::from_value::<StepIntent>(value.clone()).ok());
        let Some(intent) = intent.filter(|intent| intent.step.id == step.id) else {
            continue;
        };
        if intent.step != *step
            || intent.execution_id != journal.execution.request.id
            || intent.plan_hash != journal.execution.request.plan.plan_hash
            || intent.step_id != row.step_id
            || intent.compensation != row.compensation
        {
            partial = true;
            continue;
        }
        let Some(mut observation) = baseline(row, step) else {
            partial = true;
            continue;
        };
        let Some(encoded) = &row.snapshot else {
            observation.proof_state = ProofState::Unavailable;
            observation.reason_codes = vec![Reason::RetainedProofUnavailable];
            output.push(observation);
            partial = true;
            continue;
        };
        let result = serde_json::from_value::<VerificationResult>(encoded.clone());
        let Ok(result) = result else {
            observation.proof_state = ProofState::Invalid;
            observation.reason_codes = vec![Reason::WindowProofInvalid];
            output.push(observation);
            partial = true;
            continue;
        };
        if result.step_id != row.step_id
            || result.outcome != observation.recorded_outcome
            || result.started_at.timestamp_micros() != row.started_at.timestamp_micros()
            || result.completed_at.timestamp_micros() != row.completed_at.timestamp_micros()
            || result.completed_at < result.started_at
            || result.stable_window_seconds != step.verification.stable_window_seconds
        {
            observation.proof_state = ProofState::Invalid;
            observation.reason_codes = vec![Reason::WindowProofInvalid];
            output.push(observation);
            partial = true;
            continue;
        }
        observation.started_at = result.started_at;
        observation.completed_at = result.completed_at;
        // Even legacy saved condition strings are only exposed where they name
        // recognized exact compiled declarations. Raw stored text is never UI.
        observation.satisfied_conditions = safe_conditions(&result.satisfied_conditions, &allowed);
        observation.failed_conditions = safe_conditions(&result.failed_conditions, &allowed);
        partial |= result.satisfied_conditions.len() != observation.satisfied_conditions.len()
            || result.failed_conditions.len() != observation.failed_conditions.len();
        let ids: BTreeSet<_> = result
            .pre_evidence_ids
            .iter()
            .chain(&result.during_evidence_ids)
            .chain(&result.post_evidence_ids)
            .copied()
            .collect();
        partial |= ids.len() > 64;
        observation.evidence_ids = ids.into_iter().take(64).collect();
        match &result.window_proof {
            None => {
                observation.proof_state = ProofState::LegacyMissing;
                observation.reason_codes = vec![Reason::LegacyWindowProofMissing];
            }
            Some(saved) => match proof::validate(journal, row, step, &result, declared, saved) {
                Ok(()) => {
                    observation.proof_state = if result.outcome == VerificationOutcome::Inconclusive {
                        ProofState::Inconclusive
                    } else {
                        ProofState::ValidHistorical
                    };
                    observation.provenance = saved.provenance;
                    observation.observed_stable_seconds = Some(saved.observed_stable_seconds);
                    observation.sample_count = u32::try_from(saved.samples.len()).ok();
                    observation.source_first_at = saved
                        .samples
                        .iter()
                        .map(|sample| sample.resource_observed_at.min(sample.technical_sli_observed_at))
                        .min();
                    observation.source_last_at = saved
                        .samples
                        .iter()
                        .map(|sample| sample.resource_observed_at.min(sample.technical_sli_observed_at))
                        .max();
                    if let Some(sample) = saved.samples.last() {
                        observation.resource_observed_at = Some(sample.resource_observed_at);
                        observation.technical_sli_observed_at = Some(sample.technical_sli_observed_at);
                        observation.collected_at = Some(sample.collected_at);
                    }
                    if let Some(policy) = &saved.policy {
                        observation.max_wait_seconds = Some(policy.max_wait_seconds);
                        observation.max_source_age_seconds = Some(policy.max_source_age_seconds);
                        observation.max_sample_gap_seconds = Some(policy.max_sample_gap_seconds);
                        observation.policy_digest = Some(policy.policy_digest.clone());
                        observation.declared_target_version = Some(policy.declared_target_version.clone());
                        observation.target_version_verified = saved.provenance
                            == VerificationProofOrigin::ProductionRead
                            && saved.target_version_observed.as_ref() == Some(&policy.declared_target_version)
                            && !saved.samples.is_empty()
                            && saved.samples.iter().all(|sample| sample.target_version_matched);
                    }
                    observation.missing_conditions = saved
                        .missing_conditions
                        .iter()
                        .filter(|name| allowed.contains(*name))
                        .cloned()
                        .collect();
                    observation.reason_codes = saved.reason_codes.clone();
                }
                Err(reason) => {
                    observation.proof_state = if reason == Reason::RetainedProofUnavailable {
                        ProofState::Unavailable
                    } else {
                        ProofState::Invalid
                    };
                    observation.reason_codes = vec![reason];
                    partial = true;
                }
            },
        }
        output.push(observation);
    }
    (output, partial)
}

fn safe_conditions(values: &[String], allowed: &BTreeSet<String>) -> Vec<String> {
    values
        .iter()
        .filter(|name| allowed.contains(*name))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .take(64)
        .collect()
}

fn baseline(row: &VerificationRecord, step: &PlanStep) -> Option<ExecutionVerificationObservation> {
    let outcome = match row.outcome.as_str() {
        "succeeded" => VerificationOutcome::Succeeded,
        "failed" => VerificationOutcome::Failed,
        "inconclusive" => VerificationOutcome::Inconclusive,
        _ => return None,
    };
    Some(ExecutionVerificationObservation {
        step_id: row.step_id,
        attempt: u16::try_from(row.attempt).ok()?,
        compensation: row.compensation,
        recorded_outcome: outcome,
        proof_state: ProofState::NotObserved,
        provenance: VerificationProofOrigin::RecordedOnly,
        started_at: row.started_at,
        completed_at: row.completed_at,
        required_stable_window_seconds: step.verification.stable_window_seconds,
        observed_stable_seconds: None,
        max_wait_seconds: None,
        sample_count: None,
        max_source_age_seconds: None,
        max_sample_gap_seconds: None,
        source_first_at: None,
        source_last_at: None,
        resource_observed_at: None,
        technical_sli_observed_at: None,
        collected_at: None,
        policy_digest: None,
        satisfied_conditions: Vec::new(),
        failed_conditions: Vec::new(),
        missing_conditions: Vec::new(),
        declared_target_version: None,
        target_version_verified: false,
        evidence_ids: Vec::new(),
        reason_codes: Vec::new(),
    })
}
