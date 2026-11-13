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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::{
    DiagnosticEvidenceQuality, InvestigationRunStatus, InvestigationStepKind, InvestigationStepOutcome,
};

use super::quality::{exact_pack, input_fingerprint, requirements};
use super::*;

pub(super) fn plan(
    registry: &DiagnosticPackRegistry,
    input: InvestigationPlanningInput<'_>,
) -> Result<InvestigationPlanDecision, InvestigationRunFailure> {
    let run = input.run;
    run.validate()?;
    validate_inputs(registry, &input)?;
    if run.status != InvestigationRunStatus::Running {
        return Ok(InvestigationPlanDecision::Wait);
    }
    if input.as_of_unix_ms >= run.deadline.timestamp_millis() {
        return Ok(stop(InvestigationStopReason::DeadlineExceeded));
    }
    if run.in_flight_count() != 0 {
        return Ok(InvestigationPlanDecision::Wait);
    }
    let remaining = remaining(run)?;
    let packs: Vec<_> = run
        .effective_packs
        .iter()
        .map(|pack| Ok((pack, requirements(exact_pack(registry, pack)?, pack, &input))))
        .collect::<Result<_, InvestigationRunFailure>>()?;
    let mut groups: Vec<Vec<&InvestigationReadBinding>> = vec![];
    // Required evidence for every selected pack precedes optional follow-ups.
    for required in [true, false] {
        for (pack, qualities) in &packs {
            for requirement in qualities.iter().filter(|item| item.required == required) {
                let reference = InvestigationRequirementRef {
                    pack: (*pack).clone(),
                    requirement_id: requirement.id.clone(),
                };
                let Some(binding) = input
                    .bindings
                    .iter()
                    .find(|binding| binding.requirement == reference && binding.scope == run.effective_scope)
                else {
                    continue;
                };
                if let Some(group) = groups.iter_mut().find(|group| same_read(group[0], binding)) {
                    group.push(binding);
                } else {
                    groups.push(vec![binding]);
                }
            }
        }
    }
    let mut budget_blocked = false;
    for group in groups {
        let consumers: Vec<_> = group.iter().map(|binding| binding.requirement.clone()).collect();
        let complete = consumers.iter().all(|consumer| {
            packs
                .iter()
                .find(|(pack, _)| *pack == &consumer.pack)
                .and_then(|(_, qualities)| qualities.iter().find(|quality| quality.id == consumer.requirement_id))
                .is_some_and(|quality| quality.quality == DiagnosticEvidenceQuality::Available)
        });
        if complete {
            continue;
        }
        let history: Vec<_> = run
            .steps
            .iter()
            .filter(|step| {
                let InvestigationStepKind::CollectEvidence { requirement_id } = &step.spec.work else {
                    return false;
                };
                input.bindings.iter().any(|binding| {
                    binding.requirement.pack == step.spec.pack
                        && &binding.requirement.requirement_id == requirement_id
                        && binding.scope == step.spec.scope
                        && same_read(binding, group[0])
                })
            })
            .collect();
        if history.iter().any(|step| {
            step.attempts.iter().any(|attempt| {
                attempt
                    .result
                    .as_ref()
                    .is_some_and(|result| result.outcome == InvestigationStepOutcome::Succeeded)
            })
        }) {
            continue; // A successful read is not repeated merely because its fields were missing.
        }
        let previous = history.first().copied();
        let latest = history
            .iter()
            .flat_map(|step| &step.attempts)
            .max_by_key(|attempt| attempt.admitted_revision);
        if history
            .iter()
            .flat_map(|step| &step.attempts)
            .any(|attempt| attempt.interruption.is_some())
            || latest.is_some_and(|attempt| {
                !attempt.result.as_ref().is_some_and(|result| {
                    result.outcome == InvestigationStepOutcome::Failed
                        && result.failure == Some(rocketmq_sre_contracts::InvestigationStepFailure::SourceUnavailable)
                })
            })
        {
            continue;
        }
        if previous.is_some_and(|step| {
            step.attempts.len() >= usize::from(run.budget.max_attempts_per_step)
                || !run.effective_packs.contains(&step.spec.pack)
        }) {
            continue;
        }
        if previous.is_some() && run.query_retry_count()? >= u64::from(run.budget.max_query_retries) {
            budget_blocked = true;
            continue;
        }
        let reservation = InvestigationBudgetCost {
            read_queries: 1,
            model_calls: 0,
            evidence_items: 1,
            output_bytes: input.reservations.collection_output_bytes,
        };
        if !can_admit(run, reservation, remaining) {
            budget_blocked = true;
            continue;
        }
        let primary = group[0];
        let spec = previous.map_or_else(
            || InvestigationStepSpec {
                scope: primary.scope.clone(),
                pack: primary.requirement.pack.clone(),
                work: InvestigationStepKind::CollectEvidence {
                    requirement_id: primary.requirement.requirement_id.clone(),
                },
            },
            |step| step.spec.clone(),
        );
        return Ok(InvestigationPlanDecision::Collect(InvestigationPlannedCollection {
            spec,
            source: primary.source.clone(),
            resource: primary.resource.clone(),
            consumers,
            retry_step_id: previous.map(|step| step.id),
            reservation,
        }));
    }
    let mut incomplete = false;
    for (pack, qualities) in &packs {
        incomplete |= qualities
            .iter()
            .any(|requirement| requirement.required && requirement.quality != DiagnosticEvidenceQuality::Available);
        let mut evidence_ids: Vec<_> = qualities
            .iter()
            .flat_map(|requirement| requirement.evidence_ids.iter().copied())
            .collect();
        evidence_ids.sort();
        evidence_ids.dedup();
        let fingerprint = input_fingerprint(pack, qualities, &input)?;
        if run.current_pack_result(pack).is_some_and(|result| {
            let mut saved = result.evidence_ids.clone();
            saved.sort();
            saved == evidence_ids && result.input_fingerprint == fingerprint
        }) {
            continue;
        }
        if run.pack_results.iter().any(|result| {
            result.pack == **pack && result.scope == run.effective_scope && result.input_fingerprint == fingerprint
        }) {
            return Ok(stop(InvestigationStopReason::NoProgress));
        }
        let spec = InvestigationStepSpec {
            scope: run.effective_scope.clone(),
            pack: (*pack).clone(),
            work: InvestigationStepKind::EvaluateRules { evidence_ids },
        };
        let key = spec.semantic_key()?;
        let previous = run
            .steps
            .iter()
            .find(|step| step.spec.semantic_key().is_ok_and(|previous| previous == key));
        if let Some(previous) = previous {
            // D15 stores successful evaluation and saved R atomically. A legacy
            // completed step without current proof cannot be silently re-admitted.
            if previous.attempts.iter().any(|attempt| {
                attempt
                    .result
                    .as_ref()
                    .is_some_and(|result| result.outcome == InvestigationStepOutcome::Succeeded)
            }) {
                return Ok(stop(InvestigationStopReason::NoProgress));
            }
            return Ok(stop(InvestigationStopReason::NoProgress));
        }
        let reservation = InvestigationBudgetCost {
            output_bytes: input.reservations.evaluation_output_bytes,
            ..Default::default()
        };
        if !can_admit(run, reservation, remaining) {
            return Ok(stop(InvestigationStopReason::BudgetExhausted));
        }
        return Ok(InvestigationPlanDecision::Evaluate(InvestigationPlannedEvaluation {
            spec,
            input_fingerprint: fingerprint,
            requirements: qualities.clone(),
            reservation,
        }));
    }
    if input.explanation == InvestigationExplanationPolicy::ExplainOnce && !run.evidence_ids.is_empty() {
        let attempted = run.steps.iter().any(|step| {
            step.spec.scope == run.effective_scope
                && matches!(step.spec.work, InvestigationStepKind::ExplainEvidence { .. })
        });
        let reservation = InvestigationBudgetCost {
            model_calls: 1,
            output_bytes: input.reservations.explanation_output_bytes,
            ..Default::default()
        };
        if !attempted && can_admit(run, reservation, remaining) {
            let mut evidence_ids = run.evidence_ids.clone();
            evidence_ids.sort();
            return Ok(InvestigationPlanDecision::Explain(InvestigationPlannedExplanation {
                spec: InvestigationStepSpec {
                    scope: run.effective_scope.clone(),
                    pack: run.effective_packs[0].clone(),
                    work: InvestigationStepKind::ExplainEvidence { evidence_ids },
                },
                reservation,
            }));
        }
    }
    Ok(InvestigationPlanDecision::Finalize {
        stop_reason: if incomplete {
            Some(if budget_blocked {
                InvestigationStopReason::BudgetExhausted
            } else {
                InvestigationStopReason::EvidenceInsufficient
            })
        } else {
            None
        },
    })
}

pub(super) fn validate_inputs(
    registry: &DiagnosticPackRegistry,
    input: &InvestigationPlanningInput<'_>,
) -> Result<(), InvestigationRunFailure> {
    if input.bindings.len() > 512
        || input.evidence.len() > 256
        || input.as_of_unix_ms < input.run.updated_at.timestamp_millis()
        || input.reservations.collection_output_bytes == 0
        || input.reservations.evaluation_output_bytes == 0
        || input.reservations.explanation_output_bytes == 0
    {
        return Err(InvestigationRunFailure::InvalidInput);
    }
    for pack in &input.run.effective_packs {
        exact_pack(registry, pack)?;
    }
    let mut seen = BTreeSet::new();
    for binding in input.bindings {
        if !input.run.initial_packs.contains(&binding.requirement.pack)
            || !binding.scope.narrows(&input.run.initial_scope)
        {
            return Err(InvestigationRunFailure::ScopeMismatch);
        }
        let pack = exact_pack(registry, &binding.requirement.pack)?;
        let requirement = pack
            .required_evidence()
            .iter()
            .chain(pack.optional_evidence())
            .find(|requirement| requirement.key == binding.requirement.requirement_id)
            .ok_or(InvestigationRunFailure::PackNotAllowed)?;
        if binding.source != requirement.source
            || !binding.resource.starts_with(requirement.resource_prefix)
            || binding.resource.len() <= requirement.resource_prefix.len()
            || binding.resource.len() > 512
            || binding.resource.contains("://")
            || binding.resource.chars().any(char::is_control)
        {
            return Err(InvestigationRunFailure::ScopeMismatch);
        }
        let key = rocketmq_sre_contracts::canonical_sha256(&(&binding.requirement, &binding.scope))
            .map_err(|_| InvestigationRunFailure::InvalidInput)?;
        if !seen.insert(key) {
            return Err(InvestigationRunFailure::InvalidInput);
        }
    }
    if input.evidence.iter().any(|snapshot| {
        snapshot.tenant_id != input.run.initial_scope.tenant_id
            || snapshot.cluster_id != input.run.initial_scope.cluster_id
    }) {
        return Err(InvestigationRunFailure::ScopeMismatch);
    }
    Ok(())
}
fn same_read(left: &InvestigationReadBinding, right: &InvestigationReadBinding) -> bool {
    left.source == right.source && left.resource == right.resource && left.scope == right.scope
}
fn stop(reason: InvestigationStopReason) -> InvestigationPlanDecision {
    InvestigationPlanDecision::Stop { reason }
}
fn remaining(run: &InvestigationRun) -> Result<InvestigationBudgetCost, InvestigationRunFailure> {
    let (used, reserved) = run.budget_usage()?;
    fn meter(limit: u64, used: u64, reserved: u64) -> Result<u64, InvestigationRunFailure> {
        limit
            .checked_sub(
                used.checked_add(reserved)
                    .ok_or(InvestigationRunFailure::CounterExhausted)?,
            )
            .ok_or(InvestigationRunFailure::BudgetExceeded)
    }
    Ok(InvestigationBudgetCost {
        read_queries: meter(run.budget.limit.read_queries, used.read_queries, reserved.read_queries)?,
        model_calls: meter(run.budget.limit.model_calls, used.model_calls, reserved.model_calls)?,
        evidence_items: meter(
            run.budget.limit.evidence_items,
            used.evidence_items,
            reserved.evidence_items,
        )?,
        output_bytes: meter(run.budget.limit.output_bytes, used.output_bytes, reserved.output_bytes)?,
    })
}
fn can_admit(run: &InvestigationRun, reservation: InvestigationBudgetCost, remaining: InvestigationBudgetCost) -> bool {
    reservation.output_bytes > 0
        && reservation.fits_within(remaining)
        && run.steps.iter().map(|step| step.attempts.len()).sum::<usize>() < run.budget.max_steps as usize
        && run.in_flight_count() < usize::from(run.budget.max_in_flight)
}
