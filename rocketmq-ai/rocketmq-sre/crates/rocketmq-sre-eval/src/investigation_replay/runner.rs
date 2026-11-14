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

use rocketmq_sre_contracts::*;
use rocketmq_sre_core::diagnostics::*;
use serde::{Deserialize, Serialize};

use super::validation::{COLLECTION_BYTES, LOCAL_BYTES, context, create_request, json_len};
use super::*;

mod execution;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    run: InvestigationRun,
    revisions: Vec<SavedInvestigationRunRevision>,
    evidence: Vec<EvidenceSnapshot>,
    reads: Vec<ReplayReadObservation>,
    model_callbacks: u32,
    model_invocations: u32,
    accepted_output_bytes: u64,
    checkpointed: bool,
    refined: bool,
}

pub(super) fn run(
    case: &InvestigationReplayCase,
    arm: ReplayArm,
) -> Result<InvestigationReplayArm, InvestigationReplayRejection> {
    let packs = match arm {
        ReplayArm::SinglePack => vec![case.baseline_pack.clone()],
        ReplayArm::MultiPack => case.packs.clone(),
    };
    let authority = context(case, &packs, &[])?;
    let mut run = InvestigationRun::create(&authority, &create_request(case, &packs), None, case.as_of)?.run;
    run.start(&authority, run.revision, case.as_of)?;
    let bindings = case
        .bindings
        .iter()
        .filter(|binding| packs.contains(&binding.requirement.pack))
        .map(|binding| InvestigationReadBinding {
            requirement: binding.requirement.clone(),
            source: binding.source.clone(),
            resource: binding.resource.clone(),
            scope: case.scope.clone(),
        })
        .collect::<Vec<_>>();
    let registry = full_registry().map_err(|_| InvestigationReplayRejection::UnknownPack)?;
    let mut state = State {
        run,
        revisions: vec![],
        evidence: vec![],
        reads: vec![],
        model_callbacks: 0,
        model_invocations: 0,
        accepted_output_bytes: 0,
        checkpointed: false,
        refined: false,
    };
    // Match the dispatcher decision-loop guard. The pure admission caps remain
    // authoritative; reaching this guard persists a typed no-progress stop.
    for _ in 0..64 {
        let decision = plan_investigation(&registry, input(case, &state, &bindings))?;
        match decision {
            InvestigationPlanDecision::Collect(plan) => {
                state.collect(case, plan)?;
                if case.checkpoint == ReplayCheckpoint::AfterFirstCollection
                    && !state.checkpointed
                    && !state.evidence.is_empty()
                {
                    state = restore_checkpoint(case, state)?;
                }
            }
            InvestigationPlanDecision::Evaluate(plan) => {
                state.evaluate(case, plan)?;
                if case.refinement == ReplayRefinement::ReassessSameFactsOnce
                    && !state.refined
                    && state.run.status == InvestigationRunStatus::Running
                {
                    state.reassess(case)?;
                }
            }
            InvestigationPlanDecision::Explain(plan) => {
                state.explain(case, plan)?;
                // The runtime preserves the unknown model reservation and
                // finalizes actual saved rules instead of losing their result.
                if state.run.status == InvestigationRunStatus::NeedsEvidence {
                    state.finalize(case, &registry, &bindings, None)?;
                    break;
                }
            }
            InvestigationPlanDecision::Finalize { stop_reason } => {
                state.finalize(case, &registry, &bindings, stop_reason)?;
                break;
            }
            InvestigationPlanDecision::Stop { reason } => {
                state.stop(case, reason)?;
                break;
            }
            InvestigationPlanDecision::Wait => break,
        }
    }
    if state.run.status == InvestigationRunStatus::Running {
        state.stop(case, InvestigationStopReason::NoProgress)?;
    }
    state.run.validate()?;
    let (settled, reserved) = state.run.budget_usage()?;
    let known_reported_cost = state
        .run
        .steps
        .iter()
        .flat_map(|step| &step.attempts)
        .filter_map(|attempt| attempt.result.as_ref())
        .try_fold(InvestigationBudgetCost::default(), |sum, result| {
            Ok::<_, InvestigationReplayRejection>(InvestigationBudgetCost {
                read_queries: sum
                    .read_queries
                    .checked_add(result.actual_cost.read_queries)
                    .ok_or(InvestigationReplayRejection::ContractRejected)?,
                model_calls: sum
                    .model_calls
                    .checked_add(result.actual_cost.model_calls)
                    .ok_or(InvestigationReplayRejection::ContractRejected)?,
                evidence_items: sum
                    .evidence_items
                    .checked_add(result.actual_cost.evidence_items)
                    .ok_or(InvestigationReplayRejection::ContractRejected)?,
                output_bytes: sum
                    .output_bytes
                    .checked_add(result.actual_cost.output_bytes)
                    .ok_or(InvestigationReplayRejection::ContractRejected)?,
            })
        })?;
    let read_callbacks = state
        .reads
        .iter()
        .try_fold(0_u32, |count, read| count.checked_add(read.callbacks))
        .ok_or(InvestigationReplayRejection::ContractRejected)?;
    let mut evidence_ids = state.run.evidence_ids.clone();
    evidence_ids.sort();
    let findings = state
        .run
        .effective_packs
        .iter()
        .filter_map(|pack| state.run.current_pack_result(pack))
        .filter_map(|result| {
            state
                .revisions
                .iter()
                .find(|revision| revision.id == result.run_revision_id)
        })
        .flat_map(|revision| revision.findings.clone())
        .collect();
    let observation = ReplayObservation {
        scope: state.run.effective_scope.clone(),
        as_of: case.as_of,
        packs: state.run.effective_packs.clone(),
        status: state.run.status,
        stop_reason: state.run.stop_reason,
        conclusion: state
            .run
            .assessments
            .last()
            .map(|assessment| assessment.conclusion.clone()),
        evidence_ids,
        findings,
    };
    let digest = canonical_sha256(&observation).map_err(|_| InvestigationReplayRejection::ContractRejected)?;
    Ok(InvestigationReplayArm {
        arm,
        observation,
        observation_digest: digest,
        read_observations: state.reads,
        read_callbacks,
        model_callbacks: state.model_callbacks,
        model_invocations: state.model_invocations,
        accepted_output_bytes: state.accepted_output_bytes,
        known_reported_cost,
        settled,
        reserved,
        serialized_checkpoint_restored: state.checkpointed,
        snapshot: state.run,
        revisions: state.revisions,
    })
}

fn input<'a>(
    case: &InvestigationReplayCase,
    state: &'a State,
    bindings: &'a [InvestigationReadBinding],
) -> InvestigationPlanningInput<'a> {
    InvestigationPlanningInput {
        run: &state.run,
        bindings,
        evidence: &state.evidence,
        as_of_unix_ms: case.as_of.timestamp_millis(),
        explanation: if case.model == ReplayModel::Disabled {
            InvestigationExplanationPolicy::RuleOnly
        } else {
            InvestigationExplanationPolicy::ExplainOnce
        },
        reservations: InvestigationReservationPolicy {
            collection_output_bytes: COLLECTION_BYTES,
            evaluation_output_bytes: LOCAL_BYTES,
            explanation_output_bytes: LOCAL_BYTES,
        },
    }
}

fn restore_checkpoint(case: &InvestigationReplayCase, mut state: State) -> Result<State, InvestigationReplayRejection> {
    use InvestigationReplayRejection::InvalidCheckpoint as Invalid;
    state.checkpointed = true;
    json_len(&state, 16 * 1024 * 1024)?;
    let encoded = serde_json::to_vec(&state).map_err(|_| Invalid)?;
    let restored: State = serde_json::from_slice(&encoded).map_err(|_| Invalid)?;
    restored.run.validate().map_err(|_| Invalid)?;
    if restored.run.initial_scope != case.scope
        || restored.run.budget != case.budget
        || restored.run.deadline != case.deadline
        || restored.run.in_flight_count() != 0
        || restored.evidence.iter().any(|snapshot| {
            snapshot.verify_content_hash().is_err() || !restored.run.evidence_ids.contains(&snapshot.evidence_id)
        })
        || restored.revisions.iter().any(|revision| {
            revision.pack_result().is_err()
                || !restored
                    .run
                    .pack_results
                    .iter()
                    .any(|result| revision.pack_result().is_ok_and(|expected| expected == *result))
        })
    {
        return Err(Invalid);
    }
    Ok(restored)
}

impl State {
    fn authority(
        &self,
        case: &InvestigationReplayCase,
    ) -> Result<InvestigationRunContext, InvestigationReplayRejection> {
        context(case, &self.run.initial_packs, &self.evidence)
    }

    fn admit(
        &mut self,
        case: &InvestigationReplayCase,
        spec: InvestigationStepSpec,
        reservation: InvestigationBudgetCost,
        retry: Option<InvestigationStepId>,
    ) -> Result<InvestigationStepAdmission, InvestigationReplayRejection> {
        let admission = InvestigationStepAdmission {
            step_id: retry.unwrap_or_default(),
            attempt_id: StepAttemptId::new(),
            expected_revision: self.run.revision,
            spec,
            reservation,
        };
        self.run.admit_step(&self.authority(case)?, &admission, case.as_of)?;
        Ok(admission)
    }

    fn interrupt(
        &mut self,
        case: &InvestigationReplayCase,
        admission: &InvestigationStepAdmission,
        reason: InvestigationInterruptionReason,
    ) -> Result<(), InvestigationReplayRejection> {
        self.run.interrupt_with_reason(
            &self.authority(case)?,
            InvestigationInterruptAttemptRequest {
                expected_revision: self.run.revision,
                step_id: admission.step_id,
                attempt_id: admission.attempt_id,
                reason,
            },
            case.as_of,
        )?;
        Ok(())
    }

    fn stop(
        &mut self,
        case: &InvestigationReplayCase,
        reason: InvestigationStopReason,
    ) -> Result<(), InvestigationReplayRejection> {
        self.run
            .stop_with_reason(&self.authority(case)?, self.run.revision, reason, case.as_of)?;
        Ok(())
    }

    fn reassess(&mut self, case: &InvestigationReplayCase) -> Result<(), InvestigationReplayRejection> {
        let authority = self.authority(case)?;
        self.run.apply_command(
            &authority,
            &InvestigationCommandRequest {
                schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
                command_id: InvestigationCommandId::new(),
                run_id: self.run.id,
                expected_revision: self.run.revision,
                idempotency_key: "synthetic-repeat-facts".to_owned(),
                reason: "Explicit fixture-only same-fact reassessment".to_owned(),
                command: InvestigationCommand::Steer {
                    refinement: InvestigationRefinement {
                        scope: self.run.effective_scope.clone(),
                        packs: self.run.effective_packs.clone(),
                        evidence_ids: self.run.evidence_ids.clone(),
                    },
                },
            },
            None,
            case.as_of,
        )?;
        if self.run.status == InvestigationRunStatus::Paused {
            self.run.apply_command(
                &authority,
                &InvestigationCommandRequest {
                    schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
                    command_id: InvestigationCommandId::new(),
                    run_id: self.run.id,
                    expected_revision: self.run.revision,
                    idempotency_key: "synthetic-resume".to_owned(),
                    reason: "Continue the explicit replay fixture".to_owned(),
                    command: InvestigationCommand::Resume {},
                },
                None,
                case.as_of,
            )?;
        }
        self.refined = true;
        Ok(())
    }

    fn finalize(
        &mut self,
        case: &InvestigationReplayCase,
        registry: &DiagnosticPackRegistry,
        bindings: &[InvestigationReadBinding],
        reason: Option<InvestigationStopReason>,
    ) -> Result<(), InvestigationReplayRejection> {
        match aggregate_investigation(registry, &input(case, self, bindings), &self.revisions, reason)? {
            InvestigationAggregateDecision::Stop { reason } => self.stop(case, reason),
            InvestigationAggregateDecision::Assess {
                basis_revision_id,
                conclusion,
                inconclusive,
            } => {
                let basis = self
                    .revisions
                    .iter()
                    .find(|revision| revision.id == basis_revision_id)
                    .ok_or(InvestigationReplayRejection::ContractRejected)?;
                let mut assessment = HypothesisAssessment {
                    schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
                    id: HypothesisAssessmentId::new(),
                    run_id: self.run.id,
                    scope: self.run.effective_scope.clone(),
                    control_epoch: self.run.control_epoch,
                    diagnosis_revision_id: None,
                    run_revision_id: Some(basis.id),
                    conclusion,
                    summary: "Fixed-evidence replay of saved deterministic results; no execution authority.".to_owned(),
                    created_at: case.as_of,
                };
                if self.run.validate_planned_conclusion(&assessment, basis).is_err() {
                    assessment.conclusion = inconclusive;
                }
                self.run.finish_planned_assessment(
                    &self.authority(case)?,
                    self.run.revision,
                    &assessment,
                    basis,
                    case.as_of,
                )?;
                Ok(())
            }
        }
    }
}
