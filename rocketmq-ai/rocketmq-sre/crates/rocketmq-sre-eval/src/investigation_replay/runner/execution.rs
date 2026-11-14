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

use super::super::validation::exact_pack;
use super::*;

struct KnownOutcome {
    outcome: InvestigationStepOutcome,
    failure: Option<InvestigationStepFailure>,
    ids: Vec<EvidenceId>,
    cost: InvestigationBudgetCost,
}

impl KnownOutcome {
    fn succeeded(ids: Vec<EvidenceId>, cost: InvestigationBudgetCost) -> Self {
        Self {
            outcome: InvestigationStepOutcome::Succeeded,
            failure: None,
            ids,
            cost,
        }
    }
    fn failed(failure: InvestigationStepFailure, cost: InvestigationBudgetCost) -> Self {
        Self {
            outcome: InvestigationStepOutcome::Failed,
            failure: Some(failure),
            ids: vec![],
            cost,
        }
    }
}

impl State {
    pub(super) fn collect(
        &mut self,
        case: &InvestigationReplayCase,
        plan: InvestigationPlannedCollection,
    ) -> Result<(), InvestigationReplayRejection> {
        let admission = self.admit(case, plan.spec, plan.reservation, plan.retry_step_id)?;
        let index = if let Some(index) = self
            .reads
            .iter()
            .position(|read| read.source == plan.source && read.resource == plan.resource)
        {
            index
        } else {
            self.reads.push(ReplayReadObservation {
                source: plan.source.clone(),
                resource: plan.resource.clone(),
                callbacks: 0,
            });
            self.reads.len() - 1
        };
        let position = self.reads[index].callbacks as usize;
        // This is the actual closed fixture callback, reached only after the
        // production run accepted its reservation. There is no network escape.
        self.reads[index].callbacks = self.reads[index]
            .callbacks
            .checked_add(1)
            .ok_or(InvestigationReplayRejection::ContractRejected)?;
        let outcome = case
            .reads
            .iter()
            .find(|read| read.source == plan.source && read.resource == plan.resource)
            .and_then(|read| read.outcomes.get(position).or_else(|| read.outcomes.last()))
            .cloned()
            .unwrap_or(ReplayReadOutcome::Rejected {});
        let sequence = self.reads.iter().map(|read| u64::from(read.callbacks)).sum();
        let response_bytes = if matches!(outcome, ReplayReadOutcome::OutcomeUnknown {}) {
            0
        } else {
            json_len(
                &super::super::validation::response_frame(&outcome, sequence),
                admission.reservation.output_bytes,
            )?
        };
        let report = match outcome {
            ReplayReadOutcome::SavedEvidence { snapshot } => {
                let new_id = !self.evidence.iter().any(|old| old.evidence_id == snapshot.evidence_id);
                let id = snapshot.evidence_id;
                if new_id {
                    self.evidence.push(*snapshot);
                }
                KnownOutcome::succeeded(
                    vec![id],
                    InvestigationBudgetCost {
                        read_queries: 1,
                        evidence_items: u64::from(new_id),
                        output_bytes: response_bytes,
                        ..Default::default()
                    },
                )
            }
            ReplayReadOutcome::RetryableUnavailable {} => KnownOutcome::failed(
                InvestigationStepFailure::SourceUnavailable,
                InvestigationBudgetCost {
                    read_queries: 1,
                    output_bytes: response_bytes,
                    ..Default::default()
                },
            ),
            ReplayReadOutcome::Rejected {} => KnownOutcome::failed(
                InvestigationStepFailure::SourceRejected,
                InvestigationBudgetCost {
                    read_queries: 1,
                    output_bytes: response_bytes,
                    ..Default::default()
                },
            ),
            ReplayReadOutcome::OutcomeUnknown {} => {
                return self.interrupt(case, &admission, InvestigationInterruptionReason::SourceOutcomeUnknown);
            }
        };
        self.settle_known(case, &admission, report)
    }

    pub(super) fn evaluate(
        &mut self,
        case: &InvestigationReplayCase,
        plan: InvestigationPlannedEvaluation,
        bindings: &[InvestigationReadBinding],
    ) -> Result<(), InvestigationReplayRejection> {
        let admission = self.admit(case, plan.spec, plan.reservation, None)?;
        let InvestigationStepKind::EvaluateRules { evidence_ids } = &admission.spec.work else {
            return Err(InvestigationReplayRejection::ContractRejected);
        };
        let selected = self
            .evidence
            .iter()
            .filter(|snapshot| evidence_ids.contains(&snapshot.evidence_id))
            .cloned()
            .collect::<Vec<_>>();
        let registry = full_registry().map_err(|_| InvestigationReplayRejection::UnknownPack)?;
        let pack = exact_pack(&registry, &admission.spec.pack)?;
        let engine = DiagnosticEngine::new(full_registry().map_err(|_| InvestigationReplayRejection::UnknownPack)?);
        let input = InvestigationPlanningInput {
            evidence: &selected,
            ..input(case, self, bindings)
        };
        let report = match investigation_rule_inputs(&registry, &admission.spec.pack, &input)
            .map_err(|_| ())
            .and_then(|inputs| {
                engine
                    .evaluate_version(pack.id(), pack.version(), &inputs)
                    .map_err(|_| ())
            }) {
            Ok(report) => report,
            Err(_) => {
                self.settle_known(
                    case,
                    &admission,
                    KnownOutcome::failed(
                        InvestigationStepFailure::EvidenceInsufficient,
                        InvestigationBudgetCost::default(),
                    ),
                )?;
                return self.stop(case, InvestigationStopReason::EvidenceInsufficient);
            }
        };
        let partial = plan
            .requirements
            .iter()
            .any(|quality| quality.required && quality.quality != DiagnosticEvidenceQuality::Available);
        let status = if partial {
            InvestigationDiagnosisStatus::Inconclusive
        } else {
            match report.status {
                DiagnosticStatus::Healthy => InvestigationDiagnosisStatus::Healthy,
                DiagnosticStatus::Fault => InvestigationDiagnosisStatus::Fault,
                DiagnosticStatus::Inconclusive => InvestigationDiagnosisStatus::Inconclusive,
                DiagnosticStatus::Unsupported => InvestigationDiagnosisStatus::Unsupported,
            }
        };
        let revision = SavedInvestigationRunRevision {
            schema_version: INVESTIGATION_RUN_REVISION_SCHEMA_VERSION.to_owned(),
            id: InvestigationRunRevisionId::new(),
            run_id: self.run.id,
            scope: admission.spec.scope.clone(),
            control_epoch: self.run.control_epoch,
            pack: admission.spec.pack.clone(),
            step_id: admission.step_id,
            attempt_id: admission.attempt_id,
            status,
            partial,
            evidence_ids: evidence_ids.clone(),
            requirements: plan.requirements,
            findings: investigation_findings(&report),
            input_fingerprint: plan.input_fingerprint,
            created_at: case.as_of,
        };
        let bytes = match json_len(&revision, admission.reservation.output_bytes) {
            Ok(bytes) => bytes,
            Err(InvestigationReplayRejection::InputTooLarge) => {
                self.settle_known(
                    case,
                    &admission,
                    KnownOutcome::failed(
                        InvestigationStepFailure::EvidenceInsufficient,
                        InvestigationBudgetCost::default(),
                    ),
                )?;
                return self.stop(case, InvestigationStopReason::BudgetExhausted);
            }
            Err(error) => return Err(error),
        };
        self.settle_known(
            case,
            &admission,
            KnownOutcome::succeeded(
                evidence_ids.clone(),
                InvestigationBudgetCost {
                    output_bytes: bytes,
                    ..Default::default()
                },
            ),
        )?;
        self.run
            .record_pack_result(&self.authority(case)?, self.run.revision, &revision, case.as_of)?;
        self.revisions.push(revision);
        Ok(())
    }

    pub(super) fn explain(
        &mut self,
        case: &InvestigationReplayCase,
        plan: InvestigationPlannedExplanation,
    ) -> Result<(), InvestigationReplayRejection> {
        let admission = self.admit(case, plan.spec, plan.reservation, None)?;
        self.model_callbacks = self
            .model_callbacks
            .checked_add(1)
            .ok_or(InvestigationReplayRejection::ContractRejected)?;
        match case.model {
            ReplayModel::Disabled => Err(InvestigationReplayRejection::ContractRejected),
            ReplayModel::NoEligibleProvider => self.settle_known(
                case,
                &admission,
                KnownOutcome::failed(
                    InvestigationStepFailure::ModelUnavailable,
                    InvestigationBudgetCost::default(),
                ),
            ),
            ReplayModel::InvokedFailure => {
                self.model_invocations += 1;
                self.settle_known(
                    case,
                    &admission,
                    KnownOutcome::failed(
                        InvestigationStepFailure::ModelUnavailable,
                        InvestigationBudgetCost {
                            model_calls: 1,
                            ..Default::default()
                        },
                    ),
                )
            }
            ReplayModel::OutcomeUnknown => {
                self.model_invocations += 1;
                self.interrupt(case, &admission, InvestigationInterruptionReason::ModelOutcomeUnknown)
            }
        }
    }

    fn settle_known(
        &mut self,
        case: &InvestigationReplayCase,
        admission: &InvestigationStepAdmission,
        report: KnownOutcome,
    ) -> Result<(), InvestigationReplayRejection> {
        let result = InvestigationStepResult {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            run_id: self.run.id,
            step_id: admission.step_id,
            attempt_id: admission.attempt_id,
            control_epoch: self.run.control_epoch,
            outcome: report.outcome,
            failure: report.failure,
            evidence_ids: report.ids,
            actual_cost: report.cost,
            finished_at: case.as_of,
        };
        self.run
            .apply_step_result(&self.authority(case)?, &result, case.as_of)?;
        self.accepted_output_bytes = self
            .accepted_output_bytes
            .checked_add(report.cost.output_bytes)
            .ok_or(InvestigationReplayRejection::ContractRejected)?;
        Ok(())
    }
}
