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

use super::*;

impl RunDispatcher {
    pub(super) async fn evaluate(
        &self,
        execution: &RunExecution<'_>,
        run: &InvestigationRun,
        plan: InvestigationPlannedEvaluation,
        bindings: &[InvestigationReadBinding],
        evidence: &[EvidenceSnapshot],
    ) -> Result<(), ControlPlaneRequestFailure> {
        let RunExecution {
            service,
            delegation,
            lease,
            ..
        } = *execution;
        let auth = delegation.read_auth(run)?;
        let context = self.context(service, auth, run, &[]).await?;
        let worker = RunWorkerContext {
            auth,
            context: &context,
            fence: &lease.fence,
        };
        let admission = InvestigationStepAdmission {
            step_id: InvestigationStepId::new(),
            attempt_id: StepAttemptId::new(),
            expected_revision: run.revision,
            spec: plan.spec,
            reservation: plan.reservation,
        };
        let admitted = service
            .repository
            .investigation_run_dispatch_admit(
                delegation,
                &worker,
                &service.catalog,
                run.id,
                &admission,
                RunAdmissionProof::Rules,
            )
            .await?;
        let InvestigationStepKind::EvaluateRules { evidence_ids } = &admission.spec.work else {
            return Err(contract_failure(InvestigationRunFailure::InvalidInput));
        };
        let selected = evidence
            .iter()
            .filter(|snapshot| evidence_ids.contains(&snapshot.evidence_id))
            .cloned()
            .collect::<Vec<_>>();
        let engine = DiagnosticEngine::new(
            full_registry().map_err(|_| ControlPlaneRequestFailure::configuration("diagnostic registry rejected"))?,
        );
        let exact = super::super::bindings::exact_pack(&service.catalog, &admission.spec.pack)?;
        // Version-owned contextual observations retain exact findings without
        // supplying missing proof. Other packs remain Available-only. Recheck
        // current quality after admission, using the actual admitted EIDs.
        let input = self.planning_input(&admitted, bindings, &selected);
        let report = match investigation_rule_inputs(&service.catalog, &admission.spec.pack, &input)
            .map_err(|_| ())
            .and_then(|inputs| {
                engine
                    .evaluate_version(exact.id(), exact.version(), &inputs)
                    .map_err(|_| ())
            }) {
            Ok(report) => report,
            Err(_) => {
                self.fail_local_evaluation(
                    service,
                    &worker,
                    &admitted,
                    &admission,
                    InvestigationStopReason::EvidenceInsufficient,
                )
                .await?;
                return Ok(());
            }
        };
        let partial = plan
            .requirements
            .iter()
            .any(|requirement| requirement.required && requirement.quality != DiagnosticEvidenceQuality::Available);
        let status = if partial {
            InvestigationDiagnosisStatus::Inconclusive
        } else {
            status(report.status)
        };
        let findings = investigation_findings(&report);
        let at = Utc::now();
        let revision = SavedInvestigationRunRevision {
            schema_version: INVESTIGATION_RUN_REVISION_SCHEMA_VERSION.to_owned(),
            id: InvestigationRunRevisionId::new(),
            run_id: run.id,
            scope: admission.spec.scope.clone(),
            control_epoch: admitted.control_epoch,
            pack: admission.spec.pack.clone(),
            step_id: admission.step_id,
            attempt_id: admission.attempt_id,
            status,
            partial,
            evidence_ids: evidence_ids.clone(),
            requirements: plan.requirements,
            findings,
            input_fingerprint: plan.input_fingerprint,
            created_at: at,
        };
        let Some(bytes) = bounded_json_len(&revision, admission.reservation.output_bytes) else {
            self.fail_local_evaluation(
                service,
                &worker,
                &admitted,
                &admission,
                InvestigationStopReason::BudgetExhausted,
            )
            .await?;
            return Ok(());
        };
        let mut result = super::collect::step_result(
            &admitted,
            &admission,
            InvestigationStepOutcome::Succeeded,
            None,
            evidence_ids.clone(),
            InvestigationBudgetCost {
                output_bytes: bytes,
                ..Default::default()
            },
        );
        result.finished_at = at;
        service
            .repository
            .investigation_run_save_evaluation(&worker, &result, &revision)
            .await?;
        Ok(())
    }

    async fn fail_local_evaluation(
        &self,
        service: &InvestigationRunService,
        worker: &RunWorkerContext<'_>,
        run: &InvestigationRun,
        admission: &InvestigationStepAdmission,
        reason: InvestigationStopReason,
    ) -> Result<(), ControlPlaneRequestFailure> {
        // No external call occurred and no rule-output payload was accepted.
        // This is a known failed computation, not an unknown remote outcome.
        let result = super::collect::step_result(
            run,
            admission,
            InvestigationStepOutcome::Failed,
            Some(InvestigationStepFailure::EvidenceInsufficient),
            vec![],
            InvestigationBudgetCost::default(),
        );
        let current = service
            .repository
            .investigation_run_settle(worker, &result, Utc::now())
            .await?;
        if current.status == InvestigationRunStatus::Running {
            service
                .repository
                .investigation_run_stop_planned(worker, current.id, current.revision, reason)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn explain(
        &self,
        execution: &RunExecution<'_>,
        run: &InvestigationRun,
        plan: InvestigationPlannedExplanation,
        evidence: &[EvidenceSnapshot],
    ) -> Result<(), ControlPlaneRequestFailure> {
        let RunExecution {
            service,
            group,
            delegation,
            lease,
        } = *execution;
        let auth = delegation.read_auth(run)?;
        let context = self.context(service, auth, run, &[]).await?;
        let mut saved_rules = Vec::new();
        for pack in &run.effective_packs {
            if let Some(result) = run.current_pack_result(pack) {
                let revision = service
                    .repository
                    .investigation_run_revision(auth, run.id, result.run_revision_id)
                    .await?;
                if revision.pack_result().map_err(contract_failure)? != *result {
                    return Err(contract_failure(InvestigationRunFailure::ResultConflict));
                }
                saved_rules.push(revision);
            }
        }
        let worker = RunWorkerContext {
            auth,
            context: &context,
            fence: &lease.fence,
        };
        let admission = InvestigationStepAdmission {
            step_id: InvestigationStepId::new(),
            attempt_id: StepAttemptId::new(),
            expected_revision: run.revision,
            spec: plan.spec,
            reservation: plan.reservation,
        };
        let correlation = CorrelationId::new();
        let admitted = service
            .repository
            .investigation_run_dispatch_admit(
                delegation,
                &worker,
                &service.catalog,
                run.id,
                &admission,
                RunAdmissionProof::Model(correlation),
            )
            .await?;
        let explanation = match self
            .wait_operation(
                service,
                group,
                delegation,
                lease,
                &admitted,
                self.models
                    .explain_investigation_once(auth, &admitted, evidence, &saved_rules, correlation),
            )
            .await
        {
            Ok(explanation) => explanation,
            Err(error) => {
                let current = service.repository.investigation_run_get(auth, run.id).await?;
                if group.cancellation_token().is_cancelled()
                    || current.status != InvestigationRunStatus::Running
                    || current.control_epoch != admitted.control_epoch
                {
                    return Err(error);
                }
                // A failed model transport does not refund unknown consumption
                // or replace usable saved rules with a model failure diagnosis.
                let current = service
                    .repository
                    .investigation_run_interrupt_owned(
                        &worker,
                        run.id,
                        InvestigationInterruptionReason::ModelOutcomeUnknown,
                    )
                    .await?;
                let evidence = self.snapshots(auth, &current.evidence_ids).await?;
                let mut connection = service.repository.pool.acquire().await?;
                let bindings = super::super::bindings::resolve(&mut connection, &service.catalog, &current).await?;
                drop(connection);
                self.finalize(service, lease, &current, None, &bindings, &evidence)
                    .await?;
                return Ok(());
            }
        };
        let provenance = explanation
            .as_ref()
            .map(|explanation| (explanation.invocation_id, explanation.summary.as_deref()));
        let (outcome, failure, bytes, model_calls) = match &explanation {
            Some(explanation) if explanation.output_bytes <= admission.reservation.output_bytes => {
                // Its invocation is persisted with the real parent investigation.
                // Explanation text is advisory and cannot replace rule findings.
                (
                    if explanation.summary.is_some() {
                        InvestigationStepOutcome::Succeeded
                    } else {
                        InvestigationStepOutcome::Failed
                    },
                    explanation
                        .summary
                        .is_none()
                        .then_some(InvestigationStepFailure::ModelUnavailable),
                    explanation.output_bytes,
                    1,
                )
            }
            None => (
                InvestigationStepOutcome::Failed,
                Some(InvestigationStepFailure::ModelUnavailable),
                0,
                0,
            ),
            Some(_) => {
                return Err(ControlPlaneRequestFailure::state(
                    "model_outcome_unknown",
                    "model response exceeds the reserved output",
                ));
            }
        };
        let result = super::collect::step_result(
            &admitted,
            &admission,
            outcome,
            failure,
            vec![],
            InvestigationBudgetCost {
                model_calls,
                output_bytes: bytes,
                ..Default::default()
            },
        );
        if let Some((invocation, summary)) = provenance {
            service
                .repository
                .investigation_run_save_explanation(&worker, &result, invocation, summary)
                .await?;
        } else {
            service
                .repository
                .investigation_run_settle(&worker, &result, Utc::now())
                .await?;
        }
        Ok(())
    }
}

fn status(status: DiagnosticStatus) -> InvestigationDiagnosisStatus {
    match status {
        DiagnosticStatus::Healthy => InvestigationDiagnosisStatus::Healthy,
        DiagnosticStatus::Fault => InvestigationDiagnosisStatus::Fault,
        DiagnosticStatus::Inconclusive => InvestigationDiagnosisStatus::Inconclusive,
        DiagnosticStatus::Unsupported => InvestigationDiagnosisStatus::Unsupported,
    }
}

fn bounded_json_len(value: &impl serde::Serialize, limit: u64) -> Option<u64> {
    struct Counter {
        accepted: u64,
        limit: u64,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let length = bytes.len() as u64;
            if length > self.limit.saturating_sub(self.accepted) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "bounded rule output exceeded",
                ));
            }
            self.accepted += length;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { accepted: 0, limit };
    serde_json::to_writer(&mut counter, value).ok()?;
    Some(counter.accepted)
}

#[cfg(test)]
mod tests {
    use super::bounded_json_len;
    #[test]
    fn rejects_oversized_rule_json_before_accepting_a_payload() {
        let payload = serde_json::json!({"finding":"synthetic-output".repeat(1024)});
        assert!(bounded_json_len(&payload, 128).is_none());
        let exact = serde_json::to_vec(&payload).unwrap().len() as u64;
        assert_eq!(bounded_json_len(&payload, exact), Some(exact));
        assert!(bounded_json_len(&payload, exact - 1).is_none());
    }
}
