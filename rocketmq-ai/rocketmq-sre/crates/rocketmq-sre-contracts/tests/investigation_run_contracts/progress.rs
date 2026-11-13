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

fn saved(
    ctx: &InvestigationRunContext,
    run: &mut InvestigationRun,
    pack: usize,
    status: InvestigationDiagnosisStatus,
    partial: bool,
    counter: bool,
) -> SavedInvestigationRunRevision {
    let ids = vec![evidence(21), evidence(22)];
    let admission = InvestigationStepAdmission {
        step_id: InvestigationStepId::new(),
        attempt_id: StepAttemptId::new(),
        expected_revision: run.revision,
        spec: InvestigationStepSpec {
            scope: run.effective_scope.clone(),
            pack: run.effective_packs[pack].clone(),
            work: InvestigationStepKind::EvaluateRules {
                evidence_ids: ids.clone(),
            },
        },
        reservation: InvestigationBudgetCost {
            output_bytes: 100,
            ..Default::default()
        },
    };
    run.admit_step(ctx, &admission, run.updated_at).unwrap();
    let mut observed = result(run, &admission, ids.clone());
    observed.actual_cost.evidence_items = 0;
    run.apply_step_result(ctx, &observed, observed.finished_at).unwrap();
    SavedInvestigationRunRevision {
        schema_version: INVESTIGATION_RUN_REVISION_SCHEMA_VERSION.to_owned(),
        id: InvestigationRunRevisionId::new(),
        run_id: run.id,
        scope: run.effective_scope.clone(),
        control_epoch: run.control_epoch,
        pack: admission.spec.pack,
        step_id: admission.step_id,
        attempt_id: admission.attempt_id,
        status,
        partial,
        input_fingerprint: canonical_sha256(&(pack, partial)).unwrap(),
        evidence_ids: ids.clone(),
        requirements: vec![DiagnosticRequirementQuality {
            id: if pack == 0 { "consumer-lag" } else { "store-pressure" }.to_owned(),
            source: "fixture".to_owned(),
            resource_prefix: "fixture/".to_owned(),
            required: true,
            max_age_seconds: 300,
            fields: vec![EvidenceFieldContract {
                path: "ready".to_owned(),
                kind: EvidenceFieldKind::Boolean,
            }],
            quality: if partial {
                DiagnosticEvidenceQuality::Partial
            } else {
                DiagnosticEvidenceQuality::Available
            },
            reason_code: "synthetic_contract_fixture".to_owned(),
            observed_at: Some(run.updated_at),
            evidence_ids: ids.clone(),
            missing_fields: vec![],
        }],
        findings: vec![InvestigationRunFinding {
            reason_code: "SAVED_DETERMINISTIC_RULE".to_owned(),
            root_cause: "Only this saved deterministic finding".to_owned(),
            outcome: status,
            supporting_evidence_ids: vec![ids[0]],
            counter_evidence_ids: if counter { vec![ids[1]] } else { vec![] },
        }],
        created_at: run.updated_at,
    }
}
fn planned(
    run: &InvestigationRun,
    basis: &SavedInvestigationRunRevision,
    conclusion: HypothesisAssessmentConclusion,
) -> HypothesisAssessment {
    HypothesisAssessment {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        id: HypothesisAssessmentId::new(),
        run_id: run.id,
        scope: run.effective_scope.clone(),
        control_epoch: run.control_epoch,
        diagnosis_revision_id: None,
        run_revision_id: Some(basis.id),
        conclusion,
        summary: "Rule-only advisory from actual saved run revision".to_owned(),
        created_at: run.updated_at,
    }
}
fn prepared() -> (InvestigationRunContext, InvestigationRun) {
    let (mut ctx, mut run) = fixture();
    seed(&mut ctx, &mut run, &[evidence(21), evidence(22)]);
    (ctx, run)
}

#[test]
fn all_selected_true_saved_revisions_gate_healthy_and_unique_fault_conclusions() {
    for fault in [false, true] {
        let (ctx, mut run) = prepared();
        let basis = saved(
            &ctx,
            &mut run,
            0,
            if fault {
                InvestigationDiagnosisStatus::Fault
            } else {
                InvestigationDiagnosisStatus::Healthy
            },
            false,
            false,
        );
        run.record_pack_result(&ctx, run.revision, &basis, run.updated_at)
            .unwrap();
        let other = saved(&ctx, &mut run, 1, InvestigationDiagnosisStatus::Healthy, false, false);
        run.record_pack_result(&ctx, run.revision, &other, run.updated_at)
            .unwrap();
        let conclusion = if fault {
            HypothesisAssessmentConclusion::RootCause {
                statement: basis.findings[0].root_cause.clone(),
                supporting_evidence_ids: vec![evidence(21)],
                counter_evidence_ids: vec![],
            }
        } else {
            HypothesisAssessmentConclusion::Healthy {
                supporting_evidence_ids: vec![evidence(21)],
            }
        };
        let assessment = planned(&run, &basis, conclusion);
        run.finish_planned_assessment(&ctx, run.revision, &assessment, &basis, run.updated_at)
            .unwrap();
        assert_eq!(run.status, InvestigationRunStatus::Completed);
        run.validate().unwrap();
        let mut tampered = run.clone();
        tampered.pack_results.remove(1);
        assert!(tampered.validate().is_err());
    }
}

#[test]
fn partial_missing_opposite_multiple_fault_and_counter_evidence_never_become_a_strong_run() {
    for case in 0..6 {
        let (ctx, mut run) = prepared();
        let mut basis = saved(
            &ctx,
            &mut run,
            0,
            InvestigationDiagnosisStatus::Fault,
            case == 0,
            case == 1,
        );
        if case == 2 {
            let mut second = basis.findings[0].clone();
            second.reason_code = "ANOTHER_CAUSE".to_owned();
            second.root_cause = "A different independent fault".to_owned();
            basis.findings.push(second);
        }
        run.record_pack_result(&ctx, run.revision, &basis, run.updated_at)
            .unwrap();
        if case != 3 {
            let other = saved(
                &ctx,
                &mut run,
                1,
                if case == 4 {
                    InvestigationDiagnosisStatus::Fault
                } else if case == 5 {
                    InvestigationDiagnosisStatus::Inconclusive
                } else {
                    InvestigationDiagnosisStatus::Healthy
                },
                false,
                false,
            );
            run.record_pack_result(&ctx, run.revision, &other, run.updated_at)
                .unwrap();
        }
        let assessment = planned(
            &run,
            &basis,
            HypothesisAssessmentConclusion::RootCause {
                statement: basis.findings[0].root_cause.clone(),
                supporting_evidence_ids: vec![evidence(21)],
                counter_evidence_ids: vec![],
            },
        );
        assert_eq!(
            run.finish_planned_assessment(&ctx, run.revision, &assessment, &basis, run.updated_at),
            Err(InvestigationRunFailure::InvalidAssessment)
        );
        let inconclusive = planned(
            &run,
            &basis,
            HypothesisAssessmentConclusion::Inconclusive {
                reason: InvestigationStopReason::EvidenceInsufficient,
                considered_evidence_ids: vec![evidence(21)],
                missing_requirements: vec![],
            },
        );
        run.finish_planned_assessment(&ctx, run.revision, &inconclusive, &basis, run.updated_at)
            .unwrap();
        assert_eq!(run.status, InvestigationRunStatus::Inconclusive);
    }
}

#[test]
fn recording_requires_settled_evaluation_lineage_and_exact_report_replay() {
    let (ctx, mut run) = prepared();
    let revision = saved(&ctx, &mut run, 0, InvestigationDiagnosisStatus::Healthy, false, false);
    let mut wrong = revision.clone();
    wrong.attempt_id = StepAttemptId::new();
    assert_eq!(
        run.record_pack_result(&ctx, run.revision, &wrong, run.updated_at),
        Err(InvestigationRunFailure::AttemptMismatch)
    );
    wrong = revision.clone();
    wrong.scope.cluster_id = ClusterId::new();
    assert!(
        run.record_pack_result(&ctx, run.revision, &wrong, run.updated_at)
            .is_err()
    );
    run.record_pack_result(&ctx, run.revision, &revision, run.updated_at)
        .unwrap();
    let unchanged = run.clone();
    assert_eq!(
        run.record_pack_result(&ctx, InvestigationRevision::from_u64(0), &revision, run.updated_at)
            .unwrap(),
        InvestigationProgressAcceptance::Replay
    );
    assert_eq!(run, unchanged);
    wrong = revision.clone();
    wrong.findings[0].root_cause = "Different content with the same counts and references".to_owned();
    assert_eq!(
        run.record_pack_result(&ctx, run.revision, &wrong, run.updated_at),
        Err(InvestigationRunFailure::ResultConflict)
    );
    let mut altered = run.clone();
    altered.pack_results[0].evidence_ids.push(evidence(9999));
    assert!(altered.validate().is_err());
}

#[test]
fn legacy_assessment_payload_keeps_its_uuid_and_new_basis_is_exclusive() {
    let (ctx, mut run) = prepared();
    let old = revision(&run, vec![evidence(21)]);
    let assessment = assessment(
        &run,
        &old,
        HypothesisAssessmentConclusion::Inconclusive {
            reason: InvestigationStopReason::EvidenceInsufficient,
            considered_evidence_ids: vec![evidence(21)],
            missing_requirements: vec![],
        },
    );
    let payload = serde_json::to_value(&assessment).unwrap();
    assert_eq!(payload["diagnosis_revision_id"], json!(old.id));
    assert!(payload.get("run_revision_id").is_none());
    assert_eq!(
        serde_json::from_value::<HypothesisAssessment>(payload).unwrap(),
        assessment
    );
    for neither in [false, true] {
        let mut invalid = assessment.clone();
        if neither {
            invalid.diagnosis_revision_id = None;
        } else {
            invalid.run_revision_id = Some(InvestigationRunRevisionId::new());
        }
        assert_eq!(
            run.finish_with_assessment(&ctx, run.revision, &invalid, &old, run.updated_at),
            Err(InvestigationRunFailure::InvalidAssessment)
        );
    }
    let empty = InvestigationRun::create(&context(), &create_request(), None, at())
        .unwrap()
        .run;
    let payload = serde_json::to_value(&empty).unwrap();
    assert!(payload.get("pack_results").is_none());
    assert!(payload.get("stop_reason").is_none());
    assert_eq!(serde_json::from_value::<InvestigationRun>(payload).unwrap(), empty);
}

#[test]
fn unknown_source_and_shutdown_preserve_cost_and_respect_explicit_pause_cancel() {
    for reason in [
        InvestigationInterruptionReason::SourceOutcomeUnknown,
        InvestigationInterruptionReason::OwnerShutdown,
    ] {
        for control in 0..3 {
            let (ctx, mut run) = fixture();
            let admission = collect(&run, 1, 0);
            run.admit_step(&ctx, &admission, at()).unwrap();
            if control != 0 {
                apply(
                    &mut run,
                    &ctx,
                    if control == 1 {
                        InvestigationCommand::Pause {}
                    } else {
                        InvestigationCommand::Cancel {}
                    },
                    "explicit-control",
                );
            }
            let revision = run.revision;
            run.interrupt_with_reason(
                &ctx,
                InvestigationInterruptAttemptRequest {
                    expected_revision: revision,
                    step_id: admission.step_id,
                    attempt_id: admission.attempt_id,
                    reason,
                },
                run.updated_at,
            )
            .unwrap();
            assert_eq!(
                run.status,
                match control {
                    0 => InvestigationRunStatus::NeedsEvidence,
                    1 => InvestigationRunStatus::Paused,
                    _ => InvestigationRunStatus::Cancelled,
                }
            );
            assert_eq!(
                run.budget_usage().unwrap(),
                (InvestigationBudgetCost::default(), admission.reservation)
            );
            assert!(run.steps[0].attempts[0].result.is_none());
            assert_eq!(
                run.stop_reason,
                if control == 0 {
                    Some(InvestigationStopReason::SourceUnavailable)
                } else {
                    None
                }
            );
        }
    }
}

#[test]
fn zero_admitted_steps_can_stop_without_inventing_a_saved_revision() {
    let ctx = context();
    let mut request = create_request();
    request.budget.limit.read_queries = 0;
    let mut run = InvestigationRun::create(&ctx, &request, None, at()).unwrap().run;
    run.stop_with_reason(&ctx, run.revision, InvestigationStopReason::BudgetExhausted, at())
        .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::NeedsEvidence);
    assert_eq!(run.stop_reason, Some(InvestigationStopReason::BudgetExhausted));
    assert!(run.steps.is_empty() && run.pack_results.is_empty() && run.assessments.is_empty());
    run.validate().unwrap();
}

#[test]
fn unknown_model_usage_can_finish_rule_only_without_refunding_its_reservation() {
    let (ctx, mut run) = prepared();
    let first = saved(&ctx, &mut run, 0, InvestigationDiagnosisStatus::Healthy, false, false);
    run.record_pack_result(&ctx, run.revision, &first, run.updated_at)
        .unwrap();
    let second = saved(&ctx, &mut run, 1, InvestigationDiagnosisStatus::Healthy, false, false);
    run.record_pack_result(&ctx, run.revision, &second, run.updated_at)
        .unwrap();
    let admission = InvestigationStepAdmission {
        step_id: InvestigationStepId::new(),
        attempt_id: StepAttemptId::new(),
        expected_revision: run.revision,
        spec: InvestigationStepSpec {
            scope: run.effective_scope.clone(),
            pack: run.effective_packs[0].clone(),
            work: InvestigationStepKind::ExplainEvidence {
                evidence_ids: run.evidence_ids.clone(),
            },
        },
        reservation: InvestigationBudgetCost {
            model_calls: 1,
            output_bytes: 100,
            ..Default::default()
        },
    };
    run.admit_step(&ctx, &admission, run.updated_at).unwrap();
    run.interrupt_with_reason(
        &ctx,
        InvestigationInterruptAttemptRequest {
            expected_revision: run.revision,
            step_id: admission.step_id,
            attempt_id: admission.attempt_id,
            reason: InvestigationInterruptionReason::ModelOutcomeUnknown,
        },
        run.updated_at,
    )
    .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::NeedsEvidence);
    assert_eq!(run.stop_reason, Some(InvestigationStopReason::ModelUnavailable));
    let assessment = planned(
        &run,
        &first,
        HypothesisAssessmentConclusion::Healthy {
            supporting_evidence_ids: vec![evidence(21)],
        },
    );
    run.finish_planned_assessment(&ctx, run.revision, &assessment, &first, run.updated_at)
        .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Completed);
    assert_eq!(run.stop_reason, None);
    assert_eq!(run.budget_usage().unwrap().1, admission.reservation);
}
