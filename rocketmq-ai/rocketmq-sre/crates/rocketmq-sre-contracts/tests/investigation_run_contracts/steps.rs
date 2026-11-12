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

#[test]
fn query_retries_are_shared_across_steps_and_control_epochs() {
    let mut invalid = create_request();
    invalid.budget.max_query_retries = 3;
    assert_eq!(
        InvestigationRun::create(&context(), &invalid, None, at()).unwrap_err(),
        InvestigationRunFailure::InvalidInput
    );
    let (context, mut run) = fixture();
    run.budget.limit.read_queries = 12;
    for number in 0..3 {
        if number == 2 {
            let refinement = InvestigationRefinement {
                scope: run.effective_scope.clone(),
                packs: run.effective_packs.clone(),
                evidence_ids: vec![],
            };
            apply(
                &mut run,
                &context,
                InvestigationCommand::Steer { refinement },
                "keep-retry-budget",
            );
        }
        let mut admitted = collect(&run, number + 1, 0);
        admitted.spec.scope.resource = Some(InvestigationResource {
            kind: ResourceKind::ConsumerGroup,
            key: format!("consumer-{number}"),
        });
        run.admit_step(&context, &admitted, run.updated_at).unwrap();
        let mut failed = result(&run, &admitted, vec![]);
        failed.outcome = InvestigationStepOutcome::Failed;
        failed.failure = Some(InvestigationStepFailure::SourceUnavailable);
        failed.actual_cost = InvestigationBudgetCost::default();
        run.apply_step_result(&context, &failed, failed.finished_at).unwrap();
        admitted.attempt_id = StepAttemptId::new();
        admitted.expected_revision = run.revision;
        if number < 2 {
            run.admit_step(&context, &admitted, run.updated_at).unwrap();
            let mut failed = result(&run, &admitted, vec![]);
            failed.outcome = InvestigationStepOutcome::Failed;
            failed.failure = Some(InvestigationStepFailure::SourceUnavailable);
            failed.actual_cost = InvestigationBudgetCost::default();
            run.apply_step_result(&context, &failed, failed.finished_at).unwrap();
        } else {
            assert_eq!(run.query_retry_count().unwrap(), 2);
            assert_eq!(run.budget_usage().unwrap().0.read_queries, 5);
            let unchanged = run.clone();
            assert_eq!(
                run.admit_step(&context, &admitted, run.updated_at).unwrap_err(),
                InvestigationRunFailure::BudgetExceeded
            );
            assert_eq!(run, unchanged);
            let mut corrupt = run.clone();
            corrupt.budget.max_query_retries = 1;
            assert_eq!(corrupt.validate().unwrap_err(), InvestigationRunFailure::BudgetExceeded);
        }
    }
    let (context, mut no_retry) = fixture();
    no_retry.budget.max_query_retries = 0;
    let mut admitted = collect(&no_retry, 1, 0);
    no_retry.admit_step(&context, &admitted, no_retry.updated_at).unwrap();
    let mut failed = result(&no_retry, &admitted, vec![]);
    failed.outcome = InvestigationStepOutcome::Failed;
    failed.failure = Some(InvestigationStepFailure::SourceUnavailable);
    failed.actual_cost = InvestigationBudgetCost::default();
    no_retry
        .apply_step_result(&context, &failed, failed.finished_at)
        .unwrap();
    admitted.attempt_id = StepAttemptId::new();
    admitted.expected_revision = no_retry.revision;
    assert_eq!(
        no_retry
            .admit_step(&context, &admitted, no_retry.updated_at)
            .unwrap_err(),
        InvestigationRunFailure::BudgetExceeded
    );
}

#[test]
fn shared_reservations_bound_parallel_admission_and_retries() {
    let (context, mut run) = fixture();
    run.budget.limit.read_queries = 1;
    let first = collect(&run, 1, 0);
    run.admit_step(&context, &first, at()).unwrap();
    let second = collect(&run, 2, 1);
    assert_eq!(
        run.admit_step(&context, &second, at()).unwrap_err(),
        InvestigationRunFailure::BudgetExceeded
    );
    let mut failure = result(&run, &first, vec![]);
    failure.outcome = InvestigationStepOutcome::Failed;
    failure.failure = Some(InvestigationStepFailure::SourceUnavailable);
    run.apply_step_result(&context, &failure, failure.finished_at).unwrap();
    let mut retry = first.clone();
    retry.attempt_id = StepAttemptId::new();
    retry.expected_revision = run.revision;
    assert_eq!(
        run.admit_step(&context, &retry, run.updated_at).unwrap_err(),
        InvestigationRunFailure::BudgetExceeded
    );
}

#[test]
fn retries_keep_the_same_work_identity_and_completed_work_is_never_readmitted() {
    let (mut context, mut run) = fixture();
    let first = collect(&run, 1, 0);
    run.admit_step(&context, &first, at()).unwrap();
    let mut concurrent = first.clone();
    concurrent.expected_revision = run.revision;
    concurrent.attempt_id = StepAttemptId::new();
    assert_eq!(
        run.admit_step(&context, &concurrent, at()).unwrap_err(),
        InvestigationRunFailure::StepInFlight
    );
    let mut failure = result(&run, &first, vec![]);
    failure.outcome = InvestigationStepOutcome::Failed;
    failure.failure = Some(InvestigationStepFailure::SourceUnavailable);
    run.apply_step_result(&context, &failure, failure.finished_at).unwrap();
    concurrent.expected_revision = run.revision;
    run.admit_step(&context, &concurrent, run.updated_at).unwrap();
    proof(&mut context, evidence(700), &concurrent.spec.scope);
    let completed = result(&run, &concurrent, vec![evidence(700)]);
    run.apply_step_result(&context, &completed, completed.finished_at)
        .unwrap();
    concurrent.expected_revision = run.revision;
    concurrent.attempt_id = StepAttemptId::new();
    assert_eq!(
        run.admit_step(&context, &concurrent, run.updated_at).unwrap_err(),
        InvestigationRunFailure::StepAlreadyCompleted
    );
    concurrent.step_id = InvestigationStepId::new();
    assert_eq!(
        run.admit_step(&context, &concurrent, run.updated_at).unwrap_err(),
        InvestigationRunFailure::StepIdentityConflict
    );
}

#[test]
fn ordinary_revisions_do_not_discard_a_valid_parallel_result_and_ids_deduplicate_cost() {
    let (mut context, mut run) = fixture();
    let first = collect(&run, 1, 0);
    run.admit_step(&context, &first, at()).unwrap();
    let second = collect(&run, 2, 1);
    run.admit_step(&context, &second, at()).unwrap();
    proof(&mut context, evidence(700), &first.spec.scope);
    let completed = result(&run, &second, vec![evidence(700)]);
    run.apply_step_result(&context, &completed, completed.finished_at)
        .unwrap();
    let mut later = result(&run, &first, vec![evidence(700)]);
    later.actual_cost.evidence_items = 0;
    assert_eq!(
        run.apply_step_result(&context, &later, later.finished_at).unwrap(),
        InvestigationResultAcceptance::Applied
    );
    assert_eq!(run.evidence_ids, vec![evidence(700)]);
    assert_eq!(run.budget_usage().unwrap().0.evidence_items, 1);
    assert_eq!(run.budget_usage().unwrap().0.read_queries, 2);
}

#[test]
fn wrong_attempt_epoch_and_overreported_cost_fail_without_mutation() {
    for variant in 0..3 {
        let (context, mut run) = fixture();
        let admitted = collect(&run, 1, 0);
        run.admit_step(&context, &admitted, at()).unwrap();
        let mut wrong = result(&run, &admitted, vec![]);
        match variant {
            0 => wrong.attempt_id = StepAttemptId::new(),
            1 => wrong.control_epoch = InvestigationControlEpoch::from_u64(99),
            _ => wrong.actual_cost.output_bytes = 101,
        }
        let unchanged = run.clone();
        assert!(run.apply_step_result(&context, &wrong, wrong.finished_at).is_err());
        assert_eq!(run, unchanged);
    }
}

#[test]
fn rule_and_model_steps_cannot_substitute_another_visible_known_evidence() {
    for explain in [false, true] {
        let (mut context, mut run) = fixture();
        seed(&mut context, &mut run, &[evidence(700), evidence(701)]);
        let mut admitted = collect(&run, 1, 0);
        admitted.spec.work = if explain {
            InvestigationStepKind::ExplainEvidence {
                evidence_ids: vec![evidence(700)],
            }
        } else {
            InvestigationStepKind::EvaluateRules {
                evidence_ids: vec![evidence(700)],
            }
        };
        admitted.reservation = InvestigationBudgetCost {
            model_calls: u64::from(explain),
            output_bytes: 100,
            ..InvestigationBudgetCost::default()
        };
        run.admit_step(&context, &admitted, run.updated_at).unwrap();
        let mut wrong = result(&run, &admitted, vec![evidence(701)]);
        wrong.actual_cost.evidence_items = 0;
        let unchanged = run.clone();
        assert_eq!(
            run.apply_step_result(&context, &wrong, wrong.finished_at).unwrap_err(),
            InvestigationRunFailure::ScopeMismatch
        );
        assert_eq!(run, unchanged);
        let mut accepted = wrong;
        accepted.evidence_ids = vec![evidence(700)];
        run.apply_step_result(&context, &accepted, accepted.finished_at)
            .unwrap();
        run.steps[0].attempts[0].result.as_mut().unwrap().evidence_ids = vec![evidence(701)];
        assert_eq!(run.validate().unwrap_err(), InvestigationRunFailure::ScopeMismatch);
    }
}

#[test]
fn malformed_hydration_rejects_an_open_attempt_before_a_retry() {
    let (context, mut run) = fixture();
    let admitted = collect(&run, 1, 0);
    run.admit_step(&context, &admitted, at()).unwrap();
    let mut duplicate = run.steps[0].attempts[0].clone();
    duplicate.id = StepAttemptId::new();
    duplicate.admitted_revision = InvestigationRevision::from_u64(run.revision.get() + 1);
    run.revision = duplicate.admitted_revision;
    run.steps[0].attempts.push(duplicate);
    assert!(run.validate().is_err());
}

#[test]
fn failed_attempts_cannot_evade_the_per_step_retry_bound_by_using_new_ids() {
    let (context, mut run) = fixture();
    let mut admitted = collect(&run, 1, 0);
    for _ in 0..2 {
        admitted.expected_revision = run.revision;
        admitted.attempt_id = StepAttemptId::new();
        run.admit_step(&context, &admitted, run.updated_at).unwrap();
        let mut failed = result(&run, &admitted, vec![]);
        failed.outcome = InvestigationStepOutcome::Failed;
        failed.failure = Some(InvestigationStepFailure::SourceUnavailable);
        run.apply_step_result(&context, &failed, failed.finished_at).unwrap();
    }
    admitted.expected_revision = run.revision;
    admitted.attempt_id = StepAttemptId::new();
    assert_eq!(
        run.admit_step(&context, &admitted, run.updated_at).unwrap_err(),
        InvestigationRunFailure::BudgetExceeded
    );
    admitted.step_id = InvestigationStepId::new();
    assert_eq!(
        run.admit_step(&context, &admitted, run.updated_at).unwrap_err(),
        InvestigationRunFailure::StepIdentityConflict
    );
}

#[test]
fn trusted_catalog_membership_and_read_only_step_shapes_are_both_required() {
    let (mut context, mut run) = fixture();
    let admitted = collect(&run, 1, 0);
    context.allowed_requirements.clear();
    assert_eq!(
        run.admit_step(&context, &admitted, at()).unwrap_err(),
        InvestigationRunFailure::PackNotAllowed
    );
    let mut wire = serde_json::to_value(&admitted).unwrap();
    wire["spec"]["work"] = json!({"kind":"execute","action":"reset_offset"});
    assert!(serde_json::from_value::<InvestigationStepAdmission>(wire).is_err());
}

#[test]
fn thirteen_calls_are_rejected_and_failed_zero_reports_never_refund_admissions() {
    let mut valid = create_request();
    valid.budget.limit.read_queries = 12;
    valid.budget.limit.model_calls = 1;
    InvestigationRun::create(&context(), &valid, None, at()).unwrap();
    let mut request = create_request();
    request.budget.limit.read_queries = 13;
    assert_eq!(
        InvestigationRun::create(&context(), &request, None, at()).unwrap_err(),
        InvestigationRunFailure::InvalidInput
    );
    let (context, mut run) = fixture();
    run.budget.limit.read_queries = 12;
    run.budget.limit.model_calls = 0;
    run.budget.max_steps = 64;
    for number in 0..12 {
        let mut admitted = collect(&run, number + 1, 0);
        admitted.spec.scope.resource = Some(InvestigationResource {
            kind: ResourceKind::ConsumerGroup,
            key: format!("consumer-{number}"),
        });
        run.admit_step(&context, &admitted, run.updated_at).unwrap();
        let mut failed = result(&run, &admitted, vec![]);
        failed.outcome = InvestigationStepOutcome::Failed;
        failed.failure = Some(InvestigationStepFailure::SourceUnavailable);
        failed.actual_cost = InvestigationBudgetCost::default();
        run.apply_step_result(&context, &failed, failed.finished_at).unwrap();
        let charged = run.budget_usage().unwrap();
        assert_eq!(charged.0.read_queries, number as u64 + 1);
        assert_eq!(
            run.apply_step_result(&context, &failed, failed.finished_at).unwrap(),
            InvestigationResultAcceptance::Replay
        );
        assert_eq!(run.budget_usage().unwrap(), charged);
        assert_eq!(
            run.steps.last().unwrap().attempts[0]
                .result
                .as_ref()
                .unwrap()
                .actual_cost
                .read_queries,
            0
        );
    }
    let mut next = collect(&run, 13, 0);
    next.spec.scope.resource = Some(InvestigationResource {
        kind: ResourceKind::ConsumerGroup,
        key: "consumer-13".into(),
    });
    assert_eq!(
        run.admit_step(&context, &next, run.updated_at).unwrap_err(),
        InvestigationRunFailure::BudgetExceeded
    );
}

#[test]
fn unavailable_model_zero_reports_do_not_refund_the_independent_model_budget() {
    let (mut context, mut run) = fixture();
    seed(&mut context, &mut run, &[evidence(700)]);
    run.budget.limit.model_calls = 1;
    let mut admitted = collect(&run, 1, 0);
    admitted.spec.work = InvestigationStepKind::ExplainEvidence {
        evidence_ids: vec![evidence(700)],
    };
    admitted.reservation = InvestigationBudgetCost {
        model_calls: 1,
        output_bytes: 100,
        ..InvestigationBudgetCost::default()
    };
    run.admit_step(&context, &admitted, run.updated_at).unwrap();
    let mut failed = result(&run, &admitted, vec![]);
    failed.outcome = InvestigationStepOutcome::Failed;
    failed.failure = Some(InvestigationStepFailure::ModelUnavailable);
    failed.actual_cost = InvestigationBudgetCost::default();
    run.apply_step_result(&context, &failed, failed.finished_at).unwrap();
    assert_eq!(run.budget_usage().unwrap().0.model_calls, 1);
    let charged = run.budget_usage().unwrap();
    assert_eq!(
        run.apply_step_result(&context, &failed, failed.finished_at).unwrap(),
        InvestigationResultAcceptance::Replay
    );
    assert_eq!(run.budget_usage().unwrap(), charged);
    admitted.attempt_id = StepAttemptId::new();
    admitted.expected_revision = run.revision;
    assert_eq!(
        run.admit_step(&context, &admitted, run.updated_at).unwrap_err(),
        InvestigationRunFailure::BudgetExceeded
    );
}
