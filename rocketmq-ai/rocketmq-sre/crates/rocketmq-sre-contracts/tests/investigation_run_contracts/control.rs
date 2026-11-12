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
fn create_replays_only_the_same_authorized_actor_scope_and_semantics() {
    let mut context = context();
    let request = create_request();
    let first = InvestigationRun::create(&context, &request, None, at()).unwrap();
    let replay =
        InvestigationRun::create(&context, &request, Some(&first), request.deadline + Duration::hours(1)).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.run, first.run);
    let mut changed = request.clone();
    changed.budget.max_steps += 1;
    assert_eq!(
        InvestigationRun::create(&context, &changed, Some(&first), at()).unwrap_err(),
        InvestigationRunFailure::IdempotencyConflict
    );
    context.permission = InvestigationRunPermission::Read;
    assert_eq!(
        InvestigationRun::create(&context, &request, Some(&first), at()).unwrap_err(),
        InvestigationRunFailure::Unauthorized
    );
    context.permission = InvestigationRunPermission::Control;
    context.actor_subject = "other-operator".into();
    assert_eq!(
        InvestigationRun::create(&context, &request, Some(&first), at()).unwrap_err(),
        InvestigationRunFailure::IdempotencyConflict
    );
}

#[test]
fn current_authority_precedes_command_replay_and_stale_new_commands_conflict() {
    let (mut context, mut run) = fixture();
    let (request, receipt) = apply(&mut run, &context, InvestigationCommand::Pause {}, "pause");
    apply(&mut run, &context, InvestigationCommand::Resume {}, "resume");
    let unchanged = run.clone();
    assert_eq!(
        run.classify_command(&context, &request, Some(&receipt)).unwrap(),
        InvestigationCommandClassification::Replay
    );
    assert_eq!(
        run.apply_command(&context, &request, Some(&receipt), run.deadline + Duration::seconds(1))
            .unwrap(),
        receipt
    );
    assert_eq!(run, unchanged);
    let mut stale = request.clone();
    stale.idempotency_key = "new-key".into();
    stale.command_id = InvestigationCommandId::new();
    assert!(matches!(
        run.apply_command(&context, &stale, None, at()),
        Err(InvestigationRunFailure::RevisionConflict { .. })
    ));
    let mut changed = request.clone();
    changed.reason = "Different intent".into();
    assert_eq!(
        run.classify_command(&context, &changed, Some(&receipt)).unwrap_err(),
        InvestigationRunFailure::IdempotencyConflict
    );
    context.permission = InvestigationRunPermission::Read;
    assert_eq!(
        run.classify_command(&context, &request, Some(&receipt)).unwrap_err(),
        InvestigationRunFailure::Unauthorized
    );
    context.permission = InvestigationRunPermission::Control;
    context.authorized_scope.cluster_id = ClusterId::new();
    assert_eq!(
        run.classify_command(&context, &request, Some(&receipt)).unwrap_err(),
        InvestigationRunFailure::ScopeMismatch
    );
}

#[test]
fn pause_drains_existing_work_without_new_admission_or_automatic_resume() {
    let (mut context, mut run) = fixture();
    let first = collect(&run, 1, 0);
    run.admit_step(&context, &first, at()).unwrap();
    let second = collect(&run, 2, 1);
    run.admit_step(&context, &second, at()).unwrap();
    let epoch = run.control_epoch;
    apply(&mut run, &context, InvestigationCommand::Pause {}, "pause");
    assert_eq!(run.status, InvestigationRunStatus::Pausing);
    assert_eq!(
        run.admit_step(&context, &collect(&run, 3, 0), run.updated_at)
            .unwrap_err(),
        InvestigationRunFailure::InvalidStateTransition
    );
    let premature = command(&run, InvestigationCommand::Resume {}, "too-soon");
    assert_eq!(
        run.apply_command(&context, &premature, None, run.updated_at)
            .unwrap_err(),
        InvestigationRunFailure::InvalidStateTransition
    );
    for (index, admitted) in [first, second].iter().enumerate() {
        let id = evidence(700 + index as u128);
        proof(&mut context, id, &admitted.spec.scope);
        let finished = result(&run, admitted, vec![id]);
        assert_eq!(
            run.apply_step_result(&context, &finished, finished.finished_at)
                .unwrap(),
            InvestigationResultAcceptance::Applied
        );
    }
    assert_eq!(run.status, InvestigationRunStatus::Paused);
    assert_eq!(run.control_epoch, epoch);
    assert_eq!(run.budget_usage().unwrap().0.read_queries, 2);
    apply(&mut run, &context, InvestigationCommand::Resume {}, "resume");
    assert_eq!(run.status, InvestigationRunStatus::Running);
    assert_eq!(run.budget_usage().unwrap().0.read_queries, 2);
}

#[test]
fn cancellation_keeps_late_observations_and_cost_but_never_revives_a_run() {
    let (mut context, mut run) = fixture();
    let admitted = collect(&run, 1, 0);
    run.admit_step(&context, &admitted, at()).unwrap();
    let (cancel, receipt) = apply(&mut run, &context, InvestigationCommand::Cancel {}, "cancel");
    assert_eq!(run.status, InvestigationRunStatus::Cancelling);
    assert_eq!(run.in_flight_count(), 1);
    assert_eq!(
        run.admit_step(&context, &collect(&run, 2, 1), run.updated_at)
            .unwrap_err(),
        InvestigationRunFailure::InvalidStateTransition
    );
    proof(&mut context, evidence(700), &admitted.spec.scope);
    let mut finished = result(&run, &admitted, vec![evidence(700)]);
    finished.finished_at = run.deadline + Duration::seconds(10);
    assert_eq!(
        run.apply_step_result(&context, &finished, finished.finished_at)
            .unwrap(),
        InvestigationResultAcceptance::Superseded
    );
    assert_eq!(run.status, InvestigationRunStatus::Cancelled);
    assert!(run.evidence_ids.is_empty());
    assert_eq!(run.steps[0].attempts[0].result.as_ref(), Some(&finished));
    assert_eq!(run.budget_usage().unwrap().0.read_queries, 1);
    let snapshot = run.clone();
    assert_eq!(
        run.apply_step_result(&context, &finished, finished.finished_at)
            .unwrap(),
        InvestigationResultAcceptance::Replay
    );
    assert_eq!(run, snapshot);
    let mut conflicting = finished.clone();
    conflicting.actual_cost.output_bytes += 1;
    assert_eq!(
        run.apply_step_result(&context, &conflicting, finished.finished_at)
            .unwrap_err(),
        InvestigationRunFailure::ResultConflict
    );
    assert_eq!(
        run.apply_command(&context, &cancel, Some(&receipt), finished.finished_at)
            .unwrap(),
        receipt
    );
    assert_eq!(run, snapshot);
}

#[test]
fn cancelled_ack_is_worker_settlement_and_keeps_known_usage() {
    let (context, mut run) = fixture();
    let admitted = collect(&run, 1, 0);
    run.admit_step(&context, &admitted, at()).unwrap();
    apply(&mut run, &context, InvestigationCommand::Cancel {}, "cancel");
    let mut settled = result(&run, &admitted, vec![]);
    settled.outcome = InvestigationStepOutcome::CancelledAcknowledged;
    settled.actual_cost.output_bytes = 0;
    run.apply_step_result(&context, &settled, settled.finished_at).unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Cancelled);
    assert_eq!(run.budget_usage().unwrap().0.read_queries, 1);
    assert_eq!(
        run.steps[0].attempts[0].disposition,
        Some(InvestigationResultDisposition::Superseded)
    );
}

#[test]
fn steering_invalidates_decisions_not_in_flight_accounting_and_cannot_widen_again() {
    let (mut context, mut run) = fixture();
    let admitted = collect(&run, 1, 0);
    run.admit_step(&context, &admitted, at()).unwrap();
    let initial = run.initial_scope.clone();
    let mut narrow = initial.clone();
    narrow.resource = Some(InvestigationResource {
        kind: ResourceKind::Broker,
        key: "broker-a".into(),
    });
    narrow.time_range.start += Duration::minutes(5);
    let refinement = InvestigationRefinement {
        scope: narrow.clone(),
        packs: vec![run.initial_packs[1].clone()],
        evidence_ids: vec![],
    };
    apply(&mut run, &context, InvestigationCommand::Steer { refinement }, "narrow");
    assert_eq!(run.initial_scope, initial);
    assert_eq!(run.effective_scope, narrow);
    let widen = command(
        &run,
        InvestigationCommand::Steer {
            refinement: InvestigationRefinement {
                scope: initial,
                packs: packs(),
                evidence_ids: vec![],
            },
        },
        "widen",
    );
    let unchanged = run.clone();
    assert_eq!(
        run.apply_command(&context, &widen, None, run.updated_at).unwrap_err(),
        InvestigationRunFailure::ScopeMismatch
    );
    assert_eq!(run, unchanged);
    proof(&mut context, evidence(700), &admitted.spec.scope);
    let old_result = result(&run, &admitted, vec![evidence(700)]);
    assert_eq!(
        run.apply_step_result(&context, &old_result, old_result.finished_at)
            .unwrap(),
        InvestigationResultAcceptance::Superseded
    );
    assert!(run.evidence_ids.is_empty());
    assert_eq!(run.budget_usage().unwrap().0.evidence_items, 1);
}

#[test]
fn tenant_cluster_case_and_resource_changes_never_retarget_an_existing_run() {
    for changed in 0..4 {
        let (context, mut run) = fixture();
        let mut narrowed = run.effective_scope.clone();
        narrowed.resource = Some(InvestigationResource {
            kind: ResourceKind::ConsumerGroup,
            key: "orders".into(),
        });
        apply(
            &mut run,
            &context,
            InvestigationCommand::Steer {
                refinement: InvestigationRefinement {
                    scope: narrowed,
                    packs: packs(),
                    evidence_ids: vec![],
                },
            },
            "first",
        );
        let mut wrong = run.effective_scope.clone();
        match changed {
            0 => wrong.tenant_id = TenantId::new(),
            1 => wrong.cluster_id = ClusterId::new(),
            2 => wrong.investigation_id = InvestigationId::new(),
            _ => wrong.resource.as_mut().unwrap().key = "orders-other".into(),
        }
        let request = command(
            &run,
            InvestigationCommand::Steer {
                refinement: InvestigationRefinement {
                    scope: wrong,
                    packs: packs(),
                    evidence_ids: vec![],
                },
            },
            "wrong-scope",
        );
        let unchanged = run.clone();
        assert_eq!(
            run.apply_command(&context, &request, None, run.updated_at).unwrap_err(),
            InvestigationRunFailure::ScopeMismatch
        );
        assert_eq!(run, unchanged);
    }
}

#[test]
fn evidence_visibility_is_bound_to_subject_and_window_not_just_cluster_membership() {
    let (mut context, mut run) = fixture();
    let mut narrow = run.effective_scope.clone();
    narrow.time_range.start += Duration::minutes(10);
    proof(&mut context, evidence(700), &run.initial_scope);
    let mut request = command(
        &run,
        InvestigationCommand::Steer {
            refinement: InvestigationRefinement {
                scope: narrow.clone(),
                packs: packs(),
                evidence_ids: vec![evidence(700)],
            },
        },
        "narrow-with-proof",
    );
    assert_eq!(
        run.apply_command(&context, &request, None, run.updated_at).unwrap_err(),
        InvestigationRunFailure::ScopeMismatch
    );
    proof(&mut context, evidence(700), &narrow);
    let receipt = run.apply_command(&context, &request, None, run.updated_at).unwrap();
    context.visible_evidence.clear();
    assert_eq!(
        run.classify_command(&context, &request, Some(&receipt)).unwrap_err(),
        InvestigationRunFailure::ScopeMismatch
    );
    request.idempotency_key = "not-a-replay".into();
    assert_eq!(
        run.classify_command(&context, &request, None).unwrap_err(),
        InvestigationRunFailure::ScopeMismatch
    );
}

#[test]
fn needs_evidence_requires_an_explicit_refinement_pause_and_resume_before_more_work() {
    let (mut context, mut run) = fixture();
    run.stop_without_assessment(&context, run.revision, InvestigationRunStatus::NeedsEvidence, at())
        .unwrap();
    let request = command(&run, InvestigationCommand::Resume {}, "not-paused");
    assert_eq!(
        run.apply_command(&context, &request, None, at()).unwrap_err(),
        InvestigationRunFailure::InvalidStateTransition
    );
    seed(&mut context, &mut run, &[evidence(700)]);
    assert_eq!(run.status, InvestigationRunStatus::Paused);
    apply(&mut run, &context, InvestigationCommand::Resume {}, "resume");
    let admitted = collect(&run, 1, 0);
    run.admit_step(&context, &admitted, run.updated_at).unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Running);
}

#[test]
fn deadlines_block_new_work_and_new_conclusions_but_not_stopping_or_old_receipts() {
    let (context, mut run) = fixture();
    let deadline = run.deadline;
    assert_eq!(
        run.admit_step(&context, &collect(&run, 1, 0), deadline).unwrap_err(),
        InvestigationRunFailure::DeadlineExceeded
    );
    let (pause, receipt) = apply(&mut run, &context, InvestigationCommand::Pause {}, "pause");
    let resume = command(&run, InvestigationCommand::Resume {}, "resume");
    assert_eq!(
        run.apply_command(&context, &resume, None, deadline).unwrap_err(),
        InvestigationRunFailure::DeadlineExceeded
    );
    run.apply_command(&context, &pause, Some(&receipt), deadline).unwrap();
    let cancel = command(&run, InvestigationCommand::Cancel {}, "cancel");
    run.apply_command(&context, &cancel, None, deadline).unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Cancelled);
}
