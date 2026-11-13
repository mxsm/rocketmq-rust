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
fn interrupted_work_retains_unknown_reservation_across_reload_and_retry() {
    let (ctx, mut run) = fixture();
    let mut admission = collect(&run, 1, 0);
    run.admit_step(&ctx, &admission, at()).unwrap();
    let late = result(&run, &admission, vec![]);
    assert_eq!(
        run.interrupt_attempt(&ctx, run.revision, admission.step_id, admission.attempt_id, at())
            .unwrap(),
        InvestigationInterruptionAcceptance::Recorded
    );
    assert_eq!(run.status, InvestigationRunStatus::Paused);
    assert_eq!(run.in_flight_count(), 0);
    assert_eq!(
        run.budget_usage().unwrap(),
        (InvestigationBudgetCost::default(), admission.reservation)
    );
    let json = serde_json::to_value(&run).unwrap();
    let mut run: InvestigationRun = serde_json::from_value(json).unwrap();
    run.validate().unwrap();
    let unchanged = run.clone();
    assert_eq!(
        run.interrupt_attempt(
            &ctx,
            InvestigationRevision::from_u64(0),
            admission.step_id,
            admission.attempt_id,
            at()
        )
        .unwrap(),
        InvestigationInterruptionAcceptance::Replay
    );
    assert_eq!(run, unchanged);
    assert_eq!(
        run.apply_step_result(&ctx, &late, late.finished_at),
        Err(InvestigationRunFailure::ResultConflict)
    );
    let resume = command(&run, InvestigationCommand::Resume {}, "resume-recovered");
    run.apply_command(&ctx, &resume, None, at()).unwrap();
    admission.attempt_id = StepAttemptId::new();
    admission.expected_revision = run.revision;
    run.admit_step(&ctx, &admission, at()).unwrap();
    let (_, reserved) = run.budget_usage().unwrap();
    assert_eq!(reserved.read_queries, 2);
    assert_eq!(reserved.evidence_items, 2);
    assert_eq!(run.query_retry_count().unwrap(), 1);
}

#[test]
fn old_attempt_json_remains_compatible_and_known_result_cannot_be_interrupted() {
    let (ctx, mut run) = fixture();
    let admission = collect(&run, 2, 0);
    run.admit_step(&ctx, &admission, at()).unwrap();
    let old = serde_json::to_value(&run).unwrap();
    assert!(old["steps"][0]["attempts"][0].get("interruption").is_none());
    let restored: InvestigationRun = serde_json::from_value(old).unwrap();
    assert_eq!(run, restored);
    let value = result(&run, &admission, vec![]);
    run.apply_step_result(&ctx, &value, value.finished_at).unwrap();
    assert_eq!(
        run.interrupt_attempt(
            &ctx,
            run.revision,
            admission.step_id,
            admission.attempt_id,
            value.finished_at
        ),
        Err(InvestigationRunFailure::ResultConflict)
    );
    assert!(
        serde_json::to_value(&run).unwrap()["steps"][0]["attempts"][0]
            .get("interruption")
            .is_none()
    );
    let mut retry = admission.clone();
    retry.expected_revision = run.revision;
    retry.attempt_id = StepAttemptId::new();
    assert_eq!(
        run.admit_step(&ctx, &retry, value.finished_at),
        Err(InvestigationRunFailure::StepAlreadyCompleted)
    );
}

#[test]
fn interruption_drains_cancel_without_claiming_measured_usage() {
    let (ctx, mut run) = fixture();
    let admission = collect(&run, 3, 0);
    run.admit_step(&ctx, &admission, at()).unwrap();
    let cancel = command(&run, InvestigationCommand::Cancel {}, "cancel-interruption");
    run.apply_command(&ctx, &cancel, None, at()).unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Cancelling);
    run.interrupt_attempt(&ctx, run.revision, admission.step_id, admission.attempt_id, at())
        .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Cancelled);
    assert_eq!(run.budget_usage().unwrap().0, InvestigationBudgetCost::default());
    let mut invalid = run.clone();
    invalid.steps[0].attempts[0].interruption.as_mut().unwrap().recovered_at = at() - Duration::seconds(1);
    assert!(invalid.validate().is_err());
}
