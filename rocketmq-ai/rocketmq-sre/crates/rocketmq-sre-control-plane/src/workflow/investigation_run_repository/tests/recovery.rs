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

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn unknown_work_is_fenced_paused_and_conservatively_reserved_after_restart() {
    let (repo, auth, ctx, lease) = running().await;
    let admission = collect(&lease.run);
    let admitted = repo
        .investigation_run_admit(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence,
            },
            lease.run.id,
            &admission,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(
        repo.investigation_run_release(&auth, admitted.id, &lease.fence, Utc::now())
            .await
            .unwrap_err()
            .code(),
        "step_in_flight"
    );
    let late = result(&admitted, &admission);
    expire(&repo, admitted.id).await;
    let candidates = repo.investigation_run_recoverable(32, Utc::now()).await.unwrap();
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate.run_id == admitted.id && candidate.tenant_id == auth.tenant_id)
    );
    // A new repository represents a restarted process using the same durable DB.
    let restarted = repository().await;
    let new_owner = Uuid::new_v4();
    let recovered = restarted
        .investigation_run_reconcile(&auth, admitted.id, new_owner, 30, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status, InvestigationRunStatus::Paused);
    assert_eq!(recovered.in_flight_count(), 0);
    assert!(recovered.steps[0].attempts[0].result.is_none());
    assert!(recovered.steps[0].attempts[0].interruption.is_some());
    assert_eq!(
        recovered.budget_usage().unwrap(),
        (InvestigationBudgetCost::default(), admission.reservation)
    );
    assert_eq!(
        restarted.investigation_run_get(&auth, admitted.id).await.unwrap(),
        recovered
    );
    assert!(
        restarted
            .investigation_run_reconcile(&auth, admitted.id, new_owner, 30, Utc::now())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        repo.investigation_run_settle(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence
            },
            &late,
            Utc::now()
        )
        .await
        .unwrap_err()
        .code(),
        "worker_lease_lost"
    );
    let next = restarted
        .investigation_run_claim(&auth, admitted.id, Uuid::new_v4(), 60, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let resume = command(&recovered, InvestigationCommand::Resume {}, "resume-after-recovery");
    restarted
        .investigation_run_command(&auth, &ctx, &resume, Utc::now())
        .await
        .unwrap();
    let current = restarted.investigation_run_get(&auth, admitted.id).await.unwrap();
    let mut retry = admission;
    retry.attempt_id = StepAttemptId::new();
    retry.expected_revision = current.revision;
    let retried = restarted
        .investigation_run_admit(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &next.fence,
            },
            admitted.id,
            &retry,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(retried.query_retry_count().unwrap(), 1);
    assert_eq!(retried.budget_usage().unwrap().1.read_queries, 2);
    assert_eq!(
        restarted
            .investigation_run_settle(
                &RunWorkerContext {
                    auth: &auth,
                    context: &ctx,
                    fence: &next.fence
                },
                &late,
                Utc::now()
            )
            .await
            .unwrap_err()
            .code(),
        "result_conflict"
    );
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn exact_results_do_not_recharge_or_emit_and_success_never_redispatches() {
    let (repo, auth, ctx, lease) = running().await;
    let admission = collect(&lease.run);
    let admitted = repo
        .investigation_run_admit(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence,
            },
            lease.run.id,
            &admission,
            Utc::now(),
        )
        .await
        .unwrap();
    let value = result(&admitted, &admission);
    let settled = repo
        .investigation_run_settle(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence,
            },
            &value,
            Utc::now(),
        )
        .await
        .unwrap();
    let count = head(&repo, settled.id).await;
    assert_eq!(
        repo.investigation_run_settle(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence
            },
            &value,
            Utc::now()
        )
        .await
        .unwrap(),
        settled
    );
    assert_eq!(head(&repo, settled.id).await, count);
    let mut conflict_result = value.clone();
    conflict_result.actual_cost.output_bytes += 1;
    assert_eq!(
        repo.investigation_run_settle(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence
            },
            &conflict_result,
            Utc::now()
        )
        .await
        .unwrap_err()
        .code(),
        "result_conflict"
    );
    expire(&repo, settled.id).await;
    let takeover = repo
        .investigation_run_claim(&auth, settled.id, Uuid::new_v4(), 60, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(takeover.run.status, InvestigationRunStatus::Paused);
    assert!(takeover.run.steps[0].attempts[0].interruption.is_none());
    let resume = command(&takeover.run, InvestigationCommand::Resume {}, "resume-success");
    repo.investigation_run_command(&auth, &ctx, &resume, Utc::now())
        .await
        .unwrap();
    let current = repo.investigation_run_get(&auth, settled.id).await.unwrap();
    let mut retry = admission;
    retry.expected_revision = current.revision;
    retry.attempt_id = StepAttemptId::new();
    assert_eq!(
        repo.investigation_run_admit(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &takeover.fence
            },
            current.id,
            &retry,
            Utc::now()
        )
        .await
        .unwrap_err()
        .code(),
        "step_already_completed"
    );
    assert_eq!(
        repo.investigation_run_settle(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence
            },
            &value,
            Utc::now()
        )
        .await
        .unwrap_err()
        .code(),
        "worker_lease_lost"
    );
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn cancel_drains_active_owner_but_expired_owner_cannot_submit_late_results() {
    let (repo, auth, ctx, lease) = running().await;
    let admission = collect(&lease.run);
    let admitted = repo
        .investigation_run_admit(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence,
            },
            lease.run.id,
            &admission,
            Utc::now(),
        )
        .await
        .unwrap();
    let value = result(&admitted, &admission);
    let cancel = command(&admitted, InvestigationCommand::Cancel {}, "cancel-drain");
    assert_eq!(
        repo.investigation_run_command(&auth, &ctx, &cancel, Utc::now())
            .await
            .unwrap()
            .receipt
            .status,
        InvestigationRunStatus::Cancelling
    );
    let done = repo
        .investigation_run_settle(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence,
            },
            &value,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(done.status, InvestigationRunStatus::Cancelled);
    assert_eq!(
        done.steps[0].attempts[0].disposition,
        Some(InvestigationResultDisposition::Superseded)
    );
    assert_eq!(done.budget_usage().unwrap().0.read_queries, 1);
    repo.investigation_run_release(&auth, done.id, &lease.fence, Utc::now())
        .await
        .unwrap();
    assert!(
        repo.investigation_run_claim(&auth, done.id, Uuid::new_v4(), 60, Utc::now())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn queued_recovery_never_starts_and_worker_epoch_overflow_is_atomic() {
    let (repo, auth, ctx, request) = fixture().await;
    repo.investigation_run_create(&auth, &ctx, &request, Utc::now())
        .await
        .unwrap();
    let first = repo
        .investigation_run_claim(&auth, request.run_id, Uuid::new_v4(), 60, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert!(
        repo.investigation_run_claim(&auth, request.run_id, Uuid::new_v4(), 60, Utc::now())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repo.investigation_run_renew(&auth, request.run_id, &first.fence, 60, Utc::now())
            .await
            .unwrap()
            >= first.expires_at
    );
    expire(&repo, request.run_id).await;
    let recovered = repo
        .investigation_run_reconcile(&auth, request.run_id, Uuid::new_v4(), 30, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status, InvestigationRunStatus::Queued);
    assert_eq!(recovered.revision.get(), 0);
    sqlx::query("UPDATE investigation_runs SET worker_epoch=$1 WHERE id=$2")
        .bind(i64::MAX)
        .bind(request.run_id.as_uuid())
        .execute(&repo.pool)
        .await
        .unwrap();
    let count = head(&repo, request.run_id).await;
    assert_eq!(
        repo.investigation_run_claim(&auth, request.run_id, Uuid::new_v4(), 30, Utc::now())
            .await
            .unwrap_err()
            .code(),
        "counter_exhausted"
    );
    assert_eq!(head(&repo, request.run_id).await, count);
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn offboarding_allows_metadata_recovery_and_bad_candidates_do_not_starve_inventory() {
    let (repo, auth, ctx, lease) = running().await;
    let admission = collect(&lease.run);
    let run = repo
        .investigation_run_admit(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence,
            },
            lease.run.id,
            &admission,
            Utc::now(),
        )
        .await
        .unwrap();
    expire(&repo, run.id).await;
    sqlx::query("UPDATE clusters SET onboarding_state='offboarded' WHERE id=$1")
        .bind(auth.clusters.first().unwrap().as_uuid())
        .execute(&repo.pool)
        .await
        .unwrap();
    assert!(repo.investigation_run_get(&auth, run.id).await.is_err());
    assert!(
        repo.investigation_run_claim(&auth, run.id, Uuid::new_v4(), 30, Utc::now())
            .await
            .is_err()
    );
    let recovered = repo
        .investigation_run_reconcile(&auth, run.id, Uuid::new_v4(), 30, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status, InvestigationRunStatus::Paused);
    assert_eq!(recovered.budget_usage().unwrap().1, admission.reservation);
    assert!(repo.investigation_run_get(&auth, run.id).await.is_err());
    let (healthy_auth, healthy_scope) = seed_parent(&repo).await;
    let mut healthy_ctx = ctx;
    healthy_ctx.actor_subject = healthy_auth.subject.clone();
    healthy_ctx.authorized_scope = healthy_scope.clone();
    let mut ids = vec![];
    for _ in 0..34 {
        let request = CreateInvestigationRunRequest {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            run_id: InvestigationRunId::new(),
            scope: healthy_scope.clone(),
            packs: healthy_ctx.allowed_packs.clone(),
            budget: run.budget.clone(),
            deadline: Utc::now() + Duration::minutes(10),
            idempotency_key: "inventory-rotation".to_owned(),
        };
        repo.investigation_run_create(&healthy_auth, &healthy_ctx, &request, Utc::now())
            .await
            .unwrap();
        repo.investigation_run_claim(&healthy_auth, request.run_id, Uuid::new_v4(), 30, Utc::now())
            .await
            .unwrap();
        expire(&repo, request.run_id).await;
        // Simulate malformed older snapshots: inventory can still rotate them.
        if ids.len() < 33 {
            sqlx::query("UPDATE investigation_runs SET run_snapshot=jsonb_set(run_snapshot,'{created_by_subject}','\"\"'::jsonb) WHERE id=$1").bind(request.run_id.as_uuid()).execute(&repo.pool).await.unwrap();
        }
        ids.push(request.run_id);
    }
    let mut reached = false;
    // Shared PG tests may leave other expired candidates; enough bounded batches
    // cover all rows this test creates without relying on a particular row order.
    for _ in 0..8 {
        for candidate in repo.investigation_run_recoverable(32, Utc::now()).await.unwrap() {
            if candidate.run_id == ids[33] {
                let healthy = repo
                    .investigation_run_reconcile(&healthy_auth, candidate.run_id, Uuid::new_v4(), 30, Utc::now())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(healthy.status, InvestigationRunStatus::Queued);
                reached = true;
            }
        }
        if reached {
            break;
        }
    }
    assert!(
        reached,
        "malformed earlier candidates must not monopolize the first batch"
    );
    // Do not leave intentionally poisoned ownership for later lifecycle tests.
    sqlx::query("UPDATE investigation_runs SET worker_owner=NULL,worker_lease_until=NULL WHERE id=ANY($1)")
        .bind(ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .execute(&repo.pool)
        .await
        .unwrap();
}
