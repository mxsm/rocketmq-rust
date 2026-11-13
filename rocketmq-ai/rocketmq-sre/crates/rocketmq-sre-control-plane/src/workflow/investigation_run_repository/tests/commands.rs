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
async fn concurrent_creation_and_commands_replay_original_receipts_without_new_events() {
    let (repo, auth, ctx, request) = fixture().await;
    let (left, right) = tokio::join!(
        repo.investigation_run_create(&auth, &ctx, &request, Utc::now()),
        repo.investigation_run_create(&auth, &ctx, &request, Utc::now())
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_ne!(left.replayed, right.replayed);
    assert_eq!(left.run, right.run);
    assert_eq!(head(&repo, request.run_id).await, 1);
    let cancel = command(&left.run, InvestigationCommand::Cancel {}, "cancel-one");
    let (left, right) = tokio::join!(
        repo.investigation_run_command(&auth, &ctx, &cancel, Utc::now()),
        repo.investigation_run_command(&auth, &ctx, &cancel, Utc::now())
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_ne!(left.replayed, right.replayed);
    assert_eq!(left.receipt, right.receipt);
    assert_eq!(head(&repo, request.run_id).await, 2);
    let saved = repo.investigation_run_get(&auth, request.run_id).await.unwrap();
    assert_eq!(saved.status, InvestigationRunStatus::Cancelled);
    // A replay does not replace the current snapshot with the queued create response.
    assert!(
        repo.investigation_run_create(&auth, &ctx, &request, Utc::now() + Duration::hours(1))
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(repo.investigation_run_get(&auth, request.run_id).await.unwrap(), saved);
    assert!(
        repo.investigation_run_command(&auth, &ctx, &cancel, Utc::now() + Duration::hours(1))
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(head(&repo, request.run_id).await, 2);
    let mut changed = cancel.clone();
    changed.reason = "different semantics".to_owned();
    assert_eq!(
        repo.investigation_run_command(&auth, &ctx, &changed, Utc::now())
            .await
            .unwrap_err()
            .code(),
        "idempotency_conflict"
    );
    changed = cancel.clone();
    changed.idempotency_key = "new-key".to_owned();
    changed.command_id = InvestigationCommandId::new();
    assert_eq!(
        repo.investigation_run_command(&auth, &ctx, &changed, Utc::now())
            .await
            .unwrap_err()
            .code(),
        "revision_conflict"
    );
    let mut actor = auth.clone();
    actor.subject = "other-operator".to_owned();
    let mut actor_ctx = ctx;
    actor_ctx.actor_subject = actor.subject.clone();
    assert!(
        repo.investigation_run_command(&actor, &actor_ctx, &cancel, Utc::now())
            .await
            .is_err()
    );
    actor.roles.clear();
    assert_eq!(
        repo.investigation_run_command(&actor, &actor_ctx, &cancel, Utc::now())
            .await
            .unwrap_err()
            .code(),
        "unauthorized"
    );
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn stale_distinct_commands_conflict_and_scope_is_checked_before_replay() {
    let (repo, auth, ctx, lease) = running().await;
    let pause = command(&lease.run, InvestigationCommand::Pause {}, "pause");
    let cancel = command(&lease.run, InvestigationCommand::Cancel {}, "cancel-race");
    let (left, right) = tokio::join!(
        repo.investigation_run_command(&auth, &ctx, &pause, Utc::now()),
        repo.investigation_run_command(&auth, &ctx, &cancel, Utc::now())
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let error = left.as_ref().err().or(right.as_ref().err()).unwrap();
    assert_eq!(error.code(), "revision_conflict");
    let winner = if left.is_ok() { pause } else { cancel };
    let mut revoked = auth.clone();
    revoked.clusters.clear();
    assert!(
        repo.investigation_run_command(&revoked, &ctx, &winner, Utc::now())
            .await
            .is_err()
    );
    assert!(repo.investigation_run_get(&revoked, lease.run.id).await.is_err());
    assert!(
        repo.investigation_run_events(&revoked, lease.run.id, 0, 10)
            .await
            .is_err()
    );
    let mut foreign = auth.clone();
    foreign.tenant_id = TenantId::new();
    assert!(repo.investigation_run_get(&foreign, lease.run.id).await.is_err());
    assert_eq!(head(&repo, lease.run.id).await, 4);
}
