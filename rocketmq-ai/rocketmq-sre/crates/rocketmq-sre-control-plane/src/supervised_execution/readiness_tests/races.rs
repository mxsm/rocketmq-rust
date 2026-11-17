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

use axum::http::StatusCode;
use chrono::Duration;
use rocketmq_sre_contracts::{CorrelationId, IncidentStatus};
use serde_json::json;

use super::support::*;

async fn incident_lock_waiters(repository: &crate::PostgresRepository, blocker_pid: i32) -> i64 {
    // A second row waiter can queue behind the first waiter rather than directly
    // behind the holder. Follow that chain without admitting another fixture's locks.
    sqlx::query_scalar(
        "WITH RECURSIVE blocked(pid) AS (
            SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))
            UNION
            SELECT activity.pid FROM pg_stat_activity activity
            JOIN blocked ON blocked.pid = ANY(pg_blocking_pids(activity.pid))
        )
        SELECT COUNT(*) FROM pg_stat_activity activity JOIN blocked USING (pid)
        WHERE datname=current_database() AND wait_event_type='Lock'
          AND query LIKE '%FROM sre_incidents%' AND query LIKE '%FOR UPDATE%'",
    )
    .bind(blocker_pid)
    .fetch_one(&repository.pool)
    .await
    .unwrap()
}

pub(super) async fn wait_for_incident_locks(repository: &crate::PostgresRepository, blocker_pid: i32, minimum: i64) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting = incident_lock_waiters(repository, blocker_pid).await;
            if waiting >= minimum {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real SQL admission must wait on the Incident lock");
}

#[tokio::test]
#[ignore = "requires explicit disposable PostgreSQL and independent Incident lock holders"]
async fn postgres_readiness_lock_waiters_ignore_unrelated_incidents() {
    let fixture = Fixture::new().await;
    let other = Fixture::new().await;
    let mut lock = fixture.repository.pool.begin().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid() FROM sre_incidents WHERE id=$1 FOR UPDATE")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let mut unrelated_lock = other.repository.pool.begin().await.unwrap();
    let unrelated_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid() FROM sre_incidents WHERE id=$1 FOR UPDATE")
        .bind(other.incident_id.as_uuid())
        .fetch_one(&mut *unrelated_lock)
        .await
        .unwrap();
    let unrelated_waiter = sqlx::query("SELECT id FROM sre_incidents WHERE id=$1 FOR UPDATE")
        .bind(other.incident_id.as_uuid())
        .execute(&other.repository.pool);
    let check = async {
        wait_for_incident_locks(&fixture.repository, unrelated_pid, 1).await;
        assert_eq!(incident_lock_waiters(&fixture.repository, blocker_pid).await, 0);
        unrelated_lock.rollback().await.unwrap();
    };
    let (result, ()) = tokio::join!(unrelated_waiter, check);
    result.unwrap();
    lock.rollback().await.unwrap();
    other.close().await;
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL and synchronized expiry boundary"]
async fn postgres_readiness_plan_expiry_is_exclusive_even_when_evidence_is_still_fresh() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let expiry = *fixture.clock.lock().unwrap() + Duration::seconds(5);
    let mut body = fixture.create_body();
    body["expires_at"] = json!(expiry);
    let mut lock = fixture.repository.pool.begin().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM sre_incidents WHERE id=$1 FOR UPDATE")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let pending = fixture.call("/v1/plans", &fixture.auth, body);
    let release = async {
        wait_for_incident_locks(&fixture.repository, blocker_pid, 1).await;
        *fixture.clock.lock().unwrap() = expiry;
        lock.commit().await.unwrap();
    };
    let ((status, body), ()) = tokio::join!(pending, release);
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "action_readiness_expired");
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL and two concurrent immutable candidates"]
async fn postgres_readiness_concurrent_plan_versions_have_one_winner_and_a_safe_conflict() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let mut lock = fixture.repository.pool.begin().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM sre_incidents WHERE id=$1 FOR UPDATE")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let first = fixture.call("/v1/plans", &fixture.auth, fixture.create_body());
    let second = async {
        wait_for_incident_locks(&fixture.repository, blocker_pid, 1).await;
        let pending = fixture.call("/v1/plans", &fixture.auth, fixture.create_body());
        let release = async {
            wait_for_incident_locks(&fixture.repository, blocker_pid, 2).await;
            lock.commit().await.unwrap();
        };
        tokio::join!(pending, release).0
    };
    let results = tokio::join!(first, second);
    let outcomes = [results.0, results.1];
    assert_eq!(
        outcomes.iter().filter(|(status, _)| *status == StatusCode::OK).count(),
        1
    );
    let (_, conflict) = outcomes
        .iter()
        .find(|(status, _)| *status == StatusCode::CONFLICT)
        .unwrap();
    assert_eq!(conflict["code"], "plan_revision_changed");
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM action_plans WHERE incident_id=$1")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL and synchronized row-lock waits"]
async fn postgres_readiness_plan_and_approval_sample_fresh_clock_after_row_lock() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let original_now = *fixture.clock.lock().unwrap();
    for approving in [false, true] {
        let plan = if approving { Some(fixture.draft().await) } else { None };
        let mut lock = fixture.repository.pool.begin().await.unwrap();
        let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        sqlx::query("SELECT id FROM sre_incidents WHERE id=$1 FOR UPDATE")
            .bind(fixture.incident_id.as_uuid())
            .fetch_one(&mut *lock)
            .await
            .unwrap();
        let path = plan.as_ref().map_or_else(
            || "/v1/plans".to_owned(),
            |plan| format!("/v1/plans/{}/approve", plan.id),
        );
        let body = plan.as_ref().map_or_else(|| fixture.create_body(), |plan| json!({"plan_hash":plan.plan_hash,
            "precondition_hash":plan.compute_precondition_hash().unwrap(), "reason":"Independent synchronized expiry regression"}));
        let actor = if approving {
            fixture.approver()
        } else {
            fixture.auth.clone()
        };
        let pending = fixture.call(&path, &actor, body);
        let release = async {
            wait_for_incident_locks(&fixture.repository, blocker_pid, 1).await;
            *fixture.clock.lock().unwrap() = original_now + Duration::seconds(121);
            lock.commit().await.unwrap();
        };
        let ((status, result), ()) = tokio::join!(pending, release);
        assert_eq!(status, StatusCode::CONFLICT, "{result}");
        assert_eq!(result["code"], "action_readiness_expired");
        if let Some(plan) = plan {
            let approvals: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM approvals WHERE plan_id=$1")
                .bind(plan.id.as_uuid())
                .fetch_one(&fixture.repository.pool)
                .await
                .unwrap();
            assert_eq!(approvals, 0);
        }
        *fixture.clock.lock().unwrap() = original_now;
    }
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL and actual production diagnosis append"]
async fn postgres_readiness_current_revision_serializes_with_real_diagnosis_writer() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let mut lock = fixture.repository.pool.begin().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM sre_incidents WHERE id=$1 FOR UPDATE")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    // Queue the actual production append first, then the action transaction.
    // Both must use the same Incident row, so the append wins before admission.
    let append = fixture.repository.persist_diagnosis_revision(
        &fixture.auth,
        fixture.incident_id,
        IncidentStatus::Diagnosing,
        json!({"provenance":"SyntheticFixture","reason":"new diagnosis invalidates old intent"}),
        json!([]),
        vec![fixture.diagnostic_id],
        false,
        None,
        "rules_only",
        CorrelationId::new(),
    );
    let queued_action = async {
        wait_for_incident_locks(&fixture.repository, blocker_pid, 1).await;
        let action = fixture.call("/v1/plans", &fixture.auth, fixture.create_body());
        let unlock = async {
            wait_for_incident_locks(&fixture.repository, blocker_pid, 2).await;
            lock.commit().await.unwrap();
        };
        tokio::join!(action, unlock).0
    };
    let (revision, (status, body)) = tokio::join!(append, queued_action);
    revision.expect("real diagnosis append should win the shared lock");
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "diagnosis_revision_stale");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM action_plans WHERE incident_id=$1")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    fixture.close().await;
}
