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

use std::time::Duration;

use chrono::Utc;
use rocketmq_runtime::RuntimeContext;
use rocketmq_sre_contracts::*;
use sqlx::postgres::PgPoolOptions;

use super::{InvestigationRunService, test_support::fixture_in};
use crate::PostgresRepository;
use crate::workflow::investigation_run_repository::RunWorkerContext;

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL; owns an isolated UUID schema"]
async fn postgres_managed_recovery_cancels_a_blocked_transaction_and_a_new_owner_can_resume() {
    let url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").unwrap();
    let admin = PostgresRepository::connect(&url, 2).await.unwrap();
    let schema = format!("run_lifecycle_{}", uuid::Uuid::new_v4().simple());
    // Only a fixed prefix and generated UUID enter these owned-schema statements.
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin.pool)
        .await
        .unwrap();
    let fixture_pool = scoped_pool(&url, &schema, 6).await;
    sqlx::migrate!("../../migrations").run(&fixture_pool).await.unwrap();
    let fixture = fixture_in(PostgresRepository::from_pool(fixture_pool.clone())).await;
    let created = fixture.create().await;
    let context = fixture
        .state
        .investigation_runs
        .context(
            &fixture.auth,
            &created.run.initial_scope,
            &created.run.initial_scope,
            &[],
        )
        .await
        .unwrap();
    let lease = fixture
        .repository
        .investigation_run_claim(&fixture.auth, created.run.id, uuid::Uuid::new_v4(), 30, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let worker = RunWorkerContext {
        auth: &fixture.auth,
        context: &context,
        fence: &lease.fence,
    };
    let started = fixture
        .repository
        .investigation_run_start(&worker, created.run.id, created.run.revision, Utc::now())
        .await
        .unwrap();
    let requirement = context
        .allowed_requirements
        .iter()
        .find(|requirement| requirement.pack == fixture.request.packs[0])
        .unwrap();
    let admission = InvestigationStepAdmission {
        step_id: InvestigationStepId::new(),
        attempt_id: StepAttemptId::new(),
        expected_revision: started.revision,
        spec: InvestigationStepSpec {
            scope: started.effective_scope.clone(),
            pack: requirement.pack.clone(),
            work: InvestigationStepKind::CollectEvidence {
                requirement_id: requirement.requirement_id.clone(),
            },
        },
        reservation: InvestigationBudgetCost {
            read_queries: 1,
            evidence_items: 1,
            output_bytes: 64,
            ..Default::default()
        },
    };
    let before = fixture
        .repository
        .investigation_run_admit(&worker, started.id, &admission, Utc::now())
        .await
        .unwrap();
    sqlx::query("UPDATE investigation_runs SET worker_lease_until=NOW()-INTERVAL '1 second',recovery_checked_at=NULL WHERE id=$1")
        .bind(before.id.as_uuid()).execute(&fixture.repository.pool).await.unwrap();
    let original_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM investigation_run_events WHERE run_id=$1")
        .bind(before.id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    let pool = scoped_pool(&url, &schema, 1).await;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let service = InvestigationRunService::new(PostgresRepository::from_pool(pool.clone())).unwrap();
    let mut barrier = fixture.repository.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE investigation_run_events IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *barrier)
        .await
        .unwrap();
    let runtime = RuntimeContext::from_current("d14-recovery-lifecycle");
    let owner = runtime.service_context("recovery");
    service.schedule_recovery(&owner).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND query LIKE '%INSERT INTO investigation_run_events%')")
                .bind(pid).fetch_one(&fixture.repository.pool).await.unwrap();
            if blocked { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("recovery reached event insertion under the controlled table lock");
    owner.task_group().cancel();
    let report = tokio::time::timeout(
        Duration::from_secs(3),
        owner.task_group().shutdown(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
    barrier.rollback().await.unwrap();
    // Reusing this single connection waits for SQLx's queued transaction
    // rollback after the blocked statement is released.
    tokio::time::timeout(Duration::from_secs(5), sqlx::query("SELECT 1").execute(&pool))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture
            .repository
            .investigation_run_get(&fixture.auth, before.id)
            .await
            .unwrap(),
        before
    );
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM investigation_run_events WHERE run_id=$1")
        .bind(before.id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(event_count, original_events);
    let recovered = fixture
        .repository
        .investigation_run_reconcile(&fixture.auth, before.id, uuid::Uuid::new_v4(), 30, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status, InvestigationRunStatus::Paused);
    assert!(recovered.steps[0].attempts[0].interruption.is_some());
    assert!(recovered.steps[0].attempts[0].result.is_none());
    assert_eq!(recovered.budget_usage().unwrap().1, admission.reservation);
    assert!(
        fixture
            .repository
            .investigation_run_renew(&fixture.auth, before.id, &lease.fence, 30, Utc::now())
            .await
            .is_err()
    );
    pool.close().await;
    fixture_pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin.pool)
        .await
        .unwrap();
}

async fn scoped_pool(url: &str, schema: &str, connections: u32) -> sqlx::PgPool {
    let schema = schema.to_owned();
    PgPoolOptions::new()
        .max_connections(connections)
        .after_connect(move |connection, _| {
            let schema = schema.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path',$1,false)")
                    .bind(schema)
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .unwrap()
}
