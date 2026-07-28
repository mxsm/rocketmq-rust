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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use sqlx::postgres::PgPoolOptions;

use super::*;

#[test]
fn before_snapshot_contains_only_fields_changed_by_the_plan() {
    let live = BrokerConfigPatch {
        send_message_thread_pool_nums: Some(16),
        pull_message_thread_pool_nums: Some(12),
        flush_delay_offset_interval_ms: Some(10_000),
    };
    let requested = BrokerConfigPatch {
        flush_delay_offset_interval_ms: Some(20_000),
        ..BrokerConfigPatch::default()
    };

    let before = select_before_values(&live, &requested).expect("supported before state");
    assert_eq!(
        before,
        BrokerConfigPatch {
            flush_delay_offset_interval_ms: Some(10_000),
            ..BrokerConfigPatch::default()
        }
    );
    assert_eq!(
        broker_properties(&before).expect("inverse patch"),
        BTreeMap::from([("flushDelayOffsetInterval".to_owned(), "10000".to_owned())])
    );
}

#[test]
fn missing_live_field_fails_closed_before_any_write() {
    let live = BrokerConfigPatch::default();
    let requested = BrokerConfigPatch {
        send_message_thread_pool_nums: Some(32),
        ..BrokerConfigPatch::default()
    };

    assert!(matches!(
        select_before_values(&live, &requested),
        Err(ExecutionAgentError::DriverFailed)
    ));
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn journal_is_append_only_idempotent_and_detects_conflicts() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase3_broker_journal_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");

    let journal = BrokerConfigJournal::new(pool.clone());
    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let before = BrokerBeforeState {
        broker_addr: "broker-a:10911".to_owned(),
        operation_id: "sre-forward-1".to_owned(),
        expected_generation: 7,
        before: BrokerConfigPatch {
            flush_delay_offset_interval_ms: Some(10_000),
            ..BrokerConfigPatch::default()
        },
        forward_patch: BrokerConfigPatch {
            flush_delay_offset_interval_ms: Some(20_000),
            ..BrokerConfigPatch::default()
        },
    };
    let persisted = journal
        .persist_before(execution_id, plan_step_id, &before, Utc::now())
        .await
        .expect("first before snapshot");
    assert_eq!(persisted, before);
    assert_eq!(
        journal
            .persist_before(execution_id, plan_step_id, &before, Utc::now())
            .await
            .expect("identical replay"),
        before
    );

    let mut conflicting = before.clone();
    conflicting.operation_id = "sre-forward-conflict".to_owned();
    assert!(matches!(
        journal
            .persist_before(execution_id, plan_step_id, &conflicting, Utc::now())
            .await,
        Err(AgentStoreError::IdempotencyConflict)
    ));

    let outcome = BrokerConfigPatchApplyOutcome::Applied {
        previous_generation: 7,
        generation: 8,
    };
    journal
        .append_result(
            execution_id,
            plan_step_id,
            &before.broker_addr,
            &before.operation_id,
            OperationDirection::Forward,
            7,
            outcome,
            Utc::now(),
        )
        .await
        .expect("first result");
    journal
        .append_result(
            execution_id,
            plan_step_id,
            &before.broker_addr,
            &before.operation_id,
            OperationDirection::Forward,
            7,
            outcome,
            Utc::now(),
        )
        .await
        .expect("identical result replay");
    assert_eq!(
        journal
            .last_applied_operation(&before.broker_addr, 8)
            .await
            .expect("last operation"),
        Some(before.operation_id.clone())
    );
    assert!(
        sqlx::query(
            "UPDATE execution_agent_broker_config_before_states
         SET broker_addr = 'tampered'
         WHERE execution_id = $1",
        )
        .bind(execution_id.as_uuid())
        .execute(&pool)
        .await
        .is_err()
    );

    cleanup_schema(&pool, &schema).await;
}

async fn isolated_pool(database_url: &str, schema: &str) -> PgPool {
    let search_path: Arc<str> = Arc::from(format!("SET search_path TO \"{schema}\""));
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .after_connect(move |connection, _metadata| {
            let search_path = Arc::clone(&search_path);
            Box::pin(async move {
                sqlx::query(search_path.as_ref()).execute(connection).await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
        .expect("Docker PostgreSQL");
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("isolated schema");
    pool
}

async fn cleanup_schema(pool: &PgPool, schema: &str) {
    sqlx::raw_sql(&format!("SET search_path TO public; DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(pool)
        .await
        .expect("drop isolated schema");
    pool.close().await;
}
