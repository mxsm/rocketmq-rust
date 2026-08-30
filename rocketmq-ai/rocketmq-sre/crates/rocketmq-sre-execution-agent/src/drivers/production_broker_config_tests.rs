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

use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_runtime::RuntimeContext;
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
        max_client_event_count: Some(100),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an explicitly configured disposable RocketMQ Broker and Docker PostgreSQL"]
async fn real_broker_generation_cas_rejects_stale_write_and_rolls_back() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let namesrv_addr =
        std::env::var("ROCKETMQ_SRE_TEST_NAMESRV_ADDR").expect("test NameServer address must be explicit");
    let broker_addr = std::env::var("ROCKETMQ_SRE_TEST_BROKER_ADDR").expect("test Broker address must be explicit");
    let schema = format!("phase3_broker_cas_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let runtime = RuntimeContext::from_current("phase3-broker-cas-smoke");
    let client = ProductionBrokerConfigPatchClient::start(
        &BrokerAdminDriverConfig {
            namesrv_addr,
            use_tls: false,
            request_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(10),
            read_credentials: test_credentials("READ"),
            mutation_credentials: test_credentials("MUTATION"),
        },
        pool.clone(),
        runtime.service_context("broker-config-driver"),
    )
    .await
    .expect("start real Broker adapter");

    let before = client.live_state(&broker_addr).await.expect("read live Broker state");
    assert!(
        before.supported_fields.contains(MAX_CLIENT_EVENTS),
        "live maxClientEventCount must be executable"
    );
    assert!(
        before.restart_required_fields.contains(FLUSH_DELAY),
        "visible flushDelayOffsetInterval must remain fail-closed"
    );
    let original = before
        .values
        .max_client_event_count
        .expect("test Broker must expose maxClientEventCount");
    let changed = if original == 101 { 102 } else { 101 };
    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let forward_operation = format!("phase3-forward-{}", Uuid::new_v4());
    let forward = client
        .patch_broker_config(&BrokerConfigPatchWrite {
            broker_addr: broker_addr.clone(),
            expected_generation: before.generation,
            patch: BrokerConfigPatch {
                max_client_event_count: Some(changed),
                ..BrokerConfigPatch::default()
            },
            operation_id: forward_operation.clone(),
            execution_id,
            plan_step_id,
        })
        .await;

    let scenario = async {
        let BrokerConfigPatchApplyOutcome::Applied {
            previous_generation,
            generation: forward_generation,
        } = forward.map_err(|error| format!("forward patch failed: {error:?}"))?
        else {
            return Err("fresh generation unexpectedly conflicted".to_owned());
        };
        if previous_generation != before.generation || forward_generation <= before.generation {
            return Err("forward patch did not advance from the observed generation".to_owned());
        }
        let after_forward = client
            .live_state(&broker_addr)
            .await
            .map_err(|error| format!("forward verification failed: {error:?}"))?;
        if after_forward.generation != forward_generation
            || after_forward.values.max_client_event_count != Some(changed)
            || after_forward.last_operation_id.as_deref() != Some(&forward_operation)
        {
            return Err("forward state or durable operation reconciliation drifted".to_owned());
        }

        let stale = client
            .patch_broker_config(&BrokerConfigPatchWrite {
                broker_addr: broker_addr.clone(),
                expected_generation: before.generation,
                patch: BrokerConfigPatch {
                    max_client_event_count: Some(original),
                    ..BrokerConfigPatch::default()
                },
                operation_id: format!("phase3-stale-{}", Uuid::new_v4()),
                execution_id: ExecutionId::new(),
                plan_step_id: PlanStepId::new(),
            })
            .await
            .map_err(|error| format!("stale patch returned an unknown result: {error:?}"))?;
        if stale
            != (BrokerConfigPatchApplyOutcome::GenerationConflict {
                expected_generation: before.generation,
                actual_generation: forward_generation,
            })
        {
            return Err("stale generation was not rejected without overwrite".to_owned());
        }
        Ok::<u64, String>(forward_generation)
    }
    .await;

    let rollback_operation = format!("phase3-rollback-{}", Uuid::new_v4());
    let rollback = client
        .restore_broker_config(&BrokerConfigPatchRestore {
            broker_addr: broker_addr.clone(),
            operation_id: rollback_operation.clone(),
            execution_id,
            plan_step_id,
        })
        .await;
    let restored = client.live_state(&broker_addr).await;
    client.shutdown().await;
    let shutdown = runtime.shutdown_tasks(Duration::from_secs(10)).await;
    cleanup_schema(&pool, &schema).await;

    let forward_generation = scenario.expect("real generation-CAS scenario");
    let BrokerConfigPatchApplyOutcome::Applied {
        previous_generation,
        generation: rollback_generation,
    } = rollback.expect("inverse patch at latest generation")
    else {
        panic!("inverse patch unexpectedly conflicted");
    };
    assert_eq!(previous_generation, forward_generation);
    assert!(rollback_generation > forward_generation);
    let restored = restored.expect("read restored Broker state");
    assert_eq!(restored.generation, rollback_generation);
    assert_eq!(restored.values.max_client_event_count, Some(original));
    assert_eq!(restored.last_operation_id.as_deref(), Some(rollback_operation.as_str()));
    assert!(shutdown.is_healthy(), "runtime shutdown report: {shutdown:?}");
}

fn test_credentials(identity: &str) -> Option<AdminCredentials> {
    let access_key = std::env::var(format!("ROCKETMQ_SRE_TEST_BROKER_{identity}_ACCESS_KEY")).ok();
    let secret_key = std::env::var(format!("ROCKETMQ_SRE_TEST_BROKER_{identity}_SECRET_KEY")).ok();
    match (access_key, secret_key) {
        (None, None) => None,
        (Some(access_key), Some(secret_key)) => {
            Some(AdminCredentials::try_new(access_key, secret_key, None).expect("valid explicit test credentials"))
        }
        _ => panic!("test Broker credentials must provide both access and secret keys"),
    }
}

async fn isolated_pool(database_url: &str, schema: &str) -> PgPool {
    assert_test_schema(schema);
    let search_path: Arc<str> = Arc::from(format!("SET search_path TO \"{schema}\""));
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .after_connect(move |connection, _metadata| {
            let search_path = Arc::clone(&search_path);
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(search_path))
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
        .expect("Docker PostgreSQL");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&pool)
        .await
        .expect("isolated schema");
    pool
}

async fn cleanup_schema(pool: &PgPool, schema: &str) {
    assert_test_schema(schema);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "SET search_path TO public; DROP SCHEMA \"{schema}\" CASCADE"
    )))
    .execute(pool)
    .await
    .expect("drop isolated schema");
    pool.close().await;
}

fn assert_test_schema(schema: &str) {
    assert!(
        !schema.is_empty()
            && schema
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "test schema must be a generated lowercase ASCII identifier"
    );
}
