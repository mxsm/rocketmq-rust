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

use chrono::TimeDelta;
use chrono::Timelike;
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_runtime::RuntimeContext;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use super::*;
use crate::config::BrokerAdminDriverConfig;

#[test]
fn ttl_rounds_up_and_rejects_expired_or_unbounded_windows() {
    let now = Utc::now();
    assert_eq!(
        ttl_seconds(now + TimeDelta::milliseconds(59_001), now).expect("rounded TTL"),
        60
    );
    assert_eq!(
        ttl_seconds(now + TimeDelta::seconds(900), now).expect("maximum TTL"),
        900
    );
    assert!(ttl_seconds(now + TimeDelta::seconds(59), now).is_err());
    assert!(ttl_seconds(now + TimeDelta::seconds(901), now).is_err());
    assert!(ttl_seconds(now, now).is_err());
}

#[test]
fn admin_state_requires_supported_bounded_level() {
    let supported = logger_state(BrokerLogFilterState {
        schema_version: "rocketmq-admin.broker-log-filter-state.v1".to_owned(),
        supported: true,
        logger: "rocketmq_broker::processor".to_owned(),
        level: Some(BrokerLogLevel::Debug),
        active_operation_id: Some("forward-1".to_owned()),
        last_completed_operation_id: None,
        expires_at_millis: Some(1),
    })
    .expect("supported state");
    assert_eq!(supported.level, "DEBUG");
    assert_eq!(supported.active_operation_id.as_deref(), Some("forward-1"));

    assert!(
        logger_state(BrokerLogFilterState {
            schema_version: "rocketmq-admin.broker-log-filter-state.v1".to_owned(),
            supported: false,
            logger: "rocketmq_broker::processor".to_owned(),
            level: None,
            active_operation_id: None,
            last_completed_operation_id: None,
            expires_at_millis: None,
        })
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn logger_journal_is_append_only_and_idempotent() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase4_logger_journal_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");

    let journal = LoggerLevelJournal::new(pool.clone());
    let before = before_state();
    assert_eq!(
        journal.persist_before(&before).await.expect("first before state"),
        before
    );
    let mut replay = before.clone();
    replay.expires_at += TimeDelta::seconds(5);
    assert_eq!(
        journal
            .persist_before(&replay)
            .await
            .expect("expiry drift does not change step identity"),
        before
    );
    let mut conflict = before.clone();
    conflict.requested_level = "INFO".to_owned();
    assert!(matches!(
        journal.persist_before(&conflict).await,
        Err(AgentStoreError::IdempotencyConflict)
    ));

    let observed = LoggerLevelState {
        level: "DEBUG".to_owned(),
        active_operation_id: Some(before.forward_operation_id.clone()),
        last_completed_operation_id: None,
    };
    journal
        .append_result(&before, &before.forward_operation_id, Direction::Forward, &observed)
        .await
        .expect("first result");
    journal
        .append_result(&before, &before.forward_operation_id, Direction::Forward, &observed)
        .await
        .expect("identical replay");
    assert!(
        sqlx::query(
            "UPDATE execution_agent_logger_level_before_states
             SET requested_level = 'INFO'
             WHERE execution_id = $1",
        )
        .bind(before.execution_id.as_uuid())
        .execute(&pool)
        .await
        .is_err()
    );
    cleanup_schema(&pool, &schema).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an explicitly configured disposable reload-enabled Broker and Docker PostgreSQL"]
async fn real_broker_logger_ttl_applies_verifies_and_restores() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let namesrv_addr =
        std::env::var("ROCKETMQ_SRE_TEST_NAMESRV_ADDR").expect("test NameServer address must be explicit");
    let broker_addr = std::env::var("ROCKETMQ_SRE_TEST_BROKER_ADDR").expect("test Broker address must be explicit");
    let schema = format!("phase4_logger_driver_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let runtime = RuntimeContext::from_current("phase4-logger-driver-smoke");
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
        runtime.service_context("broker-logger-driver"),
    )
    .await
    .expect("start real Broker adapter");
    let logger = "rocketmq_broker::processor";
    let before = client
        .live_logger_state(COMPONENT, &broker_addr, logger)
        .await
        .expect("read logger state");
    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let forward_operation = format!("phase4-logger-forward-{}", Uuid::new_v4());
    client
        .set_logger_level_ttl(&LoggerLevelTtlWrite {
            component: COMPONENT.to_owned(),
            broker_addr: broker_addr.clone(),
            logger: logger.to_owned(),
            level: "DEBUG".to_owned(),
            expires_at: Utc::now() + TimeDelta::seconds(60),
            operation_id: forward_operation.clone(),
            execution_id,
            plan_step_id,
        })
        .await
        .expect("apply logger TTL");
    let active = client
        .live_logger_state(COMPONENT, &broker_addr, logger)
        .await
        .expect("verify logger TTL");
    assert_eq!(active.level, "DEBUG");
    assert_eq!(active.active_operation_id.as_deref(), Some(forward_operation.as_str()));

    let restore_operation = format!("phase4-logger-restore-{}", Uuid::new_v4());
    client
        .restore_logger_level(&LoggerLevelTtlRestore {
            component: COMPONENT.to_owned(),
            broker_addr: broker_addr.clone(),
            logger: logger.to_owned(),
            execution_id,
            plan_step_id,
            operation_id: restore_operation,
        })
        .await
        .expect("restore logger baseline");
    let restored = client
        .live_logger_state(COMPONENT, &broker_addr, logger)
        .await
        .expect("verify logger restoration");
    assert_eq!(restored.level, before.level);
    assert_eq!(restored.active_operation_id, None);
    assert_eq!(
        restored.last_completed_operation_id.as_deref(),
        Some(forward_operation.as_str())
    );

    client.shutdown().await;
    let shutdown = runtime.shutdown_tasks(Duration::from_secs(10)).await;
    cleanup_schema(&pool, &schema).await;
    assert!(shutdown.is_healthy(), "runtime shutdown report: {shutdown:?}");
}

fn before_state() -> LoggerBeforeState {
    LoggerBeforeState {
        execution_id: ExecutionId::new(),
        plan_step_id: PlanStepId::new(),
        component: COMPONENT.to_owned(),
        broker_addr: "127.0.0.1:10911".to_owned(),
        logger: "rocketmq_broker::processor".to_owned(),
        before_level: "INFO".to_owned(),
        requested_level: "DEBUG".to_owned(),
        forward_operation_id: "forward-1".to_owned(),
        expires_at: Utc::now().with_nanosecond(0).expect("zero nanoseconds are valid") + TimeDelta::seconds(60),
    }
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
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(pool)
        .await
        .expect("drop isolated schema");
    pool.close().await;
}
