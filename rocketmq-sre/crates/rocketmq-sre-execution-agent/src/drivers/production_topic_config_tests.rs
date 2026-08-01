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
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use super::*;

#[test]
fn before_snapshot_contains_only_topic_fields_changed_by_the_plan() {
    let live = TopicConfigPatch {
        read_queue_nums: Some(4),
        write_queue_nums: Some(6),
        order: Some(false),
    };
    let requested = TopicConfigPatch {
        read_queue_nums: Some(8),
        order: Some(true),
        ..TopicConfigPatch::default()
    };

    assert_eq!(
        select_before_values(live, &requested).expect("closed before state"),
        TopicConfigPatch {
            read_queue_nums: Some(4),
            write_queue_nums: None,
            order: Some(false),
        }
    );
}

#[test]
fn missing_live_topic_field_fails_closed_before_any_write() {
    let requested = TopicConfigPatch {
        write_queue_nums: Some(8),
        ..TopicConfigPatch::default()
    };

    assert!(matches!(
        select_before_values(TopicConfigPatch::default(), &requested),
        Err(ExecutionAgentError::DriverFailed)
    ));
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn topic_journal_is_append_only_and_requires_all_brokers_for_reconciliation() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase3_topic_journal_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");

    let journal = TopicConfigJournal::new(pool.clone());
    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let before = TopicBeforeState {
        topic: "SRE_TOPIC_CAS_TEST".to_owned(),
        operation_id: "topic-forward-1".to_owned(),
        expected_version: 7,
        brokers: vec![
            TopicBeforeBroker {
                broker_addr: "broker-a:10911".to_owned(),
                version: 7,
                before: TopicConfigPatch {
                    read_queue_nums: Some(4),
                    ..TopicConfigPatch::default()
                },
            },
            TopicBeforeBroker {
                broker_addr: "broker-b:10911".to_owned(),
                version: 7,
                before: TopicConfigPatch {
                    read_queue_nums: Some(4),
                    ..TopicConfigPatch::default()
                },
            },
        ],
        forward_patch: TopicConfigPatch {
            read_queue_nums: Some(6),
            ..TopicConfigPatch::default()
        },
    };
    assert_eq!(
        journal
            .persist_before(execution_id, plan_step_id, &before, Utc::now())
            .await
            .expect("first before state"),
        before
    );
    assert_eq!(
        journal
            .persist_before(execution_id, plan_step_id, &before, Utc::now())
            .await
            .expect("identical replay"),
        before
    );

    let applied = TopicConfigPatchApplyOutcome::Applied {
        previous_version: 7,
        version: 8,
    };
    journal
        .append_result(
            execution_id,
            plan_step_id,
            &before.topic,
            &before.brokers[0].broker_addr,
            &before.operation_id,
            OperationDirection::Forward,
            7,
            applied,
            Utc::now(),
        )
        .await
        .expect("first Broker result");
    let targets = before
        .brokers
        .iter()
        .map(|broker| broker.broker_addr.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        journal
            .last_applied_operation(&before.topic, 8, &targets)
            .await
            .expect("partial reconciliation"),
        None
    );
    journal
        .append_result(
            execution_id,
            plan_step_id,
            &before.topic,
            &before.brokers[1].broker_addr,
            &before.operation_id,
            OperationDirection::Forward,
            7,
            applied,
            Utc::now(),
        )
        .await
        .expect("second Broker result");
    assert_eq!(
        journal
            .last_applied_operation(&before.topic, 8, &targets)
            .await
            .expect("complete reconciliation"),
        Some(before.operation_id.clone())
    );
    assert!(
        sqlx::query(
            "UPDATE execution_agent_topic_config_before_states
             SET topic = 'tampered'
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
#[ignore = "requires an explicitly configured disposable RocketMQ Topic and Docker PostgreSQL"]
async fn real_topic_version_cas_rejects_stale_write_and_rolls_back() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let namesrv_addr =
        std::env::var("ROCKETMQ_SRE_TEST_NAMESRV_ADDR").expect("test NameServer address must be explicit");
    let topic = std::env::var("ROCKETMQ_SRE_TEST_TOPIC").expect("dedicated test Topic must be explicit");
    let schema = format!("phase3_topic_cas_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let runtime = RuntimeContext::from_current("phase3-topic-cas-smoke");
    let client = ProductionTopicConfigPatchClient::start(
        &BrokerAdminDriverConfig {
            namesrv_addr,
            use_tls: false,
            request_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(10),
            read_credentials: test_credentials("READ"),
            mutation_credentials: test_credentials("MUTATION"),
        },
        pool.clone(),
        runtime.service_context("topic-config-driver"),
    )
    .await
    .expect("start real Topic adapter");

    let before = client.live_state(&topic).await.expect("read live Topic state");
    assert!(before.aggregate.configuration_consistent);
    let original = before
        .aggregate
        .values
        .read_queue_nums
        .expect("test Topic must expose read queues");
    let changed = if original == 6 { 7 } else { 6 };
    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let forward_operation = format!("phase3-topic-forward-{}", Uuid::new_v4());
    let forward = client
        .patch_topic_config(&TopicConfigPatchWrite {
            topic: topic.clone(),
            expected_version: before.aggregate.version,
            patch: TopicConfigPatch {
                read_queue_nums: Some(changed),
                ..TopicConfigPatch::default()
            },
            operation_id: forward_operation.clone(),
            execution_id,
            plan_step_id,
        })
        .await;

    let scenario = async {
        let TopicConfigPatchApplyOutcome::Applied {
            previous_version,
            version: forward_version,
        } = forward.map_err(|error| format!("forward patch failed: {error:?}"))?
        else {
            return Err("fresh Topic version unexpectedly conflicted".to_owned());
        };
        if previous_version != before.aggregate.version || forward_version <= before.aggregate.version {
            return Err("forward patch did not advance from the observed Topic version".to_owned());
        }
        let after_forward = client
            .live_state(&topic)
            .await
            .map_err(|error| format!("forward verification failed: {error:?}"))?;
        if !after_forward.aggregate.configuration_consistent
            || after_forward.aggregate.version != forward_version
            || after_forward.aggregate.values.read_queue_nums != Some(changed)
            || after_forward.aggregate.last_operation_id.as_deref() != Some(&forward_operation)
        {
            return Err("forward Topic state or durable reconciliation drifted".to_owned());
        }

        let stale = client
            .patch_topic_config(&TopicConfigPatchWrite {
                topic: topic.clone(),
                expected_version: before.aggregate.version,
                patch: TopicConfigPatch {
                    read_queue_nums: Some(original),
                    ..TopicConfigPatch::default()
                },
                operation_id: format!("phase3-topic-stale-{}", Uuid::new_v4()),
                execution_id: ExecutionId::new(),
                plan_step_id: PlanStepId::new(),
            })
            .await
            .map_err(|error| format!("stale Topic patch returned an unknown result: {error:?}"))?;
        if stale
            != (TopicConfigPatchApplyOutcome::VersionConflict {
                expected_version: before.aggregate.version,
                actual_version: forward_version,
            })
        {
            return Err("stale Topic version was not rejected without overwrite".to_owned());
        }
        Ok::<u64, String>(forward_version)
    }
    .await;

    let rollback_operation = format!("phase3-topic-rollback-{}", Uuid::new_v4());
    let rollback = client
        .restore_topic_config(&TopicConfigPatchRestore {
            topic: topic.clone(),
            operation_id: rollback_operation.clone(),
            execution_id,
            plan_step_id,
        })
        .await;
    let restored = client.live_state(&topic).await;
    client.shutdown().await;
    let shutdown = runtime.shutdown_tasks(Duration::from_secs(10)).await;
    cleanup_schema(&pool, &schema).await;

    let forward_version = scenario.expect("real Topic version-CAS scenario");
    let TopicConfigPatchApplyOutcome::Applied {
        previous_version,
        version: rollback_version,
    } = rollback.expect("inverse Topic patch at latest version")
    else {
        panic!("inverse Topic patch unexpectedly conflicted");
    };
    assert_eq!(previous_version, forward_version);
    assert!(rollback_version > forward_version);
    let restored = restored.expect("read restored Topic state");
    assert!(restored.aggregate.configuration_consistent);
    assert_eq!(restored.aggregate.version, rollback_version);
    assert_eq!(restored.aggregate.values.read_queue_nums, Some(original));
    assert_eq!(
        restored.aggregate.last_operation_id.as_deref(),
        Some(rollback_operation.as_str())
    );
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
