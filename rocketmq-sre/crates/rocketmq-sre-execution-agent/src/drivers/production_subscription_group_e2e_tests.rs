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

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn subscription_group_journal_is_append_only_and_requires_all_brokers() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let schema = format!("phase3_subscription_journal_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");

    let journal = SubscriptionGroupJournal::new(pool.clone());
    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let safety = safety_state(test_state());
    let before = SubscriptionGroupBeforeState {
        group: "SRE_SUBSCRIPTION_CAS_TEST".to_owned(),
        operation_id: "subscription-forward-1".to_owned(),
        expected_version: 7,
        brokers: vec![
            SubscriptionGroupBeforeBroker {
                broker_addr: "broker-a:10911".to_owned(),
                version: 7,
                before: SubscriptionGroupPatch {
                    retry_max_times: Some(16),
                    ..SubscriptionGroupPatch::default()
                },
                safety,
            },
            SubscriptionGroupBeforeBroker {
                broker_addr: "broker-b:10911".to_owned(),
                version: 7,
                before: SubscriptionGroupPatch {
                    retry_max_times: Some(16),
                    ..SubscriptionGroupPatch::default()
                },
                safety,
            },
        ],
        forward_patch: SubscriptionGroupPatch {
            retry_max_times: Some(8),
            ..SubscriptionGroupPatch::default()
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
    assert_eq!(
        journal
            .load_before_by_operation(&before.operation_id)
            .await
            .expect("operation lookup"),
        before
    );

    let applied = SubscriptionGroupPatchApplyOutcome::Applied {
        previous_version: 7,
        version: 8,
    };
    journal
        .append_result(
            execution_id,
            plan_step_id,
            &before.group,
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
            .last_applied_operation(&before.group, 8, &targets)
            .await
            .expect("partial reconciliation"),
        None
    );
    journal
        .append_result(
            execution_id,
            plan_step_id,
            &before.group,
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
            .last_applied_operation(&before.group, 8, &targets)
            .await
            .expect("complete reconciliation"),
        Some(before.operation_id.clone())
    );
    assert!(
        sqlx::query(
            "UPDATE execution_agent_subscription_group_before_states
             SET consumer_group = 'tampered'
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
#[ignore = "requires an explicitly configured disposable RocketMQ Subscription Group and Docker PostgreSQL"]
async fn real_subscription_group_version_cas_rejects_stale_write_and_rolls_back() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let namesrv_addr =
        std::env::var("ROCKETMQ_SRE_TEST_NAMESRV_ADDR").expect("test NameServer address must be explicit");
    let group =
        std::env::var("ROCKETMQ_SRE_TEST_CONSUMER_GROUP").expect("dedicated test consumer group must be explicit");
    let schema = format!("phase3_subscription_cas_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let runtime = RuntimeContext::from_current("phase3-subscription-cas-smoke");
    let client = ProductionSubscriptionGroupPatchClient::start(
        &BrokerAdminDriverConfig {
            namesrv_addr,
            use_tls: false,
            request_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(10),
            read_credentials: test_credentials("READ"),
            mutation_credentials: test_credentials("MUTATION"),
        },
        pool.clone(),
        runtime.service_context("subscription-group-config-driver"),
    )
    .await
    .expect("start real Subscription Group adapter");

    let before = client
        .live_state(&group)
        .await
        .expect("read live Subscription Group state");
    assert!(before.aggregate.retry_semantics_known);
    assert!(before.aggregate.permissions_unchanged);
    let original = before
        .aggregate
        .values
        .retry_max_times
        .expect("test Subscription Group must expose retryMaxTimes");
    let changed = if original == 8 { 9 } else { 8 };
    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let forward_operation = format!("phase3-subscription-forward-{}", Uuid::new_v4());
    let forward = client
        .patch_subscription_group(&SubscriptionGroupPatchWrite {
            group: group.clone(),
            expected_version: before.aggregate.version,
            patch: SubscriptionGroupPatch {
                retry_max_times: Some(changed),
                ..SubscriptionGroupPatch::default()
            },
            operation_id: forward_operation.clone(),
            execution_id,
            plan_step_id,
        })
        .await;

    let scenario = async {
        let SubscriptionGroupPatchApplyOutcome::Applied {
            previous_version,
            version: forward_version,
        } = forward.map_err(|error| format!("forward patch failed: {error:?}"))?
        else {
            return Err("fresh Subscription Group version unexpectedly conflicted".to_owned());
        };
        if previous_version != before.aggregate.version || forward_version <= before.aggregate.version {
            return Err("forward patch did not advance the observed Subscription Group version".to_owned());
        }
        let after_forward = client
            .live_state(&group)
            .await
            .map_err(|error| format!("forward verification failed: {error:?}"))?;
        if !after_forward.aggregate.retry_semantics_known
            || !after_forward.aggregate.permissions_unchanged
            || after_forward.aggregate.version != forward_version
            || after_forward.aggregate.values.retry_max_times != Some(changed)
            || after_forward.aggregate.last_operation_id.as_deref() != Some(&forward_operation)
        {
            return Err("forward Subscription Group state or durable reconciliation drifted".to_owned());
        }

        let stale = client
            .patch_subscription_group(&SubscriptionGroupPatchWrite {
                group: group.clone(),
                expected_version: before.aggregate.version,
                patch: SubscriptionGroupPatch {
                    retry_max_times: Some(original),
                    ..SubscriptionGroupPatch::default()
                },
                operation_id: format!("phase3-subscription-stale-{}", Uuid::new_v4()),
                execution_id: ExecutionId::new(),
                plan_step_id: PlanStepId::new(),
            })
            .await
            .map_err(|error| format!("stale Subscription Group patch returned an unknown result: {error:?}"))?;
        if stale
            != (SubscriptionGroupPatchApplyOutcome::VersionConflict {
                expected_version: before.aggregate.version,
                actual_version: forward_version,
            })
        {
            return Err("stale Subscription Group version was not rejected without overwrite".to_owned());
        }
        Ok::<u64, String>(forward_version)
    }
    .await;

    let rollback_operation = format!("phase3-subscription-rollback-{}", Uuid::new_v4());
    let rollback = client
        .restore_subscription_group(&SubscriptionGroupPatchRestore {
            group: group.clone(),
            operation_id: rollback_operation.clone(),
            execution_id,
            plan_step_id,
        })
        .await;
    let restored = client.live_state(&group).await;
    client.shutdown().await;
    let shutdown = runtime.shutdown_tasks(Duration::from_secs(10)).await;
    cleanup_schema(&pool, &schema).await;

    let forward_version = scenario.expect("real Subscription Group version-CAS scenario");
    let SubscriptionGroupPatchApplyOutcome::Applied {
        previous_version,
        version: rollback_version,
    } = rollback.expect("inverse Subscription Group patch at latest version")
    else {
        panic!("inverse Subscription Group patch unexpectedly conflicted");
    };
    assert_eq!(previous_version, forward_version);
    assert!(rollback_version > forward_version);
    let restored = restored.expect("read restored Subscription Group state");
    assert!(restored.aggregate.retry_semantics_known);
    assert!(restored.aggregate.permissions_unchanged);
    assert_eq!(restored.aggregate.version, rollback_version);
    assert_eq!(restored.aggregate.values.retry_max_times, Some(original));
    assert_eq!(
        restored.aggregate.last_operation_id.as_deref(),
        Some(rollback_operation.as_str())
    );
    assert!(shutdown.is_healthy(), "runtime shutdown report: {shutdown:?}");
}

fn test_state() -> SubscriptionGroupConfigCasState {
    SubscriptionGroupConfigCasState {
        version: 7,
        retry_max_times: 16,
        retry_queue_nums: 1,
        consume_timeout_minutes: 15,
        consume_enable: true,
        consume_from_min_enable: true,
        consume_broadcast_enable: true,
        consume_message_orderly: false,
        broker_id: 0,
        which_broker_when_consume_slowly: 1,
        notify_consumer_ids_changed_enable: true,
        group_sys_flag: 0,
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
    sqlx::raw_sql(&format!("SET search_path TO public; DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(pool)
        .await
        .expect("drop isolated schema");
}
