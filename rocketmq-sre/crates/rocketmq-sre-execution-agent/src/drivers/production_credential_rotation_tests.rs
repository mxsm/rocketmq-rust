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

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use rocketmq_runtime::RuntimeContext;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use super::*;

#[test]
fn kubernetes_secret_references_are_exact_and_namespace_scoped() {
    let reference =
        parse_secret_reference("kubernetes://rocketmq-sre/broker-admin-v2").expect("exact Secret reference");
    assert_eq!(reference.namespace, "rocketmq-sre");
    assert_eq!(reference.name, "broker-admin-v2");
    for invalid in [
        "vault://rocketmq-sre/broker-admin-v2",
        "kubernetes://rocketmq-sre",
        "kubernetes:///broker-admin-v2",
        "kubernetes://rocketmq-sre/BrokerAdmin",
        "kubernetes://rocketmq-sre/broker-admin-v2/extra",
        "kubernetes://*/broker-admin-v2",
    ] {
        assert!(parse_secret_reference(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn selector_requires_a_complete_overlap_tuple() {
    let mut selector = selector_fixture();
    let state = parse_selector(selector.clone(), "broker-admin").expect("baseline selector");
    assert_eq!(state.active_version, "v1");
    assert_eq!(state.active_secret_ref, "kubernetes://rocketmq-sre/broker-admin-v1");
    assert!(state.retiring_version.is_none());

    selector
        .metadata
        .annotations
        .as_mut()
        .expect("annotations")
        .insert(RETIRING_VERSION_ANNOTATION.to_owned(), "v0".to_owned());
    assert!(parse_selector(selector, "broker-admin").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an authorized Kind namespace, authenticated Broker, and Docker PostgreSQL"]
async fn real_credential_overlap_rejects_bad_candidate_and_restores_previous_selector() {
    if std::env::var("ROCKETMQ_SRE_TEST_CREDENTIAL_ROTATION").as_deref() != Ok("1") {
        panic!("set ROCKETMQ_SRE_TEST_CREDENTIAL_ROTATION=1 to authorize the real credential rotation round trip");
    }
    let database_url = required_env("ROCKETMQ_SRE_TEST_DATABASE_URL");
    let namesrv_addr = required_env("ROCKETMQ_SRE_TEST_NAMESRV_ADDR");
    let broker_addr = required_env("ROCKETMQ_SRE_TEST_BROKER_ADDR");
    let namespace = required_env("ROCKETMQ_SRE_TEST_CREDENTIAL_NAMESPACE");
    let selector_name = required_env("ROCKETMQ_SRE_TEST_CREDENTIAL_SELECTOR");
    let credential_set = required_env("ROCKETMQ_SRE_TEST_CREDENTIAL_SET");
    let probe_topic = required_env("ROCKETMQ_SRE_TEST_CREDENTIAL_PROBE_TOPIC");
    let active_reference = required_env("ROCKETMQ_SRE_TEST_ACTIVE_SECRET_REF");
    let candidate_reference = required_env("ROCKETMQ_SRE_TEST_CANDIDATE_SECRET_REF");
    let bad_candidate_reference = required_env("ROCKETMQ_SRE_TEST_BAD_CANDIDATE_SECRET_REF");
    let kubeconfig = required_env("KUBECONFIG");
    assert!(!kubeconfig.is_empty());

    let schema = format!("phase3_credential_rotation_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let runtime = RuntimeContext::from_current("phase3-credential-rotation-smoke");
    let client = ProductionCredentialRotationClient::start(
        &CredentialRotationDriverConfig {
            targets: BTreeMap::from([(
                credential_set.clone(),
                CredentialRotationTarget {
                    namespace: namespace.clone(),
                    selector_name,
                    broker_addr,
                    validation_probe_topic: probe_topic.clone(),
                },
            )]),
            namesrv_addr,
            use_tls: false,
            request_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(10),
        },
        pool.clone(),
        runtime.service_context("credential-rotation-driver"),
    )
    .await
    .expect("start credential rotation driver");
    let baseline = client
        .credential_rotation_state(&credential_set)
        .await
        .expect("read baseline selector");
    assert_eq!(baseline.active_version, "v1");
    assert!(baseline.active_healthy);
    assert!(baseline.retiring_version.is_none());
    assert!(!baseline.candidate_probe_healthy);

    let bad_write = CredentialOverlapWrite {
        credential_set: credential_set.clone(),
        active_version: "v1".to_owned(),
        candidate_version: "v-bad".to_owned(),
        candidate_secret_ref: bad_candidate_reference,
        overlap_seconds: 60,
        validation_probe_topic: probe_topic.clone(),
        operation_id: format!("sre-{}", Uuid::new_v4().simple()),
        execution_id: ExecutionId::new(),
        plan_step_id: PlanStepId::new(),
    };
    assert!(
        client.begin_credential_overlap(&bad_write).await.is_err(),
        "invalid candidate credentials must fail before selector mutation"
    );
    let after_rejection = client
        .credential_rotation_state(&credential_set)
        .await
        .expect("selector remains readable after rejection");
    assert_eq!(after_rejection.active_version, "v1");
    assert!(after_rejection.retiring_version.is_none());

    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let forward_operation = format!("sre-{}", Uuid::new_v4().simple());
    let write = CredentialOverlapWrite {
        credential_set: credential_set.clone(),
        active_version: "v1".to_owned(),
        candidate_version: "v2".to_owned(),
        candidate_secret_ref: candidate_reference,
        overlap_seconds: 60,
        validation_probe_topic: probe_topic,
        operation_id: forward_operation.clone(),
        execution_id,
        plan_step_id,
    };
    client
        .begin_credential_overlap(&write)
        .await
        .expect("activate validated candidate with overlap");
    let overlapping = client
        .credential_rotation_state(&credential_set)
        .await
        .expect("read overlapping selector");
    assert_eq!(overlapping.active_version, "v2");
    assert_eq!(overlapping.retiring_version.as_deref(), Some("v1"));
    assert!(overlapping.active_healthy);
    assert!(overlapping.candidate_probe_healthy);
    assert!(overlapping.overlap_deadline.is_some());
    assert_eq!(
        overlapping.last_operation_id.as_deref(),
        Some(forward_operation.as_str())
    );

    let rollback_operation = format!("sre-{}", Uuid::new_v4().simple());
    client
        .restore_previous_credential(&CredentialOverlapRestore {
            credential_set: credential_set.clone(),
            operation_id: rollback_operation,
            execution_id,
            plan_step_id,
        })
        .await
        .expect("restore previous selector through durable before state");
    let restored = client
        .credential_rotation_state(&credential_set)
        .await
        .expect("read restored selector");
    assert_eq!(restored.active_version, "v1");
    assert!(restored.active_healthy);
    assert!(restored.retiring_version.is_none());
    assert!(!restored.candidate_probe_healthy);
    assert!(
        client
            .load_credentials(&credential_set, "v1", &active_reference, &namespace,)
            .await
            .is_ok()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_agent_credential_rotation_before_states",)
            .fetch_one(&pool)
            .await
            .expect("before journal count"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_agent_credential_rotation_results",)
            .fetch_one(&pool)
            .await
            .expect("result journal count"),
        2
    );

    cleanup_schema(&pool, &schema).await;
    let shutdown = runtime.shutdown_tasks(Duration::from_secs(10)).await;
    assert!(shutdown.is_healthy(), "runtime shutdown report: {shutdown:?}");
}

fn selector_fixture() -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some("broker-admin-selector".to_owned()),
            namespace: Some("rocketmq-sre".to_owned()),
            uid: Some(Uuid::new_v4().to_string()),
            resource_version: Some("100".to_owned()),
            annotations: Some(BTreeMap::from([
                (CREDENTIAL_SET_ANNOTATION.to_owned(), "broker-admin".to_owned()),
                (ACTIVE_VERSION_ANNOTATION.to_owned(), "v1".to_owned()),
                (
                    ACTIVE_SECRET_REF_ANNOTATION.to_owned(),
                    "kubernetes://rocketmq-sre/broker-admin-v1".to_owned(),
                ),
                (PROBE_HEALTHY_ANNOTATION.to_owned(), "false".to_owned()),
            ])),
            ..ObjectMeta::default()
        },
        ..ConfigMap::default()
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be explicit"))
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
