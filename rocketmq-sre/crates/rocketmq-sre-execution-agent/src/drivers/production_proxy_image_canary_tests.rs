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
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use super::*;

#[test]
fn image_repository_preserves_registry_ports_and_removes_only_tag_or_digest() {
    assert_eq!(
        image_repository("registry.example.test:5443/messaging/proxy:1.0.0"),
        Some("registry.example.test:5443/messaging/proxy")
    );
    assert_eq!(
        image_repository(
            "registry.example.test:5443/messaging/proxy@sha256:\
             aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        Some("registry.example.test:5443/messaging/proxy")
    );
    assert_eq!(image_repository("proxy:local"), Some("proxy"));
    assert_eq!(image_repository(""), None);
    assert_eq!(image_repository("proxy image:local"), None);
}

#[test]
fn operation_label_accepts_agent_ids_and_rejects_unbounded_or_ambiguous_values() {
    assert_eq!(
        label_value("sre-0123456789abcdef0123456789abcdef").expect("agent operation"),
        "sre-0123456789abcdef0123456789abcdef"
    );
    assert!(label_value("-invalid").is_err());
    assert!(label_value("invalid/operation").is_err());
    assert!(label_value(&"a".repeat(64)).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an explicitly authorized Kind cluster, digest image, and Docker PostgreSQL"]
async fn real_kind_proxy_image_canary_is_one_replica_and_reversible() {
    if std::env::var("ROCKETMQ_SRE_TEST_PROXY_CANARY").as_deref() != Ok("1") {
        panic!("set ROCKETMQ_SRE_TEST_PROXY_CANARY=1 to authorize the real canary round trip");
    }
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let namespace = std::env::var("ROCKETMQ_SRE_TEST_PROXY_NAMESPACE").unwrap_or_else(|_| "rocketmq-system".to_owned());
    let workload = std::env::var("ROCKETMQ_SRE_TEST_PROXY_WORKLOAD").unwrap_or_else(|_| "rocketmq-proxy".to_owned());
    let container = std::env::var("ROCKETMQ_SRE_TEST_PROXY_CONTAINER").unwrap_or_else(|_| "rocketmq-proxy".to_owned());
    let image_digest =
        std::env::var("ROCKETMQ_SRE_TEST_PROXY_IMAGE_DIGEST").expect("immutable canary image digest must be explicit");
    assert!(valid_digest(&image_digest));
    let kubeconfig = std::env::var("KUBECONFIG").expect("KUBECONFIG must name the authorized test cluster");
    assert!(!kubeconfig.is_empty());

    let schema = format!("phase3_proxy_canary_{}", Uuid::new_v4().simple());
    let pool = isolated_pool(&database_url, &schema).await;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("empty-schema migrations");
    let client =
        ProductionProxyImageCanaryClient::start(BTreeSet::from([format!("{namespace}/{workload}")]), pool.clone())
            .await
            .expect("authorized Kubernetes client");
    let before_main = client
        .deployment(&namespace, &workload)
        .await
        .expect("initial Proxy Deployment");
    assert!(
        client
            .canary(&namespace, &workload)
            .await
            .expect("canary lookup")
            .is_none(),
        "the disposable cluster must not have a pre-existing SRE canary"
    );
    let before_generation = generation(&before_main).expect("main generation");
    let before_replicas = desired_replicas(&before_main).expect("main replicas");
    let before_image = container_image(&before_main, &container).expect("main image");
    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let operation_id = format!("sre-{}", Uuid::new_v4().simple());
    let write = ProxyImageCanaryWrite {
        namespace: namespace.clone(),
        workload: workload.clone(),
        container: container.clone(),
        expected_generation: before_generation,
        image_digest: image_digest.clone(),
        canary_replicas: 1,
        operation_id: operation_id.clone(),
        execution_id,
        plan_step_id,
    };
    client
        .rollout_proxy_image_canary(&write)
        .await
        .expect("create one Proxy canary");
    let canary_state = wait_for_canary(&client, &namespace, &workload, &container, Some(&operation_id))
        .await
        .expect("canary became ready");

    let rollback_operation = format!("sre-{}", Uuid::new_v4().simple());
    client
        .restore_proxy_image(&ProxyImageCanaryRestore {
            namespace: namespace.clone(),
            workload: workload.clone(),
            container: container.clone(),
            operation_id: rollback_operation,
            execution_id,
            plan_step_id,
        })
        .await
        .expect("delete the exact canary UID");
    wait_for_canary(&client, &namespace, &workload, &container, None)
        .await
        .expect("canary removed");
    let after_main = client
        .deployment(&namespace, &workload)
        .await
        .expect("restored main Deployment");

    assert_eq!(canary_state.ready_canary_replicas, 1);
    assert_eq!(canary_state.image_digest, image_digest);
    assert!(canary_state.old_replicas_unchanged);
    assert_eq!(generation(&after_main).expect("main generation"), before_generation);
    assert_eq!(desired_replicas(&after_main).expect("main replicas"), before_replicas);
    assert_eq!(
        container_image(&after_main, &container).expect("main image"),
        before_image
    );
    assert!(client.deployment(&namespace, "not-allowlisted").await.is_err());
    cleanup_schema(&pool, &schema).await;
}

async fn wait_for_canary(
    client: &ProductionProxyImageCanaryClient,
    namespace: &str,
    workload: &str,
    container: &str,
    operation_id: Option<&str>,
) -> Result<ProxyImageCanaryState, ExecutionAgentError> {
    for _ in 0..120 {
        let state = client.proxy_image_canary_state(namespace, workload, container).await?;
        let matches = match operation_id {
            Some(operation_id) => {
                state.ready_canary_replicas == 1
                    && state.old_replicas_unchanged
                    && state.slo_healthy
                    && state.last_operation_id.as_deref() == Some(operation_id)
            }
            None => state.ready_canary_replicas == 0 && state.last_operation_id.is_none(),
        };
        if matches {
            return Ok(state);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(ExecutionAgentError::DriverFailed)
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
