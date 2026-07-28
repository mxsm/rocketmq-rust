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

use k8s_openapi::api::apps::v1::DeploymentSpec;
use k8s_openapi::api::core::v1::Container;
use k8s_openapi::api::core::v1::PodCondition;
use k8s_openapi::api::core::v1::PodSpec;
use k8s_openapi::api::core::v1::PodStatus;
use k8s_openapi::api::core::v1::PodTemplateSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;
use kube::api::ObjectMeta;
use rocketmq_admin_core::core::proxy::ProxyDrainPending;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::TenantId;

use super::*;

#[test]
fn drain_acceptance_requires_open_ingress_and_consistent_zero_state() {
    let mut state = ProxyDrainState {
        schema_version: "rocketmq.proxy-drain.v1".to_owned(),
        phase: ProxyDrainPhase::Accepting,
        operation_id: None,
        admission_open: true,
        routing_open: true,
        readiness_published: true,
        zero_pending: true,
        pending: ProxyDrainPending::default(),
    };
    assert!(accepting(&state));
    state.admission_open = false;
    assert!(!accepting(&state));
}

#[test]
fn pod_address_and_readiness_are_typed_and_fail_closed() {
    let pod = Pod {
        status: Some(PodStatus {
            pod_ip: Some("10.244.0.9".to_owned()),
            conditions: Some(vec![PodCondition {
                status: "True".to_owned(),
                type_: "Ready".to_owned(),
                ..PodCondition::default()
            }]),
            ..PodStatus::default()
        }),
        ..Pod::default()
    };
    assert!(pod_ready(&pod));
    assert_eq!(proxy_addr(&pod, 8080).expect("pod address"), "10.244.0.9:8080");
    let ipv6 = Pod {
        status: Some(PodStatus {
            pod_ip: Some("fd00::9".to_owned()),
            ..PodStatus::default()
        }),
        ..Pod::default()
    };
    assert_eq!(proxy_addr(&ipv6, 8080).expect("IPv6 pod address"), "[fd00::9]:8080");
}

#[test]
fn deployment_selector_supports_exact_kubernetes_operators() {
    let labels = BTreeMap::from([
        ("app".to_owned(), "proxy".to_owned()),
        ("track".to_owned(), "stable".to_owned()),
    ]);
    let selector = LabelSelector {
        match_labels: Some(BTreeMap::from([("app".to_owned(), "proxy".to_owned())])),
        match_expressions: Some(vec![
            LabelSelectorRequirement {
                key: "track".to_owned(),
                operator: "In".to_owned(),
                values: Some(vec!["stable".to_owned()]),
            },
            LabelSelectorRequirement {
                key: "quarantined".to_owned(),
                operator: "DoesNotExist".to_owned(),
                values: None,
            },
        ]),
    };
    assert!(selector_matches(&selector, Some(&labels)));
}

#[test]
fn verification_observation_is_bound_to_scope_and_exact_conditions() {
    let query = ExecutionSliQuery {
        schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        correlation_id: CorrelationId::new(),
        conditions: VERIFICATION_CONDITIONS.iter().map(ToString::to_string).collect(),
    };
    let mut observation = ExecutionSliObservation {
        schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: query.tenant_id,
        cluster_id: query.cluster_id,
        correlation_id: query.correlation_id,
        conditions: query
            .conditions
            .iter()
            .map(|condition| (condition.clone(), true))
            .collect(),
        complete: true,
        evidence_ids: Vec::new(),
        observed_at: Utc::now(),
    };
    assert!(validate_observation(&query, &observation).is_ok());
    observation.cluster_id = ClusterId::new();
    assert!(validate_observation(&query, &observation).is_err());
}

#[tokio::test]
#[ignore = "requires an explicitly authorized Kubernetes test cluster"]
async fn real_kind_restart_replaces_exactly_one_uid_and_preserves_a_healthy_peer() {
    if std::env::var("ROCKETMQ_SRE_TEST_PROXY_RESTART").as_deref() != Ok("1") {
        panic!("set ROCKETMQ_SRE_TEST_PROXY_RESTART=1 to authorize the real restart fixture");
    }
    let namespace = std::env::var("ROCKETMQ_SRE_TEST_PROXY_NAMESPACE").unwrap_or_else(|_| "rocketmq-system".to_owned());
    let kubeconfig = std::env::var("KUBECONFIG").expect("KUBECONFIG must name the authorized test cluster");
    assert!(!kubeconfig.is_empty());
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut config = Config::infer().await.expect("authorized Kubernetes configuration");
    config.proxy_url = None;
    let client = Client::try_from(config).expect("authorized Kubernetes client");
    let fixture_name = format!("sre-proxy-restart-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), &namespace);
    let fixture = fixture_deployment(&fixture_name);
    deployments
        .create(&PostParams::default(), &fixture)
        .await
        .expect("create dedicated restart fixture");

    let result = run_real_restart_fixture(client, &namespace, &fixture_name).await;
    deployments
        .delete(&fixture_name, &DeleteParams::default())
        .await
        .expect("delete dedicated restart fixture");
    result.expect("exact one-Pod restart fixture");
}

async fn run_real_restart_fixture(
    client: Client,
    namespace: &str,
    deployment_name: &str,
) -> Result<(), ExecutionAgentError> {
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let before = wait_for_fixture_pods(&pods, deployment_name, None).await?;
    let original_uids = before
        .iter()
        .map(|pod| pod.metadata.uid.clone().ok_or(ExecutionAgentError::DriverFailed))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    if original_uids.len() != 2 {
        return Err(ExecutionAgentError::DriverFailed);
    }
    let target = before.first().ok_or(ExecutionAgentError::DriverFailed)?;
    let target_name = target.name_any();
    let target_uid = target.metadata.uid.clone().ok_or(ExecutionAgentError::DriverFailed)?;
    let mut deployment = deployments
        .get(deployment_name)
        .await
        .map_err(|_| ExecutionAgentError::DriverFailed)?;
    let annotations = deployment.metadata.annotations.get_or_insert_with(BTreeMap::new);
    annotations.insert(OPERATION_ANNOTATION.to_owned(), "kind-restart-fixture".to_owned());
    annotations.insert(ORIGINAL_POD_ANNOTATION.to_owned(), target_name.clone());
    annotations.insert(ORIGINAL_UID_ANNOTATION.to_owned(), target_uid.clone());
    deployments
        .replace(deployment_name, &PostParams::default(), &deployment)
        .await
        .map_err(|_| ExecutionAgentError::DriverFailed)?;

    pods.delete(
        &target_name,
        &DeleteParams {
            grace_period_seconds: Some(GRACE_PERIOD_SECONDS),
            preconditions: Some(Preconditions {
                uid: Some(target_uid.clone()),
                resource_version: None,
            }),
            ..DeleteParams::default()
        },
    )
    .await
    .map_err(|_| ExecutionAgentError::DriverFailed)?;
    let after = wait_for_fixture_pods(&pods, deployment_name, Some(&target_uid)).await?;
    let after_uids = after
        .iter()
        .map(|pod| pod.metadata.uid.clone().ok_or(ExecutionAgentError::DriverFailed))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    if after_uids.len() != 2 || after_uids.contains(&target_uid) || original_uids.intersection(&after_uids).count() != 1
    {
        return Err(ExecutionAgentError::DriverFailed);
    }
    Ok(())
}

async fn wait_for_fixture_pods(
    pods: &Api<Pod>,
    deployment_name: &str,
    replaced_uid: Option<&str>,
) -> Result<Vec<Pod>, ExecutionAgentError> {
    let selector = format!("rocketmq.apache.org/sre-restart-fixture={deployment_name}");
    for _ in 0..120 {
        let ready = pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?
            .items
            .into_iter()
            .filter(pod_ready)
            .collect::<Vec<_>>();
        let replaced = replaced_uid.is_none_or(|uid| ready.iter().all(|pod| pod.metadata.uid.as_deref() != Some(uid)));
        if ready.len() == 2 && replaced {
            return Ok(ready);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(ExecutionAgentError::DriverFailed)
}

fn fixture_deployment(name: &str) -> Deployment {
    let labels = BTreeMap::from([("rocketmq.apache.org/sre-restart-fixture".to_owned(), name.to_owned())]);
    Deployment {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            ..ObjectMeta::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(2),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..LabelSelector::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "pause".to_owned(),
                        image: Some("registry.k8s.io/pause:3.10".to_owned()),
                        ..Container::default()
                    }],
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        }),
        ..Deployment::default()
    }
}
