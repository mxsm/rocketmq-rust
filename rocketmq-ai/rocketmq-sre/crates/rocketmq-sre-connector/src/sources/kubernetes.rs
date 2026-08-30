// Copyright 2023 The RocketMQ Rust Authors
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

use std::sync::OnceLock;

use chrono::DateTime;
use chrono::Utc;
use reqwest::Client;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_sre_contracts::EvidenceExposure;
use serde_json::Value;
use serde_json::json;

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::common::bounded_future;
use super::common::bounded_response;
use super::common::parse_json;
use super::common::pseudonymize_identifier;
use super::common::require_label;
use super::common::validate_identifier;
use super::deployment_state::project_certificate;
use super::deployment_state::project_deployment;
use super::deployment_state::project_stateful_set;
use super::kubernetes_events::project_event;
use crate::ConnectorError;
use crate::config::KubernetesBearerToken;
use crate::config::KubernetesSourceConfig;

#[derive(Clone, Copy)]
enum KubernetesResource {
    Pods,
    Events,
    Nodes,
    PersistentVolumeClaims,
    PodDisruptionBudgets,
    Deployments,
    StatefulSets,
    Certificates,
    ChangeTimeline,
}

impl KubernetesResource {
    fn parse(resource: &str) -> Result<Self, ConnectorError> {
        match resource {
            "kubernetes/pods" | "pods" => Ok(Self::Pods),
            "kubernetes/events" | "events" => Ok(Self::Events),
            "kubernetes/nodes" | "nodes" => Ok(Self::Nodes),
            "kubernetes/persistent-volume-claims"
            | "kubernetes/persistentvolumeclaims"
            | "persistent-volume-claims"
            | "persistentvolumeclaims"
            | "pvcs" => Ok(Self::PersistentVolumeClaims),
            "kubernetes/pod-disruption-budgets"
            | "kubernetes/poddisruptionbudgets"
            | "pod-disruption-budgets"
            | "poddisruptionbudgets"
            | "pdbs" => Ok(Self::PodDisruptionBudgets),
            "kubernetes/deployments" | "deployments" => Ok(Self::Deployments),
            "kubernetes/statefulsets" | "statefulsets" | "stateful-sets" => Ok(Self::StatefulSets),
            "kubernetes/certificates" | "certificates" | "certs" => Ok(Self::Certificates),
            "kubernetes/change-timeline" | "change-timeline" | "release-events" => Ok(Self::ChangeTimeline),
            _ => Err(ConnectorError::new(
                crate::ConnectorErrorCode::InvalidEvidenceQuery,
                false,
                "Kubernetes source supports only bounded workload, Event, Node, PVC, PDB, Certificate, and change \
                 metadata",
            )),
        }
    }

    const fn wire_kind(self) -> &'static str {
        match self {
            Self::Pods => "pods",
            Self::Events => "events",
            Self::Nodes => "nodes",
            Self::PersistentVolumeClaims => "persistent_volume_claims",
            Self::PodDisruptionBudgets => "pod_disruption_budgets",
            Self::Deployments => "deployments",
            Self::StatefulSets => "stateful_sets",
            Self::Certificates => "certificates",
            Self::ChangeTimeline => "change_timeline",
        }
    }

    fn endpoint(self, namespace: &str) -> String {
        match self {
            Self::Nodes => "api/v1/nodes".to_owned(),
            Self::PodDisruptionBudgets => {
                format!("apis/policy/v1/namespaces/{namespace}/poddisruptionbudgets")
            }
            Self::Deployments => format!("apis/apps/v1/namespaces/{namespace}/deployments"),
            Self::StatefulSets => format!("apis/apps/v1/namespaces/{namespace}/statefulsets"),
            Self::Certificates => {
                format!("apis/cert-manager.io/v1/namespaces/{namespace}/certificates")
            }
            Self::ChangeTimeline => format!("api/v1/namespaces/{namespace}/events"),
            Self::Pods => format!("api/v1/namespaces/{namespace}/pods"),
            Self::Events => format!("api/v1/namespaces/{namespace}/events"),
            Self::PersistentVolumeClaims => {
                format!("api/v1/namespaces/{namespace}/persistentvolumeclaims")
            }
        }
    }

    fn query_scope(self, cluster: &str, namespace: &str) -> (&'static str, String) {
        match self {
            Self::Events | Self::ChangeTimeline => ("fieldSelector", format!("involvedObject.namespace={namespace}")),
            _ => ("labelSelector", format!("rocketmqrust.com/cluster={cluster}")),
        }
    }
}

pub(crate) struct KubernetesSource {
    client: Option<Client>,
    config: Option<KubernetesSourceConfig>,
    pseudonymization_key: Vec<u8>,
    metadata_io: OnceLock<BlockingExecutor>,
}

impl KubernetesSource {
    pub(crate) fn new(
        config: Option<KubernetesSourceConfig>,
        pseudonymization_key: &[u8],
    ) -> Result<Self, ConnectorError> {
        let client = config.as_ref().map(build_client).transpose()?;
        Ok(Self {
            client,
            config,
            pseudonymization_key: pseudonymization_key.to_vec(),
            metadata_io: OnceLock::new(),
        })
    }

    pub(crate) fn initialize(&self, metadata_io: BlockingExecutor) {
        let _ = self.metadata_io.set(metadata_io);
    }

    pub(crate) fn configured(&self) -> bool {
        self.config.is_some()
    }

    pub(crate) async fn query(
        &self,
        cluster: &str,
        resource: &str,
        max_rows: usize,
        max_bytes: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| ConnectorError::source("Kubernetes source is not configured"))?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| ConnectorError::source("Kubernetes source is unavailable"))?;
        require_label(&config.label_allowlist, "rocketmqrust.com/cluster")?;
        validate_identifier(cluster, "cluster")?;
        let resource = KubernetesResource::parse(resource)?;
        let kind = resource.wire_kind();
        let endpoint = config
            .api_url
            .join(&resource.endpoint(&config.namespace))
            .map_err(|_| ConnectorError::configuration("Kubernetes query URL cannot be constructed"))?;
        let (selector_name, selector_value) = resource.query_scope(cluster, &config.namespace);
        let bearer_token = self.bearer_token(config, deadline, cancel).await?;
        let request = client
            .get(endpoint)
            .bearer_auth(bearer_token.expose())
            .query(&[(selector_name, selector_value), ("limit", max_rows.to_string())]);
        let response = bounded_future(deadline, cancel, async {
            request
                .send()
                .await
                .map_err(|_| ConnectorError::source("Kubernetes metadata query failed"))
        })
        .await?;
        if !response.status().is_success() {
            return Err(ConnectorError::source("Kubernetes rejected the bounded metadata query"));
        }
        let body = bounded_response(response, max_bytes, deadline, cancel).await?;
        let raw = parse_json(&body)?;
        let (items, truncated) = project_items(
            &raw,
            resource,
            max_rows,
            &config.label_allowlist,
            &self.pseudonymization_key,
        );
        let mut output = SourceOutput::available(
            json!({
                "schema_version": "rocketmq.kubernetes-evidence.v1",
                "kind": kind,
                "namespace": config.namespace,
                "items": items
            }),
            Utc::now(),
        )
        .with_exposure(EvidenceExposure::KubernetesApi);
        if truncated {
            output.partial = true;
            output.coverage = rocketmq_sre_contracts::CoverageStatus::Partial;
            output.warnings.push("row_limit_applied".to_owned());
        }
        Ok(output)
    }

    async fn bearer_token(
        &self,
        config: &KubernetesSourceConfig,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<crate::config::SecretValue, ConnectorError> {
        match &config.bearer_token {
            KubernetesBearerToken::DevelopmentEnvironment(token) => Ok(token.clone()),
            KubernetesBearerToken::ProjectedFile(projected) => {
                let metadata_io = self.metadata_io.get().cloned().ok_or_else(|| {
                    ConnectorError::source("Kubernetes projected credential reader is not initialized")
                })?;
                let projected = projected.clone();
                bounded_future(deadline, cancel, async move {
                    metadata_io
                        .spawn_io("rocketmq-sre.kubernetes-token-read", move || projected.read())
                        .await
                        .map_err(|_| {
                            ConnectorError::source("Kubernetes projected credential refresh was unavailable")
                        })?
                })
                .await
            }
        }
    }
}

fn build_client(config: &KubernetesSourceConfig) -> Result<Client, ConnectorError> {
    let mut builder = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(config.request_timeout)
        .user_agent(concat!("rocketmq-sre-connector/", env!("CARGO_PKG_VERSION")));
    if !config.ca_pem.is_empty() {
        let certificates = reqwest::Certificate::from_pem_bundle(&config.ca_pem)
            .map_err(|_| ConnectorError::configuration("Kubernetes CA bundle is invalid"))?;
        for certificate in certificates {
            builder = builder.add_root_certificate(certificate);
        }
    }
    builder
        .build()
        .map_err(|_| ConnectorError::configuration("Kubernetes HTTP client cannot be built"))
}

fn project_items(
    raw: &Value,
    resource: KubernetesResource,
    max_rows: usize,
    allowed_labels: &std::collections::BTreeSet<String>,
    pseudonymization_key: &[u8],
) -> (Vec<Value>, bool) {
    let source_items = raw
        .get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let truncated = source_items.len() > max_rows
        || raw
            .get("metadata")
            .and_then(|value| value.get("continue"))
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty());
    let items = source_items
        .iter()
        .take(max_rows)
        .map(|item| match resource {
            KubernetesResource::Pods => project_pod(item, allowed_labels, pseudonymization_key),
            KubernetesResource::Events => project_event(item),
            KubernetesResource::Nodes => project_node(item, allowed_labels, pseudonymization_key),
            KubernetesResource::PersistentVolumeClaims => project_pvc(item, allowed_labels),
            KubernetesResource::PodDisruptionBudgets => project_pdb(item, allowed_labels),
            KubernetesResource::Deployments => project_deployment(item, allowed_labels),
            KubernetesResource::StatefulSets => project_stateful_set(item, allowed_labels),
            KubernetesResource::Certificates => project_certificate(item, allowed_labels),
            KubernetesResource::ChangeTimeline => super::change_timeline::project_change(item).unwrap_or(Value::Null),
        })
        .filter(|item| !item.is_null())
        .collect();
    (items, truncated)
}

fn project_pod(
    item: &Value,
    allowed_labels: &std::collections::BTreeSet<String>,
    pseudonymization_key: &[u8],
) -> Value {
    let labels = filtered_labels(item.pointer("/metadata/labels"), allowed_labels);
    let containers = item
        .pointer("/status/containerStatuses")
        .and_then(Value::as_array)
        .map(|containers| {
            containers
                .iter()
                .map(|container| {
                    json!({
                        "name": container.get("name"),
                        "ready": container.get("ready"),
                        "restart_count": container.get("restartCount"),
                        "state": container.get("state").and_then(container_state_reason)
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let persistent_volume_claims = item
        .pointer("/spec/volumes")
        .and_then(Value::as_array)
        .map(|volumes| {
            volumes
                .iter()
                .filter_map(|volume| {
                    volume
                        .pointer("/persistentVolumeClaim/claimName")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({
        "name": item.pointer("/metadata/name"),
        "namespace": item.pointer("/metadata/namespace"),
        "labels": labels,
        "node_name": item
            .pointer("/spec/nodeName")
            .and_then(Value::as_str)
            .map(|name| pseudonymize_identifier(name, pseudonymization_key)),
        "persistent_volume_claims": persistent_volume_claims,
        "phase": item.pointer("/status/phase"),
        "reason": item.pointer("/status/reason"),
        "containers": containers
    })
}

fn project_node(
    item: &Value,
    allowed_labels: &std::collections::BTreeSet<String>,
    pseudonymization_key: &[u8],
) -> Value {
    let conditions = item
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .map(|conditions| {
            conditions
                .iter()
                .map(|condition| {
                    json!({
                        "type": condition.get("type"),
                        "status": condition.get("status"),
                        "reason": condition.get("reason"),
                        "last_transition_time": condition.get("lastTransitionTime")
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({
        "name": item
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .map(|name| pseudonymize_identifier(name, pseudonymization_key)),
        "labels": filtered_labels(item.pointer("/metadata/labels"), allowed_labels),
        "unschedulable": item.pointer("/spec/unschedulable"),
        "pod_capacity": item.pointer("/status/capacity/pods"),
        "pod_allocatable": item.pointer("/status/allocatable/pods"),
        "conditions": conditions
    })
}

fn project_pvc(item: &Value, allowed_labels: &std::collections::BTreeSet<String>) -> Value {
    json!({
        "name": item.pointer("/metadata/name"),
        "namespace": item.pointer("/metadata/namespace"),
        "labels": filtered_labels(item.pointer("/metadata/labels"), allowed_labels),
        "phase": item.pointer("/status/phase"),
        "storage_class": item.pointer("/spec/storageClassName"),
        "access_modes": item.pointer("/spec/accessModes"),
        "requested_storage": item.pointer("/spec/resources/requests/storage"),
        "capacity_storage": item.pointer("/status/capacity/storage")
    })
}

fn project_pdb(item: &Value, allowed_labels: &std::collections::BTreeSet<String>) -> Value {
    let match_labels = filtered_labels(item.pointer("/spec/selector/matchLabels"), allowed_labels);
    let has_match_expressions = item
        .pointer("/spec/selector/matchExpressions")
        .and_then(Value::as_array)
        .is_some_and(|expressions| !expressions.is_empty());
    json!({
        "name": item.pointer("/metadata/name"),
        "namespace": item.pointer("/metadata/namespace"),
        "labels": filtered_labels(item.pointer("/metadata/labels"), allowed_labels),
        "selector_match_labels": match_labels,
        "selector_has_match_expressions": has_match_expressions,
        "min_available": item.pointer("/spec/minAvailable"),
        "max_unavailable": item.pointer("/spec/maxUnavailable"),
        "current_healthy": item.pointer("/status/currentHealthy"),
        "desired_healthy": item.pointer("/status/desiredHealthy"),
        "expected_pods": item.pointer("/status/expectedPods"),
        "disruptions_allowed": item.pointer("/status/disruptionsAllowed")
    })
}

pub(super) fn filtered_labels(
    labels: Option<&Value>,
    allowed_labels: &std::collections::BTreeSet<String>,
) -> serde_json::Map<String, Value> {
    labels
        .and_then(Value::as_object)
        .map(|labels| {
            labels
                .iter()
                .filter(|(key, _)| allowed_labels.contains(*key))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn container_state_reason(state: &Value) -> Option<Value> {
    for kind in ["waiting", "terminated", "running"] {
        if let Some(value) = state.get(kind) {
            return Some(json!({
                "kind": kind,
                "reason": value.get("reason"),
                "exit_code": value.get("exitCode"),
                "started_at": value.get("startedAt"),
                "finished_at": value.get("finishedAt")
            }));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use axum::Json;
    use axum::Router;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::routing::get;
    use rocketmq_runtime::RuntimeContext;
    use tokio::sync::Mutex;
    use url::Url;

    use super::*;
    use crate::config::KubernetesBearerToken;
    use crate::config::ProjectedTokenFile;

    #[test]
    fn event_queries_use_the_configured_namespace_as_the_read_boundary() {
        assert_eq!(
            KubernetesResource::Events.query_scope("cluster-a", "rocketmq"),
            ("fieldSelector", "involvedObject.namespace=rocketmq".to_owned())
        );
        assert_eq!(
            KubernetesResource::Deployments.query_scope("cluster-a", "rocketmq"),
            ("labelSelector", "rocketmqrust.com/cluster=cluster-a".to_owned())
        );
    }

    #[tokio::test]
    async fn projected_token_rotation_is_used_by_the_next_request_without_restart() {
        type ObservedAuthorizations = Arc<Mutex<Vec<String>>>;

        async fn kubernetes_fixture(State(observed): State<ObservedAuthorizations>, headers: HeaderMap) -> Json<Value> {
            let authorization = headers
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("<missing>")
                .to_owned();
            observed.lock().await.push(authorization);
            Json(json!({"items": []}))
        }

        let observed = ObservedAuthorizations::default();
        let app = Router::new()
            .fallback(get(kubernetes_fixture))
            .with_state(observed.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Kubernetes fixture");
        let address = listener.local_addr().expect("fixture address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve Kubernetes fixture");
        });

        let unique_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        let mount = std::env::temp_dir().join(format!(
            "rocketmq-sre-kubernetes-token-{}-{}",
            std::process::id(),
            unique_suffix
        ));
        std::fs::create_dir(&mount).expect("create projected mount");
        let token_path = mount.join("token");
        std::fs::write(&token_path, "first-token\n").expect("write initial token");

        let config = KubernetesSourceConfig {
            api_url: Url::parse(&format!("http://{address}/")).expect("fixture URL"),
            namespace: "rocketmq-system".to_owned(),
            bearer_token: KubernetesBearerToken::ProjectedFile(
                ProjectedTokenFile::try_new(token_path.clone()).expect("projected token"),
            ),
            ca_pem: Vec::new(),
            request_timeout: Duration::from_secs(5),
            label_allowlist: BTreeSet::from(["rocketmqrust.com/cluster".to_owned()]),
        };
        let source = KubernetesSource::new(Some(config), b"test-scope").expect("Kubernetes source");
        let runtime = RuntimeContext::from_current("kubernetes-token-rotation-test");
        source.initialize(runtime.service_context("source").metadata_io().clone());
        let cancel = CancelSignal::default();

        source
            .query(
                "local",
                "pods",
                10,
                64 * 1024,
                Utc::now() + chrono::Duration::seconds(5),
                &cancel,
            )
            .await
            .expect("initial Kubernetes request");
        std::fs::write(&token_path, "rotated-token\n").expect("rotate projected token");
        source
            .query(
                "local",
                "pods",
                10,
                64 * 1024,
                Utc::now() + chrono::Duration::seconds(5),
                &cancel,
            )
            .await
            .expect("Kubernetes request after rotation");

        assert_eq!(
            *observed.lock().await,
            vec!["Bearer first-token", "Bearer rotated-token"]
        );

        server.abort();
        let _ = server.await;
        let report = runtime.shutdown_tasks(Duration::from_secs(5)).await;
        assert!(report.is_healthy(), "runtime shutdown: {report:?}");
        std::fs::remove_dir_all(&mount).expect("remove projected mount");
    }

    #[test]
    fn pod_projection_excludes_spec_env_and_addresses() {
        let raw = json!({
            "items": [{
                "metadata": {
                    "name": "broker-0",
                    "namespace": "rocketmq",
                    "labels": {
                        "rocketmqrust.com/cluster": "local",
                        "secret-label": "drop"
                    }
                },
                "spec": {"containers": [{"env": [{"name": "PASSWORD", "value": "no"}]}]},
                "status": {"phase": "Running", "podIP": "10.0.0.2"}
            }]
        });
        let (items, truncated) = project_items(
            &raw,
            KubernetesResource::Pods,
            10,
            &BTreeSet::from(["rocketmqrust.com/cluster".to_owned()]),
            b"test-scope",
        );
        assert!(!truncated);
        let encoded = serde_json::to_string(&items).expect("projection");
        assert!(!encoded.contains("PASSWORD"));
        assert!(!encoded.contains("10.0.0.2"));
        assert!(!encoded.contains("secret-label"));
    }

    #[test]
    fn storage_and_node_projection_excludes_addresses_and_secret_volumes() {
        let raw = json!({
            "items": [{
                "metadata": {
                    "name": "broker-0",
                    "namespace": "rocketmq",
                    "labels": {
                        "rocketmqrust.com/cluster": "local",
                        "rocketmqrust.com/service": "broker"
                    }
                },
                "spec": {
                    "nodeName": "worker-a",
                    "volumes": [
                        {"name": "data", "persistentVolumeClaim": {"claimName": "broker-data-0"}},
                        {"name": "secret", "secret": {"secretName": "broker-credentials"}}
                    ],
                    "containers": [{"envFrom": [{"secretRef": {"name": "broker-credentials"}}]}]
                },
                "status": {"phase": "Running", "podIP": "10.0.0.2", "hostIP": "10.0.0.1"}
            }]
        });
        let (items, truncated) = project_items(
            &raw,
            KubernetesResource::Pods,
            10,
            &BTreeSet::from([
                "rocketmqrust.com/cluster".to_owned(),
                "rocketmqrust.com/service".to_owned(),
            ]),
            b"test-scope",
        );
        assert!(!truncated);
        assert!(
            items[0]["node_name"]
                .as_str()
                .is_some_and(|value| value.starts_with("sha256:"))
        );
        assert_eq!(items[0]["persistent_volume_claims"], json!(["broker-data-0"]));
        let encoded = serde_json::to_string(&items).expect("projection");
        assert!(!encoded.contains("10.0.0"));
        assert!(!encoded.contains("broker-credentials"));
        assert!(!encoded.contains("secretName"));
    }

    #[test]
    fn pdb_projection_keeps_only_allowlisted_selector_labels() {
        let raw = json!({
            "items": [{
                "metadata": {
                    "name": "rocketmq-broker",
                    "namespace": "rocketmq",
                    "labels": {"rocketmqrust.com/service": "broker"}
                },
                "spec": {
                    "minAvailable": 2,
                    "selector": {
                        "matchLabels": {
                            "rocketmqrust.com/service": "broker",
                            "private.example/tenant": "drop"
                        }
                    }
                },
                "status": {"currentHealthy": 3, "desiredHealthy": 2}
            }]
        });
        let (items, truncated) = project_items(
            &raw,
            KubernetesResource::PodDisruptionBudgets,
            10,
            &BTreeSet::from(["rocketmqrust.com/service".to_owned()]),
            b"test-scope",
        );
        assert!(!truncated);
        assert_eq!(items[0]["selector_match_labels"]["rocketmqrust.com/service"], "broker");
        assert!(
            items[0]["selector_match_labels"]
                .get("private.example/tenant")
                .is_none()
        );
    }
}
