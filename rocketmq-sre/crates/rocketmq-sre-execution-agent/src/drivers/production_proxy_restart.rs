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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::SecondsFormat;
use chrono::Utc;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::apps::v1::ReplicaSet;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::Api;
use kube::Client;
use kube::Config;
use kube::ResourceExt;
use kube::api::DeleteParams;
use kube::api::ListParams;
use kube::api::PostParams;
use kube::api::Preconditions;
use reqwest::StatusCode;
use rocketmq_admin_core::core::proxy::ProxyDrainOperationRequest;
use rocketmq_admin_core::core::proxy::ProxyDrainPhase;
use rocketmq_admin_core::core::proxy::ProxyDrainState;
use rocketmq_admin_core::core::proxy::ProxyMutationAdmin;
use rocketmq_admin_core::core::proxy::ProxyQueryAdmin;
use rocketmq_admin_core::core::proxy::QueryProxyDrainStateRequest;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminBuilder;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminSession;
use rocketmq_admin_core::read_client_adapter::ClientRuntime;
use rocketmq_admin_core::read_client_adapter::ClientRuntimeConfig;
use rocketmq_admin_core::read_client_adapter::ReadAdminBuilder;
use rocketmq_admin_core::read_client_adapter::ReadAdminSession;
use rocketmq_admin_core::read_client_adapter::TelemetryHandle;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EXECUTION_VERIFICATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionSliObservation;
use rocketmq_sre_contracts::ExecutionSliQuery;
use tokio::sync::Mutex;

use super::DriverFuture;
use super::ProxyRestartClient;
use super::ProxyRestartOneWrite;
use super::ProxyRestartRestore;
use super::ProxyRestartRestoreOutcome;
use super::ProxyRestartState;
use crate::ExecutionAgentError;
use crate::config::BrokerAdminDriverConfig;
use crate::config::ProxyRestartDriverConfig;

const OPERATION_ANNOTATION: &str = "rocketmq.apache.org/sre-restart-operation";
const LAST_OPERATION_ANNOTATION: &str = "rocketmq.apache.org/sre-restart-last-operation";
const EXECUTION_ANNOTATION: &str = "rocketmq.apache.org/sre-restart-execution";
const PLAN_STEP_ANNOTATION: &str = "rocketmq.apache.org/sre-restart-plan-step";
const ORIGINAL_POD_ANNOTATION: &str = "rocketmq.apache.org/sre-restart-original-pod";
const ORIGINAL_UID_ANNOTATION: &str = "rocketmq.apache.org/sre-restart-original-uid";
const STARTED_AT_ANNOTATION: &str = "rocketmq.apache.org/sre-restart-started-at";
const MAX_VERIFICATION_RESPONSE_BYTES: usize = 128 * 1024;
const REPLACEMENT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const GRACE_PERIOD_SECONDS: u32 = 30;
const VERIFICATION_CONDITIONS: [&str; 3] = ["synthetic_message_path", "proxy_error_ratio", "proxy_p99_latency"];

#[derive(Clone)]
struct VerificationClient {
    client: reqwest::Client,
    base_url: url::Url,
    bearer_token: Arc<str>,
    tenant_id: rocketmq_sre_contracts::TenantId,
    cluster_id: rocketmq_sre_contracts::ClusterId,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct VerificationHealth {
    synthetic_path_healthy: bool,
    slo_healthy: bool,
}

#[derive(Clone)]
struct AllowedTarget {
    namespace: String,
    deployment: String,
    remoting_port: u16,
}

struct ResolvedTarget {
    allowed: AllowedTarget,
    deployment: Deployment,
    pod: Option<Pod>,
}

/// Production client that combines authenticated Proxy drain management with
/// an exact, UID-preconditioned Kubernetes Pod restart.
pub(crate) struct ProductionProxyRestartClient {
    kube: Client,
    targets: Arc<BTreeMap<String, u16>>,
    read_admin: Mutex<ReadAdminSession>,
    mutation_admin: Mutex<MutationAdminSession>,
    verification: VerificationClient,
    _client_runtime: Arc<ClientRuntime>,
}

impl ProductionProxyRestartClient {
    pub(crate) async fn start(
        admin_config: &BrokerAdminDriverConfig,
        restart_config: &ProxyRestartDriverConfig,
        request_timeout: Duration,
        dev_insecure_http: bool,
        context: ChildServiceContext,
    ) -> Result<Self, ExecutionAgentError> {
        if restart_config.targets.is_empty() {
            return Err(ExecutionAgentError::Configuration);
        }
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut kube_config = Config::infer().await.map_err(|_| ExecutionAgentError::Configuration)?;
        kube_config.proxy_url = None;
        let kube = Client::try_from(kube_config).map_err(|_| ExecutionAgentError::Configuration)?;

        let client_runtime = ClientRuntime::try_new(
            context.component("proxy-restart-admin-client"),
            ClientRuntimeConfig {
                shutdown_timeout: admin_config.shutdown_timeout,
                ..ClientRuntimeConfig::default()
            },
            TelemetryHandle::noop(),
        )
        .map_err(|_| ExecutionAgentError::Configuration)?;
        let timeout_millis = duration_millis(admin_config.request_timeout)?;
        let mut read_builder = ReadAdminBuilder::new(Arc::clone(&client_runtime))
            .namesrv_addr(admin_config.namesrv_addr.clone())
            .admin_group("rocketmq-sre-agent-proxy-restart-read")
            .instance_name("rocketmq-sre-proxy-restart-read")
            .timeout_millis(timeout_millis)
            .use_tls(admin_config.use_tls);
        if let Some(credentials) = &admin_config.read_credentials {
            read_builder = read_builder.credentials(credentials.clone());
        }
        let mut read_admin = read_builder
            .build_and_start()
            .await
            .map_err(|_| ExecutionAgentError::Configuration)?;

        let mut mutation_builder = MutationAdminBuilder::new(Arc::clone(&client_runtime))
            .namesrv_addr(admin_config.namesrv_addr.clone())
            .admin_group("rocketmq-sre-agent-proxy-restart-mutation")
            .instance_name("rocketmq-sre-proxy-restart-mutation")
            .timeout_millis(timeout_millis)
            .use_tls(admin_config.use_tls);
        if let Some(credentials) = &admin_config.mutation_credentials {
            mutation_builder = mutation_builder.credentials(credentials.clone());
        }
        let mutation_admin = match mutation_builder.build_and_start().await {
            Ok(session) => session,
            Err(_) => {
                read_admin.shutdown().await;
                return Err(ExecutionAgentError::Configuration);
            }
        };

        let verification_client = reqwest::Client::builder()
            .https_only(!dev_insecure_http)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(request_timeout)
            .build()
            .map_err(|_| ExecutionAgentError::Configuration)?;
        Ok(Self {
            kube,
            targets: Arc::new(restart_config.targets.clone()),
            read_admin: Mutex::new(read_admin),
            mutation_admin: Mutex::new(mutation_admin),
            verification: VerificationClient {
                client: verification_client,
                base_url: restart_config.verification_base_url.clone(),
                bearer_token: Arc::from(restart_config.verification_token.as_str()),
                tenant_id: restart_config.tenant_id,
                cluster_id: restart_config.cluster_id,
            },
            _client_runtime: client_runtime,
        })
    }

    pub(crate) async fn shutdown(&self) {
        self.read_admin.lock().await.shutdown().await;
        self.mutation_admin.lock().await.shutdown().await;
    }

    async fn resolve_target(
        &self,
        namespace: &str,
        requested_pod: &str,
    ) -> Result<ResolvedTarget, ExecutionAgentError> {
        let pods: Api<Pod> = Api::namespaced(self.kube.clone(), namespace);
        if let Some(pod) = pods
            .get_opt(requested_pod)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?
        {
            let deployment_name = self.deployment_for_pod(namespace, &pod).await?;
            let allowed = self.allowed_target(namespace, &deployment_name)?;
            let deployment = self.deployment(namespace, &deployment_name).await?;
            return Ok(ResolvedTarget {
                allowed,
                deployment,
                pod: Some(pod),
            });
        }

        let mut matches = Vec::new();
        for key in self
            .targets
            .keys()
            .filter(|key| key.starts_with(&format!("{namespace}/")))
        {
            let deployment_name = key
                .split_once('/')
                .map(|(_, deployment)| deployment)
                .ok_or(ExecutionAgentError::Configuration)?;
            let deployment = self.deployment(namespace, deployment_name).await?;
            if annotation(&deployment, ORIGINAL_POD_ANNOTATION) == Some(requested_pod) {
                matches.push((self.allowed_target(namespace, deployment_name)?, deployment));
            }
        }
        if matches.len() != 1 {
            return Err(ExecutionAgentError::DriverFailed);
        }
        let (allowed, deployment) = matches.pop().ok_or(ExecutionAgentError::DriverFailed)?;
        let pod = self.replacement_pod(&deployment).await?;
        Ok(ResolvedTarget {
            allowed,
            deployment,
            pod,
        })
    }

    async fn deployment_for_pod(&self, namespace: &str, pod: &Pod) -> Result<String, ExecutionAgentError> {
        let replica_set_name = controller_owner_name(pod).ok_or(ExecutionAgentError::DriverFailed)?;
        let replica_sets: Api<ReplicaSet> = Api::namespaced(self.kube.clone(), namespace);
        let replica_set = replica_sets
            .get(replica_set_name)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        controller_owner_name(&replica_set)
            .map(str::to_owned)
            .ok_or(ExecutionAgentError::DriverFailed)
    }

    fn allowed_target(&self, namespace: &str, deployment: &str) -> Result<AllowedTarget, ExecutionAgentError> {
        let key = format!("{namespace}/{deployment}");
        let remoting_port = self
            .targets
            .get(&key)
            .copied()
            .ok_or(ExecutionAgentError::DriverFailed)?;
        Ok(AllowedTarget {
            namespace: namespace.to_owned(),
            deployment: deployment.to_owned(),
            remoting_port,
        })
    }

    async fn deployment(&self, namespace: &str, deployment: &str) -> Result<Deployment, ExecutionAgentError> {
        Api::<Deployment>::namespaced(self.kube.clone(), namespace)
            .get(deployment)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn replacement_pod(&self, deployment: &Deployment) -> Result<Option<Pod>, ExecutionAgentError> {
        let namespace = deployment.namespace().ok_or(ExecutionAgentError::DriverFailed)?;
        let selector = deployment
            .spec
            .as_ref()
            .map(|spec| &spec.selector)
            .ok_or(ExecutionAgentError::DriverFailed)?;
        let original_uid = annotation(deployment, ORIGINAL_UID_ANNOTATION);
        let started_at = annotation(deployment, STARTED_AT_ANNOTATION);
        let pods = Api::<Pod>::namespaced(self.kube.clone(), &namespace)
            .list(&ListParams::default())
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        let mut candidates = pods
            .items
            .into_iter()
            .filter(|pod| selector_matches(selector, pod.metadata.labels.as_ref()))
            .filter(|pod| pod.metadata.uid.as_deref() != original_uid)
            .filter(|pod| {
                started_at.is_some_and(|started_at| {
                    pod.metadata
                        .creation_timestamp
                        .as_ref()
                        .is_some_and(|created| replacement_started_at_or_after(&created.0, started_at))
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|pod| pod.metadata.creation_timestamp.clone());
        if candidates.len() > 1 {
            return Err(ExecutionAgentError::DriverFailed);
        }
        Ok(candidates.pop())
    }

    async fn remaining_replicas_healthy(
        &self,
        deployment: &Deployment,
        excluded_uid: &str,
    ) -> Result<bool, ExecutionAgentError> {
        let namespace = deployment.namespace().ok_or(ExecutionAgentError::DriverFailed)?;
        let selector = deployment
            .spec
            .as_ref()
            .map(|spec| &spec.selector)
            .ok_or(ExecutionAgentError::DriverFailed)?;
        let pods = Api::<Pod>::namespaced(self.kube.clone(), &namespace)
            .list(&ListParams::default())
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        Ok(pods.items.iter().any(|pod| {
            selector_matches(selector, pod.metadata.labels.as_ref())
                && pod.metadata.uid.as_deref() != Some(excluded_uid)
                && pod_ready(pod)
        }))
    }

    async fn query_drain(&self, proxy_addr: &str) -> Result<ProxyDrainState, ExecutionAgentError> {
        self.read_admin
            .lock()
            .await
            .query_drain_state(&QueryProxyDrainStateRequest {
                proxy_addr: proxy_addr.to_owned(),
            })
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn begin_drain(&self, proxy_addr: &str, operation_id: &str) -> Result<ProxyDrainState, ExecutionAgentError> {
        self.mutation_admin
            .lock()
            .await
            .begin_drain(&ProxyDrainOperationRequest {
                proxy_addr: proxy_addr.to_owned(),
                operation_id: operation_id.to_owned(),
            })
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn cancel_drain(&self, proxy_addr: &str, operation_id: &str) -> Result<(), ExecutionAgentError> {
        let state = self
            .mutation_admin
            .lock()
            .await
            .cancel_drain(&ProxyDrainOperationRequest {
                proxy_addr: proxy_addr.to_owned(),
                operation_id: operation_id.to_owned(),
            })
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        if accepting(&state) {
            Ok(())
        } else {
            Err(ExecutionAgentError::DriverFailed)
        }
    }

    async fn wait_for_zero_drain(
        &self,
        proxy_addr: &str,
        operation_id: &str,
        deadline: tokio::time::Instant,
    ) -> Result<(), ExecutionAgentError> {
        loop {
            let state = self.query_drain(proxy_addr).await?;
            if state.operation_id.as_deref() != Some(operation_id)
                || state.admission_open
                || state.routing_open
                || state.readiness_published
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            if state.phase == ProxyDrainPhase::Drained && state.zero_pending && state.pending.is_zero() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ExecutionAgentError::DriverFailed);
            }
            tokio::time::sleep(REPLACEMENT_POLL_INTERVAL).await;
        }
    }

    async fn mark_restart(
        &self,
        target: &AllowedTarget,
        request: &ProxyRestartOneWrite,
    ) -> Result<Deployment, ExecutionAgentError> {
        self.replace_annotations(target, Some(request)).await
    }

    async fn clear_restart(
        &self,
        target: &AllowedTarget,
        operation_id: &str,
    ) -> Result<Deployment, ExecutionAgentError> {
        let deployments: Api<Deployment> = Api::namespaced(self.kube.clone(), &target.namespace);
        let mut deployment = deployments
            .get(&target.deployment)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        if annotation(&deployment, OPERATION_ANNOTATION) != Some(operation_id) {
            return Err(ExecutionAgentError::DriverFailed);
        }
        if let Some(annotations) = deployment.metadata.annotations.as_mut() {
            for key in [
                OPERATION_ANNOTATION,
                EXECUTION_ANNOTATION,
                PLAN_STEP_ANNOTATION,
                ORIGINAL_POD_ANNOTATION,
                ORIGINAL_UID_ANNOTATION,
                STARTED_AT_ANNOTATION,
            ] {
                annotations.remove(key);
            }
        }
        deployments
            .replace(&target.deployment, &PostParams::default(), &deployment)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn complete_restart(
        &self,
        target: &AllowedTarget,
        operation_id: &str,
    ) -> Result<Deployment, ExecutionAgentError> {
        let deployments: Api<Deployment> = Api::namespaced(self.kube.clone(), &target.namespace);
        let mut deployment = deployments
            .get(&target.deployment)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        if annotation(&deployment, OPERATION_ANNOTATION) != Some(operation_id) {
            return Err(ExecutionAgentError::DriverFailed);
        }
        let annotations = deployment.metadata.annotations.get_or_insert_with(BTreeMap::new);
        annotations.insert(LAST_OPERATION_ANNOTATION.to_owned(), operation_id.to_owned());
        annotations.remove(OPERATION_ANNOTATION);
        deployments
            .replace(&target.deployment, &PostParams::default(), &deployment)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn replace_annotations(
        &self,
        target: &AllowedTarget,
        request: Option<&ProxyRestartOneWrite>,
    ) -> Result<Deployment, ExecutionAgentError> {
        let request = request.ok_or(ExecutionAgentError::InvalidRequest)?;
        let deployments: Api<Deployment> = Api::namespaced(self.kube.clone(), &target.namespace);
        let mut deployment = deployments
            .get(&target.deployment)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        if annotation(&deployment, OPERATION_ANNOTATION).is_some() {
            return Err(ExecutionAgentError::DriverFailed);
        }
        let annotations = deployment.metadata.annotations.get_or_insert_with(BTreeMap::new);
        annotations.insert(OPERATION_ANNOTATION.to_owned(), request.operation_id.clone());
        annotations.insert(EXECUTION_ANNOTATION.to_owned(), request.execution_id.to_string());
        annotations.insert(PLAN_STEP_ANNOTATION.to_owned(), request.plan_step_id.to_string());
        annotations.insert(ORIGINAL_POD_ANNOTATION.to_owned(), request.pod.clone());
        annotations.insert(ORIGINAL_UID_ANNOTATION.to_owned(), request.expected_uid.clone());
        annotations.insert(
            STARTED_AT_ANNOTATION.to_owned(),
            Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        );
        deployments
            .replace(&target.deployment, &PostParams::default(), &deployment)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn delete_expected_pod(
        &self,
        namespace: &str,
        pod: &str,
        expected_uid: &str,
    ) -> Result<(), ExecutionAgentError> {
        let params = DeleteParams {
            grace_period_seconds: Some(GRACE_PERIOD_SECONDS),
            preconditions: Some(Preconditions {
                uid: Some(expected_uid.to_owned()),
                resource_version: None,
            }),
            ..DeleteParams::default()
        };
        Api::<Pod>::namespaced(self.kube.clone(), namespace)
            .delete(pod, &params)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        Ok(())
    }

    async fn wait_for_replacement(
        &self,
        namespace: &str,
        requested_pod: &str,
        expected_uid: &str,
        deadline: tokio::time::Instant,
    ) -> Result<Pod, ExecutionAgentError> {
        loop {
            let resolved = self.resolve_target(namespace, requested_pod).await?;
            if let Some(pod) = resolved.pod
                && pod.metadata.uid.as_deref() != Some(expected_uid)
                && pod_ready(&pod)
                && proxy_addr(&pod, resolved.allowed.remoting_port).is_ok()
            {
                return Ok(pod);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ExecutionAgentError::DriverFailed);
            }
            tokio::time::sleep(REPLACEMENT_POLL_INTERVAL).await;
        }
    }
}

impl VerificationClient {
    async fn observe(&self) -> Result<VerificationHealth, ExecutionAgentError> {
        let query = ExecutionSliQuery {
            schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
            tenant_id: self.tenant_id,
            cluster_id: self.cluster_id,
            correlation_id: CorrelationId::new(),
            conditions: VERIFICATION_CONDITIONS.iter().map(ToString::to_string).collect(),
        };
        let url = self
            .base_url
            .join("/internal/v1/execution-verification/sli")
            .map_err(|_| ExecutionAgentError::Configuration)?;
        let response = self
            .client
            .post(url)
            .bearer_auth(self.bearer_token.as_ref())
            .header("x-forwarded-client-cert", "URI=spiffe://rocketmq-sre/executor")
            .header("x-rocketmq-tenant", self.tenant_id.to_string())
            .header("x-rocketmq-clusters", self.cluster_id.to_string())
            .header("x-rocketmq-subject", "rocketmq-sre-execution-agent")
            .json(&query)
            .send()
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        let observation = decode_observation(response).await?;
        validate_observation(&query, &observation)?;
        Ok(VerificationHealth {
            synthetic_path_healthy: observation.complete
                && observation.conditions.get("synthetic_message_path") == Some(&true),
            slo_healthy: observation.complete
                && observation.conditions.get("proxy_error_ratio") == Some(&true)
                && observation.conditions.get("proxy_p99_latency") == Some(&true),
        })
    }
}

impl ProxyRestartClient for ProductionProxyRestartClient {
    fn proxy_restart_state<'a>(&'a self, namespace: &'a str, pod: &'a str) -> DriverFuture<'a, ProxyRestartState> {
        Box::pin(async move {
            let resolved = self.resolve_target(namespace, pod).await?;
            let active_operation_id = annotation(&resolved.deployment, OPERATION_ANNOTATION).map(str::to_owned);
            let last_operation_id = annotation(&resolved.deployment, LAST_OPERATION_ANNOTATION).map(str::to_owned);
            let original_uid = annotation(&resolved.deployment, ORIGINAL_UID_ANNOTATION);
            let current_pod = resolved.pod.ok_or(ExecutionAgentError::DriverFailed)?;
            let pod_uid = current_pod
                .metadata
                .uid
                .clone()
                .ok_or(ExecutionAgentError::DriverFailed)?;
            let pod_is_ready = pod_ready(&current_pod);
            let remaining_replicas_healthy = self.remaining_replicas_healthy(&resolved.deployment, &pod_uid).await?;
            let replacement_ready = (active_operation_id.is_some() || last_operation_id.is_some())
                && original_uid.is_some_and(|uid| uid != pod_uid)
                && pod_is_ready;
            let addr = proxy_addr(&current_pod, resolved.allowed.remoting_port)?;
            let drain = self.query_drain(&addr).await.ok();
            let verification = if replacement_ready {
                self.verification.observe().await.unwrap_or_default()
            } else {
                VerificationHealth::default()
            };
            Ok(ProxyRestartState {
                drain_supported: drain.is_some(),
                pod_uid,
                pod_ready: pod_is_ready,
                remaining_replicas_healthy,
                replacement_ready,
                synthetic_path_healthy: verification.synthetic_path_healthy,
                slo_healthy: verification.slo_healthy,
                active_operation_id,
                last_operation_id,
                drain,
            })
        })
    }

    fn restart_one_drained<'a>(&'a self, request: &'a ProxyRestartOneWrite) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let resolved = self.resolve_target(&request.namespace, &request.pod).await?;
            let pod = resolved.pod.ok_or(ExecutionAgentError::DriverFailed)?;
            if pod.metadata.uid.as_deref() != Some(request.expected_uid.as_str())
                || !pod_ready(&pod)
                || !self
                    .remaining_replicas_healthy(&resolved.deployment, &request.expected_uid)
                    .await?
                || annotation(&resolved.deployment, OPERATION_ANNOTATION).is_some()
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let original_proxy_addr = proxy_addr(&pod, resolved.allowed.remoting_port)?;
            let before = self.query_drain(&original_proxy_addr).await?;
            if !accepting(&before) {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let begun = match self.begin_drain(&original_proxy_addr, &request.operation_id).await {
                Ok(state) => state,
                Err(error) => {
                    let _ = self.cancel_drain(&original_proxy_addr, &request.operation_id).await;
                    return Err(error);
                }
            };
            if begun.operation_id.as_deref() != Some(request.operation_id.as_str())
                || begun.admission_open
                || begun.routing_open
                || begun.readiness_published
            {
                let _ = self.cancel_drain(&original_proxy_addr, &request.operation_id).await;
                return Err(ExecutionAgentError::DriverFailed);
            }

            let deadline = tokio::time::Instant::now() + Duration::from_secs(u64::from(request.drain_timeout_seconds));
            if self
                .wait_for_zero_drain(&original_proxy_addr, &request.operation_id, deadline)
                .await
                .is_err()
            {
                self.cancel_drain(&original_proxy_addr, &request.operation_id).await?;
                return Err(ExecutionAgentError::DriverFailed);
            }
            if self.mark_restart(&resolved.allowed, request).await.is_err() {
                self.cancel_drain(&original_proxy_addr, &request.operation_id).await?;
                return Err(ExecutionAgentError::DriverFailed);
            }
            let final_drain = self.query_drain(&original_proxy_addr).await;
            let remaining_replicas_healthy = self
                .remaining_replicas_healthy(&resolved.deployment, &request.expected_uid)
                .await;
            let final_drain_is_safe = final_drain.as_ref().is_ok_and(|state| {
                state.operation_id.as_deref() == Some(request.operation_id.as_str())
                    && state.phase == ProxyDrainPhase::Drained
                    && state.zero_pending
                    && state.pending.is_zero()
            });
            if !final_drain_is_safe || !matches!(remaining_replicas_healthy, Ok(true)) {
                self.cancel_drain(&original_proxy_addr, &request.operation_id).await?;
                self.clear_restart(&resolved.allowed, &request.operation_id).await?;
                return Err(ExecutionAgentError::DriverFailed);
            }
            if self
                .delete_expected_pod(&request.namespace, &request.pod, &request.expected_uid)
                .await
                .is_err()
            {
                self.cancel_drain(&original_proxy_addr, &request.operation_id).await?;
                self.clear_restart(&resolved.allowed, &request.operation_id).await?;
                return Err(ExecutionAgentError::DriverFailed);
            }

            let replacement = self
                .wait_for_replacement(&request.namespace, &request.pod, &request.expected_uid, deadline)
                .await?;
            let replacement_addr = proxy_addr(&replacement, resolved.allowed.remoting_port)?;
            let replacement_drain = self.query_drain(&replacement_addr).await?;
            let verification = self.verification.observe().await?;
            if !accepting(&replacement_drain) || !verification.synthetic_path_healthy || !verification.slo_healthy {
                return Err(ExecutionAgentError::DriverFailed);
            }
            self.complete_restart(&resolved.allowed, &request.operation_id).await?;
            Ok(())
        })
    }

    fn cancel_restart_and_restore<'a>(
        &'a self,
        request: &'a ProxyRestartRestore,
    ) -> DriverFuture<'a, ProxyRestartRestoreOutcome> {
        Box::pin(async move {
            let resolved = self.resolve_target(&request.namespace, &request.pod).await?;
            if annotation(&resolved.deployment, OPERATION_ANNOTATION) != Some(request.operation_id.as_str()) {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let Some(pod) = resolved.pod else {
                return Ok(ProxyRestartRestoreOutcome::ManualTakeoverRequired);
            };
            if pod.metadata.uid.as_deref() != Some(request.expected_uid.as_str()) {
                return Ok(ProxyRestartRestoreOutcome::ManualTakeoverRequired);
            }
            let proxy_addr = proxy_addr(&pod, resolved.allowed.remoting_port)?;
            let state = self.query_drain(&proxy_addr).await?;
            if !accepting(&state) {
                self.cancel_drain(&proxy_addr, &request.operation_id).await?;
            }
            self.clear_restart(&resolved.allowed, &request.operation_id).await?;
            Ok(ProxyRestartRestoreOutcome::IngressRestored)
        })
    }
}

async fn decode_observation(mut response: reqwest::Response) -> Result<ExecutionSliObservation, ExecutionAgentError> {
    if response.status() != StatusCode::OK
        || response
            .content_length()
            .is_some_and(|length| length > MAX_VERIFICATION_RESPONSE_BYTES as u64)
    {
        return Err(ExecutionAgentError::DriverFailed);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| ExecutionAgentError::DriverFailed)? {
        if bytes.len().saturating_add(chunk.len()) > MAX_VERIFICATION_RESPONSE_BYTES {
            return Err(ExecutionAgentError::DriverFailed);
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| ExecutionAgentError::DriverFailed)
}

fn validate_observation(
    query: &ExecutionSliQuery,
    observation: &ExecutionSliObservation,
) -> Result<(), ExecutionAgentError> {
    let expected = query.conditions.iter().collect::<std::collections::BTreeSet<_>>();
    let actual = observation.conditions.keys().collect::<std::collections::BTreeSet<_>>();
    if observation.schema_version != EXECUTION_VERIFICATION_SCHEMA_VERSION
        || observation.tenant_id != query.tenant_id
        || observation.cluster_id != query.cluster_id
        || observation.correlation_id != query.correlation_id
        || expected.len() != query.conditions.len()
        || actual != expected
    {
        Err(ExecutionAgentError::DriverFailed)
    } else {
        Ok(())
    }
}

fn controller_owner_name<T>(resource: &T) -> Option<&str>
where
    T: kube::Resource<DynamicType = ()>,
{
    resource
        .meta()
        .owner_references
        .as_ref()?
        .iter()
        .find(|owner| owner.controller == Some(true))
        .map(|owner| owner.name.as_str())
}

fn annotation<'a>(deployment: &'a Deployment, key: &str) -> Option<&'a str> {
    deployment
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
        .map(String::as_str)
}

fn replacement_started_at_or_after(created: &k8s_openapi::jiff::Timestamp, started_at: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(started_at)
        .is_ok_and(|started_at| created.as_second() >= started_at.timestamp())
}

fn accepting(state: &ProxyDrainState) -> bool {
    state.phase == ProxyDrainPhase::Accepting
        && state.operation_id.is_none()
        && state.admission_open
        && state.routing_open
        && state.readiness_published
        && state.zero_pending == state.pending.is_zero()
}

fn pod_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}

fn proxy_addr(pod: &Pod, port: u16) -> Result<String, ExecutionAgentError> {
    let ip = pod
        .status
        .as_ref()
        .and_then(|status| status.pod_ip.as_deref())
        .ok_or(ExecutionAgentError::DriverFailed)?;
    if ip.contains(':') {
        Ok(format!("[{ip}]:{port}"))
    } else {
        Ok(format!("{ip}:{port}"))
    }
}

fn selector_matches(selector: &LabelSelector, labels: Option<&BTreeMap<String, String>>) -> bool {
    let empty = BTreeMap::new();
    let labels = labels.unwrap_or(&empty);
    if selector
        .match_labels
        .as_ref()
        .is_some_and(|required| required.iter().any(|(key, value)| labels.get(key) != Some(value)))
    {
        return false;
    }
    selector.match_expressions.as_ref().is_none_or(|requirements| {
        requirements.iter().all(|requirement| {
            let value = labels.get(&requirement.key);
            match requirement.operator.as_str() {
                "In" => {
                    value.is_some_and(|value| requirement.values.as_ref().is_some_and(|values| values.contains(value)))
                }
                "NotIn" => {
                    value.is_none_or(|value| requirement.values.as_ref().is_none_or(|values| !values.contains(value)))
                }
                "Exists" => value.is_some(),
                "DoesNotExist" => value.is_none(),
                _ => false,
            }
        })
    })
}

fn duration_millis(duration: Duration) -> Result<u64, ExecutionAgentError> {
    u64::try_from(duration.as_millis()).map_err(|_| ExecutionAgentError::Configuration)
}

#[cfg(test)]
#[path = "production_proxy_restart_tests.rs"]
mod tests;
