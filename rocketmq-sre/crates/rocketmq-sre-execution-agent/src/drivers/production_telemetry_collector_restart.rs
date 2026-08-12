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
use std::collections::BTreeSet;
use std::sync::Arc;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::Client;
use kube::Config;
use kube::api::ListParams;
use kube::api::PostParams;

use super::DriverFuture;
use super::TelemetryCollectorRestartClient;
use super::TelemetryCollectorRestartOneWrite;
use super::TelemetryCollectorRestartState;
use crate::ExecutionAgentError;

const OPERATION_ANNOTATION: &str = "rocketmqrust.com/sre-collector-restart-operation";
const EXECUTION_ANNOTATION: &str = "rocketmqrust.com/sre-collector-restart-execution";
const PLAN_STEP_ANNOTATION: &str = "rocketmqrust.com/sre-collector-restart-plan-step";
const EXPECTED_UID_ANNOTATION: &str = "rocketmqrust.com/sre-collector-restart-expected-uid";

/// Production typed Kubernetes client for one allowlisted Collector rollout.
#[derive(Clone)]
pub(crate) struct ProductionTelemetryCollectorRestartClient {
    client: Client,
    allowed_targets: Arc<BTreeSet<String>>,
}

impl ProductionTelemetryCollectorRestartClient {
    pub(crate) async fn start(allowed_targets: BTreeSet<String>) -> Result<Self, ExecutionAgentError> {
        if allowed_targets.is_empty() {
            return Err(ExecutionAgentError::Configuration);
        }
        let mut config = Config::infer().await.map_err(|_| ExecutionAgentError::Configuration)?;
        config.proxy_url = None;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::try_from(config).map_err(|_| ExecutionAgentError::Configuration)?;
        Ok(Self {
            client,
            allowed_targets: Arc::new(allowed_targets),
        })
    }

    fn workload_for_namespace(&self, namespace: &str) -> Result<&str, ExecutionAgentError> {
        let prefix = format!("{namespace}/");
        let mut workloads = self
            .allowed_targets
            .iter()
            .filter_map(|target| target.strip_prefix(&prefix));
        let workload = workloads.next().ok_or(ExecutionAgentError::InvalidRequest)?;
        if workloads.next().is_some() {
            return Err(ExecutionAgentError::Configuration);
        }
        Ok(workload)
    }

    async fn deployment(&self, namespace: &str, workload: &str) -> Result<Deployment, ExecutionAgentError> {
        Api::<Deployment>::namespaced(self.client.clone(), namespace)
            .get(workload)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn deployment_pods(&self, namespace: &str, deployment: &Deployment) -> Result<Vec<Pod>, ExecutionAgentError> {
        let selector = deployment_selector(deployment)?;
        Api::<Pod>::namespaced(self.client.clone(), namespace)
            .list(&ListParams::default().labels(&selector))
            .await
            .map(|list| list.items)
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn live_state(
        &self,
        namespace: &str,
        requested_pod: &str,
    ) -> Result<(Deployment, Pod, bool), ExecutionAgentError> {
        let workload = self.workload_for_namespace(namespace)?;
        let deployment = self.deployment(namespace, workload).await?;
        let mut pods = self.deployment_pods(namespace, &deployment).await?;
        pods.retain(|pod| pod.metadata.deletion_timestamp.is_none());
        pods.sort_by(|left, right| {
            left.metadata
                .creation_timestamp
                .cmp(&right.metadata.creation_timestamp)
                .then_with(|| left.metadata.name.cmp(&right.metadata.name))
        });
        let replacement = pods
            .iter()
            .rev()
            .find(|pod| pod.metadata.name.as_deref() != Some(requested_pod) && pod_ready(pod))
            .cloned();
        let requested = pods
            .iter()
            .find(|pod| pod.metadata.name.as_deref() == Some(requested_pod))
            .cloned();
        let active = replacement
            .or(requested)
            .or_else(|| pods.into_iter().rev().find(pod_ready))
            .ok_or(ExecutionAgentError::DriverFailed)?;
        let replaced = active.metadata.name.as_deref() != Some(requested_pod);
        Ok((deployment, active, replaced))
    }

    async fn replace_template_annotations(
        &self,
        request: &TelemetryCollectorRestartOneWrite,
    ) -> Result<(), ExecutionAgentError> {
        let workload = self.workload_for_namespace(&request.namespace)?.to_owned();
        let mut deployment = self.deployment(&request.namespace, &workload).await?;
        let pod = Api::<Pod>::namespaced(self.client.clone(), &request.namespace)
            .get(&request.pod)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        let uid = pod.metadata.uid.as_deref().ok_or(ExecutionAgentError::DriverFailed)?;
        if uid != request.expected_uid || !pod_ready(&pod) || !pod_matches_deployment(&pod, &deployment)? {
            return Err(ExecutionAgentError::DriverFailed);
        }
        if !deployment_ready(&deployment)? {
            return Err(ExecutionAgentError::DriverFailed);
        }
        let annotations = deployment
            .spec
            .as_mut()
            .ok_or(ExecutionAgentError::DriverFailed)?
            .template
            .metadata
            .get_or_insert_with(Default::default)
            .annotations
            .get_or_insert_with(BTreeMap::new);
        annotations.insert(OPERATION_ANNOTATION.to_owned(), request.operation_id.clone());
        annotations.insert(EXECUTION_ANNOTATION.to_owned(), request.execution_id.to_string());
        annotations.insert(PLAN_STEP_ANNOTATION.to_owned(), request.plan_step_id.to_string());
        annotations.insert(EXPECTED_UID_ANNOTATION.to_owned(), request.expected_uid.clone());
        let execution_id = request.execution_id.to_string();
        let plan_step_id = request.plan_step_id.to_string();
        let stored = Api::<Deployment>::namespaced(self.client.clone(), &request.namespace)
            .replace(&workload, &PostParams::default(), &deployment)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        if template_annotation(&stored, OPERATION_ANNOTATION) != Some(request.operation_id.as_str())
            || template_annotation(&stored, EXECUTION_ANNOTATION) != Some(execution_id.as_str())
            || template_annotation(&stored, PLAN_STEP_ANNOTATION) != Some(plan_step_id.as_str())
            || template_annotation(&stored, EXPECTED_UID_ANNOTATION) != Some(request.expected_uid.as_str())
        {
            return Err(ExecutionAgentError::DriverUnknown);
        }
        Ok(())
    }
}

impl TelemetryCollectorRestartClient for ProductionTelemetryCollectorRestartClient {
    fn telemetry_collector_restart_state<'a>(
        &'a self,
        namespace: &'a str,
        pod: &'a str,
        pipeline: &'a str,
    ) -> DriverFuture<'a, TelemetryCollectorRestartState> {
        Box::pin(async move {
            if !matches!(pipeline, "metrics" | "logs" | "traces" | "combined") {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let (deployment, active, replacement_ready) = self.live_state(namespace, pod).await?;
            let pod_uid = active.metadata.uid.clone().ok_or(ExecutionAgentError::DriverFailed)?;
            let active_pod = active.metadata.name.clone().ok_or(ExecutionAgentError::DriverFailed)?;
            let pod_ready = pod_ready(&active);
            let deployment_ready = deployment_ready(&deployment)?;
            // The Collector readiness probe is backed by the Collector health
            // extension. Queue/exporter health is independently checked again
            // by the Executor technical SLI verification window.
            let pipeline_ready = pod_ready && deployment_ready;
            Ok(TelemetryCollectorRestartState {
                pod_uid,
                pod_ready,
                deployment_ready,
                replacement_ready,
                exporter_connected: pipeline_ready,
                queue_healthy: pipeline_ready,
                // This typed client can mutate only its allowlisted
                // observability Deployment; it has no RocketMQ data-plane
                // operation in this action surface.
                data_plane_unaffected: true,
                active_pod,
                last_operation_id: template_annotation(&deployment, OPERATION_ANNOTATION).map(str::to_owned),
                last_execution_id: template_annotation(&deployment, EXECUTION_ANNOTATION).map(str::to_owned),
                last_plan_step_id: template_annotation(&deployment, PLAN_STEP_ANNOTATION).map(str::to_owned),
            })
        })
    }

    fn restart_one_telemetry_collector<'a>(
        &'a self,
        request: &'a TelemetryCollectorRestartOneWrite,
    ) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            if !matches!(request.pipeline.as_str(), "metrics" | "logs" | "traces" | "combined") {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            self.replace_template_annotations(request).await
        })
    }
}

fn deployment_selector(deployment: &Deployment) -> Result<String, ExecutionAgentError> {
    let selector = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.match_labels.as_ref())
        .filter(|labels| !labels.is_empty())
        .ok_or(ExecutionAgentError::DriverFailed)?;
    if deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.match_expressions.as_ref())
        .is_some_and(|expressions| !expressions.is_empty())
    {
        return Err(ExecutionAgentError::DriverFailed);
    }
    Ok(selector
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(","))
}

fn pod_matches_deployment(pod: &Pod, deployment: &Deployment) -> Result<bool, ExecutionAgentError> {
    let required = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.match_labels.as_ref())
        .ok_or(ExecutionAgentError::DriverFailed)?;
    let labels = pod.metadata.labels.as_ref().ok_or(ExecutionAgentError::DriverFailed)?;
    Ok(required.iter().all(|(key, expected)| labels.get(key) == Some(expected)))
}

fn deployment_ready(deployment: &Deployment) -> Result<bool, ExecutionAgentError> {
    let desired = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.replicas)
        .ok_or(ExecutionAgentError::DriverFailed)?;
    let generation = deployment
        .metadata
        .generation
        .ok_or(ExecutionAgentError::DriverFailed)?;
    let status = deployment.status.as_ref().ok_or(ExecutionAgentError::DriverFailed)?;
    Ok(status.observed_generation == Some(generation)
        && status.ready_replicas == Some(desired)
        && status.unavailable_replicas.unwrap_or_default() == 0)
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

fn template_annotation<'a>(deployment: &'a Deployment, key: &str) -> Option<&'a str> {
    deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.metadata.as_ref())
        .and_then(|metadata| metadata.annotations.as_ref())
        .and_then(|annotations| annotations.get(key))
        .map(String::as_str)
}

#[cfg(test)]
#[path = "production_telemetry_collector_restart_tests.rs"]
mod tests;
