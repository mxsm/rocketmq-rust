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

use chrono::Utc;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::Api;
use kube::Client;
use kube::Config;
use kube::api::DeleteParams;
use kube::api::ListParams;
use kube::api::PostParams;
use kube::api::Preconditions;
use sqlx::PgPool;
use sqlx::Row;
use uuid::Uuid;

use super::DriverFuture;
use super::ProxyImageCanaryClient;
use super::ProxyImageCanaryRestore;
use super::ProxyImageCanaryState;
use super::ProxyImageCanaryWrite;
use crate::ExecutionAgentError;

const CANARY_SUFFIX: &str = "-sre-canary";
const CANARY_LABEL: &str = "rocketmq.apache.org/sre-canary-operation";
const OPERATION_ANNOTATION: &str = "rocketmq.apache.org/sre-canary-operation";
const EXECUTION_ANNOTATION: &str = "rocketmq.apache.org/sre-canary-execution";
const PLAN_STEP_ANNOTATION: &str = "rocketmq.apache.org/sre-canary-plan-step";
const BASE_GENERATION_ANNOTATION: &str = "rocketmq.apache.org/sre-canary-base-generation";
const ORIGINAL_REPLICAS_ANNOTATION: &str = "rocketmq.apache.org/sre-canary-original-replicas";
const PREVIOUS_IMAGE_ANNOTATION: &str = "rocketmq.apache.org/sre-canary-previous-image";
const IMAGE_DIGEST_ANNOTATION: &str = "rocketmq.apache.org/sre-canary-image-digest";

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanaryBeforeState {
    namespace: String,
    workload: String,
    container: String,
    operation_id: String,
    base_generation: u64,
    previous_image: String,
    image_digest: String,
    original_replicas: u32,
}

/// Production client that stages one isolated Proxy Deployment using only the
/// current image repository plus an immutable SHA-256 digest.
#[derive(Clone)]
pub(crate) struct ProductionProxyImageCanaryClient {
    client: Client,
    allowed_targets: Arc<BTreeSet<String>>,
    pool: PgPool,
}

impl ProductionProxyImageCanaryClient {
    pub(crate) async fn start(allowed_targets: BTreeSet<String>, pool: PgPool) -> Result<Self, ExecutionAgentError> {
        if allowed_targets.is_empty()
            || allowed_targets.iter().any(|target| {
                target
                    .split_once('/')
                    .is_none_or(|(_, workload)| workload.len() + CANARY_SUFFIX.len() > 253)
            })
        {
            return Err(ExecutionAgentError::Configuration);
        }
        let mut config = Config::infer().await.map_err(|_| ExecutionAgentError::Configuration)?;
        config.proxy_url = None;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::try_from(config).map_err(|_| ExecutionAgentError::Configuration)?;
        Ok(Self {
            client,
            allowed_targets: Arc::new(allowed_targets),
            pool,
        })
    }

    fn require_target(&self, namespace: &str, workload: &str) -> Result<(), ExecutionAgentError> {
        self.allowed_targets
            .contains(&format!("{namespace}/{workload}"))
            .then_some(())
            .ok_or(ExecutionAgentError::InvalidRequest)
    }

    fn canary_name(&self, namespace: &str, workload: &str) -> Result<String, ExecutionAgentError> {
        self.require_target(namespace, workload)?;
        Ok(format!("{workload}{CANARY_SUFFIX}"))
    }

    async fn deployment(&self, namespace: &str, workload: &str) -> Result<Deployment, ExecutionAgentError> {
        self.require_target(namespace, workload)?;
        Api::<Deployment>::namespaced(self.client.clone(), namespace)
            .get(workload)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn canary(&self, namespace: &str, workload: &str) -> Result<Option<Deployment>, ExecutionAgentError> {
        let name = self.canary_name(namespace, workload)?;
        Api::<Deployment>::namespaced(self.client.clone(), namespace)
            .get_opt(&name)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn pdb_healthy(&self, namespace: &str, deployment: &Deployment) -> Result<bool, ExecutionAgentError> {
        let labels = deployment
            .spec
            .as_ref()
            .and_then(|spec| spec.template.metadata.as_ref())
            .and_then(|metadata| metadata.labels.as_ref())
            .ok_or(ExecutionAgentError::DriverFailed)?;
        let budgets = Api::<PodDisruptionBudget>::namespaced(self.client.clone(), namespace)
            .list(&ListParams::default())
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        let matching = budgets
            .items
            .iter()
            .filter(|budget| {
                budget
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.selector.as_ref())
                    .is_some_and(|selector| selector_matches(selector, labels))
            })
            .collect::<Vec<_>>();
        Ok(!matching.is_empty() && matching.into_iter().all(pdb_status_healthy))
    }

    async fn persist_before(
        &self,
        request: &ProxyImageCanaryWrite,
        state: &CanaryBeforeState,
    ) -> Result<CanaryBeforeState, ExecutionAgentError> {
        sqlx::query(
            "INSERT INTO execution_agent_proxy_canary_before_states (
                 id, execution_id, plan_step_id, namespace, workload,
                 container_name, operation_id, base_generation, previous_image,
                 candidate_image_digest, original_replicas, created_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (execution_id, plan_step_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(request.execution_id.as_uuid())
        .bind(request.plan_step_id.as_uuid())
        .bind(&state.namespace)
        .bind(&state.workload)
        .bind(&state.container)
        .bind(&state.operation_id)
        .bind(i64::try_from(state.base_generation).map_err(|_| ExecutionAgentError::InvalidRequest)?)
        .bind(&state.previous_image)
        .bind(&state.image_digest)
        .bind(i32::try_from(state.original_replicas).map_err(|_| ExecutionAgentError::InvalidRequest)?)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|_| ExecutionAgentError::DriverFailed)?;
        let stored = self
            .load_before(&request.execution_id.as_uuid(), &request.plan_step_id.as_uuid())
            .await?;
        if stored == *state {
            Ok(stored)
        } else {
            Err(ExecutionAgentError::DriverFailed)
        }
    }

    async fn load_before(
        &self,
        execution_id: &Uuid,
        plan_step_id: &Uuid,
    ) -> Result<CanaryBeforeState, ExecutionAgentError> {
        let row = sqlx::query(
            "SELECT namespace, workload, container_name, operation_id,
                    base_generation, previous_image, candidate_image_digest,
                    original_replicas
             FROM execution_agent_proxy_canary_before_states
             WHERE execution_id = $1 AND plan_step_id = $2",
        )
        .bind(execution_id)
        .bind(plan_step_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ExecutionAgentError::DriverFailed)?
        .ok_or(ExecutionAgentError::DriverFailed)?;
        Ok(CanaryBeforeState {
            namespace: row
                .try_get("namespace")
                .map_err(|_| ExecutionAgentError::DriverFailed)?,
            workload: row.try_get("workload").map_err(|_| ExecutionAgentError::DriverFailed)?,
            container: row
                .try_get("container_name")
                .map_err(|_| ExecutionAgentError::DriverFailed)?,
            operation_id: row
                .try_get("operation_id")
                .map_err(|_| ExecutionAgentError::DriverFailed)?,
            base_generation: u64::try_from(
                row.try_get::<i64, _>("base_generation")
                    .map_err(|_| ExecutionAgentError::DriverFailed)?,
            )
            .map_err(|_| ExecutionAgentError::DriverFailed)?,
            previous_image: row
                .try_get("previous_image")
                .map_err(|_| ExecutionAgentError::DriverFailed)?,
            image_digest: row
                .try_get("candidate_image_digest")
                .map_err(|_| ExecutionAgentError::DriverFailed)?,
            original_replicas: u32::try_from(
                row.try_get::<i32, _>("original_replicas")
                    .map_err(|_| ExecutionAgentError::DriverFailed)?,
            )
            .map_err(|_| ExecutionAgentError::DriverFailed)?,
        })
    }

    async fn append_result(
        &self,
        request: &ProxyImageCanaryWrite,
        canary_name: &str,
        canary_uid: Option<&str>,
        ready: bool,
    ) -> Result<(), ExecutionAgentError> {
        self.insert_result(
            &request.execution_id.as_uuid(),
            &request.plan_step_id.as_uuid(),
            &request.namespace,
            &request.workload,
            canary_name,
            &request.operation_id,
            "forward",
            canary_uid,
            &request.image_digest,
            ready,
        )
        .await
    }

    #[allow(clippy::too_many_arguments, reason = "one closed append-only journal row")]
    async fn insert_result(
        &self,
        execution_id: &Uuid,
        plan_step_id: &Uuid,
        namespace: &str,
        workload: &str,
        canary_name: &str,
        operation_id: &str,
        direction: &str,
        canary_uid: Option<&str>,
        image_digest: &str,
        ready: bool,
    ) -> Result<(), ExecutionAgentError> {
        sqlx::query(
            "INSERT INTO execution_agent_proxy_canary_results (
                 execution_id, plan_step_id, namespace, workload, canary_name,
                 operation_id, direction, canary_uid, image_digest, ready,
                 recorded_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (operation_id, direction) DO NOTHING",
        )
        .bind(execution_id)
        .bind(plan_step_id)
        .bind(namespace)
        .bind(workload)
        .bind(canary_name)
        .bind(operation_id)
        .bind(direction)
        .bind(canary_uid)
        .bind(image_digest)
        .bind(ready)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|_| ExecutionAgentError::DriverFailed)?;
        Ok(())
    }

    fn build_canary(
        &self,
        main: &Deployment,
        request: &ProxyImageCanaryWrite,
        previous_image: &str,
        target_image: &str,
        original_replicas: u32,
    ) -> Result<Deployment, ExecutionAgentError> {
        let canary_name = self.canary_name(&request.namespace, &request.workload)?;
        let mut canary = main.clone();
        canary.metadata.name = Some(canary_name);
        canary.metadata.namespace = None;
        canary.metadata.uid = None;
        canary.metadata.resource_version = None;
        canary.metadata.generation = None;
        canary.metadata.creation_timestamp = None;
        canary.metadata.deletion_timestamp = None;
        canary.metadata.deletion_grace_period_seconds = None;
        canary.metadata.managed_fields = None;
        canary.metadata.owner_references = None;
        canary.metadata.finalizers = None;
        let operation_label = label_value(&request.operation_id)?;
        let labels = canary.metadata.labels.get_or_insert_with(BTreeMap::new);
        labels.insert(CANARY_LABEL.to_owned(), operation_label.clone());
        let annotations = canary.metadata.annotations.get_or_insert_with(BTreeMap::new);
        annotations.insert(OPERATION_ANNOTATION.to_owned(), request.operation_id.clone());
        annotations.insert(EXECUTION_ANNOTATION.to_owned(), request.execution_id.to_string());
        annotations.insert(PLAN_STEP_ANNOTATION.to_owned(), request.plan_step_id.to_string());
        annotations.insert(
            BASE_GENERATION_ANNOTATION.to_owned(),
            request.expected_generation.to_string(),
        );
        annotations.insert(ORIGINAL_REPLICAS_ANNOTATION.to_owned(), original_replicas.to_string());
        annotations.insert(PREVIOUS_IMAGE_ANNOTATION.to_owned(), previous_image.to_owned());
        annotations.insert(IMAGE_DIGEST_ANNOTATION.to_owned(), request.image_digest.clone());

        let spec = canary.spec.as_mut().ok_or(ExecutionAgentError::DriverFailed)?;
        spec.replicas = Some(1);
        let selector = spec.selector.match_labels.get_or_insert_with(BTreeMap::new);
        selector.insert(CANARY_LABEL.to_owned(), operation_label.clone());
        let template_metadata = spec.template.metadata.get_or_insert_with(Default::default);
        template_metadata
            .labels
            .get_or_insert_with(BTreeMap::new)
            .insert(CANARY_LABEL.to_owned(), operation_label);
        let template_annotations = template_metadata.annotations.get_or_insert_with(BTreeMap::new);
        template_annotations.insert(OPERATION_ANNOTATION.to_owned(), request.operation_id.clone());
        template_annotations.insert(EXECUTION_ANNOTATION.to_owned(), request.execution_id.to_string());
        template_annotations.insert(PLAN_STEP_ANNOTATION.to_owned(), request.plan_step_id.to_string());
        let containers = spec
            .template
            .spec
            .as_mut()
            .ok_or(ExecutionAgentError::DriverFailed)?
            .containers
            .as_mut_slice();
        let container = containers
            .iter_mut()
            .find(|container| container.name == request.container)
            .ok_or(ExecutionAgentError::InvalidRequest)?;
        container.image = Some(target_image.to_owned());
        Ok(canary)
    }
}

impl ProxyImageCanaryClient for ProductionProxyImageCanaryClient {
    fn proxy_image_canary_state<'a>(
        &'a self,
        namespace: &'a str,
        workload: &'a str,
        container: &'a str,
    ) -> DriverFuture<'a, ProxyImageCanaryState> {
        Box::pin(async move {
            let main = self.deployment(namespace, workload).await?;
            let main_generation = generation(&main)?;
            let main_replicas = desired_replicas(&main)?;
            let main_image = container_image(&main, container)?;
            let main_ready = deployment_ready(&main)?;
            let pdb_healthy = self.pdb_healthy(namespace, &main).await?;
            let Some(canary) = self.canary(namespace, workload).await? else {
                return Ok(ProxyImageCanaryState {
                    generation: main_generation,
                    observed_generation: observed_generation(&main)?,
                    image_digest: image_digest(&main_image).unwrap_or_default().to_owned(),
                    ready_canary_replicas: 0,
                    old_replicas_unchanged: main_ready,
                    pdb_healthy,
                    slo_healthy: main_ready,
                    last_operation_id: None,
                });
            };
            let base_generation = annotation_u64(&canary, BASE_GENERATION_ANNOTATION)?;
            let original_replicas = annotation_u32(&canary, ORIGINAL_REPLICAS_ANNOTATION)?;
            let previous_image =
                annotation(&canary, PREVIOUS_IMAGE_ANNOTATION).ok_or(ExecutionAgentError::DriverFailed)?;
            let candidate_digest =
                annotation(&canary, IMAGE_DIGEST_ANNOTATION).ok_or(ExecutionAgentError::DriverFailed)?;
            let ready_canary_replicas = canary
                .status
                .as_ref()
                .and_then(|status| status.ready_replicas)
                .map_or(Ok(0), |value| {
                    u32::try_from(value).map_err(|_| ExecutionAgentError::DriverFailed)
                })?;
            let canary_ready = deployment_ready(&canary)? && ready_canary_replicas == 1;
            let old_replicas_unchanged = main_generation == base_generation
                && main_replicas == original_replicas
                && main_image == previous_image
                && main_ready;
            Ok(ProxyImageCanaryState {
                generation: base_generation
                    .checked_add(1)
                    .ok_or(ExecutionAgentError::DriverFailed)?,
                observed_generation: if canary_ready {
                    base_generation
                        .checked_add(1)
                        .ok_or(ExecutionAgentError::DriverFailed)?
                } else {
                    base_generation
                },
                image_digest: candidate_digest.to_owned(),
                ready_canary_replicas,
                old_replicas_unchanged,
                pdb_healthy,
                slo_healthy: old_replicas_unchanged && canary_ready,
                last_operation_id: annotation(&canary, OPERATION_ANNOTATION).map(str::to_owned),
            })
        })
    }

    fn rollout_proxy_image_canary<'a>(&'a self, request: &'a ProxyImageCanaryWrite) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            if request.canary_replicas != 1 || !valid_digest(&request.image_digest) {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let main = self.deployment(&request.namespace, &request.workload).await?;
            if generation(&main)? != request.expected_generation
                || !deployment_ready(&main)?
                || !self.pdb_healthy(&request.namespace, &main).await?
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let previous_image = container_image(&main, &request.container)?;
            let repository = image_repository(&previous_image).ok_or(ExecutionAgentError::DriverFailed)?;
            let target_image = format!("{repository}@{}", request.image_digest);
            let original_replicas = desired_replicas(&main)?;
            let before = CanaryBeforeState {
                namespace: request.namespace.clone(),
                workload: request.workload.clone(),
                container: request.container.clone(),
                operation_id: request.operation_id.clone(),
                base_generation: request.expected_generation,
                previous_image: previous_image.clone(),
                image_digest: request.image_digest.clone(),
                original_replicas,
            };
            self.persist_before(request, &before).await?;
            let canary_name = self.canary_name(&request.namespace, &request.workload)?;
            if let Some(existing) = self.canary(&request.namespace, &request.workload).await? {
                if annotation(&existing, OPERATION_ANNOTATION) == Some(request.operation_id.as_str())
                    && annotation(&existing, IMAGE_DIGEST_ANNOTATION) == Some(request.image_digest.as_str())
                {
                    return Ok(());
                }
                return Err(ExecutionAgentError::DriverFailed);
            }
            let canary = self.build_canary(&main, request, &previous_image, &target_image, original_replicas)?;
            let created = Api::<Deployment>::namespaced(self.client.clone(), &request.namespace)
                .create(&PostParams::default(), &canary)
                .await
                .map_err(|_| ExecutionAgentError::DriverFailed)?;
            let uid = created
                .metadata
                .uid
                .as_deref()
                .ok_or(ExecutionAgentError::DriverUnknown)?;
            if annotation(&created, OPERATION_ANNOTATION) != Some(request.operation_id.as_str())
                || container_image(&created, &request.container)? != target_image
            {
                return Err(ExecutionAgentError::DriverUnknown);
            }
            self.append_result(request, &canary_name, Some(uid), false).await
        })
    }

    fn restore_proxy_image<'a>(&'a self, request: &'a ProxyImageCanaryRestore) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let before = self
                .load_before(&request.execution_id.as_uuid(), &request.plan_step_id.as_uuid())
                .await?;
            if before.namespace != request.namespace
                || before.workload != request.workload
                || before.container != request.container
            {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let canary_name = self.canary_name(&request.namespace, &request.workload)?;
            let Some(canary) = self.canary(&request.namespace, &request.workload).await? else {
                return Ok(());
            };
            let execution_id = request.execution_id.to_string();
            let plan_step_id = request.plan_step_id.to_string();
            if annotation(&canary, OPERATION_ANNOTATION) != Some(before.operation_id.as_str())
                || annotation(&canary, EXECUTION_ANNOTATION) != Some(execution_id.as_str())
                || annotation(&canary, PLAN_STEP_ANNOTATION) != Some(plan_step_id.as_str())
                || annotation(&canary, IMAGE_DIGEST_ANNOTATION) != Some(before.image_digest.as_str())
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let uid = canary.metadata.uid.clone().ok_or(ExecutionAgentError::DriverFailed)?;
            Api::<Deployment>::namespaced(self.client.clone(), &request.namespace)
                .delete(
                    &canary_name,
                    &DeleteParams {
                        grace_period_seconds: Some(0),
                        preconditions: Some(Preconditions {
                            uid: Some(uid.clone()),
                            resource_version: canary.metadata.resource_version.clone(),
                        }),
                        ..DeleteParams::default()
                    },
                )
                .await
                .map_err(|_| ExecutionAgentError::DriverFailed)?;
            self.insert_result(
                &request.execution_id.as_uuid(),
                &request.plan_step_id.as_uuid(),
                &request.namespace,
                &request.workload,
                &canary_name,
                &request.operation_id,
                "compensation",
                Some(&uid),
                &before.image_digest,
                false,
            )
            .await
        })
    }
}

fn generation(deployment: &Deployment) -> Result<u64, ExecutionAgentError> {
    deployment
        .metadata
        .generation
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(ExecutionAgentError::DriverFailed)
}

fn observed_generation(deployment: &Deployment) -> Result<u64, ExecutionAgentError> {
    deployment
        .status
        .as_ref()
        .and_then(|status| status.observed_generation)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(ExecutionAgentError::DriverFailed)
}

fn desired_replicas(deployment: &Deployment) -> Result<u32, ExecutionAgentError> {
    deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.replicas)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(ExecutionAgentError::DriverFailed)
}

fn deployment_ready(deployment: &Deployment) -> Result<bool, ExecutionAgentError> {
    let desired = i32::try_from(desired_replicas(deployment)?).map_err(|_| ExecutionAgentError::DriverFailed)?;
    let generation = deployment
        .metadata
        .generation
        .ok_or(ExecutionAgentError::DriverFailed)?;
    let status = deployment.status.as_ref().ok_or(ExecutionAgentError::DriverFailed)?;
    Ok(status.observed_generation == Some(generation)
        && status.ready_replicas == Some(desired)
        && status.unavailable_replicas.unwrap_or_default() == 0)
}

fn container_image(deployment: &Deployment, container_name: &str) -> Result<String, ExecutionAgentError> {
    deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .and_then(|spec| {
            spec.containers
                .iter()
                .find(|container| container.name == container_name)
        })
        .and_then(|container| container.image.clone())
        .filter(|image| !image.is_empty() && image.len() <= 512)
        .ok_or(ExecutionAgentError::DriverFailed)
}

fn image_repository(image: &str) -> Option<&str> {
    let without_digest = image.split_once('@').map_or(image, |(repository, _)| repository);
    let last_slash = without_digest.rfind('/').unwrap_or(0);
    let repository = match without_digest.rfind(':') {
        Some(index) if index > last_slash => &without_digest[..index],
        _ => without_digest,
    };
    (!repository.is_empty() && !repository.bytes().any(|byte| byte.is_ascii_whitespace())).then_some(repository)
}

fn image_digest(image: &str) -> Option<&str> {
    image
        .split_once('@')
        .map(|(_, digest)| digest)
        .filter(|digest| valid_digest(digest))
}

fn valid_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn label_value(value: &str) -> Result<String, ExecutionAgentError> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && value
            .as_bytes()
            .first()
            .zip(value.as_bytes().last())
            .is_some_and(|(first, last)| first.is_ascii_alphanumeric() && last.is_ascii_alphanumeric());
    valid
        .then(|| value.to_owned())
        .ok_or(ExecutionAgentError::InvalidRequest)
}

fn annotation<'a>(deployment: &'a Deployment, key: &str) -> Option<&'a str> {
    deployment
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
        .map(String::as_str)
}

fn annotation_u64(deployment: &Deployment, key: &str) -> Result<u64, ExecutionAgentError> {
    annotation(deployment, key)
        .and_then(|value| value.parse().ok())
        .ok_or(ExecutionAgentError::DriverFailed)
}

fn annotation_u32(deployment: &Deployment, key: &str) -> Result<u32, ExecutionAgentError> {
    annotation(deployment, key)
        .and_then(|value| value.parse().ok())
        .ok_or(ExecutionAgentError::DriverFailed)
}

fn selector_matches(selector: &LabelSelector, labels: &BTreeMap<String, String>) -> bool {
    let labels_match = selector
        .match_labels
        .as_ref()
        .is_none_or(|required| required.iter().all(|(key, value)| labels.get(key) == Some(value)));
    let expressions_match = selector.match_expressions.as_ref().is_none_or(|requirements| {
        requirements.iter().all(|requirement| {
            let values = requirement.values.as_deref().unwrap_or_default();
            match requirement.operator.as_str() {
                "In" => labels
                    .get(&requirement.key)
                    .is_some_and(|value| values.iter().any(|candidate| candidate == value)),
                "NotIn" => labels
                    .get(&requirement.key)
                    .is_some_and(|value| values.iter().all(|candidate| candidate != value)),
                "Exists" => labels.contains_key(&requirement.key),
                "DoesNotExist" => !labels.contains_key(&requirement.key),
                _ => false,
            }
        })
    });
    labels_match && expressions_match
}

fn pdb_status_healthy(budget: &PodDisruptionBudget) -> bool {
    let Some(status) = budget.status.as_ref() else {
        return false;
    };
    let generation_current = budget
        .metadata
        .generation
        .zip(status.observed_generation)
        .is_some_and(|(generation, observed)| generation == observed);
    let health_current = status
        .current_healthy
        .zip(status.desired_healthy)
        .is_some_and(|(current, desired)| current >= desired);
    let no_pending_disruptions = status.disrupted_pods.as_ref().is_none_or(BTreeMap::is_empty);
    generation_current && health_current && no_pending_disruptions
}

#[cfg(test)]
#[path = "production_proxy_image_canary_tests.rs"]
mod tests;
