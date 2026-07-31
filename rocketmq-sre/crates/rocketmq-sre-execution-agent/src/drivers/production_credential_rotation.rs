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

use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::api::core::v1::Secret;
use kube::Api;
use kube::Client;
use kube::Config;
use kube::api::PostParams;
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_admin_core::core::topic::QueryTopicConfigCasRequest;
use rocketmq_admin_core::core::topic::TopicQueryAdmin;
use rocketmq_admin_core::read_client_adapter::ClientRuntime;
use rocketmq_admin_core::read_client_adapter::ClientRuntimeConfig;
use rocketmq_admin_core::read_client_adapter::ReadAdminBuilder;
use rocketmq_admin_core::read_client_adapter::TelemetryHandle;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_sre_contracts::canonical_precondition_hash;
use sqlx::PgPool;
use uuid::Uuid;

use self::journal::CredentialBeforeState;
use self::journal::CredentialResult;
use self::journal::CredentialRotationJournal;
use self::journal::OperationDirection;
use super::CredentialOverlapRestore;
use super::CredentialOverlapWrite;
use super::CredentialRotationClient;
use super::CredentialRotationState;
use super::DriverFuture;
use crate::ExecutionAgentError;
use crate::config::CredentialRotationDriverConfig;
use crate::config::CredentialRotationTarget;

mod journal;

const CREDENTIAL_SET_ANNOTATION: &str = "rocketmq.apache.org/sre-credential-set";
const ACTIVE_VERSION_ANNOTATION: &str = "rocketmq.apache.org/sre-active-credential-version";
const ACTIVE_SECRET_REF_ANNOTATION: &str = "rocketmq.apache.org/sre-active-credential-ref";
const RETIRING_VERSION_ANNOTATION: &str = "rocketmq.apache.org/sre-retiring-credential-version";
const RETIRING_SECRET_REF_ANNOTATION: &str = "rocketmq.apache.org/sre-retiring-credential-ref";
const OVERLAP_DEADLINE_ANNOTATION: &str = "rocketmq.apache.org/sre-credential-overlap-deadline";
const PROBE_HEALTHY_ANNOTATION: &str = "rocketmq.apache.org/sre-candidate-probe-healthy";
const OPERATION_ANNOTATION: &str = "rocketmq.apache.org/sre-credential-operation";
const EXECUTION_ANNOTATION: &str = "rocketmq.apache.org/sre-credential-execution";
const PLAN_STEP_ANNOTATION: &str = "rocketmq.apache.org/sre-credential-plan-step";
const CREDENTIAL_VERSION_ANNOTATION: &str = "rocketmq.apache.org/sre-credential-version";
const ACCESS_KEY_DATA: &str = "access-key";
const SECRET_KEY_DATA: &str = "secret-key";
const SECURITY_TOKEN_DATA: &str = "security-token";

struct SelectorState {
    resource: ConfigMap,
    uid: String,
    resource_version: String,
    active_version: String,
    active_secret_ref: String,
    retiring_version: Option<String>,
    retiring_secret_ref: Option<String>,
    overlap_deadline: Option<DateTime<Utc>>,
    candidate_probe_healthy: bool,
    last_operation_id: Option<String>,
}

/// Production credential-overlap controller.
///
/// The mutable resource is an allowlisted ConfigMap selector. Credential
/// values are read only from immutable, version-labelled Secrets, used for a
/// direct read-only RocketMQ probe, and never persisted or logged.
pub(crate) struct ProductionCredentialRotationClient {
    client: Client,
    targets: Arc<BTreeMap<String, CredentialRotationTarget>>,
    client_runtime: Arc<ClientRuntime>,
    namesrv_addr: String,
    use_tls: bool,
    timeout_millis: u64,
    journal: CredentialRotationJournal,
}

impl ProductionCredentialRotationClient {
    pub(crate) async fn start(
        config: &CredentialRotationDriverConfig,
        pool: PgPool,
        context: ChildServiceContext,
    ) -> Result<Self, ExecutionAgentError> {
        if config.targets.is_empty() {
            return Err(ExecutionAgentError::Configuration);
        }
        let mut kubernetes_config = Config::infer().await.map_err(|_| ExecutionAgentError::Configuration)?;
        kubernetes_config.proxy_url = None;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::try_from(kubernetes_config).map_err(|_| ExecutionAgentError::Configuration)?;
        let timeout_millis = u64::try_from(config.request_timeout.as_millis())
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ExecutionAgentError::Configuration)?;
        let client_runtime = ClientRuntime::try_new(
            context.component("credential-probe-admin-client"),
            ClientRuntimeConfig {
                shutdown_timeout: config.shutdown_timeout,
                ..ClientRuntimeConfig::default()
            },
            TelemetryHandle::noop(),
        )
        .map_err(|_| ExecutionAgentError::Configuration)?;
        Ok(Self {
            client,
            targets: Arc::new(config.targets.clone()),
            client_runtime,
            namesrv_addr: config.namesrv_addr.clone(),
            use_tls: config.use_tls,
            timeout_millis,
            journal: CredentialRotationJournal::new(pool),
        })
    }

    fn target(&self, credential_set: &str) -> Result<&CredentialRotationTarget, ExecutionAgentError> {
        self.targets
            .get(credential_set)
            .ok_or(ExecutionAgentError::InvalidRequest)
    }

    async fn selector_state(&self, credential_set: &str) -> Result<SelectorState, ExecutionAgentError> {
        let target = self.target(credential_set)?;
        let selector = Api::<ConfigMap>::namespaced(self.client.clone(), &target.namespace)
            .get(&target.selector_name)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        parse_selector(selector, credential_set)
    }

    async fn probe(
        &self,
        credential_set: &str,
        version: &str,
        secret_reference: &str,
        target: &CredentialRotationTarget,
    ) -> Result<bool, ExecutionAgentError> {
        let credentials = self
            .load_credentials(credential_set, version, secret_reference, &target.namespace)
            .await?;
        let suffix = Uuid::new_v4().simple();
        let builder = ReadAdminBuilder::new(Arc::clone(&self.client_runtime))
            .namesrv_addr(self.namesrv_addr.clone())
            .admin_group(format!("rocketmq-sre-credential-probe-{suffix}"))
            .instance_name(format!("rocketmq-sre-credential-probe-{suffix}"))
            .timeout_millis(self.timeout_millis)
            .use_tls(self.use_tls)
            .credentials(credentials);
        let Ok(mut admin) = builder.build_and_start().await else {
            return Ok(false);
        };
        let request = QueryTopicConfigCasRequest::try_new(&target.broker_addr, &target.validation_probe_topic)
            .map_err(|_| ExecutionAgentError::Configuration)?;
        let healthy = admin.query_config_cas_state(&request).await.is_ok();
        admin.shutdown().await;
        Ok(healthy)
    }

    async fn load_credentials(
        &self,
        credential_set: &str,
        version: &str,
        secret_reference: &str,
        required_namespace: &str,
    ) -> Result<AdminCredentials, ExecutionAgentError> {
        let reference = parse_secret_reference(secret_reference)?;
        if reference.namespace != required_namespace {
            return Err(ExecutionAgentError::InvalidRequest);
        }
        let secret = Api::<Secret>::namespaced(self.client.clone(), &reference.namespace)
            .get(&reference.name)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        if secret.immutable != Some(true)
            || annotation(&secret.metadata.annotations, CREDENTIAL_SET_ANNOTATION) != Some(credential_set)
            || annotation(&secret.metadata.annotations, CREDENTIAL_VERSION_ANNOTATION) != Some(version)
        {
            return Err(ExecutionAgentError::DriverFailed);
        }
        let data = secret.data.as_ref().ok_or(ExecutionAgentError::DriverFailed)?;
        let access_key = secret_string(data, ACCESS_KEY_DATA, 128)?;
        let secret_key = secret_string(data, SECRET_KEY_DATA, 4096)?;
        let security_token = data
            .get(SECURITY_TOKEN_DATA)
            .map(|value| bounded_utf8(value.0.as_slice(), 16 * 1024))
            .transpose()?;
        AdminCredentials::try_new(access_key, secret_key, security_token).map_err(|_| ExecutionAgentError::DriverFailed)
    }

    async fn replace_selector(
        &self,
        target: &CredentialRotationTarget,
        selector: ConfigMap,
    ) -> Result<SelectorState, ExecutionAgentError> {
        let replaced = Api::<ConfigMap>::namespaced(self.client.clone(), &target.namespace)
            .replace(&target.selector_name, &PostParams::default(), &selector)
            .await
            .map_err(|_| ExecutionAgentError::DriverFailed)?;
        parse_selector(
            replaced,
            annotation(&selector.metadata.annotations, CREDENTIAL_SET_ANNOTATION)
                .ok_or(ExecutionAgentError::DriverFailed)?,
        )
    }

    async fn state_with_probes(
        &self,
        credential_set: &str,
    ) -> Result<(SelectorState, bool, bool), ExecutionAgentError> {
        let target = self.target(credential_set)?;
        let state = self.selector_state(credential_set).await?;
        let candidate_healthy = self
            .probe(credential_set, &state.active_version, &state.active_secret_ref, target)
            .await?;
        let retiring_healthy = match (state.retiring_version.as_deref(), state.retiring_secret_ref.as_deref()) {
            (Some(version), Some(reference)) => self.probe(credential_set, version, reference, target).await?,
            (None, None) => true,
            _ => return Err(ExecutionAgentError::DriverFailed),
        };
        Ok((state, candidate_healthy && retiring_healthy, candidate_healthy))
    }
}

impl CredentialRotationClient for ProductionCredentialRotationClient {
    fn credential_rotation_state<'a>(&'a self, credential_set: &'a str) -> DriverFuture<'a, CredentialRotationState> {
        Box::pin(async move {
            let (state, all_active_healthy, candidate_healthy) = self.state_with_probes(credential_set).await?;
            Ok(CredentialRotationState {
                active_version: state.active_version,
                retiring_version: state.retiring_version,
                active_healthy: all_active_healthy,
                candidate_probe_healthy: state.retiring_secret_ref.is_some()
                    && state.candidate_probe_healthy
                    && candidate_healthy,
                overlap_deadline: state.overlap_deadline,
                last_operation_id: state.last_operation_id,
            })
        })
    }

    fn begin_credential_overlap<'a>(&'a self, request: &'a CredentialOverlapWrite) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let target = self.target(&request.credential_set)?;
            if request.validation_probe_topic != target.validation_probe_topic {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let state = self.selector_state(&request.credential_set).await?;
            if state.active_version == request.candidate_version
                && state.retiring_version.as_deref() == Some(request.active_version.as_str())
                && state.last_operation_id.as_deref() == Some(request.operation_id.as_str())
                && state.candidate_probe_healthy
                && state.overlap_deadline.is_some()
            {
                return self
                    .probe(
                        &request.credential_set,
                        &request.candidate_version,
                        &request.candidate_secret_ref,
                        target,
                    )
                    .await?
                    .then_some(())
                    .ok_or(ExecutionAgentError::DriverFailed);
            }
            if state.active_version != request.active_version
                || state.retiring_version.is_some()
                || state.retiring_secret_ref.is_some()
                || state.overlap_deadline.is_some()
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            if !self
                .probe(
                    &request.credential_set,
                    &request.active_version,
                    &state.active_secret_ref,
                    target,
                )
                .await?
                || !self
                    .probe(
                        &request.credential_set,
                        &request.candidate_version,
                        &request.candidate_secret_ref,
                        target,
                    )
                    .await?
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let candidate_secret_ref_hash = canonical_precondition_hash(&request.candidate_secret_ref)
                .map_err(|_| ExecutionAgentError::InvalidRequest)?;
            let before = CredentialBeforeState {
                credential_set: request.credential_set.clone(),
                selector_namespace: target.namespace.clone(),
                selector_name: target.selector_name.clone(),
                selector_uid: state.uid.clone(),
                selector_resource_version: state.resource_version.clone(),
                operation_id: request.operation_id.clone(),
                previous_active_version: state.active_version.clone(),
                previous_active_secret_ref: state.active_secret_ref.clone(),
                candidate_version: request.candidate_version.clone(),
                candidate_secret_ref_hash,
                validation_probe_topic: request.validation_probe_topic.clone(),
            };
            self.journal
                .persist_before(request.execution_id, request.plan_step_id, &before, Utc::now())
                .await?;
            let overlap_deadline = Utc::now()
                .checked_add_signed(chrono::Duration::seconds(i64::from(request.overlap_seconds)))
                .ok_or(ExecutionAgentError::DriverFailed)?;
            let mut selector = state.resource;
            let annotations = selector.metadata.annotations.get_or_insert_with(BTreeMap::new);
            annotations.insert(ACTIVE_VERSION_ANNOTATION.to_owned(), request.candidate_version.clone());
            annotations.insert(
                ACTIVE_SECRET_REF_ANNOTATION.to_owned(),
                request.candidate_secret_ref.clone(),
            );
            annotations.insert(RETIRING_VERSION_ANNOTATION.to_owned(), request.active_version.clone());
            annotations.insert(RETIRING_SECRET_REF_ANNOTATION.to_owned(), state.active_secret_ref);
            annotations.insert(
                OVERLAP_DEADLINE_ANNOTATION.to_owned(),
                overlap_deadline.to_rfc3339_opts(SecondsFormat::Secs, true),
            );
            annotations.insert(PROBE_HEALTHY_ANNOTATION.to_owned(), "true".to_owned());
            annotations.insert(OPERATION_ANNOTATION.to_owned(), request.operation_id.clone());
            annotations.insert(EXECUTION_ANNOTATION.to_owned(), request.execution_id.to_string());
            annotations.insert(PLAN_STEP_ANNOTATION.to_owned(), request.plan_step_id.to_string());
            let replaced = self.replace_selector(target, selector).await?;
            if replaced.uid != before.selector_uid
                || replaced.active_version != request.candidate_version
                || replaced.retiring_version.as_deref() != Some(request.active_version.as_str())
                || replaced.last_operation_id.as_deref() != Some(request.operation_id.as_str())
                || !replaced.candidate_probe_healthy
            {
                return Err(ExecutionAgentError::DriverUnknown);
            }
            self.journal
                .append_result(
                    request.execution_id,
                    request.plan_step_id,
                    &CredentialResult {
                        credential_set: &request.credential_set,
                        operation_id: &request.operation_id,
                        direction: OperationDirection::Forward,
                        active_version: &replaced.active_version,
                        retiring_version: replaced.retiring_version.as_deref(),
                        overlap_deadline: replaced.overlap_deadline,
                        candidate_probe_healthy: true,
                        selector_resource_version: &replaced.resource_version,
                    },
                    Utc::now(),
                )
                .await?;
            Ok(())
        })
    }

    fn restore_previous_credential<'a>(&'a self, request: &'a CredentialOverlapRestore) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let before = self
                .journal
                .load_before(request.execution_id, request.plan_step_id)
                .await?;
            if before.credential_set != request.credential_set {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let target = self.target(&request.credential_set)?;
            if before.selector_namespace != target.namespace
                || before.selector_name != target.selector_name
                || before.validation_probe_topic != target.validation_probe_topic
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let state = self.selector_state(&request.credential_set).await?;
            if state.uid != before.selector_uid {
                return Err(ExecutionAgentError::DriverFailed);
            }
            if state.active_version == before.previous_active_version
                && state.active_secret_ref == before.previous_active_secret_ref
                && state.retiring_version.is_none()
                && state.retiring_secret_ref.is_none()
                && state.overlap_deadline.is_none()
            {
                return Ok(());
            }
            let candidate_hash =
                canonical_precondition_hash(&state.active_secret_ref).map_err(|_| ExecutionAgentError::DriverFailed)?;
            let execution_id = request.execution_id.to_string();
            let plan_step_id = request.plan_step_id.to_string();
            if state.active_version != before.candidate_version
                || state.retiring_version.as_deref() != Some(before.previous_active_version.as_str())
                || state.retiring_secret_ref.as_deref() != Some(before.previous_active_secret_ref.as_str())
                || state.last_operation_id.as_deref() != Some(before.operation_id.as_str())
                || candidate_hash != before.candidate_secret_ref_hash
                || annotation(&state.resource.metadata.annotations, EXECUTION_ANNOTATION) != Some(execution_id.as_str())
                || annotation(&state.resource.metadata.annotations, PLAN_STEP_ANNOTATION) != Some(plan_step_id.as_str())
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            if !self
                .probe(
                    &request.credential_set,
                    &before.previous_active_version,
                    &before.previous_active_secret_ref,
                    target,
                )
                .await?
            {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let mut selector = state.resource;
            let annotations = selector.metadata.annotations.get_or_insert_with(BTreeMap::new);
            annotations.insert(
                ACTIVE_VERSION_ANNOTATION.to_owned(),
                before.previous_active_version.clone(),
            );
            annotations.insert(
                ACTIVE_SECRET_REF_ANNOTATION.to_owned(),
                before.previous_active_secret_ref.clone(),
            );
            annotations.remove(RETIRING_VERSION_ANNOTATION);
            annotations.remove(RETIRING_SECRET_REF_ANNOTATION);
            annotations.remove(OVERLAP_DEADLINE_ANNOTATION);
            annotations.insert(PROBE_HEALTHY_ANNOTATION.to_owned(), "false".to_owned());
            annotations.insert(OPERATION_ANNOTATION.to_owned(), request.operation_id.clone());
            annotations.insert(EXECUTION_ANNOTATION.to_owned(), execution_id);
            annotations.insert(PLAN_STEP_ANNOTATION.to_owned(), plan_step_id);
            let restored = self.replace_selector(target, selector).await?;
            if restored.uid != before.selector_uid
                || restored.active_version != before.previous_active_version
                || restored.active_secret_ref != before.previous_active_secret_ref
                || restored.retiring_version.is_some()
                || restored.retiring_secret_ref.is_some()
                || restored.overlap_deadline.is_some()
                || restored.last_operation_id.as_deref() != Some(request.operation_id.as_str())
            {
                return Err(ExecutionAgentError::DriverUnknown);
            }
            self.journal
                .append_result(
                    request.execution_id,
                    request.plan_step_id,
                    &CredentialResult {
                        credential_set: &request.credential_set,
                        operation_id: &request.operation_id,
                        direction: OperationDirection::Compensation,
                        active_version: &restored.active_version,
                        retiring_version: None,
                        overlap_deadline: None,
                        candidate_probe_healthy: false,
                        selector_resource_version: &restored.resource_version,
                    },
                    Utc::now(),
                )
                .await?;
            Ok(())
        })
    }
}

struct KubernetesSecretReference {
    namespace: String,
    name: String,
}

fn parse_secret_reference(value: &str) -> Result<KubernetesSecretReference, ExecutionAgentError> {
    let path = value
        .strip_prefix("kubernetes://")
        .ok_or(ExecutionAgentError::InvalidRequest)?;
    let (namespace, name) = path
        .split_once('/')
        .filter(|(namespace, name)| {
            !namespace.is_empty() && !name.is_empty() && !name.contains('/') && dns_name(namespace) && dns_name(name)
        })
        .ok_or(ExecutionAgentError::InvalidRequest)?;
    Ok(KubernetesSecretReference {
        namespace: namespace.to_owned(),
        name: name.to_owned(),
    })
}

fn parse_selector(selector: ConfigMap, credential_set: &str) -> Result<SelectorState, ExecutionAgentError> {
    let annotations = selector
        .metadata
        .annotations
        .as_ref()
        .ok_or(ExecutionAgentError::DriverFailed)?;
    if annotation(&selector.metadata.annotations, CREDENTIAL_SET_ANNOTATION) != Some(credential_set) {
        return Err(ExecutionAgentError::DriverFailed);
    }
    let active_version = required_annotation(annotations, ACTIVE_VERSION_ANNOTATION, 128)?;
    let active_secret_ref = required_annotation(annotations, ACTIVE_SECRET_REF_ANNOTATION, 255)?;
    parse_secret_reference(&active_secret_ref)?;
    let retiring_version = optional_annotation(annotations, RETIRING_VERSION_ANNOTATION, 128)?;
    let retiring_secret_ref = optional_annotation(annotations, RETIRING_SECRET_REF_ANNOTATION, 255)?;
    if retiring_version.is_some() != retiring_secret_ref.is_some() {
        return Err(ExecutionAgentError::DriverFailed);
    }
    if let Some(reference) = &retiring_secret_ref {
        parse_secret_reference(reference)?;
    }
    let overlap_deadline = optional_annotation(annotations, OVERLAP_DEADLINE_ANNOTATION, 64)?
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|value| value.with_timezone(&Utc))
                .map_err(|_| ExecutionAgentError::DriverFailed)
        })
        .transpose()?;
    if overlap_deadline.is_some() != retiring_version.is_some() {
        return Err(ExecutionAgentError::DriverFailed);
    }
    let candidate_probe_healthy = match annotation(&selector.metadata.annotations, PROBE_HEALTHY_ANNOTATION) {
        Some("true") => true,
        Some("false") | None => false,
        Some(_) => return Err(ExecutionAgentError::DriverFailed),
    };
    let last_operation_id = optional_annotation(annotations, OPERATION_ANNOTATION, 128)?;
    let uid = selector
        .metadata
        .uid
        .clone()
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .ok_or(ExecutionAgentError::DriverFailed)?;
    let resource_version = selector
        .metadata
        .resource_version
        .clone()
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .ok_or(ExecutionAgentError::DriverFailed)?;
    Ok(SelectorState {
        resource: selector,
        uid,
        resource_version,
        active_version,
        active_secret_ref,
        retiring_version,
        retiring_secret_ref,
        overlap_deadline,
        candidate_probe_healthy,
        last_operation_id,
    })
}

fn secret_string(
    data: &BTreeMap<String, k8s_openapi::ByteString>,
    key: &str,
    maximum: usize,
) -> Result<String, ExecutionAgentError> {
    data.get(key)
        .ok_or(ExecutionAgentError::DriverFailed)
        .and_then(|value| bounded_utf8(value.0.as_slice(), maximum))
}

fn bounded_utf8(value: &[u8], maximum: usize) -> Result<String, ExecutionAgentError> {
    if value.is_empty() || value.len() > maximum {
        return Err(ExecutionAgentError::DriverFailed);
    }
    String::from_utf8(value.to_vec())
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or(ExecutionAgentError::DriverFailed)
}

fn required_annotation(
    annotations: &BTreeMap<String, String>,
    key: &str,
    maximum: usize,
) -> Result<String, ExecutionAgentError> {
    optional_annotation(annotations, key, maximum)?.ok_or(ExecutionAgentError::DriverFailed)
}

fn optional_annotation(
    annotations: &BTreeMap<String, String>,
    key: &str,
    maximum: usize,
) -> Result<Option<String>, ExecutionAgentError> {
    annotations
        .get(key)
        .map(|value| {
            (!value.is_empty() && value.len() <= maximum)
                .then(|| value.clone())
                .ok_or(ExecutionAgentError::DriverFailed)
        })
        .transpose()
}

fn annotation<'a>(annotations: &'a Option<BTreeMap<String, String>>, key: &str) -> Option<&'a str> {
    annotations
        .as_ref()
        .and_then(|annotations| annotations.get(key))
        .map(String::as_str)
}

fn dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                && label
                    .as_bytes()
                    .first()
                    .zip(label.as_bytes().last())
                    .is_some_and(|(first, last)| {
                        (first.is_ascii_lowercase() || first.is_ascii_digit())
                            && (last.is_ascii_lowercase() || last.is_ascii_digit())
                    })
        })
}

#[cfg(test)]
#[path = "production_credential_rotation_tests.rs"]
mod tests;
