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

use super::*;
use rocketmq_admin_core::core::security::AdminCredentials;
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    path::PathBuf,
};

pub(super) type TestResult<T> = Result<T, &'static str>;
const RUN_LABEL: &str = "rocketmqrust.com/sre-drill-run";
const OWNED_KEYS: [&str; 7] = [
    OPERATION_ANNOTATION,
    LAST_OPERATION_ANNOTATION,
    EXECUTION_ANNOTATION,
    PLAN_STEP_ANNOTATION,
    ORIGINAL_POD_ANNOTATION,
    ORIGINAL_UID_ANNOTATION,
    STARTED_AT_ANNOTATION,
];

pub(super) struct Settings {
    pub(super) namespace: String,
    workload: String,
    container: String,
    deployment_uid: String,
    pod_uids: BTreeSet<String>,
    target_uid: String,
    image_id: String,
    pub(super) run_id: String,
    report_path: PathBuf,
    pub(super) admin: BrokerAdminDriverConfig,
    pub(super) restart: ProxyRestartDriverConfig,
    pub(super) dev_http: bool,
}

impl Settings {
    pub(super) fn from_env() -> TestResult<Self> {
        if required("ROCKETMQ_SRE_TEST_PROXY_RESTART_UNKNOWN", 1)? != "1" {
            return Err("explicit_real_proxy_restart_authorization_required");
        }
        for key in ["KUBECONFIG", "SSL_CERT_FILE"] {
            let path = PathBuf::from(required(key, 4096)?);
            if !path.is_absolute() || !path.is_file() {
                return Err("explicit_kubeconfig_and_task_ca_files_required");
            }
        }
        let namespace = required("ROCKETMQ_SRE_TEST_PROXY_NAMESPACE", 63)?;
        let workload = required("ROCKETMQ_SRE_TEST_PROXY_WORKLOAD", 63)?;
        let container = required("ROCKETMQ_SRE_TEST_PROXY_CONTAINER", 63)?;
        if ![&namespace, &workload, &container]
            .into_iter()
            .all(|value| dns_label(value))
            || ["default", "kube-system", "kube-public", "kube-node-lease"].contains(&namespace.as_str())
        {
            return Err("exact_dedicated_proxy_target_required");
        }
        let run_id = uuid_value("ROCKETMQ_SRE_TEST_PROXY_RUN_ID")?;
        let deployment_uid = uuid_value("ROCKETMQ_SRE_TEST_PROXY_DEPLOYMENT_UID")?;
        let target_uid = uuid_value("ROCKETMQ_SRE_TEST_PROXY_TARGET_POD_UID")?;
        let raw_uids: Vec<String> = serde_json::from_str(&required("ROCKETMQ_SRE_TEST_PROXY_POD_UIDS", 256)?)
            .map_err(|_| "exact_two_baseline_pod_uids_required")?;
        if raw_uids.len() != 2 || !raw_uids.iter().all(|value| canonical_uuid(value)) {
            return Err("exact_two_baseline_pod_uids_required");
        }
        let pod_uids = raw_uids.into_iter().collect::<BTreeSet<_>>();
        if pod_uids.len() != 2 || !pod_uids.contains(&target_uid) {
            return Err("target_must_be_one_of_two_exact_original_uids");
        }
        let report_path = PathBuf::from(required("ROCKETMQ_SRE_TEST_PROXY_PRIVATE_REPORT", 4096)?);
        if !report_path.is_absolute() {
            return Err("absolute_private_report_path_required");
        }
        let dev_http = match std::env::var("ROCKETMQ_SRE_TEST_PROXY_VERIFICATION_DEV_HTTP").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("false") => false,
            Ok("true") => true,
            _ => return Err("explicit_closed_verification_dev_http_flag_required"),
        };
        let verification_base_url: url::Url = required("ROCKETMQ_SRE_TEST_PROXY_VERIFICATION_URL", 2048)?
            .parse()
            .map_err(|_| "verification_url_invalid")?;
        if !matches!(verification_base_url.scheme(), "http" | "https")
            || (verification_base_url.scheme() == "http" && !dev_http)
            || verification_base_url.host_str().is_none()
            || !verification_base_url.username().is_empty()
            || verification_base_url.password().is_some()
            || verification_base_url.query().is_some()
            || verification_base_url.fragment().is_some()
            || !matches!(verification_base_url.path(), "" | "/")
        {
            return Err("exact_authorized_verification_service_required");
        }
        let read = AdminCredentials::try_new(
            required("ROCKETMQ_SRE_TEST_PROXY_READ_ACCESS_KEY", 256)?,
            required("ROCKETMQ_SRE_TEST_PROXY_READ_SECRET_KEY", 1024)?,
            None,
        )
        .map_err(|_| "read_credentials_invalid")?;
        let mutation = AdminCredentials::try_new(
            required("ROCKETMQ_SRE_TEST_PROXY_ACCESS_KEY", 256)?,
            required("ROCKETMQ_SRE_TEST_PROXY_SECRET_KEY", 1024)?,
            None,
        )
        .map_err(|_| "mutation_credentials_invalid")?;
        let tenant = uuid_value("ROCKETMQ_SRE_TEST_PROXY_TENANT_ID")?;
        let cluster = uuid_value("ROCKETMQ_SRE_TEST_PROXY_CLUSTER_ID")?;
        Ok(Self {
            admin: BrokerAdminDriverConfig {
                namesrv_addr: required("ROCKETMQ_SRE_TEST_NAMESRV_ADDR", 1024)?,
                use_tls: true,
                request_timeout: Duration::from_secs(10),
                shutdown_timeout: Duration::from_secs(10),
                read_credentials: Some(read),
                mutation_credentials: Some(mutation),
            },
            restart: ProxyRestartDriverConfig {
                targets: BTreeMap::from([(format!("{namespace}/{workload}"), 8080)]),
                verification_base_url,
                verification_token: required("ROCKETMQ_SRE_TEST_PROXY_VERIFICATION_TOKEN", 4096)?,
                tenant_id: tenant.parse().map_err(|_| "tenant_identity_invalid")?,
                cluster_id: cluster.parse().map_err(|_| "cluster_identity_invalid")?,
            },
            image_id: required("ROCKETMQ_SRE_TEST_PROXY_IMAGE_ID", 512)?,
            namespace,
            workload,
            container,
            deployment_uid,
            pod_uids,
            target_uid,
            run_id,
            report_path,
            dev_http,
        })
    }

    pub(super) fn open_report(&self) -> std::io::Result<File> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(&self.report_path)
    }

    fn verify_deployment(&self, deployment: &Deployment) -> TestResult<()> {
        if deployment.metadata.uid.as_deref() != Some(self.deployment_uid.as_str())
            || deployment.metadata.namespace.as_deref() != Some(self.namespace.as_str())
            || deployment.metadata.name.as_deref() != Some(self.workload.as_str())
            || deployment.metadata.deletion_timestamp.is_some()
            || deployment
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(RUN_LABEL))
                != Some(&self.run_id)
        {
            return Err("task_deployment_identity_mismatch");
        }
        let spec = deployment.spec.as_ref().ok_or("deployment_spec_missing")?;
        if spec.replicas != Some(2) || spec.paused == Some(true) {
            return Err("two_active_real_proxy_replicas_required");
        }
        let container = spec
            .template
            .spec
            .as_ref()
            .and_then(|spec| {
                spec.containers
                    .iter()
                    .find(|container| container.name == self.container)
            })
            .ok_or("declared_proxy_container_missing")?;
        if container
            .readiness_probe
            .as_ref()
            .and_then(|probe| probe.http_get.as_ref())
            .is_none()
        {
            return Err("real_proxy_health_probe_required");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct PodIdentity {
    pub(super) name: String,
    pub(super) uid: String,
    image_id: String,
}

pub(super) struct Baseline {
    pub(super) pods: Vec<PodIdentity>,
    spec: k8s_openapi::api::apps::v1::DeploymentSpec,
    labels: Option<BTreeMap<String, String>>,
    annotations: BTreeMap<String, String>,
}

impl Baseline {
    pub(super) async fn capture(driver: &ProductionProxyRestartClient, settings: &Settings) -> TestResult<Self> {
        let deployment = driver
            .deployment(&settings.namespace, &settings.workload)
            .await
            .map_err(|_| "deployment_read_failed")?;
        settings.verify_deployment(&deployment)?;
        if OWNED_KEYS.iter().any(|key| annotation(&deployment, key).is_some()) {
            return Err("preexisting_restart_operation_or_history_refused");
        }
        let pods = identities(driver, settings, &deployment).await?;
        if pods.iter().map(|pod| pod.uid.clone()).collect::<BTreeSet<_>>() != settings.pod_uids {
            return Err("baseline_proxy_uid_set_mismatch");
        }
        Ok(Self {
            pods,
            spec: deployment.spec.clone().ok_or("deployment_spec_missing")?,
            labels: deployment.metadata.labels.clone(),
            annotations: user_annotations(&deployment),
        })
    }

    pub(super) fn target(&self, settings: &Settings) -> TestResult<&PodIdentity> {
        self.pods
            .iter()
            .find(|pod| pod.uid == settings.target_uid)
            .ok_or("target_uid_missing")
    }

    pub(super) async fn current(
        &self,
        driver: &ProductionProxyRestartClient,
        settings: &Settings,
    ) -> TestResult<(Deployment, Vec<PodIdentity>)> {
        let deployment = driver
            .deployment(&settings.namespace, &settings.workload)
            .await
            .map_err(|_| "deployment_read_failed")?;
        self.verify_unchanged(&deployment, settings)?;
        let pods = identities(driver, settings, &deployment).await?;
        Ok((deployment, pods))
    }

    fn verify_unchanged(&self, deployment: &Deployment, settings: &Settings) -> TestResult<()> {
        settings.verify_deployment(deployment)?;
        if deployment.spec.as_ref() != Some(&self.spec)
            || deployment.metadata.labels != self.labels
            || user_annotations(deployment) != self.annotations
        {
            return Err("concurrent_fixture_spec_or_user_metadata_change");
        }
        Ok(())
    }

    pub(super) fn verify_owned(
        &self,
        deployment: &Deployment,
        settings: &Settings,
        request: &ProxyRestartOneWrite,
    ) -> TestResult<()> {
        self.verify_unchanged(deployment, settings)?;
        let execution = request.execution_id.to_string();
        let step = request.plan_step_id.to_string();
        for (key, expected) in [
            (OPERATION_ANNOTATION, request.operation_id.as_str()),
            (EXECUTION_ANNOTATION, execution.as_str()),
            (PLAN_STEP_ANNOTATION, step.as_str()),
            (ORIGINAL_POD_ANNOTATION, request.pod.as_str()),
            (ORIGINAL_UID_ANNOTATION, request.expected_uid.as_str()),
        ] {
            if annotation(deployment, key) != Some(expected) {
                return Err("original_operation_binding_changed");
            }
        }
        if annotation(deployment, LAST_OPERATION_ANNOTATION).is_some()
            || annotation(deployment, STARTED_AT_ANNOTATION)
                .is_none_or(|value| chrono::DateTime::parse_from_rfc3339(value).is_err())
        {
            return Err("unknown_operation_must_not_be_completed");
        }
        Ok(())
    }

    pub(super) fn verify_one_replacement(&self, settings: &Settings, pods: &[PodIdentity]) -> TestResult<()> {
        let original = self.target(settings)?;
        let survivor = self
            .pods
            .iter()
            .find(|pod| pod.uid != original.uid)
            .ok_or("original_peer_missing")?;
        if pods.len() != 2
            || !pods.contains(survivor)
            || pods.iter().any(|pod| pod.uid == original.uid)
            || pods.iter().filter(|pod| !settings.pod_uids.contains(&pod.uid)).count() != 1
        {
            return Err("exact_one_replacement_and_original_healthy_peer_required");
        }
        Ok(())
    }

    pub(super) fn verify_restored(&self, deployment: &Deployment, settings: &Settings) -> TestResult<()> {
        self.verify_unchanged(deployment, settings)?;
        if OWNED_KEYS.iter().any(|key| annotation(deployment, key).is_some()) {
            return Err("operator_cleanup_did_not_restore_original_metadata");
        }
        Ok(())
    }

    pub(super) async fn operator_cleanup(
        &self,
        driver: &ProductionProxyRestartClient,
        settings: &Settings,
        request: &ProxyRestartOneWrite,
    ) -> TestResult<bool> {
        let mut deployment = driver
            .deployment(&settings.namespace, &settings.workload)
            .await
            .map_err(|_| "cleanup_read_failed")?;
        self.verify_unchanged(&deployment, settings)?;
        if OWNED_KEYS.iter().all(|key| annotation(&deployment, key).is_none()) {
            return Ok(false);
        }
        self.verify_owned(&deployment, settings, request)?;
        let original = Api::<Pod>::namespaced(driver.kube.clone(), &settings.namespace)
            .get_opt(&request.pod)
            .await
            .map_err(|_| "cleanup_original_pod_read_failed")?;
        if original.is_some() {
            let result = driver
                .cancel_restart_and_restore(&ProxyRestartRestore {
                    namespace: request.namespace.clone(),
                    pod: request.pod.clone(),
                    expected_uid: request.expected_uid.clone(),
                    operation_id: request.operation_id.clone(),
                    execution_id: request.execution_id,
                    plan_step_id: request.plan_step_id,
                })
                .await
                .map_err(|_| "cleanup_original_ingress_restore_failed")?;
            return match result {
                ProxyRestartRestoreOutcome::IngressRestored => Ok(false),
                ProxyRestartRestoreOutcome::ManualTakeoverRequired => {
                    Err("owner_inspection_required_for_original_name_reuse")
                }
            };
        }
        let pods = identities(driver, settings, &deployment).await?;
        self.verify_one_replacement(settings, &pods)?;
        for pod in &pods {
            let raw = Api::<Pod>::namespaced(driver.kube.clone(), &settings.namespace)
                .get(&pod.name)
                .await
                .map_err(|_| "operator_cleanup_pod_read_failed")?;
            if raw.metadata.uid.as_deref() != Some(pod.uid.as_str()) {
                return Err("operator_cleanup_pod_identity_changed");
            }
            let drain = driver
                .query_drain(&proxy_addr(&raw, 8080).map_err(|_| "operator_cleanup_address_missing")?)
                .await
                .map_err(|_| "operator_cleanup_drain_read_failed")?;
            if !accepting(&drain) {
                return Err("operator_cleanup_requires_ready_accepting_proxies");
            }
        }
        let annotations = deployment
            .metadata
            .annotations
            .as_mut()
            .ok_or("owned_annotations_missing")?;
        for key in OWNED_KEYS {
            annotations.remove(key);
        }
        // Retain the read UID/resourceVersion. replace is an operator CAS, not
        // an adapter success receipt and never a second Pod deletion.
        let stored = Api::<Deployment>::namespaced(driver.kube.clone(), &settings.namespace)
            .replace(&settings.workload, &PostParams::default(), &deployment)
            .await
            .map_err(|_| "operator_cleanup_cas_failed")?;
        self.verify_restored(&stored, settings)?;
        Ok(true)
    }
}

async fn identities(
    driver: &ProductionProxyRestartClient,
    settings: &Settings,
    deployment: &Deployment,
) -> TestResult<Vec<PodIdentity>> {
    let pods = Api::<Pod>::namespaced(driver.kube.clone(), &settings.namespace)
        .list(&ListParams::default().limit(16))
        .await
        .map_err(|_| "pod_list_failed")?;
    if pods
        .metadata
        .continue_
        .as_ref()
        .is_some_and(|cursor| !cursor.is_empty())
    {
        return Err("dedicated_namespace_pod_list_exceeded_bound");
    }
    let selector = &deployment.spec.as_ref().ok_or("deployment_spec_missing")?.selector;
    let selected = pods
        .items
        .into_iter()
        .filter(|pod| selector_matches(selector, pod.metadata.labels.as_ref()))
        .collect::<Vec<_>>();
    if selected.len() != 2 {
        return Err("exact_two_ready_real_proxies_required");
    }
    let mut identities = Vec::new();
    for pod in selected {
        if pod.metadata.deletion_timestamp.is_some() || !pod_ready(&pod) {
            return Err("proxy_pod_not_ready");
        }
        let replica_set = controller_owner_name(&pod).ok_or("proxy_controller_missing")?;
        let replica_set = Api::<ReplicaSet>::namespaced(driver.kube.clone(), &settings.namespace)
            .get(replica_set)
            .await
            .map_err(|_| "replica_set_read_failed")?;
        if pod.metadata.owner_references.as_ref().is_none_or(|owners| {
            !owners.iter().any(|owner| {
                owner.controller == Some(true)
                    && owner.kind == "ReplicaSet"
                    && Some(owner.uid.as_str()) == replica_set.metadata.uid.as_deref()
            })
        }) {
            return Err("pod_controller_replica_set_uid_mismatch");
        }
        if replica_set.metadata.owner_references.as_ref().is_none_or(|owners| {
            !owners.iter().any(|owner| {
                owner.controller == Some(true) && owner.kind == "Deployment" && owner.uid == settings.deployment_uid
            })
        }) {
            return Err("pod_controller_deployment_uid_mismatch");
        }
        let status = pod
            .status
            .as_ref()
            .and_then(|status| status.container_statuses.as_ref())
            .and_then(|statuses| statuses.iter().find(|status| status.name == settings.container))
            .ok_or("proxy_container_status_missing")?;
        if !status.ready || status.image_id != settings.image_id {
            return Err("actual_proxy_image_identity_mismatch");
        }
        identities.push(PodIdentity {
            name: pod.metadata.name.clone().ok_or("pod_name_missing")?,
            uid: pod
                .metadata
                .uid
                .clone()
                .filter(|uid| canonical_uuid(uid))
                .ok_or("pod_uid_invalid")?,
            image_id: status.image_id.clone(),
        });
    }
    identities.sort_by(|a, b| a.uid.cmp(&b.uid));
    Ok(identities)
}

fn user_annotations(deployment: &Deployment) -> BTreeMap<String, String> {
    deployment
        .metadata
        .annotations
        .clone()
        .unwrap_or_default()
        .into_iter()
        .filter(|(key, _)| !OWNED_KEYS.contains(&key.as_str()))
        .collect()
}

fn required(key: &str, maximum: usize) -> TestResult<String> {
    std::env::var(key)
        .ok()
        .filter(|value| {
            !value.is_empty() && value.len() <= maximum && value.trim() == value && !value.chars().any(char::is_control)
        })
        .ok_or("required_private_fixture_setting_missing_or_invalid")
}

fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|uuid| !uuid.is_nil() && uuid.to_string() == value)
}

fn uuid_value(key: &str) -> TestResult<String> {
    let value = required(key, 36)?;
    canonical_uuid(&value)
        .then_some(value)
        .ok_or("exact_canonical_fixture_uuid_required")
}

fn dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && value.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
}
