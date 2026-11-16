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
use k8s_openapi::api::apps::v1::DeploymentSpec;
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

pub(super) type TestResult<T> = Result<T, &'static str>;
const TASK_MARKER: &str = "rocketmqrust.com/sre-drill-run";
const SERVER_REVISION: &str = "deployment.kubernetes.io/revision";
const OWNED_KEYS: [&str; 4] = [
    OPERATION_ANNOTATION,
    EXECUTION_ANNOTATION,
    PLAN_STEP_ANNOTATION,
    EXPECTED_UID_ANNOTATION,
];

pub(super) struct Settings {
    pub(super) namespace: String,
    pub(super) workload: String,
    pub(super) pipeline: String,
    pub(super) run_id: String,
    container: String,
    deployment_uid: String,
    original_pod_uid: String,
    expected_image_id: String,
    report_path: PathBuf,
}

impl Settings {
    pub(super) fn from_env() -> TestResult<Self> {
        if required("ROCKETMQ_SRE_TEST_COLLECTOR_RESTART", 1)? != "1" {
            return Err("explicit_collector_test_authorization_required");
        }
        let kubeconfig = PathBuf::from(required("KUBECONFIG", 4096)?);
        if !kubeconfig.is_absolute() || !kubeconfig.is_file() {
            return Err("explicit_kubeconfig_file_required");
        }
        let namespace = required("ROCKETMQ_SRE_TEST_COLLECTOR_NAMESPACE", 63)?;
        let workload = required("ROCKETMQ_SRE_TEST_COLLECTOR_WORKLOAD", 253)?;
        let container = required("ROCKETMQ_SRE_TEST_COLLECTOR_CONTAINER", 63)?;
        if !dns_name(&namespace)
            || !dns_name(&workload)
            || !dns_name(&container)
            || ["default", "kube-system", "kube-public", "kube-node-lease"].contains(&namespace.as_str())
        {
            return Err("dedicated_exact_collector_target_required");
        }
        let pipeline = required("ROCKETMQ_SRE_TEST_COLLECTOR_PIPELINE", 16)?;
        if !["metrics", "logs", "traces", "combined"].contains(&pipeline.as_str()) {
            return Err("closed_pipeline_required");
        }
        let run_id = required("ROCKETMQ_SRE_TEST_COLLECTOR_RUN_ID", 36)?;
        let parsed = uuid::Uuid::parse_str(&run_id).map_err(|_| "task_run_id_invalid")?;
        if parsed.is_nil() || parsed.to_string() != run_id {
            return Err("task_run_id_invalid");
        }
        let report_path = PathBuf::from(required("ROCKETMQ_SRE_TEST_COLLECTOR_PRIVATE_REPORT", 4096)?);
        if !report_path.is_absolute() {
            return Err("absolute_private_report_required");
        }
        Ok(Self {
            namespace,
            workload,
            pipeline,
            run_id,
            container,
            deployment_uid: required("ROCKETMQ_SRE_TEST_COLLECTOR_DEPLOYMENT_UID", 128)?,
            original_pod_uid: required("ROCKETMQ_SRE_TEST_COLLECTOR_POD_UID", 128)?,
            expected_image_id: required("ROCKETMQ_SRE_TEST_COLLECTOR_IMAGE_ID", 512)?,
            report_path,
        })
    }

    pub(super) fn target(&self) -> String {
        format!("{}/{}", self.namespace, self.workload)
    }

    pub(super) fn open_report(&self) -> std::io::Result<File> {
        // Exclusive creation preserves an existing input, prior report or alias.
        OpenOptions::new().write(true).create_new(true).open(&self.report_path)
    }

    fn verify_target(&self, deployment: &Deployment) -> TestResult<()> {
        if deployment.metadata.name.as_deref() != Some(self.workload.as_str())
            || deployment.metadata.namespace.as_deref() != Some(self.namespace.as_str())
            || deployment.metadata.uid.as_deref() != Some(self.deployment_uid.as_str())
            || deployment.metadata.deletion_timestamp.is_some()
            || deployment
                .metadata
                .annotations
                .as_ref()
                .and_then(|map| map.get(TASK_MARKER))
                != Some(&self.run_id)
        {
            return Err("task_deployment_identity_mismatch");
        }
        Ok(())
    }

    pub(super) fn ready_identity(&self, deployment: &Deployment, pods: &[Pod]) -> TestResult<Option<Identity>> {
        self.verify_target(deployment)?;
        let spec = deployment.spec.as_ref().ok_or("deployment_spec_missing")?;
        if spec.replicas != Some(1) || spec.paused == Some(true) {
            return Err("single_active_collector_required");
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
            .ok_or("declared_collector_container_missing")?;
        if container
            .readiness_probe
            .as_ref()
            .and_then(|probe| probe.http_get.as_ref())
            .is_none()
        {
            return Err("collector_http_health_probe_required");
        }
        if !deployment_ready(deployment).map_err(|_| "deployment_status_invalid")? {
            return Ok(None);
        }
        // Do not count a surge or a still-terminating old pod as a completed drain.
        if pods.len() != 1 || pods[0].metadata.deletion_timestamp.is_some() || !pod_ready(&pods[0]) {
            return Ok(None);
        }
        let pod = &pods[0];
        if pod.metadata.namespace.as_deref() != Some(self.namespace.as_str())
            || !pod_matches_deployment(pod, deployment).map_err(|_| "pod_selector_invalid")?
        {
            return Err("pod_target_binding_invalid");
        }
        let status = pod
            .status
            .as_ref()
            .and_then(|status| status.container_statuses.as_ref())
            .and_then(|statuses| statuses.iter().find(|status| status.name == self.container))
            .ok_or("collector_container_status_missing")?;
        if !status.ready || status.image_id != self.expected_image_id {
            return Err("collector_image_identity_mismatch");
        }
        let pod_uid = pod
            .metadata
            .uid
            .clone()
            .filter(|value| !value.is_empty())
            .ok_or("pod_uid_missing")?;
        let pod_name = pod
            .metadata
            .name
            .clone()
            .filter(|value| !value.is_empty())
            .ok_or("pod_name_missing")?;
        Ok(Some(Identity {
            deployment_uid: self.deployment_uid.clone(),
            pod_uid,
            pod_name,
            image_id: status.image_id.clone(),
        }))
    }
}

#[derive(Clone, Serialize)]
pub(super) struct Identity {
    pub(super) deployment_uid: String,
    pub(super) pod_uid: String,
    pub(super) pod_name: String,
    pub(super) image_id: String,
}

pub(super) struct Baseline {
    pub(super) identity: Identity,
    spec: DeploymentSpec,
    labels: Option<BTreeMap<String, String>>,
    annotations: BTreeMap<String, String>,
}

impl Baseline {
    pub(super) async fn capture(
        driver: &ProductionTelemetryCollectorRestartClient,
        settings: &Settings,
    ) -> TestResult<Self> {
        let deployment = driver
            .deployment(&settings.namespace, &settings.workload)
            .await
            .map_err(|_| "baseline_deployment_failed")?;
        let pods = driver
            .deployment_pods(&settings.namespace, &deployment)
            .await
            .map_err(|_| "baseline_pods_failed")?;
        let identity = settings
            .ready_identity(&deployment, &pods)?
            .ok_or("baseline_not_ready_and_single")?;
        if identity.pod_uid != settings.original_pod_uid {
            return Err("original_pod_identity_mismatch");
        }
        if OWNED_KEYS
            .iter()
            .any(|key| template_annotation(&deployment, key).is_some())
        {
            return Err("preexisting_restart_operation_refused");
        }
        Ok(Self {
            identity,
            spec: deployment.spec.clone().ok_or("deployment_spec_missing")?,
            labels: deployment.metadata.labels.clone(),
            annotations: operator_annotations(&deployment),
        })
    }

    fn verify_metadata(&self, deployment: &Deployment, settings: &Settings) -> TestResult<()> {
        settings.verify_target(deployment)?;
        if deployment.metadata.labels != self.labels || operator_annotations(deployment) != self.annotations {
            return Err("concurrent_operator_metadata_change");
        }
        Ok(())
    }

    fn restored_spec(&self, deployment: &Deployment) -> TestResult<DeploymentSpec> {
        let mut spec = deployment.spec.clone().ok_or("deployment_spec_missing")?;
        let meta = spec.template.metadata.as_mut().ok_or("template_metadata_missing")?;
        if let Some(annotations) = &mut meta.annotations {
            for key in OWNED_KEYS {
                annotations.remove(key);
            }
            if annotations.is_empty()
                && self
                    .spec
                    .template
                    .metadata
                    .as_ref()
                    .is_none_or(|meta| meta.annotations.is_none())
            {
                meta.annotations = None;
            }
        }
        if spec != self.spec {
            return Err("concurrent_spec_change_cleanup_refused");
        }
        Ok(spec)
    }

    pub(super) fn verify_owned_rollout(
        &self,
        deployment: &Deployment,
        settings: &Settings,
        request: &TelemetryCollectorRestartOneWrite,
    ) -> TestResult<()> {
        self.verify_metadata(deployment, settings)?;
        let execution_id = request.execution_id.to_string();
        let plan_step_id = request.plan_step_id.to_string();
        for (key, expected) in [
            (OPERATION_ANNOTATION, request.operation_id.as_str()),
            (EXECUTION_ANNOTATION, execution_id.as_str()),
            (PLAN_STEP_ANNOTATION, plan_step_id.as_str()),
            (EXPECTED_UID_ANNOTATION, request.expected_uid.as_str()),
        ] {
            if template_annotation(deployment, key) != Some(expected) {
                return Err("restart_operation_binding_mismatch");
            }
        }
        self.restored_spec(deployment)?;
        Ok(())
    }

    pub(super) fn verify_restored(&self, deployment: &Deployment, settings: &Settings) -> TestResult<()> {
        self.verify_metadata(deployment, settings)?;
        if deployment.spec.as_ref() != Some(&self.spec) {
            return Err("baseline_template_not_restored");
        }
        Ok(())
    }

    pub(super) async fn restore(
        &self,
        driver: &ProductionTelemetryCollectorRestartClient,
        settings: &Settings,
        request: &TelemetryCollectorRestartOneWrite,
    ) -> TestResult<bool> {
        let mut deployment = driver
            .deployment(&settings.namespace, &settings.workload)
            .await
            .map_err(|_| "cleanup_deployment_failed")?;
        self.verify_metadata(&deployment, settings)?;
        if deployment.spec.as_ref() == Some(&self.spec) {
            return Ok(false);
        }
        // resourceVersion on replace guards any race after this exact ownership check.
        self.verify_owned_rollout(&deployment, settings, request)?;
        deployment.spec = Some(self.restored_spec(&deployment)?);
        let restored = Api::<Deployment>::namespaced(driver.client.clone(), &settings.namespace)
            .replace(&settings.workload, &PostParams::default(), &deployment)
            .await
            .map_err(|_| "cleanup_cas_failed")?;
        self.verify_restored(&restored, settings)?;
        Ok(true)
    }
}

fn operator_annotations(deployment: &Deployment) -> BTreeMap<String, String> {
    deployment
        .metadata
        .annotations
        .clone()
        .unwrap_or_default()
        .into_iter()
        .filter(|(key, _)| key != SERVER_REVISION)
        .collect()
}

pub(super) fn verify_pod_binding(pod: &Pod, request: &TelemetryCollectorRestartOneWrite) -> TestResult<()> {
    let execution_id = request.execution_id.to_string();
    let plan_step_id = request.plan_step_id.to_string();
    for (key, expected) in [
        (OPERATION_ANNOTATION, request.operation_id.as_str()),
        (EXECUTION_ANNOTATION, execution_id.as_str()),
        (PLAN_STEP_ANNOTATION, plan_step_id.as_str()),
        (EXPECTED_UID_ANNOTATION, request.expected_uid.as_str()),
    ] {
        if pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(key))
            .map(String::as_str)
            != Some(expected)
        {
            return Err("replacement_pod_operation_binding_invalid");
        }
    }
    Ok(())
}

fn required(name: &'static str, limit: usize) -> TestResult<String> {
    std::env::var(name)
        .ok()
        .filter(|value| {
            !value.is_empty() && value.len() <= limit && value.trim() == value && !value.chars().any(char::is_control)
        })
        .ok_or("explicit_bounded_test_setting_missing")
}

fn dns_name(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.'))
        && value.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && value.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
}
