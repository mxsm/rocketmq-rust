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

//! Explicitly authorized, disposable real-Collector adapter gate. Private
//! environment values identify an existing task Deployment; this test never
//! creates a substitute workload. The report belongs in private task state.

use super::super::*;
use chrono::Utc;
use rocketmq_sre_contracts::{ExecutionId, PlanStepId};
use serde::Serialize;
use std::{future::Future, time::Duration};
use tokio::time::{Instant, MissedTickBehavior};

#[path = "live/fixture.rs"]
mod fixture;
use fixture::{Baseline, Identity, Settings, TestResult};

const READ_TIMEOUT: Duration = Duration::from_secs(10);
const ROLLOUT_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Serialize)]
struct PrivateReport {
    schema_version: &'static str,
    evidence_class: &'static str,
    execution_path: &'static str,
    run_id: String,
    started_at: chrono::DateTime<Utc>,
    finished_at: Option<chrono::DateTime<Utc>>,
    baseline: Option<Identity>,
    replacement: Option<Identity>,
    restored: Option<Identity>,
    operation_id: String,
    execution_id: ExecutionId,
    plan_step_id: PlanStepId,
    pipeline: String,
    adapter_request_started: bool,
    adapter_request_returned: bool,
    cleanup_template_write: bool,
    metadata_and_resources_restored: bool,
    exercise_failure: Option<&'static str>,
    cleanup_failure: Option<&'static str>,
    pipeline_observation: &'static str,
    queue_observation: &'static str,
    exporter_observation: &'static str,
    business_recovery: &'static str,
    observed_software_version: Option<&'static str>,
    production_qualified: bool,
}

/// Required private settings include exact namespace/workload/pipeline,
/// Deployment and original Pod UIDs, container/imageID, a task-run annotation,
/// KUBECONFIG and a new absolute private report path. No defaults select a target.
///
/// Removing our restart annotations restores the original Pod template and can
/// cause a second rollout. Pod UID, generation, resourceVersion and the server's
/// revision counter are not reversible. Only our four annotations are removed;
/// unrelated drift refuses cleanup and requires the fixture owner's inspection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicit task-owned real Collector authorization, exact identities, and a private report path"]
async fn real_collector_adapter_rollout_binds_one_task_target_and_restores_its_template() {
    let settings = Settings::from_env().expect("explicit, bounded disposable Collector settings are required");
    let mut output = settings
        .open_report()
        .expect("a new private report file is required before any mutation");
    let execution_id = ExecutionId::new();
    let plan_step_id = PlanStepId::new();
    let mut report = PrivateReport {
        schema_version: "rocketmq-sre.collector-direct-adapter-drill.v1",
        evidence_class: "disposable_target_driver",
        execution_path: "direct_adapter",
        run_id: settings.run_id.clone(),
        started_at: Utc::now(),
        finished_at: None,
        baseline: None,
        replacement: None,
        restored: None,
        operation_id: format!("collector-drill-{}", settings.run_id),
        execution_id,
        plan_step_id,
        pipeline: settings.pipeline.clone(),
        adapter_request_started: false,
        adapter_request_returned: false,
        cleanup_template_write: false,
        metadata_and_resources_restored: false,
        exercise_failure: None,
        cleanup_failure: None,
        pipeline_observation: "not_observed",
        queue_observation: "not_observed",
        exporter_observation: "not_observed",
        business_recovery: "not_observed",
        observed_software_version: None,
        production_qualified: false,
    };
    if let Err(reason) = run_case(&settings, &mut report).await {
        report.exercise_failure.get_or_insert(reason);
    }
    report.finished_at = Some(Utc::now());
    serde_json::to_writer_pretty(&mut output, &report).expect("write bounded private identity/cleanup report");
    output.sync_all().expect("flush private report");
    assert!(
        report.exercise_failure.is_none() && report.cleanup_failure.is_none() && report.metadata_and_resources_restored,
        "Collector direct-adapter drill failed: exercise={:?}, cleanup={:?}; inspect its private report",
        report.exercise_failure,
        report.cleanup_failure,
    );
}

async fn run_case(settings: &Settings, report: &mut PrivateReport) -> TestResult<()> {
    let driver = bounded(
        ProductionTelemetryCollectorRestartClient::start(BTreeSet::from([settings.target()])),
        "kubernetes_client_start_failed",
    )
    .await?;
    let baseline = bounded(Baseline::capture(&driver, settings), "baseline_read_failed").await?;
    report.baseline = Some(baseline.identity.clone());
    let request = TelemetryCollectorRestartOneWrite {
        namespace: settings.namespace.clone(),
        pod: baseline.identity.pod_name.clone(),
        expected_uid: baseline.identity.pod_uid.clone(),
        pipeline: settings.pipeline.clone(),
        operation_id: report.operation_id.clone(),
        execution_id: report.execution_id,
        plan_step_id: report.plan_step_id,
    };
    report.adapter_request_started = true;
    let exercise = async {
        bounded(
            driver.restart_one_telemetry_collector(&request),
            "adapter_request_failed",
        )
        .await?;
        report.adapter_request_returned = true;
        let identity = await_replacement(&driver, settings, &baseline, &request).await?;
        report.replacement = Some(identity);
        Ok::<(), &'static str>(())
    }
    .await;
    report.exercise_failure = exercise.err();

    // The adapter call may have reached Kubernetes even when its waiter failed.
    // Always inspect ownership and attempt exact guarded cleanup after that call.
    let cleanup = async {
        report.cleanup_template_write =
            bounded(baseline.restore(&driver, settings, &request), "cleanup_request_failed").await?;
        report.restored = Some(await_restored(&driver, settings, &baseline).await?);
        report.metadata_and_resources_restored = true;
        Ok::<(), &'static str>(())
    }
    .await;
    report.cleanup_failure = cleanup.err();
    Ok(())
}

async fn await_replacement(
    driver: &ProductionTelemetryCollectorRestartClient,
    settings: &Settings,
    baseline: &Baseline,
    request: &TelemetryCollectorRestartOneWrite,
) -> TestResult<Identity> {
    let deadline = Instant::now() + ROLLOUT_TIMEOUT;
    let mut ticks = tokio::time::interval(Duration::from_millis(250));
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let execution_id = request.execution_id.to_string();
    let plan_step_id = request.plan_step_id.to_string();
    loop {
        let poll = async {
            ticks.tick().await;
            let deployment = driver
                .deployment(&settings.namespace, &settings.workload)
                .await
                .map_err(|_| "deployment_read_failed")?;
            baseline.verify_owned_rollout(&deployment, settings, request)?;
            let pods = driver
                .deployment_pods(&settings.namespace, &deployment)
                .await
                .map_err(|_| "pods_read_failed")?;
            let Some(identity) = settings.ready_identity(&deployment, &pods)? else {
                return Ok(None);
            };
            if identity.pod_uid == baseline.identity.pod_uid {
                return Ok(None);
            }
            fixture::verify_pod_binding(&pods[0], request)?;
            let state = driver
                .telemetry_collector_restart_state(&settings.namespace, &request.pod, &settings.pipeline)
                .await
                .map_err(|_| "adapter_state_failed")?;
            if state.pod_uid != identity.pod_uid
                || state.active_pod != identity.pod_name
                || !state.pod_ready
                || !state.deployment_ready
                || !state.replacement_ready
                || state.last_operation_id.as_deref() != Some(request.operation_id.as_str())
                || state.last_execution_id.as_deref() != Some(execution_id.as_str())
                || state.last_plan_step_id.as_deref() != Some(plan_step_id.as_str())
            {
                return Err("replacement_operation_binding_invalid");
            }
            // exporter_connected/queue_healthy/data_plane_unaffected are derived
            // adapter fields, not independent pipeline or business measurements.
            Ok(Some(identity))
        };
        if let Some(identity) = tokio::time::timeout_at(deadline, poll)
            .await
            .map_err(|_| "replacement_deadline")??
        {
            return Ok(identity);
        }
    }
}

async fn await_restored(
    driver: &ProductionTelemetryCollectorRestartClient,
    settings: &Settings,
    baseline: &Baseline,
) -> TestResult<Identity> {
    let deadline = Instant::now() + ROLLOUT_TIMEOUT;
    let mut ticks = tokio::time::interval(Duration::from_millis(250));
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let poll = async {
            ticks.tick().await;
            let deployment = driver
                .deployment(&settings.namespace, &settings.workload)
                .await
                .map_err(|_| "deployment_read_failed")?;
            baseline.verify_restored(&deployment, settings)?;
            let pods = driver
                .deployment_pods(&settings.namespace, &deployment)
                .await
                .map_err(|_| "pods_read_failed")?;
            settings.ready_identity(&deployment, &pods)
        };
        if let Some(identity) = tokio::time::timeout_at(deadline, poll)
            .await
            .map_err(|_| "cleanup_ready_deadline")??
        {
            return Ok(identity);
        }
    }
}

async fn bounded<T, E>(future: impl Future<Output = Result<T, E>>, reason: &'static str) -> TestResult<T> {
    tokio::time::timeout(READ_TIMEOUT, future)
        .await
        .map_err(|_| reason)?
        .map_err(|_| reason)
}
