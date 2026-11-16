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

//! An explicitly authorized real-Proxy mutation with an expected Unknown
//! result from the normal Control Plane. It never supplies a successful SLI
//! response or treats replacement readiness as business recovery.

use super::super::*;
use crate::drivers::{AgentActionHandler, ProxyRestartOneHandler};
use rocketmq_runtime::RuntimeContext;
use rocketmq_sre_contracts::{
    AgentReadRequest, EXECUTION_AGENT_SCHEMA_VERSION, ExecutionAction, ExecutionId, ExecutionObservationReason,
    PlanStepId, ReconcileEffectState,
};
use serde::Serialize;
use std::future::Future;

#[path = "live_unknown/fixture.rs"]
mod fixture;
use fixture::{Baseline, PodIdentity, Settings, TestResult};

const READ_TIMEOUT: Duration = Duration::from_secs(15);
const MUTATION_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Serialize)]
struct PrivateReport {
    schema_version: &'static str,
    evidence_class: &'static str,
    execution_path: &'static str,
    started_at: chrono::DateTime<Utc>,
    finished_at: Option<chrono::DateTime<Utc>>,
    run_id: String,
    operation_id: String,
    execution_id: ExecutionId,
    plan_step_id: PlanStepId,
    baseline: Vec<PodIdentity>,
    replacement: Vec<PodIdentity>,
    restored: Vec<PodIdentity>,
    adapter_started: bool,
    adapter_returned: bool,
    incomplete_normal_source_before: bool,
    incomplete_normal_source_after: bool,
    original_operation_retained: bool,
    reconcile_unknown: bool,
    retry_refused_without_another_replacement: bool,
    original_uid_restore_requires_manual_takeover: bool,
    operator_metadata_cleanup_written: bool,
    operator_metadata_and_resources_restored: bool,
    runtime_shutdown_healthy: bool,
    exercise_failure: Option<&'static str>,
    cleanup_failure: Option<&'static str>,
    verification: &'static str,
    business_recovery: &'static str,
    observed_software_version: Option<&'static str>,
    production_qualified: bool,
}

/// Uses an existing, exact two-Proxy task Deployment. The selected Pod is
/// intentionally replaced once and its UID cannot be restored. Cleanup is a
/// separate task-operator CAS of owned annotations, never compensation success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires two authorized real TLS Proxies, exact identities, normal CP, and a private report"]
async fn real_proxy_restart_preserves_unknown_and_manual_takeover_after_one_replacement() {
    let settings = Settings::from_env().expect("explicit bounded real-Proxy drill settings are required");
    let mut output = settings
        .open_report()
        .expect("a new private report is required before mutation");
    let mut report = PrivateReport {
        schema_version: "rocketmq-sre.proxy-restart-unknown-drill.v1",
        evidence_class: "disposable_target_driver",
        execution_path: "direct_adapter",
        started_at: Utc::now(),
        finished_at: None,
        run_id: settings.run_id.clone(),
        operation_id: format!("proxy-unknown-{}", settings.run_id),
        execution_id: ExecutionId::new(),
        plan_step_id: PlanStepId::new(),
        baseline: Vec::new(),
        replacement: Vec::new(),
        restored: Vec::new(),
        adapter_started: false,
        adapter_returned: false,
        incomplete_normal_source_before: false,
        incomplete_normal_source_after: false,
        original_operation_retained: false,
        reconcile_unknown: false,
        retry_refused_without_another_replacement: false,
        original_uid_restore_requires_manual_takeover: false,
        operator_metadata_cleanup_written: false,
        operator_metadata_and_resources_restored: false,
        runtime_shutdown_healthy: false,
        exercise_failure: None,
        cleanup_failure: None,
        verification: "unknown_exact_sli_source_missing",
        business_recovery: "not_observed",
        observed_software_version: None,
        production_qualified: false,
    };
    let runtime = RuntimeContext::from_current("real-proxy-restart-unknown-drill");
    if let Err(reason) = run_case(&settings, &runtime, &mut report).await {
        report.exercise_failure.get_or_insert(reason);
    }
    report.runtime_shutdown_healthy = runtime.shutdown_tasks(Duration::from_secs(15)).await.is_healthy();
    report.finished_at = Some(Utc::now());
    serde_json::to_writer_pretty(&mut output, &report).expect("write private bounded drill report");
    output.sync_all().expect("flush private drill report");
    assert!(
        report.exercise_failure.is_none()
            && report.cleanup_failure.is_none()
            && report.operator_metadata_and_resources_restored
            && report.runtime_shutdown_healthy,
        "real Proxy Unknown/manual drill failed: exercise={:?}, cleanup={:?}; inspect private report",
        report.exercise_failure,
        report.cleanup_failure,
    );
}

async fn run_case(settings: &Settings, runtime: &RuntimeContext, report: &mut PrivateReport) -> TestResult<()> {
    let driver = Arc::new(
        read(
            ProductionProxyRestartClient::start(
                &settings.admin,
                &settings.restart,
                Duration::from_secs(10),
                settings.dev_http,
                runtime.service_context("proxy-unknown-client"),
            ),
            "production_client_start_failed",
        )
        .await?
        .map_err(|_| "production_client_start_failed")?,
    );
    let result = exercise_and_cleanup(settings, &driver, report).await;
    driver.shutdown().await;
    result
}

async fn exercise_and_cleanup(
    settings: &Settings,
    driver: &Arc<ProductionProxyRestartClient>,
    report: &mut PrivateReport,
) -> TestResult<()> {
    let baseline = read(Baseline::capture(driver, settings), "baseline_read_failed").await??;
    report.baseline = baseline.pods.clone();
    normal_source_is_incomplete(driver).await?;
    report.incomplete_normal_source_before = true;
    let original = baseline.target(settings)?;
    let request = ProxyRestartOneWrite {
        namespace: settings.namespace.clone(),
        pod: original.name.clone(),
        expected_uid: original.uid.clone(),
        drain_timeout_seconds: 120,
        operation_id: report.operation_id.clone(),
        execution_id: report.execution_id,
        plan_step_id: report.plan_step_id,
    };
    let exercise = async {
        let before = read(
            driver.proxy_restart_state(&settings.namespace, &request.pod),
            "initial_state_failed",
        )
        .await?
        .map_err(|_| "initial_state_failed")?;
        if !before.drain_supported
            || !before.pod_ready
            || !before.remaining_replicas_healthy
            || before.active_operation_id.is_some()
            || !before.drain.as_ref().is_some_and(accepting)
        {
            return Err("initial_proxy_not_accepting_and_redundant");
        }
        report.adapter_started = true;
        let result = tokio::time::timeout(MUTATION_TIMEOUT, driver.restart_one_drained(&request))
            .await
            .map_err(|_| "adapter_wait_expired_remote_outcome_unknown")?;
        report.adapter_returned = true;
        if result.is_ok() {
            return Err("incomplete_source_must_not_complete_restart");
        }
        let (deployment, pods) = read(baseline.current(driver, settings), "replacement_read_failed").await??;
        baseline.verify_one_replacement(settings, &pods)?;
        baseline.verify_owned(&deployment, settings, &request)?;
        report.original_operation_retained = true;
        report.replacement = pods.clone();
        let after = read(
            driver.proxy_restart_state(&settings.namespace, &request.pod),
            "replacement_state_failed",
        )
        .await?
        .map_err(|_| "replacement_state_failed")?;
        if !after.replacement_ready
            || !after.remaining_replicas_healthy
            || !after.drain.as_ref().is_some_and(accepting)
            || after.synthetic_path_healthy
            || after.slo_healthy
            || after.active_operation_id.as_deref() != Some(request.operation_id.as_str())
            || after.last_operation_id.is_some()
        {
            return Err("replacement_did_not_retain_unknown_operation");
        }
        normal_source_is_incomplete(driver).await?;
        report.incomplete_normal_source_after = true;
        let handler = ProxyRestartOneHandler::new(Arc::clone(driver));
        let observation = AgentReadRequest {
            schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
            tenant_id: settings.restart.tenant_id,
            cluster_id: settings.restart.cluster_id,
            execution_id: request.execution_id,
            plan_step_id: request.plan_step_id,
            action: ExecutionAction::ProxyRestartOne,
            descriptor_version: "1.0.0".to_owned(),
            target: format!("pod/{}/{}", settings.namespace, request.pod),
            parameters: serde_json::json!({
                "namespace": request.namespace, "pod": request.pod, "expected_uid": request.expected_uid,
            }),
        };
        let reconciliation = read(
            handler.reconcile(&observation, Some(&request.operation_id)),
            "reconcile_failed",
        )
        .await?
        .map_err(|_| "reconcile_failed")?;
        if reconciliation.state != ReconcileEffectState::Unknown {
            return Err("replacement_without_source_must_reconcile_unknown");
        }
        report.reconcile_unknown = true;
        if read(driver.restart_one_drained(&request), "original_retry_wait_failed")
            .await?
            .is_ok()
        {
            return Err("original_retry_must_refuse_a_second_restart");
        }
        let (deployment, retried_pods) = read(baseline.current(driver, settings), "retry_read_failed").await??;
        if retried_pods != pods {
            return Err("original_retry_changed_the_post_restart_pod_set");
        }
        baseline.verify_owned(&deployment, settings, &request)?;
        report.retry_refused_without_another_replacement = true;
        let restore = read(
            driver.cancel_restart_and_restore(&ProxyRestartRestore {
                namespace: request.namespace.clone(),
                pod: request.pod.clone(),
                expected_uid: request.expected_uid.clone(),
                operation_id: request.operation_id.clone(),
                execution_id: request.execution_id,
                plan_step_id: request.plan_step_id,
            }),
            "original_restore_failed",
        )
        .await?
        .map_err(|_| "original_restore_failed")?;
        if restore != ProxyRestartRestoreOutcome::ManualTakeoverRequired {
            return Err("deleted_original_uid_requires_manual_takeover");
        }
        report.original_uid_restore_requires_manual_takeover = true;
        Ok::<(), &'static str>(())
    }
    .await;
    report.exercise_failure = exercise.err();
    // A timed-out waiter does not establish that remote mutation has stopped.
    // Leave such cases to the fixture owner rather than racing a late write.
    if report.adapter_started && !report.adapter_returned {
        report.cleanup_failure = Some("owner_inspection_required_after_unknown_remote_wait");
        return Ok(());
    }
    let cleanup = async {
        report.operator_metadata_cleanup_written = read(
            baseline.operator_cleanup(driver, settings, &request),
            "operator_cleanup_failed",
        )
        .await??;
        let (deployment, pods) = read(baseline.current(driver, settings), "cleanup_read_failed").await??;
        baseline.verify_restored(&deployment, settings)?;
        if report.original_uid_restore_requires_manual_takeover {
            baseline.verify_one_replacement(settings, &pods)?;
        } else if pods != baseline.pods {
            return Err("failed_drill_changed_pods_requires_owner_inspection");
        }
        report.restored = pods;
        report.operator_metadata_and_resources_restored = true;
        Ok::<(), &'static str>(())
    }
    .await;
    report.cleanup_failure = cleanup.err();
    Ok(())
}

async fn normal_source_is_incomplete(driver: &ProductionProxyRestartClient) -> TestResult<()> {
    let source = &driver.verification;
    let query = ExecutionSliQuery {
        schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: source.tenant_id,
        cluster_id: source.cluster_id,
        correlation_id: CorrelationId::new(),
        conditions: VERIFICATION_CONDITIONS.iter().map(ToString::to_string).collect(),
        binding: None,
    };
    let request = async {
        let url = source
            .base_url
            .join("/internal/v1/execution-verification/sli")
            .map_err(|_| "normal_cp_url_invalid")?;
        let response = source
            .client
            .post(url)
            .bearer_auth(source.bearer_token.as_ref())
            .header("x-forwarded-client-cert", "URI=spiffe://rocketmq-sre/executor")
            .header("x-rocketmq-tenant", source.tenant_id.to_string())
            .header("x-rocketmq-clusters", source.cluster_id.to_string())
            .header("x-rocketmq-subject", "rocketmq-sre-execution-agent")
            .json(&query)
            .send()
            .await
            .map_err(|_| "normal_cp_read_failed")?;
        let observation = decode_observation(response)
            .await
            .map_err(|_| "normal_cp_response_invalid")?;
        validate_observation(&query, &observation).map_err(|_| "normal_cp_scope_or_conditions_invalid")?;
        if observation.complete
            || observation.source_facts.as_ref().is_none_or(|facts| {
                facts.exact_target_conditions
                    || facts.source_observed_at.is_some()
                    || !facts
                        .reason_codes
                        .contains(&ExecutionObservationReason::ExactSliSourceMissing)
            })
        {
            return Err("normal_cp_did_not_report_missing_exact_source");
        }
        Ok(())
    };
    read(request, "normal_cp_read_timeout").await?
}

async fn read<T>(future: impl Future<Output = T>, reason: &'static str) -> TestResult<T> {
    tokio::time::timeout(READ_TIMEOUT, future).await.map_err(|_| reason)
}
