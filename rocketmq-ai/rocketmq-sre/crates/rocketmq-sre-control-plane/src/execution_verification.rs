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

use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::HeaderMap;
use axum::routing::post;
use rocketmq_sre_contracts::EXECUTION_VERIFICATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionObservationReason;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::ExecutionSliObservation;
use rocketmq_sre_contracts::ExecutionSliQuery;
use rocketmq_sre_contracts::ExecutionSliSourceFacts;
use rocketmq_sre_contracts::HealthDataQuality;
use rocketmq_sre_contracts::HealthStatus;
use rocketmq_sre_contracts::SliHealth;
use rocketmq_sre_contracts::canonical_sha256;

use crate::ControlPlaneRequestFailure;
use crate::api::AppState;

const MAX_CONDITIONS: usize = 32;
const MAX_CONDITION_BYTES: usize = 128;

pub(crate) fn routes() -> Router<AppState> {
    Router::new().route(
        "/internal/v1/execution-verification/sli",
        post(observe).layer(DefaultBodyLimit::max(16 * 1024)),
    )
}

#[tracing::instrument(name = "sre.execution_verification.sli", skip_all, fields(access = "read_only"))]
async fn observe(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Result<Json<ExecutionSliQuery>, JsonRejection>,
) -> Result<Json<ExecutionSliObservation>, ControlPlaneRequestFailure> {
    let Json(request) = request.map_err(|_| {
        ControlPlaneRequestFailure::validation(
            "invalid_execution_verification",
            "a bounded technical verification JSON request is required",
        )
    })?;
    validate_query(&request)?;
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    if !auth.roles.contains("executor_service") || auth.tenant_id != request.tenant_id {
        return Err(ControlPlaneRequestFailure::forbidden(
            "execution_verification_forbidden",
            "technical SLI verification requires the scoped Executor workload identity",
        ));
    }
    validate_binding(&state, &auth, &request).await?;
    let report = state.slo.evaluate_cluster(&auth, request.cluster_id).await?;
    let evaluation = evaluate_conditions(&request.conditions, &report.slis)?;
    let source_facts = ExecutionSliSourceFacts {
        binding: request.binding.clone(),
        // Cluster burn-rate context is not an exact action/target-version
        // measurement. Its evaluation clock cannot become a data timestamp.
        source_observed_at: None,
        collected_at: chrono::Utc::now(),
        exact_target_conditions: false,
        reason_codes: vec![ExecutionObservationReason::ExactSliSourceMissing],
    };
    Ok(Json(ExecutionSliObservation {
        schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: request.tenant_id,
        cluster_id: request.cluster_id,
        correlation_id: request.correlation_id,
        conditions: evaluation.conditions,
        complete: evaluation.complete && source_facts.exact_target_conditions,
        evidence_ids: evaluation.evidence_ids,
        observed_at: report.observed_at,
        source_facts: Some(source_facts),
    }))
}

fn validate_query(request: &ExecutionSliQuery) -> Result<(), ControlPlaneRequestFailure> {
    if request.schema_version != EXECUTION_VERIFICATION_SCHEMA_VERSION
        || request.conditions.is_empty()
        || request.conditions.len() > MAX_CONDITIONS
        || request
            .conditions
            .iter()
            .any(|condition| condition.is_empty() || condition.len() > MAX_CONDITION_BYTES)
    {
        return Err(ControlPlaneRequestFailure::validation(
            "invalid_execution_verification",
            "technical SLI verification request is incompatible or outside bounded limits",
        ));
    }
    let unique = request.conditions.iter().collect::<BTreeSet<_>>();
    if unique.len() != request.conditions.len() {
        return Err(ControlPlaneRequestFailure::validation(
            "invalid_execution_verification",
            "technical SLI verification conditions must be unique",
        ));
    }
    Ok(())
}

struct ConditionEvaluation {
    conditions: BTreeMap<String, bool>,
    complete: bool,
    evidence_ids: Vec<EvidenceId>,
}

fn evaluate_conditions(
    requested: &[String],
    slis: &[SliHealth],
) -> Result<ConditionEvaluation, ControlPlaneRequestFailure> {
    let by_id = slis
        .iter()
        .map(|sli| (sli.id.as_str(), sli))
        .collect::<BTreeMap<_, _>>();
    let mut conditions = BTreeMap::new();
    let mut complete = true;
    let mut evidence_ids = Vec::new();
    for condition in requested {
        let sli_id = sli_for_condition(condition).ok_or_else(|| {
            ControlPlaneRequestFailure::validation(
                "unknown_verification_condition",
                "technical SLI verification condition is not registered",
            )
        })?;
        let Some(sli) = by_id.get(sli_id) else {
            conditions.insert(condition.clone(), false);
            complete = false;
            continue;
        };
        let condition_complete = sli.data_quality == HealthDataQuality::Complete;
        conditions.insert(
            condition.clone(),
            condition_complete && sli.status == HealthStatus::Healthy,
        );
        complete &= condition_complete;
        for evidence_id in &sli.evidence_ids {
            if !evidence_ids.contains(evidence_id) {
                evidence_ids.push(*evidence_id);
            }
        }
    }
    Ok(ConditionEvaluation {
        conditions,
        complete,
        evidence_ids,
    })
}

fn sli_for_condition(condition: &str) -> Option<&str> {
    match condition {
        "runtime_error_ratio"
        | "proxy_error_ratio"
        | "proxy_p99_latency"
        | "synthetic_message_path"
        | "send_success_ratio"
        | "consume_success_ratio"
        | "retry_backlog_growth"
        | "broker_error_ratio"
        | "store_dispatch_latency"
        | "telemetry_export_success_ratio"
        | "telemetry_queue_utilization"
        | "credential_probe_success_ratio"
        | "authentication_error_ratio" => Some(condition),
        _ => None,
    }
}

async fn validate_binding(
    state: &AppState,
    auth: &crate::auth::AuthContext,
    query: &ExecutionSliQuery,
) -> Result<(), ControlPlaneRequestFailure> {
    let Some(binding) = &query.binding else {
        // Legacy cluster-context callers remain readable but never qualified.
        return Ok(());
    };
    let stored: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT request_snapshot FROM executions WHERE id=$1 AND tenant_id=$2 AND cluster_id=$3")
            .bind(binding.execution_id.as_uuid())
            .bind(auth.tenant_id.as_uuid())
            .bind(query.cluster_id.as_uuid())
            .fetch_optional(&state.repository.pool)
            .await?;
    let request: ExecutionRequest = stored
        .and_then(|snapshot| serde_json::from_value(snapshot).ok())
        .ok_or_else(invalid_binding)?;
    request.plan.verify_plan_hash().map_err(|_| invalid_binding())?;
    let step = request
        .plan
        .steps
        .iter()
        .find(|step| step.id == binding.plan_step_id)
        .ok_or_else(invalid_binding)?;
    let expected = step.verification.technical_slis.iter().collect::<BTreeSet<_>>();
    let actual = query.conditions.iter().collect::<BTreeSet<_>>();
    if request.id != binding.execution_id
        || request.correlation_id != query.correlation_id
        || request.tenant_id != auth.tenant_id
        || request.cluster_id != query.cluster_id
        || request.plan.tenant_id != auth.tenant_id
        || request.plan.cluster_id != query.cluster_id
        || step.action != binding.action
        || step.descriptor_version != binding.descriptor_version
        || step.resource != binding.target
        || canonical_sha256(&step.parameters).map_err(|_| invalid_binding())? != binding.parameters_digest
        || expected != actual
    {
        return Err(invalid_binding());
    }
    Ok(())
}

fn invalid_binding() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::forbidden(
        "execution_verification_binding_invalid",
        "technical verification requires the exact accepted execution step",
    )
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::SloDimension;

    use super::*;

    #[test]
    fn registered_conditions_project_only_complete_healthy_slis() {
        let evidence_id = EvidenceId::new();
        let slis = vec![
            sli(
                "broker_runtime",
                HealthStatus::Healthy,
                HealthDataQuality::Complete,
                evidence_id,
            ),
            sli(
                "flush_dispatch",
                HealthStatus::Degraded,
                HealthDataQuality::Complete,
                evidence_id,
            ),
            sli(
                "telemetry_freshness",
                HealthStatus::Healthy,
                HealthDataQuality::Complete,
                evidence_id,
            ),
            sli(
                "delivery_ratio",
                HealthStatus::Healthy,
                HealthDataQuality::Complete,
                evidence_id,
            ),
        ];

        let result = evaluate_conditions(
            &[
                "broker_error_ratio".to_owned(),
                "store_dispatch_latency".to_owned(),
                "telemetry_export_success_ratio".to_owned(),
                "telemetry_queue_utilization".to_owned(),
                "credential_probe_success_ratio".to_owned(),
                "authentication_error_ratio".to_owned(),
                "retry_backlog_growth".to_owned(),
            ],
            &slis,
        )
        .expect("registered SLI conditions");

        assert_eq!(result.conditions.get("broker_error_ratio"), Some(&false));
        assert_eq!(result.conditions.get("store_dispatch_latency"), Some(&false));
        assert_eq!(result.conditions.get("telemetry_export_success_ratio"), Some(&false));
        assert_eq!(result.conditions.get("telemetry_queue_utilization"), Some(&false));
        assert_eq!(result.conditions.get("credential_probe_success_ratio"), Some(&false));
        assert_eq!(result.conditions.get("authentication_error_ratio"), Some(&false));
        assert_eq!(result.conditions.get("retry_backlog_growth"), Some(&false));
        assert!(!result.complete);
        assert!(result.evidence_ids.is_empty());
    }

    #[test]
    fn missing_or_unknown_slis_fail_closed() {
        let missing = evaluate_conditions(&["proxy_error_ratio".to_owned()], &[]).expect("known condition");
        assert_eq!(missing.conditions.get("proxy_error_ratio"), Some(&false));
        assert!(!missing.complete);
        assert!(evaluate_conditions(&["unregistered_sli".to_owned()], &[]).is_err());
    }

    fn sli(id: &str, status: HealthStatus, data_quality: HealthDataQuality, evidence_id: EvidenceId) -> SliHealth {
        SliHealth {
            id: id.to_owned(),
            display_name: id.to_owned(),
            dimension: SloDimension::Broker,
            objective: 0.99,
            status,
            data_quality,
            windows: Vec::new(),
            evidence_ids: vec![evidence_id],
            reason_codes: Vec::new(),
        }
    }
}
