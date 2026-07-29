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
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use rocketmq_sre_contracts::EXECUTION_VERIFICATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ExecutionSliObservation;
use rocketmq_sre_contracts::ExecutionSliQuery;
use rocketmq_sre_contracts::HealthDataQuality;
use rocketmq_sre_contracts::HealthStatus;
use rocketmq_sre_contracts::SliHealth;

use crate::ControlPlaneError;
use crate::api::AppState;

const MAX_CONDITIONS: usize = 32;
const MAX_CONDITION_BYTES: usize = 128;

pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/internal/v1/execution-verification/sli", post(observe))
}

#[tracing::instrument(
    name = "sre.execution_verification.sli",
    skip_all,
    fields(cluster_id = %request.cluster_id, condition_count = request.conditions.len(), access = "read_only")
)]
async fn observe(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ExecutionSliQuery>,
) -> Result<Json<ExecutionSliObservation>, ControlPlaneError> {
    validate_query(&request)?;
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    if !auth.roles.contains("executor_service") || auth.tenant_id != request.tenant_id {
        return Err(ControlPlaneError::forbidden(
            "execution_verification_forbidden",
            "technical SLI verification requires the scoped Executor workload identity",
        ));
    }
    let report = state.slo.evaluate_cluster(&auth, request.cluster_id).await?;
    let evaluation = evaluate_conditions(&request.conditions, &report.slis)?;
    Ok(Json(ExecutionSliObservation {
        schema_version: EXECUTION_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: request.tenant_id,
        cluster_id: request.cluster_id,
        correlation_id: request.correlation_id,
        conditions: evaluation.conditions,
        complete: evaluation.complete,
        evidence_ids: evaluation.evidence_ids,
        observed_at: report.observed_at,
    }))
}

fn validate_query(request: &ExecutionSliQuery) -> Result<(), ControlPlaneError> {
    if request.schema_version != EXECUTION_VERIFICATION_SCHEMA_VERSION
        || request.conditions.is_empty()
        || request.conditions.len() > MAX_CONDITIONS
        || request
            .conditions
            .iter()
            .any(|condition| condition.is_empty() || condition.len() > MAX_CONDITION_BYTES)
    {
        return Err(ControlPlaneError::validation(
            "invalid_execution_verification",
            "technical SLI verification request is incompatible or outside bounded limits",
        ));
    }
    let unique = request.conditions.iter().collect::<BTreeSet<_>>();
    if unique.len() != request.conditions.len() {
        return Err(ControlPlaneError::validation(
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

fn evaluate_conditions(requested: &[String], slis: &[SliHealth]) -> Result<ConditionEvaluation, ControlPlaneError> {
    let by_id = slis
        .iter()
        .map(|sli| (sli.id.as_str(), sli))
        .collect::<BTreeMap<_, _>>();
    let mut conditions = BTreeMap::new();
    let mut complete = true;
    let mut evidence_ids = Vec::new();
    for condition in requested {
        let sli_id = sli_for_condition(condition).ok_or_else(|| {
            ControlPlaneError::validation(
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

fn sli_for_condition(condition: &str) -> Option<&'static str> {
    match condition {
        "runtime_error_ratio" => Some("runtime_saturation"),
        "proxy_error_ratio" | "proxy_p99_latency" => Some("proxy_connection"),
        "synthetic_message_path" | "send_success_ratio" | "consume_success_ratio" => Some("delivery_ratio"),
        "broker_error_ratio" => Some("broker_runtime"),
        "store_dispatch_latency" => Some("flush_dispatch"),
        "telemetry_export_success_ratio" | "telemetry_queue_utilization" => Some("telemetry_freshness"),
        _ => None,
    }
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
        ];

        let result = evaluate_conditions(
            &[
                "broker_error_ratio".to_owned(),
                "store_dispatch_latency".to_owned(),
                "telemetry_export_success_ratio".to_owned(),
                "telemetry_queue_utilization".to_owned(),
            ],
            &slis,
        )
        .expect("registered SLI conditions");

        assert_eq!(result.conditions.get("broker_error_ratio"), Some(&true));
        assert_eq!(result.conditions.get("store_dispatch_latency"), Some(&false));
        assert_eq!(result.conditions.get("telemetry_export_success_ratio"), Some(&true));
        assert_eq!(result.conditions.get("telemetry_queue_utilization"), Some(&true));
        assert!(result.complete);
        assert_eq!(result.evidence_ids, [evidence_id]);
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
