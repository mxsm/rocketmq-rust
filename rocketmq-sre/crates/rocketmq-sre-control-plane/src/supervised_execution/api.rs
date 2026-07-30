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

use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::get;
use axum::routing::post;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DiagnosisRevisionId;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::ResourceQuarantine;
use rocketmq_sre_contracts::ResourceQuarantineId;

use super::model::ActionPlanView;
use super::model::ApprovalDecisionRequest;
use super::model::ApprovalDecisionResponse;
use super::model::AuditPage;
use super::model::ClearQuarantineRequest;
use super::model::CreatePlanRequest;
use super::model::CreatePlanResponse;
use super::model::CriticReviewRequest;
use super::model::CriticReviewResponse;
use super::model::ExecutionSubmissionView;
use super::model::QuarantineListQuery;
use super::model::QuarantinePage;
use super::model::SubmitExecutionRequest;
use crate::ControlPlaneError;
use crate::api::AppState;
use crate::observability::CORRELATION_ID_HEADER;
use crate::workflow::ConfirmDiagnosisExecutionRequest;
use crate::workflow::DiagnosisExecutionConfirmation;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/incidents/{incident_id}/diagnosis-revisions/{revision_id}/confirm-execution",
            post(confirm_diagnosis_for_execution).layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route("/v1/plans", post(create_plan).layer(DefaultBodyLimit::max(256 * 1024)))
        .route("/v1/plans/{id}", get(get_plan))
        .route(
            "/v1/plans/{id}/critic",
            post(review_plan_with_critic).layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route(
            "/v1/plans/{id}/approve",
            post(approve_plan).layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route(
            "/v1/plans/{id}/reject",
            post(reject_plan).layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route(
            "/v1/executions",
            post(submit_execution).layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route("/v1/executions/{id}", get(get_execution))
        .route("/v1/audit/{correlation_id}", get(get_audit))
        .route("/v1/resource-quarantines", get(list_quarantines))
        .route(
            "/v1/resource-quarantines/{id}/clear",
            post(clear_quarantine).layer(DefaultBodyLimit::max(32 * 1024)),
        )
}

async fn confirm_diagnosis_for_execution(
    State(state): State<AppState>,
    Path((incident_id, revision_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<ConfirmDiagnosisExecutionRequest>,
) -> Result<Json<DiagnosisExecutionConfirmation>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .workflow
        .confirm_diagnosis_for_execution(
            &auth,
            parse_incident_id(&incident_id)?,
            parse_diagnosis_revision_id(&revision_id)?,
            &request,
            correlation_id(&headers),
        )
        .await
        .map(Json)
}

async fn create_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreatePlanRequest>,
) -> Result<Json<CreatePlanResponse>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .supervised_execution
        .create_plan(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn get_plan(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ActionPlanView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .plan(&auth, parse_plan_id(&id)?)
        .await
        .map(Json)
}

async fn approve_plan(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ApprovalDecisionRequest>,
) -> Result<Json<ApprovalDecisionResponse>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .approve(&auth, parse_plan_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn review_plan_with_critic(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CriticReviewRequest>,
) -> Result<Json<CriticReviewResponse>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .review_with_critic(&auth, parse_plan_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn reject_plan(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ApprovalDecisionRequest>,
) -> Result<Json<ApprovalDecisionResponse>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .reject(&auth, parse_plan_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn submit_execution(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SubmitExecutionRequest>,
) -> Result<Json<ExecutionSubmissionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .submit_execution(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn get_execution(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ExecutionSubmissionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .execution(&auth, parse_execution_id(&id)?)
        .await
        .map(Json)
}

async fn get_audit(
    State(state): State<AppState>,
    Path(correlation_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<AuditPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .audit(&auth, parse_correlation_id(&correlation_id)?)
        .await
        .map(Json)
}

async fn list_quarantines(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<QuarantineListQuery>,
) -> Result<Json<QuarantinePage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.supervised_execution.quarantines(&auth, &query).await.map(Json)
}

async fn clear_quarantine(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ClearQuarantineRequest>,
) -> Result<Json<ResourceQuarantine>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .clear_quarantine(&auth, parse_quarantine_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

fn parse_plan_id(value: &str) -> Result<ActionPlanId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "plan identifier must be a UUID"))
}

fn parse_incident_id(value: &str) -> Result<IncidentId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "incident identifier must be a UUID"))
}

fn parse_diagnosis_revision_id(value: &str) -> Result<DiagnosisRevisionId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "diagnosis revision identifier must be a UUID"))
}

fn parse_execution_id(value: &str) -> Result<ExecutionId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "execution identifier must be a UUID"))
}

fn parse_correlation_id(value: &str) -> Result<CorrelationId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "correlation identifier must be a UUID"))
}

fn parse_quarantine_id(value: &str) -> Result<ResourceQuarantineId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "quarantine identifier must be a UUID"))
}

fn correlation_id(headers: &HeaderMap) -> CorrelationId {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}
