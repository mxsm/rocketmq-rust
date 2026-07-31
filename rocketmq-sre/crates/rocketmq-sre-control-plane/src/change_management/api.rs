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
use rocketmq_sre_contracts::ChangeSchedule;
use rocketmq_sre_contracts::ChangeScheduleId;
use rocketmq_sre_contracts::ChangeWindow;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::RunbookDefinition;
use rocketmq_sre_contracts::RunbookId;
use rocketmq_sre_contracts::RunbookStepId;

use super::model::ChangeScheduleListQuery;
use super::model::ChangeSchedulePage;
use super::model::ChangeSchedulePreview;
use super::model::ChangeWindowListQuery;
use super::model::ChangeWindowPage;
use super::model::CreateChangeScheduleRequest;
use super::model::CreateChangeWindowRequest;
use super::model::CreateRunbookRequest;
use super::model::ManualGateDecision;
use super::model::ManualGateDecisionRequest;
use super::model::RunbookGetQuery;
use super::model::RunbookListQuery;
use super::model::RunbookPage;
use super::model::ScheduleTransitionRequest;
use crate::ControlPlaneError;
use crate::api::AppState;
use crate::observability::CORRELATION_ID_HEADER;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/runbooks",
            post(create_runbook)
                .get(list_runbooks)
                .layer(DefaultBodyLimit::max(512 * 1024)),
        )
        .route("/v1/runbooks/{id}/versions/{version}", get(get_runbook))
        .route(
            "/v1/change-windows",
            post(create_window)
                .get(list_windows)
                .layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/change-schedules/preview",
            post(preview_schedule).layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route(
            "/v1/change-schedules",
            post(create_schedule)
                .get(list_schedules)
                .layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route("/v1/change-schedules/{id}", get(get_schedule))
        .route(
            "/v1/change-schedules/{id}/pause",
            post(pause_schedule).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/v1/change-schedules/{id}/resume",
            post(resume_schedule).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/v1/change-schedules/{id}/cancel",
            post(cancel_schedule).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/v1/change-schedules/{id}/reconcile",
            post(reconcile_schedule).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/v1/change-schedules/{id}/manual-gates/{step_id}/approve",
            post(approve_manual_gate).layer(DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/v1/change-schedules/{id}/manual-gates/{step_id}/reject",
            post(reject_manual_gate).layer(DefaultBodyLimit::max(8 * 1024)),
        )
}

async fn create_runbook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateRunbookRequest>,
) -> Result<Json<RunbookDefinition>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .change_management
        .create_runbook(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_runbooks(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<RunbookListQuery>,
) -> Result<Json<RunbookPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state
        .change_management
        .runbooks(&auth, query.cluster_id, query.limit)
        .await
        .map(Json)
}

async fn get_runbook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, version)): Path<(String, String)>,
    Query(query): Query<RunbookGetQuery>,
) -> Result<Json<RunbookDefinition>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state
        .change_management
        .runbook(&auth, query.cluster_id, parse_runbook_id(&id)?, &version)
        .await
        .map(Json)
}

async fn create_window(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateChangeWindowRequest>,
) -> Result<Json<ChangeWindow>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .change_management
        .create_window(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_windows(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ChangeWindowListQuery>,
) -> Result<Json<ChangeWindowPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state
        .change_management
        .windows(&auth, query.cluster_id, query.from, query.to, query.limit)
        .await
        .map(Json)
}

async fn preview_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateChangeScheduleRequest>,
) -> Result<Json<ChangeSchedulePreview>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .change_management
        .preview_schedule(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn create_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateChangeScheduleRequest>,
) -> Result<Json<ChangeSchedule>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .change_management
        .create_schedule(&auth, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn list_schedules(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ChangeScheduleListQuery>,
) -> Result<Json<ChangeSchedulePage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state
        .change_management
        .schedules(&auth, query.cluster_id, query.status, query.limit)
        .await
        .map(Json)
}

async fn get_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<ChangeSchedule>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .change_management
        .schedule(&auth, parse_schedule_id(&id)?)
        .await
        .map(Json)
}

async fn pause_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ScheduleTransitionRequest>,
) -> Result<Json<ChangeSchedule>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .change_management
        .pause(&auth, parse_schedule_id(&id)?, &request.reason)
        .await
        .map(Json)
}

async fn resume_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ScheduleTransitionRequest>,
) -> Result<Json<ChangeSchedule>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .change_management
        .resume(&auth, parse_schedule_id(&id)?, &request.reason)
        .await
        .map(Json)
}

async fn cancel_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ScheduleTransitionRequest>,
) -> Result<Json<ChangeSchedule>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .change_management
        .cancel(&auth, parse_schedule_id(&id)?, &request.reason)
        .await
        .map(Json)
}

async fn reconcile_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<ScheduleTransitionRequest>,
) -> Result<Json<ChangeSchedule>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .change_management
        .reconcile(&auth, parse_schedule_id(&id)?, &request.reason)
        .await
        .map(Json)
}

async fn approve_manual_gate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, step_id)): Path<(String, String)>,
    Json(request): Json<ManualGateDecisionRequest>,
) -> Result<Json<ChangeSchedule>, ControlPlaneError> {
    decide_manual_gate(state, headers, id, step_id, request, ManualGateDecision::Approved).await
}

async fn reject_manual_gate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, step_id)): Path<(String, String)>,
    Json(request): Json<ManualGateDecisionRequest>,
) -> Result<Json<ChangeSchedule>, ControlPlaneError> {
    decide_manual_gate(state, headers, id, step_id, request, ManualGateDecision::Rejected).await
}

async fn decide_manual_gate(
    state: AppState,
    headers: HeaderMap,
    id: String,
    step_id: String,
    request: ManualGateDecisionRequest,
    decision: ManualGateDecision,
) -> Result<Json<ChangeSchedule>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .change_management
        .decide_manual_gate(
            &auth,
            parse_schedule_id(&id)?,
            parse_step_id(&step_id)?,
            decision,
            &request.reason,
        )
        .await
        .map(Json)
}

fn parse_runbook_id(value: &str) -> Result<RunbookId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "runbook identifier must be a UUID"))
}

fn parse_schedule_id(value: &str) -> Result<ChangeScheduleId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "schedule identifier must be a UUID"))
}

fn parse_step_id(value: &str) -> Result<RunbookStepId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "runbook step identifier must be a UUID"))
}

fn correlation_id(headers: &HeaderMap) -> CorrelationId {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}
