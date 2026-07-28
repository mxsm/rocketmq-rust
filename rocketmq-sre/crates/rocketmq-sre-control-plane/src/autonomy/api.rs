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
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::get;
use axum::routing::post;
use rocketmq_sre_contracts::AutonomyGrant;
use rocketmq_sre_contracts::AutonomyOutcome;
use rocketmq_sre_contracts::AutonomyQualificationCohort;
use rocketmq_sre_contracts::AutonomyQualificationSample;

use super::model::AutonomyFreezeView;
use super::model::AutonomyKillSwitchView;
use super::model::AutonomyListQuery;
use super::model::AutonomyScopePage;
use super::model::AutonomyScopeQuery;
use super::model::AutonomyScopeView;
use super::model::AutonomyTransitionRequest;
use super::model::CreateAutonomyPolicyRequest;
use super::model::CreateShadowCohortRequest;
use super::model::DynamicSafetyView;
use super::model::EvaluateDynamicSafetyRequest;
use super::model::IssueAutonomyGrantRequest;
use super::model::PrepareAutonomousCohortRequest;
use super::model::RecordAutonomyOutcomeRequest;
use super::model::RecordQualificationSampleRequest;
use super::model::RecordShadowOutcomeRequest;
use super::model::SetAutonomyFreezeRequest;
use super::model::SetAutonomyKillSwitchRequest;
use super::model::ShadowOutcomeListQuery;
use super::model::ShadowOutcomePage;
use super::model::ShadowOutcomeView;
use crate::ControlPlaneError;
use crate::api::AppState;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/autonomy/policies",
            post(create_policy).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route("/v1/autonomy/scopes", get(list_scopes))
        .route("/v1/autonomy/scope", get(get_scope))
        .route(
            "/v1/autonomy/transitions",
            post(transition).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/v1/autonomy/freezes",
            post(set_freeze).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/v1/autonomy/kill-switches",
            post(set_kill_switch).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/internal/v1/autonomy/cohorts/shadow",
            post(create_shadow_cohort).layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route(
            "/internal/v1/autonomy/cohorts/autonomous",
            post(prepare_autonomous_cohort).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/internal/v1/autonomy/qualification-samples",
            post(record_qualification_sample).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/autonomy/shadow-outcomes",
            get(list_shadow_outcomes)
                .post(record_shadow_outcome)
                .layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route(
            "/internal/v1/autonomy/grants",
            post(issue_grant).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/internal/v1/autonomy/outcomes",
            post(record_outcome).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/internal/v1/autonomy/dynamic-safety",
            post(evaluate_dynamic_safety).layer(DefaultBodyLimit::max(32 * 1024)),
        )
}

async fn create_policy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateAutonomyPolicyRequest>,
) -> Result<Json<AutonomyScopeView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.autonomy.create_policy(&auth, &request).await.map(Json)
}

async fn list_scopes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AutonomyListQuery>,
) -> Result<Json<AutonomyScopePage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state
        .autonomy
        .scopes(&auth, query.cluster_id, query.limit)
        .await
        .map(Json)
}

async fn get_scope(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AutonomyScopeQuery>,
) -> Result<Json<AutonomyScopeView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.autonomy.scope(&auth, &query).await.map(Json)
}

async fn transition(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(scope): Query<AutonomyScopeQuery>,
    Json(request): Json<AutonomyTransitionRequest>,
) -> Result<Json<AutonomyScopeView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(scope.cluster_id)).await?;
    state.autonomy.transition(&auth, &scope, &request).await.map(Json)
}

async fn set_freeze(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SetAutonomyFreezeRequest>,
) -> Result<Json<AutonomyFreezeView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.autonomy.set_freeze(&auth, &request).await.map(Json)
}

async fn set_kill_switch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<SetAutonomyKillSwitchRequest>,
) -> Result<Json<AutonomyKillSwitchView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.autonomy.set_kill_switch(&auth, &request).await.map(Json)
}

async fn create_shadow_cohort(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateShadowCohortRequest>,
) -> Result<Json<AutonomyQualificationCohort>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.autonomy.create_shadow_cohort(&auth, &request).await.map(Json)
}

async fn prepare_autonomous_cohort(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PrepareAutonomousCohortRequest>,
) -> Result<Json<AutonomyQualificationCohort>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .autonomy
        .prepare_autonomous_cohort(&auth, &request)
        .await
        .map(Json)
}

async fn record_qualification_sample(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RecordQualificationSampleRequest>,
) -> Result<Json<AutonomyQualificationSample>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state
        .autonomy
        .record_qualification_sample(&auth, &request)
        .await
        .map(Json)
}

async fn record_shadow_outcome(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RecordShadowOutcomeRequest>,
) -> Result<Json<ShadowOutcomeView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.autonomy.record_shadow_outcome(&auth, &request).await.map(Json)
}

async fn list_shadow_outcomes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ShadowOutcomeListQuery>,
) -> Result<Json<ShadowOutcomePage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.autonomy.shadow_outcomes(&auth, &query).await.map(Json)
}

async fn issue_grant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IssueAutonomyGrantRequest>,
) -> Result<Json<AutonomyGrant>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.autonomy.issue_grant(&auth, &request).await.map(Json)
}

async fn record_outcome(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RecordAutonomyOutcomeRequest>,
) -> Result<Json<AutonomyOutcome>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.autonomy.record_outcome(&auth, &request).await.map(Json)
}

async fn evaluate_dynamic_safety(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<EvaluateDynamicSafetyRequest>,
) -> Result<Json<DynamicSafetyView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.autonomy.evaluate_dynamic_safety(&auth, &request).await.map(Json)
}
