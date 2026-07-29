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
use rocketmq_sre_contracts::DrActionItem;
use rocketmq_sre_contracts::DrActionItemId;
use rocketmq_sre_contracts::DrBackupAsset;
use rocketmq_sre_contracts::DrExercise;
use rocketmq_sre_contracts::DrExerciseId;
use rocketmq_sre_contracts::DrFinding;
use rocketmq_sre_contracts::DrPlan;
use rocketmq_sre_contracts::DrPlanId;
use rocketmq_sre_contracts::RecoveryCheckpoint;

use super::model::CreateDrPlanRequest;
use super::model::DrActionItemPage;
use super::model::DrActionItemQuery;
use super::model::DrBackupAssetPage;
use super::model::DrExercisePage;
use super::model::DrExerciseQuery;
use super::model::DrFindingPage;
use super::model::DrPlanPage;
use super::model::DrPlanQuery;
use super::model::RecordDrFindingRequest;
use super::model::RecordRecoveryCheckpointRequest;
use super::model::RecoveryCheckpointPage;
use super::model::StartDrExerciseRequest;
use super::model::TransitionDrExerciseRequest;
use super::model::UpdateDrActionItemRequest;
use super::model::UpsertDrBackupAssetRequest;
use crate::ControlPlaneError;
use crate::api::AppState;

const DR_WRITE_BODY_LIMIT: usize = 256 * 1024;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/dr/plans",
            post(create_plan)
                .get(plans)
                .layer(DefaultBodyLimit::max(DR_WRITE_BODY_LIMIT)),
        )
        .route(
            "/v1/dr/plans/{id}/backup-assets",
            post(upsert_backup_asset)
                .get(backup_assets)
                .layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/dr/exercises",
            post(create_exercise)
                .get(exercises)
                .layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route(
            "/v1/dr/exercises/{id}/state",
            post(transition_exercise).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/dr/exercises/{id}/checkpoints",
            post(record_checkpoint)
                .get(checkpoints)
                .layer(DefaultBodyLimit::max(DR_WRITE_BODY_LIMIT)),
        )
        .route(
            "/v1/dr/exercises/{id}/findings",
            post(record_finding)
                .get(findings)
                .layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route("/v1/dr/action-items", get(action_items))
        .route(
            "/v1/dr/action-items/{id}",
            post(update_action_item).layer(DefaultBodyLimit::max(64 * 1024)),
        )
}

async fn create_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateDrPlanRequest>,
) -> Result<Json<DrPlan>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.dr.create_plan(&auth, &request).await.map(Json)
}

async fn plans(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DrPlanQuery>,
) -> Result<Json<DrPlanPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.dr.plans(&auth, &query).await.map(Json)
}

async fn upsert_backup_asset(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpsertDrBackupAssetRequest>,
) -> Result<Json<DrBackupAsset>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .dr
        .upsert_backup_asset(&auth, parse_plan_id(&id)?, &request)
        .await
        .map(Json)
}

async fn backup_assets(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<DrBackupAssetPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.dr.backup_assets(&auth, parse_plan_id(&id)?).await.map(Json)
}

async fn create_exercise(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StartDrExerciseRequest>,
) -> Result<Json<DrExercise>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.dr.create_exercise(&auth, &request).await.map(Json)
}

async fn exercises(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DrExerciseQuery>,
) -> Result<Json<DrExercisePage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.dr.exercises(&auth, &query).await.map(Json)
}

async fn transition_exercise(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<TransitionDrExerciseRequest>,
) -> Result<Json<DrExercise>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .dr
        .transition_exercise(&auth, parse_exercise_id(&id)?, &request)
        .await
        .map(Json)
}

async fn record_checkpoint(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<RecordRecoveryCheckpointRequest>,
) -> Result<Json<RecoveryCheckpoint>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .dr
        .record_checkpoint(&auth, parse_exercise_id(&id)?, &request)
        .await
        .map(Json)
}

async fn checkpoints(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<RecoveryCheckpointPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.dr.checkpoints(&auth, parse_exercise_id(&id)?).await.map(Json)
}

async fn record_finding(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<RecordDrFindingRequest>,
) -> Result<Json<DrFinding>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .dr
        .record_finding(&auth, parse_exercise_id(&id)?, &request)
        .await
        .map(Json)
}

async fn findings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<DrFindingPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.dr.findings(&auth, parse_exercise_id(&id)?).await.map(Json)
}

async fn action_items(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DrActionItemQuery>,
) -> Result<Json<DrActionItemPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.dr.action_items(&auth, &query).await.map(Json)
}

async fn update_action_item(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdateDrActionItemRequest>,
) -> Result<Json<DrActionItem>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .dr
        .update_action_item(&auth, parse_action_item_id(&id)?, &request)
        .await
        .map(Json)
}

fn parse_plan_id(value: &str) -> Result<DrPlanId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_dr_plan_id", "DR plan ID must be a UUID"))
}

fn parse_exercise_id(value: &str) -> Result<DrExerciseId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_dr_exercise_id", "DR exercise ID must be a UUID"))
}

fn parse_action_item_id(value: &str) -> Result<DrActionItemId, ControlPlaneError> {
    value.parse().map_err(|_| {
        ControlPlaneError::validation("invalid_dr_action_item_id", "DR action item ID must be a UUID")
    })
}
