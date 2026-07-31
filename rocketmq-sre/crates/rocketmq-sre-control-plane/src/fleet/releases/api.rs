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
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FleetReleaseId;
use rocketmq_sre_contracts::FleetReleaseReport;

use super::model::CreateFleetReleaseRequest;
use super::model::FleetReleasePage;
use super::model::FleetReleaseQuery;
use super::model::FleetReleaseReasonRequest;
use super::model::FleetReleaseView;
use super::model::RecordFleetTargetOutcomeRequest;
use super::model::RecordFleetTargetReadinessRequest;
use super::model::StartFleetReleaseBatchRequest;
use crate::ControlPlaneError;
use crate::api::AppState;

const FLEET_RELEASE_BODY_LIMIT: usize = 256 * 1024;

pub(in crate::fleet) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/fleet/releases",
            post(create_release)
                .get(releases)
                .layer(DefaultBodyLimit::max(FLEET_RELEASE_BODY_LIMIT)),
        )
        .route("/v1/fleet/releases/{id}", get(release))
        .route(
            "/v1/fleet/releases/{id}/readiness/start",
            post(begin_readiness).layer(DefaultBodyLimit::max(1)),
        )
        .route(
            "/v1/fleet/releases/{id}/targets/{cluster_id}/readiness",
            post(record_readiness).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/fleet/releases/{id}/batches/{sequence}/start",
            post(start_batch).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/v1/fleet/releases/{id}/targets/{cluster_id}/outcome",
            post(record_outcome).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/fleet/releases/{id}/pause",
            post(pause).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/v1/fleet/releases/{id}/resume",
            post(resume).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/v1/fleet/releases/{id}/report", get(report))
}

async fn create_release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateFleetReleaseRequest>,
) -> Result<Json<FleetReleaseView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.fleet.create_fleet_release(&auth, &request).await.map(Json)
}

async fn releases(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FleetReleaseQuery>,
) -> Result<Json<FleetReleasePage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.fleet.fleet_releases(&auth, &query).await.map(Json)
}

async fn release(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<FleetReleaseView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.fleet.fleet_release(&auth, parse_release_id(&id)?).await.map(Json)
}

async fn begin_readiness(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<FleetReleaseView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .fleet
        .begin_fleet_release_readiness(&auth, parse_release_id(&id)?)
        .await
        .map(Json)
}

async fn record_readiness(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, cluster_id)): Path<(String, String)>,
    Json(request): Json<RecordFleetTargetReadinessRequest>,
) -> Result<Json<FleetReleaseView>, ControlPlaneError> {
    let cluster_id = parse_cluster_id(&cluster_id)?;
    let auth = state.auth.authorize(&headers, Some(cluster_id)).await?;
    state
        .fleet
        .record_fleet_target_readiness(&auth, parse_release_id(&id)?, cluster_id, &request)
        .await
        .map(Json)
}

async fn start_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, sequence)): Path<(String, u32)>,
    Json(request): Json<StartFleetReleaseBatchRequest>,
) -> Result<Json<FleetReleaseView>, ControlPlaneError> {
    if request.expected_sequence != sequence {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "Fleet release batch sequence does not match the path",
        ));
    }
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .fleet
        .start_fleet_release_batch(&auth, parse_release_id(&id)?, &request)
        .await
        .map(Json)
}

async fn record_outcome(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((id, cluster_id)): Path<(String, String)>,
    Json(request): Json<RecordFleetTargetOutcomeRequest>,
) -> Result<Json<FleetReleaseView>, ControlPlaneError> {
    let cluster_id = parse_cluster_id(&cluster_id)?;
    let auth = state.auth.authorize(&headers, Some(cluster_id)).await?;
    state
        .fleet
        .record_fleet_target_outcome(&auth, parse_release_id(&id)?, cluster_id, &request)
        .await
        .map(Json)
}

async fn pause(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<FleetReleaseReasonRequest>,
) -> Result<Json<FleetReleaseView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .fleet
        .pause_fleet_release(&auth, parse_release_id(&id)?, &request)
        .await
        .map(Json)
}

async fn resume(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<FleetReleaseReasonRequest>,
) -> Result<Json<FleetReleaseView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .fleet
        .resume_fleet_release(&auth, parse_release_id(&id)?, &request)
        .await
        .map(Json)
}

async fn report(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<FleetReleaseReport>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .fleet
        .fleet_release_report(&auth, parse_release_id(&id)?)
        .await
        .map(Json)
}

fn parse_release_id(value: &str) -> Result<FleetReleaseId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_fleet_release_id", "Fleet release ID must be a UUID"))
}

fn parse_cluster_id(value: &str) -> Result<ClusterId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_cluster_id", "cluster ID must be a UUID"))
}
