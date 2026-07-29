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
use rocketmq_sre_contracts::ClusterRegistration;
use rocketmq_sre_contracts::FleetAssetIndex;
use rocketmq_sre_contracts::FleetInspectionRun;
use rocketmq_sre_contracts::FleetInspectionRunId;
use rocketmq_sre_contracts::RegionalEndpoint;

use super::model::ClusterRegistrationPage;
use super::model::ComplianceEvaluationView;
use super::model::ComplianceFindingPage;
use super::model::ComplianceFindingQuery;
use super::model::CreateFleetInspectionRequest;
use super::model::CreateQuotaPolicyRequest;
use super::model::EvaluateComplianceRequest;
use super::model::EvaluateFleetQuotaRequest;
use super::model::FleetAssetPage;
use super::model::FleetInspectionPage;
use super::model::FleetInspectionQuery;
use super::model::FleetOffboardRequest;
use super::model::FleetOnboardingRequest;
use super::model::FleetOnboardingView;
use super::model::FleetOverview;
use super::model::FleetQuotaDecisionPage;
use super::model::FleetQuotaDecisionQuery;
use super::model::FleetQuotaDecisionView;
use super::model::FleetScopeQuery;
use super::model::QuotaPolicyQuery;
use super::model::QuotaPolicyView;
use super::model::RegionalEndpointPage;
use super::model::RegionalEndpointQuery;
use super::model::RegionalRouteDecision;
use super::model::RegionalRouteRequest;
use super::model::RegisterRegionalEndpointRequest;
use super::model::UpdateFleetInspectionRequest;
use super::model::UpsertFleetAssetRequest;
use crate::ControlPlaneError;
use crate::api::AppState;

const FLEET_WRITE_BODY_LIMIT: usize = 256 * 1024;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/fleet/overview", get(overview))
        .route("/v1/fleet/clusters", get(registrations))
        .route(
            "/v1/fleet/onboarding/assess",
            post(assess_onboarding).layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route(
            "/v1/fleet/onboarding/register",
            post(onboard_cluster).layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route(
            "/v1/fleet/clusters/{id}/offboard",
            post(offboard_cluster).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route(
            "/v1/fleet/quotas",
            post(create_quota_policy)
                .get(quota_policy)
                .layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/fleet/regional-endpoints",
            post(register_endpoint)
                .get(endpoints)
                .layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route(
            "/v1/fleet/quotas/evaluate",
            post(evaluate_quota).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/v1/fleet/quotas/decisions", get(quota_decisions))
        .route(
            "/v1/fleet/regional-route",
            post(route).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/fleet/assets",
            post(upsert_asset)
                .get(assets)
                .layer(DefaultBodyLimit::max(FLEET_WRITE_BODY_LIMIT)),
        )
        .route(
            "/v1/fleet/compliance",
            post(evaluate_compliance)
                .get(findings)
                .layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route(
            "/v1/fleet/inspections",
            post(create_inspection)
                .get(inspections)
                .layer(DefaultBodyLimit::max(FLEET_WRITE_BODY_LIMIT)),
        )
        .route(
            "/v1/fleet/inspections/{id}/progress",
            post(update_inspection).layer(DefaultBodyLimit::max(16 * 1024)),
        )
}

async fn assess_onboarding(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<FleetOnboardingRequest>,
) -> Result<Json<FleetOnboardingView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.fleet.assess_onboarding(&auth, &request).await.map(Json)
}

async fn onboard_cluster(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<FleetOnboardingRequest>,
) -> Result<Json<FleetOnboardingView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.fleet.onboard_cluster(&auth, &request).await.map(Json)
}

async fn offboard_cluster(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<FleetOffboardRequest>,
) -> Result<Json<ClusterRegistration>, ControlPlaneError> {
    let cluster_id = parse_cluster_id(&id)?;
    let auth = state.auth.authorize(&headers, Some(cluster_id)).await?;
    state
        .fleet
        .offboard_cluster(&auth, cluster_id, &request)
        .await
        .map(Json)
}

async fn overview(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<FleetOverview>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.fleet.overview(&auth).await.map(Json)
}

async fn registrations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FleetScopeQuery>,
) -> Result<Json<ClusterRegistrationPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.fleet.registrations(&auth, &query).await.map(Json)
}

async fn create_quota_policy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateQuotaPolicyRequest>,
) -> Result<Json<QuotaPolicyView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.fleet.create_quota_policy(&auth, &request).await.map(Json)
}

async fn quota_policy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<QuotaPolicyQuery>,
) -> Result<Json<QuotaPolicyView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.fleet.quota_policy(&auth, query.cluster_id).await.map(Json)
}

async fn evaluate_quota(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<EvaluateFleetQuotaRequest>,
) -> Result<Json<FleetQuotaDecisionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.fleet.evaluate_quota(&auth, &request).await.map(Json)
}

async fn quota_decisions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FleetQuotaDecisionQuery>,
) -> Result<Json<FleetQuotaDecisionPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.fleet.quota_decisions(&auth, &query).await.map(Json)
}

async fn register_endpoint(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RegisterRegionalEndpointRequest>,
) -> Result<Json<RegionalEndpoint>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.endpoint.cluster_id).await?;
    state.fleet.register_endpoint(&auth, &request).await.map(Json)
}

async fn endpoints(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<RegionalEndpointQuery>,
) -> Result<Json<RegionalEndpointPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.fleet.endpoints(&auth, &query).await.map(Json)
}

async fn route(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RegionalRouteRequest>,
) -> Result<Json<RegionalRouteDecision>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.fleet.route(&auth, &request).await.map(Json)
}

async fn upsert_asset(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<UpsertFleetAssetRequest>,
) -> Result<Json<FleetAssetIndex>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.asset.cluster_id)).await?;
    state.fleet.upsert_asset(&auth, &request).await.map(Json)
}

async fn assets(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FleetScopeQuery>,
) -> Result<Json<FleetAssetPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.fleet.assets(&auth, &query).await.map(Json)
}

async fn evaluate_compliance(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<EvaluateComplianceRequest>,
) -> Result<Json<ComplianceEvaluationView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.fleet.evaluate_compliance(&auth, &request).await.map(Json)
}

async fn findings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ComplianceFindingQuery>,
) -> Result<Json<ComplianceFindingPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.fleet.findings(&auth, &query).await.map(Json)
}

async fn create_inspection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateFleetInspectionRequest>,
) -> Result<Json<FleetInspectionRun>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.fleet.create_inspection(&auth, &request).await.map(Json)
}

async fn update_inspection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdateFleetInspectionRequest>,
) -> Result<Json<FleetInspectionRun>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .fleet
        .update_inspection(&auth, parse_inspection_id(&id)?, &request)
        .await
        .map(Json)
}

async fn inspections(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FleetInspectionQuery>,
) -> Result<Json<FleetInspectionPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.fleet.inspections(&auth, query.limit).await.map(Json)
}

fn parse_inspection_id(value: &str) -> Result<FleetInspectionRunId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "Fleet inspection identifier must be a UUID"))
}

fn parse_cluster_id(value: &str) -> Result<rocketmq_sre_contracts::ClusterId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "cluster identifier must be a UUID"))
}
