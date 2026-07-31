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
use rocketmq_sre_contracts::GovernanceArtifact;
use rocketmq_sre_contracts::GovernanceArtifactId;
use rocketmq_sre_contracts::GovernanceImpact;
use rocketmq_sre_contracts::GovernanceVersion;
use rocketmq_sre_contracts::GovernanceVersionId;

use super::model::CreateGovernanceArtifactRequest;
use super::model::CreateGovernanceVersionRequest;
use super::model::EvaluateGovernanceAdmissionRequest;
use super::model::GovernanceAdmissionView;
use super::model::GovernanceArtifactPage;
use super::model::GovernanceArtifactQuery;
use super::model::GovernanceAuditExport;
use super::model::GovernanceAuditQuery;
use super::model::GovernanceComplianceReport;
use super::model::GovernanceImpactPage;
use super::model::GovernanceImpactQuery;
use super::model::GovernanceVersionPage;
use super::model::GovernanceVersionQuery;
use super::model::RecordGovernanceImpactRequest;
use super::model::TransitionGovernanceVersionRequest;
use crate::ControlPlaneError;
use crate::api::AppState;

const GOVERNANCE_WRITE_BODY_LIMIT: usize = 256 * 1024;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/governance/artifacts",
            post(create_artifact)
                .get(artifacts)
                .layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/governance/artifacts/{id}/versions",
            post(create_version)
                .get(versions)
                .layer(DefaultBodyLimit::max(GOVERNANCE_WRITE_BODY_LIMIT)),
        )
        .route(
            "/v1/governance/versions/{id}/transition",
            post(transition_version).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/governance/versions/{id}/impacts",
            post(record_impact).get(impacts).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/governance/admissions/evaluate",
            post(evaluate_admission).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route("/v1/governance/audit/export", get(audit_export))
        .route("/v1/governance/compliance", get(compliance))
}

async fn create_artifact(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateGovernanceArtifactRequest>,
) -> Result<Json<GovernanceArtifact>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.governance.create_artifact(&auth, &request).await.map(Json)
}

async fn artifacts(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<GovernanceArtifactQuery>,
) -> Result<Json<GovernanceArtifactPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.governance.artifacts(&auth, &query).await.map(Json)
}

async fn create_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<CreateGovernanceVersionRequest>,
) -> Result<Json<GovernanceVersion>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .governance
        .create_version(&auth, parse_artifact_id(&id)?, &request)
        .await
        .map(Json)
}

async fn versions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<GovernanceVersionQuery>,
) -> Result<Json<GovernanceVersionPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .governance
        .versions(&auth, parse_artifact_id(&id)?, &query)
        .await
        .map(Json)
}

async fn transition_version(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<TransitionGovernanceVersionRequest>,
) -> Result<Json<GovernanceVersion>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .governance
        .transition_version(&auth, parse_version_id(&id)?, &request)
        .await
        .map(Json)
}

async fn record_impact(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<RecordGovernanceImpactRequest>,
) -> Result<Json<GovernanceImpact>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state
        .governance
        .record_impact(&auth, parse_version_id(&id)?, &request)
        .await
        .map(Json)
}

async fn impacts(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<GovernanceImpactQuery>,
) -> Result<Json<GovernanceImpactPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state
        .governance
        .impacts(&auth, parse_version_id(&id)?, &query)
        .await
        .map(Json)
}

async fn evaluate_admission(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<EvaluateGovernanceAdmissionRequest>,
) -> Result<Json<GovernanceAdmissionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.governance.evaluate_admission(&auth, &request).await.map(Json)
}

async fn audit_export(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<GovernanceAuditQuery>,
) -> Result<Json<GovernanceAuditExport>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.governance.audit_export(&auth, &query).await.map(Json)
}

async fn compliance(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<GovernanceComplianceReport>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.governance.compliance(&auth).await.map(Json)
}

fn parse_artifact_id(value: &str) -> Result<GovernanceArtifactId, ControlPlaneError> {
    value.parse().map_err(|_| {
        ControlPlaneError::validation(
            "invalid_governance_artifact_id",
            "governance artifact ID must be a UUID",
        )
    })
}

fn parse_version_id(value: &str) -> Result<GovernanceVersionId, ControlPlaneError> {
    value.parse().map_err(|_| {
        ControlPlaneError::validation("invalid_governance_version_id", "governance version ID must be a UUID")
    })
}
