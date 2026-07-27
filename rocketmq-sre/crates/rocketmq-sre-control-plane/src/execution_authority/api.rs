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
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use rocketmq_sre_contracts::ActivateLeaseRequest;
use rocketmq_sre_contracts::BeginLeaseTakeoverRequest;
use rocketmq_sre_contracts::BeginLeaseTakeoverResponse;
use rocketmq_sre_contracts::ExecutorLease;
use rocketmq_sre_contracts::GrantVerification;
use rocketmq_sre_contracts::IssueFenceGrantRequest;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::VerifyExecutionRequest;
use rocketmq_sre_contracts::VerifyFenceGrantRequest;
use rocketmq_sre_contracts::VerifyReconcileGrantRequest;

use crate::ControlPlaneError;
use crate::api::AppState;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/internal/v1/execution-authority/leases/takeover", post(begin_takeover))
        .route("/internal/v1/execution-authority/leases/activate", post(activate))
        .route(
            "/internal/v1/execution-authority/leases/fence-grant",
            post(issue_fence_grant),
        )
        .route(
            "/internal/v1/execution-authority/verify/execution",
            post(verify_execution),
        )
        .route(
            "/internal/v1/execution-authority/verify/fence-grant",
            post(verify_fence_grant),
        )
        .route(
            "/internal/v1/execution-authority/verify/reconcile-grant",
            post(verify_reconcile_grant),
        )
}

async fn begin_takeover(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<BeginLeaseTakeoverRequest>,
) -> Result<Json<BeginLeaseTakeoverResponse>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.lease_authority.begin_takeover(&auth, &request).await.map(Json)
}

async fn activate(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ActivateLeaseRequest>,
) -> Result<Json<ExecutorLease>, ControlPlaneError> {
    let lease = state.lease_authority.repository.lease(request.lease_id).await?;
    let auth = state.auth.authorize(&headers, Some(lease.cluster_id)).await?;
    state.lease_authority.activate(&auth, &request).await.map(Json)
}

async fn issue_fence_grant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<IssueFenceGrantRequest>,
) -> Result<Json<LeaseFenceGrant>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.cluster_id)).await?;
    state.lease_authority.issue_fence_grant(&auth, &request).await.map(Json)
}

async fn verify_execution(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<VerifyExecutionRequest>,
) -> Result<Json<GrantVerification>, ControlPlaneError> {
    let auth = state
        .auth
        .authorize(&headers, Some(request.execution.cluster_id))
        .await?;
    state.lease_authority.verify_execution(&auth, &request).await.map(Json)
}

async fn verify_fence_grant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<VerifyFenceGrantRequest>,
) -> Result<Json<GrantVerification>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.grant.cluster_id)).await?;
    state
        .lease_authority
        .verify_fence_grant(&auth, &request)
        .await
        .map(Json)
}

async fn verify_reconcile_grant(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<VerifyReconcileGrantRequest>,
) -> Result<Json<GrantVerification>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(request.grant.cluster_id)).await?;
    state
        .lease_authority
        .verify_reconcile_grant(&auth, &request)
        .await
        .map(Json)
}
