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
use rocketmq_sre_contracts::FinOpsBudget;
use rocketmq_sre_contracts::FinOpsCostEntry;
use rocketmq_sre_contracts::FinOpsReport;

use super::model::CreateFinOpsAllocationPolicyRequest;
use super::model::CreateFinOpsBudgetRequest;
use super::model::EvaluateFinOpsBudgetRequest;
use super::model::FinOpsAllocationPolicyView;
use super::model::FinOpsBudgetDecisionView;
use super::model::FinOpsBudgetPage;
use super::model::FinOpsBudgetQuery;
use super::model::FinOpsLedgerPage;
use super::model::FinOpsLedgerQuery;
use super::model::FinOpsReportQuery;
use super::model::RecordFinOpsCostRequest;
use crate::ControlPlaneError;
use crate::api::AppState;

const FINOPS_WRITE_BODY_LIMIT: usize = 128 * 1024;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/finops/ledger",
            post(record_cost)
                .get(ledger)
                .layer(DefaultBodyLimit::max(FINOPS_WRITE_BODY_LIMIT)),
        )
        .route(
            "/v1/finops/budgets",
            post(create_budget)
                .get(budgets)
                .layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/finops/budgets/evaluate",
            post(evaluate_budget).layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route(
            "/v1/finops/allocation-policy",
            post(create_allocation_policy)
                .get(allocation_policy)
                .layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route("/v1/finops/report", get(report))
}

async fn record_cost(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RecordFinOpsCostRequest>,
) -> Result<Json<FinOpsCostEntry>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.finops.record_cost(&auth, &request).await.map(Json)
}

async fn ledger(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FinOpsLedgerQuery>,
) -> Result<Json<FinOpsLedgerPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.finops.ledger(&auth, &query).await.map(Json)
}

async fn create_budget(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateFinOpsBudgetRequest>,
) -> Result<Json<FinOpsBudget>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.finops.create_budget(&auth, &request).await.map(Json)
}

async fn budgets(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FinOpsBudgetQuery>,
) -> Result<Json<FinOpsBudgetPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.finops.budgets(&auth, &query).await.map(Json)
}

async fn evaluate_budget(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<EvaluateFinOpsBudgetRequest>,
) -> Result<Json<FinOpsBudgetDecisionView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.finops.evaluate_budget(&auth, &request).await.map(Json)
}

async fn create_allocation_policy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateFinOpsAllocationPolicyRequest>,
) -> Result<Json<FinOpsAllocationPolicyView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.finops.create_allocation_policy(&auth, &request).await.map(Json)
}

async fn allocation_policy(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<FinOpsAllocationPolicyView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.finops.allocation_policy(&auth).await.map(Json)
}

async fn report(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FinOpsReportQuery>,
) -> Result<Json<FinOpsReport>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.finops.report(&auth, &query).await.map(Json)
}
