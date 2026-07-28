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
use rocketmq_sre_contracts::AutomationOperatorFeedback;
use rocketmq_sre_contracts::NoSideEffectAutomationRequest;
use rocketmq_sre_contracts::NoSideEffectAutomationRun;

use super::model::AutomationRunListQuery;
use super::model::AutomationRunPage;
use super::model::RecordAutomationFeedbackRequest;
use crate::ControlPlaneError;
use crate::api::AppState;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/automation/no-side-effect/runs",
            get(list_runs).post(submit_run).layer(DefaultBodyLimit::max(128 * 1024)),
        )
        .route(
            "/v1/automation/feedback",
            post(record_feedback).layer(DefaultBodyLimit::max(16 * 1024)),
        )
}

async fn submit_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<NoSideEffectAutomationRequest>,
) -> Result<Json<NoSideEffectAutomationRun>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.automation.submit(&auth, &request).await.map(Json)
}

async fn list_runs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AutomationRunListQuery>,
) -> Result<Json<AutomationRunPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.automation.list(&auth, &query).await.map(Json)
}

async fn record_feedback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RecordAutomationFeedbackRequest>,
) -> Result<Json<AutomationOperatorFeedback>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, request.cluster_id).await?;
    state.automation.record_feedback(&auth, &request).await.map(Json)
}
