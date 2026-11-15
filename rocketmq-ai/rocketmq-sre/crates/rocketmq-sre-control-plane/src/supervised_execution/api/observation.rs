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
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::extract::rejection::QueryRejection;
use axum::http::HeaderMap;
use rocketmq_sre_contracts::ExecutionLookupRequest;
use rocketmq_sre_contracts::ExecutionLookupView;
use rocketmq_sre_contracts::ExecutionObservationView;
use rocketmq_sre_contracts::PlanExecutionPage;
use serde::Deserialize;

use crate::ControlPlaneRequestFailure;
use crate::api::AppState;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PlanExecutionQuery {
    plan_hash: String,
}

pub(super) async fn execution_observation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ExecutionObservationView>, ControlPlaneRequestFailure> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .execution_observation(&auth, super::parse_execution_id(&id)?)
        .await
        .map(Json)
}

pub(super) async fn lookup(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Result<Json<ExecutionLookupRequest>, JsonRejection>,
) -> Result<Json<ExecutionLookupView>, ControlPlaneRequestFailure> {
    let Json(request) = request.map_err(|_| invalid_selector())?;
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .lookup_execution(&auth, &request)
        .await
        .map(Json)
}

pub(super) async fn plan_executions(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    query: Result<Query<PlanExecutionQuery>, QueryRejection>,
) -> Result<Json<PlanExecutionPage>, ControlPlaneRequestFailure> {
    let Query(query) = query.map_err(|_| invalid_selector())?;
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .supervised_execution
        .plan_executions(&auth, super::parse_plan_id(&id)?, &query.plan_hash)
        .await
        .map(Json)
}

fn invalid_selector() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::validation(
        "invalid_execution_lookup",
        "a bounded exact execution selector is required",
    )
}
