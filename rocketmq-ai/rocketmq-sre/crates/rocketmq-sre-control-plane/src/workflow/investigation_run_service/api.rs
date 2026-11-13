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

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use rocketmq_sre_contracts::{
    CreateInvestigationRunRequest, CreateInvestigationRunResponse, InvestigationCommandReceipt,
    InvestigationCommandRequest, InvestigationId, InvestigationRun, InvestigationRunEventPage, InvestigationRunId,
    InvestigationRunStatus,
};
use serde::Deserialize;

use crate::{ControlPlaneRequestFailure, api::AppState};

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/investigations/{id}/runs", post(create))
        .route("/v1/investigation-runs/{id}", get(snapshot))
        .route("/v1/investigation-runs/{id}/commands", post(command))
        .route("/v1/investigation-runs/{id}/events", get(events))
        .layer(DefaultBodyLimit::max(32 * 1024))
}

async fn create(
    State(state): State<AppState>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    headers: HeaderMap,
    payload: Result<Json<CreateInvestigationRunRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<CreateInvestigationRunResponse>), ControlPlaneRequestFailure> {
    let auth = state.auth.authorize(&headers, None).await?;
    let Path(id) = path.map_err(|_| invalid_input())?;
    let Json(request) = payload.map_err(|_| invalid_input())?;
    let id = id.parse::<InvestigationId>().map_err(|_| invalid_input())?;
    let response = state.investigation_runs.create(&auth, id, &request).await?;
    Ok((
        if response.replayed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(response),
    ))
}

async fn snapshot(
    State(state): State<AppState>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    headers: HeaderMap,
) -> Result<Json<InvestigationRun>, ControlPlaneRequestFailure> {
    let auth = state.auth.authorize(&headers, None).await?;
    let Path(id) = path.map_err(|_| invalid_input())?;
    state.investigation_runs.get(&auth, parse_run_id(&id)?).await.map(Json)
}

async fn command(
    State(state): State<AppState>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    headers: HeaderMap,
    payload: Result<Json<InvestigationCommandRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<InvestigationCommandReceipt>), ControlPlaneRequestFailure> {
    let auth = state.auth.authorize(&headers, None).await?;
    let Path(id) = path.map_err(|_| invalid_input())?;
    let Json(request) = payload.map_err(|_| invalid_input())?;
    let outcome = state
        .investigation_runs
        .command(&auth, parse_run_id(&id)?, &request)
        .await?;
    let draining = matches!(
        outcome.receipt.status,
        InvestigationRunStatus::Pausing | InvestigationRunStatus::Cancelling
    );
    Ok((
        if !outcome.replayed && draining {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        },
        Json(outcome.receipt),
    ))
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventsQuery {
    after: Option<String>,
    limit: Option<u32>,
}

async fn events(
    State(state): State<AppState>,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    headers: HeaderMap,
    query: Result<Query<EventsQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<InvestigationRunEventPage>, ControlPlaneRequestFailure> {
    let auth = state.auth.authorize(&headers, None).await?;
    let Path(id) = path.map_err(|_| invalid_input())?;
    let Query(query) = query.map_err(|_| invalid_input())?;
    let after = query.after.as_deref().map(parse_sequence).transpose()?.unwrap_or(0);
    let limit = query.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(invalid_input());
    }
    state
        .investigation_runs
        .events(&auth, parse_run_id(&id)?, after, limit)
        .await
        .map(Json)
}

fn parse_sequence(value: &str) -> Result<u64, ControlPlaneRequestFailure> {
    let parsed = value.parse::<u64>().map_err(|_| invalid_input())?;
    if parsed.to_string() != value {
        return Err(invalid_input());
    }
    Ok(parsed)
}

fn parse_run_id(value: &str) -> Result<InvestigationRunId, ControlPlaneRequestFailure> {
    value.parse().map_err(|_| invalid_input())
}

fn invalid_input() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::validation("invalid_input", "invalid run request")
}
