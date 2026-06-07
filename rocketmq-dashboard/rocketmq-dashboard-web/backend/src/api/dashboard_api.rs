// Copyright 2023 The RocketMQ Rust Authors
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
use crate::error::DashboardError;
use crate::model::ApiResponse;
use crate::model::DashboardHistoryQuery;
use crate::model::DashboardHistorySeries;
use crate::model::DashboardOverview;
use crate::model::DashboardTopicCurrent;
use crate::service::broker_history as broker_history_service;
use crate::service::overview as overview_service;
use crate::service::topic_current as topic_current_service;
use crate::service::topic_history as topic_history_service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Query;
use axum::extract::State;

pub async fn overview(State(state): State<AppState>) -> Result<Json<ApiResponse<DashboardOverview>>, DashboardError> {
    Ok(Json(ApiResponse::success(overview_service(&state).await?)))
}

pub async fn topic_current(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<DashboardTopicCurrent>>, DashboardError> {
    Ok(Json(ApiResponse::success(topic_current_service(&state).await?)))
}

pub async fn broker_history(
    State(state): State<AppState>,
    Query(query): Query<DashboardHistoryQuery>,
) -> Result<Json<ApiResponse<DashboardHistorySeries>>, DashboardError> {
    Ok(Json(ApiResponse::success(broker_history_service(&state, query).await?)))
}

pub async fn topic_history(
    State(state): State<AppState>,
    Query(query): Query<DashboardHistoryQuery>,
) -> Result<Json<ApiResponse<DashboardHistorySeries>>, DashboardError> {
    Ok(Json(ApiResponse::success(topic_history_service(&state, query).await?)))
}
