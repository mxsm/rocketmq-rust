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
use crate::model::ConsumerMonitorMutationResult;
use crate::model::ConsumerMonitorUpsertRequest;
use crate::model::ConsumerMonitorView;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Path;
use axum::extract::State;

pub async fn list_consumer_monitors(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<ConsumerMonitorView>>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::list_consumer_monitors(&state).await?,
    )))
}

pub async fn create_consumer_monitor(
    State(state): State<AppState>,
    Json(payload): Json<ConsumerMonitorUpsertRequest>,
) -> Result<Json<ApiResponse<ConsumerMonitorMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::create_or_update_consumer_monitor(&state, payload).await?,
    )))
}

pub async fn delete_consumer_monitor(
    State(state): State<AppState>,
    Path(consumer_group): Path<String>,
) -> Result<Json<ApiResponse<ConsumerMonitorMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::delete_consumer_monitor(&state, &consumer_group).await?,
    )))
}
