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
use crate::model::ConsumerListView;
use crate::model::ConsumerProgress;
use crate::model::ConsumerResetOffsetRequest;
use crate::model::MutationResult;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Path;
use axum::extract::State;

pub async fn list_consumers(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<ConsumerListView>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::list_consumers(&state).await?)))
}

pub async fn consumer_progress(
    State(state): State<AppState>,
    Path(group): Path<String>,
) -> Result<Json<ApiResponse<ConsumerProgress>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::consumer_progress(&state, &group).await?,
    )))
}

pub async fn reset_offset(
    State(state): State<AppState>,
    Path(group): Path<String>,
    Json(request): Json<ConsumerResetOffsetRequest>,
) -> Result<Json<ApiResponse<MutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::reset_consumer_offset(&state, &group, request).await?,
    )))
}
