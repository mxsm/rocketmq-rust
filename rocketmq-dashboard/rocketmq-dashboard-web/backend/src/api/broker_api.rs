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
use crate::model::BrokerConfigUpdateRequest;
use crate::model::BrokerConfigView;
use crate::model::BrokerListView;
use crate::model::BrokerRuntimeStats;
use crate::model::MutationResult;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Path;
use axum::extract::State;

pub async fn list_brokers(State(state): State<AppState>) -> Result<Json<ApiResponse<BrokerListView>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::list_brokers(&state).await?)))
}

pub async fn broker_runtime(
    State(state): State<AppState>,
    Path(broker_name): Path<String>,
) -> Result<Json<ApiResponse<BrokerRuntimeStats>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::broker_runtime(&state, &broker_name).await?,
    )))
}

pub async fn broker_config(
    State(state): State<AppState>,
    Path(broker_name): Path<String>,
) -> Result<Json<ApiResponse<BrokerConfigView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::broker_config(&state, &broker_name).await?,
    )))
}

pub async fn update_broker_config(
    State(state): State<AppState>,
    Path(broker_name): Path<String>,
    Json(request): Json<BrokerConfigUpdateRequest>,
) -> Result<Json<ApiResponse<MutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::update_broker_config(&state, &broker_name, request).await?,
    )))
}
