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
use crate::model::LoginRequest;
use crate::model::SessionView;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::State;

pub async fn session(State(state): State<AppState>) -> Result<Json<ApiResponse<SessionView>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::session(&state).await?)))
}

pub async fn login(
    State(state): State<AppState>,
    Json(request): Json<LoginRequest>,
) -> Result<Json<ApiResponse<SessionView>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::login(&state, request).await?)))
}

pub async fn logout(State(state): State<AppState>) -> Result<Json<ApiResponse<SessionView>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::logout(&state).await?)))
}
