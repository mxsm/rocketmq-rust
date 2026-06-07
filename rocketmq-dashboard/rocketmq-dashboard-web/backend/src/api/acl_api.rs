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
use crate::model::AclMutationResult;
use crate::model::AclPolicyRequest;
use crate::model::AclPolicyView;
use crate::model::AclQuery;
use crate::model::AclUserUpsertRequest;
use crate::model::AclUserView;
use crate::model::ApiResponse;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;

pub async fn list_users(
    State(state): State<AppState>,
    Query(query): Query<AclQuery>,
) -> Result<Json<ApiResponse<Vec<AclUserView>>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::list_users(&state, query).await?)))
}

pub async fn create_user(
    State(state): State<AppState>,
    Json(payload): Json<AclUserUpsertRequest>,
) -> Result<Json<ApiResponse<AclMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::create_user(&state, payload).await?)))
}

pub async fn update_user(
    State(state): State<AppState>,
    Path(access_key): Path<String>,
    Json(payload): Json<AclUserUpsertRequest>,
) -> Result<Json<ApiResponse<AclMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::update_user(&state, &access_key, payload).await?,
    )))
}

pub async fn delete_user(
    State(state): State<AppState>,
    Path(access_key): Path<String>,
    Query(query): Query<AclQuery>,
) -> Result<Json<ApiResponse<AclMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::delete_user(&state, &access_key, query).await?,
    )))
}

pub async fn list_policies(
    State(state): State<AppState>,
    Query(query): Query<AclQuery>,
) -> Result<Json<ApiResponse<Vec<AclPolicyView>>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::list_policies(&state, query).await?)))
}

pub async fn create_policy(
    State(state): State<AppState>,
    Json(payload): Json<AclPolicyRequest>,
) -> Result<Json<ApiResponse<AclMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::create_policy(&state, payload).await?,
    )))
}

pub async fn update_policy(
    State(state): State<AppState>,
    Path(policy_name): Path<String>,
    Json(payload): Json<AclPolicyRequest>,
) -> Result<Json<ApiResponse<AclMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::update_policy(&state, &policy_name, payload).await?,
    )))
}

pub async fn delete_policy(
    State(state): State<AppState>,
    Path(policy_name): Path<String>,
    Query(query): Query<AclQuery>,
) -> Result<Json<ApiResponse<AclMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::delete_policy(&state, &policy_name, query).await?,
    )))
}
