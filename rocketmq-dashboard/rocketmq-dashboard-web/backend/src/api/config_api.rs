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
use crate::model::AddressRequest;
use crate::model::ApiResponse;
use crate::model::BoolSettingRequest;
use crate::model::ConfigMutationResult;
use crate::model::DashboardConfigView;
use crate::model::EndpointId;
use crate::model::EndpointRequest;
use crate::model::ExpectedRevision;
use crate::model::NameserverAvailabilityView;
use crate::model::NameserverListRequest;
use crate::service::delete_nameserver as delete_nameserver_service;
use crate::service::delete_proxy as delete_proxy_service;
use crate::service::get_config as get_config_service;
use crate::service::set_tls as set_tls_service;
use crate::service::set_vip_channel as set_vip_channel_service;
use crate::service::switch_nameserver as switch_nameserver_service;
use crate::service::switch_proxy as switch_proxy_service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;

pub async fn get_config(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<DashboardConfigView>>, DashboardError> {
    Ok(Json(ApiResponse::success(get_config_service(&state).await?)))
}

pub async fn get_nameserver_availability(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<NameserverAvailabilityView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        crate::service::get_nameserver_availability(&state).await?,
    )))
}

pub async fn replace_nameservers(
    State(state): State<AppState>,
    Json(request): Json<NameserverListRequest>,
) -> Result<Json<ApiResponse<ConfigMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        crate::service::replace_nameservers(&state, request).await?,
    )))
}

pub async fn add_nameserver(
    State(state): State<AppState>,
    Json(request): Json<AddressRequest>,
) -> Result<Json<ApiResponse<ConfigMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        crate::service::add_nameserver(&state, request).await?,
    )))
}

pub async fn switch_nameserver(
    State(state): State<AppState>,
    Json(request): Json<EndpointRequest>,
) -> Result<Json<ApiResponse<ConfigMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        switch_nameserver_service(&state, request).await?,
    )))
}

pub async fn delete_nameserver(
    State(state): State<AppState>,
    Path(endpoint_id): Path<EndpointId>,
    Query(expected_revision): Query<ExpectedRevision>,
) -> Result<Json<ApiResponse<ConfigMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        delete_nameserver_service(&state, &endpoint_id, expected_revision.expected_revision).await?,
    )))
}

pub async fn set_vip_channel(
    State(state): State<AppState>,
    Json(request): Json<BoolSettingRequest>,
) -> Result<Json<ApiResponse<ConfigMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        set_vip_channel_service(&state, request).await?,
    )))
}

pub async fn set_tls(
    State(state): State<AppState>,
    Json(request): Json<BoolSettingRequest>,
) -> Result<Json<ApiResponse<ConfigMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(set_tls_service(&state, request).await?)))
}

pub async fn add_proxy(
    State(state): State<AppState>,
    Json(request): Json<AddressRequest>,
) -> Result<Json<ApiResponse<ConfigMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        crate::service::add_proxy(&state, request).await?,
    )))
}

pub async fn switch_proxy(
    State(state): State<AppState>,
    Json(request): Json<EndpointRequest>,
) -> Result<Json<ApiResponse<ConfigMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(switch_proxy_service(&state, request).await?)))
}

pub async fn delete_proxy(
    State(state): State<AppState>,
    Path(endpoint_id): Path<EndpointId>,
    Query(expected_revision): Query<ExpectedRevision>,
) -> Result<Json<ApiResponse<ConfigMutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        delete_proxy_service(&state, &endpoint_id, expected_revision.expected_revision).await?,
    )))
}
