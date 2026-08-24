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
use crate::middleware::AuditTerminalFactSink;
use crate::model::ApiResponse;
use crate::model::ConsumerBrokerListView;
use crate::model::ConsumerConfigView;
use crate::model::ConsumerConnectionView;
use crate::model::ConsumerDeleteView;
use crate::model::ConsumerGroupListView;
use crate::model::ConsumerJStackView;
use crate::model::ConsumerOperationResult;
use crate::model::ConsumerProgressView;
use crate::model::ConsumerQuery;
use crate::model::ConsumerResetOffsetRequest;
use crate::model::ConsumerRunningInfoView;
use crate::model::ConsumerSummaryView;
use crate::model::ConsumerUpsertView;
use crate::model::MutationResult;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Extension;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;

pub async fn list_consumers(
    State(state): State<AppState>,
    Query(query): Query<ConsumerQuery>,
) -> Result<Json<ApiResponse<ConsumerGroupListView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::list_consumers(&state, query).await?,
    )))
}

pub async fn consumer_summary(
    State(state): State<AppState>,
    Path(group): Path<String>,
    Query(query): Query<ConsumerQuery>,
) -> Result<Json<ApiResponse<ConsumerSummaryView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::consumer_summary(&state, &group, query).await?,
    )))
}

pub async fn consumer_connections(
    State(state): State<AppState>,
    Path(group): Path<String>,
    Query(query): Query<ConsumerQuery>,
) -> Result<Json<ApiResponse<ConsumerConnectionView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::consumer_connections(&state, &group, query).await?,
    )))
}

pub async fn consumer_progress(
    State(state): State<AppState>,
    Path(group): Path<String>,
    Query(query): Query<ConsumerQuery>,
) -> Result<Json<ApiResponse<ConsumerProgressView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::consumer_progress(&state, &group, query).await?,
    )))
}

pub async fn consumer_config(
    State(state): State<AppState>,
    Path(group): Path<String>,
    Query(query): Query<ConsumerQuery>,
) -> Result<Json<ApiResponse<ConsumerConfigView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::consumer_config(&state, &group, query).await?,
    )))
}

pub async fn consumer_running_info(
    State(state): State<AppState>,
    Path((group, client_id)): Path<(String, String)>,
    Query(query): Query<ConsumerQuery>,
) -> Result<Json<ApiResponse<ConsumerRunningInfoView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::consumer_running_info(&state, &group, &client_id, query).await?,
    )))
}

pub async fn consumer_jstack(
    State(state): State<AppState>,
    Path((group, client_id)): Path<(String, String)>,
    Query(query): Query<ConsumerQuery>,
) -> Result<Json<ApiResponse<ConsumerJStackView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::consumer_jstack(&state, &group, &client_id, query).await?,
    )))
}

pub async fn consumer_brokers(
    State(state): State<AppState>,
    Path(group): Path<String>,
) -> Result<Json<ApiResponse<ConsumerBrokerListView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::consumer_brokers(&state, &group).await?,
    )))
}

pub async fn create_consumer(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Json(request): Json<ConsumerUpsertView>,
) -> Result<Json<ApiResponse<ConsumerOperationResult>>, DashboardError> {
    let group = request
        .consumer_group
        .clone()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| DashboardError::Validation("Consumer group is required".to_string()))?;
    let result = service::create_consumer(&state, &group, request).await?;
    record_consumer_operation(&audit, &state, &result).await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn update_consumer(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Path(group): Path<String>,
    Json(request): Json<ConsumerUpsertView>,
) -> Result<Json<ApiResponse<ConsumerOperationResult>>, DashboardError> {
    let result = service::update_consumer(&state, &group, request).await?;
    record_consumer_operation(&audit, &state, &result).await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn delete_consumer(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Path(group): Path<String>,
    Json(request): Json<ConsumerDeleteView>,
) -> Result<Json<ApiResponse<ConsumerOperationResult>>, DashboardError> {
    let result = service::delete_consumer(&state, &group, request).await?;
    record_consumer_operation(&audit, &state, &result).await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn reset_offset(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Path(group): Path<String>,
    Json(request): Json<ConsumerResetOffsetRequest>,
) -> Result<Json<ApiResponse<MutationResult>>, DashboardError> {
    let result = service::reset_consumer_offset(&state, &group, request).await?;
    let environment_id = Some(state.published().environment.environment_id);
    audit.record_success(Some(&group), environment_id).await;
    Ok(Json(ApiResponse::success(result)))
}

async fn record_consumer_operation(audit: &AuditTerminalFactSink, state: &AppState, result: &ConsumerOperationResult) {
    let environment_id = Some(state.published().environment.environment_id);
    if result.success {
        audit.record_success(Some(&result.consumer_group), environment_id).await;
    } else {
        audit.record_failed(Some(&result.consumer_group), environment_id).await;
    }
}
