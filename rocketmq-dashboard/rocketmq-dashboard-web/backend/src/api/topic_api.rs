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
use crate::model::TopicConfigView;
use crate::model::TopicConsumersView;
use crate::model::TopicInfo;
use crate::model::TopicListView;
use crate::model::TopicMutationRequest;
use crate::model::TopicOffsetResult;
use crate::model::TopicOperationResult;
use crate::model::TopicResetOffsetRequest;
use crate::model::TopicRouteInfo;
use crate::model::TopicSendResultView;
use crate::model::TopicSkipOffsetRequest;
use crate::model::TopicStatsInfo;
use crate::model::TopicTestMessageRequest;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Extension;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigQuery {
    broker_name: Option<String>,
}

pub async fn list_topics(State(state): State<AppState>) -> Result<Json<ApiResponse<TopicListView>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::list_topics(&state).await?)))
}

pub async fn get_topic(
    State(state): State<AppState>,
    Path(topic): Path<String>,
) -> Result<Json<ApiResponse<TopicInfo>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::get_topic(&state, &topic).await?)))
}

pub async fn create_topic(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Json(request): Json<TopicMutationRequest>,
) -> Result<Json<ApiResponse<TopicOperationResult>>, DashboardError> {
    let result = service::create_topic(&state, request).await?;
    record_topic_operation(&audit, &state, &result).await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn update_topic(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Path(topic): Path<String>,
    Json(mut request): Json<TopicMutationRequest>,
) -> Result<Json<ApiResponse<TopicOperationResult>>, DashboardError> {
    request.topic = topic;
    let result = service::create_or_update_topic(&state, request).await?;
    record_topic_operation(&audit, &state, &result).await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn delete_topic(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Path(topic): Path<String>,
) -> Result<Json<ApiResponse<TopicOperationResult>>, DashboardError> {
    let result = service::delete_topic(&state, &topic).await?;
    record_topic_operation(&audit, &state, &result).await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn send_topic_test_message(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Path(topic): Path<String>,
    Json(request): Json<TopicTestMessageRequest>,
) -> Result<Json<ApiResponse<TopicSendResultView>>, DashboardError> {
    let result = service::send_topic_test_message(&state, &topic, request).await?;
    record_terminal_outcome(&audit, &state, &result.topic, result.success).await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn reset_topic_consumer_offset(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Path(topic): Path<String>,
    Json(request): Json<TopicResetOffsetRequest>,
) -> Result<Json<ApiResponse<TopicOffsetResult>>, DashboardError> {
    let result = service::reset_topic_consumer_offset(&state, &topic, request).await?;
    record_terminal_outcome(&audit, &state, &result.topic, result.success).await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn skip_topic_consumer_offset(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Path(topic): Path<String>,
    Json(request): Json<TopicSkipOffsetRequest>,
) -> Result<Json<ApiResponse<TopicOffsetResult>>, DashboardError> {
    let result = service::skip_topic_consumer_offset(&state, &topic, request).await?;
    record_terminal_outcome(&audit, &state, &result.topic, result.success).await;
    Ok(Json(ApiResponse::success(result)))
}

pub async fn delete_topic_from_broker(
    State(state): State<AppState>,
    Extension(audit): Extension<AuditTerminalFactSink>,
    Path((topic, broker_name)): Path<(String, String)>,
) -> Result<Json<ApiResponse<TopicOperationResult>>, DashboardError> {
    let result = service::delete_topic_from_broker(&state, &topic, &broker_name).await?;
    record_topic_operation(&audit, &state, &result).await;
    Ok(Json(ApiResponse::success(result)))
}

async fn record_topic_operation(audit: &AuditTerminalFactSink, state: &AppState, result: &TopicOperationResult) {
    record_terminal_outcome(audit, state, &result.topic, result.success).await;
}

async fn record_terminal_outcome(
    audit: &AuditTerminalFactSink,
    state: &AppState,
    resource_name: &str,
    succeeded: bool,
) {
    let environment_id = Some(state.published().environment.environment_id);
    if succeeded {
        audit.record_success(Some(resource_name), environment_id).await;
    } else {
        audit.record_failed(Some(resource_name), environment_id).await;
    }
}

pub async fn topic_route(
    State(state): State<AppState>,
    Path(topic): Path<String>,
) -> Result<Json<ApiResponse<TopicRouteInfo>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::topic_route(&state, &topic).await?)))
}

pub async fn topic_stats(
    State(state): State<AppState>,
    Path(topic): Path<String>,
) -> Result<Json<ApiResponse<TopicStatsInfo>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::topic_stats(&state, &topic).await?)))
}

pub async fn topic_config(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    Query(query): Query<TopicConfigQuery>,
) -> Result<Json<ApiResponse<TopicConfigView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::topic_config(&state, &topic, query.broker_name.as_deref()).await?,
    )))
}

pub async fn topic_consumers(
    State(state): State<AppState>,
    Path(topic): Path<String>,
) -> Result<Json<ApiResponse<TopicConsumersView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::topic_consumers(&state, &topic).await?,
    )))
}
