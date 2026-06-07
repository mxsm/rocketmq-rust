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
use crate::model::DlqBatchResendRequest;
use crate::model::DlqExportView;
use crate::model::DlqMessageQuery;
use crate::model::DlqMessageResendResult;
use crate::model::MessageListView;
use crate::model::MessageQuery;
use crate::model::MessageResendRequest;
use crate::model::MessageTraceView;
use crate::model::MutationResult;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MessageKeyQuery {
    pub topic: String,
    pub key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageTraceQuery {
    pub topic: Option<String>,
    pub trace_topic: Option<String>,
}

pub async fn query_messages(
    State(state): State<AppState>,
    Query(query): Query<MessageQuery>,
) -> Result<Json<ApiResponse<MessageListView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::query_messages(&state, query).await?,
    )))
}

pub async fn query_message_by_key(
    State(state): State<AppState>,
    Query(query): Query<MessageKeyQuery>,
) -> Result<Json<ApiResponse<MessageListView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::query_message_by_key(&state, &query.topic, &query.key).await?,
    )))
}

pub async fn query_message_by_id(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
) -> Result<Json<ApiResponse<MessageListView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::query_message_by_id(&state, &message_id).await?,
    )))
}

pub async fn message_trace(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
    Query(query): Query<MessageTraceQuery>,
) -> Result<Json<ApiResponse<MessageTraceView>>, DashboardError> {
    let trace_topic = query.trace_topic.unwrap_or_else(|| "RMQ_SYS_TRACE_TOPIC".to_string());
    Ok(Json(ApiResponse::success(
        service::message_trace(&state, &message_id, query.topic.as_deref(), &trace_topic).await?,
    )))
}

pub async fn resend_message(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
    Json(request): Json<MessageResendRequest>,
) -> Result<Json<ApiResponse<MutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::resend_message(&state, &message_id, request).await?,
    )))
}

pub async fn query_dlq_messages(
    State(state): State<AppState>,
    Query(query): Query<DlqMessageQuery>,
) -> Result<Json<ApiResponse<MessageListView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::query_dlq_messages(&state, query).await?,
    )))
}

pub async fn resend_dlq_message(
    State(state): State<AppState>,
    Json(payload): Json<DlqBatchResendRequest>,
) -> Result<Json<ApiResponse<Vec<DlqMessageResendResult>>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::resend_dlq_messages(&state, payload).await?,
    )))
}

pub async fn export_dlq_messages(
    State(state): State<AppState>,
    Query(query): Query<DlqMessageQuery>,
) -> Result<Json<ApiResponse<DlqExportView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::export_dlq_messages(&state, query).await?,
    )))
}
