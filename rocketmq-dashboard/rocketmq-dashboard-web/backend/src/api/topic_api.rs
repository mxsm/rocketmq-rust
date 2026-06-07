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
use crate::model::MutationResult;
use crate::model::TopicInfo;
use crate::model::TopicListView;
use crate::model::TopicMutationRequest;
use crate::model::TopicRouteInfo;
use crate::model::TopicStatsInfo;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Path;
use axum::extract::State;

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
    Json(request): Json<TopicMutationRequest>,
) -> Result<Json<ApiResponse<MutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::create_or_update_topic(&state, request).await?,
    )))
}

pub async fn update_topic(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    Json(mut request): Json<TopicMutationRequest>,
) -> Result<Json<ApiResponse<MutationResult>>, DashboardError> {
    request.topic = topic;
    Ok(Json(ApiResponse::success(
        service::create_or_update_topic(&state, request).await?,
    )))
}

pub async fn delete_topic(
    State(state): State<AppState>,
    Path(topic): Path<String>,
) -> Result<Json<ApiResponse<MutationResult>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::delete_topic(&state, &topic).await?)))
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
