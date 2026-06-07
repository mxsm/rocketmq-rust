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
use crate::model::ProducerConnectionView;
use crate::model::ProducerInfo;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Query;
use axum::extract::State;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProducerConnectionQuery {
    pub topic: String,
    pub producer_group: String,
}

pub async fn list_producers(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<ProducerInfo>>>, DashboardError> {
    Ok(Json(ApiResponse::success(service::list_producers(&state).await?)))
}

pub async fn producer_connections(
    State(state): State<AppState>,
    Query(query): Query<ProducerConnectionQuery>,
) -> Result<Json<ApiResponse<ProducerConnectionView>>, DashboardError> {
    Ok(Json(ApiResponse::success(
        service::producer_connections(&state, &query.topic, &query.producer_group).await?,
    )))
}
