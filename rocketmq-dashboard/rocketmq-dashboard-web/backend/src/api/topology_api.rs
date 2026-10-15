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
use crate::api::request::ApiJson as Json;
use crate::api::request::ApiQuery as Query;
use crate::error::DashboardError;
use crate::model::ApiResponse;
use crate::model::ConsumerQuery;
use crate::model::TopologyView;
use crate::service::topology as topology_service;
use crate::state::AppState;
use axum::extract::State;

/// The consumer query options select how consumer groups are listed, as on `GET /api/consumers`.
pub async fn topology(
    State(state): State<AppState>,
    Query(query): Query<ConsumerQuery>,
) -> Result<Json<ApiResponse<TopologyView>>, DashboardError> {
    Ok(Json(ApiResponse::success(topology_service(&state, query).await?)))
}
