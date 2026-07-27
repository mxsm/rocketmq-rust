// Copyright 2026 The RocketMQ Rust Authors
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

use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::get;
use axum::routing::patch;
use axum::routing::post;
use rocketmq_sre_contracts::ActionItem;
use rocketmq_sre_contracts::ActionItemId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::PostmortemId;

use super::ActionItemListQuery;
use super::ActionItemPage;
use super::ActionItemPatchRequest;
use super::CreatePostmortemRequest;
use super::PostmortemPatchRequest;
use super::PostmortemPublishRequest;
use super::PostmortemView;
use crate::ControlPlaneError;
use crate::api::AppState;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/incidents/{id}/postmortems",
            post(create_postmortem).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/v1/postmortems/{id}",
            get(get_postmortem)
                .patch(patch_postmortem)
                .layer(DefaultBodyLimit::max(256 * 1024)),
        )
        .route(
            "/v1/postmortems/{id}/publish",
            post(publish_postmortem).layer(DefaultBodyLimit::max(32 * 1024)),
        )
        .route("/v1/action-items", get(list_action_items))
        .route(
            "/v1/action-items/{id}",
            patch(patch_action_item).layer(DefaultBodyLimit::max(32 * 1024)),
        )
}

async fn create_postmortem(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CreatePostmortemRequest>,
) -> Result<Json<PostmortemView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .postmortems
        .create(&auth, parse_incident_id(&id)?, &request)
        .await
        .map(Json)
}

async fn get_postmortem(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<PostmortemView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state.postmortems.get(&auth, parse_postmortem_id(&id)?).await.map(Json)
}

async fn patch_postmortem(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PostmortemPatchRequest>,
) -> Result<Json<PostmortemView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .postmortems
        .patch(&auth, parse_postmortem_id(&id)?, &request)
        .await
        .map(Json)
}

async fn publish_postmortem(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<PostmortemPublishRequest>,
) -> Result<Json<PostmortemView>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .postmortems
        .publish(&auth, parse_postmortem_id(&id)?, &request)
        .await
        .map(Json)
}

async fn list_action_items(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ActionItemListQuery>,
) -> Result<Json<ActionItemPage>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, Some(query.cluster_id)).await?;
    state.postmortems.list_action_items(&auth, &query).await.map(Json)
}

async fn patch_action_item(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ActionItemPatchRequest>,
) -> Result<Json<ActionItem>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .postmortems
        .patch_action_item(&auth, parse_action_item_id(&id)?, &request)
        .await
        .map(Json)
}

fn parse_incident_id(value: &str) -> Result<IncidentId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "incident id must be a UUID"))
}

fn parse_postmortem_id(value: &str) -> Result<PostmortemId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "postmortem id must be a UUID"))
}

fn parse_action_item_id(value: &str) -> Result<ActionItemId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "action item id must be a UUID"))
}
