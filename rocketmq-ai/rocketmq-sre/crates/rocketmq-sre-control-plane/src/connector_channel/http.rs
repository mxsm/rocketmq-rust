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

use std::collections::BTreeSet;

use axum::Json;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::extract::FromRef;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::routing::post;
use rocketmq_sre_contracts::ConnectorHeartbeat;
use rocketmq_sre_contracts::ConnectorRegister;
use rocketmq_sre_contracts::ConnectorResponseEnvelope;
use rocketmq_sre_contracts::ConnectorSessionId;

use super::ConnectorChannelService;
use super::PollRequest;
use super::PollResponse;
use super::PostgresConnectorChannelStore;
use super::RegisterAcknowledgement;
use crate::ControlPlaneError;
use crate::api::AppState;
use crate::assets::AssetTopologyService;
use crate::assets::IngestInventoryRequest;
use crate::auth::AuthContext;

const MAX_CHANNEL_REQUEST_BYTES: usize = 640 * 1024;

pub(crate) type PostgresConnectorChannelService = ConnectorChannelService<PostgresConnectorChannelStore>;

impl FromRef<AppState> for PostgresConnectorChannelService {
    fn from_ref(state: &AppState) -> Self {
        state.connector_channel.clone()
    }
}

impl FromRef<AppState> for AssetTopologyService {
    fn from_ref(state: &AppState) -> Self {
        state.assets.clone()
    }
}

pub(crate) fn router<S>(_service: PostgresConnectorChannelService) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    PostgresConnectorChannelService: FromRef<S>,
    AssetTopologyService: FromRef<S>,
{
    Router::<S>::new()
        .route("/internal/v1/connectors/v1/register", post(register_connector))
        .route("/internal/v1/connectors/v1/heartbeat", post(heartbeat_connector))
        .route(
            "/internal/v1/connectors/v1/{session_id}/commands:poll",
            post(poll_commands),
        )
        .route(
            "/internal/v1/connectors/v1/{session_id}/responses",
            post(submit_response),
        )
        .route(
            "/internal/v1/connectors/v1/{session_id}/inventory",
            post(upload_inventory),
        )
        .layer(DefaultBodyLimit::max(MAX_CHANNEL_REQUEST_BYTES))
}

pub(crate) async fn register_connector(
    State(service): State<PostgresConnectorChannelService>,
    headers: HeaderMap,
    Json(request): Json<ConnectorRegister>,
) -> Result<Json<RegisterAcknowledgement>, ControlPlaneError> {
    let principal = service.authenticate(&headers)?;
    service.register(&principal, &request).await.map(Json)
}

pub(crate) async fn heartbeat_connector(
    State(service): State<PostgresConnectorChannelService>,
    headers: HeaderMap,
    Json(request): Json<ConnectorHeartbeat>,
) -> Result<StatusCode, ControlPlaneError> {
    let principal = service.authenticate(&headers)?;
    service.heartbeat(&principal, &request).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn poll_commands(
    State(service): State<PostgresConnectorChannelService>,
    Path(session_id): Path<ConnectorSessionId>,
    headers: HeaderMap,
    Json(request): Json<PollRequest>,
) -> Result<Json<PollResponse>, ControlPlaneError> {
    let principal = service.authenticate(&headers)?;
    service.poll(&principal, session_id, &request).await.map(Json)
}

pub(crate) async fn submit_response(
    State(service): State<PostgresConnectorChannelService>,
    Path(session_id): Path<ConnectorSessionId>,
    headers: HeaderMap,
    Json(response): Json<ConnectorResponseEnvelope>,
) -> Result<StatusCode, ControlPlaneError> {
    let principal = service.authenticate(&headers)?;
    service.submit_response(&principal, session_id, &response).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn upload_inventory(
    State(service): State<PostgresConnectorChannelService>,
    State(assets): State<AssetTopologyService>,
    Path(session_id): Path<ConnectorSessionId>,
    headers: HeaderMap,
    Json(request): Json<IngestInventoryRequest>,
) -> Result<StatusCode, ControlPlaneError> {
    let principal = service.authenticate(&headers)?;
    let scope = service.authorize_session(&principal, session_id).await?;
    if request.cluster_id != scope.cluster_id {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "inventory upload crosses the registered Connector cluster boundary",
        ));
    }
    let auth = AuthContext {
        tenant_id: scope.tenant_id,
        subject: scope.subject,
        clusters: BTreeSet::from([scope.cluster_id]),
        roles: BTreeSet::from(["diagnose".to_owned()]),
    };
    assets.ingest(&auth, &request).await?;
    Ok(inventory_upload_success())
}

const fn inventory_upload_success() -> StatusCode {
    StatusCode::NO_CONTENT
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::response::IntoResponse;

    use super::*;

    #[tokio::test]
    async fn successful_connector_inventory_upload_has_an_empty_response_body() {
        let response = inventory_upload_success().into_response();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let body = to_bytes(response.into_body(), 1).await.expect("response body");
        assert!(body.is_empty());
    }
}
