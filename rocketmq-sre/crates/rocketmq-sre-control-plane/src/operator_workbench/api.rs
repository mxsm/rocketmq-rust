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
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::IncidentOperationRequest;
use rocketmq_sre_contracts::IncidentOperationResult;
use rocketmq_sre_contracts::IncidentOperationsState;
use rocketmq_sre_contracts::ShiftHandoffSummary;

use super::model::OperationsReportFormat;
use super::model::OperationsReportQuery;
use super::model::ShiftHandoffQuery;
use super::service::render_report_html;
use super::service::render_report_markdown;
use crate::ControlPlaneError;
use crate::api::AppState;
use crate::observability::CORRELATION_ID_HEADER;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/incidents/{id}/operations",
            get(get_incident_operations)
                .post(apply_incident_operation)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/v1/operations/shift-handoff", get(get_shift_handoff))
        .route("/v1/operations/reports", get(get_operations_report))
}

async fn get_incident_operations(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<IncidentOperationsState>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .operations
        .incident_state(&auth, parse_incident_id(&id)?)
        .await
        .map(Json)
}

async fn apply_incident_operation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<IncidentOperationRequest>,
) -> Result<Json<IncidentOperationResult>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, None).await?;
    state
        .operations
        .apply_incident_operation(&auth, parse_incident_id(&id)?, &request, correlation_id(&headers))
        .await
        .map(Json)
}

async fn get_shift_handoff(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ShiftHandoffQuery>,
) -> Result<Json<ShiftHandoffSummary>, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    state.operations.shift_handoff(&auth, query.cluster_id).await.map(Json)
}

async fn get_operations_report(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<OperationsReportQuery>,
) -> Result<Response, ControlPlaneError> {
    let auth = state.auth.authorize(&headers, query.cluster_id).await?;
    let report = state.operations.report(&auth, query.cluster_id, query.window).await?;
    match query.format {
        OperationsReportFormat::Json => Ok(Json(report).into_response()),
        OperationsReportFormat::Markdown => download_response(
            "text/markdown; charset=utf-8",
            &format!("rocketmq-sre-{}-operations.md", window_name(query.window)),
            render_report_markdown(&report),
        ),
        OperationsReportFormat::Html => download_response(
            "text/html; charset=utf-8",
            &format!("rocketmq-sre-{}-operations.html", window_name(query.window)),
            render_report_html(&report),
        ),
    }
}

fn download_response(content_type: &str, filename: &str, body: String) -> Result<Response, ControlPlaneError> {
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .map_err(|_| ControlPlaneError::configuration("report content type is invalid"))?,
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
            .map_err(|_| ControlPlaneError::configuration("report filename is invalid"))?,
    );
    Ok(response)
}

fn parse_incident_id(value: &str) -> Result<IncidentId, ControlPlaneError> {
    value
        .parse()
        .map_err(|_| ControlPlaneError::validation("invalid_request", "incident id must be a UUID"))
}

fn correlation_id(headers: &HeaderMap) -> CorrelationId {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

const fn window_name(window: rocketmq_sre_contracts::OperationsReportWindow) -> &'static str {
    match window {
        rocketmq_sre_contracts::OperationsReportWindow::Daily => "daily",
        rocketmq_sre_contracts::OperationsReportWindow::Weekly => "weekly",
    }
}
