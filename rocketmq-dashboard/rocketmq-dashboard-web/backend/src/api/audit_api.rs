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

//! Safe, administrator-only session and audit history projections.

use crate::error::DashboardError;
use crate::middleware::require_administrator;
use crate::model::ApiResponse;
use crate::model::AuditAction;
use crate::model::AuditEventView;
use crate::model::AuditOutcome;
use crate::model::AuthenticatedActor;
use crate::model::SessionListPage;
use crate::persistence::audit_repository::AuditCursor;
use crate::persistence::audit_repository::AuditQuery;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Extension;
use axum::extract::Query;
use axum::extract::State;
use serde::Deserialize;
use serde::Serialize;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

const DEFAULT_AUDIT_PAGE_SIZE: usize = 50;
const DEFAULT_AUDIT_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_AUDIT_WINDOW_MS: i64 = 31 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionListRequest {
    pub username: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeAllRequest {
    pub username: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditListRequest {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub actor: Option<String>,
    pub action: Option<String>,
    pub outcome: Option<String>,
    pub environment_id: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditListPage {
    pub events: Vec<AuditEventView>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeAllResult {
    pub revoked: u64,
}

pub async fn list_sessions(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthenticatedActor>,
    Query(request): Query<SessionListRequest>,
) -> Result<Json<ApiResponse<SessionListPage>>, DashboardError> {
    require_administrator(&actor)?;
    let limit = request.limit.unwrap_or(DEFAULT_AUDIT_PAGE_SIZE);
    if limit == 0 || limit > 200 {
        return Err(DashboardError::Validation("session page limit is invalid".to_string()));
    }
    Ok(Json(ApiResponse::success(
        service::list_sessions(&state, &actor, request.username, request.cursor, limit).await?,
    )))
}

pub async fn revoke_all_sessions(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthenticatedActor>,
    Json(request): Json<RevokeAllRequest>,
) -> Result<Json<ApiResponse<RevokeAllResult>>, DashboardError> {
    require_administrator(&actor)?;
    let revoked = service::revoke_all_sessions(&state, &actor, request.username).await?;
    Ok(Json(ApiResponse::success(RevokeAllResult { revoked })))
}

pub async fn list_events(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthenticatedActor>,
    Query(request): Query<AuditListRequest>,
) -> Result<Json<ApiResponse<AuditListPage>>, DashboardError> {
    require_administrator(&actor)?;
    let now = now_millis();
    let end_ms = request.end_ms.unwrap_or(now);
    let start_ms = request
        .start_ms
        .unwrap_or_else(|| end_ms.saturating_sub(DEFAULT_AUDIT_WINDOW_MS));
    if start_ms < 0 || end_ms < start_ms || end_ms.saturating_sub(start_ms) > MAX_AUDIT_WINDOW_MS {
        return Err(DashboardError::Validation("audit time range is invalid".to_string()));
    }
    let limit = request.limit.unwrap_or(DEFAULT_AUDIT_PAGE_SIZE);
    if limit == 0 || limit > 200 {
        return Err(DashboardError::Validation("audit page limit is invalid".to_string()));
    }
    let action = request
        .action
        .as_deref()
        .map(AuditAction::parse)
        .transpose_option("audit action is invalid")?;
    let outcome = request
        .outcome
        .as_deref()
        .map(AuditOutcome::parse)
        .transpose_option("audit outcome is invalid")?;
    let page = state
        .persistence
        .query_audit_events(AuditQuery {
            start_ms,
            end_ms,
            actor: bounded_filter(request.actor, "audit actor is invalid")?,
            action,
            outcome,
            environment_id: bounded_filter(request.environment_id, "audit environment is invalid")?,
            cursor: request.cursor.as_deref().map(parse_audit_cursor).transpose()?,
            limit,
        })
        .await?;
    Ok(Json(ApiResponse::success(AuditListPage {
        events: page.events.into_iter().map(Into::into).collect(),
        next_cursor: page
            .next_cursor
            .map(|next| format!("{},{}", next.created_at_ms, next.event_id)),
    })))
}

fn bounded_filter(value: Option<String>, message: &'static str) -> Result<Option<String>, DashboardError> {
    if value
        .as_ref()
        .is_some_and(|value| value.is_empty() || value.len() > 128)
    {
        return Err(DashboardError::Validation(message.to_string()));
    }
    Ok(value)
}

fn parse_audit_cursor(value: &str) -> Result<AuditCursor, DashboardError> {
    let (created_at_ms, event_id) = value
        .split_once(',')
        .ok_or_else(|| DashboardError::Validation("audit cursor is invalid".to_string()))?;
    if event_id.len() != 36 || uuid::Uuid::parse_str(event_id).is_err() {
        return Err(DashboardError::Validation("audit cursor is invalid".to_string()));
    }
    Ok(AuditCursor {
        created_at_ms: created_at_ms
            .parse()
            .map_err(|_| DashboardError::Validation("audit cursor is invalid".to_string()))?,
        event_id: event_id.to_string(),
    })
}

trait AuditFilterExt<T> {
    fn transpose_option(self, message: &'static str) -> Result<Option<T>, DashboardError>;
}

impl<T> AuditFilterExt<T> for Option<Option<T>> {
    fn transpose_option(self, message: &'static str) -> Result<Option<T>, DashboardError> {
        match self {
            None => Ok(None),
            Some(Some(value)) => Ok(Some(value)),
            Some(None) => Err(DashboardError::Validation(message.to_string())),
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
