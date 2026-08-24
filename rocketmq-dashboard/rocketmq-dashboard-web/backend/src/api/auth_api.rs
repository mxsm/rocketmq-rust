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
use crate::model::AuthenticatedActor;
use crate::model::LoginRequest;
use crate::model::SessionAuthenticationFailure;
use crate::service;
use crate::state::AppState;
use axum::Json;
use axum::extract::Extension;
use axum::extract::State;
use axum::http::HeaderValue;
use axum::http::header::SET_COOKIE;
use axum::response::IntoResponse;
use axum::response::Response;

pub async fn session(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
    failure: Option<Extension<SessionAuthenticationFailure>>,
) -> Result<Response, DashboardError> {
    let mut session = service::session_view(&state, actor.as_ref().map(|actor| &actor.0)).await?;
    let stale_credential = failure.is_some();
    if let Some(failure) = failure {
        session.auth_reason = Some(failure.0.code().to_string());
    }
    let mut response = Json(ApiResponse::success(session)).into_response();
    if stale_credential {
        response
            .headers_mut()
            .append(SET_COOKIE, clear_session_cookie(state.auth_state.cookie_secure())?);
    }
    Ok(response)
}

pub async fn login(
    State(state): State<AppState>,
    Json(request): Json<LoginRequest>,
) -> Result<Response, DashboardError> {
    let session = service::login(&state, request).await?;
    let mut response = Json(ApiResponse::success(session.clone())).into_response();
    if let Some(token) = session.session_id {
        response
            .headers_mut()
            .append(SET_COOKIE, session_cookie(&token, state.auth_state.cookie_secure())?);
    }
    Ok(response)
}

pub async fn logout(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthenticatedActor>,
) -> Result<Response, DashboardError> {
    let session = service::logout(&state, &actor).await?;
    let mut response = Json(ApiResponse::success(session)).into_response();
    response
        .headers_mut()
        .append(SET_COOKIE, clear_session_cookie(state.auth_state.cookie_secure())?);
    Ok(response)
}

fn session_cookie(token: &str, secure: bool) -> Result<HeaderValue, DashboardError> {
    let secure = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "dashboard_session={token}; Path=/; HttpOnly; SameSite=Strict{secure}"
    ))
    .map_err(|_| DashboardError::Internal("Could not create session cookie".to_string()))
}

fn clear_session_cookie(secure: bool) -> Result<HeaderValue, DashboardError> {
    let secure = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "dashboard_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{secure}"
    ))
    .map_err(|_| DashboardError::Internal("Could not clear session cookie".to_string()))
}
