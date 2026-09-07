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
use crate::model::AuthenticatedActor;
use crate::model::SessionAuthenticationFailure;
use crate::service::authenticate;
use crate::service::authenticate_session_token;
use crate::state::AppState;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::header::COOKIE;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;

const DASHBOARD_SESSION_COOKIE: &str = "dashboard_session";
const MAX_SESSION_TOKEN_BYTES: usize = 128;

/// Validates the HttpOnly session cookie on every protected request.
pub async fn require_auth(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    let token = match session_token_from_cookies(request.headers()) {
        Ok(token) => token,
        Err(error) => return error.into_response(),
    };
    let actor = match authenticate(&state, token.as_deref()).await {
        Ok(actor) => actor,
        Err(error) => return error.into_response(),
    };
    request.extensions_mut().insert(actor);
    next.run(request).await
}

/// The session status endpoint uses the same parser and persistent validation,
/// but can report `authenticated=false` when no credential is supplied.
pub async fn optional_auth(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    let token = match session_token_from_cookies(request.headers()) {
        Ok(token) => token,
        // Credential conflicts are a security failure, not an unauthenticated
        // session-status result. Keep the typed public error intact so callers
        // can distinguish it without learning either supplied credential.
        Err(DashboardError::AuthTokenAmbiguous) => {
            return DashboardError::AuthTokenAmbiguous.into_response();
        }
        Err(_) => {
            request.extensions_mut().insert(SessionAuthenticationFailure::Invalid);
            return next.run(request).await;
        }
    };
    if token.is_some() || !state.auth_state.login_required() {
        match authenticate_session_token(&state, token.as_deref()).await {
            Ok(Ok(actor)) => {
                request.extensions_mut().insert(actor);
            }
            Ok(Err(reason)) => {
                request.extensions_mut().insert(reason);
            }
            Err(error) => return error.into_response(),
        }
    }
    next.run(request).await
}

pub fn require_administrator(actor: &AuthenticatedActor) -> Result<(), DashboardError> {
    if actor.actor.is_administrator() {
        Ok(())
    } else {
        Err(DashboardError::Forbidden(
            "Administrator permission is required".to_string(),
        ))
    }
}

fn session_token_from_cookies(headers: &HeaderMap) -> Result<Option<String>, DashboardError> {
    let values = cookie_values(headers)?;
    let Some(first) = values.first() else {
        return Ok(None);
    };
    if values.iter().any(|value| value != first) {
        return Err(DashboardError::AuthTokenAmbiguous);
    }
    Ok(Some(first.clone()))
}

fn cookie_values(headers: &HeaderMap) -> Result<Vec<String>, DashboardError> {
    let mut values = Vec::new();
    for header in headers.get_all(COOKIE) {
        let value = header
            .to_str()
            .map_err(|_| DashboardError::Auth("Invalid session credential".to_string()))?;
        for pair in value.split(';') {
            let Some((name, token)) = pair.trim().split_once('=') else {
                continue;
            };
            if name == DASHBOARD_SESSION_COOKIE {
                values.push(validate_token(token.trim())?);
            }
        }
    }
    Ok(values)
}

fn validate_token(token: &str) -> Result<String, DashboardError> {
    if token.is_empty()
        || token.len() > MAX_SESSION_TOKEN_BYTES
        || !token.is_ascii()
        || token
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(DashboardError::Auth("Invalid session credential".to_string()));
    }
    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::session_token_from_cookies;
    use crate::error::DashboardError;
    use axum::http::HeaderMap;
    use axum::http::HeaderValue;
    use axum::http::header::COOKIE;

    #[test]
    fn accepts_cookie_and_rejects_conflicting_cookie_values_without_exposing_credentials() {
        let mut headers = HeaderMap::new();
        headers.append(COOKIE, HeaderValue::from_static("dashboard_session=session-a"));
        headers.append(
            COOKIE,
            HeaderValue::from_static("dashboard_session=session-a; theme=dark"),
        );
        assert_eq!(
            session_token_from_cookies(&headers).expect("same cookie value"),
            Some("session-a".to_string())
        );

        headers.append(COOKIE, HeaderValue::from_static("dashboard_session=session-b"));
        let error = session_token_from_cookies(&headers).expect_err("conflicting cookies must fail");
        assert!(matches!(error, DashboardError::AuthTokenAmbiguous));
        assert_eq!(error.code(), "AUTH_TOKEN_AMBIGUOUS");
        assert_eq!(error.status_code(), axum::http::StatusCode::UNAUTHORIZED);
        assert!(!error.to_string().contains("session-b"));
    }
}
