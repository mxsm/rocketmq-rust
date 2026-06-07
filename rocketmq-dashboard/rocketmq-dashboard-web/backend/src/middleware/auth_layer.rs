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
use crate::state::AppState;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;

const DASHBOARD_SESSION_HEADER: &str = "x-dashboard-session";

pub async fn require_auth(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let session_id = session_id_from_headers(request.headers());
    if state.auth_state.is_authorized(session_id.as_deref()).await {
        return next.run(request).await;
    }

    DashboardError::Auth("Authentication required".to_string()).into_response()
}

fn session_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(DASHBOARD_SESSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| bearer_token_from_headers(headers))
}

fn bearer_token_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::bearer_token_from_headers;
    use super::session_id_from_headers;
    use axum::http::HeaderMap;
    use axum::http::HeaderValue;
    use axum::http::header::AUTHORIZATION;

    #[test]
    fn session_header_takes_precedence_over_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert("x-dashboard-session", HeaderValue::from_static("session-a"));
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer session-b"));

        assert_eq!(session_id_from_headers(&headers).as_deref(), Some("session-a"));
    }

    #[test]
    fn bearer_token_is_trimmed() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer session-a "));

        assert_eq!(bearer_token_from_headers(&headers).as_deref(), Some("session-a"));
    }
}
