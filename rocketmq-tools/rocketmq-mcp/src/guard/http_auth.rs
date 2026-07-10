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

use std::sync::Arc;

use axum::extract::Request;
use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;

use crate::guard::Guard;

pub const HTTP_BEARER_TOKEN_ENV: &str = "ROCKETMQ_MCP_HTTP_TOKEN";

#[derive(Debug, Clone)]
pub struct HttpAuthState {
    require_auth: bool,
    bearer_token: Option<Arc<str>>,
    guard: Guard,
}

impl HttpAuthState {
    pub fn from_env(require_auth: bool, guard: Guard) -> Result<Self, HttpAuthError> {
        let bearer_token = std::env::var(HTTP_BEARER_TOKEN_ENV)
            .ok()
            .map(|token| token.trim().to_string())
            .filter(|token| !token.is_empty())
            .map(Arc::from);
        Self::new(require_auth, bearer_token, guard)
    }

    pub fn new(require_auth: bool, bearer_token: Option<Arc<str>>, guard: Guard) -> Result<Self, HttpAuthError> {
        if require_auth && bearer_token.is_none() {
            return Err(HttpAuthError::MissingTokenConfig);
        }

        Ok(Self {
            require_auth,
            bearer_token,
            guard,
        })
    }

    pub fn check_headers(&self, headers: &HeaderMap) -> Result<(), HttpAuthError> {
        if self.require_auth {
            self.check_authorization(headers)?;
        }

        self.guard
            .check_http_rate_limit()
            .map_err(|error| HttpAuthError::RateLimited(error.to_string()))?;
        Ok(())
    }

    pub fn record_rejection(&self, error: &HttpAuthError) {
        self.guard.record_http_rejection(error.to_string());
    }

    fn check_authorization(&self, headers: &HeaderMap) -> Result<(), HttpAuthError> {
        let expected = self.bearer_token.as_deref().ok_or(HttpAuthError::MissingTokenConfig)?;
        let header = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(HttpAuthError::Unauthorized)?;
        let token = header
            .strip_prefix("Bearer ")
            .or_else(|| header.strip_prefix("bearer "))
            .ok_or(HttpAuthError::Unauthorized)?;

        if token == expected {
            Ok(())
        } else {
            Err(HttpAuthError::Unauthorized)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpAuthError {
    #[error("{HTTP_BEARER_TOKEN_ENV} must be set when HTTP auth is required")]
    MissingTokenConfig,

    #[error("HTTP authorization failed")]
    Unauthorized,

    #[error("{0}")]
    RateLimited(String),
}

impl HttpAuthError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::MissingTokenConfig => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
        }
    }
}

impl IntoResponse for HttpAuthError {
    fn into_response(self) -> Response {
        (self.status_code(), self.to_string()).into_response()
    }
}

pub async fn http_auth_middleware(State(state): State<HttpAuthState>, request: Request, next: Next) -> Response {
    match state.check_headers(request.headers()) {
        Ok(()) => next.run(request).await,
        Err(error) => {
            state.record_rejection(&error);
            error.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;
    use crate::config::AuditConfig;
    use crate::config::ClusterConfig;
    use crate::config::SecurityConfig;
    use crate::guard::audit::AuditStatus;

    #[test]
    fn required_auth_rejects_missing_bearer_header() {
        let state = test_state(true, Some("secret"), 60);
        let headers = HeaderMap::new();

        let err = state.check_headers(&headers).unwrap_err();

        assert!(matches!(err, HttpAuthError::Unauthorized));
    }

    #[test]
    fn required_auth_accepts_matching_bearer_header() {
        let state = test_state(true, Some("secret"), 60);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));

        state.check_headers(&headers).unwrap();
    }

    #[test]
    fn rejected_http_request_is_audited() {
        let state = test_state(true, Some("secret"), 60);
        let err = HttpAuthError::Unauthorized;

        state.record_rejection(&err);

        let records = state.guard.audit_log().records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tool, "http_request");
        assert_eq!(records[0].status, AuditStatus::Failure);
    }

    #[test]
    fn http_rate_limit_is_enforced() {
        let state = test_state(false, None, 1);
        let headers = HeaderMap::new();

        state.check_headers(&headers).unwrap();
        let err = state.check_headers(&headers).unwrap_err();

        assert!(matches!(err, HttpAuthError::RateLimited(_)));
    }

    fn test_state(require_auth: bool, token: Option<&'static str>, rate_limit_per_minute: u32) -> HttpAuthState {
        HttpAuthState::new(
            require_auth,
            token.map(Arc::<str>::from),
            Guard::new(
                SecurityConfig {
                    profile: "diagnose".to_string(),
                    allow_change_planning: false,
                    sanitize_output: true,
                    rate_limit_per_minute,
                },
                AuditConfig {
                    enabled: true,
                    sink: "memory".to_string(),
                    path: String::new(),
                },
                &[ClusterConfig {
                    name: "local-dev".to_string(),
                    namesrv_addr: "127.0.0.1:9876".to_string(),
                    default: Some(true),
                }],
            ),
        )
        .unwrap()
    }
}
