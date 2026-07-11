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

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::Request;
use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::header::WWW_AUTHENTICATE;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::Validation;
use serde::Deserialize;

use crate::config::HttpAuthConfig;
use crate::config::HttpAuthMode;
use crate::config::JwtAlgorithm;
use crate::guard::context::Principal;
use crate::guard::context::RequestContext;
use crate::guard::Guard;

#[derive(Clone)]
pub struct HttpAuthState {
    authenticator: HttpAuthenticator,
    guard: Guard,
}

#[derive(Clone)]
enum HttpAuthenticator {
    DevelopmentToken {
        token: Arc<str>,
    },
    OAuthJwt {
        key: Arc<DecodingKey>,
        validation: Box<Validation>,
        required_scopes: BTreeSet<String>,
    },
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    sub: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    azp: Option<String>,
    #[serde(default)]
    rocketmq_clusters: Option<Vec<String>>,
}

impl HttpAuthState {
    pub fn from_config(config: &HttpAuthConfig, guard: Guard) -> Result<Self, HttpAuthError> {
        let authenticator = match config.mode {
            HttpAuthMode::DevelopmentToken => {
                let token = std::env::var(&config.development_token_env)
                    .ok()
                    .map(|token| token.trim().to_string())
                    .filter(|token| !token.is_empty())
                    .ok_or_else(|| HttpAuthError::MissingTokenConfig(config.development_token_env.clone()))?;
                HttpAuthenticator::DevelopmentToken {
                    token: Arc::from(token),
                }
            }
            HttpAuthMode::OAuthJwt => {
                let key_material = std::env::var(&config.jwt_key_env)
                    .map_err(|_| HttpAuthError::MissingTokenConfig(config.jwt_key_env.clone()))?;
                let algorithm = jwt_algorithm(config.jwt_algorithm);
                let key = decoding_key(config.jwt_algorithm, key_material.as_bytes())?;
                let mut validation = Validation::new(algorithm);
                validation.set_issuer(&[config.issuer.as_str()]);
                validation.set_audience(&[config.audience.as_str()]);
                HttpAuthenticator::OAuthJwt {
                    key: Arc::new(key),
                    validation: Box::new(validation),
                    required_scopes: config.required_scopes.iter().cloned().collect(),
                }
            }
        };
        Ok(Self { authenticator, guard })
    }

    pub fn authenticate(&self, headers: &HeaderMap) -> Result<RequestContext, HttpAuthError> {
        let token = bearer_token(headers)?;
        let context = match &self.authenticator {
            HttpAuthenticator::DevelopmentToken { token: expected } if expected.as_ref() == token => RequestContext {
                principal: Principal {
                    id: "development-http-client".to_string(),
                    roles: ["diagnose".to_string()].into_iter().collect(),
                    scopes: ["rocketmq:read".to_string(), "rocketmq:diagnose".to_string()]
                        .into_iter()
                        .collect(),
                    allowed_clusters: None,
                },
                client: Some("development-token".to_string()),
            },
            HttpAuthenticator::DevelopmentToken { .. } => return Err(HttpAuthError::Unauthorized),
            HttpAuthenticator::OAuthJwt {
                key,
                validation,
                required_scopes,
            } => {
                let decoded = jsonwebtoken::decode::<JwtClaims>(token, key, validation)
                    .map_err(|_| HttpAuthError::Unauthorized)?;
                let scopes = decoded
                    .claims
                    .scope
                    .split_ascii_whitespace()
                    .filter(|scope| !scope.is_empty())
                    .map(ToString::to_string)
                    .collect::<BTreeSet<_>>();
                if !required_scopes.is_subset(&scopes) {
                    return Err(HttpAuthError::InsufficientScope);
                }
                RequestContext {
                    principal: Principal {
                        id: decoded.claims.sub,
                        roles: decoded.claims.roles.into_iter().collect(),
                        scopes,
                        allowed_clusters: decoded
                            .claims
                            .rocketmq_clusters
                            .map(|clusters| clusters.into_iter().collect()),
                    },
                    client: decoded.claims.client_id.or(decoded.claims.azp),
                }
            }
        };
        self.guard
            .check_http_rate_limit(&context)
            .map_err(|error| HttpAuthError::RateLimited(error.to_string()))?;
        Ok(context)
    }

    pub fn record_rejection(&self, context: &RequestContext, error: &HttpAuthError) {
        self.guard.record_http_rejection(context, error.to_string());
    }

    pub fn anonymous_context(&self) -> RequestContext {
        RequestContext {
            principal: Principal {
                id: "http-anonymous".to_string(),
                roles: BTreeSet::new(),
                scopes: BTreeSet::new(),
                allowed_clusters: None,
            },
            client: None,
        }
    }
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, HttpAuthError> {
    let header = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(HttpAuthError::Unauthorized)?;
    header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .filter(|token| !token.is_empty())
        .ok_or(HttpAuthError::Unauthorized)
}

fn jwt_algorithm(algorithm: JwtAlgorithm) -> Algorithm {
    match algorithm {
        JwtAlgorithm::Hs256 => Algorithm::HS256,
        JwtAlgorithm::Rs256 => Algorithm::RS256,
    }
}

fn decoding_key(algorithm: JwtAlgorithm, key_material: &[u8]) -> Result<DecodingKey, HttpAuthError> {
    match algorithm {
        JwtAlgorithm::Hs256 => Ok(DecodingKey::from_secret(key_material)),
        JwtAlgorithm::Rs256 => DecodingKey::from_rsa_pem(key_material).map_err(|_| HttpAuthError::InvalidKeyConfig),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpAuthError {
    #[error("HTTP token configuration `{0}` is required")]
    MissingTokenConfig(String),

    #[error("HTTP authorization failed")]
    Unauthorized,

    #[error("HTTP token is missing a required scope")]
    InsufficientScope,

    #[error("HTTP JWT key configuration is invalid")]
    InvalidKeyConfig,

    #[error("{0}")]
    RateLimited(String),
}

impl HttpAuthError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::MissingTokenConfig(_) | Self::InvalidKeyConfig => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::InsufficientScope => StatusCode::FORBIDDEN,
            Self::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
        }
    }
}

impl IntoResponse for HttpAuthError {
    fn into_response(self) -> Response {
        let mut response = (self.status_code(), self.to_string()).into_response();
        let challenge = match self {
            Self::Unauthorized => Some("Bearer error=\"invalid_token\""),
            Self::InsufficientScope => Some("Bearer error=\"insufficient_scope\""),
            _ => None,
        };
        if let Some(challenge) = challenge {
            response
                .headers_mut()
                .insert(WWW_AUTHENTICATE, HeaderValue::from_static(challenge));
        }
        response
    }
}

pub async fn http_auth_middleware(State(state): State<HttpAuthState>, mut request: Request, next: Next) -> Response {
    match state.authenticate(request.headers()) {
        Ok(context) => {
            request.extensions_mut().insert(context);
            next.run(request).await
        }
        Err(error) => {
            let context = state.anonymous_context();
            state.record_rejection(&context, &error);
            error.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::encode;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use serde::Serialize;

    use super::*;
    use crate::config::AuditConfig;
    use crate::config::ClusterConfig;
    use crate::config::SecurityConfig;

    #[derive(Serialize)]
    struct TestClaims<'a> {
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: usize,
        scope: &'a str,
        roles: Vec<&'a str>,
        rocketmq_clusters: Vec<&'a str>,
    }

    #[test]
    fn oauth_jwt_attributes_the_verified_principal() {
        let state = oauth_state(["rocketmq:read"]);
        let token = signed_token("rocketmq:read rocketmq:diagnose");
        let headers = bearer_headers(&token);

        let context = state.authenticate(&headers).unwrap();

        assert_eq!(context.principal.id, "sre@example.test");
        assert_eq!(context.client, None);
        assert!(context.principal.roles.contains("diagnose"));
        assert_eq!(context.principal.allowed_clusters.unwrap().len(), 1);
    }

    #[test]
    fn oauth_jwt_rejects_missing_required_scope_with_403_semantics() {
        let state = oauth_state(["rocketmq:read", "rocketmq:diagnose"]);
        let token = signed_token("rocketmq:read");

        let error = state.authenticate(&bearer_headers(&token)).unwrap_err();

        assert!(matches!(error, HttpAuthError::InsufficientScope));
        assert_eq!(error.status_code(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn oauth_jwt_rejects_invalid_signature() {
        let state = oauth_state(["rocketmq:read"]);
        let token = encode(
            &Header::default(),
            &TestClaims {
                sub: "sre@example.test",
                iss: "https://issuer.example.test",
                aud: "rocketmq-mcp",
                exp: 4_102_444_800,
                scope: "rocketmq:read",
                roles: vec!["diagnose"],
                rocketmq_clusters: vec!["local-dev"],
            },
            &EncodingKey::from_secret(b"different-test-secret"),
        )
        .unwrap();

        assert!(matches!(
            state.authenticate(&bearer_headers(&token)),
            Err(HttpAuthError::Unauthorized)
        ));
    }

    fn oauth_state(required_scopes: impl IntoIterator<Item = &'static str>) -> HttpAuthState {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&["https://issuer.example.test"]);
        validation.set_audience(&["rocketmq-mcp"]);
        HttpAuthState {
            authenticator: HttpAuthenticator::OAuthJwt {
                key: Arc::new(DecodingKey::from_secret(b"test-secret")),
                validation: Box::new(validation),
                required_scopes: required_scopes.into_iter().map(ToString::to_string).collect(),
            },
            guard: Guard::new(
                SecurityConfig {
                    profile: "diagnose".to_string(),
                    allow_change_planning: false,
                    sanitize_output: true,
                    rate_limit_per_minute: 60,
                    permissions_file: permission_path(),
                    max_concurrent_requests_per_cluster: 8,
                },
                AuditConfig {
                    enabled: true,
                    sink: "memory".to_string(),
                    path: String::new(),
                    queue_capacity: 16,
                },
                &[ClusterConfig {
                    name: "local-dev".to_string(),
                    namesrv_addr: "127.0.0.1:9876".to_string(),
                    default: Some(true),
                }],
            )
            .unwrap(),
        }
    }

    fn signed_token(scope: &str) -> String {
        encode(
            &Header::default(),
            &TestClaims {
                sub: "sre@example.test",
                iss: "https://issuer.example.test",
                aud: "rocketmq-mcp",
                exp: 4_102_444_800,
                scope,
                roles: vec!["diagnose"],
                rocketmq_clusters: vec!["local-dev"],
            },
            &EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap()
    }

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        headers
    }

    fn permission_path() -> String {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("permissions.example.toml")
            .to_string_lossy()
            .into_owned()
    }
}
