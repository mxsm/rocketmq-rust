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
use std::time::Duration;

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
use jsonwebtoken::Validation;
use serde::Deserialize;

use crate::config::HttpAuthConfig;
use crate::config::HttpAuthMode;
use crate::config::HttpConfig;
use crate::guard::context::Principal;
use crate::guard::context::RequestContext;
use crate::guard::jwks::HttpJwksSource;
use crate::guard::jwks::JwksVerifier;
use crate::guard::Guard;

#[derive(Clone)]
pub struct HttpAuthState {
    authenticator: HttpAuthenticator,
    guard: Guard,
    resource_metadata: Option<Arc<str>>,
}

#[derive(Clone)]
enum HttpAuthenticator {
    DevelopmentToken {
        token: Arc<str>,
    },
    OAuthJwt {
        verifier: JwksVerifier,
        validation: Arc<Validation>,
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
        Self::from_parts(config, guard, None)
    }

    pub fn from_http_config(config: &HttpConfig, guard: Guard) -> Result<Self, HttpAuthError> {
        let resource_metadata = format!(
            "{}{}",
            config.public_base_url.trim_end_matches('/'),
            config.auth.protected_resource_metadata_path
        );
        Self::from_parts(&config.auth, guard, Some(Arc::from(resource_metadata)))
    }

    fn from_parts(
        config: &HttpAuthConfig,
        guard: Guard,
        resource_metadata: Option<Arc<str>>,
    ) -> Result<Self, HttpAuthError> {
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
                let source = Arc::new(
                    HttpJwksSource::new(Arc::<str>::from(config.jwks_url.clone()))
                        .map_err(|_| HttpAuthError::InvalidJwksConfig)?,
                );
                let verifier = JwksVerifier::new(
                    source,
                    Duration::from_secs(config.jwks_refresh_seconds),
                    Duration::from_secs(config.jwks_max_stale_seconds),
                );
                HttpAuthenticator::OAuthJwt {
                    verifier,
                    validation: Arc::new(jwt_validation(config)),
                    required_scopes: config.required_scopes.iter().cloned().collect(),
                }
            }
        };
        Ok(Self {
            authenticator,
            guard,
            resource_metadata,
        })
    }

    pub async fn warm_up(&self) -> Result<(), HttpAuthError> {
        if let HttpAuthenticator::OAuthJwt { verifier, .. } = &self.authenticator {
            verifier.warm_up().await.map_err(|_| HttpAuthError::InvalidJwksConfig)?;
        }
        Ok(())
    }

    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<RequestContext, HttpAuthError> {
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
                verifier,
                validation,
                required_scopes,
            } => {
                let key = verifier
                    .decoding_key(token)
                    .await
                    .map_err(|_| HttpAuthError::Unauthorized)?;
                let decoded = jsonwebtoken::decode::<JwtClaims>(token, key.as_ref(), validation)
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
            .map_err(|_| HttpAuthError::RateLimited)?;
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

    fn challenge(&self, error: &HttpAuthError) -> Option<HeaderValue> {
        let error_parameter = match error {
            HttpAuthError::Unauthorized => "error=\"invalid_token\"",
            HttpAuthError::InsufficientScope => "error=\"insufficient_scope\"",
            _ => return None,
        };
        let challenge = match &self.resource_metadata {
            Some(resource_metadata) => {
                format!("Bearer resource_metadata=\"{resource_metadata}\", {error_parameter}")
            }
            None => format!("Bearer {error_parameter}"),
        };
        HeaderValue::from_str(&challenge).ok()
    }
}

fn jwt_validation(config: &HttpAuthConfig) -> Validation {
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[config.issuer.as_str()]);
    validation.set_audience(&[config.audience.as_str()]);
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    validation.leeway = 0;
    validation.validate_nbf = true;
    validation
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

#[derive(Debug, thiserror::Error)]
pub enum HttpAuthError {
    #[error("HTTP token configuration `{0}` is required")]
    MissingTokenConfig(String),
    #[error("HTTP authorization failed")]
    Unauthorized,
    #[error("HTTP token is missing a required scope")]
    InsufficientScope,
    #[error("HTTP JWKS configuration is invalid or unavailable")]
    InvalidJwksConfig,
    #[error("HTTP request rate limit exceeded")]
    RateLimited,
}

impl HttpAuthError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::MissingTokenConfig(_) | Self::InvalidJwksConfig => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::InsufficientScope => StatusCode::FORBIDDEN,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
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
    match state.authenticate(request.headers()).await {
        Ok(context) => {
            request.extensions_mut().insert(context);
            next.run(request).await
        }
        Err(error) => {
            let context = state.anonymous_context();
            state.record_rejection(&context, &error);
            let challenge = state.challenge(&error);
            let mut response = error.into_response();
            if let Some(challenge) = challenge {
                response.headers_mut().insert(WWW_AUTHENTICATE, challenge);
            }
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use jsonwebtoken::encode;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use serde::Serialize;

    use super::*;
    use crate::config::AuditConfig;
    use crate::config::ClusterConfig;
    use crate::config::SecurityConfig;
    use crate::guard::jwks::JwksError;
    use crate::guard::jwks::JwksSource;

    const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

    #[derive(Serialize)]
    struct TestClaims<'a> {
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: usize,
        scope: &'a str,
        roles: Vec<&'a str>,
        client_id: &'a str,
        rocketmq_clusters: Vec<&'a str>,
    }

    #[tokio::test]
    async fn oauth_jwt_attributes_the_verified_principal() {
        let state = oauth_state(["rocketmq:read"]).await;
        let token = signed_token("rocketmq:read rocketmq:diagnose", "test-key");
        let context = state.authenticate(&bearer_headers(&token)).await.unwrap();

        assert_eq!(context.principal.id, "sre@example.test");
        assert_eq!(context.client.as_deref(), Some("mcp-test-client"));
        assert!(context.principal.roles.contains("diagnose"));
        assert_eq!(context.principal.allowed_clusters.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn oauth_jwt_rejects_missing_scope_unknown_kid_and_algorithm_mismatch() {
        let state = oauth_state(["rocketmq:read", "rocketmq:diagnose"]).await;
        let missing_scope = signed_token("rocketmq:read", "test-key");
        let unknown_kid = signed_token("rocketmq:read rocketmq:diagnose", "unknown");
        let hmac = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &TestClaims {
                sub: "sre@example.test",
                iss: "https://issuer.example.test",
                aud: "rocketmq-mcp",
                exp: 4_102_444_800,
                scope: "rocketmq:read rocketmq:diagnose",
                roles: vec!["diagnose"],
                client_id: "mcp-test-client",
                rocketmq_clusters: vec!["local-dev"],
            },
            &EncodingKey::from_secret(b"symmetric-keys-are-rejected"),
        )
        .unwrap();

        let error = state.authenticate(&bearer_headers(&missing_scope)).await.unwrap_err();
        assert!(matches!(error, HttpAuthError::InsufficientScope));
        assert_eq!(error.status_code(), StatusCode::FORBIDDEN);
        assert!(matches!(
            state.authenticate(&bearer_headers(&unknown_kid)).await,
            Err(HttpAuthError::Unauthorized)
        ));
        assert!(matches!(
            state.authenticate(&bearer_headers(&hmac)).await,
            Err(HttpAuthError::Unauthorized)
        ));
    }

    #[tokio::test]
    async fn oauth_jwt_fails_closed_for_issuer_audience_expiry_and_signature() {
        let state = oauth_state(["rocketmq:read"]).await;
        let wrong_issuer = signed_token_with(
            "rocketmq:read",
            "test-key",
            "https://other-issuer.example.test",
            "rocketmq-mcp",
            4_102_444_800,
        );
        let wrong_audience = signed_token_with(
            "rocketmq:read",
            "test-key",
            "https://issuer.example.test",
            "different-audience",
            4_102_444_800,
        );
        let expired = signed_token_with(
            "rocketmq:read",
            "test-key",
            "https://issuer.example.test",
            "rocketmq-mcp",
            1,
        );
        let valid = signed_token("rocketmq:read", "test-key");
        let (signed_data, _) = valid.rsplit_once('.').unwrap();
        let invalid_signature = format!("{signed_data}.AAAA");

        for token in [wrong_issuer, wrong_audience, expired, invalid_signature] {
            assert!(matches!(
                state.authenticate(&bearer_headers(&token)).await,
                Err(HttpAuthError::Unauthorized)
            ));
        }
    }

    async fn oauth_state(required_scopes: impl IntoIterator<Item = &'static str>) -> HttpAuthState {
        let verifier = JwksVerifier::new(
            Arc::new(StaticSource),
            Duration::from_secs(300),
            Duration::from_secs(900),
        );
        verifier.warm_up().await.unwrap();
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&["https://issuer.example.test"]);
        validation.set_audience(&["rocketmq-mcp"]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.leeway = 0;
        HttpAuthState {
            authenticator: HttpAuthenticator::OAuthJwt {
                verifier,
                validation: Arc::new(validation),
                required_scopes: required_scopes.into_iter().map(ToString::to_string).collect(),
            },
            guard: test_guard(),
            resource_metadata: None,
        }
    }

    fn test_guard() -> Guard {
        Guard::new(
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
                max_record_bytes: 16 * 1024,
                queue_max_bytes: 1024 * 1024,
            },
            &[ClusterConfig {
                name: "local-dev".to_string(),
                namesrv_addr: "127.0.0.1:9876".to_string(),
                default: Some(true),
            }],
        )
        .unwrap()
    }

    fn signed_token(scope: &str, kid: &str) -> String {
        signed_token_with(scope, kid, "https://issuer.example.test", "rocketmq-mcp", 4_102_444_800)
    }

    fn signed_token_with(scope: &str, kid: &str, issuer: &str, audience: &str, expires_at: usize) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        encode(
            &header,
            &TestClaims {
                sub: "sre@example.test",
                iss: issuer,
                aud: audience,
                exp: expires_at,
                scope,
                roles: vec!["diagnose"],
                client_id: "mcp-test-client",
                rocketmq_clusters: vec!["local-dev"],
            },
            &EncodingKey::from_rsa_pem(include_bytes!("../../tests/fixtures/oauth-private-key.pem")).unwrap(),
        )
        .unwrap()
    }

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        headers
    }

    struct StaticSource;

    #[async_trait]
    impl JwksSource for StaticSource {
        async fn fetch(&self) -> Result<Vec<u8>, JwksError> {
            Ok(serde_json::to_vec(&serde_json::json!({"keys": [{
                "kty": "RSA", "kid": "test-key", "alg": "RS256", "use": "sig",
                "key_ops": ["verify"], "n": RSA_N, "e": "AQAB"
            }]}))
            .unwrap())
        }
    }

    fn permission_path() -> String {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("permissions.example.toml")
            .to_string_lossy()
            .into_owned()
    }
}
