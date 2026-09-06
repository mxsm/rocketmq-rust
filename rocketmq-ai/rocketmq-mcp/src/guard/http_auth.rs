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
use crate::guard::jwks::JwksSource;
use crate::guard::jwks::JwksVerifier;
use crate::guard::Guard;
use crate::{McpError, McpResult};

pub struct HttpAuthState<S = HttpJwksSource> {
    authenticator: HttpAuthenticator<S>,
    guard: Guard,
    resource_metadata: Option<Arc<str>>,
}

enum HttpAuthenticator<S = HttpJwksSource> {
    DevelopmentToken {
        token: Arc<str>,
        tenant: Option<String>,
    },
    OAuthJwt {
        verifier: JwksVerifier<S>,
        validation: Arc<Validation>,
        required_scopes: BTreeSet<String>,
    },
}

impl<S> Clone for HttpAuthState<S> {
    fn clone(&self) -> Self {
        Self {
            authenticator: self.authenticator.clone(),
            guard: self.guard.clone(),
            resource_metadata: self.resource_metadata.clone(),
        }
    }
}

impl<S> Clone for HttpAuthenticator<S> {
    fn clone(&self) -> Self {
        match self {
            Self::DevelopmentToken { token, tenant } => Self::DevelopmentToken {
                token: token.clone(),
                tenant: tenant.clone(),
            },
            Self::OAuthJwt {
                verifier,
                validation,
                required_scopes,
            } => Self::OAuthJwt {
                verifier: verifier.clone(),
                validation: validation.clone(),
                required_scopes: required_scopes.clone(),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    sub: String,
    #[serde(default)]
    rocketmq_tenant: Option<String>,
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

impl HttpAuthState<HttpJwksSource> {
    pub fn from_config(config: &HttpAuthConfig, guard: Guard) -> McpResult<Self> {
        Self::from_parts(config, guard, None)
    }

    pub fn from_http_config(config: &HttpConfig, guard: Guard) -> McpResult<Self> {
        let resource_metadata = format!(
            "{}{}",
            config.public_base_url.trim_end_matches('/'),
            config.auth.protected_resource_metadata_path
        );
        Self::from_parts(&config.auth, guard, Some(Arc::from(resource_metadata)))
    }

    fn from_parts(config: &HttpAuthConfig, guard: Guard, resource_metadata: Option<Arc<str>>) -> McpResult<Self> {
        let authenticator = match config.mode {
            HttpAuthMode::DevelopmentToken => {
                let token = std::env::var(&config.development_token_env).map_err(McpError::from_source)?;
                let token = token.trim().to_string();
                if token.is_empty() {
                    return Err(McpError::invalid_config("missing development token".to_string()));
                }
                HttpAuthenticator::DevelopmentToken {
                    token: Arc::from(token),
                    tenant: config.development_tenant.clone(),
                }
            }
            HttpAuthMode::OAuthJwt => {
                let source = Arc::new(HttpJwksSource::new(
                    Arc::<str>::from(config.jwks_url.clone()),
                    config.jwks_ca_path.as_deref().map(std::path::Path::new),
                )?);
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
}

impl<S> HttpAuthState<S>
where
    S: JwksSource,
{
    pub async fn warm_up(&self) -> McpResult<()> {
        if let HttpAuthenticator::OAuthJwt { verifier, .. } = &self.authenticator {
            verifier.warm_up().await?;
        }
        Ok(())
    }

    pub async fn authenticate(&self, headers: &HeaderMap) -> McpResult<Result<RequestContext, HttpAuthRejection>> {
        let token = match bearer_token(headers) {
            Ok(token) => token,
            Err(rejection) => return Ok(Err(rejection)),
        };
        let context = match &self.authenticator {
            HttpAuthenticator::DevelopmentToken {
                token: expected,
                tenant,
            } if expected.as_ref() == token => RequestContext {
                principal: Principal {
                    id: "development-http-client".to_string(),
                    tenant: tenant.clone(),
                    roles: ["diagnose".to_string()].into_iter().collect(),
                    scopes: ["rocketmq:read".to_string(), "rocketmq:diagnose".to_string()]
                        .into_iter()
                        .collect(),
                    allowed_clusters: None,
                },
                client: Some("development-token".to_string()),
            },
            HttpAuthenticator::DevelopmentToken { .. } => return Ok(Err(HttpAuthRejection::unauthorized())),
            HttpAuthenticator::OAuthJwt {
                verifier,
                validation,
                required_scopes,
            } => {
                let key = match verifier.decoding_key(token).await? {
                    Ok(key) => key,
                    Err(rejection) => return Ok(Err(rejection)),
                };
                let decoded = match jsonwebtoken::decode::<JwtClaims>(token, key.as_ref(), validation) {
                    Ok(decoded) => decoded,
                    Err(source) => return Ok(Err(HttpAuthRejection::invalid_token(source))),
                };
                let scopes = decoded
                    .claims
                    .scope
                    .split_ascii_whitespace()
                    .filter(|scope| !scope.is_empty())
                    .map(ToString::to_string)
                    .collect::<BTreeSet<_>>();
                if !required_scopes.is_subset(&scopes) {
                    return Ok(Err(HttpAuthRejection::insufficient_scope()));
                }
                RequestContext {
                    principal: Principal {
                        id: decoded.claims.sub,
                        tenant: decoded.claims.rocketmq_tenant,
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
        if self.guard.check_http_rate_limit(&context).is_err() {
            return Ok(Err(HttpAuthRejection::rate_limited()));
        }
        Ok(Ok(context))
    }

    pub fn record_rejection(&self, context: &RequestContext, error: &HttpAuthRejection) {
        self.guard.record_http_rejection(context, error.to_string());
    }

    pub fn anonymous_context(&self) -> RequestContext {
        RequestContext {
            principal: Principal {
                id: "http-anonymous".to_string(),
                tenant: None,
                roles: BTreeSet::new(),
                scopes: BTreeSet::new(),
                allowed_clusters: None,
            },
            client: None,
        }
    }

    fn challenge(&self, error: &HttpAuthRejection) -> Option<HeaderValue> {
        let error_parameter = match error.kind {
            AuthRejectionKind::InvalidToken => "error=\"invalid_token\"",
            AuthRejectionKind::InsufficientScope => "error=\"insufficient_scope\"",
            AuthRejectionKind::RateLimited => return None,
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

fn bearer_token(headers: &HeaderMap) -> Result<&str, HttpAuthRejection> {
    let header = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(HttpAuthRejection::unauthorized())?;
    header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .filter(|token| !token.is_empty())
        .ok_or(HttpAuthRejection::unauthorized())
}

pub struct HttpAuthRejection {
    kind: AuthRejectionKind,
    _source: Option<jsonwebtoken::errors::Error>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthRejectionKind {
    InvalidToken,
    InsufficientScope,
    RateLimited,
}

impl std::fmt::Display for HttpAuthRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.kind {
            AuthRejectionKind::InvalidToken => "invalid_token",
            AuthRejectionKind::InsufficientScope => "insufficient_scope",
            AuthRejectionKind::RateLimited => "rate_limited",
        })
    }
}

impl std::fmt::Debug for HttpAuthRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl HttpAuthRejection {
    pub(crate) fn unauthorized() -> Self {
        Self {
            kind: AuthRejectionKind::InvalidToken,
            _source: None,
        }
    }

    pub(crate) fn invalid_token(source: jsonwebtoken::errors::Error) -> Self {
        Self {
            kind: AuthRejectionKind::InvalidToken,
            _source: Some(source),
        }
    }

    fn insufficient_scope() -> Self {
        Self {
            kind: AuthRejectionKind::InsufficientScope,
            _source: None,
        }
    }

    fn rate_limited() -> Self {
        Self {
            kind: AuthRejectionKind::RateLimited,
            _source: None,
        }
    }

    fn status_code(&self) -> StatusCode {
        match self.kind {
            AuthRejectionKind::InvalidToken => StatusCode::UNAUTHORIZED,
            AuthRejectionKind::InsufficientScope => StatusCode::FORBIDDEN,
            AuthRejectionKind::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        }
    }
}

impl IntoResponse for HttpAuthRejection {
    fn into_response(self) -> Response {
        let mut response = (self.status_code(), self.to_string()).into_response();
        let challenge = match self.kind {
            AuthRejectionKind::InvalidToken => Some("Bearer error=\"invalid_token\""),
            AuthRejectionKind::InsufficientScope => Some("Bearer error=\"insufficient_scope\""),
            AuthRejectionKind::RateLimited => None,
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
        Ok(Ok(context)) => {
            request.extensions_mut().insert(context);
            next.run(request).await
        }
        Ok(Err(error)) => {
            let context = state.anonymous_context();
            state.record_rejection(&context, &error);
            let challenge = state.challenge(&error);
            let mut response = error.into_response();
            if let Some(challenge) = challenge {
                response.headers_mut().insert(WWW_AUTHENTICATE, challenge);
            }
            response
        }
        Err(error) => {
            state
                .guard
                .record_http_rejection(&state.anonymous_context(), "MCP authentication failed");
            operational_auth_response(error)
        }
    }
}

fn operational_auth_response(_error: McpError) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, "MCP authentication failed").into_response()
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::encode;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use serde::Serialize;
    use std::error::Error;

    use super::*;
    use crate::config::AuditConfig;
    use crate::config::ClusterConfig;
    use crate::config::SecurityConfig;
    use crate::guard::context::VisibilityClass;

    use crate::guard::jwks::JwksSource;

    const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

    #[test]
    fn token_rejection_retains_its_private_typed_cause() {
        let cause = jsonwebtoken::errors::Error::from(jsonwebtoken::errors::ErrorKind::InvalidSignature);
        let rejection = HttpAuthRejection::invalid_token(cause);
        assert_eq!(
            rejection._source.as_ref().unwrap().kind(),
            &jsonwebtoken::errors::ErrorKind::InvalidSignature
        );
        assert_eq!(rejection.to_string(), "invalid_token");
        assert_eq!(format!("{rejection:?}"), "invalid_token");
    }

    #[tokio::test]
    async fn http_rejections_have_fixed_status_body_and_challenge() {
        for (rejection, status, body, challenge) in [
            (
                HttpAuthRejection::unauthorized(),
                StatusCode::UNAUTHORIZED,
                "invalid_token",
                Some("Bearer error=\"invalid_token\""),
            ),
            (
                HttpAuthRejection::invalid_token(jsonwebtoken::errors::Error::from(
                    jsonwebtoken::errors::ErrorKind::InvalidToken,
                )),
                StatusCode::UNAUTHORIZED,
                "invalid_token",
                Some("Bearer error=\"invalid_token\""),
            ),
            (
                HttpAuthRejection::insufficient_scope(),
                StatusCode::FORBIDDEN,
                "insufficient_scope",
                Some("Bearer error=\"insufficient_scope\""),
            ),
            (
                HttpAuthRejection::rate_limited(),
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                None,
            ),
        ] {
            let response = rejection.into_response();
            assert_eq!(response.status(), status);
            assert_eq!(
                response
                    .headers()
                    .get(WWW_AUTHENTICATE)
                    .map(|value| value.to_str().unwrap()),
                challenge
            );
            let bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), body.as_bytes());
        }
    }

    #[tokio::test]
    async fn jwks_outage_is_operational_with_a_typed_source_and_safe_500() {
        struct FailedSource;
        impl JwksSource for FailedSource {
            async fn fetch(&self) -> McpResult<Vec<u8>> {
                Err(McpError::from_source(std::io::Error::other(
                    "sentinel-token JWKS https://private.invalid/key.json",
                )))
            }
        }
        let state = HttpAuthState {
            authenticator: HttpAuthenticator::OAuthJwt {
                verifier: JwksVerifier::new(Arc::new(FailedSource), Duration::from_secs(60), Duration::from_secs(60)),
                validation: Arc::new(Validation::new(Algorithm::RS256)),
                required_scopes: BTreeSet::new(),
            },
            guard: test_guard(),
            resource_metadata: None,
        };
        let error = state
            .authenticate(&bearer_headers(&signed_token("rocketmq:read", "test-key")))
            .await
            .unwrap_err();
        assert!(error.source().unwrap().is::<std::io::Error>());
        assert_eq!(format!("{error}"), "MCP operation failed");
        let response = operational_auth_response(error);
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response.headers().get(WWW_AUTHENTICATE).is_none());
        let bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(bytes.as_ref(), b"MCP authentication failed");
    }

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
        let _cloned = state.clone();
        let token = signed_token("rocketmq:read rocketmq:diagnose", "test-key");
        let context = state.authenticate(&bearer_headers(&token)).await.unwrap().unwrap();

        assert_eq!(context.principal.id, "sre@example.test");
        assert_eq!(context.client.as_deref(), Some("mcp-test-client"));
        assert!(context.principal.roles.contains("diagnose"));
        assert_eq!(context.visibility_class(), VisibilityClass::Sensitive);
        assert_eq!(context.principal.allowed_clusters.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn http_auth_visibility_uses_verified_scopes_for_oauth_and_development_mode() {
        let oauth = oauth_state(["rocketmq:read"]).await;
        let read_only = oauth
            .authenticate(&bearer_headers(&signed_token("rocketmq:read", "test-key")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_only.visibility_class(), VisibilityClass::Standard);

        let development: HttpAuthState<StaticSource> = HttpAuthState {
            authenticator: HttpAuthenticator::DevelopmentToken {
                token: Arc::from("local-test-token"),
                tenant: None,
            },
            guard: test_guard(),
            resource_metadata: None,
        };
        let context = development
            .authenticate(&bearer_headers("local-test-token"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.visibility_class(), VisibilityClass::Sensitive);
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

        let error = state
            .authenticate(&bearer_headers(&missing_scope))
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error.kind, AuthRejectionKind::InsufficientScope));
        assert_eq!(error.status_code(), StatusCode::FORBIDDEN);
        assert!(matches!(
            state.authenticate(&bearer_headers(&unknown_kid)).await.unwrap(),
            Err(HttpAuthRejection {
                kind: AuthRejectionKind::InvalidToken,
                ..
            })
        ));
        assert!(matches!(
            state.authenticate(&bearer_headers(&hmac)).await.unwrap(),
            Err(HttpAuthRejection {
                kind: AuthRejectionKind::InvalidToken,
                ..
            })
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
                state.authenticate(&bearer_headers(&token)).await.unwrap(),
                Err(HttpAuthRejection {
                    kind: AuthRejectionKind::InvalidToken,
                    ..
                })
            ));
        }
    }

    async fn oauth_state(required_scopes: impl IntoIterator<Item = &'static str>) -> HttpAuthState<StaticSource> {
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
                rocketmq_cluster_name: None,
                tenant: None,
                credentials: None,
                proxies: Vec::new(),
                controllers: Vec::new(),
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

    impl JwksSource for StaticSource {
        async fn fetch(&self) -> McpResult<Vec<u8>> {
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
