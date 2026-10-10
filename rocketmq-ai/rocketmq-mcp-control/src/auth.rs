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
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::extract::State;
use axum::http::header::WWW_AUTHENTICATE;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use jsonwebtoken::Algorithm;
use jsonwebtoken::Validation;
use rocketmq_mcp_auth::bearer_token;
pub(crate) use rocketmq_mcp_auth::HttpJwksSource;
use rocketmq_mcp_auth::JwksPolicy;
pub(crate) use rocketmq_mcp_auth::JwksSource;
use rocketmq_mcp_auth::JwksVerifier;
use rocketmq_mcp_auth::KeyError;
use rocketmq_mcp_auth::KeySetError;
use rocketmq_mcp_auth::KidCharset;
use rocketmq_mcp_auth::OutboundAddressPolicy;
use serde::Deserialize;

use crate::config::OAuthConfig;
use crate::config::REQUIRED_WRITE_SCOPE;
use crate::error::ControlError;
use crate::model::valid_operator;
use crate::model::ClusterName;
use crate::model::ControlOperation;
use crate::model::Principal;
use crate::telemetry::AuthenticationRejection;
use crate::telemetry::ControlSignals;

/// Limits of the control plane for JWKS documents, cached keys, and Bearer tokens.
///
/// `max_stale` is zero: a key is never used past `cache_ttl`, so a key the issuer has withdrawn
/// stops working even while the issuer cannot be reached.
const JWKS_POLICY: JwksPolicy = JwksPolicy {
    max_jwks_bytes: 256 * 1024,
    max_jwks_keys: 64,
    fetch_timeout: Duration::from_secs(5),
    cache_ttl: Duration::from_secs(300),
    max_stale: Duration::ZERO,
    unknown_kid_cooldown: Duration::from_secs(5),
    max_negative_kids: 256,
    min_rsa_modulus_bits: 2048,
    max_rsa_modulus_bits: 8192,
    required_rsa_exponent: 65_537,
    max_bearer_token_bytes: 16 * 1024,
    kid_charset: KidCharset::Token,
};

/// The HTTPS source of the issuer's JWKS document. It connects only to public addresses.
fn jwks_source(config: &OAuthConfig) -> Result<HttpJwksSource, AuthError> {
    HttpJwksSource::new(
        Arc::<str>::from(config.jwks_url.clone()),
        config.jwks_ca_path.as_deref().map(Path::new),
        &JWKS_POLICY,
        OutboundAddressPolicy::PublicOnly,
    )
    .map_err(|_| AuthError::Unavailable)
}

pub(crate) struct AuthState<S = HttpJwksSource> {
    verifier: JwksVerifier<S>,
    validation: Arc<Validation>,
    resource_metadata: Arc<str>,
    signals: ControlSignals,
}

impl<S> Clone for AuthState<S> {
    fn clone(&self) -> Self {
        Self {
            verifier: self.verifier.clone(),
            validation: self.validation.clone(),
            resource_metadata: self.resource_metadata.clone(),
            signals: self.signals.clone(),
        }
    }
}

impl<S> AuthState<S> {
    pub(crate) fn with_signals(mut self, signals: ControlSignals) -> Self {
        self.signals = signals;
        self
    }

    pub(crate) fn signals(&self) -> &ControlSignals {
        &self.signals
    }
}

impl AuthState<HttpJwksSource> {
    pub(crate) async fn initialize(config: &OAuthConfig, resource_metadata: String) -> Result<Self, AuthError> {
        Self::from_source(config, resource_metadata, jwks_source(config)?).await
    }
}

impl<S: JwksSource> AuthState<S> {
    pub(crate) async fn from_source(
        config: &OAuthConfig,
        resource_metadata: String,
        source: S,
    ) -> Result<Self, AuthError> {
        let verifier = JwksVerifier::new(Arc::new(source), JWKS_POLICY);
        verifier.warm_up().await?;
        Ok(Self {
            verifier,
            validation: Arc::new(jwt_validation(config)),
            resource_metadata: Arc::from(resource_metadata),
            signals: ControlSignals::default(),
        })
    }

    pub(crate) async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let token = bearer_token(headers, JWKS_POLICY.max_bearer_token_bytes).ok_or(AuthError::Unauthorized)?;
        let key = self.verifier.decoding_key(token).await?;
        let decoded = jsonwebtoken::decode::<JwtClaims>(token, key.as_ref(), &self.validation)
            .map_err(|_| AuthError::Unauthorized)?;
        let scopes = decoded
            .claims
            .scope
            .split_ascii_whitespace()
            .filter(|scope| !scope.is_empty())
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        if !scopes.contains(REQUIRED_WRITE_SCOPE) {
            return Err(AuthError::InsufficientScope);
        }
        if !valid_operator(&decoded.claims.sub)
            || scopes.len() > 64
            || scopes.iter().any(|scope| scope.len() > 128 || !scope.is_ascii())
            || decoded.claims.rocketmq_operations.len() > 64
            || decoded.claims.rocketmq_clusters.len() > 64
        {
            return Err(AuthError::Unauthorized);
        }
        Ok(Principal {
            subject: decoded.claims.sub,
            scopes,
            allowed_operations: decoded.claims.rocketmq_operations.into_iter().collect(),
            allowed_clusters: decoded.claims.rocketmq_clusters.into_iter().collect(),
        })
    }

    fn challenge(&self, error: AuthError) -> Option<HeaderValue> {
        let parameter = match error {
            AuthError::Unauthorized => "invalid_token",
            AuthError::InsufficientScope => "insufficient_scope",
            AuthError::Unavailable => return None,
        };
        HeaderValue::from_str(&format!(
            "Bearer resource_metadata=\"{}\", error=\"{parameter}\"",
            self.resource_metadata
        ))
        .ok()
    }
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    sub: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    rocketmq_operations: Vec<ControlOperation>,
    #[serde(default)]
    rocketmq_clusters: Vec<ClusterName>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum AuthError {
    #[error("OAuth token was rejected")]
    Unauthorized,
    #[error("OAuth token is missing rocketmq:write")]
    InsufficientScope,
    #[error("OAuth JWKS is unavailable")]
    Unavailable,
}

impl AuthError {
    const fn status(self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::InsufficientScope => StatusCode::FORBIDDEN,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    const fn control_error(self) -> ControlError {
        match self {
            Self::Unauthorized => ControlError::unauthorized(),
            Self::InsufficientScope => ControlError::permission_denied(),
            Self::Unavailable => ControlError::unauthorized(),
        }
    }

    const fn rejection(self) -> AuthenticationRejection {
        match self {
            Self::Unauthorized => AuthenticationRejection::InvalidToken,
            Self::InsufficientScope => AuthenticationRejection::InsufficientScope,
            Self::Unavailable => AuthenticationRejection::KeysUnavailable,
        }
    }
}

/// A key set that cannot be loaded at startup makes the service unavailable.
impl<E> From<KeySetError<E>> for AuthError {
    fn from(_: KeySetError<E>) -> Self {
        Self::Unavailable
    }
}

/// While serving, a token whose key cannot be found is rejected whether its key id is unknown or
/// the key set could not be refreshed.
impl<E> From<KeyError<E>> for AuthError {
    fn from(_: KeyError<E>) -> Self {
        Self::Unauthorized
    }
}

pub(crate) async fn oauth_middleware<S: JwksSource + 'static>(
    State(state): State<AuthState<S>>,
    mut request: Request,
    next: Next,
) -> Response {
    match state.authenticate(request.headers()).await {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(error) => {
            let control_error = error.control_error();
            state
                .signals
                .authentication_rejected(error.rejection(), control_error.code());
            let mut response = (error.status(), axum::Json(control_error.envelope())).into_response();
            if let Some(challenge) = state.challenge(error) {
                response.headers_mut().insert(WWW_AUTHENTICATE, challenge);
            }
            response
        }
    }
}

fn jwt_validation(config: &OAuthConfig) -> Validation {
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[config.issuer.as_str()]);
    validation.set_audience(&[config.audience.as_str()]);
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    validation.leeway = 0;
    validation.validate_nbf = true;
    validation
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex as StdMutex;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use axum::http::header::AUTHORIZATION;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use futures_util::future::join_all;
    use jsonwebtoken::encode;
    use jsonwebtoken::EncodingKey;
    use jsonwebtoken::Header;
    use serde::Serialize;

    use super::*;

    const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

    struct StaticSource {
        bytes: Vec<u8>,
    }

    impl JwksSource for StaticSource {
        type Error = AuthError;

        async fn fetch(&self) -> Result<Vec<u8>, AuthError> {
            Ok(self.bytes.clone())
        }
    }

    struct SequenceSource {
        documents: StdMutex<VecDeque<Result<Vec<u8>, AuthError>>>,
        last: StdMutex<Result<Vec<u8>, AuthError>>,
        fetches: AtomicUsize,
    }

    impl SequenceSource {
        fn new(document: Vec<u8>) -> Self {
            Self {
                documents: StdMutex::new(VecDeque::new()),
                last: StdMutex::new(Ok(document)),
                fetches: AtomicUsize::new(0),
            }
        }

        fn push(&self, document: Result<Vec<u8>, AuthError>) {
            self.documents.lock().unwrap().push_back(document);
        }

        fn fetch_count(&self) -> usize {
            self.fetches.load(Ordering::SeqCst)
        }
    }

    impl JwksSource for SequenceSource {
        type Error = AuthError;

        async fn fetch(&self) -> Result<Vec<u8>, AuthError> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            if let Some(document) = self.documents.lock().unwrap().pop_front() {
                *self.last.lock().unwrap() = document.clone();
                document
            } else {
                self.last.lock().unwrap().clone()
            }
        }
    }

    #[derive(Serialize)]
    struct TestClaims<'a> {
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        nbf: Option<usize>,
        scope: &'a str,
        rocketmq_operations: Vec<&'a str>,
        rocketmq_clusters: Vec<&'a str>,
    }

    fn config() -> OAuthConfig {
        OAuthConfig {
            issuer: "https://issuer.example.test".to_string(),
            audience: "rocketmq-mcp-control".to_string(),
            jwks_url: "https://issuer.example.test/jwks".to_string(),
            jwks_ca_path: None,
        }
    }

    fn jwks() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"keys": [{
            "kty": "RSA",
            "kid": "test-key",
            "alg": "RS256",
            "use": "sig",
            "key_ops": ["verify"],
            "n": RSA_N,
            "e": "AQAB"
        }]}))
        .unwrap()
    }

    async fn state() -> AuthState<StaticSource> {
        AuthState::from_source(
            &config(),
            "https://control.example.test/.well-known/oauth-protected-resource".to_string(),
            StaticSource { bytes: jwks() },
        )
        .await
        .unwrap()
    }

    fn token_with(
        algorithm: Algorithm,
        kid: Option<&str>,
        issuer: &str,
        audience: &str,
        expiry: usize,
        scope: &str,
    ) -> String {
        let mut header = Header::new(algorithm);
        header.kid = kid.map(ToString::to_string);
        let claims = TestClaims {
            sub: "operator@example.test",
            iss: issuer,
            aud: audience,
            exp: expiry,
            nbf: None,
            scope,
            rocketmq_operations: vec!["topic_upsert"],
            rocketmq_clusters: vec!["cluster-a"],
        };
        match algorithm {
            Algorithm::RS256 => encode(
                &header,
                &claims,
                &EncodingKey::from_rsa_pem(include_bytes!("../tests/fixtures/oauth-private-key.pem")).unwrap(),
            )
            .unwrap(),
            Algorithm::HS256 => encode(&header, &claims, &EncodingKey::from_secret(b"not-accepted")).unwrap(),
            _ => unreachable!(),
        }
    }

    fn token_with_kid(kid: &str) -> String {
        token_with(
            Algorithm::RS256,
            Some(kid),
            "https://issuer.example.test",
            "rocketmq-mcp-control",
            4_102_444_800,
            "rocketmq:write",
        )
    }

    fn token_with_subject(subject: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key".to_owned());
        let claims = TestClaims {
            sub: subject,
            iss: "https://issuer.example.test",
            aud: "rocketmq-mcp-control",
            exp: 4_102_444_800,
            nbf: None,
            scope: "rocketmq:write",
            rocketmq_operations: vec!["topic_upsert"],
            rocketmq_clusters: vec!["cluster-a"],
        };
        encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(include_bytes!("../tests/fixtures/oauth-private-key.pem")).unwrap(),
        )
        .unwrap()
    }

    fn alternate_modulus() -> String {
        let mut bytes = URL_SAFE_NO_PAD.decode(RSA_N).unwrap();
        bytes[64] ^= 1;
        URL_SAFE_NO_PAD.encode(bytes)
    }

    fn jwks_document(entries: &[(&str, &str, &str)]) -> Vec<u8> {
        let keys = entries
            .iter()
            .map(|(kid, modulus, exponent)| {
                serde_json::json!({
                    "kty": "RSA",
                    "kid": kid,
                    "alg": "RS256",
                    "use": "sig",
                    "key_ops": ["verify"],
                    "n": modulus,
                    "e": exponent,
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_vec(&serde_json::json!({"keys": keys})).unwrap()
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        headers
    }

    #[tokio::test]
    async fn valid_rs256_token_yields_closed_principal_claims() {
        let token = token_with(
            Algorithm::RS256,
            Some("test-key"),
            "https://issuer.example.test",
            "rocketmq-mcp-control",
            4_102_444_800,
            "rocketmq:write",
        );
        let principal = state().await.authenticate(&bearer(&token)).await.unwrap();
        assert_eq!(principal.subject, "operator@example.test");
        assert_eq!(
            principal.allowed_operations,
            BTreeSet::from([ControlOperation::TopicUpsert])
        );
        assert_eq!(
            principal.allowed_clusters,
            BTreeSet::from([ClusterName::try_new("cluster-a").unwrap()])
        );
    }

    #[tokio::test]
    async fn oauth_negative_matrix_fails_closed() {
        let state = state().await;
        assert_eq!(
            state.authenticate(&HeaderMap::new()).await.unwrap_err(),
            AuthError::Unauthorized
        );

        let cases = [
            token_with(
                Algorithm::HS256,
                Some("test-key"),
                "https://issuer.example.test",
                "rocketmq-mcp-control",
                4_102_444_800,
                "rocketmq:write",
            ),
            token_with(
                Algorithm::RS256,
                None,
                "https://issuer.example.test",
                "rocketmq-mcp-control",
                4_102_444_800,
                "rocketmq:write",
            ),
            token_with(
                Algorithm::RS256,
                Some("unknown"),
                "https://issuer.example.test",
                "rocketmq-mcp-control",
                4_102_444_800,
                "rocketmq:write",
            ),
            token_with(
                Algorithm::RS256,
                Some("test-key"),
                "https://wrong.example.test",
                "rocketmq-mcp-control",
                4_102_444_800,
                "rocketmq:write",
            ),
            token_with(
                Algorithm::RS256,
                Some("test-key"),
                "https://issuer.example.test",
                "wrong-audience",
                4_102_444_800,
                "rocketmq:write",
            ),
            token_with(
                Algorithm::RS256,
                Some("test-key"),
                "https://issuer.example.test",
                "rocketmq-mcp-control",
                1,
                "rocketmq:write",
            ),
        ];
        for token in cases {
            assert_eq!(
                state.authenticate(&bearer(&token)).await.unwrap_err(),
                AuthError::Unauthorized
            );
        }

        let oversized = "x".repeat(129);
        for subject in [
            "",
            " operator",
            "operator ",
            "operator name",
            "operator\nadmin",
            "https://identity.invalid/operator",
            "token=top-secret",
            "token",
            "svc-secret",
            "Bearer abc.def.ghi",
            "a.b._",
            "a.b.",
            "a.b._@example.test",
            "eyJhbGciOiJSUzI1NiJ9.e30.x@example.test",
            "eyJhbGciOiJub25lIn0.e30.x@example.test",
            "eyJhbGciOiJSUzk5OSJ9.e30.x@example.test",
            "eyJ0eXAiOiJKV1QifQ.e30.x@example.test",
            "eyJhbGciOm51bGx9.e30.x@example.test",
            "10.0.0.1",
            "127.1",
            "127.0.1",
            "127.000.000.001",
            "2130706433",
            "0x7f000001",
            "017700000001",
            "0x7f.0.0.1",
            "0177.0.0.1",
            "svc_10.0.0.1_ops",
            "svc_127.1_ops",
            "svc_0x7f000001_ops",
            "svc_017700000001_ops",
            "10.0.0.1@example.test",
            "2130706433@example.test",
            "svc_127.1@example.test",
            "10.0.0.1:10911",
            "broker.internal.",
            "operator@10.0.0.1.",
            "operator@127.0x1",
            "operator@127.0.0x1",
            "operator@0X7F.0X1",
            "operator@broker.internal",
            "operator@broker.internal.",
            "operator@example.123",
            "operator%25admin",
            "operator/path",
            "operator\u{202e}admin",
            "operator\u{2028}admin",
            "operator：admin",
            "＠operator",
            oversized.as_str(),
        ] {
            let token = token_with_subject(subject);
            let error = state.authenticate(&bearer(&token)).await.unwrap_err();
            assert_eq!(error, AuthError::Unauthorized);
            assert_eq!(error.to_string(), "OAuth token was rejected");
        }

        let missing_scope = token_with(
            Algorithm::RS256,
            Some("test-key"),
            "https://issuer.example.test",
            "rocketmq-mcp-control",
            4_102_444_800,
            "rocketmq:read",
        );
        assert_eq!(
            state.authenticate(&bearer(&missing_scope)).await.unwrap_err(),
            AuthError::InsufficientScope
        );

        let valid = token_with(
            Algorithm::RS256,
            Some("test-key"),
            "https://issuer.example.test",
            "rocketmq-mcp-control",
            4_102_444_800,
            "rocketmq:write",
        );
        let (signed, _) = valid.rsplit_once('.').unwrap();
        assert_eq!(
            state
                .authenticate(&bearer(&format!("{signed}.AAAA")))
                .await
                .unwrap_err(),
            AuthError::Unauthorized
        );
    }

    #[tokio::test]
    async fn oauth_subject_accepts_documented_audit_operator_ids() {
        let state = state().await;
        for subject in [
            "operator@example.test",
            "operator@sub.example.test",
            "operator@team.example.com",
            "first.middle.last@example.test",
            "operator@mail.example.co.uk",
            "123e4567-e89b-12d3-a456-426614174000",
            "12345678-1234-4234-8234-123456789012",
            "svc-control_01",
            "service-2026",
            "svc_1024",
            "svc_2130706433_ops",
            "1-service",
        ] {
            let principal = state.authenticate(&bearer(&token_with_subject(subject))).await.unwrap();
            assert_eq!(principal.subject, subject);
        }
    }

    #[tokio::test]
    async fn malformed_or_symmetric_jwks_is_rejected_at_startup() {
        for bytes in [
            b"not-json".to_vec(),
            serde_json::to_vec(&serde_json::json!({"keys": [{
                "kty": "oct", "kid": "test-key", "alg": "HS256", "use": "sig",
                "key_ops": ["verify"], "n": "secret", "e": "AQAB"
            }]}))
            .unwrap(),
        ] {
            let result = AuthState::from_source(
                &config(),
                "https://control.example.test/.well-known/oauth-protected-resource".to_string(),
                StaticSource { bytes },
            )
            .await;
            assert!(matches!(result, Err(AuthError::Unavailable)));
        }
    }

    async fn state_with(document: serde_json::Value) -> Result<AuthState<StaticSource>, AuthError> {
        AuthState::from_source(
            &config(),
            "https://control.example.test/.well-known/oauth-protected-resource".to_string(),
            StaticSource {
                bytes: serde_json::to_vec(&document).unwrap(),
            },
        )
        .await
    }

    #[tokio::test]
    async fn common_issuer_jwks_shapes_verify_a_signed_token() {
        let with_certificates = serde_json::json!({"keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": "test-key",
            "x5t": "nOo3ZDrODXEK1jKWhXslHR_KXEg",
            "x5c": ["MIIDBTCCAe2gAwIBAgIQ+/placeholder+certificate/chain=="],
            "n": RSA_N,
            "e": "AQAB",
        }]});
        let without_alg = serde_json::json!({"keys": [
            {"kty": "RSA", "use": "sig", "kid": "test-key", "n": RSA_N, "e": "AQAB"},
        ]});
        let with_encryption_key = serde_json::json!({"keys": [
            {"kty": "RSA", "use": "enc", "alg": "RSA-OAEP", "kid": "encryption-key", "n": alternate_modulus(), "e": "AQAB"},
            {"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "test-key", "n": RSA_N, "e": "AQAB"},
        ]});
        for document in [with_certificates, without_alg, with_encryption_key] {
            let state = state_with(document).await.unwrap();
            let principal = state.authenticate(&bearer(&token_with_kid("test-key"))).await.unwrap();
            assert_eq!(principal.subject, "operator@example.test");
        }
    }

    #[tokio::test]
    async fn skipped_jwks_keys_verify_nothing_and_rs256_stays_mandatory() {
        // The key that signs the test tokens is published for encryption only, so it is skipped.
        let state = state_with(serde_json::json!({"keys": [
            {"kty": "RSA", "use": "enc", "kid": "test-key", "n": RSA_N, "e": "AQAB"},
            {"kty": "RSA", "use": "sig", "kid": "other-key", "n": alternate_modulus(), "e": "AQAB"},
        ]}))
        .await
        .unwrap();
        assert_eq!(
            state
                .authenticate(&bearer(&token_with_kid("test-key")))
                .await
                .unwrap_err(),
            AuthError::Unauthorized
        );

        // A signature key that names no algorithm does not let a token choose one.
        let state = state_with(serde_json::json!({"keys": [
            {"kty": "RSA", "use": "sig", "kid": "test-key", "n": RSA_N, "e": "AQAB"},
        ]}))
        .await
        .unwrap();
        let symmetric = token_with(
            Algorithm::HS256,
            Some("test-key"),
            "https://issuer.example.test",
            "rocketmq-mcp-control",
            4_102_444_800,
            "rocketmq:write",
        );
        assert_eq!(
            state.authenticate(&bearer(&symmetric)).await.unwrap_err(),
            AuthError::Unauthorized
        );

        // Nothing usable at all still stops the service from starting.
        let unusable = state_with(serde_json::json!({"keys": [
            {"kty": "RSA", "use": "enc", "kid": "test-key", "n": RSA_N, "e": "AQAB"},
            {"kty": "EC", "use": "sig", "alg": "ES256", "kid": "curve", "crv": "P-256", "x": "eA", "y": "eQ"},
        ]}))
        .await;
        assert!(matches!(unusable, Err(AuthError::Unavailable)));
    }

    #[tokio::test(start_paused = true)]
    async fn cache_refreshes_rotation_and_revocation_after_ttl() {
        let source = Arc::new(SequenceSource::new(jwks()));
        let verifier = JwksVerifier::new(source.clone(), JWKS_POLICY);
        verifier.warm_up().await.unwrap();
        assert_eq!(source.fetch_count(), 1);
        let token = token_with_kid("test-key");
        let key = verifier.decoding_key(&token).await.unwrap();
        assert!(jsonwebtoken::decode::<serde_json::Value>(&token, &key, &jwt_validation(&config())).is_ok());

        let rotated = alternate_modulus();
        source.push(Ok(jwks_document(&[("test-key", &rotated, "AQAB")])));
        tokio::time::advance(JWKS_POLICY.cache_ttl + Duration::from_secs(1)).await;
        let key = verifier.decoding_key(&token).await.unwrap();
        assert!(jsonwebtoken::decode::<serde_json::Value>(&token, &key, &jwt_validation(&config())).is_err());
        assert_eq!(source.fetch_count(), 2);

        source.push(Ok(jwks_document(&[("replacement-key", &rotated, "AQAB")])));
        tokio::time::advance(JWKS_POLICY.cache_ttl + Duration::from_secs(1)).await;
        assert!(matches!(
            verifier.decoding_key(&token).await.map_err(AuthError::from),
            Err(AuthError::Unauthorized)
        ));
        assert_eq!(source.fetch_count(), 3);
    }

    #[tokio::test]
    async fn concurrent_random_kids_trigger_one_bounded_refresh() {
        let source = Arc::new(SequenceSource::new(jwks()));
        let verifier = JwksVerifier::new(source.clone(), JWKS_POLICY);
        verifier.warm_up().await.unwrap();
        let attempts = (0..64)
            .map(|index| {
                let token = token_with_kid(&format!("random-{index}"));
                let verifier = verifier.clone();
                async move { verifier.decoding_key(&token).await }
            })
            .collect::<Vec<_>>();
        assert!(join_all(attempts).await.into_iter().all(|result| result.is_err()));
        assert_eq!(source.fetch_count(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_failure_is_cooled_down_without_using_stale_keys() {
        let source = Arc::new(SequenceSource::new(jwks()));
        let verifier = JwksVerifier::new(source.clone(), JWKS_POLICY);
        verifier.warm_up().await.unwrap();
        source.push(Err(AuthError::Unavailable));
        tokio::time::advance(JWKS_POLICY.cache_ttl + Duration::from_secs(1)).await;
        let token = token_with_kid("test-key");
        assert!(verifier.decoding_key(&token).await.is_err());
        assert!(verifier.decoding_key(&token).await.is_err());
        assert_eq!(source.fetch_count(), 2);
    }

    #[tokio::test]
    async fn weak_oversized_and_unsafe_exponent_jwks_are_rejected() {
        let weak = vec![0x81; 128];
        let oversized = vec![0x81; 1025];
        let cases = [
            jwks_document(&[("weak", &URL_SAFE_NO_PAD.encode(weak), "AQAB")]),
            jwks_document(&[("oversized", &URL_SAFE_NO_PAD.encode(oversized), "AQAB")]),
            jwks_document(&[("exponent", RSA_N, &URL_SAFE_NO_PAD.encode([3_u8]))]),
        ];
        for document in cases {
            let result = AuthState::from_source(
                &config(),
                "https://control.example.test/.well-known/oauth-protected-resource".to_string(),
                StaticSource { bytes: document },
            )
            .await;
            assert!(matches!(result, Err(AuthError::Unavailable)));
        }
    }

    #[tokio::test]
    async fn nbf_is_enforced_for_future_and_permits_past() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as usize;
        let make = |nbf| {
            let mut header = Header::new(Algorithm::RS256);
            header.kid = Some("test-key".to_string());
            encode(
                &header,
                &TestClaims {
                    sub: "operator@example.test",
                    iss: "https://issuer.example.test",
                    aud: "rocketmq-mcp-control",
                    exp: now + 3600,
                    nbf: Some(nbf),
                    scope: "rocketmq:write",
                    rocketmq_operations: vec!["topic_upsert"],
                    rocketmq_clusters: vec!["cluster-a"],
                },
                &EncodingKey::from_rsa_pem(include_bytes!("../tests/fixtures/oauth-private-key.pem")).unwrap(),
            )
            .unwrap()
        };
        let state = state().await;
        assert!(matches!(
            state.authenticate(&bearer(&make(now + 60))).await,
            Err(AuthError::Unauthorized)
        ));
        assert!(state.authenticate(&bearer(&make(now.saturating_sub(1)))).await.is_ok());
    }

    #[test]
    fn jwks_source_connects_only_to_public_addresses() {
        assert!(jwks_source(&config()).is_ok());
        // Configuration validation already refuses address literals; the source refuses them again.
        for url in ["https://127.0.0.1/jwks", "https://10.0.0.9/jwks", "https://[::1]/jwks"] {
            let mut config = config();
            config.jwks_url = url.to_string();
            assert!(
                matches!(jwks_source(&config), Err(AuthError::Unavailable)),
                "accepted {url}"
            );
        }
    }
}
