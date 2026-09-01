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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::path::Path;
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
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::Validation;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::config::OAuthConfig;
use crate::config::REQUIRED_WRITE_SCOPE;
use crate::error::ControlError;
use crate::model::ClusterName;
use crate::model::ControlOperation;
use crate::model::Principal;

const MAX_JWKS_BYTES: usize = 256 * 1024;
const MAX_JWKS_KEYS: usize = 64;
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const JWKS_CACHE_TTL: Duration = Duration::from_secs(300);
const UNKNOWN_KID_COOLDOWN: Duration = Duration::from_secs(5);
const MAX_NEGATIVE_KIDS: usize = 256;
const MIN_RSA_MODULUS_BITS: usize = 2048;
const MAX_RSA_MODULUS_BITS: usize = 8192;
const REQUIRED_RSA_EXPONENT: u64 = 65_537;
const MAX_BEARER_TOKEN_BYTES: usize = 16 * 1024;

pub(crate) trait JwksSource: Send + Sync {
    fn fetch(&self) -> impl Future<Output = Result<Vec<u8>, AuthError>> + Send;
}

#[derive(Clone)]
pub(crate) struct HttpJwksSource {
    client: reqwest::Client,
    url: Arc<str>,
}

impl HttpJwksSource {
    fn new(url: impl Into<Arc<str>>, ca_path: Option<&Path>) -> Result<Self, AuthError> {
        let url = url.into();
        let parsed = url::Url::parse(&url).map_err(|_| AuthError::Unavailable)?;
        let host = parsed.host_str().ok_or(AuthError::Unavailable)?;
        let mut builder = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(JWKS_FETCH_TIMEOUT)
            .dns_resolver(Arc::new(SafeDnsResolver::new(host)));
        if let Some(path) = ca_path {
            let pem = std::fs::read(path).map_err(|_| AuthError::Unavailable)?;
            let certificates = reqwest::Certificate::from_pem_bundle(&pem).map_err(|_| AuthError::Unavailable)?;
            if certificates.is_empty() {
                return Err(AuthError::Unavailable);
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let client = builder.build().map_err(|_| AuthError::Unavailable)?;
        Ok(Self { client, url })
    }
}

#[derive(Clone)]
struct SafeDnsResolver {
    allowed_host: Arc<str>,
}

impl SafeDnsResolver {
    fn new(host: &str) -> Self {
        Self {
            allowed_host: Arc::from(host.to_ascii_lowercase()),
        }
    }
}

impl reqwest::dns::Resolve for SafeDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let expected = self.allowed_host.clone();
        Box::pin(async move {
            if !name.as_str().eq_ignore_ascii_case(&expected) {
                return Err(Box::new(DnsPolicyError) as Box<dyn std::error::Error + Send + Sync>);
            }
            let addresses = tokio::net::lookup_host((name.as_str(), 0))
                .await
                .map_err(|_| Box::new(DnsPolicyError) as Box<dyn std::error::Error + Send + Sync>)?
                .collect::<Vec<_>>();
            validated_public_addresses(addresses)
                .map(|addresses| Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct DnsPolicyError;

impl std::fmt::Display for DnsPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DNS policy rejected the endpoint")
    }
}

impl std::error::Error for DnsPolicyError {}

impl JwksSource for HttpJwksSource {
    async fn fetch(&self) -> Result<Vec<u8>, AuthError> {
        let mut response = self
            .client
            .get(self.url.as_ref())
            .send()
            .await
            .map_err(|_| AuthError::Unavailable)?
            .error_for_status()
            .map_err(|_| AuthError::Unavailable)?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_JWKS_BYTES as u64)
        {
            return Err(AuthError::Unavailable);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| AuthError::Unavailable)? {
            let next_len = body.len().checked_add(chunk.len()).ok_or(AuthError::Unavailable)?;
            if next_len > MAX_JWKS_BYTES {
                return Err(AuthError::Unavailable);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

struct JwksVerifier<S> {
    source: Arc<S>,
    cache: Arc<RwLock<JwksCache>>,
    refresh: Arc<Mutex<()>>,
}

struct JwksCache {
    keys: BTreeMap<String, Arc<DecodingKey>>,
    generation: u64,
    expires_at: Instant,
    refresh_retry_at: Instant,
    unknown_retry_at: Instant,
    negative_kids: BTreeMap<String, Instant>,
}

impl JwksCache {
    fn empty(now: Instant) -> Self {
        Self {
            keys: BTreeMap::new(),
            generation: 0,
            expires_at: now,
            refresh_retry_at: now,
            unknown_retry_at: now,
            negative_kids: BTreeMap::new(),
        }
    }

    fn cached_key(&self, kid: &str, now: Instant) -> Option<Arc<DecodingKey>> {
        (now < self.expires_at).then(|| self.keys.get(kid).cloned()).flatten()
    }

    fn rejects_without_refresh(&self, kid: &str, now: Instant) -> bool {
        self.negative_kids.get(kid).is_some_and(|expires| now < *expires)
            || (now < self.expires_at && now < self.unknown_retry_at)
            || (now >= self.expires_at && now < self.refresh_retry_at)
    }

    fn record_negative(&mut self, kid: String, now: Instant) {
        self.unknown_retry_at = now + UNKNOWN_KID_COOLDOWN;
        if self.negative_kids.len() >= MAX_NEGATIVE_KIDS {
            self.negative_kids.clear();
        }
        self.negative_kids.insert(kid, now + UNKNOWN_KID_COOLDOWN);
    }
}

impl<S> Clone for JwksVerifier<S> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            cache: self.cache.clone(),
            refresh: self.refresh.clone(),
        }
    }
}

impl<S: JwksSource> JwksVerifier<S> {
    fn new(source: Arc<S>) -> Self {
        let now = Instant::now();
        Self {
            source,
            cache: Arc::new(RwLock::new(JwksCache::empty(now))),
            refresh: Arc::new(Mutex::new(())),
        }
    }

    async fn warm_up(&self) -> Result<(), AuthError> {
        let _writer = self.refresh.lock().await;
        self.fetch_generation(Instant::now()).await
    }

    async fn decoding_key(&self, token: &str) -> Result<Arc<DecodingKey>, AuthError> {
        let header = jsonwebtoken::decode_header(token).map_err(|_| AuthError::Unauthorized)?;
        if header.alg != Algorithm::RS256 {
            return Err(AuthError::Unauthorized);
        }
        let kid = header
            .kid
            .filter(|value| valid_kid(value))
            .ok_or(AuthError::Unauthorized)?;
        let now = Instant::now();
        let cache = self.cache.read().await;
        if let Some(key) = cache.cached_key(&kid, now) {
            return Ok(key);
        }
        if cache.rejects_without_refresh(&kid, now) {
            return Err(AuthError::Unauthorized);
        }
        drop(cache);

        let _writer = self.refresh.lock().await;
        let now = Instant::now();
        let cache = self.cache.read().await;
        if let Some(key) = cache.cached_key(&kid, now) {
            return Ok(key);
        }
        if cache.rejects_without_refresh(&kid, now) {
            return Err(AuthError::Unauthorized);
        }
        drop(cache);

        if self.fetch_generation(now).await.is_err() {
            let mut cache = self.cache.write().await;
            cache.refresh_retry_at = now + UNKNOWN_KID_COOLDOWN;
            cache.record_negative(kid, now);
            return Err(AuthError::Unauthorized);
        }
        let mut cache = self.cache.write().await;
        if let Some(key) = cache.cached_key(&kid, now) {
            return Ok(key);
        }
        cache.record_negative(kid, now);
        Err(AuthError::Unauthorized)
    }

    async fn fetch_generation(&self, now: Instant) -> Result<(), AuthError> {
        let bytes = self.source.fetch().await?;
        let parsed = parse_jwks(&bytes)?;
        let mut cache = self.cache.write().await;
        cache.generation = cache.generation.checked_add(1).ok_or(AuthError::Unavailable)?;
        cache.keys = parsed;
        cache.expires_at = now + JWKS_CACHE_TTL;
        cache.refresh_retry_at = now;
        cache.unknown_retry_at = now;
        cache.negative_kids.clear();
        Ok(())
    }
}

pub(crate) struct AuthState<S = HttpJwksSource> {
    verifier: JwksVerifier<S>,
    validation: Arc<Validation>,
    resource_metadata: Arc<str>,
}

impl<S> Clone for AuthState<S> {
    fn clone(&self) -> Self {
        Self {
            verifier: self.verifier.clone(),
            validation: self.validation.clone(),
            resource_metadata: self.resource_metadata.clone(),
        }
    }
}

impl AuthState<HttpJwksSource> {
    pub(crate) async fn initialize(config: &OAuthConfig, resource_metadata: String) -> Result<Self, AuthError> {
        let source = HttpJwksSource::new(
            Arc::<str>::from(config.jwks_url.clone()),
            config.jwks_ca_path.as_deref().map(Path::new),
        )?;
        Self::from_source(config, resource_metadata, source).await
    }
}

impl<S: JwksSource> AuthState<S> {
    pub(crate) async fn from_source(
        config: &OAuthConfig,
        resource_metadata: String,
        source: S,
    ) -> Result<Self, AuthError> {
        let verifier = JwksVerifier::new(Arc::new(source));
        verifier.warm_up().await?;
        Ok(Self {
            verifier,
            validation: Arc::new(jwt_validation(config)),
            resource_metadata: Arc::from(resource_metadata),
        })
    }

    pub(crate) async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let token = bearer_token(headers)?;
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
        if decoded.claims.sub.is_empty()
            || decoded.claims.sub.len() > 128
            || decoded.claims.sub.chars().any(char::is_control)
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
            let mut response = (error.status(), axum::Json(error.control_error().envelope())).into_response();
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

fn bearer_token(headers: &HeaderMap) -> Result<&str, AuthError> {
    let header = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(AuthError::Unauthorized)?;
    header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .filter(|token| !token.is_empty() && token.len() <= MAX_BEARER_TOKEN_BYTES)
        .ok_or(AuthError::Unauthorized)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawJwks {
    keys: Vec<RawJwk>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawJwk {
    kty: String,
    kid: String,
    alg: String,
    #[serde(rename = "use")]
    public_key_use: Option<String>,
    key_ops: Option<Vec<String>>,
    n: String,
    e: String,
}

fn parse_jwks(bytes: &[u8]) -> Result<BTreeMap<String, Arc<DecodingKey>>, AuthError> {
    if bytes.len() > MAX_JWKS_BYTES {
        return Err(AuthError::Unavailable);
    }
    let document: RawJwks = serde_json::from_slice(bytes).map_err(|_| AuthError::Unavailable)?;
    if document.keys.is_empty() || document.keys.len() > MAX_JWKS_KEYS {
        return Err(AuthError::Unavailable);
    }
    let mut keys = BTreeMap::new();
    for jwk in document.keys {
        if jwk.kty != "RSA"
            || jwk.alg != "RS256"
            || !valid_kid(&jwk.kid)
            || jwk.public_key_use.as_deref().is_some_and(|value| value != "sig")
            || jwk
                .key_ops
                .as_ref()
                .is_some_and(|operations| !operations.iter().any(|operation| operation == "verify"))
            || !valid_rsa_parameters(&jwk.n, &jwk.e)
        {
            return Err(AuthError::Unavailable);
        }
        let key = DecodingKey::from_rsa_components(&jwk.n, &jwk.e).map_err(|_| AuthError::Unavailable)?;
        if keys.insert(jwk.kid, Arc::new(key)).is_some() {
            return Err(AuthError::Unavailable);
        }
    }
    Ok(keys)
}

fn valid_kid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

fn valid_rsa_parameters(modulus: &str, exponent: &str) -> bool {
    let Ok(modulus) = URL_SAFE_NO_PAD.decode(modulus) else {
        return false;
    };
    if modulus.first().is_none_or(|byte| *byte == 0) || modulus.last().is_none_or(|byte| byte & 1 == 0) {
        return false;
    }
    let leading = modulus[0].leading_zeros() as usize;
    let bits = modulus.len().saturating_mul(8).saturating_sub(leading);
    if !(MIN_RSA_MODULUS_BITS..=MAX_RSA_MODULUS_BITS).contains(&bits) {
        return false;
    }
    let Ok(exponent) = URL_SAFE_NO_PAD.decode(exponent) else {
        return false;
    };
    if exponent.is_empty() || exponent.len() > 8 || (exponent.len() > 1 && exponent[0] == 0) {
        return false;
    }
    let value = exponent.into_iter().fold(0_u64, |value, byte| {
        value.saturating_mul(256).saturating_add(u64::from(byte))
    });
    value == REQUIRED_RSA_EXPONENT
}

fn validated_public_addresses(
    addresses: Vec<std::net::SocketAddr>,
) -> Result<Vec<std::net::SocketAddr>, DnsPolicyError> {
    if addresses.is_empty() || addresses.iter().any(|address| !is_public_ip(address.ip())) {
        return Err(DnsPolicyError);
    }
    Ok(addresses)
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, d] = address.octets();
    if a == 192 && b == 0 && c == 0 {
        return matches!(d, 9 | 10);
    }
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    if address.to_ipv4_mapped().is_some() {
        return false;
    }
    // IANA currently allocates global unicast from 2000::/3. Conservatively
    // reject all other scopes and every special or transition block inside it.
    let segments = address.segments();
    let global_unicast = (segments[0] & 0xe000) == 0x2000;
    let ietf_special = segments[0] == 0x2001 && segments[1] <= 0x01ff;
    let documentation =
        (segments[0] == 0x2001 && segments[1] == 0x0db8) || (segments[0] == 0x3fff && (segments[1] & 0xf000) == 0);
    let six_to_four = segments[0] == 0x2002;
    global_unicast && !ietf_special && !documentation && !six_to_four
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex as StdMutex;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

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

    #[tokio::test(start_paused = true)]
    async fn cache_refreshes_rotation_and_revocation_after_ttl() {
        let source = Arc::new(SequenceSource::new(jwks()));
        let verifier = JwksVerifier::new(source.clone());
        verifier.warm_up().await.unwrap();
        assert_eq!(source.fetch_count(), 1);
        let token = token_with_kid("test-key");
        let key = verifier.decoding_key(&token).await.unwrap();
        assert!(jsonwebtoken::decode::<serde_json::Value>(&token, &key, &jwt_validation(&config())).is_ok());

        let rotated = alternate_modulus();
        source.push(Ok(jwks_document(&[("test-key", &rotated, "AQAB")])));
        tokio::time::advance(JWKS_CACHE_TTL + Duration::from_secs(1)).await;
        let key = verifier.decoding_key(&token).await.unwrap();
        assert!(jsonwebtoken::decode::<serde_json::Value>(&token, &key, &jwt_validation(&config())).is_err());
        assert_eq!(source.fetch_count(), 2);

        source.push(Ok(jwks_document(&[("replacement-key", &rotated, "AQAB")])));
        tokio::time::advance(JWKS_CACHE_TTL + Duration::from_secs(1)).await;
        assert!(matches!(
            verifier.decoding_key(&token).await,
            Err(AuthError::Unauthorized)
        ));
        assert_eq!(source.fetch_count(), 3);
    }

    #[tokio::test]
    async fn concurrent_random_kids_trigger_one_bounded_refresh() {
        let source = Arc::new(SequenceSource::new(jwks()));
        let verifier = JwksVerifier::new(source.clone());
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
        assert!(verifier.cache.read().await.negative_kids.len() <= MAX_NEGATIVE_KIDS);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_failure_is_cooled_down_without_using_stale_keys() {
        let source = Arc::new(SequenceSource::new(jwks()));
        let verifier = JwksVerifier::new(source.clone());
        verifier.warm_up().await.unwrap();
        source.push(Err(AuthError::Unavailable));
        tokio::time::advance(JWKS_CACHE_TTL + Duration::from_secs(1)).await;
        let token = token_with_kid("test-key");
        assert!(verifier.decoding_key(&token).await.is_err());
        assert!(verifier.decoding_key(&token).await.is_err());
        assert_eq!(source.fetch_count(), 2);
    }

    #[test]
    fn weak_oversized_and_unsafe_exponent_jwks_are_rejected() {
        let weak = vec![0x81; 128];
        let oversized = vec![0x81; 1025];
        let cases = [
            jwks_document(&[("weak", &URL_SAFE_NO_PAD.encode(weak), "AQAB")]),
            jwks_document(&[("oversized", &URL_SAFE_NO_PAD.encode(oversized), "AQAB")]),
            jwks_document(&[("exponent", RSA_N, &URL_SAFE_NO_PAD.encode([3_u8]))]),
        ];
        for document in cases {
            assert!(matches!(parse_jwks(&document), Err(AuthError::Unavailable)));
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
    fn connect_time_dns_policy_rejects_non_public_answers() {
        for address in [
            "0.0.0.1:443",
            "127.0.0.1:443",
            "10.0.0.1:443",
            "169.254.1.1:443",
            "100.64.0.1:443",
            "192.0.0.8:443",
            "192.0.0.170:443",
            "192.0.2.1:443",
            "192.88.99.2:443",
            "192.168.1.1:443",
            "198.18.0.1:443",
            "198.51.100.1:443",
            "203.0.113.1:443",
            "240.0.0.1:443",
            "[::1]:443",
            "[fe80::1]:443",
            "[fd00::1]:443",
        ] {
            assert!(validated_public_addresses(vec![address.parse().unwrap()]).is_err());
        }
        for address in [
            "8.8.8.8:443",
            "192.0.0.9:443",
            "192.0.0.10:443",
            "192.31.196.1:443",
            "192.52.193.1:443",
            "192.175.48.1:443",
        ] {
            assert!(
                validated_public_addresses(vec![address.parse().unwrap()]).is_ok(),
                "rejected {address}"
            );
        }
        for address in [
            "[::]:443",
            "[::127.0.0.1]:443",
            "[::ffff:127.0.0.1]:443",
            "[::ffff:8.8.8.8]:443",
            "[64:ff9b::7f00:1]:443",
            "[64:ff9b:1::7f00:1]:443",
            "[2002:7f00:1::]:443",
            "[2001:0:4136:e378:8000:63bf:3fff:fdd2]:443",
            "[2001:2::1]:443",
            "[2001:10::1]:443",
            "[2001:20::1]:443",
            "[2001:db8::1]:443",
            "[3fff:0fff::1]:443",
            "[ff02::1]:443",
        ] {
            assert!(
                validated_public_addresses(vec![address.parse().unwrap()]).is_err(),
                "accepted {address}"
            );
        }
        for address in [
            "[2606:4700:4700::1111]:443",
            "[2001:4860:4860::8888]:443",
            "[3ff1::1]:443",
            "[3fff:1000::1]:443",
        ] {
            assert!(
                validated_public_addresses(vec![address.parse().unwrap()]).is_ok(),
                "rejected {address}"
            );
        }
        assert!(validated_public_addresses(vec![
            "192.31.196.1:443".parse().unwrap(),
            "127.0.0.1:443".parse().unwrap(),
        ])
        .is_err());
    }
}
