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
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::{McpError, McpResult};
use arc_swap::ArcSwap;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use serde::Deserialize;
use tokio::sync::Mutex;

const MAX_JWKS_BYTES: usize = 256 * 1024;
const MAX_JWKS_KEYS: usize = 64;
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);

pub trait JwksSource: Send + Sync {
    fn fetch(&self) -> impl Future<Output = McpResult<Vec<u8>>> + Send;
}

#[derive(Clone)]
pub struct HttpJwksSource {
    client: reqwest::Client,
    url: Arc<str>,
}

impl HttpJwksSource {
    pub fn new(url: impl Into<Arc<str>>, ca_path: Option<&Path>) -> McpResult<Self> {
        let mut builder = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(JWKS_FETCH_TIMEOUT);
        if let Some(ca_path) = ca_path {
            let pem = std::fs::read(ca_path).map_err(McpError::from_source)?;
            let certificates = reqwest::Certificate::from_pem_bundle(&pem).map_err(McpError::from_source)?;
            if certificates.is_empty() {
                return Err(JwksError::InvalidCa.into());
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let client = builder.build().map_err(McpError::from_source)?;
        Ok(Self {
            client,
            url: url.into(),
        })
    }
}

impl JwksSource for HttpJwksSource {
    async fn fetch(&self) -> McpResult<Vec<u8>> {
        let mut response = self
            .client
            .get(self.url.as_ref())
            .send()
            .await
            .map_err(McpError::from_source)?
            .error_for_status()
            .map_err(McpError::from_source)?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_JWKS_BYTES as u64)
        {
            return Err(JwksError::DocumentTooLarge.into());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(McpError::from_source)? {
            let next_len = body.len().checked_add(chunk.len()).ok_or(JwksError::DocumentTooLarge)?;
            if next_len > MAX_JWKS_BYTES {
                return Err(JwksError::DocumentTooLarge.into());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

pub struct JwksVerifier<S = HttpJwksSource> {
    source: Arc<S>,
    active: Arc<ArcSwap<JwksSnapshot>>,
    refresh_writer: Arc<Mutex<()>>,
    refresh_after: Duration,
    max_stale: Duration,
}

impl<S> Clone for JwksVerifier<S> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            active: self.active.clone(),
            refresh_writer: self.refresh_writer.clone(),
            refresh_after: self.refresh_after,
            max_stale: self.max_stale,
        }
    }
}

impl<S> std::fmt::Debug for JwksVerifier<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JwksVerifier")
            .field("generation", &self.active.load().generation)
            .field("refresh_after", &self.refresh_after)
            .field("max_stale", &self.max_stale)
            .finish_non_exhaustive()
    }
}

impl<S> JwksVerifier<S>
where
    S: JwksSource,
{
    pub fn new(source: Arc<S>, refresh_after: Duration, max_stale: Duration) -> Self {
        Self {
            source,
            active: Arc::new(ArcSwap::from_pointee(JwksSnapshot::empty())),
            refresh_writer: Arc::new(Mutex::new(())),
            refresh_after,
            max_stale,
        }
    }

    pub async fn warm_up(&self) -> McpResult<()> {
        let observed_generation = self.active.load().generation;
        self.refresh(observed_generation).await?;
        if self.active.load().generation == 0 {
            return Err(JwksError::Unavailable.into());
        }
        Ok(())
    }

    pub(crate) async fn decoding_key(
        &self,
        token: &str,
    ) -> McpResult<Result<Arc<DecodingKey>, super::http_auth::HttpAuthRejection>> {
        use super::http_auth::HttpAuthRejection;
        let header = match jsonwebtoken::decode_header(token) {
            Ok(header) => header,
            Err(source) => return Ok(Err(HttpAuthRejection::invalid_token(source))),
        };
        if header.alg != Algorithm::RS256 {
            return Ok(Err(HttpAuthRejection::unauthorized()));
        }
        let Some(kid) = header.kid.filter(|kid| valid_kid(kid)) else {
            return Ok(Err(HttpAuthRejection::unauthorized()));
        };

        let snapshot = self.active.load_full();
        let should_refresh = snapshot.generation == 0
            || snapshot.loaded_at.elapsed() >= self.refresh_after
            || !snapshot.keys.contains_key(&kid);
        if should_refresh {
            let refresh_result = self.refresh(snapshot.generation).await;
            if let Err(error) = refresh_result {
                let active = self.active.load_full();
                if active.generation == 0
                    || active.loaded_at.elapsed() > self.max_stale
                    || !active.keys.contains_key(&kid)
                {
                    return Err(error);
                }
            }
        }

        Ok(self
            .active
            .load()
            .keys
            .get(&kid)
            .cloned()
            .ok_or(HttpAuthRejection::unauthorized()))
    }

    pub fn active_generation(&self) -> u64 {
        self.active.load().generation
    }

    async fn refresh(&self, observed_generation: u64) -> McpResult<()> {
        let _writer = self.refresh_writer.lock().await;
        if self.active.load().generation != observed_generation {
            return Ok(());
        }
        let bytes = self.source.fetch().await?;
        let keys = parse_jwks(&bytes)?;
        let generation = observed_generation
            .checked_add(1)
            .ok_or(JwksError::GenerationExhausted)?;
        self.active.store(Arc::new(JwksSnapshot {
            generation,
            loaded_at: Instant::now(),
            keys,
        }));
        Ok(())
    }
}

struct JwksSnapshot {
    generation: u64,
    loaded_at: Instant,
    keys: BTreeMap<String, Arc<DecodingKey>>,
}

impl JwksSnapshot {
    fn empty() -> Self {
        Self {
            generation: 0,
            loaded_at: Instant::now(),
            keys: BTreeMap::new(),
        }
    }
}

#[derive(Deserialize)]
struct RawJwks {
    keys: Vec<RawJwk>,
}

#[derive(Deserialize)]
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

fn parse_jwks(bytes: &[u8]) -> McpResult<BTreeMap<String, Arc<DecodingKey>>> {
    if bytes.len() > MAX_JWKS_BYTES {
        return Err(JwksError::DocumentTooLarge.into());
    }
    let document: RawJwks = serde_json::from_slice(bytes).map_err(McpError::from_source)?;
    if document.keys.is_empty() || document.keys.len() > MAX_JWKS_KEYS {
        return Err(JwksError::InvalidDocument.into());
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
        {
            return Err(JwksError::InvalidDocument.into());
        }
        let key = DecodingKey::from_rsa_components(&jwk.n, &jwk.e).map_err(McpError::from_source)?;
        if keys.insert(jwk.kid, Arc::new(key)).is_some() {
            return Err(JwksError::InvalidDocument.into());
        }
    }
    Ok(keys)
}

fn valid_kid(kid: &str) -> bool {
    !kid.is_empty() && kid.len() <= 128 && kid.bytes().all(|byte| byte.is_ascii_graphic())
}

#[derive(Debug, thiserror::Error)]
enum JwksError {
    #[error("JWKS endpoint is unavailable")]
    Unavailable,
    #[error("JWKS document exceeds the configured size limit")]
    DocumentTooLarge,
    #[error("JWKS document is invalid")]
    InvalidDocument,
    #[error("JWKS generation is exhausted")]
    GenerationExhausted,
    #[error("JWKS CA bundle is invalid")]
    InvalidCa,
}

impl From<JwksError> for McpError {
    fn from(source: JwksError) -> Self {
        Self::from_source(source)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

    #[test]
    fn parser_rejects_symmetric_algorithm_and_duplicate_kids() {
        let symmetric = br#"{"keys":[{"kty":"oct","kid":"one","alg":"HS256","k":"c2VjcmV0"}]}"#;
        assert!(parse_jwks(symmetric).is_err());

        let duplicate = jwks_document(&["one", "one"]);
        assert!(parse_jwks(&duplicate).is_err());
    }

    #[test]
    fn http_source_accepts_only_readable_pem_ca_bundles() {
        let temp_dir = tempfile::tempdir().unwrap();
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["issuer.example.test".to_string()]).unwrap();
        let ca_path = temp_dir.path().join("issuer-ca.pem");
        std::fs::write(&ca_path, cert.pem()).unwrap();

        assert!(HttpJwksSource::new("https://issuer.example.test/jwks", Some(&ca_path)).is_ok());

        let invalid_path = temp_dir.path().join("invalid-ca.pem");
        std::fs::write(&invalid_path, b"not a certificate").unwrap();
        assert!(HttpJwksSource::new("https://issuer.example.test/jwks", Some(&invalid_path)).is_err());
    }

    #[tokio::test]
    async fn rotation_publishes_whole_generations_and_failed_refresh_keeps_last_known_good() {
        let source = Arc::new(QueueSource::new([
            Ok(jwks_document(&["one"])),
            Ok(jwks_document(&["two"])),
            Err(JwksError::Unavailable.into()),
        ]));
        let verifier = JwksVerifier::new(source, Duration::from_secs(60), Duration::from_secs(60));

        verifier.warm_up().await.unwrap();
        assert_eq!(verifier.active_generation(), 1);
        assert!(verifier
            .decoding_key(&token_header("one", "RS256"))
            .await
            .unwrap()
            .is_ok());
        assert!(verifier
            .decoding_key(&token_header("two", "RS256"))
            .await
            .unwrap()
            .is_ok());
        assert_eq!(verifier.active_generation(), 2);
        assert!(verifier.decoding_key(&token_header("one", "RS256")).await.is_err());
        assert_eq!(verifier.active_generation(), 2);
        assert!(verifier
            .decoding_key(&token_header("two", "RS256"))
            .await
            .unwrap()
            .is_ok());
    }

    #[tokio::test]
    async fn verifier_requires_kid_and_rs256() {
        let source = Arc::new(QueueSource::new([Ok(jwks_document(&["one"]))]));
        let verifier = JwksVerifier::new(source, Duration::from_secs(60), Duration::from_secs(60));
        verifier.warm_up().await.unwrap();

        assert!(verifier
            .decoding_key(&token_header("one", "HS256"))
            .await
            .unwrap()
            .is_err());
        assert!(verifier
            .decoding_key("eyJhbGciOiJSUzI1NiJ9.e30.invalid")
            .await
            .unwrap()
            .is_err());
    }

    #[test]
    fn source_future_is_send_and_verifier_clone_does_not_require_source_clone() {
        fn assert_send<T: Send>(_: T) {}
        fn assert_source_future_is_send<S: JwksSource>(source: &S) {
            assert_send(source.fetch());
        }

        let source = Arc::new(QueueSource::new([Ok(jwks_document(&["one"]))]));
        assert_source_future_is_send(source.as_ref());
        let verifier = JwksVerifier::new(source, Duration::from_secs(60), Duration::from_secs(60));
        let _cloned = verifier.clone();
    }

    fn jwks_document(kids: &[&str]) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "keys": kids.iter().map(|kid| serde_json::json!({
                "kty": "RSA",
                "kid": kid,
                "alg": "RS256",
                "use": "sig",
                "key_ops": ["verify"],
                "n": RSA_N,
                "e": "AQAB"
            })).collect::<Vec<_>>()
        }))
        .unwrap()
    }

    fn token_header(kid: &str, algorithm: &str) -> String {
        use base64::Engine;
        let header = serde_json::json!({"alg": algorithm, "kid": kid, "typ": "JWT"});
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(header.to_string());
        format!("{encoded}.e30.invalid")
    }

    struct QueueSource {
        responses: Mutex<VecDeque<McpResult<Vec<u8>>>>,
    }

    impl QueueSource {
        fn new(responses: impl IntoIterator<Item = McpResult<Vec<u8>>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }
    }

    impl JwksSource for QueueSource {
        async fn fetch(&self) -> McpResult<Vec<u8>> {
            self.responses
                .lock()
                .await
                .pop_front()
                .unwrap_or_else(|| Err(JwksError::Unavailable.into()))
        }
    }
}
