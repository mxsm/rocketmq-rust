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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use serde::Deserialize;
use tokio::sync::Mutex;

const MAX_JWKS_BYTES: usize = 256 * 1024;
const MAX_JWKS_KEYS: usize = 64;
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(5);

#[async_trait]
pub trait JwksSource: Send + Sync {
    async fn fetch(&self) -> Result<Vec<u8>, JwksError>;
}

#[derive(Clone)]
pub struct HttpJwksSource {
    client: reqwest::Client,
    url: Arc<str>,
}

impl HttpJwksSource {
    pub fn new(url: impl Into<Arc<str>>) -> Result<Self, JwksError> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(JWKS_FETCH_TIMEOUT)
            .build()
            .map_err(|_| JwksError::Unavailable)?;
        Ok(Self {
            client,
            url: url.into(),
        })
    }
}

#[async_trait]
impl JwksSource for HttpJwksSource {
    async fn fetch(&self) -> Result<Vec<u8>, JwksError> {
        let mut response = self
            .client
            .get(self.url.as_ref())
            .send()
            .await
            .map_err(|_| JwksError::Unavailable)?
            .error_for_status()
            .map_err(|_| JwksError::Unavailable)?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_JWKS_BYTES as u64)
        {
            return Err(JwksError::DocumentTooLarge);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| JwksError::Unavailable)? {
            let next_len = body.len().checked_add(chunk.len()).ok_or(JwksError::DocumentTooLarge)?;
            if next_len > MAX_JWKS_BYTES {
                return Err(JwksError::DocumentTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[derive(Clone)]
pub struct JwksVerifier {
    source: Arc<dyn JwksSource>,
    active: Arc<ArcSwap<JwksSnapshot>>,
    refresh_writer: Arc<Mutex<()>>,
    refresh_after: Duration,
    max_stale: Duration,
}

impl std::fmt::Debug for JwksVerifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JwksVerifier")
            .field("generation", &self.active.load().generation)
            .field("refresh_after", &self.refresh_after)
            .field("max_stale", &self.max_stale)
            .finish_non_exhaustive()
    }
}

impl JwksVerifier {
    pub fn new(source: Arc<dyn JwksSource>, refresh_after: Duration, max_stale: Duration) -> Self {
        Self {
            source,
            active: Arc::new(ArcSwap::from_pointee(JwksSnapshot::empty())),
            refresh_writer: Arc::new(Mutex::new(())),
            refresh_after,
            max_stale,
        }
    }

    pub async fn warm_up(&self) -> Result<(), JwksError> {
        let observed_generation = self.active.load().generation;
        self.refresh(observed_generation).await?;
        if self.active.load().generation == 0 {
            return Err(JwksError::Unavailable);
        }
        Ok(())
    }

    pub async fn decoding_key(&self, token: &str) -> Result<Arc<DecodingKey>, JwksError> {
        let header = jsonwebtoken::decode_header(token).map_err(|_| JwksError::RejectedToken)?;
        if header.alg != Algorithm::RS256 {
            return Err(JwksError::RejectedToken);
        }
        let kid = header
            .kid
            .filter(|kid| valid_kid(kid))
            .ok_or(JwksError::RejectedToken)?;

        let snapshot = self.active.load_full();
        let should_refresh = snapshot.generation == 0
            || snapshot.loaded_at.elapsed() >= self.refresh_after
            || !snapshot.keys.contains_key(&kid);
        if should_refresh {
            let refresh_result = self.refresh(snapshot.generation).await;
            if refresh_result.is_err() {
                let active = self.active.load_full();
                if active.generation == 0
                    || active.loaded_at.elapsed() > self.max_stale
                    || !active.keys.contains_key(&kid)
                {
                    return Err(JwksError::RejectedToken);
                }
            }
        }

        self.active
            .load()
            .keys
            .get(&kid)
            .cloned()
            .ok_or(JwksError::RejectedToken)
    }

    pub fn active_generation(&self) -> u64 {
        self.active.load().generation
    }

    async fn refresh(&self, observed_generation: u64) -> Result<(), JwksError> {
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

fn parse_jwks(bytes: &[u8]) -> Result<BTreeMap<String, Arc<DecodingKey>>, JwksError> {
    if bytes.len() > MAX_JWKS_BYTES {
        return Err(JwksError::DocumentTooLarge);
    }
    let document: RawJwks = serde_json::from_slice(bytes).map_err(|_| JwksError::InvalidDocument)?;
    if document.keys.is_empty() || document.keys.len() > MAX_JWKS_KEYS {
        return Err(JwksError::InvalidDocument);
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
            return Err(JwksError::InvalidDocument);
        }
        let key = DecodingKey::from_rsa_components(&jwk.n, &jwk.e).map_err(|_| JwksError::InvalidDocument)?;
        if keys.insert(jwk.kid, Arc::new(key)).is_some() {
            return Err(JwksError::InvalidDocument);
        }
    }
    Ok(keys)
}

fn valid_kid(kid: &str) -> bool {
    !kid.is_empty() && kid.len() <= 128 && kid.bytes().all(|byte| byte.is_ascii_graphic())
}

#[derive(Debug, thiserror::Error)]
pub enum JwksError {
    #[error("JWKS endpoint is unavailable")]
    Unavailable,
    #[error("JWKS document exceeds the configured size limit")]
    DocumentTooLarge,
    #[error("JWKS document is invalid")]
    InvalidDocument,
    #[error("JWT was rejected")]
    RejectedToken,
    #[error("JWKS generation is exhausted")]
    GenerationExhausted,
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

    #[test]
    fn parser_rejects_symmetric_algorithm_and_duplicate_kids() {
        let symmetric = br#"{"keys":[{"kty":"oct","kid":"one","alg":"HS256","k":"c2VjcmV0"}]}"#;
        assert!(matches!(parse_jwks(symmetric), Err(JwksError::InvalidDocument)));

        let duplicate = jwks_document(&["one", "one"]);
        assert!(matches!(parse_jwks(&duplicate), Err(JwksError::InvalidDocument)));
    }

    #[tokio::test]
    async fn rotation_publishes_whole_generations_and_failed_refresh_keeps_last_known_good() {
        let source = Arc::new(QueueSource::new([
            Ok(jwks_document(&["one"])),
            Ok(jwks_document(&["two"])),
            Err(JwksError::Unavailable),
        ]));
        let verifier = JwksVerifier::new(source, Duration::from_secs(60), Duration::from_secs(60));

        verifier.warm_up().await.unwrap();
        assert_eq!(verifier.active_generation(), 1);
        assert!(verifier.decoding_key(&token_header("one", "RS256")).await.is_ok());
        assert!(verifier.decoding_key(&token_header("two", "RS256")).await.is_ok());
        assert_eq!(verifier.active_generation(), 2);
        assert!(verifier.decoding_key(&token_header("one", "RS256")).await.is_err());
        assert_eq!(verifier.active_generation(), 2);
        assert!(verifier.decoding_key(&token_header("two", "RS256")).await.is_ok());
    }

    #[tokio::test]
    async fn verifier_requires_kid_and_rs256() {
        let source = Arc::new(QueueSource::new([Ok(jwks_document(&["one"]))]));
        let verifier = JwksVerifier::new(source, Duration::from_secs(60), Duration::from_secs(60));
        verifier.warm_up().await.unwrap();

        assert!(verifier.decoding_key(&token_header("one", "HS256")).await.is_err());
        assert!(verifier.decoding_key("eyJhbGciOiJSUzI1NiJ9.e30.invalid").await.is_err());
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
        responses: Mutex<VecDeque<Result<Vec<u8>, JwksError>>>,
    }

    impl QueueSource {
        fn new(responses: impl IntoIterator<Item = Result<Vec<u8>, JwksError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl JwksSource for QueueSource {
        async fn fetch(&self) -> Result<Vec<u8>, JwksError> {
            self.responses
                .lock()
                .await
                .pop_front()
                .unwrap_or(Err(JwksError::Unavailable))
        }
    }
}
