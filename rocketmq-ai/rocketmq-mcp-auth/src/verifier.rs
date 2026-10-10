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
use std::sync::Arc;

use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::document::parse_jwks;
use crate::document::JwksDocumentError;
use crate::policy::JwksPolicy;

/// Where JWKS documents come from.
pub trait JwksSource: Send + Sync {
    /// Why a document could not be fetched.
    type Error: Send;

    /// Fetches the current JWKS document.
    fn fetch(&self) -> impl Future<Output = Result<Vec<u8>, Self::Error>> + Send;
}

/// Why no verification key was returned for a token.
#[derive(Debug, thiserror::Error)]
pub enum KeyError<E> {
    /// The token names no key that the current key set holds. Fetching again would not help yet.
    #[error("token was rejected")]
    Rejected(#[from] TokenRejection),
    /// The key set could not be loaded, so nothing is known about the token.
    #[error("JWKS is unavailable")]
    Unavailable(#[source] KeySetError<E>),
}

/// What is wrong with a token before its signature is looked at.
#[derive(Debug, thiserror::Error)]
pub enum TokenRejection {
    /// The token does not start with a readable JOSE header.
    #[error("token header is malformed")]
    MalformedHeader(#[source] jsonwebtoken::errors::Error),
    /// The header names an algorithm other than RS256.
    #[error("token is not signed with RS256")]
    Algorithm,
    /// The header has no `kid`, or one outside the policy's character set.
    #[error("token key id is missing or malformed")]
    KeyId,
    /// The key set holds no key with the token's `kid`.
    #[error("token key id is unknown")]
    UnknownKey,
}

/// Why a key set could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum KeySetError<E> {
    /// The source failed to return a document.
    #[error("JWKS source failed")]
    Source(#[source] E),
    /// The source returned a document that was not accepted.
    #[error("JWKS document was rejected")]
    Document(#[from] JwksDocumentError),
    /// The last fetch failed and the pause before the next one has not passed.
    #[error("JWKS refresh is paused after a failure")]
    CoolingDown,
    /// The generation counter cannot advance.
    #[error("JWKS generation is exhausted")]
    GenerationExhausted,
}

/// Selects the RS256 verification key for a token from a cached, refreshed JWKS document.
///
/// Fetches are serialized: concurrent lookups that all need a refresh share one fetch.
pub struct JwksVerifier<S> {
    source: Arc<S>,
    policy: JwksPolicy,
    cache: Arc<RwLock<JwksCache>>,
    refresh: Arc<Mutex<()>>,
}

struct JwksCache {
    keys: BTreeMap<String, Arc<DecodingKey>>,
    generation: u64,
    expires_at: Instant,
    stale_until: Instant,
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
            stale_until: now,
            refresh_retry_at: now,
            unknown_retry_at: now,
            negative_kids: BTreeMap::new(),
        }
    }

    fn cached_key(&self, kid: &str, now: Instant) -> Option<Arc<DecodingKey>> {
        (now < self.expires_at).then(|| self.keys.get(kid).cloned()).flatten()
    }

    /// A key past its time to live that may still stand in while fetching keeps failing.
    fn stale_key(&self, kid: &str, now: Instant) -> Option<Arc<DecodingKey>> {
        (now < self.stale_until).then(|| self.keys.get(kid).cloned()).flatten()
    }

    fn rejects_without_refresh(&self, kid: &str, now: Instant) -> bool {
        self.negative_kids.get(kid).is_some_and(|expires| now < *expires)
            || (now < self.expires_at && now < self.unknown_retry_at)
            || (now >= self.expires_at && now < self.refresh_retry_at)
    }

    /// The answer for `kid` that needs no fetch, or `None` when a fetch is due.
    fn settled<E>(&self, kid: &str, now: Instant) -> Option<Result<Arc<DecodingKey>, KeyError<E>>> {
        if let Some(key) = self.cached_key(kid, now) {
            return Some(Ok(key));
        }
        if !self.rejects_without_refresh(kid, now) {
            return None;
        }
        if now >= self.refresh_retry_at {
            // The last fetch succeeded and did not hold this key.
            return Some(Err(TokenRejection::UnknownKey.into()));
        }
        Some(
            self.stale_key(kid, now)
                .ok_or(KeyError::Unavailable(KeySetError::CoolingDown)),
        )
    }

    fn record_negative(&mut self, kid: String, now: Instant, policy: &JwksPolicy) {
        self.unknown_retry_at = now + policy.unknown_kid_cooldown;
        if self.negative_kids.len() >= policy.max_negative_kids {
            self.negative_kids.clear();
        }
        self.negative_kids.insert(kid, now + policy.unknown_kid_cooldown);
    }
}

impl<S> Clone for JwksVerifier<S> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            policy: self.policy,
            cache: self.cache.clone(),
            refresh: self.refresh.clone(),
        }
    }
}

impl<S> std::fmt::Debug for JwksVerifier<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JwksVerifier")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl<S: JwksSource> JwksVerifier<S> {
    /// Creates a verifier with no keys. The first lookup, or [`warm_up`](Self::warm_up), fetches them.
    pub fn new(source: Arc<S>, policy: JwksPolicy) -> Self {
        let now = Instant::now();
        Self {
            source,
            policy,
            cache: Arc::new(RwLock::new(JwksCache::empty(now))),
            refresh: Arc::new(Mutex::new(())),
        }
    }

    /// The policy this verifier applies.
    pub fn policy(&self) -> &JwksPolicy {
        &self.policy
    }

    /// Number of documents accepted so far.
    pub async fn generation(&self) -> u64 {
        self.cache.read().await.generation
    }

    /// Fetches the key set now, whatever the cache holds.
    ///
    /// # Errors
    ///
    /// Returns a [`KeySetError`] when the source fails or its document is not accepted. Keys that
    /// were already cached stay in place.
    pub async fn warm_up(&self) -> Result<(), KeySetError<S::Error>> {
        let _writer = self.refresh.lock().await;
        self.fetch_generation(Instant::now()).await
    }

    /// Returns the key that must verify `token`, fetching the key set when the cache cannot answer.
    ///
    /// Only the token header is read. The caller still verifies the signature and the claims.
    ///
    /// # Errors
    ///
    /// Returns [`KeyError::Rejected`] when the header is unusable or names a key that the current
    /// key set does not hold, and [`KeyError::Unavailable`] when the key set could not be loaded and
    /// no cached key may stand in.
    pub async fn decoding_key(&self, token: &str) -> Result<Arc<DecodingKey>, KeyError<S::Error>> {
        let header = jsonwebtoken::decode_header(token).map_err(TokenRejection::MalformedHeader)?;
        if header.alg != Algorithm::RS256 {
            return Err(TokenRejection::Algorithm.into());
        }
        let kid = header
            .kid
            .filter(|value| self.policy.kid_charset.accepts(value))
            .ok_or(TokenRejection::KeyId)?;
        let now = Instant::now();
        if let Some(settled) = self.cache.read().await.settled(&kid, now) {
            return settled;
        }

        let _writer = self.refresh.lock().await;
        let now = Instant::now();
        if let Some(settled) = self.cache.read().await.settled(&kid, now) {
            return settled;
        }

        if let Err(error) = self.fetch_generation(now).await {
            let mut cache = self.cache.write().await;
            cache.refresh_retry_at = now + self.policy.unknown_kid_cooldown;
            let stale = cache.stale_key(&kid, now);
            cache.record_negative(kid, now, &self.policy);
            return stale.ok_or(KeyError::Unavailable(error));
        }
        let mut cache = self.cache.write().await;
        if let Some(key) = cache.cached_key(&kid, now) {
            return Ok(key);
        }
        cache.record_negative(kid, now, &self.policy);
        Err(TokenRejection::UnknownKey.into())
    }

    async fn fetch_generation(&self, now: Instant) -> Result<(), KeySetError<S::Error>> {
        let bytes = self.source.fetch().await.map_err(KeySetError::Source)?;
        let parsed = parse_jwks(&bytes, &self.policy)?;
        let mut cache = self.cache.write().await;
        cache.generation = cache
            .generation
            .checked_add(1)
            .ok_or(KeySetError::GenerationExhausted)?;
        cache.keys = parsed;
        cache.expires_at = now + self.policy.cache_ttl;
        cache.stale_until = now + self.policy.max_stale;
        cache.refresh_retry_at = now;
        cache.unknown_retry_at = now;
        cache.negative_kids.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::future::join_all;

    use super::*;
    use crate::test_support::jwks_document;
    use crate::test_support::policy;
    use crate::test_support::token_header;
    use crate::test_support::SequenceSource;
    use crate::test_support::SourceDown;
    use crate::test_support::RSA_N;

    fn jwks(kids: &[&str]) -> Vec<u8> {
        jwks_document(&kids.iter().map(|kid| (*kid, RSA_N, "AQAB")).collect::<Vec<_>>())
    }

    fn token(kid: &str) -> String {
        token_header(Some(kid), "RS256")
    }

    #[tokio::test(start_paused = true)]
    async fn cache_refreshes_rotation_and_revocation_after_ttl() {
        let source = Arc::new(SequenceSource::new(jwks(&["test-key"])));
        let verifier = JwksVerifier::new(source.clone(), policy());
        verifier.warm_up().await.unwrap();
        assert_eq!(source.fetch_count(), 1);
        assert_eq!(verifier.generation().await, 1);
        let first = verifier.decoding_key(&token("test-key")).await.unwrap();
        assert!(Arc::ptr_eq(
            &first,
            &verifier.decoding_key(&token("test-key")).await.unwrap()
        ));
        assert_eq!(source.fetch_count(), 1);

        source.push(Ok(jwks(&["test-key", "next-key"])));
        tokio::time::advance(policy().cache_ttl + Duration::from_secs(1)).await;
        let second = verifier.decoding_key(&token("test-key")).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(source.fetch_count(), 2);
        assert_eq!(verifier.generation().await, 2);

        source.push(Ok(jwks(&["replacement-key"])));
        tokio::time::advance(policy().cache_ttl + Duration::from_secs(1)).await;
        assert!(matches!(
            verifier.decoding_key(&token("test-key")).await,
            Err(KeyError::Rejected(TokenRejection::UnknownKey))
        ));
        assert_eq!(source.fetch_count(), 3);
    }

    #[tokio::test]
    async fn concurrent_random_kids_trigger_one_bounded_refresh() {
        let source = Arc::new(SequenceSource::new(jwks(&["test-key"])));
        let verifier = JwksVerifier::new(source.clone(), policy());
        verifier.warm_up().await.unwrap();
        let attempts = (0..64)
            .map(|index| {
                let token = token(&format!("random-{index}"));
                let verifier = verifier.clone();
                async move { verifier.decoding_key(&token).await }
            })
            .collect::<Vec<_>>();
        assert!(join_all(attempts).await.into_iter().all(|result| result.is_err()));
        assert_eq!(source.fetch_count(), 2);
        assert!(verifier.cache.read().await.negative_kids.len() <= policy().max_negative_kids);
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_kid_does_not_refetch_within_the_cooldown() {
        let source = Arc::new(SequenceSource::new(jwks(&["test-key"])));
        let verifier = JwksVerifier::new(source.clone(), policy());
        verifier.warm_up().await.unwrap();

        for kid in ["unknown-a", "unknown-b", "unknown-a"] {
            assert!(matches!(
                verifier.decoding_key(&token(kid)).await,
                Err(KeyError::Rejected(TokenRejection::UnknownKey))
            ));
        }
        assert_eq!(source.fetch_count(), 2);
        // A key that is known keeps working during the pause.
        verifier.decoding_key(&token("test-key")).await.unwrap();

        tokio::time::advance(policy().unknown_kid_cooldown - Duration::from_millis(1)).await;
        assert!(verifier.decoding_key(&token("unknown-c")).await.is_err());
        assert_eq!(source.fetch_count(), 2);

        // Once the pause is over, a key published in the meantime is picked up by one fetch.
        source.push(Ok(jwks(&["test-key", "unknown-a"])));
        tokio::time::advance(Duration::from_millis(1)).await;
        verifier.decoding_key(&token("unknown-a")).await.unwrap();
        assert_eq!(source.fetch_count(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn negative_cache_is_bounded() {
        let mut bounded = policy();
        bounded.max_negative_kids = 4;
        let source = Arc::new(SequenceSource::new(jwks(&["test-key"])));
        let verifier = JwksVerifier::new(source.clone(), bounded);
        verifier.warm_up().await.unwrap();
        // Only failed fetches let entries pile up: an accepted document starts the list afresh.
        source.push(Err(SourceDown));
        let mut largest = 0;
        for index in 0..32 {
            assert!(verifier
                .decoding_key(&token(&format!("unknown-{index}")))
                .await
                .is_err());
            largest = largest.max(verifier.cache.read().await.negative_kids.len());
            tokio::time::advance(bounded.unknown_kid_cooldown).await;
        }
        assert_eq!(largest, bounded.max_negative_kids);
        assert_eq!(source.fetch_count(), 33);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_failure_is_cooled_down_without_using_stale_keys() {
        let source = Arc::new(SequenceSource::new(jwks(&["test-key"])));
        let verifier = JwksVerifier::new(source.clone(), policy());
        verifier.warm_up().await.unwrap();
        source.push(Err(SourceDown));
        tokio::time::advance(policy().cache_ttl + Duration::from_secs(1)).await;
        assert!(matches!(
            verifier.decoding_key(&token("test-key")).await,
            Err(KeyError::Unavailable(KeySetError::Source(SourceDown)))
        ));
        assert!(matches!(
            verifier.decoding_key(&token("test-key")).await,
            Err(KeyError::Unavailable(KeySetError::CoolingDown))
        ));
        assert_eq!(source.fetch_count(), 2);
        assert_eq!(verifier.generation().await, 1);

        // The source has recovered; the next fetch waits for the pause to pass.
        source.push(Ok(jwks(&["test-key"])));
        tokio::time::advance(policy().unknown_kid_cooldown).await;
        verifier.decoding_key(&token("test-key")).await.unwrap();
        assert_eq!(source.fetch_count(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_keys_stand_in_only_while_refresh_fails_and_only_until_max_stale() {
        let mut lenient = policy();
        lenient.max_stale = lenient.cache_ttl * 3;
        let source = Arc::new(SequenceSource::new(jwks(&["test-key"])));
        let verifier = JwksVerifier::new(source.clone(), lenient);
        verifier.warm_up().await.unwrap();
        let fresh = verifier.decoding_key(&token("test-key")).await.unwrap();

        source.push(Err(SourceDown));
        tokio::time::advance(lenient.cache_ttl + Duration::from_secs(1)).await;
        // The failed fetch itself and the lookups during the pause are served by the old key.
        for _ in 0..3 {
            let stale = verifier.decoding_key(&token("test-key")).await.unwrap();
            assert!(Arc::ptr_eq(&fresh, &stale));
        }
        assert_eq!(source.fetch_count(), 2);
        // An old key set says nothing about a key it never held.
        assert!(matches!(
            verifier.decoding_key(&token("unknown")).await,
            Err(KeyError::Unavailable(KeySetError::CoolingDown))
        ));

        // Each pause that ends costs one more fetch, not one per lookup.
        tokio::time::advance(lenient.unknown_kid_cooldown).await;
        for _ in 0..3 {
            verifier.decoding_key(&token("test-key")).await.unwrap();
        }
        assert_eq!(source.fetch_count(), 3);

        tokio::time::advance(lenient.max_stale).await;
        assert!(matches!(
            verifier.decoding_key(&token("test-key")).await,
            Err(KeyError::Unavailable(KeySetError::Source(SourceDown)))
        ));
        assert!(matches!(
            verifier.decoding_key(&token("test-key")).await,
            Err(KeyError::Unavailable(KeySetError::CoolingDown))
        ));
        assert_eq!(source.fetch_count(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn rejected_document_keeps_the_last_accepted_key_set() {
        let source = Arc::new(SequenceSource::new(jwks(&["one"])));
        let verifier = JwksVerifier::new(source.clone(), policy());
        verifier.warm_up().await.unwrap();

        source.push(Ok(b"not-json".to_vec()));
        assert!(matches!(
            verifier.warm_up().await,
            Err(KeySetError::Document(JwksDocumentError::Malformed(_)))
        ));
        assert_eq!(verifier.generation().await, 1);
        verifier.decoding_key(&token("one")).await.unwrap();

        // An unknown key id during an outage is not a verdict on the token.
        assert!(matches!(
            verifier.decoding_key(&token("two")).await,
            Err(KeyError::Unavailable(KeySetError::Document(
                JwksDocumentError::Malformed(_)
            )))
        ));
        assert!(matches!(
            verifier.decoding_key(&token("two")).await,
            Err(KeyError::Unavailable(KeySetError::CoolingDown))
        ));
        verifier.decoding_key(&token("one")).await.unwrap();
    }

    #[tokio::test]
    async fn first_lookup_loads_keys_without_a_warm_up() {
        let source = Arc::new(SequenceSource::new(jwks(&["one"])));
        let verifier = JwksVerifier::new(source.clone(), policy());
        assert_eq!(verifier.generation().await, 0);
        verifier.decoding_key(&token("one")).await.unwrap();
        assert_eq!(source.fetch_count(), 1);
        assert_eq!(verifier.generation().await, 1);
    }

    fn verifier_for(document: &serde_json::Value) -> JwksVerifier<SequenceSource> {
        let source = SequenceSource::new(serde_json::to_vec(document).unwrap());
        JwksVerifier::new(Arc::new(source), policy())
    }

    #[tokio::test]
    async fn common_issuer_document_shapes_warm_up() {
        let with_certificates = serde_json::json!({"keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": "with-certificates",
            "x5t": "nOo3ZDrODXEK1jKWhXslHR_KXEg",
            "x5c": ["MIIDBTCCAe2gAwIBAgIQ+/placeholder+certificate/chain=="],
            "n": RSA_N,
            "e": "AQAB",
        }]});
        let without_alg = serde_json::json!({"keys": [
            {"kty": "RSA", "use": "sig", "kid": "without-alg", "n": RSA_N, "e": "AQAB"},
        ]});
        let with_encryption_key = serde_json::json!({"keys": [
            {"kty": "RSA", "use": "enc", "alg": "RSA-OAEP", "kid": "encryption", "n": RSA_N, "e": "AQAB"},
            {"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "signature", "n": RSA_N, "e": "AQAB"},
        ]});
        for (document, kid) in [
            (with_certificates, "with-certificates"),
            (without_alg, "without-alg"),
            (with_encryption_key, "signature"),
        ] {
            let verifier = verifier_for(&document);
            verifier.warm_up().await.unwrap();
            verifier.decoding_key(&token(kid)).await.unwrap();
        }
    }

    #[tokio::test]
    async fn skipped_keys_verify_nothing_and_a_key_without_alg_still_means_rs256() {
        let verifier = verifier_for(&serde_json::json!({"keys": [
            {"kty": "RSA", "use": "enc", "kid": "encryption", "n": RSA_N, "e": "AQAB"},
            {"kty": "RSA", "use": "sig", "kid": "signature", "n": RSA_N, "e": "AQAB"},
        ]}));
        verifier.warm_up().await.unwrap();

        // A key published for another purpose is not there as far as tokens are concerned.
        assert!(matches!(
            verifier.decoding_key(&token("encryption")).await,
            Err(KeyError::Rejected(TokenRejection::UnknownKey))
        ));
        // The signature key names no algorithm, and a token still cannot pick one.
        for algorithm in ["HS256", "RS384", "RS512", "PS256", "ES256"] {
            assert!(matches!(
                verifier.decoding_key(&token_header(Some("signature"), algorithm)).await,
                Err(KeyError::Rejected(TokenRejection::Algorithm))
            ));
        }
        verifier.decoding_key(&token("signature")).await.unwrap();
    }

    #[tokio::test]
    async fn document_without_a_usable_key_fails_the_warm_up() {
        let verifier = verifier_for(&serde_json::json!({"keys": [
            {"kty": "RSA", "use": "enc", "alg": "RSA-OAEP", "kid": "encryption", "n": RSA_N, "e": "AQAB"},
            {"kty": "EC", "use": "sig", "alg": "ES256", "kid": "curve", "crv": "P-256", "x": "eA", "y": "eQ"},
        ]}));
        assert!(matches!(
            verifier.warm_up().await,
            Err(KeySetError::Document(JwksDocumentError::NoUsableKey))
        ));
        assert_eq!(verifier.generation().await, 0);
    }

    #[tokio::test]
    async fn token_header_must_name_rs256_and_a_key_id_of_the_policy_charset() {
        let source = Arc::new(SequenceSource::new(jwks(&["one"])));
        let verifier = JwksVerifier::new(source.clone(), policy());
        verifier.warm_up().await.unwrap();

        assert!(matches!(
            verifier.decoding_key("not-a-token").await,
            Err(KeyError::Rejected(TokenRejection::MalformedHeader(_)))
        ));
        for algorithm in ["HS256", "RS384", "PS256", "ES256"] {
            assert!(matches!(
                verifier.decoding_key(&token_header(Some("one"), algorithm)).await,
                Err(KeyError::Rejected(TokenRejection::Algorithm))
            ));
        }
        let too_long = "k".repeat(129);
        for kid in [
            None,
            Some(""),
            Some("key one"),
            Some("key/one"),
            Some(too_long.as_str()),
        ] {
            assert!(matches!(
                verifier.decoding_key(&token_header(kid, "RS256")).await,
                Err(KeyError::Rejected(TokenRejection::KeyId))
            ));
        }
        // None of these reached the source.
        assert_eq!(source.fetch_count(), 1);
    }

    #[test]
    fn source_future_is_send_and_verifier_clone_does_not_require_source_clone() {
        fn assert_send<T: Send>(_: T) {}
        fn assert_source_future_is_send<S: JwksSource>(source: &S) {
            assert_send(source.fetch());
        }

        let source = Arc::new(SequenceSource::new(jwks(&["one"])));
        assert_source_future_is_send(source.as_ref());
        let verifier = JwksVerifier::new(source, policy());
        let cloned = verifier.clone();
        assert_send(async move { cloned.decoding_key("token").await.is_ok() });
        assert!(!format!("{verifier:?}").contains("source"));
    }
}
