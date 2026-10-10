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

//! The JWKS limits of this server, applied through the verifier shared with the control MCP.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub use rocketmq_mcp_auth::HttpJwksSource;
use rocketmq_mcp_auth::JwksPolicy;
pub use rocketmq_mcp_auth::JwksSource;
pub use rocketmq_mcp_auth::JwksVerifier;
use rocketmq_mcp_auth::KeySetError;
use rocketmq_mcp_auth::KidCharset;
use rocketmq_mcp_auth::OutboundAddressPolicy;

use crate::McpError;
use crate::McpResult;

/// Largest Bearer token accepted, in bytes.
pub(crate) const MAX_BEARER_TOKEN_BYTES: usize = 16 * 1024;

/// The limits of this server around the two configured windows.
///
/// Keys are fetched again once they are older than `refresh_after`. While that keeps failing, the
/// last accepted keys stay in use until they are older than `max_stale`.
pub(crate) fn policy(refresh_after: Duration, max_stale: Duration) -> JwksPolicy {
    JwksPolicy {
        max_jwks_bytes: 256 * 1024,
        max_jwks_keys: 64,
        fetch_timeout: Duration::from_secs(5),
        cache_ttl: refresh_after,
        max_stale,
        unknown_kid_cooldown: Duration::from_secs(5),
        max_negative_kids: 256,
        min_rsa_modulus_bits: 2048,
        max_rsa_modulus_bits: 8192,
        required_rsa_exponent: 65_537,
        max_bearer_token_bytes: MAX_BEARER_TOKEN_BYTES,
        kid_charset: KidCharset::Graphic,
    }
}

/// The HTTPS source of the issuer's JWKS document.
///
/// The issuer may be private, reached through `ca_path` or a proxy, so addresses are not restricted.
pub(crate) fn http_source(
    url: impl Into<Arc<str>>,
    ca_path: Option<&Path>,
    policy: &JwksPolicy,
) -> McpResult<HttpJwksSource> {
    HttpJwksSource::new(url, ca_path, policy, OutboundAddressPolicy::Unrestricted).map_err(McpError::from_source)
}

/// A key set that cannot be loaded is an operational failure, not a verdict on a token.
///
/// The failure of the source itself stays the direct source of the error.
pub(crate) fn unavailable<E>(error: KeySetError<E>) -> McpError
where
    E: std::error::Error + Send + Sync + 'static,
{
    match error {
        KeySetError::Source(source) => McpError::from_source(source),
        other => McpError::from_source(other),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::error::Error;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use tokio::sync::Mutex;

    use super::*;

    const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

    fn test_policy() -> JwksPolicy {
        policy(Duration::from_secs(60), Duration::from_secs(180))
    }

    #[test]
    fn http_source_accepts_only_readable_pem_ca_bundles() {
        let temp_dir = tempfile::tempdir().unwrap();
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["issuer.example.test".to_string()]).unwrap();
        let ca_path = temp_dir.path().join("issuer-ca.pem");
        std::fs::write(&ca_path, cert.pem()).unwrap();

        assert!(http_source("https://issuer.example.test/jwks", Some(&ca_path), &test_policy()).is_ok());

        let invalid_path = temp_dir.path().join("invalid-ca.pem");
        std::fs::write(&invalid_path, b"not a certificate").unwrap();
        assert!(http_source("https://issuer.example.test/jwks", Some(&invalid_path), &test_policy()).is_err());
    }

    #[test]
    fn http_source_may_reach_a_private_issuer() {
        for url in [
            "https://keycloak.identity.svc.cluster.local/realms/rocketmq/protocol/openid-connect/certs",
            "https://localhost:8443/jwks",
            "https://127.0.0.1:8443/jwks",
            "https://10.0.0.9/jwks",
            "https://[fd00::9]/jwks",
        ] {
            assert!(http_source(url, None, &test_policy()).is_ok(), "refused {url}");
        }
    }

    #[tokio::test]
    async fn rotation_publishes_whole_generations_and_failed_refresh_keeps_last_known_good() {
        let source = Arc::new(QueueSource::new([
            Ok(jwks_document(&["one"])),
            Ok(jwks_document(&["two"])),
            Err(std::io::Error::other("unavailable")),
        ]));
        let verifier = JwksVerifier::new(source, test_policy());

        verifier.warm_up().await.unwrap();
        assert_eq!(verifier.generation().await, 1);
        assert!(verifier.decoding_key(&token_header("one", "RS256")).await.is_ok());
        assert!(verifier.decoding_key(&token_header("two", "RS256")).await.is_ok());
        assert_eq!(verifier.generation().await, 2);
        assert!(verifier.decoding_key(&token_header("one", "RS256")).await.is_err());
        assert_eq!(verifier.generation().await, 2);
        assert!(verifier.decoding_key(&token_header("two", "RS256")).await.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn last_known_good_keys_serve_until_max_stale_while_the_issuer_is_down() {
        let source = Arc::new(QueueSource::new([Ok(jwks_document(&["one"]))]));
        let verifier = JwksVerifier::new(source.clone(), test_policy());
        verifier.warm_up().await.unwrap();
        let token = token_header("one", "RS256");

        // Past the refresh window every fetch fails, and the accepted key keeps verifying.
        tokio::time::advance(Duration::from_secs(61)).await;
        for _ in 0..4 {
            verifier.decoding_key(&token).await.unwrap();
        }
        assert_eq!(source.fetches(), 2);
        assert_eq!(verifier.generation().await, 1);

        // Past the last-known-good window the outage is reported, with the cause of the fetch.
        tokio::time::advance(Duration::from_secs(120)).await;
        let Err(rocketmq_mcp_auth::KeyError::Unavailable(error)) = verifier.decoding_key(&token).await else {
            panic!("a key older than the last-known-good window was used");
        };
        let error = unavailable(error);
        assert!(error.source().unwrap().is::<std::io::Error>());
        assert_eq!(error.to_string(), "MCP operation failed");
    }

    #[tokio::test]
    async fn verifier_requires_kid_and_rs256() {
        let source = Arc::new(QueueSource::new([Ok(jwks_document(&["one"]))]));
        let verifier = JwksVerifier::new(source, test_policy());
        verifier.warm_up().await.unwrap();

        assert!(verifier.decoding_key(&token_header("one", "HS256")).await.is_err());
        assert!(verifier.decoding_key("eyJhbGciOiJSUzI1NiJ9.e30.invalid").await.is_err());
    }

    #[tokio::test]
    async fn key_ids_may_use_any_printable_ascii_character() {
        // Some issuers publish standard base64 key ids.
        let kid = "tU6QcYdtXQ3ClC6f+Nj5lkZSJzW0/EHsFPq3M9QWZjCjg=";
        let source = Arc::new(QueueSource::new([Ok(jwks_document(&[kid]))]));
        let verifier = JwksVerifier::new(source, test_policy());
        verifier.warm_up().await.unwrap();
        assert!(verifier.decoding_key(&token_header(kid, "RS256")).await.is_ok());
    }

    #[tokio::test]
    async fn key_below_the_rsa_strength_floor_is_not_used() {
        use base64::Engine;
        let weak = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0x81_u8; 128]);
        let document = serde_json::to_vec(&serde_json::json!({"keys": [
            {"kty": "RSA", "kid": "weak", "alg": "RS256", "use": "sig", "n": weak, "e": "AQAB"},
            {"kty": "RSA", "kid": "strong", "alg": "RS256", "use": "sig", "n": RSA_N, "e": "AQAB"},
        ]}))
        .unwrap();
        let verifier = JwksVerifier::new(Arc::new(QueueSource::new([Ok(document)])), test_policy());
        verifier.warm_up().await.unwrap();
        assert!(verifier.decoding_key(&token_header("strong", "RS256")).await.is_ok());
        assert!(verifier.decoding_key(&token_header("weak", "RS256")).await.is_err());
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

    /// Returns the queued answers in order, then fails.
    struct QueueSource {
        responses: Mutex<VecDeque<std::io::Result<Vec<u8>>>>,
        fetches: AtomicUsize,
    }

    impl QueueSource {
        fn new(responses: impl IntoIterator<Item = std::io::Result<Vec<u8>>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                fetches: AtomicUsize::new(0),
            }
        }

        fn fetches(&self) -> usize {
            self.fetches.load(Ordering::SeqCst)
        }
    }

    impl JwksSource for QueueSource {
        type Error = std::io::Error;

        async fn fetch(&self) -> std::io::Result<Vec<u8>> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.responses
                .lock()
                .await
                .pop_front()
                .unwrap_or_else(|| Err(std::io::Error::other("unavailable")))
        }
    }
}
