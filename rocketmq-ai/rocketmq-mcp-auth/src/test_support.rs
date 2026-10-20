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

//! Fixtures shared by the unit tests of this crate.

use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use crate::policy::JwksPolicy;
use crate::policy::KidCharset;
use crate::verifier::JwksSource;

/// Modulus of a 2048-bit RSA public key.
pub(crate) const RSA_N: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

pub(crate) fn policy() -> JwksPolicy {
    JwksPolicy {
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
    }
}

/// A JWKS document of RS256 signature keys given as `(kid, modulus, exponent)`.
pub(crate) fn jwks_document(entries: &[(&str, &str, &str)]) -> Vec<u8> {
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

/// An unsigned token whose header names `algorithm` and, when given, `kid`.
pub(crate) fn token_header(kid: Option<&str>, algorithm: &str) -> String {
    let mut header = serde_json::json!({"alg": algorithm, "typ": "JWT"});
    if let Some(kid) = kid {
        header["kid"] = kid.into();
    }
    format!("{}.e30.invalid", URL_SAFE_NO_PAD.encode(header.to_string()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceDown;

/// Returns the queued answers in order, then repeats the last one.
pub(crate) struct SequenceSource {
    documents: Mutex<VecDeque<Result<Vec<u8>, SourceDown>>>,
    last: Mutex<Result<Vec<u8>, SourceDown>>,
    fetches: AtomicUsize,
}

impl SequenceSource {
    pub(crate) fn new(document: Vec<u8>) -> Self {
        Self {
            documents: Mutex::new(VecDeque::new()),
            last: Mutex::new(Ok(document)),
            fetches: AtomicUsize::new(0),
        }
    }

    pub(crate) fn push(&self, document: Result<Vec<u8>, SourceDown>) {
        self.documents.lock().unwrap().push_back(document);
    }

    pub(crate) fn fetch_count(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }
}

impl JwksSource for SequenceSource {
    type Error = SourceDown;

    async fn fetch(&self) -> Result<Vec<u8>, SourceDown> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        if let Some(document) = self.documents.lock().unwrap().pop_front() {
            *self.last.lock().unwrap() = document.clone();
            document
        } else {
            self.last.lock().unwrap().clone()
        }
    }
}
