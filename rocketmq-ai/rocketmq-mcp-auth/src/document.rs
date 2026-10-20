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

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use jsonwebtoken::DecodingKey;
use serde::Deserialize;

use crate::policy::JwksPolicy;

/// Why a JWKS document was not accepted.
///
/// `Display` never repeats document content. The JSON error kept as a source may quote it.
#[derive(Debug, thiserror::Error)]
pub enum JwksDocumentError {
    /// The document is larger than [`JwksPolicy::max_jwks_bytes`].
    #[error("JWKS document exceeds the size limit")]
    TooLarge,
    /// The document is not a JSON object with a `keys` array.
    #[error("JWKS document is malformed")]
    Malformed(#[source] serde_json::Error),
    /// The document lists no entry, or more entries than [`JwksPolicy::max_jwks_keys`].
    #[error("JWKS document lists no key or too many keys")]
    KeyCount,
    /// Two usable entries carry the same `kid`, so a token naming it has no single key.
    #[error("JWKS document repeats a key id")]
    DuplicateKeyId,
    /// No entry can verify RS256 tokens under the policy.
    #[error("JWKS document holds no key that can verify RS256 tokens")]
    NoUsableKey,
}

#[derive(Deserialize)]
struct RawJwks {
    /// Kept as loose JSON so that one entry of an unexpected shape does not hide the others.
    keys: Vec<serde_json::Value>,
}

/// The members of a JWK that decide whether it can verify RS256 tokens. Others are ignored.
#[derive(Deserialize)]
struct RawJwk {
    kty: Option<String>,
    kid: Option<String>,
    alg: Option<String>,
    #[serde(rename = "use")]
    public_key_use: Option<String>,
    key_ops: Option<Vec<String>>,
    n: Option<String>,
    e: Option<String>,
}

impl RawJwk {
    /// The key id and verification key, when this entry may verify RS256 tokens under `policy`.
    fn rs256_key(self, policy: &JwksPolicy) -> Option<(String, DecodingKey)> {
        let (kid, modulus, exponent) = (self.kid?, self.n?, self.e?);
        if self.kty.as_deref() != Some("RSA")
            || self.alg.as_deref().is_some_and(|value| value != "RS256")
            || !policy.kid_charset.accepts(&kid)
            || self.public_key_use.as_deref().is_some_and(|value| value != "sig")
            || self
                .key_ops
                .as_ref()
                .is_some_and(|operations| !operations.iter().any(|operation| operation == "verify"))
            || !valid_rsa_parameters(&modulus, &exponent, policy)
        {
            return None;
        }
        let key = DecodingKey::from_rsa_components(&modulus, &exponent).ok()?;
        Some((kid, key))
    }
}

/// Reads the RS256 verification keys of a JWKS document, by `kid`.
///
/// An entry is used when its `kty` is `RSA`, its `use` is absent or `sig`, its `key_ops` is absent
/// or lists `verify`, its `alg` is absent or `RS256`, its `kid` fits the policy's character set,
/// and its modulus and exponent fit the policy. Every other entry is skipped, as are members this
/// crate does not read, such as `x5c`.
///
/// # Errors
///
/// Returns a [`JwksDocumentError`] when the document breaks a size or count limit of `policy`, is
/// not a JSON object with a `keys` array, repeats a `kid` among its usable entries, or has no usable
/// entry at all.
pub fn parse_jwks(bytes: &[u8], policy: &JwksPolicy) -> Result<BTreeMap<String, Arc<DecodingKey>>, JwksDocumentError> {
    if bytes.len() > policy.max_jwks_bytes {
        return Err(JwksDocumentError::TooLarge);
    }
    // Going through a JSON object keeps serde from also reading the document, or an entry, from an
    // array of positional values.
    let document: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(bytes).map_err(JwksDocumentError::Malformed)?;
    let document = RawJwks::deserialize(serde_json::Value::Object(document)).map_err(JwksDocumentError::Malformed)?;
    if document.keys.is_empty() || document.keys.len() > policy.max_jwks_keys {
        return Err(JwksDocumentError::KeyCount);
    }
    let listed = document.keys.len();
    let mut keys = BTreeMap::new();
    for entry in document.keys {
        let Some((kid, key)) = entry
            .is_object()
            .then(|| RawJwk::deserialize(entry).ok())
            .flatten()
            .and_then(|jwk| jwk.rs256_key(policy))
        else {
            continue;
        };
        if keys.insert(kid, Arc::new(key)).is_some() {
            return Err(JwksDocumentError::DuplicateKeyId);
        }
    }
    if keys.is_empty() {
        return Err(JwksDocumentError::NoUsableKey);
    }
    if keys.len() < listed {
        tracing::debug!(
            listed,
            usable = keys.len(),
            "JWKS entries that cannot verify RS256 tokens were skipped"
        );
    }
    Ok(keys)
}

fn valid_rsa_parameters(modulus: &str, exponent: &str, policy: &JwksPolicy) -> bool {
    let Ok(modulus) = URL_SAFE_NO_PAD.decode(modulus) else {
        return false;
    };
    if modulus.first().is_none_or(|byte| *byte == 0) || modulus.last().is_none_or(|byte| byte & 1 == 0) {
        return false;
    }
    let leading = modulus[0].leading_zeros() as usize;
    let bits = modulus.len().saturating_mul(8).saturating_sub(leading);
    if !(policy.min_rsa_modulus_bits..=policy.max_rsa_modulus_bits).contains(&bits) {
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
    value == policy.required_rsa_exponent
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::test_support::jwks_document;
    use crate::test_support::policy;
    use crate::test_support::RSA_N;

    fn kids(document: &serde_json::Value) -> Result<Vec<String>, JwksDocumentError> {
        parse_jwks(&serde_json::to_vec(document).unwrap(), &policy()).map(|keys| keys.into_keys().collect())
    }

    #[test]
    fn weak_oversized_and_unsafe_exponent_jwks_are_rejected() {
        let weak = vec![0x81; 128];
        let oversized = vec![0x81; 1025];
        let cases = [
            jwks_document(&[("weak", &URL_SAFE_NO_PAD.encode(weak), "AQAB")]),
            jwks_document(&[("oversized", &URL_SAFE_NO_PAD.encode(oversized), "AQAB")]),
            jwks_document(&[("exponent", RSA_N, &URL_SAFE_NO_PAD.encode([3_u8]))]),
            jwks_document(&[("even", &URL_SAFE_NO_PAD.encode(vec![0x80; 256]), "AQAB")]),
            jwks_document(&[(
                "padded",
                &URL_SAFE_NO_PAD.encode([&[0_u8][..], &[0x81; 256][..]].concat()),
                "AQAB",
            )]),
            jwks_document(&[("not-base64url", "++//", "AQAB")]),
        ];
        for document in cases {
            assert!(matches!(
                parse_jwks(&document, &policy()),
                Err(JwksDocumentError::NoUsableKey)
            ));
        }
    }

    #[test]
    fn rsa_bounds_follow_the_policy() {
        let document = jwks_document(&[("one", RSA_N, "AQAB")]);
        let mut stricter = policy();
        assert_eq!(parse_jwks(&document, &stricter).unwrap().len(), 1);
        stricter.min_rsa_modulus_bits = 3072;
        assert!(matches!(
            parse_jwks(&document, &stricter),
            Err(JwksDocumentError::NoUsableKey)
        ));
        let mut other_exponent = policy();
        other_exponent.required_rsa_exponent = 3;
        assert!(matches!(
            parse_jwks(&document, &other_exponent),
            Err(JwksDocumentError::NoUsableKey)
        ));
    }

    #[test]
    fn symmetric_keys_and_repeated_key_ids_are_rejected() {
        let symmetric = br#"{"keys":[{"kty":"oct","kid":"one","alg":"HS256","k":"c2VjcmV0"}]}"#;
        assert!(matches!(
            parse_jwks(symmetric, &policy()),
            Err(JwksDocumentError::NoUsableKey)
        ));

        let repeated = jwks_document(&[("one", RSA_N, "AQAB"), ("one", RSA_N, "AQAB")]);
        assert!(matches!(
            parse_jwks(&repeated, &policy()),
            Err(JwksDocumentError::DuplicateKeyId)
        ));
    }

    #[test]
    fn document_size_and_key_count_are_bounded() {
        let mut small = policy();
        small.max_jwks_keys = 2;
        let three = jwks_document(&[("one", RSA_N, "AQAB"), ("two", RSA_N, "AQAB"), ("three", RSA_N, "AQAB")]);
        assert!(matches!(parse_jwks(&three, &small), Err(JwksDocumentError::KeyCount)));
        // Entries count against the limit whether or not they are usable.
        let padded =
            json!({"keys": [{"kty": "EC"}, {"kty": "EC"}, {"kty": "RSA", "kid": "one", "n": RSA_N, "e": "AQAB"}]});
        assert!(matches!(
            parse_jwks(&serde_json::to_vec(&padded).unwrap(), &small),
            Err(JwksDocumentError::KeyCount)
        ));
        assert!(matches!(
            parse_jwks(br#"{"keys":[]}"#, &small),
            Err(JwksDocumentError::KeyCount)
        ));

        let one = jwks_document(&[("one", RSA_N, "AQAB")]);
        small.max_jwks_bytes = one.len() - 1;
        assert!(matches!(parse_jwks(&one, &small), Err(JwksDocumentError::TooLarge)));
        small.max_jwks_bytes = one.len();
        assert_eq!(parse_jwks(&one, &small).unwrap().len(), 1);

        let positional = format!(r#"[[{{"kty":"RSA","kid":"one","n":"{RSA_N}","e":"AQAB"}}]]"#);
        for malformed in [
            &b"not-json"[..],
            b"[]",
            positional.as_bytes(),
            br#"{"keys":{}}"#,
            br#"{"key":[]}"#,
        ] {
            assert!(matches!(
                parse_jwks(malformed, &policy()),
                Err(JwksDocumentError::Malformed(_))
            ));
        }
    }

    #[test]
    fn certificate_members_and_other_unknown_members_are_ignored() {
        // Shape published by issuers that attach the signing certificate to each key.
        let document = json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": "nOo3ZDrODXEK1jKWhXslHR_KXEg",
                "x5t": "nOo3ZDrODXEK1jKWhXslHR_KXEg",
                "x5t#S256": "Fqd0kBYtDHFqBZ1QKPSI4wYbyh8bMsk5TGmQHcVYzuE",
                "x5c": ["MIIDBTCCAe2gAwIBAgIQ+/placeholder+certificate/chain=="],
                "issuer": "https://issuer.example.test/v2.0",
                "n": RSA_N,
                "e": "AQAB",
            }],
            "next_update": "2026-10-10T00:00:00Z",
        });
        assert_eq!(kids(&document).unwrap(), ["nOo3ZDrODXEK1jKWhXslHR_KXEg"]);
    }

    #[test]
    fn key_without_alg_is_usable() {
        let document = json!({"keys": [{"kty": "RSA", "use": "sig", "kid": "no-alg", "n": RSA_N, "e": "AQAB"}]});
        assert_eq!(kids(&document).unwrap(), ["no-alg"]);
        // The bare minimum: only the key type, the id, and the RSA parameters.
        let document = json!({"keys": [{"kty": "RSA", "kid": "bare", "n": RSA_N, "e": "AQAB"}]});
        assert_eq!(kids(&document).unwrap(), ["bare"]);
    }

    #[test]
    fn keys_for_other_purposes_are_skipped_beside_a_signature_key() {
        let signature = json!({"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "sig-1", "n": RSA_N, "e": "AQAB"});
        let skipped = [
            json!({"kty": "RSA", "use": "enc", "alg": "RSA-OAEP", "kid": "enc-1", "n": RSA_N, "e": "AQAB"}),
            json!({"kty": "RSA", "use": "enc", "kid": "enc-2", "n": RSA_N, "e": "AQAB"}),
            json!({"kty": "RSA", "use": "sig", "alg": "RS384", "kid": "rs384", "n": RSA_N, "e": "AQAB"}),
            json!({"kty": "RSA", "use": "sig", "alg": "PS256", "kid": "ps256", "n": RSA_N, "e": "AQAB"}),
            json!({"kty": "RSA", "key_ops": ["sign"], "kid": "sign-only", "n": RSA_N, "e": "AQAB"}),
            json!({"kty": "EC", "use": "sig", "alg": "ES256", "kid": "ec-1", "crv": "P-256", "x": "eA", "y": "eQ"}),
            json!({"kty": "oct", "alg": "HS256", "kid": "secret", "k": "c2VjcmV0"}),
            json!({"kty": "RSA", "use": "sig", "n": RSA_N, "e": "AQAB"}),
            json!({"kty": "RSA", "use": "sig", "kid": "key id with spaces", "n": RSA_N, "e": "AQAB"}),
            json!({"kty": "RSA", "use": "sig", "kid": "no-parameters"}),
            json!({"kty": "RSA", "use": "sig", "kid": 7, "n": RSA_N, "e": "AQAB"}),
            json!({"use": "sig", "kid": "no-kty", "n": RSA_N, "e": "AQAB"}),
            json!(["RSA", "positional", "RS256", "sig", ["verify"], RSA_N, "AQAB"]),
            json!("not an object"),
            json!(null),
        ];
        for entry in &skipped {
            let document = json!({"keys": [entry, signature]});
            assert_eq!(kids(&document).unwrap(), ["sig-1"], "used {entry}");
            // On its own the entry leaves nothing to verify with.
            assert!(
                matches!(kids(&json!({"keys": [entry]})), Err(JwksDocumentError::NoUsableKey)),
                "used {entry}"
            );
        }
        let mut all = skipped.to_vec();
        all.push(signature);
        all.push(json!({"kty": "RSA", "key_ops": ["sign", "verify"], "kid": "sig-2", "n": RSA_N, "e": "AQAB"}));
        assert_eq!(kids(&json!({"keys": all})).unwrap(), ["sig-1", "sig-2"]);
    }

    #[test]
    fn a_skipped_entry_may_share_its_key_id_with_a_usable_one() {
        let document = json!({"keys": [
            {"kty": "EC", "use": "sig", "alg": "ES256", "kid": "shared", "crv": "P-256", "x": "eA", "y": "eQ"},
            {"kty": "RSA", "use": "enc", "kid": "shared", "n": RSA_N, "e": "AQAB"},
            {"kty": "RSA", "use": "sig", "kid": "shared", "n": RSA_N, "e": "AQAB"},
        ]});
        assert_eq!(kids(&document).unwrap(), ["shared"]);
    }
}
