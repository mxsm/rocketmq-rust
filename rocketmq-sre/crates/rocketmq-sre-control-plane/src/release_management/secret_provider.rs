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

use hmac::Hmac;
use hmac::Mac;
use sha2::Sha256;
use subtle::ConstantTimeEq;

/// Resolved secret material deliberately omits `Debug`, `Display`, and
/// serialization implementations.
pub(super) struct ResolvedSecret(String);

impl ResolvedSecret {
    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Secret reference boundary shared by outbound and inbound integrations.
pub(super) trait SecretProvider: Send + Sync {
    fn resolve(&self, reference: &str) -> Result<ResolvedSecret, &'static str>;

    fn available(&self, reference: &str) -> bool {
        self.resolve(reference).is_ok()
    }
}

/// Production provider for environment-backed secret references. Vault and
/// KMS providers can implement the same narrow interface without changing an
/// adapter or storing plaintext in PostgreSQL.
#[derive(Clone, Copy, Default)]
pub(super) struct EnvSecretProvider;

impl SecretProvider for EnvSecretProvider {
    fn resolve(&self, reference: &str) -> Result<ResolvedSecret, &'static str> {
        let name = environment_name(reference)?;
        std::env::var(name)
            .ok()
            .filter(|secret| !secret.is_empty() && secret.len() <= 4_096)
            .map(ResolvedSecret)
            .ok_or("secret_unavailable")
    }
}

pub(super) fn hmac_sha256(key: &ResolvedSecret, message: &[u8]) -> Result<String, &'static str> {
    hmac_sha256_bytes(key.as_bytes(), message)
}

pub(super) fn hmac_sha256_bytes(key: &[u8], message: &[u8]) -> Result<String, &'static str> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| "invalid_signature_key")?;
    mac.update(message);
    Ok(hex_lower(&mac.finalize().into_bytes()))
}

pub(super) fn signature_matches(expected_hex: &str, supplied: &str) -> bool {
    let supplied = supplied.strip_prefix("sha256=").unwrap_or(supplied);
    if supplied.len() != expected_hex.len() {
        return false;
    }
    expected_hex.as_bytes().ct_eq(supplied.as_bytes()).into()
}

pub(super) fn valid_secret_reference(reference: &str) -> bool {
    environment_name(reference).is_ok()
}

fn environment_name(reference: &str) -> Result<&str, &'static str> {
    let name = reference.strip_prefix("env:").ok_or("unsupported_secret_reference")?;
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err("invalid_secret_reference");
    }
    Ok(name)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::BTreeMap;

    use super::*;

    pub(super) struct StaticSecretProvider {
        secrets: BTreeMap<String, String>,
    }

    impl StaticSecretProvider {
        pub(super) fn one(reference: &str, secret: &str) -> Self {
            Self {
                secrets: BTreeMap::from([(reference.to_owned(), secret.to_owned())]),
            }
        }
    }

    impl SecretProvider for StaticSecretProvider {
        fn resolve(&self, reference: &str) -> Result<ResolvedSecret, &'static str> {
            self.secrets
                .get(reference)
                .filter(|secret| !secret.is_empty())
                .cloned()
                .map(ResolvedSecret)
                .ok_or("secret_unavailable")
        }
    }

    #[test]
    fn provider_and_signature_boundary_fail_closed() {
        let provider = StaticSecretProvider::one("env:TEST_SECRET", "bounded-secret");
        let secret = provider.resolve("env:TEST_SECRET").expect("secret fixture");
        let signature = hmac_sha256(&secret, b"message").expect("signature");
        assert!(signature_matches(&signature, &format!("sha256={signature}")));
        assert!(!signature_matches(&signature, "sha256:invalid"));
        assert!(!provider.available("env:MISSING"));
        assert!(valid_secret_reference("env:TEST_SECRET"));
        assert!(!valid_secret_reference("literal:secret"));
    }
}
