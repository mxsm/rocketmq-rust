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

use std::time::Duration;

/// Longest `kid` accepted from a token header or a JWKS entry, in bytes.
const MAX_KID_BYTES: usize = 128;

/// Limits and timings for JWKS documents, cached verification keys, and Bearer tokens.
///
/// Every value is chosen by the server that owns the trust decision, so the crate has no default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JwksPolicy {
    /// Largest JWKS document accepted, in bytes.
    pub max_jwks_bytes: usize,
    /// Largest number of entries a JWKS document may list, whether or not they are usable.
    pub max_jwks_keys: usize,
    /// Time allowed for one HTTP fetch of the JWKS document.
    pub fetch_timeout: Duration,
    /// How long fetched keys are used before a token lookup fetches the document again.
    pub cache_ttl: Duration,
    /// How long after a fetch its keys may still be used while fetching again keeps failing.
    ///
    /// A value that is not above `cache_ttl` means keys are never used past `cache_ttl`.
    pub max_stale: Duration,
    /// Pause before fetching again after a fetch failed or did not hold the requested `kid`.
    pub unknown_kid_cooldown: Duration,
    /// Number of unknown `kid` values remembered during that pause.
    pub max_negative_kids: usize,
    /// Smallest RSA modulus accepted, in bits.
    pub min_rsa_modulus_bits: usize,
    /// Largest RSA modulus accepted, in bits.
    pub max_rsa_modulus_bits: usize,
    /// The only RSA public exponent accepted.
    pub required_rsa_exponent: u64,
    /// Largest Bearer token accepted, in bytes.
    pub max_bearer_token_bytes: usize,
    /// Which characters a `kid` may contain.
    pub kid_charset: KidCharset,
}

/// The characters accepted in a `kid` of 1 to 128 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KidCharset {
    /// ASCII letters, digits, and `.`, `_`, `:`, `-`.
    Token,
    /// Any printable ASCII character except the space.
    Graphic,
}

impl KidCharset {
    pub(crate) fn accepts(self, kid: &str) -> bool {
        !kid.is_empty()
            && kid.len() <= MAX_KID_BYTES
            && kid.bytes().all(|byte| match self {
                Self::Token => byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'),
                Self::Graphic => byte.is_ascii_graphic(),
            })
    }
}

/// Which addresses an [`HttpJwksSource`](crate::HttpJwksSource) may connect to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutboundAddressPolicy {
    /// Connect only to globally routable addresses.
    ///
    /// Names are resolved by the process itself, only for the host of the JWKS URL, and every
    /// answer must be public. Proxies are bypassed, because a proxy would resolve the name instead.
    #[default]
    PublicOnly,
    /// Follow the resolver and proxy settings of the process without checking addresses.
    Unrestricted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kid_charsets_bound_length_and_characters() {
        let longest = "k".repeat(MAX_KID_BYTES);
        let too_long = "k".repeat(MAX_KID_BYTES + 1);
        for charset in [KidCharset::Token, KidCharset::Graphic] {
            assert!(charset.accepts("key-2026.01_a:b"));
            assert!(charset.accepts(&longest));
            assert!(!charset.accepts(""));
            assert!(!charset.accepts(&too_long));
            assert!(!charset.accepts("key one"));
            assert!(!charset.accepts("key\n"));
            assert!(!charset.accepts("kľúč"));
        }
        // Standard base64 key ids need the wider set.
        for kid in ["tU6QcYdtXQ3ClC6f+Nj5lkZSJzW0/EHs=", "key@issuer", "{key}"] {
            assert!(!KidCharset::Token.accepts(kid), "token charset accepted {kid}");
            assert!(KidCharset::Graphic.accepts(kid), "graphic charset rejected {kid}");
        }
    }

    #[test]
    fn outbound_policy_defaults_to_public_addresses() {
        assert_eq!(OutboundAddressPolicy::default(), OutboundAddressPolicy::PublicOnly);
    }
}
