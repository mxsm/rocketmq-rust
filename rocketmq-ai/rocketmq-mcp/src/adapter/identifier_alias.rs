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

use std::sync::Arc;

use hmac::digest::KeyInit;
use hmac::Hmac;
use hmac::Mac;
use sha2::Sha256;

const MAX_ALIAS_INPUT_BYTES: usize = 1_024;
const MAX_ALIAS_PARTS: usize = 4;
/// Keeps these pseudonyms apart from any other use of the same key.
const ALIAS_CONTEXT: &[u8] = b"rocketmq-mcp/identifier-alias/v1";
/// How much of the MAC a pseudonym keeps: 128 bits, written as 32 hexadecimal digits.
const ALIAS_TAG_BYTES: usize = 16;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Keyed pseudonyms for client and message identifiers.
///
/// A pseudonym is a truncated HMAC-SHA256 of the identifier. Nothing is kept per identifier, so
/// the number of distinct identifiers is unbounded, and every process that holds the same key
/// derives the same pseudonym. The default key is random: its pseudonyms are valid for the
/// lifetime of one process. Clones share one key.
#[derive(Clone)]
pub(crate) struct IdentifierAliaser {
    mac: Arc<Hmac<Sha256>>,
}

/// The identifier is longer than a pseudonym may cover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AliasInputBoundExceeded;

impl std::fmt::Display for AliasInputBoundExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("identifier alias input exceeds the process safety bound")
    }
}

impl std::fmt::Debug for IdentifierAliaser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("IdentifierAliaser").finish_non_exhaustive()
    }
}

impl Default for IdentifierAliaser {
    /// Draws a random key that no other process shares.
    fn default() -> Self {
        // A key of exactly one hash block needs no length handling, so this cannot fail.
        let mut key = [0u8; 64];
        rand::fill(&mut key);
        Self {
            mac: Arc::new(Hmac::new(&key.into())),
        }
    }
}

impl IdentifierAliaser {
    /// Uses a key that several replicas share, so that they all derive the same pseudonyms.
    ///
    /// # Errors
    ///
    /// Returns an error when the MAC rejects the key. HMAC accepts every key length, so this
    /// reports a broken invariant instead of panicking.
    pub(crate) fn with_key(key: &[u8]) -> crate::McpResult<Self> {
        let mac = Hmac::new_from_slice(key).map_err(crate::McpError::from_source)?;
        Ok(Self { mac: Arc::new(mac) })
    }

    pub(crate) fn client_alias(&self, client_id: &str, client_addr: &str) -> Result<String, AliasInputBoundExceeded> {
        self.alias("client", &[client_id, client_addr])
    }

    pub(crate) fn message_alias(&self, message_id: &str) -> Result<String, AliasInputBoundExceeded> {
        self.alias("message", &[message_id])
    }

    pub(crate) fn unique_message_alias(&self, message_id: &str) -> Result<String, AliasInputBoundExceeded> {
        self.alias("unique-message", &[message_id])
    }

    fn alias(&self, domain: &'static str, parts: &[&str]) -> Result<String, AliasInputBoundExceeded> {
        let input_bytes = parts
            .iter()
            .try_fold(0usize, |total, part| total.checked_add(part.len()))
            .ok_or(AliasInputBoundExceeded)?;
        if parts.is_empty() || parts.len() > MAX_ALIAS_PARTS || input_bytes > MAX_ALIAS_INPUT_BYTES {
            return Err(AliasInputBoundExceeded);
        }
        // Every field carries its length, so no two inputs share an encoding: neither a
        // different split of the same text nor the same text under another domain.
        let mut mac = Hmac::clone(&self.mac);
        update_framed(&mut mac, ALIAS_CONTEXT);
        update_framed(&mut mac, domain.as_bytes());
        mac.update(&length_prefix(parts.len()));
        for part in parts {
            update_framed(&mut mac, part.as_bytes());
        }
        let tag = mac.finalize().into_bytes();

        let mut alias = String::with_capacity(domain.len() + 1 + ALIAS_TAG_BYTES * 2);
        alias.push_str(domain);
        alias.push('-');
        for byte in &tag[..ALIAS_TAG_BYTES] {
            alias.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
            alias.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
        }
        Ok(alias)
    }
}

fn update_framed(mac: &mut Hmac<Sha256>, field: &[u8]) {
    mac.update(&length_prefix(field.len()));
    mac.update(field);
}

fn length_prefix(length: usize) -> [u8; 8] {
    // `usize` is at most 64 bits on every supported target.
    (length as u64).to_be_bytes()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    const SHARED_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

    #[test]
    fn alias_is_stable_and_domain_separated() {
        let aliases = IdentifierAliaser::default();
        let clone = aliases.clone();
        let client = aliases.client_alias("raw-client", "10.0.0.1:1234").unwrap();

        assert_eq!(client, clone.client_alias("raw-client", "10.0.0.1:1234").unwrap());
        assert_eq!(client, aliases.client_alias("raw-client", "10.0.0.1:1234").unwrap());
        assert_ne!(client, aliases.client_alias("raw-client", "10.0.0.2:1234").unwrap());
        // The same text split differently, or under another domain, is another identifier.
        assert_ne!(
            aliases.client_alias("ab", "c").unwrap(),
            aliases.client_alias("a", "bc").unwrap()
        );
        assert_ne!(
            aliases.message_alias("raw-client").unwrap()["message-".len()..],
            aliases.unique_message_alias("raw-client").unwrap()["unique-message-".len()..]
        );

        let digits = client.strip_prefix("client-").unwrap();
        assert_eq!(digits.len(), 32);
        assert!(digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        assert!(!client.contains("raw-client"));
        assert!(!client.contains("10.0.0.1"));
    }

    #[test]
    fn aliases_never_run_out() {
        let aliases = IdentifierAliaser::default();
        let distinct = (0..20_000)
            .map(|index| aliases.message_alias(&format!("message-{index}")).unwrap())
            .collect::<HashSet<_>>();

        assert_eq!(distinct.len(), 20_000);
    }

    #[test]
    fn shared_key_yields_the_same_alias_across_instances() {
        let first = IdentifierAliaser::with_key(SHARED_KEY).unwrap();
        let second = IdentifierAliaser::with_key(SHARED_KEY).unwrap();
        let other_key = IdentifierAliaser::with_key(b"another-key-another-key-another-key").unwrap();
        let alias = first.client_alias("consumer-1", "10.0.0.7:51234").unwrap();

        assert_eq!(alias, second.client_alias("consumer-1", "10.0.0.7:51234").unwrap());
        assert_ne!(alias, other_key.client_alias("consumer-1", "10.0.0.7:51234").unwrap());
        // Two processes without a configured key do not agree, by design.
        assert_ne!(
            IdentifierAliaser::default().message_alias("message-1").unwrap(),
            IdentifierAliaser::default().message_alias("message-1").unwrap()
        );
    }

    /// Replicas that share a key may run different builds, so the derivation is part of the
    /// contract. The expected values were computed with an independent HMAC-SHA256.
    #[test]
    fn shared_key_derivation_is_pinned() {
        let aliases = IdentifierAliaser::with_key(SHARED_KEY).unwrap();

        assert_eq!(
            aliases.message_alias("7F000001000078BF000000000000022A").unwrap(),
            "message-66decfcbab57428005f1483eed772524"
        );
        assert_eq!(
            aliases
                .unique_message_alias("7F000001000078BF000000000000022A")
                .unwrap(),
            "unique-message-019c942efb02ab17d234660ca7d3c528"
        );
        assert_eq!(
            aliases.client_alias("consumer-1@10.0.0.7", "10.0.0.7:51234").unwrap(),
            "client-ea5e2065d7b53b5f5d0676474a23f51b"
        );
    }

    #[test]
    fn input_bound_fails_without_exposing_input() {
        let aliases = IdentifierAliaser::default();
        let at_the_bound = "x".repeat(MAX_ALIAS_INPUT_BYTES);
        aliases.message_alias(&at_the_bound).unwrap();

        let too_long = format!("raw-secret-{at_the_bound}");
        let error = aliases.message_alias(&too_long).unwrap_err();
        assert_eq!(error, AliasInputBoundExceeded);
        assert!(!format!("{error} {error:?}").contains("raw-secret"));
        // The bound covers all parts together.
        assert_eq!(
            aliases.client_alias(&at_the_bound, "x").unwrap_err(),
            AliasInputBoundExceeded
        );
    }

    #[test]
    fn key_never_appears_in_debug_output() {
        let aliases = IdentifierAliaser::with_key(SHARED_KEY).unwrap();

        assert_eq!(format!("{aliases:?}"), "IdentifierAliaser { .. }");
    }
}
