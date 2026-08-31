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

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::Mutex;

const MAX_ALIAS_IDENTITIES: usize = 16_384;
const MAX_ALIAS_INPUT_BYTES: usize = 1_024;
const MAX_ALIAS_PARTS: usize = 4;
const MAX_COLLISION_ATTEMPTS: u16 = 256;

/// Process-local keyed pseudonyms. Clones share one randomly keyed lifetime.
#[derive(Clone)]
pub(crate) struct IdentifierAliaser {
    keys: Arc<AliasKeys>,
}

struct AliasKeys {
    primary: RandomState,
    secondary: RandomState,
    state: Mutex<AliasState>,
    max_identities: usize,
    max_input_bytes: usize,
    #[cfg(test)]
    forced_collision_attempts: u16,
}

#[derive(Default)]
struct AliasState {
    identities_by_alias: HashMap<String, AliasIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AliasIdentity {
    domain: &'static str,
    parts: Vec<String>,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdentifierAliasError {
    #[error("identifier alias input exceeds the process safety bound")]
    InputBoundExceeded,
    #[error("identifier alias capacity is unavailable")]
    CapacityUnavailable,
}

impl std::fmt::Debug for IdentifierAliaser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("IdentifierAliaser").finish_non_exhaustive()
    }
}

impl Default for IdentifierAliaser {
    fn default() -> Self {
        Self {
            keys: Arc::new(AliasKeys {
                primary: RandomState::new(),
                secondary: RandomState::new(),
                state: Mutex::new(AliasState::default()),
                max_identities: MAX_ALIAS_IDENTITIES,
                max_input_bytes: MAX_ALIAS_INPUT_BYTES,
                #[cfg(test)]
                forced_collision_attempts: 0,
            }),
        }
    }
}

impl IdentifierAliaser {
    pub(crate) fn client_alias(&self, client_id: &str, client_addr: &str) -> Result<String, IdentifierAliasError> {
        self.alias("client", &[client_id, client_addr])
    }

    pub(crate) fn message_alias(&self, message_id: &str) -> Result<String, IdentifierAliasError> {
        self.alias("message", &[message_id])
    }

    pub(crate) fn unique_message_alias(&self, message_id: &str) -> Result<String, IdentifierAliasError> {
        self.alias("unique-message", &[message_id])
    }

    fn alias(&self, domain: &'static str, parts: &[&str]) -> Result<String, IdentifierAliasError> {
        let input_bytes = parts
            .iter()
            .try_fold(0usize, |total, part| total.checked_add(part.len()))
            .ok_or(IdentifierAliasError::InputBoundExceeded)?;
        if parts.is_empty() || parts.len() > MAX_ALIAS_PARTS || input_bytes > self.keys.max_input_bytes {
            return Err(IdentifierAliasError::InputBoundExceeded);
        }
        let identity = AliasIdentity {
            domain,
            parts: parts.iter().map(|part| (*part).to_string()).collect(),
        };
        let mut state = self
            .keys
            .state
            .lock()
            .map_err(|_| IdentifierAliasError::CapacityUnavailable)?;
        for attempt in 0..MAX_COLLISION_ATTEMPTS {
            let candidate = self.candidate(domain, parts, attempt);
            match state.identities_by_alias.get(&candidate) {
                Some(existing) if existing == &identity => return Ok(candidate),
                Some(_) => continue,
                None if state.identities_by_alias.len() >= self.keys.max_identities => {
                    return Err(IdentifierAliasError::CapacityUnavailable);
                }
                None => {
                    state.identities_by_alias.insert(candidate.clone(), identity);
                    return Ok(candidate);
                }
            }
        }
        Err(IdentifierAliasError::CapacityUnavailable)
    }

    fn candidate(&self, domain: &'static str, parts: &[&str], attempt: u16) -> String {
        #[cfg(test)]
        if attempt < self.keys.forced_collision_attempts {
            return format!("{domain}-00000000000000000000000000000000");
        }
        let primary = keyed_hash(&self.keys.primary, domain, parts, attempt);
        let secondary = keyed_hash(&self.keys.secondary, domain, parts, attempt);
        format!("{domain}-{primary:016x}{secondary:016x}")
    }

    #[cfg(test)]
    fn with_test_limits(max_identities: usize, max_input_bytes: usize, forced_collision_attempts: u16) -> Self {
        Self {
            keys: Arc::new(AliasKeys {
                primary: RandomState::new(),
                secondary: RandomState::new(),
                state: Mutex::new(AliasState::default()),
                max_identities,
                max_input_bytes,
                forced_collision_attempts,
            }),
        }
    }
}

fn keyed_hash(state: &RandomState, domain: &str, parts: &[&str], attempt: u16) -> u64 {
    let mut hasher = state.build_hasher();
    domain.hash(&mut hasher);
    attempt.hash(&mut hasher);
    parts.len().hash(&mut hasher);
    for part in parts {
        part.len().hash(&mut hasher);
        part.hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_are_stable_bounded_and_domain_separated() {
        let aliases = IdentifierAliaser::default();
        let clone = aliases.clone();
        let client = aliases.client_alias("raw-client", "10.0.0.1:1234").unwrap();

        assert_eq!(client, clone.client_alias("raw-client", "10.0.0.1:1234").unwrap());
        assert_ne!(client, aliases.client_alias("raw-client", "10.0.0.2:1234").unwrap());
        assert_ne!(client, aliases.message_alias("raw-client").unwrap());
        assert_eq!(client.len(), "client-".len() + 32);
        assert!(!client.contains("raw-client"));
        assert!(!client.contains("10.0.0.1"));
    }

    #[test]
    fn collisions_are_detected_without_rebinding_existing_identities() {
        let aliases = IdentifierAliaser::with_test_limits(8, 128, 1);
        let first = aliases.client_alias("client-a", "addr-a").unwrap();
        let second = aliases.client_alias("client-b", "addr-b").unwrap();

        assert_ne!(first, second);
        assert_eq!(first, aliases.client_alias("client-a", "addr-a").unwrap());
        assert_eq!(second, aliases.client_alias("client-b", "addr-b").unwrap());
        assert_ne!(first, aliases.message_alias("client-a").unwrap());
    }

    #[test]
    fn capacity_and_input_bounds_fail_without_exposing_input() {
        let capacity = IdentifierAliaser::with_test_limits(1, 16, 0);
        capacity.message_alias("first").unwrap();
        assert_eq!(
            capacity.message_alias("second-secret").unwrap_err(),
            IdentifierAliasError::CapacityUnavailable
        );

        let bounded = IdentifierAliaser::with_test_limits(2, 4, 0);
        let error = bounded.message_alias("raw-secret").unwrap_err();
        assert_eq!(error, IdentifierAliasError::InputBoundExceeded);
        assert!(!error.to_string().contains("raw-secret"));
    }
}
