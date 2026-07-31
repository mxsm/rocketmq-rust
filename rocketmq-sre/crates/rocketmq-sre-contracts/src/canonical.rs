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

use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use crate::ContractError;

/// Computes an RFC 8785 canonical JSON SHA-256 digest.
///
/// # Errors
///
/// Returns a typed contract error when the value cannot be canonicalized.
pub fn canonical_sha256<T>(value: &T) -> Result<String, ContractError>
where
    T: Serialize + ?Sized,
{
    let canonical = serde_jcs::to_vec(value).map_err(|error| ContractError::InvalidDescriptor {
        reason: format!("value cannot be canonicalized: {error}"),
    })?;
    let digest = Sha256::digest(canonical);
    Ok(format!("sha256:{digest:x}"))
}

/// Computes the evidence-set digest used by an action plan.
///
/// # Errors
///
/// Returns a typed contract error when the value cannot be canonicalized.
pub fn canonical_evidence_hash<T>(value: &T) -> Result<String, ContractError>
where
    T: Serialize + ?Sized,
{
    canonical_sha256(value)
}

/// Computes the live-state precondition digest used by a plan step.
///
/// # Errors
///
/// Returns a typed contract error when the value cannot be canonicalized.
pub fn canonical_precondition_hash<T>(value: &T) -> Result<String, ContractError>
where
    T: Serialize + ?Sized,
{
    canonical_sha256(value)
}

/// Returns whether a value is a lowercase or uppercase SHA-256 digest with the
/// required `sha256:` prefix.
#[must_use]
pub fn is_sha256_digest(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn canonical_hash_is_stable_across_object_field_order() {
        let first = json!({"broker": "broker-a", "generation": 7});
        let second = json!({"generation": 7, "broker": "broker-a"});

        assert_eq!(
            canonical_precondition_hash(&first).expect("first value should hash"),
            canonical_precondition_hash(&second).expect("second value should hash")
        );
    }

    #[test]
    fn digest_validation_rejects_placeholders_and_wrong_lengths() {
        assert!(is_sha256_digest(&format!("sha256:{}", "a".repeat(64))));
        assert!(!is_sha256_digest("sha256:evidence"));
        assert!(!is_sha256_digest(&format!("sha256:{}", "a".repeat(63))));
        assert!(!is_sha256_digest(&format!("sha512:{}", "a".repeat(64))));
    }
}
