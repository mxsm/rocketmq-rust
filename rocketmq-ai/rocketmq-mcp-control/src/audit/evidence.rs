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

//! What a version-3 audit record says about the object of a mutation and about its outcome.
//!
//! Everything here is a validated logical name, a SHA-256 digest or a closed counter. A record
//! therefore answers who changed which object to what without holding the requested values, a
//! request key, an address or a backend error.

use std::fmt::Write;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::error::ControlError;
use crate::model::ControlOperation;
use crate::tools::validate_user_name;
use crate::tools::NameKind;

/// Largest serialized Broker name list that a record carries inline.
///
/// A longer list is recorded by its count and digest only, which keeps a record for 64 Brokers
/// inside the smallest record bound that the configuration accepts.
pub const MAX_INLINE_BROKER_NAMES_BYTES: usize = 1_024;
const MAX_TARGET_BROKERS: usize = 64;
const DIGEST_HEX_LENGTH: usize = 64;

/// The logical object of one mutation, shaped like the `target` of the Tool response.
#[derive(Clone, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditTarget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    /// The one Broker of a Broker configuration patch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker: Option<String>,
    /// The Brokers selected by a Topic or Consumer Group upsert.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brokers: Option<AuditBrokerSet>,
}

impl std::fmt::Debug for AuditTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Logical names belong to the durable record only, not to logs.
        formatter
            .debug_struct("AuditTarget")
            .field("topic_recorded", &self.topic.is_some())
            .field("consumer_group_recorded", &self.consumer_group.is_some())
            .field("broker_recorded", &self.broker.is_some())
            .field("broker_count", &self.brokers.as_ref().map(|brokers| brokers.count))
            .finish()
    }
}

/// The Brokers a mutation selected: always their number and digest, and their sorted names when
/// the list is short enough to carry inline.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditBrokerSet {
    pub count: u32,
    /// SHA-256 of the sorted names as a JSON array without whitespace.
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,
}

impl AuditBrokerSet {
    /// Summarizes the selected Broker names. The caller has validated them.
    pub fn from_names(names: &[String]) -> Self {
        let mut sorted = names.to_vec();
        sorted.sort_unstable();
        let encoded = encode_names(&sorted);
        Self {
            count: u32::try_from(sorted.len()).unwrap_or(u32::MAX),
            digest: sha256_hex(&encoded),
            names: (encoded.len() <= MAX_INLINE_BROKER_NAMES_BYTES).then_some(sorted),
        }
    }

    fn is_valid(&self) -> bool {
        let count = usize::try_from(self.count).unwrap_or(usize::MAX);
        if !(1..=MAX_TARGET_BROKERS).contains(&count) || !is_digest(&self.digest) {
            return false;
        }
        let Some(names) = &self.names else {
            return true;
        };
        let encoded = encode_names(names);
        names.len() == count
            && names.windows(2).all(|pair| pair[0] < pair[1])
            && names
                .iter()
                .all(|name| validate_user_name(name, NameKind::Broker).is_ok())
            && encoded.len() <= MAX_INLINE_BROKER_NAMES_BYTES
            && sha256_hex(&encoded) == self.digest
    }
}

fn encode_names(names: &[String]) -> Vec<u8> {
    let names = Value::Array(names.iter().cloned().map(Value::String).collect());
    let mut encoded = Vec::new();
    write_canonical(&names, &mut encoded);
    encoded
}

/// What a call asks for. It is known before any RPC, written to the `started` record and
/// repeated unchanged on the terminal record.
#[derive(Clone, PartialEq, Eq)]
pub struct AuditSubject {
    operation: ControlOperation,
    pub(super) target: AuditTarget,
    pub(super) requested_digest: String,
    pub(super) request_key_digest: Option<String>,
}

impl AuditSubject {
    /// Binds the object and digests of one call to its operation.
    ///
    /// # Errors
    ///
    /// Returns `audit_unavailable` when the target does not have the exact shape of the
    /// operation, holds a name that argument validation rejects, or a digest is malformed.
    pub fn try_new(
        operation: ControlOperation,
        target: AuditTarget,
        requested_digest: String,
        request_key_digest: Option<String>,
    ) -> Result<Self, ControlError> {
        let subject = Self {
            operation,
            target,
            requested_digest,
            request_key_digest,
        };
        if validate_subject(
            operation,
            &subject.target,
            &subject.requested_digest,
            subject.request_key_digest.as_deref(),
        ) {
            Ok(subject)
        } else {
            Err(ControlError::audit_unavailable())
        }
    }

    pub(super) const fn operation(&self) -> ControlOperation {
        self.operation
    }
}

#[cfg(test)]
impl AuditSubject {
    /// A well-formed subject for tests that do not look at the object of the mutation.
    pub(crate) fn sample(operation: ControlOperation) -> Self {
        let name = |name: &str| Some(name.to_owned());
        let brokers = || Some(AuditBrokerSet::from_names(&["broker-a".to_owned()]));
        let target = match operation {
            ControlOperation::TopicUpsert => AuditTarget {
                topic: name("orders"),
                brokers: brokers(),
                ..AuditTarget::default()
            },
            ControlOperation::ConsumerGroupUpsert => AuditTarget {
                consumer_group: name("orders_consumers"),
                brokers: brokers(),
                ..AuditTarget::default()
            },
            ControlOperation::ConsumerOffsetReset | ControlOperation::ConsumerRequestMode => AuditTarget {
                topic: name("orders"),
                consumer_group: name("orders_consumers"),
                ..AuditTarget::default()
            },
            ControlOperation::BrokerConfigPatch => AuditTarget {
                broker: name("broker-a"),
                ..AuditTarget::default()
            },
        };
        match Self::try_new(operation, target, sha256_hex(b"requested state"), None) {
            Ok(subject) => subject,
            Err(_) => unreachable!("the sample subject is well formed"),
        }
    }
}

/// Checks the object and digests that a version-3 record carries for `operation`.
pub(super) fn validate_subject(
    operation: ControlOperation,
    target: &AuditTarget,
    requested_digest: &str,
    request_key_digest: Option<&str>,
) -> bool {
    let name = |value: &Option<String>, kind| {
        value
            .as_deref()
            .is_some_and(|value| validate_user_name(value, kind).is_ok())
    };
    let brokers = target.brokers.as_ref().is_some_and(AuditBrokerSet::is_valid);
    let shape = match operation {
        ControlOperation::TopicUpsert => {
            name(&target.topic, NameKind::Topic)
                && brokers
                && target.consumer_group.is_none()
                && target.broker.is_none()
        }
        ControlOperation::ConsumerGroupUpsert => {
            name(&target.consumer_group, NameKind::ConsumerGroup)
                && brokers
                && target.topic.is_none()
                && target.broker.is_none()
        }
        ControlOperation::ConsumerOffsetReset | ControlOperation::ConsumerRequestMode => {
            name(&target.topic, NameKind::Topic)
                && name(&target.consumer_group, NameKind::ConsumerGroup)
                && target.broker.is_none()
                && target.brokers.is_none()
        }
        ControlOperation::BrokerConfigPatch => {
            name(&target.broker, NameKind::Broker)
                && target.topic.is_none()
                && target.consumer_group.is_none()
                && target.brokers.is_none()
        }
    };
    shape && is_digest(requested_digest) && request_key_digest.is_none_or(is_digest)
}

/// What a finished attempt did, as far as its result tells.
///
/// Every member is absent when the attempt ended without a result, for example after a timeout:
/// the record then does not claim to know whether a write happened.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditOutcome {
    /// Digest of the state that preflight read before the change.
    pub before_digest: Option<String>,
    /// Whether at least one target was written.
    pub changed: Option<bool>,
    pub target_results: Option<AuditTargetResults>,
}

impl AuditOutcome {
    pub(super) fn is_valid(&self) -> bool {
        self.before_digest.as_deref().is_none_or(is_digest)
    }
}

/// How many targets of one attempt ended in each state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuditTargetResults {
    /// Written and changed.
    pub applied: u32,
    /// Left as they were: already in the requested state, or only planned by a dry run.
    pub unchanged: u32,
    /// Refused because the state sealed by preflight had changed.
    pub conflict: u32,
    /// Failed for any other reason, including an unverified write.
    pub failed: u32,
}

/// Returns the lowercase hexadecimal SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(Sha256::digest(bytes).as_slice())
}

pub(super) fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a `String` cannot fail.
        let _ = write!(text, "{byte:02x}");
    }
    text
}

/// Returns the SHA-256 of `value` in canonical form: object keys sorted, no whitespace.
///
/// The canonical form does not depend on the order in which a value was built, so an auditor can
/// recompute a digest from the same data.
pub fn digest_canonical_json(value: &Value) -> String {
    let mut encoded = Vec::new();
    write_canonical(value, &mut encoded);
    sha256_hex(&encoded)
}

fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(members) => {
            let mut keys = members.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            out.push(b'{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_scalar(&Value::String(key.clone()), out);
                out.push(b':');
                write_canonical(&members[key.as_str()], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        scalar => write_scalar(scalar, out),
    }
}

fn write_scalar(scalar: &Value, out: &mut Vec<u8>) {
    // Encoding a scalar into memory cannot fail.
    if let Ok(encoded) = serde_json::to_vec(scalar) {
        out.extend_from_slice(&encoded);
    }
}

pub(super) fn is_digest(value: &str) -> bool {
    value.len() == DIGEST_HEX_LENGTH
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(seed: &str) -> String {
        sha256_hex(seed.as_bytes())
    }

    #[test]
    fn digests_are_stable_and_ignore_key_order() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let forward: Value =
            serde_json::from_str(r#"{"topic":"orders","perm":6,"nested":{"b":[1,"x"],"a":null}}"#).unwrap();
        let backward: Value =
            serde_json::from_str(r#"{"nested":{"a":null,"b":[1,"x"]},"perm":6,"topic":"orders"}"#).unwrap();
        assert_eq!(digest_canonical_json(&forward), digest_canonical_json(&backward));
        let mut canonical = Vec::new();
        write_canonical(&forward, &mut canonical);
        assert_eq!(
            String::from_utf8(canonical).unwrap(),
            r#"{"nested":{"a":null,"b":[1,"x"]},"perm":6,"topic":"orders"}"#
        );
        let changed: Value =
            serde_json::from_str(r#"{"topic":"orders","perm":4,"nested":{"b":[1,"x"],"a":null}}"#).unwrap();
        assert_ne!(digest_canonical_json(&forward), digest_canonical_json(&changed));
        assert!(is_digest(&digest("x")));
        for malformed in ["", "abc", &digest("x").to_uppercase(), &format!("{}0", digest("x"))] {
            assert!(!is_digest(malformed));
        }
    }

    #[test]
    fn broker_sets_carry_short_lists_inline_and_long_lists_by_digest() {
        let short = AuditBrokerSet::from_names(&["broker-b".to_owned(), "broker-a".to_owned()]);
        assert_eq!(short.count, 2);
        assert_eq!(
            short.names.as_deref(),
            Some(&["broker-a".to_owned(), "broker-b".to_owned()][..])
        );
        assert_eq!(short.digest, sha256_hex(br#"["broker-a","broker-b"]"#));
        assert!(short.is_valid());

        let long_names = (0..64)
            .map(|index| format!("{index:02}{}", "b".repeat(125)))
            .collect::<Vec<_>>();
        let long = AuditBrokerSet::from_names(&long_names);
        assert_eq!(long.count, 64);
        assert_eq!(long.names, None);
        assert!(long.is_valid());

        let mut tampered = short.clone();
        tampered.names = Some(vec!["broker-a".to_owned(), "broker-c".to_owned()]);
        assert!(!tampered.is_valid());
        let mut unsorted = short.clone();
        unsorted.names = Some(vec!["broker-b".to_owned(), "broker-a".to_owned()]);
        assert!(!unsorted.is_valid());
        let mut address = AuditBrokerSet::from_names(&["10.0.0.1".to_owned()]);
        assert!(!address.is_valid());
        address.count = 0;
        assert!(!address.is_valid());
    }

    #[test]
    fn subjects_have_the_exact_shape_of_their_operation() {
        let brokers = || Some(AuditBrokerSet::from_names(&["broker-a".to_owned()]));
        let topic = AuditTarget {
            topic: Some("orders".to_owned()),
            brokers: brokers(),
            ..AuditTarget::default()
        };
        let group = AuditTarget {
            consumer_group: Some("orders_consumers".to_owned()),
            brokers: brokers(),
            ..AuditTarget::default()
        };
        let pair = AuditTarget {
            topic: Some("orders".to_owned()),
            consumer_group: Some("orders_consumers".to_owned()),
            ..AuditTarget::default()
        };
        let broker = AuditTarget {
            broker: Some("broker-a".to_owned()),
            ..AuditTarget::default()
        };
        let accepted = [
            (ControlOperation::TopicUpsert, &topic),
            (ControlOperation::ConsumerGroupUpsert, &group),
            (ControlOperation::ConsumerOffsetReset, &pair),
            (ControlOperation::ConsumerRequestMode, &pair),
            (ControlOperation::BrokerConfigPatch, &broker),
        ];
        for (operation, target) in accepted {
            assert!(AuditSubject::try_new(operation, target.clone(), digest("requested"), Some(digest("key"))).is_ok());
            assert!(AuditSubject::try_new(operation, target.clone(), digest("requested"), None).is_ok());
            // Every other shape belongs to a different operation.
            for other in [&topic, &group, &pair, &broker] {
                if other != target {
                    let rejected = AuditSubject::try_new(operation, other.clone(), digest("requested"), None);
                    assert_eq!(rejected.err(), Some(ControlError::audit_unavailable()));
                }
            }
            assert!(AuditSubject::try_new(operation, target.clone(), "not-a-digest".to_owned(), None).is_err());
            assert!(
                AuditSubject::try_new(operation, target.clone(), digest("requested"), Some("key".to_owned())).is_err()
            );
        }

        for unsafe_name in ["10.0.0.1", "broker.example.test:10911", "token=secret", ""] {
            let target = AuditTarget {
                broker: Some(unsafe_name.to_owned()),
                ..AuditTarget::default()
            };
            assert!(
                AuditSubject::try_new(ControlOperation::BrokerConfigPatch, target, digest("requested"), None).is_err()
            );
        }
        let rendered = format!("{topic:?}");
        assert!(!rendered.contains("orders") && !rendered.contains("broker-a"));
    }
}
