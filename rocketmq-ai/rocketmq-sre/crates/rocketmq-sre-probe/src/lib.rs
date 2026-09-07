// Copyright 2023 The RocketMQ Rust Authors
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

//! Safe configuration boundary for a future producer/consumer probe.
//!
//! This crate intentionally has no RocketMQ Admin or mutation dependency.

pub mod cleanup;
pub mod consumer;
mod evidence;
pub mod producer;
pub mod scenario;

use std::env;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::Read;

use rocketmq_client_rust::AclClientRPCHook;
use rocketmq_client_rust::SessionCredentials;
use rocketmq_client_rust::SigningAlgorithm;
use rocketmq_error::REDACTED;
use rocketmq_sre_contracts::ClusterId;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

pub use evidence::capture_probe_evidence;

pub const PROBE_TOPIC_PREFIX: &str = "SRE_PROBE_";
pub const PROBE_GROUP_PREFIX: &str = "SRE_PROBE_G_";
pub const MAX_MESSAGES_LIMIT: u16 = 100;
pub const MAX_PAYLOAD_BYTES_LIMIT: u32 = 4_096;
pub const MAX_DURATION_SECONDS_LIMIT: u16 = 300;
pub const MAX_MESSAGES_PER_SECOND_LIMIT: u16 = 20;
pub const MAX_SECRET_KEY_FILE_BYTES: usize = 65_536;
const MAX_SECRET_KEY_FILE_BYTES_U64: u64 = 65_536;
pub const PROBE_ACCESS_KEY_ENV: &str = "ROCKETMQ_SRE_PROBE_ACCESS_KEY";
pub const PROBE_SECRET_KEY_FILE_ENV: &str = "ROCKETMQ_SRE_PROBE_SECRET_KEY_FILE";

/// ACL credentials dedicated to the bounded synthetic probe.
///
/// The secret is loaded from the file referenced by
/// [`PROBE_SECRET_KEY_FILE_ENV`]. This type deliberately does not implement
/// serialization and redacts both credential fields from `Debug`.
#[derive(Clone, Eq, PartialEq)]
pub struct ProbeAclConfig {
    access_key: String,
    secret_key: String,
}

impl fmt::Debug for ProbeAclConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProbeAclConfig")
            .field("access_key", &REDACTED)
            .field("secret_key", &REDACTED)
            .finish()
    }
}

impl ProbeAclConfig {
    /// Creates a signing hook without exposing the credential values.
    pub fn rpc_hook(&self) -> AclClientRPCHook {
        AclClientRPCHook::with_signature_algorithm(
            SessionCredentials::with_keys(self.access_key.clone(), self.secret_key.clone()),
            SigningAlgorithm::HmacSha256,
        )
    }
}

/// Closed optional-identity configuration rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeAclRejection {
    IncompleteCredentials,
    EmptyAccessKey,
    EmptySecretKeyFile,
    SecretKeyFileUnavailable,
    SecretKeyFileInvalid,
    SecretKeyFileTooLarge,
    EmptySecretKey,
    InvalidEnvironment,
}

/// Loads the optional dedicated probe identity from environment references.
///
/// Both variables absent keeps local, unauthenticated development compatible.
/// Any partial or invalid configuration fails closed.
///
/// # Errors
///
/// Returns [`ProbeAclRejection`] when only one reference is configured, a
/// value is empty or non-Unicode, or the referenced secret file is unreadable
/// or empty.
pub fn load_probe_acl_config() -> Result<Option<ProbeAclConfig>, ProbeAclRejection> {
    let access_key = optional_env(PROBE_ACCESS_KEY_ENV)?;
    let secret_key_file = optional_env(PROBE_SECRET_KEY_FILE_ENV)?;
    resolve_probe_acl_config(access_key, secret_key_file, read_secret_key_file)
}

fn optional_env(name: &'static str) -> Result<Option<String>, ProbeAclRejection> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(ProbeAclRejection::InvalidEnvironment),
    }
}

fn read_secret_key_file(path: &str) -> Result<String, ProbeAclRejection> {
    let metadata = fs::metadata(path).map_err(|_| ProbeAclRejection::SecretKeyFileUnavailable)?;
    if !metadata.is_file() {
        return Err(ProbeAclRejection::SecretKeyFileInvalid);
    }
    if metadata.len() > MAX_SECRET_KEY_FILE_BYTES_U64 {
        return Err(ProbeAclRejection::SecretKeyFileTooLarge);
    }

    let file = File::open(path).map_err(|_| ProbeAclRejection::SecretKeyFileUnavailable)?;
    let mut secret = String::new();
    file.take(MAX_SECRET_KEY_FILE_BYTES_U64 + 1)
        .read_to_string(&mut secret)
        .map_err(|_| ProbeAclRejection::SecretKeyFileUnavailable)?;
    if secret.len() > MAX_SECRET_KEY_FILE_BYTES {
        return Err(ProbeAclRejection::SecretKeyFileTooLarge);
    }
    Ok(secret)
}

fn resolve_probe_acl_config<F>(
    access_key: Option<String>,
    secret_key_file: Option<String>,
    read_secret: F,
) -> Result<Option<ProbeAclConfig>, ProbeAclRejection>
where
    F: FnOnce(&str) -> Result<String, ProbeAclRejection>,
{
    let (access_key, secret_key_file) = match (access_key, secret_key_file) {
        (None, None) => return Ok(None),
        (Some(access_key), Some(secret_key_file)) => (access_key, secret_key_file),
        _ => return Err(ProbeAclRejection::IncompleteCredentials),
    };
    let access_key = access_key.trim();
    if access_key.is_empty() {
        return Err(ProbeAclRejection::EmptyAccessKey);
    }
    let secret_key_file = secret_key_file.trim();
    if secret_key_file.is_empty() {
        return Err(ProbeAclRejection::EmptySecretKeyFile);
    }
    let secret_key = read_secret(secret_key_file)?;
    let secret_key = secret_key.trim_end_matches(['\r', '\n']);
    if secret_key.trim().is_empty() {
        return Err(ProbeAclRejection::EmptySecretKey);
    }
    Ok(Some(ProbeAclConfig {
        access_key: access_key.to_owned(),
        secret_key: secret_key.to_owned(),
    }))
}

/// Bounded parameters accepted by the synthetic probe.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProbeConfig {
    pub cluster_id: ClusterId,
    pub max_messages: u16,
    pub max_messages_per_second: u16,
    pub max_payload_bytes: u32,
    pub max_duration_seconds: u16,
}

/// Dedicated topic and group derived by the probe rather than supplied by a caller.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProbeIdentity {
    pub topic: String,
    pub producer_group: String,
    pub consumer_group: String,
}

/// A validated Phase 00 probe plan.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProbePlan {
    pub cluster_id: ClusterId,
    pub run_id: Uuid,
    pub identity: ProbeIdentity,
    pub max_messages: u16,
    pub max_messages_per_second: u16,
    pub max_payload_bytes: u32,
    pub max_duration_seconds: u16,
    /// Topic provisioning is deliberately external because this crate has no
    /// Admin capability.
    pub requires_preprovisioned_topic: bool,
}

/// Closed parameter validation rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeConfigRejection {
    MessagesOutOfRange,
    PayloadOutOfRange,
    RateOutOfRange,
    DurationOutOfRange,
}

impl ProbeConfig {
    /// Validates hard resource limits and derives a dedicated identity.
    ///
    /// Returns a range rejection rather than clamping unsafe input.
    pub fn plan(&self, run_id: Uuid) -> Result<ProbePlan, ProbeConfigRejection> {
        if !(1..=MAX_MESSAGES_LIMIT).contains(&self.max_messages) {
            return Err(ProbeConfigRejection::MessagesOutOfRange);
        }
        if !(1..=MAX_PAYLOAD_BYTES_LIMIT).contains(&self.max_payload_bytes) {
            return Err(ProbeConfigRejection::PayloadOutOfRange);
        }
        if !(1..=MAX_MESSAGES_PER_SECOND_LIMIT).contains(&self.max_messages_per_second) {
            return Err(ProbeConfigRejection::RateOutOfRange);
        }
        if !(1..=MAX_DURATION_SECONDS_LIMIT).contains(&self.max_duration_seconds) {
            return Err(ProbeConfigRejection::DurationOutOfRange);
        }
        let cluster = self.cluster_id.as_uuid().simple();
        let run = run_id.simple();
        Ok(ProbePlan {
            cluster_id: self.cluster_id,
            run_id,
            identity: ProbeIdentity {
                topic: format!("{PROBE_TOPIC_PREFIX}{cluster}_{run}"),
                producer_group: format!("{PROBE_GROUP_PREFIX}P_{cluster}_{run}"),
                consumer_group: format!("{PROBE_GROUP_PREFIX}C_{cluster}_{run}"),
            },
            max_messages: self.max_messages,
            max_messages_per_second: self.max_messages_per_second,
            max_payload_bytes: self.max_payload_bytes,
            max_duration_seconds: self.max_duration_seconds,
            requires_preprovisioned_topic: true,
        })
    }
}

impl ProbePlan {
    /// Replaces derived names with externally pre-provisioned probe resources.
    ///
    /// All names remain inside the dedicated probe namespaces. This supports
    /// POP groups and short-retention Topics that must be provisioned outside
    /// the probe because this crate has no Admin capability.
    ///
    /// Returns a closed rejection when any resource is outside the probe namespace.
    pub fn with_preprovisioned_identity(mut self, identity: ProbeIdentity) -> Result<Self, ProbeIdentityRejection> {
        identity.validate()?;
        self.identity = identity;
        Ok(self)
    }
}

/// Closed resource namespace rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeIdentityRejection {
    TopicOutsideNamespace,
    ProducerGroupOutsideNamespace,
    ConsumerGroupOutsideNamespace,
}

impl ProbeIdentity {
    /// Validates that no business Topic or Group can be selected.
    ///
    /// Returns the exact resource whose namespace was rejected.
    pub fn validate(&self) -> Result<(), ProbeIdentityRejection> {
        if !self.topic.starts_with(PROBE_TOPIC_PREFIX) {
            return Err(ProbeIdentityRejection::TopicOutsideNamespace);
        }
        if !self.producer_group.starts_with(PROBE_GROUP_PREFIX) {
            return Err(ProbeIdentityRejection::ProducerGroupOutsideNamespace);
        }
        if !self.consumer_group.starts_with(PROBE_GROUP_PREFIX) {
            return Err(ProbeIdentityRejection::ConsumerGroupOutsideNamespace);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn acl_config_is_disabled_only_when_both_references_are_absent() {
        let config = resolve_probe_acl_config(None, None, |_| {
            panic!("disabled ACL configuration must not read a secret file")
        })
        .expect("absent references should preserve local development");

        assert!(config.is_none());
    }

    #[test]
    fn acl_config_rejects_every_missing_reference_combination() {
        assert_eq!(
            resolve_probe_acl_config(Some("probe-ak".to_owned()), None, |_| {
                panic!("incomplete configuration must not read a secret file")
            }),
            Err(ProbeAclRejection::IncompleteCredentials)
        );
        assert_eq!(
            resolve_probe_acl_config(None, Some("/run/secrets/probe".to_owned()), |_| {
                panic!("incomplete configuration must not read a secret file")
            }),
            Err(ProbeAclRejection::IncompleteCredentials)
        );
    }

    #[test]
    fn acl_config_loads_secret_reference_and_redacts_debug_output() {
        let config = resolve_probe_acl_config(
            Some("probe-access-key".to_owned()),
            Some("/run/secrets/probe-secret".to_owned()),
            |path| {
                assert_eq!(path, "/run/secrets/probe-secret");
                Ok("probe-secret-value\r\n".to_owned())
            },
        )
        .expect("complete credentials should load")
        .expect("ACL should be enabled");

        assert_eq!(config.access_key, "probe-access-key");
        assert_eq!(config.secret_key, "probe-secret-value");
        let debug = format!("{config:?}");
        assert!(!debug.contains("probe-access-key"));
        assert!(!debug.contains("probe-secret-value"));
        assert!(debug.contains(REDACTED));

        let hook_debug = format!("{:?}", config.rpc_hook());
        assert!(hook_debug.contains("HmacSha256"));
        assert!(!hook_debug.contains("probe-access-key"));
        assert!(!hook_debug.contains("probe-secret-value"));
    }

    #[test]
    fn acl_config_rejects_empty_values_and_unreadable_secret() {
        assert_eq!(
            resolve_probe_acl_config(Some(" ".to_owned()), Some("/run/secrets/probe".to_owned()), |_| panic!(
                "empty access key must be rejected before file access"
            ),),
            Err(ProbeAclRejection::EmptyAccessKey)
        );
        assert_eq!(
            resolve_probe_acl_config(Some("probe-ak".to_owned()), Some(" ".to_owned()), |_| panic!(
                "empty path must be rejected before file access"
            ),),
            Err(ProbeAclRejection::EmptySecretKeyFile)
        );
        assert_eq!(
            resolve_probe_acl_config(
                Some("probe-ak".to_owned()),
                Some("/run/secrets/probe".to_owned()),
                |_| Err(ProbeAclRejection::SecretKeyFileUnavailable),
            ),
            Err(ProbeAclRejection::SecretKeyFileUnavailable)
        );
        assert_eq!(
            resolve_probe_acl_config(
                Some("probe-ak".to_owned()),
                Some("/run/secrets/probe".to_owned()),
                |_| Ok(" \t\r\n".to_owned()),
            ),
            Err(ProbeAclRejection::EmptySecretKey)
        );
    }

    #[test]
    fn secret_file_reader_rejects_non_regular_and_oversized_files() {
        let fixture_dir = env::temp_dir().join(format!("rocketmq-sre-probe-acl-{}", Uuid::new_v4().simple()));
        fs::create_dir(&fixture_dir).expect("fixture directory should be created");
        assert_eq!(
            read_secret_key_file(fixture_dir.to_str().expect("fixture path should be Unicode")),
            Err(ProbeAclRejection::SecretKeyFileInvalid)
        );

        let oversized_file = fixture_dir.join("oversized-secret");
        fs::write(&oversized_file, vec![b'x'; MAX_SECRET_KEY_FILE_BYTES + 1])
            .expect("oversized fixture should be written");
        assert_eq!(
            read_secret_key_file(oversized_file.to_str().expect("fixture path should be Unicode")),
            Err(ProbeAclRejection::SecretKeyFileTooLarge)
        );

        fs::remove_file(oversized_file).expect("fixture file should be removed");
        fs::remove_dir(fixture_dir).expect("fixture directory should be removed");
    }

    #[test]
    fn derives_only_dedicated_bounded_probe_identity() {
        let config = ProbeConfig {
            cluster_id: ClusterId::new(),
            max_messages: MAX_MESSAGES_LIMIT,
            max_messages_per_second: MAX_MESSAGES_PER_SECOND_LIMIT,
            max_payload_bytes: MAX_PAYLOAD_BYTES_LIMIT,
            max_duration_seconds: MAX_DURATION_SECONDS_LIMIT,
        };

        let plan = config.plan(Uuid::nil()).expect("limits should validate");

        assert!(plan.identity.topic.starts_with(PROBE_TOPIC_PREFIX));
        assert!(plan.identity.producer_group.starts_with(PROBE_GROUP_PREFIX));
        assert!(plan.identity.consumer_group.starts_with(PROBE_GROUP_PREFIX));
        assert_eq!(plan.cluster_id, config.cluster_id);
        assert_eq!(plan.run_id, Uuid::nil());
        assert!(plan.requires_preprovisioned_topic);
    }

    #[test]
    fn rejects_business_resources_from_preprovisioned_overrides() {
        let config = ProbeConfig {
            cluster_id: ClusterId::new(),
            max_messages: 1,
            max_messages_per_second: 1,
            max_payload_bytes: 1,
            max_duration_seconds: 1,
        };
        let plan = config.plan(Uuid::nil()).expect("plan should validate");

        assert_eq!(
            plan.with_preprovisioned_identity(ProbeIdentity {
                topic: "orders".to_owned(),
                producer_group: "SRE_PROBE_G_P".to_owned(),
                consumer_group: "SRE_PROBE_G_C".to_owned(),
            }),
            Err(ProbeIdentityRejection::TopicOutsideNamespace)
        );
    }

    #[test]
    fn rejects_unbounded_input_instead_of_clamping() {
        let config = ProbeConfig {
            cluster_id: ClusterId::new(),
            max_messages: MAX_MESSAGES_LIMIT + 1,
            max_messages_per_second: 1,
            max_payload_bytes: 1,
            max_duration_seconds: 1,
        };

        assert_eq!(config.plan(Uuid::nil()), Err(ProbeConfigRejection::MessagesOutOfRange));
    }

    #[test]
    fn rejects_an_unbounded_send_rate() {
        let config = ProbeConfig {
            cluster_id: ClusterId::new(),
            max_messages: 1,
            max_messages_per_second: MAX_MESSAGES_PER_SECOND_LIMIT + 1,
            max_payload_bytes: 1,
            max_duration_seconds: 1,
        };

        assert_eq!(config.plan(Uuid::nil()), Err(ProbeConfigRejection::RateOutOfRange));
    }
}
