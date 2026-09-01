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

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::error::ControlError;

pub const CAPABILITY_SCHEMA_VERSION: &str = "rocketmq-mcp-control.capability.v1";
pub const MUTATION_ARGUMENTS_SCHEMA_VERSION: &str = "rocketmq-mcp-control.arguments.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ControlOperation {
    TopicUpsert,
    ConsumerGroupUpsert,
    ConsumerOffsetReset,
    BrokerConfigPatch,
    ConsumerRequestMode,
}

impl ControlOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TopicUpsert => "topic_upsert",
            Self::ConsumerGroupUpsert => "consumer_group_upsert",
            Self::ConsumerOffsetReset => "consumer_offset_reset",
            Self::BrokerConfigPatch => "broker_config_patch",
            Self::ConsumerRequestMode => "consumer_request_mode",
        }
    }
}

impl FromStr for ControlOperation {
    type Err = ControlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "topic_upsert" => Ok(Self::TopicUpsert),
            "consumer_group_upsert" => Ok(Self::ConsumerGroupUpsert),
            "consumer_offset_reset" => Ok(Self::ConsumerOffsetReset),
            "broker_config_patch" => Ok(Self::BrokerConfigPatch),
            "consumer_request_mode" => Ok(Self::ConsumerRequestMode),
            _ => Err(ControlError::permission_denied()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct ClusterName(String);

impl ClusterName {
    pub fn try_new(value: impl Into<String>) -> Result<Self, ControlError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(ControlError::permission_denied());
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ClusterName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Principal {
    pub subject: String,
    pub scopes: BTreeSet<String>,
    pub allowed_operations: BTreeSet<ControlOperation>,
    pub allowed_clusters: BTreeSet<ClusterName>,
}

impl fmt::Debug for Principal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Principal")
            .field("authenticated", &true)
            .field("scope_count", &self.scopes.len())
            .field("operation_count", &self.allowed_operations.len())
            .field("cluster_count", &self.allowed_clusters.len())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MutationArguments {
    pub schema_version: String,
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    #[serde(default)]
    pub confirm: bool,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub request_key: Option<String>,
}

impl MutationArguments {
    pub fn validate(&self) -> Result<(), ControlError> {
        if self.schema_version != MUTATION_ARGUMENTS_SCHEMA_VERSION
            || self.reason.as_deref().is_some_and(|value| !valid_reason(value))
            || self
                .request_key
                .as_deref()
                .is_some_and(|value| !valid_request_key(value))
            || (!self.dry_run && (!self.confirm || self.reason.is_none()))
        {
            return Err(ControlError::invalid_arguments());
        }
        Ok(())
    }
}

fn valid_reason(value: &str) -> bool {
    (5..=256).contains(&value.len())
        && value.trim().len() >= 5
        && value
            .chars()
            .all(|character| !character.is_control() && !matches!(character, '`' | '<' | '>'))
}

const fn default_dry_run() -> bool {
    true
}

fn valid_request_key(value: &str) -> bool {
    (8..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

/// Capability state derived only from the closed operation catalog.
///
/// Fields are intentionally private so callers cannot fabricate support.
///
/// ```compile_fail
/// use rocketmq_mcp_control::model::ControlCapabilities;
/// let _ = ControlCapabilities {
///     schema_version: "fabricated",
///     write_tools_compiled: true,
///     mutations_runtime_enabled: true,
///     registered_operations: 1,
///     mutation_supported: true,
///     transport: "other",
///     authentication: "other",
///     max_request_bytes: 0,
///     request_timeout_seconds: 0,
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ControlCapabilities {
    schema_version: &'static str,
    write_tools_compiled: bool,
    mutations_runtime_enabled: bool,
    registered_operations: u32,
    mutation_supported: bool,
    transport: &'static str,
    authentication: &'static str,
    max_request_bytes: usize,
    request_timeout_seconds: u64,
}

impl ControlCapabilities {
    pub(crate) fn from_catalog(mutations_runtime_enabled: bool, catalog: &crate::catalog::OperationCatalog) -> Self {
        let write_tools_compiled = cfg!(feature = "write-tools");
        let registered_operations = catalog.registered_operations();
        Self {
            schema_version: CAPABILITY_SCHEMA_VERSION,
            write_tools_compiled,
            mutations_runtime_enabled,
            registered_operations,
            mutation_supported: write_tools_compiled && mutations_runtime_enabled && registered_operations > 0,
            transport: "streamable_https",
            authentication: "oauth_rs256_jwks",
            max_request_bytes: 1024 * 1024,
            request_timeout_seconds: 30,
        }
    }

    pub const fn schema_version(&self) -> &str {
        self.schema_version
    }

    pub const fn write_tools_compiled(&self) -> bool {
        self.write_tools_compiled
    }

    pub const fn mutations_runtime_enabled(&self) -> bool {
        self.mutations_runtime_enabled
    }

    pub const fn registered_operations(&self) -> u32 {
        self.registered_operations
    }

    pub const fn mutation_supported(&self) -> bool {
        self.mutation_supported
    }

    pub const fn transport(&self) -> &str {
        self.transport
    }

    pub const fn authentication(&self) -> &str {
        self.authentication
    }

    pub const fn max_request_bytes(&self) -> usize {
        self.max_request_bytes
    }

    pub const fn request_timeout_seconds(&self) -> u64 {
        self.request_timeout_seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_arguments() -> serde_json::Value {
        serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "dry_run": true,
            "confirm": false
        })
    }

    #[test]
    fn common_arguments_are_closed_and_bounded() {
        let arguments: MutationArguments = serde_json::from_value(valid_arguments()).unwrap();
        arguments.validate().unwrap();
        assert!(arguments.dry_run);
        assert!(!arguments.confirm);
        assert!(arguments.reason.is_none());
        assert!(arguments.request_key.is_none());

        let optional_nulls: MutationArguments = serde_json::from_value(serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "reason": null,
            "request_key": null
        }))
        .unwrap();
        optional_nulls.validate().unwrap();

        let execute: MutationArguments = serde_json::from_value(serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "dry_run": false,
            "confirm": true,
            "reason": "planned change",
            "request_key": "request-1234"
        }))
        .unwrap();
        execute.validate().unwrap();

        let mut cases = Vec::new();
        let mut unknown = valid_arguments();
        unknown["unknown"] = serde_json::json!(true);
        cases.push(unknown);
        for (field, value) in [
            ("reason", serde_json::json!("")),
            ("reason", serde_json::json!("four")),
            ("reason", serde_json::json!("operator\ncommand")),
            ("reason", serde_json::json!(42)),
            ("reason", serde_json::json!("`command`")),
            ("reason", serde_json::json!("x".repeat(257))),
            ("request_key", serde_json::json!("short")),
            ("request_key", serde_json::json!("bad key")),
            ("request_key", serde_json::json!("x".repeat(65))),
            ("request_key", serde_json::json!(42)),
            ("confirm", serde_json::Value::Null),
            ("confirm", serde_json::json!("yes")),
            ("dry_run", serde_json::Value::Null),
            ("dry_run", serde_json::json!("true")),
        ] {
            let mut case = valid_arguments();
            case[field] = value;
            cases.push(case);
        }
        let mut execute_without_confirmation = valid_arguments();
        execute_without_confirmation["dry_run"] = serde_json::json!(false);
        cases.push(execute_without_confirmation);

        let mut execute_without_reason = valid_arguments();
        execute_without_reason["dry_run"] = serde_json::json!(false);
        execute_without_reason["confirm"] = serde_json::json!(true);
        cases.push(execute_without_reason);

        for case in cases {
            let rejected = serde_json::from_value::<MutationArguments>(case)
                .map_err(|_| ControlError::invalid_arguments())
                .and_then(|arguments| arguments.validate());
            assert!(rejected.is_err());
        }
    }

    #[test]
    fn cluster_aliases_and_operation_ids_are_closed() {
        for valid in ["cluster-a", "cluster_A", "A1"] {
            assert!(ClusterName::try_new(valid).is_ok());
        }
        for invalid in ["", "cluster.a", "10.0.0.1", "host:9876", "token=secret"] {
            assert!(ClusterName::try_new(invalid).is_err());
        }
        assert!(ClusterName::try_new("x".repeat(65)).is_err());
        for operation in [
            "topic_upsert",
            "consumer_group_upsert",
            "consumer_offset_reset",
            "broker_config_patch",
            "consumer_request_mode",
        ] {
            assert!(ControlOperation::from_str(operation).is_ok());
        }
        for rejected in [
            "skip_accumulated_messages",
            "resend_dead_letter_message",
            "free_form_rpc",
        ] {
            assert!(ControlOperation::from_str(rejected).is_err());
        }
    }
}
