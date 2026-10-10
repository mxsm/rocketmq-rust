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

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Deserializer, Serialize};

use super::super::{
    default_dry_run, validate_common, validate_user_name, FailureCode, MutationMode, MutationResultSchemaVersion,
    MutationStatus, NameKind, PersistenceState, VerificationState,
};
use super::nullable_schema;
use crate::error::ControlError;
use crate::error::ControlErrorCode;

pub const PATCH_BROKER_CONFIG_TOOL: &str = "rocketmq_patch_broker_config";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BrokerConfigPatchOperation {
    #[serde(rename = "broker_config_patch")]
    BrokerConfigPatch,
}

operation_schema!(
    BrokerConfigPatchOperation,
    "BrokerConfigPatchOperation",
    "broker_config_patch"
);

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BrokerConfigProperties {
    /// `true` or `false`: whether the Broker creates a missing Topic on first use.
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub auto_create_topic_enable: Option<String>,
    /// `true` or `false`: whether the Broker creates a missing Consumer Group on first use.
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub auto_create_subscription_group: Option<String>,
    /// Permission bits as a decimal string from `1` to `7`: 2 allows writing, 4 allows reading, 6 allows both.
    /// At least one of reading or writing must be allowed.
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub broker_permission: Option<String>,
    /// Queue count of automatically created Topics, as a decimal string from `1` to `128`.
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub default_topic_queue_nums: Option<String>,
    /// `true` or `false`: whether the Broker builds the message index.
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub message_index_enable: Option<String>,
    /// `true` or `false`: whether the Broker hosts the system message-trace Topic.
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub trace_topic_enable: Option<String>,
}

fn deserialize_present_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

fn string_schema(_generator: &mut SchemaGenerator) -> Schema {
    schemars::json_schema!({"type": "string"})
}

/// Decodes the settings from a JSON object only, as the input schema declares.
///
/// A derived struct also decodes from an array by field position, which would bind values to
/// settings that the caller never named.
fn deserialize_properties_object<'de, D>(deserializer: D) -> Result<BrokerConfigProperties, D::Error>
where
    D: Deserializer<'de>,
{
    struct ObjectOnly;

    impl<'de> serde::de::Visitor<'de> for ObjectOnly {
        type Value = BrokerConfigProperties;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an object of Broker settings")
        }

        fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            BrokerConfigProperties::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_map(ObjectOnly)
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchBrokerConfigArgs {
    /// Version of this argument schema. Always `rocketmq-mcp-control.arguments.v1`.
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    /// Logical cluster name from the server configuration, never a NameServer or Broker address. Both the
    /// caller and the server policy must allow it.
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    /// Logical name of the one Broker to patch; never an address.
    #[schemars(length(min = 1, max = 127), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub broker_name: String,
    /// Settings to change, at least one. Each value is a string, and a setting left out keeps its current
    /// value.
    #[serde(deserialize_with = "deserialize_properties_object")]
    pub properties: BrokerConfigProperties,
    /// Plan only: read the current state and report what would change, without writing. When omitted, the
    /// server's configured default applies, which is a dry run unless the operator changed it.
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    /// Explicit confirmation. Must be true to execute, that is when `dry_run` is false; a dry run does not
    /// need it.
    #[serde(default)]
    pub confirm: bool,
    /// Why the change is made; kept only in the durable audit log. Required to execute. 5 to 256 characters of
    /// letters, digits, spaces and `._,#-`, without addresses, host names or tokens.
    #[serde(default)]
    #[schemars(length(min = 5, max = 256))]
    pub reason: Option<String>,
    /// Optional idempotency key of 8 to 64 letters, digits and `._:-`. An execute call that repeats a key with
    /// the same arguments returns the outcome already recorded for it instead of writing again; the same key
    /// with different arguments is rejected.
    #[serde(default)]
    #[schemars(length(min = 8, max = 64), regex(pattern = "^[a-zA-Z0-9._:-]+$"))]
    pub request_key: Option<String>,
}

impl std::fmt::Debug for PatchBrokerConfigArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PatchBrokerConfigArgs")
            .field("schema_version", &self.schema_version)
            .field("property_count", &self.properties.count())
            .field("dry_run", &self.dry_run)
            .field("confirm", &self.confirm)
            .finish_non_exhaustive()
    }
}

impl BrokerConfigProperties {
    fn count(&self) -> usize {
        [
            self.auto_create_topic_enable.is_some(),
            self.auto_create_subscription_group.is_some(),
            self.broker_permission.is_some(),
            self.default_topic_queue_nums.is_some(),
            self.message_index_enable.is_some(),
            self.trace_topic_enable.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count()
    }

    pub fn typed(&self) -> Result<BrokerConfigPatch, ControlError> {
        if self.count() == 0 {
            return Err(ControlError::invalid_argument());
        }
        Ok(BrokerConfigPatch {
            auto_create_topic_enable: parse_bool(self.auto_create_topic_enable.as_deref())?,
            auto_create_subscription_group: parse_bool(self.auto_create_subscription_group.as_deref())?,
            broker_permission: parse_u32(self.broker_permission.as_deref(), 1, 7)?
                .map(|value| {
                    if value & 0b110 == 0 {
                        Err(ControlError::invalid_argument())
                    } else {
                        Ok(value)
                    }
                })
                .transpose()?,
            default_topic_queue_nums: parse_u32(self.default_topic_queue_nums.as_deref(), 1, 128)?,
            message_index_enable: parse_bool(self.message_index_enable.as_deref())?,
            trace_topic_enable: parse_bool(self.trace_topic_enable.as_deref())?,
        })
    }
}

impl PatchBrokerConfigArgs {
    pub fn validate(&self, configured_default: bool, omitted: bool) -> Result<BrokerConfigPatch, ControlError> {
        validate_common(
            &self.schema_version,
            self.effective_dry_run(configured_default, omitted),
            self.confirm,
            self.reason.as_deref(),
            self.request_key.as_deref(),
        )?;
        validate_user_name(&self.broker_name, NameKind::Broker)?;
        self.properties.typed()
    }

    pub fn effective_dry_run(&self, configured_default: bool, omitted: bool) -> bool {
        if omitted {
            configured_default
        } else {
            self.dry_run
        }
    }
}

fn parse_bool(value: Option<&str>) -> Result<Option<bool>, ControlError> {
    value
        .map(|value| match value {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(ControlError::invalid_argument()),
        })
        .transpose()
}

fn parse_u32(value: Option<&str>, min: u32, max: u32) -> Result<Option<u32>, ControlError> {
    value
        .map(|value| {
            let parsed = value.parse::<u32>().map_err(|_| ControlError::invalid_argument())?;
            if !(min..=max).contains(&parsed) || parsed.to_string() != value {
                return Err(ControlError::invalid_argument());
            }
            Ok(parsed)
        })
        .transpose()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BrokerConfigState {
    pub generation: u64,
    pub auto_create_topic_enable: bool,
    pub auto_create_subscription_group: bool,
    pub broker_permission: u32,
    pub default_topic_queue_nums: u32,
    pub message_index_enable: bool,
    pub trace_topic_enable: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BrokerConfigPatch {
    pub auto_create_topic_enable: Option<bool>,
    pub auto_create_subscription_group: Option<bool>,
    pub broker_permission: Option<u32>,
    pub default_topic_queue_nums: Option<u32>,
    pub message_index_enable: Option<bool>,
    pub trace_topic_enable: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfigResource {
    pub broker_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfigMutationTarget {
    pub broker_name: String,
    #[schemars(required, schema_with = "nullable_schema::<BrokerConfigState>")]
    pub before: Option<BrokerConfigState>,
    pub requested: BrokerConfigPatch,
    #[schemars(required, schema_with = "nullable_schema::<BrokerConfigState>")]
    pub after: Option<BrokerConfigState>,
    pub applied: bool,
    pub changed: bool,
    pub persistence: PersistenceState,
    pub verification: VerificationState,
    #[schemars(required, schema_with = "nullable_schema::<FailureCode>")]
    pub failure: Option<FailureCode>,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfigMutationToolResponse {
    pub schema_version: MutationResultSchemaVersion,
    pub operation: BrokerConfigPatchOperation,
    pub cluster: String,
    pub mode: MutationMode,
    pub status: MutationStatus,
    #[schemars(required, schema_with = "nullable_schema::<ControlErrorCode>")]
    pub error_code: Option<ControlErrorCode>,
    pub target: BrokerConfigResource,
    pub before: BTreeMap<String, BrokerConfigState>,
    pub requested: BrokerConfigPatch,
    #[schemars(required, schema_with = "nullable_schema::<BTreeMap<String, BrokerConfigState>>")]
    pub after: Option<BTreeMap<String, BrokerConfigState>>,
    pub targets: Vec<BrokerConfigMutationTarget>,
    pub warnings: Vec<String>,
}

impl BrokerConfigMutationToolResponse {
    pub fn is_error(&self) -> bool {
        matches!(
            self.status,
            MutationStatus::Partial | MutationStatus::Conflict | MutationStatus::Failed
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION;

    #[test]
    fn broker_properties_are_closed_and_canonical() {
        let value = serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "broker_name": "broker-a",
            "properties": {"brokerPermission":"6","traceTopicEnable":"false"},
            "dry_run": true,
            "confirm": false
        });
        let args: PatchBrokerConfigArgs = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(args.validate(true, false).unwrap().broker_permission, Some(6));
        for properties in [
            serde_json::json!({}),
            serde_json::json!({"brokerPermission":"0"}),
            serde_json::json!({"brokerPermission":"06"}),
            serde_json::json!({"traceTopicEnable":"False"}),
            serde_json::json!({"traceTopicEnable":null}),
            serde_json::json!({"brokerPermission":"6","traceTopicEnable":null}),
            serde_json::json!({"unknown":"true"}),
        ] {
            let mut case = value.clone();
            case["properties"] = properties;
            let rejected = serde_json::from_value::<PatchBrokerConfigArgs>(case)
                .map_err(|_| ControlError::invalid_argument())
                .and_then(|args| args.validate(true, false));
            assert!(rejected.is_err());
        }
    }

    #[test]
    fn broker_properties_decode_from_an_object_only() {
        let value = serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "broker_name": "broker-a",
            "properties": {"traceTopicEnable": "true"}
        });
        let args: PatchBrokerConfigArgs = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(args.validate(true, true).unwrap().trace_topic_enable, Some(true));
        // An array must not be read by field position: its first entry would otherwise become
        // `autoCreateTopicEnable`, a setting the caller did not name.
        for properties in [
            serde_json::json!(["true"]),
            serde_json::json!(["true", "true", "6", "8", "true", "true"]),
            serde_json::json!([]),
            serde_json::json!("true"),
            serde_json::Value::Null,
        ] {
            let mut case = value.clone();
            case["properties"] = properties;
            assert!(serde_json::from_value::<PatchBrokerConfigArgs>(case).is_err());
        }
    }
}
