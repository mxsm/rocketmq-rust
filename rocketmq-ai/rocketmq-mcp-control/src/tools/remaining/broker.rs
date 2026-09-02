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
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub auto_create_topic_enable: Option<String>,
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub auto_create_subscription_group: Option<String>,
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub broker_permission: Option<String>,
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub default_topic_queue_nums: Option<String>,
    #[serde(default, deserialize_with = "deserialize_present_string")]
    #[schemars(schema_with = "string_schema")]
    pub message_index_enable: Option<String>,
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

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchBrokerConfigArgs {
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    #[schemars(length(min = 1, max = 127), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub broker_name: String,
    pub properties: BrokerConfigProperties,
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    #[serde(default)]
    pub confirm: bool,
    #[serde(default)]
    #[schemars(length(min = 5, max = 256))]
    pub reason: Option<String>,
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
            return Err(ControlError::invalid_arguments());
        }
        Ok(BrokerConfigPatch {
            auto_create_topic_enable: parse_bool(self.auto_create_topic_enable.as_deref())?,
            auto_create_subscription_group: parse_bool(self.auto_create_subscription_group.as_deref())?,
            broker_permission: parse_u32(self.broker_permission.as_deref(), 1, 7)?
                .map(|value| {
                    if value & 0b110 == 0 {
                        Err(ControlError::invalid_arguments())
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
            _ => Err(ControlError::invalid_arguments()),
        })
        .transpose()
}

fn parse_u32(value: Option<&str>, min: u32, max: u32) -> Result<Option<u32>, ControlError> {
    value
        .map(|value| {
            let parsed = value.parse::<u32>().map_err(|_| ControlError::invalid_arguments())?;
            if !(min..=max).contains(&parsed) || parsed.to_string() != value {
                return Err(ControlError::invalid_arguments());
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
                .map_err(|_| ControlError::invalid_arguments())
                .and_then(|args| args.validate(true, false));
            assert!(rejected.is_err());
        }
    }
}
