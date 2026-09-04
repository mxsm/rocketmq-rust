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

use chrono::DateTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::super::{
    default_dry_run, validate_common, validate_user_name, FailureCode, MutationMode, MutationResultSchemaVersion,
    MutationStatus, NameKind,
};
use super::nullable_schema;
use super::validate_consumer_group;
use crate::error::ControlError;
use crate::error::ControlErrorCode;

pub const RESET_CONSUMER_OFFSET_TOOL: &str = "rocketmq_reset_consumer_offset";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ConsumerOffsetResetOperation {
    #[serde(rename = "consumer_offset_reset")]
    ConsumerOffsetReset,
}

operation_schema!(
    ConsumerOffsetResetOperation,
    "ConsumerOffsetResetOperation",
    "consumer_offset_reset"
);

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResetConsumerOffsetArgs {
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    #[schemars(length(min = 1, max = 127), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub topic: String,
    #[schemars(length(min = 1, max = 255), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub consumer_group: String,
    #[schemars(length(min = 20, max = 40))]
    pub timestamp: String,
    #[serde(default)]
    pub force: bool,
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

impl std::fmt::Debug for ResetConsumerOffsetArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResetConsumerOffsetArgs")
            .field("schema_version", &self.schema_version)
            .field("dry_run", &self.dry_run)
            .field("confirm", &self.confirm)
            .field("force", &self.force)
            .finish_non_exhaustive()
    }
}

impl ResetConsumerOffsetArgs {
    pub fn validate(&self, configured_default: bool, omitted: bool) -> Result<i64, ControlError> {
        validate_common(
            &self.schema_version,
            self.effective_dry_run(configured_default, omitted),
            self.confirm,
            self.reason.as_deref(),
            self.request_key.as_deref(),
        )?;
        validate_user_name(&self.topic, NameKind::Topic)?;
        validate_consumer_group(&self.consumer_group)?;
        let timestamp = DateTime::parse_from_rfc3339(&self.timestamp)
            .map_err(|_| ControlError::invalid_argument())?
            .timestamp_millis();
        if timestamp < 0 {
            return Err(ControlError::invalid_argument());
        }
        Ok(timestamp)
    }

    pub fn effective_dry_run(&self, configured_default: bool, omitted: bool) -> bool {
        if omitted {
            configured_default
        } else {
            self.dry_run
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OffsetResetResource {
    pub topic: String,
    pub consumer_group: String,
    pub brokers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OffsetRequested {
    pub timestamp: String,
    pub timestamp_millis: i64,
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OffsetQueueState {
    pub broker_name: String,
    pub queue_id: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OffsetMutationTarget {
    pub broker_name: String,
    #[schemars(required, schema_with = "nullable_schema::<i32>")]
    pub queue_id: Option<i32>,
    #[schemars(required, schema_with = "nullable_schema::<i64>")]
    pub before: Option<i64>,
    #[schemars(required, schema_with = "nullable_schema::<i64>")]
    pub planned: Option<i64>,
    #[schemars(required, schema_with = "nullable_schema::<i64>")]
    pub delta: Option<i64>,
    #[schemars(required, schema_with = "nullable_schema::<i64>")]
    pub after: Option<i64>,
    pub applied: bool,
    pub changed: bool,
    #[schemars(required, schema_with = "nullable_schema::<FailureCode>")]
    pub failure: Option<FailureCode>,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OffsetMutationToolResponse {
    pub schema_version: MutationResultSchemaVersion,
    pub operation: ConsumerOffsetResetOperation,
    pub cluster: String,
    pub mode: MutationMode,
    pub status: MutationStatus,
    #[schemars(required, schema_with = "nullable_schema::<ControlErrorCode>")]
    pub error_code: Option<ControlErrorCode>,
    pub target: OffsetResetResource,
    pub before: Vec<OffsetQueueState>,
    pub requested: OffsetRequested,
    #[schemars(required, schema_with = "nullable_schema::<Vec<OffsetQueueState>>")]
    pub after: Option<Vec<OffsetQueueState>>,
    pub targets: Vec<OffsetMutationTarget>,
    pub warnings: Vec<String>,
}

impl OffsetMutationToolResponse {
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
    fn offset_timestamp_and_names_are_closed() {
        let mut value = serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": "orders",
            "consumer_group": "workers",
            "timestamp": "2026-08-30T08:00:00+08:00",
            "dry_run": true,
            "confirm": false
        });
        let args: ResetConsumerOffsetArgs = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(args.validate(true, false).unwrap(), 1_788_048_000_000);
        for invalid in ["2026-08-30T08:00:00", "1969-12-31T23:59:59Z", "not-a-time"] {
            let mut case = value.clone();
            case["timestamp"] = serde_json::json!(invalid);
            assert!(serde_json::from_value::<ResetConsumerOffsetArgs>(case)
                .unwrap()
                .validate(true, false)
                .is_err());
        }
        value["unknown"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ResetConsumerOffsetArgs>(value).is_err());
    }
}
