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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::super::{
    default_dry_run, validate_common, validate_user_name, FailureCode, MutationMode, MutationResultSchemaVersion,
    MutationStatus, NameKind, PersistenceState, VerificationState,
};
use super::nullable_schema;
use super::validate_consumer_group;
use crate::error::ControlError;
use crate::error::ControlErrorCode;

pub const SET_CONSUMER_REQUEST_MODE_TOOL: &str = "rocketmq_set_consumer_request_mode";
pub const MAX_REQUEST_MODE_TIMEOUT_MILLIS: u64 = 24_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ConsumerRequestModeOperation {
    #[serde(rename = "consumer_request_mode")]
    ConsumerRequestMode,
}

operation_schema!(
    ConsumerRequestModeOperation,
    "ConsumerRequestModeOperation",
    "consumer_request_mode"
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerRequestMode {
    Pull,
    Pop,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SetConsumerRequestModeArgs {
    /// Version of this argument schema. Always `rocketmq-mcp-control.arguments.v1`.
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    /// Logical cluster name from the server configuration, never a NameServer or Broker address. Both the
    /// caller and the server policy must allow it.
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    /// Topic that the request mode applies to. System Topics are rejected.
    #[schemars(length(min = 1, max = 127), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub topic: String,
    /// Consumer Group that the request mode applies to. Built-in system groups are rejected.
    #[schemars(length(min = 1, max = 255), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub consumer_group: String,
    /// How the group fetches messages of the Topic: `pull` or `pop`.
    pub mode: ConsumerRequestMode,
    /// Number of queues that pop consumers share, 0 or more.
    #[schemars(range(min = 0))]
    pub pop_share_queue_num: i32,
    /// Timeout of the change request sent to each Broker, 1 to 24000 milliseconds.
    #[schemars(range(min = 1, max = 24000))]
    pub timeout_millis: u64,
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

impl std::fmt::Debug for SetConsumerRequestModeArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SetConsumerRequestModeArgs")
            .field("schema_version", &self.schema_version)
            .field("mode", &self.mode)
            .field("pop_share_queue_num", &self.pop_share_queue_num)
            .field("timeout_millis", &self.timeout_millis)
            .field("dry_run", &self.dry_run)
            .field("confirm", &self.confirm)
            .finish_non_exhaustive()
    }
}

impl SetConsumerRequestModeArgs {
    pub fn validate(&self, configured_default: bool, omitted: bool) -> Result<(), ControlError> {
        validate_common(
            &self.schema_version,
            self.effective_dry_run(configured_default, omitted),
            self.confirm,
            self.reason.as_deref(),
            self.request_key.as_deref(),
        )?;
        validate_user_name(&self.topic, NameKind::Topic)?;
        validate_consumer_group(&self.consumer_group)?;
        if self.pop_share_queue_num < 0 || !(1..=MAX_REQUEST_MODE_TIMEOUT_MILLIS).contains(&self.timeout_millis) {
            return Err(ControlError::invalid_argument());
        }
        Ok(())
    }

    pub fn effective_dry_run(&self, configured_default: bool, omitted: bool) -> bool {
        if omitted {
            configured_default
        } else {
            self.dry_run
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestModeValue {
    pub mode: ConsumerRequestMode,
    pub pop_share_queue_num: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestModeRequested {
    pub mode: ConsumerRequestMode,
    pub pop_share_queue_num: i32,
    #[schemars(range(min = 1, max = 24000))]
    pub timeout_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestModeResource {
    pub topic: String,
    pub consumer_group: String,
    pub brokers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestModeMutationTarget {
    pub broker_name: String,
    #[schemars(required, schema_with = "nullable_schema::<RequestModeValue>")]
    pub before: Option<RequestModeValue>,
    pub requested: RequestModeValue,
    #[schemars(required, schema_with = "nullable_schema::<RequestModeValue>")]
    pub after: Option<RequestModeValue>,
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
pub struct RequestModeMutationToolResponse {
    pub schema_version: MutationResultSchemaVersion,
    pub operation: ConsumerRequestModeOperation,
    pub cluster: String,
    pub mode: MutationMode,
    pub status: MutationStatus,
    #[schemars(required, schema_with = "nullable_schema::<ControlErrorCode>")]
    pub error_code: Option<ControlErrorCode>,
    pub target: RequestModeResource,
    pub before: BTreeMap<String, Option<RequestModeValue>>,
    pub requested: RequestModeRequested,
    #[schemars(
        required,
        schema_with = "nullable_schema::<BTreeMap<String, Option<RequestModeValue>>>"
    )]
    pub after: Option<BTreeMap<String, Option<RequestModeValue>>>,
    pub targets: Vec<RequestModeMutationTarget>,
    pub warnings: Vec<String>,
}

impl RequestModeMutationToolResponse {
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
    fn request_mode_bounds_and_debug_are_closed() {
        let mut value = serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "topic": "orders",
            "consumer_group": "workers",
            "mode": "pop",
            "pop_share_queue_num": 4,
            "timeout_millis": 24000,
            "dry_run": true,
            "confirm": false
        });
        let args: SetConsumerRequestModeArgs = serde_json::from_value(value.clone()).unwrap();
        args.validate(true, false).unwrap();
        assert!(!format!("{args:?}").contains("orders"));
        value["timeout_millis"] = serde_json::json!(24_001);
        assert!(serde_json::from_value::<SetConsumerRequestModeArgs>(value)
            .unwrap()
            .validate(true, false)
            .is_err());
    }
}
