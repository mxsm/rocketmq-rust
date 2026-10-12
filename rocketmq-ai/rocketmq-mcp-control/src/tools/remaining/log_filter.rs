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
    default_dry_run, validate_common, validate_operation_id, validate_user_name, FailureCode, MutationMode,
    MutationResultSchemaVersion, MutationStatus, NameKind, VerificationState,
};
use super::nullable_schema;
use super::OperationHolder;
use crate::error::ControlError;
use crate::error::ControlErrorCode;

pub const SET_BROKER_LOG_FILTER_TTL_TOOL: &str = "rocketmq_set_broker_log_filter_ttl";
pub const RESTORE_BROKER_LOG_FILTER_TOOL: &str = "rocketmq_restore_broker_log_filter";
pub const MIN_LOG_FILTER_TTL_SECONDS: u32 = 60;
pub const MAX_LOG_FILTER_TTL_SECONDS: u32 = 900;

const LOGGER_ROOT: &str = "rocketmq_broker::";
const MAX_LOGGER_BYTES: usize = 128;

/// Which of the two log-filter operations a response answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BrokerLogFilterOperation {
    BrokerLogFilterSet,
    BrokerLogFilterRestore,
}

/// The levels a temporary override can give a logger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum BrokerLogLevel {
    Info,
    Debug,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SetBrokerLogFilterTtlArgs {
    /// Version of this argument schema. Always `rocketmq-mcp-control.arguments.v1`.
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    /// Logical cluster name from the server configuration, never a NameServer or Broker address. Both the
    /// caller and the server policy must allow it.
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    /// Logical name of the Broker whose master gets the override; never an address.
    #[schemars(length(min = 1, max = 127), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub broker_name: String,
    /// Logger to change: a lower-case `rocketmq_broker::` module path such as `rocketmq_broker::processor`, at
    /// most 128 characters. Loggers of modules that handle message content, credentials or storage are
    /// rejected.
    #[schemars(length(min = 18, max = 128), regex(pattern = LOGGER_PATTERN))]
    pub logger: String,
    /// Level the logger gets until the override ends.
    pub level: BrokerLogLevel,
    /// Seconds after which the Broker ends the override by itself, 60 to 900.
    #[schemars(range(min = 60, max = 900))]
    pub ttl_seconds: u32,
    /// Identifier you choose for this override: 8 to 64 letters, digits, `_` and `-`. The Broker keeps it
    /// while the override is active, and `rocketmq_restore_broker_log_filter` must pass the same one.
    #[schemars(length(min = 8, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub operation_id: String,
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

impl std::fmt::Debug for SetBrokerLogFilterTtlArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SetBrokerLogFilterTtlArgs")
            .field("schema_version", &self.schema_version)
            .field("level", &self.level)
            .field("ttl_seconds", &self.ttl_seconds)
            .field("dry_run", &self.dry_run)
            .field("confirm", &self.confirm)
            .finish_non_exhaustive()
    }
}

impl SetBrokerLogFilterTtlArgs {
    pub fn validate(&self, configured_default: bool, omitted: bool) -> Result<(), ControlError> {
        validate_common(
            &self.schema_version,
            self.effective_dry_run(configured_default, omitted),
            self.confirm,
            self.reason.as_deref(),
            self.request_key.as_deref(),
        )?;
        validate_user_name(&self.broker_name, NameKind::Broker)?;
        validate_operation_id(&self.operation_id)?;
        if self.logger.len() > MAX_LOGGER_BYTES
            || !is_broker_module_path(&self.logger)
            || !(MIN_LOG_FILTER_TTL_SECONDS..=MAX_LOG_FILTER_TTL_SECONDS).contains(&self.ttl_seconds)
        {
            return Err(ControlError::invalid_argument());
        }
        // The adapter owns the list of modules whose logs stay closed.
        #[cfg(feature = "write-tools")]
        if !rocketmq_admin_core::core::supervised_operation::is_supervised_broker_logger(&self.logger) {
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

/// The pattern that the input schema declares for `logger`.
pub(crate) const LOGGER_PATTERN: &str = "^rocketmq_broker(::[a-z_][a-z0-9_]*)+$";

/// Returns whether `logger` matches [`LOGGER_PATTERN`], without a regex engine.
pub(crate) fn is_broker_module_path(logger: &str) -> bool {
    logger.strip_prefix(LOGGER_ROOT).is_some_and(|path| {
        path.split("::").all(|segment| {
            let mut bytes = segment.bytes();
            bytes
                .next()
                .is_some_and(|first| first.is_ascii_lowercase() || first == b'_')
                && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        })
    })
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RestoreBrokerLogFilterArgs {
    /// Version of this argument schema. Always `rocketmq-mcp-control.arguments.v1`.
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    /// Logical cluster name from the server configuration, never a NameServer or Broker address. Both the
    /// caller and the server policy must allow it.
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    /// Logical name of the Broker whose master holds the override; never an address.
    #[schemars(length(min = 1, max = 127), regex(pattern = "^[%|a-zA-Z0-9_-]+$"))]
    pub broker_name: String,
    /// Identifier the override was set with: 8 to 64 letters, digits, `_` and `-`. An override that another
    /// identifier holds is left alone.
    #[schemars(length(min = 8, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub operation_id: String,
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

impl std::fmt::Debug for RestoreBrokerLogFilterArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RestoreBrokerLogFilterArgs")
            .field("schema_version", &self.schema_version)
            .field("dry_run", &self.dry_run)
            .field("confirm", &self.confirm)
            .finish_non_exhaustive()
    }
}

impl RestoreBrokerLogFilterArgs {
    pub fn validate(&self, configured_default: bool, omitted: bool) -> Result<(), ControlError> {
        validate_common(
            &self.schema_version,
            self.effective_dry_run(configured_default, omitted),
            self.confirm,
            self.reason.as_deref(),
            self.request_key.as_deref(),
        )?;
        validate_user_name(&self.broker_name, NameKind::Broker)?;
        validate_operation_id(&self.operation_id)
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
pub struct BrokerLogFilterState {
    /// Effective level of the logger a set names. Null for a restore, which names no logger, and when the
    /// level is neither `INFO` nor `DEBUG`.
    #[schemars(required, schema_with = "nullable_schema::<BrokerLogLevel>")]
    pub level: Option<BrokerLogLevel>,
    /// Whether an override is active, and whether the `operation_id` of this call set it.
    pub held_by: OperationHolder,
    /// Unix time in milliseconds at which the Broker ends the active override by itself; null when none is
    /// active.
    #[schemars(required, schema_with = "nullable_schema::<u64>")]
    pub expires_at_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum BrokerLogFilterRequested {
    Set {
        logger: String,
        level: BrokerLogLevel,
        ttl_seconds: u32,
    },
    Restore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrokerLogFilterResource {
    pub broker_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrokerLogFilterTarget {
    pub broker_name: String,
    /// State read before the call changed anything; null when the Broker could not be read.
    #[schemars(required, schema_with = "nullable_schema::<BrokerLogFilterState>")]
    pub before: Option<BrokerLogFilterState>,
    pub requested: BrokerLogFilterRequested,
    /// State read after an execution; null for a dry run and when the Broker could not be read again.
    #[schemars(required, schema_with = "nullable_schema::<BrokerLogFilterState>")]
    pub after: Option<BrokerLogFilterState>,
    /// Whether the Broker acknowledged the action or already was in the requested state.
    pub applied: bool,
    /// For a dry run, whether executing would change the Broker; otherwise whether it changed.
    pub changed: bool,
    pub verification: VerificationState,
    #[schemars(required, schema_with = "nullable_schema::<FailureCode>")]
    pub failure: Option<FailureCode>,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BrokerLogFilterToolResponse {
    pub schema_version: MutationResultSchemaVersion,
    pub operation: BrokerLogFilterOperation,
    pub cluster: String,
    pub mode: MutationMode,
    pub status: MutationStatus,
    #[schemars(required, schema_with = "nullable_schema::<ControlErrorCode>")]
    pub error_code: Option<ControlErrorCode>,
    pub target: BrokerLogFilterResource,
    /// State before the call, by Broker name; empty when the Broker could not be read.
    pub before: BTreeMap<String, BrokerLogFilterState>,
    pub requested: BrokerLogFilterRequested,
    #[schemars(required, schema_with = "nullable_schema::<BTreeMap<String, BrokerLogFilterState>>")]
    pub after: Option<BTreeMap<String, BrokerLogFilterState>>,
    pub targets: Vec<BrokerLogFilterTarget>,
    pub warnings: Vec<String>,
}

impl BrokerLogFilterToolResponse {
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

    fn set_arguments() -> serde_json::Value {
        serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "broker_name": "broker-a",
            "logger": "rocketmq_broker::processor",
            "level": "DEBUG",
            "ttl_seconds": 300,
            "operation_id": "debug-0001",
            "dry_run": true,
            "confirm": false
        })
    }

    fn set(value: serde_json::Value) -> Result<(), ControlError> {
        serde_json::from_value::<SetBrokerLogFilterTtlArgs>(value)
            .map_err(|_| ControlError::invalid_argument())
            .and_then(|args| args.validate(true, false))
    }

    fn with(member: &str, value: serde_json::Value) -> serde_json::Value {
        let mut case = set_arguments();
        case[member] = value;
        case
    }

    #[test]
    fn log_filter_ttl_outside_sixty_to_nine_hundred_is_rejected() {
        for ttl_seconds in [MIN_LOG_FILTER_TTL_SECONDS, 300, MAX_LOG_FILTER_TTL_SECONDS] {
            set(with("ttl_seconds", serde_json::json!(ttl_seconds))).unwrap();
        }
        for ttl_seconds in [
            serde_json::json!(0),
            serde_json::json!(59),
            serde_json::json!(901),
            serde_json::json!(7_200),
            serde_json::json!(-60),
            serde_json::json!("300"),
            serde_json::Value::Null,
        ] {
            assert!(
                set(with("ttl_seconds", ttl_seconds.clone())).is_err(),
                "accepted {ttl_seconds}"
            );
        }
        let mut missing = set_arguments();
        missing.as_object_mut().unwrap().remove("ttl_seconds");
        assert!(set(missing).is_err());
    }

    #[test]
    fn logger_outside_the_broker_namespace_is_rejected() {
        for logger in ["rocketmq_broker::processor", "rocketmq_broker::processor::send_2"] {
            set(with("logger", serde_json::json!(logger))).unwrap();
        }
        for logger in [
            "",
            "rocketmq_broker",
            "rocketmq_broker::",
            "rocketmq_store::commit_log",
            "rocketmq_remoting::codec",
            "tokio::runtime",
            "info",
            "rocketmq_broker::Processor",
            "rocketmq_broker::processor=trace",
            "rocketmq_broker::processor,rocketmq_store=trace",
            "rocketmq_broker::processor[span]",
            " rocketmq_broker::processor",
            &format!("rocketmq_broker::{}", "a".repeat(128)),
        ] {
            assert!(
                set(with("logger", serde_json::json!(logger))).is_err(),
                "accepted {logger}"
            );
        }
        // Modules that log message content, credentials or storage internals stay closed.
        #[cfg(feature = "write-tools")]
        for logger in [
            "rocketmq_broker::message",
            "rocketmq_broker::processor::send_message",
            "rocketmq_broker::auth",
            "rocketmq_broker::acl::plain",
            "rocketmq_broker::store",
        ] {
            assert!(
                set(with("logger", serde_json::json!(logger))).is_err(),
                "accepted {logger}"
            );
        }
    }

    #[test]
    fn the_published_logger_pattern_is_the_one_validation_applies() {
        let schema = serde_json::to_value(schemars::schema_for!(SetBrokerLogFilterTtlArgs)).unwrap();
        assert_eq!(schema["properties"]["logger"]["pattern"], LOGGER_PATTERN);
        assert_eq!(schema["properties"]["logger"]["maxLength"], MAX_LOGGER_BYTES);
        assert_eq!(
            schema["properties"]["ttl_seconds"]["minimum"],
            MIN_LOG_FILTER_TTL_SECONDS
        );
        assert_eq!(
            schema["properties"]["ttl_seconds"]["maximum"],
            MAX_LOG_FILTER_TTL_SECONDS
        );
    }

    #[test]
    fn levels_names_and_identifiers_are_closed() {
        set(with("level", serde_json::json!("INFO"))).unwrap();
        for level in ["TRACE", "WARN", "ERROR", "OFF", "debug", ""] {
            assert!(
                set(with("level", serde_json::json!(level))).is_err(),
                "accepted {level}"
            );
        }
        for broker_name in ["10.0.0.1", "broker-a.example.test:10911", "broker a", ""] {
            assert!(
                set(with("broker_name", serde_json::json!(broker_name))).is_err(),
                "accepted {broker_name}"
            );
        }
        for operation_id in ["short", "debug.0001", "10.0.0.1:10911", ""] {
            assert!(
                set(with("operation_id", serde_json::json!(operation_id))).is_err(),
                "accepted {operation_id}"
            );
        }
        assert!(set(with("filter", serde_json::json!("trace"))).is_err());

        let args: SetBrokerLogFilterTtlArgs = serde_json::from_value(set_arguments()).unwrap();
        let rendered = format!("{args:?}");
        for hidden in ["broker-a", "debug-0001", "rocketmq_broker::processor"] {
            assert!(!rendered.contains(hidden), "{rendered}");
        }
    }

    #[test]
    fn a_restore_names_a_broker_and_an_operation_and_nothing_else() {
        let restore = |value: serde_json::Value| {
            serde_json::from_value::<RestoreBrokerLogFilterArgs>(value)
                .map_err(|_| ControlError::invalid_argument())
                .and_then(|args| args.validate(true, false))
        };
        let arguments = serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "broker_name": "broker-a",
            "operation_id": "debug-0001"
        });
        restore(arguments.clone()).unwrap();
        for (member, value) in [
            ("logger", serde_json::json!("rocketmq_broker::processor")),
            ("level", serde_json::json!("INFO")),
            ("ttl_seconds", serde_json::json!(60)),
            ("broker_addr", serde_json::json!("10.0.0.1:10911")),
        ] {
            let mut case = arguments.clone();
            case[member] = value;
            assert!(restore(case).is_err(), "accepted {member}");
        }
        for (member, value) in [("broker_name", "10.0.0.1:10911"), ("operation_id", "short")] {
            let mut case = arguments.clone();
            case[member] = serde_json::json!(value);
            assert!(restore(case).is_err(), "accepted {member}");
        }
    }

    #[test]
    fn a_requested_restore_is_an_object_that_names_only_its_action() {
        assert_eq!(
            serde_json::to_value(BrokerLogFilterRequested::Restore).unwrap(),
            serde_json::json!({"action": "restore"})
        );
        assert_eq!(
            serde_json::to_value(BrokerLogFilterRequested::Set {
                logger: "rocketmq_broker::processor".to_owned(),
                level: BrokerLogLevel::Debug,
                ttl_seconds: 300,
            })
            .unwrap(),
            serde_json::json!({
                "action": "set",
                "logger": "rocketmq_broker::processor",
                "level": "DEBUG",
                "ttl_seconds": 300
            })
        );
    }
}
