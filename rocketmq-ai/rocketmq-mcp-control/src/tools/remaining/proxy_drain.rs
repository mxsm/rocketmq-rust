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
    default_dry_run, validate_alias, validate_common, validate_operation_id, FailureCode, MutationMode,
    MutationResultSchemaVersion, MutationStatus, VerificationState,
};
use super::nullable_schema;
use super::OperationHolder;
use crate::error::ControlError;
use crate::error::ControlErrorCode;

pub const BEGIN_PROXY_DRAIN_TOOL: &str = "rocketmq_begin_proxy_drain";
pub const CANCEL_PROXY_DRAIN_TOOL: &str = "rocketmq_cancel_proxy_drain";

/// Which of the two drain operations a response answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProxyDrainOperation {
    ProxyDrainBegin,
    ProxyDrainCancel,
}

/// The arguments of `rocketmq_begin_proxy_drain` and of `rocketmq_cancel_proxy_drain`.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProxyDrainArgs {
    /// Version of this argument schema. Always `rocketmq-mcp-control.arguments.v1`.
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    /// Logical cluster name from the server configuration, never a NameServer or Broker address. Both the
    /// caller and the server policy must allow it.
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    /// Alias of the Proxy as the server configuration names it for this cluster; never an address.
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub proxy_name: String,
    /// Identifier of the drain: 8 to 64 letters, digits, `_` and `-`. The call that begins a drain chooses
    /// it and the Proxy keeps it while it drains; a call that cancels the drain must pass the same one.
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
    /// Digest of the plan to execute: the `plan_digest` that a dry run with the same arguments returned. With
    /// it, the server writes only if the plan it reads now is that one, and otherwise ends the call with
    /// `precondition_conflict`. A dry run ignores it; the server policy can require it to execute.
    #[serde(default)]
    #[schemars(length(min = 64, max = 64), regex(pattern = "^[0-9a-f]{64}$"))]
    pub expected_plan_digest: Option<String>,
}

impl std::fmt::Debug for ProxyDrainArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProxyDrainArgs")
            .field("schema_version", &self.schema_version)
            .field("dry_run", &self.dry_run)
            .field("confirm", &self.confirm)
            .finish_non_exhaustive()
    }
}

impl ProxyDrainArgs {
    pub fn validate(&self, configured_default: bool, omitted: bool) -> Result<(), ControlError> {
        validate_common(
            &self.schema_version,
            self.effective_dry_run(configured_default, omitted),
            self.confirm,
            self.reason.as_deref(),
            self.request_key.as_deref(),
            self.expected_plan_digest.as_deref(),
        )?;
        validate_alias(&self.proxy_name)?;
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
#[serde(rename_all = "snake_case")]
pub enum ProxyDrainPhase {
    /// The Proxy admits work.
    Accepting,
    /// The Proxy admits no new work and still has work pending.
    Draining,
    /// The Proxy admits no new work and has none pending, so it can be restarted.
    Drained,
}

/// Work that a Proxy still holds; every member is zero once it is drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProxyDrainPending {
    pub active_connections: u64,
    pub sessions: u64,
    pub receipt_handles: u64,
    pub prepared_transactions: u64,
    pub telemetry_links: u64,
    pub remoting_channels: u64,
    pub telemetry_commands: u64,
    pub rpc_in_flight: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProxyDrainState {
    pub phase: ProxyDrainPhase,
    /// Whether a drain holds the Proxy, and whether the `operation_id` of this call began it.
    pub held_by: OperationHolder,
    pub admission_open: bool,
    pub routing_open: bool,
    pub readiness_published: bool,
    pub zero_pending: bool,
    pub pending: ProxyDrainPending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProxyDrainAction {
    Begin,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProxyDrainRequested {
    pub action: ProxyDrainAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProxyDrainResource {
    pub proxy_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProxyDrainTarget {
    pub proxy_name: String,
    /// State read before the call changed anything; null when the Proxy could not be read.
    #[schemars(required, schema_with = "nullable_schema::<ProxyDrainState>")]
    pub before: Option<ProxyDrainState>,
    pub requested: ProxyDrainRequested,
    /// State read after an execution; null for a dry run and when the Proxy could not be read again.
    #[schemars(required, schema_with = "nullable_schema::<ProxyDrainState>")]
    pub after: Option<ProxyDrainState>,
    /// Whether the Proxy acknowledged the action or already was in the requested state.
    pub applied: bool,
    /// For a dry run, whether executing would change the Proxy; otherwise whether it changed.
    pub changed: bool,
    pub verification: VerificationState,
    #[schemars(required, schema_with = "nullable_schema::<FailureCode>")]
    pub failure: Option<FailureCode>,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProxyDrainToolResponse {
    pub schema_version: MutationResultSchemaVersion,
    pub operation: ProxyDrainOperation,
    pub cluster: String,
    pub mode: MutationMode,
    pub status: MutationStatus,
    #[schemars(required, schema_with = "nullable_schema::<ControlErrorCode>")]
    pub error_code: Option<ControlErrorCode>,
    pub target: ProxyDrainResource,
    /// State before the call, by Proxy alias; empty when the Proxy could not be read.
    pub before: BTreeMap<String, ProxyDrainState>,
    pub requested: ProxyDrainRequested,
    #[schemars(required, schema_with = "nullable_schema::<BTreeMap<String, ProxyDrainState>>")]
    pub after: Option<BTreeMap<String, ProxyDrainState>>,
    pub targets: Vec<ProxyDrainTarget>,
    pub warnings: Vec<String>,
}

impl ProxyDrainToolResponse {
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

    fn arguments() -> serde_json::Value {
        serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
            "proxy_name": "proxy-a",
            "operation_id": "drain-0001",
            "dry_run": true,
            "confirm": false
        })
    }

    fn validated(value: serde_json::Value) -> Result<(), ControlError> {
        serde_json::from_value::<ProxyDrainArgs>(value)
            .map_err(|_| ControlError::invalid_argument())
            .and_then(|args| args.validate(true, false))
    }

    #[test]
    fn drain_tools_never_accept_addresses() {
        validated(arguments()).unwrap();
        for proxy_name in [
            "10.0.0.7",
            "10.0.0.7:8080",
            "proxy-a.example.test",
            "proxy-a.example.test:8080",
            "[2001:db8::1]:8080",
            "https://proxy-a",
            "proxy a",
            "",
        ] {
            let mut case = arguments();
            case["proxy_name"] = serde_json::json!(proxy_name);
            assert!(validated(case).is_err(), "accepted {proxy_name}");
        }
        assert!(validated({
            let mut case = arguments();
            case["proxy_name"] = serde_json::json!("p".repeat(65));
            case
        })
        .is_err());

        // No member takes an address under another name either.
        for member in ["proxy_addr", "proxy_endpoint", "endpoint", "address"] {
            let mut case = arguments();
            case[member] = serde_json::json!("10.0.0.7:8080");
            assert!(validated(case).is_err(), "accepted {member}");
        }
        let schema = serde_json::to_value(schemars::schema_for!(ProxyDrainArgs)).unwrap();
        let members = schema["properties"].as_object().unwrap();
        assert_eq!(
            members.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "cluster",
                "confirm",
                "dry_run",
                // A digest in lower-case hexadecimal, which no address is.
                "expected_plan_digest",
                "operation_id",
                "proxy_name",
                "reason",
                "request_key",
                "schema_version"
            ]
        );
        assert_eq!(schema["additionalProperties"], false);
    }

    #[test]
    fn operation_identifiers_are_closed_and_stay_out_of_debug() {
        for operation_id in ["drain-0001", "INC_42-drain", &"a".repeat(64)] {
            let mut case = arguments();
            case["operation_id"] = serde_json::json!(operation_id);
            validated(case).unwrap();
        }
        for operation_id in [
            "",
            "short",
            "drain 0001",
            "drain.0001",
            "10.0.0.7:8080",
            "drain/0001",
            &"a".repeat(65),
        ] {
            let mut case = arguments();
            case["operation_id"] = serde_json::json!(operation_id);
            assert!(validated(case).is_err(), "accepted {operation_id}");
        }
        let mut missing = arguments();
        missing.as_object_mut().unwrap().remove("operation_id");
        assert!(validated(missing).is_err());

        let args: ProxyDrainArgs = serde_json::from_value(arguments()).unwrap();
        let rendered = format!("{args:?}");
        assert!(!rendered.contains("proxy-a") && !rendered.contains("drain-0001"));
    }

    #[test]
    fn an_execution_needs_confirmation_and_a_reason() {
        let mut execute = arguments();
        execute["dry_run"] = serde_json::json!(false);
        assert_eq!(
            validated(execute.clone()).unwrap_err(),
            ControlError::confirmation_required()
        );
        execute["confirm"] = serde_json::json!(true);
        assert_eq!(
            validated(execute.clone()).unwrap_err(),
            ControlError::invalid_argument()
        );
        execute["reason"] = serde_json::json!("planned proxy restart");
        validated(execute).unwrap();
    }
}
