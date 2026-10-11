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

//! `rocketmq_get_mutation_result`: how a mutation call of the caller ended.
//!
//! The Tool reads what the audit trail remembers and nothing else. It opens no session, sends no
//! RPC, and never returns who made a call or why.

use std::borrow::Cow;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use super::nullable_schema;
use super::validate_common;
use super::MutationMode;
use super::MutationStatus;
use crate::audit::AuditMode;
use crate::audit::AuditResult;
use crate::audit::AuditTarget;
use crate::audit::InvocationState;
use crate::audit::RecordedInvocation;
use crate::error::ControlError;
use crate::error::ControlErrorCode;
use crate::model::ControlOperation;

pub const GET_MUTATION_RESULT_TOOL: &str = "rocketmq_get_mutation_result";
pub const MUTATION_LOOKUP_SCHEMA_VERSION: &str = "rocketmq-mcp-mutation-result.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MutationLookupSchemaVersion {
    #[serde(rename = "rocketmq-mcp-mutation-result.v1")]
    V1,
}

impl JsonSchema for MutationLookupSchemaVersion {
    fn schema_name() -> Cow<'static, str> {
        "MutationLookupSchemaVersion".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type": "string", "const": MUTATION_LOOKUP_SCHEMA_VERSION})
    }
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GetMutationResultArgs {
    /// Version of this argument schema. Always `rocketmq-mcp-control.arguments.v1`.
    #[schemars(regex(pattern = "^rocketmq-mcp-control\\.arguments\\.v1$"))]
    pub schema_version: String,
    /// Logical cluster name the mutation was called with, never a NameServer or Broker address. Both the
    /// caller and the server policy must allow it.
    #[schemars(length(min = 1, max = 64), regex(pattern = "^[a-zA-Z0-9_-]+$"))]
    pub cluster: String,
    /// The `invocation_id` of the mutation response. Give it, `request_key`, or both; with both, the
    /// invocation must also have carried that key.
    #[serde(default)]
    #[schemars(range(min = 1))]
    pub invocation_id: Option<u64>,
    /// The `request_key` the mutation was called with: 8 to 64 letters, digits and `._:-`. It selects the
    /// newest of your calls that carried it, which is how to find a call whose response never arrived.
    #[serde(default)]
    #[schemars(length(min = 8, max = 64), regex(pattern = "^[a-zA-Z0-9._:-]+$"))]
    pub request_key: Option<String>,
}

impl std::fmt::Debug for GetMutationResultArgs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A request key is caller data: it is hashed for the lookup and never logged.
        formatter
            .debug_struct("GetMutationResultArgs")
            .field("cluster", &self.cluster)
            .field("invocation_id", &self.invocation_id)
            .field("request_key_given", &self.request_key.is_some())
            .finish()
    }
}

impl GetMutationResultArgs {
    /// Checks the schema version, the shape of the request key, and that the call selects an
    /// invocation at all.
    ///
    /// # Errors
    ///
    /// Returns `invalid_argument` otherwise.
    pub fn validate(&self) -> Result<(), ControlError> {
        // A lookup plans nothing and writes nothing, so it is validated like a dry run.
        validate_common(&self.schema_version, true, false, None, self.request_key.as_deref())?;
        if self.invocation_id == Some(0) || (self.invocation_id.is_none() && self.request_key.is_none()) {
            return Err(ControlError::invalid_argument());
        }
        Ok(())
    }
}

/// What the server knows about the selected invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MutationResultState {
    /// The mutation ended; `status` says how.
    Finished,
    /// The mutation is still running. Ask again later.
    InProgress,
    /// The server stopped while the mutation was running, so its outcome was never recorded. The
    /// cluster may or may not have changed: read its current state before repeating the call.
    Interrupted,
    /// None of your recent calls on this cluster matches. The call may never have started, or it is
    /// older than what the server remembers.
    NotFound,
}

impl MutationResultState {
    /// The wire name of the state, which also labels the lookup in logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Finished => "finished",
            Self::InProgress => "in_progress",
            Self::Interrupted => "interrupted",
            Self::NotFound => "not_found",
        }
    }
}

/// The logical object of the mutation: names only, never addresses.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MutationResultTarget {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    /// The one Broker of a Broker configuration patch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker: Option<String>,
    /// How many Brokers a Topic or Consumer Group upsert selected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_count: Option<u32>,
    /// Their sorted names, when the list is short enough to be kept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_names: Option<Vec<String>>,
}

impl From<AuditTarget> for MutationResultTarget {
    fn from(target: AuditTarget) -> Self {
        let (broker_count, broker_names) = match target.brokers {
            Some(brokers) => (Some(brokers.count), brokers.names),
            None => (None, None),
        };
        Self {
            topic: target.topic,
            consumer_group: target.consumer_group,
            broker: target.broker,
            broker_count,
            broker_names,
        }
    }
}

/// How many targets of the mutation ended in each state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MutationResultCounts {
    /// Written and changed.
    pub applied: u32,
    /// Left as they were: already in the requested state, or only planned by a dry run.
    pub unchanged: u32,
    /// Refused because the state read before the change had changed.
    pub conflict: u32,
    /// Failed for any other reason, including an unverified write.
    pub failed: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MutationResultToolResponse {
    pub schema_version: MutationLookupSchemaVersion,
    pub cluster: String,
    pub state: MutationResultState,
    /// The invocation this answer is about; null when `state` is `not_found`.
    #[schemars(required, schema_with = "nullable_schema::<u64>")]
    pub invocation_id: Option<u64>,
    /// Which mutation it was; null when `state` is `not_found`.
    #[schemars(required, schema_with = "nullable_schema::<ControlOperation>")]
    pub operation: Option<ControlOperation>,
    /// Whether it was a dry run or an execution; null when `state` is `not_found`.
    #[schemars(required, schema_with = "nullable_schema::<MutationMode>")]
    pub mode: Option<MutationMode>,
    /// What it was aimed at; null when `state` is `not_found` or the object was not recorded.
    #[schemars(required, schema_with = "nullable_schema::<MutationResultTarget>")]
    pub target: Option<MutationResultTarget>,
    /// How it ended; null unless `state` is `finished`.
    #[schemars(required, schema_with = "nullable_schema::<MutationStatus>")]
    pub status: Option<MutationStatus>,
    /// The error code of a mutation that did not end as planned or applied; null otherwise.
    #[schemars(required, schema_with = "nullable_schema::<ControlErrorCode>")]
    pub error_code: Option<ControlErrorCode>,
    /// How long it ran, in milliseconds; null unless `state` is `finished`.
    #[schemars(required, schema_with = "nullable_schema::<u64>")]
    pub duration_millis: Option<u64>,
    /// Whether at least one target was written; null when the mutation ended without reporting it, for
    /// example after a timeout. Null does not mean that nothing changed.
    #[schemars(required, schema_with = "nullable_schema::<bool>")]
    pub changed: Option<bool>,
    /// Per-target outcome counts; null when the mutation ended without reporting them.
    #[schemars(required, schema_with = "nullable_schema::<MutationResultCounts>")]
    pub target_results: Option<MutationResultCounts>,
}

impl MutationResultToolResponse {
    /// The member names of the published output schema.
    #[cfg(all(test, feature = "write-tools"))]
    pub(crate) fn json_schema_properties() -> std::collections::BTreeSet<String> {
        let schema = serde_json::to_value(schemars::schema_for!(Self)).unwrap();
        schema["properties"].as_object().unwrap().keys().cloned().collect()
    }

    /// Describes the invocation the trail found on `cluster`, or reports that it found none.
    pub fn new(cluster: &str, recorded: Option<RecordedInvocation>) -> Self {
        let mut response = Self {
            schema_version: MutationLookupSchemaVersion::V1,
            cluster: cluster.to_owned(),
            state: MutationResultState::NotFound,
            invocation_id: None,
            operation: None,
            mode: None,
            target: None,
            status: None,
            error_code: None,
            duration_millis: None,
            changed: None,
            target_results: None,
        };
        let Some(recorded) = recorded else {
            return response;
        };
        response.invocation_id = Some(recorded.invocation_id.get());
        response.operation = Some(recorded.operation);
        response.mode = Some(match recorded.mode {
            AuditMode::DryRun => MutationMode::DryRun,
            AuditMode::Execute => MutationMode::Execute,
        });
        response.target = recorded.target.map(MutationResultTarget::from);
        response.state = match recorded.state {
            InvocationState::InProgress => MutationResultState::InProgress,
            InvocationState::Interrupted => MutationResultState::Interrupted,
            InvocationState::Finished(outcome) => {
                response.status = match outcome.result {
                    AuditResult::Planned => Some(MutationStatus::Planned),
                    AuditResult::Applied => Some(MutationStatus::Applied),
                    AuditResult::Partial => Some(MutationStatus::Partial),
                    AuditResult::Conflict => Some(MutationStatus::Conflict),
                    AuditResult::Failed => Some(MutationStatus::Failed),
                    // A terminal record never says `started`; without a status the answer stays honest.
                    AuditResult::Started => None,
                };
                response.error_code = outcome.error_code;
                response.duration_millis = outcome.duration_millis;
                response.changed = outcome.changed;
                response.target_results = outcome.target_results.map(|results| MutationResultCounts {
                    applied: results.applied,
                    unchanged: results.unchanged,
                    conflict: results.conflict,
                    failed: results.failed,
                });
                MutationResultState::Finished
            }
        };
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION;

    fn arguments(members: serde_json::Value) -> Result<GetMutationResultArgs, ControlError> {
        let mut raw = serde_json::json!({
            "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
            "cluster": "cluster-a",
        });
        for (name, value) in members.as_object().unwrap() {
            raw[name] = value.clone();
        }
        serde_json::from_value::<GetMutationResultArgs>(raw)
            .map_err(|_| ControlError::invalid_argument())
            .and_then(|args| args.validate().map(|()| args))
    }

    #[test]
    fn a_lookup_selects_by_invocation_request_key_or_both() {
        for accepted in [
            serde_json::json!({"invocation_id": 1}),
            serde_json::json!({"invocation_id": u64::MAX}),
            serde_json::json!({"request_key": "release-2026.10:a"}),
            serde_json::json!({"invocation_id": 7, "request_key": "release-2026.10:a"}),
        ] {
            assert!(arguments(accepted.clone()).is_ok(), "{accepted}");
        }
        for rejected in [
            serde_json::json!({}),
            serde_json::json!({"invocation_id": 0}),
            serde_json::json!({"invocation_id": -1}),
            serde_json::json!({"invocation_id": "7"}),
            serde_json::json!({"request_key": "short"}),
            serde_json::json!({"request_key": "has a space in it"}),
            serde_json::json!({"request_key": "k".repeat(65)}),
            serde_json::json!({"invocation_id": 7, "schema_version": "rocketmq-mcp-control.arguments.v2"}),
            // The Tool reads results; it takes none of the arguments that drive a mutation.
            serde_json::json!({"invocation_id": 7, "dry_run": false}),
            serde_json::json!({"invocation_id": 7, "confirm": true}),
            serde_json::json!({"invocation_id": 7, "reason": "ticket 42"}),
            serde_json::json!({"invocation_id": 7, "operator": "alice"}),
            serde_json::json!({"invocation_id": 7, "broker_addr": "10.0.0.1:10911"}),
        ] {
            let error = arguments(rejected.clone()).unwrap_err();
            assert_eq!(error.code(), ControlErrorCode::InvalidArgument, "{rejected}");
        }
        let debug = format!(
            "{:?}",
            arguments(serde_json::json!({"request_key": "release-2026.10:a"})).unwrap()
        );
        assert!(!debug.contains("release-2026"), "{debug}");
    }

    #[test]
    fn an_answer_says_only_what_a_record_says() {
        use crate::audit::AuditBrokerSet;
        use crate::audit::AuditTargetResults;
        use crate::audit::InvocationOutcome;

        let missing = serde_json::to_value(MutationResultToolResponse::new("cluster-a", None)).unwrap();
        assert_eq!(
            missing,
            serde_json::json!({
                "schema_version": MUTATION_LOOKUP_SCHEMA_VERSION,
                "cluster": "cluster-a",
                "state": "not_found",
                "invocation_id": null,
                "operation": null,
                "mode": null,
                "target": null,
                "status": null,
                "error_code": null,
                "duration_millis": null,
                "changed": null,
                "target_results": null,
            })
        );

        let recorded = |state| {
            let invocation_id = serde_json::from_value(serde_json::json!(7)).unwrap();
            RecordedInvocation {
                invocation_id,
                operation: ControlOperation::TopicUpsert,
                mode: AuditMode::Execute,
                target: Some(AuditTarget {
                    topic: Some("orders".to_owned()),
                    brokers: Some(AuditBrokerSet::from_names(&[
                        "broker-b".to_owned(),
                        "broker-a".to_owned(),
                    ])),
                    ..AuditTarget::default()
                }),
                state,
            }
        };
        let finished = MutationResultToolResponse::new(
            "cluster-a",
            Some(recorded(InvocationState::Finished(InvocationOutcome {
                result: AuditResult::Partial,
                error_code: Some(ControlErrorCode::PartialApply),
                duration_millis: Some(12),
                changed: Some(true),
                target_results: Some(AuditTargetResults {
                    applied: 1,
                    unchanged: 0,
                    conflict: 0,
                    failed: 1,
                }),
            }))),
        );
        assert_eq!(
            serde_json::to_value(&finished).unwrap(),
            serde_json::json!({
                "schema_version": MUTATION_LOOKUP_SCHEMA_VERSION,
                "cluster": "cluster-a",
                "state": "finished",
                "invocation_id": 7,
                "operation": "topic_upsert",
                "mode": "execute",
                // Names and a count: the digest of the durable record stays in the record.
                "target": {"topic": "orders", "broker_count": 2, "broker_names": ["broker-a", "broker-b"]},
                "status": "partial",
                "error_code": "partial_apply",
                "duration_millis": 12,
                "changed": true,
                "target_results": {"applied": 1, "unchanged": 0, "conflict": 0, "failed": 1},
            })
        );

        for (state, name) in [
            (InvocationState::InProgress, "in_progress"),
            (InvocationState::Interrupted, "interrupted"),
        ] {
            let answer =
                serde_json::to_value(MutationResultToolResponse::new("cluster-a", Some(recorded(state)))).unwrap();
            assert_eq!(answer["state"], name);
            assert_eq!(answer["invocation_id"], 7);
            for unknown in ["status", "error_code", "duration_millis", "changed", "target_results"] {
                assert!(answer[unknown].is_null(), "{name} claims a {unknown}");
            }
        }
        assert_eq!(MutationResultState::Finished.as_str(), "finished");
        assert_eq!(MutationResultState::NotFound.as_str(), "not_found");
    }
}
