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

use std::sync::Arc;

use chrono::Utc;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::AgentStepRequest;
use rocketmq_sre_contracts::EXECUTION_AGENT_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ReconcileEffectResponse;
use rocketmq_sre_contracts::ReconcileEffectState;
use rocketmq_sre_contracts::canonical_precondition_hash;
use serde::Deserialize;
use serde::Serialize;

use super::AdminCoreDriver;
use super::AgentActionHandler;
use super::DriverDispatchOutcome;
use super::DriverFuture;
use super::TopicConfigPatch;
use super::TopicConfigPatchApplyOutcome;
use super::TopicConfigPatchClient;
use super::TopicConfigPatchRestore;
use super::TopicConfigPatchState;
use super::TopicConfigPatchWrite;
use crate::ExecutionAgentError;

/// Exact parameters accepted by `topic.config.patch_allowlisted.v1`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicConfigPatchParameters {
    pub topic: String,
    pub expected_version: u64,
    pub patch: TopicConfigPatch,
}

#[derive(Serialize)]
struct TopicPatchPrecondition<'a> {
    schema_version: &'static str,
    action: ExecutionAction,
    target: &'a str,
    parameters: &'a TopicConfigPatchParameters,
    live_state: &'a TopicConfigPatchState,
}

/// Version-CAS Topic patch handler with no delete or permission API.
#[derive(Clone)]
pub struct TopicConfigPatchHandler<C> {
    client: Arc<C>,
}

impl<C> TopicConfigPatchHandler<C>
where
    C: TopicConfigPatchClient,
{
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> AgentActionHandler for TopicConfigPatchHandler<C>
where
    C: TopicConfigPatchClient + 'static,
{
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            let mut reasons = validate_parameters(&parameters);
            let state = self.client.topic_config_patch_state(&parameters.topic).await?;
            if state.version != parameters.expected_version {
                reasons.push("topic_config_version_changed".to_owned());
            }
            if !state.configuration_consistent {
                reasons.push("topic_config_inconsistent_across_brokers".to_owned());
            }
            if patch_matches(&parameters.patch, &state.values) {
                reasons.push("topic_config_patch_has_no_effect".to_owned());
            }
            let precondition_hash = canonical_precondition_hash(&TopicPatchPrecondition {
                schema_version: "rocketmq-sre.topic-config-patch-precondition.v1",
                action: request.action,
                target: &request.target,
                parameters: &parameters,
                live_state: &state,
            })
            .map_err(|_| ExecutionAgentError::InvalidRequest)?;
            Ok(AgentReadResult {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                action: request.action,
                target: request.target.clone(),
                precondition_hash,
                ready: reasons.is_empty(),
                reason_codes: reasons,
                resource_conditions: [
                    (
                        "topic_version_incremented".to_owned(),
                        state.version > parameters.expected_version,
                    ),
                    (
                        "patch_visible".to_owned(),
                        state.configuration_consistent && patch_matches(&parameters.patch, &state.values),
                    ),
                ]
                .into_iter()
                .collect(),
                observed_at: Utc::now(),
            })
        })
    }

    fn dispatch<'a>(
        &'a self,
        request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            validate_for_mutation(&parameters)?;
            let outcome = self
                .client
                .patch_topic_config(&TopicConfigPatchWrite {
                    topic: parameters.topic,
                    expected_version: parameters.expected_version,
                    patch: parameters.patch,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(dispatch_outcome(operation_id, outcome, false))
        })
    }

    fn reconcile<'a>(
        &'a self,
        request: &'a AgentReadRequest,
        operation_id: Option<&str>,
    ) -> DriverFuture<'a, ReconcileEffectResponse> {
        let operation_id = operation_id.map(str::to_owned);
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            validate_for_mutation(&parameters)?;
            let state = self.client.topic_config_patch_state(&parameters.topic).await?;
            let effect_state = if state.version > parameters.expected_version
                && state.configuration_consistent
                && patch_matches(&parameters.patch, &state.values)
                && operation_id
                    .as_deref()
                    .is_some_and(|expected| state.last_operation_id.as_deref() == Some(expected))
            {
                ReconcileEffectState::Applied
            } else if state.version == parameters.expected_version
                && state.configuration_consistent
                && !patch_matches(&parameters.patch, &state.values)
            {
                ReconcileEffectState::NotApplied
            } else {
                ReconcileEffectState::Unknown
            };
            Ok(ReconcileEffectResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: effect_state,
                outcome_code: match effect_state {
                    ReconcileEffectState::Applied => "topic_config_patch_verified",
                    ReconcileEffectState::NotApplied => "topic_config_patch_absent",
                    ReconcileEffectState::Failed => "topic_config_patch_failed",
                    ReconcileEffectState::Unknown => "topic_config_patch_unknown",
                }
                .to_owned(),
                sanitized_summary: "Topic version and allowlisted fields were reconciled".to_owned(),
                observed_at: Utc::now(),
            })
        })
    }

    fn compensate<'a>(
        &'a self,
        request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            validate_for_mutation(&parameters)?;
            let outcome = self
                .client
                .restore_topic_config(&TopicConfigPatchRestore {
                    topic: parameters.topic,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(dispatch_outcome(operation_id, outcome, true))
        })
    }
}

impl<C> AdminCoreDriver for TopicConfigPatchHandler<C> where C: TopicConfigPatchClient + 'static {}

fn dispatch_outcome(
    operation_id: &str,
    outcome: TopicConfigPatchApplyOutcome,
    compensation: bool,
) -> DriverDispatchOutcome {
    match outcome {
        TopicConfigPatchApplyOutcome::Applied { .. } => DriverDispatchOutcome {
            operation_id: operation_id.to_owned(),
            outcome_code: if compensation {
                "topic_config_inverse_patch_applied"
            } else {
                "topic_config_patch_applied"
            }
            .to_owned(),
            sanitized_summary: if compensation {
                "allowlisted Topic fields restored with the latest version CAS"
            } else {
                "allowlisted Topic fields updated with version CAS"
            }
            .to_owned(),
        },
        TopicConfigPatchApplyOutcome::VersionConflict { .. } => DriverDispatchOutcome {
            operation_id: operation_id.to_owned(),
            outcome_code: if compensation {
                "topic_config_rollback_version_conflict"
            } else {
                "topic_config_version_conflict"
            }
            .to_owned(),
            sanitized_summary: "Topic configuration version changed; no overwrite was attempted".to_owned(),
        },
    }
}

fn require_action(action: ExecutionAction) -> Result<(), ExecutionAgentError> {
    if action == ExecutionAction::TopicConfigPatchAllowlisted {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn parameters(value: &serde_json::Value) -> Result<TopicConfigPatchParameters, ExecutionAgentError> {
    serde_json::from_value(value.clone()).map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn validate_parameters(parameters: &TopicConfigPatchParameters) -> Vec<String> {
    let mut reasons = Vec::new();
    if parameters.topic.is_empty() || parameters.topic.len() > 255 {
        reasons.push("topic_name_invalid".to_owned());
    }
    if parameters.patch.is_empty() {
        reasons.push("topic_config_patch_empty".to_owned());
    }
    if parameters
        .patch
        .read_queue_nums
        .is_some_and(|value| !(1..=128).contains(&value))
        || parameters
            .patch
            .write_queue_nums
            .is_some_and(|value| !(1..=128).contains(&value))
    {
        reasons.push("topic_config_value_out_of_range".to_owned());
    }
    reasons
}

fn validate_for_mutation(parameters: &TopicConfigPatchParameters) -> Result<(), ExecutionAgentError> {
    if validate_parameters(parameters).is_empty() {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn patch_matches(patch: &TopicConfigPatch, state: &TopicConfigPatch) -> bool {
    patch
        .read_queue_nums
        .is_none_or(|value| state.read_queue_nums == Some(value))
        && patch
            .write_queue_nums
            .is_none_or(|value| state.write_queue_nums == Some(value))
        && patch.order.is_none_or(|value| state.order == Some(value))
}

#[cfg(test)]
#[path = "topic_config_patch_tests.rs"]
mod tests;
