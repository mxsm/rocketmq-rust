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

use super::AgentActionHandler;
use super::DriverDispatchOutcome;
use super::DriverFuture;
use super::SubscriptionGroupPatch;
use super::SubscriptionGroupPatchApplyOutcome;
use super::SubscriptionGroupPatchClient;
use super::SubscriptionGroupPatchRestore;
use super::SubscriptionGroupPatchState;
use super::SubscriptionGroupPatchWrite;
use crate::ExecutionAgentError;

/// Exact parameters accepted by `subscription_group.patch_allowlisted.v1`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionGroupPatchParameters {
    pub group: String,
    pub expected_version: u64,
    pub patch: SubscriptionGroupPatch,
}

#[derive(Serialize)]
struct SubscriptionGroupPrecondition<'a> {
    schema_version: &'static str,
    action: ExecutionAction,
    target: &'a str,
    parameters: &'a SubscriptionGroupPatchParameters,
    live_state: &'a SubscriptionGroupPatchState,
}

/// Version-CAS Subscription Group handler without delete or permission APIs.
#[derive(Clone)]
pub struct SubscriptionGroupPatchHandler<C> {
    client: Arc<C>,
}

impl<C> SubscriptionGroupPatchHandler<C>
where
    C: SubscriptionGroupPatchClient,
{
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> AgentActionHandler for SubscriptionGroupPatchHandler<C>
where
    C: SubscriptionGroupPatchClient + 'static,
{
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            let mut reasons = validate_parameters(&parameters);
            let state = self.client.subscription_group_patch_state(&parameters.group).await?;
            if state.version != parameters.expected_version {
                reasons.push("subscription_group_version_changed".to_owned());
            }
            if !state.retry_semantics_known {
                reasons.push("retry_semantics_unknown".to_owned());
            }
            if !state.permissions_unchanged {
                reasons.push("subscription_group_permissions_changed".to_owned());
            }
            if patch_matches(&parameters.patch, &state.values) {
                reasons.push("subscription_group_patch_has_no_effect".to_owned());
            }
            let precondition_hash = canonical_precondition_hash(&SubscriptionGroupPrecondition {
                schema_version: "rocketmq-sre.subscription-group-patch-precondition.v1",
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
                        "subscription_group_version_incremented".to_owned(),
                        state.version > parameters.expected_version,
                    ),
                    (
                        "patch_visible".to_owned(),
                        state.permissions_unchanged && patch_matches(&parameters.patch, &state.values),
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
                .patch_subscription_group(&SubscriptionGroupPatchWrite {
                    group: parameters.group,
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
            let state = self.client.subscription_group_patch_state(&parameters.group).await?;
            let effect_state = if state.version > parameters.expected_version
                && state.retry_semantics_known
                && state.permissions_unchanged
                && patch_matches(&parameters.patch, &state.values)
                && operation_id
                    .as_deref()
                    .is_some_and(|expected| state.last_operation_id.as_deref() == Some(expected))
            {
                ReconcileEffectState::Applied
            } else if state.version == parameters.expected_version
                && state.permissions_unchanged
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
                    ReconcileEffectState::Applied => "subscription_group_patch_verified",
                    ReconcileEffectState::NotApplied => "subscription_group_patch_absent",
                    ReconcileEffectState::Failed => "subscription_group_patch_failed",
                    ReconcileEffectState::Unknown => "subscription_group_patch_unknown",
                }
                .to_owned(),
                sanitized_summary: "Subscription Group version and allowlisted retry fields were reconciled".to_owned(),
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
                .restore_subscription_group(&SubscriptionGroupPatchRestore {
                    group: parameters.group,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(dispatch_outcome(operation_id, outcome, true))
        })
    }
}

fn dispatch_outcome(
    operation_id: &str,
    outcome: SubscriptionGroupPatchApplyOutcome,
    compensation: bool,
) -> DriverDispatchOutcome {
    match outcome {
        SubscriptionGroupPatchApplyOutcome::Applied { .. } => DriverDispatchOutcome {
            operation_id: operation_id.to_owned(),
            outcome_code: if compensation {
                "subscription_group_inverse_patch_applied"
            } else {
                "subscription_group_patch_applied"
            }
            .to_owned(),
            sanitized_summary: if compensation {
                "Subscription Group retry fields restored with version CAS"
            } else {
                "Subscription Group retry fields updated with version CAS"
            }
            .to_owned(),
        },
        SubscriptionGroupPatchApplyOutcome::VersionConflict { .. } => DriverDispatchOutcome {
            operation_id: operation_id.to_owned(),
            outcome_code: if compensation {
                "subscription_group_rollback_version_conflict"
            } else {
                "subscription_group_version_conflict"
            }
            .to_owned(),
            sanitized_summary: "Subscription Group version changed; no overwrite was attempted".to_owned(),
        },
    }
}

fn require_action(action: ExecutionAction) -> Result<(), ExecutionAgentError> {
    if action == ExecutionAction::SubscriptionGroupPatchAllowlisted {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn parameters(value: &serde_json::Value) -> Result<SubscriptionGroupPatchParameters, ExecutionAgentError> {
    serde_json::from_value(value.clone()).map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn validate_parameters(parameters: &SubscriptionGroupPatchParameters) -> Vec<String> {
    let mut reasons = Vec::new();
    if parameters.group.is_empty() || parameters.group.len() > 255 {
        reasons.push("subscription_group_name_invalid".to_owned());
    }
    if parameters.patch.is_empty() {
        reasons.push("subscription_group_patch_empty".to_owned());
    }
    if parameters
        .patch
        .retry_max_times
        .is_some_and(|value| !(1..=16).contains(&value))
        || parameters
            .patch
            .retry_queue_nums
            .is_some_and(|value| !(1..=8).contains(&value))
        || parameters
            .patch
            .consume_timeout_minutes
            .is_some_and(|value| !(1..=1440).contains(&value))
    {
        reasons.push("subscription_group_value_out_of_range".to_owned());
    }
    reasons
}

fn validate_for_mutation(parameters: &SubscriptionGroupPatchParameters) -> Result<(), ExecutionAgentError> {
    if validate_parameters(parameters).is_empty() {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn patch_matches(patch: &SubscriptionGroupPatch, state: &SubscriptionGroupPatch) -> bool {
    patch
        .retry_max_times
        .is_none_or(|value| state.retry_max_times == Some(value))
        && patch
            .retry_queue_nums
            .is_none_or(|value| state.retry_queue_nums == Some(value))
        && patch
            .consume_timeout_minutes
            .is_none_or(|value| state.consume_timeout_minutes == Some(value))
}

#[cfg(test)]
#[path = "subscription_group_patch_tests.rs"]
mod tests;
