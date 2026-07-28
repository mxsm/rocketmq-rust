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
use super::BrokerConfigPatch;
use super::BrokerConfigPatchApplyOutcome;
use super::BrokerConfigPatchClient;
use super::BrokerConfigPatchRestore;
use super::BrokerConfigPatchState;
use super::BrokerConfigPatchWrite;
use super::DriverDispatchOutcome;
use super::DriverFuture;
use crate::ExecutionAgentError;

/// Exact parameters accepted by `broker.config.patch_allowlisted.v1`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfigPatchParameters {
    pub broker: String,
    pub expected_generation: u64,
    pub patch: BrokerConfigPatch,
}

#[derive(Serialize)]
struct BrokerPatchPrecondition<'a> {
    schema_version: &'static str,
    action: ExecutionAction,
    target: &'a str,
    parameters: &'a BrokerConfigPatchParameters,
    live_state: &'a BrokerConfigPatchState,
}

/// Generation-CAS Broker patch handler for a closed, capability-checked field set.
#[derive(Clone)]
pub struct BrokerConfigPatchHandler<C> {
    client: Arc<C>,
}

impl<C> BrokerConfigPatchHandler<C>
where
    C: BrokerConfigPatchClient,
{
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> AgentActionHandler for BrokerConfigPatchHandler<C>
where
    C: BrokerConfigPatchClient + 'static,
{
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            let mut reasons = validate_parameters(&parameters);
            let state = self.client.broker_config_patch_state(&parameters.broker).await?;
            if state.generation != parameters.expected_generation {
                reasons.push("broker_config_generation_changed".to_owned());
            }
            let requested_fields = parameters.patch.field_names();
            if !requested_fields.is_subset(&state.supported_fields) {
                reasons.push("broker_config_field_unsupported".to_owned());
            }
            if !requested_fields.is_disjoint(&state.restart_required_fields) {
                reasons.push("broker_config_restart_required".to_owned());
            }
            if patch_matches(&parameters.patch, &state.values) {
                reasons.push("broker_config_patch_has_no_effect".to_owned());
            }
            let precondition_hash = canonical_precondition_hash(&BrokerPatchPrecondition {
                schema_version: "rocketmq-sre.broker-config-patch-precondition.v1",
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
                .patch_broker_config(&BrokerConfigPatchWrite {
                    broker_addr: parameters.broker,
                    expected_generation: parameters.expected_generation,
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
            let state = self.client.broker_config_patch_state(&parameters.broker).await?;
            let effect_state = if state.generation > parameters.expected_generation
                && patch_matches(&parameters.patch, &state.values)
                && operation_id
                    .as_deref()
                    .is_some_and(|expected| state.last_operation_id.as_deref() == Some(expected))
            {
                ReconcileEffectState::Applied
            } else if state.generation == parameters.expected_generation
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
                    ReconcileEffectState::Applied => "broker_config_patch_verified",
                    ReconcileEffectState::NotApplied => "broker_config_patch_absent",
                    ReconcileEffectState::Failed => "broker_config_patch_failed",
                    ReconcileEffectState::Unknown => "broker_config_patch_unknown",
                }
                .to_owned(),
                sanitized_summary: "Broker generation and allowlisted fields were reconciled".to_owned(),
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
                .restore_broker_config(&BrokerConfigPatchRestore {
                    broker_addr: parameters.broker,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(dispatch_outcome(operation_id, outcome, true))
        })
    }
}

impl<C> AdminCoreDriver for BrokerConfigPatchHandler<C> where C: BrokerConfigPatchClient + 'static {}

fn dispatch_outcome(
    operation_id: &str,
    outcome: BrokerConfigPatchApplyOutcome,
    compensation: bool,
) -> DriverDispatchOutcome {
    match outcome {
        BrokerConfigPatchApplyOutcome::Applied { .. } => DriverDispatchOutcome {
            operation_id: operation_id.to_owned(),
            outcome_code: if compensation {
                "broker_config_inverse_patch_applied"
            } else {
                "broker_config_patch_applied"
            }
            .to_owned(),
            sanitized_summary: if compensation {
                "allowlisted Broker fields restored with the latest generation CAS"
            } else {
                "allowlisted Broker fields updated with generation CAS"
            }
            .to_owned(),
        },
        BrokerConfigPatchApplyOutcome::GenerationConflict { .. } => DriverDispatchOutcome {
            operation_id: operation_id.to_owned(),
            outcome_code: if compensation {
                "broker_config_rollback_generation_conflict"
            } else {
                "broker_config_generation_conflict"
            }
            .to_owned(),
            sanitized_summary: "Broker generation changed; no overwrite was attempted".to_owned(),
        },
    }
}

fn require_action(action: ExecutionAction) -> Result<(), ExecutionAgentError> {
    if action == ExecutionAction::BrokerConfigPatchAllowlisted {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn parameters(value: &serde_json::Value) -> Result<BrokerConfigPatchParameters, ExecutionAgentError> {
    serde_json::from_value(value.clone()).map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn validate_parameters(parameters: &BrokerConfigPatchParameters) -> Vec<String> {
    let mut reasons = Vec::new();
    if parameters.broker.is_empty() || parameters.broker.len() > 128 {
        reasons.push("broker_address_invalid".to_owned());
    }
    if parameters.expected_generation == 0 {
        reasons.push("broker_generation_invalid".to_owned());
    }
    if parameters.patch.is_empty() {
        reasons.push("broker_config_patch_empty".to_owned());
    }
    if parameters
        .patch
        .send_message_thread_pool_nums
        .is_some_and(|value| !(1..=512).contains(&value))
        || parameters
            .patch
            .pull_message_thread_pool_nums
            .is_some_and(|value| !(1..=512).contains(&value))
        || parameters
            .patch
            .flush_delay_offset_interval_ms
            .is_some_and(|value| !(1_000..=60_000).contains(&value))
        || parameters
            .patch
            .max_client_event_count
            .is_some_and(|value| !(1..=10_000).contains(&value))
    {
        reasons.push("broker_config_value_out_of_range".to_owned());
    }
    reasons
}

fn validate_for_mutation(parameters: &BrokerConfigPatchParameters) -> Result<(), ExecutionAgentError> {
    if validate_parameters(parameters).is_empty() {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn patch_matches(patch: &BrokerConfigPatch, state: &BrokerConfigPatch) -> bool {
    patch
        .send_message_thread_pool_nums
        .is_none_or(|value| state.send_message_thread_pool_nums == Some(value))
        && patch
            .pull_message_thread_pool_nums
            .is_none_or(|value| state.pull_message_thread_pool_nums == Some(value))
        && patch
            .flush_delay_offset_interval_ms
            .is_none_or(|value| state.flush_delay_offset_interval_ms == Some(value))
        && patch
            .max_client_event_count
            .is_none_or(|value| state.max_client_event_count == Some(value))
}

#[cfg(test)]
#[path = "broker_config_patch_tests.rs"]
mod tests;
