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
use super::ProxyScaleClient;
use super::ProxyScaleOutOneWrite;
use super::ProxyScaleRestore;
use super::ProxyScaleState;
use crate::ExecutionAgentError;

const MAX_ORIGINAL_REPLICAS: u32 = 999;

/// Exact parameters accepted by `proxy.scale_out_one.v1`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyScaleOutOneParameters {
    pub namespace: String,
    pub workload: String,
    pub expected_replicas: u32,
}

#[derive(Serialize)]
struct ScalePrecondition<'a> {
    schema_version: &'static str,
    action: ExecutionAction,
    target: &'a str,
    parameters: &'a ProxyScaleOutOneParameters,
    live_state: &'a ProxyScaleState,
}

/// Typed one-replica Proxy scale-out with a closed compensation path.
#[derive(Clone)]
pub struct ProxyScaleOutOneHandler<C> {
    client: Arc<C>,
}

impl<C> ProxyScaleOutOneHandler<C>
where
    C: ProxyScaleClient,
{
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> AgentActionHandler for ProxyScaleOutOneHandler<C>
where
    C: ProxyScaleClient + 'static,
{
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            let mut reasons = validate_parameters(&parameters);
            let state = self
                .client
                .proxy_scale_state(&parameters.namespace, &parameters.workload)
                .await?;
            if state.desired_replicas != parameters.expected_replicas {
                reasons.push("desired_replicas_changed".to_owned());
            }
            if state.ready_replicas != state.desired_replicas || state.unavailable_replicas != 0 {
                reasons.push("proxy_workload_not_fully_ready".to_owned());
            }
            if !state.quota_available {
                reasons.push("namespace_quota_unavailable".to_owned());
            }
            if !state.capacity_available {
                reasons.push("cluster_capacity_unavailable".to_owned());
            }
            if !state.pdb_healthy {
                reasons.push("proxy_pdb_not_healthy".to_owned());
            }
            let precondition_hash = canonical_precondition_hash(&ScalePrecondition {
                schema_version: "rocketmq-sre.proxy-scale-precondition.v1",
                action: request.action,
                target: &request.target,
                parameters: &parameters,
                live_state: &state,
            })
            .map_err(|_| ExecutionAgentError::InvalidRequest)?;
            let target_replicas = parameters.expected_replicas.saturating_add(1);
            Ok(AgentReadResult {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                action: request.action,
                target: request.target.clone(),
                precondition_hash,
                ready: reasons.is_empty(),
                reason_codes: reasons,
                resource_conditions: [
                    (
                        "desired_replicas_plus_one".to_owned(),
                        state.desired_replicas == target_replicas,
                    ),
                    (
                        "new_replica_ready".to_owned(),
                        state.ready_replicas == target_replicas && state.unavailable_replicas == 0,
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
            let target_replicas = parameters
                .expected_replicas
                .checked_add(1)
                .ok_or(ExecutionAgentError::InvalidRequest)?;
            self.client
                .scale_out_one(&ProxyScaleOutOneWrite {
                    namespace: parameters.namespace,
                    workload: parameters.workload,
                    expected_replicas: parameters.expected_replicas,
                    target_replicas,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "proxy_scaled_out_one".to_owned(),
                sanitized_summary: "Proxy workload desired replicas increased by exactly one".to_owned(),
            })
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
            let state = self
                .client
                .proxy_scale_state(&parameters.namespace, &parameters.workload)
                .await?;
            let target = parameters.expected_replicas + 1;
            let effect_state = if state.desired_replicas == target
                && state.ready_replicas == target
                && state.unavailable_replicas == 0
                && operation_id
                    .as_deref()
                    .is_some_and(|expected| state.last_operation_id.as_deref() == Some(expected))
            {
                ReconcileEffectState::Applied
            } else if state.desired_replicas == parameters.expected_replicas
                && state.ready_replicas == parameters.expected_replicas
            {
                ReconcileEffectState::NotApplied
            } else {
                ReconcileEffectState::Unknown
            };
            Ok(ReconcileEffectResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: effect_state,
                outcome_code: match effect_state {
                    ReconcileEffectState::Applied => "proxy_scale_effect_verified",
                    ReconcileEffectState::NotApplied => "proxy_scale_effect_absent",
                    ReconcileEffectState::Failed => "proxy_scale_effect_failed",
                    ReconcileEffectState::Unknown => "proxy_scale_effect_unknown",
                }
                .to_owned(),
                sanitized_summary: "Proxy replica state reconciled through the typed Kubernetes client".to_owned(),
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
            self.client
                .restore_proxy_replicas(&ProxyScaleRestore {
                    namespace: parameters.namespace,
                    workload: parameters.workload,
                    original_replicas: parameters.expected_replicas,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "proxy_replicas_restored".to_owned(),
                sanitized_summary: "Proxy workload restored to the recorded original replica count".to_owned(),
            })
        })
    }
}

fn require_action(action: ExecutionAction) -> Result<(), ExecutionAgentError> {
    if action == ExecutionAction::ProxyScaleOutOne {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn parameters(value: &serde_json::Value) -> Result<ProxyScaleOutOneParameters, ExecutionAgentError> {
    serde_json::from_value(value.clone()).map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn validate_parameters(parameters: &ProxyScaleOutOneParameters) -> Vec<String> {
    let mut reasons = Vec::new();
    if parameters.namespace.is_empty() || parameters.namespace.len() > 128 {
        reasons.push("namespace_invalid".to_owned());
    }
    if parameters.workload.is_empty() || parameters.workload.len() > 128 {
        reasons.push("workload_invalid".to_owned());
    }
    if !(1..=MAX_ORIGINAL_REPLICAS).contains(&parameters.expected_replicas) {
        reasons.push("expected_replicas_out_of_range".to_owned());
    }
    reasons
}

fn validate_for_mutation(parameters: &ProxyScaleOutOneParameters) -> Result<(), ExecutionAgentError> {
    if validate_parameters(parameters).is_empty() {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

#[cfg(test)]
#[path = "proxy_scale_out_one_tests.rs"]
mod tests;
