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
use super::ProxyImageCanaryClient;
use super::ProxyImageCanaryRestore;
use super::ProxyImageCanaryState;
use super::ProxyImageCanaryWrite;
use crate::ExecutionAgentError;

/// Exact parameters accepted by `proxy.rollout_image_canary.v1`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyImageCanaryParameters {
    pub namespace: String,
    pub workload: String,
    pub container: String,
    pub expected_generation: u64,
    pub image_digest: String,
    pub canary_replicas: u32,
}

#[derive(Serialize)]
struct CanaryPrecondition<'a> {
    schema_version: &'static str,
    action: ExecutionAction,
    target: &'a str,
    parameters: &'a ProxyImageCanaryParameters,
    live_state: &'a ProxyImageCanaryState,
}

/// Digest-only, single-replica Proxy canary rollout handler.
#[derive(Clone)]
pub struct ProxyImageCanaryHandler<C> {
    client: Arc<C>,
}

impl<C> ProxyImageCanaryHandler<C>
where
    C: ProxyImageCanaryClient,
{
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> AgentActionHandler for ProxyImageCanaryHandler<C>
where
    C: ProxyImageCanaryClient + 'static,
{
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            let mut reasons = validate_parameters(&parameters);
            let state = self
                .client
                .proxy_image_canary_state(&parameters.namespace, &parameters.workload, &parameters.container)
                .await?;
            if state.generation != parameters.expected_generation {
                reasons.push("proxy_workload_generation_changed".to_owned());
            }
            if state.observed_generation != state.generation {
                reasons.push("proxy_rollout_already_in_progress".to_owned());
            }
            if !state.pdb_healthy {
                reasons.push("proxy_pdb_not_healthy".to_owned());
            }
            if !state.slo_healthy {
                reasons.push("proxy_slo_not_healthy".to_owned());
            }
            if state.image_digest == parameters.image_digest {
                reasons.push("proxy_image_canary_has_no_effect".to_owned());
            }
            let precondition_hash = canonical_precondition_hash(&CanaryPrecondition {
                schema_version: "rocketmq-sre.proxy-image-canary-precondition.v1",
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
                        "canary_generation_observed".to_owned(),
                        state.generation > parameters.expected_generation
                            && state.observed_generation == state.generation
                            && state.image_digest == parameters.image_digest,
                    ),
                    (
                        "canary_ready".to_owned(),
                        state.ready_canary_replicas == 1 && state.slo_healthy,
                    ),
                    ("old_replicas_unchanged".to_owned(), state.old_replicas_unchanged),
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
            self.client
                .rollout_proxy_image_canary(&ProxyImageCanaryWrite {
                    namespace: parameters.namespace,
                    workload: parameters.workload,
                    container: parameters.container,
                    expected_generation: parameters.expected_generation,
                    image_digest: parameters.image_digest,
                    canary_replicas: 1,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "proxy_image_canary_started".to_owned(),
                sanitized_summary: "one Proxy canary was started with an immutable image digest".to_owned(),
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
                .proxy_image_canary_state(&parameters.namespace, &parameters.workload, &parameters.container)
                .await?;
            let effect_state = if state.generation > parameters.expected_generation
                && state.observed_generation == state.generation
                && state.image_digest == parameters.image_digest
                && state.ready_canary_replicas == 1
                && state.old_replicas_unchanged
                && state.slo_healthy
                && operation_id
                    .as_deref()
                    .is_some_and(|expected| state.last_operation_id.as_deref() == Some(expected))
            {
                ReconcileEffectState::Applied
            } else if state.generation == parameters.expected_generation
                && state.image_digest != parameters.image_digest
            {
                ReconcileEffectState::NotApplied
            } else {
                ReconcileEffectState::Unknown
            };
            Ok(ReconcileEffectResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: effect_state,
                outcome_code: match effect_state {
                    ReconcileEffectState::Applied => "proxy_image_canary_verified",
                    ReconcileEffectState::NotApplied => "proxy_image_canary_absent",
                    ReconcileEffectState::Failed => "proxy_image_canary_failed",
                    ReconcileEffectState::Unknown => "proxy_image_canary_unknown",
                }
                .to_owned(),
                sanitized_summary: "Proxy canary generation, readiness, old replicas, and SLO were reconciled"
                    .to_owned(),
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
                .restore_proxy_image(&ProxyImageCanaryRestore {
                    namespace: parameters.namespace,
                    workload: parameters.workload,
                    container: parameters.container,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "proxy_image_canary_rolled_back".to_owned(),
                sanitized_summary: "Proxy image was restored from the durable rollout snapshot".to_owned(),
            })
        })
    }
}

fn require_action(action: ExecutionAction) -> Result<(), ExecutionAgentError> {
    if action == ExecutionAction::ProxyRolloutImageCanary {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn parameters(value: &serde_json::Value) -> Result<ProxyImageCanaryParameters, ExecutionAgentError> {
    serde_json::from_value(value.clone()).map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn validate_parameters(parameters: &ProxyImageCanaryParameters) -> Vec<String> {
    let mut reasons = Vec::new();
    for (value, reason) in [
        (&parameters.namespace, "namespace_invalid"),
        (&parameters.workload, "workload_invalid"),
        (&parameters.container, "container_invalid"),
    ] {
        if value.is_empty() || value.len() > 128 {
            reasons.push(reason.to_owned());
        }
    }
    if parameters.expected_generation == 0 {
        reasons.push("expected_generation_invalid".to_owned());
    }
    if parameters.canary_replicas != 1 {
        reasons.push("canary_replicas_must_equal_one".to_owned());
    }
    if !is_sha256_digest(&parameters.image_digest) {
        reasons.push("image_reference_must_be_sha256_digest".to_owned());
    }
    reasons
}

fn validate_for_mutation(parameters: &ProxyImageCanaryParameters) -> Result<(), ExecutionAgentError> {
    if validate_parameters(parameters).is_empty() {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn is_sha256_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

#[cfg(test)]
#[path = "proxy_image_canary_tests.rs"]
mod tests;
