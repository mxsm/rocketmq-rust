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
use rocketmq_admin_core::core::proxy::ProxyDrainPhase;
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
use super::KubernetesDriver;
use super::ProxyRestartClient;
use super::ProxyRestartOneWrite;
use super::ProxyRestartRestore;
use super::ProxyRestartRestoreOutcome;
use super::ProxyRestartState;
use crate::ExecutionAgentError;

// Keep the mutation deadline below the Agent, Executor, and Control Plane
// request deadlines so an outer transport cannot cancel an in-flight restart.
const DRAIN_TIMEOUT_SECONDS: u32 = 120;

/// Exact parameters accepted by `proxy.restart_one.v1`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyRestartOneParameters {
    pub namespace: String,
    pub pod: String,
    pub expected_uid: String,
}

#[derive(Serialize)]
struct RestartPrecondition<'a> {
    schema_version: &'static str,
    action: ExecutionAction,
    target: &'a str,
    parameters: &'a ProxyRestartOneParameters,
    live_state: &'a ProxyRestartState,
}

/// Conditional R1 restart handler backed by authenticated Proxy drain state.
#[derive(Clone)]
pub struct ProxyRestartOneHandler<C> {
    client: Arc<C>,
}

impl<C> ProxyRestartOneHandler<C>
where
    C: ProxyRestartClient,
{
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> AgentActionHandler for ProxyRestartOneHandler<C>
where
    C: ProxyRestartClient + 'static,
{
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            let mut reasons = validate_parameters(&parameters);
            let state = self
                .client
                .proxy_restart_state(&parameters.namespace, &parameters.pod)
                .await?;
            if !state.drain_supported || state.drain.is_none() {
                reasons.push("authenticated_drain_state_unavailable".to_owned());
            }
            if state.pod_uid != parameters.expected_uid {
                reasons.push("proxy_pod_uid_changed".to_owned());
            }
            if !state.pod_ready {
                reasons.push("proxy_pod_not_ready".to_owned());
            }
            if !state.remaining_replicas_healthy {
                reasons.push("proxy_remaining_replicas_unhealthy".to_owned());
            }
            if let Some(drain) = &state.drain
                && (drain.phase != ProxyDrainPhase::Accepting
                    || !drain.admission_open
                    || !drain.routing_open
                    || !drain.readiness_published
                    || drain.operation_id.is_some())
            {
                reasons.push("proxy_drain_already_active".to_owned());
            }
            if state.active_operation_id.is_some() {
                reasons.push("proxy_restart_operation_not_clear".to_owned());
            }
            let precondition_hash = canonical_precondition_hash(&RestartPrecondition {
                schema_version: "rocketmq-sre.proxy-restart-precondition.v1",
                action: request.action,
                target: &request.target,
                parameters: &parameters,
                live_state: &state,
            })
            .map_err(|_| ExecutionAgentError::InvalidRequest)?;
            let accepting_and_routed = state.drain.as_ref().is_some_and(|drain| {
                drain.phase == ProxyDrainPhase::Accepting
                    && drain.admission_open
                    && drain.routing_open
                    && drain.readiness_published
                    && drain.operation_id.is_none()
            });
            Ok(AgentReadResult {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                action: request.action,
                target: request.target.clone(),
                precondition_hash,
                ready: reasons.is_empty(),
                reason_codes: reasons,
                resource_conditions: [
                    (
                        "replacement_ready".to_owned(),
                        state.replacement_ready && state.pod_ready,
                    ),
                    (
                        "remaining_replicas_healthy".to_owned(),
                        state.remaining_replicas_healthy,
                    ),
                    (
                        "restart_operation_clear".to_owned(),
                        state.active_operation_id.is_none(),
                    ),
                    ("accepting_and_routed".to_owned(), accepting_and_routed),
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
                .restart_one_drained(&ProxyRestartOneWrite {
                    namespace: parameters.namespace,
                    pod: parameters.pod,
                    expected_uid: parameters.expected_uid,
                    drain_timeout_seconds: DRAIN_TIMEOUT_SECONDS,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "proxy_restarted_after_zero_drain".to_owned(),
                sanitized_summary: "one expected Proxy pod was drained to zero and restarted".to_owned(),
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
                .proxy_restart_state(&parameters.namespace, &parameters.pod)
                .await?;
            let accepting = state.drain.as_ref().is_some_and(|drain| {
                drain.phase == ProxyDrainPhase::Accepting
                    && drain.admission_open
                    && drain.routing_open
                    && drain.readiness_published
                    && drain.operation_id.is_none()
            });
            let effect_state = if state.pod_uid != parameters.expected_uid
                && state.pod_ready
                && state.replacement_ready
                && state.synthetic_path_healthy
                && state.slo_healthy
                && accepting
                && operation_id
                    .as_deref()
                    .is_some_and(|expected| state.last_operation_id.as_deref() == Some(expected))
            {
                ReconcileEffectState::Applied
            } else if state.pod_uid == parameters.expected_uid
                && state.pod_ready
                && accepting
                && state.last_operation_id.is_none()
            {
                ReconcileEffectState::NotApplied
            } else {
                ReconcileEffectState::Unknown
            };
            Ok(ReconcileEffectResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: effect_state,
                outcome_code: match effect_state {
                    ReconcileEffectState::Applied => "proxy_restart_verified",
                    ReconcileEffectState::NotApplied => "proxy_restart_absent",
                    ReconcileEffectState::Failed => "proxy_restart_failed",
                    ReconcileEffectState::Unknown => "proxy_restart_unknown",
                }
                .to_owned(),
                sanitized_summary: "Proxy replacement, re-admission, synthetic path, and SLO state were reconciled"
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
            let outcome = self
                .client
                .cancel_restart_and_restore(&ProxyRestartRestore {
                    namespace: parameters.namespace,
                    pod: parameters.pod,
                    expected_uid: parameters.expected_uid,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            let (outcome_code, summary) = match outcome {
                ProxyRestartRestoreOutcome::IngressRestored => (
                    "proxy_restart_cancelled_ingress_restored",
                    "pending Proxy restart was cancelled and ingress state was restored",
                ),
                ProxyRestartRestoreOutcome::ManualTakeoverRequired => (
                    "proxy_restart_manual_takeover_required",
                    "replacement cannot be automatically reversed; manual takeover is required",
                ),
            };
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: outcome_code.to_owned(),
                sanitized_summary: summary.to_owned(),
            })
        })
    }
}

impl<C> KubernetesDriver for ProxyRestartOneHandler<C> where C: ProxyRestartClient + 'static {}

fn require_action(action: ExecutionAction) -> Result<(), ExecutionAgentError> {
    if action == ExecutionAction::ProxyRestartOne {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn parameters(value: &serde_json::Value) -> Result<ProxyRestartOneParameters, ExecutionAgentError> {
    serde_json::from_value(value.clone()).map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn validate_parameters(parameters: &ProxyRestartOneParameters) -> Vec<String> {
    let mut reasons = Vec::new();
    if parameters.namespace.is_empty() || parameters.namespace.len() > 128 {
        reasons.push("namespace_invalid".to_owned());
    }
    if parameters.pod.is_empty() || parameters.pod.len() > 128 {
        reasons.push("proxy_pod_invalid".to_owned());
    }
    if parameters.expected_uid.is_empty() || parameters.expected_uid.len() > 128 {
        reasons.push("proxy_expected_uid_invalid".to_owned());
    }
    reasons
}

fn validate_for_mutation(parameters: &ProxyRestartOneParameters) -> Result<(), ExecutionAgentError> {
    if validate_parameters(parameters).is_empty() {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

#[cfg(test)]
#[path = "proxy_restart_one_tests.rs"]
mod tests;
