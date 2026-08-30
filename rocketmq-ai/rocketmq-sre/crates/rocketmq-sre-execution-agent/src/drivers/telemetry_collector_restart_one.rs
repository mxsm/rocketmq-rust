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
use super::TelemetryCollectorRestartClient;
use super::TelemetryCollectorRestartOneWrite;
use super::TelemetryCollectorRestartState;
use crate::ExecutionAgentError;

/// Exact parameters accepted by `telemetry.collector.restart_one.v1`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryCollectorRestartOneParameters {
    pub namespace: String,
    pub pod: String,
    pub expected_uid: String,
    pub pipeline: String,
}

#[derive(Serialize)]
struct RestartPrecondition<'a> {
    schema_version: &'static str,
    action: ExecutionAction,
    target: &'a str,
    parameters: &'a TelemetryCollectorRestartOneParameters,
    live_state: &'a TelemetryCollectorRestartState,
}

/// R1 handler for one allowlisted OpenTelemetry Collector replacement.
#[derive(Clone)]
pub struct TelemetryCollectorRestartOneHandler<C> {
    client: Arc<C>,
}

impl<C> TelemetryCollectorRestartOneHandler<C>
where
    C: TelemetryCollectorRestartClient,
{
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> AgentActionHandler for TelemetryCollectorRestartOneHandler<C>
where
    C: TelemetryCollectorRestartClient + 'static,
{
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            let mut reasons = validate_parameters(&parameters);
            let state = self
                .client
                .telemetry_collector_restart_state(&parameters.namespace, &parameters.pod, &parameters.pipeline)
                .await?;
            if state.pod_uid != parameters.expected_uid {
                reasons.push("collector_pod_uid_changed".to_owned());
            }
            if !state.pod_ready || !state.deployment_ready {
                reasons.push("collector_not_ready".to_owned());
            }
            if !state.exporter_connected {
                reasons.push("collector_exporter_not_connected".to_owned());
            }
            if !state.queue_healthy {
                reasons.push("collector_queue_unhealthy".to_owned());
            }
            if !state.data_plane_unaffected {
                reasons.push("rocketmq_data_plane_not_proven_healthy".to_owned());
            }
            let precondition_hash = canonical_precondition_hash(&RestartPrecondition {
                schema_version: "rocketmq-sre.telemetry-collector-restart-precondition.v1",
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
                        "replacement_uid_observed".to_owned(),
                        state.replacement_ready && state.pod_uid != parameters.expected_uid,
                    ),
                    ("collector_ready".to_owned(), state.pod_ready && state.deployment_ready),
                    ("exporter_connected".to_owned(), state.exporter_connected),
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
                .restart_one_telemetry_collector(&TelemetryCollectorRestartOneWrite {
                    namespace: parameters.namespace,
                    pod: parameters.pod,
                    expected_uid: parameters.expected_uid,
                    pipeline: parameters.pipeline,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "telemetry_collector_restart_requested".to_owned(),
                sanitized_summary: "one expected telemetry Collector pod entered a typed rolling restart".to_owned(),
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
                .telemetry_collector_restart_state(&parameters.namespace, &parameters.pod, &parameters.pipeline)
                .await?;
            let execution_id = request.execution_id.to_string();
            let plan_step_id = request.plan_step_id.to_string();
            let effect_state = if state.pod_uid != parameters.expected_uid
                && state.replacement_ready
                && state.pod_ready
                && state.deployment_ready
                && state.exporter_connected
                && state.queue_healthy
                && state.data_plane_unaffected
                && operation_id
                    .as_deref()
                    .is_some_and(|expected| state.last_operation_id.as_deref() == Some(expected))
                && state.last_execution_id.as_deref() == Some(execution_id.as_str())
                && state.last_plan_step_id.as_deref() == Some(plan_step_id.as_str())
            {
                ReconcileEffectState::Applied
            } else if state.pod_uid == parameters.expected_uid && state.last_operation_id.is_none() {
                ReconcileEffectState::NotApplied
            } else {
                ReconcileEffectState::Unknown
            };
            Ok(ReconcileEffectResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: effect_state,
                outcome_code: match effect_state {
                    ReconcileEffectState::Applied => "telemetry_collector_restart_verified",
                    ReconcileEffectState::NotApplied => "telemetry_collector_restart_absent",
                    ReconcileEffectState::Failed => "telemetry_collector_restart_failed",
                    ReconcileEffectState::Unknown => "telemetry_collector_restart_unknown",
                }
                .to_owned(),
                sanitized_summary: "Collector replacement, readiness, exporter connection, queue, and data-plane \
                                    isolation were reconciled"
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
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "telemetry_collector_manual_takeover_required".to_owned(),
                sanitized_summary: "a completed Collector replacement cannot be reversed; manual takeover is required"
                    .to_owned(),
            })
        })
    }
}

fn require_action(action: ExecutionAction) -> Result<(), ExecutionAgentError> {
    if action == ExecutionAction::TelemetryCollectorRestartOne {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn parameters(value: &serde_json::Value) -> Result<TelemetryCollectorRestartOneParameters, ExecutionAgentError> {
    serde_json::from_value(value.clone()).map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn validate_parameters(parameters: &TelemetryCollectorRestartOneParameters) -> Vec<String> {
    let mut reasons = Vec::new();
    if parameters.namespace.is_empty() || parameters.namespace.len() > 128 {
        reasons.push("namespace_invalid".to_owned());
    }
    if parameters.pod.is_empty() || parameters.pod.len() > 128 {
        reasons.push("collector_pod_invalid".to_owned());
    }
    if parameters.expected_uid.is_empty() || parameters.expected_uid.len() > 128 {
        reasons.push("collector_expected_uid_invalid".to_owned());
    }
    if !matches!(parameters.pipeline.as_str(), "metrics" | "logs" | "traces" | "combined") {
        reasons.push("collector_pipeline_invalid".to_owned());
    }
    reasons
}

fn validate_for_mutation(parameters: &TelemetryCollectorRestartOneParameters) -> Result<(), ExecutionAgentError> {
    if validate_parameters(parameters).is_empty() {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

#[cfg(test)]
#[path = "telemetry_collector_restart_one_tests.rs"]
mod tests;
