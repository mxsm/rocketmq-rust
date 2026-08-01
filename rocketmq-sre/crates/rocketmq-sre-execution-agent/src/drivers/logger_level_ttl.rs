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

use chrono::TimeDelta;
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
use super::LoggerLevelControlClient;
use super::LoggerLevelState;
use super::LoggerLevelTtlRestore;
use super::LoggerLevelTtlWrite;
use crate::ExecutionAgentError;

const MAX_TTL_SECONDS: i64 = 15 * 60;
const MIN_TTL_SECONDS: i64 = 60;
const BROKER_TARGET_PREFIX: &str = "broker/";
const MAX_BROKER_ADDR_BYTES: usize = 512;

/// Exact parameters accepted by `observability.logger_level_ttl.v1`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoggerLevelTtlParameters {
    pub component: String,
    pub logger: String,
    pub level: String,
    pub ttl_seconds: i64,
}

#[derive(Serialize)]
struct LoggerPrecondition<'a> {
    schema_version: &'static str,
    action: ExecutionAction,
    target: &'a str,
    parameters: &'a LoggerLevelTtlParameters,
    live_state: &'a LoggerLevelState,
}

/// Bounded logger-level action. It cannot write arbitrary configuration keys.
#[derive(Clone)]
pub struct LoggerLevelTtlHandler<C> {
    client: Arc<C>,
}

impl<C> LoggerLevelTtlHandler<C>
where
    C: LoggerLevelControlClient,
{
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> AgentActionHandler for LoggerLevelTtlHandler<C>
where
    C: LoggerLevelControlClient + 'static,
{
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            let mut reason_codes = validate_parameters(&parameters);
            let broker_addr = broker_addr(&request.target)?;
            let live_state = self
                .client
                .logger_level_state(&parameters.component, broker_addr, &parameters.logger)
                .await?;
            if live_state.active_operation_id.is_some() {
                reason_codes.push("logger_override_already_active".to_owned());
            }
            let precondition_hash = canonical_precondition_hash(&LoggerPrecondition {
                schema_version: "rocketmq-sre.logger-level-precondition.v1",
                action: request.action,
                target: &request.target,
                parameters: &parameters,
                live_state: &live_state,
            })
            .map_err(|_| ExecutionAgentError::InvalidRequest)?;
            let override_active = live_state.active_operation_id.is_some();
            Ok(AgentReadResult {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                action: request.action,
                target: request.target.clone(),
                precondition_hash,
                ready: reason_codes.is_empty(),
                reason_codes,
                resource_conditions: [
                    (
                        "logger_level_applied".to_owned(),
                        override_active && live_state.level == parameters.level,
                    ),
                    ("ttl_restore_scheduled".to_owned(), override_active),
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
            if !validate_parameters(&parameters).is_empty() {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let broker_addr = broker_addr(&request.target)?.to_owned();
            let expires_at = Utc::now()
                .checked_add_signed(TimeDelta::seconds(parameters.ttl_seconds))
                .ok_or(ExecutionAgentError::InvalidRequest)?;
            self.client
                .set_logger_level_ttl(&LoggerLevelTtlWrite {
                    component: parameters.component,
                    broker_addr,
                    logger: parameters.logger,
                    level: parameters.level,
                    expires_at,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "logger_level_ttl_applied".to_owned(),
                sanitized_summary: "bounded logger level override scheduled for automatic restoration".to_owned(),
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
            if !validate_parameters(&parameters).is_empty() {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let broker_addr = broker_addr(&request.target)?;
            let state = self
                .client
                .logger_level_state(&parameters.component, broker_addr, &parameters.logger)
                .await?;
            let reconciliation_state = match operation_id.as_deref() {
                Some(expected) if state.active_operation_id.as_deref() == Some(expected) => {
                    ReconcileEffectState::Applied
                }
                Some(_) if state.active_operation_id.is_none() && state.level != parameters.level => {
                    ReconcileEffectState::NotApplied
                }
                Some(expected)
                    if state.last_completed_operation_id.as_deref() == Some(expected)
                        && state.level == parameters.level =>
                {
                    ReconcileEffectState::Applied
                }
                _ => ReconcileEffectState::Unknown,
            };
            Ok(ReconcileEffectResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: reconciliation_state,
                outcome_code: match reconciliation_state {
                    ReconcileEffectState::Applied => "logger_level_effect_observed",
                    ReconcileEffectState::NotApplied => "logger_level_effect_absent",
                    ReconcileEffectState::Failed => "logger_level_effect_failed",
                    ReconcileEffectState::Unknown => "logger_level_effect_unknown",
                }
                .to_owned(),
                sanitized_summary: "logger level state was reconciled through the typed configuration client"
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
            if !validate_parameters(&parameters).is_empty() {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let broker_addr = broker_addr(&request.target)?.to_owned();
            self.client
                .restore_logger_level(&LoggerLevelTtlRestore {
                    component: parameters.component,
                    broker_addr,
                    logger: parameters.logger,
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                    operation_id: operation_id.to_owned(),
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "logger_level_restored".to_owned(),
                sanitized_summary: "original logger level restored from the durable typed override record".to_owned(),
            })
        })
    }
}

fn require_action(action: ExecutionAction) -> Result<(), ExecutionAgentError> {
    if action == ExecutionAction::ObservabilityLoggerLevelTtl {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn parameters(value: &serde_json::Value) -> Result<LoggerLevelTtlParameters, ExecutionAgentError> {
    serde_json::from_value(value.clone()).map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn validate_parameters(parameters: &LoggerLevelTtlParameters) -> Vec<String> {
    let mut reasons = Vec::new();
    if !matches!(parameters.level.as_str(), "INFO" | "DEBUG") {
        reasons.push("logger_level_not_allowlisted".to_owned());
    }
    if !(MIN_TTL_SECONDS..=MAX_TTL_SECONDS).contains(&parameters.ttl_seconds) {
        reasons.push("logger_ttl_out_of_range".to_owned());
    }
    if parameters.component != "broker" {
        reasons.push("logger_component_not_executable".to_owned());
    }
    if !parameters.logger.starts_with("rocketmq_broker::") {
        reasons.push("logger_prefix_not_allowlisted".to_owned());
    }
    let logger = parameters.logger.to_ascii_lowercase();
    if [
        "message", "body", "security", "auth", "acl", "secret", "payload", "store",
    ]
    .iter()
    .any(|forbidden| logger.contains(forbidden))
    {
        reasons.push("payload_or_security_logger_forbidden".to_owned());
    }
    reasons
}

fn broker_addr(target: &str) -> Result<&str, ExecutionAgentError> {
    let broker_addr = target
        .strip_prefix(BROKER_TARGET_PREFIX)
        .ok_or(ExecutionAgentError::InvalidRequest)?;
    if broker_addr.is_empty()
        || broker_addr.len() > MAX_BROKER_ADDR_BYTES
        || broker_addr
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(ExecutionAgentError::InvalidRequest);
    }
    Ok(broker_addr)
}

#[cfg(test)]
#[path = "logger_level_ttl_tests.rs"]
mod tests;
