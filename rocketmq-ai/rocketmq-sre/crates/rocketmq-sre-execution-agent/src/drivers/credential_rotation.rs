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
use super::CredentialOverlapRestore;
use super::CredentialOverlapWrite;
use super::CredentialRotationClient;
use super::CredentialRotationState;
use super::DriverDispatchOutcome;
use super::DriverFuture;
use crate::ExecutionAgentError;

/// Exact parameters accepted by `security.credential_rotate_overlap.v1`.
///
/// The candidate field is a secret reference, never credential material. This
/// DTO intentionally does not implement `Debug`.
#[derive(Clone, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialRotationParameters {
    pub credential_set: String,
    pub active_version: String,
    pub candidate_version: String,
    pub candidate_secret_ref: String,
    pub overlap_seconds: u32,
    pub validation_probe_topic: String,
}

#[derive(Serialize)]
struct CredentialPrecondition<'a> {
    schema_version: &'static str,
    action: ExecutionAction,
    target: &'a str,
    credential_set: &'a str,
    active_version: &'a str,
    candidate_version: &'a str,
    candidate_secret_ref_hash: &'a str,
    overlap_seconds: u32,
    validation_probe_topic: &'a str,
    live_state: &'a CredentialRotationState,
}

/// Bounded active-to-retiring credential overlap handler.
#[derive(Clone)]
pub struct CredentialRotationHandler<C> {
    client: Arc<C>,
}

impl<C> CredentialRotationHandler<C>
where
    C: CredentialRotationClient,
{
    #[must_use]
    pub fn new(client: Arc<C>) -> Self {
        Self { client }
    }
}

impl<C> AgentActionHandler for CredentialRotationHandler<C>
where
    C: CredentialRotationClient + 'static,
{
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult> {
        Box::pin(async move {
            require_action(request.action)?;
            let parameters = parameters(&request.parameters)?;
            let mut reasons = validate_parameters(&parameters);
            let state = self
                .client
                .credential_rotation_state(&parameters.credential_set)
                .await?;
            let rotation_applied = state.active_version == parameters.candidate_version
                && state.retiring_version.as_deref() == Some(parameters.active_version.as_str());
            if !rotation_applied && state.active_version != parameters.active_version {
                reasons.push("active_credential_version_changed".to_owned());
            }
            if !rotation_applied && state.retiring_version.is_some() {
                reasons.push("credential_overlap_already_active".to_owned());
            }
            if !state.active_healthy {
                reasons.push("active_credential_unhealthy".to_owned());
            }
            let candidate_secret_ref_hash = canonical_precondition_hash(&parameters.candidate_secret_ref)
                .map_err(|_| ExecutionAgentError::InvalidRequest)?;
            let precondition_hash = canonical_precondition_hash(&CredentialPrecondition {
                schema_version: "rocketmq-sre.credential-overlap-precondition.v1",
                action: request.action,
                target: &request.target,
                credential_set: &parameters.credential_set,
                active_version: &parameters.active_version,
                candidate_version: &parameters.candidate_version,
                candidate_secret_ref_hash: &candidate_secret_ref_hash,
                overlap_seconds: parameters.overlap_seconds,
                validation_probe_topic: &parameters.validation_probe_topic,
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
                        "candidate_active".to_owned(),
                        rotation_applied && state.candidate_probe_healthy,
                    ),
                    ("previous_retiring".to_owned(), rotation_applied),
                    ("overlap_deadline_recorded".to_owned(), state.overlap_deadline.is_some()),
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
                .begin_credential_overlap(&CredentialOverlapWrite {
                    credential_set: parameters.credential_set,
                    active_version: parameters.active_version,
                    candidate_version: parameters.candidate_version,
                    candidate_secret_ref: parameters.candidate_secret_ref,
                    overlap_seconds: parameters.overlap_seconds,
                    validation_probe_topic: parameters.validation_probe_topic,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "credential_overlap_started".to_owned(),
                sanitized_summary: "candidate credential activated with the previous version retained".to_owned(),
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
                .credential_rotation_state(&parameters.credential_set)
                .await?;
            let effect_state = if state.active_version == parameters.candidate_version
                && state.retiring_version.as_deref() == Some(parameters.active_version.as_str())
                && state.candidate_probe_healthy
                && state.overlap_deadline.is_some()
                && operation_id
                    .as_deref()
                    .is_some_and(|expected| state.last_operation_id.as_deref() == Some(expected))
            {
                ReconcileEffectState::Applied
            } else if state.active_version == parameters.active_version && state.retiring_version.is_none() {
                ReconcileEffectState::NotApplied
            } else {
                ReconcileEffectState::Unknown
            };
            Ok(ReconcileEffectResponse {
                schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
                state: effect_state,
                outcome_code: match effect_state {
                    ReconcileEffectState::Applied => "credential_overlap_verified",
                    ReconcileEffectState::NotApplied => "credential_overlap_absent",
                    ReconcileEffectState::Failed => "credential_overlap_failed",
                    ReconcileEffectState::Unknown => "credential_overlap_unknown",
                }
                .to_owned(),
                sanitized_summary: "credential lifecycle and synthetic probe state were reconciled".to_owned(),
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
                .restore_previous_credential(&CredentialOverlapRestore {
                    credential_set: parameters.credential_set,
                    operation_id: operation_id.to_owned(),
                    execution_id: request.intent.execution_id,
                    plan_step_id: request.intent.step.id,
                })
                .await?;
            Ok(DriverDispatchOutcome {
                operation_id: operation_id.to_owned(),
                outcome_code: "credential_overlap_rolled_back".to_owned(),
                sanitized_summary: "previous credential restored from the durable overlap snapshot".to_owned(),
            })
        })
    }
}

fn require_action(action: ExecutionAction) -> Result<(), ExecutionAgentError> {
    if action == ExecutionAction::SecurityCredentialRotateOverlap {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn parameters(value: &serde_json::Value) -> Result<CredentialRotationParameters, ExecutionAgentError> {
    serde_json::from_value(value.clone()).map_err(|_| ExecutionAgentError::InvalidRequest)
}

fn validate_parameters(parameters: &CredentialRotationParameters) -> Vec<String> {
    let mut reasons = Vec::new();
    for (value, reason, maximum) in [
        (&parameters.credential_set, "credential_set_invalid", 128),
        (&parameters.active_version, "active_version_invalid", 128),
        (&parameters.candidate_version, "candidate_version_invalid", 128),
        (
            &parameters.validation_probe_topic,
            "validation_probe_topic_invalid",
            255,
        ),
    ] {
        if value.is_empty() || value.len() > maximum {
            reasons.push(reason.to_owned());
        }
    }
    if parameters.active_version == parameters.candidate_version {
        reasons.push("candidate_version_must_differ".to_owned());
    }
    if !(60..=3600).contains(&parameters.overlap_seconds) {
        reasons.push("credential_overlap_out_of_range".to_owned());
    }
    if !parameters.validation_probe_topic.starts_with("SRE_PROBE_") {
        reasons.push("validation_probe_topic_not_dedicated".to_owned());
    }
    if !valid_secret_reference(&parameters.candidate_secret_ref) {
        reasons.push("candidate_secret_reference_invalid".to_owned());
    }
    reasons
}

fn validate_for_mutation(parameters: &CredentialRotationParameters) -> Result<(), ExecutionAgentError> {
    if validate_parameters(parameters).is_empty() {
        Ok(())
    } else {
        Err(ExecutionAgentError::InvalidRequest)
    }
}

fn valid_secret_reference(value: &str) -> bool {
    (value.starts_with("kubernetes://") || value.starts_with("vault://"))
        && value.len() <= 255
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b":/_-.".contains(&byte))
        && !value.ends_with('/')
}

#[cfg(test)]
#[path = "credential_rotation_tests.rs"]
mod tests;
