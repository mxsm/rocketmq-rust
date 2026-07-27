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

use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::CriticAssessment;
use rocketmq_sre_contracts::CriticConclusion;
use rocketmq_sre_contracts::CriticReviewStatus;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::ModelInvocationRecord;
use rocketmq_sre_contracts::canonical_sha256;
use rocketmq_sre_model_gateway::heterogeneous_critic_profiles;
use serde::Serialize;

use super::super::model::CriticInvocationIdentity;
use super::super::model::ModelCriticDecision;
use super::*;

const CRITIC_PURPOSE: &str = "critic";
const CRITIC_PROMPT_VERSION: &str = "rocketmq-sre.critic.prompt.v1";
const CRITIC_SCHEMA_VERSION: &str = "rocketmq-sre.critic-assessment.v1";
const MAX_CRITIC_OUTPUT_TOKENS: u32 = 1_024;

#[derive(Serialize)]
struct DegradedCriticPayload<'a> {
    plan_hash: &'a str,
    primary_invocation_id: ModelInvocationId,
    status: CriticReviewStatus,
    reason_code: &'a str,
}

impl ModelGatewayService {
    #[allow(
        clippy::too_many_arguments,
        reason = "the Critic call binds immutable plan, diagnosis, primary identity, and provenance"
    )]
    pub(crate) async fn critique_plan(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
        primary: &ModelInvocationRecord,
        allowed_evidence_ids: &[EvidenceId],
        correlation_id: CorrelationId,
    ) -> Result<ModelCriticDecision, ControlPlaneError> {
        if !self.config.enabled {
            return degraded_decision(
                plan,
                primary.id,
                CriticReviewStatus::Unavailable,
                "critic_gateway_disabled",
                None,
            );
        }
        let profiles = self.configured_profiles(auth).await?;
        let configured = profiles
            .iter()
            .map(|profile| profile.profile.clone())
            .collect::<Vec<_>>();
        let ordered = heterogeneous_critic_profiles(&primary.model_family, &configured, DataClass::Internal).map_err(
            |error| {
                ControlPlaneError::configuration(format!(
                    "Critic model-family configuration is invalid: {:?}",
                    error.code
                ))
            },
        )?;
        let candidates = ordered
            .into_iter()
            .filter_map(|selected| profiles.iter().find(|candidate| candidate.profile.id == selected.id))
            .collect::<Vec<_>>();
        let Some(requested) = candidates.first() else {
            return degraded_decision(
                plan,
                primary.id,
                CriticReviewStatus::Unavailable,
                "heterogeneous_critic_unavailable",
                None,
            );
        };
        let requested_profile_id = requested.id;

        let prompt = critic_prompt(plan, allowed_evidence_ids)?;
        let prompt_text = serde_json::to_string(&prompt)
            .map_err(|_| ControlPlaneError::configuration("Critic prompt cannot be serialized"))?;
        if prompt_text.len() > self.config.max_request_bytes {
            return degraded_decision(
                plan,
                primary.id,
                CriticReviewStatus::Invalid,
                "critic_request_too_large",
                None,
            );
        }
        let schema = critic_output_schema();
        let deadline = rocketmq_sre_model_gateway::current_unix_ms()
            .saturating_add(self.config.request_timeout.as_millis().min(u128::from(u64::MAX)) as u64);
        let mut attempted = Vec::new();
        let max_attempts = self.config.max_fallbacks.saturating_add(1);
        for profile in candidates.into_iter().take(max_attempts) {
            let started_at = Utc::now();
            let mut request = CanonicalModelRequest::new(
                correlation_id,
                profile.profile.model.clone(),
                vec![
                    ModelMessage::text(
                        ModelRole::System,
                        "Review the immutable RocketMQ SRE plan using only the fixed JSON checklist. Return only the \
                         requested schema. Never alter the plan, descriptor, policy, action parameters, or execution \
                         state.",
                    ),
                    ModelMessage::text(ModelRole::User, prompt_text.clone()),
                ],
            );
            request.response_format = ResponseFormat::JsonSchema {
                name: "rocketmq_sre_critic_assessment".to_owned(),
                schema: schema.clone(),
                strict: true,
            };
            request.temperature_milli = Some(0);
            request.max_output_tokens = Some(MAX_CRITIC_OUTPUT_TOKENS);
            request.tool_choice = ToolChoice::None;
            let mut context = InvocationContext::new(correlation_id);
            context.deadline_unix_ms = Some(deadline);
            context.max_response_bytes = self.config.max_response_bytes;

            let result = self
                .invoke_critic_candidate(
                    auth,
                    plan,
                    primary,
                    profile,
                    requested_profile_id,
                    &attempted,
                    correlation_id,
                    started_at,
                    &context,
                    &request,
                )
                .await?;
            match result {
                CriticCandidateResult::Completed { response, identity } => {
                    let assessment = serde_json::from_str::<CriticAssessment>(&response.content);
                    let assessment = match assessment {
                        Ok(assessment) if assessment.validate(allowed_evidence_ids).is_ok() => assessment,
                        _ => {
                            return degraded_decision(
                                plan,
                                primary.id,
                                CriticReviewStatus::Invalid,
                                "critic_output_invalid",
                                Some(identity),
                            );
                        }
                    };
                    let payload_hash = canonical_sha256(&assessment).map_err(|error| {
                        ControlPlaneError::configuration(format!("Critic assessment cannot be hashed: {error}"))
                    })?;
                    let status = assessment_status(&assessment);
                    let reason_code = match status {
                        CriticReviewStatus::Valid => "critic_review_valid",
                        CriticReviewStatus::Conflict => "critic_conclusion_conflict",
                        _ => "critic_output_invalid",
                    };
                    return Ok(ModelCriticDecision {
                        status,
                        conclusion: assessment.conclusion,
                        assessment: Some(assessment),
                        invocation: Some(identity),
                        payload_hash,
                        reason_code,
                        prompt_version: CRITIC_PROMPT_VERSION,
                        schema_version: CRITIC_SCHEMA_VERSION,
                    });
                }
                CriticCandidateResult::Failed { error, identity } => {
                    attempted.push(profile);
                    if fallback_safe(&error) {
                        if attempted.len() < max_attempts {
                            continue;
                        }
                        return degraded_decision(
                            plan,
                            primary.id,
                            CriticReviewStatus::Unavailable,
                            "critic_provider_unavailable",
                            Some(identity),
                        );
                    }
                    let status = if error.code == ProviderErrorCode::SchemaValidationFailed
                        || error.code == ProviderErrorCode::SafetyRefusal
                    {
                        CriticReviewStatus::Invalid
                    } else {
                        CriticReviewStatus::Unavailable
                    };
                    return degraded_decision(plan, primary.id, status, "critic_provider_rejected", Some(identity));
                }
            }
        }
        degraded_decision(
            plan,
            primary.id,
            CriticReviewStatus::Unavailable,
            "critic_provider_unavailable",
            None,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "candidate invocation persistence binds exact model and diagnosis lineage"
    )]
    async fn invoke_critic_candidate(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
        primary: &ModelInvocationRecord,
        profile: &RuntimeModelProfile,
        requested_profile_id: rocketmq_sre_contracts::ModelProfileId,
        attempted: &[&RuntimeModelProfile],
        correlation_id: CorrelationId,
        started_at: chrono::DateTime<Utc>,
        context: &InvocationContext,
        request: &CanonicalModelRequest,
    ) -> Result<CriticCandidateResult, ControlPlaneError> {
        let result = match self.resolve_credential(&profile.profile).await {
            Ok(credential) => match &self.transport {
                Some(transport) => match AsyncBuiltinProviderClient::new(profile.profile.clone(), transport.clone()) {
                    Ok(client) => {
                        self.invoke_model(
                            &profile.profile,
                            ModelPurposeLabel::Critic,
                            &client,
                            context,
                            request,
                            credential,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                },
                None => Err(ProviderError::service_unavailable("model transport is not configured")),
            },
            Err(error) => Err(error),
        };
        let completed_at = Utc::now();
        let (response, error) = match result {
            Ok(response)
                if matches!(response.finish_reason, FinishReason::Stop)
                    && response.content.len() <= self.config.max_response_bytes =>
            {
                (Some(response), None)
            }
            Ok(response) => (
                Some(response),
                Some(ProviderError::new(
                    ProviderErrorCode::SchemaValidationFailed,
                    "Critic response was incomplete, refused, or exceeded bounds",
                )),
            ),
            Err(error) => (None, Some(error)),
        };
        let fallback_ids = attempted.iter().map(|candidate| candidate.id).collect::<Vec<_>>();
        let fallback_chain = attempted
            .iter()
            .map(|candidate| critic_profile_identity(candidate))
            .collect::<Vec<_>>();
        let invocation_id = ModelInvocationId::new();
        let provider_family = enum_name(profile.profile.provider_family);
        let model_family = rocketmq_sre_model_gateway::normalize_model_family(&profile.profile.model_family)
            .map_err(|_| ControlPlaneError::configuration("Critic model family cannot be normalized"))?;
        let identity = CriticInvocationIdentity {
            id: invocation_id,
            provider_family: provider_family.clone(),
            model_family: model_family.clone(),
            profile: profile.profile.id.clone(),
            model_revision: profile.profile.model_revision.clone(),
            endpoint_instance: profile.profile.endpoint_instance.clone(),
            fallback_chain,
        };
        let input_tokens = response
            .as_ref()
            .and_then(|response| response.usage.input_tokens.or(response.input_tokens));
        let output_tokens = response
            .as_ref()
            .and_then(|response| response.usage.output_tokens.or(response.output_tokens));
        let cost_micros = response.as_ref().and_then(|response| {
            response_invocation_cost(profile.profile.estimated_cost_microusd_per_1k_tokens, response)
        });
        let error_code = error.as_ref().map(|error| enum_name(error.code));
        self.repository
            .persist_model_invocation(&PersistInvocation {
                id: invocation_id,
                tenant_id: auth.tenant_id,
                cluster_id: plan.cluster_id,
                incident_id: plan.incident_id,
                diagnosis_revision_id: Some(plan.diagnosis_revision),
                parent_invocation_id: Some(primary.id),
                purpose: CRITIC_PURPOSE,
                requested_profile_id,
                actual_profile_id: profile.id,
                provider_family,
                model_family,
                actual_model: response
                    .as_ref()
                    .map_or_else(|| profile.profile.model.clone(), |response| response.model.clone()),
                model_revision: profile.profile.model_revision.clone(),
                endpoint_instance: profile.profile.endpoint_instance.clone(),
                fallback_chain: fallback_ids,
                prompt_version: CRITIC_PROMPT_VERSION,
                schema_version: CRITIC_SCHEMA_VERSION,
                input_tokens,
                output_tokens,
                cost_micros,
                rationale: error_code.as_ref().map_or_else(
                    || "structured heterogeneous Critic review".to_owned(),
                    |code| format!("Critic provider attempt failed with {code}"),
                ),
                error_code,
                correlation_id,
                started_at,
                completed_at,
            })
            .await?;
        if let Some(error) = error {
            self.record_failure_health(auth, profile, &error).await;
            Ok(CriticCandidateResult::Failed { error, identity })
        } else {
            let response = response.ok_or_else(|| {
                ControlPlaneError::configuration("successful Critic invocation did not contain a response")
            })?;
            if let Err(error) = self
                .repository
                .record_model_health(auth.tenant_id, profile, ProviderHealth::Healthy, None)
                .await
            {
                tracing::warn!(error = %error, "Critic provider health could not be persisted");
            }
            Ok(CriticCandidateResult::Completed {
                response: Box::new(response),
                identity,
            })
        }
    }
}

enum CriticCandidateResult {
    Completed {
        response: Box<CanonicalModelResponse>,
        identity: CriticInvocationIdentity,
    },
    Failed {
        error: ProviderError,
        identity: CriticInvocationIdentity,
    },
}

fn degraded_decision(
    plan: &ActionPlan,
    primary_invocation_id: ModelInvocationId,
    status: CriticReviewStatus,
    reason_code: &'static str,
    invocation: Option<CriticInvocationIdentity>,
) -> Result<ModelCriticDecision, ControlPlaneError> {
    let payload_hash = canonical_sha256(&DegradedCriticPayload {
        plan_hash: &plan.plan_hash,
        primary_invocation_id,
        status,
        reason_code,
    })
    .map_err(|error| ControlPlaneError::configuration(format!("Critic degradation cannot be hashed: {error}")))?;
    Ok(ModelCriticDecision {
        status,
        conclusion: CriticConclusion::NeedsRevision,
        assessment: None,
        invocation,
        payload_hash,
        reason_code,
        prompt_version: CRITIC_PROMPT_VERSION,
        schema_version: CRITIC_SCHEMA_VERSION,
    })
}

fn assessment_status(assessment: &CriticAssessment) -> CriticReviewStatus {
    let has_issue = !assessment.parameter_ranges_valid
        || !assessment.missing_preconditions.is_empty()
        || !assessment.impact_scope_valid
        || !assessment.rollback_available
        || !assessment.findings.is_empty();
    if (assessment.conclusion == CriticConclusion::Accept && has_issue)
        || (assessment.conclusion != CriticConclusion::Accept && !has_issue)
    {
        CriticReviewStatus::Conflict
    } else {
        CriticReviewStatus::Valid
    }
}

fn critic_profile_identity(profile: &RuntimeModelProfile) -> String {
    format!(
        "{}|{}|{}|{}",
        profile.profile.id,
        profile.profile.model_family,
        profile.profile.model_revision,
        profile.profile.endpoint_instance
    )
}

fn critic_prompt(plan: &ActionPlan, evidence_ids: &[EvidenceId]) -> Result<Value, ControlPlaneError> {
    Ok(json!({
        "schema_version": "rocketmq-sre.critic-input.v1",
        "immutable_plan": plan,
        "allowed_evidence_ids": evidence_ids,
        "checks": [
            "evidence_references",
            "counter_evidence",
            "parameter_ranges",
            "missing_preconditions",
            "impact_scope",
            "rollback_availability"
        ],
        "constraints": {
            "may_modify_plan": false,
            "may_modify_descriptor": false,
            "may_modify_policy": false,
            "may_execute": false
        }
    }))
}

fn critic_output_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "conclusion",
            "cited_evidence_ids",
            "counter_evidence_ids",
            "parameter_ranges_valid",
            "missing_preconditions",
            "impact_scope_valid",
            "rollback_available",
            "findings",
            "rationale"
        ],
        "properties": {
            "conclusion": {"type": "string", "enum": ["accept", "needs_revision", "reject"]},
            "cited_evidence_ids": bounded_id_array(),
            "counter_evidence_ids": bounded_id_array(),
            "parameter_ranges_valid": {"type": "boolean"},
            "missing_preconditions": {
                "type": "array",
                "maxItems": 32,
                "uniqueItems": true,
                "items": {"type": "string", "minLength": 1, "maxLength": 256}
            },
            "impact_scope_valid": {"type": "boolean"},
            "rollback_available": {"type": "boolean"},
            "findings": {
                "type": "array",
                "maxItems": 32,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["code", "message", "evidence_ids"],
                    "properties": {
                        "code": {
                            "type": "string",
                            "enum": [
                                "evidence_reference_invalid",
                                "counter_evidence_missing",
                                "parameter_out_of_range",
                                "missing_precondition",
                                "impact_scope_exceeded",
                                "rollback_unavailable"
                            ]
                        },
                        "message": {"type": "string", "minLength": 1, "maxLength": 1000},
                        "evidence_ids": bounded_id_array()
                    }
                }
            },
            "rationale": {"type": "string", "minLength": 1, "maxLength": 4000}
        }
    })
}

fn bounded_id_array() -> Value {
    json!({
        "type": "array",
        "maxItems": 32,
        "uniqueItems": true,
        "items": {"type": "string", "format": "uuid"}
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assessment(conclusion: CriticConclusion) -> CriticAssessment {
        CriticAssessment {
            conclusion,
            cited_evidence_ids: vec![EvidenceId::new()],
            counter_evidence_ids: Vec::new(),
            parameter_ranges_valid: true,
            missing_preconditions: Vec::new(),
            impact_scope_valid: true,
            rollback_available: true,
            findings: Vec::new(),
            rationale: "bounded rationale".to_owned(),
        }
    }

    #[test]
    fn conclusion_conflict_is_fail_closed() {
        assert_eq!(
            assessment_status(&assessment(CriticConclusion::Accept)),
            CriticReviewStatus::Valid
        );
        assert_eq!(
            assessment_status(&assessment(CriticConclusion::NeedsRevision)),
            CriticReviewStatus::Conflict
        );
        let mut accepted_with_issue = assessment(CriticConclusion::Accept);
        accepted_with_issue.rollback_available = false;
        assert_eq!(assessment_status(&accepted_with_issue), CriticReviewStatus::Conflict);
    }

    #[test]
    fn schema_exposes_only_the_fixed_review_surface() {
        let schema = critic_output_schema();
        assert_eq!(schema["additionalProperties"], false);
        for forbidden in ["plan", "descriptor", "policy", "action", "parameters", "execution"] {
            assert!(schema["properties"].get(forbidden).is_none());
        }
    }
}
