// Copyright 2023 The RocketMQ Rust Authors
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

use std::collections::BTreeSet;
use std::time::Duration;
use std::time::Instant;

use chrono::Utc;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::ModelProfileId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_model_gateway::AsyncBuiltinProviderClient;
use rocketmq_sre_model_gateway::CanonicalModelRequest;
use rocketmq_sre_model_gateway::CanonicalModelResponse;
use rocketmq_sre_model_gateway::InvocationContext;
use rocketmq_sre_model_gateway::ModelMessage;
use rocketmq_sre_model_gateway::ModelRole;
use rocketmq_sre_model_gateway::ModelTool;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::ProviderErrorCode;
use rocketmq_sre_model_gateway::ResponseFormat;
use rocketmq_sre_model_gateway::ToolChoice;
use serde_json::Value;
use serde_json::json;

use super::ModelGatewayService;
use super::lifecycle::require_model_governance;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::models::lifecycle::ModelProfileLifecycleState;
use crate::models::lifecycle::ProviderSmokeResultView;
use crate::models::model::RuntimeModelProfile;
use crate::models::smoke_repository::PersistProviderSmokeResult;
use crate::observability::ModelPurposeLabel;

const SMOKE_EVIDENCE_ID: &str = "provider-smoke-evidence";
const SMOKE_RESOURCE: &str = "provider-smoke-resource";
const MAX_SMOKES_PER_RUN: usize = 32;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProviderSmokeRunSummary {
    pub(crate) tenants: u32,
    pub(crate) attempted: u32,
    pub(crate) passed: u32,
    pub(crate) quarantined: u32,
    pub(crate) persistence_failures: u32,
}

impl ModelGatewayService {
    pub(crate) async fn run_provider_smoke(
        &self,
        auth: &AuthContext,
        profile_id: ModelProfileId,
    ) -> Result<ProviderSmokeResultView, ControlPlaneError> {
        require_model_governance(auth)?;
        let profiles = self.configured_profiles(auth).await?;
        let lifecycle = self
            .repository
            .model_profile_lifecycle(auth.tenant_id, profile_id)
            .await?;
        if lifecycle.state == ModelProfileLifecycleState::Retired {
            return Err(ControlPlaneError::conflict_code(
                "model_profile_retired",
                "a retired model profile cannot run provider smoke",
            ));
        }
        let profile = profiles
            .into_iter()
            .find(|profile| profile.id == profile_id)
            .ok_or(ControlPlaneError::NotFound)?;
        self.execute_provider_smoke(auth.tenant_id, &profile).await
    }

    pub(crate) async fn run_due_provider_smokes(&self) -> Result<ProviderSmokeRunSummary, ControlPlaneError> {
        if !self.config.enabled || self.transport.is_none() {
            return Ok(ProviderSmokeRunSummary::default());
        }
        let tenant_ids = self.repository.model_profile_tenants().await?;
        let mut summary = ProviderSmokeRunSummary {
            tenants: u32::try_from(tenant_ids.len()).unwrap_or(u32::MAX),
            ..ProviderSmokeRunSummary::default()
        };
        let due_before = Utc::now() - chrono::Duration::minutes(15);
        let mut remaining = MAX_SMOKES_PER_RUN;
        for tenant_id in tenant_ids {
            if remaining == 0 {
                break;
            }
            let profiles = self
                .repository
                .ensure_model_profiles(tenant_id, &self.config.profiles)
                .await?;
            let due = self
                .repository
                .model_profiles_due_smoke(tenant_id, due_before, u32::try_from(remaining).unwrap_or(u32::MAX))
                .await?
                .into_iter()
                .collect::<BTreeSet<_>>();
            for profile in profiles
                .iter()
                .filter(|profile| due.contains(&profile.id))
                .take(remaining)
            {
                remaining = remaining.saturating_sub(1);
                summary.attempted = summary.attempted.saturating_add(1);
                match self.execute_provider_smoke(tenant_id, profile).await {
                    Ok(result) if result.overall_ok => {
                        summary.passed = summary.passed.saturating_add(1);
                    }
                    Ok(_) => {
                        summary.quarantined = summary.quarantined.saturating_add(1);
                    }
                    Err(error) => {
                        summary.persistence_failures = summary.persistence_failures.saturating_add(1);
                        tracing::warn!(
                            tenant_id = %tenant_id,
                            profile_id = %profile.id,
                            error = %error,
                            "provider smoke result could not be persisted"
                        );
                    }
                }
            }
        }
        Ok(summary)
    }

    async fn execute_provider_smoke(
        &self,
        tenant_id: TenantId,
        profile: &RuntimeModelProfile,
    ) -> Result<ProviderSmokeResultView, ControlPlaneError> {
        let started_at = Instant::now();
        let correlation_id = CorrelationId::new();
        let mut connectivity_ok = false;
        let mut structured_output_ok = false;
        let mut tool_arguments_ok = false;
        let mut evidence_citation_ok = false;
        let mut failure_codes = Vec::new();
        let mut calls_attempted = 0_u8;
        let mut calls_succeeded = 0_u8;

        let client = self
            .transport
            .clone()
            .ok_or_else(|| ProviderError::new(ProviderErrorCode::ServiceUnavailable, "model transport is disabled"))
            .and_then(|transport| AsyncBuiltinProviderClient::new(profile.profile.clone(), transport));
        let credential = match self.resolve_credential(&profile.profile).await {
            Ok(credential) => Some(credential),
            Err(error) => {
                push_provider_failure(&mut failure_codes, "credential", &error);
                None
            }
        };

        match (client, credential) {
            (Ok(client), Some(credential)) => {
                calls_attempted = calls_attempted.saturating_add(1);
                match self
                    .smoke_invoke(
                        profile,
                        &client,
                        correlation_id,
                        connectivity_request(correlation_id, &profile.profile.model),
                        credential.clone(),
                    )
                    .await
                {
                    Ok(response) => {
                        calls_succeeded = calls_succeeded.saturating_add(1);
                        connectivity_ok = !response.content.trim().is_empty();
                        if !connectivity_ok {
                            failure_codes.push("connectivity.empty_response".to_owned());
                        }
                    }
                    Err(error) => push_provider_failure(&mut failure_codes, "connectivity", &error),
                }

                calls_attempted = calls_attempted.saturating_add(1);
                match self
                    .smoke_invoke(
                        profile,
                        &client,
                        correlation_id,
                        structured_request(correlation_id, &profile.profile.model),
                        credential.clone(),
                    )
                    .await
                {
                    Ok(response) => {
                        calls_succeeded = calls_succeeded.saturating_add(1);
                        let parsed = serde_json::from_str::<Value>(&response.content).ok();
                        structured_output_ok = parsed
                            .as_ref()
                            .is_some_and(|value| value.get("status").and_then(Value::as_str) == Some("ok"));
                        evidence_citation_ok = parsed.as_ref().is_some_and(|value| {
                            value.get("evidence_id").and_then(Value::as_str) == Some(SMOKE_EVIDENCE_ID)
                        });
                        if !structured_output_ok {
                            failure_codes.push("structured_output.invalid".to_owned());
                        }
                        if !evidence_citation_ok {
                            failure_codes.push("evidence_citation.invalid".to_owned());
                        }
                    }
                    Err(error) => push_provider_failure(&mut failure_codes, "structured_output", &error),
                }

                calls_attempted = calls_attempted.saturating_add(1);
                match self
                    .smoke_invoke(
                        profile,
                        &client,
                        correlation_id,
                        tool_request(correlation_id, &profile.profile.model),
                        credential,
                    )
                    .await
                {
                    Ok(response) => {
                        calls_succeeded = calls_succeeded.saturating_add(1);
                        tool_arguments_ok = response.tool_calls.iter().any(|call| {
                            call.name == "read_smoke_evidence"
                                && call.arguments.get("resource").and_then(Value::as_str) == Some(SMOKE_RESOURCE)
                        });
                        if !tool_arguments_ok {
                            failure_codes.push("tool_arguments.invalid".to_owned());
                        }
                    }
                    Err(error) => push_provider_failure(&mut failure_codes, "tool_arguments", &error),
                }
            }
            (Err(error), _) => push_provider_failure(&mut failure_codes, "profile", &error),
            (Ok(_), None) => {}
        }

        failure_codes.sort();
        failure_codes.dedup();
        let elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let result = PersistProviderSmokeResult {
            connectivity_ok,
            structured_output_ok,
            tool_arguments_ok,
            evidence_citation_ok,
            latency_ms: Some(elapsed_ms),
            result_snapshot: json!({
                "schema_version": "rocketmq-sre.provider-smoke-result.v1",
                "checks": {
                    "connectivity": connectivity_ok,
                    "structured_output": structured_output_ok,
                    "tool_arguments": tool_arguments_ok,
                    "evidence_citation": evidence_citation_ok,
                },
                "failure_codes": failure_codes,
                "calls_attempted": calls_attempted,
                "calls_succeeded": calls_succeeded,
            }),
            failure_codes,
            observed_at: Utc::now(),
        };
        self.repository
            .persist_provider_smoke_result(tenant_id, profile.id, &result, "provider-smoke-worker", correlation_id)
            .await
    }

    async fn smoke_invoke(
        &self,
        profile: &RuntimeModelProfile,
        client: &AsyncBuiltinProviderClient,
        correlation_id: CorrelationId,
        request: CanonicalModelRequest,
        credential: Option<rocketmq_sre_model_gateway::SecretMaterial>,
    ) -> Result<CanonicalModelResponse, ProviderError> {
        let mut context = InvocationContext::new(correlation_id);
        context.max_response_bytes = 64 * 1024;
        context.deadline_unix_ms = Some(
            u64::try_from(Utc::now().timestamp_millis())
                .unwrap_or_default()
                .saturating_add(u64::try_from(Duration::from_secs(15).as_millis()).unwrap_or(15_000)),
        );
        self.invoke_model(
            &profile.profile,
            ModelPurposeLabel::Other,
            client,
            &context,
            &request,
            credential,
        )
        .await
    }
}

fn connectivity_request(correlation_id: CorrelationId, model: &str) -> CanonicalModelRequest {
    let mut request = CanonicalModelRequest::new(
        correlation_id,
        model,
        vec![ModelMessage::text(
            ModelRole::User,
            "Provider health probe. Respond with the single word OK.",
        )],
    );
    request.temperature_milli = Some(0);
    request.max_output_tokens = Some(16);
    request
}

fn structured_request(correlation_id: CorrelationId, model: &str) -> CanonicalModelRequest {
    let mut request = CanonicalModelRequest::new(
        correlation_id,
        model,
        vec![ModelMessage::text(
            ModelRole::User,
            format!(
                "Return the required JSON object and cite evidence_id {SMOKE_EVIDENCE_ID}. Do not include other \
                 fields."
            ),
        )],
    );
    request.response_format = ResponseFormat::JsonSchema {
        name: "provider_smoke".to_owned(),
        schema: json!({
            "type": "object",
            "properties": {
                "status": {"type": "string", "const": "ok"},
                "evidence_id": {"type": "string", "const": SMOKE_EVIDENCE_ID}
            },
            "required": ["status", "evidence_id"],
            "additionalProperties": false
        }),
        strict: true,
    };
    request.temperature_milli = Some(0);
    request.max_output_tokens = Some(64);
    request
}

fn tool_request(correlation_id: CorrelationId, model: &str) -> CanonicalModelRequest {
    let mut request = CanonicalModelRequest::new(
        correlation_id,
        model,
        vec![ModelMessage::text(
            ModelRole::User,
            format!("Call read_smoke_evidence for resource {SMOKE_RESOURCE}."),
        )],
    );
    request.tools = vec![ModelTool::read_only(
        "read_smoke_evidence",
        "Reads a synthetic provider smoke evidence reference.",
        json!({
            "type": "object",
            "properties": {
                "resource": {"type": "string", "const": SMOKE_RESOURCE}
            },
            "required": ["resource"],
            "additionalProperties": false
        }),
    )];
    request.tool_choice = ToolChoice::Specific {
        name: "read_smoke_evidence".to_owned(),
    };
    request.temperature_milli = Some(0);
    request.max_output_tokens = Some(32);
    request
}

fn push_provider_failure(failure_codes: &mut Vec<String>, check: &str, error: &ProviderError) {
    let code = serde_json::to_value(error.code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned());
    failure_codes.push(format!("{check}.{code}"));
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;

    use rocketmq_sre_model_gateway::AsyncModelTransport;
    use rocketmq_sre_model_gateway::TransportFuture;
    use rocketmq_sre_model_gateway::TransportRequest;
    use rocketmq_sre_model_gateway::TransportResponse;
    use uuid::Uuid;

    use super::*;
    use crate::PostgresRepository;
    use crate::models::lifecycle::ModelProfileLifecycleTransitionRequest;
    use crate::models::lifecycle::ModelProfileRollbackRequest;

    struct QueueTransport {
        responses: Mutex<VecDeque<Result<TransportResponse, ProviderError>>>,
    }

    impl QueueTransport {
        fn new(responses: impl IntoIterator<Item = Result<TransportResponse, ProviderError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }
    }

    impl AsyncModelTransport for QueueTransport {
        fn invoke(&self, _request: TransportRequest) -> TransportFuture<'_> {
            let response = self
                .responses
                .lock()
                .expect("provider smoke response queue")
                .pop_front()
                .expect("provider smoke scripted response");
            Box::pin(async move { response })
        }
    }

    #[test]
    fn smoke_requests_are_bounded_and_read_only() {
        let request = structured_request(CorrelationId::new(), "smoke-model");
        assert_eq!(request.max_output_tokens, Some(64));
        assert!(request.tools.is_empty());

        let request = tool_request(CorrelationId::new(), "smoke-model");
        assert_eq!(request.tools.len(), 1);
        assert!(!request.tools[0].mutates_cluster);
        assert_eq!(
            request.tool_choice,
            ToolChoice::Specific {
                name: "read_smoke_evidence".to_owned()
            }
        );
    }

    #[test]
    fn provider_failures_are_reduced_to_stable_codes() {
        let mut codes = Vec::new();
        push_provider_failure(
            &mut codes,
            "connectivity",
            &ProviderError::new(ProviderErrorCode::Timeout, "redacted"),
        );
        assert_eq!(codes, vec!["connectivity.timeout"]);
    }

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn postgres_provider_lifecycle_smoke_promote_rollback_retire_and_quarantine() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 2)
            .await
            .expect("database and migrations");
        let tenant_id = TenantId::new();
        let suffix = Uuid::new_v4();
        let profile_a = test_profile(format!("provider-a-{suffix}"));
        let profile_b = test_profile(format!("provider-b-{suffix}"));
        let transport = Arc::new(QueueTransport::new(
            passing_smoke_responses().into_iter().chain(passing_smoke_responses()),
        ));
        let service = ModelGatewayService::for_tests(
            repository.clone(),
            vec![profile_a.clone(), profile_b.clone()],
            transport,
        );
        let auth = AuthContext {
            tenant_id,
            subject: "model-governance-test".to_owned(),
            clusters: BTreeSet::new(),
            roles: BTreeSet::from(["model-governance".to_owned()]),
        };
        let configured = service.configured_profiles(&auth).await.expect("configured profiles");
        let profile_a_id = configured
            .iter()
            .find(|profile| profile.profile.id == profile_a.id)
            .expect("profile A")
            .id;
        let profile_b_id = configured
            .iter()
            .find(|profile| profile.profile.id == profile_b.id)
            .expect("profile B")
            .id;

        assert!(
            service
                .run_provider_smoke(&auth, profile_a_id)
                .await
                .expect("profile A smoke")
                .overall_ok
        );
        assert!(
            service
                .run_provider_smoke(&auth, profile_b_id)
                .await
                .expect("profile B smoke")
                .overall_ok
        );
        let profile_b_lifecycle = service
            .transition_profile_lifecycle(
                &auth,
                profile_b_id,
                &transition(ModelProfileLifecycleState::Certified, 1, None, "certify_b"),
                CorrelationId::new(),
            )
            .await
            .expect("certify profile B");
        assert_eq!(profile_b_lifecycle.state, ModelProfileLifecycleState::Certified);
        let profile_a_lifecycle = service
            .transition_profile_lifecycle(
                &auth,
                profile_a_id,
                &transition(ModelProfileLifecycleState::Certified, 1, None, "certify_a"),
                CorrelationId::new(),
            )
            .await
            .expect("certify profile A");
        assert_eq!(profile_a_lifecycle.revision, 2);
        let promoted = service
            .transition_profile_lifecycle(
                &auth,
                profile_a_id,
                &transition(ModelProfileLifecycleState::Promoted, 2, Some(profile_b_id), "promote_a"),
                CorrelationId::new(),
            )
            .await
            .expect("promote profile A");
        assert_eq!(promoted.state, ModelProfileLifecycleState::Promoted);
        assert_eq!(promoted.rollback_profile_id, Some(profile_b_id));

        let rolled_back = service
            .rollback_profile(
                &auth,
                profile_a_id,
                &ModelProfileRollbackRequest {
                    expected_revision: 3,
                    reason_code: "rollback_a".to_owned(),
                    operator_confirmed: true,
                },
                CorrelationId::new(),
            )
            .await
            .expect("rollback profile A");
        assert_eq!(rolled_back.profile_id, profile_b_id);
        assert_eq!(rolled_back.state, ModelProfileLifecycleState::Promoted);
        assert_eq!(rolled_back.rollback_profile_id, Some(profile_a_id));
        let quarantined_a = service
            .profile_lifecycle(&auth, profile_a_id)
            .await
            .expect("profile A lifecycle");
        assert_eq!(quarantined_a.state, ModelProfileLifecycleState::Quarantined);

        let retired_a = service
            .transition_profile_lifecycle(
                &auth,
                profile_a_id,
                &transition(ModelProfileLifecycleState::Retired, 4, None, "retire_a"),
                CorrelationId::new(),
            )
            .await
            .expect("retire profile A");
        assert_eq!(retired_a.state, ModelProfileLifecycleState::Retired);
        let retired_transition = service
            .transition_profile_lifecycle(
                &auth,
                profile_a_id,
                &transition(ModelProfileLifecycleState::Certified, 5, None, "revive_a"),
                CorrelationId::new(),
            )
            .await
            .expect_err("retired lifecycle must be terminal");
        assert!(matches!(retired_transition, ControlPlaneError::Conflict { .. }));

        let failing_service = ModelGatewayService::for_tests(
            repository.clone(),
            vec![profile_a, profile_b],
            Arc::new(QueueTransport::new([
                Err(ProviderError::new(ProviderErrorCode::ServiceUnavailable, "redacted")),
                Err(ProviderError::new(ProviderErrorCode::ServiceUnavailable, "redacted")),
                Err(ProviderError::new(ProviderErrorCode::ServiceUnavailable, "redacted")),
            ])),
        );
        let failed_smoke = failing_service
            .run_provider_smoke(&auth, profile_b_id)
            .await
            .expect("failed smoke must still persist");
        assert!(!failed_smoke.overall_ok);
        let quarantined_b = failing_service
            .profile_lifecycle(&auth, profile_b_id)
            .await
            .expect("profile B lifecycle");
        assert_eq!(quarantined_b.state, ModelProfileLifecycleState::Quarantined);
        assert!(!quarantined_b.operator_confirmed);
        assert_eq!(quarantined_b.reason_code, "provider_smoke_failed");

        let events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM model_profile_lifecycle_events
             WHERE tenant_id = $1 AND profile_id = ANY($2)",
        )
        .bind(tenant_id.as_uuid())
        .bind(vec![profile_a_id.as_uuid(), profile_b_id.as_uuid()])
        .fetch_one(&repository.pool)
        .await
        .expect("lifecycle event count");
        assert_eq!(events, 9);
    }

    fn transition(
        target_state: ModelProfileLifecycleState,
        expected_revision: u64,
        rollback_profile_id: Option<ModelProfileId>,
        reason_code: &str,
    ) -> ModelProfileLifecycleTransitionRequest {
        ModelProfileLifecycleTransitionRequest {
            target_state,
            expected_revision,
            rollback_profile_id,
            reason_code: reason_code.to_owned(),
            operator_confirmed: true,
        }
    }

    fn test_profile(id: String) -> rocketmq_sre_model_gateway::ProviderProfile {
        let mut profile = rocketmq_sre_model_gateway::builtin_provider_profiles()
            .into_iter()
            .find(|profile| profile.id == "deepseek")
            .expect("DeepSeek profile fixture");
        profile.id = id;
        profile.credential_ref = None;
        profile
    }

    fn passing_smoke_responses() -> Vec<Result<TransportResponse, ProviderError>> {
        vec![
            Ok(openai_response("OK", Vec::new(), "stop")),
            Ok(openai_response(
                &json!({"status": "ok", "evidence_id": SMOKE_EVIDENCE_ID}).to_string(),
                Vec::new(),
                "stop",
            )),
            Ok(openai_response(
                "",
                vec![json!({
                    "id": "smoke-tool-call",
                    "type": "function",
                    "function": {
                        "name": "read_smoke_evidence",
                        "arguments": json!({"resource": SMOKE_RESOURCE}).to_string()
                    }
                })],
                "tool_calls",
            )),
        ]
    }

    fn openai_response(content: &str, tool_calls: Vec<Value>, finish_reason: &str) -> TransportResponse {
        TransportResponse {
            status: 200,
            body: json!({
                "id": "provider-smoke",
                "model": "deepseek-chat",
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": content,
                        "tool_calls": tool_calls,
                    },
                    "finish_reason": finish_reason,
                }],
                "usage": {
                    "prompt_tokens": 4,
                    "completion_tokens": 2,
                    "total_tokens": 6,
                }
            }),
        }
    }
}
