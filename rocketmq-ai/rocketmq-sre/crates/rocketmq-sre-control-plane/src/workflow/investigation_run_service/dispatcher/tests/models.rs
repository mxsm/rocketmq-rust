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

use rocketmq_sre_model_gateway::{
    AsyncModelTransport, ProviderError, ProviderOperationalFailure, TransportFuture, TransportRequest,
    TransportResponse,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::*;

struct ModelTransport {
    mode: &'static str,
    calls: Arc<AtomicUsize>,
}

impl AsyncModelTransport for ModelTransport {
    fn invoke(&self, request: TransportRequest) -> TransportFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.max_response_bytes, 64 * 1024);
        let mode = self.mode;
        Box::pin(async move {
            if mode == "unavailable" {
                return Err(
                    ProviderError::from_operational_failure(ProviderOperationalFailure::ServiceUnavailable).into(),
                );
            }
            let prompt: serde_json::Value =
                serde_json::from_str(request.body["messages"][1]["content"].as_str().unwrap()).unwrap();
            assert!(prompt["saved_rule_results"][0]["findings"][0]["reason_code"].is_string());
            let citation = prompt["evidence"][0]["evidence_id"].clone();
            let content = if mode == "invalid" {
                "{}".to_owned()
            } else {
                json!({"answer":"Saved deterministic Store findings are available for review.","cited_evidence_ids":[citation]}).to_string()
            };
            Ok(TransportResponse {
                status: 200,
                body: json!({"id":"synthetic-d15-model-response","model":"fixture-model","choices":[{"message":{"role":"assistant","content":content},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":8,"total_tokens":20}}),
            })
        })
    }
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL; synthetic bounded model transport"]
async fn postgres_model_explanation_never_repairs_or_falls_back_and_preserves_rule_only_results() {
    for mode in ["none", "invalid", "unavailable", "success"] {
        let (mut fixture, principal, session) = prepared_range().await;
        let mut profiles = rocketmq_sre_model_gateway::builtin_provider_profiles()
            .into_iter()
            .filter(|profile| matches!(profile.id.as_str(), "deepseek" | "openai"))
            .collect::<Vec<_>>();
        assert_eq!(profiles.len(), 2);
        for profile in &mut profiles {
            profile.id = format!("d15-{}-{}", profile.id, uuid::Uuid::new_v4());
            profile.credential_ref = None;
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let model = ModelGatewayService::for_tests(
            fixture.repository.clone(),
            profiles.clone(),
            Arc::new(ModelTransport {
                mode,
                calls: calls.clone(),
            }),
        );
        if mode != "none" {
            let mut governance = fixture.auth.clone();
            governance.roles.insert("model-governance".to_owned());
            for profile in &profiles {
                model
                    .certify_profile_for_tests(&governance, &profile.id, CorrelationId::new())
                    .await
                    .unwrap();
            }
        }
        fixture.state.investigation_runs = InvestigationRunService::new(fixture.repository.clone())
            .unwrap()
            .with_dispatcher(
                fixture.state.connector_channel.clone(),
                fixture.state.evidence.clone(),
                model,
            );
        let created = fixture.create().await;
        let owner = RuntimeContext::from_current("d15-model-regression").service_context("dispatch");
        let drive = fixture
            .state
            .investigation_runs
            .dispatch_one(owner.task_group(), created.run.id);
        let wire = async {
            let mut after = 0;
            for _ in 0..4 {
                let envelope = next_query(&fixture, &principal, session, after).await;
                respond(&fixture, &principal, &envelope, healthy_snapshot(&envelope.query)).await;
                after = envelope.sequence;
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(15), async { tokio::join!(drive, wire) })
            .await
            .unwrap();
        result.unwrap();
        let run = fixture
            .state
            .investigation_runs
            .get(&fixture.auth, created.run.id)
            .await
            .unwrap();
        assert_eq!(run.status, InvestigationRunStatus::Completed, "{mode}: {run:?}");
        assert_eq!(run.stop_reason, None);
        assert_eq!(run.pack_results.len(), 2);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if mode == "none" { 0 } else { 1 },
            "configured fallbacks/repairs cannot bypass the persisted model1 reservation"
        );
        let step = run
            .steps
            .iter()
            .find(|step| matches!(step.spec.work, InvestigationStepKind::ExplainEvidence { .. }))
            .unwrap();
        let attempt = &step.attempts[0];
        if mode == "unavailable" {
            assert!(attempt.result.is_none());
            assert_eq!(
                attempt.interruption.as_ref().unwrap().reason,
                InvestigationInterruptionReason::ModelOutcomeUnknown
            );
        } else {
            assert!(attempt.interruption.is_none());
            assert_eq!(
                attempt.result.as_ref().unwrap().actual_cost.model_calls,
                if mode == "none" { 0 } else { 1 }
            );
        }
        let (used, reserved) = run.budget_usage().unwrap();
        assert_eq!(used.model_calls + reserved.model_calls, 1);
        let requests: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM investigation_run_model_requests WHERE run_id=$1")
            .bind(run.id.as_uuid())
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
        assert_eq!(requests, 1);
        let linked:i64=sqlx::query_scalar("SELECT COUNT(*) FROM investigation_run_explanations e JOIN investigation_run_model_requests q USING(tenant_id,run_id,attempt_id) JOIN model_invocations m ON m.id=e.invocation_id AND m.correlation_id=q.correlation_id WHERE e.run_id=$1").bind(run.id.as_uuid()).fetch_one(&fixture.repository.pool).await.unwrap();
        assert_eq!(linked, if matches!(mode, "invalid" | "success") { 1 } else { 0 });
    }
}
