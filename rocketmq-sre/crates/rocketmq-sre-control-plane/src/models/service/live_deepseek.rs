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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::Utc;
use rocketmq_runtime::RuntimeContext;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::Sensitivity;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_model_gateway::AsyncModelTransport;
use rocketmq_sre_model_gateway::DevSecretProvider;
use rocketmq_sre_model_gateway::HttpModelTransport;
use rocketmq_sre_model_gateway::HttpTransportConfig;
use rocketmq_sre_model_gateway::ProviderDialect;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::SecretReference;
use rocketmq_sre_model_gateway::TransportFuture;
use rocketmq_sre_model_gateway::TransportRequest;
use serde_json::json;

use super::MODEL_ADOPTED_REASON;
use super::ModelGatewayService;
use super::ModelRuntimeConfig;
use super::ModelSecretProviderConfig;
use super::StructuredModelDiagnosis;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::models::model::ModelInvocationListQuery;

const LIVE_API_KEY_ENV: &str = "ROCKETMQ_SRE_LIVE_DEEPSEEK_API_KEY";
const LIVE_DATABASE_URL_ENV: &str = "ROCKETMQ_SRE_TEST_DATABASE_URL";
const FORBIDDEN_EVIDENCE_MARKER: &str = "qualification-body-must-not-leave";

struct InspectingTransport {
    inner: HttpModelTransport,
    calls: AtomicUsize,
}

impl InspectingTransport {
    fn new(inner: HttpModelTransport) -> Self {
        Self {
            inner,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl AsyncModelTransport for InspectingTransport {
    fn invoke(&self, request: TransportRequest) -> TransportFuture<'_> {
        let serialized = serde_json::to_string(&request.body).unwrap_or_default();
        if request.dialect != ProviderDialect::DeepSeekResponses
            || request.path != "/responses"
            || request.endpoint != "https://api.deepseek.com"
            || request.credential.is_none()
            || request.body.get("tools").is_some()
            || request.body.get("messages").is_some()
            || serialized.contains(FORBIDDEN_EVIDENCE_MARKER)
            || serialized.contains("message_body")
            || serialized.contains("access_token")
        {
            return Box::pin(async {
                Err(ProviderError::policy_denied(
                    "live diagnosis request violated the read-only qualification boundary",
                ))
            });
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.invoke(request)
    }
}

/// Credential-gated, disposable-database qualification of the real AI SRE
/// diagnosis path. The runner injects the secret through one process-local
/// environment variable; this test never prints or persists the value, prompt,
/// evidence body, or model response.
#[tokio::test]
#[ignore = "requires an explicit DeepSeek credential and disposable PostgreSQL database"]
async fn deepseek_responses_produces_persisted_read_only_sre_diagnosis() {
    let database_url = std::env::var(LIVE_DATABASE_URL_ENV).expect("live test database URL must be explicit");
    assert!(
        std::env::var(LIVE_API_KEY_ENV).is_ok_and(|value| !value.trim().is_empty()),
        "live DeepSeek credential must be injected explicitly"
    );

    let repository = PostgresRepository::connect(&database_url, 2)
        .await
        .expect("disposable database and migrations");
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    let incident_id = IncidentId::new();
    seed_diagnosis_scope(&repository, tenant_id, cluster_id, incident_id).await;

    let mut profile = rocketmq_sre_model_gateway::builtin_provider_profiles()
        .into_iter()
        .find(|profile| profile.id == "deepseek-responses")
        .expect("DeepSeek Responses profile fixture");
    profile.credential_ref = Some(
        SecretReference::parse(&format!("env://{LIVE_API_KEY_ENV}"))
            .expect("qualification environment secret reference"),
    );
    profile.validate().expect("DeepSeek Responses profile");

    let inner = HttpModelTransport::new(
        HttpTransportConfig::default()
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(45))
            .with_body_limits(256 * 1024, 256 * 1024),
    )
    .expect("DeepSeek HTTPS transport");
    let transport = Arc::new(InspectingTransport::new(inner));
    let mut service = ModelGatewayService::for_tests(repository.clone(), vec![profile.clone()], transport.clone());
    service.config = Arc::new(ModelRuntimeConfig {
        enabled: true,
        profiles: vec![profile],
        max_fallbacks: 0,
        request_timeout: Duration::from_secs(45),
        max_request_bytes: 256 * 1024,
        max_response_bytes: 256 * 1024,
        allow_insecure_non_loopback_http: false,
        secret_provider: ModelSecretProviderConfig::Development {
            env_prefix: "ROCKETMQ_SRE_LIVE_DEEPSEEK_".to_owned(),
            file_root: None,
        },
    });
    service.secret_provider = Arc::new(DevSecretProvider::new(true, "ROCKETMQ_SRE_LIVE_DEEPSEEK_", None));
    let runtime = RuntimeContext::from_current("sre-live-deepseek-qualification");
    let service_context = runtime.service_context("model-secret-resolution");
    service.metadata_io = Some(service_context.metadata_io().clone());

    let auth = AuthContext {
        tenant_id,
        subject: "deepseek-qualification".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["model-governance".to_owned()]),
    };
    service
        .certify_profile_for_tests(&auth, "deepseek-responses", CorrelationId::new())
        .await
        .expect("fixture-backed operator certification");

    let correlation_id = CorrelationId::new();
    let evidence = synthetic_lag_evidence(tenant_id, cluster_id, correlation_id);
    let evidence_id = evidence.evidence_id;
    let decision = service
        .diagnose(
            &auth,
            incident_id,
            cluster_id,
            "Consumer lag is increasing on the qualification group",
            "consumer-lag.v1",
            &json!({
                "finding": "consumer_lag_positive",
                "lag": 42,
                "mutation_allowed": false,
            }),
            &[evidence],
            correlation_id,
        )
        .await
        .expect("real DeepSeek AI SRE diagnosis");

    assert_eq!(decision.mode, "model_assisted");
    assert_eq!(decision.reason, MODEL_ADOPTED_REASON);
    assert!(decision.input_tokens > 0);
    assert!(decision.output_tokens > 0);
    assert!(decision.schema_repairs_used <= 1);
    let diagnosis: StructuredModelDiagnosis = serde_json::from_value(
        decision
            .conclusion
            .clone()
            .expect("model-assisted conclusion must be present"),
    )
    .expect("locally validated diagnosis shape");
    assert_eq!(diagnosis.cited_evidence_ids, vec![evidence_id]);
    assert!(diagnosis.validate(&[evidence_id]));

    let invocation_id = decision.invocation_id.expect("persisted model invocation ID");
    let invocations = service
        .invocations(
            &auth,
            &ModelInvocationListQuery {
                cluster_id,
                incident_id: Some(incident_id),
                limit: Some(10),
            },
        )
        .await
        .expect("persisted model invocation page");
    let successful = invocations
        .items
        .iter()
        .find(|item| item.id == invocation_id)
        .expect("successful invocation provenance");
    assert_eq!(successful.actual_model, "deepseek-v4-flash");
    assert_eq!(successful.correlation_id, Some(correlation_id));
    assert_eq!(successful.error_code, None);
    assert!(successful.input_tokens.is_some_and(|tokens| tokens > 0));
    assert!(successful.output_tokens.is_some_and(|tokens| tokens > 0));
    assert!(invocations.items.len() <= 2);
    assert!((1..=2).contains(&transport.calls()));

    let credential_reference: String = sqlx::query_scalar(
        "SELECT credential_ref FROM model_profiles WHERE tenant_id = $1 AND profile_name = 'deepseek-responses'",
    )
    .bind(tenant_id.as_uuid())
    .fetch_one(&repository.pool)
    .await
    .expect("reference-only model profile");
    assert_eq!(credential_reference, format!("env://{LIVE_API_KEY_ENV}"));

    let report = json!({
        "schema_version": "rocketmq-sre.deepseek-diagnosis-test-result.v1",
        "provider": "deepseek",
        "protocol": "responses_api",
        "model": "deepseek-v4-flash",
        "mode": decision.mode,
        "authorized_evidence_citations": true,
        "cited_evidence_count": diagnosis.cited_evidence_ids.len(),
        "input_tokens_present": decision.input_tokens > 0,
        "output_tokens_present": decision.output_tokens > 0,
        "schema_repairs": decision.schema_repairs_used,
        "model_network_calls": transport.calls(),
        "invocation_persisted": true,
        "tool_calls": 0,
        "mutation_calls": 0,
        "execution_eligible": false,
        "sensitive_payloads_recorded": false,
    });
    eprintln!("DEEPSEEK_DIAGNOSIS_QUALIFICATION_OK {report}");

    drop(service);
    drop(service_context);
    let shutdown = runtime.shutdown_tasks(Duration::from_secs(5)).await;
    assert!(shutdown.is_healthy(), "qualification runtime must shut down cleanly");
}

async fn seed_diagnosis_scope(
    repository: &PostgresRepository,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    incident_id: IncidentId,
) {
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'qualification', 'local', '5.3.0', 'test',
            'deepseek-qualification', 'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .bind(format!("deepseek-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("qualification cluster");
    sqlx::query(
        "INSERT INTO sre_incidents (
            id, tenant_id, cluster_id, title, symptom_family, fingerprint,
            status, created_by_subject, created_at, updated_at
         ) VALUES (
            $1, $2, $3, 'DeepSeek diagnosis qualification', 'consumer-lag',
            $4, 'diagnosing', 'deepseek-qualification', NOW(), NOW()
         )",
    )
    .bind(incident_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(cluster_id.as_uuid())
    .bind(format!("deepseek-{incident_id}"))
    .execute(&repository.pool)
    .await
    .expect("qualification incident");
}

fn synthetic_lag_evidence(
    tenant_id: TenantId,
    cluster_id: ClusterId,
    correlation_id: CorrelationId,
) -> EvidenceSnapshot {
    let observed_at = Utc::now();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id,
        tenant_id,
        cluster_id,
        source: "qualification.connector.consumer_lag".to_owned(),
        resource: "topic/sre-qualification/group/sre-qualification".to_owned(),
        time_range: TimeRange::new(observed_at - chrono::Duration::minutes(1), observed_at)
            .expect("qualification time range"),
    };
    let mut snapshot = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        observed_at,
        EvidenceContent::Inline(json!({
            "topic": "sre-qualification",
            "consumer_group": "sre-qualification",
            "lag": 42,
            "trend": "increasing",
            "message_body": FORBIDDEN_EVIDENCE_MARKER,
            "access_token": FORBIDDEN_EVIDENCE_MARKER,
        })),
    )
    .expect("canonical qualification evidence");
    snapshot.sensitivity = Sensitivity::Internal;
    snapshot
}
