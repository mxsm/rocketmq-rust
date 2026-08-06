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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::http::header::LOCATION;
use axum::response::IntoResponse;
use axum::response::Response;
use rocketmq_runtime::RuntimeContext;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_model_gateway::AsyncModelTransport;
use rocketmq_sre_model_gateway::DevSecretProvider;
use rocketmq_sre_model_gateway::HttpModelTransport;
use rocketmq_sre_model_gateway::HttpTransportConfig;
use rocketmq_sre_model_gateway::ProviderDialect;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::ProviderErrorCode;
use rocketmq_sre_model_gateway::SecretReference;
use rocketmq_sre_model_gateway::StreamBounds;
use rocketmq_sre_model_gateway::TransportFuture;
use rocketmq_sre_model_gateway::TransportRequest;
use rocketmq_sre_model_gateway::TransportStreamFuture;
use serde_json::json;
use tokio::net::TcpListener;

use super::MODEL_ADOPTED_REASON;
use super::ModelGatewayService;
use super::ModelRuntimeConfig;
use super::ModelSecretProviderConfig;
use super::StructuredModelDiagnosis;
use super::live_deepseek::seed_diagnosis_scope;
use super::live_deepseek::synthetic_lag_evidence;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::models::model::ModelInvocationListQuery;

const LIVE_API_KEY_ENV: &str = "ROCKETMQ_SRE_LIVE_DEEPSEEK_API_KEY";
const LOOPBACK_CREDENTIAL_ENV: &str = "ROCKETMQ_SRE_LIVE_DEEPSEEK_LOOPBACK_TOKEN";
const LIVE_DATABASE_URL_ENV: &str = "ROCKETMQ_SRE_TEST_DATABASE_URL";
const DEEPSEEK_ENDPOINT: &str = "https://api.deepseek.com";

#[derive(Default)]
struct FaultProviderState {
    transient_calls: AtomicUsize,
    policy_calls: AtomicUsize,
    invalid_schema_calls: AtomicUsize,
    invalid_citation_calls: AtomicUsize,
    unavailable_calls: AtomicUsize,
}

impl FaultProviderState {
    fn calls(&self, scenario: &str) -> usize {
        match scenario {
            "transient" => self.transient_calls.load(Ordering::SeqCst),
            "policy" => self.policy_calls.load(Ordering::SeqCst),
            "invalid-schema" => self.invalid_schema_calls.load(Ordering::SeqCst),
            "invalid-citation" => self.invalid_citation_calls.load(Ordering::SeqCst),
            "unavailable" => self.unavailable_calls.load(Ordering::SeqCst),
            _ => 0,
        }
    }
}

struct QualificationTransport {
    inner: HttpModelTransport,
    loopback_authority: String,
    capability_calls: AtomicUsize,
    deepseek_calls: AtomicUsize,
}

impl QualificationTransport {
    fn new(inner: HttpModelTransport, loopback_authority: String) -> Self {
        Self {
            inner,
            loopback_authority,
            capability_calls: AtomicUsize::new(0),
            deepseek_calls: AtomicUsize::new(0),
        }
    }

    fn deepseek_calls(&self) -> usize {
        self.deepseek_calls.load(Ordering::SeqCst)
    }

    fn capability_calls(&self) -> usize {
        self.capability_calls.load(Ordering::SeqCst)
    }

    fn authorize(&self, request: &TransportRequest) -> Result<(), ProviderError> {
        if request.dialect != ProviderDialect::DeepSeekResponses || request.path != "/responses" {
            return Err(ProviderError::policy_denied(
                "provider-failover qualification received an unsupported protocol surface",
            ));
        }
        let serialized = serde_json::to_string(&request.body).unwrap_or_default();
        if serialized.contains("message_body")
            || serialized.contains("access_token")
            || serialized.contains("qualification-body-must-not-leave")
        {
            return Err(ProviderError::policy_denied(
                "provider-failover qualification received sensitive evidence",
            ));
        }
        if request.endpoint == DEEPSEEK_ENDPOINT {
            if request.credential.is_none() {
                return Err(ProviderError::policy_denied(
                    "live secondary qualification requires a process-local credential",
                ));
            }
            self.deepseek_calls.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        if request.endpoint == format!("{}/capability", self.loopback_authority) && request.credential.is_some() {
            self.capability_calls.fetch_add(1, Ordering::SeqCst);
            return Err(ProviderError::new(
                ProviderErrorCode::CapabilityUnsupported,
                "qualification primary does not support the requested capability",
            ));
        }
        let loopback_prefix = format!("{}/", self.loopback_authority);
        if request.endpoint.starts_with(&loopback_prefix) && request.credential.is_some() {
            return Ok(());
        }
        Err(ProviderError::policy_denied(
            "provider-failover qualification attempted an unapproved endpoint",
        ))
    }
}

impl AsyncModelTransport for QualificationTransport {
    fn invoke(&self, request: TransportRequest) -> TransportFuture<'_> {
        if let Err(error) = self.authorize(&request) {
            return Box::pin(async move { Err(error) });
        }
        self.inner.invoke(request)
    }

    fn invoke_stream(
        &self,
        request: TransportRequest,
        bounds: StreamBounds,
        cancellation: rocketmq_sre_model_gateway::CancellationToken,
    ) -> TransportStreamFuture<'_> {
        if let Err(error) = self.authorize(&request) {
            return Box::pin(async move { Err(error) });
        }
        self.inner.invoke_stream(request, bounds, cancellation)
    }
}

async fn fault_provider(State(state): State<Arc<FaultProviderState>>, uri: Uri) -> Response {
    let scenario = uri.path().trim_start_matches('/').split('/').next().unwrap_or_default();
    match scenario {
        "transient" => {
            state.transient_calls.fetch_add(1, Ordering::SeqCst);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": {"type": "service_unavailable_error"}})),
            )
                .into_response()
        }
        "policy" => {
            state.policy_calls.fetch_add(1, Ordering::SeqCst);
            Response::builder()
                .status(StatusCode::TEMPORARY_REDIRECT)
                .header(LOCATION, "/unapproved-redirect")
                .body(axum::body::Body::empty())
                .expect("qualification redirect response")
        }
        "invalid-schema" => {
            state.invalid_schema_calls.fetch_add(1, Ordering::SeqCst);
            Json(json!({
                "id": "resp-qualification-invalid",
                "object": "response",
                "status": "completed",
                "model": "qualification-local",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"unexpected\":true}",
                        "annotations": []
                    }]
                }],
                "usage": {"input_tokens": 4, "output_tokens": 3, "total_tokens": 7}
            }))
            .into_response()
        }
        "invalid-citation" => {
            state.invalid_citation_calls.fetch_add(1, Ordering::SeqCst);
            Json(json!({
                "id": "resp-qualification-invalid-citation",
                "object": "response",
                "status": "completed",
                "model": "qualification-local",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": "{\"summary\":\"Bounded fixture\",\"assessment\":\"Citation is intentionally outside the authorized set\",\"confidence_percent\":50,\"cited_evidence_ids\":[\"00000000-0000-0000-0000-000000000000\"],\"recommended_read_only_queries\":[],\"rationale\":\"Qualification fixture\"}",
                        "annotations": []
                    }]
                }],
                "usage": {"input_tokens": 9, "output_tokens": 8, "total_tokens": 17}
            }))
            .into_response()
        }
        "unavailable" => {
            state.unavailable_calls.fetch_add(1, Ordering::SeqCst);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": {"type": "service_unavailable_error"}})),
            )
                .into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

#[tokio::test]
async fn transient_fixture_returns_a_json_service_unavailable_envelope() {
    let state = Arc::new(FaultProviderState::default());
    let response = fault_provider(State(state.clone()), Uri::from_static("/transient/responses")).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 1_024)
        .await
        .expect("bounded loopback error body");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("loopback JSON error envelope");
    assert_eq!(value["error"]["type"], "service_unavailable_error");
    assert_eq!(state.calls("transient"), 1);
}

#[tokio::test]
#[ignore = "requires an explicit DeepSeek credential and disposable PostgreSQL database"]
async fn transient_primary_falls_back_to_live_deepseek_and_failures_remain_rules_only() {
    let database_url = std::env::var(LIVE_DATABASE_URL_ENV).expect("live test database URL must be explicit");
    assert!(
        std::env::var(LIVE_API_KEY_ENV).is_ok_and(|value| !value.trim().is_empty()),
        "live DeepSeek credential must be injected explicitly"
    );
    assert!(
        std::env::var(LOOPBACK_CREDENTIAL_ENV).is_ok_and(|value| !value.trim().is_empty()),
        "loopback fixture credential must be generated explicitly"
    );
    let repository = PostgresRepository::connect(&database_url, 2)
        .await
        .expect("disposable database and migrations");
    let runtime = RuntimeContext::from_current("sre-live-provider-failover-qualification");
    let service_context = runtime.service_context("provider-failover-qualification");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback fault provider");
    let address = listener.local_addr().expect("loopback fault provider address");
    let loopback_authority = format!("http://{address}");
    let fault_state = Arc::new(FaultProviderState::default());
    let app = Router::new().fallback(fault_provider).with_state(fault_state.clone());
    service_context
        .spawn_cancellable_service("provider-failover-loopback-fixture", async move {
            let _ = axum::serve(listener, app).await;
        })
        .expect("own loopback provider lifecycle");

    let transport = Arc::new(QualificationTransport::new(
        HttpModelTransport::new(
            HttpTransportConfig::default()
                .with_timeouts(Duration::from_secs(5), Duration::from_secs(45))
                .with_body_limits(256 * 1024, 256 * 1024),
        )
        .expect("bounded provider qualification transport"),
        loopback_authority.clone(),
    ));

    let live = run_live_fallback(
        &repository,
        &service_context,
        transport.clone(),
        &loopback_authority,
        fault_state.clone(),
    )
    .await;
    let policy = run_rules_only_scenario(
        &repository,
        &service_context,
        transport.clone(),
        &loopback_authority,
        fault_state.clone(),
        "policy",
        true,
    )
    .await;
    let unsupported_capability = run_rules_only_scenario(
        &repository,
        &service_context,
        transport.clone(),
        &loopback_authority,
        fault_state.clone(),
        "capability",
        true,
    )
    .await;
    let invalid_schema = run_rules_only_scenario(
        &repository,
        &service_context,
        transport.clone(),
        &loopback_authority,
        fault_state.clone(),
        "invalid-schema",
        true,
    )
    .await;
    let invalid_citation = run_rules_only_scenario(
        &repository,
        &service_context,
        transport.clone(),
        &loopback_authority,
        fault_state.clone(),
        "invalid-citation",
        true,
    )
    .await;
    let unavailable = run_rules_only_scenario(
        &repository,
        &service_context,
        transport.clone(),
        &loopback_authority,
        fault_state,
        "unavailable",
        false,
    )
    .await;
    assert!((1..=2).contains(&transport.deepseek_calls()));

    let report = json!({
        "schema_version": "rocketmq-sre.provider-failover-test-result.v1",
        "scenarios": {
            "transient_primary_to_live_secondary": live,
            "policy_denial_stops_fallback": policy,
            "unsupported_capability_stops_fallback": unsupported_capability,
            "invalid_schema_stops_fallback": invalid_schema,
            "invalid_citation_stops_fallback": invalid_citation,
            "all_unavailable_rules_only": unavailable,
        },
        "provider_certification": {
            "deepseek": "live_smoke_passed",
            "zhipu_glm": "descriptor_only",
            "kimi_moonshot": "descriptor_only",
        },
        "mutation_calls": 0,
        "executor_calls": 0,
        "execution_agent_calls": 0,
        "execution_eligible": false,
        "sensitive_payloads_recorded": false,
    });
    eprintln!("PROVIDER_FAILOVER_QUALIFICATION_OK {report}");

    drop(service_context);
    let shutdown = runtime.shutdown_tasks(Duration::from_secs(5)).await;
    assert!(shutdown.is_healthy(), "qualification runtime must shut down cleanly");
}

async fn run_live_fallback(
    repository: &PostgresRepository,
    service_context: &rocketmq_runtime::ChildServiceContext,
    transport: Arc<QualificationTransport>,
    loopback_authority: &str,
    fault_state: Arc<FaultProviderState>,
) -> serde_json::Value {
    let primary = local_profile(
        "qualification-primary-transient",
        &format!("{loopback_authority}/transient"),
    );
    let secondary = live_deepseek_profile();
    let (service, auth, cluster_id, incident_id, evidence) = scenario_service(
        repository,
        service_context,
        transport.clone(),
        vec![primary, secondary],
        1,
    )
    .await;
    let primary_before = fault_state.calls("transient");
    let secondary_before = transport.deepseek_calls();
    let decision = diagnose(&service, &auth, cluster_id, incident_id, &evidence).await;
    assert_eq!(decision.mode, "model_assisted");
    assert_eq!(decision.reason, MODEL_ADOPTED_REASON);
    assert!(
        decision.schema_repairs_used <= 1,
        "live fallback permits only one bounded same-provider schema repair"
    );
    let diagnosis: StructuredModelDiagnosis =
        serde_json::from_value(decision.conclusion.clone().expect("model-assisted fallback conclusion"))
            .expect("locally validated fallback diagnosis");
    assert_eq!(diagnosis.cited_evidence_ids, vec![evidence.evidence_id]);
    let invocations = invocations(&service, &auth, cluster_id, incident_id).await;
    let primary_attempts = fault_state.calls("transient") - primary_before;
    let secondary_attempts = transport.deepseek_calls() - secondary_before;
    assert_eq!(primary_attempts, 1);
    assert!((1..=2).contains(&secondary_attempts));
    assert_eq!(invocations.len(), primary_attempts + secondary_attempts);
    let failed = invocations
        .iter()
        .find(|invocation| invocation.error_code.as_deref() == Some("service_unavailable"))
        .expect("persisted transient primary failure");
    let completed = invocations
        .iter()
        .find(|invocation| invocation.error_code.is_none())
        .expect("persisted live secondary success");
    let secondary_failed_attempts = invocations
        .iter()
        .filter(|invocation| invocation.actual_model == "deepseek-v4-flash" && invocation.error_code.is_some())
        .count();
    assert_eq!(completed.actual_model, "deepseek-v4-flash");
    assert_eq!(completed.fallback_chain, vec![failed.actual_profile_id]);
    assert_eq!(secondary_attempts, secondary_failed_attempts + 1);
    assert_eq!(secondary_failed_attempts, usize::from(decision.schema_repairs_used));
    json!({
        "result": "model_assisted",
        "primary_attempts": primary_attempts,
        "primary_error": "service_unavailable",
        "secondary_attempts": secondary_attempts,
        "diagnosis_attempts": 1,
        "rules_only_attempts": 0,
        "secondary_failed_attempts": secondary_failed_attempts,
        "schema_repairs": decision.schema_repairs_used,
        "actual_provider": "deepseek",
        "actual_model": "deepseek-v4-flash",
        "fallback_chain": ["qualification-primary-transient"],
        "authorized_evidence_citations": true,
        "cited_evidence_count": diagnosis.cited_evidence_ids.len(),
        "invocation_persisted": true,
    })
}

async fn run_rules_only_scenario(
    repository: &PostgresRepository,
    service_context: &rocketmq_runtime::ChildServiceContext,
    transport: Arc<QualificationTransport>,
    loopback_authority: &str,
    fault_state: Arc<FaultProviderState>,
    scenario: &str,
    include_secondary: bool,
) -> serde_json::Value {
    let primary_name = format!("qualification-primary-{scenario}");
    let primary = local_profile(&primary_name, &format!("{loopback_authority}/{scenario}"));
    let mut profiles = vec![primary];
    if include_secondary {
        profiles.push(live_deepseek_profile());
    }
    let (service, auth, cluster_id, incident_id, evidence) = scenario_service(
        repository,
        service_context,
        transport.clone(),
        profiles,
        usize::from(include_secondary),
    )
    .await;
    let primary_before = if scenario == "capability" {
        transport.capability_calls()
    } else {
        fault_state.calls(scenario)
    };
    let secondary_before = transport.deepseek_calls();
    let decision = diagnose(&service, &auth, cluster_id, incident_id, &evidence).await;
    assert_eq!(decision.mode, "rules_only");
    assert!(decision.invocation_id.is_none());
    let primary_attempts = if scenario == "capability" {
        transport.capability_calls() - primary_before
    } else {
        fault_state.calls(scenario) - primary_before
    };
    let secondary_attempts = transport.deepseek_calls() - secondary_before;
    assert_eq!(
        secondary_attempts, 0,
        "non-fallback-safe or exhausted scenarios must not call DeepSeek"
    );
    let expected_attempts = if matches!(scenario, "invalid-schema" | "invalid-citation") {
        2
    } else {
        1
    };
    assert_eq!(primary_attempts, expected_attempts);
    let error_code = match scenario {
        "policy" => "policy_denied",
        "capability" => "capability_unsupported",
        "invalid-schema" => "schema_validation_failed",
        "invalid-citation" => "schema_validation_failed",
        "unavailable" => "service_unavailable",
        _ => panic!("unsupported qualification scenario"),
    };
    let invocations = invocations(&service, &auth, cluster_id, incident_id).await;
    assert_eq!(invocations.len(), primary_attempts);
    assert!(
        invocations
            .iter()
            .all(|invocation| invocation.error_code.as_deref() == Some(error_code))
    );
    let mut result = json!({
        "result": "rules_only",
        "primary_attempts": primary_attempts,
        "primary_error": error_code,
        "secondary_attempts": secondary_attempts,
    });
    if matches!(scenario, "invalid-schema" | "invalid-citation") {
        result["schema_repairs"] = json!(decision.schema_repairs_used);
    }
    if scenario == "unavailable" {
        result["execution_eligible"] = json!(false);
    }
    result
}

async fn scenario_service(
    repository: &PostgresRepository,
    service_context: &rocketmq_runtime::ChildServiceContext,
    transport: Arc<QualificationTransport>,
    profiles: Vec<rocketmq_sre_model_gateway::ProviderProfile>,
    max_fallbacks: usize,
) -> (
    ModelGatewayService,
    AuthContext,
    ClusterId,
    IncidentId,
    rocketmq_sre_contracts::EvidenceSnapshot,
) {
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    let incident_id = IncidentId::new();
    seed_diagnosis_scope(repository, tenant_id, cluster_id, incident_id).await;
    let auth = AuthContext {
        tenant_id,
        subject: "provider-failover-qualification".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["model-governance".to_owned()]),
    };
    let mut service = ModelGatewayService::for_tests(repository.clone(), profiles.clone(), transport);
    service.config = Arc::new(ModelRuntimeConfig {
        enabled: true,
        profiles: profiles.clone(),
        max_fallbacks,
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
    service.metadata_io = Some(service_context.metadata_io().clone());
    for profile in &profiles {
        service
            .certify_profile_for_tests(&auth, &profile.id, CorrelationId::new())
            .await
            .expect("fixture-backed operator certification");
    }
    let evidence = synthetic_lag_evidence(tenant_id, cluster_id, CorrelationId::new());
    (service, auth, cluster_id, incident_id, evidence)
}

async fn diagnose(
    service: &ModelGatewayService,
    auth: &AuthContext,
    cluster_id: ClusterId,
    incident_id: IncidentId,
    evidence: &rocketmq_sre_contracts::EvidenceSnapshot,
) -> super::ModelDiagnosisDecision {
    service
        .diagnose(
            auth,
            incident_id,
            cluster_id,
            "Consumer lag is increasing on the qualification group",
            "consumer-lag.v1",
            &json!({
                "finding": "consumer_lag_positive",
                "lag": 42,
                "mutation_allowed": false,
            }),
            std::slice::from_ref(evidence),
            evidence.correlation_id,
        )
        .await
        .expect("bounded provider-failover diagnosis")
}

async fn invocations(
    service: &ModelGatewayService,
    auth: &AuthContext,
    cluster_id: ClusterId,
    incident_id: IncidentId,
) -> Vec<crate::models::model::ModelInvocationView> {
    service
        .invocations(
            auth,
            &ModelInvocationListQuery {
                cluster_id,
                incident_id: Some(incident_id),
                conversation_id: None,
                limit: Some(10),
            },
        )
        .await
        .expect("persisted provider-failover invocation page")
        .items
}

fn local_profile(id: &str, endpoint: &str) -> rocketmq_sre_model_gateway::ProviderProfile {
    let mut profile = base_responses_profile();
    profile.id = id.to_owned();
    profile.endpoint = endpoint.to_owned();
    profile.model_family = "qualification-local".to_owned();
    profile.model = "qualification-local".to_owned();
    profile.model_revision = "fixture-v1".to_owned();
    profile.endpoint_instance = id.to_owned();
    profile.priority = 0;
    profile.credential_ref = Some(
        SecretReference::parse(&format!("env://{LOOPBACK_CREDENTIAL_ENV}"))
            .expect("loopback fixture environment secret reference"),
    );
    profile.validate().expect("loopback fault profile");
    profile
}

fn live_deepseek_profile() -> rocketmq_sre_model_gateway::ProviderProfile {
    let mut profile = base_responses_profile();
    profile.model = "deepseek-v4-flash".to_owned();
    profile.priority = 10;
    profile.credential_ref = Some(
        SecretReference::parse(&format!("env://{LIVE_API_KEY_ENV}"))
            .expect("qualification environment secret reference"),
    );
    profile.validate().expect("DeepSeek Responses fallback profile");
    profile
}

fn base_responses_profile() -> rocketmq_sre_model_gateway::ProviderProfile {
    rocketmq_sre_model_gateway::builtin_provider_profiles()
        .into_iter()
        .find(|profile| profile.id == "deepseek-responses")
        .expect("DeepSeek Responses profile fixture")
}
