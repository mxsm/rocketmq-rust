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

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rocketmq_sre_contracts::*;
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{
    PostgresRepository,
    api::{AppState, capability_verification_test_state},
    auth::{AuthContext, AuthService},
    workflow::InvestigationCreateRequest,
};

const ISSUER: &str = "https://d14-oidc-fixture.invalid";
const AUDIENCE: &str = "rocketmq-sre-control-plane";
// Public modulus matching the existing synthetic dev fixture key.
const RSA_MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

pub(super) struct Fixture {
    pub repository: PostgresRepository,
    pub state: AppState,
    pub auth: AuthContext,
    pub request: CreateInvestigationRunRequest,
}

impl Fixture {
    pub fn router(&self) -> Router {
        super::routes().with_state(self.state.clone())
    }

    pub async fn create(&self) -> CreateInvestigationRunResponse {
        let (status, body) = call(
            self.router(),
            "POST",
            &format!("/v1/investigations/{}/runs", self.request.scope.investigation_id),
            Some(&self.auth),
            Some(json!(self.request)),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        serde_json::from_value(body).unwrap()
    }
}

pub(super) async fn fixture() -> Fixture {
    let url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit disposable Docker PostgreSQL");
    let repository = PostgresRepository::connect(&url, 6).await.unwrap();
    fixture_in(repository).await
}

pub(super) async fn fixture_in(repository: PostgresRepository) -> Fixture {
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    sqlx::query("INSERT INTO clusters(id,tenant_id,external_cluster_key,environment,region,rocketmq_version,deployment_mode,owner_name,requested_access_profile,effective_access_profile,onboarding_state) VALUES($1,$2,$3,'test','local','test','test','d14-transport-fixture','read_only','read_only','ready_read_only')")
        .bind(cluster_id.as_uuid()).bind(tenant_id.to_string()).bind(format!("d14-{cluster_id}"))
        .execute(&repository.pool).await.unwrap();
    let auth = AuthContext {
        tenant_id,
        subject: "d14-operator".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["diagnose".to_owned()]),
    };
    let parent = repository
        .create_investigation(
            &auth,
            &InvestigationCreateRequest {
                cluster_id,
                conversation_id: None,
                title: "Synthetic D14 persistence transport".to_owned(),
                resource: None,
                symptom_family: "transport-regression".to_owned(),
            },
            CorrelationId::new(),
        )
        .await
        .unwrap()
        .investigation;
    let mut state = capability_verification_test_state(repository.clone()).unwrap();
    state.auth = AuthService::from_test_jwks(
        ISSUER,
        AUDIENCE,
        serde_json::from_value(
            json!({"keys":[{"kty":"RSA","kid":"d14-key","alg":"RS256","use":"sig","n":RSA_MODULUS,"e":"AQAB"}]}),
        )
        .unwrap(),
    );
    let now = Utc::now();
    let request = CreateInvestigationRunRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        run_id: InvestigationRunId::new(),
        scope: InvestigationRunScope {
            tenant_id,
            cluster_id,
            investigation_id: parent.id,
            resource: None,
            time_range: TimeRange::new(now - Duration::minutes(20), now - Duration::minutes(10)).unwrap(),
        },
        packs: vec![InvestigationPackRef {
            pack_id: "broker-health.v1".to_owned(),
            pack_version: "1.0.0".to_owned(),
        }],
        budget: InvestigationBudget {
            limit: InvestigationBudgetCost {
                read_queries: 12,
                model_calls: 8,
                evidence_items: 8,
                output_bytes: 4096,
            },
            max_steps: 8,
            max_in_flight: 2,
            max_attempts_per_step: 3,
            max_query_retries: 2,
        },
        deadline: now + Duration::minutes(15),
        idempotency_key: "d14-create".to_owned(),
    };
    Fixture {
        repository,
        state,
        auth,
        request,
    }
}

pub(super) async fn call(
    router: Router,
    method: &str,
    path: &str,
    auth: Option<&AuthContext>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("d14-key".to_owned());
        let token = jsonwebtoken::encode(
            &header,
            &json!({
                "iss":ISSUER,"aud":AUDIENCE,"sub":auth.subject,"exp":Utc::now().timestamp()+3600,
                "scope":"rocketmq:read","roles":auth.roles,"rocketmq_tenant":auth.tenant_id.to_string(),
                "rocketmq_clusters":auth.clusters.iter().map(ToString::to_string).collect::<Vec<_>>(),
            }),
            &EncodingKey::from_rsa_pem(include_bytes!(
                "../../../../../deploy/dev/fixtures/oauth-private-key.pem"
            ))
            .unwrap(),
        )
        .unwrap();
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = router
        .oneshot(
            request
                .body(body.map_or_else(Body::empty, |body| Body::from(serde_json::to_vec(&body).unwrap())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 8 * 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    if !status.is_success() {
        let encoded = body.to_string();
        assert!(!encoded.contains("private-marker"));
        assert!(!encoded.contains("SELECT "));
        assert!(!encoded.contains("postgresql://"));
        assert!(body["message"].is_string(), "safe envelope: {body}");
    }
    (status, body)
}

pub(super) fn command(run: &InvestigationRun, command: InvestigationCommand, key: &str) -> InvestigationCommandRequest {
    InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: run.id,
        expected_revision: run.revision,
        idempotency_key: key.to_owned(),
        reason: "bounded transport regression".to_owned(),
        command,
    }
}

pub(super) async fn saved_reference_fixture() -> (Fixture, EvidenceId, EvidenceId) {
    use crate::{
        evidence::PersistEvidenceRequest,
        workflow::{
            ConversationCreateRequest, ConversationTurnRequest,
            conversation_repository::{ConversationCompletion, InvestigationDiagnosisDraft},
        },
    };

    let mut fixture = fixture().await;
    let view = fixture
        .repository
        .create_conversation(
            &fixture.auth,
            &ConversationCreateRequest {
                cluster_id: fixture.request.scope.cluster_id,
                question: "Synthetic saved evidence binding; no target read".to_owned(),
                resource: Some("brokers/broker-a".to_owned()),
                persist_investigation: true,
            },
            CorrelationId::new(),
        )
        .await
        .unwrap();
    let parent = view.investigation.unwrap();
    fixture.request.scope.investigation_id = parent.id;
    fixture.request.scope.resource = Some(InvestigationResource {
        kind: ResourceKind::Broker,
        key: "broker-a".to_owned(),
    });
    let mut ids = Vec::new();
    for resource in ["store-pressure/broker-a", "store-pressure/broker-b"] {
        let mut evidence = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: fixture.auth.tenant_id,
                cluster_id: fixture.request.scope.cluster_id,
                source: "prometheus".to_owned(),
                resource: resource.to_owned(),
                time_range: fixture.request.scope.time_range.clone(),
            },
            current_evidence_schema(),
            Utc::now(),
            EvidenceContent::Inline(json!({"synthetic":true})),
        )
        .unwrap();
        evidence.exposure = EvidenceExposure::Synthetic;
        evidence.partial = true;
        evidence.coverage = CoverageStatus::Partial;
        evidence.freshness_seconds = 10_000;
        let snapshot = fixture
            .state
            .evidence
            .persist(
                &fixture.auth,
                PersistEvidenceRequest {
                    investigation_id: Some(parent.id),
                    incident_id: None,
                    evidence,
                },
            )
            .await
            .unwrap();
        // Persistence canonicalizes PostgreSQL timestamp precision. Bind the
        // run to the actual saved window, as a client selecting saved Evidence does.
        fixture.request.scope.time_range = snapshot.time_range.clone();
        ids.push(snapshot.evidence_id);
    }
    let turn = fixture
        .repository
        .begin_conversation_turn(
            &fixture.auth,
            &view.conversation,
            &ConversationTurnRequest {
                question: "Synthetic rule-only record".to_owned(),
                resource: Some("brokers/broker-a".to_owned()),
                window_seconds: Some(300),
            },
            None,
            CorrelationId::new(),
        )
        .await
        .unwrap();
    fixture
        .repository
        .complete_conversation_turn(
            &fixture.auth,
            &turn,
            ConversationCompletion {
                status: ConversationTurnStatus::Answered,
                intent: None,
                answer: "Synthetic fixture remains inconclusive; not production validation".to_owned(),
                mode: ConversationAnswerMode::RulesOnly,
                citations: vec![],
                evidence_ids: vec![ids[0]],
                model_invocation_id: None,
                partial: true,
                warnings: vec!["synthetic_fixture".to_owned()],
                diagnosis: Some(InvestigationDiagnosisDraft {
                    investigation_id: parent.id,
                    pack_id: "broker-health.v1".to_owned(),
                    pack_version: "1.0.0".to_owned(),
                    status: InvestigationDiagnosisStatus::Inconclusive,
                    rule_result: json!({"synthetic":true}),
                    hypotheses: json!([]),
                    evidence_ids: vec![ids[0]],
                    primary_model_invocation_id: None,
                    partial: true,
                }),
            },
        )
        .await
        .unwrap();
    (fixture, ids[0], ids[1])
}
