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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::post,
};
use chrono::{DateTime, Duration, Timelike, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rocketmq_runtime::{RuntimeContext, TaskGroup};
use rocketmq_sre_contracts::*;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

use super::super::{ExecutorSubmissionClient, service::SupervisedExecutionService, target_scope::ActionTargetScope};
use crate::{
    PostgresRepository,
    api::{AppState, capability_verification_test_state},
    auth::{AuthContext, AuthService},
    workflow::{WorkflowEventBus, WorkflowService},
};

pub(super) const ACTION: &str = "observability.logger_level_ttl.v1";
pub(super) const TARGET: &str = "broker/127.0.0.1:10911";
pub(super) const KEY: &str = "d18-stable-preparation-signing-key-at-least-32-bytes";
const ISSUER: &str = "https://d18-oidc-fixture.invalid";
const AUDIENCE: &str = "rocketmq-sre-control-plane";
const RSA_MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

#[derive(Clone)]
pub(super) struct ReadObserver {
    pub(super) reads: Arc<AtomicUsize>,
    pub(super) writes: Arc<AtomicUsize>,
    pub(super) sample_at: Arc<Mutex<DateTime<Utc>>>,
    pub(super) accepted: Arc<Mutex<Option<ExecutionRequest>>>,
    pub(super) reconciliations: Arc<AtomicUsize>,
}

async fn precondition(
    State(observer): State<ReadObserver>,
    Json(request): Json<AgentReadRequest>,
) -> Json<AgentReadResult> {
    observer.reads.fetch_add(1, Ordering::SeqCst);
    Json(AgentReadResult {
        schema_version: EXECUTION_AGENT_SCHEMA_VERSION.to_owned(),
        action: request.action,
        target: request.target,
        precondition_hash: canonical_sha256(&request.parameters).unwrap(),
        ready: true,
        reason_codes: Vec::new(),
        resource_conditions: BTreeMap::from([
            ("logger_level_applied".to_owned(), false),
            ("ttl_restore_scheduled".to_owned(), false),
        ]),
        observed_at: *observer.sample_at.lock().unwrap(),
    })
}

async fn execution(
    State(observer): State<ReadObserver>,
    Json(request): Json<ExecutionRequest>,
) -> axum::response::Response {
    if observer.accepted.lock().unwrap().as_ref() == Some(&request) {
        observer.reconciliations.fetch_add(1, Ordering::SeqCst);
        return Json(json!({"execution_id":request.id,"state":"pending","replayed":true,"accepted_steps":request.plan.steps.len()}))
            .into_response();
    }
    observer.writes.fetch_add(1, Ordering::SeqCst);
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

pub(super) struct Fixture {
    pub(super) repository: PostgresRepository,
    pub(super) state: AppState,
    pub(super) auth: AuthContext,
    pub(super) request: ActionReadinessRequest,
    pub(super) incident_id: IncidentId,
    pub(super) diagnostic_id: EvidenceId,
    pub(super) source_revision: DiagnosisRevisionId,
    pub(super) clock: Arc<Mutex<DateTime<Utc>>>,
    pub(super) observer: ReadObserver,
    pub(super) group: TaskGroup,
    pub(super) scope: Arc<ActionTargetScope>,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        let url =
            std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit Docker PostgreSQL, no skipped claim");
        let repository = PostgresRepository::connect(&url, 6).await.unwrap();
        let old = super::super::service_tests::seed_fixture(&repository).await;
        let auth = AuthContext {
            tenant_id: old.tenant_id,
            subject: "d18-preparer-a".to_owned(),
            clusters: BTreeSet::from([old.cluster_id]),
            roles: BTreeSet::from(["operator".to_owned()]),
        };
        let sample_at = (Utc::now() - Duration::seconds(2))
            .with_nanosecond(123_456_789)
            .unwrap();
        let now = sample_at + Duration::seconds(1);
        let clock = Arc::new(Mutex::new(now));
        sqlx::query("UPDATE sre_incidents SET status = 'monitoring', resource = 'broker/broker-a' WHERE id = $1")
            .bind(old.incident_id.as_uuid())
            .execute(&repository.pool)
            .await
            .unwrap();
        let at = (sample_at - Duration::seconds(1)).with_nanosecond(123_456_000).unwrap();
        let mut snapshot = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: auth.tenant_id,
                cluster_id: old.cluster_id,
                source: "synthetic-d18-diagnostic".to_owned(),
                resource: "broker/broker-a".to_owned(),
                time_range: TimeRange::new(at, at).unwrap(),
            },
            current_evidence_schema(),
            at,
            EvidenceContent::Inline(
                json!({"provenance":"SyntheticFixture", "purpose":"action readiness engineering regression"}),
            ),
        )
        .unwrap();
        snapshot.freshness_seconds = 5;
        snapshot.exposure = EvidenceExposure::Synthetic;
        let snapshot = repository
            .persist_evidence(&auth, &snapshot, None, Some(old.incident_id), &snapshot.content_hash)
            .await
            .unwrap();
        let source_revision = DiagnosisRevisionId::new();
        sqlx::query("INSERT INTO diagnosis_revisions(id,incident_id,revision,status,rule_result,hypotheses,evidence_ids,partial,execution_eligible,created_at)
            VALUES($1,$2,3,'monitoring','{\"provenance\":\"SyntheticFixture\"}'::JSONB,'[]',ARRAY[$3]::UUID[],FALSE,FALSE,$4)")
            .bind(source_revision.as_uuid()).bind(old.incident_id.as_uuid()).bind(snapshot.evidence_id.as_uuid()).bind(now)
            .execute(&repository.pool).await.unwrap();
        let invocation = Uuid::new_v4();
        sqlx::query("INSERT INTO model_invocations(id,tenant_id,cluster_id,incident_id,diagnosis_revision_id,purpose,requested_profile_id,actual_profile_id,
                provider_family,model_family,model_revision,endpoint_instance,fallback_chain,prompt_version,schema_version,rationale,started_at,completed_at)
            SELECT $1,tenant_id,cluster_id,incident_id,$2,'primary_diagnosis',requested_profile_id,actual_profile_id,
                provider_family,model_family,model_revision,endpoint_instance,fallback_chain,prompt_version,schema_version,'SyntheticFixture',$3,$3
            FROM model_invocations WHERE diagnosis_revision_id=$4")
            .bind(invocation).bind(source_revision.as_uuid()).bind(now).bind(old.diagnosis_id.as_uuid())
            .execute(&repository.pool).await.unwrap();
        sqlx::query("UPDATE diagnosis_revisions SET primary_model_invocation_id=$2 WHERE id=$1")
            .bind(source_revision.as_uuid())
            .bind(invocation)
            .execute(&repository.pool)
            .await
            .unwrap();
        let (executor, group, observer) = start_observer(sample_at).await;
        let workflow = WorkflowService::new(repository.clone(), WorkflowEventBus::new(64));
        let controlled_clock = clock.clone();
        let scope = scope(&auth, "1");
        let service = SupervisedExecutionService::new_with_clock_and_executor(
            repository.clone(),
            workflow,
            KEY,
            executor,
            Arc::new(move || *controlled_clock.lock().unwrap()),
        )
        .unwrap()
        .with_target_scope(scope.clone());
        let mut state = capability_verification_test_state(repository.clone()).unwrap();
        state.supervised_execution = service;
        state.auth = AuthService::from_test_jwks(
            ISSUER,
            AUDIENCE,
            serde_json::from_value(
                json!({"keys":[{"kty":"RSA","kid":"d18-key","alg":"RS256","use":"sig","n":RSA_MODULUS,"e":"AQAB"}]}),
            )
            .unwrap(),
        );
        let request = ActionReadinessRequest {
            cluster_id: old.cluster_id,
            diagnosis_revision_id: source_revision,
            action_id: ACTION.to_owned(),
            descriptor_version: "1.0.0".to_owned(),
            resource: TARGET.to_owned(),
            parameters: json!({"component":"broker", "logger":"rocketmq_broker::processor", "level":"DEBUG", "ttl_seconds":120}),
            precondition_evidence_id: None,
            plan_id: None,
            plan_step_id: None,
        };
        Self {
            repository,
            state,
            auth,
            request,
            incident_id: old.incident_id,
            diagnostic_id: snapshot.evidence_id,
            source_revision,
            clock,
            observer,
            group,
            scope,
        }
    }

    pub(super) fn router(&self) -> Router {
        super::super::routes()
            .merge(crate::phase1_api::connector_ingest_routes())
            .with_state(self.state.clone())
    }

    pub(super) async fn call(&self, path: &str, actor: &AuthContext, body: Value) -> (StatusCode, Value) {
        call(self.router(), path, actor, body).await
    }

    pub(super) async fn readiness(&self, actor: &AuthContext, request: &ActionReadinessRequest) -> ActionReadinessView {
        let (status, body) = self
            .call(
                &format!("/v1/incidents/{}/action-readiness", self.incident_id),
                actor,
                json!(request),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        serde_json::from_value(body).unwrap()
    }

    pub(super) async fn confirm(&mut self) {
        let (status, body) = self
            .call(
                &format!(
                    "/v1/incidents/{}/diagnosis-revisions/{}/confirm-execution",
                    self.incident_id, self.source_revision
                ),
                &self.auth,
                json!({"human_confirmed":true,"reason":"SyntheticFixture independent diagnosis confirmation"}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        self.request.diagnosis_revision_id = serde_json::from_value(body["confirmed_revision_id"].clone()).unwrap();
    }

    pub(super) async fn prepare(&mut self) -> EvidenceSnapshot {
        let (status, body) = self
            .call(
                &format!("/v1/incidents/{}/execution-preconditions", self.incident_id),
                &self.auth,
                self.prepare_body(),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let evidence: EvidenceSnapshot = serde_json::from_value(body["evidence"].clone()).unwrap();
        self.request.precondition_evidence_id = Some(evidence.evidence_id);
        evidence
    }

    pub(super) fn prepare_body(&self) -> Value {
        json!({"cluster_id":self.request.cluster_id,"diagnosis_revision_id":self.request.diagnosis_revision_id,
            "action_id":self.request.action_id,"descriptor_version":self.request.descriptor_version,
            "resource":self.request.resource,"parameters":self.request.parameters})
    }

    pub(super) fn create_body(&self) -> Value {
        json!({"cluster_id":self.request.cluster_id,"incident_id":self.incident_id,"diagnosis_revision_id":self.request.diagnosis_revision_id,
            "steps":[{"action_id":self.request.action_id,"descriptor_version":self.request.descriptor_version,"resource":self.request.resource,
                "parameters":self.request.parameters,"evidence_ids":[self.diagnostic_id,self.request.precondition_evidence_id.unwrap()]}]})
    }

    pub(super) async fn draft(&self) -> ActionPlan {
        let (status, body) = self.call("/v1/plans", &self.auth, self.create_body()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        serde_json::from_value(body["plan"].clone()).unwrap()
    }

    pub(super) fn approver(&self) -> AuthContext {
        let mut actor = self.auth.clone();
        actor.subject = "d18-approver-b".to_owned();
        actor.roles = BTreeSet::from(["approver".to_owned()]);
        actor
    }

    pub(super) async fn approve(&self, plan: &ActionPlan) -> ApprovalGrant {
        let (status, body) = self
            .call(
                &format!("/v1/plans/{}/approve", plan.id),
                &self.approver(),
                json!({"plan_hash":plan.plan_hash,"precondition_hash":plan.compute_precondition_hash().unwrap(),
                "reason":"Independent consent for immutable engineering candidate"}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        serde_json::from_value(body["grant"].clone()).unwrap()
    }

    pub(super) async fn close(&self) {
        assert_eq!(self.observer.writes.load(Ordering::SeqCst), 0);
        let report = self.group.shutdown(std::time::Duration::from_secs(2)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}

pub(super) fn scope(actor: &AuthContext, revision: &str) -> Arc<ActionTargetScope> {
    Arc::new(ActionTargetScope::parse(Some(&json!({"schema_version":"rocketmq-sre.action-target-scope.v1","rules":[{
        "tenant_id":actor.tenant_id,"cluster_id":actor.clusters.iter().next().unwrap(),"action_id":ACTION,"descriptor_version":"1.0.0",
        "incident_resource":"broker/broker-a","target":TARGET,"security_revision":revision,
        "lookup":{"kind":"logger","component":"broker","logger":"rocketmq_broker::processor"}}]}).to_string())).unwrap())
}

pub(super) async fn call(router: Router, path: &str, actor: &AuthContext, body: Value) -> (StatusCode, Value) {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("d18-key".to_owned());
    let token = jsonwebtoken::encode(&header, &json!({"iss":ISSUER,"aud":AUDIENCE,"sub":actor.subject,
        "exp":Utc::now().timestamp()+3600,"scope":"rocketmq:read","roles":actor.roles,
        "rocketmq_tenant":actor.tenant_id.to_string(),"rocketmq_clusters":actor.clusters.iter().map(ToString::to_string).collect::<Vec<_>>()}),
        &EncodingKey::from_rsa_pem(include_bytes!("../../../../../deploy/dev/fixtures/oauth-private-key.pem")).unwrap()).unwrap();
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap();
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    if !status.is_success() {
        assert!(!result.to_string().contains("postgresql://"));
        assert!(!result.to_string().contains("SELECT "));
    }
    (status, result)
}

async fn start_observer(sample_at: DateTime<Utc>) -> (ExecutorSubmissionClient, TaskGroup, ReadObserver) {
    let runtime = RuntimeContext::from_current("d18-readiness-regression");
    let group = runtime.service_context("executor-observer").task_group().clone();
    let observer = ReadObserver {
        reads: Arc::new(AtomicUsize::new(0)),
        writes: Arc::new(AtomicUsize::new(0)),
        sample_at: Arc::new(Mutex::new(sample_at)),
        accepted: Arc::new(Mutex::new(None)),
        reconciliations: Arc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = Router::new()
        .route("/internal/v1/executor/preconditions", post(precondition))
        .route("/internal/v1/executor/executions", post(execution))
        .with_state(observer.clone());
    let stop = group.cancellation_token();
    group
        .spawn_service("readonly-agent-observer", async move {
            axum::serve(listener, server)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
                .unwrap();
        })
        .unwrap();
    let client = ExecutorSubmissionClient::http(
        endpoint.parse().unwrap(),
        "d18-observer-workload",
        std::time::Duration::from_secs(2),
        true,
    )
    .unwrap();
    (client, group, observer)
}

pub(in crate::supervised_execution) async fn readonly_executor(
    sample_at: DateTime<Utc>,
) -> (ExecutorSubmissionClient, TaskGroup) {
    let (client, group, _) = start_observer(sample_at).await;
    (client, group)
}
