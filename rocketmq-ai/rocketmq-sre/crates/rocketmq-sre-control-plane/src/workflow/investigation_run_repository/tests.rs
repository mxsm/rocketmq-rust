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

use chrono::Duration;
use rocketmq_sre_contracts::*;

use super::*;

mod commands;
mod events;
mod recovery;
mod settlement;
mod upgrade;

async fn repository() -> PostgresRepository {
    PostgresRepository::connect(
        &std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit Docker test database"),
        8,
    )
    .await
    .unwrap()
}

async fn fixture() -> (
    PostgresRepository,
    AuthContext,
    InvestigationRunContext,
    CreateInvestigationRunRequest,
) {
    let repository = repository().await;
    let (auth, scope) = seed_parent(&repository).await;
    let pack = InvestigationPackRef {
        pack_id: "consumer-lag.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
    };
    let context = InvestigationRunContext {
        actor_subject: auth.subject.clone(),
        permission: InvestigationRunPermission::Control,
        authorized_scope: scope.clone(),
        allowed_packs: vec![pack.clone()],
        allowed_requirements: vec![InvestigationRequirementRef {
            pack: pack.clone(),
            requirement_id: "consumer-lag".to_owned(),
        }],
        visible_evidence: vec![],
    };
    let request = CreateInvestigationRunRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        run_id: InvestigationRunId::new(),
        scope,
        packs: vec![pack],
        budget: InvestigationBudget {
            limit: InvestigationBudgetCost {
                read_queries: 12,
                model_calls: 2,
                evidence_items: 24,
                output_bytes: 4096,
            },
            max_steps: 32,
            max_in_flight: 4,
            max_attempts_per_step: 3,
            max_query_retries: 2,
        },
        deadline: Utc::now() + Duration::minutes(20),
        idempotency_key: "create-durable".to_owned(),
    };
    (repository, auth, context, request)
}

async fn seed_parent(repository: &PostgresRepository) -> (AuthContext, InvestigationRunScope) {
    let tenant = TenantId::new();
    let cluster = ClusterId::new();
    let parent = InvestigationId::new();
    let now = Utc::now();
    sqlx::query("INSERT INTO clusters(id,tenant_id,external_cluster_key,environment,region,rocketmq_version,deployment_mode,owner_name,requested_access_profile,effective_access_profile,onboarding_state) VALUES($1,$2,$3,'test','local','test','test','run-test','read_only','read_only','ready_read_only')")
        .bind(cluster.as_uuid()).bind(tenant.to_string()).bind(format!("run-{cluster}")).execute(&repository.pool).await.unwrap();
    sqlx::query("INSERT INTO investigations(id,tenant_id,cluster_id,title,resource,symptom_family,fingerprint,status,created_by_subject,created_at,updated_at) VALUES($1,$2,$3,'durable parent','cluster/overview','latency',$4,'open','run-test',$5,$5)")
        .bind(parent.as_uuid()).bind(tenant.as_uuid()).bind(cluster.as_uuid()).bind(format!("run-{parent}")).bind(now).execute(&repository.pool).await.unwrap();
    (
        AuthContext {
            tenant_id: tenant,
            subject: "run-test-operator".to_owned(),
            clusters: BTreeSet::from([cluster]),
            roles: BTreeSet::from(["operator".to_owned()]),
        },
        InvestigationRunScope {
            tenant_id: tenant,
            cluster_id: cluster,
            investigation_id: parent,
            resource: None,
            time_range: TimeRange::new(now - Duration::minutes(5), now).unwrap(),
        },
    )
}

fn command(run: &InvestigationRun, command: InvestigationCommand, key: &str) -> InvestigationCommandRequest {
    InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: run.id,
        expected_revision: run.revision,
        idempotency_key: key.to_owned(),
        reason: "operator reviewed bounded investigation".to_owned(),
        command,
    }
}
fn collect(run: &InvestigationRun) -> InvestigationStepAdmission {
    InvestigationStepAdmission {
        step_id: InvestigationStepId::new(),
        attempt_id: StepAttemptId::new(),
        expected_revision: run.revision,
        spec: InvestigationStepSpec {
            scope: run.effective_scope.clone(),
            pack: run.effective_packs[0].clone(),
            work: InvestigationStepKind::CollectEvidence {
                requirement_id: "consumer-lag".to_owned(),
            },
        },
        reservation: InvestigationBudgetCost {
            read_queries: 1,
            model_calls: 0,
            evidence_items: 1,
            output_bytes: 64,
        },
    }
}
fn result(run: &InvestigationRun, admission: &InvestigationStepAdmission) -> InvestigationStepResult {
    InvestigationStepResult {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        run_id: run.id,
        step_id: admission.step_id,
        attempt_id: admission.attempt_id,
        control_epoch: run.control_epoch,
        outcome: InvestigationStepOutcome::Succeeded,
        failure: None,
        evidence_ids: vec![],
        actual_cost: InvestigationBudgetCost {
            read_queries: 1,
            output_bytes: 1,
            ..Default::default()
        },
        finished_at: Utc::now().max(run.updated_at),
    }
}
async fn head(repository: &PostgresRepository, id: InvestigationRunId) -> i64 {
    sqlx::query_scalar("SELECT event_sequence FROM investigation_runs WHERE id=$1")
        .bind(id.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .unwrap()
}
async fn running() -> (PostgresRepository, AuthContext, InvestigationRunContext, RunLease) {
    let (repository, auth, context, request) = fixture().await;
    repository
        .investigation_run_create(&auth, &context, &request, Utc::now())
        .await
        .unwrap();
    let mut lease = repository
        .investigation_run_claim(&auth, request.run_id, Uuid::new_v4(), 60, Utc::now())
        .await
        .unwrap()
        .unwrap();
    lease.run = repository
        .investigation_run_start(
            &RunWorkerContext {
                auth: &auth,
                context: &context,
                fence: &lease.fence,
            },
            request.run_id,
            lease.run.revision,
            Utc::now(),
        )
        .await
        .unwrap();
    (repository, auth, context, lease)
}
async fn expire(repository: &PostgresRepository, id: InvestigationRunId) {
    sqlx::query("UPDATE investigation_runs SET worker_lease_until=now()-interval '1 second' WHERE id=$1")
        .bind(id.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
}
