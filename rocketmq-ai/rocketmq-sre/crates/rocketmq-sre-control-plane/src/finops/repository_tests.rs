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
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FinOpsAllocationMode;
use rocketmq_sre_contracts::FinOpsBudgetPeriod;
use rocketmq_sre_contracts::FinOpsBudgetScopeKind;
use rocketmq_sre_contracts::FinOpsCostSource;
use rocketmq_sre_contracts::FinOpsDegradation;
use rocketmq_sre_contracts::FinOpsWorkClass;
use rocketmq_sre_contracts::FinOpsWorkloadKind;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::TenantId;

use super::FinOpsService;
use super::model::CreateFinOpsAllocationPolicyRequest;
use super::model::CreateFinOpsBudgetRequest;
use super::model::EvaluateFinOpsBudgetRequest;
use super::model::FinOpsReportQuery;
use super::model::RecordFinOpsCostRequest;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_finops_tracks_cost_enforces_budget_and_preserves_safety() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with FinOps migrations");
    let fixture = seed_fixture(&repository).await;
    let service = FinOpsService::new(repository.clone());
    let auth = operator_auth(fixture.tenant_id, fixture.cluster_id);
    let occurred_at = Utc::now() - Duration::minutes(10);

    let sources = [
        FinOpsCostSource::ControlPlane,
        FinOpsCostSource::Connector,
        FinOpsCostSource::ExecutionAgent,
        FinOpsCostSource::Observability,
        FinOpsCostSource::ObjectStorage,
        FinOpsCostSource::SyntheticProbe,
    ];
    let mut first_entry = None;
    for (index, source) in sources.into_iter().enumerate() {
        let request = cost_request(&fixture, source, index, occurred_at, 100);
        let entry = service
            .record_cost(&auth, &request)
            .await
            .expect("infrastructure cost entry");
        if index == 0 {
            let duplicate = service
                .record_cost(&auth, &request)
                .await
                .expect("idempotent cost retry");
            assert_eq!(duplicate.id, entry.id);
            let mut conflict = request.clone();
            conflict.cost_micros = 101;
            assert!(service.record_cost(&auth, &conflict).await.is_err());
            first_entry = Some(entry);
        }
    }
    let first_entry = first_entry.expect("first FinOps entry");
    let append_only = sqlx::query("UPDATE finops_cost_ledger SET cost_micros = 1 WHERE id = $1")
        .bind(first_entry.id.as_uuid())
        .execute(&repository.pool)
        .await;
    assert!(append_only.is_err(), "FinOps ledger must be append-only");

    seed_model_invocation(&repository, &fixture, occurred_at).await;
    let budget = service
        .create_budget(
            &auth,
            &CreateFinOpsBudgetRequest {
                scope_kind: FinOpsBudgetScopeKind::Tenant,
                scope_key: fixture.tenant_id.to_string(),
                period: FinOpsBudgetPeriod::Daily,
                soft_limit_micros: 400,
                hard_limit_micros: 700,
                owner: auth.subject.clone(),
            },
        )
        .await
        .expect("tenant FinOps budget");
    let background = service
        .evaluate_budget(
            &auth,
            &EvaluateFinOpsBudgetRequest {
                budget_id: budget.id,
                cluster_id: Some(fixture.cluster_id),
                work_class: FinOpsWorkClass::Background,
                requested_cost_micros: 1,
            },
        )
        .await
        .expect("background budget decision");
    assert!(!background.decision.allowed);
    assert_eq!(background.decision.degradation, FinOpsDegradation::DenyLowPriority);
    let rollback = service
        .evaluate_budget(
            &auth,
            &EvaluateFinOpsBudgetRequest {
                budget_id: budget.id,
                cluster_id: Some(fixture.cluster_id),
                work_class: FinOpsWorkClass::Rollback,
                requested_cost_micros: 1_000_000,
            },
        )
        .await
        .expect("cost-protected rollback");
    assert!(rollback.decision.allowed);
    assert_eq!(rollback.decision.degradation, FinOpsDegradation::None);
    rollback
        .decision
        .validate_safety_boundary()
        .expect("rollback safety boundary");

    let unconfirmed_chargeback = service
        .create_allocation_policy(
            &auth,
            &CreateFinOpsAllocationPolicyRequest {
                mode: FinOpsAllocationMode::Chargeback,
                allocation_keys: BTreeSet::from(["tenant".to_owned()]),
                organization_confirmed: false,
                owner: auth.subject.clone(),
            },
        )
        .await;
    assert!(unconfirmed_chargeback.is_err());
    let chargeback = service
        .create_allocation_policy(
            &auth,
            &CreateFinOpsAllocationPolicyRequest {
                mode: FinOpsAllocationMode::Chargeback,
                allocation_keys: BTreeSet::from(["tenant".to_owned(), "cluster".to_owned(), "provider".to_owned()]),
                organization_confirmed: true,
                owner: auth.subject.clone(),
            },
        )
        .await
        .expect("confirmed chargeback policy");
    assert!(chargeback.policy.organization_confirmed);

    let report = service
        .report(
            &auth,
            &FinOpsReportQuery {
                from: occurred_at - Duration::minutes(5),
                to: Utc::now() + Duration::minutes(1),
                cluster_id: Some(fixture.cluster_id),
                limit: 100,
            },
        )
        .await
        .expect("FinOps showback report");
    assert!(report.chargeback_enabled);
    assert_eq!(report.total_cost_micros, 800);
    assert_eq!(report.entries_missing_cost, 0);
    assert_eq!(report.cost_coverage_basis_points, Some(10_000));
    assert!(report.rows.iter().any(|row| {
        row.dimensions
            .get("source")
            .is_some_and(|source| source == "model_invocation")
            && row.input_tokens == 80
            && row.output_tokens == 20
            && row.cost_micros == 200
    }));
    assert!(report.rows.iter().any(|row| {
        row.dimensions
            .get("source")
            .is_some_and(|source| source == "observability")
    }));
    assert!(!report.forecasts.is_empty());
    assert!(!report.anomalies.is_empty());
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.starts_with("slo_outcome_attribution_not_available"))
    );
}

fn cost_request(
    fixture: &FinOpsFixture,
    source: FinOpsCostSource,
    index: usize,
    occurred_at: chrono::DateTime<Utc>,
    cost_micros: u64,
) -> RecordFinOpsCostRequest {
    RecordFinOpsCostRequest {
        idempotency_key: format!("finops-test-{source:?}-{index}-{}", fixture.cluster_id),
        fleet_id: fixture.fleet_id,
        region_id: fixture.region_id,
        cluster_id: Some(fixture.cluster_id),
        source,
        workload_kind: FinOpsWorkloadKind::System,
        provider_profile: None,
        model_family: None,
        incident_id: None,
        pack_id: None,
        workflow_id: None,
        request_count: 1,
        input_tokens: 0,
        output_tokens: 0,
        latency_millis: 10,
        error_count: 0,
        quantity_millis: 1_000,
        cost_micros,
        occurred_at,
    }
}

#[derive(Clone, Copy)]
struct FinOpsFixture {
    fleet_id: FleetId,
    tenant_id: TenantId,
    region_id: RegionId,
    cluster_id: ClusterId,
}

async fn seed_fixture(repository: &PostgresRepository) -> FinOpsFixture {
    let fixture = FinOpsFixture {
        fleet_id: FleetId::new(),
        tenant_id: TenantId::new(),
        region_id: RegionId::new(),
        cluster_id: ClusterId::new(),
    };
    sqlx::query("INSERT INTO fleets (id, name, owner_name) VALUES ($1, $2, 'finops-test')")
        .bind(fixture.fleet_id.as_uuid())
        .bind(format!("finops-fleet-{}", fixture.fleet_id))
        .execute(&repository.pool)
        .await
        .expect("FinOps fleet fixture");
    sqlx::query(
        "INSERT INTO fleet_tenants (id, fleet_id, name, owner_name)
         VALUES ($1, $2, $3, 'finops-test')",
    )
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.fleet_id.as_uuid())
    .bind(format!("finops-tenant-{}", fixture.tenant_id))
    .execute(&repository.pool)
    .await
    .expect("FinOps tenant fixture");
    sqlx::query(
        "INSERT INTO fleet_regions (
            id, fleet_id, region_key, display_name, owner_name, residency_tags
         ) VALUES ($1, $2, $3, 'FinOps test region', 'finops-test', $4)",
    )
    .bind(fixture.region_id.as_uuid())
    .bind(fixture.fleet_id.as_uuid())
    .bind(format!("finops-region-{}", fixture.region_id))
    .bind(serde_json::json!(["test-residency"]))
    .execute(&repository.pool)
    .await
    .expect("FinOps region fixture");
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', $4, '5.3.2', 'docker', 'finops-test',
            'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(format!("finops-test-{}", fixture.cluster_id))
    .bind(fixture.region_id.to_string())
    .execute(&repository.pool)
    .await
    .expect("FinOps cluster fixture");
    sqlx::query(
        "INSERT INTO fleet_cluster_registrations (
            cluster_id, fleet_id, tenant_id, region_id, environment,
            owner_name, lifecycle_state, residency_tags
         ) VALUES ($1, $2, $3, $4, 'test', 'finops-test', 'active', $5)",
    )
    .bind(fixture.cluster_id.as_uuid())
    .bind(fixture.fleet_id.as_uuid())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.region_id.as_uuid())
    .bind(serde_json::json!(["test-residency"]))
    .execute(&repository.pool)
    .await
    .expect("FinOps cluster registration fixture");
    fixture
}

async fn seed_model_invocation(
    repository: &PostgresRepository,
    fixture: &FinOpsFixture,
    occurred_at: chrono::DateTime<Utc>,
) {
    let profile_id = uuid::Uuid::new_v4();
    sqlx::query(
        "INSERT INTO model_profiles (
            id, tenant_id, profile_name, provider_family, protocol_family,
            model_family, model_name, model_revision, endpoint_instance,
            region, data_residency, data_classes, capabilities, priority,
            credential_ref, credential_owner, health, created_at, updated_at,
            estimated_cost_microusd_per_1k_tokens
         ) VALUES (
            $1, $2, $3, 'deepseek', 'open_ai', 'deepseek-chat', 'deepseek-chat',
            '2026-01', 'finops-test', $4, 'regional', $5, $6, 10,
            'secret://finops-test', 'gateway', 'healthy', $7, $7, 2000
         )",
    )
    .bind(profile_id)
    .bind(fixture.tenant_id.as_uuid())
    .bind(format!("finops-deepseek-{profile_id}"))
    .bind(fixture.region_id.to_string())
    .bind(serde_json::json!(["internal"]))
    .bind(serde_json::json!(["chat", "json_schema"]))
    .bind(occurred_at)
    .execute(&repository.pool)
    .await
    .expect("FinOps model profile");
    sqlx::query(
        "INSERT INTO model_invocations (
            id, tenant_id, cluster_id, incident_id, diagnosis_revision_id,
            parent_invocation_id, purpose, requested_profile_id, actual_profile_id,
            provider_family, model_family, model_revision, endpoint_instance,
            fallback_chain, prompt_version, schema_version, input_tokens,
            output_tokens, cost_micros, rationale, error_code, started_at,
            completed_at, correlation_id, actual_model
         ) VALUES (
            $1, $2, $3, NULL, NULL, NULL, 'primary_diagnosis', $4, $4,
            'deepseek', 'deepseek-chat', '2026-01', 'finops-test',
            '{}', 'prompt-v1', 'schema-v1', 80, 20, 200,
            'FinOps integration fixture', NULL, $5, $6, $7, 'deepseek-chat'
         )",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.cluster_id.as_uuid())
    .bind(profile_id)
    .bind(occurred_at)
    .bind(occurred_at + Duration::milliseconds(20))
    .bind(uuid::Uuid::new_v4())
    .execute(&repository.pool)
    .await
    .expect("FinOps model invocation");
}

fn operator_auth(tenant_id: TenantId, cluster_id: ClusterId) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: "finops-operator".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["operator".to_owned(), "finops".to_owned()]),
    }
}
