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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FleetAssetIndex;
use rocketmq_sre_contracts::FleetEnvironment;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::FleetInspectionState;
use rocketmq_sre_contracts::QuotaLimits;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::TenantId;

use super::model::CreateFleetInspectionRequest;
use super::model::CreateQuotaPolicyRequest;
use super::model::FleetScopeQuery;
use super::model::UpdateFleetInspectionRequest;
use super::model::UpsertFleetAssetRequest;
use super::service::FleetService;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const SCALE_CLUSTER_COUNT: usize = 100;
const PAGE_SIZE: u16 = 25;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_fleet_scale_demo_pages_100_clusters_and_applies_backpressure() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with Fleet migrations");
    let fixture = seed_scale_fixture(&repository).await;
    let service = FleetService::new(repository);
    let auth = operator_auth(
        fixture.tenant_id,
        fixture.clusters.iter().map(|(cluster_id, _)| *cluster_id),
    );

    service
        .create_quota_policy(
            &auth,
            &CreateQuotaPolicyRequest {
                fleet_id: fixture.fleet_id,
                region_id: None,
                cluster_id: None,
                limits: QuotaLimits {
                    queries_per_minute: 10_000,
                    model_tokens_per_hour: 10_000_000,
                    concurrent_workflows: 32,
                    concurrent_inspections: 1,
                    evidence_bytes_per_hour: 64_000_000,
                    notifications_per_hour: 10_000,
                    automatic_actions_per_hour: 100,
                },
                owner: "fleet-scale-test".to_owned(),
            },
        )
        .await
        .expect("scale-test Fleet quota");

    for (index, (cluster_id, region_id)) in fixture.clusters.iter().enumerate() {
        let health = match index {
            98 => "degraded",
            99 => "critical",
            _ => "healthy",
        };
        service
            .upsert_asset(
                &auth,
                &UpsertFleetAssetRequest {
                    asset: scale_asset(&fixture, *cluster_id, *region_id, health),
                },
            )
            .await
            .expect("scale-test Fleet asset");
    }

    let mut observed_clusters = BTreeSet::new();
    for offset in [0_u32, 25, 50, 75] {
        let page = service
            .assets(
                &auth,
                &FleetScopeQuery {
                    limit: PAGE_SIZE,
                    offset,
                    ..FleetScopeQuery::default()
                },
            )
            .await
            .expect("bounded Fleet asset page");
        assert_eq!(page.total, SCALE_CLUSTER_COUNT as u64);
        assert_eq!(page.limit, PAGE_SIZE);
        assert_eq!(page.offset, offset);
        assert_eq!(page.items.len(), usize::from(PAGE_SIZE));
        assert_eq!(page.worst_health.as_deref(), Some("critical"));
        assert_eq!(page.health_distribution.get("critical"), Some(&1));
        assert_eq!(page.health_distribution.get("degraded"), Some(&1));
        assert_eq!(
            page.health_distribution.get("healthy"),
            Some(&((SCALE_CLUSTER_COUNT - 2) as u64))
        );
        observed_clusters.extend(page.items.into_iter().map(|asset| asset.cluster_id));
    }
    assert_eq!(observed_clusters.len(), SCALE_CLUSTER_COUNT);

    let first_page = service
        .assets(
            &auth,
            &FleetScopeQuery {
                limit: PAGE_SIZE,
                ..FleetScopeQuery::default()
            },
        )
        .await
        .expect("worst-first Fleet page");
    assert_eq!(
        first_page.items.first().map(|asset| asset.health.as_str()),
        Some("critical")
    );

    for region_id in fixture.regions {
        let regional = service
            .assets(
                &auth,
                &FleetScopeQuery {
                    region_id: Some(region_id),
                    limit: 200,
                    ..FleetScopeQuery::default()
                },
            )
            .await
            .expect("regional Fleet page");
        assert_eq!(regional.total, (SCALE_CLUSTER_COUNT / 2) as u64);
        assert!(regional.items.iter().all(|asset| asset.region_id == region_id));
    }

    let region_a_auth = operator_auth(
        fixture.tenant_id,
        fixture
            .clusters
            .iter()
            .filter(|(_, region_id)| *region_id == fixture.regions[0])
            .map(|(cluster_id, _)| *cluster_id),
    );
    let isolated = service
        .assets(
            &region_a_auth,
            &FleetScopeQuery {
                region_id: Some(fixture.regions[1]),
                limit: 200,
                ..FleetScopeQuery::default()
            },
        )
        .await
        .expect("cross-region scoped Fleet page");
    assert_eq!(isolated.total, 0);
    assert!(isolated.items.is_empty());

    let inspection_request = CreateFleetInspectionRequest {
        fleet_id: fixture.fleet_id,
        region_ids: fixture.regions.into_iter().collect(),
        cluster_ids: fixture.clusters.iter().map(|(cluster_id, _)| *cluster_id).collect(),
        pack_ids: vec!["broker-health.v1".to_owned()],
        max_concurrency: 8,
        timeout_seconds: 1_800,
        model_token_budget: 100_000,
        evidence_byte_budget: 10_000_000,
    };
    let inspection = service
        .create_inspection(&auth, &inspection_request)
        .await
        .expect("100-cluster bounded Fleet inspection");
    assert_eq!(inspection.state, FleetInspectionState::Pending);
    assert_eq!(inspection.cluster_ids.len(), SCALE_CLUSTER_COUNT);
    assert_eq!(inspection.max_concurrency, 8);
    assert!(service.create_inspection(&auth, &inspection_request).await.is_err());

    let completed = service
        .update_inspection(
            &auth,
            inspection.id,
            &UpdateFleetInspectionRequest {
                completed_clusters: SCALE_CLUSTER_COUNT as u32,
                failed_clusters: 0,
                terminal: true,
            },
        )
        .await
        .expect("completed scale inspection");
    assert_eq!(completed.state, FleetInspectionState::Completed);

    let mut oversized = inspection_request;
    oversized.cluster_ids.push(ClusterId::new());
    assert!(service.create_inspection(&auth, &oversized).await.is_err());
}

struct ScaleFixture {
    fleet_id: FleetId,
    tenant_id: TenantId,
    regions: [RegionId; 2],
    clusters: Vec<(ClusterId, RegionId)>,
}

async fn seed_scale_fixture(repository: &PostgresRepository) -> ScaleFixture {
    let fleet_id = FleetId::new();
    let tenant_id = TenantId::new();
    let regions = [RegionId::new(), RegionId::new()];
    sqlx::query("INSERT INTO fleets (id, name, owner_name) VALUES ($1, $2, 'fleet-scale-test')")
        .bind(fleet_id.as_uuid())
        .bind(format!("fleet-scale-{fleet_id}"))
        .execute(&repository.pool)
        .await
        .expect("scale-test Fleet");
    sqlx::query(
        "INSERT INTO fleet_tenants (id, fleet_id, name, owner_name)
         VALUES ($1, $2, $3, 'fleet-scale-test')",
    )
    .bind(tenant_id.as_uuid())
    .bind(fleet_id.as_uuid())
    .bind(format!("tenant-scale-{tenant_id}"))
    .execute(&repository.pool)
    .await
    .expect("scale-test tenant");
    for (index, region_id) in regions.iter().enumerate() {
        let region_key = format!("scale-region-{index}-{region_id}");
        sqlx::query(
            "INSERT INTO fleet_regions (
                id, fleet_id, region_key, display_name, owner_name, residency_tags
             ) VALUES ($1, $2, $3, $3, 'fleet-scale-test', $4)",
        )
        .bind(region_id.as_uuid())
        .bind(fleet_id.as_uuid())
        .bind(region_key)
        .bind(serde_json::json!([format!("scale-region-{index}")]))
        .execute(&repository.pool)
        .await
        .expect("scale-test region");
    }

    let mut clusters = Vec::with_capacity(SCALE_CLUSTER_COUNT);
    for index in 0..SCALE_CLUSTER_COUNT {
        let cluster_id = ClusterId::new();
        let region_id = regions[index % regions.len()];
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', $4, '5.3.2', 'docker',
                'fleet-scale-test', 'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(format!("scale-cluster-{index:03}-{cluster_id}"))
        .bind(region_id.to_string())
        .execute(&repository.pool)
        .await
        .expect("scale-test cluster");
        sqlx::query(
            "INSERT INTO fleet_cluster_registrations (
                cluster_id, fleet_id, tenant_id, region_id, environment,
                owner_name, lifecycle_state, residency_tags
             ) VALUES ($1, $2, $3, $4, 'test', 'fleet-scale-test', 'active', $5)",
        )
        .bind(cluster_id.as_uuid())
        .bind(fleet_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(region_id.as_uuid())
        .bind(serde_json::json!([region_id.to_string()]))
        .execute(&repository.pool)
        .await
        .expect("scale-test cluster registration");
        clusters.push((cluster_id, region_id));
    }
    ScaleFixture {
        fleet_id,
        tenant_id,
        regions,
        clusters,
    }
}

fn operator_auth(tenant_id: TenantId, clusters: impl IntoIterator<Item = ClusterId>) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: "fleet-scale-operator".to_owned(),
        clusters: clusters.into_iter().collect(),
        roles: BTreeSet::from(["operator".to_owned()]),
    }
}

fn scale_asset(fixture: &ScaleFixture, cluster_id: ClusterId, region_id: RegionId, health: &str) -> FleetAssetIndex {
    FleetAssetIndex {
        cluster_id,
        fleet_id: fixture.fleet_id,
        tenant_id: fixture.tenant_id,
        region_id,
        environment: FleetEnvironment::Test,
        owner: "fleet-scale-test".to_owned(),
        component: "broker".to_owned(),
        component_version: "5.3.2".to_owned(),
        image_digest: Some(digest('d')),
        feature_digest: Some(digest('e')),
        configuration_digest: Some(digest('f')),
        health: health.to_owned(),
        attributes: BTreeMap::from([("deployment_mode".to_owned(), "docker".to_owned())]),
        observed_at: Utc::now(),
    }
}

fn digest(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
