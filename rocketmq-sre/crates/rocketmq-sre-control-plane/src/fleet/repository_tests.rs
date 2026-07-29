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
use rocketmq_sre_contracts::ComplianceFindingState;
use rocketmq_sre_contracts::ComplianceSeverity;
use rocketmq_sre_contracts::DataResidencyClass;
use rocketmq_sre_contracts::FleetAssetIndex;
use rocketmq_sre_contracts::FleetEnvironment;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::FleetInspectionState;
use rocketmq_sre_contracts::QuotaLimits;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::RegionalEndpoint;
use rocketmq_sre_contracts::RegionalEndpointHealth;
use rocketmq_sre_contracts::RegionalEndpointKind;
use rocketmq_sre_contracts::TenantId;

use super::model::ComplianceFindingQuery;
use super::model::CreateFleetInspectionRequest;
use super::model::CreateQuotaPolicyRequest;
use super::model::EvaluateComplianceRequest;
use super::model::FleetScopeQuery;
use super::model::RegionalRouteMode;
use super::model::RegionalRouteRequest;
use super::model::RegisterRegionalEndpointRequest;
use super::model::UpdateFleetInspectionRequest;
use super::model::UpsertFleetAssetRequest;
use super::service::FleetService;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_fleet_scope_routing_compliance_and_inspection_are_bounded() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with Fleet migrations");
    let fixture = seed_fixture(&repository).await;
    let service = FleetService::new(repository);
    let auth = operator_auth(fixture.tenant_id, [fixture.cluster_a, fixture.cluster_b]);
    let restricted_auth = operator_auth(fixture.tenant_id, [fixture.cluster_a]);

    let overview = service.overview(&auth).await.expect("Fleet overview");
    assert_eq!(overview.fleet.id, fixture.fleet_id);
    assert_eq!(overview.regions.len(), 2);
    assert_eq!(overview.registrations.len(), 2);
    let isolated = service
        .overview(&restricted_auth)
        .await
        .expect("cluster-scoped Fleet overview");
    assert_eq!(isolated.regions.len(), 1);
    assert_eq!(isolated.registrations.len(), 1);

    let quota = service
        .create_quota_policy(
            &auth,
            &CreateQuotaPolicyRequest {
                fleet_id: fixture.fleet_id,
                region_id: None,
                cluster_id: None,
                limits: QuotaLimits {
                    queries_per_minute: 1_000,
                    model_tokens_per_hour: 1_000_000,
                    concurrent_workflows: 8,
                    concurrent_inspections: 1,
                    evidence_bytes_per_hour: 8_000_000,
                    notifications_per_hour: 1_000,
                    automatic_actions_per_hour: 10,
                },
                owner: "fleet-platform".to_owned(),
            },
        )
        .await
        .expect("Fleet quota");
    assert_eq!(quota.policy.tenant_id, fixture.tenant_id);
    assert_eq!(quota.policy.limits.concurrent_inspections, 1);

    let endpoint = regional_endpoint(
        &fixture,
        "connector-a",
        fixture.region_a,
        Some(fixture.cluster_a),
        "2.0.0",
        RegionalEndpointHealth::Healthy,
    );
    service
        .register_endpoint(
            &auth,
            &RegisterRegionalEndpointRequest {
                endpoint: endpoint.clone(),
            },
        )
        .await
        .expect("current endpoint");
    let request = route_request(&fixture, DataResidencyClass::ExportAllowed);
    assert_eq!(
        service.route(&auth, &request).await.expect("current route").mode,
        RegionalRouteMode::Full
    );

    let previous = RegionalEndpoint {
        protocol_version: "1.0.0".to_owned(),
        ..endpoint.clone()
    };
    service
        .register_endpoint(&auth, &RegisterRegionalEndpointRequest { endpoint: previous })
        .await
        .expect("N-1 endpoint");
    let degraded = service.route(&auth, &request).await.expect("N-1 route");
    assert_eq!(degraded.mode, RegionalRouteMode::ReadOnlyDegraded);
    assert!(
        degraded
            .reason_codes
            .contains(&"protocol_n_minus_one_read_only".to_owned())
    );

    service
        .register_endpoint(
            &auth,
            &RegisterRegionalEndpointRequest {
                endpoint: RegionalEndpoint {
                    protocol_version: "0.9.0".to_owned(),
                    ..endpoint.clone()
                },
            },
        )
        .await
        .expect("incompatible endpoint");
    let incompatible = service.route(&auth, &request).await.expect("incompatible route");
    assert_eq!(incompatible.mode, RegionalRouteMode::Denied);
    assert_eq!(incompatible.reason_codes, vec!["protocol_incompatible".to_owned()]);
    service
        .register_endpoint(
            &auth,
            &RegisterRegionalEndpointRequest {
                endpoint: RegionalEndpoint {
                    schema_digest: digest('9'),
                    ..endpoint.clone()
                },
            },
        )
        .await
        .expect("digest-drifted endpoint");
    let drifted = service.route(&auth, &request).await.expect("digest drift route");
    assert_eq!(drifted.mode, RegionalRouteMode::Denied);
    assert_eq!(drifted.reason_codes, vec!["schema_digest_mismatch".to_owned()]);

    let disconnected = RegionalEndpoint {
        health: RegionalEndpointHealth::Disconnected,
        ..endpoint
    };
    service
        .register_endpoint(&auth, &RegisterRegionalEndpointRequest { endpoint: disconnected })
        .await
        .expect("disconnect local endpoint");
    let endpoint_b = regional_endpoint(
        &fixture,
        "connector-b",
        fixture.region_b,
        None,
        "2.0.0",
        RegionalEndpointHealth::Healthy,
    );
    service
        .register_endpoint(&auth, &RegisterRegionalEndpointRequest { endpoint: endpoint_b })
        .await
        .expect("regional shared endpoint");
    let local_only = service
        .route(&auth, &route_request(&fixture, DataResidencyClass::RegionLocal))
        .await
        .expect("region-local route decision");
    assert_eq!(local_only.mode, RegionalRouteMode::Denied);
    assert_eq!(
        service.route(&auth, &request).await.expect("export route").mode,
        RegionalRouteMode::Full
    );

    let healthy_asset = fleet_asset(&fixture, fixture.cluster_a, fixture.region_a, "healthy");
    service
        .upsert_asset(&auth, &UpsertFleetAssetRequest { asset: healthy_asset })
        .await
        .expect("healthy asset");
    let critical_asset = fleet_asset(&fixture, fixture.cluster_b, fixture.region_b, "critical");
    assert!(
        service
            .upsert_asset(
                &restricted_auth,
                &UpsertFleetAssetRequest {
                    asset: critical_asset.clone(),
                },
            )
            .await
            .is_err()
    );
    service
        .upsert_asset(&auth, &UpsertFleetAssetRequest { asset: critical_asset })
        .await
        .expect("critical asset");
    let assets = service
        .assets(&auth, &FleetScopeQuery::default())
        .await
        .expect("Fleet assets");
    assert_eq!(assets.total, 2);
    assert_eq!(assets.worst_health.as_deref(), Some("critical"));
    assert_eq!(assets.health_distribution.get("healthy"), Some(&1));
    assert_eq!(assets.health_distribution.get("critical"), Some(&1));

    let expected = digest('a');
    let live = digest('b');
    let drift = EvaluateComplianceRequest {
        fleet_id: fixture.fleet_id,
        region_id: fixture.region_a,
        cluster_id: fixture.cluster_a,
        category: "broker_configuration".to_owned(),
        expected_digest: expected.clone(),
        live_digest: live,
        evidence_ids: Vec::new(),
        severity: ComplianceSeverity::Error,
        owner: "broker-owner".to_owned(),
        recommendation: "Review the drift before creating a supervised plan".to_owned(),
    };
    let finding = service
        .evaluate_compliance(&auth, &drift)
        .await
        .expect("compliance drift");
    assert!(!finding.compliant);
    assert_eq!(
        finding.finding.as_ref().map(|item| item.state),
        Some(ComplianceFindingState::Open)
    );
    let resolved = service
        .evaluate_compliance(
            &auth,
            &EvaluateComplianceRequest {
                live_digest: expected,
                ..drift
            },
        )
        .await
        .expect("compliance recovery");
    assert!(resolved.compliant);
    assert_eq!(resolved.resolved_findings, 1);
    let findings = service
        .findings(
            &auth,
            &ComplianceFindingQuery {
                state: Some(ComplianceFindingState::Resolved),
                ..ComplianceFindingQuery::default()
            },
        )
        .await
        .expect("resolved findings");
    assert_eq!(findings.total, 1);

    let inspection_request = CreateFleetInspectionRequest {
        fleet_id: fixture.fleet_id,
        region_ids: BTreeSet::from([fixture.region_a, fixture.region_b]),
        cluster_ids: vec![fixture.cluster_a, fixture.cluster_b],
        pack_ids: vec!["broker-health.v1".to_owned()],
        max_concurrency: 2,
        timeout_seconds: 600,
        model_token_budget: 10_000,
        evidence_byte_budget: 1_000_000,
    };
    let inspection = service
        .create_inspection(&auth, &inspection_request)
        .await
        .expect("bounded Fleet inspection");
    assert_eq!(inspection.state, FleetInspectionState::Pending);
    assert!(service.create_inspection(&auth, &inspection_request).await.is_err());
    let completed = service
        .update_inspection(
            &auth,
            inspection.id,
            &UpdateFleetInspectionRequest {
                completed_clusters: 1,
                failed_clusters: 1,
                terminal: true,
            },
        )
        .await
        .expect("partial inspection result");
    assert_eq!(completed.state, FleetInspectionState::PartiallyCompleted);
    assert_eq!(
        service
            .inspections(&auth, 10)
            .await
            .expect("Fleet inspection history")
            .items
            .first()
            .map(|item| item.id),
        Some(inspection.id)
    );
}

#[derive(Clone, Copy)]
struct FleetFixture {
    fleet_id: FleetId,
    tenant_id: TenantId,
    region_a: RegionId,
    region_b: RegionId,
    cluster_a: ClusterId,
    cluster_b: ClusterId,
}

async fn seed_fixture(repository: &PostgresRepository) -> FleetFixture {
    let fixture = FleetFixture {
        fleet_id: FleetId::new(),
        tenant_id: TenantId::new(),
        region_a: RegionId::new(),
        region_b: RegionId::new(),
        cluster_a: ClusterId::new(),
        cluster_b: ClusterId::new(),
    };
    sqlx::query(
        "INSERT INTO fleets (id, name, owner_name)
         VALUES ($1, $2, 'fleet-repository-test')",
    )
    .bind(fixture.fleet_id.as_uuid())
    .bind(format!("fleet-repository-{}", fixture.fleet_id))
    .execute(&repository.pool)
    .await
    .expect("Fleet fixture");
    sqlx::query(
        "INSERT INTO fleet_tenants (id, fleet_id, name, owner_name)
         VALUES ($1, $2, $3, 'fleet-repository-test')",
    )
    .bind(fixture.tenant_id.as_uuid())
    .bind(fixture.fleet_id.as_uuid())
    .bind(format!("tenant-{}", fixture.tenant_id))
    .execute(&repository.pool)
    .await
    .expect("tenant fixture");
    for (region_id, key) in [(fixture.region_a, "region-a"), (fixture.region_b, "region-b")] {
        sqlx::query(
            "INSERT INTO fleet_regions (
                id, fleet_id, region_key, display_name, owner_name, residency_tags
             ) VALUES ($1, $2, $3, $3, 'fleet-repository-test', $4)",
        )
        .bind(region_id.as_uuid())
        .bind(fixture.fleet_id.as_uuid())
        .bind(format!("{key}-{region_id}"))
        .bind(serde_json::json!([key]))
        .execute(&repository.pool)
        .await
        .expect("region fixture");
    }
    for (cluster_id, region_id, key) in [
        (fixture.cluster_a, fixture.region_a, "cluster-a"),
        (fixture.cluster_b, fixture.region_b, "cluster-b"),
    ] {
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', $4, '5.3.2', 'docker',
                'fleet-repository-test', 'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(fixture.tenant_id.as_uuid())
        .bind(format!("{key}-{cluster_id}"))
        .bind(region_id.to_string())
        .execute(&repository.pool)
        .await
        .expect("cluster fixture");
        sqlx::query(
            "INSERT INTO fleet_cluster_registrations (
                cluster_id, fleet_id, tenant_id, region_id, environment,
                owner_name, lifecycle_state, residency_tags
             ) VALUES ($1, $2, $3, $4, 'test', 'fleet-repository-test', 'active', $5)",
        )
        .bind(cluster_id.as_uuid())
        .bind(fixture.fleet_id.as_uuid())
        .bind(fixture.tenant_id.as_uuid())
        .bind(region_id.as_uuid())
        .bind(serde_json::json!([region_id.to_string()]))
        .execute(&repository.pool)
        .await
        .expect("cluster registration fixture");
    }
    fixture
}

fn operator_auth(tenant_id: TenantId, clusters: impl IntoIterator<Item = ClusterId>) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: "fleet-operator".to_owned(),
        clusters: clusters.into_iter().collect(),
        roles: BTreeSet::from(["operator".to_owned()]),
    }
}

fn regional_endpoint(
    fixture: &FleetFixture,
    id: &str,
    region_id: RegionId,
    cluster_id: Option<ClusterId>,
    protocol_version: &str,
    health: RegionalEndpointHealth,
) -> RegionalEndpoint {
    RegionalEndpoint {
        id: format!("{id}-{}", fixture.fleet_id),
        fleet_id: fixture.fleet_id,
        tenant_id: fixture.tenant_id,
        region_id,
        cluster_id,
        kind: RegionalEndpointKind::Connector,
        component_version: "1.0.0".to_owned(),
        protocol_version: protocol_version.to_owned(),
        schema_digest: digest('c'),
        capabilities: BTreeSet::from(["evidence.query".to_owned()]),
        residency_tags: BTreeSet::from([region_id.to_string()]),
        capacity: 8,
        health,
        last_heartbeat_at: Utc::now(),
    }
}

fn route_request(fixture: &FleetFixture, residency: DataResidencyClass) -> RegionalRouteRequest {
    RegionalRouteRequest {
        cluster_id: fixture.cluster_a,
        endpoint_kind: RegionalEndpointKind::Connector,
        source_region_id: fixture.region_a,
        residency,
        current_protocol_version: "2.0.0".to_owned(),
        previous_protocol_version: "1.0.0".to_owned(),
        required_schema_digest: digest('c'),
        required_capabilities: BTreeSet::from(["evidence.query".to_owned()]),
    }
}

fn fleet_asset(fixture: &FleetFixture, cluster_id: ClusterId, region_id: RegionId, health: &str) -> FleetAssetIndex {
    FleetAssetIndex {
        cluster_id,
        fleet_id: fixture.fleet_id,
        tenant_id: fixture.tenant_id,
        region_id,
        environment: FleetEnvironment::Test,
        owner: "fleet-platform".to_owned(),
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
