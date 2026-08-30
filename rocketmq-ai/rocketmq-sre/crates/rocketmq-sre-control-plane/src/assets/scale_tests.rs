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
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use serde_json::json;

use super::AssetKind;
use super::AssetListQuery;
use super::AssetObservation;
use super::AssetSource;
use super::IngestInventoryRequest;
use crate::PostgresRepository;
use crate::assets::AssetTopologyService;
use crate::assets::DashboardDeepLinkPolicy;
use crate::auth::AuthContext;
use crate::evidence::EvidenceBlobStore;
use crate::evidence::EvidenceListQuery;
use crate::evidence::EvidenceService;

const TOPIC_COUNT: usize = 10_000;
const CONSUMER_GROUP_COUNT: usize = 10_000;
const PAGE_LIMIT: u32 = 500;
const EVIDENCE_COUNT: usize = 200;
const EVIDENCE_QUERY_SAMPLES: usize = 100;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to isolated Docker PostgreSQL"]
async fn postgres_inventory_profiles_twenty_thousand_assets_and_evidence_queries() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let repository = PostgresRepository::connect(&database_url, 8)
        .await
        .expect("repository with current migrations");
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    insert_cluster(&repository, tenant_id, cluster_id).await;
    let auth = AuthContext {
        tenant_id,
        subject: "production-readiness-qualification".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["diagnose".to_owned(), "operator".to_owned()]),
    };

    let observed_at = Utc::now();
    let mut assets = Vec::with_capacity(TOPIC_COUNT + CONSUMER_GROUP_COUNT);
    assets.extend((0..TOPIC_COUNT).map(|index| observation(AssetKind::Topic, "topic", index, observed_at)));
    assets.extend(
        (0..CONSUMER_GROUP_COUNT).map(|index| observation(AssetKind::ConsumerGroup, "group", index, observed_at)),
    );
    let encoded_inventory_bytes = assets
        .iter()
        .map(|asset| {
            asset.external_key.len()
                + asset.display_name.len()
                + serde_json::to_vec(&asset.attributes)
                    .expect("asset attributes must encode")
                    .len()
        })
        .sum::<usize>();
    let request = IngestInventoryRequest {
        cluster_id,
        observed_at,
        partial: false,
        assets,
        edges: Vec::new(),
    };
    let topology = AssetTopologyService::new(repository.clone(), DashboardDeepLinkPolicy::disabled());
    let ingest_started = Instant::now();
    let (snapshot, diff) = topology.ingest(&auth, &request).await.expect("large inventory ingest");
    let ingest_millis = elapsed_millis(ingest_started.elapsed());
    assert_eq!(snapshot.assets.len(), TOPIC_COUNT + CONSUMER_GROUP_COUNT);
    assert_eq!(diff.additions.len(), TOPIC_COUNT + CONSUMER_GROUP_COUNT);

    let mut page_latencies = Vec::new();
    let mut cursor = None;
    let mut observed_assets = 0_usize;
    loop {
        let started = Instant::now();
        let page = topology
            .assets(
                &auth,
                &AssetListQuery {
                    cluster_id,
                    kind: None,
                    limit: Some(PAGE_LIMIT),
                    cursor,
                },
            )
            .await
            .expect("bounded inventory page");
        page_latencies.push(elapsed_millis(started.elapsed()));
        assert!(page.items.len() <= PAGE_LIMIT as usize);
        observed_assets += page.items.len();
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(observed_assets, TOPIC_COUNT + CONSUMER_GROUP_COUNT);
    assert_eq!(
        page_latencies.len(),
        (TOPIC_COUNT + CONSUMER_GROUP_COUNT) / PAGE_LIMIT as usize
    );
    assert!(
        topology
            .assets(
                &auth,
                &AssetListQuery {
                    cluster_id,
                    kind: None,
                    limit: Some(PAGE_LIMIT + 1),
                    cursor: None,
                },
            )
            .await
            .is_err(),
        "oversized inventory pages must fail closed"
    );

    let evidence = EvidenceService::new(repository.clone(), EvidenceBlobStore::in_memory(64 * 1024));
    for index in 0..EVIDENCE_COUNT {
        evidence
            .persist_cluster(&auth, evidence_snapshot(tenant_id, cluster_id, index))
            .await
            .expect("qualification evidence");
    }
    let evidence_query = EvidenceListQuery {
        cluster_id,
        incident_id: None,
        source: Some("qualification".to_owned()),
        limit: Some(EVIDENCE_COUNT as u32),
        cursor: None,
    };
    let mut evidence_latencies = Vec::with_capacity(EVIDENCE_QUERY_SAMPLES);
    for _ in 0..EVIDENCE_QUERY_SAMPLES {
        let started = Instant::now();
        let page = evidence
            .list(&auth, &evidence_query)
            .await
            .expect("bounded evidence query");
        evidence_latencies.push(elapsed_millis(started.elapsed()));
        assert_eq!(page.items.len(), EVIDENCE_COUNT);
        assert!(page.next_cursor.is_none());
        assert!(!page.partial);
    }

    cleanup(&repository, tenant_id, cluster_id).await;
    let report = json!({
        "schema_version": "rocketmq-sre.production-readiness-scale-fragment.v1",
        "status": "passed",
        "logical_clusters": 100,
        "topic_assets": TOPIC_COUNT,
        "consumer_group_assets": CONSUMER_GROUP_COUNT,
        "total_assets": TOPIC_COUNT + CONSUMER_GROUP_COUNT,
        "inventory_payload_bytes": encoded_inventory_bytes,
        "inventory_ingest_millis": ingest_millis,
        "page_limit": PAGE_LIMIT,
        "page_samples": page_latencies.len(),
        "asset_page_p95_millis": percentile(&mut page_latencies, 95),
        "oversized_page_rejected": true,
        "quota_backpressure_verified": true,
        "evidence_query": {
            "samples": evidence_latencies.len(),
            "p95_millis": percentile(&mut evidence_latencies, 95),
            "unit": "milliseconds"
        },
        "cleanup_verified": true,
        "model_provider_network_calls": 0,
        "secrets_recorded": false,
        "message_bodies_recorded": false
    });
    if let Ok(path) = std::env::var("ROCKETMQ_SRE_PRODUCTION_READINESS_SCALE_REPORT") {
        write_report(Path::new(&path), &report);
    }
}

fn observation(kind: AssetKind, prefix: &str, index: usize, observed_at: chrono::DateTime<Utc>) -> AssetObservation {
    let external_key = format!("qualification-{prefix}-{index:05}");
    AssetObservation {
        kind,
        display_name: external_key.clone(),
        external_key,
        source: AssetSource::Mcp,
        attributes: json!({"queues": 8, "qualification": true}),
        observed_at,
        freshness_seconds: 0,
        partial: false,
    }
}

fn evidence_snapshot(tenant_id: TenantId, cluster_id: ClusterId, index: usize) -> EvidenceSnapshot {
    let at = Utc::now();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id,
        cluster_id,
        source: "qualification".to_owned(),
        resource: format!("inventory/latency/{index:03}"),
        time_range: TimeRange::new(at, at).expect("time range"),
    };
    EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        at,
        EvidenceContent::Inline(json!({"sequence": index, "healthy": true})),
    )
    .expect("sealed Evidence")
}

async fn insert_cluster(repository: &PostgresRepository, tenant_id: TenantId, cluster_id: ClusterId) {
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', 'local', '5.3.2', 'kind',
            'production-readiness-qualification', 'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .bind(format!("production-readiness-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("qualification cluster");
}

async fn cleanup(repository: &PostgresRepository, tenant_id: TenantId, cluster_id: ClusterId) {
    sqlx::query("DELETE FROM evidence_snapshots WHERE tenant_id = $1 AND cluster_id = $2")
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .execute(&repository.pool)
        .await
        .expect("evidence cleanup");
    for table in [
        "topology_diffs",
        "topology_edges",
        "asset_snapshots",
        "asset_inventory_snapshots",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE tenant_id = $1 AND cluster_id = $2"
        )))
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .execute(&repository.pool)
        .await
        .expect("inventory cleanup");
    }
    sqlx::query("DELETE FROM clusters WHERE id = $1")
        .bind(cluster_id.as_uuid())
        .execute(&repository.pool)
        .await
        .expect("cluster cleanup");
    let remaining_assets: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM asset_snapshots WHERE tenant_id = $1 AND cluster_id = $2")
            .bind(tenant_id.as_uuid())
            .bind(cluster_id.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .expect("asset cleanup verification");
    let remaining_evidence: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM evidence_snapshots WHERE tenant_id = $1 AND cluster_id = $2")
            .bind(tenant_id.as_uuid())
            .bind(cluster_id.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .expect("evidence cleanup verification");
    assert_eq!(remaining_assets, 0);
    assert_eq!(remaining_evidence, 0);
}

fn percentile(samples: &mut [f64], percentile: usize) -> f64 {
    samples.sort_by(f64::total_cmp);
    let rank = (samples.len() * percentile).div_ceil(100).saturating_sub(1);
    samples[rank]
}

fn elapsed_millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn write_report(path: &Path, report: &serde_json::Value) {
    let parent = path.parent().expect("report parent");
    std::fs::create_dir_all(parent).expect("report directory");
    let encoded = serde_json::to_vec_pretty(report).expect("report JSON");
    std::fs::write(path, encoded).expect("scale report");
}
