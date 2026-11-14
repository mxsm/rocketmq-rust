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

use axum::http::StatusCode;
use rocketmq_runtime::RuntimeContext;
use serde_json::json;

use super::super::test_support::{Fixture, call, command, fixture, saved_reference_fixture};
use super::*;
use crate::connector_channel::{ConnectorCommand, ConnectorPrincipal, PollRequest, channel_schema};

async fn prepared() -> (Fixture, ConnectorPrincipal, ConnectorSessionId) {
    let mut fixture = fixture().await;
    fixture.request.scope.resource = Some(InvestigationResource {
        kind: ResourceKind::Broker,
        key: "broker-a".to_owned(),
    });
    fixture.request.packs = vec![
        InvestigationPackRef {
            pack_id: "store-pressure.v1".to_owned(),
            pack_version: "1.0.0".to_owned(),
        },
        InvestigationPackRef {
            pack_id: "store-integrity.v1".to_owned(),
            pack_version: "1.0.0".to_owned(),
        },
    ];
    fixture.request.budget.limit.evidence_items = 16;
    fixture.request.budget.limit.output_bytes = 2 * 1024 * 1024;
    fixture.request.budget.max_steps = 32;
    let principal = ConnectorPrincipal {
        subject: format!("synthetic-d15-{}", fixture.request.run_id),
        issuer: "https://d15-connector.invalid".to_owned(),
    };
    sqlx::query("INSERT INTO connector_identities(id,cluster_id,subject,issuer) VALUES($1,$2,$3,$4)")
        .bind(uuid::Uuid::new_v4())
        .bind(fixture.request.scope.cluster_id.as_uuid())
        .bind(&principal.subject)
        .bind(&principal.issuer)
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    let session = ConnectorSessionId::new();
    let capability = ConnectorCapabilityState {
        mutation_supported: false,
        sources: ["admin-query", "prometheus", "rocketmq-mcp"]
            .map(|source| ConnectorSourceCapability {
                source: source.to_owned(),
                schema_major: 1,
                status: ConnectorSourceStatus::Queryable,
                max_rows: 100,
                max_bytes: crate::connector_channel::MAX_RESPONSE_BYTES as u64,
                max_time_range_seconds: 3600,
                last_success_at: None,
                freshness_seconds: None,
            })
            .to_vec(),
    };
    fixture
        .state
        .connector_channel
        .register(
            &principal,
            &ConnectorRegister {
                schema: channel_schema(),
                session_id: session,
                tenant_id: fixture.auth.tenant_id,
                cluster_id: fixture.request.scope.cluster_id,
                subject: principal.subject.clone(),
                capability,
                observed_at: Utc::now(),
            },
        )
        .await
        .unwrap();
    (fixture, principal, session)
}

async fn prepared_range() -> (Fixture, ConnectorPrincipal, ConnectorSessionId) {
    let (mut fixture, principal, session) = prepared().await;
    fixture.request.scope.resource = Some(InvestigationResource {
        kind: ResourceKind::Topic,
        key: "orders".to_owned(),
    });
    fixture.request.packs = vec![
        InvestigationPackRef {
            pack_id: "broker-health.v1".to_owned(),
            pack_version: "1.0.0".to_owned(),
        },
        InvestigationPackRef {
            pack_id: "send-latency.v1".to_owned(),
            pack_version: "1.0.0".to_owned(),
        },
    ];
    let inventory = uuid::Uuid::new_v4();
    let at = Utc::now();
    let digest = format!("sha256:{}", "0".repeat(64));
    sqlx::query("INSERT INTO asset_inventory_snapshots(id,tenant_id,cluster_id,sources,observed_at,freshness_seconds,partial,content_hash) VALUES($1,$2,$3,$4,$5,0,false,$6)").bind(inventory).bind(fixture.auth.tenant_id.as_uuid()).bind(fixture.request.scope.cluster_id.as_uuid()).bind(json!(["synthetic regression; no production verification"])).bind(at).bind(&digest).execute(&fixture.repository.pool).await.unwrap();
    sqlx::query("INSERT INTO topology_edges(id,tenant_id,cluster_id,from_key,to_key,relation,source,observed_at,freshness_seconds,partial,content_hash,inventory_snapshot_id) VALUES($1,$2,$3,'topic:orders','broker:broker-a','routes_to','synthetic-regression',$4,0,false,$5,$6)").bind(uuid::Uuid::new_v4()).bind(fixture.auth.tenant_id.as_uuid()).bind(fixture.request.scope.cluster_id.as_uuid()).bind(at).bind(&digest).bind(inventory).execute(&fixture.repository.pool).await.unwrap();
    (fixture, principal, session)
}

async fn next_query(
    fixture: &Fixture,
    principal: &ConnectorPrincipal,
    session: ConnectorSessionId,
    after: u64,
) -> ConnectorQueryEnvelope {
    let poll = fixture
        .state
        .connector_channel
        .poll(
            principal,
            session,
            &PollRequest {
                schema: channel_schema(),
                session_id: session,
                after_sequence: after,
                wait_millis: 10_000,
                max_commands: 1,
            },
        )
        .await
        .unwrap();
    let [ConnectorCommand::Query { envelope }] = poll.commands.as_slice() else {
        panic!("expected the next persisted fixed read: {:?}", poll.commands);
    };
    envelope.clone()
}

fn healthy_snapshot(query: &EvidenceQuery) -> EvidenceSnapshot {
    let content = match query.resource.split('/').next().unwrap() {
        "store-pressure" => {
            json!({"disk_pressure":false,"flush_or_dispatch_stalled":false,"tiered_wal_pressure":false})
        }
        "store-integrity" => {
            json!({"offset_inconsistent":false,"recovery_incomplete":false,"index_rebuild_failed":false})
        }
        "store-trend" => json!({"synthetic_context":"bounded synthetic fixture; no production verification"}),
        "broker-health" => {
            json!({"ready":true,"request_error_rate_percent":0.0,"active_connections":10,"cpu_percent":10.0,"rss_bytes":1024,"disk_used_percent":10.0,"flush_latency_ms":1.0,"dispatch_backlog":0,"ha_lag_bytes":0})
        }
        "send-latency" => json!({"client_or_proxy_slow":false,"remoting_or_broker_slow":false,"store_slow":false}),
        "broker-runtime" => json!({"broker_up":false}),
        "broker-connections" => json!({"synthetic_context":"current connection inventory"}),
        unexpected => panic!("unexpected closed query {unexpected}"),
    };
    let mut snapshot = EvidenceSnapshot {
        schema: SchemaVersion::new("rocketmq-sre.evidence", 1, 0),
        evidence_id: EvidenceId::new(),
        query_id: query.query_id,
        correlation_id: query.correlation_id,
        tenant_id: query.tenant_id,
        cluster_id: query.cluster_id,
        source: query.source.clone(),
        resource: query.resource.clone(),
        time_range: query.time_range.clone(),
        observed_at: Utc::now(),
        freshness_seconds: 0,
        partial: false,
        warnings: vec!["synthetic regression, not production verification".to_owned()],
        sensitivity: Sensitivity::Internal,
        coverage: CoverageStatus::Available,
        exposure: EvidenceExposure::Synthetic,
        content: EvidenceContent::Inline(content),
        content_hash: String::new(),
    };
    snapshot.content_hash = snapshot.compute_content_hash().unwrap();
    snapshot
}

async fn respond(
    fixture: &Fixture,
    principal: &ConnectorPrincipal,
    envelope: &ConnectorQueryEnvelope,
    snapshot: EvidenceSnapshot,
) {
    fixture
        .state
        .connector_channel
        .submit_response(
            principal,
            envelope.session_id,
            &ConnectorResponseEnvelope {
                schema: channel_schema(),
                session_id: envelope.session_id,
                correlation_id: envelope.correlation_id,
                sequence: envelope.sequence,
                evidence: Some(snapshot),
                error_code: None,
                retryable: false,
            },
        )
        .await
        .unwrap();
}

fn charged_reads(run: &InvestigationRun) -> u64 {
    let (settled, reserved) = run.budget_usage().unwrap();
    settled.read_queries + reserved.read_queries
}

mod models;
mod persistence;
mod quality;
mod runtime;
mod store_pressure;
