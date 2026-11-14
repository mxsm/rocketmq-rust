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

use super::*;

pub(super) fn snapshot(
    scope: &InvestigationRunScope,
    source: &str,
    resource: &str,
    at: chrono::DateTime<chrono::Utc>,
    content: serde_json::Value,
) -> Box<EvidenceSnapshot> {
    let digest = canonical_sha256(&(scope, source, resource, at, &content)).unwrap();
    let hex = digest.strip_prefix("sha256:").unwrap();
    let identity = format!(
        "{}-{}-4{}-8{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[13..16],
        &hex[17..20],
        &hex[20..32]
    );
    let mut snapshot = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: identity.parse().unwrap(),
            correlation_id: identity.parse().unwrap(),
            tenant_id: scope.tenant_id,
            cluster_id: scope.cluster_id,
            source: source.to_owned(),
            resource: resource.to_owned(),
            time_range: scope.time_range.clone(),
        },
        current_evidence_schema(),
        at,
        EvidenceContent::Inline(content),
    )
    .unwrap();
    snapshot.evidence_id = identity.parse().unwrap();
    snapshot.exposure = EvidenceExposure::Synthetic;
    Box::new(snapshot)
}

pub(super) fn case() -> InvestigationReplayCase {
    let at = "2026-11-14T12:00:00Z".parse::<chrono::DateTime<chrono::Utc>>().unwrap();
    let scope = InvestigationRunScope {
        tenant_id: "00000000-0000-4000-8000-000000000001".parse().unwrap(),
        cluster_id: "00000000-0000-4000-8000-000000000002".parse().unwrap(),
        investigation_id: "00000000-0000-4000-8000-000000000003".parse().unwrap(),
        resource: Some(InvestigationResource {
            kind: ResourceKind::Broker,
            key: "broker-a".to_owned(),
        }),
        time_range: TimeRange::new(at - chrono::Duration::minutes(10), at).unwrap(),
    };
    let packs = ["store-pressure.v1", "store-integrity.v1"].map(|id| InvestigationPackRef {
        pack_id: id.to_owned(),
        pack_version: "1.0.0".to_owned(),
    });
    let mut bindings = vec![];
    for (pack, requirement, source, resource) in [
        (&packs[0], "store-pressure", "admin-query", "store-pressure/broker-a"),
        (&packs[1], "store-integrity", "admin-query", "store-integrity/broker-a"),
        (&packs[0], "store-trend", "prometheus", "store-trend/broker-a"),
        (&packs[1], "store-trend", "prometheus", "store-trend/broker-a"),
    ] {
        bindings.push(ReplayBinding {
            requirement: InvestigationRequirementRef {
                pack: pack.clone(),
                requirement_id: requirement.to_owned(),
            },
            source: source.to_owned(),
            resource: resource.to_owned(),
        });
    }
    let reads = [
        (
            "admin-query",
            "store-pressure/broker-a",
            json!({"disk_pressure":true,"flush_or_dispatch_stalled":false,"tiered_wal_pressure":false}),
        ),
        (
            "admin-query",
            "store-integrity/broker-a",
            json!({"offset_inconsistent":false,"recovery_incomplete":false,"index_rebuild_failed":false}),
        ),
        ("prometheus", "store-trend/broker-a", json!({"counter_signal":false})),
    ]
    .into_iter()
    .map(|(source, resource, content)| ReplayRead {
        source: source.to_owned(),
        resource: resource.to_owned(),
        outcomes: vec![ReplayReadOutcome::SavedEvidence {
            snapshot: snapshot(&scope, source, resource, at, content),
        }],
    })
    .collect();
    InvestigationReplayCase {
        schema_version: INVESTIGATION_REPLAY_SCHEMA_VERSION.to_owned(),
        case_id: "synthetic-store-disk-pressure".to_owned(),
        provenance: ReplayProvenance::SyntheticFixture {
            fixture_ref: "compiled-store-rules/pressure-and-integrity".to_owned(),
        },
        scope,
        packs: packs.to_vec(),
        baseline_pack: packs[0].clone(),
        as_of: at,
        deadline: at + chrono::Duration::minutes(15),
        budget: InvestigationBudget {
            limit: InvestigationBudgetCost {
                read_queries: 12,
                model_calls: 8,
                evidence_items: 32,
                output_bytes: 4_194_304,
            },
            max_steps: 32,
            max_in_flight: 2,
            max_attempts_per_step: 3,
            max_query_retries: 2,
        },
        bindings,
        reads,
        model: ReplayModel::Disabled,
        checkpoint: ReplayCheckpoint::None,
        refinement: ReplayRefinement::None,
    }
}

pub(super) fn counter_case() -> InvestigationReplayCase {
    let mut case = case();
    case.scope.resource = None;
    let packs =
        [("consumer-lag.v2", "2.0.0"), ("broker-health.v1", "1.0.0")].map(|(id, version)| InvestigationPackRef {
            pack_id: id.to_owned(),
            pack_version: version.to_owned(),
        });
    case.baseline_pack = packs[0].clone();
    case.packs = packs.to_vec();
    case.case_id = "synthetic-lag-with-healthy-store-counterevidence".to_owned();
    case.provenance = ReplayProvenance::SyntheticFixture {
        fixture_ref: "compiled-consumer-lag/broker-counterevidence".to_owned(),
    };
    case.bindings = [
        (&packs[0], "consumer-lag", "rocketmq-mcp", "consumer-lag/orders"),
        (&packs[0], "broker-store", "rocketmq-mcp", "broker-runtime/broker-a"),
        (&packs[1], "broker-metrics", "prometheus", "broker-health/broker-a"),
        (&packs[1], "broker-runtime", "rocketmq-mcp", "broker-runtime/broker-a"),
    ]
    .into_iter()
    .map(|(pack, id, source, resource)| ReplayBinding {
        requirement: InvestigationRequirementRef {
            pack: pack.clone(),
            requirement_id: id.to_owned(),
        },
        source: source.to_owned(),
        resource: resource.to_owned(),
    })
    .collect();
    case.reads = [("rocketmq-mcp","consumer-lag/orders",json!({"total_lag":100000,"lag_slope_per_min":100,"queue_skew_ratio":1,"consume_rate_per_sec":50,"produce_rate_per_sec":100})),("rocketmq-mcp","broker-runtime/broker-a",json!({"broker_ready":true,"store_healthy":true,"broker_up":true})),("prometheus","broker-health/broker-a",json!({"ready":true,"request_error_rate_percent":0,"active_connections":10,"cpu_percent":10,"rss_bytes":1024,"disk_used_percent":10,"flush_latency_ms":1,"dispatch_backlog":0,"ha_lag_bytes":0}))].into_iter().map(|(source,resource,content)|ReplayRead { source:source.to_owned(),resource:resource.to_owned(),outcomes:vec![ReplayReadOutcome::SavedEvidence { snapshot:snapshot(&case.scope,source,resource,case.as_of,content) }] }).collect();
    case
}

pub(super) fn broker_case() -> InvestigationReplayCase {
    let mut case = counter_case();
    case.case_id = "synthetic-broker-readiness".to_owned();
    case.provenance = ReplayProvenance::SyntheticFixture {
        fixture_ref: "compiled-broker-health".to_owned(),
    };
    case.packs.retain(|pack| pack.pack_id == "broker-health.v1");
    case.baseline_pack = case.packs[0].clone();
    case.bindings
        .retain(|binding| binding.requirement.pack == case.baseline_pack);
    case.reads.retain(|read| !read.resource.starts_with("consumer-lag/"));
    case
}
