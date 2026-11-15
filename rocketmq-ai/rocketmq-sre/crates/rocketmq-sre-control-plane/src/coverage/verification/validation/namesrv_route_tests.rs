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
use rocketmq_sre_contracts::*;

#[test]
fn namesrv_current_advisory_and_forged_complete_flag_never_establish_historical_or_production_proof() {
    let content: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../tests/fixtures/diagnostics/namesrv-route-observation.v1.json"
    ))
    .unwrap();
    let tenant: TenantId = content["tenant_id"].as_str().unwrap().parse().unwrap();
    let cluster: ClusterId = content["cluster_id"].as_str().unwrap().parse().unwrap();
    let at: DateTime<Utc> = content["collected_at"].as_str().unwrap().parse().unwrap();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: tenant,
        cluster_id: cluster,
        source: "admin-query".to_owned(),
        resource: "namesrv-route/orders".to_owned(),
        time_range: TimeRange::new(
            content["requested_window"]["start"].as_str().unwrap().parse().unwrap(),
            content["requested_window"]["end"].as_str().unwrap().parse().unwrap(),
        )
        .unwrap(),
    };
    let mut snapshot =
        EvidenceSnapshot::capture(query, current_evidence_schema(), at, EvidenceContent::Inline(content)).unwrap();
    snapshot.exposure = EvidenceExposure::AdminRpc;
    snapshot.freshness_seconds = 1;
    snapshot.partial = false;
    snapshot.coverage = CoverageStatus::Available;
    snapshot
        .warnings
        .push("SyntheticFixture: no source or production validation record".to_owned());
    let registry = full_registry().unwrap();
    let packs = [
        registry.resolve("namesrv-route.v1").unwrap(),
        registry.resolve("namesrv-route.v2").unwrap(),
        registry.resolve("producer-connectivity.v1").unwrap(),
    ];
    let projection = quality::project(
        tenant,
        Some(cluster),
        at,
        &packs,
        &BTreeMap::new(),
        std::slice::from_ref(&snapshot),
        false,
    );
    let current = projection
        .packs
        .iter()
        .find(|pack| pack.pack_id == "namesrv-route.v2")
        .unwrap();
    assert_eq!(current.requirements[0].quality, DiagnosticEvidenceQuality::Partial);
    assert_eq!(current.requirements[0].evidence_ids, vec![snapshot.evidence_id]);
    assert!(
        current.requirements[0]
            .missing_fields
            .iter()
            .any(|field| field == "registration.last_heartbeat_time")
    );
    assert_ne!(current.layers.target_observed.status, CapabilityFactStatus::Present);
    assert!(!projection.execution_eligible);
    let scenario = projection
        .scenarios
        .iter()
        .find(|scenario| scenario.id == "routing_disconnect")
        .unwrap();
    assert_eq!(scenario.pack_ids, ["namesrv-route.v2", "producer-connectivity.v1"]);
    let mut legacy = projection.clone();
    quality::bind_primary_revision(&mut legacy, "namesrv-route.v1");
    assert_eq!(
        legacy
            .scenarios
            .iter()
            .find(|scenario| scenario.id == "routing_disconnect")
            .unwrap()
            .pack_ids,
        ["namesrv-route.v1", "producer-connectivity.v1"]
    );
    let request = CapabilityVerificationRequest {
        schema_version: CAPABILITY_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: tenant,
        cluster_id: cluster,
        pack_id: "namesrv-route.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
        level: Level::Production,
        method: Method::LiveReadOnly,
        outcome: Outcome::Passed,
        resource: Some(snapshot.resource.clone()),
        investigation_id: None,
        revision_id: None,
        time_range: snapshot.time_range.clone(),
        observed_at: at,
        valid_until: at + Duration::seconds(60),
        source_ref: "SyntheticFixture-no-production-proof".to_owned(),
        report_digest: format!("sha256:{}", "0".repeat(64)),
        evidence_ids: vec![snapshot.evidence_id],
    };
    assert!(!qualified(&request, std::slice::from_ref(&snapshot), at).unwrap());
    for invalid in ["foreign", "hash", "stale"] {
        let mut bad = snapshot.clone();
        let when = if invalid == "stale" {
            at + Duration::seconds(301)
        } else {
            at
        };
        if invalid == "foreign" {
            bad.resource = "namesrv-route/other".to_owned();
            bad.content_hash = bad.compute_content_hash().unwrap();
        }
        if invalid == "hash" {
            bad.content_hash = "invalid".to_owned();
        }
        let quality = quality::project(tenant, Some(cluster), when, &packs, &BTreeMap::new(), &[bad], false);
        let requirement = &quality
            .packs
            .iter()
            .find(|pack| pack.pack_id == "namesrv-route.v2")
            .unwrap()
            .requirements[0];
        assert!(matches!(
            requirement.quality,
            DiagnosticEvidenceQuality::Unknown | DiagnosticEvidenceQuality::Stale
        ));
    }
}
