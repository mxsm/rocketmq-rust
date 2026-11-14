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
fn send_latency_available_flag_and_local_p99_do_not_establish_full_path_or_production_proof() {
    let content: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../tests/fixtures/diagnostics/send-latency-window.v1.json"
    ))
    .unwrap();
    let tenant: TenantId = content["scope"]["tenant_id"].as_str().unwrap().parse().unwrap();
    let cluster: ClusterId = content["scope"]["cluster_id"].as_str().unwrap().parse().unwrap();
    let at: DateTime<Utc> = content["collected_at"].as_str().unwrap().parse().unwrap();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: tenant,
        cluster_id: cluster,
        source: "prometheus".to_owned(),
        resource: "send-latency/orders".to_owned(),
        time_range: TimeRange::new(
            content["window"]["requested_start"].as_str().unwrap().parse().unwrap(),
            content["window"]["requested_end"].as_str().unwrap().parse().unwrap(),
        )
        .unwrap(),
    };
    let mut snapshot =
        EvidenceSnapshot::capture(query, current_evidence_schema(), at, EvidenceContent::Inline(content)).unwrap();
    snapshot.freshness_seconds = 1;
    snapshot.partial = false;
    snapshot.coverage = CoverageStatus::Available;
    // SyntheticFixture: simulate verified transport metadata to reach the
    // independent field/proof gate. No verifier record is written by this test.
    snapshot.exposure = EvidenceExposure::PrometheusApi;
    snapshot
        .warnings
        .push("SyntheticFixture, no collected or production verification record".to_owned());
    let registry = full_registry().unwrap();
    let packs = [
        registry.resolve("send-latency.v1").unwrap(),
        registry.resolve("send-latency.v2").unwrap(),
        registry.resolve("broker-ha.v1").unwrap(),
    ];
    let mut projection = quality::project(
        tenant,
        Some(cluster),
        at,
        &packs,
        &BTreeMap::new(),
        std::slice::from_ref(&snapshot),
        false,
    );
    let numeric = projection
        .packs
        .iter()
        .find(|pack| pack.pack_id == "send-latency.v2")
        .unwrap();
    assert_eq!(numeric.requirements[0].quality, DiagnosticEvidenceQuality::Partial);
    assert_eq!(numeric.requirements[0].evidence_ids, vec![snapshot.evidence_id]);
    assert_ne!(numeric.layers.target_observed.status, CapabilityFactStatus::Present);
    assert!(
        numeric.requirements[0]
            .missing_fields
            .iter()
            .any(|field| field.contains("client_exact_topic"))
    );
    let scenario = projection
        .scenarios
        .iter()
        .find(|scenario| scenario.id == "send_latency_ha")
        .unwrap();
    assert_eq!(scenario.pack_ids, ["send-latency.v2", "broker-ha.v1"]);
    quality::bind_primary_revision(&mut projection, "send-latency.v1");
    let scenario = projection
        .scenarios
        .iter()
        .find(|scenario| scenario.id == "send_latency_ha")
        .unwrap();
    assert_eq!(scenario.pack_ids, ["send-latency.v1", "broker-ha.v1"]);
    assert!(
        scenario
            .required_signal_ids
            .iter()
            .any(|id| id.starts_with("send-latency.v1:"))
    );
    assert!(!projection.execution_eligible);
    let request = CapabilityVerificationRequest {
        schema_version: CAPABILITY_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: tenant,
        cluster_id: cluster,
        pack_id: "send-latency.v2".to_owned(),
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
        source_ref: "synthetic-sufficiency-not-production-verification".to_owned(),
        report_digest: format!("sha256:{}", "0".repeat(64)),
        evidence_ids: vec![snapshot.evidence_id],
    };
    assert!(!qualified(&request, &[snapshot], at).unwrap());
}
