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

use chrono::{DateTime, Duration, Utc};
use rocketmq_sre_contracts::*;
use serde_json::json;

#[test]
fn store_pressure_v2_proof_sufficiency_rejects_forged_completeness_with_real_missing_facts() {
    let at = DateTime::<Utc>::from_timestamp_millis(1794657600000).unwrap();
    let tenant = TenantId::new();
    let cluster = ClusterId::new();
    let observation: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../tests/fixtures/diagnostics/store-pressure-observation.v1.json"
    ))
    .unwrap();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: tenant,
        cluster_id: cluster,
        source: "admin-query".to_owned(),
        resource: "store-pressure/broker-a".to_owned(),
        time_range: TimeRange::new(at, at).unwrap(),
    };
    let mut snapshot = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        at,
        EvidenceContent::Inline(json!({
        "schema_version":"rocketmq.sre-diagnostic-source.v1","profile":"store_pressure","observed_brokers":1,
        "brokers":[{"broker_name":"broker-a","broker_id":0,"store_pressure":observation}]})),
    )
    .unwrap();
    snapshot.exposure = EvidenceExposure::Synthetic;
    // This pure helper never writes a record. The service separately rejects
    // synthetic evidence; here even a caller's full-coverage claim is insufficient.
    assert!(!snapshot.partial);
    assert_eq!(snapshot.coverage, CoverageStatus::Available);
    let request = CapabilityVerificationRequest {
        schema_version: CAPABILITY_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: tenant,
        cluster_id: cluster,
        pack_id: "store-pressure.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
        level: CapabilityVerificationLevel::Production,
        method: CapabilityVerificationMethod::LiveReadOnly,
        outcome: CapabilityVerificationOutcome::Passed,
        resource: Some(snapshot.resource.clone()),
        investigation_id: None,
        revision_id: None,
        time_range: snapshot.time_range.clone(),
        observed_at: at,
        valid_until: at + Duration::seconds(60),
        source_ref: "synthetic-contract-sufficiency-not-a-production-report".to_owned(),
        report_digest: format!("sha256:{}", "0".repeat(64)),
        evidence_ids: vec![snapshot.evidence_id],
    };
    assert!(!super::qualified(&request, &[snapshot], at).unwrap());
}
