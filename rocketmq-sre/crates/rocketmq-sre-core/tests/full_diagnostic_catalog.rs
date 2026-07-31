// Copyright 2023 The RocketMQ Rust Authors
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

use chrono::TimeZone;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_core::diagnostics::DiagnosticEngine;
use rocketmq_sre_core::diagnostics::DiagnosticStatus;
use rocketmq_sre_core::diagnostics::full_registry;
use serde_json::Value;

const WAVE_B_FIXTURES: &str = include_str!("../../../tests/fixtures/diagnostics/wave-b/catalog.v1.json");
const WAVE_C_FIXTURES: &str = include_str!("../../../tests/fixtures/diagnostics/wave-c/catalog.v1.json");
const WAVE_B_CONFIG: &str = include_str!("../../../config/diagnostics/wave-b/packs.v1.yaml");
const WAVE_C_CONFIG: &str = include_str!("../../../config/diagnostics/wave-c/packs.v1.yaml");

#[test]
fn all_saved_wave_b_and_c_fixtures_replay_offline() {
    let engine = DiagnosticEngine::new(full_registry().expect("complete registry"));
    let mut pack_ids = BTreeSet::new();
    let mut fixture_count = 0usize;

    for raw in [WAVE_B_FIXTURES, WAVE_C_FIXTURES] {
        let catalog: Value = serde_json::from_str(raw).expect("fixture catalog should be JSON");
        assert_eq!(catalog["schema"], "rocketmq-sre.diagnostic-fixture-catalog.v1");
        let fixtures = catalog["fixtures"]
            .as_array()
            .expect("fixture catalog should contain fixtures");
        fixture_count = fixture_count.saturating_add(fixtures.len());
        for fixture in fixtures {
            reject_sensitive_fixture_keys(fixture);
            let pack = required_text(fixture, "pack");
            let scenario = required_text(fixture, "scenario");
            pack_ids.insert(pack.to_owned());
            let evidence = fixture["evidence"]
                .as_array()
                .expect("fixture evidence should be an array")
                .iter()
                .map(snapshot)
                .collect::<Vec<_>>();
            let report = engine
                .evaluate(pack, &evidence)
                .unwrap_or_else(|error| panic!("{pack} {scenario} should replay: {error}"));
            assert_eq!(
                report.status,
                parse_status(required_text(fixture, "expected_status")),
                "{pack} {scenario}"
            );
            let actual_codes = report
                .findings
                .iter()
                .map(|finding| finding.reason_code.as_str())
                .collect::<BTreeSet<_>>();
            let expected_codes = fixture["expected_reason_codes"]
                .as_array()
                .expect("expected reason codes should be an array")
                .iter()
                .map(|code| code.as_str().expect("reason code should be text"))
                .collect::<BTreeSet<_>>();
            assert_eq!(actual_codes, expected_codes, "{pack} {scenario}");
            if scenario == "missing" {
                assert!(!report.missing_required_evidence.is_empty(), "{pack}");
            }
        }
    }

    assert_eq!(fixture_count, 72);
    assert_eq!(pack_ids.len(), 24);
}

#[test]
fn generated_threshold_catalogs_document_every_fixture_pack() {
    for raw in [WAVE_B_FIXTURES, WAVE_C_FIXTURES] {
        let catalog: Value = serde_json::from_str(raw).expect("fixture catalog should be JSON");
        let config = if catalog["wave"] == "B" {
            WAVE_B_CONFIG
        } else {
            WAVE_C_CONFIG
        };
        for fixture in catalog["fixtures"]
            .as_array()
            .expect("fixture catalog should contain fixtures")
        {
            let pack = required_text(fixture, "pack");
            assert!(config.contains(&format!("qualified_id: {pack}")), "{pack}");
            assert!(config.contains("rules_dsl_enabled: false"));
            assert!(config.contains("fixture_scenarios: [normal, fault, missing]"));
        }
    }
}

fn snapshot(value: &Value) -> EvidenceSnapshot {
    let observed_at = Utc
        .with_ymd_and_hms(2026, 7, 27, 0, 0, 0)
        .single()
        .expect("valid fixture timestamp");
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: "00000000-0000-4000-8000-000000000001"
            .parse::<TenantId>()
            .expect("valid tenant ID"),
        cluster_id: "00000000-0000-4000-8000-000000000002"
            .parse::<ClusterId>()
            .expect("valid cluster ID"),
        source: required_text(value, "source").to_owned(),
        resource: required_text(value, "resource").to_owned(),
        time_range: TimeRange::new(observed_at, observed_at).expect("valid fixture range"),
    };
    let mut snapshot = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        observed_at,
        EvidenceContent::Inline(value["content"].clone()),
    )
    .expect("fixture should canonicalize");
    snapshot.coverage = match required_text(value, "coverage") {
        "available" => CoverageStatus::Available,
        "partial" => CoverageStatus::Partial,
        "missing" => CoverageStatus::Missing,
        "not_production_verified" => CoverageStatus::NotProductionVerified,
        other => panic!("unknown fixture coverage {other}"),
    };
    snapshot
}

fn required_text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field]
        .as_str()
        .unwrap_or_else(|| panic!("fixture field {field} should be text"))
}

fn parse_status(value: &str) -> DiagnosticStatus {
    match value {
        "healthy" => DiagnosticStatus::Healthy,
        "fault" => DiagnosticStatus::Fault,
        "inconclusive" => DiagnosticStatus::Inconclusive,
        "unsupported" => DiagnosticStatus::Unsupported,
        other => panic!("unknown fixture status {other}"),
    }
}

fn reject_sensitive_fixture_keys(value: &Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                assert!(
                    !matches!(
                        key.as_str(),
                        "body" | "message_body" | "token" | "secret" | "private_key" | "acl_material" | "tls_material"
                    ),
                    "fixture must not contain sensitive key {key}"
                );
                reject_sensitive_fixture_keys(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                reject_sensitive_fixture_keys(value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}
