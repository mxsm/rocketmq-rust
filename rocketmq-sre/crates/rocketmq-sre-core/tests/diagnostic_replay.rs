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
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_core::diagnostics::DIAGNOSTIC_OUTPUT_SCHEMA_FAMILY;
use rocketmq_sre_core::diagnostics::DiagnosticEngine;
use rocketmq_sre_core::diagnostics::DiagnosticError;
use rocketmq_sre_core::diagnostics::DiagnosticStatus;
use rocketmq_sre_core::diagnostics::wave_a_registry;
use serde_json::Value;

const REPLAY_FIXTURES: &[&str] = &[
    include_str!("../../../tests/fixtures/diagnostics/cluster-topology.v1/normal.json"),
    include_str!("../../../tests/fixtures/diagnostics/cluster-topology.v1/fault.json"),
    include_str!("../../../tests/fixtures/diagnostics/cluster-topology.v1/missing.json"),
    include_str!("../../../tests/fixtures/diagnostics/consumer-lag.v2/normal.json"),
    include_str!("../../../tests/fixtures/diagnostics/consumer-lag.v2/fault.json"),
    include_str!("../../../tests/fixtures/diagnostics/consumer-lag.v2/missing.json"),
    include_str!("../../../tests/fixtures/diagnostics/consumer-runtime.v1/normal.json"),
    include_str!("../../../tests/fixtures/diagnostics/consumer-runtime.v1/fault.json"),
    include_str!("../../../tests/fixtures/diagnostics/consumer-runtime.v1/missing.json"),
    include_str!("../../../tests/fixtures/diagnostics/producer-connectivity.v1/normal.json"),
    include_str!("../../../tests/fixtures/diagnostics/producer-connectivity.v1/fault.json"),
    include_str!("../../../tests/fixtures/diagnostics/producer-connectivity.v1/missing.json"),
    include_str!("../../../tests/fixtures/diagnostics/broker-health.v1/normal.json"),
    include_str!("../../../tests/fixtures/diagnostics/broker-health.v1/fault.json"),
    include_str!("../../../tests/fixtures/diagnostics/broker-health.v1/missing.json"),
    include_str!("../../../tests/fixtures/diagnostics/message-path.v1/normal.json"),
    include_str!("../../../tests/fixtures/diagnostics/message-path.v1/fault.json"),
    include_str!("../../../tests/fixtures/diagnostics/message-path.v1/missing.json"),
    include_str!("../../../tests/fixtures/diagnostics/telemetry-pipeline.v1/normal.json"),
    include_str!("../../../tests/fixtures/diagnostics/telemetry-pipeline.v1/fault.json"),
    include_str!("../../../tests/fixtures/diagnostics/telemetry-pipeline.v1/missing.json"),
    include_str!("../../../tests/fixtures/diagnostics/deployment-drift.v1/normal.json"),
    include_str!("../../../tests/fixtures/diagnostics/deployment-drift.v1/fault.json"),
    include_str!("../../../tests/fixtures/diagnostics/deployment-drift.v1/missing.json"),
];

const PACK_CONFIGS: &[(&str, &str)] = &[
    (
        "cluster-topology.v1",
        include_str!("../../../config/diagnostics/cluster-topology.v1.yaml"),
    ),
    (
        "consumer-lag.v2",
        include_str!("../../../config/diagnostics/consumer-lag.v2.yaml"),
    ),
    (
        "consumer-runtime.v1",
        include_str!("../../../config/diagnostics/consumer-runtime.v1.yaml"),
    ),
    (
        "producer-connectivity.v1",
        include_str!("../../../config/diagnostics/producer-connectivity.v1.yaml"),
    ),
    (
        "broker-health.v1",
        include_str!("../../../config/diagnostics/broker-health.v1.yaml"),
    ),
    (
        "message-path.v1",
        include_str!("../../../config/diagnostics/message-path.v1.yaml"),
    ),
    (
        "telemetry-pipeline.v1",
        include_str!("../../../config/diagnostics/telemetry-pipeline.v1.yaml"),
    ),
    (
        "deployment-drift.v1",
        include_str!("../../../config/diagnostics/deployment-drift.v1.yaml"),
    ),
];

struct ReplayFixture {
    pack: String,
    scenario: String,
    expected_status: DiagnosticStatus,
    expected_reason_codes: BTreeSet<String>,
    evidence: Vec<EvidenceSnapshot>,
}

#[test]
fn offline_replay_is_deterministic_and_every_conclusion_cites_existing_evidence() {
    let engine = DiagnosticEngine::new(wave_a_registry().expect("Wave A registry should be valid"));
    assert_eq!(REPLAY_FIXTURES.len(), 24);

    for raw in REPLAY_FIXTURES {
        let fixture = parse_fixture(raw);
        let first = engine
            .evaluate(&fixture.pack, &fixture.evidence)
            .unwrap_or_else(|error| panic!("{} {} should replay: {error}", fixture.pack, fixture.scenario));
        let second = engine
            .evaluate(&fixture.pack, &fixture.evidence)
            .unwrap_or_else(|error| panic!("{} {} should replay twice: {error}", fixture.pack, fixture.scenario));

        assert_eq!(
            first, second,
            "{} {} must be deterministic",
            fixture.pack, fixture.scenario
        );
        assert_eq!(
            first.status, fixture.expected_status,
            "{} {} returned an unexpected status",
            fixture.pack, fixture.scenario
        );
        assert_eq!(first.output_schema.family, DIAGNOSTIC_OUTPUT_SCHEMA_FAMILY);
        let actual_codes = first
            .findings
            .iter()
            .map(|finding| finding.reason_code.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual_codes, fixture.expected_reason_codes,
            "{} {} returned unexpected reason codes",
            fixture.pack, fixture.scenario
        );

        let existing_ids = fixture
            .evidence
            .iter()
            .map(|snapshot| snapshot.evidence_id)
            .collect::<BTreeSet<_>>();
        for finding in &first.findings {
            assert!(
                !finding.supporting_evidence.is_empty(),
                "{} must cite supporting evidence",
                finding.reason_code
            );
            for citation in finding.supporting_evidence.iter().chain(&finding.counter_evidence) {
                assert!(
                    existing_ids.contains(&citation.evidence_id),
                    "{} cites missing evidence {}",
                    finding.reason_code,
                    citation.evidence_id
                );
            }
        }

        if fixture.scenario == "missing" {
            assert!(!first.missing_required_evidence.is_empty());
            assert!(
                first.findings.iter().all(|finding| finding.confidence.percent <= 49),
                "missing required evidence must not produce high confidence"
            );
        }
    }
}

#[test]
fn config_catalog_matches_registered_pack_metadata_and_rule_codes() {
    let registry = wave_a_registry().expect("Wave A registry should be valid");

    assert_eq!(PACK_CONFIGS.len(), registry.len());
    for (qualified_id, config) in PACK_CONFIGS {
        let pack = registry
            .resolve(qualified_id)
            .unwrap_or_else(|| panic!("{qualified_id} should be registered"));
        assert!(config.contains(&format!("qualified_id: {qualified_id}")));
        assert!(config.contains("output_schema: rocketmq-sre.diagnostic-result.v1"));
        assert!(config.contains("missing_required_cap: 49"));
        for code in pack.rule_codes() {
            assert!(config.contains(code), "{qualified_id} config must document {code}");
        }
    }
}

#[test]
fn message_path_rejects_body_content_before_rules_run() {
    let fixture = parse_fixture(include_str!(
        "../../../tests/fixtures/diagnostics/message-path.v1/body-rejected.json"
    ));
    let evidence_id = fixture.evidence[0].evidence_id;
    let engine = DiagnosticEngine::new(wave_a_registry().expect("Wave A registry should be valid"));

    assert_eq!(
        engine.evaluate(&fixture.pack, &fixture.evidence),
        Err(DiagnosticError::MessageBodyRejected { evidence_id })
    );
}

#[test]
fn local_only_required_evidence_returns_unsupported_without_guessing() {
    let mut fixture = parse_fixture(include_str!(
        "../../../tests/fixtures/diagnostics/telemetry-pipeline.v1/normal.json"
    ));
    fixture.evidence[0].coverage = CoverageStatus::NotProductionVerified;
    let engine = DiagnosticEngine::new(wave_a_registry().expect("Wave A registry should be valid"));

    let report = engine
        .evaluate(&fixture.pack, &fixture.evidence)
        .expect("local-only coverage is a supported diagnostic outcome");

    assert_eq!(report.status, DiagnosticStatus::Unsupported);
    assert!(report.findings.is_empty());
}

#[test]
fn incomplete_required_snapshot_is_inconclusive_and_low_confidence() {
    let mut fixture = parse_fixture(include_str!(
        "../../../tests/fixtures/diagnostics/consumer-lag.v2/normal.json"
    ));
    let EvidenceContent::Inline(content) = &mut fixture.evidence[0].content else {
        panic!("fixture evidence should be inline");
    };
    content
        .as_object_mut()
        .expect("fixture content should be an object")
        .remove("lag_slope_per_min");
    fixture.evidence[0].content_hash = fixture.evidence[0]
        .compute_content_hash()
        .expect("modified fixture should canonicalize");
    let engine = DiagnosticEngine::new(wave_a_registry().expect("Wave A registry should be valid"));

    let report = engine
        .evaluate(&fixture.pack, &fixture.evidence)
        .expect("incomplete snapshots produce an inconclusive report");

    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert_eq!(report.findings.len(), 1);
    assert!(report.findings[0].confidence.percent <= 49);
    assert!(
        report.findings[0]
            .missing_evidence
            .contains(&"consumer-lag.lag_slope_per_min".to_owned())
    );
}

fn parse_fixture(raw: &str) -> ReplayFixture {
    let value: Value = serde_json::from_str(raw).expect("fixture should be valid JSON");
    let pack = required_string(&value, "pack");
    let scenario = required_string(&value, "scenario");
    let expected_status = value
        .get("expected_status")
        .and_then(Value::as_str)
        .map(parse_status)
        .unwrap_or(DiagnosticStatus::Inconclusive);
    let expected_reason_codes = value
        .get("expected_reason_codes")
        .and_then(Value::as_array)
        .map(|codes| {
            codes
                .iter()
                .map(|code| code.as_str().expect("reason code should be a string").to_owned())
                .collect()
        })
        .unwrap_or_default();
    let evidence = value
        .get("evidence")
        .and_then(Value::as_array)
        .expect("fixture evidence should be an array")
        .iter()
        .map(snapshot_from_fixture)
        .collect();

    ReplayFixture {
        pack,
        scenario,
        expected_status,
        expected_reason_codes,
        evidence,
    }
}

fn snapshot_from_fixture(value: &Value) -> EvidenceSnapshot {
    let observed_at = Utc
        .with_ymd_and_hms(2026, 7, 27, 0, 0, 0)
        .single()
        .expect("fixture timestamp should be valid");
    let tenant_id = "00000000-0000-4000-8000-000000000001"
        .parse::<TenantId>()
        .expect("fixture tenant ID should be valid");
    let cluster_id = "00000000-0000-4000-8000-000000000002"
        .parse::<ClusterId>()
        .expect("fixture cluster ID should be valid");
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id,
        cluster_id,
        source: required_string(value, "source"),
        resource: required_string(value, "resource"),
        time_range: TimeRange::new(observed_at, observed_at).expect("fixture time range should be valid"),
    };
    let content = value.get("content").cloned().expect("fixture content should exist");
    let mut snapshot = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        observed_at,
        EvidenceContent::Inline(content),
    )
    .expect("fixture content should canonicalize");
    snapshot.evidence_id = required_string(value, "evidence_id")
        .parse::<EvidenceId>()
        .expect("fixture evidence ID should be valid");
    snapshot.partial = value.get("partial").and_then(Value::as_bool).unwrap_or(false);
    snapshot.coverage = value
        .get("coverage")
        .and_then(Value::as_str)
        .map(parse_coverage)
        .unwrap_or(CoverageStatus::Available);
    snapshot
}

fn required_string(value: &Value, field: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("fixture field `{field}` should be a string"))
        .to_owned()
}

fn parse_status(value: &str) -> DiagnosticStatus {
    match value {
        "healthy" => DiagnosticStatus::Healthy,
        "fault" => DiagnosticStatus::Fault,
        "inconclusive" => DiagnosticStatus::Inconclusive,
        "unsupported" => DiagnosticStatus::Unsupported,
        other => panic!("unknown fixture status `{other}`"),
    }
}

fn parse_coverage(value: &str) -> CoverageStatus {
    match value {
        "available" => CoverageStatus::Available,
        "partial" => CoverageStatus::Partial,
        "missing" => CoverageStatus::Missing,
        "not_production_verified" => CoverageStatus::NotProductionVerified,
        other => panic!("unknown fixture coverage `{other}`"),
    }
}
