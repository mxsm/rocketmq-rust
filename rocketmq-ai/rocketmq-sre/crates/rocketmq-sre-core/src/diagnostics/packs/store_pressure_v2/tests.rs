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
use rocketmq_sre_contracts::{
    ClusterId, CorrelationId, EvidenceQuery, EvidenceSnapshot, QueryId, TenantId, TimeRange, current_evidence_schema,
};
use serde_json::{Value, json};

use super::*;
use crate::diagnostics::{DiagnosticEngine, DiagnosticStatus, full_registry};

fn observation() -> Value {
    serde_json::from_str(include_str!(
        "../../../../../../tests/fixtures/diagnostics/store-pressure-observation.v1.json"
    ))
    .unwrap()
}

fn signal(value: &Value, key: &str) -> StorePressureSignal {
    assess_store_pressure_observation(value)
        .into_iter()
        .find(|signal| signal.key == key)
        .unwrap()
}

fn snapshot(content: Value) -> EvidenceSnapshot {
    let point = DateTime::<Utc>::from_timestamp_millis(1794657600000).unwrap();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        source: "admin-query".to_owned(),
        resource: "store-pressure/broker-a".to_owned(),
        time_range: TimeRange::new(point - Duration::seconds(1), point + Duration::seconds(1)).unwrap(),
        correlation_id: CorrelationId::new(),
    };
    EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        point,
        EvidenceContent::Inline(content),
    )
    .unwrap()
}

fn envelope(observations: Vec<Value>) -> Value {
    let brokers = observations
        .into_iter()
        .enumerate()
        .map(|(id, observation)| json!({"broker_name":"broker-a", "broker_id":id, "store_pressure":observation}))
        .collect::<Vec<_>>();
    json!({"schema_version":"rocketmq.sre-diagnostic-source.v1", "profile":"store_pressure", "observed_brokers":brokers.len(), "brokers":brokers})
}

#[test]
fn real_numeric_v2_keeps_v1_exact_and_does_not_call_admission_pressure_a_stall() {
    let engine = DiagnosticEngine::new(full_registry().unwrap());
    let report = engine
        .evaluate("store-pressure.v2", &[snapshot(envelope(vec![observation()]))])
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(
        report
            .missing_required_evidence
            .iter()
            .any(|fact| fact.contains("monotonic_progress"))
    );
    assert!(
        report
            .findings
            .iter()
            .any(|finding| finding.reason_code == "STORE_DISK_WARNING_RATIO_EXCEEDED")
    );
    assert!(
        report
            .findings
            .iter()
            .all(|finding| !finding.root_cause.contains("stalled"))
    );
    assert!(report.findings.iter().any(|finding| {
        finding
            .missing_evidence
            .iter()
            .any(|missing| missing.contains("flush_dispatch_progress"))
    }));
    let old = snapshot(json!({"disk_pressure":false, "flush_or_dispatch_stalled":false, "tiered_wal_pressure":false}));
    assert_eq!(
        engine
            .evaluate_version("store-pressure", PackVersion::new(1, 0, 0), &[old])
            .unwrap()
            .status,
        DiagnosticStatus::Healthy
    );
    assert_eq!(
        engine.registry().resolve("store-pressure.v2").unwrap().version(),
        PackVersion::new(2, 0, 0)
    );
    assert_eq!(
        engine.registry().resolve("store-pressure.v1").unwrap().version(),
        PackVersion::new(1, 0, 0)
    );
}

#[test]
fn strict_disk_boundaries_and_inclusive_admission_limits_match_the_actual_producers() {
    use StorePressureSignalState::*;
    let mut value = observation();
    for (key, threshold) in [
        ("disk_warning", 0.9),
        ("disk_forcible_cleanup", 0.85),
        ("disk_reclaim", 0.75),
    ] {
        value["disk"]["commit_log_max_used_ratio"] = json!(threshold);
        value["disk"]["consume_queue_used_ratio"] = json!(threshold);
        assert_eq!(signal(&value, key).state, WithinObservedPolicy);
        value["disk"]["commit_log_max_used_ratio"] = json!(threshold + 0.001);
        assert_eq!(signal(&value, key).state, Crossed);
    }
    value["runtime"]["sync_flush_queue_depth"] = json!(32);
    assert_eq!(signal(&value, "sync_flush_admission").state, Crossed);
    value["runtime"]["sync_flush_queue_depth"] = json!(31);
    value["runtime"]["sync_flush_oldest_wait_millis"] = json!(1500);
    assert_eq!(signal(&value, "sync_flush_admission").state, Crossed);
    value["runtime"]["dispatch_behind_bytes"] = json!(16384);
    assert_eq!(signal(&value, "dispatch_admission").state, Crossed);
}

#[test]
fn positive_counters_and_pins_are_context_and_disabled_limits_are_not_health() {
    use StorePressureSignalState::*;
    let mut value = observation();
    value["disk"]["commit_log_max_used_ratio"] = json!(0.3);
    for key in [
        "sync_flush_backlog_reject_depth",
        "sync_flush_backlog_reject_wait_millis",
        "dispatch_backlog_reject_bytes",
    ] {
        value["policy"][key] = json!(0);
    }
    assert_eq!(signal(&value, "sync_flush_admission").state, NotConfigured);
    assert_eq!(signal(&value, "dispatch_admission").state, NotConfigured);
    assert_eq!(signal(&value, "tiered_wal_pressure").state, Unknown);
    value["tiered_configured"] = json!(true);
    value["minimum_pinned_wal_segment"] = json!(999);
    assert_eq!(signal(&value, "tiered_wal_pressure").state, Unknown);
    value["tiered_configured"] = json!(false);
    assert_eq!(signal(&value, "tiered_wal_pressure").state, NotConfigured);
    let engine = DiagnosticEngine::new(full_registry().unwrap());
    let report = engine
        .evaluate("store-pressure.v2", &[snapshot(envelope(vec![value]))])
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(
        report
            .findings
            .iter()
            .all(|finding| finding.outcome == FindingOutcome::Inconclusive)
    );
}

#[test]
fn invalid_domains_probe_counts_and_unverified_policy_never_become_safe_disk_measurements() {
    use StorePressureSignalState::Unknown;
    for invalid in [json!(-1), json!(1.2), json!("NaN"), Value::Null] {
        let mut value = observation();
        value["disk"]["commit_log_max_used_ratio"] = invalid;
        value["disk"]["consume_queue_used_ratio"] = Value::Null;
        assert_eq!(signal(&value, "disk_warning").state, Unknown);
    }
    for (field, invalid) in [
        ("commit_log_expected_paths", json!(0)),
        ("commit_log_unknown_paths", json!(3)),
        ("commit_log_observed_paths", json!(3)),
    ] {
        let mut value = observation();
        value["disk"][field] = invalid;
        assert_eq!(signal(&value, "disk_warning").state, Unknown);
    }
    let mut value = observation();
    value["policy"]["disk_max_used_ratio"] = json!(0.95);
    assert_eq!(signal(&value, "disk_warning").state, Unknown);
    let mut value = observation();
    value["generation_status"] = json!("store_policy_generation_unverified");
    assert_eq!(signal(&value, "disk_warning").state, Unknown);
    assert_eq!(signal(&value, "sync_flush_admission").state, Unknown);
}

#[test]
fn instances_cannot_fill_each_others_absent_fields_and_stale_or_historical_points_are_not_qualified() {
    let engine = DiagnosticEngine::new(full_registry().unwrap());
    let mut first = observation();
    first["policy"] = Value::Null;
    let mut second = observation();
    second["disk"] = Value::Null;
    let report = engine
        .evaluate("store-pressure.v2", &[snapshot(envelope(vec![first, second]))])
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(
        report
            .findings
            .iter()
            .all(|finding| finding.outcome != FindingOutcome::Fault)
    );
    let mut outside = observation();
    outside["observed_at_millis"] = json!(1794657602000_u64);
    let report = engine
        .evaluate("store-pressure.v2", &[snapshot(envelope(vec![outside]))])
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(report.findings.iter().any(|finding| {
        finding
            .missing_evidence
            .iter()
            .any(|missing| missing.contains("point_in_window"))
    }));
    let mut stale = snapshot(envelope(vec![observation()]));
    stale.freshness_seconds = 301;
    assert_eq!(
        engine.evaluate("store-pressure.v2", &[stale]).unwrap().status,
        DiagnosticStatus::Inconclusive
    );
}

#[test]
fn an_old_instance_cannot_borrow_the_age_of_a_new_query_envelope_and_future_points_fail_closed() {
    let engine = DiagnosticEngine::new(full_registry().unwrap());
    for point in [1794657299000_u64, 1794657600001_u64] {
        let mut value = observation();
        value["observed_at_millis"] = json!(point);
        let mut evidence = snapshot(envelope(vec![value]));
        evidence.time_range.start -= Duration::minutes(10);
        evidence.content_hash = evidence.compute_content_hash().unwrap();
        let report = engine.evaluate("store-pressure.v2", &[evidence]).unwrap();
        assert_eq!(report.status, DiagnosticStatus::Inconclusive);
        assert!(
            report
                .findings
                .iter()
                .all(|finding| finding.outcome == FindingOutcome::Inconclusive)
        );
    }
}

#[test]
fn an_exact_broker_name_and_unique_instance_ids_are_required_even_for_saved_replay() {
    let engine = DiagnosticEngine::new(full_registry().unwrap());
    let mut wrong = envelope(vec![observation()]);
    wrong["brokers"][0]["broker_name"] = json!("other-broker");
    let mut repeated = envelope(vec![observation(), observation()]);
    repeated["brokers"][1]["broker_id"] = json!(0);
    let mut mismatched_generation = envelope(vec![observation()]);
    mismatched_generation["brokers"][0]["config"] = json!({"generation":9});
    for value in [wrong, repeated, mismatched_generation] {
        let report = engine.evaluate("store-pressure.v2", &[snapshot(value)]).unwrap();
        assert_eq!(report.status, DiagnosticStatus::Inconclusive);
        assert!(
            report
                .findings
                .iter()
                .all(|finding| finding.outcome == FindingOutcome::Inconclusive)
        );
    }
}

#[test]
fn source_points_keep_submillisecond_window_and_age_boundaries() {
    let engine = DiagnosticEngine::new(full_registry().unwrap());
    let mut evidence = snapshot(envelope(vec![observation()]));
    let point = evidence.observed_at;
    evidence.observed_at += Duration::milliseconds(10);
    evidence.time_range.start = point;
    evidence.content_hash = evidence.compute_content_hash().unwrap();
    let equal = engine
        .evaluate("store-pressure.v2", std::slice::from_ref(&evidence))
        .unwrap();
    assert!(
        equal
            .findings
            .iter()
            .any(|finding| finding.outcome == FindingOutcome::Fault)
    );
    evidence.time_range.start = point + Duration::microseconds(500);
    evidence.content_hash = evidence.compute_content_hash().unwrap();
    let outside = engine
        .evaluate("store-pressure.v2", std::slice::from_ref(&evidence))
        .unwrap();
    assert!(
        outside
            .findings
            .iter()
            .all(|finding| finding.outcome == FindingOutcome::Inconclusive)
    );
    evidence.time_range.start = point;
    evidence.content_hash = evidence.compute_content_hash().unwrap();
    evidence.observed_at = point + Duration::seconds(300) + Duration::nanoseconds(1);
    let stale = engine.evaluate("store-pressure.v2", &[evidence]).unwrap();
    assert!(
        stale
            .findings
            .iter()
            .all(|finding| finding.outcome == FindingOutcome::Inconclusive)
    );
}

#[test]
fn no_progress_digest_ignores_capture_clock_but_preserves_measured_counters_and_policy() {
    let mut first = snapshot(envelope(vec![observation()]));
    first.partial = true;
    first.coverage = rocketmq_sre_contracts::CoverageStatus::Partial;
    let original = StorePressureV2.semantic_evidence_fingerprint(&first).unwrap();
    let mut later = first.clone();
    later.evidence_id = rocketmq_sre_contracts::EvidenceId::new();
    later.observed_at += Duration::milliseconds(1);
    if let EvidenceContent::Inline(content) = &mut later.content {
        content["collected_at"] = json!(later.observed_at.to_rfc3339());
        content["brokers"][0]["store_pressure"]["observed_at_millis"] = json!(later.observed_at.timestamp_millis());
        content["brokers"][0]["observation_status"] = json!("instantaneous");
        content["brokers"][0]["signals"] = json!([]);
    }
    later.content_hash = later.compute_content_hash().unwrap();
    assert_ne!(first.content_hash, later.content_hash);
    assert_eq!(original, StorePressureV2.semantic_evidence_fingerprint(&later).unwrap());
    let mut new_counter = later.clone();
    if let EvidenceContent::Inline(content) = &mut new_counter.content {
        content["brokers"][0]["store_pressure"]["runtime"]["sync_flush_timeout_total"] = json!(38);
    }
    new_counter.content_hash = new_counter.compute_content_hash().unwrap();
    assert_ne!(
        original,
        StorePressureV2.semantic_evidence_fingerprint(&new_counter).unwrap()
    );
    let mut new_policy = later.clone();
    if let EvidenceContent::Inline(content) = &mut new_policy.content {
        content["brokers"][0]["store_pressure"]["policy"]["sync_flush_backlog_reject_depth"] = json!(64);
    }
    new_policy.content_hash = new_policy.compute_content_hash().unwrap();
    assert_ne!(
        original,
        StorePressureV2.semantic_evidence_fingerprint(&new_policy).unwrap()
    );
    let mut actual_queue_age = later.clone();
    if let EvidenceContent::Inline(content) = &mut actual_queue_age.content {
        content["brokers"][0]["store_pressure"]["runtime"]["sync_flush_oldest_wait_millis"] = json!(1235);
    }
    actual_queue_age.content_hash = actual_queue_age.compute_content_hash().unwrap();
    assert_ne!(
        original,
        StorePressureV2
            .semantic_evidence_fingerprint(&actual_queue_age)
            .unwrap()
    );
}
