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
use crate::diagnostics::{DiagnosticEngine, DiagnosticStatus, FindingOutcome, full_registry};
use chrono::Duration;
use rocketmq_sre_contracts::*;
use serde_json::json;

fn sample() -> EvidenceSnapshot {
    let content: Value = serde_json::from_str(include_str!(
        "../../../../../../tests/fixtures/diagnostics/send-latency-window.v1.json"
    ))
    .unwrap();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: content["scope"]["tenant_id"].as_str().unwrap().parse().unwrap(),
        cluster_id: content["scope"]["cluster_id"].as_str().unwrap().parse().unwrap(),
        source: "prometheus".to_owned(),
        resource: "send-latency/orders".to_owned(),
        time_range: TimeRange::new(
            content["window"]["requested_start"].as_str().unwrap().parse().unwrap(),
            content["window"]["requested_end"].as_str().unwrap().parse().unwrap(),
        )
        .unwrap(),
    };
    let observed = content["collected_at"].as_str().unwrap().parse().unwrap();
    let mut sample = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        observed,
        EvidenceContent::Inline(content),
    )
    .unwrap();
    sample.exposure = EvidenceExposure::Synthetic;
    sample.freshness_seconds = 1;
    sample.partial = true;
    sample.coverage = CoverageStatus::Partial;
    sample
}
fn change(sample: &mut EvidenceSnapshot, change: impl FnOnce(&mut Value)) {
    let EvidenceContent::Inline(value) = &mut sample.content else {
        panic!("inline fixture");
    };
    change(value);
    sample.content_hash = sample.compute_content_hash().unwrap();
}
fn engine() -> DiagnosticEngine {
    let registry = full_registry().unwrap();
    DiagnosticEngine::new(registry)
}

#[test]
fn send_latency_valid_local_predicate_retains_its_real_evidence_but_whole_path_is_inconclusive() {
    let mut sample = sample();
    sample.partial = false;
    sample.coverage = CoverageStatus::Available;
    let assessed = assess_send_latency_observation(&sample, sample.observed_at.timestamp_millis()).unwrap();
    assert_eq!(assessed.policy, LatencyPolicyState::Exceeded);
    assert_eq!(assessed.window.quantile.unwrap().estimate_millis, 248.5);
    let report = engine()
        .evaluate("send-latency.v2", std::slice::from_ref(&sample))
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(report.findings.iter().any(|finding| {
        finding.reason_code == CODES[0]
            && finding.outcome == FindingOutcome::Fault
            && finding
                .supporting_evidence
                .iter()
                .any(|reference| reference.evidence_id == sample.evidence_id)
    }));
    assert!(
        report
            .missing_required_evidence
            .iter()
            .any(|fact| fact.contains("client_exact_topic"))
    );
}

#[test]
fn send_latency_legacy_boolean_rules_and_old_references_are_not_reinterpreted() {
    let mut sample = sample();
    change(
        &mut sample,
        |value| *value = json!({"client_or_proxy_slow":false,"remoting_or_broker_slow":false,"store_slow":false}),
    );
    sample.partial = false;
    sample.coverage = CoverageStatus::Available;
    assert_eq!(
        engine()
            .evaluate("send-latency.v1", std::slice::from_ref(&sample))
            .unwrap()
            .status,
        DiagnosticStatus::Healthy
    );
    assert_eq!(
        engine().evaluate("send-latency.v2", &[sample]).unwrap().status,
        DiagnosticStatus::Inconclusive
    );
}

#[test]
fn send_latency_scope_window_and_future_interval_cannot_be_replaced_by_echoed_metadata() {
    let sample = sample();
    assert!(assess_send_latency_observation(&sample, sample.observed_at.timestamp_millis()).is_ok());
    for mutate in [
        ("scope", "topic", json!("foreign")),
        ("scope", "cluster_id", json!(ClusterId::new())),
        ("window", "data_first_at", json!("2026-11-14T18:00:00Z")),
        ("window", "query_start", json!("2026-11-14T18:00:00.001Z")),
    ] {
        let mut invalid = sample.clone();
        change(&mut invalid, |value| value[mutate.0][mutate.1] = mutate.2);
        assert!(assess_send_latency_observation(&invalid, invalid.observed_at.timestamp_millis()).is_err());
    }
    let mut future = sample.clone();
    future.time_range.end = "2026-11-14T18:00:00.200000800Z".parse().unwrap();
    change(&mut future, |value| {
        value["window"]["requested_end"] = json!("2026-11-14T18:00:00.200000800Z");
        value["window"]["query_end"] = json!("2026-11-14T18:00:00.200Z");
    });
    assert_eq!(
        assess_send_latency_observation(&future, future.observed_at.timestamp_millis()),
        Err(LatencyObservationIssue::Future)
    );
    let mut stored = sample.clone();
    stored.time_range.start =
        chrono::DateTime::from_timestamp_micros(stored.time_range.start.timestamp_micros()).unwrap();
    stored.time_range.end = chrono::DateTime::from_timestamp_micros(stored.time_range.end.timestamp_micros()).unwrap();
    stored.content_hash = stored.compute_content_hash().unwrap();
    assert!(assess_send_latency_observation(&stored, stored.observed_at.timestamp_millis()).is_ok());
}

#[test]
fn send_latency_old_data_cannot_borrow_a_new_collection_time_or_renew_on_requery() {
    let original = sample();
    let mut renewed = original.clone();
    renewed.observed_at += Duration::minutes(10);
    let now = renewed.observed_at;
    change(&mut renewed, |value| {
        value["collected_at"] = json!(now.to_rfc3339());
        value["source_age_seconds"] = json!(0);
    });
    renewed.freshness_seconds = 0;
    assert_eq!(
        assess_send_latency_observation(&renewed, now.timestamp_millis()),
        Err(LatencyObservationIssue::Window)
    );
    change(&mut renewed, |value| value["source_age_seconds"] = json!(601));
    renewed.freshness_seconds = 601;
    assert_eq!(
        assess_send_latency_observation(&renewed, now.timestamp_millis()),
        Err(LatencyObservationIssue::Stale)
    );
    assert!(assess_send_latency_observation(&original, original.observed_at.timestamp_millis() + 299_000).is_ok());
    assert_eq!(
        assess_send_latency_observation(&original, original.observed_at.timestamp_millis() + 301_000),
        Err(LatencyObservationIssue::Stale)
    );
}

#[test]
fn send_latency_operator_policy_is_optional_exact_and_not_authorized_by_serialized_status() {
    let mut sample = sample();
    change(&mut sample, |value| {
        value["policy"]["source"] = Value::Null;
        value["policy"]["p99_millis"] = Value::Null;
    });
    assert_eq!(
        assess_send_latency_observation(&sample, sample.observed_at.timestamp_millis())
            .unwrap()
            .policy,
        LatencyPolicyState::NotConfigured
    );
    assert!(
        !engine()
            .evaluate("send-latency.v2", &[sample])
            .unwrap()
            .findings
            .iter()
            .any(|finding| finding.outcome == FindingOutcome::Fault)
    );
    let mut foreign = super::tests::sample();
    change(&mut foreign, |value| {
        value["policy"]["scope"]["topic"] = json!("foreign")
    });
    assert_eq!(
        assess_send_latency_observation(&foreign, foreign.observed_at.timestamp_millis()),
        Err(LatencyObservationIssue::Policy)
    );
}

#[test]
fn send_latency_declared_complete_or_slow_flags_do_not_replace_actual_histogram_proof() {
    let mut sample = sample();
    sample.partial = false;
    sample.coverage = CoverageStatus::Available;
    change(&mut sample, |value| {
        value["raw_histograms"] = json!([]);
        value["complete"] = json!(true);
        value["broker_slow"] = json!(true);
    });
    let report = engine().evaluate("send-latency.v2", &[sample]).unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(
        report
            .findings
            .iter()
            .all(|finding| finding.outcome == FindingOutcome::Inconclusive)
    );
}

#[test]
fn send_latency_fact_fingerprint_excludes_collection_age_but_keeps_actual_scrape_times() {
    let original = sample();
    let first = SendLatencyV2.semantic_evidence_fingerprint(&original).unwrap();
    let mut recaptured = original.clone();
    recaptured.evidence_id = EvidenceId::new();
    recaptured.observed_at += Duration::milliseconds(1);
    let now = recaptured.observed_at;
    change(&mut recaptured, |value| {
        value["collected_at"] = json!(now.to_rfc3339());
        value["source_age_seconds"] = json!(1);
        value["segments"][3]["availability"] = json!("stale");
    });
    assert_eq!(first, SendLatencyV2.semantic_evidence_fingerprint(&recaptured).unwrap());
    change(&mut recaptured, |value| {
        value["raw_histograms"][0]["buckets"][7]["samples"][1][0] = json!(1_794_679_200_026_i64)
    });
    assert_ne!(first, SendLatencyV2.semantic_evidence_fingerprint(&recaptured).unwrap());
}
