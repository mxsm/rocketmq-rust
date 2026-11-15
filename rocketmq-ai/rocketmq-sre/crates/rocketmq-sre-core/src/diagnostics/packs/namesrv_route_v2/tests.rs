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
use crate::diagnostics::{DiagnosticEngine, DiagnosticStatus, full_registry};
use rocketmq_sre_contracts::*;
use serde_json::json;

// Synthetic protocol fixture, not an independently adjudicated deployment proof.
fn sample() -> EvidenceSnapshot {
    let content: ContractJsonValue = serde_json::from_str(include_str!(
        "../../../../../../tests/fixtures/diagnostics/namesrv-route-observation.v1.json"
    ))
    .unwrap();
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id: CorrelationId::new(),
        tenant_id: content["tenant_id"].as_str().unwrap().parse().unwrap(),
        cluster_id: content["cluster_id"].as_str().unwrap().parse().unwrap(),
        source: "admin-query".to_owned(),
        resource: "namesrv-route/orders".to_owned(),
        time_range: TimeRange::new(
            content["requested_window"]["start"].as_str().unwrap().parse().unwrap(),
            content["requested_window"]["end"].as_str().unwrap().parse().unwrap(),
        )
        .unwrap(),
    };
    let observed = content["collected_at"].as_str().unwrap().parse().unwrap();
    let mut snapshot = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        observed,
        EvidenceContent::Inline(content),
    )
    .unwrap();
    snapshot.exposure = EvidenceExposure::AdminRpc;
    snapshot.partial = true;
    snapshot.coverage = CoverageStatus::Partial;
    snapshot.freshness_seconds = 1;
    snapshot
}
fn change(sample: &mut EvidenceSnapshot, change: impl FnOnce(&mut ContractJsonValue)) {
    let EvidenceContent::Inline(value) = &mut sample.content else {
        panic!("inline fixture")
    };
    change(value);
    if let Ok(derived) = compare_namesrv_route_content(value) {
        value["comparison"] = derived.comparison_json();
    }
    sample.content_hash = sample.compute_content_hash().unwrap();
}
fn assess(sample: &EvidenceSnapshot) -> NamesrvRouteObservation {
    assess_namesrv_route_observation(sample, sample.observed_at.timestamp_millis()).unwrap()
}

#[test]
fn namesrv_route_current_difference_is_cited_context_and_never_a_historical_cause() {
    let mut snapshot = sample();
    snapshot.partial = false;
    snapshot.coverage = CoverageStatus::Available;
    assert_eq!(assess(&snapshot).state, NamesrvComparisonState::Difference);
    let report = DiagnosticEngine::new(full_registry().unwrap())
        .evaluate("namesrv-route.v2", std::slice::from_ref(&snapshot))
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert_eq!(
        report.findings[0].reason_code,
        "NAMESRV_CURRENT_ROUTE_DIFFERENCE_OBSERVED"
    );
    assert_eq!(report.findings[0].outcome, FindingOutcome::Inconclusive);
    assert_eq!(
        report.findings[0].supporting_evidence[0].evidence_id,
        snapshot.evidence_id
    );
    for fact in NAMESRV_MISSING_FACTS {
        assert!(report.missing_required_evidence.contains(&fact.to_owned()));
    }
}

#[test]
fn namesrv_route_unavailable_is_not_absence_or_configuration_change() {
    for status in ["unavailable", "permission_denied", "unsupported"] {
        let mut snapshot = sample();
        change(&mut snapshot, |content| {
            for round in content["rounds"].as_array_mut().unwrap() {
                round["peers"][1]["route"]["status"] = json!(status);
            }
        });
        let result = assess(&snapshot);
        assert_eq!(result.state, NamesrvComparisonState::Incomparable);
        assert!(!result.change_observed);
        assert!(!result.differing_dimensions.contains(&"topic_presence"));
    }
    let mut snapshot = sample();
    change(&mut snapshot, |content| {
        let row = &mut content["rounds"][0]["peers"][1];
        row["configuration"]["status"] = json!("unavailable");
        row["configuration"]["flags"] =
            json!({"cluster_test":null,"order_message_enable":null,"support_acting_master":null});
        row["route"]["status"] = json!("not_attempted");
        row["route"]["started_at"] = json!(null);
        row["route"]["finished_at"] = json!(null);
        row["route"]["received_body_bytes"] = json!(null);
        content["read_statistics"]["started_route_rpcs"] = json!(3);
    });
    assert_eq!(assess(&snapshot).state, NamesrvComparisonState::Incomparable);
    assert!(!assess(&snapshot).change_observed);
}

#[test]
fn namesrv_route_two_round_actual_change_is_not_reduced_to_stable_divergence() {
    let mut snapshot = sample();
    change(&mut snapshot, |content| {
        let route = content["rounds"][1]["peers"][0]["route"]["normalized"].clone();
        content["rounds"][1]["peers"][1]["route"]["status"] = json!("found");
        content["rounds"][1]["peers"][1]["route"]["normalized"] = route;
    });
    assert_eq!(assess(&snapshot).state, NamesrvComparisonState::Transient);
}

#[test]
fn namesrv_route_null_native_fields_cannot_be_laundered_by_clearing_missing_dimensions() {
    let mut snapshot = sample();
    change(&mut snapshot, |content| {
        for round in content["rounds"].as_array_mut().unwrap() {
            round["peers"][0]["route"]["normalized"]["brokers"]["broker-a"]["acting_master"] = json!(null);
            round["peers"][0]["route"]["normalized"]["unknown_dimensions"] = json!([]);
        }
    });
    let result = assess(&snapshot);
    assert_eq!(result.state, NamesrvComparisonState::Incomparable);
    assert!(result.unknown_dimensions.contains(&"acting_master"));
}

#[test]
fn namesrv_route_snapshot_scope_hash_precision_and_actual_freshness_are_checked() {
    let original = sample();
    let now = original.observed_at.timestamp_millis();
    assert!(assess_namesrv_route_observation(&original, now + 298_000).is_ok());
    assert_eq!(
        assess_namesrv_route_observation(&original, now + 300_000).unwrap_err(),
        NamesrvObservationIssue::Stale
    );
    assert_eq!(
        assess_namesrv_route_observation(&original, now - 1).unwrap_err(),
        NamesrvObservationIssue::Future
    );
    let mut persisted = original.clone();
    persisted.time_range.start =
        rocketmq_sre_contracts::SreTimestamp::from_timestamp_micros(persisted.time_range.start.timestamp_micros())
            .unwrap();
    persisted.time_range.end =
        SreTimestamp::from_timestamp_micros(persisted.time_range.end.timestamp_micros()).unwrap();
    persisted.content_hash = persisted.compute_content_hash().unwrap();
    assert!(assess_namesrv_route_observation(&persisted, now).is_ok());
    persisted.time_range.start += chrono::Duration::microseconds(1);
    persisted.content_hash = persisted.compute_content_hash().unwrap();
    assert!(assess_namesrv_route_observation(&persisted, now).is_err());
    let mut foreign = original.clone();
    foreign.resource = "namesrv-route/foreign".to_owned();
    foreign.content_hash = foreign.compute_content_hash().unwrap();
    assert!(assess_namesrv_route_observation(&foreign, now).is_err());
    let mut bad = original;
    bad.content_hash = "sha256:invalid".to_owned();
    assert!(assess_namesrv_route_observation(&bad, now).is_err());
}

#[test]
fn namesrv_route_semantics_ignore_capture_and_declaration_renaming_but_keep_real_route_changes() {
    let snapshot = sample();
    let first = NamesrvRouteV2.semantic_evidence_fingerprint(&snapshot).unwrap();
    let mut renamed = snapshot.clone();
    change(&mut renamed, |content| {
        content["configuration"]["revision"] = json!("revision-renamed");
        content["configuration"]["deployment_reference"] = json!("deployment-renamed");
        for index in 0..2 {
            let name = if index == 0 { "renamed-a" } else { "renamed-b" };
            content["configuration"]["peers"][index]["peer_id"] = json!(name);
            content["configuration"]["peers"][index]["instance_reference"] = json!(name);
            for round in content["rounds"].as_array_mut().unwrap() {
                round["peers"][index]["peer_id"] = json!(name);
            }
        }
    });
    assert_eq!(first, NamesrvRouteV2.semantic_evidence_fingerprint(&renamed).unwrap());
    change(&mut renamed, |content| {
        content["rounds"][0]["peers"][0]["route"]["normalized"]["queues"]["broker-a"]["perm"] = json!(4)
    });
    assert_ne!(first, NamesrvRouteV2.semantic_evidence_fingerprint(&renamed).unwrap());
    assert!(namesrv_route_domain(&snapshot).is_ok());
    assert_eq!(
        assess_namesrv_route_observation(&snapshot, snapshot.observed_at.timestamp_millis() + 500_000).unwrap_err(),
        NamesrvObservationIssue::Stale
    );
    assert_eq!(
        namesrv_route_domain(&snapshot).unwrap(),
        namesrv_route_domain(&sample()).unwrap()
    );
}

#[test]
fn namesrv_route_age_uses_actual_read_span_and_derived_comparison_cannot_be_forged() {
    let mut snapshot = sample();
    change(&mut snapshot, |content| {
        content["collected_at"] = json!("2026-11-14T12:00:01.2Z");
        for round in content["rounds"].as_array_mut().unwrap() {
            round["started_at"] = json!("2026-11-14T12:00:01.1Z");
            round["finished_at"] = json!("2026-11-14T12:00:01.1Z");
            for peer in round["peers"].as_array_mut().unwrap() {
                for read in ["configuration", "route"] {
                    peer[read]["started_at"] = json!("2026-11-14T12:00:01.1Z");
                    peer[read]["finished_at"] = json!("2026-11-14T12:00:01.1Z");
                }
            }
        }
    });
    snapshot.observed_at = "2026-11-14T12:00:01.2Z".parse().unwrap();
    snapshot.content_hash = snapshot.compute_content_hash().unwrap();
    assert_eq!(assess(&snapshot).source_age_seconds, 1);
    assert_eq!(assess(&snapshot).acquisition_span_millis, 100);
    let EvidenceContent::Inline(content) = &mut snapshot.content else {
        panic!()
    };
    content["comparison"]["state"] = json!("compared_equal");
    snapshot.content_hash = snapshot.compute_content_hash().unwrap();
    assert_eq!(
        assess_namesrv_route_observation(&snapshot, snapshot.observed_at.timestamp_millis()).unwrap_err(),
        NamesrvObservationIssue::Schema
    );
}

#[test]
fn namesrv_route_unknown_native_fields_becoming_known_are_not_observed_route_changes() {
    for same_peer in [false, true] {
        let mut snapshot = sample();
        change(&mut snapshot, |content| {
            for round in content["rounds"].as_array_mut().unwrap() {
                round["peers"][1]["route"]["status"] = json!("found");
                round["peers"][1]["route"]["normalized"] = round["peers"][0]["route"]["normalized"].clone();
            }
            content["rounds"][0]["peers"][0]["route"]["normalized"]["brokers"]["broker-a"]["acting_master"] =
                json!(null);
            if !same_peer {
                content["rounds"][1]["peers"][0]["route"]["normalized"]["brokers"]["broker-a"]["acting_master"] =
                    json!(null);
            }
        });
        let result = assess(&snapshot);
        assert_eq!(result.state, NamesrvComparisonState::Incomparable);
        assert!(!result.change_observed);
        assert!(!result.differing_dimensions.contains(&"acting_master"));
    }
}
