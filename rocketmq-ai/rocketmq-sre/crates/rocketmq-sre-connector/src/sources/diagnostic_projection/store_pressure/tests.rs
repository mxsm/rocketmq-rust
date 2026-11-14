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
use chrono::{DateTime, Utc};

fn observation() -> Value {
    serde_json::from_str(include_str!(
        "../../../../../../tests/fixtures/diagnostics/store-pressure-observation.v1.json"
    ))
    .unwrap()
}

fn source(content: Value) -> SourceOutput {
    let mut source = SourceOutput::available(content, DateTime::<Utc>::from_timestamp_millis(1794657600000).unwrap());
    // The deterministic fixture's source clock is the supplied evaluation
    // clock, rather than the wall clock on the machine running this test.
    source.freshness_seconds = 0;
    source
}

#[test]
fn exact_nonzero_public_observations_keep_units_and_each_replica_missing_state() {
    let mut obs = observation();
    obs["private_path"] = json!("/private/root/commitlog");
    let mut output = source(json!({"summary":{"unavailable_brokers":1}, "brokers":[
        {"broker_name":"broker-a", "broker_id":0, "store_pressure":obs},
        {"broker_name":"broker-a", "broker_id":1, "store_pressure":null}
    ]}));
    output.partial = true;
    output.warnings.push("upstream_unavailable".to_owned());
    let projected = project_at(output, DateTime::<Utc>::from_timestamp_millis(1794657600000).unwrap()).unwrap();
    let first = &projected.content["brokers"][0];
    assert_eq!(first["store_pressure"]["runtime"]["sync_flush_timeout_total"], 37);
    assert_eq!(first["store_pressure"]["runtime"]["dispatch_behind_bytes"], 8192);
    assert_eq!(
        first["store_pressure"]["runtime"]["sync_flush_oldest_wait_millis"],
        1234
    );
    assert_eq!(first["store_pressure"]["disk"]["commit_log_max_used_ratio"], 0.92);
    assert_eq!(
        projected.content["brokers"][1]["observation_status"],
        "store_pressure_observation_missing"
    );
    assert_eq!(projected.content["unavailable_brokers"], 1);
    assert_eq!(projected.content["pressure_observed_brokers"], 1);
    assert!(projected.partial);
    assert_eq!(projected.coverage, CoverageStatus::Partial);
    assert!(projected.warnings.contains(&"upstream_unavailable".to_owned()));
    let text = projected.content.to_string();
    assert!(!text.contains("/private/root"));
    assert!(!text.contains("flush_or_dispatch_stalled"));
    assert!(text.contains("monotonic_progress_and_stall_policy_unavailable"));
}

#[test]
fn malformed_ratios_fail_closed_without_dropping_the_other_instance_or_becoming_zero() {
    let mut invalid = observation();
    invalid["disk"]["commit_log_max_used_ratio"] = json!(-1);
    let projected = project(source(json!({"brokers":[
        {"broker_name":"broker-a", "broker_id":0, "store_pressure":invalid},
        {"broker_name":"broker-a", "broker_id":1, "store_pressure":observation()}
    ]})))
    .unwrap();
    assert_eq!(
        projected.content["brokers"][0]["observation_status"],
        "store_pressure_observation_invalid"
    );
    assert!(projected.content["brokers"][0].get("store_pressure").is_none());
    assert_eq!(
        projected.content["brokers"][1]["store_pressure"]["runtime"]["sync_flush_queue_depth"],
        3
    );
    assert!(projected.content["unavailable_brokers"].is_null());
    let mut unsupported = source(json!({"brokers":[]}));
    unsupported.coverage = CoverageStatus::NotProductionVerified;
    assert_eq!(
        project(unsupported).unwrap().coverage,
        CoverageStatus::NotProductionVerified
    );
}

#[test]
fn exact_target_and_unique_instance_ids_are_checked_before_canonical_capture() {
    let valid = json!({"brokers":[{"broker_name":"broker-a", "broker_id":0}]});
    assert!(validate_target(&valid, "store-pressure/broker-a").is_ok());
    assert!(validate_target(&valid, "store-pressure/other-broker").is_err());
    let duplicate =
        json!({"brokers":[{"broker_name":"broker-a", "broker_id":0}, {"broker_name":"broker-a", "broker_id":0}]});
    assert!(validate_target(&duplicate, "store-pressure/broker-a").is_err());
}

#[test]
fn source_age_and_future_points_cannot_be_renewed_by_a_new_envelope() {
    let point = DateTime::<Utc>::from_timestamp_millis(1794657600000).unwrap();
    let raw = json!({"brokers":[{"broker_name":"broker-a", "broker_id":0, "store_pressure":observation()}]});
    let boundary = project_at(source(raw.clone()), point + chrono::Duration::seconds(300)).unwrap();
    assert_eq!(boundary.freshness_seconds, 300);
    assert_eq!(
        boundary.content["brokers"][0]["observation_status"],
        "instantaneous_point_not_range"
    );
    let over_boundary = project_at(
        source(raw.clone()),
        point + chrono::Duration::seconds(300) + chrono::Duration::nanoseconds(1),
    )
    .unwrap();
    assert_eq!(over_boundary.freshness_seconds, 301);
    assert_eq!(
        over_boundary.content["brokers"][0]["observation_status"],
        "store_pressure_source_observation_stale"
    );
    let projected = project_at(source(raw.clone()), point + chrono::Duration::seconds(301)).unwrap();
    assert_eq!(projected.freshness_seconds, 301);
    assert_eq!(
        projected.content["brokers"][0]["observation_status"],
        "store_pressure_source_observation_stale"
    );
    assert!(
        projected.content["brokers"][0]["signals"]
            .as_array()
            .unwrap()
            .iter()
            .all(|signal| signal["state"] == "unknown")
    );
    let future = project_at(source(raw), point - chrono::Duration::milliseconds(1)).unwrap();
    assert_eq!(
        future.content["brokers"][0]["observation_status"],
        "store_pressure_source_time_missing_or_future"
    );
}

#[test]
fn a_contradictory_response_generation_loses_policy_qualification_without_losing_measurements() {
    let point = DateTime::<Utc>::from_timestamp_millis(1794657600000).unwrap();
    let value = json!({"brokers":[{"broker_name":"broker-a","broker_id":0,"config":{"generation":9},"store_pressure":observation()}]});
    let projected = project_at(source(value), point).unwrap();
    let row = &projected.content["brokers"][0];
    assert!(row["store_pressure"]["configuration_generation"].is_null());
    assert_eq!(
        row["store_pressure"]["generation_status"],
        "store_policy_generation_unverified"
    );
    assert_eq!(row["store_pressure"]["runtime"]["sync_flush_timeout_total"], 37);
    assert!(
        row["signals"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|signal| signal["key"].as_str().unwrap().starts_with("disk_"))
            .all(|signal| signal["state"] == "unknown")
    );
}
