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

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::body::kv_table::KVTable;

use super::*;

fn fixture() -> KVTable {
    KVTable {
        table: [
            ("storePressureSchemaVersion", STORE_PRESSURE_DIAGNOSTICS_SCHEMA_VERSION),
            ("sreDiagnosticsObservedAtMillis", "1700000000000"),
            ("storePressurePolicyGeneration", "7"),
            ("brokerConfigGeneration", "7"),
            ("storePressurePolicyGenerationStatus", "matched_runtime_configuration"),
            ("storePressureCommitLogPathCount", "2"),
            ("commitLogDiskRatio_/private/store-a", "0.2"),
            ("commitLogDiskRatio_/private/store-b", "0.92"),
            ("commitLogDiskRatio", "0.2"),
            ("consumeQueueDiskRatio", "0.3"),
            ("storePressureDiskWarningRatio", "0.9"),
            ("storePressureDiskCleanForciblyRatio", "0.85"),
            ("storePressureDiskMaxUsedRatio", "0.75"),
            ("storePressureCleanFileForciblyEnabled", "true"),
            ("storePressureSyncFlushRejectDepth", "8"),
            ("storePressureSyncFlushRejectWaitMillis", "250"),
            ("storePressureDispatchRejectBytes", "4096"),
            ("storePressurePageCacheBusyTimeoutMillis", "1000"),
            ("storePressureTieredConfigured", "false"),
            ("storeWriteable", "true"),
            ("storeLastFlushError", "false"),
            ("storeOsPageCacheBusy", "false"),
            ("storeTransientPoolDeficient", "false"),
            ("storeDispatchBehindBytes", "8192"),
            ("storeShutdown", "false"),
            ("storeSyncFlushQueueDepth", "3"),
            ("storeSyncFlushTimeoutTotal", "37"),
            ("storeSyncFlushOldestWaitMillis", "1234"),
            ("storePressureSyncFlushQueueDepth", "3"),
            ("storePressureSyncFlushTimeoutTotal", "37"),
            ("storePressureSyncFlushOldestWaitMillis", "1234"),
            ("storeHaPendingRequestCount", "0"),
            ("storeHaPendingOldestWaitMillis", "0"),
        ]
        .into_iter()
        .map(|(key, value)| (CheetahString::from(key), CheetahString::from(value)))
        .collect(),
    }
}

fn put(runtime: &mut KVTable, key: &str, value: &str) {
    runtime
        .table
        .insert(CheetahString::from(key), CheetahString::from(value));
}

#[test]
fn real_numbers_keep_their_units_and_a_healthy_spare_does_not_hide_the_higher_root() {
    let observed = parse(&fixture()).unwrap();
    assert_eq!(observed.configuration_generation, Some(7));
    assert_eq!(
        observed.generation_status,
        StorePressureGenerationStatus::MatchedRuntimeConfiguration
    );
    assert_eq!(observed.runtime.sync_flush_queue_depth, Some(3));
    assert_eq!(observed.runtime.sync_flush_timeout_total, Some(37));
    assert_eq!(observed.runtime.sync_flush_oldest_wait_millis, Some(1234));
    assert_eq!(observed.runtime.dispatch_behind_bytes, Some(8192));
    assert_eq!(observed.disk.commit_log_legacy_min_used_ratio.unwrap().value(), 0.2);
    assert_eq!(observed.disk.commit_log_max_used_ratio.unwrap().value(), 0.92);
    assert_eq!(observed.disk.consume_queue_used_ratio.unwrap().value(), 0.3);
    assert!(observed.disk.commit_log_samples_complete);
    assert_eq!(observed.policy.sync_flush_backlog_reject_wait_millis, Some(250));
    assert_eq!(observed.policy.dispatch_backlog_reject_bytes, Some(4096));
    let json = serde_json::to_string(&observed).unwrap();
    assert!(!json.contains("/private/"));
    assert!(!json.contains("disk_pressure"));
    assert!(!json.contains("stalled"));
}

#[test]
fn one_missing_counter_does_not_discard_other_observations_or_default_it_to_zero() {
    let mut runtime = fixture();
    assert!(super::super::parse_store_health(&runtime).is_some());
    runtime.table.remove(&CheetahString::from("storeSyncFlushTimeoutTotal"));
    runtime
        .table
        .remove(&CheetahString::from("storePressureSyncFlushTimeoutTotal"));
    let observed = parse(&runtime).unwrap();
    assert!(
        super::super::parse_store_health(&runtime).is_none(),
        "legacy all-required semantics remain intact"
    );
    assert_eq!(observed.runtime.sync_flush_timeout_total, None);
    assert_eq!(observed.runtime.sync_flush_queue_depth, Some(3));
    assert_eq!(observed.runtime.dispatch_behind_bytes, Some(8192));
    runtime
        .table
        .remove(&CheetahString::from("storePressureTieredConfigured"));
    assert_eq!(parse(&runtime).unwrap().tiered_configured, None);
    put(&mut runtime, "storePressureSyncFlushRejectDepth", "0");
    assert_eq!(parse(&runtime).unwrap().policy.sync_flush_backlog_reject_depth, Some(0));
    runtime
        .table
        .remove(&CheetahString::from("storePressureSyncFlushRejectDepth"));
    assert_eq!(parse(&runtime).unwrap().policy.sync_flush_backlog_reject_depth, None);
}

#[test]
fn unknown_disk_probes_and_an_unverified_generation_cannot_become_complete_measurements() {
    let mut runtime = fixture();
    put(&mut runtime, "commitLogDiskRatio_/private/store-a", "-1");
    put(&mut runtime, "commitLogDiskRatio_/private/store-b", "NaN");
    put(&mut runtime, "commitLogDiskRatio", "-1");
    put(&mut runtime, "consumeQueueDiskRatio", "1.1");
    put(
        &mut runtime,
        "storePressurePolicyGenerationStatus",
        "store_policy_generation_unverified",
    );
    let observed = parse(&runtime).unwrap();
    assert_eq!(observed.disk.commit_log_max_used_ratio, None);
    assert_eq!(observed.disk.commit_log_legacy_min_used_ratio, None);
    assert_eq!(observed.disk.consume_queue_used_ratio, None);
    assert_eq!(observed.disk.commit_log_unknown_paths, 2);
    assert!(!observed.disk.commit_log_samples_complete);
    assert_eq!(observed.configuration_generation, None);
    assert_eq!(
        observed.generation_status,
        StorePressureGenerationStatus::StorePolicyGenerationUnverified
    );
    assert_eq!(observed.runtime.sync_flush_queue_depth, Some(3));
    runtime.table.remove(&CheetahString::from("storePressureSchemaVersion"));
    assert!(parse(&runtime).is_none());
}

#[test]
fn finite_ratio_construction_and_deserialization_both_fail_closed() {
    for value in [-1.0, 1.01, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(StoreUsageRatio::try_new(value).is_err());
    }
    for encoded in ["-1", "1.01", "null", "\"0.5\""] {
        assert!(serde_json::from_str::<StoreUsageRatio>(encoded).is_err());
    }
    for value in [0.0, 0.5, 1.0] {
        let ratio = StoreUsageRatio::try_new(value).unwrap();
        let encoded = serde_json::to_value(ratio).unwrap();
        assert!(encoded.is_number());
        assert_eq!(serde_json::from_value::<StoreUsageRatio>(encoded).unwrap(), ratio);
    }
}

#[test]
fn pressure_policy_must_match_the_same_response_broker_generation() {
    for broker_generation in [None, Some("9"), Some("0")] {
        let mut runtime = fixture();
        runtime.table.remove("brokerConfigGeneration");
        if let Some(generation) = broker_generation {
            put(&mut runtime, "brokerConfigGeneration", generation);
        }
        let observed = parse(&runtime).unwrap();
        assert_eq!(observed.configuration_generation, None);
        assert_eq!(
            observed.generation_status,
            StorePressureGenerationStatus::StorePolicyGenerationUnverified
        );
        assert_eq!(observed.runtime.sync_flush_timeout_total, Some(37));
    }
}
