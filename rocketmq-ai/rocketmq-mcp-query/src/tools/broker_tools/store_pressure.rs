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

use rocketmq_admin_core::core::broker as admin;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Safe Store observations from the public read-only Admin contract. Absence
/// remains distinct from a measured zero; no filesystem roots are exported.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StorePressureDiagnostics {
    pub schema_version: String,
    pub source: StorePressureObservationSource,
    pub observed_at_millis: Option<u64>,
    pub configuration_generation: Option<u64>,
    pub generation_status: StorePressureGenerationStatus,
    pub runtime: StorePressureRuntimeDiagnostics,
    pub disk: StoreDiskUsageDiagnostics,
    pub policy: StorePressurePolicyDiagnostics,
    pub tiered_configured: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorePressureObservationSource {
    BrokerRuntimeInfo,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorePressureGenerationStatus {
    MatchedRuntimeConfiguration,
    StorePolicyGenerationUnverified,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StoreDiskUsageDiagnostics {
    #[schemars(with = "Option<f64>", range(min = 0.0, max = 1.0))]
    pub commit_log_max_used_ratio: Option<admin::StoreUsageRatio>,
    #[schemars(with = "Option<f64>", range(min = 0.0, max = 1.0))]
    pub commit_log_legacy_min_used_ratio: Option<admin::StoreUsageRatio>,
    pub commit_log_expected_paths: Option<u32>,
    pub commit_log_observed_paths: u32,
    pub commit_log_unknown_paths: u32,
    pub commit_log_samples_complete: bool,
    #[schemars(with = "Option<f64>", range(min = 0.0, max = 1.0))]
    pub consume_queue_used_ratio: Option<admin::StoreUsageRatio>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StorePressurePolicyDiagnostics {
    #[schemars(with = "Option<f64>", range(min = 0.0, max = 1.0))]
    pub disk_warning_used_ratio: Option<admin::StoreUsageRatio>,
    #[schemars(with = "Option<f64>", range(min = 0.0, max = 1.0))]
    pub disk_clean_forcibly_used_ratio: Option<admin::StoreUsageRatio>,
    #[schemars(with = "Option<f64>", range(min = 0.0, max = 1.0))]
    pub disk_max_used_ratio: Option<admin::StoreUsageRatio>,
    pub clean_file_forcibly_enabled: Option<bool>,
    pub sync_flush_backlog_reject_depth: Option<u64>,
    pub sync_flush_backlog_reject_wait_millis: Option<u64>,
    pub dispatch_backlog_reject_bytes: Option<u64>,
    pub page_cache_busy_timeout_millis: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StorePressureRuntimeDiagnostics {
    pub writeable: Option<bool>,
    pub last_flush_error: Option<bool>,
    pub os_page_cache_busy: Option<bool>,
    pub transient_store_pool_deficient: Option<bool>,
    pub dispatch_behind_bytes: Option<u64>,
    pub shutdown: Option<bool>,
    pub sync_flush_queue_depth: Option<u64>,
    pub sync_flush_timeout_total: Option<u64>,
    pub sync_flush_oldest_wait_millis: Option<u64>,
}

impl From<&admin::StorePressureDiagnostics> for StorePressureDiagnostics {
    fn from(value: &admin::StorePressureDiagnostics) -> Self {
        Self {
            schema_version: admin::STORE_PRESSURE_DIAGNOSTICS_SCHEMA_VERSION.to_owned(),
            source: match value.source {
                admin::StorePressureObservationSource::BrokerRuntimeInfo => {
                    StorePressureObservationSource::BrokerRuntimeInfo
                }
            },
            observed_at_millis: value.observed_at_millis,
            configuration_generation: value.configuration_generation,
            generation_status: match value.generation_status {
                admin::StorePressureGenerationStatus::MatchedRuntimeConfiguration => {
                    StorePressureGenerationStatus::MatchedRuntimeConfiguration
                }
                admin::StorePressureGenerationStatus::StorePolicyGenerationUnverified => {
                    StorePressureGenerationStatus::StorePolicyGenerationUnverified
                }
            },
            runtime: StorePressureRuntimeDiagnostics {
                writeable: value.runtime.writeable,
                last_flush_error: value.runtime.last_flush_error,
                os_page_cache_busy: value.runtime.os_page_cache_busy,
                transient_store_pool_deficient: value.runtime.transient_store_pool_deficient,
                dispatch_behind_bytes: value.runtime.dispatch_behind_bytes,
                shutdown: value.runtime.shutdown,
                sync_flush_queue_depth: value.runtime.sync_flush_queue_depth,
                sync_flush_timeout_total: value.runtime.sync_flush_timeout_total,
                sync_flush_oldest_wait_millis: value.runtime.sync_flush_oldest_wait_millis,
            },
            disk: StoreDiskUsageDiagnostics {
                commit_log_max_used_ratio: value.disk.commit_log_max_used_ratio,
                commit_log_legacy_min_used_ratio: value.disk.commit_log_legacy_min_used_ratio,
                commit_log_expected_paths: value.disk.commit_log_expected_paths,
                commit_log_observed_paths: value.disk.commit_log_observed_paths,
                commit_log_unknown_paths: value.disk.commit_log_unknown_paths,
                commit_log_samples_complete: value.disk.commit_log_samples_complete,
                consume_queue_used_ratio: value.disk.consume_queue_used_ratio,
            },
            policy: StorePressurePolicyDiagnostics {
                disk_warning_used_ratio: value.policy.disk_warning_used_ratio,
                disk_clean_forcibly_used_ratio: value.policy.disk_clean_forcibly_used_ratio,
                disk_max_used_ratio: value.policy.disk_max_used_ratio,
                clean_file_forcibly_enabled: value.policy.clean_file_forcibly_enabled,
                sync_flush_backlog_reject_depth: value.policy.sync_flush_backlog_reject_depth,
                sync_flush_backlog_reject_wait_millis: value.policy.sync_flush_backlog_reject_wait_millis,
                dispatch_backlog_reject_bytes: value.policy.dispatch_backlog_reject_bytes,
                page_cache_busy_timeout_millis: value.policy.page_cache_busy_timeout_millis,
            },
            tiered_configured: value.tiered_configured,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture() -> serde_json::Value {
        json!({"schema_version":admin::STORE_PRESSURE_DIAGNOSTICS_SCHEMA_VERSION,
            "source":"broker_runtime_info", "observed_at_millis":42, "configuration_generation":7,
            "generation_status":"matched_runtime_configuration", "runtime":{"sync_flush_queue_depth":3,"sync_flush_timeout_total":37,"sync_flush_oldest_wait_millis":1234,"dispatch_behind_bytes":8192},
            "disk":{"commit_log_max_used_ratio":0.92, "commit_log_legacy_min_used_ratio":0.2,
                "commit_log_expected_paths":2,"commit_log_observed_paths":2,"commit_log_unknown_paths":0,
                "commit_log_samples_complete":true,"consume_queue_used_ratio":0.3},
            "policy":{"disk_warning_used_ratio":0.9,"sync_flush_backlog_reject_depth":0}, "tiered_configured":null})
    }

    #[test]
    fn store_pressure_wire_keeps_safe_numbers_missing_and_explicit_disabled_separate() {
        let mut raw = fixture();
        raw["private_root"] = json!("/private/store/commitlog");
        let admin: admin::StorePressureDiagnostics = serde_json::from_value(raw).unwrap();
        let wire = StorePressureDiagnostics::from(&admin);
        let value = serde_json::to_value(&wire).unwrap();
        assert_eq!(value["disk"]["commit_log_max_used_ratio"], 0.92);
        assert_eq!(value["runtime"]["sync_flush_timeout_total"], 37);
        assert_eq!(value["runtime"]["sync_flush_oldest_wait_millis"], 1234);
        assert_eq!(value["runtime"]["dispatch_behind_bytes"], 8192);
        assert_eq!(value["policy"]["sync_flush_backlog_reject_depth"], 0);
        assert!(value["policy"]["sync_flush_backlog_reject_wait_millis"].is_null());
        assert!(!value.to_string().contains("/private/store"));
        assert_eq!(serde_json::from_value::<StorePressureDiagnostics>(value).unwrap(), wire);
        let schema = serde_json::to_value(schemars::schema_for!(StorePressureDiagnostics)).unwrap();
        assert!(schema.to_string().contains("commit_log_max_used_ratio"));
    }

    #[test]
    fn store_pressure_wire_rejects_out_of_domain_disk_values() {
        for invalid in [json!(-1), json!(1.01), json!("NaN")] {
            let mut raw = fixture();
            raw["disk"]["commit_log_max_used_ratio"] = invalid;
            assert!(serde_json::from_value::<StorePressureDiagnostics>(raw).is_err());
        }
    }
}
