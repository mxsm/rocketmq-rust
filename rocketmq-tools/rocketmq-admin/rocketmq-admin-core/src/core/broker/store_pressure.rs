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

use serde::{Deserialize, Deserializer, Serialize};

use crate::core::{AdminError, AdminResult};

/// Public, bounded Store observation schema. It does not assert a stall or grant
/// permission to modify a Broker.
pub const STORE_PRESSURE_DIAGNOSTICS_SCHEMA_VERSION: &str = "rocketmq.store-pressure-observation.v1";

/// A finite used-capacity fraction in the inclusive interval 0..=1.
///
/// The inner value is private and deserialization uses the same checked
/// constructor. This keeps equality reflexive without accepting NaN.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct StoreUsageRatio(f64);

impl StoreUsageRatio {
    /// Constructs a measured ratio, without converting an absent probe to zero.
    ///
    /// # Errors
    /// Rejects non-finite, negative and greater-than-one values.
    pub fn try_new(value: f64) -> AdminResult<Self> {
        if value.is_finite() && (0.0..=1.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(AdminError::invalid_argument(
                "store_usage_ratio",
                "a finite fraction in 0..=1 is required",
            ))
        }
    }

    #[must_use]
    pub const fn value(self) -> f64 {
        self.0
    }
}

// Every construction path excludes NaN, so equality is reflexive.
impl Eq for StoreUsageRatio {}

impl<'de> Deserialize<'de> for StoreUsageRatio {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = f64::deserialize(deserializer)?;
        Self::try_new(value).map_err(|_| serde::de::Error::custom("invalid Store usage ratio"))
    }
}

/// Safe aggregation of configured CommitLog root probes. Root names and paths
/// are deliberately absent. A legacy minimum is context, not an active-root
/// health decision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoreDiskUsageDiagnostics {
    pub commit_log_max_used_ratio: Option<StoreUsageRatio>,
    pub commit_log_legacy_min_used_ratio: Option<StoreUsageRatio>,
    pub commit_log_expected_paths: Option<u32>,
    pub commit_log_observed_paths: u32,
    pub commit_log_unknown_paths: u32,
    pub commit_log_samples_complete: bool,
    pub consume_queue_used_ratio: Option<StoreUsageRatio>,
}

/// Actual policy values, retaining missing separately from explicit zero
/// (disabled). Ratios use fractions; queue depth uses requests; dispatch uses
/// bytes; wait durations use milliseconds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorePressurePolicyDiagnostics {
    pub disk_warning_used_ratio: Option<StoreUsageRatio>,
    pub disk_clean_forcibly_used_ratio: Option<StoreUsageRatio>,
    pub disk_max_used_ratio: Option<StoreUsageRatio>,
    pub clean_file_forcibly_enabled: Option<bool>,
    pub sync_flush_backlog_reject_depth: Option<u64>,
    pub sync_flush_backlog_reject_wait_millis: Option<u64>,
    pub dispatch_backlog_reject_bytes: Option<u64>,
    pub page_cache_busy_timeout_millis: Option<u64>,
}

/// Independently observed values. A missing legacy member does not discard the
/// other real measurements, unlike the compatibility StoreHealth projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

/// Read-only observation of runtime pressure and its actual policy. This is
/// separate from the old StoreHealth contract, which remains all-required.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorePressureDiagnostics {
    pub schema_version: String,
    pub source: StorePressureObservationSource,
    pub observed_at_millis: Option<u64>,
    pub configuration_generation: Option<u64>,
    pub generation_status: StorePressureGenerationStatus,
    pub runtime: StorePressureRuntimeDiagnostics,
    pub disk: StoreDiskUsageDiagnostics,
    pub policy: StorePressurePolicyDiagnostics,
    /// Explicit field presence; never the old Tiered configured=false fallback.
    pub tiered_configured: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorePressureObservationSource {
    BrokerRuntimeInfo,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorePressureGenerationStatus {
    MatchedRuntimeConfiguration,
    StorePolicyGenerationUnverified,
}

#[cfg(any(feature = "read-client-adapter", test))]
pub(super) fn parse(
    runtime: &rocketmq_protocol::protocol::body::kv_table::KVTable,
) -> Option<StorePressureDiagnostics> {
    use super::{runtime_bool, runtime_u64, runtime_value};

    if runtime_value(runtime, "storePressureSchemaVersion") != Some(STORE_PRESSURE_DIAGNOSTICS_SCHEMA_VERSION) {
        return None;
    }
    let ratio = |key| {
        runtime_value(runtime, key)
            .and_then(|value| value.parse::<f64>().ok())
            .and_then(|value| StoreUsageRatio::try_new(value).ok())
    };
    let mut observed = 0_u32;
    let mut unknown = 0_u32;
    let mut maximum: Option<StoreUsageRatio> = None;
    for (key, value) in &runtime.table {
        if !key.starts_with("commitLogDiskRatio_") {
            continue;
        }
        observed = observed.checked_add(1)?;
        let value = value
            .parse::<f64>()
            .ok()
            .and_then(|value| StoreUsageRatio::try_new(value).ok());
        if let Some(value) = value {
            if maximum.is_none_or(|maximum| value.value() > maximum.value()) {
                maximum = Some(value);
            }
        } else {
            unknown = unknown.checked_add(1)?;
        }
    }
    let expected = runtime_u64(runtime, "storePressureCommitLogPathCount").and_then(|count| u32::try_from(count).ok());
    let generation = runtime_u64(runtime, "storePressurePolicyGeneration").filter(|generation| *generation > 0);
    let verified_generation = runtime_value(runtime, "storePressurePolicyGenerationStatus")
        == Some("matched_runtime_configuration")
        && generation.is_some()
        && generation == runtime_u64(runtime, "brokerConfigGeneration");
    Some(StorePressureDiagnostics {
        schema_version: STORE_PRESSURE_DIAGNOSTICS_SCHEMA_VERSION.to_owned(),
        source: StorePressureObservationSource::BrokerRuntimeInfo,
        observed_at_millis: runtime_u64(runtime, "sreDiagnosticsObservedAtMillis"),
        configuration_generation: if verified_generation { generation } else { None },
        generation_status: if verified_generation {
            StorePressureGenerationStatus::MatchedRuntimeConfiguration
        } else {
            StorePressureGenerationStatus::StorePolicyGenerationUnverified
        },
        runtime: StorePressureRuntimeDiagnostics {
            writeable: runtime_bool(runtime, "storeWriteable"),
            last_flush_error: runtime_bool(runtime, "storeLastFlushError"),
            os_page_cache_busy: runtime_bool(runtime, "storeOsPageCacheBusy"),
            transient_store_pool_deficient: runtime_bool(runtime, "storeTransientPoolDeficient"),
            dispatch_behind_bytes: runtime_u64(runtime, "storeDispatchBehindBytes"),
            shutdown: runtime_bool(runtime, "storeShutdown"),
            sync_flush_queue_depth: runtime_u64(runtime, "storePressureSyncFlushQueueDepth"),
            sync_flush_timeout_total: runtime_u64(runtime, "storePressureSyncFlushTimeoutTotal"),
            sync_flush_oldest_wait_millis: runtime_u64(runtime, "storePressureSyncFlushOldestWaitMillis"),
        },
        disk: StoreDiskUsageDiagnostics {
            commit_log_max_used_ratio: maximum,
            commit_log_legacy_min_used_ratio: ratio("commitLogDiskRatio"),
            commit_log_expected_paths: expected,
            commit_log_observed_paths: observed,
            commit_log_unknown_paths: unknown,
            commit_log_samples_complete: expected
                .is_some_and(|expected| expected > 0 && expected == observed && unknown == 0),
            consume_queue_used_ratio: ratio("consumeQueueDiskRatio"),
        },
        policy: StorePressurePolicyDiagnostics {
            disk_warning_used_ratio: ratio("storePressureDiskWarningRatio"),
            disk_clean_forcibly_used_ratio: ratio("storePressureDiskCleanForciblyRatio"),
            disk_max_used_ratio: ratio("storePressureDiskMaxUsedRatio"),
            clean_file_forcibly_enabled: runtime_bool(runtime, "storePressureCleanFileForciblyEnabled"),
            sync_flush_backlog_reject_depth: runtime_u64(runtime, "storePressureSyncFlushRejectDepth"),
            sync_flush_backlog_reject_wait_millis: runtime_u64(runtime, "storePressureSyncFlushRejectWaitMillis"),
            dispatch_backlog_reject_bytes: runtime_u64(runtime, "storePressureDispatchRejectBytes"),
            page_cache_busy_timeout_millis: runtime_u64(runtime, "storePressurePageCacheBusyTimeoutMillis"),
        },
        tiered_configured: runtime_bool(runtime, "storePressureTieredConfigured"),
    })
}

#[cfg(test)]
mod tests;
