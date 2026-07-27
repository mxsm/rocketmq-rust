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

//! Broker capability contracts.

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

/// Stable schema version emitted by the read-only broker diagnostics query.
pub const BROKER_DIAGNOSTICS_SCHEMA_VERSION: &str = "rocketmq.admin-broker-diagnostics.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListBrokersRequest {
    pub cluster: String,
}

impl ListBrokersRequest {
    pub fn try_new(cluster: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerSummary {
    pub cluster: String,
    pub broker_name: String,
    pub broker_id: u64,
    pub broker_addr: String,
    pub version: String,
    pub in_tps: String,
    pub out_tps: String,
    pub timer_progress: String,
    pub page_cache_lock_time_millis: String,
    pub hour: String,
    pub space: String,
    pub broker_active: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ListBrokersResult {
    pub brokers: Vec<BrokerSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeBrokerRuntimeRequest {
    pub cluster: String,
}

impl ProbeBrokerRuntimeRequest {
    pub fn try_new(cluster: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeBrokerRuntimeResult {
    pub attempted: usize,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBrokerDiagnosticsRequest {
    pub cluster: String,
}

impl QueryBrokerDiagnosticsRequest {
    pub fn try_new(cluster: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerDiagnosticsCoverage {
    Available,
    Partial,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerReadinessDiagnostics {
    pub ready: bool,
    pub active: bool,
    pub shutdown: bool,
    pub registration_accepting: bool,
    pub registration_configured: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigSummary {
    pub generation: u64,
    pub broker_role: String,
    pub store_type: String,
    pub timer_wheel_enabled: bool,
    pub transient_store_pool_enabled: bool,
    pub tiered_store_configured: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncFlushDiagnostics {
    pub queue_depth: u64,
    pub timeout_total: u64,
    pub oldest_wait_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreHealthDiagnostics {
    pub writeable: bool,
    pub last_flush_error: bool,
    pub os_page_cache_busy: bool,
    pub transient_store_pool_deficient: bool,
    pub dispatch_behind_bytes: i64,
    pub shutdown: bool,
    pub ha_pending_request_count: u64,
    pub ha_pending_oldest_wait_millis: u64,
    pub sync_flush: SyncFlushDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryDiagnostics {
    pub available: bool,
    pub total_duration_millis: Option<u64>,
    pub phase_count: Option<u64>,
    pub failed_phase_count: Option<u64>,
    pub fallback_phase_count: Option<u64>,
    pub fallback_reason_present: Option<bool>,
    pub scanned_bytes: Option<u64>,
    pub recovered_messages: Option<u64>,
    pub invalid_messages: Option<u64>,
    pub truncated_files: Option<u64>,
    pub index_files_removed: Option<u64>,
    pub index_files_rebuilt: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackgroundIndexRebuildDiagnostics {
    pub state: String,
    pub effective_enabled: bool,
    pub gray_mode: bool,
    pub current_safe_offset: i64,
    pub target_offset: i64,
    pub backlog_bytes: i64,
    pub rebuilt_bytes: u64,
    pub rebuilt_messages: u64,
    pub failure_count: u64,
    pub bytes_per_second: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RocksDbMaintenanceDiagnostics {
    pub supported: bool,
    pub maintenance_running: Option<bool>,
    pub message_maintenance_running: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TieredDispatchDiagnostics {
    pub configured: bool,
    pub dispatch_ready: Option<bool>,
    pub minimum_pinned_wal_segment: Option<u64>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthSecurityDiagnostics {
    pub supported: bool,
    pub authentication_enabled: Option<bool>,
    pub authorization_enabled: Option<bool>,
    pub acl_file_watch_enabled: Option<bool>,
    pub acl_generation: Option<u64>,
    pub acl_reload_attempts: Option<u64>,
    pub acl_reload_successes: Option<u64>,
    pub acl_reload_failures: Option<u64>,
    pub acl_reload_skipped: Option<u64>,
    pub credential_rotation_supported: bool,
}

impl std::fmt::Debug for AuthSecurityDiagnostics {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthSecurityDiagnostics")
            .field("supported", &self.supported)
            .field("authentication_enabled", &self.authentication_enabled)
            .field("authorization_enabled", &self.authorization_enabled)
            .field("acl_file_watch_enabled", &self.acl_file_watch_enabled)
            .field("acl_generation", &self.acl_generation)
            .field("acl_reload_attempts", &self.acl_reload_attempts)
            .field("acl_reload_successes", &self.acl_reload_successes)
            .field("acl_reload_failures", &self.acl_reload_failures)
            .field("acl_reload_skipped", &self.acl_reload_skipped)
            .field("credential_rotation_supported", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerDiagnostics {
    pub broker_name: String,
    pub broker_id: u64,
    pub observed_at_millis: Option<u64>,
    pub coverage: BrokerDiagnosticsCoverage,
    pub readiness: Option<BrokerReadinessDiagnostics>,
    pub config: Option<BrokerConfigSummary>,
    pub store_health: Option<StoreHealthDiagnostics>,
    pub recovery: Option<RecoveryDiagnostics>,
    pub background_index_rebuild: Option<BackgroundIndexRebuildDiagnostics>,
    pub rocksdb: RocksDbMaintenanceDiagnostics,
    pub tiered: TieredDispatchDiagnostics,
    pub auth: AuthSecurityDiagnostics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBrokerDiagnosticsResult {
    pub schema_version: String,
    pub observed_at_millis: u64,
    pub brokers: Vec<BrokerDiagnostics>,
    pub unavailable_brokers: usize,
    pub partial: bool,
}

pub trait BrokerAdmin: Send {
    fn list_brokers<'a>(&'a mut self, request: &'a ListBrokersRequest) -> AdminFuture<'a, ListBrokersResult>;

    fn probe_broker_runtime<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, ProbeBrokerRuntimeResult>;

    fn query_broker_diagnostics<'a>(
        &'a mut self,
        request: &'a QueryBrokerDiagnosticsRequest,
    ) -> AdminFuture<'a, QueryBrokerDiagnosticsResult>;
}

/// Read-only broker administration capability.
pub trait BrokerQueryAdmin: Send {
    fn list_brokers<'a>(&'a mut self, request: &'a ListBrokersRequest) -> AdminFuture<'a, ListBrokersResult>;

    fn probe_broker_runtime<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, ProbeBrokerRuntimeResult>;

    fn query_broker_diagnostics<'a>(
        &'a mut self,
        request: &'a QueryBrokerDiagnosticsRequest,
    ) -> AdminFuture<'a, QueryBrokerDiagnosticsResult>;
}

impl<T: BrokerAdmin + ?Sized> BrokerQueryAdmin for T {
    fn list_brokers<'a>(&'a mut self, request: &'a ListBrokersRequest) -> AdminFuture<'a, ListBrokersResult> {
        BrokerAdmin::list_brokers(self, request)
    }

    fn probe_broker_runtime<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, ProbeBrokerRuntimeResult> {
        BrokerAdmin::probe_broker_runtime(self, request)
    }

    fn query_broker_diagnostics<'a>(
        &'a mut self,
        request: &'a QueryBrokerDiagnosticsRequest,
    ) -> AdminFuture<'a, QueryBrokerDiagnosticsResult> {
        BrokerAdmin::query_broker_diagnostics(self, request)
    }
}

/// Marker for future broker mutation operations.
pub trait BrokerMutationAdmin: Send {}

pub(crate) fn project_broker_diagnostics(broker_name: String, broker_id: u64, runtime: &KVTable) -> BrokerDiagnostics {
    let schema = runtime_value(runtime, "sreDiagnosticsSchemaVersion");
    if schema != Some("rocketmq.broker-diagnostics.v1") {
        return unsupported_diagnostics(broker_name, broker_id);
    }

    let readiness = parse_readiness(runtime);
    let config = parse_config(runtime);
    let store_health = parse_store_health(runtime);
    let background_index_rebuild = parse_background_index(runtime);
    let missing_required =
        readiness.is_none() || config.is_none() || store_health.is_none() || background_index_rebuild.is_none();
    BrokerDiagnostics {
        broker_name,
        broker_id,
        observed_at_millis: runtime_u64(runtime, "sreDiagnosticsObservedAtMillis"),
        coverage: if missing_required {
            BrokerDiagnosticsCoverage::Partial
        } else {
            BrokerDiagnosticsCoverage::Available
        },
        readiness,
        config,
        store_health,
        recovery: parse_recovery(runtime),
        background_index_rebuild,
        rocksdb: RocksDbMaintenanceDiagnostics {
            supported: runtime_bool(runtime, "rocksdbMaintenanceSupported").unwrap_or(false),
            maintenance_running: runtime_bool(runtime, "rocksdbMaintenanceRunning"),
            message_maintenance_running: runtime_bool(runtime, "messageRocksdbMaintenanceRunning"),
        },
        tiered: TieredDispatchDiagnostics {
            configured: runtime_bool(runtime, "tieredStoreConfigured").unwrap_or(false),
            dispatch_ready: runtime_bool(runtime, "tieredDispatchReady"),
            minimum_pinned_wal_segment: runtime_u64(runtime, "tieredMinimumPinnedWalSegment"),
        },
        auth: AuthSecurityDiagnostics {
            supported: runtime_bool(runtime, "authDiagnosticsSupported").unwrap_or(false),
            authentication_enabled: runtime_bool(runtime, "authAuthenticationEnabled"),
            authorization_enabled: runtime_bool(runtime, "authAuthorizationEnabled"),
            acl_file_watch_enabled: runtime_bool(runtime, "authAclFileWatchEnabled"),
            acl_generation: runtime_u64(runtime, "authAclGeneration"),
            acl_reload_attempts: runtime_u64(runtime, "authAclReloadAttempts"),
            acl_reload_successes: runtime_u64(runtime, "authAclReloadSuccesses"),
            acl_reload_failures: runtime_u64(runtime, "authAclReloadFailures"),
            acl_reload_skipped: runtime_u64(runtime, "authAclReloadSkipped"),
            credential_rotation_supported: runtime_bool(runtime, "authCredentialRotationSupported").unwrap_or(false),
        },
        warnings: missing_required
            .then_some("broker_diagnostics_fields_missing".to_owned())
            .into_iter()
            .collect(),
    }
}

fn unsupported_diagnostics(broker_name: String, broker_id: u64) -> BrokerDiagnostics {
    BrokerDiagnostics {
        broker_name,
        broker_id,
        observed_at_millis: None,
        coverage: BrokerDiagnosticsCoverage::Unsupported,
        readiness: None,
        config: None,
        store_health: None,
        recovery: None,
        background_index_rebuild: None,
        rocksdb: RocksDbMaintenanceDiagnostics {
            supported: false,
            maintenance_running: None,
            message_maintenance_running: None,
        },
        tiered: TieredDispatchDiagnostics {
            configured: false,
            dispatch_ready: None,
            minimum_pinned_wal_segment: None,
        },
        auth: AuthSecurityDiagnostics {
            supported: false,
            authentication_enabled: None,
            authorization_enabled: None,
            acl_file_watch_enabled: None,
            acl_generation: None,
            acl_reload_attempts: None,
            acl_reload_successes: None,
            acl_reload_failures: None,
            acl_reload_skipped: None,
            credential_rotation_supported: false,
        },
        warnings: vec!["broker_diagnostics_contract_unsupported".to_owned()],
    }
}

fn parse_readiness(runtime: &KVTable) -> Option<BrokerReadinessDiagnostics> {
    Some(BrokerReadinessDiagnostics {
        ready: runtime_bool(runtime, "brokerReady")?,
        active: runtime_bool(runtime, "brokerActive")?,
        shutdown: runtime_bool(runtime, "brokerShutdown")?,
        registration_accepting: runtime_bool(runtime, "brokerRegistrationAccepting")?,
        registration_configured: runtime_bool(runtime, "brokerRegistrationConfigured")?,
    })
}

fn parse_config(runtime: &KVTable) -> Option<BrokerConfigSummary> {
    Some(BrokerConfigSummary {
        generation: runtime_u64(runtime, "brokerConfigGeneration")?,
        broker_role: runtime_value(runtime, "brokerRole")?.to_owned(),
        store_type: runtime_value(runtime, "storeType")?.to_owned(),
        timer_wheel_enabled: runtime_bool(runtime, "timerWheelEnabled")?,
        transient_store_pool_enabled: runtime_bool(runtime, "transientStorePoolEnabled")?,
        tiered_store_configured: runtime_bool(runtime, "tieredStoreConfigured")?,
    })
}

fn parse_store_health(runtime: &KVTable) -> Option<StoreHealthDiagnostics> {
    Some(StoreHealthDiagnostics {
        writeable: runtime_bool(runtime, "storeWriteable")?,
        last_flush_error: runtime_bool(runtime, "storeLastFlushError")?,
        os_page_cache_busy: runtime_bool(runtime, "storeOsPageCacheBusy")?,
        transient_store_pool_deficient: runtime_bool(runtime, "storeTransientPoolDeficient")?,
        dispatch_behind_bytes: runtime_i64(runtime, "storeDispatchBehindBytes")?,
        shutdown: runtime_bool(runtime, "storeShutdown")?,
        ha_pending_request_count: runtime_u64(runtime, "storeHaPendingRequestCount")?,
        ha_pending_oldest_wait_millis: runtime_u64(runtime, "storeHaPendingOldestWaitMillis")?,
        sync_flush: SyncFlushDiagnostics {
            queue_depth: runtime_u64(runtime, "storeSyncFlushQueueDepth")?,
            timeout_total: runtime_u64(runtime, "storeSyncFlushTimeoutTotal")?,
            oldest_wait_millis: runtime_u64(runtime, "storeSyncFlushOldestWaitMillis")?,
        },
    })
}

fn parse_recovery(runtime: &KVTable) -> Option<RecoveryDiagnostics> {
    let available = runtime_bool(runtime, "recoveryReportAvailable")?;
    Some(RecoveryDiagnostics {
        available,
        total_duration_millis: runtime_u64(runtime, "recoveryTotalDurationMillis"),
        phase_count: runtime_u64(runtime, "recoveryPhaseCount"),
        failed_phase_count: runtime_u64(runtime, "recoveryFailedPhaseCount"),
        fallback_phase_count: runtime_u64(runtime, "recoveryFallbackPhaseCount"),
        fallback_reason_present: runtime_bool(runtime, "recoveryFallbackReasonPresent"),
        scanned_bytes: runtime_u64(runtime, "recoveryScannedBytes"),
        recovered_messages: runtime_u64(runtime, "recoveryRecoveredMessages"),
        invalid_messages: runtime_u64(runtime, "recoveryInvalidMessages"),
        truncated_files: runtime_u64(runtime, "recoveryTruncatedFiles"),
        index_files_removed: runtime_u64(runtime, "recoveryIndexFilesRemoved"),
        index_files_rebuilt: runtime_u64(runtime, "recoveryIndexFilesRebuilt"),
    })
}

fn parse_background_index(runtime: &KVTable) -> Option<BackgroundIndexRebuildDiagnostics> {
    Some(BackgroundIndexRebuildDiagnostics {
        state: runtime_value(runtime, "backgroundIndexRebuildState")?.to_owned(),
        effective_enabled: runtime_bool(runtime, "backgroundIndexRebuildEffectiveEnable")?,
        gray_mode: runtime_bool(runtime, "backgroundIndexRebuildGrayMode")?,
        current_safe_offset: runtime_i64(runtime, "backgroundIndexRebuildCurrentSafeOffset")?,
        target_offset: runtime_i64(runtime, "backgroundIndexRebuildTargetOffset")?,
        backlog_bytes: runtime_i64(runtime, "backgroundIndexRebuildBacklogBytes")?,
        rebuilt_bytes: runtime_u64(runtime, "backgroundIndexRebuildRebuiltBytes")?,
        rebuilt_messages: runtime_u64(runtime, "backgroundIndexRebuildRebuiltMessages")?,
        failure_count: runtime_u64(runtime, "backgroundIndexRebuildFailureCount")?,
        bytes_per_second: runtime_u64(runtime, "backgroundIndexRebuildBytesPerSecond")?,
    })
}

fn runtime_value<'a>(runtime: &'a KVTable, key: &str) -> Option<&'a str> {
    runtime.table.get(&CheetahString::from(key)).map(CheetahString::as_str)
}

fn runtime_bool(runtime: &KVTable, key: &str) -> Option<bool> {
    match runtime_value(runtime, key)? {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn runtime_u64(runtime: &KVTable, key: &str) -> Option<u64> {
    runtime_value(runtime, key)?.parse().ok()
}

fn runtime_i64(runtime: &KVTable, key: &str) -> Option<i64> {
    runtime_value(runtime, key)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn diagnostics_projection_uses_real_generation_and_never_serializes_unknown_kv_fields() {
        let table = HashMap::from([
            ("sreDiagnosticsSchemaVersion", "rocketmq.broker-diagnostics.v1"),
            ("sreDiagnosticsObservedAtMillis", "1000"),
            ("brokerConfigGeneration", "9"),
            ("brokerReady", "true"),
            ("brokerActive", "true"),
            ("brokerShutdown", "false"),
            ("brokerRegistrationAccepting", "true"),
            ("brokerRegistrationConfigured", "true"),
            ("brokerRole", "ASYNC_MASTER"),
            ("storeType", "local"),
            ("timerWheelEnabled", "false"),
            ("transientStorePoolEnabled", "false"),
            ("tieredStoreConfigured", "false"),
            ("storeWriteable", "true"),
            ("storeLastFlushError", "false"),
            ("storeOsPageCacheBusy", "false"),
            ("storeTransientPoolDeficient", "false"),
            ("storeDispatchBehindBytes", "0"),
            ("storeShutdown", "false"),
            ("storeHaPendingRequestCount", "0"),
            ("storeHaPendingOldestWaitMillis", "0"),
            ("storeSyncFlushQueueDepth", "0"),
            ("storeSyncFlushTimeoutTotal", "0"),
            ("storeSyncFlushOldestWaitMillis", "0"),
            ("recoveryReportAvailable", "false"),
            ("backgroundIndexRebuildState", "idle"),
            ("backgroundIndexRebuildEffectiveEnable", "false"),
            ("backgroundIndexRebuildGrayMode", "false"),
            ("backgroundIndexRebuildCurrentSafeOffset", "0"),
            ("backgroundIndexRebuildTargetOffset", "0"),
            ("backgroundIndexRebuildBacklogBytes", "0"),
            ("backgroundIndexRebuildRebuiltBytes", "0"),
            ("backgroundIndexRebuildRebuiltMessages", "0"),
            ("backgroundIndexRebuildFailureCount", "0"),
            ("backgroundIndexRebuildBytesPerSecond", "0"),
            ("authDiagnosticsSupported", "true"),
            ("authAuthenticationEnabled", "true"),
            ("authAuthorizationEnabled", "true"),
            ("authAclFileWatchEnabled", "true"),
            ("authAclGeneration", "7"),
            ("authAclReloadAttempts", "3"),
            ("authAclReloadSuccesses", "2"),
            ("authAclReloadFailures", "1"),
            ("authAclReloadSkipped", "0"),
            ("authCredentialRotationSupported", "false"),
            ("secretAccessKey", "must-not-escape"),
        ])
        .into_iter()
        .map(|(key, value)| (CheetahString::from(key), CheetahString::from(value)))
        .collect();
        let diagnostics = project_broker_diagnostics("broker-a".to_owned(), 0, &KVTable { table });

        assert_eq!(diagnostics.config.as_ref().map(|config| config.generation), Some(9));
        assert_eq!(diagnostics.coverage, BrokerDiagnosticsCoverage::Available);
        let encoded = serde_json::to_string(&diagnostics).expect("diagnostics should serialize");
        assert!(!encoded.contains("secretAccessKey"));
        assert!(!encoded.contains("must-not-escape"));
    }

    #[test]
    fn older_broker_is_explicitly_unsupported_instead_of_guessing_generation() {
        let diagnostics = project_broker_diagnostics("broker-a".to_owned(), 0, &KVTable { table: HashMap::new() });
        assert_eq!(diagnostics.coverage, BrokerDiagnosticsCoverage::Unsupported);
        assert!(diagnostics.config.is_none());
    }
}
