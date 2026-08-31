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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::tools::cluster_tools::BrokerSummary;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DescribeBrokerArgs {
    pub cluster: String,
    pub broker_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct DescribeBrokerOutput {
    pub cluster: String,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub namesrv_addr: String,
    pub broker_name: String,
    pub brokers: Vec<BrokerSummary>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrokerDiagnosticsArgs {
    pub cluster: String,
    pub broker_name: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BrokerDiagnosticsCoverage {
    Available,
    Partial,
    Unsupported,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BrokerReadinessDiagnostics {
    pub ready: bool,
    pub active: bool,
    pub shutdown: bool,
    pub registration_accepting: bool,
    pub registration_configured: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BrokerRole {
    #[serde(rename = "ASYNC_MASTER")]
    AsyncMaster,
    #[serde(rename = "SYNC_MASTER")]
    SyncMaster,
    #[serde(rename = "SLAVE")]
    Slave,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BrokerStoreType {
    #[serde(rename = "LocalFile")]
    LocalFile,
    #[serde(rename = "RocksDB")]
    RocksDb,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BrokerRuntimeConfigSummary {
    pub generation: u64,
    pub broker_role: BrokerRole,
    pub store_type: BrokerStoreType,
    pub timer_wheel_enabled: bool,
    pub transient_store_pool_enabled: bool,
    pub tiered_store_configured: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SyncFlushDiagnostics {
    pub queue_depth: u64,
    pub timeout_total: u64,
    pub oldest_wait_millis: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HaRole {
    Master,
    Replica,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HaAckPolicy {
    AllInSyncSet,
    LocalDurable,
    ReplicaCount,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HaDecisionCode {
    WaitingForReplicaProgress,
    NotObserved,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct HaDiagnostics {
    pub supported: bool,
    pub role: Option<HaRole>,
    pub master_epoch: Option<i32>,
    pub sync_state_set_epoch: Option<i32>,
    pub sync_state_set_size: Option<u64>,
    pub max_replica_lag_bytes: Option<u64>,
    pub ack_policy: Option<HaAckPolicy>,
    pub required_ack_count: Option<u64>,
    pub decision_code: Option<HaDecisionCode>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackgroundIndexRebuildState {
    Idle,
    Running,
    Paused,
    Completed,
    Retrying,
    Failed,
    Shutdown,
    Unknown,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BackgroundIndexRebuildDiagnostics {
    pub state: BackgroundIndexRebuildState,
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

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RocksDbMaintenanceDiagnostics {
    pub supported: bool,
    pub maintenance_running: Option<bool>,
    pub message_maintenance_running: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct TieredDispatchDiagnostics {
    pub configured: bool,
    pub dispatch_ready: Option<bool>,
    pub minimum_pinned_wal_segment: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BrokerDiagnosticsRow {
    pub broker_name: String,
    pub broker_id: u64,
    pub observed_at_millis: Option<u64>,
    pub coverage: BrokerDiagnosticsCoverage,
    pub readiness: Option<BrokerReadinessDiagnostics>,
    pub config: Option<BrokerRuntimeConfigSummary>,
    pub store_health: Option<StoreHealthDiagnostics>,
    pub ha: HaDiagnostics,
    pub recovery: Option<RecoveryDiagnostics>,
    pub background_index_rebuild: Option<BackgroundIndexRebuildDiagnostics>,
    pub rocksdb: RocksDbMaintenanceDiagnostics,
    pub tiered: TieredDispatchDiagnostics,
    pub auth: AuthSecurityDiagnostics,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BrokerDiagnosticsOutput {
    pub cluster: String,
    pub broker_name: String,
    pub diagnostics_schema_version: String,
    pub observed_at_millis: u64,
    pub brokers: Vec<BrokerDiagnosticsRow>,
    pub unavailable_brokers: usize,
}

#[cfg(test)]
mod tests {
    use serde::de::DeserializeOwned;
    use serde_json::json;

    use super::*;

    fn schema_enum_values<T: JsonSchema>() -> serde_json::Value {
        serde_json::to_value(schemars::schema_for!(T))
            .expect("enum schema should serialize")
            .get("enum")
            .cloned()
            .expect("enum schema should expose a closed value set")
    }

    fn assert_unknown_wire_value_is_rejected<T>()
    where
        T: DeserializeOwned + std::fmt::Debug,
    {
        assert!(serde_json::from_value::<T>(json!("future_backend_value")).is_err());
    }

    #[test]
    fn broker_diagnostic_enums_serialize_and_publish_closed_schemas() {
        assert_eq!(
            serde_json::to_value([
                BrokerRole::AsyncMaster,
                BrokerRole::SyncMaster,
                BrokerRole::Slave,
                BrokerRole::Unknown,
            ])
            .expect("broker roles should serialize"),
            json!(["ASYNC_MASTER", "SYNC_MASTER", "SLAVE", "unknown"])
        );
        assert_eq!(
            schema_enum_values::<BrokerRole>(),
            json!(["ASYNC_MASTER", "SYNC_MASTER", "SLAVE", "unknown"])
        );

        assert_eq!(
            serde_json::to_value([
                BrokerStoreType::LocalFile,
                BrokerStoreType::RocksDb,
                BrokerStoreType::Unknown,
            ])
            .expect("store types should serialize"),
            json!(["LocalFile", "RocksDB", "unknown"])
        );
        assert_eq!(
            schema_enum_values::<BrokerStoreType>(),
            json!(["LocalFile", "RocksDB", "unknown"])
        );

        assert_eq!(
            serde_json::to_value([HaRole::Master, HaRole::Replica]).expect("HA roles should serialize"),
            json!(["master", "replica"])
        );
        assert_eq!(schema_enum_values::<HaRole>(), json!(["master", "replica"]));

        assert_eq!(
            serde_json::to_value([
                HaAckPolicy::AllInSyncSet,
                HaAckPolicy::LocalDurable,
                HaAckPolicy::ReplicaCount,
            ])
            .expect("HA acknowledgement policies should serialize"),
            json!(["all_in_sync_set", "local_durable", "replica_count"])
        );
        assert_eq!(
            schema_enum_values::<HaAckPolicy>(),
            json!(["all_in_sync_set", "local_durable", "replica_count"])
        );

        assert_eq!(
            serde_json::to_value([HaDecisionCode::WaitingForReplicaProgress, HaDecisionCode::NotObserved,])
                .expect("HA decision codes should serialize"),
            json!(["waiting_for_replica_progress", "not_observed"])
        );
        assert_eq!(
            schema_enum_values::<HaDecisionCode>(),
            json!(["waiting_for_replica_progress", "not_observed"])
        );

        assert_eq!(
            serde_json::to_value([
                BackgroundIndexRebuildState::Idle,
                BackgroundIndexRebuildState::Running,
                BackgroundIndexRebuildState::Paused,
                BackgroundIndexRebuildState::Completed,
                BackgroundIndexRebuildState::Retrying,
                BackgroundIndexRebuildState::Failed,
                BackgroundIndexRebuildState::Shutdown,
                BackgroundIndexRebuildState::Unknown,
            ])
            .expect("background rebuild states should serialize"),
            json!([
                "idle",
                "running",
                "paused",
                "completed",
                "retrying",
                "failed",
                "shutdown",
                "unknown"
            ])
        );
        assert_eq!(
            schema_enum_values::<BackgroundIndexRebuildState>(),
            json!([
                "idle",
                "running",
                "paused",
                "completed",
                "retrying",
                "failed",
                "shutdown",
                "unknown"
            ])
        );
    }

    #[test]
    fn broker_diagnostic_enums_reject_arbitrary_wire_values() {
        assert_unknown_wire_value_is_rejected::<BrokerRole>();
        assert_unknown_wire_value_is_rejected::<BrokerStoreType>();
        assert_unknown_wire_value_is_rejected::<HaRole>();
        assert_unknown_wire_value_is_rejected::<HaAckPolicy>();
        assert_unknown_wire_value_is_rejected::<HaDecisionCode>();
        assert_unknown_wire_value_is_rejected::<BackgroundIndexRebuildState>();
    }
}
