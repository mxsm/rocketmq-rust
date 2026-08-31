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

use std::collections::BTreeMap;

#[cfg(any(feature = "read-client-adapter", test))]
use cheetah_string::CheetahString;
#[cfg(any(feature = "read-client-adapter", test))]
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::query::AdminQueryResult;
use crate::core::AdminFuture;
use crate::core::AdminResult;

/// Stable schema version emitted by the read-only broker diagnostics query.
pub const BROKER_DIAGNOSTICS_SCHEMA_VERSION: &str = "rocketmq.admin-broker-diagnostics.v1";
/// Stable schema version emitted by the bounded Broker log-filter query.
pub const BROKER_LOG_FILTER_STATE_SCHEMA_VERSION: &str = "rocketmq.admin-broker-log-filter-state.v1";
/// Maximum number of physical instances accepted for one logical Broker.
pub const MAX_EXACT_BROKER_INSTANCES: usize = 64;

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
pub struct ProbeBrokerRuntimeTargetRequest {
    pub cluster: String,
    pub broker_name: String,
}

impl ProbeBrokerRuntimeTargetRequest {
    pub fn try_new(cluster: impl Into<String>, broker_name: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            broker_name: required("broker_name", broker_name)?,
        })
    }
}

/// Exact, uncapped status for one logical Broker target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerRuntimeTargetStatus {
    Available,
    SourceUnavailable,
    NotFound,
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

/// Exact logical Broker target for bounded diagnostics queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryBrokerDiagnosticsTargetRequest {
    cluster: String,
    broker_name: String,
}

impl QueryBrokerDiagnosticsTargetRequest {
    /// Creates a diagnostics request for one logical Broker name in one cluster.
    ///
    /// # Errors
    ///
    /// Returns an error when `cluster` is blank or `broker_name` is not a
    /// bounded logical name. Network addresses are intentionally rejected.
    pub fn try_new(cluster: impl Into<String>, broker_name: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            broker_name: logical_broker_name(broker_name)?,
        })
    }

    #[must_use]
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    #[must_use]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }
}

impl<'de> Deserialize<'de> for QueryBrokerDiagnosticsTargetRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRequest {
            cluster: String,
            broker_name: String,
        }
        let wire = WireRequest::deserialize(deserializer)?;
        Self::try_new(wire.cluster, wire.broker_name).map_err(serde::de::Error::custom)
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaDiagnostics {
    pub supported: bool,
    pub role: Option<String>,
    pub master_epoch: Option<i32>,
    pub sync_state_set_epoch: Option<i32>,
    pub sync_state_set_size: Option<u64>,
    pub max_replica_lag_bytes: Option<u64>,
    pub ack_policy: Option<String>,
    pub required_ack_count: Option<u64>,
    pub decision_code: Option<String>,
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
    #[serde(default)]
    pub ha: HaDiagnostics,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBrokerAllowlistedConfigRequest {
    pub broker_addr: String,
}

impl QueryBrokerAllowlistedConfigRequest {
    pub fn try_new(broker_addr: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            broker_addr: required("broker_addr", broker_addr)?,
        })
    }
}

/// Exact logical Broker target for fixed allowlisted configuration reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryBrokerAllowlistedConfigTargetRequest {
    cluster: String,
    broker_name: String,
}

impl QueryBrokerAllowlistedConfigTargetRequest {
    /// Creates a fixed allowlisted configuration request for one logical Broker.
    ///
    /// # Errors
    ///
    /// Returns an error when `cluster` is blank or `broker_name` is not a
    /// bounded logical name. Network addresses are intentionally rejected.
    pub fn try_new(cluster: impl Into<String>, broker_name: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            broker_name: logical_broker_name(broker_name)?,
        })
    }

    #[must_use]
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    #[must_use]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }
}

impl<'de> Deserialize<'de> for QueryBrokerAllowlistedConfigTargetRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRequest {
            cluster: String,
            broker_name: String,
        }
        let wire = WireRequest::deserialize(deserializer)?;
        Self::try_new(wire.cluster, wire.broker_name).map_err(serde::de::Error::custom)
    }
}

/// Fixed, non-sensitive Broker properties supported by supervised SRE changes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerAllowlistedConfig {
    pub generation: u64,
    pub send_message_thread_pool_nums: Option<u32>,
    pub pull_message_thread_pool_nums: Option<u32>,
    pub flush_delay_offset_interval_ms: Option<u64>,
    pub max_client_event_count: Option<i32>,
}

/// One address-free Broker instance configuration observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerAllowlistedConfigTarget {
    pub broker_name: String,
    pub broker_id: u64,
    pub config: BrokerAllowlistedConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum BrokerLogLevel {
    Info,
    Debug,
}

impl BrokerLogLevel {
    #[must_use]
    pub const fn as_uppercase(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
        }
    }

    #[must_use]
    pub const fn as_filter_value(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Debug => "debug",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBrokerLogFilterStateRequest {
    pub broker_addr: String,
    pub logger: String,
}

impl QueryBrokerLogFilterStateRequest {
    /// Creates a query for one allowlisted Broker logger target.
    ///
    /// # Errors
    ///
    /// Rejects blank addresses and logger targets outside
    /// `rocketmq_broker::`.
    pub fn try_new(broker_addr: impl Into<String>, logger: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            broker_addr: required("broker_addr", broker_addr)?,
            logger: broker_logger(logger)?,
        })
    }
}

/// Exact logical Broker target for one allowlisted logger read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryBrokerLogFilterStateTargetRequest {
    cluster: String,
    broker_name: String,
    logger: String,
}

impl QueryBrokerLogFilterStateTargetRequest {
    /// Creates a log-filter query for one logical Broker and logger namespace.
    ///
    /// # Errors
    ///
    /// Returns an error when `cluster` is blank, `broker_name` is not a
    /// bounded logical name, or `logger` is not a strict ASCII Rust module path
    /// rooted at `rocketmq_broker`.
    pub fn try_new(
        cluster: impl Into<String>,
        broker_name: impl Into<String>,
        logger: impl Into<String>,
    ) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            broker_name: logical_broker_name(broker_name)?,
            logger: logical_broker_logger(logger)?,
        })
    }

    #[must_use]
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    #[must_use]
    pub fn broker_name(&self) -> &str {
        &self.broker_name
    }

    #[must_use]
    pub fn logger(&self) -> &str {
        &self.logger
    }
}

impl<'de> Deserialize<'de> for QueryBrokerLogFilterStateTargetRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRequest {
            cluster: String,
            broker_name: String,
            logger: String,
        }
        let wire = WireRequest::deserialize(deserializer)?;
        Self::try_new(wire.cluster, wire.broker_name, wire.logger).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerLogFilterState {
    pub schema_version: String,
    pub supported: bool,
    pub logger: String,
    pub level: Option<BrokerLogLevel>,
    pub active_operation_id: Option<String>,
    pub last_completed_operation_id: Option<String>,
    pub expires_at_millis: Option<u64>,
}

/// One address-free Broker instance log-filter observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerLogFilterStateTarget {
    pub broker_name: String,
    pub broker_id: u64,
    pub state: BrokerLogFilterState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetBrokerLogFilterTtlRequest {
    pub broker_addr: String,
    pub logger: String,
    pub level: BrokerLogLevel,
    pub ttl_seconds: u32,
    pub operation_id: String,
}

impl SetBrokerLogFilterTtlRequest {
    /// Creates an exact, short-lived Broker log-filter request.
    ///
    /// # Errors
    ///
    /// Rejects targets outside `rocketmq_broker::`, blank operation IDs, and
    /// TTLs outside 60 through 900 seconds.
    pub fn try_new(
        broker_addr: impl Into<String>,
        logger: impl Into<String>,
        level: BrokerLogLevel,
        ttl_seconds: u32,
        operation_id: impl Into<String>,
    ) -> AdminResult<Self> {
        if !(60..=900).contains(&ttl_seconds) {
            return Err(crate::core::AdminError::invalid_argument(
                "ttl_seconds",
                "must be between 60 and 900",
            ));
        }
        Ok(Self {
            broker_addr: required("broker_addr", broker_addr)?,
            logger: broker_logger(logger)?,
            level,
            ttl_seconds,
            operation_id: bounded_operation_id(operation_id)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreBrokerLogFilterRequest {
    pub broker_addr: String,
    pub operation_id: String,
}

impl RestoreBrokerLogFilterRequest {
    /// Creates a restoration request bound to one SRE operation.
    ///
    /// # Errors
    ///
    /// Rejects blank addresses and invalid operation IDs.
    pub fn try_new(broker_addr: impl Into<String>, operation_id: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            broker_addr: required("broker_addr", broker_addr)?,
            operation_id: bounded_operation_id(operation_id)?,
        })
    }
}

pub trait BrokerAdmin: Send {
    fn list_brokers<'a>(&'a mut self, request: &'a ListBrokersRequest) -> AdminFuture<'a, ListBrokersResult>;

    fn probe_broker_runtime<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, ProbeBrokerRuntimeResult>;

    fn list_brokers_with_evidence<'a>(
        &'a mut self,
        request: &'a ListBrokersRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListBrokersResult>> {
        Box::pin(async move { self.list_brokers(request).await.map(AdminQueryResult::complete) })
    }

    fn probe_broker_runtime_with_evidence<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ProbeBrokerRuntimeResult>> {
        Box::pin(async move { self.probe_broker_runtime(request).await.map(AdminQueryResult::complete) })
    }

    fn probe_broker_runtime_target<'a>(
        &'a mut self,
        _request: &'a ProbeBrokerRuntimeTargetRequest,
    ) -> AdminFuture<'a, BrokerRuntimeTargetStatus> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "probe_broker_runtime_target",
                "exact Broker target probing is not implemented by this adapter",
            ))
        })
    }

    fn query_broker_diagnostics<'a>(
        &'a mut self,
        request: &'a QueryBrokerDiagnosticsRequest,
    ) -> AdminFuture<'a, QueryBrokerDiagnosticsResult>;

    /// Evidence-aware exact logical-target diagnostics query.
    ///
    /// Explicit metadata non-membership is `NotFound`. Missing or inconsistent
    /// metadata is a backend error. Partial physical-source success returns rows
    /// with bounded evidence; total source failure returns an error.
    fn query_broker_diagnostics_target_with_evidence<'a>(
        &'a mut self,
        _request: &'a QueryBrokerDiagnosticsTargetRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryBrokerDiagnosticsResult>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_broker_diagnostics_target",
                "exact Broker diagnostics are not implemented by this adapter",
            ))
        })
    }

    fn query_allowlisted_config<'a>(
        &'a mut self,
        request: &'a QueryBrokerAllowlistedConfigRequest,
    ) -> AdminFuture<'a, BrokerAllowlistedConfig>;

    /// Evidence-aware exact logical-target allowlisted configuration query.
    ///
    /// Explicit metadata non-membership is `NotFound`. Missing or inconsistent
    /// metadata is a backend error. Partial physical-source success returns rows
    /// with bounded evidence; total source failure returns an error.
    fn query_allowlisted_config_target_with_evidence<'a>(
        &'a mut self,
        _request: &'a QueryBrokerAllowlistedConfigTargetRequest,
    ) -> AdminFuture<'a, AdminQueryResult<Vec<BrokerAllowlistedConfigTarget>>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_allowlisted_config_target",
                "exact Broker configuration query is not implemented by this adapter",
            ))
        })
    }

    fn query_log_filter_state<'a>(
        &'a mut self,
        _request: &'a QueryBrokerLogFilterStateRequest,
    ) -> AdminFuture<'a, BrokerLogFilterState> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_log_filter_state",
                "typed Broker log-filter query is not implemented by this adapter",
            ))
        })
    }

    /// Evidence-aware exact logical-target log-filter query.
    ///
    /// Explicit metadata non-membership is `NotFound`. Missing or inconsistent
    /// metadata is a backend error. Partial physical-source success returns rows
    /// with bounded evidence; total source failure returns an error.
    fn query_log_filter_state_target_with_evidence<'a>(
        &'a mut self,
        _request: &'a QueryBrokerLogFilterStateTargetRequest,
    ) -> AdminFuture<'a, AdminQueryResult<Vec<BrokerLogFilterStateTarget>>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_log_filter_state_target",
                "exact Broker log-filter query is not implemented by this adapter",
            ))
        })
    }
}

/// Read-only broker administration capability.
pub trait BrokerQueryAdmin: Send {
    fn list_brokers<'a>(&'a mut self, request: &'a ListBrokersRequest) -> AdminFuture<'a, ListBrokersResult>;

    fn probe_broker_runtime<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, ProbeBrokerRuntimeResult>;

    /// Evidence-aware sibling of [`Self::list_brokers`].
    fn list_brokers_with_evidence<'a>(
        &'a mut self,
        request: &'a ListBrokersRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListBrokersResult>> {
        Box::pin(async move { self.list_brokers(request).await.map(AdminQueryResult::complete) })
    }

    /// Evidence-aware sibling of [`Self::probe_broker_runtime`].
    fn probe_broker_runtime_with_evidence<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ProbeBrokerRuntimeResult>> {
        Box::pin(async move { self.probe_broker_runtime(request).await.map(AdminQueryResult::complete) })
    }

    /// Classifies one target independently of bounded public failure evidence.
    fn probe_broker_runtime_target<'a>(
        &'a mut self,
        _request: &'a ProbeBrokerRuntimeTargetRequest,
    ) -> AdminFuture<'a, BrokerRuntimeTargetStatus> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "probe_broker_runtime_target",
                "exact Broker target probing is not implemented by this adapter",
            ))
        })
    }

    fn query_broker_diagnostics<'a>(
        &'a mut self,
        request: &'a QueryBrokerDiagnosticsRequest,
    ) -> AdminFuture<'a, QueryBrokerDiagnosticsResult>;

    /// Evidence-aware exact logical-target diagnostics query.
    ///
    /// Explicit metadata non-membership is `NotFound`. Missing or inconsistent
    /// metadata is a backend error. Partial physical-source success returns rows
    /// with bounded evidence; total source failure returns an error.
    fn query_broker_diagnostics_target_with_evidence<'a>(
        &'a mut self,
        _request: &'a QueryBrokerDiagnosticsTargetRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryBrokerDiagnosticsResult>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_broker_diagnostics_target",
                "exact Broker diagnostics are not implemented by this adapter",
            ))
        })
    }

    fn query_allowlisted_config<'a>(
        &'a mut self,
        request: &'a QueryBrokerAllowlistedConfigRequest,
    ) -> AdminFuture<'a, BrokerAllowlistedConfig>;

    /// Evidence-aware exact logical-target allowlisted configuration query.
    ///
    /// Explicit metadata non-membership is `NotFound`. Missing or inconsistent
    /// metadata is a backend error. Partial physical-source success returns rows
    /// with bounded evidence; total source failure returns an error.
    fn query_allowlisted_config_target_with_evidence<'a>(
        &'a mut self,
        _request: &'a QueryBrokerAllowlistedConfigTargetRequest,
    ) -> AdminFuture<'a, AdminQueryResult<Vec<BrokerAllowlistedConfigTarget>>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_allowlisted_config_target",
                "exact Broker configuration query is not implemented by this adapter",
            ))
        })
    }

    fn query_log_filter_state<'a>(
        &'a mut self,
        _request: &'a QueryBrokerLogFilterStateRequest,
    ) -> AdminFuture<'a, BrokerLogFilterState> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_log_filter_state",
                "typed Broker log-filter query is not implemented by this adapter",
            ))
        })
    }

    /// Evidence-aware exact logical-target log-filter query.
    ///
    /// Explicit metadata non-membership is `NotFound`. Missing or inconsistent
    /// metadata is a backend error. Partial physical-source success returns rows
    /// with bounded evidence; total source failure returns an error.
    fn query_log_filter_state_target_with_evidence<'a>(
        &'a mut self,
        _request: &'a QueryBrokerLogFilterStateTargetRequest,
    ) -> AdminFuture<'a, AdminQueryResult<Vec<BrokerLogFilterStateTarget>>> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "query_log_filter_state_target",
                "exact Broker log-filter query is not implemented by this adapter",
            ))
        })
    }
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

    fn list_brokers_with_evidence<'a>(
        &'a mut self,
        request: &'a ListBrokersRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListBrokersResult>> {
        BrokerAdmin::list_brokers_with_evidence(self, request)
    }

    fn probe_broker_runtime_with_evidence<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ProbeBrokerRuntimeResult>> {
        BrokerAdmin::probe_broker_runtime_with_evidence(self, request)
    }

    fn probe_broker_runtime_target<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeTargetRequest,
    ) -> AdminFuture<'a, BrokerRuntimeTargetStatus> {
        BrokerAdmin::probe_broker_runtime_target(self, request)
    }

    fn query_broker_diagnostics<'a>(
        &'a mut self,
        request: &'a QueryBrokerDiagnosticsRequest,
    ) -> AdminFuture<'a, QueryBrokerDiagnosticsResult> {
        BrokerAdmin::query_broker_diagnostics(self, request)
    }

    fn query_broker_diagnostics_target_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryBrokerDiagnosticsTargetRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryBrokerDiagnosticsResult>> {
        BrokerAdmin::query_broker_diagnostics_target_with_evidence(self, request)
    }

    fn query_allowlisted_config<'a>(
        &'a mut self,
        request: &'a QueryBrokerAllowlistedConfigRequest,
    ) -> AdminFuture<'a, BrokerAllowlistedConfig> {
        BrokerAdmin::query_allowlisted_config(self, request)
    }

    fn query_allowlisted_config_target_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryBrokerAllowlistedConfigTargetRequest,
    ) -> AdminFuture<'a, AdminQueryResult<Vec<BrokerAllowlistedConfigTarget>>> {
        BrokerAdmin::query_allowlisted_config_target_with_evidence(self, request)
    }

    fn query_log_filter_state<'a>(
        &'a mut self,
        request: &'a QueryBrokerLogFilterStateRequest,
    ) -> AdminFuture<'a, BrokerLogFilterState> {
        BrokerAdmin::query_log_filter_state(self, request)
    }

    fn query_log_filter_state_target_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryBrokerLogFilterStateTargetRequest,
    ) -> AdminFuture<'a, AdminQueryResult<Vec<BrokerLogFilterStateTarget>>> {
        BrokerAdmin::query_log_filter_state_target_with_evidence(self, request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBrokerConfigGenerationRequest {
    pub broker_addr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBrokerConfigGenerationResult {
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchBrokerConfigRequest {
    pub broker_addr: String,
    pub expected_generation: u64,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchBrokerConfigOutcome {
    Applied {
        previous_generation: u64,
        generation: u64,
    },
    GenerationConflict {
        expected_generation: u64,
        actual_generation: u64,
    },
}

/// Narrow broker mutation operations used by supervised execution.
pub trait BrokerMutationAdmin: Send {
    fn query_config_generation<'a>(
        &'a mut self,
        request: &'a QueryBrokerConfigGenerationRequest,
    ) -> AdminFuture<'a, QueryBrokerConfigGenerationResult>;

    fn patch_config_if_generation<'a>(
        &'a mut self,
        request: &'a PatchBrokerConfigRequest,
    ) -> AdminFuture<'a, PatchBrokerConfigOutcome>;

    fn set_log_filter_ttl<'a>(&'a mut self, _request: &'a SetBrokerLogFilterTtlRequest) -> AdminFuture<'a, ()> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "set_log_filter_ttl",
                "typed Broker log-filter mutation is not implemented by this adapter",
            ))
        })
    }

    fn restore_log_filter<'a>(&'a mut self, _request: &'a RestoreBrokerLogFilterRequest) -> AdminFuture<'a, ()> {
        Box::pin(async {
            Err(crate::core::AdminError::backend(
                "restore_log_filter",
                "typed Broker log-filter mutation is not implemented by this adapter",
            ))
        })
    }
}

#[cfg(any(feature = "read-client-adapter", test))]
pub(crate) fn project_broker_log_filter_state(logger: String, runtime: &KVTable) -> BrokerLogFilterState {
    let supported = runtime_bool(runtime, "sreLogFilterControlSupported").unwrap_or(false);
    BrokerLogFilterState {
        schema_version: BROKER_LOG_FILTER_STATE_SCHEMA_VERSION.to_owned(),
        supported,
        level: supported
            .then(|| runtime_value(runtime, "sreLogFilterEffective"))
            .flatten()
            .and_then(|filter| effective_log_level(filter, &logger)),
        active_operation_id: bounded_runtime_identifier(runtime, "sreLogFilterActiveOperationId"),
        last_completed_operation_id: bounded_runtime_identifier(runtime, "sreLogFilterLastCompletedOperationId"),
        expires_at_millis: runtime_u64(runtime, "sreLogFilterExpiresAtMillis"),
        logger,
    }
}

fn broker_logger(logger: impl Into<String>) -> AdminResult<String> {
    let logger = required("logger", logger)?;
    if logger.len() > 128 || !logger.starts_with("rocketmq_broker::") {
        Err(crate::core::AdminError::invalid_argument(
            "logger",
            "must be an allowlisted rocketmq_broker:: target of at most 128 bytes",
        ))
    } else {
        Ok(logger)
    }
}

fn logical_broker_name(broker_name: impl Into<String>) -> AdminResult<String> {
    let broker_name = required("broker_name", broker_name)?;
    if broker_name.len() > 100
        || broker_name.parse::<std::net::IpAddr>().is_ok()
        || broker_name.parse::<std::net::SocketAddr>().is_ok()
        || broker_name.contains([':', '/', '\\', '@', '=', '&', '?'])
        || broker_name.chars().any(char::is_control)
        || !broker_name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
    {
        Err(crate::core::AdminError::invalid_argument(
            "broker_name",
            "must be a logical Broker identifier of at most 100 bytes",
        ))
    } else {
        Ok(broker_name)
    }
}

/// Returns whether a value is one bounded RocketMQ remoting `host:port` endpoint.
///
/// DNS names, IPv4, and bracketed IPv6 are supported. URLs, user information,
/// paths, queries, whitespace, zero ports, and unbracketed IPv6 are rejected.
#[must_use]
pub fn is_valid_remoting_endpoint(endpoint: &str) -> bool {
    if endpoint.is_empty()
        || endpoint.len() > 512
        || endpoint.chars().any(char::is_whitespace)
        || endpoint
            .chars()
            .any(|character| matches!(character, '/' | '\\' | '?' | '#' | '@' | '='))
    {
        return false;
    }
    let (host, port) = if let Some(rest) = endpoint.strip_prefix('[') {
        let Some((host, port)) = rest.split_once("]:") else {
            return false;
        };
        if host.parse::<std::net::Ipv6Addr>().is_err() {
            return false;
        }
        (host, port)
    } else {
        let Some((host, port)) = endpoint.rsplit_once(':') else {
            return false;
        };
        if host.contains(':') || !valid_remoting_host(host) {
            return false;
        }
        (host, port)
    };
    !host.is_empty() && port.parse::<u16>().is_ok_and(|port| port != 0)
}

fn valid_remoting_host(host: &str) -> bool {
    if host.parse::<std::net::Ipv4Addr>().is_ok() {
        return true;
    }
    if host
        .chars()
        .all(|character| character.is_ascii_digit() || character == '.')
    {
        return false;
    }
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '-')
        })
}

fn logical_broker_logger(logger: impl Into<String>) -> AdminResult<String> {
    let logger = logger.into();
    let valid = logger == logger.trim()
        && logger.len() <= 128
        && logger
            .strip_prefix("rocketmq_broker::")
            .is_some_and(valid_rust_module_path);
    if valid {
        Ok(logger)
    } else {
        Err(crate::core::AdminError::invalid_argument(
            "logger",
            "must be an allowlisted rocketmq_broker:: target of at most 128 bytes",
        ))
    }
}

fn valid_rust_module_path(path: &str) -> bool {
    !path.is_empty()
        && path.split("::").all(|segment| {
            let mut characters = segment.chars();
            characters
                .next()
                .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
                && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
        })
}

fn bounded_operation_id(operation_id: impl Into<String>) -> AdminResult<String> {
    let operation_id = required("operation_id", operation_id)?;
    if operation_id.len() > 128 || operation_id.chars().any(char::is_control) {
        Err(crate::core::AdminError::invalid_argument(
            "operation_id",
            "must be at most 128 bytes and contain no control characters",
        ))
    } else {
        Ok(operation_id)
    }
}

#[cfg(any(feature = "read-client-adapter", test))]
fn bounded_runtime_identifier(runtime: &KVTable, key: &str) -> Option<String> {
    runtime_value(runtime, key)
        .filter(|value| !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control))
        .map(ToOwned::to_owned)
}

#[cfg(any(feature = "read-client-adapter", test))]
fn effective_log_level(filter: &str, logger: &str) -> Option<BrokerLogLevel> {
    let mut selected = None;
    let mut selected_target_len = 0usize;
    for directive in filter.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        let (target, level) = match directive.split_once('=') {
            Some((target, level)) => (Some(target.trim()), level.trim()),
            None => (None, directive),
        };
        let target_len = target.map_or(0, str::len);
        if target.is_some_and(|target| !logger.starts_with(target)) || target_len < selected_target_len {
            continue;
        }
        let level = match level.to_ascii_lowercase().as_str() {
            "info" => BrokerLogLevel::Info,
            "debug" => BrokerLogLevel::Debug,
            _ => continue,
        };
        selected = Some(level);
        selected_target_len = target_len;
    }
    selected
}

#[cfg(any(feature = "read-client-adapter", test))]
pub(crate) fn project_broker_diagnostics(broker_name: String, broker_id: u64, runtime: &KVTable) -> BrokerDiagnostics {
    let schema = runtime_value(runtime, "sreDiagnosticsSchemaVersion");
    if !matches!(
        schema,
        Some("rocketmq.broker-diagnostics.v1" | "rocketmq.broker-diagnostics.legacy")
    ) {
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
        ha: parse_ha(runtime),
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

#[cfg(any(feature = "read-client-adapter", test))]
fn unsupported_diagnostics(broker_name: String, broker_id: u64) -> BrokerDiagnostics {
    BrokerDiagnostics {
        broker_name,
        broker_id,
        observed_at_millis: None,
        coverage: BrokerDiagnosticsCoverage::Unsupported,
        readiness: None,
        config: None,
        store_health: None,
        ha: HaDiagnostics::default(),
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

#[cfg(any(feature = "read-client-adapter", test))]
fn parse_readiness(runtime: &KVTable) -> Option<BrokerReadinessDiagnostics> {
    Some(BrokerReadinessDiagnostics {
        ready: runtime_bool(runtime, "brokerReady")?,
        active: runtime_bool(runtime, "brokerActive")?,
        shutdown: runtime_bool(runtime, "brokerShutdown")?,
        registration_accepting: runtime_bool(runtime, "brokerRegistrationAccepting")?,
        registration_configured: runtime_bool(runtime, "brokerRegistrationConfigured")?,
    })
}

#[cfg(any(feature = "read-client-adapter", test))]
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

#[cfg(any(feature = "read-client-adapter", test))]
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

#[cfg(any(feature = "read-client-adapter", test))]
fn parse_ha(runtime: &KVTable) -> HaDiagnostics {
    let supported = runtime_bool(runtime, "haDiagnosticsSupported").unwrap_or(false);
    HaDiagnostics {
        supported,
        role: supported
            .then(|| runtime_value(runtime, "haRole").map(str::to_owned))
            .flatten(),
        master_epoch: runtime_i32(runtime, "haMasterEpoch"),
        sync_state_set_epoch: runtime_i32(runtime, "haSyncStateSetEpoch"),
        sync_state_set_size: runtime_u64(runtime, "haSyncStateSetSize"),
        max_replica_lag_bytes: runtime_u64(runtime, "haMaxReplicaLagBytes"),
        ack_policy: supported
            .then(|| runtime_value(runtime, "haAckPolicy").map(str::to_owned))
            .flatten(),
        required_ack_count: runtime_u64(runtime, "haRequiredAckCount"),
        decision_code: supported
            .then(|| runtime_value(runtime, "haDecisionCode").map(str::to_owned))
            .flatten(),
    }
}

#[cfg(any(feature = "read-client-adapter", test))]
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

#[cfg(any(feature = "read-client-adapter", test))]
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

#[cfg(any(feature = "read-client-adapter", test))]
fn runtime_value<'a>(runtime: &'a KVTable, key: &str) -> Option<&'a str> {
    runtime.table.get(&CheetahString::from(key)).map(CheetahString::as_str)
}

#[cfg(any(feature = "read-client-adapter", test))]
fn runtime_bool(runtime: &KVTable, key: &str) -> Option<bool> {
    match runtime_value(runtime, key)? {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

#[cfg(any(feature = "read-client-adapter", test))]
fn runtime_u64(runtime: &KVTable, key: &str) -> Option<u64> {
    runtime_value(runtime, key)?.parse().ok()
}

#[cfg(any(feature = "read-client-adapter", test))]
fn runtime_i64(runtime: &KVTable, key: &str) -> Option<i64> {
    runtime_value(runtime, key)?.parse().ok()
}

#[cfg(any(feature = "read-client-adapter", test))]
fn runtime_i32(runtime: &KVTable, key: &str) -> Option<i32> {
    runtime_value(runtime, key)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn diagnostics_projection_accepts_the_current_broker_contract_and_uses_safe_fields_only() {
        let table = HashMap::from([
            ("sreDiagnosticsSchemaVersion", "rocketmq.broker-diagnostics.legacy"),
            ("sreDiagnosticsObservedAtMillis", "1000"),
            ("brokerConfigGeneration", "9"),
            ("brokerReady", "true"),
            ("brokerActive", "true"),
            ("brokerShutdown", "false"),
            ("brokerRegistrationAccepting", "true"),
            ("brokerRegistrationConfigured", "true"),
            ("brokerRole", "ASYNC_MASTER"),
            ("storeType", "LocalFile"),
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
        assert_eq!(
            diagnostics.config.as_ref().map(|config| config.store_type.as_str()),
            Some("LocalFile")
        );
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

    #[test]
    fn logical_broker_requests_validate_construction_and_deserialization() {
        let diagnostics = QueryBrokerDiagnosticsTargetRequest::try_new("DefaultCluster", "broker-a").unwrap();
        assert_eq!(diagnostics.cluster(), "DefaultCluster");
        assert_eq!(diagnostics.broker_name(), "broker-a");
        assert!(QueryBrokerDiagnosticsTargetRequest::try_new("DefaultCluster", "127.0.0.1:10911").is_err());
        assert!(serde_json::from_str::<QueryBrokerDiagnosticsTargetRequest>(
            r#"{"cluster":"DefaultCluster","broker_name":"broker-a","broker_addr":"secret:10911"}"#,
        )
        .is_err());

        let config = serde_json::from_str::<QueryBrokerAllowlistedConfigTargetRequest>(
            r#"{"cluster":"DefaultCluster","broker_name":"broker-a"}"#,
        )
        .unwrap();
        assert_eq!(config.cluster(), "DefaultCluster");
        assert_eq!(config.broker_name(), "broker-a");

        let log = QueryBrokerLogFilterStateTargetRequest::try_new(
            "DefaultCluster",
            "broker-a",
            "rocketmq_broker::processor::send_message",
        )
        .unwrap();
        assert_eq!(log.logger(), "rocketmq_broker::processor::send_message");
        for invalid in [
            "rocketmq_broker::",
            "rocketmq_broker::processor::",
            "rocketmq_broker::processor target",
            "rocketmq_broker::127.0.0.1:10911",
            " rocketmq_broker::processor",
        ] {
            assert!(QueryBrokerLogFilterStateTargetRequest::try_new("DefaultCluster", "broker-a", invalid).is_err());
        }
    }

    #[test]
    fn remoting_endpoint_validation_accepts_only_one_safe_host_and_port() {
        for valid in ["127.0.0.1:10911", "broker-a.internal:10911", "[2001:db8::1]:10911"] {
            assert!(is_valid_remoting_endpoint(valid), "endpoint={valid}");
        }
        for invalid in [
            "https://broker-a.internal:10911",
            "broker-a.internal:10911/path",
            "user@broker-a.internal:10911",
            "broker-a.internal:10911?token=secret",
            " broker-a.internal:10911",
            "broker-a.internal:10911 ",
            "2001:db8::1:10911",
            "broker-a.internal:0",
            "broker-a.internal:65536",
            "999.999.999.999:10911",
        ] {
            assert!(!is_valid_remoting_endpoint(invalid), "endpoint={invalid}");
        }
    }

    #[test]
    fn log_filter_contract_rejects_unbounded_targets_and_ttls() {
        assert!(SetBrokerLogFilterTtlRequest::try_new(
            "broker:10911",
            "rocketmq_broker::processor",
            BrokerLogLevel::Debug,
            60,
            "operation-1",
        )
        .is_ok());
        assert!(SetBrokerLogFilterTtlRequest::try_new(
            "broker:10911",
            "rocketmq_store::commit_log",
            BrokerLogLevel::Debug,
            60,
            "operation-1",
        )
        .is_err());
        assert!(SetBrokerLogFilterTtlRequest::try_new(
            "broker:10911",
            "rocketmq_broker::processor",
            BrokerLogLevel::Info,
            59,
            "operation-1",
        )
        .is_err());
    }

    #[test]
    fn log_filter_projection_selects_the_most_specific_target_without_exposing_raw_filter() {
        let table = HashMap::from([
            ("sreLogFilterControlSupported", "true"),
            (
                "sreLogFilterEffective",
                "info,rocketmq_broker=info,rocketmq_broker::processor=debug",
            ),
            ("sreLogFilterActiveOperationId", "operation-1"),
            ("sreLogFilterExpiresAtMillis", "123456"),
            ("secretToken", "must-not-escape"),
        ])
        .into_iter()
        .map(|(key, value)| (CheetahString::from(key), CheetahString::from(value)))
        .collect();
        let state = project_broker_log_filter_state("rocketmq_broker::processor::send".to_owned(), &KVTable { table });

        assert!(state.supported);
        assert_eq!(state.level, Some(BrokerLogLevel::Debug));
        assert_eq!(state.active_operation_id.as_deref(), Some("operation-1"));
        assert_eq!(state.expires_at_millis, Some(123456));
        let encoded = serde_json::to_string(&state).expect("state should serialize");
        assert!(!encoded.contains("sreLogFilterEffective"));
        assert!(!encoded.contains("secretToken"));
        assert!(!encoded.contains("must-not-escape"));
    }

    #[test]
    fn address_log_filter_apis_preserve_their_legacy_acceptance_contract() {
        let query = QueryBrokerLogFilterStateRequest::try_new(
            " broker.internal:10911 ",
            " rocketmq_broker::processor/legacy-target ",
        )
        .unwrap();
        assert_eq!(query.broker_addr, "broker.internal:10911");
        assert_eq!(query.logger, "rocketmq_broker::processor/legacy-target");

        let request = SetBrokerLogFilterTtlRequest::try_new(
            "broker.internal:10911",
            "rocketmq_broker::processor/legacy-target",
            BrokerLogLevel::Debug,
            60,
            "incident/123.op",
        )
        .unwrap();
        assert_eq!(request.operation_id, "incident/123.op");
    }

    #[test]
    fn legacy_log_filter_projection_preserves_bounded_operation_identifiers() {
        let table = HashMap::from([
            ("sreLogFilterControlSupported", "true"),
            ("sreLogFilterEffective", "rocketmq_broker::processor=debug"),
            ("sreLogFilterActiveOperationId", "http://secret.internal/token"),
        ])
        .into_iter()
        .map(|(key, value)| (CheetahString::from(key), CheetahString::from(value)))
        .collect();
        let state = project_broker_log_filter_state("rocketmq_broker::processor".to_owned(), &KVTable { table });

        assert_eq!(
            state.active_operation_id.as_deref(),
            Some("http://secret.internal/token")
        );
    }
}
