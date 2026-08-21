// Copyright 2023 The RocketMQ Rust Authors
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

use std::sync::Arc;

#[cfg(feature = "metrics")]
use std::collections::HashMap;

#[cfg(feature = "metrics")]
use crate::config::ControllerConfig;

use crate::config::ControllerConfigReader;

#[cfg(feature = "metrics")]
pub use rocketmq_observability::metrics::controller_manager::ControllerMetricsConfig;
#[cfg(feature = "metrics")]
use rocketmq_observability::metrics::controller_manager::ControllerMetricsManager as ObservabilityControllerMetricsManager;
use rocketmq_observability::TelemetryHandle;

#[cfg(feature = "metrics")]
pub struct ControllerMetricsManager {
    inner: Arc<ObservabilityControllerMetricsManager>,
}

#[cfg(not(feature = "metrics"))]
pub struct ControllerMetricsManager;

#[cfg(feature = "metrics")]
pub(crate) fn controller_metrics_config(config: &ControllerConfig) -> ControllerMetricsConfig {
    ControllerMetricsConfig {
        listen_addr: config.listen_addr.to_string(),
        controller_type: config.controller_type.clone(),
        node_id: config.node_id.to_string(),
        metrics_label: String::new(),
        storage_path: config.storage_path.clone(),
        controller_store_path: config.controller_store_path.clone(),
        cardinality_limit: 10_000,
    }
}

#[cfg(feature = "metrics")]
pub(crate) fn active_broker_count_from_snapshot(snapshot: &HashMap<String, HashMap<String, u32>>) -> u64 {
    snapshot
        .values()
        .flat_map(|broker_sets| broker_sets.values())
        .map(|count| u64::from(*count))
        .sum()
}

#[cfg(feature = "metrics")]
impl ControllerMetricsManager {
    pub fn new(config: ControllerConfigReader, telemetry_handle: &TelemetryHandle) -> Arc<Self> {
        Self::new_with_active_broker_source(config, telemetry_handle, || 0)
    }

    pub fn new_with_active_broker_source<F>(
        config: ControllerConfigReader,
        telemetry_handle: &TelemetryHandle,
        active_broker_source: F,
    ) -> Arc<Self>
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        let snapshot = config.snapshot();
        let inner = ObservabilityControllerMetricsManager::new(
            controller_metrics_config(&snapshot),
            telemetry_handle,
            active_broker_source,
        );
        Arc::new(Self { inner })
    }

    pub fn record_role_change(&self, new_role: i64, old_role: i64) {
        self.inner.record_role_change(new_role, old_role);
    }

    pub fn inc_request_total(&self, request_type: &str, status: super::RequestHandleStatus) {
        self.inner.inc_request_total(request_type, status);
    }

    pub fn record_request_latency(&self, request_type: &str, latency_us: u64) {
        self.inner.record_request_latency(request_type, latency_us);
    }

    pub fn inc_dledger_op_total(&self, operation: super::DLedgerOperation, status: super::DLedgerOperationStatus) {
        self.inner.inc_dledger_op_total(operation, status);
    }

    pub fn record_dledger_op_latency(&self, operation: super::DLedgerOperation, latency_us: u64) {
        self.inner.record_dledger_op_latency(operation, latency_us);
    }

    pub fn inc_election_total(&self, result: super::ElectionResult) {
        self.inner.inc_election_total(result);
    }

    pub fn record_election_latency(&self, latency_ms: u64) {
        self.inner.record_election_latency(latency_ms);
    }

    pub fn record_election_attempt(&self, latency_ms: u64) {
        self.inner.record_election_attempt(latency_ms);
    }

    pub fn record_quorum_health(&self, healthy: bool) {
        self.inner.record_quorum_health(healthy);
    }

    pub fn record_heartbeat_age(&self, age_ms: u64) {
        self.inner.record_heartbeat_age(age_ms);
    }

    pub fn record_stale_brokers(&self, count: u64) {
        self.inner.record_stale_brokers(count);
    }
}

#[cfg(not(feature = "metrics"))]
impl ControllerMetricsManager {
    pub fn new(_config: ControllerConfigReader, _telemetry_handle: &TelemetryHandle) -> Arc<Self> {
        Arc::new(Self)
    }

    pub fn new_with_active_broker_source<F>(
        _config: ControllerConfigReader,
        _telemetry_handle: &TelemetryHandle,
        _active_broker_source: F,
    ) -> Arc<Self>
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        Arc::new(Self)
    }

    pub fn record_role_change(&self, _new_role: i64, _old_role: i64) {}

    pub fn inc_request_total(&self, _request_type: &str, _status: super::RequestHandleStatus) {}

    pub fn record_request_latency(&self, _request_type: &str, _latency_us: u64) {}

    pub fn inc_dledger_op_total(&self, _operation: super::DLedgerOperation, _status: super::DLedgerOperationStatus) {}

    pub fn record_dledger_op_latency(&self, _operation: super::DLedgerOperation, _latency_us: u64) {}

    pub fn inc_election_total(&self, _result: super::ElectionResult) {}

    pub fn record_election_latency(&self, _latency_ms: u64) {}

    pub fn record_election_attempt(&self, _latency_ms: u64) {}

    pub fn record_quorum_health(&self, _healthy: bool) {}

    pub fn record_heartbeat_age(&self, _age_ms: u64) {}

    pub fn record_stale_brokers(&self, _count: u64) {}
}

#[cfg(test)]
mod tests {
    #[test]
    fn controller_metrics_api_has_no_static_recording_facade() {
        let source = include_str!("controller_metrics_manager.rs");

        for forbidden in [
            concat!("get_", "instance"),
            concat!("_static", "("),
            concat!("pub fn record_role_change(", "new_role"),
        ] {
            assert!(
                !source.contains(forbidden),
                "Controller metrics API must be instance-scoped: {forbidden}"
            );
        }
    }
}
