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

use rocketmq_common::common::controller::ControllerConfig;
use rocketmq_rust::ArcMut;

#[cfg(feature = "metrics")]
use rocketmq_observability::config::MetricsExporter;
#[cfg(feature = "metrics")]
#[path = "controller_metrics_manager_impl.rs"]
mod owner_manager;
#[cfg(feature = "metrics")]
pub use owner_manager::ControllerMetricsConfig;
#[cfg(feature = "metrics")]
use owner_manager::ControllerMetricsManager as ObservabilityControllerMetricsManager;

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
        metrics_exporter_type: match config.metrics_exporter_type {
            rocketmq_common::common::metrics::MetricsExporterType::Disable => MetricsExporter::Disable,
            rocketmq_common::common::metrics::MetricsExporterType::OtlpGrpc => MetricsExporter::OtlpGrpc,
            rocketmq_common::common::metrics::MetricsExporterType::Prom => MetricsExporter::Prometheus,
            rocketmq_common::common::metrics::MetricsExporterType::Log => MetricsExporter::Log,
        },
        metric_logging_exporter_interval_in_mills: config.metric_logging_exporter_interval_in_mills,
        metric_grpc_exporter_interval_in_mills: config.metric_grpc_exporter_interval_in_mills,
        metric_grpc_exporter_time_out_in_mills: config.metric_grpc_exporter_time_out_in_mills,
        metrics_grpc_exporter_target: config.metrics_grpc_exporter_target.clone(),
        metrics_grpc_exporter_header: config.metrics_grpc_exporter_header.clone(),
        metrics_prom_exporter_host: config.metrics_prom_exporter_host.clone(),
        metrics_prom_exporter_port: config.metrics_prom_exporter_port,
        metrics_label: config.metrics_label.clone(),
        storage_path: config.storage_path.clone(),
        controller_store_path: config.controller_store_path.clone(),
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
    pub fn get_instance(config: ArcMut<ControllerConfig>) -> Arc<Self> {
        Self::get_instance_with_active_broker_source(config, || 0)
    }

    pub fn get_instance_with_active_broker_source<F>(
        config: ArcMut<ControllerConfig>,
        active_broker_source: F,
    ) -> Arc<Self>
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        let inner = ObservabilityControllerMetricsManager::get_instance_with_active_broker_source(
            controller_metrics_config(config.as_ref()),
            active_broker_source,
        );
        Arc::new(Self { inner })
    }

    pub fn record_role_change(new_role: i64, old_role: i64) {
        ObservabilityControllerMetricsManager::record_role_change(new_role, old_role);
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

    pub fn inc_request_total_static(request_type: &str, status: super::RequestHandleStatus) {
        ObservabilityControllerMetricsManager::inc_request_total_static(request_type, status);
    }

    pub fn record_request_latency_static(request_type: &str, latency_us: u64) {
        ObservabilityControllerMetricsManager::record_request_latency_static(request_type, latency_us);
    }

    pub fn inc_dledger_op_total_static(operation: super::DLedgerOperation, status: super::DLedgerOperationStatus) {
        ObservabilityControllerMetricsManager::inc_dledger_op_total_static(operation, status);
    }

    pub fn record_dledger_op_latency_static(operation: super::DLedgerOperation, latency_us: u64) {
        ObservabilityControllerMetricsManager::record_dledger_op_latency_static(operation, latency_us);
    }

    pub fn inc_election_total_static(result: super::ElectionResult) {
        ObservabilityControllerMetricsManager::inc_election_total_static(result);
    }
}

#[cfg(not(feature = "metrics"))]
impl ControllerMetricsManager {
    pub fn get_instance(_config: ArcMut<ControllerConfig>) -> Arc<Self> {
        Arc::new(Self)
    }

    pub fn get_instance_with_active_broker_source<F>(
        _config: ArcMut<ControllerConfig>,
        _active_broker_source: F,
    ) -> Arc<Self>
    where
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        Arc::new(Self)
    }

    pub fn record_role_change(_new_role: i64, _old_role: i64) {}

    pub fn inc_request_total(&self, _request_type: &str, _status: super::RequestHandleStatus) {}

    pub fn record_request_latency(&self, _request_type: &str, _latency_us: u64) {}

    pub fn inc_dledger_op_total(&self, _operation: super::DLedgerOperation, _status: super::DLedgerOperationStatus) {}

    pub fn record_dledger_op_latency(&self, _operation: super::DLedgerOperation, _latency_us: u64) {}

    pub fn inc_election_total(&self, _result: super::ElectionResult) {}

    pub fn inc_request_total_static(_request_type: &str, _status: super::RequestHandleStatus) {}

    pub fn record_request_latency_static(_request_type: &str, _latency_us: u64) {}

    pub fn inc_dledger_op_total_static(_operation: super::DLedgerOperation, _status: super::DLedgerOperationStatus) {}

    pub fn record_dledger_op_latency_static(_operation: super::DLedgerOperation, _latency_us: u64) {}

    pub fn inc_election_total_static(_result: super::ElectionResult) {}
}
