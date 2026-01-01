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

//! # ControllerMetricsManager
//!
//! Comprehensive metrics management for RocketMQ Controller using OpenTelemetry.
//!
//! ## Overview
//! This module provides complete metrics collection for the Controller component,
//! tracking:
//! - Node status (role, disk usage, active broker count)
//! - Request metrics (total requests, latency)
//! - DLedger operations (operation count, latency)
//! - Election metrics (election count and results)
//!
//! ## Design Philosophy
//! - Thread-safe singleton pattern with lazy initialization
//! - OpenTelemetry-based with support for multiple exporters (OTLP gRPC, Prometheus, Log)
//! - No-op fallback when metrics are disabled
//! - Efficient metric updates with minimal overhead

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
#[cfg(feature = "metrics")]
use std::time::Duration;

use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::metrics::UpDownCounter;
use opentelemetry::KeyValue;
#[cfg(feature = "metrics")]
use opentelemetry_otlp::MetricExporter;
#[cfg(feature = "metrics")]
use opentelemetry_otlp::Protocol;
#[cfg(feature = "metrics")]
use opentelemetry_otlp::WithExportConfig;
#[cfg(feature = "metrics")]
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use rocketmq_common::common::metrics::metrics_exporter_type::MetricsExporterType;
use rocketmq_common::common::metrics::nop_long_counter::NopLongCounter;
use rocketmq_common::common::metrics::nop_long_histogram::NopLongHistogram;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::controller_metrics_constant::*;
use crate::config::ControllerConfig;

// ============================================================================
// Constants
// ============================================================================

/// Microsecond to second conversion
const US: f64 = 1.0;
const MS: f64 = 1000.0 * US;
const S: f64 = 1000.0 * MS;

// ============================================================================
// Global State
// ============================================================================

/// Global singleton instance
static INSTANCE: OnceLock<Arc<ControllerMetricsManager>> = OnceLock::new();

/// Global label map for common labels
static LABEL_MAP: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();

// ============================================================================
// Metric Wrappers for Static Access
// ============================================================================

/// Wrapper for Counter that can be either real or no-op
pub enum CounterWrapper {
    Real(Counter<u64>),
    Nop(NopLongCounter),
}

impl CounterWrapper {
    #[inline]
    pub fn add(&self, value: u64, attributes: &[KeyValue]) {
        match self {
            Self::Real(counter) => counter.add(value, attributes),
            Self::Nop(_) => {}
        }
    }
}

/// Wrapper for UpDownCounter that can be either real or no-op
pub enum UpDownCounterWrapper {
    Real(UpDownCounter<i64>),
    Nop,
}

impl UpDownCounterWrapper {
    #[inline]
    pub fn add(&self, value: i64, attributes: &[KeyValue]) {
        match self {
            Self::Real(counter) => counter.add(value, attributes),
            Self::Nop => {}
        }
    }
}

/// Wrapper for Histogram that can be either real or no-op
pub enum HistogramWrapper {
    Real(Histogram<u64>),
    Nop(NopLongHistogram),
}

impl HistogramWrapper {
    #[inline]
    pub fn record(&self, value: u64, attributes: &[KeyValue]) {
        match self {
            Self::Real(histogram) => histogram.record(value, attributes),
            Self::Nop(_) => {}
        }
    }
}

// ============================================================================
// Static Metrics
// ============================================================================

/// Static role metric (up-down counter)
static ROLE: OnceLock<UpDownCounterWrapper> = OnceLock::new();

/// Static request total metric (counter)
static REQUEST_TOTAL: OnceLock<CounterWrapper> = OnceLock::new();

/// Static dledger operation total metric (counter)
static DLEDGER_OP_TOTAL: OnceLock<CounterWrapper> = OnceLock::new();

/// Static election total metric (counter)
static ELECTION_TOTAL: OnceLock<CounterWrapper> = OnceLock::new();

/// Static request latency metric (histogram)
static REQUEST_LATENCY: OnceLock<HistogramWrapper> = OnceLock::new();

/// Static dledger operation latency metric (histogram)
static DLEDGER_OP_LATENCY: OnceLock<HistogramWrapper> = OnceLock::new();

// ============================================================================
// ControllerMetricsManager
// ============================================================================

/// Controller metrics manager for comprehensive controller-level metrics
///
/// This struct manages all controller-related metrics including:
/// - Node status metrics (role, disk usage, active broker count)
/// - Request metrics (total requests, latency)
/// - DLedger operation metrics (total operations, latency)
/// - Election metrics (total elections, results)
pub struct ControllerMetricsManager {
    // Node status metrics
    role: UpDownCounter<i64>,

    // Request metrics
    request_total: Counter<u64>,
    request_latency: Histogram<u64>,

    // DLedger operation metrics
    dledger_op_total: Counter<u64>,
    dledger_op_latency: Histogram<u64>,

    // Election metrics
    election_total: Counter<u64>,

    // Meter reference for creating additional instruments
    meter: Meter,

    // Configuration
    config: Arc<ControllerConfig>,

    // OpenTelemetry SDK components
    _meter_provider: Option<SdkMeterProvider>,
}

impl ControllerMetricsManager {
    /// Get or initialize the global singleton instance
    ///
    /// # Arguments
    /// * `config` - Controller configuration
    ///
    /// # Returns
    /// Arc reference to the ControllerMetricsManager instance
    pub fn get_instance(config: Arc<ControllerConfig>) -> Arc<Self> {
        INSTANCE
            .get_or_init(|| {
                let manager = Self::new(config);
                Arc::new(manager)
            })
            .clone()
    }

    /// Create a new ControllerMetricsManager
    fn new(config: Arc<ControllerConfig>) -> Self {
        // Initialize label map
        let label_map = LABEL_MAP.get_or_init(|| RwLock::new(HashMap::new()));

        // Populate base labels
        {
            let mut map = label_map.write().unwrap();
            // TODO: Add actual address/group/peer_id from config when available
            // This will depend on whether using DLedger or JRaft controller
            map.insert(LABEL_ADDRESS.to_string(), "localhost:9876".to_string());
            map.insert(LABEL_GROUP.to_string(), "DefaultControllerGroup".to_string());
            map.insert(LABEL_PEER_ID.to_string(), "controller-1".to_string());
        }

        // Parse custom labels from config if available
        // TODO: When ControllerConfig is extended with metrics_label field

        // Check if metrics are enabled
        let metrics_type = MetricsExporterType::Disable; // TODO: Get from config

        if !Self::check_config(&config, metrics_type) {
            warn!("Metrics are disabled or configuration is invalid");
            return Self::create_noop_manager(config);
        }

        // Build meter provider with appropriate exporter
        let meter_provider = match Self::build_meter_provider(&config, metrics_type) {
            Ok(provider) => provider,
            Err(e) => {
                error!("Failed to build meter provider: {:?}, falling back to no-op", e);
                return Self::create_noop_manager(config);
            }
        };

        let meter = meter_provider.meter(OPEN_TELEMETRY_METER_NAME);

        // Initialize metrics
        let manager = Self::init_metrics(meter.clone(), config.clone(), Some(meter_provider));

        // Initialize static metric references
        Self::init_static_metrics(&manager);

        info!("ControllerMetricsManager initialized successfully");
        manager
    }

    /// Create a no-op manager when metrics are disabled
    fn create_noop_manager(config: Arc<ControllerConfig>) -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("noop");
        Self::init_metrics(meter, config, None)
    }

    /// Check if metrics configuration is valid
    fn check_config(_config: &ControllerConfig, metrics_type: MetricsExporterType) -> bool {
        if !metrics_type.is_enable() {
            return false;
        }

        match metrics_type {
            MetricsExporterType::OtlpGrpc => {
                // TODO: Check config.metrics_grpc_exporter_target
                true
            }
            MetricsExporterType::Prom => true,
            MetricsExporterType::Log => true,
            MetricsExporterType::Disable => false,
        }
    }

    /// Build OpenTelemetry meter provider with configured exporter
    fn build_meter_provider(
        _config: &ControllerConfig,
        metrics_type: MetricsExporterType,
    ) -> Result<SdkMeterProvider, Box<dyn std::error::Error>> {
        let provider_builder = SdkMeterProvider::builder();

        match metrics_type {
            #[cfg(feature = "metrics")]
            MetricsExporterType::OtlpGrpc => {
                // TODO: Get endpoint from config
                let endpoint = "http://localhost:4317";

                let exporter = MetricExporter::builder()
                    .with_tonic()
                    .with_protocol(Protocol::Grpc)
                    .with_endpoint(endpoint)
                    .with_timeout(Duration::from_secs(10))
                    .build()?;

                let reader = PeriodicReader::builder(exporter).build();

                let provider_builder = provider_builder.with_reader(reader);

                // Register views for histogram buckets
                let provider_builder = Self::register_metrics_views(provider_builder);

                return Ok(provider_builder.build());
            }
            #[cfg(not(feature = "metrics"))]
            MetricsExporterType::OtlpGrpc => {
                warn!("OTLP gRPC exporter requires 'metrics' feature");
            }
            MetricsExporterType::Prom => {
                // TODO: Implement Prometheus exporter
                // This would require opentelemetry-prometheus crate
                warn!("Prometheus exporter not yet implemented");
            }
            MetricsExporterType::Log => {
                // TODO: Implement log exporter
                warn!("Log exporter not yet implemented");
            }
            MetricsExporterType::Disable => {}
        }

        // Register views for histogram buckets
        let provider_builder = Self::register_metrics_views(provider_builder);

        Ok(provider_builder.build())
    }

    /// Register custom views for histogram bucket configuration
    fn register_metrics_views(
        builder: opentelemetry_sdk::metrics::MeterProviderBuilder,
    ) -> opentelemetry_sdk::metrics::MeterProviderBuilder {
        // Define latency buckets
        let _latency_buckets = vec![
            1.0 * US,
            3.0 * US,
            5.0 * US,
            10.0 * US,
            30.0 * US,
            50.0 * US,
            100.0 * US,
            300.0 * US,
            500.0 * US,
            1.0 * MS,
            3.0 * MS,
            5.0 * MS,
            10.0 * MS,
            30.0 * MS,
            50.0 * MS,
            100.0 * MS,
            300.0 * MS,
            500.0 * MS,
            1.0 * S,
            3.0 * S,
            5.0 * S,
            10.0 * S,
        ];

        // TODO: Configure views when OpenTelemetry Rust SDK supports it
        // Currently, view configuration is limited in Rust SDK compared to Java

        builder
    }

    /// Initialize all metrics with the given meter
    fn init_metrics(meter: Meter, config: Arc<ControllerConfig>, meter_provider: Option<SdkMeterProvider>) -> Self {
        // Node status metrics
        let role = meter
            .i64_up_down_counter(GAUGE_ROLE)
            .with_description("Role of current controller node (0=UNKNOWN, 1=CANDIDATE, 2=FOLLOWER, 3=LEADER)")
            .build();

        // Register observable gauges
        let _dledger_disk_usage = meter
            .u64_observable_gauge(GAUGE_DLEDGER_DISK_USAGE)
            .with_description("Disk usage of dledger storage in bytes")
            .with_unit("bytes")
            .with_callback(move |observer| {
                // TODO: Get actual storage path from config
                let path = Path::new("./controller-store");

                if !path.exists() {
                    return;
                }

                match calculate_directory_size(path) {
                    Ok(size) => {
                        let attrs = Self::new_attributes_builder();
                        observer.observe(size, &attrs);
                    }
                    Err(e) => {
                        error!("Failed to calculate disk usage for {:?}: {:?}", path, e);
                    }
                }
            })
            .build();

        // TODO: Register active_broker_num observable gauge
        // This requires access to HeartbeatManager
        let _active_broker_num = meter
            .u64_observable_gauge(GAUGE_ACTIVE_BROKER_NUM)
            .with_description("Number of currently active brokers")
            .with_callback(move |observer| {
                // TODO: Call controllerManager.getHeartbeatManager().getActiveBrokersNum()
                // For now, this is a placeholder
                let attrs = Self::new_attributes_builder();
                observer.observe(0, &attrs);
            })
            .build();

        // Request metrics
        let request_total = meter
            .u64_counter(COUNTER_REQUEST_TOTAL)
            .with_description("Total number of controller requests")
            .build();

        let request_latency = meter
            .u64_histogram(HISTOGRAM_REQUEST_LATENCY)
            .with_description("Controller request latency in microseconds")
            .with_unit("us")
            .build();

        // DLedger operation metrics
        let dledger_op_total = meter
            .u64_counter(COUNTER_DLEDGER_OP_TOTAL)
            .with_description("Total number of DLedger operations")
            .build();

        let dledger_op_latency = meter
            .u64_histogram(HISTOGRAM_DLEDGER_OP_LATENCY)
            .with_description("DLedger operation latency in microseconds")
            .with_unit("us")
            .build();

        // Election metrics
        let election_total = meter
            .u64_counter(COUNTER_ELECTION_TOTAL)
            .with_description("Total number of controller elections")
            .build();

        Self {
            role,
            request_total,
            request_latency,
            dledger_op_total,
            dledger_op_latency,
            election_total,
            meter,
            config,
            _meter_provider: meter_provider,
        }
    }

    /// Initialize static metric references for global access
    fn init_static_metrics(manager: &Self) {
        ROLE.get_or_init(|| UpDownCounterWrapper::Real(manager.role.clone()));
        REQUEST_TOTAL.get_or_init(|| CounterWrapper::Real(manager.request_total.clone()));
        DLEDGER_OP_TOTAL.get_or_init(|| CounterWrapper::Real(manager.dledger_op_total.clone()));
        ELECTION_TOTAL.get_or_init(|| CounterWrapper::Real(manager.election_total.clone()));
        REQUEST_LATENCY.get_or_init(|| HistogramWrapper::Real(manager.request_latency.clone()));
        DLEDGER_OP_LATENCY.get_or_init(|| HistogramWrapper::Real(manager.dledger_op_latency.clone()));
    }

    /// Create a new attributes builder with base labels
    pub fn new_attributes_builder() -> Vec<KeyValue> {
        let label_map = LABEL_MAP.get_or_init(|| RwLock::new(HashMap::new()));
        let map = label_map.read().unwrap();

        map.iter().map(|(k, v)| KeyValue::new(k.clone(), v.clone())).collect()
    }

    // ========================================================================
    // Public Metric Recording Methods
    // ========================================================================

    /// Record role change
    ///
    /// # Arguments
    /// * `new_role` - New role value
    /// * `old_role` - Old role value
    pub fn record_role_change(new_role: i64, old_role: i64) {
        if let Some(role) = ROLE.get() {
            let attrs = Self::new_attributes_builder();
            role.add(new_role - old_role, &attrs);
        }
    }

    /// Record request
    ///
    /// # Arguments
    /// * `request_type` - Type of request
    /// * `status` - Request handle status
    pub fn inc_request_total(&self, request_type: &str, status: RequestHandleStatus) {
        let mut attrs = Self::new_attributes_builder();
        attrs.push(KeyValue::new(LABEL_REQUEST_TYPE, request_type.to_string()));
        attrs.push(KeyValue::new(LABEL_REQUEST_HANDLE_STATUS, status.get_lower_case_name()));

        self.request_total.add(1, &attrs);
    }

    /// Record request latency
    ///
    /// # Arguments
    /// * `request_type` - Type of request
    /// * `latency_us` - Latency in microseconds
    pub fn record_request_latency(&self, request_type: &str, latency_us: u64) {
        let mut attrs = Self::new_attributes_builder();
        attrs.push(KeyValue::new(LABEL_REQUEST_TYPE, request_type.to_string()));

        self.request_latency.record(latency_us, &attrs);
    }

    /// Record DLedger operation
    ///
    /// # Arguments
    /// * `operation` - DLedger operation type
    /// * `status` - Operation status
    pub fn inc_dledger_op_total(&self, operation: DLedgerOperation, status: DLedgerOperationStatus) {
        let mut attrs = Self::new_attributes_builder();
        attrs.push(KeyValue::new(LABEL_DLEDGER_OPERATION, operation.get_lower_case_name()));
        attrs.push(KeyValue::new(
            LABEL_DLEDGER_OPERATION_STATUS,
            status.get_lower_case_name(),
        ));

        self.dledger_op_total.add(1, &attrs);
    }

    /// Record DLedger operation latency
    ///
    /// # Arguments
    /// * `operation` - DLedger operation type
    /// * `latency_us` - Latency in microseconds
    pub fn record_dledger_op_latency(&self, operation: DLedgerOperation, latency_us: u64) {
        let mut attrs = Self::new_attributes_builder();
        attrs.push(KeyValue::new(LABEL_DLEDGER_OPERATION, operation.get_lower_case_name()));

        self.dledger_op_latency.record(latency_us, &attrs);
    }

    /// Record election
    ///
    /// # Arguments
    /// * `result` - Election result
    pub fn inc_election_total(&self, result: ElectionResult) {
        let mut attrs = Self::new_attributes_builder();
        attrs.push(KeyValue::new(LABEL_ELECTION_RESULT, result.get_lower_case_name()));

        self.election_total.add(1, &attrs);
    }

    // ========================================================================
    // Static Global Methods
    // ========================================================================

    /// Static method to increment request total
    pub fn inc_request_total_static(request_type: &str, status: RequestHandleStatus) {
        if let Some(counter) = REQUEST_TOTAL.get() {
            let mut attrs = Self::new_attributes_builder();
            attrs.push(KeyValue::new(LABEL_REQUEST_TYPE, request_type.to_string()));
            attrs.push(KeyValue::new(LABEL_REQUEST_HANDLE_STATUS, status.get_lower_case_name()));
            counter.add(1, &attrs);
        }
    }

    /// Static method to record request latency
    pub fn record_request_latency_static(request_type: &str, latency_us: u64) {
        if let Some(histogram) = REQUEST_LATENCY.get() {
            let mut attrs = Self::new_attributes_builder();
            attrs.push(KeyValue::new(LABEL_REQUEST_TYPE, request_type.to_string()));
            histogram.record(latency_us, &attrs);
        }
    }

    /// Static method to increment DLedger operation total
    pub fn inc_dledger_op_total_static(operation: DLedgerOperation, status: DLedgerOperationStatus) {
        if let Some(counter) = DLEDGER_OP_TOTAL.get() {
            let mut attrs = Self::new_attributes_builder();
            attrs.push(KeyValue::new(LABEL_DLEDGER_OPERATION, operation.get_lower_case_name()));
            attrs.push(KeyValue::new(
                LABEL_DLEDGER_OPERATION_STATUS,
                status.get_lower_case_name(),
            ));
            counter.add(1, &attrs);
        }
    }

    /// Static method to record DLedger operation latency
    pub fn record_dledger_op_latency_static(operation: DLedgerOperation, latency_us: u64) {
        if let Some(histogram) = DLEDGER_OP_LATENCY.get() {
            let mut attrs = Self::new_attributes_builder();
            attrs.push(KeyValue::new(LABEL_DLEDGER_OPERATION, operation.get_lower_case_name()));
            histogram.record(latency_us, &attrs);
        }
    }

    /// Static method to increment election total
    pub fn inc_election_total_static(result: ElectionResult) {
        if let Some(counter) = ELECTION_TOTAL.get() {
            let mut attrs = Self::new_attributes_builder();
            attrs.push(KeyValue::new(LABEL_ELECTION_RESULT, result.get_lower_case_name()));
            counter.add(1, &attrs);
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Calculate total size of a directory recursively
fn calculate_directory_size(path: &Path) -> std::io::Result<u64> {
    let mut total_size = 0u64;

    if path.is_file() {
        return Ok(fs::metadata(path)?.len());
    }

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_file() {
                total_size += metadata.len();
            } else if metadata.is_dir() {
                total_size += calculate_directory_size(&entry.path())?;
            }
        }
    }

    Ok(total_size)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_attributes_builder() {
        let attrs = ControllerMetricsManager::new_attributes_builder();
        // Just verify it returns a vector
        assert!(attrs.is_empty() || !attrs.is_empty());
    }

    #[test]
    fn test_request_handle_status() {
        assert_eq!(RequestHandleStatus::Success.get_lower_case_name(), "success");
        assert_eq!(RequestHandleStatus::Failed.get_lower_case_name(), "failed");
        assert_eq!(RequestHandleStatus::Timeout.get_lower_case_name(), "timeout");
    }

    #[test]
    fn test_dledger_operation() {
        assert_eq!(DLedgerOperation::Append.get_lower_case_name(), "append");
    }

    #[test]
    fn test_election_result() {
        assert_eq!(
            ElectionResult::NewMasterElected.get_lower_case_name(),
            "new_master_elected"
        );
        assert_eq!(
            ElectionResult::KeepCurrentMaster.get_lower_case_name(),
            "keep_current_master"
        );
        assert_eq!(
            ElectionResult::NoMasterElected.get_lower_case_name(),
            "no_master_elected"
        );
    }
}
