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

#[cfg(feature = "otel-metrics")]
use crate::DASHBOARD_METER_SCOPE;

/// Fixed storage engine labels accepted by the dashboard storage recorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardStorageEngine {
    File,
    Sqlite,
    MySql,
    Postgres,
}

#[cfg(feature = "otel-metrics")]
impl DashboardStorageEngine {
    const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Sqlite => "sqlite",
            Self::MySql => "mysql",
            Self::Postgres => "postgres",
        }
    }
}

/// Fixed availability labels accepted by the dashboard storage recorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardStorageResult {
    Available,
    Degraded,
    Unavailable,
}

/// Fixed storage operation labels accepted by the dashboard recorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardStorageOperation {
    Status,
    PersistedMutation,
    HistoryCollection,
    HistoryRetention,
    SessionAuditCleanup,
}

#[cfg(feature = "otel-metrics")]
impl DashboardStorageOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::PersistedMutation => "persisted_mutation",
            Self::HistoryCollection => "history_collection",
            Self::HistoryRetention => "history_retention",
            Self::SessionAuditCleanup => "session_audit_cleanup",
        }
    }
}

/// Fixed outcome labels accepted by the dashboard storage recorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardStorageOperationResult {
    Success,
    Failure,
}

#[cfg(feature = "otel-metrics")]
impl DashboardStorageOperationResult {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
        }
    }
}

/// Fixed failure labels that deliberately exclude backend error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardStorageFailureLabel {
    Capacity,
    Connection,
    Conflict,
    Timeout,
    Other,
}

#[cfg(feature = "otel-metrics")]
impl DashboardStorageFailureLabel {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Capacity => "capacity",
            Self::Connection => "connection",
            Self::Conflict => "conflict",
            Self::Timeout => "timeout",
            Self::Other => "other",
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl DashboardStorageResult {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Instance-owned recorder for the dashboard storage status operation.
///
/// The recorder never queries process-global telemetry state. Instruments are
/// created solely from the injected [`crate::TelemetryHandle`].
#[derive(Clone)]
pub struct DashboardStorageMetricsRecorder {
    #[cfg(feature = "otel-metrics")]
    telemetry: crate::TelemetryRecorder,
    #[cfg(feature = "otel-metrics")]
    metrics: Option<DashboardStorageMetrics>,
}

impl DashboardStorageMetricsRecorder {
    /// Creates a disabled recorder without reading global telemetry state.
    #[must_use]
    pub fn noop() -> Self {
        Self::from_handle(&crate::TelemetryHandle::noop())
    }

    /// Creates an instance-owned recorder from the injected telemetry handle.
    #[must_use]
    pub fn from_handle(handle: &crate::TelemetryHandle) -> Self {
        #[cfg(feature = "otel-metrics")]
        {
            let telemetry = handle.child(DASHBOARD_METER_SCOPE);
            let metrics = telemetry.meter().map(|meter| DashboardStorageMetrics::new(&meter));
            Self { telemetry, metrics }
        }

        #[cfg(not(feature = "otel-metrics"))]
        {
            let _ = handle;
            Self {}
        }
    }

    /// Records one status operation using only the three fixed labels.
    pub fn record_status(&self, storage_engine: DashboardStorageEngine, result: DashboardStorageResult) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.record_status(storage_engine, result);
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (storage_engine, result);
    }

    /// Records a bounded repository or background-maintenance operation.
    pub fn record_operation(
        &self,
        storage_engine: DashboardStorageEngine,
        operation: DashboardStorageOperation,
        result: DashboardStorageOperationResult,
        failure: Option<DashboardStorageFailureLabel>,
        elapsed: std::time::Duration,
    ) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.record_operation(storage_engine, operation, result, failure, elapsed);
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (storage_engine, operation, result, failure, elapsed);
    }

    /// Records safe capacity and connection-pool observations from the status view.
    pub fn record_state(
        &self,
        storage_engine: DashboardStorageEngine,
        available_bytes: Option<u64>,
        pool_size: Option<u32>,
        idle_connections: Option<usize>,
    ) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.record_state(storage_engine, available_bytes, pool_size, idle_connections);
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (storage_engine, available_bytes, pool_size, idle_connections);
    }
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
struct DashboardStorageMetrics {
    operations_total: opentelemetry::metrics::Counter<u64>,
    operation_duration_milliseconds: opentelemetry::metrics::Histogram<u64>,
    operation_errors_total: opentelemetry::metrics::Counter<u64>,
    capacity_bytes: opentelemetry::metrics::Gauge<u64>,
    pool_connections: opentelemetry::metrics::Gauge<u64>,
}

#[cfg(feature = "otel-metrics")]
impl DashboardStorageMetrics {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            operations_total: meter
                .u64_counter(crate::semantic::metrics::DASHBOARD_STORAGE_OPERATIONS_TOTAL)
                .with_description("Dashboard storage status and operation events")
                .build(),
            operation_duration_milliseconds: meter
                .u64_histogram(crate::semantic::metrics::DASHBOARD_STORAGE_OPERATION_DURATION_MILLISECONDS)
                .with_unit("ms")
                .with_description("Dashboard storage operation duration")
                .build(),
            operation_errors_total: meter
                .u64_counter(crate::semantic::metrics::DASHBOARD_STORAGE_OPERATION_ERRORS_TOTAL)
                .with_description("Dashboard storage operation failures")
                .build(),
            capacity_bytes: meter
                .u64_gauge(crate::semantic::metrics::DASHBOARD_STORAGE_CAPACITY_BYTES)
                .with_unit("By")
                .with_description("Dashboard File or SQLite safe available capacity")
                .build(),
            pool_connections: meter
                .u64_gauge(crate::semantic::metrics::DASHBOARD_STORAGE_POOL_CONNECTIONS)
                .with_description("Dashboard SQL connection pool state")
                .build(),
        }
    }

    fn record_status(&self, storage_engine: DashboardStorageEngine, result: DashboardStorageResult) {
        self.operations_total.add(
            1,
            &[
                opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_ENGINE, storage_engine.as_str()),
                opentelemetry::KeyValue::new("operation", "status"),
                opentelemetry::KeyValue::new("result", result.as_str()),
            ],
        );
    }

    fn record_operation(
        &self,
        storage_engine: DashboardStorageEngine,
        operation: DashboardStorageOperation,
        result: DashboardStorageOperationResult,
        failure: Option<DashboardStorageFailureLabel>,
        elapsed: std::time::Duration,
    ) {
        let labels = [
            opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_ENGINE, storage_engine.as_str()),
            opentelemetry::KeyValue::new("operation", operation.as_str()),
            opentelemetry::KeyValue::new("result", result.as_str()),
        ];
        self.operations_total.add(1, &labels);
        let elapsed_milliseconds = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        self.operation_duration_milliseconds
            .record(elapsed_milliseconds, &labels);
        if let Some(failure) = failure {
            self.operation_errors_total.add(
                1,
                &[
                    opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_ENGINE, storage_engine.as_str()),
                    opentelemetry::KeyValue::new("operation", operation.as_str()),
                    opentelemetry::KeyValue::new("error_kind", failure.as_str()),
                ],
            );
        }
    }

    fn record_state(
        &self,
        storage_engine: DashboardStorageEngine,
        available_bytes: Option<u64>,
        pool_size: Option<u32>,
        idle_connections: Option<usize>,
    ) {
        let storage_engine_labels = [opentelemetry::KeyValue::new(
            crate::semantic::labels::STORAGE_ENGINE,
            storage_engine.as_str(),
        )];
        if let Some(available_bytes) = available_bytes {
            self.capacity_bytes.record(available_bytes, &storage_engine_labels);
        }
        if let Some(pool_size) = pool_size {
            self.pool_connections.record(
                u64::from(pool_size),
                &[
                    opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_ENGINE, storage_engine.as_str()),
                    opentelemetry::KeyValue::new("state", "total"),
                ],
            );
        }
        if let Some(idle_connections) = idle_connections {
            self.pool_connections.record(
                u64::try_from(idle_connections).unwrap_or(u64::MAX),
                &[
                    opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_ENGINE, storage_engine.as_str()),
                    opentelemetry::KeyValue::new("state", "idle"),
                ],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DashboardStorageEngine;
    use super::DashboardStorageFailureLabel;
    use super::DashboardStorageMetricsRecorder;
    use super::DashboardStorageOperation;
    use super::DashboardStorageOperationResult;
    use super::DashboardStorageResult;

    #[test]
    fn noop_recorder_accepts_only_typed_low_cardinality_values() {
        let recorder = DashboardStorageMetricsRecorder::noop();
        recorder.record_status(DashboardStorageEngine::File, DashboardStorageResult::Available);
        recorder.record_status(DashboardStorageEngine::Postgres, DashboardStorageResult::Unavailable);
        recorder.record_operation(
            DashboardStorageEngine::Sqlite,
            DashboardStorageOperation::HistoryCollection,
            DashboardStorageOperationResult::Failure,
            Some(DashboardStorageFailureLabel::Timeout),
            std::time::Duration::from_millis(1),
        );
        recorder.record_state(DashboardStorageEngine::File, Some(1), None, None);
    }

    #[cfg(feature = "prometheus")]
    #[test]
    fn dashboard_storage_metrics_export_storage_engine_labels() {
        use crate::semantic::metrics::DASHBOARD_STORAGE_CAPACITY_BYTES;
        use crate::semantic::metrics::DASHBOARD_STORAGE_OPERATIONS_TOTAL;
        use crate::semantic::metrics::DASHBOARD_STORAGE_OPERATION_DURATION_MILLISECONDS;
        use crate::semantic::metrics::DASHBOARD_STORAGE_OPERATION_ERRORS_TOTAL;
        use crate::semantic::metrics::DASHBOARD_STORAGE_POOL_CONNECTIONS;

        let mut config = crate::ObservabilityConfig {
            enabled: true,
            ..Default::default()
        };
        config.metrics.enabled = true;
        let (provider, registry) = crate::exporter::prometheus::init_prometheus_metrics(&config)
            .unwrap()
            .into_parts();
        let handle = crate::TelemetryHandle::active(&config, Some(&provider));
        let recorder = DashboardStorageMetricsRecorder::from_handle(&handle);
        let engines = [
            (DashboardStorageEngine::File, "file"),
            (DashboardStorageEngine::Sqlite, "sqlite"),
            (DashboardStorageEngine::MySql, "mysql"),
            (DashboardStorageEngine::Postgres, "postgres"),
        ];
        for (engine, _) in engines {
            recorder.record_status(engine, DashboardStorageResult::Available);
            recorder.record_operation(
                engine,
                DashboardStorageOperation::PersistedMutation,
                DashboardStorageOperationResult::Success,
                None,
                std::time::Duration::from_millis(7),
            );
            recorder.record_operation(
                engine,
                DashboardStorageOperation::HistoryCollection,
                DashboardStorageOperationResult::Failure,
                Some(DashboardStorageFailureLabel::Timeout),
                std::time::Duration::from_millis(11),
            );
            recorder.record_state(engine, Some(4096), Some(4), Some(2));
        }

        let families = registry.gather();
        for (name, count) in [
            (DASHBOARD_STORAGE_OPERATIONS_TOTAL, 12),
            (DASHBOARD_STORAGE_OPERATION_DURATION_MILLISECONDS, 8),
            (DASHBOARD_STORAGE_OPERATION_ERRORS_TOTAL, 4),
            (DASHBOARD_STORAGE_CAPACITY_BYTES, 4),
            (DASHBOARD_STORAGE_POOL_CONNECTIONS, 8),
        ] {
            let family = families.iter().find(|family| family.name() == name).unwrap();
            assert_eq!(family.get_metric().len(), count, "{name}");
            for metric in family.get_metric() {
                let labels = metric.get_label();
                assert!(labels.iter().all(|label| label.name() != "backend"), "{name}");
                let engine_labels: Vec<_> = labels.iter().filter(|label| label.name() == "storage_engine").collect();
                assert_eq!(engine_labels.len(), 1, "{name}");
                assert!(engines.iter().any(|(_, value)| *value == engine_labels[0].value()));
            }
        }

        for (_, engine) in engines {
            let sample = |name: &str, expected: &[(&str, &str)]| {
                let family = families.iter().find(|family| family.name() == name).unwrap();
                let matches: Vec<_> = family
                    .get_metric()
                    .iter()
                    .filter(|metric| {
                        std::iter::once(("storage_engine", engine))
                            .chain(expected.iter().copied())
                            .all(|(key, value)| {
                                metric
                                    .get_label()
                                    .iter()
                                    .any(|label| label.name() == key && label.value() == value)
                            })
                    })
                    .collect();
                assert_eq!(matches.len(), 1, "{name}: {engine}, {expected:?}");
                matches[0]
            };
            assert_eq!(
                sample(
                    DASHBOARD_STORAGE_OPERATIONS_TOTAL,
                    &[("operation", "status"), ("result", "available")]
                )
                .get_counter()
                .get_value(),
                1.0
            );
            for (operation, result, elapsed) in [
                ("persisted_mutation", "success", 7.0),
                ("history_collection", "failure", 11.0),
            ] {
                let labels = [("operation", operation), ("result", result)];
                assert_eq!(
                    sample(DASHBOARD_STORAGE_OPERATIONS_TOTAL, &labels)
                        .get_counter()
                        .get_value(),
                    1.0
                );
                let histogram = sample(DASHBOARD_STORAGE_OPERATION_DURATION_MILLISECONDS, &labels).get_histogram();
                assert_eq!(histogram.get_sample_count(), 1);
                assert_eq!(histogram.get_sample_sum(), elapsed);
            }
            assert_eq!(
                sample(
                    DASHBOARD_STORAGE_OPERATION_ERRORS_TOTAL,
                    &[("operation", "history_collection"), ("error_kind", "timeout")]
                )
                .get_counter()
                .get_value(),
                1.0
            );
            assert_eq!(
                sample(DASHBOARD_STORAGE_CAPACITY_BYTES, &[]).get_gauge().get_value(),
                4096.0
            );
            for (state, count) in [("total", 4.0), ("idle", 2.0)] {
                assert_eq!(
                    sample(DASHBOARD_STORAGE_POOL_CONNECTIONS, &[("state", state)])
                        .get_gauge()
                        .get_value(),
                    count
                );
            }
        }
        provider.shutdown().unwrap();
    }
}
