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

/// Fixed storage backend labels accepted by the dashboard storage recorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardStorageBackend {
    File,
    Sqlite,
    MySql,
    Postgres,
}

#[cfg(feature = "otel-metrics")]
impl DashboardStorageBackend {
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

/// Fixed error kinds that deliberately exclude backend error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardStorageErrorKind {
    Capacity,
    Connection,
    Conflict,
    Timeout,
    Other,
}

#[cfg(feature = "otel-metrics")]
impl DashboardStorageErrorKind {
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
    pub fn record_status(&self, backend: DashboardStorageBackend, result: DashboardStorageResult) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.record_status(backend, result);
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (backend, result);
    }

    /// Records a bounded repository or background-maintenance operation.
    pub fn record_operation(
        &self,
        backend: DashboardStorageBackend,
        operation: DashboardStorageOperation,
        result: DashboardStorageOperationResult,
        error_kind: Option<DashboardStorageErrorKind>,
        elapsed: std::time::Duration,
    ) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.record_operation(backend, operation, result, error_kind, elapsed);
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (backend, operation, result, error_kind, elapsed);
    }

    /// Records safe capacity and connection-pool observations from the status view.
    pub fn record_state(
        &self,
        backend: DashboardStorageBackend,
        available_bytes: Option<u64>,
        pool_size: Option<u32>,
        idle_connections: Option<usize>,
    ) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = &self.metrics {
                metrics.record_state(backend, available_bytes, pool_size, idle_connections);
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = (backend, available_bytes, pool_size, idle_connections);
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

    fn record_status(&self, backend: DashboardStorageBackend, result: DashboardStorageResult) {
        self.operations_total.add(
            1,
            &[
                opentelemetry::KeyValue::new("backend", backend.as_str()),
                opentelemetry::KeyValue::new("operation", "status"),
                opentelemetry::KeyValue::new("result", result.as_str()),
            ],
        );
    }

    fn record_operation(
        &self,
        backend: DashboardStorageBackend,
        operation: DashboardStorageOperation,
        result: DashboardStorageOperationResult,
        error_kind: Option<DashboardStorageErrorKind>,
        elapsed: std::time::Duration,
    ) {
        let labels = [
            opentelemetry::KeyValue::new("backend", backend.as_str()),
            opentelemetry::KeyValue::new("operation", operation.as_str()),
            opentelemetry::KeyValue::new("result", result.as_str()),
        ];
        self.operations_total.add(1, &labels);
        let elapsed_milliseconds = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        self.operation_duration_milliseconds
            .record(elapsed_milliseconds, &labels);
        if let Some(error_kind) = error_kind {
            self.operation_errors_total.add(
                1,
                &[
                    opentelemetry::KeyValue::new("backend", backend.as_str()),
                    opentelemetry::KeyValue::new("operation", operation.as_str()),
                    opentelemetry::KeyValue::new("error_kind", error_kind.as_str()),
                ],
            );
        }
    }

    fn record_state(
        &self,
        backend: DashboardStorageBackend,
        available_bytes: Option<u64>,
        pool_size: Option<u32>,
        idle_connections: Option<usize>,
    ) {
        let backend_label = [opentelemetry::KeyValue::new("backend", backend.as_str())];
        if let Some(available_bytes) = available_bytes {
            self.capacity_bytes.record(available_bytes, &backend_label);
        }
        if let Some(pool_size) = pool_size {
            self.pool_connections.record(
                u64::from(pool_size),
                &[
                    opentelemetry::KeyValue::new("backend", backend.as_str()),
                    opentelemetry::KeyValue::new("state", "total"),
                ],
            );
        }
        if let Some(idle_connections) = idle_connections {
            self.pool_connections.record(
                u64::try_from(idle_connections).unwrap_or(u64::MAX),
                &[
                    opentelemetry::KeyValue::new("backend", backend.as_str()),
                    opentelemetry::KeyValue::new("state", "idle"),
                ],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DashboardStorageBackend;
    use super::DashboardStorageErrorKind;
    use super::DashboardStorageMetricsRecorder;
    use super::DashboardStorageOperation;
    use super::DashboardStorageOperationResult;
    use super::DashboardStorageResult;

    #[test]
    fn noop_recorder_accepts_only_typed_low_cardinality_values() {
        let recorder = DashboardStorageMetricsRecorder::noop();
        recorder.record_status(DashboardStorageBackend::File, DashboardStorageResult::Available);
        recorder.record_status(DashboardStorageBackend::Postgres, DashboardStorageResult::Unavailable);
        recorder.record_operation(
            DashboardStorageBackend::Sqlite,
            DashboardStorageOperation::HistoryCollection,
            DashboardStorageOperationResult::Failure,
            Some(DashboardStorageErrorKind::Timeout),
            std::time::Duration::from_millis(1),
        );
        recorder.record_state(DashboardStorageBackend::File, Some(1), None, None);
    }
}
