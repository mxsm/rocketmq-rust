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

pub use crate::semantic::metrics::DELAY_MESSAGE_LATENCY;
pub use crate::semantic::metrics::STORAGE_DISPATCH_BEHIND_BYTES;
pub use crate::semantic::metrics::STORAGE_FLUSH_BEHIND_BYTES;
pub use crate::semantic::metrics::STORAGE_MESSAGE_RESERVE_TIME;
pub use crate::semantic::metrics::STORAGE_SIZE;
pub use crate::semantic::metrics::STORE_APPEND_LATENCY;
pub use crate::semantic::metrics::STORE_COMMITLOG_SEGMENT_LEASE_ACTIVE;
pub use crate::semantic::metrics::STORE_DISK_USAGE;
pub use crate::semantic::metrics::STORE_DISPATCH_LATENCY;
pub use crate::semantic::metrics::STORE_FLUSH_LATENCY;
pub use crate::semantic::metrics::STORE_HA_ACK_LATENCY_MILLIS;
pub use crate::semantic::metrics::STORE_HA_REPLICATION_LAG_BYTES;
pub use crate::semantic::metrics::STORE_LINUX_LOCKED_BYTES;
pub use crate::semantic::metrics::STORE_LINUX_MLOCK_ATTEMPT_TOTAL;
pub use crate::semantic::metrics::STORE_LINUX_MLOCK_BYTES;
pub use crate::semantic::metrics::STORE_LINUX_MLOCK_FAILURE_TOTAL;
pub use crate::semantic::metrics::STORE_LINUX_MLOCK_SKIPPED_TOTAL;
pub use crate::semantic::metrics::STORE_LINUX_MLOCK_SUCCESS_TOTAL;
pub use crate::semantic::metrics::STORE_LINUX_MUNLOCK_FAILURE_TOTAL;
pub use crate::semantic::metrics::STORE_LINUX_PAGE_CACHE_WARMUP_MILLIS;
pub use crate::semantic::metrics::STORE_LINUX_SENDFILE_BYTES_TOTAL;
pub use crate::semantic::metrics::STORE_LINUX_STORAGE_DEGRADATION_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_BATCH_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_BYTES_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_ENGINE_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_FALLBACK_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_PARTIAL_WRITE_TOTAL;

#[cfg(feature = "otel-metrics")]
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Default)]
pub struct StoreObservableValues {
    pub storage_size_bytes: i64,
    pub flush_behind_bytes: i64,
    pub dispatch_behind_bytes: i64,
    pub message_reserve_time_millis: i64,
}

/// Cloneable Store recorder bound to one explicit telemetry runtime.
#[derive(Clone)]
pub struct StoreMetricsRecorder {
    #[cfg(feature = "otel-metrics")]
    telemetry: crate::TelemetryRecorder,
    #[cfg(feature = "otel-metrics")]
    metrics: Option<StoreMetrics>,
    #[cfg(feature = "otel-metrics")]
    label_policy: crate::MetricLabelPolicy,
}

impl std::fmt::Debug for StoreMetricsRecorder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreMetricsRecorder")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl Default for StoreMetricsRecorder {
    fn default() -> Self {
        Self::noop()
    }
}

impl StoreMetricsRecorder {
    /// Creates a recorder that never reads process-global OpenTelemetry state.
    #[must_use]
    pub fn noop() -> Self {
        Self::from_handle(&crate::TelemetryHandle::noop())
    }

    /// Creates a recorder from the fixed Store instrumentation scope.
    #[must_use]
    pub fn from_handle(handle: &crate::TelemetryHandle) -> Self {
        #[cfg(feature = "otel-metrics")]
        {
            let telemetry = handle.child(crate::STORE_METER_SCOPE);
            let metrics = telemetry.meter().map(|meter| StoreMetrics::new(&meter));
            let label_policy = telemetry.metric_label_policy();
            Self {
                telemetry,
                metrics,
                label_policy,
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        {
            let _ = handle;
            Self {}
        }
    }

    /// Returns whether this recorder is backed by an active Store meter.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        #[cfg(feature = "otel-metrics")]
        {
            self.telemetry.is_active() && self.metrics.is_some()
        }

        #[cfg(not(feature = "otel-metrics"))]
        {
            false
        }
    }

    /// Registers Store observable gauges on this recorder's explicit meter.
    pub fn register_observables<F>(&self, source: F)
    where
        F: Fn() -> StoreObservableValues + Send + Sync + 'static,
    {
        #[cfg(feature = "otel-metrics")]
        if let Some(meter) = self.telemetry.meter() {
            register_store_observables(&meter, Arc::new(source));
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = source;
    }

    #[inline]
    pub fn record_append_latency(&self, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_append_latency(latency_ms, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = latency_ms;
    }

    #[inline]
    pub fn record_flush_latency(&self, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_flush_latency(latency_ms, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = latency_ms;
    }

    #[inline]
    pub fn record_dispatch_latency(&self, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_dispatch_latency(latency_ms, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = latency_ms;
    }

    #[inline]
    pub fn record_disk_usage(&self, bytes: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_disk_usage(bytes, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = bytes;
    }

    #[inline]
    pub fn record_delay_message_latency(&self, latency_seconds: u64, topic: Option<&str>) {
        #[cfg(feature = "otel-metrics")]
        if let (Some(metrics), Some(topic)) = (self.active_metrics(), topic) {
            metrics.record_delay_message_latency(
                latency_seconds,
                &delay_message_latency_attributes(&self.label_policy, topic),
            );
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (latency_seconds, topic);
    }

    #[inline]
    pub fn record_delay_message_latency_from_timestamps(
        &self,
        deliver_time_ms: i64,
        born_timestamp_ms: i64,
        topic: Option<&str>,
    ) {
        let latency_ms = deliver_time_ms.saturating_sub(born_timestamp_ms);
        if latency_ms > 0 {
            self.record_delay_message_latency((latency_ms / 1000) as u64, topic);
        }
    }

    #[inline]
    pub fn record_transfer_batch(&self, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_transfer_batch_total(count, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = count;
    }

    #[inline]
    pub fn record_transfer_bytes(&self, bytes: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_transfer_bytes_total(bytes, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = bytes;
    }

    #[inline]
    pub fn record_transfer_engine(&self, engine: &'static str, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_transfer_engine_total(count, &transfer_engine_attributes(engine));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (engine, count);
    }

    #[inline]
    pub fn record_transfer_fallback(&self, from: &'static str, to: &'static str, reason: &'static str, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_transfer_fallback_total(count, &transfer_fallback_attributes(from, to, reason));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (from, to, reason, count);
    }

    #[inline]
    pub fn record_transfer_partial_write(&self, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_transfer_partial_write_total(count, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = count;
    }

    #[inline]
    pub fn record_linux_sendfile_bytes(&self, bytes: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_sendfile_bytes_total(bytes, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = bytes;
    }

    #[inline]
    pub fn record_ha_replication_lag_bytes(&self, bytes: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_ha_replication_lag_bytes(bytes, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = bytes;
    }

    #[inline]
    pub fn record_ha_ack_latency_millis(&self, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_ha_ack_latency_millis(latency_ms, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = latency_ms;
    }

    #[inline]
    pub fn record_linux_mlock_bytes(&self, bytes: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_mlock_bytes(bytes, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = bytes;
    }

    #[inline]
    pub fn record_linux_mlock_attempt(&self, category: &'static str, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_mlock_attempt_total(count, &memory_lock_category_attributes(category));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (category, count);
    }

    #[inline]
    pub fn record_linux_mlock_success(&self, category: &'static str, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_mlock_success_total(count, &memory_lock_category_attributes(category));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (category, count);
    }

    #[inline]
    pub fn record_linux_mlock_failure(&self, category: &'static str, errno: i32, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_mlock_failure_total(count, &memory_lock_errno_attributes(category, errno));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (category, errno, count);
    }

    #[inline]
    pub fn record_linux_mlock_skipped(&self, category: &'static str, reason: &'static str, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_mlock_skipped_total(count, &memory_lock_skip_attributes(category, reason));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (category, reason, count);
    }

    #[inline]
    pub fn record_linux_locked_bytes(&self, category: &'static str, bytes: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_locked_bytes(bytes, &memory_lock_category_attributes(category));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (category, bytes);
    }

    #[inline]
    pub fn record_linux_munlock_failure(&self, category: &'static str, errno: i32, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_munlock_failure_total(count, &memory_lock_errno_attributes(category, errno));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (category, errno, count);
    }

    #[inline]
    pub fn record_linux_page_cache_warmup_millis(&self, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_page_cache_warmup_millis(latency_ms, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = latency_ms;
    }

    #[inline]
    pub fn record_linux_storage_degradation(
        &self,
        operation: &'static str,
        reason: &'static str,
        errno: i32,
        count: u64,
    ) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_linux_storage_degradation_total(
                count,
                &linux_storage_degradation_attributes(operation, reason, errno),
            );
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (operation, reason, errno, count);
    }

    #[inline]
    pub fn record_commitlog_segment_lease_active(&self, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_commitlog_segment_lease_active(count, &[]);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = count;
    }

    #[cfg(feature = "otel-metrics")]
    fn active_metrics(&self) -> Option<&StoreMetrics> {
        self.telemetry.is_active().then_some(())?;
        self.metrics.as_ref()
    }
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Default)]
pub struct StoreMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl StoreMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_append_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_flush_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_dispatch_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_disk_usage(&self, _bytes: u64) {}

    #[inline]
    pub fn record_delay_message_latency(&self, _latency_seconds: u64) {}

    #[inline]
    pub fn record_transfer_batch_total(&self, _count: u64) {}

    #[inline]
    pub fn record_transfer_bytes_total(&self, _bytes: u64) {}

    #[inline]
    pub fn record_transfer_engine_total(&self, _count: u64) {}

    #[inline]
    pub fn record_transfer_fallback_total(&self, _count: u64) {}

    #[inline]
    pub fn record_transfer_partial_write_total(&self, _count: u64) {}

    #[inline]
    pub fn record_linux_sendfile_bytes_total(&self, _bytes: u64) {}

    #[inline]
    pub fn record_ha_replication_lag_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_ha_ack_latency_millis(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_linux_mlock_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_linux_mlock_attempt_total(&self, _count: u64) {}

    #[inline]
    pub fn record_linux_mlock_success_total(&self, _count: u64) {}

    #[inline]
    pub fn record_linux_mlock_failure_total(&self, _count: u64) {}

    #[inline]
    pub fn record_linux_mlock_skipped_total(&self, _count: u64) {}

    #[inline]
    pub fn record_linux_locked_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_linux_munlock_failure_total(&self, _count: u64) {}

    #[inline]
    pub fn record_linux_page_cache_warmup_millis(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_linux_storage_degradation_total(&self, _count: u64) {}

    #[inline]
    pub fn record_commitlog_segment_lease_active(&self, _count: u64) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct StoreMetrics {
    append_latency: opentelemetry::metrics::Histogram<u64>,
    flush_latency: opentelemetry::metrics::Histogram<u64>,
    dispatch_latency: opentelemetry::metrics::Histogram<u64>,
    disk_usage: opentelemetry::metrics::Gauge<u64>,
    delay_message_latency: opentelemetry::metrics::Histogram<u64>,
    transfer_batch_total: opentelemetry::metrics::Counter<u64>,
    transfer_bytes_total: opentelemetry::metrics::Counter<u64>,
    transfer_engine_total: opentelemetry::metrics::Counter<u64>,
    transfer_fallback_total: opentelemetry::metrics::Counter<u64>,
    transfer_partial_write_total: opentelemetry::metrics::Counter<u64>,
    linux_sendfile_bytes_total: opentelemetry::metrics::Counter<u64>,
    ha_replication_lag_bytes: Option<opentelemetry::metrics::Gauge<u64>>,
    ha_ack_latency_millis: opentelemetry::metrics::Histogram<u64>,
    linux_mlock_bytes: opentelemetry::metrics::Gauge<u64>,
    linux_mlock_attempt_total: opentelemetry::metrics::Counter<u64>,
    linux_mlock_success_total: opentelemetry::metrics::Counter<u64>,
    linux_mlock_failure_total: opentelemetry::metrics::Counter<u64>,
    linux_mlock_skipped_total: opentelemetry::metrics::Counter<u64>,
    linux_locked_bytes: opentelemetry::metrics::Gauge<u64>,
    linux_munlock_failure_total: opentelemetry::metrics::Counter<u64>,
    linux_page_cache_warmup_millis: opentelemetry::metrics::Histogram<u64>,
    linux_storage_degradation_total: opentelemetry::metrics::Counter<u64>,
    commitlog_segment_lease_active: opentelemetry::metrics::Gauge<u64>,
}

#[cfg(feature = "otel-metrics")]
impl StoreMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self::new_with_ha_recording(meter, true)
    }

    fn new_with_ha_recording(meter: &opentelemetry::metrics::Meter, record_ha_replication_lag: bool) -> Self {
        let append_latency = meter
            .u64_histogram(STORE_APPEND_LATENCY)
            .with_description("Store commit log append latency")
            .with_unit("ms")
            .build();

        let flush_latency = meter
            .u64_histogram(STORE_FLUSH_LATENCY)
            .with_description("Store flush latency")
            .with_unit("ms")
            .build();

        let dispatch_latency = meter
            .u64_histogram(STORE_DISPATCH_LATENCY)
            .with_description("Store dispatch latency")
            .with_unit("ms")
            .build();

        let disk_usage = meter
            .u64_gauge(STORE_DISK_USAGE)
            .with_description("Store disk usage")
            .with_unit("By")
            .build();

        let delay_message_latency = meter
            .u64_histogram(DELAY_MESSAGE_LATENCY)
            .with_description("Timer message set latency distribution")
            .with_unit("seconds")
            .build();

        let transfer_batch_total = meter
            .u64_counter(STORE_TRANSFER_BATCH_TOTAL)
            .with_description("Total number of HA transfer batches")
            .with_unit("{batch}")
            .build();

        let transfer_bytes_total = meter
            .u64_counter(STORE_TRANSFER_BYTES_TOTAL)
            .with_description("Total HA transfer bytes")
            .with_unit("By")
            .build();

        let transfer_engine_total = meter
            .u64_counter(STORE_TRANSFER_ENGINE_TOTAL)
            .with_description("Total HA transfer engine selections")
            .with_unit("{transfer}")
            .build();

        let transfer_fallback_total = meter
            .u64_counter(STORE_TRANSFER_FALLBACK_TOTAL)
            .with_description("Total HA transfer engine fallbacks")
            .with_unit("{fallback}")
            .build();

        let transfer_partial_write_total = meter
            .u64_counter(STORE_TRANSFER_PARTIAL_WRITE_TOTAL)
            .with_description("Total HA transfer partial writes")
            .with_unit("{write}")
            .build();

        let linux_sendfile_bytes_total = meter
            .u64_counter(STORE_LINUX_SENDFILE_BYTES_TOTAL)
            .with_description("Total Linux sendfile bytes used by HA transfer")
            .with_unit("By")
            .build();

        let ha_replication_lag_bytes = record_ha_replication_lag.then(|| {
            meter
                .u64_gauge(STORE_HA_REPLICATION_LAG_BYTES)
                .with_description("HA replication lag in bytes")
                .with_unit("By")
                .build()
        });

        let ha_ack_latency_millis = meter
            .u64_histogram(STORE_HA_ACK_LATENCY_MILLIS)
            .with_description("HA replication ack latency")
            .with_unit("ms")
            .build();

        let linux_mlock_bytes = meter
            .u64_gauge(STORE_LINUX_MLOCK_BYTES)
            .with_description("Current Linux mlock bytes tracked by store")
            .with_unit("By")
            .build();

        let linux_mlock_attempt_total = meter
            .u64_counter(STORE_LINUX_MLOCK_ATTEMPT_TOTAL)
            .with_description("Total Linux mlock attempts by store category")
            .with_unit("{operation}")
            .build();

        let linux_mlock_success_total = meter
            .u64_counter(STORE_LINUX_MLOCK_SUCCESS_TOTAL)
            .with_description("Total successful Linux mlock operations by store category")
            .with_unit("{operation}")
            .build();

        let linux_mlock_failure_total = meter
            .u64_counter(STORE_LINUX_MLOCK_FAILURE_TOTAL)
            .with_description("Total failed Linux mlock operations by store category and errno")
            .with_unit("{operation}")
            .build();

        let linux_mlock_skipped_total = meter
            .u64_counter(STORE_LINUX_MLOCK_SKIPPED_TOTAL)
            .with_description("Total skipped Linux mlock operations by store category and reason")
            .with_unit("{operation}")
            .build();

        let linux_locked_bytes = meter
            .u64_gauge(STORE_LINUX_LOCKED_BYTES)
            .with_description("Current Linux locked bytes by store category")
            .with_unit("By")
            .build();

        let linux_munlock_failure_total = meter
            .u64_counter(STORE_LINUX_MUNLOCK_FAILURE_TOTAL)
            .with_description("Total failed Linux munlock operations by store category and errno")
            .with_unit("{operation}")
            .build();

        let linux_page_cache_warmup_millis = meter
            .u64_histogram(STORE_LINUX_PAGE_CACHE_WARMUP_MILLIS)
            .with_description("Linux page cache warmup latency")
            .with_unit("ms")
            .build();

        let linux_storage_degradation_total = meter
            .u64_counter(STORE_LINUX_STORAGE_DEGRADATION_TOTAL)
            .with_description("Total Linux storage lifecycle degradation events")
            .with_unit("{operation}")
            .build();

        let commitlog_segment_lease_active = meter
            .u64_gauge(STORE_COMMITLOG_SEGMENT_LEASE_ACTIVE)
            .with_description("Active commitlog segment leases")
            .with_unit("{lease}")
            .build();

        Self {
            append_latency,
            flush_latency,
            dispatch_latency,
            disk_usage,
            delay_message_latency,
            transfer_batch_total,
            transfer_bytes_total,
            transfer_engine_total,
            transfer_fallback_total,
            transfer_partial_write_total,
            linux_sendfile_bytes_total,
            ha_replication_lag_bytes,
            ha_ack_latency_millis,
            linux_mlock_bytes,
            linux_mlock_attempt_total,
            linux_mlock_success_total,
            linux_mlock_failure_total,
            linux_mlock_skipped_total,
            linux_locked_bytes,
            linux_munlock_failure_total,
            linux_page_cache_warmup_millis,
            linux_storage_degradation_total,
            commitlog_segment_lease_active,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_observables<F>(meter: &opentelemetry::metrics::Meter, source: F) -> Self
    where
        F: Fn() -> StoreObservableValues + Send + Sync + 'static,
    {
        let metrics = Self::new(meter);
        register_store_observables(meter, Arc::new(source));
        metrics
    }

    pub fn new_with_observables_and_replication_lag<F, H>(
        meter: &opentelemetry::metrics::Meter,
        source: F,
        replication_lag_source: H,
    ) -> Self
    where
        F: Fn() -> StoreObservableValues + Send + Sync + 'static,
        H: Fn() -> Option<u64> + Send + Sync + 'static,
    {
        let metrics = Self::new_with_ha_recording(meter, false);
        register_store_observables(meter, Arc::new(source));
        let _ha_replication_lag = meter
            .u64_observable_gauge(STORE_HA_REPLICATION_LAG_BYTES)
            .with_description("HA replication lag in bytes")
            .with_unit("By")
            .with_callback(move |observer| {
                let Some(replication_lag_bytes) = replication_lag_source() else {
                    return;
                };
                observer.observe(replication_lag_bytes, &store_attributes());
            })
            .build();
        metrics
    }

    #[inline]
    pub fn record_append_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.append_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_flush_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.flush_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_dispatch_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.dispatch_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_disk_usage(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.disk_usage.record(bytes, attributes);
    }

    #[inline]
    pub fn record_delay_message_latency(&self, latency_seconds: u64, attributes: &[opentelemetry::KeyValue]) {
        self.delay_message_latency.record(latency_seconds, attributes);
    }

    #[inline]
    pub fn record_transfer_batch_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_batch_total.add(count, attributes);
    }

    #[inline]
    pub fn record_transfer_bytes_total(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_bytes_total.add(bytes, attributes);
    }

    #[inline]
    pub fn record_transfer_engine_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_engine_total.add(count, attributes);
    }

    #[inline]
    pub fn record_transfer_fallback_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_fallback_total.add(count, attributes);
    }

    #[inline]
    pub fn record_transfer_partial_write_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_partial_write_total.add(count, attributes);
    }

    #[inline]
    pub fn record_linux_sendfile_bytes_total(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_sendfile_bytes_total.add(bytes, attributes);
    }

    #[inline]
    pub fn record_ha_replication_lag_bytes(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        if let Some(ha_replication_lag_bytes) = &self.ha_replication_lag_bytes {
            ha_replication_lag_bytes.record(bytes, attributes);
        }
    }

    #[inline]
    pub fn record_ha_ack_latency_millis(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.ha_ack_latency_millis.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_linux_mlock_bytes(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_mlock_bytes.record(bytes, attributes);
    }

    #[inline]
    pub fn record_linux_mlock_attempt_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_mlock_attempt_total.add(count, attributes);
    }

    #[inline]
    pub fn record_linux_mlock_success_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_mlock_success_total.add(count, attributes);
    }

    #[inline]
    pub fn record_linux_mlock_failure_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_mlock_failure_total.add(count, attributes);
    }

    #[inline]
    pub fn record_linux_mlock_skipped_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_mlock_skipped_total.add(count, attributes);
    }

    #[inline]
    pub fn record_linux_locked_bytes(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_locked_bytes.record(bytes, attributes);
    }

    #[inline]
    pub fn record_linux_munlock_failure_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_munlock_failure_total.add(count, attributes);
    }

    #[inline]
    pub fn record_linux_page_cache_warmup_millis(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_page_cache_warmup_millis.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_linux_storage_degradation_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_storage_degradation_total.add(count, attributes);
    }

    #[inline]
    pub fn record_commitlog_segment_lease_active(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.commitlog_segment_lease_active.record(count, attributes);
    }
}

#[cfg(feature = "otel-metrics")]
fn register_store_observables<F>(meter: &opentelemetry::metrics::Meter, source: Arc<F>)
where
    F: Fn() -> StoreObservableValues + Send + Sync + 'static,
{
    let storage_size_source = source.clone();
    let _storage_size = meter
        .i64_observable_gauge(STORAGE_SIZE)
        .with_description("Broker storage size")
        .with_unit("bytes")
        .with_callback(move |observer| {
            let values = storage_size_source();
            observer.observe(values.storage_size_bytes.max(0), &store_attributes());
        })
        .build();

    let flush_behind_source = source.clone();
    let _flush_behind = meter
        .i64_observable_gauge(STORAGE_FLUSH_BEHIND_BYTES)
        .with_description("Broker flush behind bytes")
        .with_unit("bytes")
        .with_callback(move |observer| {
            let values = flush_behind_source();
            observer.observe(values.flush_behind_bytes.max(0), &store_attributes());
        })
        .build();

    let dispatch_behind_source = source.clone();
    let _dispatch_behind = meter
        .i64_observable_gauge(STORAGE_DISPATCH_BEHIND_BYTES)
        .with_description("Broker dispatch behind bytes")
        .with_unit("bytes")
        .with_callback(move |observer| {
            let values = dispatch_behind_source();
            observer.observe(values.dispatch_behind_bytes.max(0), &store_attributes());
        })
        .build();

    let _message_reserve_time = meter
        .i64_observable_gauge(STORAGE_MESSAGE_RESERVE_TIME)
        .with_description("Broker message reserve time")
        .with_unit("milliseconds")
        .with_callback(move |observer| {
            let values = source();
            observer.observe(values.message_reserve_time_millis.max(0), &store_attributes());
        })
        .build();
}

#[cfg(feature = "otel-metrics")]
fn store_attributes() -> [opentelemetry::KeyValue; 2] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_TYPE, "local"),
        opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_MEDIUM, "disk"),
    ]
}

#[cfg(feature = "otel-metrics")]
fn delay_message_latency_attributes(
    label_policy: &crate::MetricLabelPolicy,
    topic: &str,
) -> Vec<opentelemetry::KeyValue> {
    let mut attrs = Vec::from(store_attributes());
    let (topic, dropped) = label_policy.normalize_metric_label_with_outcome(crate::semantic::labels::TOPIC, topic);
    attrs.push(if dropped {
        opentelemetry::KeyValue::new(crate::semantic::labels::TOPIC, crate::METRIC_LABEL_SENTINEL)
    } else {
        opentelemetry::KeyValue::new(crate::semantic::labels::TOPIC, topic.into_owned())
    });
    attrs
}

#[cfg(feature = "otel-metrics")]
fn transfer_engine_attributes(engine: &'static str) -> [opentelemetry::KeyValue; 1] {
    [opentelemetry::KeyValue::new(crate::semantic::labels::ENGINE, engine)]
}

#[cfg(feature = "otel-metrics")]
fn transfer_fallback_attributes(
    from: &'static str,
    to: &'static str,
    reason: &'static str,
) -> [opentelemetry::KeyValue; 3] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::FROM, from),
        opentelemetry::KeyValue::new(crate::semantic::labels::TO, to),
        opentelemetry::KeyValue::new(crate::semantic::labels::REASON, reason),
    ]
}

#[cfg(feature = "otel-metrics")]
fn memory_lock_category_attributes(category: &'static str) -> [opentelemetry::KeyValue; 1] {
    [opentelemetry::KeyValue::new(
        crate::semantic::labels::CATEGORY,
        category,
    )]
}

#[cfg(feature = "otel-metrics")]
fn memory_lock_errno_attributes(category: &'static str, errno: i32) -> [opentelemetry::KeyValue; 2] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::CATEGORY, category),
        opentelemetry::KeyValue::new(crate::semantic::labels::ERRNO, errno as i64),
    ]
}

#[cfg(feature = "otel-metrics")]
fn memory_lock_skip_attributes(category: &'static str, reason: &'static str) -> [opentelemetry::KeyValue; 2] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::CATEGORY, category),
        opentelemetry::KeyValue::new(crate::semantic::labels::REASON, reason),
    ]
}

#[cfg(feature = "otel-metrics")]
fn linux_storage_degradation_attributes(
    operation: &'static str,
    reason: &'static str,
    errno: i32,
) -> [opentelemetry::KeyValue; 3] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::OPERATION, operation),
        opentelemetry::KeyValue::new(crate::semantic::labels::REASON, reason),
        opentelemetry::KeyValue::new(crate::semantic::labels::ERRNO, errno as i64),
    ]
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn store_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("store-metrics-test");
        let metrics = StoreMetrics::new(&meter);
        let attrs = [opentelemetry::KeyValue::new("store", "commitlog")];

        assert!(metrics.ha_replication_lag_bytes.is_some());
        metrics.record_append_latency(5, &attrs);
        metrics.record_flush_latency(7, &attrs);
        metrics.record_dispatch_latency(9, &attrs);
        metrics.record_disk_usage(1024, &attrs);
        metrics.record_delay_message_latency(30, &attrs);
        metrics.record_transfer_batch_total(1, &[]);
        metrics.record_transfer_bytes_total(1024, &[]);
        metrics.record_transfer_engine_total(1, &transfer_engine_attributes("sendfile"));
        metrics.record_transfer_fallback_total(1, &transfer_fallback_attributes("io_uring", "vectored", "unsupported"));
        metrics.record_transfer_partial_write_total(2, &[]);
        metrics.record_linux_sendfile_bytes_total(512, &[]);
        metrics.record_ha_replication_lag_bytes(4096, &[]);
        metrics.record_ha_ack_latency_millis(12, &[]);
        metrics.record_linux_mlock_bytes(8192, &[]);
        metrics.record_linux_page_cache_warmup_millis(20, &[]);
        metrics.record_commitlog_segment_lease_active(3, &[]);
        metrics.record_linux_mlock_attempt_total(1, &memory_lock_category_attributes("transient_store_pool"));
        metrics.record_linux_mlock_success_total(1, &memory_lock_category_attributes("transient_store_pool"));
        metrics.record_linux_mlock_failure_total(1, &memory_lock_errno_attributes("commitlog_active_file", 12));
        metrics.record_linux_mlock_skipped_total(
            1,
            &memory_lock_skip_attributes("commitlog_active_window", "budget_exhausted"),
        );
        metrics.record_linux_locked_bytes(8192, &memory_lock_category_attributes("transient_store_pool"));
        metrics.record_linux_munlock_failure_total(1, &memory_lock_errno_attributes("transient_store_pool", 22));
        metrics.record_linux_storage_degradation_total(
            1,
            &linux_storage_degradation_attributes("fallocate", "unsupported", 95),
        );
    }

    #[test]
    fn store_metrics_registers_observable_gauges() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("store-observable-metrics-test");
        let metrics = StoreMetrics::new_with_observables(&meter, || StoreObservableValues {
            storage_size_bytes: 100,
            flush_behind_bytes: 10,
            dispatch_behind_bytes: 20,
            message_reserve_time_millis: 30,
        });

        metrics.record_delay_message_latency(1, &[]);
    }

    #[test]
    fn store_metrics_registers_replication_lag_as_observable_only() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("store-replication-lag-observable-test");
        let metrics =
            StoreMetrics::new_with_observables_and_replication_lag(&meter, StoreObservableValues::default, || Some(64));

        assert!(metrics.ha_replication_lag_bytes.is_none());
        metrics.record_ha_replication_lag_bytes(32, &[]);
    }

    #[test]
    fn delay_message_latency_attributes_include_real_topic() {
        let policy = crate::MetricLabelPolicy::new(1, true, true);
        let attrs = delay_message_latency_attributes(&policy, "topic-a");

        assert!(attrs
            .iter()
            .any(|kv| kv.key.as_str() == crate::semantic::labels::TOPIC && kv.value.to_string() == "topic-a"));
        let overflow = delay_message_latency_attributes(&policy, "topic-b");
        assert!(overflow.iter().any(|kv| {
            kv.key.as_str() == crate::semantic::labels::TOPIC && kv.value.to_string() == crate::METRIC_LABEL_SENTINEL
        }));
    }
}

#[cfg(test)]
mod helper_tests {
    use super::*;

    #[test]
    fn delay_message_latency_from_timestamps_ignores_non_positive_latency() {
        let recorder = StoreMetricsRecorder::noop();

        recorder.record_delay_message_latency_from_timestamps(1, 2, Some("topic-a"));
        recorder.record_delay_message_latency_from_timestamps(2, 2, Some("topic-a"));
    }

    #[test]
    fn delay_message_latency_from_timestamps_records_positive_latency() {
        StoreMetricsRecorder::noop().record_delay_message_latency_from_timestamps(2_000, 1_000, Some("topic-a"));
    }

    #[test]
    fn ha_transfer_recorders_are_safe_without_explicit_meter() {
        let recorder = StoreMetricsRecorder::noop();

        recorder.record_transfer_batch(1);
        recorder.record_transfer_bytes(1024);
        recorder.record_transfer_engine("sendfile", 1);
        recorder.record_transfer_fallback("io_uring", "vectored", "unsupported", 1);
        recorder.record_transfer_partial_write(2);
        recorder.record_linux_sendfile_bytes(512);
        recorder.record_ha_replication_lag_bytes(4096);
        recorder.record_ha_ack_latency_millis(12);
        recorder.record_linux_mlock_bytes(8192);
        recorder.record_linux_page_cache_warmup_millis(20);
        recorder.record_commitlog_segment_lease_active(3);
        recorder.record_linux_mlock_attempt("transient_store_pool", 1);
        recorder.record_linux_mlock_success("transient_store_pool", 1);
        recorder.record_linux_mlock_failure("commitlog_active_file", 12, 1);
        recorder.record_linux_mlock_skipped("commitlog_active_window", "budget_exhausted", 1);
        recorder.record_linux_locked_bytes("transient_store_pool", 8192);
        recorder.record_linux_munlock_failure("transient_store_pool", 22, 1);
        recorder.record_linux_storage_degradation("fallocate", "unsupported", 95, 1);
    }
}
