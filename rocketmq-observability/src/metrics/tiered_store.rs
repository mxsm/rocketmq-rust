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

pub use crate::semantic::metrics::TIERED_STORE_API_LATENCY;
pub use crate::semantic::metrics::TIERED_STORE_DISPATCH_BEHIND;
pub use crate::semantic::metrics::TIERED_STORE_DISPATCH_LATENCY;
pub use crate::semantic::metrics::TIERED_STORE_GET_MESSAGE_FALLBACK_TOTAL;
pub use crate::semantic::metrics::TIERED_STORE_MESSAGES_DISPATCH_TOTAL;
pub use crate::semantic::metrics::TIERED_STORE_MESSAGES_OUT_TOTAL;
pub use crate::semantic::metrics::TIERED_STORE_PROVIDER_DOWNLOAD_BYTES;
pub use crate::semantic::metrics::TIERED_STORE_PROVIDER_RPC_LATENCY;
pub use crate::semantic::metrics::TIERED_STORE_PROVIDER_UPLOAD_BYTES;
pub use crate::semantic::metrics::TIERED_STORE_READ_AHEAD_CACHE_ACCESS_TOTAL;
pub use crate::semantic::metrics::TIERED_STORE_READ_AHEAD_CACHE_BYTES;
pub use crate::semantic::metrics::TIERED_STORE_READ_AHEAD_CACHE_COUNT;
pub use crate::semantic::metrics::TIERED_STORE_READ_AHEAD_CACHE_HIT_TOTAL;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Default)]
pub struct TieredStoreObservableValues {
    pub dispatch_behind: Vec<TieredDispatchBehind>,
    pub read_ahead_cache_count: i64,
    pub read_ahead_cache_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieredDispatchBehind {
    pub topic: String,
    pub queue_id: i32,
    pub file_type: String,
    pub count: i64,
}

/// Cloneable Tiered Store recorder bound to one explicit tiered-store telemetry scope.
#[derive(Clone)]
pub struct TieredStoreMetricsRecorder {
    #[cfg(feature = "otel-metrics")]
    telemetry: crate::TelemetryRecorder,
    #[cfg(feature = "otel-metrics")]
    metrics: Option<TieredStoreOtelMetrics>,
}

impl std::fmt::Debug for TieredStoreMetricsRecorder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TieredStoreMetricsRecorder")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl Default for TieredStoreMetricsRecorder {
    fn default() -> Self {
        Self::noop()
    }
}

impl TieredStoreMetricsRecorder {
    /// Creates a recorder that never reads process-global OpenTelemetry state.
    #[must_use]
    pub fn noop() -> Self {
        Self::from_handle(&crate::TelemetryHandle::noop())
    }

    /// Creates a recorder from the fixed Tiered Store instrumentation scope.
    #[must_use]
    pub fn from_handle(handle: &crate::TelemetryHandle) -> Self {
        #[cfg(feature = "otel-metrics")]
        {
            let telemetry = handle.child(crate::TIERED_STORE_METER_SCOPE);
            let metrics = telemetry
                .meter()
                .map(|meter| TieredStoreOtelMetrics::new_with_label_policy(&meter, telemetry.metric_label_policy()));
            Self { telemetry, metrics }
        }

        #[cfg(not(feature = "otel-metrics"))]
        {
            let _ = handle;
            Self {}
        }
    }

    /// Returns whether this recorder is backed by an active Tiered Store meter.
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

    /// Registers Tiered Store observable gauges on this recorder's explicit meter.
    pub fn register_observables<F>(&self, source: F)
    where
        F: Fn() -> TieredStoreObservableValues + Send + Sync + 'static,
    {
        #[cfg(feature = "otel-metrics")]
        if let Some(meter) = self.telemetry.meter() {
            TieredStoreOtelMetrics::register_observables_with_label_policy(
                &meter,
                source,
                self.telemetry.metric_label_policy(),
            );
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = source;
    }

    #[cfg(feature = "otel-metrics")]
    fn active_metrics(&self) -> Option<&TieredStoreOtelMetrics> {
        self.telemetry.is_active().then_some(())?;
        self.metrics.as_ref()
    }

    pub fn record_messages_dispatch(&self, topic: &str, queue_id: i32, file_type: &str, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_messages_dispatch(
                count,
                &dispatch_attributes(&metrics.label_policy, topic, queue_id, file_type),
            );
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (topic, queue_id, file_type, count);
    }

    pub fn record_messages_out(&self, topic: &str, group: &str, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_messages_out(count, &message_out_attributes(&metrics.label_policy, topic, group));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (topic, group, count);
    }

    pub fn record_get_message_fallback(&self, topic: &str, group: &str) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_get_message_fallback(1, &message_out_attributes(&metrics.label_policy, topic, group));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (topic, group);
    }

    pub fn record_provider_read(&self, path: &str, bytes: u64, success: bool, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            let attributes = provider_attributes("read", success, path);
            metrics.record_provider_download_bytes(bytes, &attributes);
            metrics.record_provider_rpc_latency(latency_ms, &attributes);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (path, bytes, success, latency_ms);
    }

    pub fn record_provider_write(&self, path: &str, bytes: u64, success: bool, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            let attributes = provider_attributes("write", success, path);
            metrics.record_provider_upload_bytes(bytes, &attributes);
            metrics.record_provider_rpc_latency(latency_ms, &attributes);
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (path, bytes, success, latency_ms);
    }

    pub fn record_api_latency(&self, operation: &str, success: bool, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_api_latency(latency_ms, &api_attributes(operation, success));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (operation, success, latency_ms);
    }

    pub fn record_dispatch_latency(&self, topic: &str, queue_id: i32, file_type: &str, latency_ms: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_dispatch_latency(
                latency_ms,
                &dispatch_attributes(&metrics.label_policy, topic, queue_id, file_type),
            );
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (topic, queue_id, file_type, latency_ms);
    }

    pub fn record_read_ahead_cache_access(&self, success: bool, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_read_ahead_cache_access(count, &cache_attributes(success));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (success, count);
    }

    pub fn record_read_ahead_cache_hit(&self, success: bool, count: u64) {
        #[cfg(feature = "otel-metrics")]
        if let Some(metrics) = self.active_metrics() {
            metrics.record_read_ahead_cache_hit(count, &cache_attributes(success));
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (success, count);
    }
}

pub struct TieredStoreMetrics {
    recorder: TieredStoreMetricsRecorder,
    dispatch_requests: AtomicU64,
    dispatch_behind: AtomicU64,
    commit_failures: AtomicU64,
    fetch_requests: AtomicU64,
    messages_dispatch_total: AtomicU64,
    messages_out_total: AtomicU64,
    get_message_fallback_total: AtomicU64,
    provider_upload_bytes: AtomicU64,
    provider_download_bytes: AtomicU64,
    read_ahead_cache_access_total: AtomicU64,
    read_ahead_cache_hit_total: AtomicU64,
    read_ahead_cache_count: AtomicU64,
    read_ahead_cache_bytes: AtomicU64,
}

impl Default for TieredStoreMetrics {
    fn default() -> Self {
        Self::new(TieredStoreMetricsRecorder::noop())
    }
}

impl TieredStoreMetrics {
    pub fn new(recorder: TieredStoreMetricsRecorder) -> Self {
        Self {
            recorder,
            dispatch_requests: AtomicU64::new(0),
            dispatch_behind: AtomicU64::new(0),
            commit_failures: AtomicU64::new(0),
            fetch_requests: AtomicU64::new(0),
            messages_dispatch_total: AtomicU64::new(0),
            messages_out_total: AtomicU64::new(0),
            get_message_fallback_total: AtomicU64::new(0),
            provider_upload_bytes: AtomicU64::new(0),
            provider_download_bytes: AtomicU64::new(0),
            read_ahead_cache_access_total: AtomicU64::new(0),
            read_ahead_cache_hit_total: AtomicU64::new(0),
            read_ahead_cache_count: AtomicU64::new(0),
            read_ahead_cache_bytes: AtomicU64::new(0),
        }
    }

    pub fn recorder(&self) -> TieredStoreMetricsRecorder {
        self.recorder.clone()
    }

    pub fn record_dispatch_request(&self) {
        self.dispatch_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dispatch_queued(&self) {
        self.record_dispatch_request();
        self.dispatch_behind.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dispatch_dequeued(&self) {
        let _ = self
            .dispatch_behind
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_sub(1));
    }

    pub fn record_commit_failure(&self) {
        self.commit_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fetch_request(&self) {
        self.fetch_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_messages_dispatch(&self, topic: &str, queue_id: i32, file_type: &str, count: u64) {
        self.messages_dispatch_total.fetch_add(count, Ordering::Relaxed);
        self.recorder
            .record_messages_dispatch(topic, queue_id, file_type, count);
    }

    pub fn record_messages_out(&self, topic: &str, group: &str, count: u64) {
        self.messages_out_total.fetch_add(count, Ordering::Relaxed);
        self.recorder.record_messages_out(topic, group, count);
    }

    pub fn record_get_message_fallback(&self, topic: &str, group: &str) {
        self.get_message_fallback_total.fetch_add(1, Ordering::Relaxed);
        self.recorder.record_get_message_fallback(topic, group);
    }

    pub fn record_dispatch_latency(&self, topic: &str, queue_id: i32, file_type: &str, latency_ms: u64) {
        self.recorder
            .record_dispatch_latency(topic, queue_id, file_type, latency_ms);
    }

    pub fn record_api_latency(&self, operation: &str, success: bool, latency_ms: u64) {
        self.recorder.record_api_latency(operation, success, latency_ms);
    }

    pub fn record_read_ahead_cache_access(&self, success: bool, count: u64) {
        self.read_ahead_cache_access_total.fetch_add(count, Ordering::Relaxed);
        self.recorder.record_read_ahead_cache_access(success, count);
    }

    pub fn record_read_ahead_cache_hit(&self, success: bool, count: u64) {
        self.read_ahead_cache_hit_total.fetch_add(count, Ordering::Relaxed);
        self.recorder.record_read_ahead_cache_hit(success, count);
    }

    pub fn record_provider_read(&self, path: &str, bytes: u64, success: bool, latency_ms: u64) {
        self.provider_download_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.recorder.record_provider_read(path, bytes, success, latency_ms);
    }

    pub fn record_provider_write(&self, path: &str, bytes: u64, success: bool, latency_ms: u64) {
        self.provider_upload_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.recorder.record_provider_write(path, bytes, success, latency_ms);
    }

    pub fn set_read_ahead_cache_gauges(&self, count: u64, bytes: u64) {
        self.read_ahead_cache_count.store(count, Ordering::Relaxed);
        self.read_ahead_cache_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn dispatch_requests(&self) -> u64 {
        self.dispatch_requests.load(Ordering::Relaxed)
    }

    pub fn commit_failures(&self) -> u64 {
        self.commit_failures.load(Ordering::Relaxed)
    }

    pub fn fetch_requests(&self) -> u64 {
        self.fetch_requests.load(Ordering::Relaxed)
    }

    pub fn messages_dispatch_total(&self) -> u64 {
        self.messages_dispatch_total.load(Ordering::Relaxed)
    }

    pub fn messages_out_total(&self) -> u64 {
        self.messages_out_total.load(Ordering::Relaxed)
    }

    pub fn get_message_fallback_total(&self) -> u64 {
        self.get_message_fallback_total.load(Ordering::Relaxed)
    }

    pub fn provider_upload_bytes(&self) -> u64 {
        self.provider_upload_bytes.load(Ordering::Relaxed)
    }

    pub fn provider_download_bytes(&self) -> u64 {
        self.provider_download_bytes.load(Ordering::Relaxed)
    }

    pub fn read_ahead_cache_access_total(&self) -> u64 {
        self.read_ahead_cache_access_total.load(Ordering::Relaxed)
    }

    pub fn read_ahead_cache_hit_total(&self) -> u64 {
        self.read_ahead_cache_hit_total.load(Ordering::Relaxed)
    }

    pub fn observable_values(&self) -> TieredStoreObservableValues {
        let dispatch_behind = self.dispatch_behind.load(Ordering::Relaxed) as i64;
        TieredStoreObservableValues {
            dispatch_behind: vec![TieredDispatchBehind {
                topic: "ALL".to_owned(),
                queue_id: -1,
                file_type: "commitlog".to_owned(),
                count: dispatch_behind,
            }],
            read_ahead_cache_count: self.read_ahead_cache_count.load(Ordering::Relaxed) as i64,
            read_ahead_cache_bytes: self.read_ahead_cache_bytes.load(Ordering::Relaxed) as i64,
        }
    }
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Default)]
pub struct TieredStoreOtelMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl TieredStoreOtelMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_messages_dispatch(&self, _count: u64) {}

    #[inline]
    pub fn record_messages_out(&self, _count: u64) {}

    #[inline]
    pub fn record_get_message_fallback(&self, _count: u64) {}

    #[inline]
    pub fn record_provider_upload_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_provider_download_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_provider_rpc_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_api_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_dispatch_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_read_ahead_cache_access(&self, _count: u64) {}

    #[inline]
    pub fn record_read_ahead_cache_hit(&self, _count: u64) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct TieredStoreOtelMetrics {
    messages_dispatch_total: opentelemetry::metrics::Counter<u64>,
    messages_out_total: opentelemetry::metrics::Counter<u64>,
    get_message_fallback_total: opentelemetry::metrics::Counter<u64>,
    provider_upload_bytes: opentelemetry::metrics::Counter<u64>,
    provider_download_bytes: opentelemetry::metrics::Counter<u64>,
    provider_rpc_latency: opentelemetry::metrics::Histogram<u64>,
    api_latency: opentelemetry::metrics::Histogram<u64>,
    dispatch_latency: opentelemetry::metrics::Histogram<u64>,
    read_ahead_cache_access_total: opentelemetry::metrics::Counter<u64>,
    read_ahead_cache_hit_total: opentelemetry::metrics::Counter<u64>,
    label_policy: crate::MetricLabelPolicy,
}

#[cfg(feature = "otel-metrics")]
impl TieredStoreOtelMetrics {
    #[cfg(test)]
    pub(crate) fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self::new_with_label_policy(meter, crate::MetricLabelPolicy::default())
    }

    pub(crate) fn new_with_label_policy(
        meter: &opentelemetry::metrics::Meter,
        label_policy: crate::MetricLabelPolicy,
    ) -> Self {
        let messages_dispatch_total = meter
            .u64_counter(TIERED_STORE_MESSAGES_DISPATCH_TOTAL)
            .with_description("Total number of messages dispatched to tiered store")
            .with_unit("{message}")
            .build();

        let messages_out_total = meter
            .u64_counter(TIERED_STORE_MESSAGES_OUT_TOTAL)
            .with_description("Total number of messages read from tiered store")
            .with_unit("{message}")
            .build();

        let get_message_fallback_total = meter
            .u64_counter(TIERED_STORE_GET_MESSAGE_FALLBACK_TOTAL)
            .with_description("Total number of get_message fallbacks to tiered store")
            .with_unit("{request}")
            .build();

        let provider_upload_bytes = meter
            .u64_counter(TIERED_STORE_PROVIDER_UPLOAD_BYTES)
            .with_description("Total bytes uploaded by the tiered store provider")
            .with_unit("By")
            .build();

        let provider_download_bytes = meter
            .u64_counter(TIERED_STORE_PROVIDER_DOWNLOAD_BYTES)
            .with_description("Total bytes downloaded by the tiered store provider")
            .with_unit("By")
            .build();

        let provider_rpc_latency = meter
            .u64_histogram(TIERED_STORE_PROVIDER_RPC_LATENCY)
            .with_description("Tiered store provider operation latency")
            .with_unit("ms")
            .build();

        let api_latency = meter
            .u64_histogram(TIERED_STORE_API_LATENCY)
            .with_description("Tiered store API latency")
            .with_unit("ms")
            .build();

        let dispatch_latency = meter
            .u64_histogram(TIERED_STORE_DISPATCH_LATENCY)
            .with_description("Tiered store dispatch latency")
            .with_unit("ms")
            .build();

        let read_ahead_cache_access_total = meter
            .u64_counter(TIERED_STORE_READ_AHEAD_CACHE_ACCESS_TOTAL)
            .with_description("Total tiered store read-ahead cache access count")
            .with_unit("{access}")
            .build();

        let read_ahead_cache_hit_total = meter
            .u64_counter(TIERED_STORE_READ_AHEAD_CACHE_HIT_TOTAL)
            .with_description("Total tiered store read-ahead cache hit count")
            .with_unit("{hit}")
            .build();

        Self {
            messages_dispatch_total,
            messages_out_total,
            get_message_fallback_total,
            provider_upload_bytes,
            provider_download_bytes,
            provider_rpc_latency,
            api_latency,
            dispatch_latency,
            read_ahead_cache_access_total,
            read_ahead_cache_hit_total,
            label_policy,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_observables<F>(meter: &opentelemetry::metrics::Meter, source: F) -> Self
    where
        F: Fn() -> TieredStoreObservableValues + Send + Sync + 'static,
    {
        let metrics = Self::new(meter);
        Self::register_observables_with_label_policy(meter, source, metrics.label_policy.clone());
        metrics
    }

    pub(crate) fn register_observables_with_label_policy<F>(
        meter: &opentelemetry::metrics::Meter,
        source: F,
        label_policy: crate::MetricLabelPolicy,
    ) where
        F: Fn() -> TieredStoreObservableValues + Send + Sync + 'static,
    {
        let source = std::sync::Arc::new(source);

        let dispatch_behind_source = source.clone();
        let _dispatch_behind = meter
            .i64_observable_gauge(TIERED_STORE_DISPATCH_BEHIND)
            .with_description("Tiered store dispatch lag")
            .with_unit("{message}")
            .with_callback(move |observer| {
                let values = dispatch_behind_source();
                for behind in values.dispatch_behind {
                    observer.observe(
                        behind.count.max(0),
                        &dispatch_attributes(&label_policy, &behind.topic, behind.queue_id, &behind.file_type),
                    );
                }
            })
            .build();

        let cache_count_source = source.clone();
        let _cache_count = meter
            .i64_observable_gauge(TIERED_STORE_READ_AHEAD_CACHE_COUNT)
            .with_description("Current tiered store read-ahead cache item count")
            .with_unit("{item}")
            .with_callback(move |observer| {
                let values = cache_count_source();
                observer.observe(values.read_ahead_cache_count.max(0), &[]);
            })
            .build();

        let _cache_bytes = meter
            .i64_observable_gauge(TIERED_STORE_READ_AHEAD_CACHE_BYTES)
            .with_description("Current tiered store read-ahead cache bytes")
            .with_unit("By")
            .with_callback(move |observer| {
                let values = source();
                observer.observe(values.read_ahead_cache_bytes.max(0), &[]);
            })
            .build();
    }

    #[inline]
    pub fn record_messages_dispatch(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.messages_dispatch_total.add(count, attributes);
    }

    #[inline]
    pub fn record_messages_out(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.messages_out_total.add(count, attributes);
    }

    #[inline]
    pub fn record_get_message_fallback(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.get_message_fallback_total.add(count, attributes);
    }

    #[inline]
    pub fn record_provider_upload_bytes(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.provider_upload_bytes.add(bytes, attributes);
    }

    #[inline]
    pub fn record_provider_download_bytes(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.provider_download_bytes.add(bytes, attributes);
    }

    #[inline]
    pub fn record_provider_rpc_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.provider_rpc_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_api_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.api_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_dispatch_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.dispatch_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_read_ahead_cache_access(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.read_ahead_cache_access_total.add(count, attributes);
    }

    #[inline]
    pub fn record_read_ahead_cache_hit(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.read_ahead_cache_hit_total.add(count, attributes);
    }
}

#[cfg(feature = "otel-metrics")]
fn dispatch_attributes(
    label_policy: &crate::MetricLabelPolicy,
    topic: &str,
    queue_id: i32,
    file_type: &str,
) -> [opentelemetry::KeyValue; 3] {
    [
        bounded_label(label_policy, crate::semantic::labels::TOPIC, topic),
        opentelemetry::KeyValue::new(crate::semantic::labels::QUEUE_ID, i64::from(queue_id)),
        opentelemetry::KeyValue::new(crate::semantic::labels::FILE_TYPE, file_type_class(file_type)),
    ]
}

#[cfg(feature = "otel-metrics")]
fn message_out_attributes(
    label_policy: &crate::MetricLabelPolicy,
    topic: &str,
    group: &str,
) -> [opentelemetry::KeyValue; 2] {
    [
        bounded_label(label_policy, crate::semantic::labels::TOPIC, topic),
        bounded_label(label_policy, crate::semantic::labels::GROUP, group),
    ]
}

#[cfg(feature = "otel-metrics")]
#[inline]
fn bounded_label(policy: &crate::MetricLabelPolicy, key: &'static str, value: &str) -> opentelemetry::KeyValue {
    let (value, dropped) = policy.normalize_metric_label_with_outcome(key, value);
    if dropped {
        opentelemetry::KeyValue::new(key, crate::METRIC_LABEL_SENTINEL)
    } else {
        opentelemetry::KeyValue::new(key, value.into_owned())
    }
}

#[cfg(feature = "otel-metrics")]
fn provider_attributes(operation: &str, success: bool, path: &str) -> [opentelemetry::KeyValue; 3] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::OPERATION, provider_operation_class(operation)),
        opentelemetry::KeyValue::new(crate::semantic::labels::SUCCESS, success),
        opentelemetry::KeyValue::new(crate::semantic::labels::FILE_TYPE, provider_path_class(path)),
    ]
}

#[cfg(feature = "otel-metrics")]
fn provider_path_class(path: &str) -> &'static str {
    if path.split(['/', '\\']).any(|component| component == "commitlog") {
        "commitlog"
    } else if path
        .split(['/', '\\'])
        .any(|component| matches!(component, "consumequeue" | "consume_queue"))
    {
        "consume_queue"
    } else if path.split(['/', '\\']).any(|component| component == "index") {
        "index"
    } else {
        "other"
    }
}

#[cfg(feature = "otel-metrics")]
fn api_attributes(operation: &str, success: bool) -> [opentelemetry::KeyValue; 2] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::OPERATION, api_operation_class(operation)),
        opentelemetry::KeyValue::new(crate::semantic::labels::SUCCESS, success),
    ]
}

#[cfg(feature = "otel-metrics")]
fn cache_attributes(success: bool) -> [opentelemetry::KeyValue; 1] {
    [opentelemetry::KeyValue::new(crate::semantic::labels::SUCCESS, success)]
}

#[cfg(feature = "otel-metrics")]
fn provider_operation_class(operation: &str) -> &'static str {
    match operation {
        "read" => "read",
        "write" => "write",
        _ => "other",
    }
}

#[cfg(feature = "otel-metrics")]
fn api_operation_class(operation: &str) -> &'static str {
    match operation {
        "get_message" => "get_message",
        "get_message_timestamp" => "get_message_timestamp",
        "get_offset_by_time_with_boundary" => "get_offset_by_time_with_boundary",
        "query_message" => "query_message",
        _ => "other",
    }
}

#[cfg(feature = "otel-metrics")]
fn file_type_class(file_type: &str) -> &'static str {
    match file_type {
        "commitlog" => "commitlog",
        "consumequeue" | "consume_queue" => "consume_queue",
        "index" => "index",
        _ => "other",
    }
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn tiered_store_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("tiered-store-metrics-test");
        let metrics = TieredStoreOtelMetrics::new_with_observables(&meter, || TieredStoreObservableValues {
            dispatch_behind: vec![TieredDispatchBehind {
                topic: "TopicA".to_owned(),
                queue_id: 0,
                file_type: "commitlog".to_owned(),
                count: 3,
            }],
            read_ahead_cache_count: 4,
            read_ahead_cache_bytes: 1024,
        });

        metrics.record_messages_dispatch(1, &dispatch_attributes(&metrics.label_policy, "TopicA", 0, "commitlog"));
        metrics.record_messages_out(2, &message_out_attributes(&metrics.label_policy, "TopicA", "GroupA"));
        metrics.record_get_message_fallback(1, &message_out_attributes(&metrics.label_policy, "TopicA", "GroupA"));
        metrics.record_provider_upload_bytes(128, &provider_attributes("write", true, "TopicA/0/commitlog/0"));
        metrics.record_provider_download_bytes(64, &provider_attributes("read", true, "TopicA/0/commitlog/0"));
        metrics.record_provider_rpc_latency(5, &provider_attributes("read", true, "TopicA/0/commitlog/0"));
        metrics.record_api_latency(7, &api_attributes("get_message", true));
        metrics.record_dispatch_latency(9, &dispatch_attributes(&metrics.label_policy, "TopicA", 0, "commitlog"));
        metrics.record_read_ahead_cache_access(1, &cache_attributes(true));
        metrics.record_read_ahead_cache_hit(1, &cache_attributes(true));
    }

    #[test]
    fn noop_recorder_is_safe_and_disabled() {
        let recorder = TieredStoreMetricsRecorder::noop();

        recorder.record_messages_dispatch("TopicA", 0, "commitlog", 1);
        recorder.record_messages_out("TopicA", "GroupA", 1);
        recorder.record_get_message_fallback("TopicA", "GroupA");
        recorder.record_provider_read("TopicA/0/commitlog/0", 1, true, 1);
        recorder.record_provider_write("TopicA/0/commitlog/0", 1, true, 1);
        recorder.record_api_latency("get_message", true, 1);
        recorder.record_dispatch_latency("TopicA", 0, "commitlog", 1);
        recorder.record_read_ahead_cache_access(true, 1);
        recorder.record_read_ahead_cache_hit(true, 1);

        assert!(!recorder.is_enabled());
    }

    #[test]
    fn provider_paths_aggregate_by_static_file_class() {
        let first = provider_path_class("TopicA/0/commitlog/00000000000000000000");
        let second = provider_path_class("TopicB/19/commitlog/00000000000098765432");
        let consume_queue = provider_path_class(r"TopicC\7\consume_queue\123");

        assert_eq!(first, "commitlog");
        assert_eq!(second, first);
        assert_eq!(consume_queue, "consume_queue");
        assert_eq!(provider_path_class("TopicD/3/unknown/456"), "other");
    }

    #[test]
    fn tiered_store_topic_and_group_attributes_use_the_bounded_policy() {
        let policy = crate::MetricLabelPolicy::new(1, true, true);

        assert_eq!(
            dispatch_attributes(&policy, "TopicA", 0, "commitlog")[0]
                .value
                .to_string(),
            "TopicA"
        );
        assert_eq!(
            dispatch_attributes(&policy, "TopicB", 0, "commitlog")[0]
                .value
                .to_string(),
            crate::METRIC_LABEL_SENTINEL
        );
        assert_eq!(
            message_out_attributes(&policy, "TopicA", "GroupA")[1].value.to_string(),
            "GroupA"
        );
        assert_eq!(
            message_out_attributes(&policy, "TopicA", "GroupB")[1].value.to_string(),
            crate::METRIC_LABEL_SENTINEL
        );
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;

    #[test]
    fn tiered_store_runtime_metrics_count_local_events() {
        let metrics = TieredStoreMetrics::default();

        metrics.record_dispatch_queued();
        metrics.record_dispatch_dequeued();
        metrics.record_commit_failure();
        metrics.record_fetch_request();
        metrics.record_messages_dispatch("TopicA", 0, "commitlog", 2);
        metrics.record_messages_out("TopicA", "GroupA", 3);
        metrics.record_get_message_fallback("TopicA", "GroupA");
        metrics.record_read_ahead_cache_access(true, 4);
        metrics.record_read_ahead_cache_hit(true, 2);

        assert_eq!(metrics.dispatch_requests(), 1);
        assert_eq!(metrics.commit_failures(), 1);
        assert_eq!(metrics.fetch_requests(), 1);
        assert_eq!(metrics.messages_dispatch_total(), 2);
        assert_eq!(metrics.messages_out_total(), 3);
        assert_eq!(metrics.get_message_fallback_total(), 1);
        assert_eq!(metrics.read_ahead_cache_access_total(), 4);
        assert_eq!(metrics.read_ahead_cache_hit_total(), 2);
    }
}
