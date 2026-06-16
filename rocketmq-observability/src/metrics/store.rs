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

pub use crate::semantic::metrics::STORE_APPEND_LATENCY;
pub use crate::semantic::metrics::STORE_DISK_USAGE;
pub use crate::semantic::metrics::STORE_DISPATCH_LATENCY;
pub use crate::semantic::metrics::STORE_FLUSH_LATENCY;

#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static STORE_METRICS: OnceLock<StoreMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
pub fn init_global(meter: &opentelemetry::metrics::Meter) -> bool {
    STORE_METRICS.set(StoreMetrics::new(meter)).is_ok()
}

pub fn record_append_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_append_latency(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_flush_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_flush_latency(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_dispatch_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_dispatch_latency(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_disk_usage(bytes: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_disk_usage(bytes, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = bytes;
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
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
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct StoreMetrics {
    append_latency: opentelemetry::metrics::Histogram<u64>,
    flush_latency: opentelemetry::metrics::Histogram<u64>,
    dispatch_latency: opentelemetry::metrics::Histogram<u64>,
    disk_usage: opentelemetry::metrics::Gauge<u64>,
}

#[cfg(feature = "otel-metrics")]
impl StoreMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
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

        Self {
            append_latency,
            flush_latency,
            dispatch_latency,
            disk_usage,
        }
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

        metrics.record_append_latency(5, &attrs);
        metrics.record_flush_latency(7, &attrs);
        metrics.record_dispatch_latency(9, &attrs);
        metrics.record_disk_usage(1024, &attrs);
    }
}
