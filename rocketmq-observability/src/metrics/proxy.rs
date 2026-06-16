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

pub use crate::semantic::metrics::PROXY_ACTIVE_CONNECTIONS;
pub use crate::semantic::metrics::PROXY_FORWARD_LATENCY;
pub use crate::semantic::metrics::PROXY_GRPC_REQUESTS_TOTAL;
pub use crate::semantic::metrics::PROXY_GRPC_REQUEST_LATENCY;

#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static PROXY_METRICS: OnceLock<ProxyMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
static PROXY_GLOBAL_METRICS: OnceLock<ProxyMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
pub fn init_global(meter: &opentelemetry::metrics::Meter) -> bool {
    PROXY_METRICS.set(ProxyMetrics::new(meter)).is_ok()
}

#[cfg(feature = "otel-metrics")]
fn global_metrics() -> &'static ProxyMetrics {
    if let Some(metrics) = PROXY_METRICS.get() {
        return metrics;
    }

    PROXY_GLOBAL_METRICS.get_or_init(|| ProxyMetrics::new(&opentelemetry::global::meter("rocketmq-proxy")))
}

pub fn record_grpc_requests_total(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_grpc_requests_total(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_grpc_request_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_grpc_request_latency(latency_ms, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_forward_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_forward_latency(latency_ms, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_active_connections(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_active_connections(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct ProxyMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl ProxyMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_grpc_requests_total(&self, _count: u64) {}

    #[inline]
    pub fn record_grpc_request_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_forward_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_active_connections(&self, _count: u64) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct ProxyMetrics {
    grpc_requests_total: opentelemetry::metrics::Counter<u64>,
    grpc_request_latency: opentelemetry::metrics::Histogram<u64>,
    forward_latency: opentelemetry::metrics::Histogram<u64>,
    active_connections: opentelemetry::metrics::Gauge<u64>,
}

#[cfg(feature = "otel-metrics")]
impl ProxyMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let grpc_requests_total = meter
            .u64_counter(PROXY_GRPC_REQUESTS_TOTAL)
            .with_description("Total number of proxy gRPC requests")
            .with_unit("{request}")
            .build();

        let grpc_request_latency = meter
            .u64_histogram(PROXY_GRPC_REQUEST_LATENCY)
            .with_description("Proxy gRPC request latency")
            .with_unit("ms")
            .build();

        let forward_latency = meter
            .u64_histogram(PROXY_FORWARD_LATENCY)
            .with_description("Proxy request forwarding latency")
            .with_unit("ms")
            .build();

        let active_connections = meter
            .u64_gauge(PROXY_ACTIVE_CONNECTIONS)
            .with_description("Number of active proxy connections")
            .with_unit("{connection}")
            .build();

        Self {
            grpc_requests_total,
            grpc_request_latency,
            forward_latency,
            active_connections,
        }
    }

    #[inline]
    pub fn record_grpc_requests_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.grpc_requests_total.add(count, attributes);
    }

    #[inline]
    pub fn record_grpc_request_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.grpc_request_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_forward_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.forward_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_active_connections(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.active_connections.record(count, attributes);
    }
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn proxy_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("proxy-metrics-test");
        let metrics = ProxyMetrics::new(&meter);
        let attrs = [opentelemetry::KeyValue::new("protocol", "grpc")];

        metrics.record_grpc_requests_total(1, &attrs);
        metrics.record_grpc_request_latency(6, &attrs);
        metrics.record_forward_latency(12, &attrs);
        metrics.record_active_connections(4, &attrs);
    }

    #[test]
    fn proxy_global_recorders_lazy_initialize() {
        record_grpc_requests_total(1);
        record_grpc_request_latency(6);
        record_forward_latency(12);
        record_active_connections(4);

        assert!(PROXY_METRICS.get().is_some() || PROXY_GLOBAL_METRICS.get().is_some());
    }
}
