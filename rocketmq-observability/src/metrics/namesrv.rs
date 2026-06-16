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

pub use crate::semantic::metrics::NAMESRV_ACTIVE_BROKERS;
pub use crate::semantic::metrics::NAMESRV_BROKER_REGISTRATIONS;
pub use crate::semantic::metrics::NAMESRV_ROUTE_REQUEST_LATENCY;
pub use crate::semantic::metrics::NAMESRV_ROUTE_REQUEST_TOTAL;

#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static NAMESRV_METRICS: OnceLock<NameServerMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
static NAMESRV_GLOBAL_METRICS: OnceLock<NameServerMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
pub fn init_global(meter: &opentelemetry::metrics::Meter) -> bool {
    NAMESRV_METRICS.set(NameServerMetrics::new(meter)).is_ok()
}

#[cfg(feature = "otel-metrics")]
fn global_metrics() -> &'static NameServerMetrics {
    if let Some(metrics) = NAMESRV_METRICS.get() {
        return metrics;
    }

    NAMESRV_GLOBAL_METRICS.get_or_init(|| NameServerMetrics::new(&opentelemetry::global::meter("rocketmq-namesrv")))
}

pub fn record_route_request_total(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_route_request_total(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_route_request_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_route_request_latency(latency_ms, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_broker_registrations(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_broker_registrations(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_active_brokers(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_active_brokers(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct NameServerMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl NameServerMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_route_request_total(&self, _count: u64) {}

    #[inline]
    pub fn record_route_request_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_broker_registrations(&self, _count: u64) {}

    #[inline]
    pub fn record_active_brokers(&self, _count: u64) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct NameServerMetrics {
    route_request_total: opentelemetry::metrics::Counter<u64>,
    route_request_latency: opentelemetry::metrics::Histogram<u64>,
    broker_registrations: opentelemetry::metrics::Counter<u64>,
    active_brokers: opentelemetry::metrics::Gauge<u64>,
}

#[cfg(feature = "otel-metrics")]
impl NameServerMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let route_request_total = meter
            .u64_counter(NAMESRV_ROUTE_REQUEST_TOTAL)
            .with_description("Total number of NameServer route requests")
            .with_unit("{request}")
            .build();

        let route_request_latency = meter
            .u64_histogram(NAMESRV_ROUTE_REQUEST_LATENCY)
            .with_description("NameServer route request latency")
            .with_unit("ms")
            .build();

        let broker_registrations = meter
            .u64_counter(NAMESRV_BROKER_REGISTRATIONS)
            .with_description("Total number of broker registrations received by NameServer")
            .with_unit("{registration}")
            .build();

        let active_brokers = meter
            .u64_gauge(NAMESRV_ACTIVE_BROKERS)
            .with_description("Number of active brokers known by NameServer")
            .with_unit("{broker}")
            .build();

        Self {
            route_request_total,
            route_request_latency,
            broker_registrations,
            active_brokers,
        }
    }

    #[inline]
    pub fn record_route_request_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.route_request_total.add(count, attributes);
    }

    #[inline]
    pub fn record_route_request_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.route_request_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_broker_registrations(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.broker_registrations.add(count, attributes);
    }

    #[inline]
    pub fn record_active_brokers(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.active_brokers.record(count, attributes);
    }
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn namesrv_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("namesrv-metrics-test");
        let metrics = NameServerMetrics::new(&meter);
        let attrs = [opentelemetry::KeyValue::new("namesrv_id", "namesrv-a")];

        metrics.record_route_request_total(1, &attrs);
        metrics.record_route_request_latency(3, &attrs);
        metrics.record_broker_registrations(1, &attrs);
        metrics.record_active_brokers(2, &attrs);
    }

    #[test]
    fn namesrv_global_recorders_lazy_initialize() {
        record_route_request_total(1);
        record_route_request_latency(3);
        record_broker_registrations(1);
        record_active_brokers(2);

        assert!(NAMESRV_METRICS.get().is_some() || NAMESRV_GLOBAL_METRICS.get().is_some());
    }
}
