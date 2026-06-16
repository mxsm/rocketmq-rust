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

pub use crate::semantic::metrics::CLIENT_CONSUME_LATENCY;
pub use crate::semantic::metrics::CLIENT_CONSUME_TOTAL;
pub use crate::semantic::metrics::CLIENT_REBALANCE_TOTAL;
pub use crate::semantic::metrics::CLIENT_SEND_LATENCY;
pub use crate::semantic::metrics::CLIENT_SEND_TOTAL;

#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static CLIENT_METRICS: OnceLock<ClientMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
static CLIENT_GLOBAL_METRICS: OnceLock<ClientMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
pub fn init_global(meter: &opentelemetry::metrics::Meter) -> bool {
    CLIENT_METRICS.set(ClientMetrics::new(meter)).is_ok()
}

#[cfg(feature = "otel-metrics")]
fn global_metrics() -> &'static ClientMetrics {
    if let Some(metrics) = CLIENT_METRICS.get() {
        return metrics;
    }

    CLIENT_GLOBAL_METRICS.get_or_init(|| ClientMetrics::new(&opentelemetry::global::meter("rocketmq-client")))
}

pub fn record_send_total(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_send_total(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_send_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_send_latency(latency_ms, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_consume_total(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_consume_total(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_consume_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_consume_latency(latency_ms, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_rebalance_total(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_rebalance_total(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct ClientMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl ClientMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_send_total(&self, _count: u64) {}

    #[inline]
    pub fn record_send_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_consume_total(&self, _count: u64) {}

    #[inline]
    pub fn record_consume_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_rebalance_total(&self, _count: u64) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct ClientMetrics {
    send_total: opentelemetry::metrics::Counter<u64>,
    send_latency: opentelemetry::metrics::Histogram<u64>,
    consume_total: opentelemetry::metrics::Counter<u64>,
    consume_latency: opentelemetry::metrics::Histogram<u64>,
    rebalance_total: opentelemetry::metrics::Counter<u64>,
}

#[cfg(feature = "otel-metrics")]
impl ClientMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let send_total = meter
            .u64_counter(CLIENT_SEND_TOTAL)
            .with_description("Total number of messages sent by client")
            .with_unit("{message}")
            .build();

        let send_latency = meter
            .u64_histogram(CLIENT_SEND_LATENCY)
            .with_description("Client send latency")
            .with_unit("ms")
            .build();

        let consume_total = meter
            .u64_counter(CLIENT_CONSUME_TOTAL)
            .with_description("Total number of messages consumed by client")
            .with_unit("{message}")
            .build();

        let consume_latency = meter
            .u64_histogram(CLIENT_CONSUME_LATENCY)
            .with_description("Client consume latency")
            .with_unit("ms")
            .build();

        let rebalance_total = meter
            .u64_counter(CLIENT_REBALANCE_TOTAL)
            .with_description("Total number of client rebalance events")
            .with_unit("{event}")
            .build();

        Self {
            send_total,
            send_latency,
            consume_total,
            consume_latency,
            rebalance_total,
        }
    }

    #[inline]
    pub fn record_send_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.send_total.add(count, attributes);
    }

    #[inline]
    pub fn record_send_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.send_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_consume_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.consume_total.add(count, attributes);
    }

    #[inline]
    pub fn record_consume_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.consume_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_rebalance_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.rebalance_total.add(count, attributes);
    }
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn client_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("client-metrics-test");
        let metrics = ClientMetrics::new(&meter);
        let attrs = [opentelemetry::KeyValue::new("client_id", "client-a")];

        metrics.record_send_total(1, &attrs);
        metrics.record_send_latency(10, &attrs);
        metrics.record_consume_total(1, &attrs);
        metrics.record_consume_latency(8, &attrs);
        metrics.record_rebalance_total(1, &attrs);
    }

    #[test]
    fn client_global_recorders_lazy_initialize() {
        record_send_total(1);
        record_send_latency(10);
        record_consume_total(1);
        record_consume_latency(8);
        record_rebalance_total(1);

        assert!(CLIENT_METRICS.get().is_some() || CLIENT_GLOBAL_METRICS.get().is_some());
    }
}
