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

pub use crate::semantic::metrics::REMOTING_NETWORK_BYTES;
pub use crate::semantic::metrics::REMOTING_REQUESTS_TOTAL;
pub use crate::semantic::metrics::REMOTING_REQUEST_LATENCY;
pub use crate::semantic::metrics::RPC_LATENCY;

#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static REMOTING_METRICS: OnceLock<RemotingMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
pub fn init_global(meter: &opentelemetry::metrics::Meter) -> bool {
    REMOTING_METRICS.set(RemotingMetrics::new(meter)).is_ok()
}

pub fn record_requests_total(count: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = REMOTING_METRICS.get() {
        metrics.record_requests_total(count, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_request_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = REMOTING_METRICS.get() {
        metrics.record_request_latency(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_network_bytes(bytes: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = REMOTING_METRICS.get() {
        metrics.record_network_bytes(bytes, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = bytes;
}

pub fn record_rpc_latency(latency_ms: u64, request_code: i32, response_code: i32, is_long_polling: bool, result: &str) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = REMOTING_METRICS.get() {
        let attrs = [
            opentelemetry::KeyValue::new(crate::semantic::labels::PROTOCOL_TYPE, "remoting"),
            opentelemetry::KeyValue::new(crate::semantic::labels::REQUEST_CODE, request_code.to_string()),
            opentelemetry::KeyValue::new(crate::semantic::labels::RESPONSE_CODE, response_code.to_string()),
            opentelemetry::KeyValue::new(crate::semantic::labels::IS_LONG_POLLING, is_long_polling.to_string()),
            opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result.to_owned()),
        ];
        metrics.record_rpc_latency(latency_ms, &attrs);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = (latency_ms, request_code, response_code, is_long_polling, result);
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct RemotingMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl RemotingMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_requests_total(&self, _count: u64) {}

    #[inline]
    pub fn record_request_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_network_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_rpc_latency(&self, _latency_ms: u64, _attributes: &[()]) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct RemotingMetrics {
    requests_total: opentelemetry::metrics::Counter<u64>,
    request_latency: opentelemetry::metrics::Histogram<u64>,
    network_bytes: opentelemetry::metrics::Counter<u64>,
    rpc_latency: opentelemetry::metrics::Histogram<u64>,
}

#[cfg(feature = "otel-metrics")]
impl RemotingMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let requests_total = meter
            .u64_counter(REMOTING_REQUESTS_TOTAL)
            .with_description("Total number of remoting requests")
            .with_unit("{request}")
            .build();

        let request_latency = meter
            .u64_histogram(REMOTING_REQUEST_LATENCY)
            .with_description("Remoting request latency")
            .with_unit("ms")
            .build();

        let network_bytes = meter
            .u64_counter(REMOTING_NETWORK_BYTES)
            .with_description("Total network bytes processed by remoting")
            .with_unit("By")
            .build();

        let rpc_latency = meter
            .u64_histogram(RPC_LATENCY)
            .with_description("Rpc latency")
            .with_unit("milliseconds")
            .build();

        Self {
            requests_total,
            request_latency,
            network_bytes,
            rpc_latency,
        }
    }

    #[inline]
    pub fn record_requests_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.requests_total.add(count, attributes);
    }

    #[inline]
    pub fn record_request_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.request_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_network_bytes(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.network_bytes.add(bytes, attributes);
    }

    #[inline]
    pub fn record_rpc_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.rpc_latency.record(latency_ms, attributes);
    }
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn remoting_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("remoting-metrics-test");
        let metrics = RemotingMetrics::new(&meter);
        let attrs = [opentelemetry::KeyValue::new("request_code", "10")];

        metrics.record_requests_total(1, &attrs);
        metrics.record_request_latency(3, &attrs);
        metrics.record_network_bytes(256, &attrs);
        metrics.record_rpc_latency(5, &attrs);
        record_rpc_latency(5, 10, 0, false, "success");
    }
}
