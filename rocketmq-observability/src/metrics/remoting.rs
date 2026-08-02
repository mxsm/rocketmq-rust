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

pub use crate::semantic::metrics::RPC_LATENCY;
pub use crate::semantic::metrics::TRANSPORT_LIFECYCLE_EVENTS_TOTAL;
pub use crate::semantic::metrics::TRANSPORT_LIFECYCLE_LISTENER_LATENCY;
pub use crate::semantic::metrics::TRANSPORT_NETWORK_BYTES;
pub use crate::semantic::metrics::TRANSPORT_REQUESTS_TOTAL;
pub use crate::semantic::metrics::TRANSPORT_REQUEST_LATENCY;

use std::time::Duration;
use std::time::Instant;

const NO_RESPONSE_CODE: i32 = -1;
const RESULT_ONEWAY: &str = "oneway";
const RESULT_SUCCESS: &str = "success";
const RESULT_CANCELED: &str = "cancelled";
const RESULT_PROCESS_REQUEST_FAILED: &str = "process_request_failed";
const RESULT_WRITE_CHANNEL_FAILED: &str = "write_channel_failed";

#[inline]
fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

/// Records the metrics for one remoting request against one explicit telemetry runtime.
///
/// The guard records request volume and inbound bytes when it starts. Dropping it records request
/// latency and, unless a terminal outcome was selected explicitly, one cancellation outcome.
pub struct RequestMetricsGuard {
    metrics: RemotingMetrics,
    start: Instant,
    request_code: i32,
    is_long_polling: bool,
    rpc_recorded: bool,
}

impl RequestMetricsGuard {
    #[inline]
    pub fn start(metrics: RemotingMetrics, request_code: i32, request_bytes: u64, is_long_polling: bool) -> Self {
        metrics.record_requests_total(1);
        metrics.record_network_bytes(request_bytes);

        Self {
            metrics,
            start: Instant::now(),
            request_code,
            is_long_polling,
            rpc_recorded: false,
        }
    }

    #[inline]
    pub fn complete_response(&mut self, response_code: i32) {
        self.record_rpc_latency(response_code, RESULT_SUCCESS);
    }

    #[inline]
    pub fn complete_oneway(&mut self) {
        self.record_rpc_latency(NO_RESPONSE_CODE, RESULT_ONEWAY);
    }

    #[inline]
    pub fn complete_cancelled(&mut self) {
        self.record_rpc_latency(NO_RESPONSE_CODE, RESULT_CANCELED);
    }

    #[inline]
    pub fn complete_process_request_failed(&mut self, response_code: i32) {
        self.record_rpc_latency(response_code, RESULT_PROCESS_REQUEST_FAILED);
    }

    #[inline]
    pub fn complete_write_channel_failed(&mut self, response_code: i32) {
        self.record_rpc_latency(response_code, RESULT_WRITE_CHANNEL_FAILED);
    }

    #[inline]
    fn record_rpc_latency(&mut self, response_code: i32, result: &'static str) {
        if self.rpc_recorded {
            return;
        }
        self.metrics.record_rpc_latency(
            duration_millis_u64(self.start.elapsed()),
            self.request_code,
            response_code,
            self.is_long_polling,
            result,
        );
        self.rpc_recorded = true;
    }
}

impl Drop for RequestMetricsGuard {
    fn drop(&mut self) {
        self.metrics
            .record_request_latency(duration_millis_u64(self.start.elapsed()));
        if !self.rpc_recorded {
            self.complete_cancelled();
        }
    }
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Default)]
pub struct RemotingMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl RemotingMetrics {
    /// Creates an instance-scoped no-op recorder.
    #[must_use]
    pub const fn noop() -> Self {
        Self
    }

    /// Creates a no-op recorder without reading process-global OpenTelemetry state.
    #[must_use]
    pub fn from_handle(_telemetry: &crate::TelemetryHandle) -> Self {
        Self::noop()
    }

    #[inline]
    pub fn record_requests_total(&self, _count: u64) {}

    #[inline]
    pub fn record_request_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_network_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_lifecycle_event(&self, _event: &'static str, _result: &'static str) {}

    #[inline]
    pub fn record_lifecycle_listener_latency(&self, _latency_ms: u64, _event: &'static str) {}

    #[inline]
    pub fn record_rpc_latency(
        &self,
        _latency_ms: u64,
        _request_code: i32,
        _response_code: i32,
        _is_long_polling: bool,
        _result: &'static str,
    ) {
    }
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone, Default)]
pub struct RemotingMetrics {
    telemetry: Option<crate::TelemetryHandle>,
    instruments: Option<RemotingMetricInstruments>,
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
struct RemotingMetricInstruments {
    requests_total: opentelemetry::metrics::Counter<u64>,
    request_latency: opentelemetry::metrics::Histogram<u64>,
    network_bytes: opentelemetry::metrics::Counter<u64>,
    lifecycle_events: opentelemetry::metrics::Counter<u64>,
    lifecycle_listener_latency: opentelemetry::metrics::Histogram<u64>,
    rpc_latency: opentelemetry::metrics::Histogram<u64>,
}

#[cfg(feature = "otel-metrics")]
impl RemotingMetrics {
    /// Creates an instance-scoped recorder that never emits metrics.
    #[must_use]
    pub fn noop() -> Self {
        Self::default()
    }

    /// Creates a recorder bound to the handle's fixed transport meter.
    ///
    /// A no-op, closing, or closed handle produces a no-op recorder. This method never reads the
    /// process-global OpenTelemetry meter provider.
    #[must_use]
    pub fn from_handle(telemetry: &crate::TelemetryHandle) -> Self {
        let Some(meter) = telemetry.meter(crate::TRANSPORT_METER_SCOPE) else {
            return Self::noop();
        };
        Self {
            telemetry: Some(telemetry.clone()),
            instruments: Some(RemotingMetricInstruments::new(&meter)),
        }
    }

    /// Creates a recorder from an explicitly supplied meter.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            telemetry: None,
            instruments: Some(RemotingMetricInstruments::new(meter)),
        }
    }

    #[inline]
    fn is_active(&self) -> bool {
        self.telemetry.as_ref().is_none_or(crate::TelemetryHandle::is_active)
    }

    #[inline]
    pub fn record_requests_total(&self, count: u64) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.requests_total.add(count, &[]);
            }
        }
    }

    #[inline]
    pub fn record_request_latency(&self, latency_ms: u64) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.request_latency.record(latency_ms, &[]);
            }
        }
    }

    #[inline]
    pub fn record_network_bytes(&self, bytes: u64) {
        if bytes != 0 && self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.network_bytes.add(bytes, &[]);
            }
        }
    }

    #[inline]
    pub fn record_lifecycle_event(&self, event: &'static str, result: &'static str) {
        if !self.is_active() {
            return;
        }
        if let Some(instruments) = &self.instruments {
            instruments.lifecycle_events.add(
                1,
                &[
                    opentelemetry::KeyValue::new(crate::semantic::labels::EVENT, event),
                    opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result),
                ],
            );
        }
    }

    #[inline]
    pub fn record_lifecycle_listener_latency(&self, latency_ms: u64, event: &'static str) {
        if !self.is_active() {
            return;
        }
        if let Some(instruments) = &self.instruments {
            instruments.lifecycle_listener_latency.record(
                latency_ms,
                &[opentelemetry::KeyValue::new(crate::semantic::labels::EVENT, event)],
            );
        }
    }

    #[inline]
    pub fn record_rpc_latency(
        &self,
        latency_ms: u64,
        request_code: i32,
        response_code: i32,
        is_long_polling: bool,
        result: &'static str,
    ) {
        if !self.is_active() {
            return;
        }
        let Some(instruments) = &self.instruments else {
            return;
        };
        let attributes = [
            opentelemetry::KeyValue::new(crate::semantic::labels::PROTOCOL_TYPE, "remoting"),
            opentelemetry::KeyValue::new(crate::semantic::labels::REQUEST_CODE, i64::from(request_code)),
            opentelemetry::KeyValue::new(crate::semantic::labels::RESPONSE_CODE, i64::from(response_code)),
            opentelemetry::KeyValue::new(crate::semantic::labels::IS_LONG_POLLING, is_long_polling),
            opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result),
        ];
        instruments.rpc_latency.record(latency_ms, &attributes);
    }
}

#[cfg(feature = "otel-metrics")]
impl RemotingMetricInstruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let requests_total = meter
            .u64_counter(TRANSPORT_REQUESTS_TOTAL)
            .with_description("Total number of remoting requests")
            .with_unit("{request}")
            .build();

        let request_latency = meter
            .u64_histogram(TRANSPORT_REQUEST_LATENCY)
            .with_description("Remoting request latency")
            .with_unit("ms")
            .build();

        let network_bytes = meter
            .u64_counter(TRANSPORT_NETWORK_BYTES)
            .with_description("Total network bytes processed by remoting")
            .with_unit("By")
            .build();

        let lifecycle_events = meter
            .u64_counter(TRANSPORT_LIFECYCLE_EVENTS_TOTAL)
            .with_description("Connection lifecycle events by enqueue and delivery result")
            .with_unit("{event}")
            .build();

        let lifecycle_listener_latency = meter
            .u64_histogram(TRANSPORT_LIFECYCLE_LISTENER_LATENCY)
            .with_description("Connection lifecycle listener callback latency")
            .with_unit("ms")
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
            lifecycle_events,
            lifecycle_listener_latency,
            rpc_latency,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_request_metrics_guard_accepts_each_terminal_outcome_once() {
        let metrics = RemotingMetrics::from_handle(&crate::TelemetryHandle::noop());

        let mut success = RequestMetricsGuard::start(metrics.clone(), 10, 128, false);
        success.complete_response(0);
        success.complete_cancelled();

        let mut process_failure = RequestMetricsGuard::start(metrics.clone(), 11, 64, true);
        process_failure.complete_process_request_failed(1);
        process_failure.complete_response(0);

        let mut write_failure = RequestMetricsGuard::start(metrics.clone(), 12, 32, false);
        write_failure.complete_write_channel_failed(2);
        write_failure.complete_oneway();

        drop(RequestMetricsGuard::start(metrics, 13, 16, false));
    }

    #[cfg(feature = "otel-metrics")]
    #[test]
    fn remoting_metrics_constructs_and_records() {
        use opentelemetry::metrics::MeterProvider;

        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
        let meter = provider.meter("remoting-metrics-test");
        let metrics = RemotingMetrics::new(&meter);

        metrics.record_requests_total(1);
        metrics.record_request_latency(3);
        metrics.record_network_bytes(256);
        metrics.record_lifecycle_event("connected", "queued");
        metrics.record_lifecycle_listener_latency(2, "connected");
        metrics.record_rpc_latency(5, 10, 0, false, RESULT_SUCCESS);
    }
}
