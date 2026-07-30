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
pub use crate::semantic::metrics::NAMESRV_ROUTE_ERRORS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_ROUTE_FRESHNESS;
pub use crate::semantic::metrics::NAMESRV_ROUTE_REQUEST_LATENCY;
pub use crate::semantic::metrics::NAMESRV_ROUTE_REQUEST_TOTAL;

use std::time::Duration;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerRouteErrorKind {
    NotFound,
    Rejected,
    Internal,
}

impl NameServerRouteErrorKind {
    #[cfg(feature = "otel-metrics")]
    const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::Rejected => "rejected",
            Self::Internal => "internal",
        }
    }
}

pub fn record_route_error(kind: NameServerRouteErrorKind) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_route_errors_total(
        1,
        &[opentelemetry::KeyValue::new(
            crate::semantic::labels::RESULT,
            kind.as_str(),
        )],
    );

    #[cfg(not(feature = "otel-metrics"))]
    let _ = kind;
}

pub fn record_route_freshness(freshness_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_route_freshness(freshness_ms, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = freshness_ms;
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

pub fn record_route_request(elapsed: Duration) {
    record_route_request_total(1);
    record_route_request_latency(duration_millis_u64(elapsed));
}

pub fn record_broker_registration(active_brokers: usize) {
    record_broker_registrations(1);
    record_active_brokers(active_brokers as u64);
}

pub fn record_active_broker_count(active_brokers: usize) {
    record_active_brokers(active_brokers as u64);
}

#[inline]
fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().clamp(0, u128::from(u64::MAX)) as u64
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Default)]
pub struct NameServerMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl NameServerMetrics {
    pub fn noop() -> Self {
        Self
    }

    pub fn from_handle(_telemetry: &crate::TelemetryHandle) -> Self {
        Self::noop()
    }

    #[inline]
    pub fn record_route_request_total(&self, _count: u64) {}

    #[inline]
    pub fn record_route_request_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_broker_registrations(&self, _count: u64) {}

    #[inline]
    pub fn record_active_brokers(&self, _count: u64) {}

    #[inline]
    pub fn record_route_errors_total(&self, _count: u64, _attributes: &[()]) {}

    #[inline]
    pub fn record_route_freshness(&self, _freshness_ms: u64, _attributes: &[()]) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
struct NameServerMetricInstruments {
    route_request_total: opentelemetry::metrics::Counter<u64>,
    route_request_latency: opentelemetry::metrics::Histogram<u64>,
    broker_registrations: opentelemetry::metrics::Counter<u64>,
    active_brokers: opentelemetry::metrics::Gauge<u64>,
    route_errors_total: opentelemetry::metrics::Counter<u64>,
    route_freshness: opentelemetry::metrics::Histogram<u64>,
}

#[cfg(feature = "otel-metrics")]
impl NameServerMetrics {
    pub fn noop() -> Self {
        Self::default()
    }

    pub fn from_handle(telemetry: &crate::TelemetryHandle) -> Self {
        let Some(meter) = telemetry.meter(crate::NAMESRV_METER_SCOPE) else {
            return Self::noop();
        };
        Self {
            telemetry: Some(telemetry.clone()),
            instruments: Some(NameServerMetricInstruments::new(&meter)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            telemetry: None,
            instruments: Some(NameServerMetricInstruments::new(meter)),
        }
    }

    fn is_active(&self) -> bool {
        self.telemetry.as_ref().is_none_or(crate::TelemetryHandle::is_active)
    }

    #[inline]
    pub fn record_route_request_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.route_request_total.add(count, attributes);
            }
        }
    }

    #[inline]
    pub fn record_route_request_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.route_request_latency.record(latency_ms, attributes);
            }
        }
    }

    #[inline]
    pub fn record_broker_registrations(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.broker_registrations.add(count, attributes);
            }
        }
    }

    #[inline]
    pub fn record_active_brokers(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.active_brokers.record(count, attributes);
            }
        }
    }

    pub fn record_route_request(&self, elapsed: Duration) {
        self.record_route_request_total(1, &[]);
        self.record_route_request_latency(duration_millis_u64(elapsed), &[]);
    }

    pub fn record_broker_registration(&self, active_brokers: usize) {
        self.record_broker_registrations(1, &[]);
        self.record_active_brokers(active_brokers as u64, &[]);
    }

    pub fn record_active_broker_count(&self, active_brokers: usize) {
        self.record_active_brokers(active_brokers as u64, &[]);
    }
}

#[cfg(feature = "otel-metrics")]
impl NameServerMetricInstruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
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
        let route_errors_total = meter
            .u64_counter(NAMESRV_ROUTE_ERRORS_TOTAL)
            .with_description("NameServer route lookup errors grouped by bounded result")
            .with_unit("{error}")
            .build();
        let route_freshness = meter
            .u64_histogram(NAMESRV_ROUTE_FRESHNESS)
            .with_description("Age of the oldest live broker entry used by a route lookup")
            .with_unit("ms")
            .build();

        Self {
            route_request_total,
            route_request_latency,
            broker_registrations,
            active_brokers,
            route_errors_total,
            route_freshness,
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

    #[inline]
    pub fn record_route_errors_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.route_errors_total.add(count, attributes);
    }

    #[inline]
    pub fn record_route_freshness(&self, freshness_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.route_freshness.record(freshness_ms, attributes);
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
        metrics.record_route_errors_total(1, &attrs);
        metrics.record_route_freshness(25, &attrs);
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

#[cfg(test)]
mod helper_tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn namesrv_high_level_recorders_are_safe_without_explicit_meter() {
        record_route_request(Duration::from_millis(1));
        record_broker_registration(2);
        record_active_broker_count(2);
        record_route_error(NameServerRouteErrorKind::NotFound);
        record_route_freshness(25);
    }
}
