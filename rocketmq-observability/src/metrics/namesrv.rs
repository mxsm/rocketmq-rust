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
pub use crate::semantic::metrics::NAMESRV_EXPIRY_SCAN_BROKERS;
pub use crate::semantic::metrics::NAMESRV_EXPIRY_SCAN_DURATION;
pub use crate::semantic::metrics::NAMESRV_REGISTRATION_DIRTY_TOPICS;
pub use crate::semantic::metrics::NAMESRV_REGISTRATION_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_ROUTE_CACHE_BYTES;
pub use crate::semantic::metrics::NAMESRV_ROUTE_CACHE_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_ROUTE_END_TO_END_LATENCY;
pub use crate::semantic::metrics::NAMESRV_ROUTE_ERRORS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_ROUTE_FRESHNESS;
pub use crate::semantic::metrics::NAMESRV_ROUTE_FRESHNESS_SAMPLED_TOTAL;
pub use crate::semantic::metrics::NAMESRV_ROUTE_REQUEST_LATENCY;
pub use crate::semantic::metrics::NAMESRV_ROUTE_REQUEST_TOTAL;
pub use crate::semantic::metrics::NAMESRV_ROUTE_RESPONSE_BYTES;
pub use crate::semantic::metrics::NAMESRV_ROUTE_RESPONSE_WRITE_ERRORS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_ROUTE_RESPONSE_WRITE_LATENCY;
pub use crate::semantic::metrics::NAMESRV_ROUTE_STAGE_LATENCY;
pub use crate::semantic::metrics::NAMESRV_UNREGISTRATION_BATCH_SIZE;
pub use crate::semantic::metrics::NAMESRV_UNREGISTRATION_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_UNREGISTRATION_QUEUE_DEPTH;
pub use crate::semantic::metrics::NAMESRV_WORKLOAD_ADMISSION_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_WORKLOAD_ADMISSION_INFLIGHT;
pub use crate::semantic::metrics::NAMESRV_WORKLOAD_ADMISSION_WAITING;

use std::time::Duration;

#[cfg(feature = "otel-metrics")]
use std::sync::atomic::AtomicU64;
#[cfg(feature = "otel-metrics")]
use std::sync::atomic::Ordering;
#[cfg(feature = "otel-metrics")]
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerRouteErrorKind {
    NotFound,
    Rejected,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerRouteStage {
    ZoneFilter,
    Encode,
    LegacyZoneHook,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerRouteCacheOutcome {
    Hit,
    Miss,
    Bypass,
    Oversize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerWorkloadClass {
    RouteRead,
    BrokerControl,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerAdmissionOutcome {
    Acquired,
    Queued,
    Released,
    Rejected,
    TimedOut,
    ObserveSaturated,
}

#[cfg(feature = "otel-metrics")]
impl NameServerRouteStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ZoneFilter => "zone-filter",
            Self::Encode => "encode",
            Self::LegacyZoneHook => "legacy-zone-hook",
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl NameServerRouteCacheOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Bypass => "bypass",
            Self::Oversize => "oversize",
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl NameServerWorkloadClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RouteRead => "route-read",
            Self::BrokerControl => "broker-control",
            Self::Admin => "admin",
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl NameServerAdmissionOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Acquired => "acquired",
            Self::Queued => "queued",
            Self::Released => "released",
            Self::Rejected => "rejected",
            Self::TimedOut => "timeout",
            Self::ObserveSaturated => "observe-saturated",
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl NameServerRouteErrorKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::Rejected => "rejected",
            Self::Internal => "internal",
        }
    }
}

#[inline]
#[cfg(feature = "otel-metrics")]
fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().clamp(0, u128::from(u64::MAX)) as u64
}

#[inline]
#[cfg(feature = "otel-metrics")]
fn duration_micros_u64(duration: Duration) -> u64 {
    duration.as_micros().clamp(0, u128::from(u64::MAX)) as u64
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
    pub fn is_enabled(&self) -> bool {
        false
    }

    #[inline]
    pub fn should_record_route_freshness(&self, _sample_interval: u64) -> bool {
        false
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
    pub fn record_route_request(&self, _elapsed: Duration) {}

    #[inline]
    pub fn record_broker_registration(&self, _active_brokers: usize) {}

    #[inline]
    pub fn record_active_broker_count(&self, _active_brokers: usize) {}

    #[inline]
    pub fn record_route_error(&self, _kind: NameServerRouteErrorKind) {}

    #[inline]
    pub fn record_route_freshness(&self, _freshness_ms: u64) {}

    #[inline]
    pub fn record_route_freshness_sampled(&self) {}

    #[inline]
    pub fn record_route_stage(&self, _stage: NameServerRouteStage, _elapsed: Duration) {}

    #[inline]
    pub fn record_route_response_bytes(&self, _bytes: usize) {}

    #[inline]
    pub fn record_route_cache(&self, _outcome: NameServerRouteCacheOutcome, _current_bytes: u64) {}

    #[inline]
    pub fn record_workload_admission(
        &self,
        _class: NameServerWorkloadClass,
        _outcome: NameServerAdmissionOutcome,
        _inflight: usize,
        _waiting: usize,
    ) {
    }

    #[inline]
    pub fn record_route_response_write(&self, _write: Duration, _end_to_end: Duration, _success: bool) {}

    #[inline]
    pub fn record_registration_delta(&self, _outcome: &'static str, _dirty_topics: usize) {}

    #[inline]
    pub fn record_unregistration_queue(&self, _outcome: &'static str, _depth: usize) {}

    #[inline]
    pub fn record_unregistration_batch(&self, _size: usize) {}

    #[inline]
    pub fn record_expiry_scan(&self, _mode: &'static str, _examined: usize, _expired: usize, _elapsed: Duration) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone, Default)]
pub struct NameServerMetrics {
    telemetry: Option<crate::TelemetryHandle>,
    instruments: Option<NameServerMetricInstruments>,
    route_freshness_sequence: Arc<AtomicU64>,
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
    route_freshness_sampled_total: opentelemetry::metrics::Counter<u64>,
    route_stage_latency: opentelemetry::metrics::Histogram<u64>,
    route_response_bytes: opentelemetry::metrics::Histogram<u64>,
    route_cache_events_total: opentelemetry::metrics::Counter<u64>,
    route_cache_bytes: opentelemetry::metrics::Gauge<u64>,
    workload_admission_events_total: opentelemetry::metrics::Counter<u64>,
    workload_admission_inflight: opentelemetry::metrics::Gauge<u64>,
    workload_admission_waiting: opentelemetry::metrics::Gauge<u64>,
    route_response_write_latency: opentelemetry::metrics::Histogram<u64>,
    route_end_to_end_latency: opentelemetry::metrics::Histogram<u64>,
    route_response_write_errors_total: opentelemetry::metrics::Counter<u64>,
    registration_events_total: opentelemetry::metrics::Counter<u64>,
    registration_dirty_topics: opentelemetry::metrics::Histogram<u64>,
    unregistration_events_total: opentelemetry::metrics::Counter<u64>,
    unregistration_queue_depth: opentelemetry::metrics::Gauge<u64>,
    unregistration_batch_size: opentelemetry::metrics::Histogram<u64>,
    expiry_scan_brokers: opentelemetry::metrics::Histogram<u64>,
    expiry_scan_duration: opentelemetry::metrics::Histogram<u64>,
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
            route_freshness_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            telemetry: None,
            instruments: Some(NameServerMetricInstruments::new(meter)),
            route_freshness_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    fn is_active(&self) -> bool {
        self.telemetry.as_ref().is_none_or(crate::TelemetryHandle::is_active)
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.instruments.is_some() && self.is_active()
    }

    /// Returns true only for requests selected by the bounded freshness sampler.
    ///
    /// Call this before looking up broker live entries so disabled metrics and
    /// discarded samples add no hash-table or allocation work to route queries.
    #[inline]
    pub fn should_record_route_freshness(&self, sample_interval: u64) -> bool {
        self.is_enabled()
            && self
                .route_freshness_sequence
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(sample_interval.max(1))
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

    pub fn record_route_error(&self, kind: NameServerRouteErrorKind) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.record_route_errors_total(
                    1,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::RESULT,
                        kind.as_str(),
                    )],
                );
            }
        }
    }

    pub fn record_route_freshness(&self, freshness_ms: u64) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.record_route_freshness(freshness_ms, &[]);
            }
        }
    }

    pub fn record_route_freshness_sampled(&self) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.route_freshness_sampled_total.add(1, &[]);
            }
        }
    }

    pub fn record_route_stage(&self, stage: NameServerRouteStage, elapsed: Duration) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.route_stage_latency.record(
                    duration_micros_u64(elapsed),
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::STAGE,
                        stage.as_str(),
                    )],
                );
            }
        }
    }

    pub fn record_route_response_bytes(&self, bytes: usize) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.route_response_bytes.record(bytes as u64, &[]);
            }
        }
    }

    pub fn record_route_cache(&self, outcome: NameServerRouteCacheOutcome, current_bytes: u64) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.route_cache_events_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::RESULT,
                        outcome.as_str(),
                    )],
                );
                instruments.route_cache_bytes.record(current_bytes, &[]);
            }
        }
    }

    pub fn record_workload_admission(
        &self,
        class: NameServerWorkloadClass,
        outcome: NameServerAdmissionOutcome,
        inflight: usize,
        waiting: usize,
    ) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                let class_attribute =
                    opentelemetry::KeyValue::new(crate::semantic::labels::REQUEST_TYPE, class.as_str());
                instruments.workload_admission_events_total.add(
                    1,
                    &[
                        class_attribute.clone(),
                        opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, outcome.as_str()),
                    ],
                );
                instruments
                    .workload_admission_inflight
                    .record(inflight as u64, std::slice::from_ref(&class_attribute));
                instruments
                    .workload_admission_waiting
                    .record(waiting as u64, &[class_attribute]);
            }
        }
    }

    pub fn record_route_response_write(&self, write: Duration, end_to_end: Duration, success: bool) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                let result = if success { "success" } else { "failed" };
                let attributes = [opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result)];
                instruments
                    .route_response_write_latency
                    .record(duration_micros_u64(write), &attributes);
                instruments
                    .route_end_to_end_latency
                    .record(duration_micros_u64(end_to_end), &attributes);
                if !success {
                    instruments.route_response_write_errors_total.add(1, &[]);
                }
            }
        }
    }

    pub fn record_registration_delta(&self, outcome: &'static str, dirty_topics: usize) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.registration_events_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, outcome)],
                );
                instruments.registration_dirty_topics.record(dirty_topics as u64, &[]);
            }
        }
    }

    pub fn record_unregistration_queue(&self, outcome: &'static str, depth: usize) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.unregistration_events_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, outcome)],
                );
                instruments.unregistration_queue_depth.record(depth as u64, &[]);
            }
        }
    }

    pub fn record_unregistration_batch(&self, size: usize) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.unregistration_batch_size.record(size as u64, &[]);
            }
        }
    }

    pub fn record_expiry_scan(&self, mode: &'static str, examined: usize, expired: usize, elapsed: Duration) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.expiry_scan_brokers.record(
                    examined as u64,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::RESULT,
                        format!("{mode}-examined"),
                    )],
                );
                instruments.expiry_scan_brokers.record(
                    expired as u64,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::RESULT,
                        format!("{mode}-expired"),
                    )],
                );
                instruments.expiry_scan_duration.record(
                    duration_micros_u64(elapsed),
                    &[opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, mode)],
                );
            }
        }
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
        let route_freshness_sampled_total = meter
            .u64_counter(NAMESRV_ROUTE_FRESHNESS_SAMPLED_TOTAL)
            .with_description("Number of NameServer route freshness samples selected")
            .with_unit("{sample}")
            .build();
        let route_stage_latency = meter
            .u64_histogram(NAMESRV_ROUTE_STAGE_LATENCY)
            .with_description("NameServer route typed-filter and encoding stage latency")
            .with_unit("us")
            .build();
        let route_response_bytes = meter
            .u64_histogram(NAMESRV_ROUTE_RESPONSE_BYTES)
            .with_description("Encoded NameServer route response body size")
            .with_unit("By")
            .build();
        let route_cache_events_total = meter
            .u64_counter(NAMESRV_ROUTE_CACHE_EVENTS_TOTAL)
            .with_description("NameServer route response cache outcomes")
            .with_unit("{event}")
            .build();
        let route_cache_bytes = meter
            .u64_gauge(NAMESRV_ROUTE_CACHE_BYTES)
            .with_description("Current weighted NameServer route response cache size")
            .with_unit("By")
            .build();
        let workload_admission_events_total = meter
            .u64_counter(NAMESRV_WORKLOAD_ADMISSION_EVENTS_TOTAL)
            .with_description("NameServer semantic workload admission outcomes")
            .with_unit("{event}")
            .build();
        let workload_admission_inflight = meter
            .u64_gauge(NAMESRV_WORKLOAD_ADMISSION_INFLIGHT)
            .with_description("Current NameServer requests holding semantic admission permits")
            .with_unit("{request}")
            .build();
        let workload_admission_waiting = meter
            .u64_gauge(NAMESRV_WORKLOAD_ADMISSION_WAITING)
            .with_description("Current NameServer requests waiting for semantic admission permits")
            .with_unit("{request}")
            .build();
        let route_response_write_latency = meter
            .u64_histogram(NAMESRV_ROUTE_RESPONSE_WRITE_LATENCY)
            .with_description("NameServer route response dispatch latency to the transport channel")
            .with_unit("us")
            .build();
        let route_end_to_end_latency = meter
            .u64_histogram(NAMESRV_ROUTE_END_TO_END_LATENCY)
            .with_description("NameServer route latency from transport dispatch through response channel completion")
            .with_unit("us")
            .build();
        let route_response_write_errors_total = meter
            .u64_counter(NAMESRV_ROUTE_RESPONSE_WRITE_ERRORS_TOTAL)
            .with_description("NameServer route response channel write failures")
            .with_unit("{error}")
            .build();
        let registration_events_total = meter
            .u64_counter(NAMESRV_REGISTRATION_EVENTS_TOTAL)
            .with_description("NameServer registration outcomes")
            .with_unit("{event}")
            .build();
        let registration_dirty_topics = meter
            .u64_histogram(NAMESRV_REGISTRATION_DIRTY_TOPICS)
            .with_description("Number of route snapshots dirtied by one broker registration")
            .with_unit("{topic}")
            .build();
        let unregistration_events_total = meter
            .u64_counter(NAMESRV_UNREGISTRATION_EVENTS_TOTAL)
            .with_description("NameServer unregistration queue and fallback outcomes")
            .with_unit("{event}")
            .build();
        let unregistration_queue_depth = meter
            .u64_gauge(NAMESRV_UNREGISTRATION_QUEUE_DEPTH)
            .with_description("Current NameServer pending unregistration queue depth")
            .with_unit("{request}")
            .build();
        let unregistration_batch_size = meter
            .u64_histogram(NAMESRV_UNREGISTRATION_BATCH_SIZE)
            .with_description("Number of broker unregistrations handled per bounded batch")
            .with_unit("{request}")
            .build();
        let expiry_scan_brokers = meter
            .u64_histogram(NAMESRV_EXPIRY_SCAN_BROKERS)
            .with_description("Brokers examined and expired by the NameServer liveness scanner")
            .with_unit("{broker}")
            .build();
        let expiry_scan_duration = meter
            .u64_histogram(NAMESRV_EXPIRY_SCAN_DURATION)
            .with_description("NameServer broker expiry scan duration")
            .with_unit("us")
            .build();

        Self {
            route_request_total,
            route_request_latency,
            broker_registrations,
            active_brokers,
            route_errors_total,
            route_freshness,
            route_freshness_sampled_total,
            route_stage_latency,
            route_response_bytes,
            route_cache_events_total,
            route_cache_bytes,
            workload_admission_events_total,
            workload_admission_inflight,
            workload_admission_waiting,
            route_response_write_latency,
            route_end_to_end_latency,
            route_response_write_errors_total,
            registration_events_total,
            registration_dirty_topics,
            unregistration_events_total,
            unregistration_queue_depth,
            unregistration_batch_size,
            expiry_scan_brokers,
            expiry_scan_duration,
        }
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
        metrics.record_route_error(NameServerRouteErrorKind::NotFound);
        metrics.record_route_freshness(25);
    }

    #[test]
    fn namesrv_noop_recorder_is_safe_without_explicit_meter() {
        let metrics = NameServerMetrics::from_handle(&crate::TelemetryHandle::noop());
        metrics.record_route_request(Duration::from_millis(1));
        metrics.record_broker_registration(2);
        metrics.record_active_broker_count(2);
        metrics.record_route_error(NameServerRouteErrorKind::NotFound);
        metrics.record_route_freshness(25);
        assert!(!metrics.is_enabled());
        assert!(!metrics.should_record_route_freshness(1));
    }

    #[test]
    fn route_freshness_sampler_selects_only_the_configured_interval() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("namesrv-freshness-sampler-test");
        let metrics = NameServerMetrics::new(&meter);

        assert!(metrics.is_enabled());
        assert!(metrics.should_record_route_freshness(3));
        assert!(!metrics.should_record_route_freshness(3));
        assert!(!metrics.should_record_route_freshness(3));
        assert!(metrics.should_record_route_freshness(3));
    }
}
