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
pub use crate::semantic::metrics::NAMESRV_ACTIVE_CONNECTIONS;
pub use crate::semantic::metrics::NAMESRV_BROKER_REGISTRATIONS;
pub use crate::semantic::metrics::NAMESRV_CONNECTION_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_EXPIRY_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_EXPIRY_SCAN_BROKERS;
pub use crate::semantic::metrics::NAMESRV_EXPIRY_SCAN_DURATION;
pub use crate::semantic::metrics::NAMESRV_KV_BATCH_SIZE;
pub use crate::semantic::metrics::NAMESRV_KV_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_KV_GENERATION;
pub use crate::semantic::metrics::NAMESRV_KV_PERSIST_LATENCY;
pub use crate::semantic::metrics::NAMESRV_KV_QUEUE_BYTES;
pub use crate::semantic::metrics::NAMESRV_KV_QUEUE_DEPTH;
pub use crate::semantic::metrics::NAMESRV_MUTATION_LATENCY;
pub use crate::semantic::metrics::NAMESRV_REGISTRATION_BODY_BYTES;
pub use crate::semantic::metrics::NAMESRV_REGISTRATION_DECODE_LATENCY;
pub use crate::semantic::metrics::NAMESRV_REGISTRATION_DIRTY_TOPICS;
pub use crate::semantic::metrics::NAMESRV_REGISTRATION_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_REQUESTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_REQUEST_HANDLER_LATENCY;
pub use crate::semantic::metrics::NAMESRV_RESPONSE_BYTES;
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
pub use crate::semantic::metrics::NAMESRV_SECURITY_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_SNAPSHOT_REBUILDS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_UNREGISTRATION_BATCH_SIZE;
pub use crate::semantic::metrics::NAMESRV_UNREGISTRATION_EVENTS_TOTAL;
pub use crate::semantic::metrics::NAMESRV_UNREGISTRATION_OLDEST_AGE;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerRequestOutcome {
    Success,
    Rejected,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerExpiryEvent {
    IndexMismatch,
    SafetyReconcile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerKvEvent {
    Queued,
    QueueFull,
    ByteLimit,
    Closed,
    Drained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerSecurityEvent {
    AuthDenied,
    TlsReloadSuccess,
    TlsReloadFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameServerConnectionEvent {
    Admitted,
    Rejected,
    Closed,
    IdleReconnect,
    SlowWrite,
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
impl NameServerRequestOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Rejected => "rejected",
            Self::Error => "error",
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl NameServerExpiryEvent {
    const fn as_str(self) -> &'static str {
        match self {
            Self::IndexMismatch => "index-mismatch",
            Self::SafetyReconcile => "safety-reconcile",
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl NameServerKvEvent {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::QueueFull => "queue-full",
            Self::ByteLimit => "byte-limit",
            Self::Closed => "closed",
            Self::Drained => "drained",
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl NameServerSecurityEvent {
    const fn labels(self) -> (&'static str, &'static str) {
        match self {
            Self::AuthDenied => ("auth", "denied"),
            Self::TlsReloadSuccess => ("tls-reload", "success"),
            Self::TlsReloadFailed => ("tls-reload", "failed"),
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl NameServerConnectionEvent {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Rejected => "rejected",
            Self::Closed => "closed",
            Self::IdleReconnect => "idle-reconnect",
            Self::SlowWrite => "slow-write",
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

    #[inline]
    pub fn record_request(
        &self,
        _class: NameServerWorkloadClass,
        _outcome: NameServerRequestOutcome,
        _elapsed: Duration,
        _response_bytes: usize,
    ) {
    }

    #[inline]
    pub fn record_registration_decode(&self, _wire_bytes: usize, _decoded_bytes: Option<usize>, _elapsed: Duration) {}

    #[inline]
    pub fn record_mutation_wait(&self, _elapsed: Duration) {}

    #[inline]
    pub fn record_mutation_hold(&self, _elapsed: Duration) {}

    #[inline]
    pub fn record_snapshot_rebuild(&self, _elapsed: Duration, _present: bool) {}

    #[inline]
    pub fn record_expiry_event(&self, _event: NameServerExpiryEvent) {}

    #[inline]
    pub fn record_unregistration_oldest_age(&self, _age: Duration) {}

    #[inline]
    pub fn record_kv_snapshot(
        &self,
        _desired: u64,
        _durable: u64,
        _applied: u64,
        _queue_depth: usize,
        _queue_bytes: usize,
    ) {
    }

    #[inline]
    pub fn record_kv_persist(&self, _elapsed: Duration, _success: bool, _batch_size: usize) {}

    #[inline]
    pub fn record_kv_event(&self, _event: NameServerKvEvent) {}

    #[inline]
    pub fn record_security_event(&self, _event: NameServerSecurityEvent) {}

    #[inline]
    pub fn record_connection_event(&self, _event: NameServerConnectionEvent, _active: usize) {}
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
    requests_total: opentelemetry::metrics::Counter<u64>,
    request_handler_latency: opentelemetry::metrics::Histogram<u64>,
    response_bytes: opentelemetry::metrics::Histogram<u64>,
    registration_body_bytes: opentelemetry::metrics::Histogram<u64>,
    registration_decode_latency: opentelemetry::metrics::Histogram<u64>,
    mutation_latency: opentelemetry::metrics::Histogram<u64>,
    snapshot_rebuilds_total: opentelemetry::metrics::Counter<u64>,
    expiry_events_total: opentelemetry::metrics::Counter<u64>,
    unregistration_oldest_age: opentelemetry::metrics::Gauge<u64>,
    kv_generation: opentelemetry::metrics::Gauge<u64>,
    kv_queue_depth: opentelemetry::metrics::Gauge<u64>,
    kv_queue_bytes: opentelemetry::metrics::Gauge<u64>,
    kv_persist_latency: opentelemetry::metrics::Histogram<u64>,
    kv_batch_size: opentelemetry::metrics::Histogram<u64>,
    kv_events_total: opentelemetry::metrics::Counter<u64>,
    security_events_total: opentelemetry::metrics::Counter<u64>,
    connection_events_total: opentelemetry::metrics::Counter<u64>,
    active_connections: opentelemetry::metrics::Gauge<u64>,
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

    pub fn record_request(
        &self,
        class: NameServerWorkloadClass,
        outcome: NameServerRequestOutcome,
        elapsed: Duration,
        response_bytes: usize,
    ) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                let class = opentelemetry::KeyValue::new(crate::semantic::labels::REQUEST_TYPE, class.as_str());
                let result = opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, outcome.as_str());
                instruments.requests_total.add(1, &[class.clone(), result.clone()]);
                instruments
                    .request_handler_latency
                    .record(duration_micros_u64(elapsed), &[class.clone(), result]);
                instruments.response_bytes.record(response_bytes as u64, &[class]);
            }
        }
    }

    pub fn record_registration_decode(&self, wire_bytes: usize, decoded_bytes: Option<usize>, elapsed: Duration) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.registration_body_bytes.record(
                    wire_bytes as u64,
                    &[opentelemetry::KeyValue::new(crate::semantic::labels::STAGE, "wire")],
                );
                if let Some(decoded_bytes) = decoded_bytes {
                    instruments.registration_body_bytes.record(
                        decoded_bytes as u64,
                        &[opentelemetry::KeyValue::new(crate::semantic::labels::STAGE, "decoded")],
                    );
                }
                instruments
                    .registration_decode_latency
                    .record(duration_micros_u64(elapsed), &[]);
            }
        }
    }

    pub fn record_mutation_wait(&self, elapsed: Duration) {
        self.record_mutation_stage("wait", elapsed);
    }

    pub fn record_mutation_hold(&self, elapsed: Duration) {
        self.record_mutation_stage("hold", elapsed);
    }

    fn record_mutation_stage(&self, stage: &'static str, elapsed: Duration) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.mutation_latency.record(
                    duration_micros_u64(elapsed),
                    &[opentelemetry::KeyValue::new(crate::semantic::labels::STAGE, stage)],
                );
            }
        }
    }

    pub fn record_snapshot_rebuild(&self, elapsed: Duration, present: bool) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.snapshot_rebuilds_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::RESULT,
                        if present { "published" } else { "deleted" },
                    )],
                );
                instruments.mutation_latency.record(
                    duration_micros_u64(elapsed),
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::STAGE,
                        "snapshot-rebuild",
                    )],
                );
            }
        }
    }

    pub fn record_expiry_event(&self, event: NameServerExpiryEvent) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.expiry_events_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::RESULT,
                        event.as_str(),
                    )],
                );
            }
        }
    }

    pub fn record_unregistration_oldest_age(&self, age: Duration) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments
                    .unregistration_oldest_age
                    .record(duration_millis_u64(age), &[]);
            }
        }
    }

    pub fn record_kv_snapshot(&self, desired: u64, durable: u64, applied: u64, queue_depth: usize, queue_bytes: usize) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                for (state, generation) in [("desired", desired), ("durable", durable), ("applied", applied)] {
                    instruments.kv_generation.record(
                        generation,
                        &[opentelemetry::KeyValue::new(crate::semantic::labels::STATE, state)],
                    );
                }
                instruments.kv_queue_depth.record(queue_depth as u64, &[]);
                instruments.kv_queue_bytes.record(queue_bytes as u64, &[]);
            }
        }
    }

    pub fn record_kv_persist(&self, elapsed: Duration, success: bool, batch_size: usize) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                let result = if success { "success" } else { "failed" };
                instruments.kv_persist_latency.record(
                    duration_micros_u64(elapsed),
                    &[opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result)],
                );
                instruments.kv_batch_size.record(batch_size as u64, &[]);
            }
        }
    }

    pub fn record_kv_event(&self, event: NameServerKvEvent) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.kv_events_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::RESULT,
                        event.as_str(),
                    )],
                );
            }
        }
    }

    pub fn record_security_event(&self, event: NameServerSecurityEvent) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                let (operation, result) = event.labels();
                instruments.security_events_total.add(
                    1,
                    &[
                        opentelemetry::KeyValue::new(crate::semantic::labels::OPERATION, operation),
                        opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result),
                    ],
                );
            }
        }
    }

    pub fn record_connection_event(&self, event: NameServerConnectionEvent, active: usize) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.connection_events_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::EVENT,
                        event.as_str(),
                    )],
                );
                instruments.active_connections.record(active as u64, &[]);
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
        let requests_total = meter
            .u64_counter(NAMESRV_REQUESTS_TOTAL)
            .with_description("NameServer requests grouped by bounded workload class and outcome")
            .with_unit("{request}")
            .build();
        let request_handler_latency = meter
            .u64_histogram(NAMESRV_REQUEST_HANDLER_LATENCY)
            .with_description("NameServer request handler latency excluding response channel completion")
            .with_unit("us")
            .build();
        let response_bytes = meter
            .u64_histogram(NAMESRV_RESPONSE_BYTES)
            .with_description("NameServer response body bytes grouped by bounded workload class")
            .with_unit("By")
            .build();
        let registration_body_bytes = meter
            .u64_histogram(NAMESRV_REGISTRATION_BODY_BYTES)
            .with_description("NameServer broker registration wire and decoded body bytes")
            .with_unit("By")
            .build();
        let registration_decode_latency = meter
            .u64_histogram(NAMESRV_REGISTRATION_DECODE_LATENCY)
            .with_description("NameServer broker registration body decode latency")
            .with_unit("us")
            .build();
        let mutation_latency = meter
            .u64_histogram(NAMESRV_MUTATION_LATENCY)
            .with_description("NameServer route mutation wait, hold, and snapshot rebuild latency")
            .with_unit("us")
            .build();
        let snapshot_rebuilds_total = meter
            .u64_counter(NAMESRV_SNAPSHOT_REBUILDS_TOTAL)
            .with_description("NameServer immutable route snapshot rebuild outcomes")
            .with_unit("{snapshot}")
            .build();
        let expiry_events_total = meter
            .u64_counter(NAMESRV_EXPIRY_EVENTS_TOTAL)
            .with_description("NameServer expiry index mismatch and safety reconciliation events")
            .with_unit("{event}")
            .build();
        let unregistration_oldest_age = meter
            .u64_gauge(NAMESRV_UNREGISTRATION_OLDEST_AGE)
            .with_description("Age of the oldest NameServer pending broker unregistration")
            .with_unit("ms")
            .build();
        let kv_generation = meter
            .u64_gauge(NAMESRV_KV_GENERATION)
            .with_description("NameServer KV desired, durable, and applied generation")
            .with_unit("{generation}")
            .build();
        let kv_queue_depth = meter
            .u64_gauge(NAMESRV_KV_QUEUE_DEPTH)
            .with_description("NameServer pending KV mutation command count")
            .with_unit("{command}")
            .build();
        let kv_queue_bytes = meter
            .u64_gauge(NAMESRV_KV_QUEUE_BYTES)
            .with_description("NameServer pending KV mutation retained bytes")
            .with_unit("By")
            .build();
        let kv_persist_latency = meter
            .u64_histogram(NAMESRV_KV_PERSIST_LATENCY)
            .with_description("NameServer KV durable snapshot persistence latency")
            .with_unit("us")
            .build();
        let kv_batch_size = meter
            .u64_histogram(NAMESRV_KV_BATCH_SIZE)
            .with_description("NameServer KV mutations committed per durable snapshot")
            .with_unit("{command}")
            .build();
        let kv_events_total = meter
            .u64_counter(NAMESRV_KV_EVENTS_TOTAL)
            .with_description("NameServer KV admission and drain outcomes")
            .with_unit("{event}")
            .build();
        let security_events_total = meter
            .u64_counter(NAMESRV_SECURITY_EVENTS_TOTAL)
            .with_description("NameServer authentication and TLS lifecycle outcomes")
            .with_unit("{event}")
            .build();
        let connection_events_total = meter
            .u64_counter(NAMESRV_CONNECTION_EVENTS_TOTAL)
            .with_description("NameServer connection admission and lifecycle outcomes")
            .with_unit("{event}")
            .build();
        let active_connections = meter
            .u64_gauge(NAMESRV_ACTIVE_CONNECTIONS)
            .with_description("Current active NameServer transport connections")
            .with_unit("{connection}")
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
            requests_total,
            request_handler_latency,
            response_bytes,
            registration_body_bytes,
            registration_decode_latency,
            mutation_latency,
            snapshot_rebuilds_total,
            expiry_events_total,
            unregistration_oldest_age,
            kv_generation,
            kv_queue_depth,
            kv_queue_bytes,
            kv_persist_latency,
            kv_batch_size,
            kv_events_total,
            security_events_total,
            connection_events_total,
            active_connections,
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
        metrics.record_registration_delta("changed", 3);
        metrics.record_unregistration_queue("queued", 2);
        metrics.record_unregistration_batch(2);
        metrics.record_expiry_scan("shadow", 10, 1, Duration::from_micros(75));
        metrics.record_request(
            NameServerWorkloadClass::Admin,
            NameServerRequestOutcome::Success,
            Duration::from_micros(10),
            128,
        );
        metrics.record_registration_decode(256, Some(512), Duration::from_micros(20));
        metrics.record_mutation_wait(Duration::from_micros(2));
        metrics.record_mutation_hold(Duration::from_micros(5));
        metrics.record_snapshot_rebuild(Duration::from_micros(3), true);
        metrics.record_expiry_event(NameServerExpiryEvent::IndexMismatch);
        metrics.record_unregistration_oldest_age(Duration::from_millis(10));
        metrics.record_kv_snapshot(3, 2, 2, 1, 64);
        metrics.record_kv_persist(Duration::from_micros(30), true, 4);
        metrics.record_kv_event(NameServerKvEvent::Queued);
        metrics.record_security_event(NameServerSecurityEvent::AuthDenied);
        metrics.record_connection_event(NameServerConnectionEvent::Admitted, 2);
    }

    #[test]
    fn namesrv_noop_recorder_is_safe_without_explicit_meter() {
        let metrics = NameServerMetrics::from_handle(&crate::TelemetryHandle::noop());
        metrics.record_route_request(Duration::from_millis(1));
        metrics.record_broker_registration(2);
        metrics.record_active_broker_count(2);
        metrics.record_route_error(NameServerRouteErrorKind::NotFound);
        metrics.record_route_freshness(25);
        metrics.record_registration_delta("unchanged", 0);
        metrics.record_unregistration_queue("coalesced", 0);
        metrics.record_unregistration_batch(1);
        metrics.record_expiry_scan("off", 10, 0, Duration::from_micros(25));
        metrics.record_request(
            NameServerWorkloadClass::Admin,
            NameServerRequestOutcome::Success,
            Duration::ZERO,
            0,
        );
        metrics.record_kv_snapshot(0, 0, 0, 0, 0);
        metrics.record_security_event(NameServerSecurityEvent::AuthDenied);
        metrics.record_connection_event(NameServerConnectionEvent::Closed, 0);
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
