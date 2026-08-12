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
pub use crate::semantic::metrics::CLIENT_NAMESRV_DISCOVERY_ENDPOINT_COUNT;
pub use crate::semantic::metrics::CLIENT_NAMESRV_DISCOVERY_FRESHNESS;
pub use crate::semantic::metrics::CLIENT_NAMESRV_DISCOVERY_REFRESH_TOTAL;
pub use crate::semantic::metrics::CLIENT_NAMESRV_DISCOVERY_SNAPSHOT_AGE;
pub use crate::semantic::metrics::CLIENT_NAMESRV_FAILOVER_TOTAL;
pub use crate::semantic::metrics::CLIENT_ONEWAY_EGRESS_BYTES;
pub use crate::semantic::metrics::CLIENT_ONEWAY_EGRESS_EVENTS_TOTAL;
pub use crate::semantic::metrics::CLIENT_ONEWAY_EGRESS_ITEMS;
pub use crate::semantic::metrics::CLIENT_ONEWAY_EGRESS_OLDEST_AGE;
pub use crate::semantic::metrics::CLIENT_ONEWAY_EGRESS_WAITERS;
pub use crate::semantic::metrics::CLIENT_REBALANCE_TOTAL;
pub use crate::semantic::metrics::CLIENT_SEND_LATENCY;
pub use crate::semantic::metrics::CLIENT_SEND_TOTAL;

use std::time::Duration;

/// Fixed discovery refresh outcomes permitted as metric labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameServerDiscoveryRefreshResult {
    Success,
    Error,
}

#[cfg(feature = "otel-metrics")]
impl NameServerDiscoveryRefreshResult {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
}

/// Fixed freshness states permitted as metric labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameServerDiscoveryFreshness {
    Fresh,
    Stale,
    Unavailable,
}

#[cfg(feature = "otel-metrics")]
impl NameServerDiscoveryFreshness {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Fixed failover reasons permitted as metric labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameServerFailoverReason {
    ConnectFailure,
    Unhealthy,
    CircuitOpen,
    Draining,
}

#[cfg(feature = "otel-metrics")]
impl NameServerFailoverReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ConnectFailure => "connect_failure",
            Self::Unhealthy => "unhealthy",
            Self::CircuitOpen => "circuit_open",
            Self::Draining => "draining",
        }
    }
}

#[cfg(feature = "otel-metrics")]
#[inline]
fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().clamp(0, u128::from(u64::MAX)) as u64
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Default)]
pub struct ClientMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl ClientMetrics {
    /// Creates an instance-scoped recorder that never emits metrics.
    #[must_use]
    pub const fn noop() -> Self {
        Self
    }

    /// Creates a recorder from an explicit telemetry capability.
    #[must_use]
    pub fn from_handle(_telemetry: &crate::TelemetryHandle) -> Self {
        Self::noop()
    }

    /// Returns whether this recorder can currently emit metrics.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        false
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

    #[inline]
    pub fn record_send(&self, _elapsed: Duration) {}

    #[inline]
    pub fn record_consume(&self, _message_count: usize, _latency_ms: u64) {}

    #[inline]
    pub fn record_rebalance(&self) {}

    #[inline]
    pub fn record_oneway_egress_state(&self, _items: u64, _bytes: u64, _oldest_age_ms: u64, _waiters: u64) {}

    #[inline]
    pub fn record_oneway_egress_event(&self, _result: &'static str) {}

    #[inline]
    pub fn record_nameserver_discovery_refresh(&self, _result: NameServerDiscoveryRefreshResult) {}

    #[inline]
    pub fn record_nameserver_discovery_snapshot(
        &self,
        _freshness: NameServerDiscoveryFreshness,
        _ipv4_count: u64,
        _ipv6_count: u64,
        _snapshot_age: Duration,
    ) {
    }

    #[inline]
    pub fn record_nameserver_failover(&self, _reason: NameServerFailoverReason) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone, Default)]
pub struct ClientMetrics {
    telemetry: Option<crate::TelemetryHandle>,
    instruments: Option<ClientMetricInstruments>,
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
struct ClientMetricInstruments {
    send_total: opentelemetry::metrics::Counter<u64>,
    send_latency: opentelemetry::metrics::Histogram<u64>,
    consume_total: opentelemetry::metrics::Counter<u64>,
    consume_latency: opentelemetry::metrics::Histogram<u64>,
    rebalance_total: opentelemetry::metrics::Counter<u64>,
    oneway_egress_items: opentelemetry::metrics::Gauge<u64>,
    oneway_egress_bytes: opentelemetry::metrics::Gauge<u64>,
    oneway_egress_oldest_age: opentelemetry::metrics::Gauge<u64>,
    oneway_egress_waiters: opentelemetry::metrics::Gauge<u64>,
    oneway_egress_events_total: opentelemetry::metrics::Counter<u64>,
    nameserver_discovery_refresh_total: opentelemetry::metrics::Counter<u64>,
    nameserver_discovery_endpoint_count: opentelemetry::metrics::Gauge<u64>,
    nameserver_discovery_freshness: opentelemetry::metrics::Gauge<u64>,
    nameserver_discovery_snapshot_age: opentelemetry::metrics::Gauge<u64>,
    nameserver_failover_total: opentelemetry::metrics::Counter<u64>,
}

#[cfg(feature = "otel-metrics")]
impl ClientMetrics {
    /// Creates an instance-scoped recorder that never emits metrics.
    #[must_use]
    pub fn noop() -> Self {
        Self::default()
    }

    /// Creates a lifecycle-gated recorder from an explicit telemetry capability.
    #[must_use]
    pub fn from_handle(telemetry: &crate::TelemetryHandle) -> Self {
        let Some(meter) = telemetry.meter(crate::CLIENT_METER_SCOPE) else {
            return Self::noop();
        };
        Self {
            telemetry: Some(telemetry.clone()),
            instruments: Some(ClientMetricInstruments::new(&meter)),
        }
    }

    /// Creates an unmanaged recorder from a caller-owned meter.
    ///
    /// Runtime composition should prefer [`Self::from_handle`] so records stop when the owning
    /// telemetry runtime closes.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            telemetry: None,
            instruments: Some(ClientMetricInstruments::new(meter)),
        }
    }

    /// Returns whether this recorder can currently emit metrics.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.instruments.is_some() && self.telemetry.as_ref().is_none_or(crate::TelemetryHandle::is_active)
    }

    #[inline]
    pub fn record_send_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        if self.is_enabled() {
            if let Some(instruments) = &self.instruments {
                instruments.send_total.add(count, attributes);
            }
        }
    }

    #[inline]
    pub fn record_send_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        if self.is_enabled() {
            if let Some(instruments) = &self.instruments {
                instruments.send_latency.record(latency_ms, attributes);
            }
        }
    }

    #[inline]
    pub fn record_consume_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        if self.is_enabled() {
            if let Some(instruments) = &self.instruments {
                instruments.consume_total.add(count, attributes);
            }
        }
    }

    #[inline]
    pub fn record_consume_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        if self.is_enabled() {
            if let Some(instruments) = &self.instruments {
                instruments.consume_latency.record(latency_ms, attributes);
            }
        }
    }

    #[inline]
    pub fn record_rebalance_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        if self.is_enabled() {
            if let Some(instruments) = &self.instruments {
                instruments.rebalance_total.add(count, attributes);
            }
        }
    }

    #[inline]
    pub fn record_send(&self, elapsed: Duration) {
        self.record_send_total(1, &[]);
        self.record_send_latency(duration_millis_u64(elapsed), &[]);
    }

    #[inline]
    pub fn record_consume(&self, message_count: usize, latency_ms: u64) {
        self.record_consume_total(message_count as u64, &[]);
        self.record_consume_latency(latency_ms, &[]);
    }

    #[inline]
    pub fn record_rebalance(&self) {
        self.record_rebalance_total(1, &[]);
    }

    #[inline]
    pub fn record_oneway_egress_state(&self, items: u64, bytes: u64, oldest_age_ms: u64, waiters: u64) {
        if self.is_enabled() {
            if let Some(instruments) = &self.instruments {
                instruments.oneway_egress_items.record(items, &[]);
                instruments.oneway_egress_bytes.record(bytes, &[]);
                instruments.oneway_egress_oldest_age.record(oldest_age_ms, &[]);
                instruments.oneway_egress_waiters.record(waiters, &[]);
            }
        }
    }

    #[inline]
    pub fn record_oneway_egress_event(&self, result: &'static str) {
        if self.is_enabled() {
            if let Some(instruments) = &self.instruments {
                instruments.oneway_egress_events_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result)],
                );
            }
        }
    }

    #[inline]
    pub fn record_nameserver_discovery_refresh(&self, result: NameServerDiscoveryRefreshResult) {
        if self.is_enabled() {
            if let Some(instruments) = &self.instruments {
                instruments.nameserver_discovery_refresh_total.add(
                    1,
                    &[
                        opentelemetry::KeyValue::new(crate::semantic::labels::SOURCE_KIND, "dns"),
                        opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result.as_str()),
                    ],
                );
            }
        }
    }

    #[inline]
    pub fn record_nameserver_discovery_snapshot(
        &self,
        freshness: NameServerDiscoveryFreshness,
        ipv4_count: u64,
        ipv6_count: u64,
        snapshot_age: Duration,
    ) {
        if !self.is_enabled() {
            return;
        }
        if let Some(instruments) = &self.instruments {
            let source = opentelemetry::KeyValue::new(crate::semantic::labels::SOURCE_KIND, "dns");
            instruments.nameserver_discovery_endpoint_count.record(
                ipv4_count,
                &[
                    source.clone(),
                    opentelemetry::KeyValue::new(crate::semantic::labels::ADDRESS_FAMILY, "ipv4"),
                ],
            );
            instruments.nameserver_discovery_endpoint_count.record(
                ipv6_count,
                &[
                    source.clone(),
                    opentelemetry::KeyValue::new(crate::semantic::labels::ADDRESS_FAMILY, "ipv6"),
                ],
            );
            let state = [
                source,
                opentelemetry::KeyValue::new(crate::semantic::labels::FRESHNESS, freshness.as_str()),
            ];
            instruments.nameserver_discovery_freshness.record(1, &state);
            instruments
                .nameserver_discovery_snapshot_age
                .record(snapshot_age.as_secs(), &state);
        }
    }

    #[inline]
    pub fn record_nameserver_failover(&self, reason: NameServerFailoverReason) {
        if self.is_enabled() {
            if let Some(instruments) = &self.instruments {
                instruments.nameserver_failover_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::REASON,
                        reason.as_str(),
                    )],
                );
            }
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl ClientMetricInstruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
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
        let oneway_egress_items = meter
            .u64_gauge(CLIENT_ONEWAY_EGRESS_ITEMS)
            .with_description("Current one-way egress messages charged to the process budget")
            .with_unit("{message}")
            .build();
        let oneway_egress_bytes = meter
            .u64_gauge(CLIENT_ONEWAY_EGRESS_BYTES)
            .with_description("Current one-way egress retained bytes charged to the process budget")
            .with_unit("By")
            .build();
        let oneway_egress_oldest_age = meter
            .u64_gauge(CLIENT_ONEWAY_EGRESS_OLDEST_AGE)
            .with_description("Age of the oldest one-way egress message")
            .with_unit("ms")
            .build();
        let oneway_egress_waiters = meter
            .u64_gauge(CLIENT_ONEWAY_EGRESS_WAITERS)
            .with_description("Current one-way egress admission waiters")
            .with_unit("{waiter}")
            .build();
        let oneway_egress_events_total = meter
            .u64_counter(CLIENT_ONEWAY_EGRESS_EVENTS_TOTAL)
            .with_description("One-way egress accepted, delivered, failed, cancelled, and rejected events")
            .with_unit("{event}")
            .build();
        let nameserver_discovery_refresh_total = meter
            .u64_counter(CLIENT_NAMESRV_DISCOVERY_REFRESH_TOTAL)
            .with_description("NameServer discovery refresh outcomes")
            .with_unit("{refresh}")
            .build();
        let nameserver_discovery_endpoint_count = meter
            .u64_gauge(CLIENT_NAMESRV_DISCOVERY_ENDPOINT_COUNT)
            .with_description("Current NameServer discovery endpoint count by address family")
            .with_unit("{endpoint}")
            .build();
        let nameserver_discovery_freshness = meter
            .u64_gauge(CLIENT_NAMESRV_DISCOVERY_FRESHNESS)
            .with_description("Current NameServer discovery freshness state")
            .with_unit("1")
            .build();
        let nameserver_discovery_snapshot_age = meter
            .u64_gauge(CLIENT_NAMESRV_DISCOVERY_SNAPSHOT_AGE)
            .with_description("Age of the current NameServer discovery snapshot")
            .with_unit("s")
            .build();
        let nameserver_failover_total = meter
            .u64_counter(CLIENT_NAMESRV_FAILOVER_TOTAL)
            .with_description("NameServer failover events grouped by bounded reason")
            .with_unit("{failover}")
            .build();

        Self {
            send_total,
            send_latency,
            consume_total,
            consume_latency,
            rebalance_total,
            oneway_egress_items,
            oneway_egress_bytes,
            oneway_egress_oldest_age,
            oneway_egress_waiters,
            oneway_egress_events_total,
            nameserver_discovery_refresh_total,
            nameserver_discovery_endpoint_count,
            nameserver_discovery_freshness,
            nameserver_discovery_snapshot_age,
            nameserver_failover_total,
        }
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
        metrics.record_send(Duration::from_millis(10));
        metrics.record_consume(1, 8);
        metrics.record_rebalance();
        metrics.record_nameserver_discovery_refresh(NameServerDiscoveryRefreshResult::Success);
        metrics.record_nameserver_discovery_snapshot(NameServerDiscoveryFreshness::Fresh, 2, 1, Duration::from_secs(3));
        metrics.record_nameserver_failover(NameServerFailoverReason::ConnectFailure);
        assert!(metrics.is_enabled());
    }

    #[test]
    fn nameserver_metric_labels_are_closed_fixed_enums() {
        assert_eq!(NameServerDiscoveryRefreshResult::Success.as_str(), "success");
        assert_eq!(NameServerDiscoveryRefreshResult::Error.as_str(), "error");
        assert_eq!(NameServerDiscoveryFreshness::Fresh.as_str(), "fresh");
        assert_eq!(NameServerDiscoveryFreshness::Stale.as_str(), "stale");
        assert_eq!(NameServerDiscoveryFreshness::Unavailable.as_str(), "unavailable");
        assert_eq!(NameServerFailoverReason::ConnectFailure.as_str(), "connect_failure");
        assert_eq!(NameServerFailoverReason::Unhealthy.as_str(), "unhealthy");
        assert_eq!(NameServerFailoverReason::CircuitOpen.as_str(), "circuit_open");
        assert_eq!(NameServerFailoverReason::Draining.as_str(), "draining");
    }

    #[test]
    fn client_noop_recorder_is_disabled() {
        let metrics = ClientMetrics::from_handle(&crate::TelemetryHandle::noop());

        metrics.record_send(Duration::from_millis(1));
        metrics.record_consume(1, 2);
        metrics.record_rebalance();

        assert!(!metrics.is_enabled());
    }

    #[test]
    fn client_metrics_source_has_no_process_global_facade() {
        let source = include_str!("client.rs");

        for forbidden in [
            concat!("Once", "Lock"),
            concat!("fn ", "init_global"),
            concat!("fn ", "global_metrics"),
            concat!("pub fn ", "record_send(elapsed"),
            concat!("pub fn ", "record_consume(message_count"),
            concat!("pub fn ", "record_rebalance()"),
        ] {
            assert!(
                !source.contains(forbidden),
                "client metrics must remain instance scoped: found {forbidden}"
            );
        }
    }
}
