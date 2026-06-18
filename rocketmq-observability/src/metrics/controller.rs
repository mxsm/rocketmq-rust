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

pub use crate::semantic::metrics::CONTROLLER_ACTIVE_BROKERS;
pub use crate::semantic::metrics::CONTROLLER_ACTIVE_BROKER_NUM;
pub use crate::semantic::metrics::CONTROLLER_DLEDGER_DISK_USAGE;
pub use crate::semantic::metrics::CONTROLLER_DLEDGER_OP_LATENCY;
pub use crate::semantic::metrics::CONTROLLER_DLEDGER_OP_TOTAL;
pub use crate::semantic::metrics::CONTROLLER_ELECTION_LATENCY;
pub use crate::semantic::metrics::CONTROLLER_ELECTION_TOTAL;
pub use crate::semantic::metrics::CONTROLLER_ELECTION_TOTAL_JAVA;
pub use crate::semantic::metrics::CONTROLLER_LEADER_CHANGES_TOTAL;
pub use crate::semantic::metrics::CONTROLLER_REQUEST_LATENCY;
pub use crate::semantic::metrics::CONTROLLER_REQUEST_TOTAL;
pub use crate::semantic::metrics::CONTROLLER_ROLE;

#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static CONTROLLER_METRICS: OnceLock<ControllerMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
pub fn init_global(meter: &opentelemetry::metrics::Meter) -> bool {
    CONTROLLER_METRICS.set(ControllerMetrics::new(meter)).is_ok()
}

pub fn record_election_total(count: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = CONTROLLER_METRICS.get() {
        metrics.record_election_total(count, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_election_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = CONTROLLER_METRICS.get() {
        metrics.record_election_latency(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_leader_changes_total(count: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = CONTROLLER_METRICS.get() {
        metrics.record_leader_changes_total(count, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_active_brokers(count: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = CONTROLLER_METRICS.get() {
        metrics.record_active_brokers(count, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct ControllerMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl ControllerMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_election_total(&self, _count: u64) {}

    #[inline]
    pub fn record_election_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_leader_changes_total(&self, _count: u64) {}

    #[inline]
    pub fn record_active_brokers(&self, _count: u64) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct ControllerMetrics {
    election_total: opentelemetry::metrics::Counter<u64>,
    election_latency: opentelemetry::metrics::Histogram<u64>,
    leader_changes_total: opentelemetry::metrics::Counter<u64>,
    active_brokers: opentelemetry::metrics::Gauge<u64>,
}

#[cfg(feature = "otel-metrics")]
impl ControllerMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let election_total = meter
            .u64_counter(CONTROLLER_ELECTION_TOTAL)
            .with_description("Total number of controller elections")
            .with_unit("{election}")
            .build();

        let election_latency = meter
            .u64_histogram(CONTROLLER_ELECTION_LATENCY)
            .with_description("Controller election latency")
            .with_unit("ms")
            .build();

        let leader_changes_total = meter
            .u64_counter(CONTROLLER_LEADER_CHANGES_TOTAL)
            .with_description("Total number of controller leader changes")
            .with_unit("{change}")
            .build();

        let active_brokers = meter
            .u64_gauge(CONTROLLER_ACTIVE_BROKERS)
            .with_description("Number of active brokers known by controller")
            .with_unit("{broker}")
            .build();

        Self {
            election_total,
            election_latency,
            leader_changes_total,
            active_brokers,
        }
    }

    #[inline]
    pub fn record_election_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.election_total.add(count, attributes);
    }

    #[inline]
    pub fn record_election_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.election_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_leader_changes_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.leader_changes_total.add(count, attributes);
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
    fn controller_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("controller-metrics-test");
        let metrics = ControllerMetrics::new(&meter);
        let attrs = [opentelemetry::KeyValue::new("controller_id", "controller-a")];

        metrics.record_election_total(1, &attrs);
        metrics.record_election_latency(15, &attrs);
        metrics.record_leader_changes_total(1, &attrs);
        metrics.record_active_brokers(3, &attrs);
    }
}
