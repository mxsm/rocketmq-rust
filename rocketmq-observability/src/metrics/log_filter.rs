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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;

use crate::LogFilterSource;

#[cfg(feature = "otel-metrics")]
use crate::semantic::labels;
#[cfg(feature = "otel-metrics")]
use crate::semantic::metrics;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogFilterMetricsSnapshot {
    pub reload_successes: u64,
    pub reload_failures: u64,
    pub audit_failures: u64,
    pub auto_restore_failures: u64,
    pub rollback_failures: u64,
}

#[derive(Default)]
struct MetricState {
    reload_successes: AtomicU64,
    reload_failures: AtomicU64,
    audit_failures: AtomicU64,
    auto_restore_failures: AtomicU64,
    rollback_failures: AtomicU64,
}

static STATE: OnceLock<MetricState> = OnceLock::new();

fn state() -> &'static MetricState {
    STATE.get_or_init(MetricState::default)
}

pub fn snapshot() -> LogFilterMetricsSnapshot {
    let state = state();
    LogFilterMetricsSnapshot {
        reload_successes: state.reload_successes.load(Ordering::Relaxed),
        reload_failures: state.reload_failures.load(Ordering::Relaxed),
        audit_failures: state.audit_failures.load(Ordering::Relaxed),
        auto_restore_failures: state.auto_restore_failures.load(Ordering::Relaxed),
        rollback_failures: state.rollback_failures.load(Ordering::Relaxed),
    }
}

pub fn record_reload(service: &str, success: bool, source: LogFilterSource) {
    if success {
        state().reload_successes.fetch_add(1, Ordering::Relaxed);
    } else {
        state().reload_failures.fetch_add(1, Ordering::Relaxed);
    }
    #[cfg(feature = "otel-metrics")]
    instruments().reload_total.add(
        1,
        &[
            opentelemetry::KeyValue::new(labels::SERVICE, service.to_owned()),
            opentelemetry::KeyValue::new("result", if success { "success" } else { "failure" }),
            opentelemetry::KeyValue::new(labels::SOURCE, source.as_str()),
        ],
    );
    #[cfg(not(feature = "otel-metrics"))]
    let _ = (service, source);
}

pub fn set_active(service: &str, previous: Option<LogFilterSource>, source: LogFilterSource) {
    #[cfg(feature = "otel-metrics")]
    {
        if let Some(previous) = previous {
            instruments().active.record(
                0,
                &[
                    opentelemetry::KeyValue::new(labels::SERVICE, service.to_owned()),
                    opentelemetry::KeyValue::new(labels::SOURCE, previous.as_str()),
                ],
            );
        }
        instruments().active.record(
            1,
            &[
                opentelemetry::KeyValue::new(labels::SERVICE, service.to_owned()),
                opentelemetry::KeyValue::new(labels::SOURCE, source.as_str()),
            ],
        );
    }
    #[cfg(not(feature = "otel-metrics"))]
    let _ = (service, previous, source);
}

pub fn set_expiry_timestamp(service: &str, timestamp_seconds: u64) {
    #[cfg(feature = "otel-metrics")]
    instruments().expiry.record(
        timestamp_seconds,
        &[opentelemetry::KeyValue::new(labels::SERVICE, service.to_owned())],
    );
    #[cfg(not(feature = "otel-metrics"))]
    let _ = (service, timestamp_seconds);
}

pub fn record_audit_failure(service: &str) {
    state().audit_failures.fetch_add(1, Ordering::Relaxed);
    #[cfg(feature = "otel-metrics")]
    instruments()
        .audit_failure_total
        .add(1, &[opentelemetry::KeyValue::new(labels::SERVICE, service.to_owned())]);
    #[cfg(not(feature = "otel-metrics"))]
    let _ = service;
}

pub fn record_auto_restore_failure(service: &str) {
    state().auto_restore_failures.fetch_add(1, Ordering::Relaxed);
    #[cfg(feature = "otel-metrics")]
    instruments()
        .auto_restore_failure_total
        .add(1, &[opentelemetry::KeyValue::new(labels::SERVICE, service.to_owned())]);
    #[cfg(not(feature = "otel-metrics"))]
    let _ = service;
}

pub fn record_rollback_failure(service: &str) {
    state().rollback_failures.fetch_add(1, Ordering::Relaxed);
    #[cfg(feature = "otel-metrics")]
    instruments()
        .rollback_failure_total
        .add(1, &[opentelemetry::KeyValue::new(labels::SERVICE, service.to_owned())]);
    #[cfg(not(feature = "otel-metrics"))]
    let _ = service;
}

#[cfg(feature = "otel-metrics")]
struct Instruments {
    reload_total: opentelemetry::metrics::Counter<u64>,
    active: opentelemetry::metrics::Gauge<u64>,
    expiry: opentelemetry::metrics::Gauge<u64>,
    audit_failure_total: opentelemetry::metrics::Counter<u64>,
    auto_restore_failure_total: opentelemetry::metrics::Counter<u64>,
    rollback_failure_total: opentelemetry::metrics::Counter<u64>,
}

#[cfg(feature = "otel-metrics")]
fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = opentelemetry::global::meter("rocketmq-observability-log-filter");
        Instruments {
            reload_total: meter.u64_counter(metrics::LOG_FILTER_RELOAD_TOTAL).build(),
            active: meter.u64_gauge(metrics::LOG_FILTER_ACTIVE).build(),
            expiry: meter.u64_gauge(metrics::LOG_FILTER_EXPIRY_TIMESTAMP_SECONDS).build(),
            audit_failure_total: meter.u64_counter(metrics::LOG_FILTER_AUDIT_FAILURE_TOTAL).build(),
            auto_restore_failure_total: meter
                .u64_counter(metrics::LOG_FILTER_AUTO_RESTORE_FAILURE_TOTAL)
                .build(),
            rollback_failure_total: meter.u64_counter(metrics::LOG_FILTER_ROLLBACK_FAILURE_TOTAL).build(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_tracks_reload_and_failure_counters() {
        let before = snapshot();
        record_reload("test", true, LogFilterSource::Runtime);
        record_reload("test", false, LogFilterSource::Runtime);
        record_audit_failure("test");
        record_auto_restore_failure("test");
        record_rollback_failure("test");
        let after = snapshot();

        assert_eq!(after.reload_successes, before.reload_successes + 1);
        assert_eq!(after.reload_failures, before.reload_failures + 1);
        assert_eq!(after.audit_failures, before.audit_failures + 1);
        assert_eq!(after.auto_restore_failures, before.auto_restore_failures + 1);
        assert_eq!(after.rollback_failures, before.rollback_failures + 1);
    }
}
