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
use std::sync::Arc;

use crate::LogFilterSource;
use crate::TelemetryHandle;

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

/// Instance-scoped log-filter telemetry bound to one explicit telemetry runtime.
///
/// The operational counters remain available for local diagnostics even when metrics export is
/// disabled. OpenTelemetry instruments are created from the injected [`TelemetryHandle`] and are
/// gated by that handle's lifecycle; this type never reads process-global OpenTelemetry state.
#[derive(Clone)]
pub(crate) struct LogFilterMetrics {
    inner: Arc<LogFilterMetricsInner>,
}

struct LogFilterMetricsInner {
    state: MetricState,
    #[cfg(feature = "otel-metrics")]
    telemetry: TelemetryHandle,
    #[cfg(feature = "otel-metrics")]
    service: Arc<str>,
    #[cfg(feature = "otel-metrics")]
    instruments: Option<Instruments>,
}

impl LogFilterMetrics {
    pub(crate) fn new(telemetry: TelemetryHandle, service: impl Into<Arc<str>>) -> Self {
        #[cfg(feature = "otel-metrics")]
        let service = service.into();
        #[cfg(feature = "otel-metrics")]
        let instruments = telemetry.meter(service.as_ref()).map(Instruments::new);
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (telemetry, service);
        Self {
            inner: Arc::new(LogFilterMetricsInner {
                state: MetricState::default(),
                #[cfg(feature = "otel-metrics")]
                telemetry,
                #[cfg(feature = "otel-metrics")]
                service,
                #[cfg(feature = "otel-metrics")]
                instruments,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> LogFilterMetricsSnapshot {
        LogFilterMetricsSnapshot {
            reload_successes: self.inner.state.reload_successes.load(Ordering::Relaxed),
            reload_failures: self.inner.state.reload_failures.load(Ordering::Relaxed),
            audit_failures: self.inner.state.audit_failures.load(Ordering::Relaxed),
            auto_restore_failures: self.inner.state.auto_restore_failures.load(Ordering::Relaxed),
            rollback_failures: self.inner.state.rollback_failures.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_reload(&self, success: bool, source: LogFilterSource) {
        if success {
            self.inner.state.reload_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.state.reload_failures.fetch_add(1, Ordering::Relaxed);
        }
        #[cfg(feature = "otel-metrics")]
        if self.inner.telemetry.is_active() {
            if let Some(instruments) = self.inner.instruments.as_ref() {
                instruments.reload_total.add(
                    1,
                    &[
                        opentelemetry::KeyValue::new(labels::SERVICE, self.inner.service.to_string()),
                        opentelemetry::KeyValue::new("result", if success { "success" } else { "failure" }),
                        opentelemetry::KeyValue::new(labels::SOURCE, source.as_str()),
                    ],
                );
            }
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = source;
    }

    pub(crate) fn set_active(&self, previous: Option<LogFilterSource>, source: LogFilterSource) {
        #[cfg(feature = "otel-metrics")]
        if self.inner.telemetry.is_active() {
            let Some(instruments) = self.inner.instruments.as_ref() else {
                return;
            };
            if let Some(previous) = previous {
                instruments.active.record(
                    0,
                    &[
                        opentelemetry::KeyValue::new(labels::SERVICE, self.inner.service.to_string()),
                        opentelemetry::KeyValue::new(labels::SOURCE, previous.as_str()),
                    ],
                );
            }
            instruments.active.record(
                1,
                &[
                    opentelemetry::KeyValue::new(labels::SERVICE, self.inner.service.to_string()),
                    opentelemetry::KeyValue::new(labels::SOURCE, source.as_str()),
                ],
            );
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = (previous, source);
    }

    pub(crate) fn set_expiry_timestamp(&self, timestamp_seconds: u64) {
        #[cfg(feature = "otel-metrics")]
        if self.inner.telemetry.is_active() {
            if let Some(instruments) = self.inner.instruments.as_ref() {
                instruments.expiry.record(
                    timestamp_seconds,
                    &[opentelemetry::KeyValue::new(
                        labels::SERVICE,
                        self.inner.service.to_string(),
                    )],
                );
            }
        }
        #[cfg(not(feature = "otel-metrics"))]
        let _ = timestamp_seconds;
    }

    pub(crate) fn record_audit_failure(&self) {
        self.inner.state.audit_failures.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "otel-metrics")]
        if self.inner.telemetry.is_active() {
            if let Some(instruments) = self.inner.instruments.as_ref() {
                instruments.audit_failure_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        labels::SERVICE,
                        self.inner.service.to_string(),
                    )],
                );
            }
        }
    }

    pub(crate) fn record_auto_restore_failure(&self) {
        self.inner.state.auto_restore_failures.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "otel-metrics")]
        if self.inner.telemetry.is_active() {
            if let Some(instruments) = self.inner.instruments.as_ref() {
                instruments.auto_restore_failure_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        labels::SERVICE,
                        self.inner.service.to_string(),
                    )],
                );
            }
        }
    }

    pub(crate) fn record_rollback_failure(&self) {
        self.inner.state.rollback_failures.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "otel-metrics")]
        if self.inner.telemetry.is_active() {
            if let Some(instruments) = self.inner.instruments.as_ref() {
                instruments.rollback_failure_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        labels::SERVICE,
                        self.inner.service.to_string(),
                    )],
                );
            }
        }
    }
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
impl Instruments {
    fn new(meter: opentelemetry::metrics::Meter) -> Self {
        Self {
            reload_total: meter.u64_counter(metrics::LOG_FILTER_RELOAD_TOTAL).build(),
            active: meter.u64_gauge(metrics::LOG_FILTER_ACTIVE).build(),
            expiry: meter.u64_gauge(metrics::LOG_FILTER_EXPIRY_TIMESTAMP_SECONDS).build(),
            audit_failure_total: meter.u64_counter(metrics::LOG_FILTER_AUDIT_FAILURE_TOTAL).build(),
            auto_restore_failure_total: meter
                .u64_counter(metrics::LOG_FILTER_AUTO_RESTORE_FAILURE_TOTAL)
                .build(),
            rollback_failure_total: meter.u64_counter(metrics::LOG_FILTER_ROLLBACK_FAILURE_TOTAL).build(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_are_isolated_between_log_filter_handles() {
        let first = LogFilterMetrics::new(TelemetryHandle::noop(), "rocketmq-broker");
        let second = LogFilterMetrics::new(TelemetryHandle::noop(), "rocketmq-broker");

        first.record_reload(true, LogFilterSource::Runtime);
        first.record_reload(false, LogFilterSource::Runtime);
        first.record_audit_failure();
        first.record_auto_restore_failure();
        first.record_rollback_failure();

        assert_eq!(
            first.snapshot(),
            LogFilterMetricsSnapshot {
                reload_successes: 1,
                reload_failures: 1,
                audit_failures: 1,
                auto_restore_failures: 1,
                rollback_failures: 1,
            }
        );
        assert_eq!(second.snapshot(), LogFilterMetricsSnapshot::default());
    }
}
