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

pub use crate::semantic::metrics::TIMER_DEQUEUE_LAG;
pub use crate::semantic::metrics::TIMER_DEQUEUE_LATENCY;
pub use crate::semantic::metrics::TIMER_DEQUEUE_TOTAL;
pub use crate::semantic::metrics::TIMER_ENQUEUE_LAG;
pub use crate::semantic::metrics::TIMER_ENQUEUE_LATENCY;
pub use crate::semantic::metrics::TIMER_ENQUEUE_TOTAL;
pub use crate::semantic::metrics::TIMER_MESSAGE_SNAPSHOT;
pub use crate::semantic::metrics::TIMING_MESSAGES;

#[derive(Debug, Clone, Default)]
pub struct TimerObservableValues {
    pub enqueue_lag: i64,
    pub enqueue_latency_millis: i64,
    pub dequeue_lag: i64,
    pub dequeue_latency_millis: i64,
    pub timing_messages: Vec<(String, i64)>,
    pub message_snapshot: Vec<(i32, i64)>,
}

/// Cloneable Timer recorder bound to one explicit Store telemetry scope.
#[derive(Clone)]
pub struct TimerMetricsRecorder {
    #[cfg(feature = "otel-metrics")]
    telemetry: crate::TelemetryRecorder,
    #[cfg(feature = "otel-metrics")]
    metrics: Option<TimerMetrics>,
}

impl std::fmt::Debug for TimerMetricsRecorder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TimerMetricsRecorder")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl Default for TimerMetricsRecorder {
    fn default() -> Self {
        Self::noop()
    }
}

impl TimerMetricsRecorder {
    /// Creates a recorder that never reads process-global OpenTelemetry state.
    #[must_use]
    pub fn noop() -> Self {
        Self::from_handle(&crate::TelemetryHandle::noop())
    }

    /// Creates a recorder from the fixed Store instrumentation scope.
    #[must_use]
    pub fn from_handle(handle: &crate::TelemetryHandle) -> Self {
        #[cfg(feature = "otel-metrics")]
        {
            let telemetry = handle.child(crate::STORE_METER_SCOPE);
            let metrics = telemetry
                .meter()
                .map(|meter| TimerMetrics::new_with_label_policy(&meter, telemetry.metric_label_policy()));
            Self { telemetry, metrics }
        }

        #[cfg(not(feature = "otel-metrics"))]
        {
            let _ = handle;
            Self {}
        }
    }

    /// Returns whether this recorder is backed by an active Store meter.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        #[cfg(feature = "otel-metrics")]
        {
            self.telemetry.is_active() && self.metrics.is_some()
        }

        #[cfg(not(feature = "otel-metrics"))]
        {
            false
        }
    }

    /// Registers Timer observable gauges on this recorder's explicit meter.
    pub fn register_observables<F>(&self, source: F)
    where
        F: Fn() -> TimerObservableValues + Send + Sync + 'static,
    {
        #[cfg(feature = "otel-metrics")]
        if let Some(meter) = self.telemetry.meter() {
            TimerMetrics::register_observables_with_label_policy(&meter, source, self.telemetry.metric_label_policy());
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = source;
    }

    #[inline]
    pub fn record_enqueue_total(&self, topic: Option<&str>) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.record_enqueue_total(topic);
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = topic;
    }

    #[inline]
    pub fn record_dequeue_total(&self, topic: &str) {
        #[cfg(feature = "otel-metrics")]
        if self.telemetry.is_active() {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.record_dequeue_total(topic);
            }
        }

        #[cfg(not(feature = "otel-metrics"))]
        let _ = topic;
    }
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Default)]
pub struct TimerMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl TimerMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_enqueue_total(&self, _topic: Option<&str>) {}

    #[inline]
    pub fn record_dequeue_total(&self, _topic: &str) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct TimerMetrics {
    enqueue_total: opentelemetry::metrics::Counter<u64>,
    dequeue_total: opentelemetry::metrics::Counter<u64>,
    label_policy: crate::MetricLabelPolicy,
}

#[cfg(feature = "otel-metrics")]
impl TimerMetrics {
    #[cfg(test)]
    pub(crate) fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self::new_with_label_policy(meter, crate::MetricLabelPolicy::default())
    }

    pub(crate) fn new_with_label_policy(
        meter: &opentelemetry::metrics::Meter,
        label_policy: crate::MetricLabelPolicy,
    ) -> Self {
        Self {
            enqueue_total: meter
                .u64_counter(TIMER_ENQUEUE_TOTAL)
                .with_description("Total number of timer enqueue")
                .build(),
            dequeue_total: meter
                .u64_counter(TIMER_DEQUEUE_TOTAL)
                .with_description("Total number of timer dequeue")
                .build(),
            label_policy,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_observables<F>(meter: &opentelemetry::metrics::Meter, source: F) -> Self
    where
        F: Fn() -> TimerObservableValues + Send + Sync + 'static,
    {
        let metrics = Self::new(meter);
        Self::register_observables_with_label_policy(meter, source, metrics.label_policy.clone());
        metrics
    }

    pub(crate) fn register_observables_with_label_policy<F>(
        meter: &opentelemetry::metrics::Meter,
        source: F,
        label_policy: crate::MetricLabelPolicy,
    ) where
        F: Fn() -> TimerObservableValues + Send + Sync + 'static,
    {
        let source = std::sync::Arc::new(source);

        let enqueue_lag_source = source.clone();
        let _enqueue_lag = meter
            .i64_observable_gauge(TIMER_ENQUEUE_LAG)
            .with_description("Timer enqueue messages lag")
            .with_callback(move |observer| {
                let values = enqueue_lag_source();
                let attrs = store_attributes();
                observer.observe(values.enqueue_lag.max(0), &attrs);
            })
            .build();

        let enqueue_latency_source = source.clone();
        let _enqueue_latency = meter
            .i64_observable_gauge(TIMER_ENQUEUE_LATENCY)
            .with_description("Timer enqueue latency")
            .with_unit("milliseconds")
            .with_callback(move |observer| {
                let values = enqueue_latency_source();
                let attrs = store_attributes();
                observer.observe(values.enqueue_latency_millis.max(0), &attrs);
            })
            .build();

        let dequeue_lag_source = source.clone();
        let _dequeue_lag = meter
            .i64_observable_gauge(TIMER_DEQUEUE_LAG)
            .with_description("Timer dequeue messages lag")
            .with_callback(move |observer| {
                let values = dequeue_lag_source();
                let attrs = store_attributes();
                observer.observe(values.dequeue_lag.max(0), &attrs);
            })
            .build();

        let dequeue_latency_source = source.clone();
        let _dequeue_latency = meter
            .i64_observable_gauge(TIMER_DEQUEUE_LATENCY)
            .with_description("Timer dequeue latency")
            .with_unit("milliseconds")
            .with_callback(move |observer| {
                let values = dequeue_latency_source();
                let attrs = store_attributes();
                observer.observe(values.dequeue_latency_millis.max(0), &attrs);
            })
            .build();

        let timing_messages_source = source.clone();
        let _timing_messages = meter
            .i64_observable_gauge(TIMING_MESSAGES)
            .with_description("Current message number in timing")
            .with_callback(move |observer| {
                let values = timing_messages_source();
                for (topic, count) in values.timing_messages {
                    let mut attrs = store_attributes().to_vec();
                    attrs.push(topic_attribute(&label_policy, &topic));
                    observer.observe(count.max(0), &attrs);
                }
            })
            .build();

        let _timer_message_snapshot = meter
            .i64_observable_gauge(TIMER_MESSAGE_SNAPSHOT)
            .with_description("Timer message distribution snapshot, only count timing messages in 24h.")
            .with_callback(move |observer| {
                let values = source();
                for (timer_bound_seconds, count) in values.message_snapshot {
                    let mut attrs = store_attributes().to_vec();
                    attrs.push(opentelemetry::KeyValue::new(
                        crate::semantic::labels::TIMER_BOUND_SECONDS,
                        i64::from(timer_bound_seconds),
                    ));
                    observer.observe(count.max(0), &attrs);
                }
            })
            .build();
    }

    #[inline]
    pub fn record_enqueue_total(&self, topic: Option<&str>) {
        let mut attrs = store_attributes().to_vec();
        if let Some(topic) = topic {
            attrs.push(topic_attribute(&self.label_policy, topic));
        }
        self.enqueue_total.add(1, &attrs);
    }

    #[inline]
    pub fn record_dequeue_total(&self, topic: &str) {
        let mut attrs = store_attributes().to_vec();
        attrs.push(topic_attribute(&self.label_policy, topic));
        self.dequeue_total.add(1, &attrs);
    }
}

#[cfg(feature = "otel-metrics")]
#[inline]
fn topic_attribute(policy: &crate::MetricLabelPolicy, topic: &str) -> opentelemetry::KeyValue {
    let (topic, dropped) = policy.normalize_metric_label_with_outcome(crate::semantic::labels::TOPIC, topic);
    if dropped {
        opentelemetry::KeyValue::new(crate::semantic::labels::TOPIC, crate::METRIC_LABEL_SENTINEL)
    } else {
        opentelemetry::KeyValue::new(crate::semantic::labels::TOPIC, topic.into_owned())
    }
}

#[cfg(feature = "otel-metrics")]
fn store_attributes() -> [opentelemetry::KeyValue; 2] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_TYPE, "local"),
        opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_MEDIUM, "disk"),
    ]
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn timer_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("timer-metrics-test");
        let metrics = TimerMetrics::new_with_observables(&meter, || TimerObservableValues {
            enqueue_lag: 1,
            enqueue_latency_millis: 2,
            dequeue_lag: 3,
            dequeue_latency_millis: 4,
            timing_messages: vec![("topic-a".to_string(), 5)],
            message_snapshot: vec![(60, 6)],
        });

        metrics.record_enqueue_total(Some("topic-a"));
        metrics.record_enqueue_total(None);
        metrics.record_dequeue_total("topic-a");
    }

    #[test]
    fn timer_topic_attributes_use_the_bounded_policy() {
        let policy = crate::MetricLabelPolicy::new(1, true, true);

        assert_eq!(topic_attribute(&policy, "topic-a").value.to_string(), "topic-a");
        assert_eq!(
            topic_attribute(&policy, "topic-b").value.to_string(),
            crate::METRIC_LABEL_SENTINEL
        );
    }
}
