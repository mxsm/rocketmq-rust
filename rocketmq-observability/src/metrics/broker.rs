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

pub use crate::semantic::metrics::MESSAGES_IN_TOTAL;
pub use crate::semantic::metrics::MESSAGES_OUT_TOTAL;
pub use crate::semantic::metrics::MESSAGE_SIZE;
pub use crate::semantic::metrics::METRICS_LABEL_DROPPED_TOTAL;
pub use crate::semantic::metrics::SEND_MESSAGE_LATENCY;
pub use crate::semantic::metrics::THROUGHPUT_IN_TOTAL;
pub use crate::semantic::metrics::THROUGHPUT_OUT_TOTAL;

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct BrokerMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl BrokerMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_messages_in(&self, _count: u64, _bytes: u64, _message_size: u64) {}

    #[inline]
    pub fn record_messages_out(&self, _count: u64, _bytes: u64) {}

    #[inline]
    pub fn record_metrics_label_dropped_total(&self, _count: u64, _attributes: &[()]) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct BrokerMetrics {
    messages_in_total: opentelemetry::metrics::Counter<u64>,
    messages_out_total: opentelemetry::metrics::Counter<u64>,
    throughput_in_total: opentelemetry::metrics::Counter<u64>,
    throughput_out_total: opentelemetry::metrics::Counter<u64>,
    message_size: opentelemetry::metrics::Histogram<u64>,
    send_message_latency: opentelemetry::metrics::Histogram<u64>,
    metrics_label_dropped_total: opentelemetry::metrics::Counter<u64>,
}

#[cfg(feature = "otel-metrics")]
impl BrokerMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let messages_in_total = meter
            .u64_counter(MESSAGES_IN_TOTAL)
            .with_description("Total number of messages received by broker")
            .with_unit("{message}")
            .build();

        let messages_out_total = meter
            .u64_counter(MESSAGES_OUT_TOTAL)
            .with_description("Total number of messages sent out from broker")
            .with_unit("{message}")
            .build();

        let throughput_in_total = meter
            .u64_counter(THROUGHPUT_IN_TOTAL)
            .with_description("Total bytes received by broker")
            .with_unit("By")
            .build();

        let throughput_out_total = meter
            .u64_counter(THROUGHPUT_OUT_TOTAL)
            .with_description("Total bytes sent out from broker")
            .with_unit("By")
            .build();

        let message_size = meter
            .u64_histogram(MESSAGE_SIZE)
            .with_description("Message body size distribution")
            .with_unit("By")
            .build();

        let send_message_latency = meter
            .u64_histogram(SEND_MESSAGE_LATENCY)
            .with_description("Broker send message processing latency")
            .with_unit("ms")
            .build();

        let metrics_label_dropped_total = meter
            .u64_counter(METRICS_LABEL_DROPPED_TOTAL)
            .with_description("Total number of metrics labels dropped by cardinality guard")
            .with_unit("{label}")
            .build();

        Self {
            messages_in_total,
            messages_out_total,
            throughput_in_total,
            throughput_out_total,
            message_size,
            send_message_latency,
            metrics_label_dropped_total,
        }
    }

    #[inline]
    pub fn record_messages_in_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.messages_in_total.add(count, attributes);
    }

    #[inline]
    pub fn record_messages_out_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.messages_out_total.add(count, attributes);
    }

    #[inline]
    pub fn record_throughput_in_total(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.throughput_in_total.add(bytes, attributes);
    }

    #[inline]
    pub fn record_throughput_out_total(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.throughput_out_total.add(bytes, attributes);
    }

    #[inline]
    pub fn record_message_size(&self, size: u64, attributes: &[opentelemetry::KeyValue]) {
        self.message_size.record(size, attributes);
    }

    #[inline]
    pub fn record_send_message_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.send_message_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_metrics_label_dropped_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.metrics_label_dropped_total.add(count, attributes);
    }
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn broker_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("broker-metrics-test");
        let metrics = BrokerMetrics::new(&meter);
        let attrs = [opentelemetry::KeyValue::new("topic", "test-topic")];

        metrics.record_messages_in_total(1, &attrs);
        metrics.record_messages_out_total(1, &attrs);
        metrics.record_throughput_in_total(128, &attrs);
        metrics.record_throughput_out_total(64, &attrs);
        metrics.record_message_size(128, &attrs);
        metrics.record_send_message_latency(10, &attrs);
        metrics.record_metrics_label_dropped_total(1, &attrs);
    }
}
