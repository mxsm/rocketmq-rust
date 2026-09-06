// Copyright 2026 The RocketMQ Rust Authors
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

#[cfg(feature = "otlp-metrics")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use opentelemetry::KeyValue;
    use rocketmq_observability::metrics::broker::BrokerMetrics;
    use rocketmq_observability::MetricsExporter;
    use rocketmq_observability::ObservabilityConfig;

    let mut config = ObservabilityConfig {
        enabled: true,
        service_name: "rocketmq-broker".to_string(),
        node_type: "broker".to_string(),
        node_id: "broker-a".to_string(),
        ..ObservabilityConfig::default()
    };
    config.metrics.enabled = true;
    config.metrics.exporter = MetricsExporter::OtlpGrpc;
    config.metrics.export_interval_millis = 5_000;
    config.metrics.export_timeout_millis = 3_000;
    config.otlp.endpoint = "http://127.0.0.1:4317".to_string();

    let guard = rocketmq_observability::init_observability(&config)?;
    let metrics = BrokerMetrics::from_handle(&guard.handle()).expect("broker metrics should be initialized");
    let attributes = [KeyValue::new("topic", "example-topic")];

    metrics.record_messages_in_total(1, &attributes);
    metrics.record_throughput_in_total(128, &attributes);
    metrics.record_message_size(128, &attributes);
    metrics.record_send_message_latency(12, &attributes);

    guard.shutdown().into_result()?;
    Ok(())
}

#[cfg(not(feature = "otlp-metrics"))]
fn main() {
    eprintln!("Run with: cargo run -p rocketmq-observability --example broker_metrics --features otlp-metrics");
}
