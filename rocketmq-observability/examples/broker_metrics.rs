#[cfg(feature = "otlp-metrics")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry::KeyValue;
    use rocketmq_observability::config::MetricsExporter;
    use rocketmq_observability::metrics::broker::BrokerMetrics;
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
    let provider = guard.meter_provider().expect("meter provider should be initialized");
    let meter = provider.meter("rocketmq-broker-example");
    let metrics = BrokerMetrics::new(&meter);
    let attributes = [KeyValue::new("topic", "example-topic")];

    metrics.record_messages_in_total(1, &attributes);
    metrics.record_throughput_in_total(128, &attributes);
    metrics.record_message_size(128, &attributes);
    metrics.record_send_message_latency(12, &attributes);

    provider.force_flush()?;
    guard.shutdown()?;
    Ok(())
}

#[cfg(not(feature = "otlp-metrics"))]
fn main() {
    eprintln!("Run with: cargo run -p rocketmq-observability --example broker_metrics --features otlp-metrics");
}
