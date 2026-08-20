use std::collections::HashMap;

use rocketmq_observability::{
    LogsExporter, MetricsExporter, ObservabilityConfig, ObservabilityOverrides, OtlpProtocol, TraceExporter,
};

#[test]
fn file_overrides_deserialize_camel_case_without_filling_absent_fields() {
    let overrides: ObservabilityOverrides = serde_yaml::from_str(
        r#"
metrics:
  exporter: prometheus
  exportIntervalMillis: 5000
traces:
  exporter: otlp_grpc
  sampleRatio: 0.25
logs:
  exporter: log
otlp:
  endpoint: http://collector:4317
"#,
    )
    .expect("observability file overrides should deserialize");

    assert_eq!(overrides.metrics.exporter, Some(MetricsExporter::Prometheus));
    assert_eq!(overrides.metrics.export_interval_millis, Some(5_000));
    assert_eq!(overrides.metrics.cardinality_limit, None);
    assert_eq!(overrides.traces.exporter, Some(TraceExporter::OtlpGrpc));
    assert_eq!(overrides.traces.sample_ratio, Some(0.25));
    assert_eq!(overrides.logs.exporter, Some(LogsExporter::Log));
    assert_eq!(overrides.otlp.endpoint.as_deref(), Some("http://collector:4317"));
}

#[test]
fn file_overrides_replace_configured_fields_and_preserve_service_identity() {
    let overrides: ObservabilityOverrides = serde_yaml::from_str(
        r#"
environment: production
serviceInstanceId: node-42
resourceAttributes:
  deployment.region: shanghai
metrics:
  exporter: prometheus
  sampleRatio: 0.5
traces:
  exporter: otlp_grpc
  recordMessageId: true
logs:
  exporter: disable
otlp:
  endpoint: http://collector:4317
  protocol: http_json
  headers: {}
  timeoutMillis: 9000
prometheus:
  host: 0.0.0.0
  port: 9464
  path: /observability/metrics
"#,
    )
    .expect("observability file overrides should deserialize");
    let mut target = ObservabilityConfig {
        service_name: "broker".to_string(),
        service_namespace: "broker-namespace".to_string(),
        resource_attributes: HashMap::from([(String::from("old"), String::from("value"))]),
        ..ObservabilityConfig::default()
    };
    target.otlp.headers = HashMap::from([(String::from("authorization"), String::from("secret"))]);

    overrides.apply_to(&mut target);

    assert_eq!(target.service_name, "broker");
    assert_eq!(target.service_namespace, "broker-namespace");
    assert_eq!(target.environment, "production");
    assert_eq!(target.service_instance_id, "node-42");
    assert_eq!(
        target.resource_attributes,
        HashMap::from([(String::from("deployment.region"), String::from("shanghai"))])
    );
    assert_eq!(target.metrics.exporter, MetricsExporter::Prometheus);
    assert!(target.metrics.enabled);
    assert_eq!(target.metrics.sample_ratio, 0.5);
    assert_eq!(target.traces.exporter, TraceExporter::OtlpGrpc);
    assert!(target.traces.enabled);
    assert!(target.traces.record_message_id);
    assert_eq!(target.logs.exporter, LogsExporter::Disable);
    assert!(!target.logs.enabled);
    assert_eq!(target.otlp.endpoint, "http://collector:4317");
    assert_eq!(target.otlp.protocol, OtlpProtocol::HttpJson);
    assert!(target.otlp.headers.is_empty());
    assert_eq!(target.otlp.timeout_millis, 9_000);
    assert_eq!(target.prometheus.host, "0.0.0.0");
    assert_eq!(target.prometheus.port, 9_464);
    assert_eq!(target.prometheus.path, "/observability/metrics");
    assert!(target.enabled);
}
