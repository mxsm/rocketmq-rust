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

use std::collections::HashMap;

use rocketmq_observability::{
    LogsExporter, MetricsExporter, ObservabilityConfig, ObservabilityOverrides, OtlpProtocol, TraceExporter,
};

const ENDPOINT_CANARY: &str = "http://collector.invalid:4317/ENDPOINT_CANARY";
const HEADER_KEY_CANARY: &str = "x-header-key-canary";
const HEADER_VALUE_CANARY: &str = "Bearer HEADER_VALUE_CANARY";
const RESOURCE_KEY_CANARY: &str = "resource.key.canary";
const RESOURCE_VALUE_CANARY: &str = "RESOURCE_VALUE_CANARY";
const RELEASE_NONCE_CANARY: &str = "release-nonce-canary";
const METRICS_PATH_CANARY: &str = "/metrics-path-canary";

fn assert_resolution_debug_values_are_absent(output: &str) {
    for canary in [
        ENDPOINT_CANARY,
        HEADER_KEY_CANARY,
        HEADER_VALUE_CANARY,
        RESOURCE_KEY_CANARY,
        RESOURCE_VALUE_CANARY,
        RELEASE_NONCE_CANARY,
        METRICS_PATH_CANARY,
    ] {
        assert!(!output.contains(canary), "Debug output exposed {canary}: {output}");
    }
}

#[test]
fn telemetry_environment_values_debug_reports_presence_without_values() {
    let environment = rocketmq_observability::TelemetryEnvironmentValues {
        release_nonce: Some(RELEASE_NONCE_CANARY.into()),
        metrics_path: Some(METRICS_PATH_CANARY.into()),
        otlp_endpoint: Some(ENDPOINT_CANARY.into()),
        otlp_protocol: Some("grpc".into()),
        ..rocketmq_observability::TelemetryEnvironmentValues::default()
    };

    let output = format!("{environment:?}");

    assert_resolution_debug_values_are_absent(&output);
    assert!(output.contains("otlp_endpoint_present"));
    assert!(output.contains("release_nonce_present"));
}

#[test]
fn telemetry_resolution_debug_redacts_bootstrap_and_process_values() {
    let file: ObservabilityOverrides = serde_yaml::from_str(&format!(
        "metrics:\n  exporter: otlp_grpc\notlp:\n  endpoint: {ENDPOINT_CANARY}\n  protocol: grpc\n  headers:\n    {HEADER_KEY_CANARY}: {HEADER_VALUE_CANARY}\nresourceAttributes:\n  {RESOURCE_KEY_CANARY}: {RESOURCE_VALUE_CANARY}\n"
    ))
    .expect("sensitive observability fixture should deserialize");
    let environment = rocketmq_observability::TelemetryEnvironmentValues {
        release_nonce: Some(RELEASE_NONCE_CANARY.into()),
        metrics_path: Some(METRICS_PATH_CANARY.into()),
        ..rocketmq_observability::TelemetryEnvironmentValues::default()
    };

    let resolution = rocketmq_observability::resolve_telemetry_values(
        "rocketmq-broker",
        rocketmq_observability::TelemetryBootstrapConfig::default(),
        &file,
        &environment,
        rocketmq_observability::TelemetryEnvironmentSpec::default(),
    )
    .expect("sensitive observability fixture should resolve");
    let output = format!("{resolution:?}");

    assert_resolution_debug_values_are_absent(&output);
    assert!(output.contains("TelemetryResolution"));
}

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

#[test]
fn absent_environment_values_preserve_file_metrics_configuration() {
    let bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
    let file: rocketmq_observability::ObservabilityOverrides = serde_yaml::from_str(
        "metrics:\n  exporter: prometheus\nprometheus:\n  host: 127.0.0.1\n  port: 9464\n  path: /rocketmq\n",
    )
    .unwrap();

    let resolved = rocketmq_observability::resolve_telemetry_values(
        "rocketmq-broker",
        bootstrap,
        &file,
        &rocketmq_observability::TelemetryEnvironmentValues::default(),
        rocketmq_observability::TelemetryEnvironmentSpec::default(),
    )
    .expect("file-only telemetry should resolve");

    assert!(resolved.bootstrap.observability.metrics.enabled);
    assert_eq!(
        resolved.bootstrap.observability.metrics.exporter,
        MetricsExporter::Prometheus
    );
    assert_eq!(resolved.prometheus_listener_addr.unwrap().port(), 9464);
}

#[test]
fn present_environment_values_override_file_fields_only() {
    let bootstrap = rocketmq_observability::TelemetryBootstrapConfig::default();
    let file: rocketmq_observability::ObservabilityOverrides = serde_yaml::from_str(
        "metrics:\n  exporter: prometheus\ntraces:\n  sampleRatio: 0.25\notlp:\n  endpoint: http://file-collector:4317\n",
    )
    .unwrap();
    let environment = rocketmq_observability::TelemetryEnvironmentValues {
        metrics_enabled: Some("true".into()),
        metrics_exporter: Some("otlp_grpc".into()),
        otlp_endpoint: Some("http://env-collector:4317".into()),
        otlp_protocol: Some("grpc".into()),
        ..rocketmq_observability::TelemetryEnvironmentValues::default()
    };

    let resolved = rocketmq_observability::resolve_telemetry_values(
        "rocketmq-broker",
        bootstrap,
        &file,
        &environment,
        rocketmq_observability::TelemetryEnvironmentSpec::default(),
    )
    .expect("environment telemetry should override matching file fields");

    assert_eq!(
        resolved.bootstrap.observability.metrics.exporter,
        MetricsExporter::OtlpGrpc
    );
    assert_eq!(
        resolved.bootstrap.observability.otlp.endpoint,
        "http://env-collector:4317"
    );
    assert_eq!(resolved.bootstrap.observability.traces.sample_ratio, 0.25);
    assert!(resolved.environment_applied);
}

#[test]
fn missing_or_blank_otlp_endpoint_preserves_file_configuration() {
    let file: rocketmq_observability::ObservabilityOverrides =
        serde_yaml::from_str("otlp:\n  endpoint: http://file-collector:4317\n  protocol: grpc\n").unwrap();
    let environments = [
        rocketmq_observability::TelemetryEnvironmentValues {
            otlp_protocol: Some("http/protobuf".into()),
            ..rocketmq_observability::TelemetryEnvironmentValues::default()
        },
        rocketmq_observability::TelemetryEnvironmentValues {
            otlp_endpoint: Some("  ".into()),
            otlp_protocol: Some("grpc".into()),
            ..rocketmq_observability::TelemetryEnvironmentValues::default()
        },
    ];

    for environment in environments {
        let resolved = rocketmq_observability::resolve_telemetry_values(
            "rocketmq-broker",
            rocketmq_observability::TelemetryBootstrapConfig::default(),
            &file,
            &environment,
            rocketmq_observability::TelemetryEnvironmentSpec::default(),
        )
        .expect("missing or blank OTLP endpoint should preserve file configuration");

        assert_eq!(
            resolved.bootstrap.observability.otlp.endpoint,
            "http://file-collector:4317"
        );
        assert_eq!(resolved.bootstrap.observability.otlp.protocol, OtlpProtocol::Grpc);
        assert!(!resolved.environment_applied);
    }
}

#[test]
fn invalid_standard_otlp_environment_fails_without_exposing_values() {
    let cases = [
        (
            rocketmq_observability::TelemetryEnvironmentValues {
                otlp_endpoint: Some("http://missing-protocol:4317".into()),
                ..rocketmq_observability::TelemetryEnvironmentValues::default()
            },
            "http://missing-protocol:4317",
        ),
        (
            rocketmq_observability::TelemetryEnvironmentValues {
                otlp_endpoint: Some("http://unsupported-protocol:4317".into()),
                otlp_protocol: Some("http/protobuf".into()),
                ..rocketmq_observability::TelemetryEnvironmentValues::default()
            },
            "http://unsupported-protocol:4317",
        ),
        (
            rocketmq_observability::TelemetryEnvironmentValues {
                otlp_endpoint: Some("http://user:secret@collector:4317".into()),
                otlp_protocol: Some("http/protobuf".into()),
                ..rocketmq_observability::TelemetryEnvironmentValues::default()
            },
            "http://user:secret@collector:4317",
        ),
    ];

    for (environment, sensitive_value) in cases {
        let error = rocketmq_observability::resolve_telemetry_values(
            "rocketmq-broker",
            rocketmq_observability::TelemetryBootstrapConfig::default(),
            &rocketmq_observability::ObservabilityOverrides::default(),
            &environment,
            rocketmq_observability::TelemetryEnvironmentSpec::default(),
        )
        .expect_err("invalid standard OTLP environment must fail closed");
        let message = error.to_string();

        assert!(!message.contains(sensitive_value));
        assert!(!message.contains("secret"));
    }
}

#[test]
fn service_trace_sample_ratio_environment_overrides_the_file_field() {
    let file: rocketmq_observability::ObservabilityOverrides =
        serde_yaml::from_str("traces:\n  sampleRatio: 0.25\n").unwrap();
    for (environment_value, expected) in [("0", 0.0), ("  1.0  ", 1.0)] {
        let environment = rocketmq_observability::TelemetryEnvironmentValues {
            trace_sample_ratio: Some(environment_value.into()),
            ..rocketmq_observability::TelemetryEnvironmentValues::default()
        };

        let resolved = rocketmq_observability::resolve_telemetry_values(
            "rocketmq-broker",
            rocketmq_observability::TelemetryBootstrapConfig::default(),
            &file,
            &environment,
            rocketmq_observability::TelemetryEnvironmentSpec {
                trace_sample_ratio_env: Some("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"),
            },
        )
        .expect("boundary trace sample ratios should override the file field");

        assert_eq!(resolved.bootstrap.observability.traces.sample_ratio, expected);
        assert!(resolved.environment_applied);
    }
}

#[test]
fn shared_validation_rejects_invalid_numeric_settings() {
    let mut configs = Vec::new();
    for sample_ratio in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        let mut metrics = ObservabilityConfig::default();
        metrics.metrics.sample_ratio = sample_ratio;
        configs.push(metrics);

        let mut traces = ObservabilityConfig::default();
        traces.traces.sample_ratio = sample_ratio;
        configs.push(traces);
    }
    let mut export_interval = ObservabilityConfig::default();
    export_interval.metrics.export_interval_millis = 0;
    configs.push(export_interval);
    let mut export_timeout = ObservabilityConfig::default();
    export_timeout.metrics.export_timeout_millis = 0;
    configs.push(export_timeout);
    let mut cardinality_limit = ObservabilityConfig::default();
    cardinality_limit.metrics.cardinality_limit = 0;
    configs.push(cardinality_limit);
    let mut otlp_timeout = ObservabilityConfig::default();
    otlp_timeout.otlp.timeout_millis = 0;
    configs.push(otlp_timeout);

    for config in configs {
        assert!(rocketmq_observability::normalize_and_validate(&config).is_err());
    }
}

#[test]
fn shared_validation_rejects_invalid_otlp_without_exposing_endpoint() {
    let sensitive_endpoint = "http://user:secret@collector:4317";
    let mut blank_endpoint = ObservabilityConfig::default();
    blank_endpoint.metrics.enabled = true;
    blank_endpoint.metrics.exporter = MetricsExporter::OtlpGrpc;
    blank_endpoint.otlp.endpoint = "  ".to_owned();
    assert!(rocketmq_observability::normalize_and_validate(&blank_endpoint).is_err());

    let mut invalid_protocol = ObservabilityConfig::default();
    invalid_protocol.traces.enabled = true;
    invalid_protocol.traces.exporter = TraceExporter::OtlpGrpc;
    invalid_protocol.otlp.endpoint = sensitive_endpoint.to_owned();
    invalid_protocol.otlp.protocol = OtlpProtocol::HttpBinary;

    let error = rocketmq_observability::normalize_and_validate(&invalid_protocol)
        .expect_err("unsupported OTLP protocol should fail");
    let message = error.to_string();
    assert!(!message.contains(sensitive_endpoint));
    assert!(!message.contains("secret"));
}

#[test]
fn shared_validation_requires_canonical_prometheus_configuration() {
    let mut invalid_host = ObservabilityConfig::default();
    invalid_host.prometheus.host = "localhost".to_owned();
    assert!(rocketmq_observability::normalize_and_validate(&invalid_host).is_err());

    let mut zero_port = ObservabilityConfig::default();
    zero_port.prometheus.port = 0;
    assert!(rocketmq_observability::normalize_and_validate(&zero_port).is_err());

    for path in ["metrics", "/metrics/", "/metrics//internal", "/metrics?token=secret"] {
        let mut invalid_path = ObservabilityConfig::default();
        invalid_path.prometheus.path = path.to_owned();
        assert!(rocketmq_observability::normalize_and_validate(&invalid_path).is_err());
    }
}

#[test]
fn invalid_trace_sample_ratio_environment_reports_only_the_variable_name() {
    for invalid_value in ["-0.1", "1.1", "NaN", "inf"] {
        let environment = rocketmq_observability::TelemetryEnvironmentValues {
            trace_sample_ratio: Some(invalid_value.into()),
            ..rocketmq_observability::TelemetryEnvironmentValues::default()
        };

        let error = rocketmq_observability::resolve_telemetry_values(
            "rocketmq-broker",
            rocketmq_observability::TelemetryBootstrapConfig::default(),
            &rocketmq_observability::ObservabilityOverrides::default(),
            &environment,
            rocketmq_observability::TelemetryEnvironmentSpec {
                trace_sample_ratio_env: Some("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"),
            },
        )
        .expect_err("invalid trace sample ratio should fail");
        let message = error.to_string();

        assert!(message.contains("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"));
        assert!(!message.contains(invalid_value));
    }

    for invalid_value in ["", "   ", "secret-value"] {
        let environment = rocketmq_observability::TelemetryEnvironmentValues {
            trace_sample_ratio: Some(invalid_value.into()),
            ..rocketmq_observability::TelemetryEnvironmentValues::default()
        };

        let message = rocketmq_observability::resolve_telemetry_values(
            "rocketmq-broker",
            rocketmq_observability::TelemetryBootstrapConfig::default(),
            &rocketmq_observability::ObservabilityOverrides::default(),
            &environment,
            rocketmq_observability::TelemetryEnvironmentSpec {
                trace_sample_ratio_env: Some("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"),
            },
        )
        .expect_err("empty or non-numeric trace sample ratio should fail")
        .to_string();

        assert_eq!(
            message,
            "invalid observability config: ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO must be a floating-point number"
        );
    }
}

#[cfg(any(unix, windows))]
#[test]
fn non_utf8_trace_sample_ratio_reports_only_the_variable_name() {
    #[cfg(unix)]
    let invalid_value = {
        use std::os::unix::ffi::OsStringExt;
        std::ffi::OsString::from_vec(vec![0xff])
    };
    #[cfg(windows)]
    let invalid_value = {
        use std::os::windows::ffi::OsStringExt;
        std::ffi::OsString::from_wide(&[0xd800])
    };
    let environment = rocketmq_observability::TelemetryEnvironmentValues {
        trace_sample_ratio: Some(invalid_value),
        ..rocketmq_observability::TelemetryEnvironmentValues::default()
    };

    let message = rocketmq_observability::resolve_telemetry_values(
        "rocketmq-broker",
        rocketmq_observability::TelemetryBootstrapConfig::default(),
        &rocketmq_observability::ObservabilityOverrides::default(),
        &environment,
        rocketmq_observability::TelemetryEnvironmentSpec {
            trace_sample_ratio_env: Some("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"),
        },
    )
    .expect_err("non-UTF-8 trace sample ratio should fail")
    .to_string();

    assert!(message.contains("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"));
}

#[test]
fn explicit_metrics_disable_normalizes_global_observability_state() {
    let file: rocketmq_observability::ObservabilityOverrides =
        serde_yaml::from_str("metrics:\n  exporter: prometheus\n").unwrap();
    let environment = rocketmq_observability::TelemetryEnvironmentValues {
        metrics_enabled: Some("false".into()),
        ..rocketmq_observability::TelemetryEnvironmentValues::default()
    };

    let resolved = rocketmq_observability::resolve_telemetry_values(
        "rocketmq-broker",
        rocketmq_observability::TelemetryBootstrapConfig::default(),
        &file,
        &environment,
        rocketmq_observability::TelemetryEnvironmentSpec::default(),
    )
    .expect("explicit metrics disable should resolve");

    assert!(!resolved.bootstrap.observability.metrics.enabled);
    assert_eq!(
        resolved.bootstrap.observability.metrics.exporter,
        MetricsExporter::Disable
    );
    assert!(!resolved.bootstrap.observability.enabled);
    assert_eq!(resolved.prometheus_listener_addr, None);
    assert!(resolved.environment_applied);
}

#[test]
fn standard_otlp_environment_clears_the_file_prometheus_listener() {
    let file: rocketmq_observability::ObservabilityOverrides =
        serde_yaml::from_str("metrics:\n  exporter: prometheus\nprometheus:\n  host: 127.0.0.1\n  port: 9464\n")
            .unwrap();
    let environment = rocketmq_observability::TelemetryEnvironmentValues {
        otlp_endpoint: Some("http://env-collector:4317".into()),
        otlp_protocol: Some("grpc".into()),
        ..rocketmq_observability::TelemetryEnvironmentValues::default()
    };

    let resolved = rocketmq_observability::resolve_telemetry_values(
        "rocketmq-broker",
        rocketmq_observability::TelemetryBootstrapConfig::default(),
        &file,
        &environment,
        rocketmq_observability::TelemetryEnvironmentSpec::default(),
    )
    .expect("standard OTLP environment should replace file Prometheus metrics");

    assert_eq!(
        resolved.bootstrap.observability.metrics.exporter,
        MetricsExporter::OtlpGrpc
    );
    assert!(resolved.process.metrics_enabled());
    assert_eq!(resolved.process.metrics_exporter(), MetricsExporter::OtlpGrpc);
    assert_eq!(resolved.prometheus_listener_addr, None);
}
