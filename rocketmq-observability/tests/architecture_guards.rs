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

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

const WORKSPACE_CRATE_DIRS: &[&str] = &[
    "rocketmq-auth",
    "rocketmq-broker",
    "rocketmq-client",
    "rocketmq-controller",
    "rocketmq-error",
    "rocketmq-filter",
    "rocketmq-macros",
    "rocketmq-model",
    "rocketmq-namesrv",
    "rocketmq-protocol",
    "rocketmq-proxy",
    "rocketmq-proxy-cluster",
    "rocketmq-proxy-core",
    "rocketmq-proxy-local",
    "rocketmq-runtime",
    "rocketmq-security-api",
    "rocketmq-store",
    "rocketmq-store-api",
    "rocketmq-store-local",
    "rocketmq-store-rocksdb",
    "rocketmq-tieredstore",
    "rocketmq-transport",
    "rocketmq-dashboard/rocketmq-dashboard-common",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-cli",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui",
    "rocketmq-tools/rocketmq-store-inspect",
    "rocketmq-ai/rocketmq-mcp",
    "rocketmq-dashboard/rocketmq-dashboard-gpui",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend",
];

const GOVERNED_ENTRYPOINTS: &[&str] = &[
    "rocketmq-broker/src/bin/broker_bootstrap_server.rs",
    "rocketmq-controller/src/bin/controller_bootstrap.rs",
    "rocketmq-namesrv/src/bin/namesrv_bootstrap_server.rs",
    "rocketmq-proxy/src/bin/rocketmq-proxy-rust.rs",
    "rocketmq-ai/rocketmq-mcp/src/app.rs",
    "rocketmq-dashboard/rocketmq-dashboard-gpui/src/main.rs",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/src/main.rs",
];

const CORE_SERVICE_ENTRYPOINTS: &[&str] = &[
    "rocketmq-broker/src/bin/broker_bootstrap_server.rs",
    "rocketmq-controller/src/bin/controller_bootstrap.rs",
    "rocketmq-namesrv/src/bin/namesrv_bootstrap_server.rs",
    "rocketmq-proxy/src/bin/rocketmq-proxy-rust.rs",
];

const TELEMETRY_ENV_GOVERNED_CRATE_DIRS: &[&str] = &[
    "rocketmq-broker",
    "rocketmq-namesrv",
    "rocketmq-controller",
    "rocketmq-proxy",
    "rocketmq-ai/rocketmq-mcp",
    "rocketmq-observability",
];

const TELEMETRY_ENV_READ_ALLOWLIST: &[&str] = &["rocketmq-observability/src/resolver.rs"];

const REMOVED_FLAT_TELEMETRY_KEYS: &[&str] = &[
    "observabilityEnvironment",
    "observabilityServiceInstanceId",
    "observabilityResourceAttributes",
    "metricsExporterType",
    "metricsExportIntervalMillis",
    "metricsCardinalityLimit",
    "metricsSampleRatio",
    "metricsTopicLabelEnabled",
    "metricsConsumerGroupLabelEnabled",
    "otlpExporterEndpoint",
    "otlpExporterHeaders",
    "otlpExporterTimeoutMillis",
    "metricsPromExporterHost",
    "metricsPromExporterPort",
    "metricsPromExporterPath",
    "traceExporterType",
    "traceSampleRatio",
    "tracePropagateContext",
    "traceRecordMessageId",
    "traceRecordMessageKeys",
    "traceRecordBodySize",
    "logExporterType",
    "metricsGrpcExporterTarget",
    "metricsGrpcExporterHeader",
    "metricGrpcExporterTimeOutInMills",
    "metricGrpcExporterIntervalInMills",
    "metricLoggingExporterIntervalInMills",
    "metricsLabel",
    "metricsInDelta",
];

const RUNTIME_CONFIGURATION_AND_DOCUMENTATION_PATHS: &[&str] = &[
    "distribution/helm/rocketmq-rust/templates",
    "distribution/kubernetes/base",
    "docker/smoke-config",
    "rocketmq-ai/rocketmq-sre/deploy/dev/config",
    "rocketmq-ai/rocketmq-sre/scripts",
    "rocketmq-broker/README.md",
    "rocketmq-broker/README-zh_cn.md",
    "rocketmq-controller/README.md",
    "rocketmq-controller/README-zh_cn.md",
    "rocketmq-example/examples/broker_observability.yaml",
    "rocketmq-website/docs/configuration/observability.md",
    "rocketmq-website/i18n/zh-CN/docusaurus-plugin-content-docs/current/configuration/observability.md",
];

const DIRECT_OTEL_PATTERNS: &[&str] = &[
    "use opentelemetry",
    "opentelemetry::",
    "use opentelemetry_sdk",
    "opentelemetry_sdk::",
];

const DIRECT_OTEL_LEGACY_ALLOWLIST: &[&str] = &[];

const METRIC_CONSTANT_CANONICAL_FILES: &[&str] = &[
    "rocketmq-observability/src/metrics/broker_constants.rs",
    "rocketmq-observability/src/metrics/catalog.rs",
    "rocketmq-observability/src/metrics/controller_constants.rs",
    "rocketmq-observability/src/metrics/pop_constants.rs",
    "rocketmq-observability/src/semantic.rs",
];

const METRIC_CONSTANT_LEGACY_ALLOWLIST: &[&str] = &[];

const SUBSCRIBER_INSTALL_PATTERNS: &[&str] = &[
    "tracing::subscriber::set_global_default",
    "tracing_subscriber::subscriber::set_global_default",
];

const SUBSCRIBER_INSTALL_ALLOWLIST: &[&str] = &[
    // Legacy observability entrypoint retained for source compatibility while production entries use logging.rs.
    "rocketmq-observability/src/init.rs",
    // Unified logging and telemetry bootstrap owns the new production subscriber installation path.
    "rocketmq-observability/src/logging.rs",
    // Trace-only compatibility helper retained for callers that have not migrated to install_global yet.
    "rocketmq-observability/src/trace.rs",
];

const CONTROLLER_METRIC_LITERAL_MARKERS: &[&str] = &[
    "\"role\"",
    "\"dledger_disk_usage\"",
    "\"active_broker_num\"",
    "\"request_total\"",
    "\"dledger_op_total\"",
    "\"election_total\"",
    "\"request_latency\"",
    "\"dledger_op_latency\"",
];

const ROCKETMQ_METRIC_SUFFIX_MARKERS: &[&str] = &[
    "_behind\"",
    "_bytes\"",
    "_connections\"",
    "_consume\"",
    "_latency\"",
    "_lag\"",
    "_messages\"",
    "_number\"",
    "_permission\"",
    "_size\"",
    "_snapshot\"",
    "_throughput\"",
    "_time\"",
    "_total\"",
    "_up\"",
    "_usage\"",
    "_value\"",
    "_watermark\"",
];

#[test]
fn business_crates_do_not_add_direct_opentelemetry_usage() {
    let workspace_root = workspace_root();
    let allowlist = path_set(DIRECT_OTEL_LEGACY_ALLOWLIST);
    let mut unexpected_files = BTreeSet::new();

    for file in workspace_src_files(&workspace_root, WORKSPACE_CRATE_DIRS) {
        let relative_path = relative_slash_path(&workspace_root, &file);
        if allowlist.contains(relative_path.as_str()) {
            continue;
        }

        let source =
            fs::read_to_string(&file).unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        if DIRECT_OTEL_PATTERNS.iter().any(|pattern| source.contains(pattern)) {
            unexpected_files.insert(relative_path);
        }
    }

    assert!(
        unexpected_files.is_empty(),
        "direct OpenTelemetry usage must live in rocketmq-observability; migrate or explicitly track legacy files \
         before adding new usages:\n{}",
        format_paths(&unexpected_files)
    );
}

#[test]
fn observability_dependency_closure_excludes_deleted_facades_and_transport() {
    let workspace_root = workspace_root();
    let manifest = fs::read_to_string(workspace_root.join("rocketmq-observability/Cargo.toml"))
        .expect("observability manifest should be readable");
    for forbidden in ["rocketmq-common", "rocketmq-remoting", "rocketmq-rust"] {
        assert!(
            !manifest.lines().any(|line| line.trim_start().starts_with(forbidden)),
            "observability manifest must not depend on {forbidden}"
        );
    }

    let mut forbidden_sources = BTreeSet::new();
    for file in workspace_src_files(&workspace_root, &["rocketmq-observability"]) {
        let source =
            fs::read_to_string(&file).unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        if source.contains("rocketmq_common::") || source.contains("rocketmq_transport::") {
            forbidden_sources.insert(relative_slash_path(&workspace_root, &file));
        }
    }
    assert!(
        forbidden_sources.is_empty(),
        "observability source closure must use model or owner adapters:\n{}",
        format_paths(&forbidden_sources)
    );
}

#[test]
fn subscriber_installation_sites_are_tracked() {
    let workspace_root = workspace_root();
    let mut allowed_files = path_set(SUBSCRIBER_INSTALL_ALLOWLIST);
    let mut scan_dirs = WORKSPACE_CRATE_DIRS.to_vec();
    scan_dirs.push("rocketmq-observability");

    let mut unexpected_files = BTreeSet::new();
    for file in workspace_src_files(&workspace_root, &scan_dirs) {
        let relative_path = relative_slash_path(&workspace_root, &file);
        if allowed_files.remove(relative_path.as_str()) {
            continue;
        }

        let source =
            fs::read_to_string(&file).unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        if has_subscriber_installation(&source) {
            unexpected_files.insert(relative_path);
        }
    }

    assert!(
        unexpected_files.is_empty(),
        "tracing subscriber installation must stay in tracked bootstrap files:\n{}",
        format_paths(&unexpected_files)
    );
}

#[test]
fn subscriber_installation_detector_catches_direct_fmt_init() {
    let source = r#"
        fn main() {
            tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();
        }
    "#;

    assert!(has_subscriber_installation(source));
}

#[test]
fn subscriber_installation_detector_catches_imported_init_extension() {
    let source = r#"
        use tracing_subscriber::fmt;
        use tracing_subscriber::util::SubscriberInitExt;

        fn install() {
            fmt().with_target(true).try_init().expect("subscriber installs");
        }
    "#;

    assert!(has_subscriber_installation(source));
}

#[test]
fn subscriber_installation_detector_ignores_unrelated_init_methods() {
    let source = r#"
        fn install_store(store: &mut Store) {
            store.init();
            let _ = ratatui::try_init();
        }
    "#;

    assert!(!has_subscriber_installation(source));
}

#[test]
fn subscriber_installation_detector_ignores_construction_and_scoped_capture() {
    for construction in ["tracing_subscriber::fmt().finish()", "tracing_subscriber::registry()"] {
        for installation in [
            "",
            "let _guard = tracing::subscriber::set_default(subscriber);",
            "tracing::subscriber::with_default(subscriber, || capture_logs());",
        ] {
            let source = format!("fn capture() {{ let subscriber = {construction}; {installation} store.init(); }}");
            assert!(!has_subscriber_installation(&source), "{source}");
        }
    }
}

#[test]
fn subscriber_installation_detector_catches_global_setters() {
    for source in [
        "tracing::subscriber::set_global_default(subscriber).unwrap();",
        "tracing_subscriber::subscriber::set_global_default(subscriber).unwrap();",
        "use tracing::subscriber::set_global_default; set_global_default(subscriber).unwrap();",
    ] {
        assert!(has_subscriber_installation(source), "{source}");
    }
}

#[test]
fn subscriber_installation_detector_catches_registry_and_bound_subscriber_init() {
    for construction in ["tracing_subscriber::fmt()", "tracing_subscriber::registry()"] {
        for method in ["init", "try_init"] {
            let direct = format!("{construction}.{method}();");
            assert!(has_subscriber_installation(&direct), "{direct}");
            let bound = format!("let subscriber = {construction}; subscriber.{method}();");
            assert!(has_subscriber_installation(&bound), "{bound}");
        }
    }
}

#[test]
fn subscriber_installation_detector_does_not_let_scoped_capture_hide_global_init() {
    let source = r#"
        let subscriber = tracing_subscriber::fmt().finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        tracing_subscriber::registry().with(layer).try_init().unwrap();
    "#;
    assert!(has_subscriber_installation(source));
}

#[test]
fn governed_entrypoints_do_not_bypass_the_shared_log_filter_resolver() {
    let workspace_root = workspace_root();
    let forbidden = [
        "std::env::var(\"RUST_LOG\")",
        "env::var(\"RUST_LOG\")",
        "EnvFilter::try_from_default_env",
        "with_max_level(tracing::Level::",
    ];
    let mut violations = BTreeSet::new();
    for relative_path in GOVERNED_ENTRYPOINTS {
        let path = workspace_root.join(relative_path);
        let source =
            fs::read_to_string(&path).unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        if forbidden.iter().any(|pattern| source.contains(pattern)) {
            violations.insert((*relative_path).to_string());
        }
    }

    assert!(
        violations.is_empty(),
        "governed entrypoints must use rocketmq-observability for RUST_LOG/default handling:\n{}",
        format_paths(&violations)
    );
}

#[test]
fn governed_services_do_not_read_telemetry_environment_outside_the_shared_resolver() {
    let workspace_root = workspace_root();
    let allowlist = path_set(TELEMETRY_ENV_READ_ALLOWLIST);
    let mut violations = BTreeSet::new();

    for file in workspace_src_files(&workspace_root, TELEMETRY_ENV_GOVERNED_CRATE_DIRS) {
        let relative_path = relative_slash_path(&workspace_root, &file);
        if allowlist.contains(relative_path.as_str()) {
            continue;
        }

        let source =
            fs::read_to_string(&file).unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        if has_direct_telemetry_environment_read(&source) {
            violations.insert(relative_path);
        }
    }

    assert!(
        violations.is_empty(),
        "Broker, NameServer, Controller, Proxy, and MCP must delegate telemetry environment reads to the shared \
         resolver:\n{}",
        format_paths(&violations)
    );
}

#[test]
fn telemetry_environment_read_detector_catches_supported_direct_forms() {
    for source in [
        r#"let _ = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT");"#,
        r#"let _ = std::env::var_os ( "ROCKETMQ_METRICS_EXPORTER" );"#,
        r#"let _ = std::env::var_os(OTEL_EXPORTER_OTLP_PROTOCOL);"#,
        r#"let _ = std::env::var(METRICS_ENABLED_ENV);"#,
        r#"use std::env; let _ = env::var("NAMESRV_ADDR");"#,
        r#"use std::env::var_os;"#,
        r#"use std::env as process_env;"#,
        r#"use std::env::*;"#,
        r#"use std::env::{self};"#,
        r#"// Copyright header
use std::env;
let _ = env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT");"#,
        r#"fn read() { use std::env; let _ = env::var("ROCKETMQ_METRICS_ENABLED"); }"#,
        r#"use std::{env as process_env};"#,
        r#"use std::env::{var_os, vars_os};"#,
        r#"use std::env::var_os as read_env;"#,
        r#"use std as process_std; let _ = process_std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT");"#,
        r#"use crate::METRICS_EXPORTER_ENV as EXPORTER_ENV; let _ = std::env::var_os(EXPORTER_ENV);"#,
        r#"let values = std::env::vars_os(); let _ = values.filter(|(key, _)| key == "OTEL_EXPORTER_OTLP_ENDPOINT");"#,
        r#"let read_env = std::env::var_os; let _ = read_env("NAMESRV_ADDR");"#,
        r#"const ENDPOINT: &str = "\x4fTEL_EXPORTER_OTLP_ENDPOINT";
const ENDPOINT_ALIAS: &str = ENDPOINT;
let selected = ENDPOINT_ALIAS;
let _ = std::env::var_os(selected);"#,
        r#"const ENV_NAME: &str = ((METRICS_ENABLED_ENV));
let selected = (((ENV_NAME)));
let _ = std::env::var((selected));"#,
        r#"let _ = std::env::var("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO");"#,
        r#"let _ = std::env::var("ROCKETMQ_MCP_TRACE_SAMPLE_RATIO");"#,
    ] {
        assert!(
            has_direct_telemetry_environment_read(source),
            "detector missed direct telemetry environment access: {source}"
        );
    }

    for source in [
        r#"let _ = std::env::var("ROCKETMQ_MCP_HTTP_TOKEN");"#,
        r#"let _ = std::env::var_os("MY_METRICS_DEBUG");"#,
        r#"let _ = std::env::var("MY_OTEL_DEBUG");"#,
        r#"let _ = std::env::var("MY_ROCKETMQ_METRICS_DEBUG");"#,
        r#"let marker = '\"'; let _ = std::env::var("NAMESRV_ADDR");"#,
        r#"let marker = '\''; let _ = std::env::var("ROCKETMQ_HOME");"#,
        r#"let marker = '\x4f'; let _ = std::env::var("NAMESRV_ADDR");"#,
        r#"let marker = '\u{4f}'; let _ = std::env::var("ROCKETMQ_HOME");"#,
        r#"let _ = std::env::var_os(CONTROLLER_AUTO_INITIALIZE_CLUSTER_ENV);"#,
        r#"use {std::io, crate::env}; let _ = std::env::var("NAMESRV_ADDR");"#,
        r#"use std::io as env; let _ = std::env::var("ROCKETMQ_HOME");"#,
    ] {
        assert!(
            !has_direct_telemetry_environment_read(source),
            "detector must ignore unrelated environment access: {source}"
        );
    }

    for source in [r"'\x4f'", r"'\u{4f}'"] {
        assert!(
            lex_rust_boundary_tokens(source).is_empty(),
            "character literal contents must not become boundary tokens: {source}"
        );
    }

    for source in [
        r#""OTEL_EXPORTER_\
  OTLP_ENDPOINT""#,
        "\"OTEL_EXPORTER_\\\r\n  OTLP_ENDPOINT\"",
    ] {
        assert_eq!(
            lex_rust_boundary_tokens(source),
            vec![RustBoundaryToken::StringLiteral(
                "OTEL_EXPORTER_OTLP_ENDPOINT".to_string()
            )],
            "cooked string continuations must follow Rust escape semantics"
        );
        assert!(
            has_direct_telemetry_environment_read(&format!("let _ = std::env::var({source});")),
            "continued telemetry environment names must remain detectable"
        );
    }
}

#[test]
fn helm_defaults_keep_file_signal_selection_authoritative() {
    let workspace_root = workspace_root();
    let values = fs::read_to_string(workspace_root.join("distribution/helm/rocketmq-rust/values.yaml"))
        .expect("Helm values should be readable");
    let parsed_values: serde_yaml::Value = serde_yaml::from_str(&values).expect("Helm values should be valid YAML");
    let helpers = fs::read_to_string(workspace_root.join("distribution/helm/rocketmq-rust/templates/_helpers.tpl"))
        .expect("Helm helpers should be readable");

    assert!(values.contains("environmentOverridesEnabled: false"));
    for exporter in ["metricsExporter", "tracesExporter", "logsExporter"] {
        assert!(
            values.contains(&format!("{exporter}: disable")),
            "default {exporter} must be disabled"
        );
    }
    assert_eq!(
        parsed_values["metrics"]["enabled"],
        serde_yaml::Value::Bool(false),
        "stock images do not expose Prometheus, so metrics services must be opt-in"
    );

    assert!(helpers.contains("define \"rocketmq.releaseIdentityEnv\""));
    assert!(helpers.contains("define \"rocketmq.observabilityEnvironmentOverrides\""));
    assert!(helpers.contains("if .Values.global.observability.environmentOverridesEnabled"));
    assert!(helpers.contains("ROCKETMQ_RELEASE_COMMIT"));
    assert!(helpers.contains("ROCKETMQ_METRICS_EXPORTER"));
    assert!(helpers.contains("OTEL_EXPORTER_OTLP_ENDPOINT"));
}

#[test]
fn helm_file_and_compatibility_modes_keep_effective_precedence() {
    let workspace_root = workspace_root();
    let schema: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(workspace_root.join("distribution/helm/rocketmq-rust/values.schema.json"))
            .expect("Helm values schema should be readable"),
    )
    .expect("Helm values schema should be valid JSON");
    let production_profile: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(workspace_root.join("distribution/config/production-feature-profile.json"))
            .expect("production feature profile should be readable"),
    )
    .expect("production feature profile should be valid JSON");
    let helpers = fs::read_to_string(workspace_root.join("distribution/helm/rocketmq-rust/templates/_helpers.tpl"))
        .expect("Helm helpers should be readable");
    let configmaps =
        fs::read_to_string(workspace_root.join("distribution/helm/rocketmq-rust/templates/configmaps.yaml"))
            .expect("Helm ConfigMap template should be readable");
    let workloads = fs::read_to_string(workspace_root.join("distribution/helm/rocketmq-rust/templates/workloads.yaml"))
        .expect("Helm workload template should be readable");

    let observability_schema = &schema["properties"]["global"]["properties"]["observability"];
    assert_eq!(
        observability_schema["properties"]["environmentOverridesEnabled"]["type"],
        "boolean"
    );
    assert!(observability_schema["required"]
        .as_array()
        .is_some_and(|required| required.iter().any(|field| field == "environmentOverridesEnabled")));
    assert_eq!(
        observability_schema["properties"]["metricsExporter"]["enum"],
        serde_json::json!(["disable", "otlp_grpc", "log"]),
        "the stock Helm chart must advertise only exporters compiled into every production image"
    );
    for (service, contract) in production_profile["services"]
        .as_object()
        .expect("production services should be an object")
    {
        for feature_set in ["features", "resolved_features"] {
            let features = contract[feature_set]
                .as_array()
                .unwrap_or_else(|| panic!("{service}.{feature_set} should be an array"));
            assert!(
                !features
                    .iter()
                    .any(|feature| feature == "prometheus" || feature == "metrics-prometheus"),
                "stock {service} images must not claim direct Prometheus without a chart-wide capability decision"
            );
        }
    }

    let release_identity = helm_template_definition(&helpers, "rocketmq.releaseIdentityEnv");
    assert!(release_identity.contains("ROCKETMQ_RELEASE_COMMIT"));
    assert!(!release_identity.contains("ROCKETMQ_METRICS_"));
    assert!(!release_identity.contains("OTEL_EXPORTER_"));

    let compatibility = helm_template_definition(&helpers, "rocketmq.observabilityEnvironmentOverrides");
    assert!(compatibility.contains("if .Values.global.observability.environmentOverridesEnabled"));
    for variable in [
        "ROCKETMQ_METRICS_ENABLED",
        "ROCKETMQ_METRICS_EXPORTER",
        "ROCKETMQ_METRICS_BIND_ADDR",
        "ROCKETMQ_METRICS_PATH",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
    ] {
        assert!(
            compatibility.contains(variable),
            "missing compatibility variable {variable}"
        );
    }

    let file_config = helm_template_definition(&helpers, "rocketmq.observabilityConfig");
    for mapping in [
        "exporter = {{ .Values.global.observability.metricsExporter | quote }}",
        "exporter = {{ .Values.global.observability.tracesExporter | quote }}",
        "exporter = {{ .Values.global.observability.logsExporter | quote }}",
    ] {
        assert!(
            file_config.contains(mapping),
            "all-disabled, OTLP, log, and mixed-signal modes require independent file exporter mapping: {mapping}"
        );
    }
    assert!(file_config.contains("include \"rocketmq.observabilityOtlpEndpoint\""));

    let endpoint = helm_template_definition(&helpers, "rocketmq.observabilityOtlpEndpoint");
    assert!(endpoint.contains("if ne $structuredEndpoint $defaultEndpoint"));
    assert!(endpoint.contains("else if ne $legacyEndpoint $defaultEndpoint"));
    assert_eq!(
        configmaps.matches("include \"rocketmq.observabilityConfig\"").count(),
        5
    );
    assert_eq!(workloads.matches("include \"rocketmq.releaseIdentityEnv\"").count(), 5);
    assert_eq!(
        workloads
            .matches("include \"rocketmq.observabilityEnvironmentOverrides\"")
            .count(),
        5
    );
    assert!(!workloads.contains("ROCKETMQ_METRICS_"));
    assert!(!workloads.contains("OTEL_EXPORTER_OTLP_ENDPOINT"));
    assert!(!workloads.contains("OTEL_EXPORTER_OTLP_PROTOCOL"));
}

#[test]
fn deployment_templates_share_canonical_observability_configuration_and_endpoint_resolution() {
    let workspace_root = workspace_root();
    let configmaps =
        fs::read_to_string(workspace_root.join("distribution/helm/rocketmq-rust/templates/configmaps.yaml"))
            .expect("Helm ConfigMap template should be readable");
    let workloads = fs::read_to_string(workspace_root.join("distribution/helm/rocketmq-rust/templates/workloads.yaml"))
        .expect("Helm workload template should be readable");
    let helpers = fs::read_to_string(workspace_root.join("distribution/helm/rocketmq-rust/templates/_helpers.tpl"))
        .expect("Helm helper template should be readable");
    let manifest = fs::read_to_string(workspace_root.join("distribution/kubernetes/base/manifest.yaml"))
        .expect("static Kubernetes manifest should be readable");

    assert_eq!(
        configmaps.matches("include \"rocketmq.observabilityConfig\"").count(),
        5,
        "all five service ConfigMaps must use the same canonical observability helper"
    );
    let file_config = helm_template_definition(&helpers, "rocketmq.observabilityConfig");
    let compatibility = helm_template_definition(&helpers, "rocketmq.observabilityEnvironmentOverrides");
    assert!(file_config.contains("include \"rocketmq.observabilityOtlpEndpoint\""));
    assert!(compatibility.contains("include \"rocketmq.observabilityOtlpEndpoint\""));
    assert!(compatibility.contains(".Values.global.observability.otlpProtocol"));
    assert_eq!(
        workloads
            .matches("include \"rocketmq.observabilityEnvironmentOverrides\"")
            .count(),
        5,
        "all five workloads must use the compatibility helper that shares ConfigMap endpoint resolution"
    );
    assert!(!configmaps.contains(".Values.global.otelEndpoint"));
    assert!(!workloads.contains(".Values.global.otelEndpoint"));
    for contract in [
        "define \"rocketmq.observabilityOtlpEndpoint\"",
        "if ne $structuredEndpoint $defaultEndpoint",
        "else if ne $legacyEndpoint $defaultEndpoint",
        "define \"rocketmq.observabilityConfig\"",
        "[observability.metrics]",
        "[observability.traces]",
        "[observability.logs]",
        "[observability.otlp]",
        "[observability.prometheus]",
    ] {
        assert!(
            helpers.contains(contract),
            "missing Helm observability contract: {contract}"
        );
    }
    assert_eq!(
        manifest.matches("[observability.metrics]").count(),
        9,
        "the static manifest must carry canonical config for three Brokers, one NameServer, three Controllers, \
         one Proxy, and one MCP"
    );
    assert!(!helpers.contains("headers ="));
    assert!(!configmaps.contains("headers ="));
    assert!(!manifest.contains("headers ="));
}

#[test]
fn runtime_configuration_and_user_documentation_exclude_removed_flat_telemetry_keys() {
    let workspace_root = workspace_root();
    let mut violations = BTreeSet::new();

    for relative_path in RUNTIME_CONFIGURATION_AND_DOCUMENTATION_PATHS {
        let path = workspace_root.join(relative_path);
        let mut files = Vec::new();
        if path.is_dir() {
            collect_migration_files(&path, &mut files);
        } else {
            files.push(path);
        }

        for file in files {
            if file.starts_with(workspace_root.join("rocketmq-ai/rocketmq-sre/scripts"))
                && !file.file_name().is_some_and(|name| {
                    name.to_string_lossy().starts_with("phase") && name.to_string_lossy().ends_with("-smoke.ps1")
                })
            {
                continue;
            }
            let source =
                fs::read_to_string(&file).unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
            for key in REMOVED_FLAT_TELEMETRY_KEYS {
                if contains_removed_config_key(&source, key) {
                    violations.insert(format!("{}: {key}", relative_slash_path(&workspace_root, &file)));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "runnable configuration, smoke scripts, and user documentation must migrate removed flat telemetry keys; \
         rejection tests and historical inventory fixtures are intentionally outside this scan:\n{}",
        format_paths(&violations)
    );
}

#[test]
fn website_documents_each_services_actual_observability_features() {
    let workspace_root = workspace_root();
    let english = fs::read_to_string(workspace_root.join("rocketmq-website/docs/configuration/observability.md"))
        .expect("English observability guide should be readable");
    let chinese = fs::read_to_string(
        workspace_root
            .join("rocketmq-website/i18n/zh-CN/docusaurus-plugin-content-docs/current/configuration/observability.md"),
    )
    .expect("Chinese observability guide should be readable");
    for (language, guide) in [("English", &english), ("Chinese", &chinese)] {
        for stock_chart_contract in [
            "global.observability.metricsExporter",
            "`disable`",
            "`otlp_grpc`",
            "`log`",
        ] {
            assert!(
                guide.contains(stock_chart_contract),
                "{language} observability guide omits stock Helm contract `{stock_chart_contract}`"
            );
        }
    }
    let contracts = [
        (
            "Broker",
            "rocketmq-broker/Cargo.toml",
            &[
                "observability",
                "otel-metrics",
                "otlp-metrics",
                "prometheus",
                "metrics-prometheus",
                "otel-traces",
                "otlp-traces",
                "otel-logs",
                "otlp-logs",
            ][..],
        ),
        (
            "NameServer",
            "rocketmq-namesrv/Cargo.toml",
            &[
                "observability",
                "otel-metrics",
                "otlp-metrics",
                "otel-traces",
                "otlp-traces",
                "otel-logs",
                "otlp-logs",
            ][..],
        ),
        (
            "Controller",
            "rocketmq-controller/Cargo.toml",
            &[
                "metrics",
                "metrics-otlp",
                "metrics-prometheus",
                "otel-traces",
                "otlp-traces",
                "otel-logs",
                "otlp-logs",
            ][..],
        ),
        (
            "Proxy",
            "rocketmq-proxy/Cargo.toml",
            &[
                "observability",
                "otlp-metrics",
                "otel-traces",
                "otlp-traces",
                "otel-logs",
                "otlp-logs",
            ][..],
        ),
        (
            "MCP",
            "rocketmq-ai/rocketmq-mcp/Cargo.toml",
            &["observability", "otlp"][..],
        ),
    ];

    for (service, manifest_path, expected_features) in contracts {
        let manifest = fs::read_to_string(workspace_root.join(manifest_path))
            .unwrap_or_else(|error| panic!("failed to read {manifest_path}: {error}"));
        let actual_features = cargo_feature_names(&manifest);
        let english_row = markdown_service_feature_row(&english, service);
        let chinese_row = markdown_service_feature_row(&chinese, service);
        for feature in expected_features {
            assert!(
                actual_features.contains(feature),
                "{manifest_path} does not define {feature}"
            );
            let formatted = format!("`{feature}`");
            assert!(
                english_row.contains(&formatted),
                "English {service} row omits {feature}"
            );
            assert!(
                chinese_row.contains(&formatted),
                "Chinese {service} row omits {feature}"
            );
        }
        if service == "Controller" {
            assert!(!actual_features.contains("observability"));
            assert!(!english_row.contains("`observability`"));
            assert!(!chinese_row.contains("`observability`"));
        }
    }
}

#[test]
fn core_services_install_telemetry_before_business_lifecycle() {
    let workspace_root = workspace_root();
    for relative_path in CORE_SERVICE_ENTRYPOINTS {
        let path = workspace_root.join(relative_path);
        let source =
            fs::read_to_string(&path).unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        let install = source
            .find("install_global_with_filter_and_service_context")
            .unwrap_or_else(|| panic!("{relative_path} must install telemetry with its service context"));
        let lifecycle = source
            .find("lifecycle.start")
            .unwrap_or_else(|| panic!("{relative_path} must start an owned business lifecycle"));
        assert!(
            install < lifecycle,
            "{relative_path} must install telemetry before starting the business lifecycle"
        );
        let diagnostics_start = source
            .find("start_runtime_diagnostics_endpoint_from_env_with_telemetry")
            .unwrap_or_else(|| panic!("{relative_path} must start protected runtime diagnostics"));
        let diagnostics_error_path = &source[diagnostics_start..];
        let rollback_end = diagnostics_error_path
            .find("return Err")
            .unwrap_or_else(|| panic!("{relative_path} diagnostics startup failure must return an error"));
        let diagnostics_rollback = &diagnostics_error_path[..rollback_end];
        assert!(
            diagnostics_rollback
                .contains("shutdown_with_service_context(&service_context, request.deadline.remaining())"),
            "{relative_path} must use contextual telemetry cleanup after diagnostics startup failure"
        );
        assert!(
            !diagnostics_rollback.contains("shutdown_with_timeout(request.deadline.remaining())"),
            "{relative_path} diagnostics startup failure must not bypass the telemetry service context"
        );
        for field in [
            "service =",
            "effective_filter =",
            "filter_source =",
            "subscriber_installed =",
            "reload_enabled =",
        ] {
            assert!(
                source.contains(field),
                "{relative_path} startup event is missing `{field}`"
            );
        }
    }
}

#[test]
fn build_scripts_do_not_inject_log_filter_defaults() {
    let workspace_root = workspace_root();
    let mut violations = BTreeSet::new();
    for crate_dir in WORKSPACE_CRATE_DIRS {
        let build_script = workspace_root.join(crate_dir).join("build.rs");
        if !build_script.exists() {
            continue;
        }
        let source = fs::read_to_string(&build_script)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", build_script.display()));
        let normalized = source.to_ascii_uppercase();
        if normalized.contains("CARGO:RUSTC-ENV=RUST_LOG")
            || (normalized.contains("CARGO:RUSTC-ENV=")
                && (normalized.contains("LOG_FILTER") || normalized.contains("LOG_LEVEL")))
        {
            violations.insert(relative_slash_path(&workspace_root, &build_script));
        }
    }

    assert!(
        violations.is_empty(),
        "build.rs must not inject process log filter defaults:\n{}",
        format_paths(&violations)
    );
}

#[test]
fn build_time_log_filter_detector_catches_known_patterns() {
    let source = r#"println!("cargo:rustc-env=RUST_LOG=debug");"#;
    let normalized = source.to_ascii_uppercase();

    assert!(normalized.contains("CARGO:RUSTC-ENV=RUST_LOG"));
}

#[test]
fn explicit_telemetry_capability_does_not_export_raw_sdk_or_unguarded_trace_paths() {
    let workspace_root = workspace_root();
    let handle_source = fs::read_to_string(workspace_root.join("rocketmq-observability/src/handle.rs"))
        .expect("telemetry handle source should be readable");
    assert!(
        !handle_source.contains("pub fn meter("),
        "TelemetryHandle and TelemetryRecorder must not export raw SDK meters"
    );

    let observability_src = workspace_root.join("rocketmq-observability/src");
    let mut observability_files = Vec::new();
    collect_rs_files(&observability_src, &mut observability_files);
    let mut raw_meter_api_files = BTreeSet::new();
    for file in observability_files {
        let source =
            fs::read_to_string(&file).unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        if source.match_indices("pub fn ").any(|(start, _)| {
            let signature = &source[start..];
            let signature = signature.split_once('{').map_or(signature, |(signature, _)| signature);
            signature.contains("Meter")
        }) {
            raw_meter_api_files.insert(relative_slash_path(&workspace_root, &file));
        }
    }
    assert!(
        raw_meter_api_files.is_empty(),
        "public observability APIs must not expose raw SDK Meter values:\n{}",
        format_paths(&raw_meter_api_files)
    );

    let lib_source = fs::read_to_string(workspace_root.join("rocketmq-observability/src/lib.rs"))
        .expect("observability root source should be readable");
    for forbidden in [
        "pub use propagation::add_current_span_event;",
        "pub use propagation::extract_context;",
        "pub use propagation::inject_current_context;",
        "pub use propagation::install_trace_context_propagators;",
        "pub use propagation::MessagePropertyExtractor;",
        "pub use propagation::MessagePropertyInjector;",
    ] {
        assert!(
            !lib_source.contains(forbidden),
            "unguarded propagation API must stay private: {forbidden}"
        );
    }

    let trace_source = fs::read_to_string(workspace_root.join("rocketmq-observability/src/trace.rs"))
        .expect("trace source should be readable");
    for forbidden in [
        "pub fn record_current_message_properties(",
        "pub fn record_message_properties(",
        "pub fn record_message_properties_with_policy(",
    ] {
        assert!(
            !trace_source.contains(forbidden),
            "message trace recording must require an explicit handle: {forbidden}"
        );
    }
}

#[test]
fn metric_name_constants_are_declared_only_in_canonical_or_legacy_files() {
    let workspace_root = workspace_root();
    let mut allowed_files = path_set(METRIC_CONSTANT_CANONICAL_FILES);
    allowed_files.extend(path_set(METRIC_CONSTANT_LEGACY_ALLOWLIST));

    let mut scan_dirs = WORKSPACE_CRATE_DIRS.to_vec();
    scan_dirs.push("rocketmq-observability");

    let mut unexpected_files = BTreeSet::new();
    for file in workspace_src_files(&workspace_root, &scan_dirs) {
        let relative_path = relative_slash_path(&workspace_root, &file);
        if allowed_files.contains(relative_path.as_str()) {
            continue;
        }

        let source =
            fs::read_to_string(&file).unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        if has_metric_constant_definition(&source) {
            unexpected_files.insert(relative_path);
        }
    }

    assert!(
        unexpected_files.is_empty(),
        "metric name constants must be declared in semantic/catalog or tracked legacy files:\n{}",
        format_paths(&unexpected_files)
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-observability must be inside the workspace root")
        .to_path_buf()
}

fn workspace_src_files(workspace_root: &Path, crate_dirs: &[&str]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for crate_dir in crate_dirs {
        let src_dir = workspace_root.join(crate_dir).join("src");
        if src_dir.exists() {
            collect_rs_files(&src_dir, &mut files);
        }
    }
    files
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(dir).unwrap_or_else(|error| panic!("failed to read directory {}: {error}", dir.display()));

    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("failed to read entry in {}: {error}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

fn has_metric_constant_definition(source: &str) -> bool {
    source.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed.contains("const ")
            && trimmed.contains('"')
            && (CONTROLLER_METRIC_LITERAL_MARKERS
                .iter()
                .any(|marker| trimmed.contains(marker))
                || (trimmed.contains("\"rocketmq_")
                    && ROCKETMQ_METRIC_SUFFIX_MARKERS
                        .iter()
                        .any(|marker| trimmed.contains(marker))))
    })
}

fn has_subscriber_installation(source: &str) -> bool {
    if SUBSCRIBER_INSTALL_PATTERNS
        .iter()
        .any(|pattern| source.contains(pattern))
    {
        return true;
    }

    if source.contains("set_global_default(")
        && (source.contains("tracing::subscriber") || source.contains("tracing_subscriber"))
    {
        return true;
    }

    if !source.contains("tracing_subscriber") {
        return false;
    }

    // A builder can be installed in a later statement; construction alone is not installation.
    let subscriber_bindings: BTreeSet<_> = source
        .split(';')
        .filter_map(|statement| {
            let (binding, value) = statement.rsplit_once("let ")?.1.split_once('=')?;
            if !(value.contains("fmt()") || value.contains("registry()")) {
                return None;
            }
            let mut tokens = binding.split_whitespace();
            let name = tokens.next()?;
            Some(if name == "mut" { tokens.next()? } else { name }.trim_end_matches(':'))
        })
        .collect();

    source.split(';').any(|statement| {
        subscriber_init_statement_installs_global_subscriber(source, statement)
            || subscriber_bindings.iter().any(|name| {
                statement.contains(&format!("{name}.init(")) || statement.contains(&format!("{name}.try_init("))
            })
    })
}

fn collect_migration_files(dir: &Path, files: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(dir).unwrap_or_else(|error| panic!("failed to read directory {}: {error}", dir.display()));

    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("failed to read entry in {}: {error}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            collect_migration_files(&path, files);
        } else if path.extension().is_some_and(|extension| {
            matches!(
                extension.to_string_lossy().as_ref(),
                "md" | "ps1" | "toml" | "tpl" | "yaml" | "yml"
            )
        }) {
            files.push(path);
        }
    }
}

fn helm_template_definition<'a>(source: &'a str, name: &str) -> &'a str {
    let marker = format!("define \"{name}\"");
    let start = source
        .find(&marker)
        .unwrap_or_else(|| panic!("missing Helm template definition {name}"));
    let remainder = &source[start + marker.len()..];
    let end = remainder.find("define \"").unwrap_or(remainder.len());
    &remainder[..end]
}

fn contains_removed_config_key(source: &str, key: &str) -> bool {
    source.match_indices(key).any(|(start, _)| {
        let before = source[..start].chars().next_back();
        let after = source[start + key.len()..].chars().next();
        !before.is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
            && !after.is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
    })
}

fn has_direct_telemetry_environment_read(source: &str) -> bool {
    const TELEMETRY_ENV_CONSTANTS: &[&str] = &[
        "METRICS_ENABLED_ENV",
        "METRICS_EXPORTER_ENV",
        "METRICS_BIND_ADDR_ENV",
        "METRICS_PATH_ENV",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
    ];

    let tokens = lex_rust_boundary_tokens(source);
    if contains_std_env_import(&tokens) {
        return true;
    }

    let telemetry_names = telemetry_environment_names(&tokens, TELEMETRY_ENV_CONSTANTS);
    for index in 0..tokens.len() {
        let Some((function, after_function)) = fully_qualified_env_function(&tokens, index) else {
            continue;
        };
        if matches!(function, "vars" | "vars_os") {
            return true;
        }
        if matches!(function, "var" | "var_os") {
            let Some(RustBoundaryToken::LeftParenthesis) = tokens.get(after_function) else {
                return true;
            };
            if telemetry_environment_argument(&tokens, after_function, &telemetry_names) {
                return true;
            }
        }
    }

    false
}

fn contains_std_env_import(tokens: &[RustBoundaryToken]) -> bool {
    let mut cursor = 0;
    while cursor < tokens.len() {
        if token_identifier(tokens.get(cursor)) != Some("use") {
            cursor += 1;
            continue;
        }
        let end = tokens[cursor..]
            .iter()
            .position(|token| matches!(token, RustBoundaryToken::Semicolon))
            .map_or(tokens.len(), |offset| cursor + offset);
        if use_tree_contains_std_env_import(&tokens[cursor + 1..end]) {
            return true;
        }
        cursor = end.saturating_add(1);
    }
    false
}

fn use_tree_contains_std_env_import(tokens: &[RustBoundaryToken]) -> bool {
    let mut group_prefixes: Vec<Vec<&str>> = Vec::new();
    let mut path = Vec::new();
    let mut alias_target = false;

    for token in tokens {
        match token {
            RustBoundaryToken::Identifier(identifier) if identifier == "as" => {
                if path == ["std"] {
                    return true;
                }
                alias_target = true;
            }
            RustBoundaryToken::Identifier(_) if alias_target => {
                alias_target = false;
            }
            RustBoundaryToken::Identifier(identifier) if identifier != "self" => {
                path.push(identifier.as_str());
                if path.starts_with(&["std", "env"]) {
                    return true;
                }
            }
            RustBoundaryToken::LeftBrace => group_prefixes.push(path.clone()),
            RustBoundaryToken::Comma => {
                path = group_prefixes.last().cloned().unwrap_or_default();
                alias_target = false;
            }
            RustBoundaryToken::RightBrace => {
                group_prefixes.pop();
                path = group_prefixes.last().cloned().unwrap_or_default();
                alias_target = false;
            }
            _ => {}
        }
    }

    false
}

fn telemetry_environment_names(tokens: &[RustBoundaryToken], constants: &[&str]) -> BTreeSet<String> {
    let mut names = constants
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();

    for index in 0..tokens.len() {
        let Some(identifier) = token_identifier(tokens.get(index)) else {
            continue;
        };
        if constants.contains(&identifier) && token_identifier(tokens.get(index + 1)) == Some("as") {
            if let Some(alias) = token_identifier(tokens.get(index + 2)) {
                names.insert(alias.to_string());
            }
        }
    }

    loop {
        let previous_len = names.len();
        for index in 0..tokens.len() {
            let Some(keyword @ ("const" | "let")) = token_identifier(tokens.get(index)) else {
                continue;
            };
            let mut name_index = index + 1;
            if keyword == "let" && token_identifier(tokens.get(name_index)) == Some("mut") {
                name_index += 1;
            }
            let Some(name) = token_identifier(tokens.get(name_index)) else {
                continue;
            };
            let end = tokens[name_index..]
                .iter()
                .position(|token| matches!(token, RustBoundaryToken::Semicolon))
                .map_or(tokens.len(), |offset| name_index + offset);
            let Some(equals) = (name_index..end).find(|cursor| matches!(tokens[*cursor], RustBoundaryToken::Equals))
            else {
                continue;
            };
            if tokens_resolve_to_telemetry_name(&tokens[equals + 1..end], &names) {
                names.insert(name.to_string());
            }
        }
        if names.len() == previous_len {
            return names;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RustBoundaryToken {
    Identifier(String),
    StringLiteral(String),
    DoubleColon,
    LeftParenthesis,
    RightParenthesis,
    LeftBrace,
    RightBrace,
    Comma,
    Equals,
    Semicolon,
}

fn lex_rust_boundary_tokens(source: &str) -> Vec<RustBoundaryToken> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() {
            index += 1;
        } else if bytes[index..].starts_with(b"//") {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
        } else if bytes[index..].starts_with(b"/*") {
            index += 2;
            let mut depth = 1;
            while index < bytes.len() && depth > 0 {
                if bytes[index..].starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if bytes[index..].starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                } else {
                    index += 1;
                }
            }
        } else if bytes[index] == b'r' && raw_string_hashes(bytes, index).is_some() {
            let hashes = raw_string_hashes(bytes, index).expect("raw string prefix was just checked");
            index += 2 + hashes;
            let content_start = index;
            while index < bytes.len()
                && !(bytes[index] == b'"'
                    && bytes
                        .get(index + 1..index + 1 + hashes)
                        .is_some_and(|closing_hashes| closing_hashes.iter().all(|byte| *byte == b'#')))
            {
                index += 1;
            }
            tokens.push(RustBoundaryToken::StringLiteral(
                String::from_utf8_lossy(&bytes[content_start..index]).into_owned(),
            ));
            index = (index + 1 + hashes).min(bytes.len());
        } else if bytes[index] == b'"' {
            tokens.push(RustBoundaryToken::StringLiteral(lex_escaped_string(bytes, &mut index)));
        } else if bytes[index] == b'\'' {
            index = character_literal_end(bytes, index).unwrap_or(index + 1);
        } else if bytes[index].is_ascii_alphabetic() || bytes[index] == b'_' {
            let start = index;
            index += 1;
            while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_') {
                index += 1;
            }
            tokens.push(RustBoundaryToken::Identifier(
                String::from_utf8_lossy(&bytes[start..index]).into_owned(),
            ));
        } else if bytes[index..].starts_with(b"::") {
            tokens.push(RustBoundaryToken::DoubleColon);
            index += 2;
        } else {
            let token = match bytes[index] {
                b'(' => Some(RustBoundaryToken::LeftParenthesis),
                b')' => Some(RustBoundaryToken::RightParenthesis),
                b'{' => Some(RustBoundaryToken::LeftBrace),
                b'}' => Some(RustBoundaryToken::RightBrace),
                b',' => Some(RustBoundaryToken::Comma),
                b'=' => Some(RustBoundaryToken::Equals),
                b';' => Some(RustBoundaryToken::Semicolon),
                _ => None,
            };
            tokens.extend(token);
            index += 1;
        }
    }
    tokens
}

fn lex_escaped_string(bytes: &[u8], index: &mut usize) -> String {
    *index += 1;
    let mut value = String::new();
    while *index < bytes.len() && bytes[*index] != b'"' {
        if bytes[*index] != b'\\' || *index + 1 >= bytes.len() {
            value.push(bytes[*index] as char);
            *index += 1;
            continue;
        }
        *index += 1;
        match bytes[*index] {
            b'\n' => {
                *index += 1;
                while bytes.get(*index).is_some_and(|byte| byte.is_ascii_whitespace()) {
                    *index += 1;
                }
            }
            b'\r' if bytes.get(*index + 1) == Some(&b'\n') => {
                *index += 2;
                while bytes.get(*index).is_some_and(|byte| byte.is_ascii_whitespace()) {
                    *index += 1;
                }
            }
            b'x' if *index + 2 < bytes.len() => {
                if let (Some(high), Some(low)) = (hex_digit(bytes[*index + 1]), hex_digit(bytes[*index + 2])) {
                    value.push((high * 16 + low) as char);
                    *index += 3;
                } else {
                    value.push('x');
                    *index += 1;
                }
            }
            b'u' if bytes.get(*index + 1) == Some(&b'{') => {
                let digits_start = *index + 2;
                let mut end = digits_start;
                while end < bytes.len() && bytes[end] != b'}' {
                    end += 1;
                }
                let decoded = std::str::from_utf8(&bytes[digits_start..end])
                    .ok()
                    .and_then(|digits| u32::from_str_radix(digits, 16).ok())
                    .and_then(char::from_u32);
                value.extend(decoded);
                *index = (end + 1).min(bytes.len());
            }
            b'n' => {
                value.push('\n');
                *index += 1;
            }
            b'r' => {
                value.push('\r');
                *index += 1;
            }
            b't' => {
                value.push('\t');
                *index += 1;
            }
            escaped => {
                value.push(escaped as char);
                *index += 1;
            }
        }
    }
    *index = (*index + 1).min(bytes.len());
    value
}

fn character_literal_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut cursor = start + 1;
    if bytes.get(cursor) == Some(&b'\\') {
        cursor += match bytes.get(cursor + 1) {
            Some(b'x') => 4,
            Some(b'u') if bytes.get(cursor + 2) == Some(&b'{') => {
                let closing_brace = bytes[cursor + 3..].iter().position(|byte| *byte == b'}')?;
                closing_brace + 4
            }
            Some(_) => 2,
            None => return None,
        };
    } else {
        cursor += std::str::from_utf8(&bytes[cursor..]).ok()?.chars().next()?.len_utf8();
    }
    (bytes.get(cursor) == Some(&b'\'')).then_some(cursor + 1)
}

const fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn raw_string_hashes(bytes: &[u8], start: usize) -> Option<usize> {
    let mut index = start + 1;
    while bytes.get(index) == Some(&b'#') {
        index += 1;
    }
    (bytes.get(index) == Some(&b'"')).then_some(index - start - 1)
}

fn fully_qualified_env_function(tokens: &[RustBoundaryToken], index: usize) -> Option<(&str, usize)> {
    if token_identifier(tokens.get(index)) == Some("std")
        && matches!(tokens.get(index + 1), Some(RustBoundaryToken::DoubleColon))
        && token_identifier(tokens.get(index + 2)) == Some("env")
        && matches!(tokens.get(index + 3), Some(RustBoundaryToken::DoubleColon))
    {
        let function = token_identifier(tokens.get(index + 4))?;
        if matches!(function, "var" | "var_os" | "vars" | "vars_os") {
            return Some((function, index + 5));
        }
    }
    None
}

fn telemetry_environment_argument(
    tokens: &[RustBoundaryToken],
    open_parenthesis: usize,
    telemetry_constants: &BTreeSet<String>,
) -> bool {
    let mut depth = 0;
    let end = (open_parenthesis + 1..tokens.len())
        .find(|index| match tokens[*index] {
            RustBoundaryToken::LeftParenthesis | RustBoundaryToken::LeftBrace => {
                depth += 1;
                false
            }
            RustBoundaryToken::RightParenthesis | RustBoundaryToken::RightBrace if depth > 0 => {
                depth -= 1;
                false
            }
            RustBoundaryToken::RightParenthesis | RustBoundaryToken::Comma if depth == 0 => true,
            _ => false,
        })
        .unwrap_or(tokens.len());
    tokens_resolve_to_telemetry_name(&tokens[open_parenthesis + 1..end], telemetry_constants)
}

fn tokens_resolve_to_telemetry_name(tokens: &[RustBoundaryToken], telemetry_names: &BTreeSet<String>) -> bool {
    let tokens = strip_balanced_outer_parentheses(tokens);
    if let [RustBoundaryToken::StringLiteral(name)] = tokens {
        return name.starts_with("OTEL_")
            || name.starts_with("ROCKETMQ_METRICS_")
            || matches!(
                name.as_str(),
                "ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO" | "ROCKETMQ_MCP_TRACE_SAMPLE_RATIO"
            );
    }
    let is_path = tokens.iter().enumerate().all(|(index, token)| {
        matches!(
            (index % 2, token),
            (0, RustBoundaryToken::Identifier(_)) | (1, RustBoundaryToken::DoubleColon)
        )
    });
    is_path
        && tokens
            .iter()
            .rev()
            .find_map(|token| token_identifier(Some(token)))
            .is_some_and(|identifier| telemetry_names.contains(identifier))
}

fn strip_balanced_outer_parentheses(mut tokens: &[RustBoundaryToken]) -> &[RustBoundaryToken] {
    while matches!(tokens.first(), Some(RustBoundaryToken::LeftParenthesis))
        && matches!(tokens.last(), Some(RustBoundaryToken::RightParenthesis))
    {
        let mut depth = 0;
        let closing = tokens.iter().enumerate().find_map(|(index, token)| match token {
            RustBoundaryToken::LeftParenthesis => {
                depth += 1;
                None
            }
            RustBoundaryToken::RightParenthesis => {
                depth -= 1;
                (depth == 0).then_some(index)
            }
            _ => None,
        });
        if closing != Some(tokens.len() - 1) {
            break;
        }
        tokens = &tokens[1..tokens.len() - 1];
    }
    tokens
}

fn token_identifier(token: Option<&RustBoundaryToken>) -> Option<&str> {
    match token {
        Some(RustBoundaryToken::Identifier(identifier)) => Some(identifier),
        _ => None,
    }
}

fn cargo_feature_names(manifest: &str) -> BTreeSet<&str> {
    manifest
        .lines()
        .skip_while(|line| line.trim() != "[features]")
        .skip(1)
        .take_while(|line| !line.trim_start().starts_with('['))
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
        .filter(|name| !name.is_empty())
        .collect()
}

fn markdown_service_feature_row<'a>(document: &'a str, service: &str) -> &'a str {
    let prefix = format!("| {service} |");
    document
        .lines()
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("missing service feature row for {service}"))
}

fn subscriber_init_statement_installs_global_subscriber(source: &str, statement: &str) -> bool {
    let invokes_init = statement.contains(".init(") || statement.contains(".try_init(");
    if !invokes_init {
        return false;
    }

    if statement.contains("tracing_subscriber::") {
        return true;
    }

    source.contains("tracing_subscriber")
        && (statement.contains("fmt()")
            || statement.contains("registry()")
            || (source.contains("SubscriberInitExt") && statement.contains(".with(")))
}

fn path_set(paths: &[&str]) -> BTreeSet<String> {
    paths.iter().map(|path| (*path).to_string()).collect()
}

fn relative_slash_path(workspace_root: &Path, path: &Path) -> String {
    path.strip_prefix(workspace_root)
        .unwrap_or(path)
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn format_paths(paths: &BTreeSet<String>) -> String {
    paths
        .iter()
        .map(|path| format!("- {path}"))
        .collect::<Vec<_>>()
        .join("\n")
}
