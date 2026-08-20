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
    "rocketmq-tools/rocketmq-mcp",
    "rocketmq-dashboard/rocketmq-dashboard-gpui",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend",
];

const GOVERNED_ENTRYPOINTS: &[&str] = &[
    "rocketmq-broker/src/bin/broker_bootstrap_server.rs",
    "rocketmq-controller/src/bin/controller_bootstrap.rs",
    "rocketmq-namesrv/src/bin/namesrv_bootstrap_server.rs",
    "rocketmq-proxy/src/bin/rocketmq-proxy-rust.rs",
    "rocketmq-tools/rocketmq-mcp/src/app.rs",
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
    "rocketmq-tools/rocketmq-mcp",
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
    "rocketmq-sre/deploy/dev/config",
    "rocketmq-sre/scripts",
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
    "tracing_subscriber::fmt()",
    "tracing_subscriber::registry()",
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
    ] {
        assert!(has_direct_telemetry_environment_read(source));
    }

    assert!(!has_direct_telemetry_environment_read(
        r#"let _ = std::env::var("ROCKETMQ_MCP_HTTP_TOKEN");"#
    ));
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
    assert_eq!(
        workloads
            .matches("include \"rocketmq.observabilityOtlpEndpoint\"")
            .count(),
        5,
        "all five workloads must use the same resolved OTLP endpoint as their ConfigMaps"
    );
    assert_eq!(
        workloads.matches(".Values.global.observability.otlpProtocol").count(),
        5,
        "every injected OTLP endpoint must carry the configured gRPC protocol"
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
            if file.starts_with(workspace_root.join("rocketmq-sre/scripts"))
                && !file.file_name().is_some_and(|name| {
                    name.to_string_lossy().starts_with("phase") && name.to_string_lossy().ends_with("-smoke.ps1")
                })
            {
                continue;
            }
            let source =
                fs::read_to_string(&file).unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
            for key in REMOVED_FLAT_TELEMETRY_KEYS {
                if source.contains(key) {
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
fn core_services_install_telemetry_before_business_lifecycle() {
    let workspace_root = workspace_root();
    for relative_path in CORE_SERVICE_ENTRYPOINTS {
        let path = workspace_root.join(relative_path);
        let source =
            fs::read_to_string(&path).unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        let install = source
            .find("install_global_with_filter")
            .unwrap_or_else(|| panic!("{relative_path} must install the shared telemetry subscriber"));
        let lifecycle = source
            .find("lifecycle.start")
            .unwrap_or_else(|| panic!("{relative_path} must start an owned business lifecycle"));
        assert!(
            install < lifecycle,
            "{relative_path} must install telemetry before starting the business lifecycle"
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

    source
        .split(';')
        .any(|statement| subscriber_init_statement_installs_global_subscriber(source, statement))
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

fn has_direct_telemetry_environment_read(source: &str) -> bool {
    let compact = source
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();

    ["std::env::var(", "std::env::var_os("]
        .iter()
        .flat_map(|call| compact.match_indices(call).map(|(index, _)| index))
        .any(|index| {
            let end = compact[index..]
                .find(';')
                .map_or(compact.len(), |offset| index + offset);
            let statement = &compact[index..end];
            statement.contains("OTEL_") || statement.contains("ROCKETMQ_METRICS_") || statement.contains("METRICS_")
        })
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
