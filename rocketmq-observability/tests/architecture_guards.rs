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
    "rocketmq",
    "rocketmq-auth",
    "rocketmq-broker",
    "rocketmq-client",
    "rocketmq-common",
    "rocketmq-controller",
    "rocketmq-error",
    "rocketmq-filter",
    "rocketmq-macros",
    "rocketmq-namesrv",
    "rocketmq-proxy",
    "rocketmq-remoting",
    "rocketmq-runtime",
    "rocketmq-store",
    "rocketmq-tieredstore",
    "rocketmq-dashboard/rocketmq-dashboard-common",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-cli",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui",
    "rocketmq-tools/rocketmq-store-inspect",
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
    // Deprecated compatibility wrapper. It must not grow new runtime behavior, but existing callers still compile.
    "rocketmq-common/src/log.rs",
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
fn observability_dependency_closure_excludes_common_and_transport_crates() {
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
        if source.contains("rocketmq_common::") || source.contains("rocketmq_remoting::") {
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
