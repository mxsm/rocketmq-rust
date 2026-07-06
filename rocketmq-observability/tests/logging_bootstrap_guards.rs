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

use std::path::PathBuf;
use std::process::Command;
use std::process::Output;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn install_global_supports_console_only_logging() {
    assert_bootstrap_case("console_only");
}

#[test]
fn install_global_supports_file_only_logging() {
    assert_bootstrap_case("file_only");
}

#[cfg(feature = "otel-metrics")]
#[test]
fn install_global_supports_metrics_without_subscriber_layers() {
    assert_bootstrap_case("metrics_only");
}

#[cfg(feature = "otel-metrics")]
#[test]
fn install_global_keeps_metrics_when_best_effort_subscriber_install_is_blocked() {
    let output = assert_bootstrap_case("metrics_best_effort_conflict");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        stderr.contains("unified logging and OpenTelemetry layers were not installed"),
        "best-effort conflict should emit a warning, stderr={stderr}"
    );
}

#[cfg(feature = "otel-traces")]
#[test]
fn install_global_supports_tracing_layer() {
    assert_bootstrap_case("traces_enabled");
}

#[cfg(feature = "otel-logs")]
#[test]
fn install_global_supports_logs_layer() {
    assert_bootstrap_case("logs_enabled");
}

#[test]
fn install_global_required_policy_fails_when_subscriber_is_blocked() {
    assert_bootstrap_case("required_conflict");
}

#[test]
#[ignore]
fn install_global_bootstrap_child() {
    let case = std::env::var("ROCKETMQ_OBSERVABILITY_BOOTSTRAP_CASE")
        .expect("bootstrap child case should be provided by parent test");

    match case.as_str() {
        "console_only" => bootstrap_console_only(),
        "file_only" => bootstrap_file_only(),
        #[cfg(feature = "otel-metrics")]
        "metrics_only" => bootstrap_metrics_only(),
        #[cfg(feature = "otel-metrics")]
        "metrics_best_effort_conflict" => bootstrap_metrics_best_effort_conflict(),
        #[cfg(feature = "otel-traces")]
        "traces_enabled" => bootstrap_traces_enabled(),
        #[cfg(feature = "otel-logs")]
        "logs_enabled" => bootstrap_logs_enabled(),
        "required_conflict" => bootstrap_required_conflict(),
        other => panic!("unknown bootstrap child case: {other}"),
    }
}

#[cfg(feature = "otel-traces")]
#[test]
fn best_effort_traces_report_blocked_subscriber_install_status() {
    install_blocking_subscriber();

    let config = traces_config(rocketmq_observability::SubscriberInstallPolicy::BestEffort);

    let guard = rocketmq_observability::init_observability(&config)
        .expect("best-effort mode should keep existing compatibility behavior");

    assert!(
        guard.tracer_provider().is_some(),
        "the tracer provider initializes even though the tracing subscriber layer is blocked"
    );
    assert_eq!(
        guard.subscriber_install_status(),
        rocketmq_observability::SubscriberInstallStatus {
            attempted: true,
            installed: false,
        }
    );

    guard.shutdown().expect("trace provider shutdown should succeed");
}

#[cfg(feature = "otel-traces")]
#[test]
fn required_traces_report_subscriber_install_failure() {
    install_blocking_subscriber();

    let config = traces_config(rocketmq_observability::SubscriberInstallPolicy::Required);

    let error = rocketmq_observability::init_observability(&config)
        .expect_err("required mode should fail when the subscriber layer cannot be installed");

    assert!(matches!(
        error,
        rocketmq_observability::ObservabilityError::SubscriberInstallFailed {
            attempted: true,
            installed: false
        }
    ));
}

#[cfg(feature = "otel-traces")]
fn traces_config(
    subscriber_install_policy: rocketmq_observability::SubscriberInstallPolicy,
) -> rocketmq_observability::ObservabilityConfig {
    let mut config = rocketmq_observability::ObservabilityConfig {
        enabled: true,
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    config.traces.enabled = true;
    config.traces.exporter = rocketmq_observability::config::TraceExporter::Disable;
    config.subscriber_install_policy = subscriber_install_policy;
    config
}

#[cfg(feature = "otel-traces")]
fn install_blocking_subscriber() {
    let _ = tracing_subscriber::fmt().with_writer(std::io::sink).try_init();
}

fn assert_bootstrap_case(case: &str) -> Output {
    let output = Command::new(std::env::current_exe().expect("current test executable should be available"))
        .arg("--exact")
        .arg("install_global_bootstrap_child")
        .arg("--ignored")
        .env("ROCKETMQ_OBSERVABILITY_BOOTSTRAP_CASE", case)
        .output()
        .unwrap_or_else(|error| panic!("failed to run bootstrap child case {case}: {error}"));

    assert!(
        output.status.success(),
        "bootstrap child case {case} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    output
}

fn bootstrap_console_only() {
    let config = rocketmq_observability::TelemetryBootstrapConfig::default();
    let guard = rocketmq_observability::install_global(&config).expect("console-only bootstrap should install");

    assert_eq!(
        guard.subscriber_install_status(),
        rocketmq_observability::SubscriberInstallStatus {
            attempted: true,
            installed: true,
        }
    );
    assert_eq!(guard.logging_guard().file_sink_count(), 0);

    let report = guard.shutdown();
    assert!(report.is_healthy());
    assert!(!report.file_log_enabled);
    report.into_result().expect("console-only shutdown should succeed");
}

fn bootstrap_file_only() {
    let directory = unique_temp_log_dir("install-global-file-only");
    let mut config = rocketmq_observability::TelemetryBootstrapConfig::default();
    config.logging.console.enabled = false;
    config.logging.file.enabled = true;
    config.logging.file.directory = directory.to_string_lossy().into_owned();
    config.logging.file.file_name_prefix = "bootstrap.log".to_string();
    config.logging.file.rotation = rocketmq_observability::LogRotation::Never;

    let guard = rocketmq_observability::install_global(&config).expect("file-only bootstrap should install");

    assert_eq!(
        guard.subscriber_install_status(),
        rocketmq_observability::SubscriberInstallStatus {
            attempted: true,
            installed: true,
        }
    );
    assert_eq!(guard.logging_guard().file_sink_count(), 1);
    assert!(directory.join("bootstrap.log").exists());

    let report = guard.shutdown();
    assert!(report.is_healthy());
    assert!(report.file_log_enabled);
    report.into_result().expect("file-only shutdown should succeed");
    cleanup_temp_log_dir(directory);
}

#[cfg(feature = "otel-metrics")]
fn bootstrap_metrics_only() {
    let mut config = rocketmq_observability::TelemetryBootstrapConfig::default();
    config.logging.enabled = false;
    config.observability.enabled = true;
    config.observability.metrics.enabled = true;
    config.observability.metrics.exporter = rocketmq_observability::config::MetricsExporter::Disable;

    let guard = rocketmq_observability::install_global(&config).expect("metrics-only bootstrap should initialize");

    assert!(guard.telemetry_guard().meter_provider().is_some());
    assert_eq!(
        guard.subscriber_install_status(),
        rocketmq_observability::SubscriberInstallStatus {
            attempted: false,
            installed: false,
        }
    );

    guard
        .shutdown()
        .into_result()
        .expect("metrics-only shutdown should succeed");
}

#[cfg(feature = "otel-metrics")]
fn bootstrap_metrics_best_effort_conflict() {
    install_stderr_blocking_subscriber();

    let mut config = rocketmq_observability::TelemetryBootstrapConfig::default();
    config.observability.enabled = true;
    config.observability.metrics.enabled = true;
    config.observability.metrics.exporter = rocketmq_observability::config::MetricsExporter::Disable;
    config.observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::BestEffort;

    let guard = rocketmq_observability::install_global(&config)
        .expect("best-effort conflict should keep initialized providers");

    assert!(guard.telemetry_guard().meter_provider().is_some());
    assert_eq!(
        guard.subscriber_install_status(),
        rocketmq_observability::SubscriberInstallStatus {
            attempted: true,
            installed: false,
        }
    );

    guard
        .shutdown()
        .into_result()
        .expect("metrics conflict shutdown should succeed");
}

#[cfg(feature = "otel-traces")]
fn bootstrap_traces_enabled() {
    let mut config = rocketmq_observability::TelemetryBootstrapConfig::default();
    config.observability.enabled = true;
    config.observability.traces.enabled = true;
    config.observability.traces.exporter = rocketmq_observability::config::TraceExporter::Disable;

    let guard = rocketmq_observability::install_global(&config).expect("trace bootstrap should install");

    assert!(guard.telemetry_guard().tracer_provider().is_some());
    assert_eq!(
        guard.subscriber_install_status(),
        rocketmq_observability::SubscriberInstallStatus {
            attempted: true,
            installed: true,
        }
    );

    guard
        .shutdown()
        .into_result()
        .expect("trace bootstrap shutdown should succeed");
}

#[cfg(feature = "otel-logs")]
fn bootstrap_logs_enabled() {
    let mut config = rocketmq_observability::TelemetryBootstrapConfig::default();
    config.observability.enabled = true;
    config.observability.logs.enabled = true;
    config.observability.logs.exporter = rocketmq_observability::config::LogsExporter::Disable;

    let guard = rocketmq_observability::install_global(&config).expect("logs bootstrap should install");

    assert!(guard.telemetry_guard().logger_provider().is_some());
    assert_eq!(
        guard.subscriber_install_status(),
        rocketmq_observability::SubscriberInstallStatus {
            attempted: true,
            installed: true,
        }
    );

    guard
        .shutdown()
        .into_result()
        .expect("logs bootstrap shutdown should succeed");
}

fn bootstrap_required_conflict() {
    install_stderr_blocking_subscriber();

    let mut config = rocketmq_observability::TelemetryBootstrapConfig::default();
    config.observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;

    let error = match rocketmq_observability::install_global(&config) {
        Ok(_) => panic!("required mode should fail when the global subscriber already exists"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        rocketmq_observability::ObservabilityError::SubscriberInstallFailed {
            attempted: true,
            installed: false
        }
    ));
}

fn install_stderr_blocking_subscriber() {
    let _ = tracing_subscriber::fmt().with_writer(std::io::stderr).try_init();
}

fn unique_temp_log_dir(test_name: &str) -> PathBuf {
    let id = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rocketmq-observability-{test_name}-{}-{id}-{nanos}",
        std::process::id()
    ))
}

fn cleanup_temp_log_dir(directory: PathBuf) {
    if directory.exists() {
        std::fs::remove_dir_all(&directory).expect("temporary log directory should be removable");
    }
}
