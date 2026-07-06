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

use tracing_appender::non_blocking::ErrorCounter;
use tracing_appender::non_blocking::NonBlockingBuilder;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;

use crate::config::ConsoleLogConfig;
use crate::config::FileLogConfig;
use crate::config::LogFormat;
use crate::config::LogRotation;
use crate::config::SubscriberInstallStatus;
use crate::error::ObservabilityError;
use crate::init::TelemetryGuard;

pub type BoxedRegistryLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

#[derive(Default)]
pub struct LoggingGuard {
    worker_guards: Vec<WorkerGuard>,
    dropped_log_counters: Vec<ErrorCounter>,
}

impl LoggingGuard {
    pub fn noop() -> Self {
        Self::new(Vec::new(), Vec::new())
    }

    pub(crate) fn new(worker_guards: Vec<WorkerGuard>, dropped_log_counters: Vec<ErrorCounter>) -> Self {
        Self {
            worker_guards,
            dropped_log_counters,
        }
    }

    pub fn dropped_log_lines(&self) -> usize {
        self.dropped_log_counters.iter().map(ErrorCounter::dropped_lines).sum()
    }

    pub fn file_sink_count(&self) -> usize {
        self.worker_guards.len()
    }
}

pub struct FileLogLayer {
    layer: BoxedRegistryLayer,
    logging_guard: LoggingGuard,
}

impl FileLogLayer {
    fn new(layer: BoxedRegistryLayer, logging_guard: LoggingGuard) -> Self {
        Self { layer, logging_guard }
    }

    pub fn logging_guard(&self) -> &LoggingGuard {
        &self.logging_guard
    }

    pub fn into_parts(self) -> (BoxedRegistryLayer, LoggingGuard) {
        (self.layer, self.logging_guard)
    }
}

pub fn build_env_filter(filter: &str) -> Result<EnvFilter, ObservabilityError> {
    EnvFilter::try_new(filter).map_err(|error| ObservabilityError::invalid_log_filter(filter, error))
}

pub fn build_console_layer(config: &ConsoleLogConfig) -> Option<BoxedRegistryLayer> {
    if !config.enabled {
        return None;
    }

    let layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(config.ansi)
        .with_target(config.target)
        .with_thread_ids(config.thread_ids)
        .with_thread_names(config.thread_names)
        .with_line_number(config.line_number);

    match config.format {
        LogFormat::Compact => Some(Box::new(layer.compact())),
        LogFormat::Pretty => Some(Box::new(layer.pretty())),
        LogFormat::Json => Some(Box::new(layer.json())),
    }
}

pub fn build_file_layer(config: &FileLogConfig) -> Result<Option<FileLogLayer>, ObservabilityError> {
    if !config.enabled {
        return Ok(None);
    }

    if config.directory.trim().is_empty() {
        return Err(ObservabilityError::logging_init("file log directory must not be empty"));
    }

    if config.file_name_prefix.trim().is_empty() {
        return Err(ObservabilityError::logging_init(
            "file log name prefix must not be empty",
        ));
    }

    let appender = build_rolling_file_appender(config)?;
    let (writer, worker_guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(config.non_blocking.buffered_lines_limit)
        .lossy(config.non_blocking.lossy)
        .finish(appender);
    let error_counter = writer.error_counter();

    let layer = tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true);

    let layer = match config.format {
        LogFormat::Compact => Box::new(layer.compact()) as BoxedRegistryLayer,
        LogFormat::Pretty => Box::new(layer.pretty()) as BoxedRegistryLayer,
        LogFormat::Json => Box::new(layer.json()) as BoxedRegistryLayer,
    };

    Ok(Some(FileLogLayer::new(
        layer,
        LoggingGuard::new(vec![worker_guard], vec![error_counter]),
    )))
}

fn build_rolling_file_appender(config: &FileLogConfig) -> Result<RollingFileAppender, ObservabilityError> {
    RollingFileAppender::builder()
        .rotation(to_tracing_rotation(config.rotation))
        .filename_prefix(config.file_name_prefix.as_str())
        .build(config.directory.as_str())
        .map_err(ObservabilityError::logging_init)
}

fn to_tracing_rotation(rotation: LogRotation) -> Rotation {
    match rotation {
        LogRotation::Never => Rotation::NEVER,
        LogRotation::Hourly => Rotation::HOURLY,
        LogRotation::Daily => Rotation::DAILY,
    }
}

pub struct TelemetryRuntimeGuard {
    telemetry_guard: TelemetryGuard,
    logging_guard: LoggingGuard,
}

impl TelemetryRuntimeGuard {
    pub fn new(telemetry_guard: TelemetryGuard, logging_guard: LoggingGuard) -> Self {
        Self {
            telemetry_guard,
            logging_guard,
        }
    }

    pub fn noop() -> Self {
        Self::new(TelemetryGuard::noop(), LoggingGuard::noop())
    }

    pub fn telemetry_guard(&self) -> &TelemetryGuard {
        &self.telemetry_guard
    }

    pub fn logging_guard(&self) -> &LoggingGuard {
        &self.logging_guard
    }

    pub fn subscriber_install_status(&self) -> SubscriberInstallStatus {
        self.telemetry_guard.subscriber_install_status()
    }

    pub fn dropped_log_lines(&self) -> usize {
        self.logging_guard.dropped_log_lines()
    }

    pub fn shutdown(self) -> Result<(), ObservabilityError> {
        let Self {
            telemetry_guard,
            logging_guard,
        } = self;

        let shutdown_result = telemetry_guard.shutdown();
        drop(logging_guard);
        shutdown_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use tracing_subscriber::prelude::*;

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn noop_logging_guard_reports_zero_dropped_lines() {
        let guard = LoggingGuard::noop();

        assert_eq!(guard.file_sink_count(), 0);
        assert_eq!(guard.dropped_log_lines(), 0);
    }

    #[test]
    fn logging_guard_tracks_worker_guards_without_static_storage() {
        let (writer, worker_guard) = tracing_appender::non_blocking(std::io::sink());
        let error_counter = writer.error_counter();
        let guard = LoggingGuard::new(vec![worker_guard], vec![error_counter]);

        assert_eq!(guard.file_sink_count(), 1);
        assert_eq!(guard.dropped_log_lines(), 0);
    }

    #[test]
    fn env_filter_accepts_target_directives() {
        let filter =
            build_env_filter("rocketmq_store=debug,rocketmq_remoting=warn").expect("target directives should parse");

        assert!(filter.to_string().contains("rocketmq_store=debug"));
    }

    #[test]
    fn invalid_env_filter_returns_typed_error() {
        let error = build_env_filter("rocketmq_store==debug").expect_err("invalid filter should fail");

        assert!(matches!(
            error,
            ObservabilityError::InvalidLogFilter { filter, .. } if filter == "rocketmq_store==debug"
        ));
    }

    #[test]
    fn disabled_console_config_builds_no_layer() {
        let config = ConsoleLogConfig {
            enabled: false,
            ..ConsoleLogConfig::default()
        };

        assert!(build_console_layer(&config).is_none());
    }

    #[test]
    fn console_layer_builder_supports_all_formats() {
        for format in [LogFormat::Compact, LogFormat::Pretty, LogFormat::Json] {
            let config = ConsoleLogConfig {
                format,
                ..ConsoleLogConfig::default()
            };

            assert!(build_console_layer(&config).is_some());
        }
    }

    #[test]
    fn disabled_file_config_builds_no_layer_or_directory() {
        let directory = unique_temp_log_dir("disabled_file_config_builds_no_layer_or_directory");
        let config = FileLogConfig {
            enabled: false,
            directory: directory.to_string_lossy().into_owned(),
            ..FileLogConfig::default()
        };

        let layer = build_file_layer(&config).expect("disabled file sink should not fail");

        assert!(layer.is_none());
        assert!(!directory.exists());
    }

    #[test]
    fn file_layer_creates_missing_directory_and_tracks_guard() {
        let directory = unique_temp_log_dir("file_layer_creates_missing_directory_and_tracks_guard");
        let config = FileLogConfig {
            enabled: true,
            directory: directory.to_string_lossy().into_owned(),
            file_name_prefix: "rocketmq-test.log".to_string(),
            rotation: LogRotation::Never,
            ..FileLogConfig::default()
        };

        let layer = build_file_layer(&config)
            .expect("file sink should build")
            .expect("enabled file sink should return a layer");

        assert!(directory.exists());
        assert_eq!(layer.logging_guard().file_sink_count(), 1);
        assert_eq!(layer.logging_guard().dropped_log_lines(), 0);
        assert!(directory.join("rocketmq-test.log").exists());

        drop(layer);
        cleanup_temp_log_dir(directory);
    }

    #[test]
    fn file_layer_supports_rotation_variants() {
        for rotation in [LogRotation::Never, LogRotation::Hourly, LogRotation::Daily] {
            let directory = unique_temp_log_dir("file_layer_supports_rotation_variants");
            let config = FileLogConfig {
                enabled: true,
                directory: directory.to_string_lossy().into_owned(),
                file_name_prefix: "rocketmq-rotation.log".to_string(),
                rotation,
                ..FileLogConfig::default()
            };

            let layer = build_file_layer(&config)
                .expect("file sink should build")
                .expect("enabled file sink should return a layer");

            assert_eq!(layer.logging_guard().file_sink_count(), 1);
            assert!(
                fs::read_dir(&directory)
                    .expect("log directory should be readable")
                    .next()
                    .is_some(),
                "rotation {rotation:?} should create a log file"
            );

            drop(layer);
            cleanup_temp_log_dir(directory);
        }
    }

    #[test]
    fn file_layer_writes_without_ansi_escape_codes() {
        let directory = unique_temp_log_dir("file_layer_writes_without_ansi_escape_codes");
        let config = FileLogConfig {
            enabled: true,
            directory: directory.to_string_lossy().into_owned(),
            file_name_prefix: "ansi-free.log".to_string(),
            rotation: LogRotation::Never,
            format: LogFormat::Compact,
            ..FileLogConfig::default()
        };

        let layer = build_file_layer(&config)
            .expect("file sink should build")
            .expect("enabled file sink should return a layer");
        let (layer, logging_guard) = layer.into_parts();
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "rocketmq_observability::logging::tests", "file sink event");
        });
        drop(logging_guard);

        let contents =
            fs::read_to_string(directory.join("ansi-free.log")).expect("file sink output should be readable");
        assert!(contents.contains("file sink event"));
        assert!(!contents.contains('\u{1b}'));

        cleanup_temp_log_dir(directory);
    }

    #[test]
    fn invalid_file_layer_config_returns_typed_error() {
        let config = FileLogConfig {
            enabled: true,
            directory: String::new(),
            ..FileLogConfig::default()
        };

        let error = match build_file_layer(&config) {
            Ok(_) => panic!("empty directory should fail"),
            Err(error) => error,
        };

        assert!(matches!(error, ObservabilityError::LoggingInit(message) if message.contains("directory")));
    }

    #[test]
    fn noop_runtime_guard_exposes_zero_logging_and_subscriber_status() {
        let guard = TelemetryRuntimeGuard::noop();

        assert_eq!(guard.dropped_log_lines(), 0);
        assert_eq!(
            guard.subscriber_install_status(),
            SubscriberInstallStatus {
                attempted: false,
                installed: false,
            }
        );
    }

    #[test]
    fn noop_runtime_guard_shutdown_succeeds() {
        TelemetryRuntimeGuard::noop()
            .shutdown()
            .expect("noop runtime guard shutdown should succeed");
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
            fs::remove_dir_all(&directory).expect("temporary log directory should be removable");
        }
    }
}
