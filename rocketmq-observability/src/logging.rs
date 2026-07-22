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
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::reload;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;

use serde::Serialize;

use crate::config::ConsoleLogConfig;
use crate::config::FileLogConfig;
use crate::config::LogFormat;
use crate::config::LogRotation;
use crate::config::SubscriberInstallPolicy;
use crate::config::SubscriberInstallStatus;
use crate::config::TelemetryBootstrapConfig;
use crate::error::ObservabilityError;
use crate::init::TelemetryGuard;
use crate::init::TelemetryProviderShutdownReport;
use crate::LogFilterHandle;
use crate::LogFilterInputs;
use crate::LogFilterResolver;
use crate::ResolvedLogFilter;

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

    pub(crate) fn append(&mut self, mut other: Self) {
        self.worker_guards.append(&mut other.worker_guards);
        self.dropped_log_counters.append(&mut other.dropped_log_counters);
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

pub fn install_global(config: &TelemetryBootstrapConfig) -> Result<TelemetryRuntimeGuard, ObservabilityError> {
    let resolved_filter = LogFilterResolver::resolve(LogFilterInputs {
        config: Some(config.logging.filter.as_str()),
        ..LogFilterInputs::default()
    })?;
    install_global_with_filter(config, resolved_filter)
}

pub fn install_global_with_filter(
    config: &TelemetryBootstrapConfig,
    resolved_filter: ResolvedLogFilter,
) -> Result<TelemetryRuntimeGuard, ObservabilityError> {
    init_log_tracer();

    let mut telemetry_guard = crate::init::init_telemetry_providers(&config.observability)?;
    let (layers, filter, logging_guard) =
        match build_subscriber_layers(config, resolved_filter.filter(), &telemetry_guard) {
            Ok(layers) => layers,
            Err(error) => {
                let _ = telemetry_guard.shutdown();
                return Err(error);
            }
        };

    let (subscriber_install_status, log_filter_handle) = install_subscriber_layers(
        layers,
        filter,
        resolved_filter.clone(),
        config.logging.reload.enabled,
        config.observability.service_name.as_str(),
    );
    telemetry_guard.set_subscriber_install_status(subscriber_install_status);

    if let Err(error) = enforce_bootstrap_subscriber_policy(config, subscriber_install_status) {
        let _ = telemetry_guard.shutdown();
        drop(logging_guard);
        return Err(error);
    }

    if subscriber_install_status.installed {
        crate::metrics::log_filter::set_active(
            config.observability.service_name.as_str(),
            None,
            resolved_filter.source(),
        );
    }

    Ok(TelemetryRuntimeGuard::new_with_log_filter_handle(
        telemetry_guard,
        logging_guard,
        log_filter_handle,
    ))
}

fn init_log_tracer() {
    if let Err(error) = tracing_log::LogTracer::init() {
        tracing::debug!(
            target: "rocketmq_observability",
            %error,
            "log tracer was already initialized; keeping the existing log facade bridge"
        );
    }
}

fn build_subscriber_layers(
    config: &TelemetryBootstrapConfig,
    resolved_filter: &str,
    _telemetry_guard: &TelemetryGuard,
) -> Result<(Vec<BoxedRegistryLayer>, Option<EnvFilter>, LoggingGuard), ObservabilityError> {
    let mut layers = Vec::new();
    let mut logging_guard = LoggingGuard::noop();

    if config.logging.enabled {
        if let Some(console_layer) = build_console_layer(&config.logging.console) {
            layers.push(console_layer);
        }

        if let Some(file_layer) = build_file_layer(&config.logging.file)? {
            let (layer, file_logging_guard) = file_layer.into_parts();
            layers.push(layer);
            logging_guard.append(file_logging_guard);
        }
    }

    #[cfg(feature = "otel-traces")]
    if let Some(tracer_provider) = _telemetry_guard.tracer_provider() {
        layers.push(Box::new(crate::trace::build_tracing_layer(
            &config.observability,
            tracer_provider,
        )));
    }

    #[cfg(feature = "otel-logs")]
    if let Some(logger_provider) = _telemetry_guard.logger_provider() {
        layers.push(Box::new(crate::logs::bridge::build_logs_layer(logger_provider)));
    }

    let filter = if layers.is_empty() {
        None
    } else {
        Some(build_env_filter(resolved_filter)?)
    };
    Ok((layers, filter, logging_guard))
}

fn compose_subscriber_layers(
    layers: Vec<BoxedRegistryLayer>,
    filter: Option<EnvFilter>,
) -> impl tracing::Subscriber + Send + Sync + 'static {
    tracing_subscriber::registry().with(layers).with(filter)
}

fn install_subscriber_layers(
    layers: Vec<BoxedRegistryLayer>,
    filter: Option<EnvFilter>,
    resolved_filter: ResolvedLogFilter,
    reload_enabled: bool,
    service_name: &str,
) -> (SubscriberInstallStatus, Option<LogFilterHandle>) {
    if layers.is_empty() && filter.is_none() {
        return (SubscriberInstallStatus::default(), None);
    }

    if reload_enabled {
        if let Some(filter) = filter {
            let (filter_layer, reload_handle) = reload::Layer::new(filter);
            let result =
                tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layers).with(filter_layer));
            let status = SubscriberInstallStatus::attempted(result.is_ok());
            let handle = status.installed.then(|| {
                LogFilterHandle::new(service_name.to_owned(), resolved_filter, move |filter| {
                    reload_handle
                        .reload(filter)
                        .map_err(|error| ObservabilityError::logging_init(format!("log filter reload failed: {error}")))
                })
            });
            return (status, handle);
        }
    }

    let result = tracing::subscriber::set_global_default(compose_subscriber_layers(layers, filter));
    (SubscriberInstallStatus::attempted(result.is_ok()), None)
}

fn enforce_bootstrap_subscriber_policy(
    config: &TelemetryBootstrapConfig,
    status: SubscriberInstallStatus,
) -> Result<(), ObservabilityError> {
    if !status.attempted || status.installed {
        return Ok(());
    }

    match config.observability.subscriber_install_policy {
        SubscriberInstallPolicy::Required => Err(ObservabilityError::subscriber_install_failed(
            status.attempted,
            status.installed,
        )),
        SubscriberInstallPolicy::BestEffort => {
            tracing::warn!(
                target: "rocketmq_observability",
                attempted = status.attempted,
                installed = status.installed,
                "tracing subscriber was already initialized; unified logging and OpenTelemetry layers were not installed"
            );
            Ok(())
        }
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
    log_filter_handle: Option<LogFilterHandle>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TelemetryShutdownReport {
    pub subscriber_installed: bool,
    pub file_log_enabled: bool,
    pub dropped_log_lines: usize,
    pub logs_shutdown_ok: bool,
    pub traces_shutdown_ok: bool,
    pub metrics_shutdown_ok: bool,
    pub logs_shutdown_error: Option<String>,
    pub traces_shutdown_error: Option<String>,
    pub metrics_shutdown_error: Option<String>,
}

impl TelemetryShutdownReport {
    fn new(
        subscriber_install_status: SubscriberInstallStatus,
        file_log_enabled: bool,
        dropped_log_lines: usize,
        provider_report: TelemetryProviderShutdownReport,
    ) -> Self {
        Self {
            subscriber_installed: subscriber_install_status.installed,
            file_log_enabled,
            dropped_log_lines,
            logs_shutdown_ok: provider_report.logs_shutdown_ok,
            traces_shutdown_ok: provider_report.traces_shutdown_ok,
            metrics_shutdown_ok: provider_report.metrics_shutdown_ok,
            logs_shutdown_error: provider_report.logs_shutdown_error,
            traces_shutdown_error: provider_report.traces_shutdown_error,
            metrics_shutdown_error: provider_report.metrics_shutdown_error,
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.logs_shutdown_ok && self.traces_shutdown_ok && self.metrics_shutdown_ok
    }

    pub fn into_result(self) -> Result<Self, ObservabilityError> {
        if let Some(error) = self.logs_shutdown_error.as_ref() {
            return Err(ObservabilityError::logs_shutdown(error));
        }
        if let Some(error) = self.traces_shutdown_error.as_ref() {
            return Err(ObservabilityError::traces_shutdown(error));
        }
        if let Some(error) = self.metrics_shutdown_error.as_ref() {
            return Err(ObservabilityError::metrics_shutdown(error));
        }
        Ok(self)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|error| format!("{{\"serialization_error\":\"{error}\"}}"))
    }
}

impl TelemetryRuntimeGuard {
    pub fn new(telemetry_guard: TelemetryGuard, logging_guard: LoggingGuard) -> Self {
        Self {
            telemetry_guard,
            logging_guard,
            log_filter_handle: None,
        }
    }

    fn new_with_log_filter_handle(
        telemetry_guard: TelemetryGuard,
        logging_guard: LoggingGuard,
        log_filter_handle: Option<LogFilterHandle>,
    ) -> Self {
        Self {
            telemetry_guard,
            logging_guard,
            log_filter_handle,
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

    pub fn log_filter_handle(&self) -> Option<LogFilterHandle> {
        self.log_filter_handle.clone()
    }

    pub fn subscriber_install_status(&self) -> SubscriberInstallStatus {
        self.telemetry_guard.subscriber_install_status()
    }

    pub fn dropped_log_lines(&self) -> usize {
        self.logging_guard.dropped_log_lines()
    }

    pub fn shutdown(self) -> TelemetryShutdownReport {
        self.shutdown_with_timeout(std::time::Duration::from_millis(
            crate::exporter::outage::DEFAULT_SHUTDOWN_TIMEOUT_MILLIS,
        ))
    }

    /// Flushes logging and telemetry providers without exceeding one shared timeout budget.
    pub fn shutdown_with_timeout(self, timeout: std::time::Duration) -> TelemetryShutdownReport {
        let Self {
            telemetry_guard,
            logging_guard,
            log_filter_handle: _,
        } = self;

        let subscriber_install_status = telemetry_guard.subscriber_install_status();
        let file_log_enabled = logging_guard.file_sink_count() > 0;
        let dropped_log_lines = logging_guard.dropped_log_lines();
        if dropped_log_lines > 0 {
            tracing::warn!(
                dropped_log_lines,
                "telemetry logging guard reported dropped log lines before shutdown"
            );
        }

        // Drop local file logging guards before shutting down OpenTelemetry providers so
        // non-blocking file writers are flushed and joined under explicit runtime ownership.
        drop(logging_guard);
        let provider_report = telemetry_guard.shutdown_with_report_timeout(timeout);
        let provider_shutdown_healthy = provider_report.is_healthy();
        let report = TelemetryShutdownReport::new(
            subscriber_install_status,
            file_log_enabled,
            dropped_log_lines,
            provider_report,
        );
        if !provider_shutdown_healthy {
            tracing::warn!(
                report = %report.to_json(),
                "telemetry provider shutdown report is unhealthy"
            );
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Clone)]
    struct RecordingLayer {
        levels: Arc<Mutex<Vec<tracing::Level>>>,
    }

    impl Layer<Registry> for RecordingLayer {
        fn on_event(&self, event: &tracing::Event<'_>, _context: tracing_subscriber::layer::Context<'_, Registry>) {
            self.levels
                .lock()
                .expect("recorded levels lock should remain available")
                .push(*event.metadata().level());
        }
    }

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
    fn global_info_filter_suppresses_debug_events_inside_info_spans() {
        let levels = Arc::new(Mutex::new(Vec::new()));
        let layers = vec![Box::new(RecordingLayer {
            levels: Arc::clone(&levels),
        }) as BoxedRegistryLayer];
        let filter = Some(build_env_filter("info").expect("info filter should parse"));
        let subscriber = compose_subscriber_layers(layers, filter);

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("info_filter_regression");
            let _guard = span.enter();
            tracing::debug!("debug event should be filtered");
            tracing::info!("info event should be recorded");
        });

        assert_eq!(
            *levels.lock().expect("recorded levels lock should remain available"),
            vec![tracing::Level::INFO]
        );
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
        let report = TelemetryRuntimeGuard::noop().shutdown();

        assert!(report.is_healthy());
        assert_eq!(report.dropped_log_lines, 0);
        assert!(!report.file_log_enabled);
        assert!(!report.subscriber_installed);
        report
            .into_result()
            .expect("noop runtime guard shutdown should succeed");
    }

    #[test]
    fn noop_runtime_guard_honors_caller_shutdown_budget() {
        let report = TelemetryRuntimeGuard::noop().shutdown_with_timeout(std::time::Duration::ZERO);

        assert!(report.is_healthy());
    }

    #[test]
    fn runtime_shutdown_report_preserves_provider_failure() {
        let report = TelemetryShutdownReport {
            subscriber_installed: true,
            file_log_enabled: true,
            dropped_log_lines: 3,
            logs_shutdown_ok: false,
            traces_shutdown_ok: true,
            metrics_shutdown_ok: true,
            logs_shutdown_error: Some("logger provider failed".to_string()),
            traces_shutdown_error: None,
            metrics_shutdown_error: None,
        };

        assert!(!report.is_healthy());
        assert!(matches!(
            report.into_result(),
            Err(ObservabilityError::LogsShutdown(message)) if message == "logger provider failed"
        ));
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
