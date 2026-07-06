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
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;

use crate::config::ConsoleLogConfig;
use crate::config::LogFormat;
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
}
