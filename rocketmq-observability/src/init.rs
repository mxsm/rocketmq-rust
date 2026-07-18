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

#[cfg(feature = "otel-logs")]
use crate::config::LogsExporter;
use crate::config::MetricsExporter;
use crate::config::ObservabilityConfig;
use crate::config::SubscriberInstallPolicy;
use crate::config::SubscriberInstallStatus;
use crate::error::ObservabilityError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TelemetryProviderShutdownReport {
    pub(crate) logs_shutdown_ok: bool,
    pub(crate) traces_shutdown_ok: bool,
    pub(crate) metrics_shutdown_ok: bool,
    pub(crate) logs_shutdown_error: Option<String>,
    pub(crate) traces_shutdown_error: Option<String>,
    pub(crate) metrics_shutdown_error: Option<String>,
}

impl Default for TelemetryProviderShutdownReport {
    fn default() -> Self {
        Self {
            logs_shutdown_ok: true,
            traces_shutdown_ok: true,
            metrics_shutdown_ok: true,
            logs_shutdown_error: None,
            traces_shutdown_error: None,
            metrics_shutdown_error: None,
        }
    }
}

impl TelemetryProviderShutdownReport {
    pub(crate) fn is_healthy(&self) -> bool {
        self.logs_shutdown_ok && self.traces_shutdown_ok && self.metrics_shutdown_ok
    }

    pub(crate) fn into_result(self) -> Result<(), ObservabilityError> {
        if let Some(error) = self.logs_shutdown_error {
            return Err(ObservabilityError::logs_shutdown(error));
        }
        if let Some(error) = self.traces_shutdown_error {
            return Err(ObservabilityError::traces_shutdown(error));
        }
        if let Some(error) = self.metrics_shutdown_error {
            return Err(ObservabilityError::metrics_shutdown(error));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct TelemetryGuard {
    subscriber_install_status: SubscriberInstallStatus,
    #[cfg(feature = "otel-metrics")]
    meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    #[cfg(feature = "prometheus")]
    prometheus_http: Option<crate::exporter::prometheus::PrometheusHttpHandle>,
    #[cfg(feature = "otel-traces")]
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
    #[cfg(feature = "otel-logs")]
    logger_provider: Option<opentelemetry_sdk::logs::SdkLoggerProvider>,
}

impl TelemetryGuard {
    pub fn noop() -> Self {
        Self::default()
    }

    pub fn shutdown(self) -> Result<(), ObservabilityError> {
        self.shutdown_with_timeout(std::time::Duration::from_millis(
            crate::exporter::outage::DEFAULT_SHUTDOWN_TIMEOUT_MILLIS,
        ))
    }

    /// Shuts down every telemetry provider within one shared timeout budget.
    ///
    /// Later providers receive only the time remaining from the original absolute deadline, so a
    /// collector outage cannot multiply the shutdown delay by the number of enabled signals.
    ///
    /// # Errors
    ///
    /// Returns an exporter-specific [`ObservabilityError`] when a provider fails or exhausts the
    /// shared timeout budget.
    pub fn shutdown_with_timeout(self, timeout: std::time::Duration) -> Result<(), ObservabilityError> {
        self.shutdown_inner(timeout).into_result()
    }

    pub(crate) fn shutdown_with_report(self) -> TelemetryProviderShutdownReport {
        self.shutdown_inner(std::time::Duration::from_millis(
            crate::exporter::outage::DEFAULT_SHUTDOWN_TIMEOUT_MILLIS,
        ))
    }

    pub fn subscriber_install_status(&self) -> SubscriberInstallStatus {
        self.subscriber_install_status
    }

    pub(crate) fn set_subscriber_install_status(&mut self, status: SubscriberInstallStatus) {
        self.subscriber_install_status = status;
    }

    #[cfg(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs"))]
    fn shutdown_inner(mut self, timeout: std::time::Duration) -> TelemetryProviderShutdownReport {
        let mut report = TelemetryProviderShutdownReport::default();
        let started_at = std::time::Instant::now();

        #[cfg(feature = "otel-logs")]
        if let Some(provider) = self.logger_provider.take() {
            if let Err(error) = provider.shutdown_with_timeout(timeout.saturating_sub(started_at.elapsed())) {
                report.logs_shutdown_ok = false;
                report.logs_shutdown_error = Some(error.to_string());
            }
        }

        #[cfg(feature = "otel-traces")]
        if let Some(provider) = self.tracer_provider.take() {
            if let Err(error) = provider.shutdown_with_timeout(timeout.saturating_sub(started_at.elapsed())) {
                report.traces_shutdown_ok = false;
                report.traces_shutdown_error = Some(error.to_string());
            }
        }

        #[cfg(feature = "otel-metrics")]
        if let Some(provider) = self.meter_provider.take() {
            if let Err(error) = provider.shutdown_with_timeout(timeout.saturating_sub(started_at.elapsed())) {
                report.metrics_shutdown_ok = false;
                report.metrics_shutdown_error = Some(error.to_string());
            }
        }

        #[cfg(feature = "prometheus")]
        if let Some(prometheus_http) = self.prometheus_http.take() {
            prometheus_http.shutdown();
        }

        report
    }

    #[cfg(not(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs")))]
    fn shutdown_inner(self, _timeout: std::time::Duration) -> TelemetryProviderShutdownReport {
        TelemetryProviderShutdownReport::default()
    }

    #[cfg(feature = "otel-metrics")]
    pub fn meter_provider(&self) -> Option<&opentelemetry_sdk::metrics::SdkMeterProvider> {
        self.meter_provider.as_ref()
    }

    #[cfg(feature = "otel-traces")]
    pub fn tracer_provider(&self) -> Option<&opentelemetry_sdk::trace::SdkTracerProvider> {
        self.tracer_provider.as_ref()
    }

    #[cfg(feature = "otel-logs")]
    pub fn logger_provider(&self) -> Option<&opentelemetry_sdk::logs::SdkLoggerProvider> {
        self.logger_provider.as_ref()
    }
}

#[cfg(feature = "otel-metrics")]
pub fn meter(
    provider: &opentelemetry_sdk::metrics::SdkMeterProvider,
    name: &'static str,
) -> opentelemetry::metrics::Meter {
    use opentelemetry::metrics::MeterProvider;

    provider.meter(name)
}

/// Initializes observability providers and keeps the legacy best-effort subscriber install
/// behavior.
///
/// New service entrypoints that need local console/file logging and OpenTelemetry trace/log layers
/// in one subscriber should use `crate::logging::install_global` instead.
pub fn init_observability(config: &ObservabilityConfig) -> Result<TelemetryGuard, ObservabilityError> {
    let mut guard = init_telemetry_providers(config)?;

    #[cfg(all(feature = "otel-traces", feature = "otel-logs"))]
    let subscriber_install_status =
        try_init_observability_subscriber(config, guard.tracer_provider.as_ref(), guard.logger_provider.as_ref());

    #[cfg(all(feature = "otel-traces", not(feature = "otel-logs")))]
    let subscriber_install_status = try_init_observability_subscriber(config, guard.tracer_provider.as_ref());

    #[cfg(all(not(feature = "otel-traces"), feature = "otel-logs"))]
    let subscriber_install_status = try_init_observability_subscriber(guard.logger_provider.as_ref());

    #[cfg(not(any(feature = "otel-traces", feature = "otel-logs")))]
    let subscriber_install_status = SubscriberInstallStatus::default();

    guard.set_subscriber_install_status(subscriber_install_status);
    if let Err(error) = enforce_subscriber_install_policy(config, guard.subscriber_install_status()) {
        let _ = guard.shutdown();
        return Err(error);
    }

    Ok(guard)
}

pub(crate) fn init_telemetry_providers(config: &ObservabilityConfig) -> Result<TelemetryGuard, ObservabilityError> {
    validate_config(config)?;
    crate::trace::configure_message_span_recording(&config.traces);

    if !config.enabled {
        return Ok(TelemetryGuard::noop());
    }

    #[cfg(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs"))]
    let mut guard = TelemetryGuard::noop();
    #[cfg(not(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs")))]
    let guard = TelemetryGuard::noop();

    if config.metrics.enabled {
        #[cfg(feature = "otel-metrics")]
        {
            #[cfg(feature = "prometheus")]
            let mut metrics_runtime = init_metrics(config)?;
            #[cfg(not(feature = "prometheus"))]
            let metrics_runtime = init_metrics(config)?;

            #[cfg(feature = "prometheus")]
            {
                if let Some(registry) = metrics_runtime.prometheus_registry.take() {
                    guard.prometheus_http = Some(crate::exporter::prometheus::spawn_prometheus_http_endpoint(
                        config, registry,
                    )?);
                }
            }
            opentelemetry::global::set_meter_provider(metrics_runtime.provider.clone());
            guard.meter_provider = Some(metrics_runtime.provider);
        }

        #[cfg(not(feature = "otel-metrics"))]
        return Err(ObservabilityError::FeatureDisabled("otel-metrics"));
    }

    if config.traces.enabled {
        #[cfg(feature = "otel-traces")]
        {
            guard.tracer_provider = Some(init_traces(config)?);
        }

        #[cfg(not(feature = "otel-traces"))]
        return Err(ObservabilityError::FeatureDisabled("otel-traces"));
    }

    if config.logs.enabled {
        #[cfg(feature = "otel-logs")]
        {
            guard.logger_provider = Some(init_logs(config)?);
        }

        #[cfg(not(feature = "otel-logs"))]
        return Err(ObservabilityError::FeatureDisabled("otel-logs"));
    }

    Ok(guard)
}

pub(crate) fn enforce_subscriber_install_policy(
    config: &ObservabilityConfig,
    status: SubscriberInstallStatus,
) -> Result<(), ObservabilityError> {
    if !subscriber_install_required(config) || status.installed {
        return Ok(());
    }

    match config.subscriber_install_policy {
        SubscriberInstallPolicy::Required => Err(ObservabilityError::subscriber_install_failed(
            status.attempted,
            status.installed,
        )),
        SubscriberInstallPolicy::BestEffort => {
            if status.attempted {
                tracing::warn!(
                    target: "rocketmq_observability",
                    attempted = status.attempted,
                    installed = status.installed,
                    "tracing subscriber was already initialized; OpenTelemetry trace/log layers were not installed"
                );
            }
            Ok(())
        }
    }
}

fn subscriber_install_required(config: &ObservabilityConfig) -> bool {
    config.traces.enabled || config.logs.enabled
}

fn validate_config(config: &ObservabilityConfig) -> Result<(), ObservabilityError> {
    if !(0.0..=1.0).contains(&config.traces.sample_ratio) {
        return Err(ObservabilityError::invalid_config(format!(
            "trace sample_ratio must be between 0.0 and 1.0, got {}",
            config.traces.sample_ratio
        )));
    }

    if config.metrics.cardinality_limit == 0 {
        return Err(ObservabilityError::invalid_config(
            "metrics cardinality_limit must be greater than 0",
        ));
    }

    if config.metrics.enabled
        && matches!(config.metrics.exporter, MetricsExporter::Prometheus)
        && config.prometheus.path.trim().is_empty()
    {
        return Err(ObservabilityError::invalid_config(
            "prometheus.path must not be empty when Prometheus metrics are enabled",
        ));
    }

    Ok(())
}

#[cfg(feature = "otel-metrics")]
struct MetricsRuntime {
    provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    #[cfg(feature = "prometheus")]
    prometheus_registry: Option<prometheus::Registry>,
}

#[cfg(feature = "otel-metrics")]
impl MetricsRuntime {
    fn new(provider: opentelemetry_sdk::metrics::SdkMeterProvider) -> Self {
        Self {
            provider,
            #[cfg(feature = "prometheus")]
            prometheus_registry: None,
        }
    }

    #[cfg(feature = "prometheus")]
    fn prometheus(provider: opentelemetry_sdk::metrics::SdkMeterProvider, registry: prometheus::Registry) -> Self {
        Self {
            provider,
            prometheus_registry: Some(registry),
        }
    }
}

#[cfg(feature = "otel-metrics")]
fn init_metrics(config: &ObservabilityConfig) -> Result<MetricsRuntime, ObservabilityError> {
    use std::time::Duration;

    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    let builder = SdkMeterProvider::builder().with_resource(crate::resource::build_resource(config));

    let runtime = match config.metrics.exporter {
        MetricsExporter::Disable => MetricsRuntime::new(builder.build()),
        MetricsExporter::Log => {
            let reader = PeriodicReader::builder(crate::exporter::stdout::StdoutExporter::default())
                .with_interval(Duration::from_millis(config.metrics.export_interval_millis))
                .build();
            MetricsRuntime::new(builder.with_reader(reader).build())
        }
        MetricsExporter::OtlpGrpc => {
            #[cfg(feature = "otlp-metrics")]
            {
                MetricsRuntime::new(crate::exporter::otlp::init_otlp_meter_provider(config)?)
            }

            #[cfg(not(feature = "otlp-metrics"))]
            {
                return Err(ObservabilityError::FeatureDisabled("otlp-metrics"));
            }
        }
        MetricsExporter::Prometheus => {
            #[cfg(feature = "prometheus")]
            {
                let prometheus = crate::exporter::prometheus::init_prometheus_metrics(config)?;
                let (provider, registry) = prometheus.into_parts();
                MetricsRuntime::prometheus(provider, registry)
            }

            #[cfg(not(feature = "prometheus"))]
            {
                return Err(ObservabilityError::FeatureDisabled("prometheus"));
            }
        }
    };

    Ok(runtime)
}

#[cfg(feature = "otel-traces")]
fn init_traces(
    config: &ObservabilityConfig,
) -> Result<opentelemetry_sdk::trace::SdkTracerProvider, ObservabilityError> {
    use opentelemetry_sdk::trace::Sampler;
    use opentelemetry_sdk::trace::SdkTracerProvider;

    let builder = SdkTracerProvider::builder()
        .with_resource(crate::resource::build_resource(config))
        .with_sampler(Sampler::TraceIdRatioBased(config.traces.sample_ratio.clamp(0.0, 1.0)));

    let provider = match config.traces.exporter {
        crate::config::TraceExporter::Disable => builder.build(),
        crate::config::TraceExporter::Log => builder
            .with_simple_exporter(crate::exporter::stdout::StdoutExporter::default())
            .build(),
        crate::config::TraceExporter::OtlpGrpc => {
            #[cfg(feature = "otlp-traces")]
            {
                crate::exporter::otlp::init_otlp_tracer_provider(config)?
            }

            #[cfg(not(feature = "otlp-traces"))]
            {
                return Err(ObservabilityError::FeatureDisabled("otlp-traces"));
            }
        }
    };

    crate::propagation::set_context_propagation_enabled(config.traces.propagate_context);
    if config.traces.propagate_context {
        crate::propagation::install_trace_context_propagators();
    }

    opentelemetry::global::set_tracer_provider(provider.clone());

    Ok(provider)
}

#[cfg(feature = "otel-logs")]
fn init_logs(config: &ObservabilityConfig) -> Result<opentelemetry_sdk::logs::SdkLoggerProvider, ObservabilityError> {
    use opentelemetry_sdk::logs::SdkLoggerProvider;

    let builder = SdkLoggerProvider::builder().with_resource(crate::resource::build_resource(config));

    let provider = match config.logs.exporter {
        LogsExporter::Disable => builder.build(),
        LogsExporter::Log => builder
            .with_simple_exporter(crate::exporter::stdout::StdoutExporter::default())
            .build(),
        LogsExporter::OtlpGrpc => {
            #[cfg(feature = "otlp-logs")]
            {
                crate::exporter::otlp::init_otlp_logger_provider(config)?
            }

            #[cfg(not(feature = "otlp-logs"))]
            {
                return Err(ObservabilityError::FeatureDisabled("otlp-logs"));
            }
        }
    };

    Ok(provider)
}

#[cfg(all(feature = "otel-traces", feature = "otel-logs"))]
fn try_init_observability_subscriber(
    config: &ObservabilityConfig,
    tracer_provider: Option<&opentelemetry_sdk::trace::SdkTracerProvider>,
    logger_provider: Option<&opentelemetry_sdk::logs::SdkLoggerProvider>,
) -> SubscriberInstallStatus {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let result = match (tracer_provider, logger_provider) {
        (Some(tracer_provider), Some(logger_provider)) => tracing_subscriber::registry()
            .with(crate::trace::build_tracing_layer(config, tracer_provider))
            .with(crate::logs::bridge::build_logs_layer(logger_provider))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_thread_ids(true)
                    .with_thread_names(true),
            )
            .try_init(),
        (Some(tracer_provider), None) => tracing_subscriber::registry()
            .with(crate::trace::build_tracing_layer(config, tracer_provider))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_thread_ids(true)
                    .with_thread_names(true),
            )
            .try_init(),
        (None, Some(logger_provider)) => tracing_subscriber::registry()
            .with(crate::logs::bridge::build_logs_layer(logger_provider))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_thread_ids(true)
                    .with_thread_names(true),
            )
            .try_init(),
        (None, None) => return SubscriberInstallStatus::default(),
    };

    SubscriberInstallStatus::attempted(log_subscriber_init_result(result))
}

#[cfg(all(feature = "otel-traces", not(feature = "otel-logs")))]
fn try_init_observability_subscriber(
    config: &ObservabilityConfig,
    tracer_provider: Option<&opentelemetry_sdk::trace::SdkTracerProvider>,
) -> SubscriberInstallStatus {
    let Some(tracer_provider) = tracer_provider else {
        return SubscriberInstallStatus::default();
    };

    let installed = crate::trace::try_init_tracing_subscriber(config, tracer_provider);
    if !installed {
        tracing::debug!(
            target: "rocketmq_observability",
            "tracing subscriber already initialized; OpenTelemetry tracing layer was not installed"
        );
    }
    SubscriberInstallStatus::attempted(installed)
}

#[cfg(all(not(feature = "otel-traces"), feature = "otel-logs"))]
fn try_init_observability_subscriber(
    logger_provider: Option<&opentelemetry_sdk::logs::SdkLoggerProvider>,
) -> SubscriberInstallStatus {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let Some(logger_provider) = logger_provider else {
        return SubscriberInstallStatus::default();
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true);

    SubscriberInstallStatus::attempted(log_subscriber_init_result(
        tracing_subscriber::registry()
            .with(crate::logs::bridge::build_logs_layer(logger_provider))
            .with(fmt_layer)
            .try_init(),
    ))
}

#[cfg(any(
    all(feature = "otel-traces", feature = "otel-logs"),
    all(not(feature = "otel-traces"), feature = "otel-logs")
))]
fn log_subscriber_init_result<E>(result: Result<(), E>) -> bool
where
    E: std::fmt::Display,
{
    match result {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(
                target: "rocketmq_observability",
                %error,
                "tracing subscriber already initialized; OpenTelemetry logs layer was not installed"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_config_returns_noop_guard() {
        let guard = init_observability(&ObservabilityConfig::default()).expect("noop init should succeed");

        assert_eq!(
            guard.subscriber_install_status(),
            SubscriberInstallStatus {
                attempted: false,
                installed: false,
            }
        );
        guard.shutdown().expect("noop shutdown should succeed");
    }

    #[test]
    fn noop_guard_accepts_an_explicit_absolute_shutdown_budget() {
        TelemetryGuard::noop()
            .shutdown_with_timeout(std::time::Duration::ZERO)
            .expect("noop shutdown should not consume a timeout budget");
    }

    #[test]
    fn invalid_sample_ratio_is_rejected() {
        let mut config = ObservabilityConfig::default();
        config.traces.sample_ratio = 1.1;

        let error = init_observability(&config).expect_err("invalid sample ratio should fail");

        assert!(matches!(error, ObservabilityError::InvalidConfig(_)));
    }

    #[test]
    fn zero_cardinality_limit_is_rejected() {
        let mut config = ObservabilityConfig::default();
        config.metrics.cardinality_limit = 0;

        let error = init_observability(&config).expect_err("zero cardinality limit should fail");

        assert!(matches!(error, ObservabilityError::InvalidConfig(_)));
    }

    #[cfg(feature = "otel-metrics")]
    #[test]
    fn disabled_metrics_exporter_initializes_meter_provider() {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = MetricsExporter::Disable;

        let guard = init_observability(&config).expect("disable metrics exporter should initialize");

        assert!(guard.meter_provider().is_some());
        guard.shutdown().expect("metrics shutdown should succeed");
    }

    #[cfg(feature = "otel-metrics")]
    #[test]
    fn log_metrics_exporter_initializes_meter_provider() {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = MetricsExporter::Log;
        config.metrics.export_interval_millis = 60_000;

        let guard = init_observability(&config).expect("log metrics exporter should initialize");

        assert!(guard.meter_provider().is_some());
        guard.shutdown().expect("metrics shutdown should succeed");
    }

    #[cfg(feature = "otel-traces")]
    #[test]
    fn disabled_trace_exporter_initializes_tracer_provider() {
        let _guard = crate::propagation::context_propagation_test_lock();
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.traces.enabled = true;
        config.traces.exporter = crate::config::TraceExporter::Disable;

        let guard = init_observability(&config).expect("disable trace exporter should initialize");

        assert!(guard.tracer_provider().is_some());
        guard.shutdown().expect("trace shutdown should succeed");
    }

    #[cfg(feature = "otel-traces")]
    #[test]
    fn log_trace_exporter_initializes_tracer_provider() {
        let _guard = crate::propagation::context_propagation_test_lock();
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.traces.enabled = true;
        config.traces.exporter = crate::config::TraceExporter::Log;

        let guard = init_observability(&config).expect("log trace exporter should initialize");

        assert!(guard.tracer_provider().is_some());
        guard.shutdown().expect("trace shutdown should succeed");
    }

    #[cfg(feature = "otel-traces")]
    #[test]
    fn trace_propagation_config_updates_global_switch() {
        let _guard = crate::propagation::context_propagation_test_lock();
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.traces.enabled = true;
        config.traces.exporter = crate::config::TraceExporter::Disable;
        config.traces.propagate_context = false;

        let guard = init_observability(&config).expect("trace init should honor propagation config");

        assert!(!crate::propagation::is_context_propagation_enabled());

        guard.shutdown().expect("trace shutdown should succeed");
        crate::propagation::set_context_propagation_enabled(true);
    }

    #[cfg(feature = "otel-logs")]
    #[test]
    fn disabled_log_exporter_initializes_logger_provider() {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.logs.enabled = true;
        config.logs.exporter = crate::config::LogsExporter::Disable;

        let guard = init_observability(&config).expect("disable log exporter should initialize");

        assert!(guard.logger_provider().is_some());
        guard.shutdown().expect("logs shutdown should succeed");
    }

    #[cfg(feature = "otel-logs")]
    #[test]
    fn log_exporter_initializes_logger_provider() {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.logs.enabled = true;
        config.logs.exporter = crate::config::LogsExporter::Log;

        let guard = init_observability(&config).expect("log exporter should initialize");

        assert!(guard.logger_provider().is_some());
        guard.shutdown().expect("logs shutdown should succeed");
    }
}
