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

mod attributes;
mod config;
mod environment;
mod error;
mod exporter;
mod exporter_types;
mod handle;
mod init;
mod log_filter;
mod logging;
pub mod logs;
pub mod metrics;
mod noop;
mod propagation;
mod resource;
mod runtime_diagnostics;
mod sampling;
pub mod semantic;
pub mod statistics;
pub mod stats;
mod status;
pub mod trace;

pub use attributes::base_attributes;
pub use attributes::Attribute;
pub use config::ConsoleLogConfig;
pub use config::FileLogConfig;
pub use config::LogFormat;
pub use config::LogRotation;
pub use config::LoggingConfig;
pub use config::LogsExporter;
pub use config::MetricsExporter;
pub use config::NonBlockingLogConfig;
pub use config::ObservabilityConfig;
pub use config::OtlpProtocol;
pub use config::ReloadConfig;
pub use config::SubscriberInstallPolicy;
pub use config::SubscriberInstallStatus;
pub use config::TelemetryBootstrapConfig;
pub use config::TraceExporter;
pub use config::TracesConfig;
pub use environment::apply_standard_otlp_environment;
pub use environment::apply_standard_otlp_environment_values;
pub use environment::StandardOtlpEnvironmentStatus;
pub use environment::OTEL_EXPORTER_OTLP_ENDPOINT;
pub use environment::OTEL_EXPORTER_OTLP_PROTOCOL;
pub use error::ObservabilityError;
pub use exporter::outage::*;
pub use exporter_types::LogExporterType;
pub use exporter_types::MetricsExporterType;
pub use exporter_types::TraceExporterType;
#[cfg(feature = "otel-metrics")]
pub use handle::ReleaseIdentityRegistrationError;
pub use handle::TelemetryHandle;
pub use handle::TelemetryRecorder;
pub use handle::TelemetryState;
pub use handle::TracePolicy;
pub use handle::BROKER_METER_SCOPE;
pub use handle::CLIENT_METER_SCOPE;
pub use handle::CONTROLLER_METER_SCOPE;
pub use handle::MCP_METER_SCOPE;
pub use handle::NAMESRV_METER_SCOPE;
pub use handle::PROXY_METER_SCOPE;
pub use handle::STORE_METER_SCOPE;
pub use handle::TIERED_STORE_METER_SCOPE;
pub use handle::TRANSPORT_METER_SCOPE;
pub use init::init_observability;
pub use init::init_observability_with_service_context;
pub use log_filter::read_rust_log;
pub use log_filter::LogFilterHandle;
pub use log_filter::LogFilterInputs;
pub use log_filter::LogFilterReloadRequest;
pub use log_filter::LogFilterResolver;
pub use log_filter::LogFilterSource;
pub use log_filter::LoggingOverrideConfig;
pub use log_filter::LoggingOverrides;
pub use log_filter::ResolvedLogFilter;
pub use log_filter::DEFAULT_LOG_FILTER;
pub use logging::install_global;
pub use logging::install_global_with_filter;
pub use logging::install_global_with_filter_and_service_context;
pub use logging::install_global_with_service_context;
pub use logging::FileLogLayer;
pub use logging::LoggingGuard;
pub use logging::TelemetryRuntimeGuard;
pub use logging::TelemetryShutdownReport;
pub use metrics::labels::MetricLabelPolicy;
pub use metrics::labels::METRIC_LABEL_SENTINEL;
#[cfg(feature = "otel-traces")]
pub use propagation::add_current_span_event_with_status;
#[cfg(feature = "otel-traces")]
pub use propagation::extract_context_with_handle;
#[cfg(feature = "otel-traces")]
pub use propagation::inject_current_context_with_handle;
#[cfg(feature = "otel-traces")]
pub use propagation::record_span_parent_assignment_error;
#[cfg(feature = "otel-traces")]
pub use propagation::set_span_parent_from_properties_with_handle;
pub use propagation::TRACEPARENT;
pub use propagation::TRACESTATE;
pub use runtime_diagnostics::start_runtime_diagnostics_endpoint;
pub use runtime_diagnostics::start_runtime_diagnostics_endpoint_from_env;
pub use runtime_diagnostics::RuntimeDiagnosticsEndpointConfig;
pub use runtime_diagnostics::RuntimeDiagnosticsEndpointHandle;
pub use runtime_diagnostics::RUNTIME_DIAGNOSTICS_ALLOW_INSECURE_HTTP_ENV;
pub use runtime_diagnostics::RUNTIME_DIAGNOSTICS_BIND_ADDR_ENV;
pub use runtime_diagnostics::RUNTIME_DIAGNOSTICS_ENDPOINT_SCHEMA;
pub use runtime_diagnostics::RUNTIME_DIAGNOSTICS_PATH;
pub use runtime_diagnostics::RUNTIME_DIAGNOSTICS_SAMPLE_INTERVAL_SECONDS_ENV;
pub use runtime_diagnostics::RUNTIME_DIAGNOSTICS_SCOPE;
pub use runtime_diagnostics::RUNTIME_DIAGNOSTICS_TOKEN_FILE_ENV;
pub use sampling::SamplingGate;
pub use status::ObservabilityCompiledFeaturesV1;
pub use status::ObservabilitySignalStatusV1;
pub use status::ObservabilityStatusHandle;
pub use status::ObservabilityStatusValueV1;
pub use status::ObservabilityStatusViewV1;
pub use status::ObservabilitySubscriberStatusV1;

#[cfg(feature = "prometheus")]
#[doc(hidden)]
pub mod bench_support {
    use std::time::Duration;
    use std::time::Instant;

    use opentelemetry::metrics::MeterProvider;
    use rocketmq_runtime::ChildServiceContext;
    use rocketmq_runtime::ShutdownReport;
    use serde::Serialize;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    use crate::config::MetricsExporter;
    use crate::config::ObservabilityConfig;

    #[derive(Clone, Debug, Serialize)]
    pub struct PrometheusLifecycleProbe {
        pub task_count_before_scrape: usize,
        pub task_count_before_shutdown: usize,
        pub response_status_ok: bool,
        pub response_contains_metric: bool,
        pub response_bytes: usize,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: ShutdownReport,
        pub healthy: bool,
    }

    pub async fn run_prometheus_lifecycle_probe(service_context: ChildServiceContext) -> PrometheusLifecycleProbe {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = MetricsExporter::Prometheus;
        config.prometheus.host = "127.0.0.1".to_string();
        config.prometheus.port = 0;
        config.prometheus.path = "/metrics".to_string();

        let prometheus = crate::exporter::prometheus::init_prometheus_metrics(&config)
            .expect("prometheus metrics should initialize");
        let meter = prometheus.provider().meter("observability-lifecycle-bench");
        let counter = meter.u64_counter("rocketmq_observability_lifecycle_total").build();
        counter.add(1, &[]);

        let handle = crate::exporter::prometheus::spawn_prometheus_http_endpoint_with_task_group(
            &config,
            prometheus.registry().clone(),
            service_context.task_group().clone(),
        )
        .expect("prometheus HTTP endpoint should start");
        let task_count_before_scrape = handle.task_count();

        let response = scrape_metrics(handle.local_addr(), config.prometheus.path.as_str()).await;
        let response_status_ok = response.starts_with("HTTP/1.1 200 OK");
        let response_contains_metric = response.contains("rocketmq_observability_lifecycle_total");
        let response_bytes = response.len();
        let task_count_before_shutdown = handle.task_count();

        let shutdown_started_at = Instant::now();
        let shutdown_report = handle.shutdown_gracefully(Duration::from_secs(5)).await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let parent_report = service_context.task_group().shutdown(Duration::from_secs(1)).await;
        let finished_accept_tasks = shutdown_report.completed + shutdown_report.cancelled;
        let healthy = response_status_ok
            && response_contains_metric
            && shutdown_report.is_healthy()
            && parent_report.is_healthy()
            && finished_accept_tasks >= 1
            && shutdown_report.leaked == 0
            && shutdown_report.timed_out == 0
            && shutdown_report.detached_still_running == 0;

        PrometheusLifecycleProbe {
            task_count_before_scrape,
            task_count_before_shutdown,
            response_status_ok,
            response_contains_metric,
            response_bytes,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    async fn scrape_metrics(addr: std::net::SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr)
            .await
            .expect("prometheus scrape should connect");
        let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("prometheus scrape request should write");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("prometheus scrape response should read");
        String::from_utf8(response).expect("prometheus response should be utf8")
    }
}

#[cfg(all(test, feature = "prometheus"))]
mod bench_support_tests {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prometheus_lifecycle_probe_reports_clean_shutdown() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("prometheus-lifecycle-test");
        let probe =
            super::bench_support::run_prometheus_lifecycle_probe(runtime.service_context("prometheus-probe")).await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.response_status_ok, "{probe:?}");
        assert!(
            probe.shutdown_report.is_healthy(),
            "{}",
            probe.shutdown_report.to_json()
        );
    }
}
