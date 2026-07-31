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

#[cfg(feature = "prometheus")]
use std::net::SocketAddr;
#[cfg(feature = "prometheus")]
use std::time::Duration;

#[cfg(feature = "prometheus")]
use opentelemetry_sdk::metrics::SdkMeterProvider;
#[cfg(feature = "prometheus")]
use rocketmq_runtime::ChildServiceContext;
#[cfg(feature = "prometheus")]
use rocketmq_runtime::OperationContext;
#[cfg(feature = "prometheus")]
use rocketmq_runtime::ShutdownReport;
#[cfg(feature = "prometheus")]
use rocketmq_runtime::TaskGroup;
#[cfg(feature = "prometheus")]
use rocketmq_runtime::TaskKind;

#[cfg(feature = "prometheus")]
use crate::config::ObservabilityConfig;
#[cfg(feature = "prometheus")]
use crate::error::ObservabilityError;

#[cfg(feature = "prometheus")]
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[cfg(feature = "prometheus")]
#[derive(Clone)]
pub(crate) struct PrometheusMetrics {
    provider: SdkMeterProvider,
    registry: prometheus::Registry,
}

#[cfg(feature = "prometheus")]
impl PrometheusMetrics {
    pub(crate) fn provider(&self) -> &SdkMeterProvider {
        &self.provider
    }

    pub(crate) fn registry(&self) -> &prometheus::Registry {
        &self.registry
    }

    pub(crate) fn into_parts(self) -> (SdkMeterProvider, prometheus::Registry) {
        (self.provider, self.registry)
    }
}

#[cfg(feature = "prometheus")]
#[derive(Debug)]
pub(crate) struct PrometheusHttpHandle {
    local_addr: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task_group: TaskGroup,
    operation: OperationContext,
}

#[cfg(feature = "prometheus")]
impl PrometheusHttpHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn task_count(&self) -> usize {
        self.operation.active_task_count()
    }

    pub(crate) async fn shutdown_gracefully(mut self, timeout: Duration) -> ShutdownReport {
        self.signal_shutdown();
        self.operation.close_admission();
        let _ = self.operation.wait(&self.task_group, timeout).await;
        self.task_group.shutdown(timeout).await
    }

    fn signal_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[cfg(feature = "prometheus")]
impl Drop for PrometheusHttpHandle {
    fn drop(&mut self) {
        // Explicit graceful shutdown owns the join and report. Drop is only a fail-safe that
        // prevents an endpoint from accepting new scrapes after startup rollback or unwinding.
        self.signal_shutdown();
        self.operation.cancel();
        self.task_group.cancel();
    }
}

#[cfg(feature = "prometheus")]
pub(crate) fn init_prometheus_metrics(config: &ObservabilityConfig) -> Result<PrometheusMetrics, ObservabilityError> {
    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_units()
        .without_counter_suffixes()
        .build()
        .map_err(ObservabilityError::metrics_init)?;

    let provider = SdkMeterProvider::builder()
        .with_resource(crate::resource::build_resource(config))
        .with_reader(exporter)
        .build();

    Ok(PrometheusMetrics { provider, registry })
}

#[cfg(feature = "prometheus")]
pub(crate) fn render_prometheus_metrics(registry: &prometheus::Registry) -> Result<String, ObservabilityError> {
    use prometheus::Encoder;

    let encoder = prometheus::TextEncoder::new();
    let metric_families = registry.gather();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(ObservabilityError::metrics_init)?;
    String::from_utf8(buffer).map_err(ObservabilityError::metrics_init)
}

#[cfg(feature = "prometheus")]
pub(crate) fn spawn_prometheus_http_endpoint(
    config: &ObservabilityConfig,
    registry: prometheus::Registry,
    service_context: &ChildServiceContext,
) -> Result<PrometheusHttpHandle, ObservabilityError> {
    let path = normalize_prometheus_path(&config.prometheus.path)?;
    let listener = bind_prometheus_listener(config)?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| ObservabilityError::metrics_init(format!("read Prometheus listener address: {error}")))?;
    let listener = tokio::net::TcpListener::from_std(listener)
        .map_err(|error| ObservabilityError::metrics_init(format!("create Prometheus listener: {error}")))?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let endpoint_context = service_context.component("rocketmq.observability.prometheus");
    let task_group = endpoint_context.task_group().clone();
    let operation = OperationContext::without_deadline(TaskKind::Service);
    let endpoint_operation = operation.clone();
    task_group
        .spawn_operation(
            &operation,
            "prometheus.accept",
            run_prometheus_http_endpoint(
                listener,
                registry,
                path,
                shutdown_rx,
                task_group.clone(),
                endpoint_operation,
            ),
        )
        .map_err(|error| ObservabilityError::metrics_init(format!("spawn Prometheus endpoint: {error}")))?;

    Ok(PrometheusHttpHandle {
        local_addr,
        shutdown: Some(shutdown_tx),
        task_group,
        operation,
    })
}

#[cfg(feature = "prometheus")]
fn bind_prometheus_listener(config: &ObservabilityConfig) -> Result<std::net::TcpListener, ObservabilityError> {
    let host = if config.prometheus.host.trim().is_empty() {
        "0.0.0.0"
    } else {
        config.prometheus.host.trim()
    };
    let bind_addr = format!("{host}:{}", config.prometheus.port);
    let listener = std::net::TcpListener::bind(&bind_addr).map_err(|error| {
        ObservabilityError::metrics_init(format!("bind Prometheus metrics endpoint {bind_addr}: {error}"))
    })?;
    listener
        .set_nonblocking(true)
        .map_err(|error| ObservabilityError::metrics_init(format!("set Prometheus listener nonblocking: {error}")))?;
    Ok(listener)
}

#[cfg(feature = "prometheus")]
fn normalize_prometheus_path(path: &str) -> Result<String, ObservabilityError> {
    let path = path.trim();
    if path.is_empty() {
        return Err(ObservabilityError::invalid_config(
            "prometheus.path must not be empty when Prometheus metrics are enabled",
        ));
    }
    if path.starts_with('/') {
        Ok(path.to_string())
    } else {
        Ok(format!("/{path}"))
    }
}

#[cfg(feature = "prometheus")]
async fn run_prometheus_http_endpoint(
    listener: tokio::net::TcpListener,
    registry: prometheus::Registry,
    path: String,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
    task_group: TaskGroup,
    operation: OperationContext,
) {
    let endpoint_cancellation = operation.cancellation_token();
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = endpoint_cancellation.cancelled() => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer_addr)) => {
                        let registry = registry.clone();
                        let path = path.clone();
                        let cancellation = operation.cancellation_token();
                        let connection_operation = operation.with_task_kind(TaskKind::Worker);
                        if let Err(error) = task_group.spawn_operation(&connection_operation, "prometheus.connection", async move {
                                tokio::select! {
                                    _ = cancellation.cancelled() => {}
                                    result = handle_prometheus_connection(stream, registry, path) => {
                                        if let Err(error) = result {
                                            tracing::debug!(%error, "failed to serve Prometheus metrics scrape");
                                        }
                                    }
                                }
                            }) {
                            tracing::debug!(%error, "failed to track Prometheus metrics scrape task");
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%error, "failed to accept Prometheus metrics scrape");
                    }
                }
            }
        }
    }
}

#[cfg(feature = "prometheus")]
async fn handle_prometheus_connection(
    mut stream: tokio::net::TcpStream,
    registry: prometheus::Registry,
    path: String,
) -> Result<(), std::io::Error> {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let mut buffer = [0_u8; 2048];
    let bytes_read = stream.read(&mut buffer).await?;
    let request_path = request_path_from_buffer(&buffer[..bytes_read]);
    let (status, content_type, body) = match request_path {
        Some(request_path) if request_path == path => match render_prometheus_metrics(&registry) {
            Ok(body) => ("200 OK", PROMETHEUS_CONTENT_TYPE, body),
            Err(error) => (
                "500 Internal Server Error",
                "text/plain; charset=utf-8",
                error.to_string(),
            ),
        },
        _ => ("404 Not Found", "text/plain; charset=utf-8", "not found\n".to_string()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(feature = "prometheus")]
fn request_path_from_buffer(buffer: &[u8]) -> Option<&str> {
    let request = std::str::from_utf8(buffer).ok()?;
    let mut parts = request.lines().next()?.split_whitespace();
    match parts.next()? {
        "GET" => {}
        _ => return None,
    }
    let target = parts.next()?;
    Some(target.split('?').next().unwrap_or(target))
}

#[cfg(all(test, feature = "prometheus"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use rocketmq_runtime::RuntimeContext;

    use super::*;

    #[test]
    fn renders_recorded_metrics() {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = crate::config::MetricsExporter::Prometheus;

        let prometheus = init_prometheus_metrics(&config).expect("prometheus metrics should initialize");
        let meter = prometheus.provider().meter("prometheus-render-test");
        let counter = meter.u64_counter("rocketmq_messages_in_total").build();

        counter.add(7, &[]);

        let rendered = render_prometheus_metrics(prometheus.registry()).expect("metrics should render");

        assert!(rendered.contains("rocketmq_messages_in_total"));
        assert!(rendered.contains(" 7"));
    }

    #[test]
    fn request_path_accepts_query_string() {
        let request = b"GET /metrics?target=broker HTTP/1.1\r\nHost: localhost\r\n\r\n";

        assert_eq!(request_path_from_buffer(request), Some("/metrics"));
    }

    #[test]
    fn normalizes_prometheus_path() {
        assert_eq!(normalize_prometheus_path("metrics").unwrap(), "/metrics");
        assert_eq!(normalize_prometheus_path("/metrics").unwrap(), "/metrics");
    }

    #[tokio::test]
    async fn spawn_prometheus_http_endpoint_uses_component_context() {
        let context = RuntimeContext::from_current("prometheus-parent-test");
        let service = context.service_context("observability-service");
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.prometheus.host = "127.0.0.1".to_string();
        config.prometheus.port = 0;
        config.prometheus.path = "/metrics".to_string();
        let registry = prometheus::Registry::new();

        let handle =
            spawn_prometheus_http_endpoint(&config, registry, &service).expect("prometheus endpoint should spawn");
        let report = handle.shutdown_gracefully(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());

        let parent_report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(parent_report.is_healthy(), "{}", parent_report.to_json());
        assert!(
            parent_report
                .children
                .iter()
                .any(|child| child.name == "rocketmq.observability.prometheus"),
            "{}",
            parent_report.to_json()
        );
    }
}
