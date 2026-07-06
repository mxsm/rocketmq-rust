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

pub mod attributes;
pub mod config;
pub mod error;
pub mod exporter;
pub mod init;
pub mod logs;
pub mod metrics;
pub mod noop;
pub mod propagation;
pub mod resource;
pub mod sampling;
pub mod semantic;
pub mod trace;

pub use config::ObservabilityConfig;
pub use config::SubscriberInstallPolicy;
pub use config::SubscriberInstallStatus;
pub use error::ObservabilityError;
pub use init::init_observability;
#[cfg(feature = "otel-metrics")]
pub use init::meter;
pub use init::TelemetryGuard;

#[cfg(feature = "prometheus")]
#[doc(hidden)]
pub mod bench_support {
    use std::time::Duration;
    use std::time::Instant;

    use opentelemetry::metrics::MeterProvider;
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

    pub async fn run_prometheus_lifecycle_probe() -> PrometheusLifecycleProbe {
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

        let handle =
            crate::exporter::prometheus::spawn_prometheus_http_endpoint(&config, prometheus.registry().clone())
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
        let finished_accept_tasks = shutdown_report.completed + shutdown_report.cancelled;
        let healthy = response_status_ok
            && response_contains_metric
            && shutdown_report.is_healthy()
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
        let probe = super::bench_support::run_prometheus_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.response_status_ok, "{probe:?}");
        assert!(
            probe.shutdown_report.is_healthy(),
            "{}",
            probe.shutdown_report.to_json()
        );
    }
}
