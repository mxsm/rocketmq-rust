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

#[cfg(not(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs")))]
#[derive(Debug, Clone, Copy, Default)]
pub struct StdoutExporter;

#[cfg(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs"))]
#[derive(Debug, Default)]
pub struct StdoutExporter {
    shutdown: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "otel-metrics")]
impl StdoutExporter {
    fn data_point_count(metrics: &opentelemetry_sdk::metrics::data::AggregatedMetrics) -> usize {
        use opentelemetry_sdk::metrics::data::AggregatedMetrics;

        match metrics {
            AggregatedMetrics::F64(data) => Self::typed_data_point_count(data),
            AggregatedMetrics::U64(data) => Self::typed_data_point_count(data),
            AggregatedMetrics::I64(data) => Self::typed_data_point_count(data),
        }
    }

    fn typed_data_point_count<T>(data: &opentelemetry_sdk::metrics::data::MetricData<T>) -> usize {
        use opentelemetry_sdk::metrics::data::MetricData;

        match data {
            MetricData::Gauge(data) => data.data_points().count(),
            MetricData::Sum(data) => data.data_points().count(),
            MetricData::Histogram(data) => data.data_points().count(),
            MetricData::ExponentialHistogram(data) => data.data_points().count(),
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl opentelemetry_sdk::metrics::exporter::PushMetricExporter for StdoutExporter {
    fn export(
        &self,
        metrics: &opentelemetry_sdk::metrics::data::ResourceMetrics,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        let is_shutdown = self.shutdown.load(std::sync::atomic::Ordering::Relaxed);
        let scope_count = metrics.scope_metrics().count();
        let mut metric_count = 0;
        let mut data_point_count = 0;
        let mut metric_names = Vec::new();

        for scope_metrics in metrics.scope_metrics() {
            for metric in scope_metrics.metrics() {
                metric_count += 1;
                data_point_count += Self::data_point_count(metric.data());
                metric_names.push(metric.name().to_string());
            }
        }

        async move {
            if is_shutdown {
                return Err(opentelemetry_sdk::error::OTelSdkError::AlreadyShutdown);
            }

            tracing::info!(
                target: "rocketmq_observability",
                scope_count,
                metric_count,
                data_point_count,
                metric_names = %metric_names.join(","),
                "exported metrics to log"
            );

            Ok(())
        }
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
        opentelemetry_sdk::metrics::Temporality::Cumulative
    }
}

#[cfg(feature = "otel-traces")]
impl opentelemetry_sdk::trace::SpanExporter for StdoutExporter {
    async fn export(&self, batch: Vec<opentelemetry_sdk::trace::SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(opentelemetry_sdk::error::OTelSdkError::AlreadyShutdown);
        }

        let span_names = batch
            .iter()
            .map(|span| span.name.as_ref())
            .collect::<Vec<_>>()
            .join(",");
        tracing::info!(
            target: "rocketmq_observability",
            span_count = batch.len(),
            span_names,
            "exported spans to log"
        );

        Ok(())
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(feature = "otel-logs")]
impl opentelemetry_sdk::logs::LogExporter for StdoutExporter {
    fn export(
        &self,
        batch: opentelemetry_sdk::logs::LogBatch<'_>,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        let is_shutdown = self.shutdown.load(std::sync::atomic::Ordering::Relaxed);
        let mut log_count = 0usize;
        let mut event_names = Vec::new();
        let mut targets = Vec::new();

        for (record, scope) in batch.iter() {
            log_count += 1;
            if let Some(event_name) = record.event_name() {
                event_names.push(event_name);
            }
            if let Some(target) = record.target() {
                targets.push(target.as_ref());
            } else if !scope.name().is_empty() {
                targets.push(scope.name());
            }
        }

        let event_names = event_names.join(",");
        let targets = targets.join(",");

        async move {
            if is_shutdown {
                return Err(opentelemetry_sdk::error::OTelSdkError::AlreadyShutdown);
            }

            println!(
                "rocketmq_observability exported logs to log log_count={log_count} event_names={event_names} \
                 targets={targets}"
            );

            Ok(())
        }
    }

    fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}
