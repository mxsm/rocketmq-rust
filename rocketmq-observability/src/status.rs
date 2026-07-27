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

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;

use crate::config::LogsExporter;
use crate::config::MetricsExporter;
use crate::config::ObservabilityConfig;
use crate::config::SubscriberInstallStatus;
use crate::config::TraceExporter;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservabilityStatusValueV1 {
    Initializing,
    Ready,
    Disabled,
    Unknown,
    NotInstrumented,
    InProgress,
    Shutdown,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservabilityCompiledFeaturesV1 {
    pub metrics: bool,
    pub traces: bool,
    pub logs: bool,
    pub otlp_grpc: bool,
    pub prometheus: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservabilitySignalStatusV1 {
    pub enabled: bool,
    pub exporter: String,
    pub initialization: ObservabilityStatusValueV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservabilitySubscriberStatusV1 {
    pub attempted: bool,
    pub installed: bool,
}

/// Authenticated, sanitized status for an initialized observability runtime.
///
/// Export, queue, and drop health are deliberately reported as `unknown` or
/// `not_instrumented` until the exporter SDK exposes authoritative state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservabilityStatusViewV1 {
    pub schema_version: String,
    pub observed_at: DateTime<Utc>,
    pub enabled: bool,
    pub compiled_features: ObservabilityCompiledFeaturesV1,
    pub metrics: ObservabilitySignalStatusV1,
    pub traces: ObservabilitySignalStatusV1,
    pub logs: ObservabilitySignalStatusV1,
    pub otlp_protocol: String,
    pub otlp_endpoint: Option<String>,
    pub subscriber: ObservabilitySubscriberStatusV1,
    pub initialization: ObservabilityStatusValueV1,
    pub export: ObservabilityStatusValueV1,
    pub queue: ObservabilityStatusValueV1,
    pub drops: ObservabilityStatusValueV1,
    pub shutdown: ObservabilityStatusValueV1,
}

impl ObservabilityStatusViewV1 {
    pub const SCHEMA_VERSION: &'static str = "rocketmq.observability-status.v1";
}

#[derive(Debug, Clone)]
pub struct ObservabilityStatusHandle {
    inner: Arc<RwLock<ObservabilityStatusState>>,
}

#[derive(Debug, Clone)]
struct ObservabilityStatusState {
    enabled: bool,
    metrics: ObservabilitySignalStatusV1,
    traces: ObservabilitySignalStatusV1,
    logs: ObservabilitySignalStatusV1,
    otlp_protocol: String,
    otlp_endpoint: Option<String>,
    subscriber: ObservabilitySubscriberStatusV1,
    initialization: ObservabilityStatusValueV1,
    export: ObservabilityStatusValueV1,
    queue: ObservabilityStatusValueV1,
    drops: ObservabilityStatusValueV1,
    shutdown: ObservabilityStatusValueV1,
}

impl Default for ObservabilityStatusHandle {
    fn default() -> Self {
        Self::from_config(&ObservabilityConfig::default())
    }
}

impl ObservabilityStatusHandle {
    pub(crate) fn from_config(config: &ObservabilityConfig) -> Self {
        let initialization = if config.enabled {
            ObservabilityStatusValueV1::Initializing
        } else {
            ObservabilityStatusValueV1::Disabled
        };
        let export = if config.enabled {
            ObservabilityStatusValueV1::Unknown
        } else {
            ObservabilityStatusValueV1::Disabled
        };
        Self {
            inner: Arc::new(RwLock::new(ObservabilityStatusState {
                enabled: config.enabled,
                metrics: metric_status(config),
                traces: trace_status(config),
                logs: log_status(config),
                otlp_protocol: otlp_protocol(config),
                otlp_endpoint: uses_otlp(config).then(|| sanitize_endpoint(config.otlp.endpoint.as_str())),
                subscriber: ObservabilitySubscriberStatusV1 {
                    attempted: false,
                    installed: false,
                },
                initialization,
                export,
                queue: status_when_enabled(config, ObservabilityStatusValueV1::NotInstrumented),
                drops: status_when_enabled(config, ObservabilityStatusValueV1::NotInstrumented),
                shutdown: status_when_enabled(config, ObservabilityStatusValueV1::Unknown),
            })),
        }
    }

    pub fn view(&self) -> ObservabilityStatusViewV1 {
        let state = self.inner.read().clone();
        ObservabilityStatusViewV1 {
            schema_version: ObservabilityStatusViewV1::SCHEMA_VERSION.to_string(),
            observed_at: Utc::now(),
            enabled: state.enabled,
            compiled_features: compiled_features(),
            metrics: state.metrics,
            traces: state.traces,
            logs: state.logs,
            otlp_protocol: state.otlp_protocol,
            otlp_endpoint: state.otlp_endpoint,
            subscriber: state.subscriber,
            initialization: state.initialization,
            export: state.export,
            queue: state.queue,
            drops: state.drops,
            shutdown: state.shutdown,
        }
    }

    pub(crate) fn mark_initialized(&self, subscriber: SubscriberInstallStatus) {
        let mut state = self.inner.write();
        state.subscriber = ObservabilitySubscriberStatusV1 {
            attempted: subscriber.attempted,
            installed: subscriber.installed,
        };
        state.initialization = if state.enabled {
            ObservabilityStatusValueV1::Ready
        } else {
            ObservabilityStatusValueV1::Disabled
        };
        mark_signal_initialized(&mut state.metrics);
        mark_signal_initialized(&mut state.traces);
        mark_signal_initialized(&mut state.logs);
        state.export = if !state.enabled || !any_signal_enabled(&state) {
            ObservabilityStatusValueV1::Disabled
        } else if uses_remote_exporter(&state) {
            ObservabilityStatusValueV1::Unknown
        } else {
            ObservabilityStatusValueV1::Ready
        };
    }

    pub(crate) fn mark_shutdown_started(&self) {
        self.inner.write().shutdown = ObservabilityStatusValueV1::InProgress;
    }

    pub(crate) fn mark_shutdown_finished(&self, healthy: bool) {
        self.inner.write().shutdown = if healthy {
            ObservabilityStatusValueV1::Shutdown
        } else {
            ObservabilityStatusValueV1::Failed
        };
    }
}

fn mark_signal_initialized(signal: &mut ObservabilitySignalStatusV1) {
    if signal.enabled && signal.initialization != ObservabilityStatusValueV1::NotInstrumented {
        signal.initialization = ObservabilityStatusValueV1::Ready;
    }
}

fn metric_status(config: &ObservabilityConfig) -> ObservabilitySignalStatusV1 {
    signal_status(
        config.enabled && config.metrics.enabled,
        match config.metrics.exporter {
            MetricsExporter::Disable => "disabled",
            MetricsExporter::OtlpGrpc => "otlp_grpc",
            MetricsExporter::Prometheus => "prometheus",
            MetricsExporter::Log => "log",
        },
        cfg!(feature = "otel-metrics"),
    )
}

fn trace_status(config: &ObservabilityConfig) -> ObservabilitySignalStatusV1 {
    signal_status(
        config.enabled && config.traces.enabled,
        match config.traces.exporter {
            TraceExporter::Disable => "disabled",
            TraceExporter::OtlpGrpc => "otlp_grpc",
            TraceExporter::Log => "log",
        },
        cfg!(feature = "otel-traces"),
    )
}

fn log_status(config: &ObservabilityConfig) -> ObservabilitySignalStatusV1 {
    signal_status(
        config.enabled && config.logs.enabled,
        match config.logs.exporter {
            LogsExporter::Disable => "disabled",
            LogsExporter::OtlpGrpc => "otlp_grpc",
            LogsExporter::Log => "log",
        },
        cfg!(feature = "otel-logs"),
    )
}

fn signal_status(enabled: bool, exporter: &str, compiled: bool) -> ObservabilitySignalStatusV1 {
    ObservabilitySignalStatusV1 {
        enabled,
        exporter: exporter.to_string(),
        initialization: if !enabled {
            ObservabilityStatusValueV1::Disabled
        } else if compiled {
            ObservabilityStatusValueV1::Initializing
        } else {
            ObservabilityStatusValueV1::NotInstrumented
        },
    }
}

fn uses_remote_exporter(state: &ObservabilityStatusState) -> bool {
    [&state.metrics, &state.traces, &state.logs]
        .into_iter()
        .any(|signal| signal.enabled && signal.exporter == "otlp_grpc")
}

fn any_signal_enabled(state: &ObservabilityStatusState) -> bool {
    state.metrics.enabled || state.traces.enabled || state.logs.enabled
}

fn status_when_enabled(
    config: &ObservabilityConfig,
    enabled_status: ObservabilityStatusValueV1,
) -> ObservabilityStatusValueV1 {
    if config.enabled {
        enabled_status
    } else {
        ObservabilityStatusValueV1::Disabled
    }
}

pub(crate) fn uses_otlp(config: &ObservabilityConfig) -> bool {
    config.enabled
        && ((config.metrics.enabled && config.metrics.exporter == MetricsExporter::OtlpGrpc)
            || (config.traces.enabled && config.traces.exporter == TraceExporter::OtlpGrpc)
            || (config.logs.enabled && config.logs.exporter == LogsExporter::OtlpGrpc))
}

fn otlp_protocol(config: &ObservabilityConfig) -> String {
    match config.otlp.protocol {
        crate::config::OtlpProtocol::Grpc => "grpc",
        crate::config::OtlpProtocol::HttpBinary => "http_binary",
        crate::config::OtlpProtocol::HttpJson => "http_json",
    }
    .to_string()
}

fn sanitize_endpoint(endpoint: &str) -> String {
    let Some((scheme, _rest)) = endpoint.trim().split_once("://") else {
        return "<redacted>".to_string();
    };
    match scheme {
        "http" | "https" | "grpc" => format!("{scheme}://<redacted>"),
        _ => "<redacted>".to_string(),
    }
}

fn compiled_features() -> ObservabilityCompiledFeaturesV1 {
    ObservabilityCompiledFeaturesV1 {
        metrics: cfg!(feature = "otel-metrics"),
        traces: cfg!(feature = "otel-traces"),
        logs: cfg!(feature = "otel-logs"),
        otlp_grpc: cfg!(feature = "otlp-grpc"),
        prometheus: cfg!(feature = "prometheus"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_status_does_not_disclose_authority_or_credentials() {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = MetricsExporter::OtlpGrpc;
        config.otlp.endpoint = "https://user:secret@collector.internal:4317/v1/metrics".to_string();

        let view = ObservabilityStatusHandle::from_config(&config).view();
        let json = serde_json::to_string(&view).expect("status should serialize");

        assert_eq!(view.otlp_endpoint.as_deref(), Some("https://<redacted>"));
        assert!(!json.contains("secret"));
        assert!(!json.contains("collector.internal"));
    }

    #[test]
    fn unavailable_exporter_health_is_reported_honestly() {
        let mut config = ObservabilityConfig {
            enabled: true,
            ..ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = MetricsExporter::OtlpGrpc;
        let handle = ObservabilityStatusHandle::from_config(&config);
        handle.mark_initialized(SubscriberInstallStatus::default());

        let view = handle.view();
        assert_eq!(view.initialization, ObservabilityStatusValueV1::Ready);
        assert_eq!(view.export, ObservabilityStatusValueV1::Unknown);
        assert_eq!(view.queue, ObservabilityStatusValueV1::NotInstrumented);
        assert_eq!(view.drops, ObservabilityStatusValueV1::NotInstrumented);
    }

    #[test]
    fn global_disable_does_not_report_signal_initialization_or_export_health() {
        let mut config = ObservabilityConfig::default();
        config.metrics.enabled = true;
        config.metrics.exporter = MetricsExporter::OtlpGrpc;
        let handle = ObservabilityStatusHandle::from_config(&config);
        handle.mark_initialized(SubscriberInstallStatus::default());

        let view = handle.view();

        assert_eq!(view.initialization, ObservabilityStatusValueV1::Disabled);
        assert_eq!(view.metrics.initialization, ObservabilityStatusValueV1::Disabled);
        assert_eq!(view.export, ObservabilityStatusValueV1::Disabled);
        assert_eq!(view.queue, ObservabilityStatusValueV1::Disabled);
        assert_eq!(view.drops, ObservabilityStatusValueV1::Disabled);
        assert_eq!(view.shutdown, ObservabilityStatusValueV1::Disabled);
        assert_eq!(view.otlp_endpoint, None);
    }
}
