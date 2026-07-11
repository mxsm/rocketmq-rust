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

use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    pub enabled: bool,
    pub service_name: String,
    pub service_namespace: String,
    pub service_instance_id: String,
    pub service_version: String,
    pub environment: String,
    pub cluster: String,
    pub node_type: String,
    pub node_id: String,
    pub metrics: MetricsConfig,
    pub traces: TracesConfig,
    pub logs: LogsConfig,
    pub otlp: OtlpConfig,
    pub prometheus: PrometheusConfig,
    pub subscriber_install_policy: SubscriberInstallPolicy,
    pub resource_attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetryBootstrapConfig {
    pub observability: ObservabilityConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub enabled: bool,
    pub filter: String,
    pub console: ConsoleLogConfig,
    pub file: FileLogConfig,
    pub reload: ReloadConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConsoleLogConfig {
    pub enabled: bool,
    pub format: LogFormat,
    pub ansi: bool,
    pub target: bool,
    pub thread_ids: bool,
    pub thread_names: bool,
    pub line_number: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct FileLogConfig {
    pub enabled: bool,
    pub directory: String,
    pub file_name_prefix: String,
    pub rotation: LogRotation,
    pub format: LogFormat,
    pub non_blocking: NonBlockingLogConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NonBlockingLogConfig {
    pub buffered_lines_limit: usize,
    pub lossy: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReloadConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub exporter: MetricsExporter,
    pub export_interval_millis: u64,
    pub export_timeout_millis: u64,
    pub cardinality_limit: usize,
    pub sample_ratio: f64,
    pub topic_label_enabled: bool,
    pub consumer_group_label_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TracesConfig {
    pub enabled: bool,
    pub exporter: TraceExporter,
    pub sample_ratio: f64,
    pub propagate_context: bool,
    pub record_message_id: bool,
    pub record_message_keys: bool,
    pub record_body_size: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LogsConfig {
    pub enabled: bool,
    pub exporter: LogsExporter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OtlpConfig {
    pub endpoint: String,
    pub protocol: OtlpProtocol,
    pub headers: HashMap<String, String>,
    pub timeout_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrometheusConfig {
    pub host: String,
    pub port: u16,
    pub path: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricsExporter {
    Disable,
    OtlpGrpc,
    Prometheus,
    Log,
}

impl MetricsExporter {
    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::Disable)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TraceExporter {
    Disable,
    OtlpGrpc,
    Log,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogsExporter {
    Disable,
    OtlpGrpc,
    Log,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    Grpc,
    HttpBinary,
    HttpJson,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Compact,
    Pretty,
    Json,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogRotation {
    Never,
    Hourly,
    #[default]
    Daily,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubscriberInstallPolicy {
    Required,
    #[default]
    BestEffort,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SubscriberInstallStatus {
    pub attempted: bool,
    pub installed: bool,
}

impl SubscriberInstallStatus {
    pub fn attempted(installed: bool) -> Self {
        Self {
            attempted: true,
            installed,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: "rocketmq-rust".to_string(),
            service_namespace: "rocketmq".to_string(),
            service_instance_id: String::new(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            environment: "dev".to_string(),
            cluster: "DefaultCluster".to_string(),
            node_type: "unknown".to_string(),
            node_id: String::new(),
            metrics: MetricsConfig::default(),
            traces: TracesConfig::default(),
            logs: LogsConfig::default(),
            otlp: OtlpConfig::default(),
            prometheus: PrometheusConfig::default(),
            subscriber_install_policy: SubscriberInstallPolicy::default(),
            resource_attributes: HashMap::new(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            filter: "info".to_string(),
            console: ConsoleLogConfig::default(),
            file: FileLogConfig::default(),
            reload: ReloadConfig::default(),
        }
    }
}

impl Default for ConsoleLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            format: LogFormat::Compact,
            ansi: true,
            target: true,
            thread_ids: true,
            thread_names: true,
            line_number: true,
        }
    }
}

impl Default for FileLogConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: "logs".to_string(),
            file_name_prefix: "rocketmq-rust".to_string(),
            rotation: LogRotation::Daily,
            format: LogFormat::Json,
            non_blocking: NonBlockingLogConfig::default(),
        }
    }
}

impl Default for NonBlockingLogConfig {
    fn default() -> Self {
        Self {
            buffered_lines_limit: 65_536,
            lossy: false,
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            exporter: MetricsExporter::Disable,
            export_interval_millis: 30_000,
            export_timeout_millis: 3_000,
            cardinality_limit: 10_000,
            sample_ratio: 1.0,
            topic_label_enabled: true,
            consumer_group_label_enabled: true,
        }
    }
}

impl Default for TracesConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            exporter: TraceExporter::Disable,
            sample_ratio: 0.01,
            propagate_context: true,
            record_message_id: false,
            record_message_keys: false,
            record_body_size: true,
        }
    }
}

impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            exporter: LogsExporter::Disable,
        }
    }
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:4317".to_string(),
            protocol: OtlpProtocol::Grpc,
            headers: HashMap::new(),
            timeout_millis: 3_000,
        }
    }
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5557,
            path: "/metrics".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_disabled_and_local_only() {
        let config = ObservabilityConfig::default();

        assert!(!config.enabled);
        assert!(!config.metrics.enabled);
        assert!(!config.traces.enabled);
        assert!((config.metrics.sample_ratio - 1.0).abs() < f64::EPSILON);
        assert_eq!(config.prometheus.host, "127.0.0.1");
        assert!(!config.traces.record_message_id);
        assert!(!config.traces.record_message_keys);
        assert_eq!(config.subscriber_install_policy, SubscriberInstallPolicy::BestEffort);
    }

    #[test]
    fn subscriber_install_policy_uses_snake_case_serde() {
        let policy = serde_json::from_str::<SubscriberInstallPolicy>("\"required\"")
            .expect("required policy should deserialize");

        assert_eq!(policy, SubscriberInstallPolicy::Required);
        assert_eq!(
            serde_json::to_string(&SubscriberInstallPolicy::BestEffort).expect("policy should serialize"),
            "\"best_effort\""
        );
    }

    #[test]
    fn logging_config_defaults_to_console_info_filter() {
        let config = LoggingConfig::default();

        assert!(config.enabled);
        assert_eq!(config.filter, "info");
        assert!(config.console.enabled);
        assert_eq!(config.console.format, LogFormat::Compact);
        assert!(config.console.ansi);
        assert!(config.console.target);
        assert!(config.console.thread_ids);
        assert!(config.console.thread_names);
        assert!(config.console.line_number);
        assert!(!config.file.enabled);
        assert_eq!(config.file.rotation, LogRotation::Daily);
        assert_eq!(config.file.format, LogFormat::Json);
        assert_eq!(config.file.non_blocking.buffered_lines_limit, 65_536);
        assert!(!config.file.non_blocking.lossy);
        assert!(!config.reload.enabled);
    }

    #[test]
    fn logging_config_serde_roundtrip_preserves_enums() {
        let config = LoggingConfig {
            filter: "rocketmq_store=debug,rocketmq_remoting=warn".to_string(),
            console: ConsoleLogConfig {
                format: LogFormat::Pretty,
                ..ConsoleLogConfig::default()
            },
            file: FileLogConfig {
                enabled: true,
                rotation: LogRotation::Hourly,
                format: LogFormat::Json,
                non_blocking: NonBlockingLogConfig {
                    buffered_lines_limit: 4_096,
                    lossy: true,
                },
                ..FileLogConfig::default()
            },
            reload: ReloadConfig { enabled: true },
            ..LoggingConfig::default()
        };

        let payload = serde_json::to_string(&config).expect("logging config should serialize");
        assert!(payload.contains("\"pretty\""));
        assert!(payload.contains("\"hourly\""));

        let decoded = serde_json::from_str::<LoggingConfig>(&payload).expect("logging config should deserialize");

        assert_eq!(decoded, config);
    }

    #[test]
    fn invalid_logging_enum_values_are_rejected() {
        assert!(serde_json::from_str::<LogFormat>("\"camelCase\"").is_err());
        assert!(serde_json::from_str::<LogRotation>("\"weekly\"").is_err());
    }

    #[test]
    fn telemetry_bootstrap_default_keeps_observability_disabled_and_logging_ready() {
        let config = TelemetryBootstrapConfig::default();

        assert!(!config.observability.enabled);
        assert!(config.logging.enabled);
        assert!(config.logging.console.enabled);
        assert!(!config.logging.file.enabled);
    }
}
