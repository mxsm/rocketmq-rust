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

use rocketmq_common::common::metrics::LogExporterType;
use rocketmq_common::common::metrics::MetricsExporterType;
use rocketmq_common::common::metrics::TraceExporterType;
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

impl From<MetricsExporterType> for MetricsExporter {
    fn from(value: MetricsExporterType) -> Self {
        match value {
            MetricsExporterType::Disable => Self::Disable,
            MetricsExporterType::OtlpGrpc => Self::OtlpGrpc,
            MetricsExporterType::Prom => Self::Prometheus,
            MetricsExporterType::Log => Self::Log,
        }
    }
}

impl From<TraceExporterType> for TraceExporter {
    fn from(value: TraceExporterType) -> Self {
        match value {
            TraceExporterType::Disable => Self::Disable,
            TraceExporterType::OtlpGrpc => Self::OtlpGrpc,
            TraceExporterType::Log => Self::Log,
        }
    }
}

impl From<LogExporterType> for LogsExporter {
    fn from(value: LogExporterType) -> Self {
        match value {
            LogExporterType::Disable => Self::Disable,
            LogExporterType::OtlpGrpc => Self::OtlpGrpc,
            LogExporterType::Log => Self::Log,
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
    fn maps_existing_metrics_exporter_type() {
        assert_eq!(
            MetricsExporter::from(MetricsExporterType::Disable),
            MetricsExporter::Disable
        );
        assert_eq!(
            MetricsExporter::from(MetricsExporterType::OtlpGrpc),
            MetricsExporter::OtlpGrpc
        );
        assert_eq!(
            MetricsExporter::from(MetricsExporterType::Prom),
            MetricsExporter::Prometheus
        );
        assert_eq!(MetricsExporter::from(MetricsExporterType::Log), MetricsExporter::Log);
    }

    #[test]
    fn maps_existing_trace_exporter_type() {
        assert_eq!(TraceExporter::from(TraceExporterType::Disable), TraceExporter::Disable);
        assert_eq!(
            TraceExporter::from(TraceExporterType::OtlpGrpc),
            TraceExporter::OtlpGrpc
        );
        assert_eq!(TraceExporter::from(TraceExporterType::Log), TraceExporter::Log);
    }

    #[test]
    fn maps_existing_log_exporter_type() {
        assert_eq!(LogsExporter::from(LogExporterType::Disable), LogsExporter::Disable);
        assert_eq!(LogsExporter::from(LogExporterType::OtlpGrpc), LogsExporter::OtlpGrpc);
        assert_eq!(LogsExporter::from(LogExporterType::Log), LogsExporter::Log);
    }
}
