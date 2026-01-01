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

/// Broker metrics constants for RocketMQ metrics collection
/// Equivalent to Java's BrokerMetricsConstant class
pub struct BrokerMetricsConstant;

impl BrokerMetricsConstant {
    // OpenTelemetry Configuration
    pub const OPEN_TELEMETRY_METER_NAME: &'static str = "broker-meter";

    // Gauge Metrics - System Status
    pub const GAUGE_PROCESSOR_WATERMARK: &'static str = "rocketmq_processor_watermark";
    pub const GAUGE_BROKER_PERMISSION: &'static str = "rocketmq_broker_permission";
    pub const GAUGE_TOPIC_NUM: &'static str = "rocketmq_topic_number";
    pub const GAUGE_CONSUMER_GROUP_NUM: &'static str = "rocketmq_consumer_group_number";

    // Counter Metrics - Message Flow
    pub const COUNTER_MESSAGES_IN_TOTAL: &'static str = "rocketmq_messages_in_total";
    pub const COUNTER_MESSAGES_OUT_TOTAL: &'static str = "rocketmq_messages_out_total";
    pub const COUNTER_THROUGHPUT_IN_TOTAL: &'static str = "rocketmq_throughput_in_total";
    pub const COUNTER_THROUGHPUT_OUT_TOTAL: &'static str = "rocketmq_throughput_out_total";

    // Histogram Metrics - Performance
    pub const HISTOGRAM_MESSAGE_SIZE: &'static str = "rocketmq_message_size";
    pub const HISTOGRAM_TOPIC_CREATE_EXECUTE_TIME: &'static str = "rocketmq_topic_create_execution_time";
    pub const HISTOGRAM_CONSUMER_GROUP_CREATE_EXECUTE_TIME: &'static str =
        "rocketmq_consumer_group_create_execution_time";

    // Gauge Metrics - Connections
    pub const GAUGE_PRODUCER_CONNECTIONS: &'static str = "rocketmq_producer_connections";
    pub const GAUGE_CONSUMER_CONNECTIONS: &'static str = "rocketmq_consumer_connections";

    // Gauge Metrics - Consumer Lag and Latency
    pub const GAUGE_CONSUMER_LAG_MESSAGES: &'static str = "rocketmq_consumer_lag_messages";
    pub const GAUGE_CONSUMER_LAG_LATENCY: &'static str = "rocketmq_consumer_lag_latency";
    pub const GAUGE_CONSUMER_INFLIGHT_MESSAGES: &'static str = "rocketmq_consumer_inflight_messages";
    pub const GAUGE_CONSUMER_QUEUEING_LATENCY: &'static str = "rocketmq_consumer_queueing_latency";
    pub const GAUGE_CONSUMER_READY_MESSAGES: &'static str = "rocketmq_consumer_ready_messages";

    // Counter Metrics - Consumer Actions
    pub const COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL: &'static str = "rocketmq_send_to_dlq_messages_total";

    // Transaction Metrics
    pub const COUNTER_COMMIT_MESSAGES_TOTAL: &'static str = "rocketmq_commit_messages_total";
    pub const COUNTER_ROLLBACK_MESSAGES_TOTAL: &'static str = "rocketmq_rollback_messages_total";
    pub const HISTOGRAM_FINISH_MSG_LATENCY: &'static str = "rocketmq_finish_message_latency";
    pub const GAUGE_HALF_MESSAGES: &'static str = "rocketmq_half_messages";

    // Label Names - Infrastructure
    pub const LABEL_CLUSTER_NAME: &'static str = "cluster";
    pub const LABEL_NODE_TYPE: &'static str = "node_type";
    pub const NODE_TYPE_BROKER: &'static str = "broker";
    pub const LABEL_NODE_ID: &'static str = "node_id";
    pub const LABEL_AGGREGATION: &'static str = "aggregation";
    pub const AGGREGATION_DELTA: &'static str = "delta";
    pub const LABEL_PROCESSOR: &'static str = "processor";

    // Label Names - Business Logic
    pub const LABEL_TOPIC: &'static str = "topic";
    pub const LABEL_INVOCATION_STATUS: &'static str = "invocation_status";
    pub const LABEL_IS_RETRY: &'static str = "is_retry";
    pub const LABEL_IS_SYSTEM: &'static str = "is_system";
    pub const LABEL_CONSUMER_GROUP: &'static str = "consumer_group";
    pub const LABEL_MESSAGE_TYPE: &'static str = "message_type";
    pub const LABEL_LANGUAGE: &'static str = "language";
    pub const LABEL_VERSION: &'static str = "version";
    pub const LABEL_CONSUME_MODE: &'static str = "consume_mode";
}

/// Organized constants by metric type for better usability
pub mod metrics {
    use super::BrokerMetricsConstant as BMC;

    /// Gauge metric names
    pub mod gauge {
        use super::BMC;

        pub const PROCESSOR_WATERMARK: &str = BMC::GAUGE_PROCESSOR_WATERMARK;
        pub const BROKER_PERMISSION: &str = BMC::GAUGE_BROKER_PERMISSION;
        pub const TOPIC_NUM: &str = BMC::GAUGE_TOPIC_NUM;
        pub const CONSUMER_GROUP_NUM: &str = BMC::GAUGE_CONSUMER_GROUP_NUM;
        pub const PRODUCER_CONNECTIONS: &str = BMC::GAUGE_PRODUCER_CONNECTIONS;
        pub const CONSUMER_CONNECTIONS: &str = BMC::GAUGE_CONSUMER_CONNECTIONS;
        pub const CONSUMER_LAG_MESSAGES: &str = BMC::GAUGE_CONSUMER_LAG_MESSAGES;
        pub const CONSUMER_LAG_LATENCY: &str = BMC::GAUGE_CONSUMER_LAG_LATENCY;
        pub const CONSUMER_INFLIGHT_MESSAGES: &str = BMC::GAUGE_CONSUMER_INFLIGHT_MESSAGES;
        pub const CONSUMER_QUEUEING_LATENCY: &str = BMC::GAUGE_CONSUMER_QUEUEING_LATENCY;
        pub const CONSUMER_READY_MESSAGES: &str = BMC::GAUGE_CONSUMER_READY_MESSAGES;
        pub const HALF_MESSAGES: &str = BMC::GAUGE_HALF_MESSAGES;
    }

    /// Counter metric names
    pub mod counter {
        use super::BMC;

        pub const MESSAGES_IN_TOTAL: &str = BMC::COUNTER_MESSAGES_IN_TOTAL;
        pub const MESSAGES_OUT_TOTAL: &str = BMC::COUNTER_MESSAGES_OUT_TOTAL;
        pub const THROUGHPUT_IN_TOTAL: &str = BMC::COUNTER_THROUGHPUT_IN_TOTAL;
        pub const THROUGHPUT_OUT_TOTAL: &str = BMC::COUNTER_THROUGHPUT_OUT_TOTAL;
        pub const CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL: &str = BMC::COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL;
        pub const COMMIT_MESSAGES_TOTAL: &str = BMC::COUNTER_COMMIT_MESSAGES_TOTAL;
        pub const ROLLBACK_MESSAGES_TOTAL: &str = BMC::COUNTER_ROLLBACK_MESSAGES_TOTAL;
    }

    /// Histogram metric names
    pub mod histogram {
        use super::BMC;

        pub const MESSAGE_SIZE: &str = BMC::HISTOGRAM_MESSAGE_SIZE;
        pub const TOPIC_CREATE_EXECUTE_TIME: &str = BMC::HISTOGRAM_TOPIC_CREATE_EXECUTE_TIME;
        pub const CONSUMER_GROUP_CREATE_EXECUTE_TIME: &str = BMC::HISTOGRAM_CONSUMER_GROUP_CREATE_EXECUTE_TIME;
        pub const FINISH_MSG_LATENCY: &str = BMC::HISTOGRAM_FINISH_MSG_LATENCY;
    }
}

/// Label constants organized by category
pub mod labels {
    use super::BrokerMetricsConstant as BMC;

    /// Infrastructure labels
    pub mod infrastructure {
        use super::BMC;

        pub const CLUSTER_NAME: &str = BMC::LABEL_CLUSTER_NAME;
        pub const NODE_TYPE: &str = BMC::LABEL_NODE_TYPE;
        pub const NODE_ID: &str = BMC::LABEL_NODE_ID;
        pub const AGGREGATION: &str = BMC::LABEL_AGGREGATION;
        pub const PROCESSOR: &str = BMC::LABEL_PROCESSOR;
    }

    /// Business logic labels
    pub mod business {
        use super::BMC;

        pub const TOPIC: &str = BMC::LABEL_TOPIC;
        pub const INVOCATION_STATUS: &str = BMC::LABEL_INVOCATION_STATUS;
        pub const IS_RETRY: &str = BMC::LABEL_IS_RETRY;
        pub const IS_SYSTEM: &str = BMC::LABEL_IS_SYSTEM;
        pub const CONSUMER_GROUP: &str = BMC::LABEL_CONSUMER_GROUP;
        pub const MESSAGE_TYPE: &str = BMC::LABEL_MESSAGE_TYPE;
        pub const LANGUAGE: &str = BMC::LABEL_LANGUAGE;
        pub const VERSION: &str = BMC::LABEL_VERSION;
        pub const CONSUME_MODE: &str = BMC::LABEL_CONSUME_MODE;
    }

    /// Node type values
    pub mod node_types {
        use super::BMC;

        pub const BROKER: &str = BMC::NODE_TYPE_BROKER;
    }

    /// Aggregation values
    pub mod aggregation_types {
        use super::BMC;

        pub const DELTA: &str = BMC::AGGREGATION_DELTA;
    }
}

/// Configuration constants
pub mod config {
    use super::BrokerMetricsConstant as BMC;

    pub const OPEN_TELEMETRY_METER_NAME: &str = BMC::OPEN_TELEMETRY_METER_NAME;
}

/// Utility functions for working with metrics constants
impl BrokerMetricsConstant {
    /// Get all gauge metric names
    pub fn get_all_gauge_metrics() -> Vec<&'static str> {
        vec![
            Self::GAUGE_PROCESSOR_WATERMARK,
            Self::GAUGE_BROKER_PERMISSION,
            Self::GAUGE_TOPIC_NUM,
            Self::GAUGE_CONSUMER_GROUP_NUM,
            Self::GAUGE_PRODUCER_CONNECTIONS,
            Self::GAUGE_CONSUMER_CONNECTIONS,
            Self::GAUGE_CONSUMER_LAG_MESSAGES,
            Self::GAUGE_CONSUMER_LAG_LATENCY,
            Self::GAUGE_CONSUMER_INFLIGHT_MESSAGES,
            Self::GAUGE_CONSUMER_QUEUEING_LATENCY,
            Self::GAUGE_CONSUMER_READY_MESSAGES,
            Self::GAUGE_HALF_MESSAGES,
        ]
    }

    /// Get all counter metric names
    pub fn get_all_counter_metrics() -> Vec<&'static str> {
        vec![
            Self::COUNTER_MESSAGES_IN_TOTAL,
            Self::COUNTER_MESSAGES_OUT_TOTAL,
            Self::COUNTER_THROUGHPUT_IN_TOTAL,
            Self::COUNTER_THROUGHPUT_OUT_TOTAL,
            Self::COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL,
            Self::COUNTER_COMMIT_MESSAGES_TOTAL,
            Self::COUNTER_ROLLBACK_MESSAGES_TOTAL,
        ]
    }

    /// Get all histogram metric names
    pub fn get_all_histogram_metrics() -> Vec<&'static str> {
        vec![
            Self::HISTOGRAM_MESSAGE_SIZE,
            Self::HISTOGRAM_TOPIC_CREATE_EXECUTE_TIME,
            Self::HISTOGRAM_CONSUMER_GROUP_CREATE_EXECUTE_TIME,
            Self::HISTOGRAM_FINISH_MSG_LATENCY,
        ]
    }

    /// Get all label names
    pub fn get_all_labels() -> Vec<&'static str> {
        vec![
            Self::LABEL_CLUSTER_NAME,
            Self::LABEL_NODE_TYPE,
            Self::LABEL_NODE_ID,
            Self::LABEL_AGGREGATION,
            Self::LABEL_PROCESSOR,
            Self::LABEL_TOPIC,
            Self::LABEL_INVOCATION_STATUS,
            Self::LABEL_IS_RETRY,
            Self::LABEL_IS_SYSTEM,
            Self::LABEL_CONSUMER_GROUP,
            Self::LABEL_MESSAGE_TYPE,
            Self::LABEL_LANGUAGE,
            Self::LABEL_VERSION,
            Self::LABEL_CONSUME_MODE,
        ]
    }

    /// Check if a metric name is a gauge
    pub fn is_gauge_metric(metric_name: &str) -> bool {
        metric_name.starts_with("rocketmq_")
            && !metric_name.contains("_total")
            && !metric_name.contains("_time")
            && !metric_name.contains("_size")
            && !metric_name.contains("_latency")
    }

    /// Check if a metric name is a counter
    pub fn is_counter_metric(metric_name: &str) -> bool {
        metric_name.ends_with("_total")
    }

    /// Check if a metric name is a histogram
    pub fn is_histogram_metric(metric_name: &str) -> bool {
        metric_name.contains("_time") || metric_name.contains("_size") || metric_name.contains("_latency")
    }
}

/// Metric type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Gauge,
    Counter,
    Histogram,
}

impl MetricType {
    /// Get metric type from metric name
    pub fn from_metric_name(metric_name: &str) -> Option<Self> {
        if BrokerMetricsConstant::is_counter_metric(metric_name) {
            Some(Self::Counter)
        } else if BrokerMetricsConstant::is_histogram_metric(metric_name) {
            Some(Self::Histogram)
        } else if BrokerMetricsConstant::is_gauge_metric(metric_name) {
            Some(Self::Gauge)
        } else {
            None
        }
    }
}

/// Metric category enumeration for organization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricCategory {
    System,
    Message,
    Consumer,
    Producer,
    Transaction,
    Performance,
}

impl MetricCategory {
    /// Categorize a metric by its name
    pub fn from_metric_name(metric_name: &str) -> Option<Self> {
        match metric_name {
            name if name.contains("processor")
                || name.contains("permission")
                || name.contains("topic_number")
                || name.contains("consumer_group_number") =>
            {
                Some(Self::System)
            }
            name if name.contains("messages") || name.contains("throughput") || name.contains("message_size") => {
                Some(Self::Message)
            }
            name if name.contains("consumer") => Some(Self::Consumer),
            name if name.contains("producer") => Some(Self::Producer),
            name if name.contains("commit") || name.contains("rollback") || name.contains("half") => {
                Some(Self::Transaction)
            }
            name if name.contains("latency") || name.contains("execution_time") => Some(Self::Performance),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_organized_modules() {
        // Test organized gauge constants
        assert_eq!(
            metrics::gauge::PROCESSOR_WATERMARK,
            BrokerMetricsConstant::GAUGE_PROCESSOR_WATERMARK
        );
        assert_eq!(
            metrics::counter::MESSAGES_IN_TOTAL,
            BrokerMetricsConstant::COUNTER_MESSAGES_IN_TOTAL
        );
        assert_eq!(
            metrics::histogram::MESSAGE_SIZE,
            BrokerMetricsConstant::HISTOGRAM_MESSAGE_SIZE
        );

        // Test label constants
        assert_eq!(
            labels::infrastructure::CLUSTER_NAME,
            BrokerMetricsConstant::LABEL_CLUSTER_NAME
        );
        assert_eq!(labels::business::TOPIC, BrokerMetricsConstant::LABEL_TOPIC);
        assert_eq!(labels::node_types::BROKER, BrokerMetricsConstant::NODE_TYPE_BROKER);
    }

    #[test]
    fn test_utility_functions() {
        let gauge_metrics = BrokerMetricsConstant::get_all_gauge_metrics();
        assert!(!gauge_metrics.is_empty());
        assert!(gauge_metrics.contains(&BrokerMetricsConstant::GAUGE_PROCESSOR_WATERMARK));

        let counter_metrics = BrokerMetricsConstant::get_all_counter_metrics();
        assert!(!counter_metrics.is_empty());
        assert!(counter_metrics.contains(&BrokerMetricsConstant::COUNTER_MESSAGES_IN_TOTAL));

        let histogram_metrics = BrokerMetricsConstant::get_all_histogram_metrics();
        assert!(!histogram_metrics.is_empty());
        assert!(histogram_metrics.contains(&BrokerMetricsConstant::HISTOGRAM_MESSAGE_SIZE));

        let all_labels = BrokerMetricsConstant::get_all_labels();
        assert!(!all_labels.is_empty());
        assert!(all_labels.contains(&BrokerMetricsConstant::LABEL_CLUSTER_NAME));
    }

    #[test]
    fn test_metric_type_detection() {
        assert!(BrokerMetricsConstant::is_counter_metric("rocketmq_messages_in_total"));
        assert!(BrokerMetricsConstant::is_histogram_metric("rocketmq_message_size"));
        assert!(BrokerMetricsConstant::is_gauge_metric("rocketmq_processor_watermark"));

        assert!(!BrokerMetricsConstant::is_gauge_metric("rocketmq_messages_in_total"));
        assert!(!BrokerMetricsConstant::is_counter_metric("rocketmq_message_size"));
    }

    #[test]
    fn test_metric_type_enum() {
        assert_eq!(
            MetricType::from_metric_name("rocketmq_messages_in_total"),
            Some(MetricType::Counter)
        );
        assert_eq!(
            MetricType::from_metric_name("rocketmq_message_size"),
            Some(MetricType::Histogram)
        );
        assert_eq!(
            MetricType::from_metric_name("rocketmq_processor_watermark"),
            Some(MetricType::Gauge)
        );
        assert_eq!(MetricType::from_metric_name("invalid_metric"), None);
    }

    #[test]
    fn test_metric_category_enum() {
        assert_eq!(
            MetricCategory::from_metric_name("rocketmq_processor_watermark"),
            Some(MetricCategory::System)
        );
        assert_eq!(
            MetricCategory::from_metric_name("rocketmq_messages_in_total"),
            Some(MetricCategory::Message)
        );
        assert_eq!(
            MetricCategory::from_metric_name("rocketmq_consumer_connections"),
            Some(MetricCategory::Consumer)
        );
        assert_eq!(
            MetricCategory::from_metric_name("rocketmq_producer_connections"),
            Some(MetricCategory::Producer)
        );
        assert_eq!(
            MetricCategory::from_metric_name("rocketmq_commit_messages_total"),
            Some(MetricCategory::Message)
        );
        assert_eq!(
            MetricCategory::from_metric_name("rocketmq_finish_message_latency"),
            Some(MetricCategory::Performance)
        );
    }

    #[test]
    fn test_config_constants() {
        assert_eq!(config::OPEN_TELEMETRY_METER_NAME, "broker-meter");
    }

    #[test]
    fn test_all_metrics_have_rocketmq_prefix() {
        let all_gauges = BrokerMetricsConstant::get_all_gauge_metrics();
        let all_counters = BrokerMetricsConstant::get_all_counter_metrics();
        let all_histograms = BrokerMetricsConstant::get_all_histogram_metrics();

        for metric in all_gauges
            .iter()
            .chain(all_counters.iter())
            .chain(all_histograms.iter())
        {
            assert!(
                metric.starts_with("rocketmq_"),
                "Metric {} should start with rocketmq_",
                metric
            );
        }
    }

    #[test]
    fn test_counter_metrics_end_with_total() {
        let counter_metrics = BrokerMetricsConstant::get_all_counter_metrics();
        for metric in counter_metrics {
            assert!(
                metric.ends_with("_total"),
                "Counter metric {} should end with _total",
                metric
            );
        }
    }
}
