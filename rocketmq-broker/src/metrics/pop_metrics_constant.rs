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

/// Pop (Pull-on-Push) metrics constants for RocketMQ metrics collection
/// Equivalent to Java's PopMetricsConstant class
pub struct PopMetricsConstant;

impl PopMetricsConstant {
    // Histogram Metrics - Performance
    pub const HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME: &'static str = "rocketmq_pop_buffer_scan_time_consume";

    // Counter Metrics - Message Flow
    pub const COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL: &'static str = "rocketmq_pop_revive_in_message_total";
    pub const COUNTER_POP_REVIVE_OUT_MESSAGE_TOTAL: &'static str = "rocketmq_pop_revive_out_message_total";
    pub const COUNTER_POP_REVIVE_RETRY_MESSAGES_TOTAL: &'static str = "rocketmq_pop_revive_retry_messages_total";

    // Gauge Metrics - System Status
    pub const GAUGE_POP_REVIVE_LAG: &'static str = "rocketmq_pop_revive_lag";
    pub const GAUGE_POP_REVIVE_LATENCY: &'static str = "rocketmq_pop_revive_latency";
    pub const GAUGE_POP_OFFSET_BUFFER_SIZE: &'static str = "rocketmq_pop_offset_buffer_size";
    pub const GAUGE_POP_CHECKPOINT_BUFFER_SIZE: &'static str = "rocketmq_pop_checkpoint_buffer_size";

    // Label Names - Pop-specific
    pub const LABEL_REVIVE_MESSAGE_TYPE: &'static str = "revive_message_type";
    pub const LABEL_PUT_STATUS: &'static str = "put_status";
    pub const LABEL_QUEUE_ID: &'static str = "queue_id";
}

/// Organized constants by metric type for better usability
pub mod pop_metrics {
    use super::PopMetricsConstant as PMC;

    /// Histogram metric names for Pop operations
    pub mod histogram {
        use super::PMC;

        pub const POP_BUFFER_SCAN_TIME_CONSUME: &str = PMC::HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME;
    }

    /// Counter metric names for Pop operations
    pub mod counter {
        use super::PMC;

        pub const POP_REVIVE_IN_MESSAGE_TOTAL: &str = PMC::COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL;
        pub const POP_REVIVE_OUT_MESSAGE_TOTAL: &str = PMC::COUNTER_POP_REVIVE_OUT_MESSAGE_TOTAL;
        pub const POP_REVIVE_RETRY_MESSAGES_TOTAL: &str = PMC::COUNTER_POP_REVIVE_RETRY_MESSAGES_TOTAL;
    }

    /// Gauge metric names for Pop operations
    pub mod gauge {
        use super::PMC;

        pub const POP_REVIVE_LAG: &str = PMC::GAUGE_POP_REVIVE_LAG;
        pub const POP_REVIVE_LATENCY: &str = PMC::GAUGE_POP_REVIVE_LATENCY;
        pub const POP_OFFSET_BUFFER_SIZE: &str = PMC::GAUGE_POP_OFFSET_BUFFER_SIZE;
        pub const POP_CHECKPOINT_BUFFER_SIZE: &str = PMC::GAUGE_POP_CHECKPOINT_BUFFER_SIZE;
    }
}

/// Pop-specific label constants
pub mod pop_labels {
    use super::PopMetricsConstant as PMC;

    pub const REVIVE_MESSAGE_TYPE: &str = PMC::LABEL_REVIVE_MESSAGE_TYPE;
    pub const PUT_STATUS: &str = PMC::LABEL_PUT_STATUS;
    pub const QUEUE_ID: &str = PMC::LABEL_QUEUE_ID;
}

/// Utility functions for working with Pop metrics constants
impl PopMetricsConstant {
    /// Get all histogram metric names
    pub fn get_all_histogram_metrics() -> Vec<&'static str> {
        vec![Self::HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME]
    }

    /// Get all counter metric names
    pub fn get_all_counter_metrics() -> Vec<&'static str> {
        vec![
            Self::COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL,
            Self::COUNTER_POP_REVIVE_OUT_MESSAGE_TOTAL,
            Self::COUNTER_POP_REVIVE_RETRY_MESSAGES_TOTAL,
        ]
    }

    /// Get all gauge metric names
    pub fn get_all_gauge_metrics() -> Vec<&'static str> {
        vec![
            Self::GAUGE_POP_REVIVE_LAG,
            Self::GAUGE_POP_REVIVE_LATENCY,
            Self::GAUGE_POP_OFFSET_BUFFER_SIZE,
            Self::GAUGE_POP_CHECKPOINT_BUFFER_SIZE,
        ]
    }

    /// Get all label names
    pub fn get_all_labels() -> Vec<&'static str> {
        vec![
            Self::LABEL_REVIVE_MESSAGE_TYPE,
            Self::LABEL_PUT_STATUS,
            Self::LABEL_QUEUE_ID,
        ]
    }

    /// Get all metric names (all types combined)
    pub fn get_all_metrics() -> Vec<&'static str> {
        let mut metrics = Vec::new();
        metrics.extend(Self::get_all_histogram_metrics());
        metrics.extend(Self::get_all_counter_metrics());
        metrics.extend(Self::get_all_gauge_metrics());
        metrics
    }

    /// Check if a metric name is a Pop-related metric
    pub fn is_pop_metric(metric_name: &str) -> bool {
        metric_name.starts_with("rocketmq_pop_")
    }

    /// Check if a metric name is a Pop counter
    pub fn is_pop_counter_metric(metric_name: &str) -> bool {
        Self::is_pop_metric(metric_name) && metric_name.ends_with("_total")
    }

    /// Check if a metric name is a Pop gauge
    pub fn is_pop_gauge_metric(metric_name: &str) -> bool {
        Self::is_pop_metric(metric_name) && !metric_name.ends_with("_total") && !metric_name.contains("_time_consume")
    }

    /// Check if a metric name is a Pop histogram
    pub fn is_pop_histogram_metric(metric_name: &str) -> bool {
        Self::is_pop_metric(metric_name) && metric_name.contains("_time_consume")
    }
}

/// Pop metric type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopMetricType {
    Histogram,
    Counter,
    Gauge,
}

impl PopMetricType {
    /// Get Pop metric type from metric name
    pub fn from_metric_name(metric_name: &str) -> Option<Self> {
        if PopMetricsConstant::is_pop_counter_metric(metric_name) {
            Some(Self::Counter)
        } else if PopMetricsConstant::is_pop_histogram_metric(metric_name) {
            Some(Self::Histogram)
        } else if PopMetricsConstant::is_pop_gauge_metric(metric_name) {
            Some(Self::Gauge)
        } else {
            None
        }
    }
}

/// Pop metric category enumeration for organization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopMetricCategory {
    Revive,
    Buffer,
    Performance,
}

impl PopMetricCategory {
    /// Categorize a Pop metric by its name
    pub fn from_metric_name(metric_name: &str) -> Option<Self> {
        match metric_name {
            name if name.contains("revive") => Some(Self::Revive),
            name if name.contains("buffer") => Some(Self::Buffer),
            name if name.contains("time_consume") => Some(Self::Performance),
            _ => None,
        }
    }
}

/// Pop revive message types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviveMessageType {
    Normal,
    Retry,
    Dlq,
}

impl ReviveMessageType {
    /// Convert to string for metric labels
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Retry => "retry",
            Self::Dlq => "dlq",
        }
    }
}

impl From<&str> for ReviveMessageType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "normal" => Self::Normal,
            "retry" => Self::Retry,
            "dlq" => Self::Dlq,
            _ => Self::Normal,
        }
    }
}

/// Pop put status types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutStatus {
    PutOk,
    CreateMappedFileFailed,
    MessageIllegal,
    PropertiesLengthTooLong,
    ServiceNotAvailable,
    OsPageCacheBusy,
    UnknownError,
}

impl PutStatus {
    /// Convert to string for metric labels
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PutOk => "put_ok",
            Self::CreateMappedFileFailed => "create_mapped_file_failed",
            Self::MessageIllegal => "message_illegal",
            Self::PropertiesLengthTooLong => "properties_length_too_long",
            Self::ServiceNotAvailable => "service_not_available",
            Self::OsPageCacheBusy => "os_page_cache_busy",
            Self::UnknownError => "unknown_error",
        }
    }
}

impl From<&str> for PutStatus {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "put_ok" => Self::PutOk,
            "create_mapped_file_failed" => Self::CreateMappedFileFailed,
            "message_illegal" => Self::MessageIllegal,
            "properties_length_too_long" => Self::PropertiesLengthTooLong,
            "service_not_available" => Self::ServiceNotAvailable,
            "os_page_cache_busy" => Self::OsPageCacheBusy,
            _ => Self::UnknownError,
        }
    }
}

/// Utility struct for building Pop metric labels
#[derive(Debug, Default)]
pub struct PopMetricLabels {
    revive_message_type: Option<ReviveMessageType>,
    put_status: Option<PutStatus>,
    queue_id: Option<i32>,
}

impl PopMetricLabels {
    /// Create new Pop metric labels builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set revive message type
    pub fn revive_message_type(mut self, message_type: ReviveMessageType) -> Self {
        self.revive_message_type = Some(message_type);
        self
    }

    /// Set put status
    pub fn put_status(mut self, status: PutStatus) -> Self {
        self.put_status = Some(status);
        self
    }

    /// Set queue ID
    pub fn queue_id(mut self, queue_id: i32) -> Self {
        self.queue_id = Some(queue_id);
        self
    }

    /// Build labels as key-value pairs
    pub fn build(self) -> Vec<(String, String)> {
        let mut labels = Vec::new();

        if let Some(msg_type) = self.revive_message_type {
            labels.push((
                PopMetricsConstant::LABEL_REVIVE_MESSAGE_TYPE.to_string(),
                msg_type.as_str().to_string(),
            ));
        }

        if let Some(status) = self.put_status {
            labels.push((
                PopMetricsConstant::LABEL_PUT_STATUS.to_string(),
                status.as_str().to_string(),
            ));
        }

        if let Some(queue_id) = self.queue_id {
            labels.push((PopMetricsConstant::LABEL_QUEUE_ID.to_string(), queue_id.to_string()));
        }

        labels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_organized_modules() {
        // Test organized metric constants
        assert_eq!(
            pop_metrics::histogram::POP_BUFFER_SCAN_TIME_CONSUME,
            PopMetricsConstant::HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME
        );
        assert_eq!(
            pop_metrics::counter::POP_REVIVE_IN_MESSAGE_TOTAL,
            PopMetricsConstant::COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL
        );
        assert_eq!(
            pop_metrics::gauge::POP_REVIVE_LAG,
            PopMetricsConstant::GAUGE_POP_REVIVE_LAG
        );

        // Test label constants
        assert_eq!(
            pop_labels::REVIVE_MESSAGE_TYPE,
            PopMetricsConstant::LABEL_REVIVE_MESSAGE_TYPE
        );
    }

    #[test]
    fn test_utility_functions() {
        let histogram_metrics = PopMetricsConstant::get_all_histogram_metrics();
        assert!(!histogram_metrics.is_empty());
        assert!(histogram_metrics.contains(&PopMetricsConstant::HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME));

        let counter_metrics = PopMetricsConstant::get_all_counter_metrics();
        assert_eq!(counter_metrics.len(), 3);
        assert!(counter_metrics.contains(&PopMetricsConstant::COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL));

        let gauge_metrics = PopMetricsConstant::get_all_gauge_metrics();
        assert_eq!(gauge_metrics.len(), 4);
        assert!(gauge_metrics.contains(&PopMetricsConstant::GAUGE_POP_REVIVE_LAG));

        let all_labels = PopMetricsConstant::get_all_labels();
        assert_eq!(all_labels.len(), 3);
        assert!(all_labels.contains(&PopMetricsConstant::LABEL_REVIVE_MESSAGE_TYPE));

        let all_metrics = PopMetricsConstant::get_all_metrics();
        assert_eq!(all_metrics.len(), 8); // 1 histogram + 3 counters + 4 gauges
    }

    #[test]
    fn test_metric_type_detection() {
        assert!(PopMetricsConstant::is_pop_metric("rocketmq_pop_revive_lag"));
        assert!(PopMetricsConstant::is_pop_counter_metric(
            "rocketmq_pop_revive_in_message_total"
        ));
        assert!(PopMetricsConstant::is_pop_gauge_metric("rocketmq_pop_revive_lag"));
        assert!(PopMetricsConstant::is_pop_histogram_metric(
            "rocketmq_pop_buffer_scan_time_consume"
        ));

        assert!(!PopMetricsConstant::is_pop_metric("rocketmq_messages_in_total"));
        assert!(!PopMetricsConstant::is_pop_counter_metric("rocketmq_pop_revive_lag"));
        assert!(!PopMetricsConstant::is_pop_gauge_metric(
            "rocketmq_pop_revive_in_message_total"
        ));
    }

    #[test]
    fn test_pop_metric_type_enum() {
        assert_eq!(
            PopMetricType::from_metric_name("rocketmq_pop_revive_in_message_total"),
            Some(PopMetricType::Counter)
        );
        assert_eq!(
            PopMetricType::from_metric_name("rocketmq_pop_revive_lag"),
            Some(PopMetricType::Gauge)
        );
        assert_eq!(
            PopMetricType::from_metric_name("rocketmq_pop_buffer_scan_time_consume"),
            Some(PopMetricType::Histogram)
        );
        assert_eq!(PopMetricType::from_metric_name("invalid_metric"), None);
    }

    #[test]
    fn test_pop_metric_category_enum() {
        assert_eq!(
            PopMetricCategory::from_metric_name("rocketmq_pop_revive_lag"),
            Some(PopMetricCategory::Revive)
        );
        assert_eq!(
            PopMetricCategory::from_metric_name("rocketmq_pop_offset_buffer_size"),
            Some(PopMetricCategory::Buffer)
        );
        assert_eq!(
            PopMetricCategory::from_metric_name("rocketmq_pop_buffer_scan_time_consume"),
            Some(PopMetricCategory::Buffer)
        );
    }

    #[test]
    fn test_revive_message_type() {
        assert_eq!(ReviveMessageType::Normal.as_str(), "normal");
        assert_eq!(ReviveMessageType::Retry.as_str(), "retry");
        assert_eq!(ReviveMessageType::Dlq.as_str(), "dlq");

        assert_eq!(ReviveMessageType::from("normal"), ReviveMessageType::Normal);
        assert_eq!(ReviveMessageType::from("RETRY"), ReviveMessageType::Retry);
        assert_eq!(ReviveMessageType::from("invalid"), ReviveMessageType::Normal);
    }

    #[test]
    fn test_put_status() {
        assert_eq!(PutStatus::PutOk.as_str(), "put_ok");
        assert_eq!(PutStatus::MessageIllegal.as_str(), "message_illegal");

        assert_eq!(PutStatus::from("put_ok"), PutStatus::PutOk);
        assert_eq!(PutStatus::from("MESSAGE_ILLEGAL"), PutStatus::MessageIllegal);
        assert_eq!(PutStatus::from("invalid"), PutStatus::UnknownError);
    }

    #[test]
    fn test_pop_metric_labels_builder() {
        let labels = PopMetricLabels::new()
            .revive_message_type(ReviveMessageType::Retry)
            .put_status(PutStatus::PutOk)
            .queue_id(5)
            .build();

        assert_eq!(labels.len(), 3);

        let label_map: std::collections::HashMap<String, String> = labels.into_iter().collect();
        assert_eq!(label_map.get("revive_message_type"), Some(&"retry".to_string()));
        assert_eq!(label_map.get("put_status"), Some(&"put_ok".to_string()));
        assert_eq!(label_map.get("queue_id"), Some(&"5".to_string()));
    }

    #[test]
    fn test_all_pop_metrics_have_rocketmq_pop_prefix() {
        let all_metrics = PopMetricsConstant::get_all_metrics();
        for metric in all_metrics {
            assert!(
                metric.starts_with("rocketmq_pop_"),
                "Metric {} should start with rocketmq_pop_",
                metric
            );
        }
    }

    #[test]
    fn test_counter_metrics_end_with_total() {
        let counter_metrics = PopMetricsConstant::get_all_counter_metrics();
        for metric in counter_metrics {
            assert!(
                metric.ends_with("_total"),
                "Counter metric {} should end with _total",
                metric
            );
        }
    }
}
