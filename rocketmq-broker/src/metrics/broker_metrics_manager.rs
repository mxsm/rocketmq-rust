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

//! # BrokerMetricsManager
//!
//! Manages all broker-level metrics for RocketMQ using OpenTelemetry.
//!
//! ## Overview
//! This module provides comprehensive metrics collection for the RocketMQ broker,
//! including:
//! - Broker stats (processor watermark, topic count, consumer group count)
//! - Request metrics (messages in/out, throughput, message size)
//! - Connection metrics (producer/consumer connections)
//! - Lag metrics (consumer lag, inflight messages, queueing latency)
//! - Transaction metrics (commit/rollback counts, half messages)
//!
//! ## Design Philosophy
//! - Uses OpenTelemetry Rust SDK for standard metrics instrumentation
//! - Provides both static global interface and instance-based API
//! - Thread-safe by design
//! - Supports multiple exporter types (OTLP gRPC, Prometheus, Log)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::Duration;

use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::metrics::metrics_exporter_type::MetricsExporterType;
use rocketmq_common::common::metrics::nop_long_counter::NopLongCounter;
use rocketmq_common::common::metrics::nop_long_histogram::NopLongHistogram;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use tracing::warn;

use super::broker_metrics_constant::BrokerMetricsConstant;
use super::consumer_attr::ConsumerAttr;
use super::producer_attr::ProducerAttr;

// ============================================================================
// Constants
// ============================================================================

/// System group prefixes for identifying system consumer groups
const SYSTEM_GROUP_PREFIX_LIST: &[&str] = &["cid_rmq_sys_"];

// ============================================================================
// Types and Traits
// ============================================================================

/// Trait for providing base attributes (cluster, node info, etc.)
pub trait AttributesBuilderSupplier: Send + Sync {
    /// Returns the base attributes that should be added to all metrics
    fn get(&self) -> Vec<KeyValue>;
}

/// Default no-op supplier that returns empty attributes
pub struct NoopAttributesSupplier;

impl AttributesBuilderSupplier for NoopAttributesSupplier {
    fn get(&self) -> Vec<KeyValue> {
        Vec::new()
    }
}

// ============================================================================
// Static Metrics Wrappers
// ============================================================================

/// Wrapper for Counter that can be either real or no-op
pub enum CounterWrapper {
    Real(Counter<u64>),
    Nop(NopLongCounter),
}

impl CounterWrapper {
    #[inline]
    pub fn add(&self, value: u64, attributes: &[KeyValue]) {
        match self {
            Self::Real(counter) => counter.add(value, attributes),
            Self::Nop(_) => {}
        }
    }
}

/// Wrapper for Histogram that can be either real or no-op
pub enum HistogramWrapper {
    Real(Histogram<u64>),
    Nop(NopLongHistogram),
}

impl HistogramWrapper {
    #[inline]
    pub fn record(&self, value: u64, attributes: &[KeyValue]) {
        match self {
            Self::Real(histogram) => histogram.record(value, attributes),
            Self::Nop(_) => {}
        }
    }
}

// ============================================================================
// Histogram Bucket Configurations
// ============================================================================

/// Get message size histogram buckets (1KB, 4KB, 512KB, 1MB, 2MB, 4MB)
pub fn get_message_size_buckets() -> Vec<f64> {
    vec![
        1.0 * 1024.0,          // 1KB
        4.0 * 1024.0,          // 4KB
        512.0 * 1024.0,        // 512KB
        1.0 * 1024.0 * 1024.0, // 1MB
        2.0 * 1024.0 * 1024.0, // 2MB
        4.0 * 1024.0 * 1024.0, // 4MB
    ]
}

/// Get commit/finish latency histogram buckets
pub fn get_commit_latency_buckets() -> Vec<f64> {
    vec![
        5.0,                // 5s
        60.0,               // 1min
        10.0 * 60.0,        // 10min
        60.0 * 60.0,        // 1h
        12.0 * 60.0 * 60.0, // 12h
        24.0 * 60.0 * 60.0, // 24h
    ]
}

/// Get create time histogram buckets (10ms, 100ms, 1s, 3s, 5s)
pub fn get_create_time_buckets() -> Vec<f64> {
    vec![
        Duration::from_millis(10).as_millis() as f64,
        Duration::from_millis(100).as_millis() as f64,
        Duration::from_secs(1).as_millis() as f64,
        Duration::from_secs(3).as_millis() as f64,
        Duration::from_secs(5).as_millis() as f64,
    ]
}

// ============================================================================
// BrokerMetricsManager
// ============================================================================

/// Global singleton for BrokerMetricsManager
static BROKER_METRICS_MANAGER: OnceLock<BrokerMetricsManager> = OnceLock::new();

/// Global label map for common labels
static LABEL_MAP: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();

/// Global attributes builder supplier
static ATTRIBUTES_BUILDER_SUPPLIER: OnceLock<Arc<dyn AttributesBuilderSupplier>> = OnceLock::new();

/// Broker metrics manager for comprehensive broker-level metrics
///
/// This struct manages all broker-related metrics including:
/// - Broker stats gauges (processor watermark, topic/consumer group counts)
/// - Request metrics (messages in/out, throughput, message size)
/// - Connection metrics (producer/consumer connections)
/// - Lag metrics (consumer lag, inflight, queueing latency)
/// - Transaction metrics (commit/rollback, half messages)
pub struct BrokerMetricsManager {
    // Request metrics - Counters
    messages_in_total: Counter<u64>,
    messages_out_total: Counter<u64>,
    throughput_in_total: Counter<u64>,
    throughput_out_total: Counter<u64>,
    send_to_dlq_messages: Counter<u64>,
    commit_messages_total: Counter<u64>,
    rollback_messages_total: Counter<u64>,

    // Request metrics - Histograms
    message_size: Histogram<u64>,
    topic_create_execute_time: Histogram<u64>,
    consumer_group_create_execute_time: Histogram<u64>,
    transaction_finish_latency: Histogram<u64>,

    // Meter reference for creating additional instruments
    meter: Meter,

    // Attributes supplier
    attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
}

impl BrokerMetricsManager {
    /// Create a new BrokerMetricsManager with OpenTelemetry Meter
    pub fn new(meter: Meter, attributes_supplier: Arc<dyn AttributesBuilderSupplier>) -> Self {
        // Request metrics - Counters
        let messages_in_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_MESSAGES_IN_TOTAL)
            .with_description("Total number of incoming messages")
            .build();

        let messages_out_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_MESSAGES_OUT_TOTAL)
            .with_description("Total number of outgoing messages")
            .build();

        let throughput_in_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_THROUGHPUT_IN_TOTAL)
            .with_description("Total traffic of incoming messages")
            .build();

        let throughput_out_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_THROUGHPUT_OUT_TOTAL)
            .with_description("Total traffic of outgoing messages")
            .build();

        let send_to_dlq_messages = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL)
            .with_description("Consumer send to DLQ messages")
            .build();

        let commit_messages_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_COMMIT_MESSAGES_TOTAL)
            .with_description("Total number of commit messages")
            .build();

        let rollback_messages_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_ROLLBACK_MESSAGES_TOTAL)
            .with_description("Total number of rollback messages")
            .build();

        // Request metrics - Histograms
        let message_size = meter
            .u64_histogram(BrokerMetricsConstant::HISTOGRAM_MESSAGE_SIZE)
            .with_description("Incoming messages size")
            .build();

        let topic_create_execute_time = meter
            .u64_histogram(BrokerMetricsConstant::HISTOGRAM_TOPIC_CREATE_EXECUTE_TIME)
            .with_description("The distribution of create topic time")
            .with_unit("milliseconds")
            .build();

        let consumer_group_create_execute_time = meter
            .u64_histogram(BrokerMetricsConstant::HISTOGRAM_CONSUMER_GROUP_CREATE_EXECUTE_TIME)
            .with_description("The distribution of create subscription time")
            .with_unit("milliseconds")
            .build();

        let transaction_finish_latency = meter
            .u64_histogram(BrokerMetricsConstant::HISTOGRAM_FINISH_MSG_LATENCY)
            .with_description("Transaction finish latency")
            .with_unit("ms")
            .build();

        Self {
            messages_in_total,
            messages_out_total,
            throughput_in_total,
            throughput_out_total,
            send_to_dlq_messages,
            commit_messages_total,
            rollback_messages_total,
            message_size,
            topic_create_execute_time,
            consumer_group_create_execute_time,
            transaction_finish_latency,
            meter,
            attributes_supplier,
        }
    }

    /// Initialize with observable gauges for stats metrics
    /// This registers all observable gauges that require callbacks
    pub fn init_with_observables<F1, F2, F3, F4, F5, F6>(
        meter_provider: &SdkMeterProvider,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        // Stats callbacks
        processor_watermark_fn: F1,
        broker_permission_fn: F2,
        topic_num_fn: F3,
        consumer_group_num_fn: F4,
        // Connection callbacks
        producer_connections_fn: F5,
        consumer_connections_fn: F6,
    ) -> Self
    where
        F1: Fn() -> Vec<(String, i64)> + Send + Sync + 'static, // (processor_name, count)
        F2: Fn() -> i64 + Send + Sync + 'static,
        F3: Fn() -> i64 + Send + Sync + 'static,
        F4: Fn() -> i64 + Send + Sync + 'static,
        F5: Fn() -> Vec<(ProducerAttr, i64)> + Send + Sync + 'static,
        F6: Fn() -> Vec<(ConsumerAttr, i64)> + Send + Sync + 'static,
    {
        let meter = meter_provider.meter(BrokerMetricsConstant::OPEN_TELEMETRY_METER_NAME);
        let manager = Self::new(meter.clone(), attributes_supplier.clone());

        // Register stats observable gauges
        let attrs1 = attributes_supplier.clone();
        let _processor_watermark = meter
            .i64_observable_gauge(BrokerMetricsConstant::GAUGE_PROCESSOR_WATERMARK)
            .with_description("Request processor watermark")
            .with_callback(move |observer| {
                for (processor_name, count) in processor_watermark_fn() {
                    let mut attrs = attrs1.get();
                    attrs.push(KeyValue::new(BrokerMetricsConstant::LABEL_PROCESSOR, processor_name));
                    observer.observe(count, &attrs);
                }
            })
            .build();

        let attrs2 = attributes_supplier.clone();
        let _broker_permission = meter
            .i64_observable_gauge(BrokerMetricsConstant::GAUGE_BROKER_PERMISSION)
            .with_description("Broker permission")
            .with_callback(move |observer| {
                let attrs = attrs2.get();
                observer.observe(broker_permission_fn(), &attrs);
            })
            .build();

        let attrs3 = attributes_supplier.clone();
        let _topic_num = meter
            .i64_observable_gauge(BrokerMetricsConstant::GAUGE_TOPIC_NUM)
            .with_description("Active topic number")
            .with_callback(move |observer| {
                let attrs = attrs3.get();
                observer.observe(topic_num_fn(), &attrs);
            })
            .build();

        let attrs4 = attributes_supplier.clone();
        let _consumer_group_num = meter
            .i64_observable_gauge(BrokerMetricsConstant::GAUGE_CONSUMER_GROUP_NUM)
            .with_description("Active subscription group number")
            .with_callback(move |observer| {
                let attrs = attrs4.get();
                observer.observe(consumer_group_num_fn(), &attrs);
            })
            .build();

        // Register connection observable gauges
        let attrs5 = attributes_supplier.clone();
        let _producer_connection = meter
            .i64_observable_gauge(BrokerMetricsConstant::GAUGE_PRODUCER_CONNECTIONS)
            .with_description("Producer connections")
            .with_callback(move |observer| {
                for (attr, count) in producer_connections_fn() {
                    let mut attrs = attrs5.get();
                    attrs.extend([
                        KeyValue::new(
                            BrokerMetricsConstant::LABEL_LANGUAGE,
                            attr.language.to_string().to_lowercase(),
                        ),
                        KeyValue::new(BrokerMetricsConstant::LABEL_VERSION, attr.version.to_string()),
                    ]);
                    observer.observe(count, &attrs);
                }
            })
            .build();

        let attrs6 = attributes_supplier;
        let _consumer_connection = meter
            .i64_observable_gauge(BrokerMetricsConstant::GAUGE_CONSUMER_CONNECTIONS)
            .with_description("Consumer connections")
            .with_callback(move |observer| {
                for (attr, count) in consumer_connections_fn() {
                    let mut attrs = attrs6.get();
                    attrs.extend([
                        KeyValue::new(BrokerMetricsConstant::LABEL_CONSUMER_GROUP, attr.group.clone()),
                        KeyValue::new(
                            BrokerMetricsConstant::LABEL_LANGUAGE,
                            attr.language.to_string().to_lowercase(),
                        ),
                        KeyValue::new(BrokerMetricsConstant::LABEL_VERSION, attr.version.to_string()),
                        KeyValue::new(
                            BrokerMetricsConstant::LABEL_CONSUME_MODE,
                            attr.consume_mode.to_string().to_lowercase(),
                        ),
                        KeyValue::new(
                            BrokerMetricsConstant::LABEL_IS_SYSTEM,
                            is_system_group(&attr.group).to_string(),
                        ),
                    ]);
                    observer.observe(count, &attrs);
                }
            })
            .build();

        manager
    }

    /// Initialize the global BrokerMetricsManager
    pub fn init_global(meter: Meter, attributes_supplier: Arc<dyn AttributesBuilderSupplier>) {
        let _ = ATTRIBUTES_BUILDER_SUPPLIER.set(attributes_supplier.clone());
        let _ = LABEL_MAP.set(RwLock::new(HashMap::new()));
        let _ = BROKER_METRICS_MANAGER.set(Self::new(meter, attributes_supplier));
    }

    /// Get the global BrokerMetricsManager instance
    pub fn try_global() -> Option<&'static BrokerMetricsManager> {
        BROKER_METRICS_MANAGER.get()
    }

    /// Get base attributes from the supplier
    #[inline]
    fn base_attributes(&self) -> Vec<KeyValue> {
        let mut attrs = self.attributes_supplier.get();
        // Add labels from global LABEL_MAP
        if let Some(label_map) = LABEL_MAP.get() {
            if let Ok(map) = label_map.read() {
                for (k, v) in map.iter() {
                    attrs.push(KeyValue::new(k.clone(), v.clone()));
                }
            }
        }
        attrs
    }

    /// Get the meter reference
    pub fn meter(&self) -> &Meter {
        &self.meter
    }

    // ========================================================================
    // Messages In/Out Metrics
    // ========================================================================

    /// Record incoming message count
    pub fn inc_messages_in_total(&self, topic: &str, message_type: TopicMessageType, num: u64, is_system: bool) {
        let mut attrs = self.base_attributes();
        attrs.extend([
            KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()),
            KeyValue::new(
                BrokerMetricsConstant::LABEL_MESSAGE_TYPE,
                message_type.to_string().to_lowercase(),
            ),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_SYSTEM, is_system.to_string()),
        ]);
        self.messages_in_total.add(num, &attrs);
    }

    /// Record outgoing message count
    pub fn inc_messages_out_total(&self, topic: &str, consumer_group: &str, num: u64, is_retry: bool) {
        let is_system = is_system(topic, consumer_group);
        let mut attrs = self.base_attributes();
        attrs.extend([
            KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()),
            KeyValue::new(BrokerMetricsConstant::LABEL_CONSUMER_GROUP, consumer_group.to_owned()),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_RETRY, is_retry.to_string()),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_SYSTEM, is_system.to_string()),
        ]);
        self.messages_out_total.add(num, &attrs);
    }

    // ========================================================================
    // Throughput Metrics
    // ========================================================================

    /// Record incoming throughput (bytes)
    pub fn inc_throughput_in_total(&self, topic: &str, bytes: u64) {
        let mut attrs = self.base_attributes();
        attrs.push(KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()));
        self.throughput_in_total.add(bytes, &attrs);
    }

    /// Record outgoing throughput (bytes)
    pub fn inc_throughput_out_total(&self, topic: &str, consumer_group: &str, bytes: u64, is_retry: bool) {
        let is_system = is_system(topic, consumer_group);
        let mut attrs = self.base_attributes();
        attrs.extend([
            KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()),
            KeyValue::new(BrokerMetricsConstant::LABEL_CONSUMER_GROUP, consumer_group.to_owned()),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_RETRY, is_retry.to_string()),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_SYSTEM, is_system.to_string()),
        ]);
        self.throughput_out_total.add(bytes, &attrs);
    }

    // ========================================================================
    // Message Size Metrics
    // ========================================================================

    /// Record message size
    pub fn record_message_size(&self, topic: &str, message_type: TopicMessageType, size: u64) {
        let mut attrs = self.base_attributes();
        attrs.extend([
            KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()),
            KeyValue::new(
                BrokerMetricsConstant::LABEL_MESSAGE_TYPE,
                message_type.to_string().to_lowercase(),
            ),
        ]);
        self.message_size.record(size, &attrs);
    }

    // ========================================================================
    // Topic/ConsumerGroup Create Time Metrics
    // ========================================================================

    /// Record topic create execution time
    pub fn record_topic_create_time(&self, time_ms: u64) {
        let attrs = self.base_attributes();
        self.topic_create_execute_time.record(time_ms, &attrs);
    }

    /// Record consumer group create execution time
    pub fn record_consumer_group_create_time(&self, time_ms: u64) {
        let attrs = self.base_attributes();
        self.consumer_group_create_execute_time.record(time_ms, &attrs);
    }

    // ========================================================================
    // DLQ Metrics
    // ========================================================================

    /// Record send to DLQ message count
    pub fn inc_send_to_dlq_messages(&self, topic: &str, consumer_group: &str, num: u64) {
        let is_system = is_system(topic, consumer_group);
        let mut attrs = self.base_attributes();
        attrs.extend([
            KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()),
            KeyValue::new(BrokerMetricsConstant::LABEL_CONSUMER_GROUP, consumer_group.to_owned()),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_SYSTEM, is_system.to_string()),
        ]);
        self.send_to_dlq_messages.add(num, &attrs);
    }

    // ========================================================================
    // Transaction Metrics
    // ========================================================================

    /// Record commit message count
    pub fn inc_commit_messages(&self, topic: &str, num: u64) {
        let mut attrs = self.base_attributes();
        attrs.push(KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()));
        self.commit_messages_total.add(num, &attrs);
    }

    /// Record rollback message count
    pub fn inc_rollback_messages(&self, topic: &str, num: u64) {
        let mut attrs = self.base_attributes();
        attrs.push(KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()));
        self.rollback_messages_total.add(num, &attrs);
    }

    /// Record transaction finish latency
    pub fn record_transaction_finish_latency(&self, topic: &str, latency_ms: u64) {
        let mut attrs = self.base_attributes();
        attrs.push(KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()));
        self.transaction_finish_latency.record(latency_ms, &attrs);
    }
}

// ============================================================================
// Static Helper Functions
// ============================================================================

/// Create a new attributes builder with common labels
pub fn new_attributes_builder() -> Vec<KeyValue> {
    let mut attrs = Vec::new();
    if let Some(supplier) = ATTRIBUTES_BUILDER_SUPPLIER.get() {
        attrs = supplier.get();
    }
    if let Some(label_map) = LABEL_MAP.get() {
        if let Ok(map) = label_map.read() {
            for (k, v) in map.iter() {
                attrs.push(KeyValue::new(k.clone(), v.clone()));
            }
        }
    }
    attrs
}

/// Check if topic is retry or DLQ topic
pub fn is_retry_or_dlq_topic(topic: &str) -> bool {
    if topic.is_empty() {
        return false;
    }
    topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(mix_all::DLQ_GROUP_TOPIC_PREFIX)
}

/// Check if consumer group is a system group
pub fn is_system_group(group: &str) -> bool {
    if group.is_empty() {
        return false;
    }
    let group_lower = group.to_lowercase();
    for prefix in SYSTEM_GROUP_PREFIX_LIST {
        if group_lower.starts_with(prefix) {
            return true;
        }
    }
    false
}

/// Check if topic/group combination is system
pub fn is_system(topic: &str, group: &str) -> bool {
    TopicValidator::is_system_topic(topic) || is_system_group(group)
}

/// Get message type from send message request header
pub fn get_message_type(request_header: &SendMessageRequestHeader) -> TopicMessageType {
    let properties = if let Some(props) = &request_header.properties {
        message_decoder::string_to_message_properties(Some(props))
    } else {
        HashMap::new()
    };

    // Check for transaction prepared flag
    if let Some(tra_flag) = properties.get("TRAN_MSG") {
        if tra_flag.eq_ignore_ascii_case("true") {
            return TopicMessageType::Transaction;
        }
    }

    // Check for FIFO (sharding key)
    if properties.contains_key("SHARDING_KEY") {
        return TopicMessageType::Fifo;
    }

    // Check for delay message
    if properties.contains_key("__STARTDELIVERTIME")
        || properties.contains_key("DELAY")
        || properties.contains_key("TIMER_DELIVER_MS")
        || properties.contains_key("TIMER_DELAY_SEC")
        || properties.contains_key("TIMER_DELAY_MS")
    {
        return TopicMessageType::Delay;
    }

    TopicMessageType::Normal
}

// ============================================================================
// Metrics Configuration
// ============================================================================

/// Configuration for metrics exporter
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    pub exporter_type: MetricsExporterType,
    pub grpc_exporter_target: Option<String>,
    pub grpc_exporter_header: Option<String>,
    pub grpc_exporter_timeout_mills: u64,
    pub grpc_exporter_interval_mills: u64,
    pub prom_exporter_host: Option<String>,
    pub prom_exporter_port: u16,
    pub logging_exporter_interval_mills: u64,
    pub metrics_in_delta: bool,
    pub metrics_label: Option<String>,
    pub otel_cardinality_limit: u32,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            exporter_type: MetricsExporterType::Disable,
            grpc_exporter_target: None,
            grpc_exporter_header: None,
            grpc_exporter_timeout_mills: 3000,
            grpc_exporter_interval_mills: 60000,
            prom_exporter_host: None,
            prom_exporter_port: 5557,
            logging_exporter_interval_mills: 10000,
            metrics_in_delta: false,
            metrics_label: None,
            otel_cardinality_limit: 10000,
        }
    }
}

impl MetricsConfig {
    /// Check if metrics configuration is valid
    pub fn check_config(&self) -> bool {
        if !self.exporter_type.is_enable() {
            return false;
        }

        match self.exporter_type {
            MetricsExporterType::OtlpGrpc => self.grpc_exporter_target.is_some(),
            MetricsExporterType::Prom => true,
            MetricsExporterType::Log => true,
            MetricsExporterType::Disable => false,
        }
    }

    /// Parse labels from config string "key1:value1,key2:value2"
    pub fn parse_labels(&self) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        if let Some(ref label_str) = self.metrics_label {
            for pair in label_str.split(',') {
                let parts: Vec<&str> = pair.split(':').collect();
                if parts.len() == 2 {
                    labels.insert(parts[0].to_string(), parts[1].to_string());
                } else {
                    warn!("metricsLabel is not valid: {}", label_str);
                }
            }
        }
        labels
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_retry_or_dlq_topic() {
        assert!(is_retry_or_dlq_topic("%RETRY%test_group"));
        assert!(is_retry_or_dlq_topic("%DLQ%test_group"));
        assert!(!is_retry_or_dlq_topic("normal_topic"));
        assert!(!is_retry_or_dlq_topic(""));
    }

    #[test]
    fn test_is_system_group() {
        assert!(is_system_group("CID_RMQ_SYS_test"));
        assert!(!is_system_group("normal_group"));
        assert!(!is_system_group(""));
    }

    #[test]
    fn test_is_system() {
        // System topic
        assert!(is_system("SCHEDULE_TOPIC_XXXX", "normal_group"));
        // System group
        assert!(is_system("normal_topic", "CID_RMQ_SYS_test"));
        // Normal
        assert!(!is_system("normal_topic", "normal_group"));
    }

    #[test]
    fn test_message_size_buckets() {
        let buckets = get_message_size_buckets();
        assert_eq!(buckets.len(), 6);
        assert_eq!(buckets[0], 1024.0); // 1KB
        assert_eq!(buckets[5], 4.0 * 1024.0 * 1024.0); // 4MB
    }

    #[test]
    fn test_commit_latency_buckets() {
        let buckets = get_commit_latency_buckets();
        assert_eq!(buckets.len(), 6);
        assert_eq!(buckets[0], 5.0); // 5s
        assert_eq!(buckets[5], 24.0 * 60.0 * 60.0); // 24h
    }

    #[test]
    fn test_create_time_buckets() {
        let buckets = get_create_time_buckets();
        assert_eq!(buckets.len(), 5);
        assert_eq!(buckets[0], 10.0); // 10ms
        assert_eq!(buckets[4], 5000.0); // 5s
    }

    #[test]
    fn test_metrics_config_default() {
        let config = MetricsConfig::default();
        assert_eq!(config.exporter_type, MetricsExporterType::Disable);
        assert!(!config.check_config());
    }

    #[test]
    fn test_metrics_config_parse_labels() {
        let config = MetricsConfig {
            metrics_label: Some("key1:value1,key2:value2".to_string()),
            ..Default::default()
        };
        let labels = config.parse_labels();
        assert_eq!(labels.get("key1"), Some(&"value1".to_string()));
        assert_eq!(labels.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_counter_wrapper_nop() {
        let wrapper = CounterWrapper::Nop(NopLongCounter::new());
        // Should not panic
        wrapper.add(100, &[]);
    }

    #[test]
    fn test_histogram_wrapper_nop() {
        let wrapper = HistogramWrapper::Nop(NopLongHistogram::new());
        // Should not panic
        wrapper.record(100, &[]);
    }

    #[test]
    fn test_noop_attributes_supplier() {
        let supplier = NoopAttributesSupplier;
        assert!(supplier.get().is_empty());
    }

    #[test]
    fn test_new_attributes_builder_empty() {
        // Without global initialization, should return empty
        let attrs = new_attributes_builder();
        // May be empty or have some values depending on test order
        let _ = attrs;
    }
}
