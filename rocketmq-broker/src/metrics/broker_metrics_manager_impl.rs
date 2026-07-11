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

use rocketmq_model::topic::is_system_topic;
use rocketmq_model::topic::TopicMessageType;
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_model::version::RocketMqVersion;
use tracing::warn;

use rocketmq_observability::config::MetricsExporter;
use rocketmq_observability::metrics::auth::AuthMetricsSnapshot;
use rocketmq_observability::metrics::broker::BrokerMetrics;
use rocketmq_observability::metrics::broker_constants::BrokerMetricsConstant;
use rocketmq_observability::metrics::labels::LabelGuard;
use rocketmq_observability::metrics::noop_instruments::NopLongCounter;
use rocketmq_observability::metrics::noop_instruments::NopLongHistogram;
use rocketmq_observability::metrics::owner_instruments::Counter;
use rocketmq_observability::metrics::owner_instruments::Histogram;
use rocketmq_observability::metrics::owner_instruments::KeyValue;
use rocketmq_observability::metrics::owner_instruments::Meter;
use rocketmq_observability::metrics::owner_instruments::MeterProvider;
use rocketmq_observability::metrics::owner_instruments::SdkMeterProvider;
use rocketmq_observability::sampling::SamplingGate;

// ============================================================================
// Constants
// ============================================================================

/// System group prefixes for identifying system consumer groups
const SYSTEM_GROUP_PREFIX_LIST: &[&str] = &["cid_rmq_sys_"];
const PROTOCOL_TYPE_REMOTING: &str = "remoting";

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

/// Broker base attributes supplier.
pub struct BrokerAttributesSupplier {
    cluster: String,
    node_id: String,
}

impl BrokerAttributesSupplier {
    pub fn new(cluster: String, node_id: String) -> Self {
        Self { cluster, node_id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerConnectionAttributes {
    pub language: String,
    pub version: i32,
}

impl ProducerConnectionAttributes {
    pub fn new(language: impl Into<String>, version: i32) -> Self {
        Self {
            language: language.into(),
            version,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerConnectionAttributes {
    pub group: String,
    pub language: String,
    pub version: i32,
    pub consume_mode: String,
}

impl ConsumerConnectionAttributes {
    pub fn new(
        group: impl Into<String>,
        language: impl Into<String>,
        version: i32,
        consume_mode: impl Into<String>,
    ) -> Self {
        Self {
            group: group.into(),
            language: language.into(),
            version,
            consume_mode: consume_mode.into(),
        }
    }
}

impl AttributesBuilderSupplier for BrokerAttributesSupplier {
    fn get(&self) -> Vec<KeyValue> {
        vec![
            KeyValue::new(BrokerMetricsConstant::LABEL_CLUSTER_NAME, self.cluster.clone()),
            KeyValue::new(
                BrokerMetricsConstant::LABEL_NODE_TYPE,
                BrokerMetricsConstant::NODE_TYPE_BROKER,
            ),
            KeyValue::new(BrokerMetricsConstant::LABEL_NODE_ID, self.node_id.clone()),
        ]
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

#[cfg(feature = "otel-metrics")]
struct GuardedLabel {
    key_value: KeyValue,
    dropped: bool,
}

#[cfg(feature = "otel-metrics")]
fn guarded_bounded_label(label_guard: &RwLock<LabelGuard>, key: &'static str, value: &str) -> GuardedLabel {
    let (value, dropped) = label_guard
        .write()
        .map(|mut guard| {
            let (value, dropped) = guard.normalize_metric_label_with_outcome(key, value);
            (value.into_owned(), dropped)
        })
        .unwrap_or_else(|_| ("other".to_string(), true));
    GuardedLabel {
        key_value: KeyValue::new(key, value),
        dropped,
    }
}

fn connection_version_label_value(version: i32) -> String {
    let ordinal = u32::try_from(version).unwrap_or(u32::MAX);
    RocketMqVersion::from_ordinal(ordinal).name().to_lowercase()
}

fn remoting_protocol_type_label() -> KeyValue {
    KeyValue::new(BrokerMetricsConstant::LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_REMOTING)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerMetricsLabelConfig {
    pub cardinality_limit: usize,
    pub topic_label_enabled: bool,
    pub consumer_group_label_enabled: bool,
}

impl BrokerMetricsLabelConfig {
    pub fn new(cardinality_limit: usize, topic_label_enabled: bool, consumer_group_label_enabled: bool) -> Self {
        Self {
            cardinality_limit,
            topic_label_enabled,
            consumer_group_label_enabled,
        }
    }
}

impl Default for BrokerMetricsLabelConfig {
    fn default() -> Self {
        Self::new(10_000, true, true)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BrokerMetricsSamplingConfig {
    pub sample_ratio: f64,
}

impl BrokerMetricsSamplingConfig {
    pub fn new(sample_ratio: f64) -> Self {
        Self { sample_ratio }
    }
}

impl Default for BrokerMetricsSamplingConfig {
    fn default() -> Self {
        Self::new(1.0)
    }
}

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
    #[cfg(not(feature = "otel-metrics"))]
    messages_in_total: Counter<u64>,
    #[cfg(not(feature = "otel-metrics"))]
    messages_out_total: Counter<u64>,
    #[cfg(not(feature = "otel-metrics"))]
    throughput_in_total: Counter<u64>,
    #[cfg(not(feature = "otel-metrics"))]
    throughput_out_total: Counter<u64>,
    send_to_dlq_messages: Counter<u64>,
    commit_messages_total: Counter<u64>,
    rollback_messages_total: Counter<u64>,

    // Request metrics - Histograms
    #[cfg(not(feature = "otel-metrics"))]
    message_size: Histogram<u64>,
    #[cfg(feature = "otel-metrics")]
    broker_metrics: BrokerMetrics,
    topic_create_execute_time: Histogram<u64>,
    consumer_group_create_execute_time: Histogram<u64>,
    transaction_finish_latency: Histogram<u64>,

    // Meter reference for creating additional instruments
    meter: Meter,

    // Attributes supplier
    attributes_supplier: Arc<dyn AttributesBuilderSupplier>,

    sampled_metric_gate: SamplingGate,

    #[cfg(feature = "otel-metrics")]
    label_guard: RwLock<LabelGuard>,
}

impl BrokerMetricsManager {
    /// Create a new BrokerMetricsManager with OpenTelemetry Meter
    pub fn new(meter: Meter, attributes_supplier: Arc<dyn AttributesBuilderSupplier>) -> Self {
        Self::new_with_label_config(meter, attributes_supplier, BrokerMetricsLabelConfig::default())
    }

    pub fn new_with_label_config(
        meter: Meter,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_config: BrokerMetricsLabelConfig,
    ) -> Self {
        Self::new_with_label_and_sampling_config(
            meter,
            attributes_supplier,
            label_config,
            BrokerMetricsSamplingConfig::default(),
        )
    }

    pub fn new_with_label_and_sampling_config(
        meter: Meter,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_config: BrokerMetricsLabelConfig,
        sampling_config: BrokerMetricsSamplingConfig,
    ) -> Self {
        #[cfg(not(feature = "otel-metrics"))]
        let _ = label_config;

        // Request metrics - Counters
        #[cfg(not(feature = "otel-metrics"))]
        let messages_in_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_MESSAGES_IN_TOTAL)
            .with_description("Total number of incoming messages")
            .build();

        #[cfg(not(feature = "otel-metrics"))]
        let messages_out_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_MESSAGES_OUT_TOTAL)
            .with_description("Total number of outgoing messages")
            .build();

        #[cfg(not(feature = "otel-metrics"))]
        let throughput_in_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_THROUGHPUT_IN_TOTAL)
            .with_description("Total traffic of incoming messages")
            .build();

        #[cfg(not(feature = "otel-metrics"))]
        let throughput_out_total = meter
            .u64_counter(BrokerMetricsConstant::COUNTER_THROUGHPUT_OUT_TOTAL)
            .with_description("Total traffic of outgoing messages")
            .build();

        #[cfg(feature = "otel-metrics")]
        let broker_metrics = BrokerMetrics::new(&meter);

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
        #[cfg(not(feature = "otel-metrics"))]
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
            #[cfg(not(feature = "otel-metrics"))]
            messages_in_total,
            #[cfg(not(feature = "otel-metrics"))]
            messages_out_total,
            #[cfg(not(feature = "otel-metrics"))]
            throughput_in_total,
            #[cfg(not(feature = "otel-metrics"))]
            throughput_out_total,
            send_to_dlq_messages,
            commit_messages_total,
            rollback_messages_total,
            #[cfg(not(feature = "otel-metrics"))]
            message_size,
            #[cfg(feature = "otel-metrics")]
            broker_metrics,
            topic_create_execute_time,
            consumer_group_create_execute_time,
            transaction_finish_latency,
            meter,
            attributes_supplier,
            sampled_metric_gate: SamplingGate::new(sampling_config.sample_ratio),
            #[cfg(feature = "otel-metrics")]
            label_guard: RwLock::new(LabelGuard::new(
                label_config.cardinality_limit,
                label_config.topic_label_enabled,
                label_config.consumer_group_label_enabled,
            )),
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
        F5: Fn() -> Vec<(ProducerConnectionAttributes, i64)> + Send + Sync + 'static,
        F6: Fn() -> Vec<(ConsumerConnectionAttributes, i64)> + Send + Sync + 'static,
    {
        Self::init_with_observables_and_label_config(
            meter_provider,
            attributes_supplier,
            BrokerMetricsLabelConfig::default(),
            Some(processor_watermark_fn),
            broker_permission_fn,
            topic_num_fn,
            consumer_group_num_fn,
            producer_connections_fn,
            consumer_connections_fn,
        )
    }

    /// Initialize with observable gauges and an explicit label guard configuration.
    pub fn init_with_observables_and_label_config<F1, F2, F3, F4, F5, F6>(
        meter_provider: &SdkMeterProvider,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_config: BrokerMetricsLabelConfig,
        // Stats callbacks
        processor_watermark_fn: Option<F1>,
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
        F5: Fn() -> Vec<(ProducerConnectionAttributes, i64)> + Send + Sync + 'static,
        F6: Fn() -> Vec<(ConsumerConnectionAttributes, i64)> + Send + Sync + 'static,
    {
        Self::init_with_observables_and_configs(
            meter_provider,
            attributes_supplier,
            label_config,
            BrokerMetricsSamplingConfig::default(),
            processor_watermark_fn,
            broker_permission_fn,
            topic_num_fn,
            consumer_group_num_fn,
            producer_connections_fn,
            consumer_connections_fn,
        )
    }

    /// Initialize with observable gauges, label guard configuration, and sampling configuration.
    pub fn init_with_observables_and_configs<F1, F2, F3, F4, F5, F6>(
        meter_provider: &SdkMeterProvider,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_config: BrokerMetricsLabelConfig,
        sampling_config: BrokerMetricsSamplingConfig,
        // Stats callbacks
        processor_watermark_fn: Option<F1>,
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
        F5: Fn() -> Vec<(ProducerConnectionAttributes, i64)> + Send + Sync + 'static,
        F6: Fn() -> Vec<(ConsumerConnectionAttributes, i64)> + Send + Sync + 'static,
    {
        let meter = meter_provider.meter(BrokerMetricsConstant::OPEN_TELEMETRY_METER_NAME);
        let manager = Self::new_with_label_and_sampling_config(
            meter.clone(),
            attributes_supplier.clone(),
            label_config,
            sampling_config,
        );

        // Register stats observable gauges
        if let Some(processor_watermark_fn) = processor_watermark_fn {
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
        }

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
                        KeyValue::new(BrokerMetricsConstant::LABEL_LANGUAGE, attr.language.to_lowercase()),
                        KeyValue::new(
                            BrokerMetricsConstant::LABEL_VERSION,
                            connection_version_label_value(attr.version),
                        ),
                        remoting_protocol_type_label(),
                    ]);
                    observer.observe(count, &attrs);
                }
            })
            .build();

        #[cfg(feature = "otel-metrics")]
        let consumer_connection_label_guard = Arc::new(RwLock::new(LabelGuard::default()));
        let attrs6 = attributes_supplier;
        let _consumer_connection = meter
            .i64_observable_gauge(BrokerMetricsConstant::GAUGE_CONSUMER_CONNECTIONS)
            .with_description("Consumer connections")
            .with_callback(move |observer| {
                for (attr, count) in consumer_connections_fn() {
                    let mut attrs = attrs6.get();
                    #[cfg(feature = "otel-metrics")]
                    let consumer_group_label = guarded_bounded_label(
                        &consumer_connection_label_guard,
                        BrokerMetricsConstant::LABEL_CONSUMER_GROUP,
                        &attr.group,
                    )
                    .key_value;
                    #[cfg(not(feature = "otel-metrics"))]
                    let consumer_group_label =
                        KeyValue::new(BrokerMetricsConstant::LABEL_CONSUMER_GROUP, attr.group.clone());
                    attrs.extend([
                        consumer_group_label,
                        KeyValue::new(BrokerMetricsConstant::LABEL_LANGUAGE, attr.language.to_lowercase()),
                        KeyValue::new(
                            BrokerMetricsConstant::LABEL_VERSION,
                            connection_version_label_value(attr.version),
                        ),
                        remoting_protocol_type_label(),
                        KeyValue::new(
                            BrokerMetricsConstant::LABEL_CONSUME_MODE,
                            attr.consume_mode.to_lowercase(),
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
        Self::init_global_with_label_config(meter, attributes_supplier, BrokerMetricsLabelConfig::default());
    }

    pub fn init_global_with_label_config(
        meter: Meter,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_config: BrokerMetricsLabelConfig,
    ) {
        let _ = ATTRIBUTES_BUILDER_SUPPLIER.set(attributes_supplier.clone());
        let _ = LABEL_MAP.set(RwLock::new(HashMap::new()));
        let _ = BROKER_METRICS_MANAGER.set(Self::new_with_label_config(meter, attributes_supplier, label_config));
    }

    pub fn init_global_with_observables_and_label_config<F1, F2, F3, F4, F5, F6>(
        meter_provider: &SdkMeterProvider,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_config: BrokerMetricsLabelConfig,
        processor_watermark_fn: Option<F1>,
        broker_permission_fn: F2,
        topic_num_fn: F3,
        consumer_group_num_fn: F4,
        producer_connections_fn: F5,
        consumer_connections_fn: F6,
    ) where
        F1: Fn() -> Vec<(String, i64)> + Send + Sync + 'static,
        F2: Fn() -> i64 + Send + Sync + 'static,
        F3: Fn() -> i64 + Send + Sync + 'static,
        F4: Fn() -> i64 + Send + Sync + 'static,
        F5: Fn() -> Vec<(ProducerConnectionAttributes, i64)> + Send + Sync + 'static,
        F6: Fn() -> Vec<(ConsumerConnectionAttributes, i64)> + Send + Sync + 'static,
    {
        Self::init_global_with_observables_and_configs(
            meter_provider,
            attributes_supplier,
            label_config,
            BrokerMetricsSamplingConfig::default(),
            processor_watermark_fn,
            broker_permission_fn,
            topic_num_fn,
            consumer_group_num_fn,
            producer_connections_fn,
            consumer_connections_fn,
        );
    }

    pub fn init_global_with_observables_and_configs<F1, F2, F3, F4, F5, F6>(
        meter_provider: &SdkMeterProvider,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_config: BrokerMetricsLabelConfig,
        sampling_config: BrokerMetricsSamplingConfig,
        processor_watermark_fn: Option<F1>,
        broker_permission_fn: F2,
        topic_num_fn: F3,
        consumer_group_num_fn: F4,
        producer_connections_fn: F5,
        consumer_connections_fn: F6,
    ) where
        F1: Fn() -> Vec<(String, i64)> + Send + Sync + 'static,
        F2: Fn() -> i64 + Send + Sync + 'static,
        F3: Fn() -> i64 + Send + Sync + 'static,
        F4: Fn() -> i64 + Send + Sync + 'static,
        F5: Fn() -> Vec<(ProducerConnectionAttributes, i64)> + Send + Sync + 'static,
        F6: Fn() -> Vec<(ConsumerConnectionAttributes, i64)> + Send + Sync + 'static,
    {
        let _ = ATTRIBUTES_BUILDER_SUPPLIER.set(attributes_supplier.clone());
        let _ = LABEL_MAP.set(RwLock::new(HashMap::new()));
        let _ = BROKER_METRICS_MANAGER.set(Self::init_with_observables_and_configs(
            meter_provider,
            attributes_supplier,
            label_config,
            sampling_config,
            processor_watermark_fn,
            broker_permission_fn,
            topic_num_fn,
            consumer_group_num_fn,
            producer_connections_fn,
            consumer_connections_fn,
        ));
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

    #[inline]
    fn topic_label(&self, topic: &str) -> KeyValue {
        self.bounded_label(BrokerMetricsConstant::LABEL_TOPIC, topic)
    }

    #[inline]
    fn consumer_group_label(&self, consumer_group: &str) -> KeyValue {
        self.bounded_label(BrokerMetricsConstant::LABEL_CONSUMER_GROUP, consumer_group)
    }

    #[cfg(feature = "otel-metrics")]
    #[inline]
    fn bounded_label(&self, key: &'static str, value: &str) -> KeyValue {
        let label = guarded_bounded_label(&self.label_guard, key, value);
        if label.dropped {
            self.record_metric_label_dropped(key);
        }
        label.key_value
    }

    #[cfg(not(feature = "otel-metrics"))]
    #[inline]
    fn bounded_label(&self, key: &'static str, value: &str) -> KeyValue {
        KeyValue::new(key, value.to_owned())
    }

    /// Get the meter reference
    pub fn meter(&self) -> &Meter {
        &self.meter
    }

    #[cfg(feature = "otel-metrics")]
    fn record_metric_label_dropped(&self, label_key: &'static str) {
        let mut attrs = self.base_attributes();
        attrs.push(KeyValue::new("label_key", label_key));
        self.broker_metrics.record_metrics_label_dropped_total(1, &attrs);
    }

    #[cfg(feature = "otel-metrics")]
    pub fn dropped_metric_labels(&self) -> u64 {
        self.label_guard
            .read()
            .map(|guard| guard.dropped_labels())
            .unwrap_or_default()
    }

    pub fn register_auth_observable_gauge<F>(&self, auth_snapshot_fn: F)
    where
        F: Fn() -> Option<AuthMetricsSnapshot> + Send + Sync + 'static,
    {
        let attributes_supplier = self.attributes_supplier.clone();
        let _auth_metrics = self
            .meter
            .u64_observable_gauge(BrokerMetricsConstant::GAUGE_AUTH_METRIC_VALUE)
            .with_description("Current RocketMQ auth metric counter values")
            .with_callback(move |observer| {
                let Some(snapshot) = auth_snapshot_fn() else {
                    return;
                };

                for sample in snapshot.samples() {
                    let mut attrs = attributes_supplier.get();
                    attrs.push(KeyValue::new(BrokerMetricsConstant::LABEL_AUTH_METRIC, sample.name));
                    observer.observe(sample.value, &attrs);
                }
            })
            .build();
    }

    // ========================================================================
    // Messages In/Out Metrics
    // ========================================================================

    /// Record incoming message count
    pub fn inc_messages_in_total(&self, topic: &str, message_type: TopicMessageType, num: u64, is_system: bool) {
        let mut attrs = self.base_attributes();
        attrs.extend([
            self.topic_label(topic),
            KeyValue::new(
                BrokerMetricsConstant::LABEL_MESSAGE_TYPE,
                message_type.to_string().to_lowercase(),
            ),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_SYSTEM, is_system.to_string()),
        ]);
        #[cfg(feature = "otel-metrics")]
        self.broker_metrics.record_messages_in_total(num, &attrs);
        #[cfg(not(feature = "otel-metrics"))]
        self.messages_in_total.add(num, &attrs);
    }

    /// Record all Java-compatible incoming message metrics for a successful send.
    pub fn record_messages_in_success(
        &self,
        topic: &str,
        message_type: &TopicMessageType,
        num: u64,
        bytes: u64,
        message_size: u64,
        is_system: bool,
    ) {
        let mut message_attrs = self.base_attributes();
        message_attrs.extend([
            self.topic_label(topic),
            KeyValue::new(
                BrokerMetricsConstant::LABEL_MESSAGE_TYPE,
                message_type.to_string().to_lowercase(),
            ),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_SYSTEM, is_system.to_string()),
        ]);

        #[cfg(feature = "otel-metrics")]
        self.broker_metrics.record_messages_in_total(num, &message_attrs);
        #[cfg(not(feature = "otel-metrics"))]
        self.messages_in_total.add(num, &message_attrs);

        let mut throughput_attrs = self.base_attributes();
        throughput_attrs.push(self.topic_label(topic));
        #[cfg(feature = "otel-metrics")]
        self.broker_metrics.record_throughput_in_total(bytes, &throughput_attrs);
        #[cfg(not(feature = "otel-metrics"))]
        self.throughput_in_total.add(bytes, &throughput_attrs);

        let mut size_attrs = self.base_attributes();
        size_attrs.extend([
            self.topic_label(topic),
            KeyValue::new(
                BrokerMetricsConstant::LABEL_MESSAGE_TYPE,
                message_type.to_string().to_lowercase(),
            ),
        ]);
        #[cfg(feature = "otel-metrics")]
        self.broker_metrics.record_message_size(message_size, &size_attrs);
        #[cfg(not(feature = "otel-metrics"))]
        self.message_size.record(message_size, &size_attrs);
    }

    /// Record outgoing message count
    pub fn inc_messages_out_total(&self, topic: &str, consumer_group: &str, num: u64, is_retry: bool) {
        let is_system = is_system(topic, consumer_group);
        let mut attrs = self.base_attributes();
        attrs.extend([
            self.topic_label(topic),
            self.consumer_group_label(consumer_group),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_RETRY, is_retry.to_string()),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_SYSTEM, is_system.to_string()),
        ]);
        #[cfg(feature = "otel-metrics")]
        self.broker_metrics.record_messages_out_total(num, &attrs);
        #[cfg(not(feature = "otel-metrics"))]
        self.messages_out_total.add(num, &attrs);
    }

    // ========================================================================
    // Throughput Metrics
    // ========================================================================

    /// Record incoming throughput (bytes)
    pub fn inc_throughput_in_total(&self, topic: &str, bytes: u64) {
        let mut attrs = self.base_attributes();
        attrs.push(self.topic_label(topic));
        #[cfg(feature = "otel-metrics")]
        self.broker_metrics.record_throughput_in_total(bytes, &attrs);
        #[cfg(not(feature = "otel-metrics"))]
        self.throughput_in_total.add(bytes, &attrs);
    }

    /// Record outgoing throughput (bytes)
    pub fn inc_throughput_out_total(&self, topic: &str, consumer_group: &str, bytes: u64, is_retry: bool) {
        let is_system = is_system(topic, consumer_group);
        let mut attrs = self.base_attributes();
        attrs.extend([
            self.topic_label(topic),
            self.consumer_group_label(consumer_group),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_RETRY, is_retry.to_string()),
            KeyValue::new(BrokerMetricsConstant::LABEL_IS_SYSTEM, is_system.to_string()),
        ]);
        #[cfg(feature = "otel-metrics")]
        self.broker_metrics.record_throughput_out_total(bytes, &attrs);
        #[cfg(not(feature = "otel-metrics"))]
        self.throughput_out_total.add(bytes, &attrs);
    }

    // ========================================================================
    // Message Size Metrics
    // ========================================================================

    /// Record message size
    pub fn record_message_size(&self, topic: &str, message_type: TopicMessageType, size: u64) {
        let mut attrs = self.base_attributes();
        attrs.extend([
            self.topic_label(topic),
            KeyValue::new(
                BrokerMetricsConstant::LABEL_MESSAGE_TYPE,
                message_type.to_string().to_lowercase(),
            ),
        ]);
        #[cfg(feature = "otel-metrics")]
        self.broker_metrics.record_message_size(size, &attrs);
        #[cfg(not(feature = "otel-metrics"))]
        self.message_size.record(size, &attrs);
    }

    /// Record broker send message processing latency.
    pub fn record_send_message_latency(&self, topic: &str, latency_ms: u64) {
        if !self.sampled_metric_gate.should_sample() {
            return;
        }

        #[cfg(feature = "otel-metrics")]
        {
            let mut attrs = self.base_attributes();
            attrs.push(self.topic_label(topic));
            self.broker_metrics.record_send_message_latency(latency_ms, &attrs);
        }

        #[cfg(not(feature = "otel-metrics"))]
        {
            let _ = (topic, latency_ms);
        }
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
            self.topic_label(topic),
            self.consumer_group_label(consumer_group),
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
        attrs.push(self.topic_label(topic));
        self.commit_messages_total.add(num, &attrs);
    }

    /// Record rollback message count
    pub fn inc_rollback_messages(&self, topic: &str, num: u64) {
        let mut attrs = self.base_attributes();
        attrs.push(self.topic_label(topic));
        self.rollback_messages_total.add(num, &attrs);
    }

    /// Record transaction finish latency
    pub fn record_transaction_finish_latency(&self, topic: &str, latency_ms: u64) {
        let mut attrs = self.base_attributes();
        attrs.push(self.topic_label(topic));
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
    topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX)
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
    is_system_topic(topic) || is_system_group(group)
}

// ============================================================================
// Metrics Configuration
// ============================================================================

/// Configuration for metrics exporter
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    pub exporter_type: MetricsExporter,
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
            exporter_type: MetricsExporter::Disable,
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
        if !self.exporter_type.is_enabled() {
            return false;
        }

        match self.exporter_type {
            MetricsExporter::OtlpGrpc => self.grpc_exporter_target.is_some(),
            MetricsExporter::Prometheus => true,
            MetricsExporter::Log => true,
            MetricsExporter::Disable => false,
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
        assert_eq!(config.exporter_type, MetricsExporter::Disable);
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
    fn connection_version_label_uses_java_version_description() {
        let version = RocketMqVersion::V5_0_0.ordinal() as i32;

        assert_eq!(connection_version_label_value(version), "v5_0_0");
    }

    #[test]
    fn remoting_connection_protocol_label_matches_java_schema() {
        let label = remoting_protocol_type_label();

        assert_eq!(label.key.as_str(), BrokerMetricsConstant::LABEL_PROTOCOL_TYPE);
        assert_eq!(label.value.to_string(), "remoting");
    }

    #[test]
    fn test_noop_attributes_supplier() {
        let supplier = NoopAttributesSupplier;
        assert!(supplier.get().is_empty());
    }

    #[test]
    #[cfg(feature = "otel-metrics")]
    fn broker_metrics_manager_bounds_topic_and_consumer_group_labels() {
        let meter_provider = SdkMeterProvider::builder().build();
        let meter = meter_provider.meter("broker-label-guard-test");
        let manager = BrokerMetricsManager::new_with_label_config(
            meter,
            Arc::new(NoopAttributesSupplier),
            BrokerMetricsLabelConfig::new(1, true, true),
        );

        let topic_a = manager.topic_label("topic-a");
        let topic_b = manager.topic_label("topic-b");
        let group_a = manager.consumer_group_label("group-a");
        let group_b = manager.consumer_group_label("group-b");

        assert_eq!(topic_a.value.to_string(), "topic-a");
        assert_eq!(topic_b.value.to_string(), "other");
        assert_eq!(group_a.value.to_string(), "group-a");
        assert_eq!(group_b.value.to_string(), "other");
        assert_eq!(manager.dropped_metric_labels(), 2);
    }

    #[test]
    #[cfg(feature = "otel-metrics")]
    fn broker_metrics_manager_can_disable_topic_labels() {
        let meter_provider = SdkMeterProvider::builder().build();
        let meter = meter_provider.meter("broker-label-disable-test");
        let manager = BrokerMetricsManager::new_with_label_config(
            meter,
            Arc::new(NoopAttributesSupplier),
            BrokerMetricsLabelConfig::new(10, false, true),
        );

        assert_eq!(manager.topic_label("topic-a").value.to_string(), "other");
        assert_eq!(manager.consumer_group_label("group-a").value.to_string(), "group-a");
        assert_eq!(manager.dropped_metric_labels(), 1);
    }

    #[test]
    fn broker_metrics_sampling_config_defaults_to_full_recording() {
        let config = BrokerMetricsSamplingConfig::default();

        assert!((config.sample_ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn broker_metrics_manager_applies_sampling_config() {
        let meter_provider = SdkMeterProvider::builder().build();
        let meter = meter_provider.meter("broker-sampling-config-test");
        let manager = BrokerMetricsManager::new_with_label_and_sampling_config(
            meter,
            Arc::new(NoopAttributesSupplier),
            BrokerMetricsLabelConfig::default(),
            BrokerMetricsSamplingConfig::new(0.0),
        );

        assert!(!manager.sampled_metric_gate.should_sample());
    }

    #[test]
    fn test_register_auth_observable_gauge() {
        let meter_provider = SdkMeterProvider::builder().build();
        let meter = meter_provider.meter("auth-metrics-test");
        let manager = BrokerMetricsManager::new(meter, Arc::new(NoopAttributesSupplier));

        manager.register_auth_observable_gauge(|| {
            Some(AuthMetricsSnapshot {
                authentication_failures: 1,
                ..AuthMetricsSnapshot::default()
            })
        });
    }

    #[test]
    fn test_record_messages_in_success() {
        let meter_provider = SdkMeterProvider::builder().build();
        let meter = meter_provider.meter("messages-in-success-test");
        let manager = BrokerMetricsManager::new(meter, Arc::new(NoopAttributesSupplier));

        manager.record_messages_in_success("topic-a", &TopicMessageType::Normal, 2, 128, 64, false);
    }

    #[test]
    fn test_new_attributes_builder_empty() {
        // Without global initialization, should return empty
        let attrs = new_attributes_builder();
        // May be empty or have some values depending on test order
        let _ = attrs;
    }
}
