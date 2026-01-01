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

//! # PopMetricsManager
//!
//! Manages Pop (Pull-on-Push) consumption metrics for RocketMQ broker using OpenTelemetry.
//!
//! ## Overview
//! This module provides metrics collection and reporting for Pop consumption mode,
//! including:
//! - Revive message put/get counters
//! - Pop buffer scan time histograms
//! - Revive lag and latency gauges
//! - Retry message counters
//!
//! ## Design Philosophy
//! - Uses OpenTelemetry Rust SDK for standard metrics instrumentation
//! - Provides both global static interface (Java-compatible) and instance-based API
//! - Thread-safe by design leveraging OpenTelemetry's internal synchronization
//! - Zero-cost abstractions for hot path operations

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::pop::ack_msg::AckMsg;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;

use super::broker_metrics_constant::BrokerMetricsConstant;
use super::pop_metrics_constant::PopMetricsConstant;
use super::pop_revive_message_type::PopReviveMessageType;

// ============================================================================
// Types and Traits
// ============================================================================

/// Trait for providing base attributes (cluster, node info, etc.)
/// This allows customizing the common attributes added to all metrics
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

/// Custom attributes supplier with cluster and node information
pub struct BrokerAttributesSupplier {
    cluster_name: String,
    broker_name: String,
    broker_id: i64,
}

impl BrokerAttributesSupplier {
    pub fn new(cluster_name: String, broker_name: String, broker_id: i64) -> Self {
        Self {
            cluster_name,
            broker_name,
            broker_id,
        }
    }
}

impl AttributesBuilderSupplier for BrokerAttributesSupplier {
    fn get(&self) -> Vec<KeyValue> {
        vec![
            KeyValue::new(BrokerMetricsConstant::LABEL_CLUSTER_NAME, self.cluster_name.clone()),
            KeyValue::new(BrokerMetricsConstant::LABEL_NODE_ID, self.broker_id.to_string()),
            KeyValue::new(
                BrokerMetricsConstant::LABEL_NODE_TYPE,
                BrokerMetricsConstant::NODE_TYPE_BROKER,
            ),
        ]
    }
}

// ============================================================================
// Metric View Configuration
// ============================================================================

/// Get the histogram bucket boundaries for pop buffer scan time
/// These buckets are used for the histogram aggregation
/// Equivalent to Java's rpcCostTimeBuckets in getMetricsView()
pub fn get_pop_buffer_scan_time_buckets() -> Vec<f64> {
    vec![
        Duration::from_millis(1).as_millis() as f64,
        Duration::from_millis(10).as_millis() as f64,
        Duration::from_millis(100).as_millis() as f64,
        Duration::from_secs(1).as_millis() as f64,
        Duration::from_secs(2).as_millis() as f64,
        Duration::from_secs(3).as_millis() as f64,
    ]
}

// ============================================================================
// PopMetricsManager - Core Implementation
// ============================================================================

/// Global singleton for PopMetricsManager
static POP_METRICS_MANAGER: OnceLock<PopMetricsManager> = OnceLock::new();

/// Pop metrics manager for tracking Pop consumption metrics using OpenTelemetry
///
/// This struct manages all Pop-related metrics including:
/// - Revive message put/get counters
/// - Pop buffer scan time histogram
/// - Retry message counter
///
/// ## Thread Safety
/// OpenTelemetry instruments are thread-safe by design.
///
/// ## Usage
/// ```ignore
/// // Initialize globally with a meter
/// let meter = global::meter("broker-meter");
/// PopMetricsManager::init_global(meter, Arc::new(NoopAttributesSupplier));
///
/// // Record metrics
/// PopMetricsManager::global().inc_pop_revive_put_count(
///     "group1",
///     "topic1",
///     PopReviveMessageType::Ack,
///     PutMessageStatus::PutOk,
///     1,
/// );
/// ```
pub struct PopMetricsManager {
    /// Histogram for pop buffer scan time (milliseconds)
    pop_buffer_scan_time_consume: Histogram<u64>,

    /// Counter for revive put operations (messages put to revive topic)
    pop_revive_put_total: Counter<u64>,

    /// Counter for revive get operations (messages read from revive topic)
    pop_revive_get_total: Counter<u64>,

    /// Counter for retry messages (messages put to pop retry topic)
    pop_revive_retry_message_total: Counter<u64>,

    /// Attributes builder supplier for common labels
    attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
}

impl PopMetricsManager {
    /// Create a new PopMetricsManager with OpenTelemetry Meter
    ///
    /// # Arguments
    /// * `meter` - OpenTelemetry Meter for creating instruments
    /// * `attributes_supplier` - Supplier for common attributes
    pub fn new(meter: &Meter, attributes_supplier: Arc<dyn AttributesBuilderSupplier>) -> Self {
        // Create histogram for pop buffer scan time
        let pop_buffer_scan_time_consume = meter
            .u64_histogram(PopMetricsConstant::HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME)
            .with_description("Time consuming of pop buffer scan")
            .with_unit("milliseconds")
            .build();

        // Create counter for revive put operations
        let pop_revive_put_total = meter
            .u64_counter(PopMetricsConstant::COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL)
            .with_description("Total number of put message to revive topic")
            .build();

        // Create counter for revive get operations
        let pop_revive_get_total = meter
            .u64_counter(PopMetricsConstant::COUNTER_POP_REVIVE_OUT_MESSAGE_TOTAL)
            .with_description("Total number of get message from revive topic")
            .build();

        // Create counter for retry messages
        let pop_revive_retry_message_total = meter
            .u64_counter(PopMetricsConstant::COUNTER_POP_REVIVE_RETRY_MESSAGES_TOTAL)
            .with_description("Total number of put message to pop retry topic")
            .build();

        Self {
            pop_buffer_scan_time_consume,
            pop_revive_put_total,
            pop_revive_get_total,
            pop_revive_retry_message_total,
            attributes_supplier,
        }
    }

    /// Initialize metrics with the given MeterProvider
    /// This also registers observable gauges for buffer sizes and revive lag/latency
    ///
    /// # Arguments
    /// * `meter_provider` - OpenTelemetry SDK MeterProvider
    /// * `attributes_supplier` - Supplier for common attributes
    /// * `offset_size_fn` - Callback to get pop buffer offset size
    /// * `ck_size_fn` - Callback to get pop buffer checkpoint size
    /// * `revive_services_fn` - Callback to get revive service metrics
    pub fn init_with_observables<F1, F2, F3>(
        meter_provider: &SdkMeterProvider,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        offset_size_fn: F1,
        ck_size_fn: F2,
        revive_services_fn: F3,
    ) -> Self
    where
        F1: Fn() -> i64 + Send + Sync + 'static,
        F2: Fn() -> i64 + Send + Sync + 'static,
        F3: Fn() -> Vec<(i32, i64, i64)> + Send + Sync + 'static, // (queue_id, lag, latency)
    {
        let meter = meter_provider.meter(BrokerMetricsConstant::OPEN_TELEMETRY_METER_NAME);

        // Create the manager with standard instruments
        let manager = Self::new(&meter, attributes_supplier.clone());

        // Register observable gauges
        let attrs_supplier1 = attributes_supplier.clone();
        let _offset_gauge = meter
            .i64_observable_gauge(PopMetricsConstant::GAUGE_POP_OFFSET_BUFFER_SIZE)
            .with_description("The number of buffered offset")
            .with_callback(move |observer| {
                let mut attrs = attrs_supplier1.get();
                attrs.extend_from_slice(&[]);
                observer.observe(offset_size_fn(), &attrs);
            })
            .build();

        let attrs_supplier2 = attributes_supplier.clone();
        let _ck_gauge = meter
            .i64_observable_gauge(PopMetricsConstant::GAUGE_POP_CHECKPOINT_BUFFER_SIZE)
            .with_description("The number of buffered checkpoint")
            .with_callback(move |observer| {
                let attrs = attrs_supplier2.get();
                observer.observe(ck_size_fn(), &attrs);
            })
            .build();

        let attrs_supplier3 = attributes_supplier.clone();
        let revive_services_fn = Arc::new(revive_services_fn);
        let revive_services_fn_clone = revive_services_fn.clone();
        let _lag_gauge = meter
            .i64_observable_gauge(PopMetricsConstant::GAUGE_POP_REVIVE_LAG)
            .with_description("The processing lag of revive topic")
            .with_unit("messages")
            .with_callback(move |observer| {
                for (queue_id, lag, _latency) in revive_services_fn_clone() {
                    let mut attrs = attrs_supplier3.get();
                    attrs.push(KeyValue::new(PopMetricsConstant::LABEL_QUEUE_ID, queue_id.to_string()));
                    observer.observe(lag, &attrs);
                }
            })
            .build();

        let attrs_supplier4 = attributes_supplier;
        let _latency_gauge = meter
            .i64_observable_gauge(PopMetricsConstant::GAUGE_POP_REVIVE_LATENCY)
            .with_description("The processing latency of revive topic")
            .with_unit("milliseconds")
            .with_callback(move |observer| {
                for (queue_id, _lag, latency) in revive_services_fn() {
                    let mut attrs = attrs_supplier4.get();
                    attrs.push(KeyValue::new(PopMetricsConstant::LABEL_QUEUE_ID, queue_id.to_string()));
                    observer.observe(latency, &attrs);
                }
            })
            .build();

        manager
    }

    /// Initialize the global PopMetricsManager
    pub fn init_global(meter: &Meter, attributes_supplier: Arc<dyn AttributesBuilderSupplier>) {
        let _ = POP_METRICS_MANAGER.set(Self::new(meter, attributes_supplier));
    }

    /// Get the global PopMetricsManager instance
    /// Returns None if not initialized
    pub fn try_global() -> Option<&'static PopMetricsManager> {
        POP_METRICS_MANAGER.get()
    }

    /// Get base attributes from supplier
    #[inline]
    fn base_attributes(&self) -> Vec<KeyValue> {
        self.attributes_supplier.get()
    }

    // ========================================================================
    // Revive Put Metrics
    // ========================================================================

    /// Increment revive ACK put count
    /// Called when an ACK message is put to revive topic
    #[inline]
    pub fn inc_pop_revive_ack_put_count(&self, ack_msg: &AckMsg, status: PutMessageStatus) {
        self.inc_pop_revive_put_count(
            ack_msg.consumer_group.as_str(),
            ack_msg.topic.as_str(),
            PopReviveMessageType::Ack,
            status,
            1,
        );
    }

    /// Increment revive checkpoint put count
    /// Called when a checkpoint is put to revive topic
    #[inline]
    pub fn inc_pop_revive_ck_put_count(&self, check_point: &PopCheckPoint, status: PutMessageStatus) {
        self.inc_pop_revive_put_count(
            check_point.cid.as_str(),
            check_point.topic.as_str(),
            PopReviveMessageType::Ck,
            status,
            1,
        );
    }

    /// Increment revive put count with full parameters
    pub fn inc_pop_revive_put_count(
        &self,
        group: &str,
        topic: &str,
        message_type: PopReviveMessageType,
        status: PutMessageStatus,
        num: u64,
    ) {
        let mut attrs = self.base_attributes();
        attrs.extend([
            KeyValue::new(BrokerMetricsConstant::LABEL_CONSUMER_GROUP, group.to_owned()),
            KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()),
            KeyValue::new(PopMetricsConstant::LABEL_REVIVE_MESSAGE_TYPE, message_type.as_str()),
            KeyValue::new(PopMetricsConstant::LABEL_PUT_STATUS, status.to_string()),
        ]);

        self.pop_revive_put_total.add(num, &attrs);
    }

    // ========================================================================
    // Revive Get Metrics
    // ========================================================================

    /// Increment revive ACK get count
    /// Called when an ACK message is retrieved from revive topic
    #[inline]
    pub fn inc_pop_revive_ack_get_count(&self, ack_msg: &AckMsg, queue_id: i32) {
        self.inc_pop_revive_get_count(
            ack_msg.consumer_group.as_str(),
            ack_msg.topic.as_str(),
            PopReviveMessageType::Ack,
            queue_id,
            1,
        );
    }

    /// Increment revive checkpoint get count
    /// Called when a checkpoint is retrieved from revive topic
    #[inline]
    pub fn inc_pop_revive_ck_get_count(&self, check_point: &PopCheckPoint, queue_id: i32) {
        self.inc_pop_revive_get_count(
            check_point.cid.as_str(),
            check_point.topic.as_str(),
            PopReviveMessageType::Ck,
            queue_id,
            1,
        );
    }

    /// Increment revive get count with full parameters
    pub fn inc_pop_revive_get_count(
        &self,
        group: &str,
        topic: &str,
        message_type: PopReviveMessageType,
        queue_id: i32,
        num: u64,
    ) {
        let mut attrs = self.base_attributes();
        attrs.extend([
            KeyValue::new(BrokerMetricsConstant::LABEL_CONSUMER_GROUP, group.to_owned()),
            KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, topic.to_owned()),
            KeyValue::new(PopMetricsConstant::LABEL_QUEUE_ID, queue_id.to_string()),
            KeyValue::new(PopMetricsConstant::LABEL_REVIVE_MESSAGE_TYPE, message_type.as_str()),
        ]);

        self.pop_revive_get_total.add(num, &attrs);
    }

    // ========================================================================
    // Retry Message Metrics
    // ========================================================================

    /// Increment retry message count
    /// Called when a message is put to pop retry topic
    pub fn inc_pop_revive_retry_message_count(&self, check_point: &PopCheckPoint, status: PutMessageStatus) {
        let mut attrs = self.base_attributes();
        attrs.extend([
            KeyValue::new(BrokerMetricsConstant::LABEL_CONSUMER_GROUP, check_point.cid.to_string()),
            KeyValue::new(BrokerMetricsConstant::LABEL_TOPIC, check_point.topic.to_string()),
            KeyValue::new(PopMetricsConstant::LABEL_PUT_STATUS, status.to_string()),
        ]);

        self.pop_revive_retry_message_total.add(1, &attrs);
    }

    // ========================================================================
    // Buffer Scan Metrics
    // ========================================================================

    /// Record pop buffer scan time consumption
    /// Called after each pop buffer scan operation
    #[inline]
    pub fn record_pop_buffer_scan_time_consume(&self, time_ms: u64) {
        let attrs = self.base_attributes();
        self.pop_buffer_scan_time_consume.record(time_ms, &attrs);
    }
}

// ============================================================================
// Static Helper Functions (Java-compatible API)
// ============================================================================

/// Static function to increment revive ACK put count
/// Equivalent to Java's static method
#[inline]
pub fn inc_pop_revive_ack_put_count(ack_msg: &AckMsg, status: PutMessageStatus) {
    if let Some(manager) = PopMetricsManager::try_global() {
        manager.inc_pop_revive_ack_put_count(ack_msg, status);
    }
}

/// Static function to increment revive checkpoint put count
#[inline]
pub fn inc_pop_revive_ck_put_count(check_point: &PopCheckPoint, status: PutMessageStatus) {
    if let Some(manager) = PopMetricsManager::try_global() {
        manager.inc_pop_revive_ck_put_count(check_point, status);
    }
}

/// Static function to increment revive put count with full parameters
#[inline]
pub fn inc_pop_revive_put_count(
    group: &str,
    topic: &str,
    message_type: PopReviveMessageType,
    status: PutMessageStatus,
    num: u64,
) {
    if let Some(manager) = PopMetricsManager::try_global() {
        manager.inc_pop_revive_put_count(group, topic, message_type, status, num);
    }
}

/// Static function to increment revive ACK get count
#[inline]
pub fn inc_pop_revive_ack_get_count(ack_msg: &AckMsg, queue_id: i32) {
    if let Some(manager) = PopMetricsManager::try_global() {
        manager.inc_pop_revive_ack_get_count(ack_msg, queue_id);
    }
}

/// Static function to increment revive checkpoint get count
#[inline]
pub fn inc_pop_revive_ck_get_count(check_point: &PopCheckPoint, queue_id: i32) {
    if let Some(manager) = PopMetricsManager::try_global() {
        manager.inc_pop_revive_ck_get_count(check_point, queue_id);
    }
}

/// Static function to increment revive get count with full parameters
#[inline]
pub fn inc_pop_revive_get_count(group: &str, topic: &str, message_type: PopReviveMessageType, queue_id: i32, num: u64) {
    if let Some(manager) = PopMetricsManager::try_global() {
        manager.inc_pop_revive_get_count(group, topic, message_type, queue_id, num);
    }
}

/// Static function to increment retry message count
#[inline]
pub fn inc_pop_revive_retry_message_count(check_point: &PopCheckPoint, status: PutMessageStatus) {
    if let Some(manager) = PopMetricsManager::try_global() {
        manager.inc_pop_revive_retry_message_count(check_point, status);
    }
}

/// Static function to record pop buffer scan time
#[inline]
pub fn record_pop_buffer_scan_time_consume(time_ms: u64) {
    if let Some(manager) = PopMetricsManager::try_global() {
        manager.record_pop_buffer_scan_time_consume(time_ms);
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    fn create_test_meter_provider() -> SdkMeterProvider {
        // Create a no-op meter provider for testing
        // The default SdkMeterProvider works for basic instrument creation tests
        SdkMeterProvider::default()
    }

    #[test]
    fn test_pop_metrics_manager_creation() {
        let provider = create_test_meter_provider();
        let meter = provider.meter("test-meter");
        let manager = PopMetricsManager::new(&meter, Arc::new(NoopAttributesSupplier));

        // Verify instruments are created (no panic)
        manager.record_pop_buffer_scan_time_consume(100);
    }

    #[test]
    fn test_inc_pop_revive_put_count() {
        let provider = create_test_meter_provider();
        let meter = provider.meter("test-meter");
        let manager = PopMetricsManager::new(&meter, Arc::new(NoopAttributesSupplier));

        // Test the method doesn't panic
        manager.inc_pop_revive_put_count(
            "test-group",
            "test-topic",
            PopReviveMessageType::Ack,
            PutMessageStatus::PutOk,
            5,
        );
    }

    #[test]
    fn test_inc_pop_revive_get_count() {
        let provider = create_test_meter_provider();
        let meter = provider.meter("test-meter");
        let manager = PopMetricsManager::new(&meter, Arc::new(NoopAttributesSupplier));

        manager.inc_pop_revive_get_count("test-group", "test-topic", PopReviveMessageType::Ck, 0, 10);
    }

    #[test]
    fn test_broker_attributes_supplier() {
        let supplier = BrokerAttributesSupplier::new("test-cluster".to_string(), "broker-0".to_string(), 0);

        let attrs = supplier.get();
        assert_eq!(attrs.len(), 3);

        // Verify cluster attribute
        assert!(attrs
            .iter()
            .any(|kv| kv.key.as_str() == BrokerMetricsConstant::LABEL_CLUSTER_NAME));
    }

    #[test]
    fn test_get_pop_buffer_scan_time_buckets() {
        let buckets = get_pop_buffer_scan_time_buckets();
        assert_eq!(buckets.len(), 6);
        assert_eq!(buckets[0], 1.0);
        assert_eq!(buckets[5], 3000.0);
    }

    #[test]
    fn test_static_functions_no_panic_when_uninitialized() {
        // These should not panic even when global manager is not initialized
        inc_pop_revive_put_count("group", "topic", PopReviveMessageType::Ack, PutMessageStatus::PutOk, 1);
        inc_pop_revive_get_count("group", "topic", PopReviveMessageType::Ck, 0, 1);
        record_pop_buffer_scan_time_consume(100);
    }
}
