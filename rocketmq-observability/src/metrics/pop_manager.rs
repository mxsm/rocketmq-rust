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
//! - Uses explicit, instance-owned managers so broker runtimes do not share metric state
//! - Thread-safe by design leveraging OpenTelemetry's internal synchronization
//! - Zero-cost abstractions for hot path operations

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

use super::broker_constants::BrokerMetricsConstant;
use super::labels::MetricLabelPolicy;
use super::labels::METRIC_LABEL_SENTINEL;
use super::pop_constants::PopMetricsConstant;
use super::pop_revive_message_type::PopReviveMessageType;
use crate::TelemetryRecorder;

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
    broker_id: i64,
}

impl BrokerAttributesSupplier {
    pub fn new(cluster_name: String, _broker_name: String, broker_id: i64) -> Self {
        Self {
            cluster_name,
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

/// Pop metrics manager for tracking Pop consumption metrics using OpenTelemetry
///
/// This struct manages all Pop-related metrics including:
/// - Revive message put/get counters
/// - Pop buffer scan time histogram
/// - Retry message counter
///
/// ## Thread Safety
/// OpenTelemetry instruments are thread-safe by design.
pub struct PopMetricsManager {
    /// Fixed instance-owned meter used to register observable instruments.
    meter: Meter,

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

    /// Per-telemetry-runtime policy for topic and consumer-group labels.
    label_policy: MetricLabelPolicy,

    /// Shared lifecycle gate for production recorders; standalone test managers remain always on.
    telemetry: Option<TelemetryRecorder>,
}

impl PopMetricsManager {
    /// Creates a production manager bound to one telemetry runtime.
    #[must_use]
    pub fn from_telemetry(
        telemetry: TelemetryRecorder,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
    ) -> Option<Self> {
        let meter = telemetry.meter()?;
        let label_policy = telemetry.metric_label_policy();
        Some(Self::build(&meter, attributes_supplier, label_policy, Some(telemetry)))
    }

    /// Create a new PopMetricsManager with OpenTelemetry Meter
    ///
    /// # Arguments
    /// * `meter` - OpenTelemetry Meter for creating instruments
    /// * `attributes_supplier` - Supplier for common attributes
    #[cfg(test)]
    pub(crate) fn new(meter: &Meter, attributes_supplier: Arc<dyn AttributesBuilderSupplier>) -> Self {
        Self::new_with_label_policy(meter, attributes_supplier, MetricLabelPolicy::default())
    }

    /// Creates a manager that shares the supplied telemetry runtime's label policy.
    #[cfg(test)]
    pub(crate) fn new_with_label_policy(
        meter: &Meter,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_policy: MetricLabelPolicy,
    ) -> Self {
        Self::build(meter, attributes_supplier, label_policy, None)
    }

    fn build(
        meter: &Meter,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_policy: MetricLabelPolicy,
        telemetry: Option<TelemetryRecorder>,
    ) -> Self {
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
            meter: meter.clone(),
            pop_buffer_scan_time_consume,
            pop_revive_put_total,
            pop_revive_get_total,
            pop_revive_retry_message_total,
            attributes_supplier,
            label_policy,
            telemetry,
        }
    }

    /// Register observable gauges for buffer sizes and revive lag/latency.
    pub fn register_observables<F1, F2, F3>(&self, offset_size_fn: F1, ck_size_fn: F2, revive_services_fn: F3)
    where
        F1: Fn() -> i64 + Send + Sync + 'static,
        F2: Fn() -> i64 + Send + Sync + 'static,
        F3: Fn() -> Vec<(i32, i64, i64)> + Send + Sync + 'static, // (queue_id, lag, latency)
    {
        if !self.is_recording_active() {
            return;
        }
        let meter = &self.meter;

        // Register observable gauges
        let telemetry1 = self.telemetry.clone();
        let attrs_supplier1 = Arc::clone(&self.attributes_supplier);
        let _offset_gauge = meter
            .i64_observable_gauge(PopMetricsConstant::GAUGE_POP_OFFSET_BUFFER_SIZE)
            .with_description("The number of buffered offset")
            .with_callback(move |observer| {
                if !telemetry_allows_recording(telemetry1.as_ref()) {
                    return;
                }
                let mut attrs = attrs_supplier1.get();
                attrs.extend_from_slice(&[]);
                observer.observe(offset_size_fn(), &attrs);
            })
            .build();

        let telemetry2 = self.telemetry.clone();
        let attrs_supplier2 = Arc::clone(&self.attributes_supplier);
        let _ck_gauge = meter
            .i64_observable_gauge(PopMetricsConstant::GAUGE_POP_CHECKPOINT_BUFFER_SIZE)
            .with_description("The number of buffered checkpoint")
            .with_callback(move |observer| {
                if !telemetry_allows_recording(telemetry2.as_ref()) {
                    return;
                }
                let attrs = attrs_supplier2.get();
                observer.observe(ck_size_fn(), &attrs);
            })
            .build();

        let telemetry3 = self.telemetry.clone();
        let attrs_supplier3 = Arc::clone(&self.attributes_supplier);
        let revive_services_fn = Arc::new(revive_services_fn);
        let revive_services_fn_clone = revive_services_fn.clone();
        let _lag_gauge = meter
            .i64_observable_gauge(PopMetricsConstant::GAUGE_POP_REVIVE_LAG)
            .with_description("The processing lag of revive topic")
            .with_unit("messages")
            .with_callback(move |observer| {
                if !telemetry_allows_recording(telemetry3.as_ref()) {
                    return;
                }
                for (queue_id, lag, _latency) in revive_services_fn_clone() {
                    let mut attrs = attrs_supplier3.get();
                    attrs.push(KeyValue::new(PopMetricsConstant::LABEL_QUEUE_ID, queue_id.to_string()));
                    observer.observe(lag, &attrs);
                }
            })
            .build();

        let telemetry4 = self.telemetry.clone();
        let attrs_supplier4 = Arc::clone(&self.attributes_supplier);
        let _latency_gauge = meter
            .i64_observable_gauge(PopMetricsConstant::GAUGE_POP_REVIVE_LATENCY)
            .with_description("The processing latency of revive topic")
            .with_unit("milliseconds")
            .with_callback(move |observer| {
                if !telemetry_allows_recording(telemetry4.as_ref()) {
                    return;
                }
                for (queue_id, _lag, latency) in revive_services_fn() {
                    let mut attrs = attrs_supplier4.get();
                    attrs.push(KeyValue::new(PopMetricsConstant::LABEL_QUEUE_ID, queue_id.to_string()));
                    observer.observe(latency, &attrs);
                }
            })
            .build();
    }

    /// Create a manager and register its observable gauges on the same explicit meter.
    #[cfg(test)]
    pub(crate) fn init_with_observables<F1, F2, F3>(
        meter: &Meter,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        offset_size_fn: F1,
        ck_size_fn: F2,
        revive_services_fn: F3,
    ) -> Self
    where
        F1: Fn() -> i64 + Send + Sync + 'static,
        F2: Fn() -> i64 + Send + Sync + 'static,
        F3: Fn() -> Vec<(i32, i64, i64)> + Send + Sync + 'static,
    {
        Self::init_with_observables_and_label_policy(
            meter,
            attributes_supplier,
            MetricLabelPolicy::default(),
            offset_size_fn,
            ck_size_fn,
            revive_services_fn,
        )
    }

    /// Creates a manager with observable gauges and a shared telemetry label policy.
    #[cfg(test)]
    pub(crate) fn init_with_observables_and_label_policy<F1, F2, F3>(
        meter: &Meter,
        attributes_supplier: Arc<dyn AttributesBuilderSupplier>,
        label_policy: MetricLabelPolicy,
        offset_size_fn: F1,
        ck_size_fn: F2,
        revive_services_fn: F3,
    ) -> Self
    where
        F1: Fn() -> i64 + Send + Sync + 'static,
        F2: Fn() -> i64 + Send + Sync + 'static,
        F3: Fn() -> Vec<(i32, i64, i64)> + Send + Sync + 'static,
    {
        let manager = Self::new_with_label_policy(meter, attributes_supplier, label_policy);
        manager.register_observables(offset_size_fn, ck_size_fn, revive_services_fn);
        manager
    }

    /// Get base attributes from supplier
    #[inline]
    fn base_attributes(&self) -> Vec<KeyValue> {
        self.attributes_supplier.get()
    }

    #[inline]
    fn is_recording_active(&self) -> bool {
        telemetry_allows_recording(self.telemetry.as_ref())
    }

    // ========================================================================
    // Revive Put Metrics
    // ========================================================================

    /// Increment revive ACK put count
    /// Called when an ACK message is put to revive topic
    #[inline]
    pub fn inc_pop_revive_ack_put_count(&self, group: &str, topic: &str, status: impl Into<String>) {
        self.inc_pop_revive_put_count(group, topic, PopReviveMessageType::Ack, status, 1);
    }

    /// Increment revive checkpoint put count
    /// Called when a checkpoint is put to revive topic
    #[inline]
    pub fn inc_pop_revive_ck_put_count(&self, group: &str, topic: &str, status: impl Into<String>) {
        self.inc_pop_revive_put_count(group, topic, PopReviveMessageType::Ck, status, 1);
    }

    /// Increment revive put count with full parameters
    pub fn inc_pop_revive_put_count(
        &self,
        group: &str,
        topic: &str,
        message_type: PopReviveMessageType,
        status: impl Into<String>,
        num: u64,
    ) {
        if !self.is_recording_active() {
            return;
        }
        let status = status.into();
        let mut attrs = self.base_attributes();
        attrs.extend([
            bounded_label(&self.label_policy, BrokerMetricsConstant::LABEL_CONSUMER_GROUP, group),
            bounded_label(&self.label_policy, BrokerMetricsConstant::LABEL_TOPIC, topic),
            KeyValue::new(PopMetricsConstant::LABEL_REVIVE_MESSAGE_TYPE, message_type.as_str()),
            KeyValue::new(PopMetricsConstant::LABEL_PUT_STATUS, status),
        ]);

        self.pop_revive_put_total.add(num, &attrs);
    }

    // ========================================================================
    // Revive Get Metrics
    // ========================================================================

    /// Increment revive ACK get count
    /// Called when an ACK message is retrieved from revive topic
    #[inline]
    pub fn inc_pop_revive_ack_get_count(&self, group: &str, topic: &str, queue_id: i32) {
        self.inc_pop_revive_get_count(group, topic, PopReviveMessageType::Ack, queue_id, 1);
    }

    /// Increment revive checkpoint get count
    /// Called when a checkpoint is retrieved from revive topic
    #[inline]
    pub fn inc_pop_revive_ck_get_count(&self, group: &str, topic: &str, queue_id: i32) {
        self.inc_pop_revive_get_count(group, topic, PopReviveMessageType::Ck, queue_id, 1);
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
        if !self.is_recording_active() {
            return;
        }
        let mut attrs = self.base_attributes();
        attrs.extend([
            bounded_label(&self.label_policy, BrokerMetricsConstant::LABEL_CONSUMER_GROUP, group),
            bounded_label(&self.label_policy, BrokerMetricsConstant::LABEL_TOPIC, topic),
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
    pub fn inc_pop_revive_retry_message_count(&self, group: &str, topic: &str, status: impl Into<String>) {
        if !self.is_recording_active() {
            return;
        }
        let status = status.into();
        let mut attrs = self.base_attributes();
        attrs.extend([
            bounded_label(&self.label_policy, BrokerMetricsConstant::LABEL_CONSUMER_GROUP, group),
            bounded_label(&self.label_policy, BrokerMetricsConstant::LABEL_TOPIC, topic),
            KeyValue::new(PopMetricsConstant::LABEL_PUT_STATUS, status),
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
        if !self.is_recording_active() {
            return;
        }
        let attrs = self.base_attributes();
        self.pop_buffer_scan_time_consume.record(time_ms, &attrs);
    }
}

#[inline]
fn telemetry_allows_recording(telemetry: Option<&TelemetryRecorder>) -> bool {
    telemetry.is_none_or(TelemetryRecorder::is_active)
}

#[inline]
fn bounded_label(policy: &MetricLabelPolicy, key: &'static str, value: &str) -> KeyValue {
    let (value, dropped) = policy.normalize_metric_label_with_outcome(key, value);
    if dropped {
        KeyValue::new(key, METRIC_LABEL_SENTINEL)
    } else {
        KeyValue::new(key, value.into_owned())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider;
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
        manager.inc_pop_revive_put_count("test-group", "test-topic", PopReviveMessageType::Ack, "PUT_OK", 5);
    }

    #[test]
    fn test_inc_pop_revive_get_count() {
        let provider = create_test_meter_provider();
        let meter = provider.meter("test-meter");
        let manager = PopMetricsManager::new(&meter, Arc::new(NoopAttributesSupplier));

        manager.inc_pop_revive_get_count("test-group", "test-topic", PopReviveMessageType::Ck, 0, 10);
    }

    #[test]
    fn observable_metrics_use_the_explicit_meter() {
        let provider = create_test_meter_provider();
        let meter = provider.meter("pop-observable-instance");

        let manager = PopMetricsManager::init_with_observables(
            &meter,
            Arc::new(NoopAttributesSupplier),
            || 1,
            || 2,
            || vec![(0, 3, 4)],
        );

        manager.record_pop_buffer_scan_time_consume(5);
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
    fn pop_topic_and_group_attributes_share_the_bounded_policy() {
        let policy = MetricLabelPolicy::new(1, true, true);

        assert_eq!(
            bounded_label(&policy, BrokerMetricsConstant::LABEL_TOPIC, "topic-a")
                .value
                .to_string(),
            "topic-a"
        );
        assert_eq!(
            bounded_label(&policy, BrokerMetricsConstant::LABEL_TOPIC, "topic-b")
                .value
                .to_string(),
            METRIC_LABEL_SENTINEL
        );
        assert_eq!(
            bounded_label(&policy, BrokerMetricsConstant::LABEL_CONSUMER_GROUP, "group-a",)
                .value
                .to_string(),
            "group-a"
        );
        assert_eq!(
            bounded_label(&policy, BrokerMetricsConstant::LABEL_CONSUMER_GROUP, "group-b",)
                .value
                .to_string(),
            METRIC_LABEL_SENTINEL
        );
    }

    #[test]
    fn pop_manager_uses_the_injected_policy_state() {
        let provider = create_test_meter_provider();
        let meter = provider.meter("pop-shared-label-policy");
        let policy = MetricLabelPolicy::new(1, true, true);
        let manager =
            PopMetricsManager::new_with_label_policy(&meter, Arc::new(NoopAttributesSupplier), policy.clone());

        manager.inc_pop_revive_ack_put_count("group-a", "topic-a", "PUT_OK");
        manager.inc_pop_revive_ack_put_count("group-b", "topic-b", "PUT_OK");

        assert_eq!(policy.dropped_labels(), 2);
    }

    #[test]
    fn pop_manager_fails_closed_before_expanding_labels() {
        let mut config = crate::ObservabilityConfig {
            enabled: true,
            ..crate::ObservabilityConfig::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = crate::MetricsExporter::Disable;
        config.metrics.cardinality_limit = 1;
        let guard = crate::init_observability(&config).expect("test telemetry runtime should initialize");
        let handle = guard.handle();
        let policy = handle.metric_label_policy();
        let manager = PopMetricsManager::from_telemetry(
            handle.child(crate::BROKER_METER_SCOPE),
            Arc::new(NoopAttributesSupplier),
        )
        .expect("active telemetry should provide a broker meter");

        manager.inc_pop_revive_ack_put_count("group-a", "topic-a", "PUT_OK");
        assert_eq!(policy.normalize_topic("topic-a"), "topic-a");
        assert_eq!(policy.normalize_consumer_group("group-a"), "group-a");
        assert_eq!(policy.dropped_labels(), 0);

        guard
            .shutdown()
            .into_result()
            .expect("test telemetry runtime should shut down");
        manager.inc_pop_revive_ack_put_count("group-b", "topic-b", "PUT_OK");

        assert_eq!(policy.dropped_labels(), 0);
    }

    #[test]
    fn pop_metrics_source_has_no_global_manager() {
        let source = include_str!("pop_manager.rs");

        assert!(!source.contains(concat!("static ", "POP_METRICS_MANAGER")));
        assert!(!source.contains(concat!("PopMetricsManager::", "try_global")));
        assert!(!source.contains(concat!("init_", "global")));
    }
}
