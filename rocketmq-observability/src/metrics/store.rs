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

pub use crate::semantic::metrics::DELAY_MESSAGE_LATENCY;
pub use crate::semantic::metrics::STORAGE_DISPATCH_BEHIND_BYTES;
pub use crate::semantic::metrics::STORAGE_FLUSH_BEHIND_BYTES;
pub use crate::semantic::metrics::STORAGE_MESSAGE_RESERVE_TIME;
pub use crate::semantic::metrics::STORAGE_SIZE;
pub use crate::semantic::metrics::STORE_APPEND_LATENCY;
pub use crate::semantic::metrics::STORE_COMMITLOG_SEGMENT_LEASE_ACTIVE;
pub use crate::semantic::metrics::STORE_DISK_USAGE;
pub use crate::semantic::metrics::STORE_DISPATCH_LATENCY;
pub use crate::semantic::metrics::STORE_FLUSH_LATENCY;
pub use crate::semantic::metrics::STORE_HA_ACK_LATENCY_MILLIS;
pub use crate::semantic::metrics::STORE_HA_REPLICATION_LAG_BYTES;
pub use crate::semantic::metrics::STORE_LINUX_MLOCK_BYTES;
pub use crate::semantic::metrics::STORE_LINUX_PAGE_CACHE_WARMUP_MILLIS;
pub use crate::semantic::metrics::STORE_LINUX_SENDFILE_BYTES_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_BATCH_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_BYTES_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_ENGINE_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_FALLBACK_TOTAL;
pub use crate::semantic::metrics::STORE_TRANSFER_PARTIAL_WRITE_TOTAL;

#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static STORE_METRICS: OnceLock<StoreMetrics> = OnceLock::new();

#[derive(Debug, Clone, Copy, Default)]
pub struct StoreObservableValues {
    pub storage_size_bytes: i64,
    pub flush_behind_bytes: i64,
    pub dispatch_behind_bytes: i64,
    pub message_reserve_time_millis: i64,
}

#[cfg(feature = "otel-metrics")]
pub fn init_global(meter: &opentelemetry::metrics::Meter) -> bool {
    STORE_METRICS.set(StoreMetrics::new(meter)).is_ok()
}

#[cfg(feature = "otel-metrics")]
pub fn init_global_with_observables<F>(meter: &opentelemetry::metrics::Meter, source: F) -> bool
where
    F: Fn() -> StoreObservableValues + Send + Sync + 'static,
{
    STORE_METRICS
        .set(StoreMetrics::new_with_observables(meter, source))
        .is_ok()
}

pub fn record_append_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_append_latency(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_flush_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_flush_latency(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_dispatch_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_dispatch_latency(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_disk_usage(bytes: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_disk_usage(bytes, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = bytes;
}

pub fn record_delay_message_latency(latency_seconds: u64) {
    record_delay_message_latency_with_topic(latency_seconds, None);
}

pub fn record_delay_message_latency_with_topic(latency_seconds: u64, topic: Option<&str>) {
    #[cfg(feature = "otel-metrics")]
    {
        let Some(topic) = topic else {
            return;
        };
        if let Some(metrics) = STORE_METRICS.get() {
            let attrs = delay_message_latency_attributes(topic);
            metrics.record_delay_message_latency(latency_seconds, &attrs);
        }
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = (latency_seconds, topic);
}

pub fn record_delay_message_latency_from_timestamps(deliver_time_ms: i64, born_timestamp_ms: i64, topic: Option<&str>) {
    let latency_ms = deliver_time_ms.saturating_sub(born_timestamp_ms);
    if latency_ms > 0 {
        record_delay_message_latency_with_topic((latency_ms / 1000) as u64, topic);
    }
}

pub fn record_transfer_batch(count: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_transfer_batch_total(count, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_transfer_bytes(bytes: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_transfer_bytes_total(bytes, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = bytes;
}

pub fn record_transfer_engine(engine: &str, count: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_transfer_engine_total(count, &transfer_engine_attributes(engine));
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = (engine, count);
}

pub fn record_transfer_fallback(from: &str, to: &str, reason: &str, count: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_transfer_fallback_total(count, &transfer_fallback_attributes(from, to, reason));
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = (from, to, reason, count);
}

pub fn record_transfer_partial_write(count: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_transfer_partial_write_total(count, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_linux_sendfile_bytes(bytes: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_linux_sendfile_bytes_total(bytes, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = bytes;
}

pub fn record_ha_replication_lag_bytes(bytes: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_ha_replication_lag_bytes(bytes, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = bytes;
}

pub fn record_ha_ack_latency_millis(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_ha_ack_latency_millis(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_linux_mlock_bytes(bytes: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_linux_mlock_bytes(bytes, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = bytes;
}

pub fn record_linux_page_cache_warmup_millis(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_linux_page_cache_warmup_millis(latency_ms, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_commitlog_segment_lease_active(count: u64) {
    #[cfg(feature = "otel-metrics")]
    if let Some(metrics) = STORE_METRICS.get() {
        metrics.record_commitlog_segment_lease_active(count, &[]);
    }

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct StoreMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl StoreMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_append_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_flush_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_dispatch_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_disk_usage(&self, _bytes: u64) {}

    #[inline]
    pub fn record_delay_message_latency(&self, _latency_seconds: u64) {}

    #[inline]
    pub fn record_transfer_batch_total(&self, _count: u64) {}

    #[inline]
    pub fn record_transfer_bytes_total(&self, _bytes: u64) {}

    #[inline]
    pub fn record_transfer_engine_total(&self, _count: u64) {}

    #[inline]
    pub fn record_transfer_fallback_total(&self, _count: u64) {}

    #[inline]
    pub fn record_transfer_partial_write_total(&self, _count: u64) {}

    #[inline]
    pub fn record_linux_sendfile_bytes_total(&self, _bytes: u64) {}

    #[inline]
    pub fn record_ha_replication_lag_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_ha_ack_latency_millis(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_linux_mlock_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_linux_page_cache_warmup_millis(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_commitlog_segment_lease_active(&self, _count: u64) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct StoreMetrics {
    append_latency: opentelemetry::metrics::Histogram<u64>,
    flush_latency: opentelemetry::metrics::Histogram<u64>,
    dispatch_latency: opentelemetry::metrics::Histogram<u64>,
    disk_usage: opentelemetry::metrics::Gauge<u64>,
    delay_message_latency: opentelemetry::metrics::Histogram<u64>,
    transfer_batch_total: opentelemetry::metrics::Counter<u64>,
    transfer_bytes_total: opentelemetry::metrics::Counter<u64>,
    transfer_engine_total: opentelemetry::metrics::Counter<u64>,
    transfer_fallback_total: opentelemetry::metrics::Counter<u64>,
    transfer_partial_write_total: opentelemetry::metrics::Counter<u64>,
    linux_sendfile_bytes_total: opentelemetry::metrics::Counter<u64>,
    ha_replication_lag_bytes: opentelemetry::metrics::Gauge<u64>,
    ha_ack_latency_millis: opentelemetry::metrics::Histogram<u64>,
    linux_mlock_bytes: opentelemetry::metrics::Gauge<u64>,
    linux_page_cache_warmup_millis: opentelemetry::metrics::Histogram<u64>,
    commitlog_segment_lease_active: opentelemetry::metrics::Gauge<u64>,
}

#[cfg(feature = "otel-metrics")]
impl StoreMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let append_latency = meter
            .u64_histogram(STORE_APPEND_LATENCY)
            .with_description("Store commit log append latency")
            .with_unit("ms")
            .build();

        let flush_latency = meter
            .u64_histogram(STORE_FLUSH_LATENCY)
            .with_description("Store flush latency")
            .with_unit("ms")
            .build();

        let dispatch_latency = meter
            .u64_histogram(STORE_DISPATCH_LATENCY)
            .with_description("Store dispatch latency")
            .with_unit("ms")
            .build();

        let disk_usage = meter
            .u64_gauge(STORE_DISK_USAGE)
            .with_description("Store disk usage")
            .with_unit("By")
            .build();

        let delay_message_latency = meter
            .u64_histogram(DELAY_MESSAGE_LATENCY)
            .with_description("Timer message set latency distribution")
            .with_unit("seconds")
            .build();

        let transfer_batch_total = meter
            .u64_counter(STORE_TRANSFER_BATCH_TOTAL)
            .with_description("Total number of HA transfer batches")
            .with_unit("{batch}")
            .build();

        let transfer_bytes_total = meter
            .u64_counter(STORE_TRANSFER_BYTES_TOTAL)
            .with_description("Total HA transfer bytes")
            .with_unit("By")
            .build();

        let transfer_engine_total = meter
            .u64_counter(STORE_TRANSFER_ENGINE_TOTAL)
            .with_description("Total HA transfer engine selections")
            .with_unit("{transfer}")
            .build();

        let transfer_fallback_total = meter
            .u64_counter(STORE_TRANSFER_FALLBACK_TOTAL)
            .with_description("Total HA transfer engine fallbacks")
            .with_unit("{fallback}")
            .build();

        let transfer_partial_write_total = meter
            .u64_counter(STORE_TRANSFER_PARTIAL_WRITE_TOTAL)
            .with_description("Total HA transfer partial writes")
            .with_unit("{write}")
            .build();

        let linux_sendfile_bytes_total = meter
            .u64_counter(STORE_LINUX_SENDFILE_BYTES_TOTAL)
            .with_description("Total Linux sendfile bytes used by HA transfer")
            .with_unit("By")
            .build();

        let ha_replication_lag_bytes = meter
            .u64_gauge(STORE_HA_REPLICATION_LAG_BYTES)
            .with_description("HA replication lag in bytes")
            .with_unit("By")
            .build();

        let ha_ack_latency_millis = meter
            .u64_histogram(STORE_HA_ACK_LATENCY_MILLIS)
            .with_description("HA replication ack latency")
            .with_unit("ms")
            .build();

        let linux_mlock_bytes = meter
            .u64_gauge(STORE_LINUX_MLOCK_BYTES)
            .with_description("Current Linux mlock bytes tracked by store")
            .with_unit("By")
            .build();

        let linux_page_cache_warmup_millis = meter
            .u64_histogram(STORE_LINUX_PAGE_CACHE_WARMUP_MILLIS)
            .with_description("Linux page cache warmup latency")
            .with_unit("ms")
            .build();

        let commitlog_segment_lease_active = meter
            .u64_gauge(STORE_COMMITLOG_SEGMENT_LEASE_ACTIVE)
            .with_description("Active commitlog segment leases")
            .with_unit("{lease}")
            .build();

        Self {
            append_latency,
            flush_latency,
            dispatch_latency,
            disk_usage,
            delay_message_latency,
            transfer_batch_total,
            transfer_bytes_total,
            transfer_engine_total,
            transfer_fallback_total,
            transfer_partial_write_total,
            linux_sendfile_bytes_total,
            ha_replication_lag_bytes,
            ha_ack_latency_millis,
            linux_mlock_bytes,
            linux_page_cache_warmup_millis,
            commitlog_segment_lease_active,
        }
    }

    pub fn new_with_observables<F>(meter: &opentelemetry::metrics::Meter, source: F) -> Self
    where
        F: Fn() -> StoreObservableValues + Send + Sync + 'static,
    {
        let metrics = Self::new(meter);
        let source = std::sync::Arc::new(source);

        let storage_size_source = source.clone();
        let _storage_size = meter
            .i64_observable_gauge(STORAGE_SIZE)
            .with_description("Broker storage size")
            .with_unit("bytes")
            .with_callback(move |observer| {
                let values = storage_size_source();
                let attrs = store_attributes();
                observer.observe(values.storage_size_bytes.max(0), &attrs);
            })
            .build();

        let flush_behind_source = source.clone();
        let _flush_behind = meter
            .i64_observable_gauge(STORAGE_FLUSH_BEHIND_BYTES)
            .with_description("Broker flush behind bytes")
            .with_unit("bytes")
            .with_callback(move |observer| {
                let values = flush_behind_source();
                let attrs = store_attributes();
                observer.observe(values.flush_behind_bytes.max(0), &attrs);
            })
            .build();

        let dispatch_behind_source = source.clone();
        let _dispatch_behind = meter
            .i64_observable_gauge(STORAGE_DISPATCH_BEHIND_BYTES)
            .with_description("Broker dispatch behind bytes")
            .with_unit("bytes")
            .with_callback(move |observer| {
                let values = dispatch_behind_source();
                let attrs = store_attributes();
                observer.observe(values.dispatch_behind_bytes.max(0), &attrs);
            })
            .build();

        let _message_reserve_time = meter
            .i64_observable_gauge(STORAGE_MESSAGE_RESERVE_TIME)
            .with_description("Broker message reserve time")
            .with_unit("milliseconds")
            .with_callback(move |observer| {
                let values = source();
                let attrs = store_attributes();
                observer.observe(values.message_reserve_time_millis.max(0), &attrs);
            })
            .build();

        metrics
    }

    #[inline]
    pub fn record_append_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.append_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_flush_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.flush_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_dispatch_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.dispatch_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_disk_usage(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.disk_usage.record(bytes, attributes);
    }

    #[inline]
    pub fn record_delay_message_latency(&self, latency_seconds: u64, attributes: &[opentelemetry::KeyValue]) {
        self.delay_message_latency.record(latency_seconds, attributes);
    }

    #[inline]
    pub fn record_transfer_batch_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_batch_total.add(count, attributes);
    }

    #[inline]
    pub fn record_transfer_bytes_total(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_bytes_total.add(bytes, attributes);
    }

    #[inline]
    pub fn record_transfer_engine_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_engine_total.add(count, attributes);
    }

    #[inline]
    pub fn record_transfer_fallback_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_fallback_total.add(count, attributes);
    }

    #[inline]
    pub fn record_transfer_partial_write_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.transfer_partial_write_total.add(count, attributes);
    }

    #[inline]
    pub fn record_linux_sendfile_bytes_total(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_sendfile_bytes_total.add(bytes, attributes);
    }

    #[inline]
    pub fn record_ha_replication_lag_bytes(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.ha_replication_lag_bytes.record(bytes, attributes);
    }

    #[inline]
    pub fn record_ha_ack_latency_millis(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.ha_ack_latency_millis.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_linux_mlock_bytes(&self, bytes: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_mlock_bytes.record(bytes, attributes);
    }

    #[inline]
    pub fn record_linux_page_cache_warmup_millis(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.linux_page_cache_warmup_millis.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_commitlog_segment_lease_active(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.commitlog_segment_lease_active.record(count, attributes);
    }
}

#[cfg(feature = "otel-metrics")]
fn store_attributes() -> [opentelemetry::KeyValue; 2] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_TYPE, "local"),
        opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_MEDIUM, "disk"),
    ]
}

#[cfg(feature = "otel-metrics")]
fn delay_message_latency_attributes(topic: &str) -> Vec<opentelemetry::KeyValue> {
    let mut attrs = Vec::from(store_attributes());
    attrs.push(opentelemetry::KeyValue::new(
        crate::semantic::labels::TOPIC,
        topic.to_owned(),
    ));
    attrs
}

#[cfg(feature = "otel-metrics")]
fn transfer_engine_attributes(engine: &str) -> [opentelemetry::KeyValue; 1] {
    [opentelemetry::KeyValue::new(
        crate::semantic::labels::ENGINE,
        engine.to_owned(),
    )]
}

#[cfg(feature = "otel-metrics")]
fn transfer_fallback_attributes(from: &str, to: &str, reason: &str) -> [opentelemetry::KeyValue; 3] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::FROM, from.to_owned()),
        opentelemetry::KeyValue::new(crate::semantic::labels::TO, to.to_owned()),
        opentelemetry::KeyValue::new(crate::semantic::labels::REASON, reason.to_owned()),
    ]
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn store_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("store-metrics-test");
        let metrics = StoreMetrics::new(&meter);
        let attrs = [opentelemetry::KeyValue::new("store", "commitlog")];

        metrics.record_append_latency(5, &attrs);
        metrics.record_flush_latency(7, &attrs);
        metrics.record_dispatch_latency(9, &attrs);
        metrics.record_disk_usage(1024, &attrs);
        metrics.record_delay_message_latency(30, &attrs);
        metrics.record_transfer_batch_total(1, &[]);
        metrics.record_transfer_bytes_total(1024, &[]);
        metrics.record_transfer_engine_total(1, &transfer_engine_attributes("sendfile"));
        metrics.record_transfer_fallback_total(1, &transfer_fallback_attributes("io_uring", "vectored", "unsupported"));
        metrics.record_transfer_partial_write_total(2, &[]);
        metrics.record_linux_sendfile_bytes_total(512, &[]);
        metrics.record_ha_replication_lag_bytes(4096, &[]);
        metrics.record_ha_ack_latency_millis(12, &[]);
        metrics.record_linux_mlock_bytes(8192, &[]);
        metrics.record_linux_page_cache_warmup_millis(20, &[]);
        metrics.record_commitlog_segment_lease_active(3, &[]);
        record_delay_message_latency(30);
    }

    #[test]
    fn store_metrics_registers_observable_gauges() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("store-observable-metrics-test");
        let metrics = StoreMetrics::new_with_observables(&meter, || StoreObservableValues {
            storage_size_bytes: 100,
            flush_behind_bytes: 10,
            dispatch_behind_bytes: 20,
            message_reserve_time_millis: 30,
        });

        metrics.record_delay_message_latency(1, &[]);
    }

    #[test]
    fn delay_message_latency_attributes_include_real_topic() {
        let attrs = delay_message_latency_attributes("topic-a");

        assert!(attrs
            .iter()
            .any(|kv| kv.key.as_str() == crate::semantic::labels::TOPIC && kv.value.to_string() == "topic-a"));
    }
}

#[cfg(test)]
mod helper_tests {
    use super::*;

    #[test]
    fn delay_message_latency_from_timestamps_ignores_non_positive_latency() {
        record_delay_message_latency_from_timestamps(1, 2, Some("topic-a"));
        record_delay_message_latency_from_timestamps(2, 2, Some("topic-a"));
    }

    #[test]
    fn delay_message_latency_from_timestamps_records_positive_latency() {
        record_delay_message_latency_from_timestamps(2_000, 1_000, Some("topic-a"));
    }

    #[test]
    fn ha_transfer_recorders_are_safe_without_explicit_meter() {
        record_transfer_batch(1);
        record_transfer_bytes(1024);
        record_transfer_engine("sendfile", 1);
        record_transfer_fallback("io_uring", "vectored", "unsupported", 1);
        record_transfer_partial_write(2);
        record_linux_sendfile_bytes(512);
        record_ha_replication_lag_bytes(4096);
        record_ha_ack_latency_millis(12);
        record_linux_mlock_bytes(8192);
        record_linux_page_cache_warmup_millis(20);
        record_commitlog_segment_lease_active(3);
    }
}
