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

pub use crate::semantic::metrics::ROCKSDB_BYTES_READ;
pub use crate::semantic::metrics::ROCKSDB_BYTES_WRITTEN;
pub use crate::semantic::metrics::ROCKSDB_RATE_CACHE_HIT;
pub use crate::semantic::metrics::ROCKSDB_READ_AMPLIFICATION_BYTES;
pub use crate::semantic::metrics::ROCKSDB_TIMES_COMPRESSED;
pub use crate::semantic::metrics::ROCKSDB_TIMES_READ;
pub use crate::semantic::metrics::ROCKSDB_TIMES_WRITTEN_OTHER;
pub use crate::semantic::metrics::ROCKSDB_TIMES_WRITTEN_SELF;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
#[cfg(feature = "otel-metrics")]
use std::sync::Arc;
#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static ROCKSDB_METRICS: OnceLock<RocksDbOtelMetrics> = OnceLock::new();

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RocksDbMetrics {
    pub write_count: u64,
    pub read_count: u64,
    pub batch_write_count: u64,
    pub scan_count: u64,
    pub flush_count: u64,
    pub manual_compaction_count: u64,
    pub checkpoint_count: u64,
    pub backup_count: u64,
    pub property_query_count: u64,
    pub error_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct RocksDbTickerMetrics {
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub times_written_self: u64,
    pub times_written_other: u64,
    pub block_cache_hit: u64,
    pub block_cache_miss: u64,
    pub times_compressed: u64,
    pub read_amplification_bytes: u64,
    pub times_read: u64,
}

#[derive(Debug, Default)]
pub struct RocksDbMetricsCollector {
    write_count: AtomicU64,
    read_count: AtomicU64,
    batch_write_count: AtomicU64,
    scan_count: AtomicU64,
    flush_count: AtomicU64,
    manual_compaction_count: AtomicU64,
    checkpoint_count: AtomicU64,
    backup_count: AtomicU64,
    property_query_count: AtomicU64,
    error_count: AtomicU64,
}

impl RocksDbMetricsCollector {
    pub fn snapshot(&self) -> RocksDbMetrics {
        RocksDbMetrics {
            write_count: self.write_count.load(Ordering::Relaxed),
            read_count: self.read_count.load(Ordering::Relaxed),
            batch_write_count: self.batch_write_count.load(Ordering::Relaxed),
            scan_count: self.scan_count.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
            manual_compaction_count: self.manual_compaction_count.load(Ordering::Relaxed),
            checkpoint_count: self.checkpoint_count.load(Ordering::Relaxed),
            backup_count: self.backup_count.load(Ordering::Relaxed),
            property_query_count: self.property_query_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
        }
    }

    pub fn record_write(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_read(&self) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_batch_write(&self) {
        self.batch_write_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_scan(&self) {
        self.scan_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_flush(&self) {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_manual_compaction(&self) {
        self.manual_compaction_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_checkpoint(&self) {
        self.checkpoint_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_backup(&self) {
        self.backup_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_property_query(&self) {
        self.property_query_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RocksDbObservableValues {
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub times_written_self: u64,
    pub times_written_other: u64,
    pub block_cache_hit: u64,
    pub block_cache_miss: u64,
    pub times_compressed: u64,
    pub read_amplification_bytes: u64,
    pub times_read: u64,
}

#[cfg(feature = "otel-metrics")]
pub fn init_global_with_observables<F>(meter: &opentelemetry::metrics::Meter, source: F) -> bool
where
    F: Fn() -> RocksDbObservableValues + Send + Sync + 'static,
{
    ROCKSDB_METRICS
        .set(RocksDbOtelMetrics::new_with_observables(meter, source))
        .is_ok()
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct RocksDbOtelMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl RocksDbOtelMetrics {
    pub fn noop() -> Self {
        Self
    }
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct RocksDbOtelMetrics;

#[cfg(feature = "otel-metrics")]
impl RocksDbOtelMetrics {
    pub fn new_with_observables<F>(meter: &opentelemetry::metrics::Meter, source: F) -> Self
    where
        F: Fn() -> RocksDbObservableValues + Send + Sync + 'static,
    {
        let source = Arc::new(source);

        let bytes_written_source = source.clone();
        let _bytes_written = meter
            .u64_observable_gauge(ROCKSDB_BYTES_WRITTEN)
            .with_description("The cumulative number of bytes written to the database.")
            .with_unit("By")
            .with_callback(move |observer| {
                let values = bytes_written_source();
                let attrs = rocksdb_attributes();
                observer.observe(values.bytes_written, &attrs);
            })
            .build();

        let bytes_read_source = source.clone();
        let _bytes_read = meter
            .u64_observable_gauge(ROCKSDB_BYTES_READ)
            .with_description("The cumulative number of bytes read from the database.")
            .with_unit("By")
            .with_callback(move |observer| {
                let values = bytes_read_source();
                let attrs = rocksdb_attributes();
                observer.observe(values.bytes_read, &attrs);
            })
            .build();

        let times_written_self_source = source.clone();
        let _times_written_self = meter
            .u64_observable_gauge(ROCKSDB_TIMES_WRITTEN_SELF)
            .with_description("The cumulative number of write operations performed by self.")
            .with_unit("{operation}")
            .with_callback(move |observer| {
                let values = times_written_self_source();
                let attrs = rocksdb_attributes();
                observer.observe(values.times_written_self, &attrs);
            })
            .build();

        let times_written_other_source = source.clone();
        let _times_written_other = meter
            .u64_observable_gauge(ROCKSDB_TIMES_WRITTEN_OTHER)
            .with_description("The cumulative number of write operations performed by other.")
            .with_unit("{operation}")
            .with_callback(move |observer| {
                let values = times_written_other_source();
                let attrs = rocksdb_attributes();
                observer.observe(values.times_written_other, &attrs);
            })
            .build();

        let previous_cache_hit = Arc::new(AtomicU64::new(0));
        let previous_cache_miss = Arc::new(AtomicU64::new(0));
        let cache_source = source.clone();
        let cache_hit_state = previous_cache_hit.clone();
        let cache_miss_state = previous_cache_miss.clone();
        let _cache_hit_rate = meter
            .f64_observable_gauge(ROCKSDB_RATE_CACHE_HIT)
            .with_description(
                "The rate at which cache lookups were served from the cache rather than needing to be fetched from \
                 disk.",
            )
            .with_unit("1")
            .with_callback(move |observer| {
                let values = cache_source();
                let previous_hit = cache_hit_state.swap(values.block_cache_hit, Ordering::Relaxed);
                let previous_miss = cache_miss_state.swap(values.block_cache_miss, Ordering::Relaxed);
                let hit_delta = values.block_cache_hit.saturating_sub(previous_hit);
                let miss_delta = values.block_cache_miss.saturating_sub(previous_miss);
                let total_delta = hit_delta + miss_delta;
                let hit_rate = if total_delta == 0 {
                    0.0
                } else {
                    hit_delta as f64 / total_delta as f64
                };
                let attrs = rocksdb_attributes();
                observer.observe(hit_rate, &attrs);
            })
            .build();

        let times_compressed_source = source.clone();
        let _times_compressed = meter
            .u64_observable_gauge(ROCKSDB_TIMES_COMPRESSED)
            .with_description("The cumulative number of compressions that have occurred.")
            .with_unit("{operation}")
            .with_callback(move |observer| {
                let values = times_compressed_source();
                let attrs = rocksdb_attributes();
                observer.observe(values.times_compressed, &attrs);
            })
            .build();

        let read_amplification_source = source.clone();
        let _read_amplification = meter
            .f64_observable_gauge(ROCKSDB_READ_AMPLIFICATION_BYTES)
            .with_description("The total bytes read for read amplification accounting.")
            .with_unit("By")
            .with_callback(move |observer| {
                let values = read_amplification_source();
                let attrs = rocksdb_attributes();
                observer.observe(values.read_amplification_bytes as f64, &attrs);
            })
            .build();

        let _times_read = meter
            .u64_observable_gauge(ROCKSDB_TIMES_READ)
            .with_description("The cumulative number of read operations performed.")
            .with_unit("{operation}")
            .with_callback(move |observer| {
                let values = source();
                let attrs = rocksdb_attributes();
                observer.observe(values.times_read, &attrs);
            })
            .build();

        Self
    }
}

#[cfg(feature = "otel-metrics")]
fn rocksdb_attributes() -> [opentelemetry::KeyValue; 3] {
    [
        opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_TYPE, "local"),
        opentelemetry::KeyValue::new(crate::semantic::labels::STORAGE_MEDIUM, "disk"),
        opentelemetry::KeyValue::new(crate::semantic::labels::TYPE, "consume_queue"),
    ]
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn rocksdb_metrics_register_observable_gauges() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("rocksdb-metrics-test");
        let _metrics = RocksDbOtelMetrics::new_with_observables(&meter, || RocksDbObservableValues {
            bytes_written: 1,
            bytes_read: 2,
            times_written_self: 3,
            times_written_other: 4,
            block_cache_hit: 5,
            block_cache_miss: 6,
            times_compressed: 7,
            read_amplification_bytes: 8,
            times_read: 9,
        });
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;

    #[test]
    fn rocksdb_runtime_metrics_collector_counts_operations() {
        let collector = RocksDbMetricsCollector::default();

        collector.record_write();
        collector.record_read();
        collector.record_batch_write();
        collector.record_scan();
        collector.record_flush();
        collector.record_manual_compaction();
        collector.record_checkpoint();
        collector.record_backup();
        collector.record_property_query();
        collector.record_error();

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.write_count, 1);
        assert_eq!(snapshot.read_count, 1);
        assert_eq!(snapshot.batch_write_count, 1);
        assert_eq!(snapshot.scan_count, 1);
        assert_eq!(snapshot.flush_count, 1);
        assert_eq!(snapshot.manual_compaction_count, 1);
        assert_eq!(snapshot.checkpoint_count, 1);
        assert_eq!(snapshot.backup_count, 1);
        assert_eq!(snapshot.property_query_count, 1);
        assert_eq!(snapshot.error_count, 1);
    }
}
