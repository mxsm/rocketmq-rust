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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

/// Owner-bound gauge guard for one live mapping generation.
///
/// The guard is deliberately non-cloneable. Its constructor performs the only matching gauge
/// increment and its [`Drop`] implementation performs the decrement and physical-drop count.
pub(crate) struct MappingGenerationGaugeGuard {
    metrics: Arc<MappedFileMetrics>,
    mapped_bytes: u64,
}

impl Drop for MappingGenerationGaugeGuard {
    fn drop(&mut self) {
        self.metrics.mapped_generations_live.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .mapped_bytes_live
            .fetch_sub(self.mapped_bytes, Ordering::Relaxed);
        self.metrics.physical_mapping_drop_total.fetch_add(1, Ordering::Relaxed);
    }
}

/// Owner-bound gauge guard for one live operating-system file owner.
///
/// Moving the guard into `FileOwner` binds the live gauge to the final `Arc<FileOwner>` drop.
pub(crate) struct FileOwnerGaugeGuard {
    metrics: Arc<MappedFileMetrics>,
}

impl Drop for FileOwnerGaugeGuard {
    fn drop(&mut self) {
        self.metrics.file_owners_live.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .physical_file_owner_drop_total
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Performance metrics for mapped file operations.
///
/// Tracks operational statistics to enable monitoring, profiling, and
/// performance tuning. All metrics use atomic operations for thread safety
/// with minimal overhead (relaxed ordering).
///
/// # Thread Safety
///
/// All methods are thread-safe and lock-free. Multiple threads can update
/// metrics concurrently without contention.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_store::MappedFileMetrics;
///
/// let metrics = MappedFileMetrics::new();
///
/// // Record a write operation
/// metrics.record_write(4096);
///
/// // Record a flush operation
/// metrics.record_flush(Duration::from_micros(250));
///
/// // Get current statistics
/// println!("Writes/sec: {}", metrics.writes_per_sec());
/// println!("Avg flush time: {:?}", metrics.avg_flush_duration());
/// ```
#[derive(Debug)]
pub struct MappedFileMetrics {
    /// Total number of write operations performed
    total_writes: AtomicU64,

    /// Total bytes written to the file
    total_bytes_written: AtomicU64,

    /// Total number of flush operations performed
    total_flushes: AtomicU64,

    /// Cumulative flush time in microseconds
    total_flush_time_us: AtomicU64,

    /// Total number of read operations performed
    total_reads: AtomicU64,

    /// Total bytes read from the file
    total_bytes_read: AtomicU64,

    /// Number of zero-copy read operations (no memory allocation)
    zero_copy_reads: AtomicU64,

    /// Number of times data was found in page cache (fast path)
    cache_hits: AtomicU64,

    /// Number of times data was not in page cache (disk I/O required)
    cache_misses: AtomicU64,

    /// Bytes copied while materializing mapped-file read selections.
    selection_copy_bytes_total: AtomicU64,

    /// Bytes compared while attaching copied selections to mapped-file owners.
    selection_compare_bytes_total: AtomicU64,

    /// Number of mapped file warm-up operations.
    warm_operations: AtomicU64,

    /// Total bytes touched by warm-up operations.
    warm_bytes: AtomicU64,

    /// Cumulative mapped file warm-up time in milliseconds.
    total_warm_time_ms: AtomicU64,

    /// Duration of the most recent mapped file warm-up in milliseconds.
    last_warm_time_ms: AtomicU64,

    /// Number of mapped file swap decisions.
    swap_operations: AtomicU64,

    /// Number of swapped-map cleanup decisions.
    clean_swap_operations: AtomicU64,

    /// Number of mapping generations with a live physical owner.
    mapped_generations_live: AtomicU64,

    /// Number of bytes covered by live mapping generations.
    mapped_bytes_live: AtomicU64,

    /// Number of live canonical operating-system file owners.
    file_owners_live: AtomicU64,

    /// Number of mapping generations whose final owner has dropped.
    physical_mapping_drop_total: AtomicU64,

    /// Number of canonical file owners whose final owner has dropped.
    physical_file_owner_drop_total: AtomicU64,

    /// Number of mapping/file slot detach winners.
    lifecycle_detach_total: AtomicU64,

    /// Timestamp when metrics collection started
    start_time: Instant,
}

impl Default for MappedFileMetrics {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl MappedFileMetrics {
    /// Creates a new metrics collector with all counters initialized to zero.
    ///
    /// # Returns
    ///
    /// A new `MappedFileMetrics` instance
    #[inline]
    pub fn new() -> Self {
        Self {
            total_writes: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            total_flushes: AtomicU64::new(0),
            total_flush_time_us: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            total_bytes_read: AtomicU64::new(0),
            zero_copy_reads: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            selection_copy_bytes_total: AtomicU64::new(0),
            selection_compare_bytes_total: AtomicU64::new(0),
            warm_operations: AtomicU64::new(0),
            warm_bytes: AtomicU64::new(0),
            total_warm_time_ms: AtomicU64::new(0),
            last_warm_time_ms: AtomicU64::new(0),
            swap_operations: AtomicU64::new(0),
            clean_swap_operations: AtomicU64::new(0),
            mapped_generations_live: AtomicU64::new(0),
            mapped_bytes_live: AtomicU64::new(0),
            file_owners_live: AtomicU64::new(0),
            physical_mapping_drop_total: AtomicU64::new(0),
            physical_file_owner_drop_total: AtomicU64::new(0),
            lifecycle_detach_total: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Starts tracking one mapping generation until the returned guard is dropped.
    ///
    /// The live byte sum cannot exceed the process address space, so its `u64` representation and
    /// matching atomic add/subtract are exact on supported targets.
    pub(crate) fn track_mapping_generation(self: &Arc<Self>, mapped_bytes: usize) -> MappingGenerationGaugeGuard {
        let mapped_bytes = mapped_bytes as u64;
        self.mapped_generations_live.fetch_add(1, Ordering::Relaxed);
        self.mapped_bytes_live.fetch_add(mapped_bytes, Ordering::Relaxed);
        MappingGenerationGaugeGuard {
            metrics: Arc::clone(self),
            mapped_bytes,
        }
    }

    /// Starts tracking one canonical file owner until the returned guard is dropped.
    pub(crate) fn track_file_owner(self: &Arc<Self>) -> FileOwnerGaugeGuard {
        self.file_owners_live.fetch_add(1, Ordering::Relaxed);
        FileOwnerGaugeGuard {
            metrics: Arc::clone(self),
        }
    }

    /// Records a write operation.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes written
    ///
    /// # Performance
    ///
    /// Uses relaxed atomic operations (~1-2 ns overhead on x86_64)
    #[inline]
    pub fn record_write(&self, bytes: usize) {
        self.total_writes.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_written.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Records a flush operation with its duration.
    ///
    /// # Arguments
    ///
    /// * `duration` - Time taken to complete the flush
    #[inline]
    pub fn record_flush(&self, duration: Duration) {
        self.total_flushes.fetch_add(1, Ordering::Relaxed);
        self.total_flush_time_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    /// Records a read operation.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes read
    /// * `zero_copy` - Whether this was a zero-copy read
    #[inline]
    pub fn record_read(&self, bytes: usize, zero_copy: bool) {
        self.total_reads.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_read.fetch_add(bytes as u64, Ordering::Relaxed);

        if zero_copy {
            self.zero_copy_reads.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a cache hit (data was in page cache).
    #[inline]
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a cache miss (disk I/O was required).
    #[inline]
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Records bytes copied into an owned mapped-file read selection.
    #[inline]
    pub(crate) fn record_selection_copy(&self, bytes: usize) {
        self.selection_copy_bytes_total
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Records bytes compared while validating a mapped-file selection attachment.
    #[inline]
    pub(crate) fn record_selection_compare(&self, bytes: usize) {
        self.selection_compare_bytes_total
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Records a mapped file warm-up operation.
    #[inline]
    pub fn record_warm(&self, bytes: usize) {
        self.warm_operations.fetch_add(1, Ordering::Relaxed);
        self.warm_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Records a mapped file warm-up operation with elapsed duration.
    #[inline]
    pub fn record_warm_with_latency(&self, bytes: usize, duration: Duration) {
        self.record_warm(bytes);
        let millis = duration_to_millis(duration);
        let _ = self
            .total_warm_time_ms
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(millis))
            });
        self.last_warm_time_ms.store(millis, Ordering::Relaxed);
    }

    /// Records a mapped file swap decision.
    #[inline]
    pub fn record_swap(&self) {
        self.swap_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a swapped-map cleanup decision.
    #[inline]
    pub fn record_clean_swap(&self) {
        self.clean_swap_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the total number of write operations.
    #[inline]
    pub fn total_writes(&self) -> u64 {
        self.total_writes.load(Ordering::Relaxed)
    }

    /// Returns the total bytes written.
    #[inline]
    pub fn total_bytes_written(&self) -> u64 {
        self.total_bytes_written.load(Ordering::Relaxed)
    }

    /// Returns the total number of flush operations.
    #[inline]
    pub fn total_flushes(&self) -> u64 {
        self.total_flushes.load(Ordering::Relaxed)
    }

    /// Returns the total number of read operations.
    #[inline]
    pub fn total_reads(&self) -> u64 {
        self.total_reads.load(Ordering::Relaxed)
    }

    /// Returns the total bytes read.
    #[inline]
    pub fn total_bytes_read(&self) -> u64 {
        self.total_bytes_read.load(Ordering::Relaxed)
    }

    /// Returns total page-cache hit observations.
    #[inline]
    pub fn cache_hits(&self) -> u64 {
        self.cache_hits.load(Ordering::Relaxed)
    }

    /// Returns total page-cache miss observations.
    #[inline]
    pub fn cache_misses(&self) -> u64 {
        self.cache_misses.load(Ordering::Relaxed)
    }

    /// Returns bytes copied into owned mapped-file read selections.
    #[inline]
    pub fn selection_copy_bytes_total(&self) -> u64 {
        self.selection_copy_bytes_total.load(Ordering::Relaxed)
    }

    /// Returns bytes compared while attaching selections to mapped-file owners.
    #[inline]
    pub fn selection_compare_bytes_total(&self) -> u64 {
        self.selection_compare_bytes_total.load(Ordering::Relaxed)
    }

    /// Returns total warm-up operations.
    #[inline]
    pub fn warm_operations(&self) -> u64 {
        self.warm_operations.load(Ordering::Relaxed)
    }

    /// Returns total bytes touched by warm-up operations.
    #[inline]
    pub fn warm_bytes(&self) -> u64 {
        self.warm_bytes.load(Ordering::Relaxed)
    }

    /// Returns total time spent in warm-up operations, in milliseconds.
    #[inline]
    pub fn total_warm_millis(&self) -> u64 {
        self.total_warm_time_ms.load(Ordering::Relaxed)
    }

    /// Returns the most recent warm-up duration, in milliseconds.
    #[inline]
    pub fn last_warm_millis(&self) -> u64 {
        self.last_warm_time_ms.load(Ordering::Relaxed)
    }

    /// Returns total swap decisions.
    #[inline]
    pub fn swap_operations(&self) -> u64 {
        self.swap_operations.load(Ordering::Relaxed)
    }

    /// Returns total swapped-map cleanup decisions.
    #[inline]
    pub fn clean_swap_operations(&self) -> u64 {
        self.clean_swap_operations.load(Ordering::Relaxed)
    }

    /// Returns the number of live mapping generation owners.
    #[inline]
    pub fn mapped_generations_live(&self) -> u64 {
        self.mapped_generations_live.load(Ordering::Relaxed)
    }

    /// Returns the number of bytes covered by live mapping generation owners.
    #[inline]
    pub fn mapped_bytes_live(&self) -> u64 {
        self.mapped_bytes_live.load(Ordering::Relaxed)
    }

    /// Returns the number of live canonical file owners.
    #[inline]
    pub fn file_owners_live(&self) -> u64 {
        self.file_owners_live.load(Ordering::Relaxed)
    }

    /// Returns the number of mapping generations released by their final owner.
    #[inline]
    pub fn physical_mapping_drop_total(&self) -> u64 {
        self.physical_mapping_drop_total.load(Ordering::Relaxed)
    }

    /// Returns the number of canonical file owners released by their final owner.
    #[inline]
    pub fn physical_file_owner_drop_total(&self) -> u64 {
        self.physical_file_owner_drop_total.load(Ordering::Relaxed)
    }

    /// Records one mapping/file slot detach winner.
    #[inline]
    pub(crate) fn record_lifecycle_detach(&self) {
        self.lifecycle_detach_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the number of mapping/file slot detach winners.
    #[inline]
    pub fn lifecycle_detach_total(&self) -> u64 {
        self.lifecycle_detach_total.load(Ordering::Relaxed)
    }

    /// Calculates write operations per second.
    ///
    /// # Returns
    ///
    /// Throughput in writes/second, or 0.0 if no time has elapsed
    pub fn writes_per_sec(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.total_writes() as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Calculates write throughput in bytes per second.
    ///
    /// # Returns
    ///
    /// Throughput in bytes/second, or 0.0 if no time has elapsed
    pub fn write_throughput_bytes_per_sec(&self) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.total_bytes_written() as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Calculates write throughput in megabytes per second.
    ///
    /// # Returns
    ///
    /// Throughput in MB/s, or 0.0 if no time has elapsed
    pub fn write_throughput_mb_per_sec(&self) -> f64 {
        self.write_throughput_bytes_per_sec() / (1024.0 * 1024.0)
    }

    /// Calculates average write size in bytes.
    ///
    /// # Returns
    ///
    /// Average bytes per write, or 0.0 if no writes occurred
    pub fn avg_write_size(&self) -> f64 {
        let writes = self.total_writes();
        if writes > 0 {
            self.total_bytes_written() as f64 / writes as f64
        } else {
            0.0
        }
    }

    /// Calculates average flush duration.
    ///
    /// # Returns
    ///
    /// Average flush duration, or `Duration::ZERO` if no flushes occurred
    pub fn avg_flush_duration(&self) -> Duration {
        let flushes = self.total_flushes();
        let total_time_us = self.total_flush_time_us.load(Ordering::Relaxed);

        total_time_us
            .checked_div(flushes)
            .map(Duration::from_micros)
            .unwrap_or(Duration::ZERO)
    }

    /// Calculates the percentage of zero-copy reads.
    ///
    /// # Returns
    ///
    /// Percentage (0.0 - 100.0) of reads that were zero-copy
    pub fn zero_copy_read_percentage(&self) -> f64 {
        let total = self.total_reads();
        if total > 0 {
            let zero_copy = self.zero_copy_reads.load(Ordering::Relaxed);
            (zero_copy as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Calculates the cache hit rate.
    ///
    /// # Returns
    ///
    /// Percentage (0.0 - 100.0) of cache accesses that were hits
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;

        if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Resets operation metrics to zero.
    ///
    /// Owner-bound live gauges and physical-drop totals are deliberately preserved: resetting them
    /// while guards are live would make the matching `Drop` decrement asymmetric. The start time is
    /// reset to the current instant.
    pub fn reset(&mut self) {
        self.total_writes.store(0, Ordering::Relaxed);
        self.total_bytes_written.store(0, Ordering::Relaxed);
        self.total_flushes.store(0, Ordering::Relaxed);
        self.total_flush_time_us.store(0, Ordering::Relaxed);
        self.total_reads.store(0, Ordering::Relaxed);
        self.total_bytes_read.store(0, Ordering::Relaxed);
        self.zero_copy_reads.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.selection_copy_bytes_total.store(0, Ordering::Relaxed);
        self.selection_compare_bytes_total.store(0, Ordering::Relaxed);
        self.warm_operations.store(0, Ordering::Relaxed);
        self.warm_bytes.store(0, Ordering::Relaxed);
        self.total_warm_time_ms.store(0, Ordering::Relaxed);
        self.last_warm_time_ms.store(0, Ordering::Relaxed);
        self.swap_operations.store(0, Ordering::Relaxed);
        self.clean_swap_operations.store(0, Ordering::Relaxed);
        self.start_time = Instant::now();
    }

    /// Returns a formatted summary of all metrics.
    ///
    /// # Returns
    ///
    /// A multi-line string with human-readable metrics
    pub fn summary(&self) -> String {
        format!(
            "MappedFile Metrics:\nWrites: {} ({:.2} writes/sec, {:.2} MB/s)\nReads: {} ({:.1}% zero-copy), \
             selection copy/compare: {}/{} bytes\nFlushes: \
             {} (avg: {:?})\nCache Hit Rate: {:.1}%\nAvg Write Size: {:.1} bytes\nWarm: {} ops, {} bytes, total {} \
             ms, last {} ms\nSwap: {} ops, clean: {} ops\nPhysical owners: {} mappings / {} bytes, {} files; drops: {} \
             mappings, {} files; detach: {}",
            self.total_writes(),
            self.writes_per_sec(),
            self.write_throughput_mb_per_sec(),
            self.total_reads(),
            self.zero_copy_read_percentage(),
            self.selection_copy_bytes_total(),
            self.selection_compare_bytes_total(),
            self.total_flushes(),
            self.avg_flush_duration(),
            self.cache_hit_rate(),
            self.avg_write_size(),
            self.warm_operations(),
            self.warm_bytes(),
            self.total_warm_millis(),
            self.last_warm_millis(),
            self.swap_operations(),
            self.clean_swap_operations(),
            self.mapped_generations_live(),
            self.mapped_bytes_live(),
            self.file_owners_live(),
            self.physical_mapping_drop_total(),
            self.physical_file_owner_drop_total(),
            self.lifecycle_detach_total()
        )
    }
}

fn duration_to_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_record_write() {
        let metrics = MappedFileMetrics::new();
        metrics.record_write(1024);
        metrics.record_write(2048);

        assert_eq!(metrics.total_writes(), 2);
        assert_eq!(metrics.total_bytes_written(), 3072);
    }

    #[test]
    fn test_record_flush() {
        let metrics = MappedFileMetrics::new();
        metrics.record_flush(Duration::from_micros(100));
        metrics.record_flush(Duration::from_micros(200));

        assert_eq!(metrics.total_flushes(), 2);
        assert_eq!(metrics.avg_flush_duration(), Duration::from_micros(150));
    }

    #[test]
    fn test_record_read() {
        let metrics = MappedFileMetrics::new();
        metrics.record_read(1024, false);
        metrics.record_read(2048, true);
        metrics.record_read(4096, true);

        assert_eq!(metrics.total_reads(), 3);
        assert_eq!(metrics.total_bytes_read(), 7168);
        // Use approximate comparison for floating point
        let percentage = metrics.zero_copy_read_percentage();
        assert!((percentage - 66.666).abs() < 0.01);
    }

    #[test]
    fn selection_copy_and_compare_bytes_are_counted_and_reset() {
        let mut metrics = MappedFileMetrics::new();
        metrics.record_selection_copy(256);
        metrics.record_selection_compare(128);

        assert_eq!(metrics.selection_copy_bytes_total(), 256);
        assert_eq!(metrics.selection_compare_bytes_total(), 128);

        metrics.reset();
        assert_eq!(metrics.selection_copy_bytes_total(), 0);
        assert_eq!(metrics.selection_compare_bytes_total(), 0);
    }

    #[test]
    fn test_cache_hit_rate() {
        let metrics = MappedFileMetrics::new();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        assert_eq!(metrics.cache_hits(), 2);
        assert_eq!(metrics.cache_misses(), 1);
        assert!((metrics.cache_hit_rate() - 66.666).abs() < 0.01);
    }

    #[test]
    fn test_warm_and_swap_metrics() {
        let metrics = MappedFileMetrics::new();

        metrics.record_warm_with_latency(4096, Duration::from_millis(7));
        metrics.record_swap();
        metrics.record_clean_swap();

        assert_eq!(metrics.warm_operations(), 1);
        assert_eq!(metrics.warm_bytes(), 4096);
        assert_eq!(metrics.total_warm_millis(), 7);
        assert_eq!(metrics.last_warm_millis(), 7);
        assert_eq!(metrics.swap_operations(), 1);
        assert_eq!(metrics.clean_swap_operations(), 1);
        assert!(metrics
            .summary()
            .contains("Warm: 1 ops, 4096 bytes, total 7 ms, last 7 ms"));
    }

    #[test]
    fn test_avg_write_size() {
        let metrics = MappedFileMetrics::new();
        metrics.record_write(1000);
        metrics.record_write(2000);
        metrics.record_write(3000);

        assert_eq!(metrics.avg_write_size(), 2000.0);
    }

    #[test]
    fn test_reset() {
        let mut metrics = MappedFileMetrics::new();
        metrics.record_write(1024);
        metrics.record_flush(Duration::from_micros(100));

        assert!(metrics.total_writes() > 0);

        metrics.reset();

        assert_eq!(metrics.total_writes(), 0);
        assert_eq!(metrics.total_flushes(), 0);
    }

    #[test]
    fn mapping_generation_gauge_tracks_owner_lifetime_symmetrically() {
        let metrics = Arc::new(MappedFileMetrics::new());

        let guard = metrics.track_mapping_generation(4096);
        assert_eq!(metrics.mapped_generations_live(), 1);
        assert_eq!(metrics.mapped_bytes_live(), 4096);
        assert_eq!(metrics.physical_mapping_drop_total(), 0);

        drop(guard);
        assert_eq!(metrics.mapped_generations_live(), 0);
        assert_eq!(metrics.mapped_bytes_live(), 0);
        assert_eq!(metrics.physical_mapping_drop_total(), 1);
    }

    #[test]
    fn file_owner_gauge_tracks_owner_lifetime_symmetrically() {
        let metrics = Arc::new(MappedFileMetrics::new());

        let guard = metrics.track_file_owner();
        assert_eq!(metrics.file_owners_live(), 1);
        assert_eq!(metrics.physical_file_owner_drop_total(), 0);

        drop(guard);
        assert_eq!(metrics.file_owners_live(), 0);
        assert_eq!(metrics.physical_file_owner_drop_total(), 1);
    }
}
