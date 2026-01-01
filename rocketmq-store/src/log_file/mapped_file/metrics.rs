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
use std::time::Duration;
use std::time::Instant;

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
/// use rocketmq_store::log_file::mapped_file::MappedFileMetrics;
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
            start_time: Instant::now(),
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
        if flushes > 0 {
            let avg_us = self.total_flush_time_us.load(Ordering::Relaxed) / flushes;
            Duration::from_micros(avg_us)
        } else {
            Duration::ZERO
        }
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

    /// Resets all metrics to zero.
    ///
    /// Also resets the start time to the current instant.
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
        self.start_time = Instant::now();
    }

    /// Returns a formatted summary of all metrics.
    ///
    /// # Returns
    ///
    /// A multi-line string with human-readable metrics
    pub fn summary(&self) -> String {
        format!(
            "MappedFile Metrics:\nWrites: {} ({:.2} writes/sec, {:.2} MB/s)\nReads: {} ({:.1}% zero-copy)\nFlushes: \
             {} (avg: {:?})\nCache Hit Rate: {:.1}%\nAvg Write Size: {:.1} bytes",
            self.total_writes(),
            self.writes_per_sec(),
            self.write_throughput_mb_per_sec(),
            self.total_reads(),
            self.zero_copy_read_percentage(),
            self.total_flushes(),
            self.avg_flush_duration(),
            self.cache_hit_rate(),
            self.avg_write_size()
        )
    }
}

#[cfg(test)]
mod tests {
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
    fn test_cache_hit_rate() {
        let metrics = MappedFileMetrics::new();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        assert!((metrics.cache_hit_rate() - 66.666).abs() < 0.01);
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
}
