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

use std::time::Duration;

/// Defines strategies for flushing (syncing) mapped file data to disk.
///
/// Different flush strategies provide trade-offs between:
/// - **Durability**: How quickly data is persisted to disk
/// - **Performance**: Throughput and latency of write operations
/// - **Resource usage**: CPU and I/O overhead
///
/// # Examples
///
/// ```rust,ignore
/// // Flush every 4 pages (default, good balance)
/// let strategy = FlushStrategy::EveryNPages(4);
///
/// // Flush every 100ms (time-based, predictable latency)
/// let strategy = FlushStrategy::Periodic(Duration::from_millis(100));
///
/// // Async flush (highest throughput, eventual consistency)
/// let strategy = FlushStrategy::Async;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushStrategy {
    /// Flush every N dirty pages.
    ///
    /// **Use when**: You want to balance durability and performance based on
    /// the amount of uncommitted data.
    ///
    /// **Trade-offs**:
    /// - Lower N = better durability, lower throughput
    /// - Higher N = worse durability, higher throughput
    ///
    /// **Typical values**: 4-32 pages (16KB - 128KB at 4KB page size)
    EveryNPages(usize),

    /// Flush periodically based on time elapsed since last flush.
    ///
    /// **Use when**: You need predictable maximum data loss window
    /// (e.g., "lose at most 100ms of writes").
    ///
    /// **Trade-offs**:
    /// - Shorter duration = better durability, more CPU overhead
    /// - Longer duration = worse durability, less CPU overhead
    ///
    /// **Typical values**: 50ms - 1000ms
    Periodic(Duration),

    /// Flush asynchronously in a background thread.
    ///
    /// **Use when**: You need maximum write throughput and can tolerate
    /// eventual consistency (data persisted within ~100ms).
    ///
    /// **Trade-offs**:
    /// - Highest throughput (no blocking on writes)
    /// - Data loss possible on crash (up to buffer size)
    /// - Background thread overhead
    ///
    /// **Typical use case**: High-volume logging, where some data loss is acceptable
    Async,

    /// Flush synchronously after every write operation.
    ///
    /// **Use when**: You need absolute durability guarantees
    /// (e.g., financial transactions, metadata updates).
    ///
    /// **Trade-offs**:
    /// - Lowest throughput (~100-1000x slower)
    /// - Highest durability (every write is persisted)
    /// - High I/O load
    ///
    /// **Warning**: Can severely impact performance. Consider using
    /// `EveryNPages(1)` or `Periodic(small_duration)` instead.
    Sync,

    /// Never flush (for testing or in-memory use cases).
    ///
    /// **Use when**: Running tests or when data persistence is not required.
    ///
    /// **Trade-offs**:
    /// - Maximum throughput (no flush overhead)
    /// - All data lost on crash/shutdown
    ///
    /// **Warning**: Do NOT use in production for persistent storage!
    Never,

    /// Flush based on both page count AND time threshold (whichever comes first).
    ///
    /// **Use when**: You need both space-based and time-based flush guarantees.
    ///
    /// **Trade-offs**:
    /// - Most predictable behavior
    /// - Slight overhead from checking both conditions
    ///
    /// **Example**: Flush every 16 pages OR every 200ms, whichever happens first
    Hybrid {
        /// Number of dirty pages threshold
        pages: usize,
        /// Time duration threshold
        duration: Duration,
    },
}

impl Default for FlushStrategy {
    /// Default strategy: flush every 4 pages.
    ///
    /// This provides a good balance between:
    /// - Durability: At most 4 pages (16KB) of data loss
    /// - Performance: Amortizes flush overhead over multiple writes
    #[inline]
    fn default() -> Self {
        Self::EveryNPages(4)
    }
}

impl FlushStrategy {
    /// Creates a strategy that flushes every N dirty pages.
    ///
    /// # Arguments
    ///
    /// * `pages` - Number of dirty pages to accumulate before flushing
    ///
    /// # Panics
    ///
    /// Panics if `pages` is 0
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let strategy = FlushStrategy::every_n_pages(8);
    /// ```
    #[inline]
    pub fn every_n_pages(pages: usize) -> Self {
        assert!(pages > 0, "pages must be greater than 0");
        Self::EveryNPages(pages)
    }

    /// Creates a strategy that flushes periodically.
    ///
    /// # Arguments
    ///
    /// * `duration` - Time interval between flushes
    ///
    /// # Panics
    ///
    /// Panics if `duration` is zero
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let strategy = FlushStrategy::periodic(Duration::from_millis(100));
    /// ```
    #[inline]
    pub fn periodic(duration: Duration) -> Self {
        assert!(!duration.is_zero(), "duration must be greater than zero");
        Self::Periodic(duration)
    }

    /// Creates a hybrid strategy with both page and time thresholds.
    ///
    /// Flush will occur when EITHER condition is met (whichever comes first).
    ///
    /// # Arguments
    ///
    /// * `pages` - Number of dirty pages threshold
    /// * `duration` - Time duration threshold
    ///
    /// # Panics
    ///
    /// Panics if `pages` is 0 or `duration` is zero
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Flush every 16 pages OR every 200ms
    /// let strategy = FlushStrategy::hybrid(16, Duration::from_millis(200));
    /// ```
    #[inline]
    pub fn hybrid(pages: usize, duration: Duration) -> Self {
        assert!(pages > 0, "pages must be greater than 0");
        assert!(!duration.is_zero(), "duration must be greater than zero");
        Self::Hybrid { pages, duration }
    }

    /// Checks if this strategy requires immediate flushing.
    ///
    /// # Returns
    ///
    /// `true` if the strategy is `Sync`, which requires flush after every write
    #[inline]
    pub fn is_sync(&self) -> bool {
        matches!(self, Self::Sync)
    }

    /// Checks if this strategy uses asynchronous flushing.
    ///
    /// # Returns
    ///
    /// `true` if the strategy is `Async`
    #[inline]
    pub fn is_async(&self) -> bool {
        matches!(self, Self::Async)
    }

    /// Checks if flushing is disabled.
    ///
    /// # Returns
    ///
    /// `true` if the strategy is `Never`
    #[inline]
    pub fn is_never(&self) -> bool {
        matches!(self, Self::Never)
    }

    /// Returns a descriptive name for the strategy.
    ///
    /// # Returns
    ///
    /// A string slice describing the strategy
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Self::EveryNPages(_) => "EveryNPages",
            Self::Periodic(_) => "Periodic",
            Self::Async => "Async",
            Self::Sync => "Sync",
            Self::Never => "Never",
            Self::Hybrid { .. } => "Hybrid",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_strategy() {
        let strategy = FlushStrategy::default();
        assert!(matches!(strategy, FlushStrategy::EveryNPages(4)));
    }

    #[test]
    fn test_every_n_pages() {
        let strategy = FlushStrategy::every_n_pages(8);
        assert!(matches!(strategy, FlushStrategy::EveryNPages(8)));
    }

    #[test]
    #[should_panic(expected = "pages must be greater than 0")]
    fn test_every_n_pages_zero() {
        FlushStrategy::every_n_pages(0);
    }

    #[test]
    fn test_periodic() {
        let strategy = FlushStrategy::periodic(Duration::from_millis(100));
        assert!(matches!(strategy, FlushStrategy::Periodic(_)));
    }

    #[test]
    #[should_panic(expected = "duration must be greater than zero")]
    fn test_periodic_zero() {
        FlushStrategy::periodic(Duration::ZERO);
    }

    #[test]
    fn test_hybrid() {
        let strategy = FlushStrategy::hybrid(16, Duration::from_millis(200));
        assert!(matches!(strategy, FlushStrategy::Hybrid { .. }));
    }

    #[test]
    fn test_is_sync() {
        assert!(FlushStrategy::Sync.is_sync());
        assert!(!FlushStrategy::Async.is_sync());
    }

    #[test]
    fn test_is_async() {
        assert!(FlushStrategy::Async.is_async());
        assert!(!FlushStrategy::Sync.is_async());
    }

    #[test]
    fn test_is_never() {
        assert!(FlushStrategy::Never.is_never());
        assert!(!FlushStrategy::Async.is_never());
    }

    #[test]
    fn test_strategy_names() {
        assert_eq!(FlushStrategy::EveryNPages(4).name(), "EveryNPages");
        assert_eq!(FlushStrategy::Async.name(), "Async");
        assert_eq!(FlushStrategy::Sync.name(), "Sync");
        assert_eq!(FlushStrategy::Never.name(), "Never");
    }
}
