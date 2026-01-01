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

//! Asynchronous segmented lock implementation using Tokio RwLock
//!
//! This module provides an async version of segmented read-write lock mechanism
//! that allows concurrent access to different segments while maintaining consistency
//! within each segment. Built on top of Tokio's async RwLock.
//!
//! ## Architecture
//!
//! - **Segment Count**: Default 16 segments (configurable)
//! - **Hash Distribution**: DefaultHasher for uniform distribution
//! - **Lock Type**: tokio::sync::RwLock for each segment
//! - **Lock Granularity**: Per-segment async read-write locks
//!
//! ## Use Cases
//!
//! 1. **Async Broker Registration**: Write lock on broker segment for atomic updates
//! 2. **Async Topic Route Query**: Read lock on topic segment for consistent reads
//! 3. **Async Broker Unregistration**: Write lock on broker segment for atomic cleanup
//!
//! ## Performance
//!
//! - **Best case**: O(1) lock acquisition with no contention
//! - **Worst case**: O(n) if all tasks hash to the same segment
//! - **Average case**: O(1) with uniform hash distribution
//! - **Async overhead**: Negligible for I/O-bound workloads
//!
//! ## Example
//!
//! ```rust,no_run
//! use rocketmq_namesrv::route::async_segmented_lock::AsyncSegmentedLock;
//!
//! # async fn example() {
//! let lock_manager = AsyncSegmentedLock::with_segments(16);
//!
//! // Acquire read lock for topic
//! {
//!     let _guard: tokio::sync::RwLockReadGuard<'_, ()> = lock_manager.read_lock("my-topic").await;
//!     // Read operations protected by this segment lock
//! }
//!
//! // Acquire write lock for broker
//! {
//!     let _guard: tokio::sync::RwLockWriteGuard<'_, ()> =
//!         lock_manager.write_lock("broker-a").await;
//!     // Write operations protected by this segment lock
//! }
//! # }
//! ```

use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;

/// Default number of segments for lock striping
const DEFAULT_SEGMENT_COUNT: usize = 16;

/// Asynchronous segmented lock manager for fine-grained concurrency control
///
/// This structure provides segment-level async read-write locks based on key hashing.
/// Keys are hashed to determine which segment lock to acquire, allowing
/// concurrent access to different segments in an async context.
///
/// # Type Parameters
///
/// * `T` - The lock data type (usually unit `()` for pure synchronization)
pub struct AsyncSegmentedLock<T = ()> {
    /// Array of async RwLock segments
    segments: Vec<RwLock<T>>,
    /// Number of segments (must be power of 2 for fast modulo)
    segment_count: usize,
    /// Bit mask for fast modulo operation (segment_count - 1)
    segment_mask: usize,
}

impl<T: Default> AsyncSegmentedLock<T> {
    /// Create a new async segmented lock manager with default segment count
    ///
    /// # Returns
    ///
    /// A new AsyncSegmentedLock with DEFAULT_SEGMENT_COUNT segments
    pub fn new() -> Self {
        Self::with_segments(DEFAULT_SEGMENT_COUNT)
    }

    /// Create a new async segmented lock manager with specified segment count
    ///
    /// # Arguments
    ///
    /// * `segment_count` - Number of segments (will be rounded up to next power of 2)
    ///
    /// # Panics
    ///
    /// Panics if segment_count is 0
    ///
    /// # Returns
    ///
    /// A new AsyncSegmentedLock with the specified number of segments
    pub fn with_segments(segment_count: usize) -> Self {
        assert!(segment_count > 0, "Segment count must be greater than 0");

        // Round up to next power of 2 for efficient modulo operation
        let segment_count = segment_count.next_power_of_two();
        let segment_mask = segment_count - 1;

        let segments = (0..segment_count).map(|_| RwLock::new(T::default())).collect();

        Self {
            segments,
            segment_count,
            segment_mask,
        }
    }

    /// Calculate segment index for a given key
    ///
    /// Uses Rust's `DefaultHasher` (typically SipHash) for fast and uniform distribution
    ///
    /// # Arguments
    ///
    /// * `key` - The key to hash
    ///
    /// # Returns
    ///
    /// The segment index (0 to segment_count-1)
    #[inline]
    fn segment_index<K: ?Sized + Hash>(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        // Fast modulo using bit mask (only works for power of 2)
        hash & self.segment_mask
    }

    /// Acquire an async read lock for the segment containing the given key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to determine which segment to lock
    ///
    /// # Returns
    ///
    /// A future that resolves to a read guard for the segment
    pub async fn read_lock<K: ?Sized + Hash>(&self, key: &K) -> RwLockReadGuard<'_, T> {
        let index = self.segment_index(key);
        self.segments[index].read().await
    }

    /// Acquire an async write lock for the segment containing the given key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to determine which segment to lock
    ///
    /// # Returns
    ///
    /// A future that resolves to a write guard for the segment
    pub async fn write_lock<K: ?Sized + Hash>(&self, key: &K) -> RwLockWriteGuard<'_, T> {
        let index = self.segment_index(key);
        self.segments[index].write().await
    }

    /// Acquire async read locks for multiple keys
    ///
    /// Locks are acquired in sorted order by segment index to prevent deadlocks.
    ///
    /// # Arguments
    ///
    /// * `keys` - Iterator of keys to lock
    ///
    /// # Returns
    ///
    /// A future that resolves to a vector of read guards in the order of acquisition
    pub async fn read_lock_multiple<'a, K: ?Sized + Hash + 'a>(
        &'a self,
        keys: impl IntoIterator<Item = &'a K>,
    ) -> Vec<RwLockReadGuard<'a, T>> {
        let mut indices: Vec<usize> = keys.into_iter().map(|k| self.segment_index(k)).collect();
        // Remove duplicates and sort to prevent deadlocks
        indices.sort_unstable();
        indices.dedup();

        let mut guards = Vec::with_capacity(indices.len());
        for idx in indices {
            guards.push(self.segments[idx].read().await);
        }
        guards
    }

    /// Acquire async write locks for multiple keys
    ///
    /// Locks are acquired in sorted order by segment index to prevent deadlocks.
    ///
    /// # Arguments
    ///
    /// * `keys` - Iterator of keys to lock
    ///
    /// # Returns
    ///
    /// A future that resolves to a vector of write guards in the order of acquisition
    pub async fn write_lock_multiple<'a, K: ?Sized + Hash + 'a>(
        &'a self,
        keys: impl IntoIterator<Item = &'a K>,
    ) -> Vec<RwLockWriteGuard<'a, T>> {
        let mut indices: Vec<usize> = keys.into_iter().map(|k| self.segment_index(k)).collect();
        // Remove duplicates and sort to prevent deadlocks
        indices.sort_unstable();
        indices.dedup();

        let mut guards = Vec::with_capacity(indices.len());
        for idx in indices {
            guards.push(self.segments[idx].write().await);
        }
        guards
    }

    /// Get the number of segments
    pub fn segment_count(&self) -> usize {
        self.segment_count
    }

    /// Acquire a global async write lock on all segments
    ///
    /// This is used for operations that need to modify multiple segments atomically.
    /// Use sparingly as it blocks all concurrent access.
    ///
    /// # Returns
    ///
    /// A future that resolves to a vector of write guards for all segments
    pub async fn global_write_lock(&self) -> Vec<RwLockWriteGuard<'_, T>> {
        let mut guards = Vec::with_capacity(self.segment_count);
        for lock in &self.segments {
            guards.push(lock.write().await);
        }
        guards
    }

    /// Acquire a global async read lock on all segments
    ///
    /// This is used for operations that need to read from multiple segments consistently.
    ///
    /// # Returns
    ///
    /// A future that resolves to a vector of read guards for all segments
    pub async fn global_read_lock(&self) -> Vec<RwLockReadGuard<'_, T>> {
        let mut guards = Vec::with_capacity(self.segment_count);
        for lock in &self.segments {
            guards.push(lock.read().await);
        }
        guards
    }
}

impl<T: Default> Default for AsyncSegmentedLock<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_segment_distribution() {
        let lock = AsyncSegmentedLock::<()>::new();
        let mut segment_indices = HashSet::new();

        // Test that different keys map to different segments
        for i in 0..100 {
            let key = format!("key-{}", i);
            let index = lock.segment_index(&key);
            segment_indices.insert(index);
        }

        // With 100 keys and 16 segments, we should see most segments used
        assert!(
            segment_indices.len() >= 10,
            "Poor hash distribution: only {} segments used",
            segment_indices.len()
        );
    }

    #[tokio::test]
    async fn test_concurrent_read_locks() {
        let lock = Arc::new(AsyncSegmentedLock::<i32>::new());
        let mut handles = vec![];

        // Spawn multiple tasks acquiring read locks
        for i in 0..10 {
            let lock_clone = Arc::clone(&lock);
            let handle = tokio::spawn(async move {
                let key = format!("key-{}", i % 3); // Use 3 different keys
                let _guard = lock_clone.read_lock(&key).await;
                sleep(Duration::from_millis(10)).await;
            });
            handles.push(handle);
        }

        // All tasks should complete without deadlock
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_write_lock_exclusivity() {
        let lock = Arc::new(AsyncSegmentedLock::<i32>::new());
        let key = "test-key";

        // Acquire write lock
        {
            let _write_guard = lock.write_lock(&key).await;
            // Write lock is exclusive, no other task should be able to acquire
        }

        // After release, should be able to acquire again
        {
            let _write_guard = lock.write_lock(&key).await;
        }
    }

    #[tokio::test]
    async fn test_multiple_lock_acquisition() {
        let lock = AsyncSegmentedLock::<()>::new();
        let keys = vec!["key1", "key2", "key3"];

        // Acquire read locks for multiple keys
        {
            let _guards = lock.read_lock_multiple(&keys).await;
            // Read locks are automatically released at end of this block
        }

        // Acquire write locks for multiple keys (after read locks are released)
        {
            let _guards = lock.write_lock_multiple(&keys).await;
            // Write locks are automatically released at end of this block
        }
    }

    #[tokio::test]
    async fn test_power_of_two_segments() {
        for count in [1, 7, 15, 16, 17, 31, 32, 33] {
            let lock = AsyncSegmentedLock::<()>::with_segments(count);
            assert!(
                lock.segment_count().is_power_of_two(),
                "Segment count {} is not power of 2",
                lock.segment_count()
            );
            assert!(
                lock.segment_count() >= count,
                "Segment count {} is less than requested {}",
                lock.segment_count(),
                count
            );
        }
    }

    #[tokio::test]
    async fn test_global_locks() {
        let lock = AsyncSegmentedLock::<i32>::new();

        // Global write lock
        {
            let _guards = lock.global_write_lock().await;
            assert_eq!(_guards.len(), lock.segment_count());
        }

        // Global read lock
        {
            let _guards = lock.global_read_lock().await;
            assert_eq!(_guards.len(), lock.segment_count());
        }
    }

    #[tokio::test]
    #[should_panic(expected = "Segment count must be greater than 0")]
    async fn test_zero_segments_panics() {
        let _lock = AsyncSegmentedLock::<()>::with_segments(0);
    }

    #[tokio::test]
    async fn test_read_write_exclusion() {
        let lock = Arc::new(AsyncSegmentedLock::<i32>::new());
        let key = "test-key";
        let counter = Arc::new(tokio::sync::Mutex::new(0));

        // Writer task
        let lock_w = Arc::clone(&lock);
        let counter_w = Arc::clone(&counter);
        let writer = tokio::spawn(async move {
            let _guard = lock_w.write_lock(&key).await;
            let mut c = counter_w.lock().await;
            *c += 1;
            sleep(Duration::from_millis(50)).await;
        });

        // Wait a bit to ensure writer acquires lock first
        sleep(Duration::from_millis(10)).await;

        // Reader task should wait for writer
        let lock_r = Arc::clone(&lock);
        let counter_r = Arc::clone(&counter);
        let reader = tokio::spawn(async move {
            let _guard = lock_r.read_lock(&key).await;
            let c = counter_r.lock().await;
            assert_eq!(*c, 1, "Reader should see writer's update");
        });

        writer.await.unwrap();
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_writers_serialized() {
        let lock = Arc::new(AsyncSegmentedLock::<Vec<i32>>::new());
        let key = "test-key";
        let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let mut handles = vec![];
        for i in 0..10 {
            let lock_clone = Arc::clone(&lock);
            let results_clone = Arc::clone(&results);
            let handle = tokio::spawn(async move {
                let _guard = lock_clone.write_lock(&key).await;
                // Simulate work
                sleep(Duration::from_millis(5)).await;
                let mut r = results_clone.lock().await;
                r.push(i);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let final_results = results.lock().await;
        assert_eq!(final_results.len(), 10, "All writers should complete");
    }

    #[tokio::test]
    async fn test_different_segments_concurrent() {
        let lock = Arc::new(AsyncSegmentedLock::<()>::new());
        let start = std::time::Instant::now();

        let mut handles = vec![];
        // Use keys that likely hash to different segments
        for i in 0..16 {
            let lock_clone = Arc::clone(&lock);
            let handle = tokio::spawn(async move {
                let key = format!("unique-key-{}", i);
                let _guard = lock_clone.write_lock(&key).await;
                sleep(Duration::from_millis(50)).await;
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let elapsed = start.elapsed();
        // If locks were truly concurrent, should take ~50ms, not 800ms
        assert!(
            elapsed < Duration::from_millis(200),
            "Different segments should allow concurrent access, took {:?}",
            elapsed
        );
    }
}
