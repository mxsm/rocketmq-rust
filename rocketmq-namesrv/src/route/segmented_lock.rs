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

//! Segmented lock implementation for fine-grained concurrency control
//!
//! This module provides a segmented read-write lock mechanism that allows
//! concurrent access to different segments while maintaining consistency
//! within each segment. This is similar to Java's ConcurrentHashMap striping
//! but provides explicit locking for cross-table operations.
//!
//! ## Architecture
//!
//! - **Segment Count**: Default 16 segments (configurable)
//! - **Hash Distribution**: DefaultHasher for hash distribution
//! - **Lock Type**: parking_lot::RwLock for each segment
//! - **Lock Granularity**: Per-segment read-write locks
//!
//! ## Use Cases
//!
//! 1. **Broker Registration**: Write lock on broker segment for atomic updates
//! 2. **Topic Route Query**: Read lock on topic segment for consistent reads
//! 3. **Broker Unregistration**: Write lock on broker segment for atomic cleanup
//!
//! ## Performance
//!
//! - **Best case**: O(1) lock acquisition with no contention
//! - **Worst case**: O(n) if all threads hash to the same segment
//! - **Average case**: O(1) with uniform hash distribution
//!
//! ## Example
//!
//! ```rust
//! use rocketmq_namesrv::route::segmented_lock::SegmentedLock;
//!
//! let lock_manager = SegmentedLock::with_segments(16);
//!
//! // Acquire read lock for topic
//! {
//!     let _guard: parking_lot::RwLockReadGuard<'_, ()> = lock_manager.read_lock("my-topic");
//!     // Read operations protected by this segment lock
//! }
//!
//! // Acquire write lock for broker
//! {
//!     let _guard: parking_lot::RwLockWriteGuard<'_, ()> = lock_manager.write_lock("broker-a");
//!     // Write operations protected by this segment lock
//! }
//! ```
use std::hash::BuildHasher;
use std::hash::Hash;

use hashbrown::DefaultHashBuilder;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;

/// Default number of segments for lock striping
const DEFAULT_SEGMENT_COUNT: usize = 16;

/// Segmented lock manager for fine-grained concurrency control
///
/// This structure provides segment-level read-write locks based on key hashing.
/// Keys are hashed to determine which segment lock to acquire, allowing
/// concurrent access to different segments.
///
/// # Type Parameters
///
/// * `T` - The lock data type (usually unit `()` for pure synchronization)
pub struct SegmentedLock<T = ()> {
    /// Array of RwLock segments
    segments: Vec<RwLock<T>>,
    /// Number of segments (must be power of 2 for fast modulo)
    segment_count: usize,
    /// Bit mask for fast modulo operation (segment_count - 1)
    segment_mask: usize,
    /// Hasher builder for consistent and fast hashing (reused across cells)
    hash_builder: DefaultHashBuilder,
}

impl<T: Default> SegmentedLock<T> {
    /// Create a new segmented lock manager with default segment count
    ///
    /// # Returns
    ///
    /// A new SegmentedLock with DEFAULT_SEGMENT_COUNT segments
    pub fn new() -> Self {
        Self::with_segments(DEFAULT_SEGMENT_COUNT)
    }

    /// Create a new segmented lock manager with specified segment count
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
    /// A new SegmentedLock with the specified number of segments
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
            hash_builder: DefaultHashBuilder::default(),
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
        let hash = self.hash_builder.hash_one(key) as usize;
        // Fast modulo using bit mask (only works for power of 2)
        hash & self.segment_mask
    }

    /// Acquire a read lock for the segment containing the given key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to determine which segment to lock
    ///
    /// # Returns
    ///
    /// A read guard for the segment
    pub fn read_lock<K: ?Sized + Hash>(&self, key: &K) -> RwLockReadGuard<'_, T> {
        let index = self.segment_index(key);
        self.segments[index].read()
    }

    /// Acquire a write lock for the segment containing the given key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to determine which segment to lock
    ///
    /// # Returns
    ///
    /// A write guard for the segment
    pub fn write_lock<K: ?Sized + Hash>(&self, key: &K) -> RwLockWriteGuard<'_, T> {
        let index = self.segment_index(key);
        self.segments[index].write()
    }

    /// Acquire read locks for multiple keys
    ///
    /// Locks are acquired in sorted order by segment index to prevent deadlocks.
    ///
    /// # Arguments
    ///
    /// * `keys` - Iterator of keys to lock
    ///
    /// # Returns
    ///
    /// Vector of read guards in the order of acquisition
    pub fn read_lock_multiple<'a, K: ?Sized + Hash + 'a>(
        &'a self,
        keys: impl IntoIterator<Item = &'a K>,
    ) -> Vec<RwLockReadGuard<'a, T>> {
        let mut indices: Vec<usize> = keys.into_iter().map(|k| self.segment_index(k)).collect();
        // Remove duplicates and sort to prevent deadlocks
        indices.sort_unstable();
        indices.dedup();

        indices.into_iter().map(|idx| self.segments[idx].read()).collect()
    }

    /// Acquire write locks for multiple keys
    ///
    /// Locks are acquired in sorted order by segment index to prevent deadlocks.
    ///
    /// # Arguments
    ///
    /// * `keys` - Iterator of keys to lock
    ///
    /// # Returns
    ///
    /// Vector of write guards in the order of acquisition
    pub fn write_lock_multiple<'a, K: ?Sized + Hash + 'a>(
        &'a self,
        keys: impl IntoIterator<Item = &'a K>,
    ) -> Vec<RwLockWriteGuard<'a, T>> {
        let mut indices: Vec<usize> = keys.into_iter().map(|k| self.segment_index(k)).collect();
        // Remove duplicates and sort to prevent deadlocks
        indices.sort_unstable();
        indices.dedup();

        indices.into_iter().map(|idx| self.segments[idx].write()).collect()
    }

    /// Get the number of segments
    pub fn segment_count(&self) -> usize {
        self.segment_count
    }

    /// Acquire a global write lock on all segments
    ///
    /// This is used for operations that need to modify multiple segments atomically.
    /// Use sparingly as it blocks all concurrent access.
    ///
    /// # Returns
    ///
    /// Vector of write guards for all segments
    pub fn global_write_lock(&self) -> Vec<RwLockWriteGuard<'_, T>> {
        self.segments.iter().map(|lock| lock.write()).collect()
    }

    /// Acquire a global read lock on all segments
    ///
    /// This is used for operations that need to read from multiple segments consistently.
    ///
    /// # Returns
    ///
    /// Vector of read guards for all segments
    pub fn global_read_lock(&self) -> Vec<RwLockReadGuard<'_, T>> {
        self.segments.iter().map(|lock| lock.read()).collect()
    }
}

impl<T: Default> Default for SegmentedLock<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn test_segment_distribution() {
        let lock = SegmentedLock::<()>::new();
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

    #[test]
    fn test_concurrent_read_locks() {
        let lock = Arc::new(SegmentedLock::<i32>::new());
        let mut handles = vec![];

        // Spawn multiple threads acquiring read locks
        for i in 0..10 {
            let lock_clone = Arc::clone(&lock);
            let handle = thread::spawn(move || {
                let key = format!("key-{}", i % 3); // Use 3 different keys
                let _guard = lock_clone.read_lock(&key);
                thread::sleep(std::time::Duration::from_millis(10));
            });
            handles.push(handle);
        }

        // All threads should complete without deadlock
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_write_lock_exclusivity() {
        let lock = Arc::new(SegmentedLock::<i32>::new());
        let key = "test-key";

        // Acquire write lock
        {
            let _write_guard = lock.write_lock(&key);
            // Write lock is exclusive, no other thread should be able to acquire
        }

        // After release, should be able to acquire again
        {
            let _write_guard = lock.write_lock(&key);
        }
    }

    #[test]
    fn test_multiple_lock_acquisition() {
        let lock = SegmentedLock::<()>::new();
        let keys = vec!["key1", "key2", "key3"];

        // Acquire read locks for multiple keys
        {
            let _guards = lock.read_lock_multiple(&keys);
            // Read locks are automatically released at end of this block
        }

        // Acquire write locks for multiple keys (after read locks are released)
        {
            let _guards = lock.write_lock_multiple(&keys);
            // Write locks are automatically released at end of this block
        }
    }

    #[test]
    fn test_power_of_two_segments() {
        for count in [1, 7, 15, 16, 17, 31, 32, 33] {
            let lock = SegmentedLock::<()>::with_segments(count);
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

    #[test]
    fn test_global_locks() {
        let lock = SegmentedLock::<i32>::new();

        // Global write lock
        {
            let _guards = lock.global_write_lock();
            assert_eq!(_guards.len(), lock.segment_count());
        }

        // Global read lock
        {
            let _guards = lock.global_read_lock();
            assert_eq!(_guards.len(), lock.segment_count());
        }
    }

    #[test]
    #[should_panic(expected = "Segment count must be greater than 0")]
    fn test_zero_segments_panics() {
        let _lock = SegmentedLock::<()>::with_segments(0);
    }
}
