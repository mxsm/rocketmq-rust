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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;

use parking_lot::Mutex;

use crate::log_file::mapped_file::reference_resource::ReferenceResource;

/// Base structure containing all atomic fields for reference counting.
///
/// This struct holds all the shared state needed for reference counting,
/// matching the fields in Java's `ReferenceResource` abstract class.
///
/// # Performance Optimizations
///
/// - Cache-line aligned to prevent false sharing
/// - Hot fields (ref_count, available) placed first for better cache locality
/// - Lock fields placed last to minimize contention impact
///
/// # Java Alignment
///
/// ```java
/// public abstract class ReferenceResource {
///     protected final AtomicLong refCount = new AtomicLong(1);
///     protected volatile boolean available = true;
///     protected volatile boolean cleanupOver = false;
///     private volatile long firstShutdownTimestamp = 0;
/// }
/// ```
#[repr(align(64))] // Cache line alignment to prevent false sharing
pub struct ReferenceResourceBase {
    /// Reference count (initial value: 1).
    /// Most frequently accessed field, placed first for cache locality.
    ///
    /// Java equivalent: `protected final AtomicLong refCount = new AtomicLong(1);`
    pub ref_count: AtomicI64,

    /// Availability flag. When false, no new holds are accepted.
    /// Second most frequently accessed, adjacent to ref_count.
    ///
    /// Java equivalent: `protected volatile boolean available = true;`
    pub available: AtomicBool,

    /// Cleanup completion flag.
    ///
    /// Java equivalent: `protected volatile boolean cleanupOver = false;`
    pub cleanup_over: AtomicBool,

    /// Timestamp when shutdown was first called.
    ///
    /// Java equivalent: `private volatile long firstShutdownTimestamp = 0;`
    pub first_shutdown_timestamp: AtomicU64,

    /// Mutex for synchronized hold operation.
    /// Placed after hot atomic fields to minimize contention impact.
    ///
    /// Uses `parking_lot::Mutex` for better performance and no lock poisoning.
    /// Java uses `synchronized` keyword on the hold() method.
    pub hold_lock: Mutex<()>,

    /// Mutex for synchronized release cleanup.
    /// Placed last as it's less frequently accessed.
    ///
    /// Uses `parking_lot::Mutex` for better performance and no lock poisoning.
    /// Java uses `synchronized (this)` block in release() method.
    pub release_lock: Mutex<()>,
}

impl ReferenceResourceBase {
    /// Creates a new reference resource base with initial refCount of 1.
    ///
    /// # Java Equivalent
    ///
    /// ```java
    /// protected final AtomicLong refCount = new AtomicLong(1);
    /// protected volatile boolean available = true;
    /// protected volatile boolean cleanupOver = false;
    /// private volatile long firstShutdownTimestamp = 0;
    /// ```
    #[inline]
    pub fn new() -> Self {
        Self {
            ref_count: AtomicI64::new(1),
            available: AtomicBool::new(true),
            cleanup_over: AtomicBool::new(false),
            first_shutdown_timestamp: AtomicU64::new(0),
            hold_lock: Mutex::new(()),
            release_lock: Mutex::new(()),
        }
    }
}

impl Default for ReferenceResourceBase {
    fn default() -> Self {
        Self::new()
    }
}

/// Reference-counted resource with lifecycle management.
///
/// Implements the exact semantics of Java's `ReferenceResource` class from Apache RocketMQ.
/// This struct provides thread-safe reference counting with graceful and forced shutdown support.
///
/// # Java Alignment
///
/// Matches the behavior of `org.apache.rocketmq.store.ReferenceResource`:
/// - Initial refCount = 1 (self-reference)
/// - Synchronized `hold()` method
/// - Atomic shutdown with timeout-based forced cleanup
/// - Single cleanup execution guarantee
pub struct ReferenceResourceCounter {
    /// Base structure containing all reference counting state.
    base: ReferenceResourceBase,
}

impl ReferenceResourceCounter {
    /// Creates a new reference resource with initial refCount of 1.
    ///
    /// # Java Equivalent
    ///
    /// ```java
    /// protected final AtomicLong refCount = new AtomicLong(1);
    /// protected volatile boolean available = true;
    /// protected volatile boolean cleanupOver = false;
    /// private volatile long firstShutdownTimestamp = 0;
    /// ```
    #[inline]
    pub fn new() -> Self {
        Self {
            base: ReferenceResourceBase::new(),
        }
    }

    /// Returns a reference to the base structure.
    ///
    /// This provides access to all reference counting fields.
    #[inline]
    pub fn base(&self) -> &ReferenceResourceBase {
        &self.base
    }
}

impl Default for ReferenceResourceCounter {
    fn default() -> Self {
        Self::new()
    }
}

// Implement ReferenceResource trait
impl ReferenceResource for ReferenceResourceCounter {
    fn base(&self) -> &ReferenceResourceBase {
        &self.base
    }

    /// Default no-op cleanup implementation.
    /// Override this in custom resource types for specific cleanup logic.
    fn cleanup(&self, _current_ref: i64) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    use super::*;

    // ========================================================================
    // Basic Correctness Tests (Aligning with Java Behavior)
    // ========================================================================

    #[test]
    fn test_initial_state() {
        let resource = ReferenceResourceCounter::new();
        assert_eq!(resource.get_ref_count(), 1, "Initial refCount should be 1");
        assert!(resource.is_available(), "Should be available initially");
        assert!(!resource.is_cleanup_over(), "Cleanup should not be over");
    }

    #[test]
    fn test_default_trait() {
        let resource = ReferenceResourceCounter::default();
        assert_eq!(resource.get_ref_count(), 1);
        assert!(resource.is_available());
    }

    #[test]
    fn test_hold_success_when_available() {
        let resource = ReferenceResourceCounter::new();
        assert!(resource.hold(), "First hold should succeed");
        assert_eq!(resource.get_ref_count(), 2);
        assert!(resource.hold(), "Second hold should succeed");
        assert_eq!(resource.get_ref_count(), 3);
    }

    #[test]
    fn test_hold_fails_after_shutdown() {
        let resource = ReferenceResourceCounter::new();
        resource.shutdown(0);
        assert!(!resource.is_available(), "Should not be available after shutdown");
        assert!(!resource.hold(), "Hold should fail after shutdown");
    }

    #[test]
    fn test_hold_rollback_when_refcount_negative() {
        let resource = ReferenceResourceCounter::new();
        // Manually set negative refCount
        resource.base.ref_count.store(-5, Ordering::Release);
        resource.base.available.store(true, Ordering::Release);

        assert!(!resource.hold(), "Hold should fail when refCount is negative");
        assert_eq!(resource.get_ref_count(), -5, "Should rollback increment");
    }

    #[test]
    fn test_release_decrements_refcount() {
        let resource = ReferenceResourceCounter::new();
        resource.hold(); // refCount = 2
        resource.release(); // refCount = 1
        assert_eq!(resource.get_ref_count(), 1);
        assert!(!resource.is_cleanup_over());
    }

    #[test]
    fn test_release_triggers_cleanup_at_zero() {
        let resource = ReferenceResourceCounter::new();
        resource.release(); // refCount: 1 -> 0
        assert_eq!(resource.get_ref_count(), 0);
        assert!(resource.is_cleanup_over(), "Cleanup should be triggered");
    }

    #[test]
    fn test_multiple_releases_go_negative() {
        let resource = ReferenceResourceCounter::new();
        resource.release(); // 1 -> 0, triggers cleanup
        resource.release(); // 0 -> -1
        assert_eq!(resource.get_ref_count(), -1);
        resource.release(); // -1 -> -2
        assert_eq!(resource.get_ref_count(), -2);
    }

    // ========================================================================
    // Shutdown Behavior Tests
    // ========================================================================

    #[test]
    fn test_shutdown_immediate_cleanup_when_no_holders() {
        let resource = ReferenceResourceCounter::new();
        resource.shutdown(0); // Releases initial reference
        assert!(!resource.is_available());
        assert_eq!(resource.get_ref_count(), 0);
        assert!(resource.is_cleanup_over());
    }

    #[test]
    fn test_shutdown_waits_for_holders() {
        let resource = ReferenceResourceCounter::new();
        resource.hold(); // refCount = 2
        resource.hold(); // refCount = 3

        resource.shutdown(0); // Releases initial ref: refCount = 2
        assert!(!resource.is_available());
        assert_eq!(resource.get_ref_count(), 2);
        assert!(!resource.is_cleanup_over(), "Should wait for holders");

        resource.release(); // refCount = 1
        assert!(!resource.is_cleanup_over());

        resource.release(); // refCount = 0, triggers cleanup
        assert!(resource.is_cleanup_over());
    }

    #[test]
    fn test_forced_shutdown_after_timeout() {
        let resource = ReferenceResourceCounter::new();
        resource.hold(); // refCount = 2
        resource.shutdown(50); // 50ms timeout, refCount = 1

        assert_eq!(resource.get_ref_count(), 1);
        assert!(!resource.is_cleanup_over());

        // Wait for timeout
        thread::sleep(Duration::from_millis(60));

        // Call shutdown again - should force
        resource.shutdown(50);

        let count = resource.get_ref_count();
        assert!(count <= -1000, "Should force negative after timeout, got {}", count);
    }

    #[test]
    fn test_no_forced_shutdown_before_timeout() {
        let resource = ReferenceResourceCounter::new();
        resource.hold(); // refCount = 2
        resource.shutdown(1000); // 1s timeout, refCount = 1

        thread::sleep(Duration::from_millis(10));
        resource.shutdown(1000); // Should NOT force yet

        assert_eq!(resource.get_ref_count(), 1, "Should not force before timeout");
    }

    #[test]
    fn test_shutdown_idempotent_after_first_call() {
        let resource = ReferenceResourceCounter::new();
        let timestamp_after_first = {
            resource.shutdown(1000);
            resource.base.first_shutdown_timestamp.load(Ordering::Acquire)
        };

        thread::sleep(Duration::from_millis(10));
        resource.shutdown(1000);
        let timestamp_after_second = resource.base.first_shutdown_timestamp.load(Ordering::Acquire);

        assert_eq!(
            timestamp_after_first, timestamp_after_second,
            "Timestamp should not change on subsequent shutdowns"
        );
    }

    // ========================================================================
    // Cleanup Execution Tests
    // ========================================================================

    #[test]
    fn test_cleanup_called_exactly_once() {
        struct TestResource {
            base: ReferenceResourceBase,
            cleanup_count: Arc<AtomicUsize>,
        }

        impl ReferenceResource for TestResource {
            fn base(&self) -> &ReferenceResourceBase {
                &self.base
            }

            fn cleanup(&self, _current_ref: i64) -> bool {
                self.cleanup_count.fetch_add(1, Ordering::SeqCst);
                true
            }
        }

        let cleanup_count = Arc::new(AtomicUsize::new(0));
        let resource = Arc::new(TestResource {
            base: ReferenceResourceBase::new(),
            cleanup_count: Arc::clone(&cleanup_count),
        });

        resource.hold(); // refCount = 2
        resource.release(); // refCount = 1
        resource.release(); // refCount = 0, triggers cleanup

        assert_eq!(cleanup_count.load(Ordering::SeqCst), 1, "Cleanup called once");

        // Additional releases should not trigger cleanup again
        resource.release(); // refCount = -1
        assert_eq!(cleanup_count.load(Ordering::SeqCst), 1, "Still only once");
    }

    // ========================================================================
    // Concurrent Tests
    // ========================================================================

    #[test]
    fn test_concurrent_hold_release() {
        let resource = Arc::new(ReferenceResourceCounter::new());
        let num_threads = 10;
        let iterations = 1000;

        let mut handles = vec![];
        for _ in 0..num_threads {
            let res = Arc::clone(&resource);
            handles.push(thread::spawn(move || {
                for _ in 0..iterations {
                    if res.hold() {
                        thread::yield_now();
                        res.release();
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(resource.get_ref_count(), 1, "Should return to initial state");
    }

    #[test]
    fn test_concurrent_shutdown() {
        let resource = Arc::new(ReferenceResourceCounter::new());
        let num_threads = 5;
        let barrier = Arc::new(Barrier::new(num_threads));

        let mut handles = vec![];
        for _ in 0..num_threads {
            let res = Arc::clone(&resource);
            let bar = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                bar.wait();
                res.shutdown(0);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(!resource.is_available());
        // Multiple shutdown calls may each call release(), leading to negative refCount
        assert!(resource.get_ref_count() <= 0, "refCount should be <= 0");
        assert!(resource.is_cleanup_over());
    }

    #[test]
    fn test_hold_release_race_with_shutdown() {
        let resource = Arc::new(ReferenceResourceCounter::new());

        let res1 = Arc::clone(&resource);
        let handle1 = thread::spawn(move || {
            for _ in 0..500 {
                if res1.hold() {
                    thread::yield_now();
                    res1.release();
                }
            }
        });

        let res2 = Arc::clone(&resource);
        let handle2 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            res2.shutdown(0);
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        assert!(!resource.is_available());
        assert!(resource.get_ref_count() <= 0);
    }

    // ========================================================================
    // Edge Cases
    // ========================================================================

    #[test]
    fn test_is_cleanup_over_checks_both_conditions() {
        let resource = ReferenceResourceCounter::new();
        resource.hold(); // refCount = 2

        // refCount > 0, cleanup_over = false
        assert!(!resource.is_cleanup_over());

        // Even if we manually set cleanup_over, refCount > 0 prevents it
        resource.base.cleanup_over.store(true, Ordering::Release);
        assert!(!resource.is_cleanup_over(), "refCount still > 0");
    }

    #[test]
    fn test_zero_timeout_forces_immediately() {
        let resource = ReferenceResourceCounter::new();
        resource.hold(); // refCount = 2
        resource.shutdown(0); // refCount = 1

        // Second shutdown with 0 timeout should force immediately
        resource.shutdown(0);

        let count = resource.get_ref_count();
        assert!(count <= -1000, "Should force with 0 timeout, got {}", count);
    }
}
