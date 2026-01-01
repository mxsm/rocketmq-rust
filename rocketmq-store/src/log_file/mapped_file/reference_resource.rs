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

use std::sync::atomic::Ordering;

use super::reference_resource_counter::ReferenceResourceBase;

/// A trait that defines a reference resource with various lifecycle methods.
///
/// This trait provides default implementations for all lifecycle management methods,
/// mirroring Java's `ReferenceResource` abstract class design where only `cleanup()`
/// needs to be implemented.
pub trait ReferenceResource: Send + Sync {
    /// Returns a reference to the base structure containing all reference counting state.
    ///
    /// This is the only method that concrete types must implement.
    /// All other methods have default implementations based on this.
    fn base(&self) -> &ReferenceResourceBase;

    /// Attempts to acquire a reference to this resource.
    ///
    /// # Returns
    /// * `true` if the reference count was successfully increased.
    /// * `false` if the resource is not available.
    #[inline]
    fn hold(&self) -> bool {
        // Fast path: check availability without lock (Relaxed is sufficient for initial check)
        if !self.base().available.load(Ordering::Relaxed) {
            return false;
        }

        // parking_lot::Mutex never panics, no need for unwrap()
        let _guard = self.base().hold_lock.lock();

        // Double-check with Acquire ordering after acquiring lock
        if self.base().available.load(Ordering::Acquire) {
            let prev_count = self.base().ref_count.fetch_add(1, Ordering::Relaxed);
            if prev_count > 0 {
                return true;
            } else {
                self.base().ref_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        false
    }

    /// Checks if the resource is available.
    ///
    /// # Returns
    /// * `true` if the resource is available.
    /// * `false` otherwise.
    #[inline]
    fn is_available(&self) -> bool {
        // Relaxed ordering is sufficient for availability check
        // Synchronization is handled by hold_lock when needed
        self.base().available.load(Ordering::Relaxed)
    }

    /// Shuts down the resource.
    ///
    ///
    /// # Parameters
    /// * `interval_forcibly` - The interval in milliseconds to forcibly shut down the resource.
    fn shutdown(&self, interval_forcibly: u64) {
        use rocketmq_common::TimeUtils::get_current_millis;

        if self.base().available.load(Ordering::Acquire) {
            self.base().available.store(false, Ordering::Release);
            self.base()
                .first_shutdown_timestamp
                .store(get_current_millis(), Ordering::Release);
            self.release();
        } else if self.get_ref_count() > 0 {
            let elapsed =
                get_current_millis().saturating_sub(self.base().first_shutdown_timestamp.load(Ordering::Acquire));

            if elapsed >= interval_forcibly {
                let current_count = self.get_ref_count();
                self.base().ref_count.store(-1000 - current_count, Ordering::Release);
                self.release();
            }
        }
    }

    /// Releases the resource, decreasing the reference count.
    #[inline]
    fn release(&self) {
        // Release ordering ensures all previous operations are visible before decrement
        let value = self.base().ref_count.fetch_sub(1, Ordering::Release) - 1;

        // Fast path: if still has references, return immediately
        if value > 0 {
            return;
        }

        // Acquire fence to synchronize with the Release above
        std::sync::atomic::fence(Ordering::Acquire);

        let _guard = self.base().release_lock.lock();

        // Relaxed is fine here as we're protected by the lock
        if !self.base().cleanup_over.load(Ordering::Relaxed) {
            let cleanup_result = self.cleanup(value);
            self.base().cleanup_over.store(cleanup_result, Ordering::Release);
        }
    }

    /// Gets the current reference count of the resource.
    /// # Returns
    /// The current reference count.
    #[inline]
    fn get_ref_count(&self) -> i64 {
        // Relaxed ordering is sufficient for reading the current count
        // for informational purposes
        self.base().ref_count.load(Ordering::Relaxed)
    }

    /// Cleans up the resource. **This is the only abstract method that must be implemented.**
    ///
    /// # Parameters
    /// * `current_ref` - The reference count when cleanup was triggered.
    ///
    /// # Returns
    /// * `true` if the resource was successfully cleaned up.
    /// * `false` otherwise.
    fn cleanup(&self, current_ref: i64) -> bool;

    /// Checks if the cleanup process is over.
    ///
    ///
    /// # Returns
    /// * `true` if the cleanup is over.
    /// * `false` otherwise.
    #[inline]
    fn is_cleanup_over(&self) -> bool {
        self.base().cleanup_over.load(Ordering::Relaxed) && self.base().ref_count.load(Ordering::Relaxed) <= 0
    }
}
