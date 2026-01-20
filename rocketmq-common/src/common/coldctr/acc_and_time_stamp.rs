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

//! Accumulator and timestamp tracking for cold data control

use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::TimeUtils::get_current_millis;

/// Accumulator and timestamp structure for tracking cold read operations
///
/// This structure tracks cold data access patterns by maintaining:
/// - An atomic counter for cold access accumulation
/// - Timestamp of the last cold read operation
/// - Timestamp when this tracker was created
#[derive(Debug)]
pub struct AccAndTimeStamp {
    /// Atomic counter for cold data access accumulation
    cold_acc: AtomicU64,
    /// Timestamp (in milliseconds) of the last cold read operation
    last_cold_read_time_millis: AtomicU64,
    /// Timestamp (in milliseconds) when this structure was created
    create_time_millis: u64,
}

impl AccAndTimeStamp {
    /// Create a new AccAndTimeStamp with the given initial cold accumulation value
    ///
    /// # Arguments
    /// * `cold_acc` - Initial value for the cold accumulation counter
    ///
    /// # Returns
    /// A new AccAndTimeStamp instance with current timestamp
    pub fn new(cold_acc: u64) -> Self {
        let current_time = get_current_millis();
        Self {
            cold_acc: AtomicU64::new(cold_acc),
            last_cold_read_time_millis: AtomicU64::new(current_time),
            create_time_millis: current_time,
        }
    }

    /// Create a new AccAndTimeStamp with zero initial accumulation
    ///
    /// # Returns
    /// A new AccAndTimeStamp instance with cold_acc initialized to 0
    pub fn default_new() -> Self {
        Self::new(0)
    }

    /// Get the current cold accumulation value
    ///
    /// # Returns
    /// The current value of the cold accumulation counter
    #[inline]
    pub fn get_cold_acc(&self) -> u64 {
        self.cold_acc.load(Ordering::Relaxed)
    }

    /// Set the cold accumulation value
    ///
    /// # Arguments
    /// * `value` - The new value for the cold accumulation counter
    #[inline]
    pub fn set_cold_acc(&self, value: u64) {
        self.cold_acc.store(value, Ordering::Relaxed);
    }

    /// Increment the cold accumulation counter by a given amount
    ///
    /// # Arguments
    /// * `delta` - The amount to add to the counter
    ///
    /// # Returns
    /// The previous value of the counter
    #[inline]
    pub fn add_cold_acc(&self, delta: u64) -> u64 {
        self.cold_acc.fetch_add(delta, Ordering::Relaxed)
    }

    /// Get a reference to the underlying AtomicU64 for cold accumulation
    ///
    /// This allows direct atomic operations on the counter if needed
    ///
    /// # Returns
    /// Reference to the AtomicU64 counter
    #[inline]
    pub fn cold_acc_atomic(&self) -> &AtomicU64 {
        &self.cold_acc
    }

    /// Get the timestamp of the last cold read operation
    ///
    /// # Returns
    /// Timestamp in milliseconds since epoch
    #[inline]
    pub fn get_last_cold_read_time_millis(&self) -> u64 {
        self.last_cold_read_time_millis.load(Ordering::Relaxed)
    }

    /// Set the timestamp of the last cold read operation
    ///
    /// # Arguments
    /// * `time_millis` - Timestamp in milliseconds since epoch
    #[inline]
    pub fn set_last_cold_read_time_millis(&self, time_millis: u64) {
        self.last_cold_read_time_millis.store(time_millis, Ordering::Relaxed);
    }

    /// Update the last cold read timestamp to the current time
    #[inline]
    pub fn update_last_cold_read_time(&self) {
        self.set_last_cold_read_time_millis(get_current_millis());
    }

    /// Get the creation timestamp of this structure
    ///
    /// # Returns
    /// Timestamp in milliseconds since epoch when this structure was created
    #[inline]
    pub fn get_create_time_millis(&self) -> u64 {
        self.create_time_millis
    }
}

impl Default for AccAndTimeStamp {
    fn default() -> Self {
        Self::default_new()
    }
}

impl fmt::Display for AccAndTimeStamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AccAndTimeStamp{{coldAcc={}, lastColdReadTimeMills={}, createTimeMills={}}}",
            self.get_cold_acc(),
            self.get_last_cold_read_time_millis(),
            self.create_time_millis
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acc_and_time_stamp_new_and_default() {
        let acc_and_time_stamp = AccAndTimeStamp::new(100);
        assert_eq!(acc_and_time_stamp.get_cold_acc(), 100);
        assert!(acc_and_time_stamp.get_last_cold_read_time_millis() > 0);
        assert!(acc_and_time_stamp.get_create_time_millis() > 0);

        let acc_and_time_stamp = AccAndTimeStamp::default_new();
        assert_eq!(acc_and_time_stamp.get_cold_acc(), 0);

        let acc_and_time_stamp = AccAndTimeStamp::default();
        assert_eq!(acc_and_time_stamp.get_cold_acc(), 0);
    }

    #[test]
    fn acc_and_time_stamp_cold_acc_methods() {
        let acc_and_time_stamp = AccAndTimeStamp::default();
        acc_and_time_stamp.set_cold_acc(100);
        assert_eq!(acc_and_time_stamp.get_cold_acc(), 100);

        let previous = acc_and_time_stamp.add_cold_acc(50);
        assert_eq!(previous, 100);
        assert_eq!(acc_and_time_stamp.get_cold_acc(), 150);

        let atomic = acc_and_time_stamp.cold_acc_atomic();
        atomic.store(200, Ordering::Relaxed);
        assert_eq!(acc_and_time_stamp.get_cold_acc(), 200);
    }

    #[test]
    fn acc_and_time_stamp_last_cold_read_time_methods() {
        let acc_and_time_stamp = AccAndTimeStamp::default();
        acc_and_time_stamp.set_last_cold_read_time_millis(123456789);
        assert_eq!(acc_and_time_stamp.get_last_cold_read_time_millis(), 123456789);

        let acc_and_time_stamp = AccAndTimeStamp::default();
        let old_time = acc_and_time_stamp.get_last_cold_read_time_millis();
        acc_and_time_stamp.update_last_cold_read_time();
        let new_time = acc_and_time_stamp.get_last_cold_read_time_millis();
        assert!(new_time >= old_time);
    }

    #[test]
    fn acc_and_time_stamp_format() {
        let acc_and_time_stamp = AccAndTimeStamp::new(100);
        acc_and_time_stamp.set_last_cold_read_time_millis(200);

        let display = format!("{}", acc_and_time_stamp);
        assert!(display.contains("AccAndTimeStamp{"));
        assert!(display.contains("coldAcc=100"));
        assert!(display.contains("lastColdReadTimeMills=200"));
        assert!(display.contains("createTimeMills="));

        let debug = format!("{:?}", acc_and_time_stamp);
        assert!(debug.contains("AccAndTimeStamp"));
        assert!(debug.contains("cold_acc"));
        assert!(debug.contains("last_cold_read_time_millis"));
        assert!(debug.contains("create_time_millis"));
    }
}
