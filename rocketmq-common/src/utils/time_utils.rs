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

//! Time utility functions for obtaining the current wall-clock time.
//!
//! All functions query [`std::time::SystemTime::now`] and return the elapsed
//! duration since the Unix epoch in the requested unit.

/// Returns the current Unix timestamp in milliseconds.
///
/// # Panics
///
/// Panics if the system clock is set to a time before the Unix epoch.
#[deprecated(since = "0.8.0", note = "use `current_millis` instead")]
#[inline(always)]
pub fn get_current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Returns the current Unix timestamp in nanoseconds.
///
/// # Panics
///
/// Panics if the system clock is set to a time before the Unix epoch.
#[deprecated(since = "0.8.0", note = "use `current_nano` instead")]
#[inline(always)]
pub fn get_current_nano() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Returns the current Unix timestamp in milliseconds.
///
/// # Panics
///
/// Panics if the system clock is set to a time before the Unix epoch.
#[inline(always)]
pub fn current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Returns the current Unix timestamp in nanoseconds.
///
/// # Panics
///
/// Panics if the system clock is set to a time before the Unix epoch.
#[inline(always)]
pub fn current_nano() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Returns the current Unix timestamp in whole seconds.
///
/// # Panics
///
/// Panics if the system clock is set to a time before the Unix epoch.
#[inline(always)]
pub fn current_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use super::*;

    #[allow(deprecated)]
    #[test]
    fn get_current_millis_returns_correct_value() {
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let current = get_current_millis();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        assert!(current >= before && current <= after);
    }

    #[allow(deprecated)]
    #[test]
    fn get_current_nano_returns_correct_value() {
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        let current = get_current_nano();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        assert!(current >= before && current <= after);
    }

    #[test]
    fn current_millis_returns_correct_value() {
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let current = current_millis();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        assert!(current >= before && current <= after);
    }

    #[test]
    fn current_nano_returns_correct_value() {
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        let current = current_nano();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        assert!(current >= before && current <= after);
    }

    #[test]
    fn current_secs_returns_correct_value() {
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let current = current_secs();
        let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        assert!(current >= before && current <= after);
    }

    #[test]
    fn all_timestamps_are_positive() {
        assert!(current_millis() > 0);
        assert!(current_nano() > 0);
        assert!(current_secs() > 0);
    }

    #[test]
    fn millis_greater_than_secs() {
        let secs = current_secs();
        let millis = current_millis();
        assert!(millis > secs);
    }

    #[test]
    fn nanos_greater_than_millis() {
        let millis = current_millis();
        let nanos = current_nano();
        assert!(nanos > millis);
    }

    #[test]
    fn timestamps_are_non_decreasing() {
        let t1 = current_millis();
        std::thread::sleep(Duration::from_millis(5));
        let t2 = current_millis();
        assert!(t2 >= t1);
    }

    #[test]
    fn secs_consistent_with_millis() {
        let secs = current_secs();
        let millis_as_secs = current_millis() / 1000;
        // millis is sampled after secs; millis_as_secs is equal to secs or one ahead.
        assert!(millis_as_secs == secs || millis_as_secs == secs + 1);
    }
}
