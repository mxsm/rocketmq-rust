// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Clock-owning compatibility helpers for the canonical data-version value.

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::protocol::DataVersion;

pub fn new_data_version() -> DataVersion {
    DataVersion::with_values(0, current_millis(), 0)
}

pub trait DataVersionExt {
    fn next_version(&mut self);
    fn next_version_with(&mut self, state_version: i64);
}

impl DataVersionExt for DataVersion {
    fn next_version(&mut self) {
        self.next_version_with(0);
    }

    fn next_version_with(&mut self, state_version: i64) {
        self.next_version_with_timestamp(state_version, current_millis());
    }
}

fn current_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::current_millis;
    use super::new_data_version;
    use super::DataVersionExt;

    #[test]
    fn new_data_version_starts_at_zero_with_wall_clock_timestamp() {
        let before = current_millis();
        let version = new_data_version();
        let after = current_millis();

        assert_eq!(version.state_version(), 0);
        assert_eq!(version.counter(), 0);
        assert!(version.timestamp() > 0);
        assert!((before..=after).contains(&version.timestamp()));
    }

    #[test]
    fn next_version_bumps_counter_and_keeps_state_version_zero() {
        let mut version = new_data_version();
        let previous_timestamp = version.timestamp();

        version.next_version();

        assert_eq!(version.counter(), 1);
        assert_eq!(version.state_version(), 0);
        assert!(version.timestamp() >= previous_timestamp);
        assert!(version.timestamp() <= current_millis());
    }

    #[test]
    fn next_version_with_sets_state_version_and_bumps_counter() {
        let mut version = new_data_version();
        let previous_timestamp = version.timestamp();

        version.next_version_with(7);

        assert_eq!(version.state_version(), 7);
        assert_eq!(version.counter(), 1);
        assert!(version.timestamp() >= previous_timestamp);
    }

    #[test]
    fn repeated_version_helpers_yield_strictly_increasing_counters() {
        let mut version = new_data_version();
        let mut previous_counter = version.counter();

        for state_version in 1..=5 {
            version.next_version();
            assert!(version.counter() > previous_counter);
            previous_counter = version.counter();

            version.next_version_with(state_version);
            assert!(version.counter() > previous_counter);
            assert_eq!(version.state_version(), state_version);
            previous_counter = version.counter();
        }

        assert_eq!(version.counter(), 10);
    }
}
