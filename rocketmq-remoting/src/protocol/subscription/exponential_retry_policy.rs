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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::subscription::retry_policy::RetryPolicy;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExponentialRetryPolicy {
    initial: u64,
    max: u64,
    multiplier: u64,
}

impl Default for ExponentialRetryPolicy {
    #[allow(clippy::incompatible_msrv)]
    fn default() -> Self {
        ExponentialRetryPolicy {
            initial: Duration::from_secs(5).as_millis() as u64,
            max: Duration::from_hours(2).as_millis() as u64,
            multiplier: 2,
        }
    }
}

impl ExponentialRetryPolicy {
    pub fn new(initial: u64, max: u64, multiplier: u64) -> Self {
        ExponentialRetryPolicy {
            initial,
            max,
            multiplier,
        }
    }

    pub fn initial(&self) -> u64 {
        self.initial
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    pub fn multiplier(&self) -> u64 {
        self.multiplier
    }

    pub fn set_initial(&mut self, initial: u64) {
        self.initial = initial;
    }

    pub fn set_max(&mut self, max: u64) {
        self.max = max;
    }

    pub fn set_multiplier(&mut self, multiplier: u64) {
        self.multiplier = multiplier;
    }
}

impl RetryPolicy for ExponentialRetryPolicy {
    fn next_delay_duration(&self, reconsume_times: i32) -> i64 {
        let reconsume_times = reconsume_times.clamp(0, 32) as u32;
        let delay = self.initial * self.multiplier.pow(reconsume_times);
        delay.min(self.max) as i64
    }
}

#[cfg(test)]
mod exponential_retry_policy_tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn default_policy_has_expected_values() {
        let policy = ExponentialRetryPolicy::default();
        assert_eq!(policy.initial(), Duration::from_secs(5).as_millis() as u64);
        assert_eq!(policy.max(), Duration::from_hours(2).as_millis() as u64);
        assert_eq!(policy.multiplier(), 2);
    }

    #[test]
    fn custom_policy_values_are_set_correctly() {
        let policy = ExponentialRetryPolicy::new(1000, 3600000, 3);
        assert_eq!(policy.initial(), 1000);
        assert_eq!(policy.max(), 3600000);
        assert_eq!(policy.multiplier(), 3);
    }

    #[test]
    fn next_delay_duration_does_not_exceed_max() {
        let policy = ExponentialRetryPolicy::new(1000, 5000, 2);
        let delay = policy.next_delay_duration(10);
        assert_eq!(delay, 5000);
    }

    #[test]
    fn next_delay_duration_follows_exponential_growth() {
        let policy = ExponentialRetryPolicy::new(1000, 100000, 2);
        let first_delay = policy.next_delay_duration(1);
        let second_delay = policy.next_delay_duration(2);
        assert!(second_delay > first_delay);
        assert_eq!(first_delay, 2000);
        assert_eq!(second_delay, 4000);
    }

    #[test]
    fn next_delay_duration_handles_negative_reconsume_times_as_zero() {
        let policy = ExponentialRetryPolicy::default();
        let delay = policy.next_delay_duration(-1);
        assert_eq!(delay, policy.initial() as i64);
    }

    #[test]
    fn next_delay_duration_caps_reconsume_times_to_avoid_overflow() {
        let policy = ExponentialRetryPolicy::new(1000, u64::MAX, 2);
        let delay = policy.next_delay_duration(100); // Use a large number to test capping
        assert!(delay > 0); // Exact value depends on implementation details
    }
}
