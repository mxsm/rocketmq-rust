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
    fn configuration_methods_and_serde_preserve_values() {
        let default = ExponentialRetryPolicy::default();
        assert_eq!(
            (default.initial(), default.max(), default.multiplier()),
            (
                Duration::from_secs(5).as_millis() as u64,
                Duration::from_hours(2).as_millis() as u64,
                2,
            )
        );

        let mut policy = ExponentialRetryPolicy::new(1_000, 3_600_000, 3);
        policy.set_initial(2_000);
        policy.set_max(7_200_000);
        policy.set_multiplier(4);

        let json = serde_json::to_string(&policy).unwrap();
        assert_eq!(json, r#"{"initial":2000,"max":7200000,"multiplier":4}"#);

        let decoded: ExponentialRetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(
            (decoded.initial(), decoded.max(), decoded.multiplier()),
            (2_000, 7_200_000, 4)
        );
    }

    #[test]
    fn next_delay_duration_handles_boundaries() {
        let policy = ExponentialRetryPolicy::new(1_000, 5_000, 2);
        for (reconsume_times, expected_delay) in [(-1, 1_000), (0, 1_000), (1, 2_000), (2, 4_000), (3, 5_000)] {
            assert_eq!(policy.next_delay_duration(reconsume_times), expected_delay);
        }

        let uncapped_max = ExponentialRetryPolicy::new(1, u64::MAX, 2);
        assert_eq!(uncapped_max.next_delay_duration(100), 1_i64 << 32);
    }
}
