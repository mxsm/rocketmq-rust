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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::subscription::retry_policy::RetryPolicy;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomizedRetryPolicy {
    next: Vec<i64>,
}

impl CustomizedRetryPolicy {
    pub fn next(&self) -> &[i64] {
        &self.next
    }
}

impl Default for CustomizedRetryPolicy {
    fn default() -> Self {
        let next = vec![
            1_000,     // 1s
            5_000,     // 5s
            10_000,    // 10s
            30_000,    // 30s
            60_000,    // 1m
            120_000,   // 2m
            180_000,   // 3m
            240_000,   // 4m
            300_000,   // 5m
            360_000,   // 6m
            420_000,   // 7m
            480_000,   // 8m
            540_000,   // 9m
            600_000,   // 10m
            1_200_000, // 20m
            1_800_000, // 30m
            3_600_000, // 1h
            7_200_000, // 2h
        ];
        CustomizedRetryPolicy { next }
    }
}

impl RetryPolicy for CustomizedRetryPolicy {
    fn next_delay_duration(&self, reconsume_times: i32) -> i64 {
        let reconsume_times = reconsume_times.max(0) as usize;
        let index = (reconsume_times + 2).min(self.next.len() - 1);
        self.next[index]
    }
}

#[cfg(test)]
mod customized_retry_policy_tests {
    use super::*;

    #[test]
    fn default_creates_expected_sequence() {
        let policy = CustomizedRetryPolicy::default();
        let expected_sequence = vec![
            1_000, 5_000, 10_000, 30_000, 60_000, 120_000, 180_000, 240_000, 300_000, 360_000, 420_000, 480_000,
            540_000, 600_000, 1_200_000, 1_800_000, 3_600_000, 7_200_000,
        ];
        assert_eq!(policy.next, expected_sequence);
    }

    #[test]
    fn next_delay_duration_returns_correct_delay_for_valid_reconsume_times() {
        let policy = CustomizedRetryPolicy::default();
        let test_cases = vec![(0, 10000), (1, 30000), (16, 7_200_000), (17, 7_200_000)];

        for (reconsume_times, expected_delay) in test_cases {
            let delay = policy.next_delay_duration(reconsume_times);
            assert_eq!(delay, expected_delay, "Failed for reconsume_times: {}", reconsume_times);
        }
    }

    #[test]
    fn next_delay_duration_handles_negative_reconsume_times() {
        let policy = CustomizedRetryPolicy::default();
        let delay = policy.next_delay_duration(-1);
        assert_eq!(
            delay, 10000,
            "Should handle negative reconsume times by treating them as 0"
        );
    }

    #[test]
    fn next_delay_duration_scales_with_reconsume_times() {
        let policy = CustomizedRetryPolicy::default();
        let first_delay = policy.next_delay_duration(0);
        let second_delay = policy.next_delay_duration(1);
        assert!(second_delay > first_delay, "Delay should increase with reconsume times");
    }
}
