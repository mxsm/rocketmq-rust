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

use std::time::Instant;

/// Clock boundary used by timer correctness decisions.
///
/// Wall time determines an absolute delivery deadline. Monotonic elapsed time is exposed from the
/// same object so later clock-safety policies do not need a second, inconsistent abstraction.
pub(crate) trait TimerClock: Send + Sync {
    fn wall_time_ms(&self) -> i64;

    fn monotonic_elapsed_ms(&self) -> u64;
}

pub(crate) struct SystemTimerClock {
    started_at: Instant,
}

impl Default for SystemTimerClock {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl TimerClock for SystemTimerClock {
    fn wall_time_ms(&self) -> i64 {
        rocketmq_runtime::common::time_utils::current_millis() as i64
    }

    fn monotonic_elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_clock_exposes_wall_and_monotonic_time() {
        let clock = SystemTimerClock::default();
        assert!(clock.wall_time_ms() > 0);
        assert!(clock.monotonic_elapsed_ms() < 1_000);
    }
}
