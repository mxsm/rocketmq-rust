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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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

/// Persistable operational state derived from wall/monotonic observations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TimerClockState {
    #[default]
    Safe = 0,
    Unsafe = 1,
}

/// Single clock-safety boundary shared by scanner, delivery, and promotion gates.
pub(crate) struct TimerClockSafety {
    clock: Arc<dyn TimerClock>,
    maximum_backward_jump_ms: i64,
    last_wall_ms: AtomicI64,
    last_monotonic_ms: AtomicU64,
    state: AtomicU8,
    backward_jumps: AtomicU64,
    forward_jump_ms: AtomicU64,
}

impl TimerClockSafety {
    pub(crate) fn new(clock: Arc<dyn TimerClock>, maximum_backward_jump_ms: i64) -> Self {
        Self {
            clock,
            maximum_backward_jump_ms: maximum_backward_jump_ms.max(0),
            last_wall_ms: AtomicI64::new(i64::MIN),
            last_monotonic_ms: AtomicU64::new(0),
            state: AtomicU8::new(TimerClockState::Safe as u8),
            backward_jumps: AtomicU64::new(0),
            forward_jump_ms: AtomicU64::new(0),
        }
    }

    /// Observes both clocks and latches CLOCK_UNSAFE after a large rollback.
    pub(crate) fn observe(&self) -> TimerClockObservation {
        let wall_ms = self.clock.wall_time_ms();
        let monotonic_ms = self.clock.monotonic_elapsed_ms();
        let previous_wall = self.last_wall_ms.swap(wall_ms, Ordering::AcqRel);
        let previous_monotonic = self.last_monotonic_ms.swap(monotonic_ms, Ordering::AcqRel);
        let mut jump_ms = 0i64;
        if previous_wall != i64::MIN {
            jump_ms = wall_ms.saturating_sub(previous_wall);
            if jump_ms < self.maximum_backward_jump_ms.saturating_neg() || monotonic_ms < previous_monotonic {
                self.state.store(TimerClockState::Unsafe as u8, Ordering::Release);
                self.backward_jumps.fetch_add(1, Ordering::Relaxed);
            } else if jump_ms > 0 {
                self.forward_jump_ms.fetch_max(jump_ms as u64, Ordering::Relaxed);
            }
        }
        TimerClockObservation {
            wall_time_ms: wall_ms,
            monotonic_time_ms: monotonic_ms,
            jump_ms,
            state: self.state(),
        }
    }

    pub(crate) fn state(&self) -> TimerClockState {
        match self.state.load(Ordering::Acquire) {
            0 => TimerClockState::Safe,
            _ => TimerClockState::Unsafe,
        }
    }

    /// Explicit operator/recovery acknowledgement after the wall clock is verified.
    pub(crate) fn acknowledge_safe(&self) {
        self.last_wall_ms.store(self.clock.wall_time_ms(), Ordering::Release);
        self.last_monotonic_ms
            .store(self.clock.monotonic_elapsed_ms(), Ordering::Release);
        self.state.store(TimerClockState::Safe as u8, Ordering::Release);
    }

    pub(crate) fn snapshot(&self) -> TimerClockSafetySnapshot {
        TimerClockSafetySnapshot {
            state: self.state(),
            backward_jumps: self.backward_jumps.load(Ordering::Relaxed),
            largest_forward_jump_ms: self.forward_jump_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerClockObservation {
    pub(crate) wall_time_ms: i64,
    pub(crate) monotonic_time_ms: u64,
    pub(crate) jump_ms: i64,
    pub(crate) state: TimerClockState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerClockSafetySnapshot {
    pub(crate) state: TimerClockState,
    pub(crate) backward_jumps: u64,
    pub(crate) largest_forward_jump_ms: u64,
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
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::AtomicU64;

    use super::*;

    struct ManualClock {
        wall: AtomicI64,
        monotonic: AtomicU64,
    }

    impl TimerClock for ManualClock {
        fn wall_time_ms(&self) -> i64 {
            self.wall.load(Ordering::Acquire)
        }

        fn monotonic_elapsed_ms(&self) -> u64 {
            self.monotonic.load(Ordering::Acquire)
        }
    }

    #[test]
    fn timer_clock_exposes_wall_and_monotonic_time() {
        let clock = SystemTimerClock::default();
        assert!(clock.wall_time_ms() > 0);
        assert!(clock.monotonic_elapsed_ms() < 1_000);
    }

    #[test]
    fn backward_wall_jump_latches_unsafe_without_treating_monotonic_time_as_a_deadline() {
        let clock = Arc::new(ManualClock {
            wall: AtomicI64::new(86_400_000),
            monotonic: AtomicU64::new(10),
        });
        let safety = TimerClockSafety::new(clock.clone(), 1_000);
        assert_eq!(safety.observe().state, TimerClockState::Safe);
        clock.wall.store(82_800_000, Ordering::Release);
        clock.monotonic.store(20, Ordering::Release);
        assert_eq!(safety.observe().state, TimerClockState::Unsafe);
        clock.wall.store(90_000_000, Ordering::Release);
        clock.monotonic.store(30, Ordering::Release);
        assert_eq!(safety.observe().state, TimerClockState::Unsafe);
        safety.acknowledge_safe();
        assert_eq!(safety.state(), TimerClockState::Safe);
    }

    #[test]
    fn forward_jumps_remain_safe_and_backward_jumps_require_acknowledgement() {
        const HOUR_MS: i64 = 3_600_000;
        let clock = Arc::new(ManualClock {
            wall: AtomicI64::new(10 * HOUR_MS),
            monotonic: AtomicU64::new(10),
        });
        let safety = TimerClockSafety::new(clock.clone(), 1_000);
        assert_eq!(safety.observe().state, TimerClockState::Safe);

        for forward in [HOUR_MS, 7 * 24 * HOUR_MS] {
            clock.wall.fetch_add(forward, Ordering::AcqRel);
            clock.monotonic.fetch_add(10, Ordering::AcqRel);
            assert_eq!(safety.observe().state, TimerClockState::Safe);
        }
        assert_eq!(safety.snapshot().largest_forward_jump_ms, (7 * 24 * HOUR_MS) as u64);

        clock.wall.fetch_sub(HOUR_MS, Ordering::AcqRel);
        clock.monotonic.fetch_add(10, Ordering::AcqRel);
        assert_eq!(safety.observe().state, TimerClockState::Unsafe);
        clock.wall.fetch_sub(24 * HOUR_MS, Ordering::AcqRel);
        clock.monotonic.fetch_add(10, Ordering::AcqRel);
        assert_eq!(safety.observe().state, TimerClockState::Unsafe);

        safety.acknowledge_safe();
        assert_eq!(safety.state(), TimerClockState::Safe);
    }
}
