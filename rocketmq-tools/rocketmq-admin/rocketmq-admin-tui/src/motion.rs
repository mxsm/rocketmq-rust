// Copyright 2026 The RocketMQ Rust Authors
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

//! Tick-driven animation primitives.
//!
//! Every effect is a function of the animation tick and of the tick at which an
//! observed value last changed. Nothing here reads a clock, so a frame is fully
//! determined by the application state and rendering stays reproducible in tests.

use std::f32::consts::TAU;

/// Frames rendered per second. One animation tick elapses per frame.
pub(crate) const FRAMES_PER_SECOND: u64 = 30;

/// A value paired with the tick of its most recent change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tracked<T> {
    current: T,
    previous: T,
    changed_at: u64,
}

impl<T: Copy + PartialEq> Tracked<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            current: value,
            previous: value,
            changed_at: 0,
        }
    }

    /// Records `value` and returns whether it differs from the last observation.
    pub(crate) fn observe(&mut self, value: T, tick: u64) -> bool {
        if value == self.current {
            return false;
        }
        self.previous = self.current;
        self.current = value;
        self.changed_at = tick;
        true
    }

    pub(crate) fn current(&self) -> T {
        self.current
    }

    /// Returns the value held before the most recent change.
    pub(crate) fn previous(&self) -> T {
        self.previous
    }

    pub(crate) fn changed_at(&self) -> u64 {
        self.changed_at
    }
}

/// Returns the completed fraction, in `0.0..=1.0`, of a transition of `duration` ticks.
pub(crate) fn progress(now: u64, started: u64, duration: u64) -> f32 {
    let elapsed = now.saturating_sub(started);
    if duration == 0 || elapsed >= duration {
        1.0
    } else {
        elapsed as f32 / duration as f32
    }
}

/// Decelerating curve: fast at the start, settling gently at the end.
pub(crate) fn ease_out_cubic(t: f32) -> f32 {
    let remaining = 1.0 - t.clamp(0.0, 1.0);
    1.0 - remaining * remaining * remaining
}

/// Returns a smooth `0.0..=1.0` oscillation that completes one cycle every `period` ticks.
pub(crate) fn wave(now: u64, period: u64) -> f32 {
    if period == 0 {
        return 0.0;
    }
    let phase = (now % period) as f32 / period as f32;
    0.5 - 0.5 * (phase * TAU).cos()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracked_value_records_the_previous_value_and_change_tick() {
        let mut tracked = Tracked::new(1_u8);

        assert!(!tracked.observe(1, 5));
        assert_eq!(tracked.changed_at(), 0);

        assert!(tracked.observe(2, 9));
        assert_eq!(tracked.current(), 2);
        assert_eq!(tracked.previous(), 1);
        assert_eq!(tracked.changed_at(), 9);
    }

    #[test]
    fn progress_is_clamped_to_the_transition_window() {
        assert_eq!(progress(10, 10, 4), 0.0);
        assert_eq!(progress(12, 10, 4), 0.5);
        assert_eq!(progress(99, 10, 4), 1.0);
        assert_eq!(progress(3, 10, 4), 0.0);
        assert_eq!(progress(10, 10, 0), 1.0);
    }

    #[test]
    fn easing_and_wave_stay_within_the_unit_interval() {
        assert_eq!(ease_out_cubic(0.0), 0.0);
        assert_eq!(ease_out_cubic(1.0), 1.0);
        assert_eq!(ease_out_cubic(7.0), 1.0);
        assert!(ease_out_cubic(0.5) > 0.5);

        assert!(wave(0, 40).abs() < f32::EPSILON);
        assert!((wave(20, 40) - 1.0).abs() < 1e-5);
        assert_eq!(wave(7, 0), 0.0);
        assert!((0..80).all(|tick| (0.0..=1.0).contains(&wave(tick, 40))));
    }
}
