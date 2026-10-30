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

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;
const DAY_MS: i64 = 24 * HOUR_MS;

/// Settings of the `native` timer engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NativeTimerConfig {
    /// Delivery lanes; fixed for the life of a `timer-native/` directory.
    pub(crate) lane_count: u16,
    /// Wake-ups are rounded up to a multiple of it, so deliveries are batched per precision.
    pub(crate) precision_ms: i64,
    /// Timers due within this much of now are kept in memory.
    pub(crate) near_window_ms: i64,
    /// Memory the near wheel may use before loading waits for deliveries.
    pub(crate) near_wheel_memory_bytes: usize,
    /// Timers due within this much of now are indexed in hour buckets that point into the
    /// CommitLog, so it must end before CommitLog cleanup could delete their sources.
    pub(crate) mid_horizon_ms: i64,
    /// The latest deadline accepted, relative to now.
    pub(crate) max_delay_ms: i64,
    /// Size a payload segment fills before the next one starts.
    pub(crate) payload_segment_bytes: u64,
}

impl Default for NativeTimerConfig {
    fn default() -> Self {
        Self {
            lane_count: 16,
            precision_ms: 1_000,
            near_window_ms: 10 * MINUTE_MS,
            near_wheel_memory_bytes: 256 * 1024 * 1024,
            mid_horizon_ms: 48 * HOUR_MS,
            max_delay_ms: 400 * DAY_MS,
            payload_segment_bytes: 256 * 1024 * 1024,
        }
    }
}

impl NativeTimerConfig {
    /// The mid horizon for CommitLog files kept `file_reserved_hours`: 48 hours, or six hours
    /// less than the retention when that is shorter.
    pub(crate) fn mid_horizon_for(file_reserved_hours: i64) -> i64 {
        (48 * HOUR_MS).min(file_reserved_hours.saturating_sub(6).saturating_mul(HOUR_MS))
    }

    /// Checks the settings, naming the first that is invalid.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.lane_count == 0 {
            return Err("laneCount");
        }
        if self.precision_ms <= 0 || MINUTE_MS % self.precision_ms != 0 {
            return Err("precisionMs");
        }
        if self.near_window_ms <= 0 || self.near_window_ms > self.mid_horizon_ms {
            return Err("nearWindowSec");
        }
        if self.near_wheel_memory_bytes == 0 {
            return Err("nearWheelMemoryBytes");
        }
        if self.mid_horizon_ms < HOUR_MS {
            return Err("midHorizonHours");
        }
        if self.max_delay_ms < self.mid_horizon_ms {
            return Err("maxDelayDays");
        }
        if self.payload_segment_bytes == 0 || self.payload_segment_bytes > u64::from(u32::MAX) {
            return Err("payloadSegmentBytes");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_follow_the_design_and_validate() {
        let config = NativeTimerConfig::default();
        assert_eq!(config.validate(), Ok(()));
        assert_eq!(config.max_delay_ms, 400 * DAY_MS);
        assert_eq!(NativeTimerConfig::mid_horizon_for(72), 48 * HOUR_MS);
        assert_eq!(NativeTimerConfig::mid_horizon_for(24), 18 * HOUR_MS);
    }

    #[test]
    fn invalid_settings_are_named() {
        let invalid = |change: fn(&mut NativeTimerConfig)| {
            let mut config = NativeTimerConfig::default();
            change(&mut config);
            config.validate()
        };
        assert_eq!(invalid(|config| config.lane_count = 0), Err("laneCount"));
        assert_eq!(invalid(|config| config.precision_ms = 7), Err("precisionMs"));
        assert_eq!(
            invalid(|config| config.near_window_ms = 49 * HOUR_MS),
            Err("nearWindowSec")
        );
        assert_eq!(
            invalid(|config| config.mid_horizon_ms = MINUTE_MS),
            Err("nearWindowSec")
        );
        assert_eq!(invalid(|config| config.max_delay_ms = HOUR_MS), Err("maxDelayDays"));
        assert_eq!(
            invalid(|config| config.payload_segment_bytes = 1 << 32),
            Err("payloadSegmentBytes")
        );
    }
}
