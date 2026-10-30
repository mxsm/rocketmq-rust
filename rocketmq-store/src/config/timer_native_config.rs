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

use serde::Deserialize;

const MIB: u64 = 1024 * 1024;

/// Settings of the native timer engine, used under `timerStoreMode = native`.
///
/// Deadlines are rounded to the store's `timerPrecisionMs`, as the Java-compatible engine
/// rounds them.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct TimerNativeConfig {
    /// The longest delay admitted, in days.
    pub max_delay_days: u16,
    /// Delivery lanes. Fixed once the `timer-native` directory exists.
    pub lane_count: u16,
    /// Timers due within this many seconds are kept in memory.
    pub near_window_sec: u64,
    /// Memory the in-memory timers may use before loading more waits for deliveries.
    pub near_wheel_memory_bytes: u64,
    /// Timers due within this many hours point into the CommitLog; later ones are copied. The
    /// engine uses at most `fileReservedTime` less six hours, so the CommitLog still holds a
    /// timer's record when it is delivered.
    pub mid_horizon_hours: u16,
    /// Size a payload segment of copied timers fills before the next one starts.
    pub payload_segment_bytes: u64,
    /// How often indexed timers and delivery progress are made durable.
    pub checkpoint_interval_ms: u64,
    /// Due timers one lane takes per batch.
    pub delivery_batch_messages: usize,
    /// Writes one batch keeps in flight, so the CommitLog can merge them.
    pub delivery_inflight_writes: usize,
}

impl Default for TimerNativeConfig {
    fn default() -> Self {
        Self {
            max_delay_days: 400,
            lane_count: 16,
            near_window_sec: 600,
            near_wheel_memory_bytes: 256 * MIB,
            mid_horizon_hours: 48,
            payload_segment_bytes: 256 * MIB,
            checkpoint_interval_ms: 5_000,
            delivery_batch_messages: 512,
            delivery_inflight_writes: 32,
        }
    }
}
