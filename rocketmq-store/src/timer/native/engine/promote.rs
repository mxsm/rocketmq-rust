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

//! Bringing timers near: an hour bucket is split into minute runs once its hour comes within
//! the near window, and a minute run is loaded into the wheel once its minute does.
//!
//! An hour bucket is read once, sorted, and rewritten as one minute run per minute, so the
//! wheel holds about the near window of timers instead of whole hours. The split hour bucket is
//! retired: the next checkpoint leaves it out, and only then is it deleted. Until that
//! checkpoint, a restart recovers the hour bucket and drops the minute runs; after it, the
//! other way round. Either way every timer is in exactly one place.

use rocketmq_store_local::timer::native_format::NativeBucketId;
use rocketmq_store_local::timer::native_format::NativeBucketKind;
use rocketmq_store_local::timer::native_format::NativeIndexEntry;

use super::hour_end_ms;
use super::EngineState;
use super::NativeEngineError;
use super::NativeTimerEngine;

impl NativeTimerEngine {
    /// Makes the wheel hold the timers due before the end of the minute the near window
    /// reaches: splits the hour buckets starting before then, and loads the minute runs, oldest
    /// first, leaving out timers their lanes delivered. Past the memory budget, only minutes
    /// that have already started load, so due timers are never held back by later ones.
    pub(super) fn load_near(&self, state: &mut EngineState, now_ms: i64) -> Result<(), NativeEngineError> {
        let minute_ms = NativeBucketKind::Minute.window_ms();
        let reach = now_ms.saturating_add(self.config.near_window_ms);
        let horizon = reach.div_euclid(minute_ms).saturating_add(1).saturating_mul(minute_ms);
        self.split_hours(state, horizon)?;
        if horizon <= state.loaded_until_ms {
            return Ok(());
        }
        let pending: Vec<_> = state
            .minutes
            .iter()
            .copied()
            .filter(|minute| minute.start_ms() >= state.loaded_until_ms && minute.start_ms() < horizon)
            .collect();
        for minute in pending {
            if minute.start_ms() > now_ms && state.wheel.memory_bytes() >= self.config.near_wheel_memory_bytes {
                state.loaded_until_ms = minute.start_ms();
                return Ok(());
            }
            self.storage.visit_entries(minute, |entries| {
                for entry in entries {
                    state.insert_undelivered(*entry);
                }
                Ok(())
            })?;
            state.loaded_until_ms = minute.end_ms();
        }
        state.loaded_until_ms = horizon;
        Ok(())
    }

    /// Splits every hour bucket starting before `horizon` into minute runs, oldest first, and
    /// retires it. Every hour starting before `horizon` then counts as split.
    ///
    /// A failure part way leaves the hour bucket in place, to be split again; the minute runs
    /// may then hold its timers twice, which the wheel adds once, keyed by their cursors.
    fn split_hours(&self, state: &mut EngineState, horizon: i64) -> Result<(), NativeEngineError> {
        let hours: Vec<_> = state
            .hours
            .iter()
            .copied()
            .take_while(|hour| hour.start_ms() < horizon)
            .collect();
        for hour in hours {
            let mut timers = Vec::new();
            self.storage.visit_entries(hour, |entries| {
                timers.extend(
                    entries
                        .iter()
                        .filter(|entry| entry.cursor() > state.watermark(entry.lane))
                        .copied(),
                );
                Ok(())
            })?;
            timers.sort_by_key(NativeIndexEntry::cursor);
            for run in timers.chunk_by(|left, right| minute_of(left) == minute_of(right)) {
                let Some(minute) = run.first().and_then(minute_of) else {
                    continue;
                };
                self.storage.append_entries(minute, run)?;
                state.add_written(minute, run);
            }
            state.hours.remove(&hour);
            state.bucket_min_offsets.remove(&hour);
            state.retired.insert(hour);
        }
        state.promoted_until_ms = state.promoted_until_ms.max(hour_end_ms(horizon.saturating_sub(1)));
        Ok(())
    }
}

fn minute_of(entry: &NativeIndexEntry) -> Option<NativeBucketId> {
    NativeBucketId::containing(NativeBucketKind::Minute, entry.due_ms).ok()
}
