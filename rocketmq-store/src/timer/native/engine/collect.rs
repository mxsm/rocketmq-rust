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

//! Collecting what delivery left behind.
//!
//! A minute run is collected once a checkpoint has made every lane's watermark durable past the
//! last entry that lane has in the run, so a restart would deliver nothing from it. Runs are
//! collected oldest first, and only for minutes that have passed and whose Timer CQ records
//! are indexed, so no timer can still be on its way to one. A collected run is retired like a
//! split hour: the next checkpoint leaves it out, and then it is deleted. The CommitLog offsets
//! it held no longer hold back the fence.
//!
//! Once collection passes the end of a UTC day, the payload copies of the timers due that day
//! are retired the same way. A recall is dropped once collection passes both its deadline and
//! the time it was indexed: the timer it names was indexed by then, at the latest at that
//! time, and has been delivered or cancelled.

use rocketmq_store_local::timer::native_format::NativeBucketKind;
use rocketmq_store_local::timer::native_format::NativeDueCursor;

use super::EngineState;
use super::NativeTimerEngine;

impl NativeTimerEngine {
    /// Collects the minute runs, payload days and recalls that a checkpoint made durable
    /// `watermarks` for leaves nothing to deliver from, at `now_ms`.
    pub(super) fn collect(&self, state: &mut EngineState, watermarks: &[NativeDueCursor], now_ms: i64) {
        let minute_ms = NativeBucketKind::Minute.window_ms();
        let limit = now_ms
            .min(state.visible_through_ms)
            .div_euclid(minute_ms)
            .saturating_mul(minute_ms);
        let runs: Vec<_> = state
            .minutes
            .iter()
            .copied()
            .take_while(|run| run.end_ms() <= limit)
            .collect();
        let mut every_run = true;
        for run in runs {
            let delivered = run.start_ms() < state.loaded_until_ms
                && state
                    .run_tails
                    .get(&run)
                    .is_some_and(|tails| tails.iter().zip(watermarks).all(|(tail, watermark)| tail <= watermark));
            if !delivered {
                every_run = false;
                break;
            }
            state.minutes.remove(&run);
            state.run_tails.remove(&run);
            state.bucket_min_offsets.remove(&run);
            state.retired.insert(run);
            state.forget_opened(run);
            state.collected_until_ms = state.collected_until_ms.max(run.end_ms());
        }
        // With every run before it collected, and every day and hour bucket before it moved on,
        // nothing indexed is due before the limit.
        let moved_on = |first: Option<i64>| first.is_none_or(|start| start >= limit);
        if every_run
            && moved_on(state.hours.first().map(|hour| hour.start_ms()))
            && moved_on(state.days.first().map(|day| day.start_ms()))
        {
            state.collected_until_ms = state.collected_until_ms.max(limit);
        }

        let day_ms = NativeBucketKind::Day.window_ms();
        for day in self.storage.payload_days() {
            let day_end = i64::from(day).saturating_add(1).saturating_mul(day_ms);
            if day_end <= state.collected_until_ms {
                state.retired_payload_days.insert(day);
            }
        }
        state.cancels.expire(state.collected_until_ms);
    }
}
