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

use std::collections::HashMap;
use std::collections::VecDeque;

use parking_lot::Mutex;

use crate::timer::slot::Slot;

pub const TIMER_LOG_RECORD_SIZE: usize = 40;
pub const TPS_WINDOW_MS: i64 = 3_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerLogRecord {
    pub deliver_time_ms: i64,
    pub commit_log_offset: i64,
    pub size: i32,
    pub queue_offset: i64,
    pub prev_pos: i64,
    pub magic: i32,
}

impl TimerLogRecord {
    pub const SIZE: usize = TIMER_LOG_RECORD_SIZE;

    pub fn encode(self) -> [u8; Self::SIZE] {
        let mut buffer = [0u8; Self::SIZE];
        buffer[0..8].copy_from_slice(&self.deliver_time_ms.to_be_bytes());
        buffer[8..16].copy_from_slice(&self.commit_log_offset.to_be_bytes());
        buffer[16..20].copy_from_slice(&self.size.to_be_bytes());
        buffer[20..28].copy_from_slice(&self.queue_offset.to_be_bytes());
        buffer[28..36].copy_from_slice(&self.prev_pos.to_be_bytes());
        buffer[36..40].copy_from_slice(&self.magic.to_be_bytes());
        buffer
    }

    pub fn decode(buffer: &[u8]) -> std::io::Result<Self> {
        if buffer.len() != Self::SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid timer log record length {}", buffer.len()),
            ));
        }
        Ok(Self {
            deliver_time_ms: read_i64(buffer, 0),
            commit_log_offset: read_i64(buffer, 8),
            size: read_i32(buffer, 16),
            queue_offset: read_i64(buffer, 20),
            prev_pos: read_i64(buffer, 28),
            magic: read_i32(buffer, 36),
        })
    }
}

#[derive(Debug)]
pub struct TimerSchedulePolicy {
    precision_ms: i64,
    slots_total: i64,
    blank_slots: i64,
    configured_roll_slots: usize,
}

impl TimerSchedulePolicy {
    pub fn new(precision_ms: u64, slots_total: usize, blank_slots: usize, configured_roll_slots: usize) -> Self {
        Self {
            precision_ms: precision_ms.max(1) as i64,
            slots_total: slots_total as i64,
            blank_slots: blank_slots as i64,
            configured_roll_slots,
        }
    }

    pub fn floor_time_ms(&self, time_ms: i64) -> i64 {
        time_ms.div_euclid(self.precision_ms) * self.precision_ms
    }

    pub const fn precision_ms(&self) -> i64 {
        self.precision_ms
    }

    pub fn ceil_time_ms(&self, time_ms: i64) -> i64 {
        if time_ms.rem_euclid(self.precision_ms) == 0 {
            time_ms
        } else {
            (time_ms.div_euclid(self.precision_ms) + 1) * self.precision_ms
        }
    }

    pub fn wheel_window_ms(&self) -> i64 {
        self.precision_ms.saturating_mul(self.slots_total)
    }

    pub fn roll_window_ms(&self) -> i64 {
        self.roll_window_slots().saturating_mul(self.precision_ms)
    }

    pub fn recover_read_time_ms(&self, checkpoint_read_time_ms: i64, now_ms: i64) -> i64 {
        let now_floor = self.floor_time_ms(now_ms);
        let ttl_floor = now_floor.saturating_sub(self.wheel_window_ms());
        let base = if checkpoint_read_time_ms <= 0 {
            now_floor
        } else {
            self.floor_time_ms(checkpoint_read_time_ms)
        };
        base.max(ttl_floor)
    }

    pub fn plan_slot(
        &self,
        deliver_time_ms: i64,
        reference_time_ms: i64,
        lower_bound_ms: i64,
        base_magic: i32,
        roll_magic: i32,
    ) -> (i64, i32) {
        let roll_slots = self.roll_window_slots();
        let roll_window_ms = roll_slots.saturating_mul(self.precision_ms);
        let mut target_time_ms = deliver_time_ms;
        let mut magic = base_magic;
        if deliver_time_ms.saturating_sub(reference_time_ms) >= roll_window_ms {
            magic |= roll_magic;
            let overflow_ms = deliver_time_ms
                .saturating_sub(reference_time_ms)
                .saturating_sub(roll_window_ms);
            let boundary_slots = if overflow_ms < (roll_slots / 3).saturating_mul(self.precision_ms) {
                roll_slots / 2
            } else {
                roll_slots
            };
            target_time_ms = reference_time_ms.saturating_add(boundary_slots.saturating_mul(self.precision_ms));
        }
        (self.ceil_time_ms(target_time_ms).max(lower_bound_ms), magic)
    }

    fn roll_window_slots(&self) -> i64 {
        let max_slots = self.slots_total.saturating_sub(self.blank_slots).max(0);
        if self.configured_roll_slots < 2 || self.configured_roll_slots as i64 > max_slots {
            max_slots
        } else {
            self.configured_roll_slots as i64
        }
    }
}

pub fn recover_timer_log_len(current_len: i64, checkpoint_len: i64) -> i64 {
    let checkpoint_len = checkpoint_len.clamp(0, current_len);
    let checkpoint_aligned = checkpoint_len - checkpoint_len.rem_euclid(TIMER_LOG_RECORD_SIZE as i64);
    let current_aligned = current_len - current_len.rem_euclid(TIMER_LOG_RECORD_SIZE as i64);
    checkpoint_aligned.min(current_aligned)
}

pub fn clamp_queue_offset(checkpoint_offset: i64, min_offset: Option<i64>, max_offset: Option<i64>) -> i64 {
    match (min_offset, max_offset) {
        (Some(min), Some(max)) => checkpoint_offset.clamp(min, max),
        _ => checkpoint_offset.max(0),
    }
}

pub fn timer_slot_is_valid(slot: Slot, recovered_log_len: i64) -> bool {
    slot.num > 0
        && slot.first_pos >= 0
        && slot.last_pos >= 0
        && slot.first_pos < recovered_log_len
        && slot.last_pos < recovered_log_len
        && slot.first_pos <= slot.last_pos
        && slot.first_pos.rem_euclid(TIMER_LOG_RECORD_SIZE as i64) == 0
        && slot.last_pos.rem_euclid(TIMER_LOG_RECORD_SIZE as i64) == 0
}

#[derive(Debug)]
pub struct TimerBacklogMetrics {
    topic_backlog: HashMap<String, i64>,
    timer_backlog_distribution: HashMap<i32, i64>,
    timer_dist: Vec<i32>,
}

impl TimerBacklogMetrics {
    pub fn new(timer_dist: Vec<i32>) -> Self {
        Self {
            topic_backlog: HashMap::new(),
            timer_backlog_distribution: HashMap::new(),
            timer_dist,
        }
    }

    pub fn observe(&mut self, topic: String, deliver_time_ms: i64, now_ms: i64, is_delete: bool) {
        let remaining_ms = deliver_time_ms.saturating_sub(now_ms).max(0);
        let remaining_secs = ((remaining_ms + 999) / 1000) as i32;
        if let Some(period) = self.timer_dist.iter().copied().find(|period| remaining_secs <= *period) {
            *self.timer_backlog_distribution.entry(period).or_default() += 1;
        }
        *self.topic_backlog.entry(topic).or_default() += if is_delete { -1 } else { 1 };
    }

    pub fn topic_snapshot(&self) -> HashMap<String, i64> {
        self.topic_backlog
            .iter()
            .filter_map(|(topic, count)| (*count > 0).then_some((topic.clone(), *count)))
            .collect()
    }

    pub fn distribution_snapshot(&self) -> HashMap<i32, i64> {
        self.timer_backlog_distribution
            .iter()
            .filter_map(|(period, count)| (*count > 0).then_some((*period, *count)))
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct TimerTpsCounter {
    state: Mutex<TimerTpsState>,
}

#[derive(Debug, Default)]
struct TimerTpsState {
    buckets: VecDeque<(i64, usize)>,
    total: usize,
}

impl TimerTpsCounter {
    pub fn record(&self, delta: usize, now_ms: i64) {
        if delta == 0 {
            return;
        }
        let mut state = self.state.lock();
        evict_expired(&mut state, now_ms);
        if let Some((bucket_ms, count)) = state.buckets.back_mut() {
            if *bucket_ms == now_ms {
                *count = count.saturating_add(delta);
                state.total = state.total.saturating_add(delta);
                return;
            }
        }
        state.buckets.push_back((now_ms, delta));
        state.total = state.total.saturating_add(delta);
    }

    pub fn get_tps(&self, now_ms: i64) -> f32 {
        let mut state = self.state.lock();
        evict_expired(&mut state, now_ms);
        state.total as f32 * 1000.0 / TPS_WINDOW_MS as f32
    }
}

fn evict_expired(state: &mut TimerTpsState, now_ms: i64) {
    while let Some((bucket_ms, count)) = state.buckets.front().copied() {
        if now_ms - bucket_ms < TPS_WINDOW_MS {
            break;
        }
        state.total = state.total.saturating_sub(count);
        state.buckets.pop_front();
    }
}

fn read_i64(buffer: &[u8], start: usize) -> i64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&buffer[start..start + 8]);
    i64::from_be_bytes(bytes)
}

fn read_i32(buffer: &[u8], start: usize) -> i32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&buffer[start..start + 4]);
    i32::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_policy_preserves_roll_boundaries_and_alignment() {
        let policy = TimerSchedulePolicy::new(1_000, 100, 10, 90);
        assert_eq!(policy.plan_slot(95_000, 0, 0, 1, 2), (45_000, 3));
        assert_eq!(policy.plan_slot(5_001, 0, 0, 1, 2), (6_000, 1));
    }

    #[test]
    fn timer_log_recovery_clamps_and_aligns_both_watermarks() {
        assert_eq!(recover_timer_log_len(99, 83), 80);
        assert_eq!(recover_timer_log_len(39, 99), 0);
    }

    #[test]
    fn timer_tps_counter_uses_bounded_three_second_window() {
        let counter = TimerTpsCounter::default();
        counter.record(3, 1_000);
        assert_eq!(counter.get_tps(1_500), 1.0);
        assert_eq!(counter.get_tps(4_000), 0.0);
    }
}
