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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;

pub fn default_timer_dist() -> Vec<i32> {
    vec![5, 60, 300, 900, 3600, 14_400, 28_800, 86_400]
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimerStorageMetricsSnapshot {
    pub logical_write_bytes: u64,
    pub physical_write_bytes: u64,
    pub dirty_pages: u64,
    pub fsync_count: u64,
    pub fsync_latency_ns: u64,
    pub live_log_bytes: u64,
    pub garbage_log_bytes: u64,
    pub segment_count: u64,
    pub hot_slot_scanned_records: u64,
    pub spill_bytes: u64,
    pub recovery_replay_records: u64,
    pub wheel_repair_pages: u64,
}

/// Incremental storage counters. Reading these values never scans log or wheel files.
#[derive(Debug, Default)]
pub struct TimerStorageMetrics {
    logical_write_bytes: AtomicU64,
    physical_write_bytes: AtomicU64,
    dirty_pages: AtomicU64,
    fsync_count: AtomicU64,
    fsync_latency_ns: AtomicU64,
    live_log_bytes: AtomicU64,
    garbage_log_bytes: AtomicU64,
    segment_count: AtomicU64,
    hot_slot_scanned_records: AtomicU64,
    spill_bytes: AtomicU64,
    recovery_replay_records: AtomicU64,
    wheel_repair_pages: AtomicU64,
}

impl TimerStorageMetrics {
    pub fn record_logical_write(&self, bytes: u64) {
        self.logical_write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_physical_write(&self, bytes: u64) {
        self.physical_write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn set_dirty_pages(&self, count: u64) {
        self.dirty_pages.store(count, Ordering::Relaxed);
    }

    pub fn record_fsync(&self, latency_ns: u64) {
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        self.fsync_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
    }

    pub fn set_log_bytes(&self, live: u64, garbage: u64) {
        self.live_log_bytes.store(live, Ordering::Relaxed);
        self.garbage_log_bytes.store(garbage, Ordering::Relaxed);
    }

    pub fn set_segment_count(&self, count: u64) {
        self.segment_count.store(count, Ordering::Relaxed);
    }

    pub fn record_hot_slot_scan(&self, records: u64) {
        self.hot_slot_scanned_records.fetch_add(records, Ordering::Relaxed);
    }

    pub fn record_spill_bytes(&self, bytes: u64) {
        self.spill_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_recovery_replay(&self, records: u64) {
        self.recovery_replay_records.fetch_add(records, Ordering::Relaxed);
    }

    pub fn record_wheel_repair(&self) {
        self.wheel_repair_pages.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TimerStorageMetricsSnapshot {
        TimerStorageMetricsSnapshot {
            logical_write_bytes: self.logical_write_bytes.load(Ordering::Relaxed),
            physical_write_bytes: self.physical_write_bytes.load(Ordering::Relaxed),
            dirty_pages: self.dirty_pages.load(Ordering::Relaxed),
            fsync_count: self.fsync_count.load(Ordering::Relaxed),
            fsync_latency_ns: self.fsync_latency_ns.load(Ordering::Relaxed),
            live_log_bytes: self.live_log_bytes.load(Ordering::Relaxed),
            garbage_log_bytes: self.garbage_log_bytes.load(Ordering::Relaxed),
            segment_count: self.segment_count.load(Ordering::Relaxed),
            hot_slot_scanned_records: self.hot_slot_scanned_records.load(Ordering::Relaxed),
            spill_bytes: self.spill_bytes.load(Ordering::Relaxed),
            recovery_replay_records: self.recovery_replay_records.load(Ordering::Relaxed),
            wheel_repair_pages: self.wheel_repair_pages.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimerMetric {
    pub count: i64,
    pub time_stamp: i64,
}

#[derive(Debug)]
pub struct TimerMetricsState {
    timing_count: RwLock<HashMap<String, TimerMetric>>,
    timing_distribution: RwLock<HashMap<i32, TimerMetric>>,
    timer_dist: RwLock<Vec<i32>>,
}

impl Default for TimerMetricsState {
    fn default() -> Self {
        Self {
            timing_count: RwLock::new(HashMap::new()),
            timing_distribution: RwLock::new(HashMap::new()),
            timer_dist: RwLock::new(default_timer_dist()),
        }
    }
}

impl TimerMetricsState {
    pub fn get_timing_count(&self, key: &str) -> i64 {
        self.timing_count
            .read()
            .get(key)
            .map(|metric| metric.count)
            .unwrap_or_default()
    }

    pub fn timing_count_snapshot(&self) -> HashMap<String, i64> {
        positive_string_counts(&self.timing_count.read())
    }

    pub fn replace_timing_count_snapshot(&self, snapshot: HashMap<String, i64>, now_ms: i64) {
        let mut counts = self.timing_count.write();
        counts.clear();
        counts.extend(snapshot.into_iter().filter_map(|(key, count)| {
            (count > 0).then_some((
                key,
                TimerMetric {
                    count,
                    time_stamp: now_ms,
                },
            ))
        }));
    }

    pub fn add_timing_count(&self, key: &str, delta: i64, now_ms: i64) {
        let mut counts = self.timing_count.write();
        let metric = counts.entry(key.to_owned()).or_insert(TimerMetric {
            count: 0,
            time_stamp: 0,
        });
        metric.count = metric.count.saturating_add(delta).max(0);
        metric.time_stamp = now_ms;
    }

    pub fn timing_distribution_snapshot(&self) -> HashMap<i32, i64> {
        positive_numeric_counts(&self.timing_distribution.read())
    }

    pub fn replace_timing_distribution_snapshot(&self, snapshot: HashMap<i32, i64>, now_ms: i64) {
        let mut counts = self.timing_distribution.write();
        counts.clear();
        counts.extend(snapshot.into_iter().map(|(period, count)| {
            (
                period,
                TimerMetric {
                    count: count.max(0),
                    time_stamp: now_ms,
                },
            )
        }));
    }

    pub fn timer_dist(&self) -> Vec<i32> {
        self.timer_dist.read().clone()
    }

    pub fn set_timer_dist(&self, timer_dist: Vec<i32>) {
        *self.timer_dist.write() = timer_dist;
    }

    pub fn export(&self) -> (HashMap<String, TimerMetric>, HashMap<i32, TimerMetric>, Vec<i32>) {
        (
            self.timing_count.read().clone(),
            self.timing_distribution.read().clone(),
            self.timer_dist(),
        )
    }

    pub fn apply(
        &self,
        timing_count: HashMap<String, TimerMetric>,
        timing_distribution: HashMap<i32, TimerMetric>,
        timer_dist: Vec<i32>,
    ) {
        *self.timing_count.write() = timing_count;
        *self.timing_distribution.write() = timing_distribution;
        *self.timer_dist.write() = timer_dist;
    }
}

fn positive_string_counts(metrics: &HashMap<String, TimerMetric>) -> HashMap<String, i64> {
    metrics
        .iter()
        .filter_map(|(key, metric)| (metric.count > 0).then_some((key.clone(), metric.count)))
        .collect()
}

fn positive_numeric_counts(metrics: &HashMap<i32, TimerMetric>) -> HashMap<i32, i64> {
    metrics
        .iter()
        .filter_map(|(key, metric)| (metric.count > 0).then_some((*key, metric.count)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_metrics_clamp_counts_and_keep_positive_snapshots() {
        let state = TimerMetricsState::default();
        state.add_timing_count("topic", 4, 10);
        state.add_timing_count("topic", -10, 20);
        assert_eq!(state.get_timing_count("topic"), 0);
        assert!(state.timing_count_snapshot().is_empty());
        state.replace_timing_count_snapshot(HashMap::from([("topic".into(), 3)]), 30);
        assert_eq!(state.timing_count_snapshot().get("topic"), Some(&3));
    }
}
