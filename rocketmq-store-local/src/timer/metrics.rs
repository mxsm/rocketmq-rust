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

use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;

pub fn default_timer_dist() -> Vec<i32> {
    vec![5, 60, 300, 900, 3600, 14_400, 28_800, 86_400]
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
