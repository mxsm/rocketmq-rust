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

use std::cmp;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::task;
use tokio::time::interval;

pub struct StatisticsBrief {
    top_percentile_meta: Vec<Vec<i64>>,
    counts: Vec<Arc<AtomicI32>>,
    total_count: AtomicU64,
    max: AtomicI64,
    min: AtomicI64,
    total: AtomicI64,
}

impl StatisticsBrief {
    pub const META_RANGE_INDEX: usize = 0;
    pub const META_SLOT_NUM_INDEX: usize = 1;

    pub fn new(top_percentile_meta: Vec<Vec<i64>>) -> Self {
        let counts = {
            let data = Arc::new(AtomicI32::new(0));
            vec![data; Self::slot_num(&top_percentile_meta)]
        };
        let brief = StatisticsBrief {
            top_percentile_meta,
            counts,
            total_count: AtomicU64::new(0),
            max: AtomicI64::new(0),
            min: AtomicI64::new(0),
            total: AtomicI64::new(0),
        };
        brief.reset();
        brief
    }

    pub fn reset(&self) {
        for count in &self.counts {
            count.store(0, Ordering::SeqCst);
        }
        self.total_count.store(0, Ordering::SeqCst);
        self.max.store(0, Ordering::SeqCst);
        self.min.store(i64::MAX, Ordering::SeqCst);
        self.total.store(0, Ordering::SeqCst);
    }

    fn slot_num(meta: &Vec<Vec<i64>>) -> usize {
        let mut ret = 1;
        for line in meta {
            ret += line[Self::META_SLOT_NUM_INDEX] as usize;
        }
        ret
    }

    pub fn sample(&self, value: i64) {
        let index = self.get_slot_index(value);
        self.counts[index as usize].fetch_add(1, Ordering::SeqCst);
        self.total_count.fetch_add(1, Ordering::SeqCst);

        let mut max = self.max.load(Ordering::SeqCst);
        let mut min = self.min.load(Ordering::SeqCst);

        while value > max {
            let _ = self
                .max
                .compare_exchange(max, value, Ordering::SeqCst, Ordering::SeqCst);
            max = self.max.load(Ordering::SeqCst);
        }

        while value < min {
            let _ = self
                .min
                .compare_exchange(min, value, Ordering::SeqCst, Ordering::SeqCst);
            min = self.min.load(Ordering::SeqCst);
        }

        self.total.fetch_add(value, Ordering::SeqCst);
    }

    pub fn tp999(&self) -> i64 {
        self.get_tp_value(0.999)
    }

    pub fn get_tp_value(&self, ratio: f64) -> i64 {
        if ratio <= 0.0 || ratio >= 1.0 {
            return self.get_max();
        }
        let count = self.total_count.load(Ordering::Relaxed) as f64;
        let excludes = (count - count * ratio) as u64;
        if excludes == 0 {
            return self.get_max();
        }

        let mut tmp = 0;
        for (i, count) in self.counts.iter().enumerate().rev() {
            tmp += count.load(Ordering::Relaxed);
            if tmp as u64 > excludes {
                return std::cmp::min(self.get_slot_tp_value(i as i64), self.get_max());
            }
        }
        0
    }

    fn get_slot_tp_value(&self, index: i64) -> i64 {
        let mut slot_num_left = index;
        for (i, meta) in self.top_percentile_meta.iter().enumerate() {
            let slot_num = meta[Self::META_SLOT_NUM_INDEX];
            if slot_num_left < slot_num {
                let meta_range_max = meta[Self::META_RANGE_INDEX];
                let meta_range_min = if i > 0 {
                    self.top_percentile_meta[i - 1][Self::META_RANGE_INDEX]
                } else {
                    0
                };

                return meta_range_min + (meta_range_max - meta_range_min) / slot_num * (slot_num_left + 1);
            } else {
                slot_num_left -= slot_num;
            }
        }
        i64::MAX
    }

    fn get_slot_index(&self, num: i64) -> i64 {
        let mut index = 0;
        for (i, meta) in self.top_percentile_meta.iter().enumerate() {
            let range_max = meta[Self::META_RANGE_INDEX];
            let slot_num = meta[Self::META_SLOT_NUM_INDEX];
            let range_min = if i > 0 {
                self.top_percentile_meta[i - 1][Self::META_RANGE_INDEX]
            } else {
                0
            };
            if range_min <= num && num < range_max {
                index += (num - range_min) / ((range_max - range_min) / slot_num);
                break;
            }
            index += slot_num;
        }
        index
    }

    pub fn get_max(&self) -> i64 {
        self.max.load(Ordering::Relaxed)
    }

    pub fn get_min(&self) -> i64 {
        if self.total_count.load(Ordering::Relaxed) > 0 {
            self.min.load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn get_total(&self) -> i64 {
        self.total.load(Ordering::Relaxed)
    }

    pub fn get_cnt(&self) -> u64 {
        self.total_count.load(Ordering::Relaxed)
    }

    pub fn get_avg(&self) -> f64 {
        let total_count = self.total_count.load(Ordering::Relaxed);
        if total_count != 0 {
            self.total.load(Ordering::Relaxed) as f64 / total_count as f64
        } else {
            0.0
        }
    }

    fn update_max(&self, value: i64) {
        let mut max = self.max.load(Ordering::Relaxed);
        while value > max
            && self
                .max
                .compare_exchange(max, value, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            max = self.max.load(Ordering::Relaxed);
        }
    }

    fn update_min(&self, value: i64) {
        let mut min = self.min.load(Ordering::Relaxed);
        while value < min
            && self
                .min
                .compare_exchange(min, value, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            min = self.min.load(Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_brief_initializes_correctly() {
        let brief = StatisticsBrief::new(vec![vec![10, 2], vec![20, 3]]);
        assert_eq!(brief.get_max(), 0);
        assert_eq!(brief.get_min(), 0);
        assert_eq!(brief.get_total(), 0);
        assert_eq!(brief.get_cnt(), 0);
        assert_eq!(brief.get_avg(), 0.0);
    }

    #[test]
    fn statistics_brief_sample_updates_values_correctly() {
        let brief = StatisticsBrief::new(vec![vec![10, 2], vec![20, 3]]);
        brief.sample(5);
        assert_eq!(brief.get_max(), 5);
        assert_eq!(brief.get_min(), 5);
        assert_eq!(brief.get_total(), 5);
        assert_eq!(brief.get_cnt(), 1);
        assert_eq!(brief.get_avg(), 5.0);
    }

    #[test]
    fn statistics_brief_sample_updates_values_with_multiple_samples() {
        let brief = StatisticsBrief::new(vec![vec![10, 2], vec![20, 3]]);
        brief.sample(5);
        brief.sample(10);
        assert_eq!(brief.get_max(), 10);
        assert_eq!(brief.get_min(), 5);
        assert_eq!(brief.get_total(), 15);
        assert_eq!(brief.get_cnt(), 2);
        assert_eq!(brief.get_avg(), 7.5);
    }

    #[test]
    fn statistics_brief_tp999_returns_correct_value() {
        let brief = StatisticsBrief::new(vec![vec![10, 2], vec![20, 3]]);
        for _ in 0..1000 {
            brief.sample(5);
        }
        assert_eq!(brief.tp999(), 5);
    }

    #[test]
    fn statistics_brief_get_tp_value_returns_correct_value() {
        let brief = StatisticsBrief::new(vec![vec![10, 2], vec![20, 3]]);
        for _ in 0..1000 {
            brief.sample(5);
        }
        assert_eq!(brief.get_tp_value(0.5), 5);
    }
}
