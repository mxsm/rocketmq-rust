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

use std::collections::LinkedList;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;

use parking_lot::Mutex;
use tracing::info;

use crate::common::stats::call_snapshot::CallSnapshot;
use crate::common::stats::stats_snapshot::StatsSnapshot;

pub struct StatsItem {
    value: Arc<AtomicU64>,
    times: Arc<AtomicU64>,
    cs_list_minute: Arc<Mutex<LinkedList<CallSnapshot>>>,
    cs_list_hour: Arc<Mutex<LinkedList<CallSnapshot>>>,
    cs_list_day: Arc<Mutex<LinkedList<CallSnapshot>>>,
    stats_name: String,
    stats_key: String,
}

impl StatsItem {
    pub fn new(stats_name: &str, stats_key: &str) -> Self {
        StatsItem {
            value: Arc::new(AtomicU64::new(0)),
            times: Arc::new(AtomicU64::new(0)),
            cs_list_minute: Arc::new(Mutex::new(LinkedList::new())),
            cs_list_hour: Arc::new(Mutex::new(LinkedList::new())),
            cs_list_day: Arc::new(Mutex::new(LinkedList::new())),
            stats_name: stats_name.to_string(),
            stats_key: stats_key.to_string(),
        }
    }

    /// Atomically increments the value by delta and increments times by 1.
    ///
    /// # Arguments
    /// * `delta` - The amount to add to the value counter
    ///
    /// # Thread Safety
    /// This method is lock-free and safe to call from multiple threads concurrently.
    /// Uses `Ordering::Relaxed` as no synchronization with other variables is required.
    #[inline]
    pub fn increment(&self, delta: u64) {
        self.value.fetch_add(delta, Ordering::Relaxed);
        self.times.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the current accumulated value.
    ///
    /// # Thread Safety
    /// This method uses `Ordering::Relaxed` for optimal performance.
    /// The returned value is a snapshot at the time of the call.
    #[inline]
    pub fn get_value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Returns the current accumulated times (call count).
    ///
    /// # Thread Safety
    /// This method uses `Ordering::Relaxed` for optimal performance.
    /// The returned value is a snapshot at the time of the call.
    #[inline]
    pub fn get_times(&self) -> u64 {
        self.times.load(Ordering::Relaxed)
    }

    pub fn compute_stats_data(cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>) -> StatsSnapshot {
        let mut stats_snapshot = StatsSnapshot::new();
        let cs_list = cs_list.lock();
        if !cs_list.is_empty() {
            let first = cs_list.front().unwrap();
            let last = cs_list.back().unwrap();
            let sum = last.get_value() - first.get_value();
            let tps = (sum as f64 * 1000.0) / (last.get_timestamp() - first.get_timestamp()) as f64;
            let times_diff = last.get_times() - first.get_times();
            let avgpt = if times_diff > 0 {
                sum as f64 / times_diff as f64
            } else {
                0.0
            };
            stats_snapshot.set_sum(sum);
            stats_snapshot.set_tps(tps);
            stats_snapshot.set_avgpt(avgpt);
            stats_snapshot.set_times(times_diff);
        }
        stats_snapshot
    }

    pub fn get_stats_data_in_minute(&self) -> StatsSnapshot {
        Self::compute_stats_data(Arc::clone(&self.cs_list_minute))
    }

    pub fn get_stats_data_in_hour(&self) -> StatsSnapshot {
        Self::compute_stats_data(Arc::clone(&self.cs_list_hour))
    }

    pub fn get_stats_data_in_day(&self) -> StatsSnapshot {
        Self::compute_stats_data(Arc::clone(&self.cs_list_day))
    }

    /// Perform second-level sampling
    pub fn sampling_in_seconds(&self) {
        let current_value = self.value.load(Ordering::Relaxed);
        let current_times = self.times.load(Ordering::Relaxed);
        Self::sampling_in_seconds_internal(Arc::clone(&self.cs_list_minute), current_value, current_times);
    }

    /// Perform minute-level sampling
    pub fn sampling_in_minutes_level(&self) {
        let current_value = self.value.load(Ordering::Relaxed);
        let current_times = self.times.load(Ordering::Relaxed);
        Self::sampling_in_minutes(Arc::clone(&self.cs_list_hour), current_value, current_times);
    }

    /// Perform hour-level sampling
    pub fn sampling_in_hour_level(&self) {
        let current_value = self.value.load(Ordering::Relaxed);
        let current_times = self.times.load(Ordering::Relaxed);
        Self::sampling_in_hour(Arc::clone(&self.cs_list_day), current_value, current_times);
    }

    pub fn sampling_in_seconds_internal(
        cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>,
        current_value: u64,
        current_times: u64,
    ) {
        let mut cs_list = cs_list.lock();
        if cs_list.is_empty() {
            cs_list.push_back(CallSnapshot::new(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    - 10 * 1000,
                0,
                0,
            ));
        }
        cs_list.push_back(CallSnapshot::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            current_times,
            current_value,
        ));
        if cs_list.len() > 7 {
            cs_list.pop_front();
        }
    }

    pub fn sampling_in_minutes(cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>, current_value: u64, current_times: u64) {
        let mut cs_list = cs_list.lock();
        if cs_list.is_empty() {
            cs_list.push_back(CallSnapshot::new(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    - 10 * 60 * 1000,
                0,
                0,
            ));
        }
        cs_list.push_back(CallSnapshot::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            current_times,
            current_value,
        ));
        if cs_list.len() > 7 {
            cs_list.pop_front();
        }
    }

    pub fn sampling_in_hour(cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>, current_value: u64, current_times: u64) {
        let mut cs_list = cs_list.lock();
        if cs_list.is_empty() {
            cs_list.push_back(CallSnapshot::new(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    - 60 * 60 * 1000,
                0,
                0,
            ));
        }
        cs_list.push_back(CallSnapshot::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            current_times,
            current_value,
        ));
        if cs_list.len() > 25 {
            cs_list.pop_front();
        }
    }

    pub fn print_at_minutes(stats_name: &str, stats_key: &str, cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>) {
        let ss = Self::compute_stats_data(cs_list);
        info!(
            "[{}] [{}] Stats In One Minute, {}",
            stats_name,
            stats_key,
            Self::stat_print_detail(ss)
        );
    }

    pub fn print_at_hour(stats_name: &str, stats_key: &str, cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>) {
        let ss = Self::compute_stats_data(cs_list);
        info!(
            "[{}] [{}] Stats In One Hour, {}",
            stats_name,
            stats_key,
            Self::stat_print_detail(ss)
        );
    }

    pub fn print_at_day(stats_name: &str, stats_key: &str, cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>) {
        let ss = Self::compute_stats_data(cs_list);
        info!(
            "[{}] [{}] Stats In One Day, {}",
            stats_name,
            stats_key,
            Self::stat_print_detail(ss)
        );
    }

    pub fn stat_print_detail(ss: StatsSnapshot) -> String {
        format!(
            "SUM: {} TPS: {:.2} AVGPT: {:.2}",
            ss.get_sum(),
            ss.get_tps(),
            ss.get_avgpt()
        )
    }

    pub fn compute_next_minutes_time_millis() -> u64 {
        // Compute the next minute boundary from the current time
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        (now / 60000 + 1) * 60000
    }

    pub fn compute_next_hour_time_millis() -> u64 {
        // Compute the next hour boundary from the current time
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        (now / 3600000 + 1) * 3600000
    }

    pub fn compute_next_morning_time_millis() -> u64 {
        // Compute the next day boundary from the current time
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let current_day = now / (24 * 3600000);
        (current_day + 1) * 24 * 3600000
    }
}

impl fmt::Debug for StatsItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatsItem")
            .field("value", &self.value.load(Ordering::Relaxed))
            .field("times", &self.times.load(Ordering::Relaxed))
            .field("stats_name", &self.stats_name)
            .field("stats_key", &self.stats_key)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::LinkedList;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn stats_item_initializes_correctly() {
        let stats_item = StatsItem::new("TestName", "TestKey");
        assert_eq!(stats_item.value.load(Ordering::Relaxed), 0);
        assert_eq!(stats_item.times.load(Ordering::Relaxed), 0);
        assert_eq!(stats_item.stats_name, "TestName");
        assert_eq!(stats_item.stats_key, "TestKey");
    }

    #[test]
    fn compute_stats_data_returns_correct_snapshot() {
        let cs_list = Arc::new(Mutex::new(LinkedList::new()));
        let mut cs_list1 = cs_list.lock();
        cs_list1.push_back(CallSnapshot::new(1000, 10, 100));
        cs_list1.push_back(CallSnapshot::new(2000, 20, 200));
        drop(cs_list1);
        let snapshot = StatsItem::compute_stats_data(Arc::clone(&cs_list));
        assert_eq!(snapshot.get_sum(), 100);
        assert_eq!(snapshot.get_tps(), 100.0);
        assert_eq!(snapshot.get_times(), 10);
        assert_eq!(snapshot.get_avgpt(), 10.0);
    }

    #[test]
    fn get_stats_data_in_minute_returns_correct_snapshot() {
        let stats_item = StatsItem::new("TestName", "TestKey");
        let snapshot = stats_item.get_stats_data_in_minute();
        assert_eq!(snapshot.get_sum(), 0);
        assert_eq!(snapshot.get_tps(), 0.0);
        assert_eq!(snapshot.get_times(), 0);
        assert_eq!(snapshot.get_avgpt(), 0.0);
    }

    #[test]
    fn get_stats_data_in_hour_returns_correct_snapshot() {
        let stats_item = StatsItem::new("TestName", "TestKey");
        let snapshot = stats_item.get_stats_data_in_hour();
        assert_eq!(snapshot.get_sum(), 0);
        assert_eq!(snapshot.get_tps(), 0.0);
        assert_eq!(snapshot.get_times(), 0);
        assert_eq!(snapshot.get_avgpt(), 0.0);
    }

    #[test]
    fn get_stats_data_in_day_returns_correct_snapshot() {
        let stats_item = StatsItem::new("TestName", "TestKey");
        let snapshot = stats_item.get_stats_data_in_day();
        assert_eq!(snapshot.get_sum(), 0);
        assert_eq!(snapshot.get_tps(), 0.0);
        assert_eq!(snapshot.get_times(), 0);
        assert_eq!(snapshot.get_avgpt(), 0.0);
    }

    #[test]
    fn test_increment_updates_value_and_times() {
        let stats = StatsItem::new("test", "key");
        stats.increment(100);
        assert_eq!(stats.get_value(), 100);
        assert_eq!(stats.get_times(), 1);

        stats.increment(50);
        assert_eq!(stats.get_value(), 150);
        assert_eq!(stats.get_times(), 2);
    }

    #[test]
    fn test_multi_threaded_increment_atomicity() {
        use std::thread;
        let stats = Arc::new(StatsItem::new("test", "key"));
        let mut handles = vec![];

        // Spawn 10 threads, each incrementing 1000 times
        for _ in 0..10 {
            let stats_clone = Arc::clone(&stats);
            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    stats_clone.increment(1);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify atomicity: 10 threads * 1000 increments = 10000
        assert_eq!(stats.get_value(), 10_000);
        assert_eq!(stats.get_times(), 10_000);
    }

    #[test]
    fn test_sampling_captures_current_values() {
        let stats = StatsItem::new("test", "key");

        // Increment some values
        stats.increment(100);
        stats.increment(200);
        stats.increment(300);

        // Perform manual sampling
        stats.sampling_in_seconds();

        // Verify snapshot contains non-zero values
        let snapshot = stats.get_stats_data_in_minute();
        assert!(snapshot.get_sum() > 0, "Snapshot should capture incremented value");
        assert!(snapshot.get_times() > 0, "Snapshot should capture times");
    }

    #[test]
    fn test_tps_and_avgpt_calculation() {
        let cs_list = Arc::new(Mutex::new(LinkedList::new()));

        // Add snapshots with known values
        // t=0ms: 0 calls, value=0
        // t=1000ms: 10 calls, value=500
        {
            let mut list = cs_list.lock();
            list.push_back(CallSnapshot::new(0, 0, 0));
            list.push_back(CallSnapshot::new(1000, 10, 500));
        }

        let snapshot = StatsItem::compute_stats_data(Arc::clone(&cs_list));

        // TPS = (500 - 0) * 1000 / (1000 - 0) = 500
        assert_eq!(snapshot.get_tps(), 500.0);

        // AVGPT = (500 - 0) / (10 - 0) = 50
        assert_eq!(snapshot.get_avgpt(), 50.0);

        // Sum = 500 - 0 = 500
        assert_eq!(snapshot.get_sum(), 500);

        // Times = 10 - 0 = 10
        assert_eq!(snapshot.get_times(), 10);
    }

    #[test]
    fn test_concurrent_read_write() {
        use std::thread;
        use std::time::Duration;

        let stats = Arc::new(StatsItem::new("concurrent", "test"));
        let stats_writer = Arc::clone(&stats);
        let stats_reader = Arc::clone(&stats);

        // Writer thread: continuously increment
        let writer = thread::spawn(move || {
            for _ in 0..1000 {
                stats_writer.increment(1);
                thread::sleep(Duration::from_micros(10));
            }
        });

        // Reader thread: continuously read values
        let reader = thread::spawn(move || {
            let mut last_value = 0;
            for _ in 0..1000 {
                let current = stats_reader.get_value();
                // Value should monotonically increase or stay same
                assert!(
                    current >= last_value,
                    "Value decreased! Was {}, now {}",
                    last_value,
                    current
                );
                last_value = current;
                thread::sleep(Duration::from_micros(10));
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();

        assert_eq!(stats.get_value(), 1000);
    }

    #[test]
    fn test_zero_times_avgpt_calculation() {
        let cs_list = Arc::new(Mutex::new(LinkedList::new()));

        // Add snapshots with same times (delta = 0)
        {
            let mut list = cs_list.lock();
            list.push_back(CallSnapshot::new(0, 5, 100));
            list.push_back(CallSnapshot::new(1000, 5, 200));
        }

        let snapshot = StatsItem::compute_stats_data(Arc::clone(&cs_list));

        // When times_diff is 0, avgpt should be 0.0 (not division by zero)
        assert_eq!(snapshot.get_avgpt(), 0.0);
    }
}
