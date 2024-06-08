/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{
    collections::LinkedList,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, SystemTime},
};

use parking_lot::Mutex;
use tracing::info;

use crate::common::stats::{call_snapshot::CallSnapshot, stats_snapshot::StatsSnapshot};

pub struct StatsItem {
    value: AtomicU64,
    times: AtomicU64,
    cs_list_minute: Arc<Mutex<LinkedList<CallSnapshot>>>,
    cs_list_hour: Arc<Mutex<LinkedList<CallSnapshot>>>,
    cs_list_day: Arc<Mutex<LinkedList<CallSnapshot>>>,
    stats_name: String,
    stats_key: String,
}

impl StatsItem {
    pub fn new(stats_name: &str, stats_key: &str) -> Self {
        StatsItem {
            value: AtomicU64::new(0),
            times: AtomicU64::new(0),
            cs_list_minute: Arc::new(Mutex::new(LinkedList::new())),
            cs_list_hour: Arc::new(Mutex::new(LinkedList::new())),
            cs_list_day: Arc::new(Mutex::new(LinkedList::new())),
            stats_name: stats_name.to_string(),
            stats_key: stats_key.to_string(),
        }
    }

    pub fn compute_stats_data(cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>) -> StatsSnapshot {
        let mut stats_snapshot = StatsSnapshot::new();
        let cs_list = cs_list.lock();
        if cs_list.len() > 0 {
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

    pub fn init(&self) {
        let cs_list_minute = Arc::clone(&self.cs_list_minute);
        let cs_list_minute_clone = Arc::clone(&self.cs_list_minute);
        let cs_list_hour = Arc::clone(&self.cs_list_hour);
        let cs_list_hour_clone = Arc::clone(&self.cs_list_hour);
        let cs_list_day = Arc::clone(&self.cs_list_day);
        let cs_list_day_clone = Arc::clone(&self.cs_list_day);
        let stats_name = self.stats_name.clone();
        let stats_key = self.stats_key.clone();

        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(10));
            Self::sampling_in_seconds(cs_list_minute.clone());
        });

        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(600));
            Self::sampling_in_minutes(cs_list_hour.clone());
        });

        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(3600));
            Self::sampling_in_hour(cs_list_day.clone());
        });

        let stats_name_clone = stats_name.clone();
        let stats_key_clone = stats_key.clone();
        thread::spawn(move || loop {
            let sleep_time = Self::compute_next_minutes_time_millis()
                - SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
            thread::sleep(Duration::from_millis(sleep_time));
            Self::print_at_minutes(
                &stats_name_clone,
                &stats_key_clone,
                cs_list_minute_clone.clone(),
            );
        });

        let stats_name_clone = stats_name.clone();
        let stats_key_clone = stats_key.clone();
        thread::spawn(move || loop {
            let sleep_time = Self::compute_next_hour_time_millis()
                - SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
            thread::sleep(Duration::from_millis(sleep_time));
            Self::print_at_hour(
                &stats_name_clone,
                &stats_key_clone,
                cs_list_hour_clone.clone(),
            );
        });

        thread::spawn(move || loop {
            let sleep_time = Self::compute_next_morning_time_millis()
                - SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                - 2000;
            thread::sleep(Duration::from_millis(sleep_time));
            Self::print_at_day(&stats_name, &stats_key, cs_list_day_clone.clone());
        });
    }

    pub fn sampling_in_seconds(cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>) {
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
            0,
            0,
        ));
        if cs_list.len() > 7 {
            cs_list.pop_front();
        }
    }

    pub fn sampling_in_minutes(cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>) {
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
            0,
            0,
        ));
        if cs_list.len() > 7 {
            cs_list.pop_front();
        }
    }

    pub fn sampling_in_hour(cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>) {
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
            0,
            0,
        ));
        if cs_list.len() > 25 {
            cs_list.pop_front();
        }
    }

    pub fn print_at_minutes(
        stats_name: &str,
        stats_key: &str,
        cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>,
    ) {
        let ss = Self::compute_stats_data(cs_list);
        info!(
            "[{}] [{}] Stats In One Minute, {}",
            stats_name,
            stats_key,
            Self::stat_print_detail(ss)
        );
    }

    pub fn print_at_hour(
        stats_name: &str,
        stats_key: &str,
        cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>,
    ) {
        let ss = Self::compute_stats_data(cs_list);
        info!(
            "[{}] [{}] Stats In One Hour, {}",
            stats_name,
            stats_key,
            Self::stat_print_detail(ss)
        );
    }

    pub fn print_at_day(
        stats_name: &str,
        stats_key: &str,
        cs_list: Arc<Mutex<LinkedList<CallSnapshot>>>,
    ) {
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
    use std::{collections::LinkedList, sync::Arc};

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
}
