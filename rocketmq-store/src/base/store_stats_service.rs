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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::fmt;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Instant;

use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_common::common::broker::broker_config::BrokerIdentity;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_common::TimeUtils::get_current_millis;
use tracing::error;

const FREQUENCY_OF_SAMPLING: u64 = 1000;
const MAX_RECORDS_OF_SAMPLING: usize = 60 * 10;
const PUT_MESSAGE_ENTIRE_TIME_MAX_DESC: [&str; 13] = [
    "[<=0ms]",
    "[0~10ms]",
    "[10~50ms]",
    "[50~100ms]",
    "[100~200ms]",
    "[200~500ms]",
    "[500ms~1s]",
    "[1~2s]",
    "[2~3s]",
    "[3~4s]",
    "[4~5s]",
    "[5~10s]",
    "[10s~]",
];

static PUT_MESSAGE_ENTIRE_TIME_BUCKETS: LazyLock<BTreeMap<i32, i32>> = LazyLock::new(|| {
    let mut m = BTreeMap::new();
    m.insert(1, 20);
    m.insert(2, 15);
    m.insert(5, 10);
    m.insert(10, 10);
    m.insert(50, 6);
    m.insert(100, 5);
    m.insert(1000, 9);
    m
});

type AtomicUsizeArray = Arc<Vec<AtomicUsize>>;

pub struct StoreStatsService {
    buckets: BTreeMap<u64, AtomicUsize>,
    last_buckets: BTreeMap<u64, AtomicUsize>,
    put_message_failed_times: AtomicUsize,
    put_message_topic_times_total: Arc<RwLock<HashMap<String, AtomicUsize>>>,
    put_message_topic_size_total: Arc<RwLock<HashMap<String, AtomicUsize>>>,
    get_message_times_total_found: AtomicUsize,
    get_message_transferred_msg_count: AtomicUsize,
    get_message_times_total_miss: AtomicUsize,
    put_times_list: Mutex<LinkedList<CallSnapshot>>,
    get_times_found_list: Mutex<LinkedList<CallSnapshot>>,
    get_times_miss_list: Mutex<LinkedList<CallSnapshot>>,
    transferred_msg_count_list: Mutex<LinkedList<CallSnapshot>>,
    put_message_distribute_time: AtomicUsizeArray,
    last_put_message_distribute_time: AtomicUsizeArray,
    message_store_boot_timestamp: u64,
    put_message_entire_time_max: Arc<AtomicUsize>,
    get_message_entire_time_max: Arc<AtomicUsize>,
    dispatch_max_buffer: Arc<AtomicUsize>,
    sampling_lock: Mutex<()>,
    last_print_timestamp: u64,
    broker_identity: Option<BrokerIdentity>,
}

impl StoreStatsService {
    #[inline]
    pub fn new(broker_identity: Option<BrokerIdentity>) -> Self {
        Self {
            buckets: BTreeMap::new(),
            last_buckets: BTreeMap::new(),
            put_message_failed_times: AtomicUsize::new(0),
            put_message_topic_times_total: Arc::new(RwLock::new(HashMap::new())),
            put_message_topic_size_total: Arc::new(RwLock::new(HashMap::new())),
            get_message_times_total_found: AtomicUsize::new(0),
            get_message_transferred_msg_count: AtomicUsize::new(0),
            get_message_times_total_miss: AtomicUsize::new(0),
            put_times_list: Mutex::new(LinkedList::new()),
            get_times_found_list: Mutex::new(LinkedList::new()),
            get_times_miss_list: Mutex::new(LinkedList::new()),
            transferred_msg_count_list: Mutex::new(LinkedList::new()),
            put_message_distribute_time: Arc::new((0..13).map(|_| AtomicUsize::new(0)).collect()),
            last_put_message_distribute_time: Arc::new((0..13).map(|_| AtomicUsize::new(0)).collect()),
            message_store_boot_timestamp: get_current_millis(),
            put_message_entire_time_max: Arc::new(AtomicUsize::new(0)),
            get_message_entire_time_max: Arc::new(AtomicUsize::new(0)),
            dispatch_max_buffer: Arc::new(AtomicUsize::new(0)),
            sampling_lock: Mutex::new(()),
            last_print_timestamp: get_current_millis(),
            broker_identity,
        }
    }
}

impl StoreStatsService {
    pub fn start(&self) {
        error!("StoreStatsService start not implemented");
    }

    pub fn shutdown(&self) {
        error!("StoreStatsService shutdown not implemented");
    }

    #[inline]
    pub fn get_message_times_total_found(&self) -> &AtomicUsize {
        &self.get_message_times_total_found
    }

    #[inline]
    pub fn get_message_times_total_miss(&self) -> &AtomicUsize {
        &self.get_message_times_total_miss
    }

    #[inline]
    pub fn get_message_transferred_msg_count(&self) -> &AtomicUsize {
        &self.get_message_transferred_msg_count
    }

    #[inline]
    pub fn get_put_message_failed_times(&self) -> &AtomicUsize {
        &self.put_message_failed_times
    }

    #[inline]
    fn reset_put_message_time_buckets(&mut self) {
        let mut next_buckets: BTreeMap<u64, AtomicUsize> = BTreeMap::new();
        let index = AtomicUsize::new(0);
        for (&interval, &times) in PUT_MESSAGE_ENTIRE_TIME_BUCKETS.iter() {
            for _ in 0..times {
                next_buckets.insert(
                    index.fetch_add(interval as usize, Ordering::SeqCst) as u64,
                    AtomicUsize::new(0),
                );
            }
        }
        next_buckets.insert(usize::MAX as u64, AtomicUsize::new(0));

        self.last_buckets = self
            .buckets
            .iter()
            .map(|(&key, value)| (key, AtomicUsize::new(value.load(Ordering::SeqCst))))
            .collect();
        self.buckets = next_buckets;
    }

    #[inline]
    fn reset_put_message_distribute_time(&self) {
        for i in 0..13 {
            self.put_message_distribute_time[i].store(0, Ordering::SeqCst);
            self.last_put_message_distribute_time[i].store(0, Ordering::SeqCst);
        }
    }

    #[inline]
    pub fn set_put_message_entire_time_max(&self, _value: u64) {}

    // Add more methods as needed for functionality

    #[inline]
    pub fn get_runtime_info(&self) -> HashMap<String, String> {
        let mut result = HashMap::new();
        let total_times = self.get_put_message_times_total();
        let total_times = if total_times == 0 { 1 } else { total_times };
        result.insert(
            "bootTimestamp".to_string(),
            format!("{:?}", self.message_store_boot_timestamp),
        );
        result.insert("runtime".to_string(), self.get_format_runtime());
        result.insert(
            "putMessageEntireTimeMax".to_string(),
            self.put_message_entire_time_max.load(Ordering::Relaxed).to_string(),
        );
        result.insert("putMessageTimesTotal".to_string(), total_times.to_string());
        result.insert(
            "putMessageFailedTimes".to_string(),
            self.put_message_failed_times.load(Ordering::Relaxed).to_string(),
        );
        result.insert(
            "putMessageSizeTotal".to_string(),
            self.get_put_message_size_total().to_string(),
        );
        result.insert(
            "putMessageDistributeTime".to_string(),
            self.get_put_message_distribute_time_string_info(total_times),
        );
        result.insert(
            "putMessageAverageSize".to_string(),
            (self.get_put_message_size_total() / total_times).to_string(),
        );
        result.insert(
            "dispatchMaxBuffer".to_string(),
            self.dispatch_max_buffer.load(Ordering::Relaxed).to_string(),
        );
        result.insert(
            "getMessageEntireTimeMax".to_string(),
            self.get_message_entire_time_max.load(Ordering::Relaxed).to_string(),
        );
        result.insert("putTps".to_string(), self.get_put_tps());
        result.insert("getFoundTps".to_string(), self.get_get_found_tps());
        result.insert("getMissTps".to_string(), self.get_get_miss_tps());
        result.insert("getTotalTps".to_string(), self.get_get_total_tps());
        result.insert("getTransferredTps".to_string(), self.get_get_transferred_tps());
        result.insert(
            "putLatency99".to_string(),
            format!("{:.2}", self.find_put_message_entire_time_px(0.99)),
        );
        result.insert(
            "putLatency999".to_string(),
            format!("{:.2}", self.find_put_message_entire_time_px(0.999)),
        );
        result
    }

    #[inline]
    pub fn find_put_message_entire_time_px(&self, px: f64) -> f64 {
        let last_buckets = &self.last_buckets;
        let start = Instant::now();
        let mut result = 0.0;
        let total_request: u64 = last_buckets.values().map(|v| v.load(Ordering::SeqCst) as u64).sum();
        let px_index = (total_request as f64 * px).round() as u64;
        let mut pass_count = 0;
        let bucket_values: Vec<_> = last_buckets.keys().collect();
        for i in 0..bucket_values.len() {
            let count = last_buckets.get(bucket_values[i]).unwrap().load(Ordering::SeqCst) as u64;
            if px_index <= pass_count + count {
                let relative_index = px_index - pass_count;
                if i == 0 {
                    result = if count == 0 {
                        0.0
                    } else {
                        *bucket_values[i] as f64 * relative_index as f64 / count as f64
                    };
                } else {
                    let last_bucket = *bucket_values[i - 1] as f64;
                    result = last_bucket
                        + if count == 0 {
                            0.0
                        } else {
                            (*bucket_values[i] as f64 - last_bucket) * relative_index as f64 / count as f64
                        };
                }
                break;
            } else {
                pass_count += count;
            }
        }
        result
    }

    #[inline]
    pub fn get_get_transferred_tps(&self) -> String {
        format!(
            "{} {} {}",
            self.get_get_transferred_tps_time(10),
            self.get_get_transferred_tps_time(60),
            self.get_get_transferred_tps_time(600)
        )
    }

    #[inline]
    pub fn get_get_transferred_tps_time(&self, time: usize) -> String {
        let mut result = String::new();
        let _guard = self.sampling_lock.lock();
        let transferred_msg_count_list = self.transferred_msg_count_list.lock();
        let transferred_msg_count_vec: Vec<_> = transferred_msg_count_list.iter().collect();
        if let Some(last) = transferred_msg_count_vec.last() {
            if transferred_msg_count_vec.len() > time {
                if let Some(last_before) = transferred_msg_count_vec.get(transferred_msg_count_vec.len() - (time + 1)) {
                    result = format!("{}", CallSnapshot::get_tps(last_before, last));
                }
            }
        }
        drop(transferred_msg_count_list);
        drop(_guard);
        result
    }

    #[inline]
    pub fn get_get_total_tps(&self) -> String {
        format!(
            "{} {} {}",
            self.get_get_total_tps_time(10),
            self.get_get_total_tps_time(60),
            self.get_get_total_tps_time(600)
        )
    }

    #[inline]
    pub fn get_get_total_tps_time(&self, time: usize) -> String {
        let _guard = self.sampling_lock.lock();
        let mut found = 0.0;
        let mut miss = 0.0;

        {
            let get_times_found_list = self.get_times_found_list.lock();
            let get_times_found_vec: Vec<_> = get_times_found_list.iter().collect();
            if let Some(last) = get_times_found_vec.last() {
                if get_times_found_vec.len() > time {
                    if let Some(last_before) = get_times_found_vec.get(get_times_found_vec.len() - (time + 1)) {
                        found = CallSnapshot::get_tps(last_before, last);
                    }
                }
            }
        }

        {
            let get_times_miss_list = self.get_times_miss_list.lock();
            let get_times_miss_vec: Vec<_> = get_times_miss_list.iter().collect();
            if let Some(last) = get_times_miss_vec.last() {
                if get_times_miss_vec.len() > time {
                    if let Some(last_before) = get_times_miss_vec.get(get_times_miss_vec.len() - (time + 1)) {
                        miss = CallSnapshot::get_tps(last_before, last);
                    }
                }
            }
        }

        drop(_guard);
        format!("{}", found + miss)
    }

    #[inline]
    pub fn get_get_miss_tps(&self) -> String {
        format!(
            "{} {} {}",
            self.get_get_miss_tps_time(10),
            self.get_get_miss_tps_time(60),
            self.get_get_miss_tps_time(600)
        )
    }

    #[inline]
    pub fn get_get_miss_tps_time(&self, time: usize) -> String {
        let mut result = String::new();
        let _guard = self.sampling_lock.lock();
        let get_times_miss_list = self.get_times_miss_list.lock();
        let get_times_miss_vec: Vec<_> = get_times_miss_list.iter().collect();
        if let Some(last) = get_times_miss_vec.last() {
            if get_times_miss_vec.len() > time {
                if let Some(last_before) = get_times_miss_vec.get(get_times_miss_vec.len() - (time + 1)) {
                    result = format!("{}", CallSnapshot::get_tps(last_before, last));
                }
            }
        }
        drop(get_times_miss_list);
        drop(_guard);
        result
    }

    #[inline]
    pub fn get_get_found_tps(&self) -> String {
        format!(
            "{} {} {}",
            self.get_get_found_tps_time(10),
            self.get_get_found_tps_time(60),
            self.get_get_found_tps_time(600)
        )
    }

    #[inline]
    pub fn get_get_found_tps_time(&self, time: usize) -> String {
        let mut result = String::new();
        let _guard = self.sampling_lock.lock();
        let get_times_found_list = self.get_times_found_list.lock();
        let get_times_found_vec: Vec<_> = get_times_found_list.iter().collect();
        if let Some(last) = get_times_found_vec.last() {
            if get_times_found_vec.len() > time {
                if let Some(last_before) = get_times_found_vec.get(get_times_found_vec.len() - (time + 1)) {
                    result = format!("{}", CallSnapshot::get_tps(last_before, last));
                }
            }
        }
        drop(get_times_found_list);
        drop(_guard);
        result
    }

    #[inline]
    pub fn get_put_tps(&self) -> String {
        format!(
            "{} {} {}",
            self.get_put_tps_time(10),
            self.get_put_tps_time(60),
            self.get_put_tps_time(600)
        )
    }

    #[inline]
    pub fn get_put_tps_time(&self, time: usize) -> String {
        let mut result = String::new();
        let _guard = self.sampling_lock.lock();
        let put_times_list = self.put_times_list.lock();
        let put_times_vec: Vec<_> = put_times_list.iter().collect();
        if let Some(last) = put_times_vec.last() {
            if put_times_vec.len() > time {
                if let Some(last_before) = put_times_vec.get(put_times_vec.len() - (time + 1)) {
                    result = format!("{}", CallSnapshot::get_tps(last_before, last));
                }
            }
        }
        drop(put_times_list);
        drop(_guard);
        result
    }

    #[inline]
    pub fn get_put_message_distribute_time_string_info(&self, total: u64) -> String {
        self.put_message_distribute_time_to_string()
    }

    #[inline]
    pub fn put_message_distribute_time_to_string(&self) -> String {
        let times = &self.last_put_message_distribute_time;
        let mut result = String::new();
        for (i, time) in times.iter().enumerate() {
            let value = time.load(Ordering::Relaxed);
            result.push_str(&format!("{}:{}, ", PUT_MESSAGE_ENTIRE_TIME_MAX_DESC[i], value));
        }
        result
    }

    #[inline]
    pub fn get_put_message_size_total(&self) -> u64 {
        let map = self.put_message_topic_size_total.read();
        map.values().map(|v| v.load(Ordering::Relaxed) as u64).sum()
    }

    #[inline]
    pub fn get_put_message_times_total(&self) -> u64 {
        let map = self.put_message_topic_times_total.read();
        map.values().map(|v| v.load(Ordering::Relaxed) as u64).sum()
    }

    #[inline]
    pub fn get_format_runtime(&self) -> String {
        let boot_time = self.message_store_boot_timestamp;
        let time = SystemClock::now() - boot_time as u128;

        let days = time / 86400;
        let hours = (time % 86400) / 3600;
        let minutes = (time % 3600) / 60;
        let seconds = time % 60;

        format!("[ {days} days, {hours} hours, {minutes} minutes, {seconds} seconds ]")
    }

    pub fn add_single_put_message_topic_times_total(&self, topic: &str, size: usize) {
        error!("add_single_put_message_topic_times_total not implemented");
    }

    pub fn add_single_put_message_topic_size_total(&self, topic: &str, size: usize) {
        error!("add_single_put_message_topic_times_total not implemented");
    }
}

impl fmt::Display for StoreStatsService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_times = self.get_put_message_times_total();
        let total_times_adj = if total_times == 0 { 1 } else { total_times };

        writeln!(f, "\truntime: {}", self.get_format_runtime())?;
        writeln!(
            f,
            "\tputMessageEntireTimeMax: {}",
            self.put_message_entire_time_max.load(Ordering::Relaxed)
        )?;
        writeln!(f, "\tputMessageTimesTotal: {total_times_adj}")?;
        writeln!(
            f,
            "\tgetPutMessageFailedTimes: {}",
            self.get_put_message_failed_times().load(Ordering::Relaxed)
        )?;
        writeln!(f, "\tputMessageSizeTotal: {}", self.get_put_message_size_total())?;
        writeln!(
            f,
            "\tputMessageDistributeTime: {}",
            self.get_put_message_distribute_time_string_info(total_times_adj)
        )?;
        writeln!(
            f,
            "\tputMessageAverageSize: {:.2}",
            self.get_put_message_size_total() as f64 / total_times_adj as f64
        )?;
        writeln!(
            f,
            "\tdispatchMaxBuffer: {}",
            self.dispatch_max_buffer.load(Ordering::Relaxed)
        )?;
        writeln!(
            f,
            "\tgetMessageEntireTimeMax: {}",
            self.get_message_entire_time_max.load(Ordering::Relaxed)
        )?;
        writeln!(f, "\tputTps: {}", self.get_put_tps())?;
        writeln!(f, "\tgetFoundTps: {}", self.get_get_found_tps())?;
        writeln!(f, "\tgetMissTps: {}", self.get_get_miss_tps())?;
        writeln!(f, "\tgetTotalTps: {}", self.get_get_total_tps())?;
        write!(f, "\tgetTransferredTps: {}", self.get_get_transferred_tps())?;

        Ok(())
    }
}

pub struct CallSnapshot {
    pub timestamp: u64,
    pub call_times_total: u64,
}

impl CallSnapshot {
    pub fn new(timestamp: u64, call_times_total: u64) -> Self {
        Self {
            timestamp,
            call_times_total,
        }
    }

    pub fn get_tps(begin: &CallSnapshot, end: &CallSnapshot) -> f64 {
        let total = end.call_times_total as i64 - begin.call_times_total as i64;
        let duration = end.timestamp as i64 - begin.timestamp as i64;
        let time_millis = duration as f64;

        if time_millis == 0.0 {
            return 0.0;
        }

        (total as f64 / time_millis) * 1000.0
    }
}

#[cfg(test)]
mod call_snapshot_tests {
    use super::*;

    #[test]
    fn calculates_tps_for_non_zero_duration() {
        let begin = CallSnapshot::new(1000, 10);
        let end = CallSnapshot::new(2000, 20);

        let tps = CallSnapshot::get_tps(&begin, &end);

        assert_eq!(tps, 10.0);
    }

    #[test]
    fn returns_zero_tps_for_zero_duration() {
        let begin = CallSnapshot::new(1000, 10);
        let end = CallSnapshot::new(1000, 20);

        let tps = CallSnapshot::get_tps(&begin, &end);

        assert_eq!(tps, 0.0);
    }

    #[test]
    fn calculates_tps_with_large_numbers() {
        let begin = CallSnapshot::new(1000, 1_000_000);
        let end = CallSnapshot::new(2000, 2_000_000);

        let tps = CallSnapshot::get_tps(&begin, &end);

        assert_eq!(tps, 1000000.0);
    }

    #[test]
    fn calculates_tps_for_negative_call_times_total() {
        let begin = CallSnapshot::new(1000, 20);
        let end = CallSnapshot::new(2000, 10);

        let tps = CallSnapshot::get_tps(&begin, &end);

        assert!(tps < 0.0);
    }
}
