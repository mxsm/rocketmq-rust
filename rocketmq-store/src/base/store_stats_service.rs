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

use std::array;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_common::common::broker::broker_config::BrokerIdentity;
use rocketmq_common::common::system_clock::SystemClock;
use rocketmq_common::TimeUtils::current_millis;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::info;
use tracing::warn;

const FREQUENCY_OF_SAMPLING: u64 = 1000;
const MAX_RECORDS_OF_SAMPLING: usize = 60 * 10;
const MAX_SNAPSHOT_RECORDS: usize = MAX_RECORDS_OF_SAMPLING + 1;
const PRINT_TPS_INTERVAL_SECS: u64 = 60;
const PUT_MESSAGE_DISTRIBUTE_BUCKETS: usize = 13;
const PUT_MESSAGE_ENTIRE_TIME_MAX_DESC: [&str; PUT_MESSAGE_DISTRIBUTE_BUCKETS] = [
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
const PUT_MESSAGE_DISTRIBUTE_LIMITS: [u64; PUT_MESSAGE_DISTRIBUTE_BUCKETS - 1] =
    [0, 10, 50, 100, 200, 500, 1000, 2000, 3000, 4000, 5000, 10000];

static PUT_MESSAGE_ENTIRE_TIME_BUCKET_BOUNDS: LazyLock<Vec<u64>> = LazyLock::new(|| {
    let mut bounds = Vec::with_capacity(77);
    let mut index = 0u64;
    for (interval, times) in [
        (1u64, 20usize),
        (2, 15),
        (5, 10),
        (10, 10),
        (50, 6),
        (100, 5),
        (1000, 9),
    ] {
        for _ in 0..times {
            index += interval;
            bounds.push(index);
        }
    }
    bounds.push(u64::MAX);
    bounds
});

type PutMessageDistributeTime = [AtomicU64; PUT_MESSAGE_DISTRIBUTE_BUCKETS];

pub struct StoreStatsService {
    put_message_time_buckets: Vec<AtomicU64>,
    last_put_message_time_buckets: Mutex<Vec<u64>>,
    put_message_failed_times: AtomicUsize,
    put_message_topic_times_total: DashMap<String, AtomicU64>,
    put_message_topic_size_total: DashMap<String, AtomicU64>,
    get_message_times_total_found: AtomicUsize,
    get_message_transferred_msg_count: AtomicUsize,
    get_message_times_total_miss: AtomicUsize,
    put_times_list: Mutex<VecDeque<CallSnapshot>>,
    get_times_found_list: Mutex<VecDeque<CallSnapshot>>,
    get_times_miss_list: Mutex<VecDeque<CallSnapshot>>,
    transferred_msg_count_list: Mutex<VecDeque<CallSnapshot>>,
    put_message_distribute_time: PutMessageDistributeTime,
    last_put_message_distribute_time: Mutex<[u64; PUT_MESSAGE_DISTRIBUTE_BUCKETS]>,
    message_store_boot_timestamp: u64,
    put_message_entire_time_max: AtomicU64,
    get_message_entire_time_max: AtomicU64,
    dispatch_max_buffer: AtomicU64,
    sampling_lock: Mutex<()>,
    last_print_timestamp: AtomicU64,
    stopped: AtomicBool,
    shutdown_notify: Notify,
    worker_handle: Mutex<Option<JoinHandle<()>>>,
    broker_identity: Option<BrokerIdentity>,
}

impl StoreStatsService {
    #[inline]
    pub fn new(broker_identity: Option<BrokerIdentity>) -> Self {
        let bucket_count = PUT_MESSAGE_ENTIRE_TIME_BUCKET_BOUNDS.len();
        Self {
            put_message_time_buckets: (0..bucket_count).map(|_| AtomicU64::new(0)).collect(),
            last_put_message_time_buckets: Mutex::new(vec![0; bucket_count]),
            put_message_failed_times: AtomicUsize::new(0),
            put_message_topic_times_total: DashMap::with_capacity(128),
            put_message_topic_size_total: DashMap::with_capacity(128),
            get_message_times_total_found: AtomicUsize::new(0),
            get_message_transferred_msg_count: AtomicUsize::new(0),
            get_message_times_total_miss: AtomicUsize::new(0),
            put_times_list: Mutex::new(VecDeque::with_capacity(MAX_SNAPSHOT_RECORDS)),
            get_times_found_list: Mutex::new(VecDeque::with_capacity(MAX_SNAPSHOT_RECORDS)),
            get_times_miss_list: Mutex::new(VecDeque::with_capacity(MAX_SNAPSHOT_RECORDS)),
            transferred_msg_count_list: Mutex::new(VecDeque::with_capacity(MAX_SNAPSHOT_RECORDS)),
            put_message_distribute_time: array::from_fn(|_| AtomicU64::new(0)),
            last_put_message_distribute_time: Mutex::new([0; PUT_MESSAGE_DISTRIBUTE_BUCKETS]),
            message_store_boot_timestamp: current_millis(),
            put_message_entire_time_max: AtomicU64::new(0),
            get_message_entire_time_max: AtomicU64::new(0),
            dispatch_max_buffer: AtomicU64::new(0),
            sampling_lock: Mutex::new(()),
            last_print_timestamp: AtomicU64::new(current_millis()),
            stopped: AtomicBool::new(true),
            shutdown_notify: Notify::new(),
            worker_handle: Mutex::new(None),
            broker_identity,
        }
    }

    pub fn start(self: &Arc<Self>) {
        let mut worker_handle = self.worker_handle.lock();
        if worker_handle.is_some() {
            return;
        }

        self.stopped.store(false, Ordering::Release);
        let service = Arc::clone(self);
        let service_name = service.get_service_name();
        let handle = tokio::spawn(async move {
            info!("{} service started", service_name);
            let mut interval = tokio::time::interval(Duration::from_millis(FREQUENCY_OF_SAMPLING));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = service.shutdown_notify.notified() => break,
                    _ = interval.tick() => {
                        if service.stopped.load(Ordering::Acquire) {
                            break;
                        }
                        service.sampling();
                        service.print_tps();
                    }
                }
            }

            info!("{} service end", service.get_service_name());
        });
        *worker_handle = Some(handle);
    }

    pub fn shutdown(&self) {
        self.stopped.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        if let Some(handle) = self.worker_handle.lock().take() {
            handle.abort();
        }
    }

    pub async fn shutdown_gracefully(&self) {
        self.stopped.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        let handle = self.worker_handle.lock().take();
        if let Some(handle) = handle {
            if let Err(error) = handle.await {
                if !error.is_cancelled() {
                    warn!("StoreStatsService task failed during shutdown: {error}");
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn has_worker_handle(&self) -> bool {
        self.worker_handle.lock().is_some()
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
    pub fn set_put_message_entire_time_max(&self, value: u64) {
        self.inc_put_message_entire_time(value);
        self.inc_put_message_distribute_time(value);
        self.put_message_entire_time_max.fetch_max(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_get_message_entire_time_max(&self, value: u64) {
        self.get_message_entire_time_max.fetch_max(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_dispatch_max_buffer(&self, value: u64) {
        self.dispatch_max_buffer.fetch_max(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_single_put_message_topic_times_total(&self, topic: &str, delta: usize) {
        self.add_topic_value(&self.put_message_topic_times_total, topic, delta as u64);
    }

    #[inline]
    pub fn add_single_put_message_topic_size_total(&self, topic: &str, delta: usize) {
        self.add_topic_value(&self.put_message_topic_size_total, topic, delta as u64);
    }

    #[inline]
    pub fn get_runtime_info(&self) -> HashMap<String, String> {
        let mut result = HashMap::with_capacity(64);
        let total_times = self.get_put_message_times_total();
        let total_times = if total_times == 0 { 1 } else { total_times };
        let put_message_size_total = self.get_put_message_size_total();

        result.insert(
            "bootTimestamp".to_string(),
            self.message_store_boot_timestamp.to_string(),
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
        result.insert("putMessageSizeTotal".to_string(), put_message_size_total.to_string());
        result.insert(
            "putMessageDistributeTime".to_string(),
            self.get_put_message_distribute_time_string_info(total_times),
        );
        result.insert(
            "putMessageAverageSize".to_string(),
            (put_message_size_total as f64 / total_times as f64).to_string(),
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
        if !(0.0..=1.0).contains(&px) {
            return 0.0;
        }

        let last_buckets = self.last_put_message_time_buckets.lock();
        let total_request: u64 = last_buckets.iter().sum();
        if total_request == 0 {
            return 0.0;
        }

        let px_index = (total_request as f64 * px) as u64;
        let bucket_bounds = PUT_MESSAGE_ENTIRE_TIME_BUCKET_BOUNDS.as_slice();
        let mut pass_count = 0u64;

        for (index, count) in last_buckets.iter().copied().enumerate() {
            if px_index <= pass_count + count {
                let relative_index = px_index.saturating_sub(pass_count);
                let bucket = bucket_bounds[index];
                if index == 0 {
                    return if count == 0 {
                        0.0
                    } else {
                        bucket as f64 * relative_index as f64 / count as f64
                    };
                }

                let last_bucket = bucket_bounds[index - 1];
                return last_bucket as f64
                    + if count == 0 {
                        0.0
                    } else {
                        (bucket - last_bucket) as f64 * relative_index as f64 / count as f64
                    };
            }

            pass_count += count;
        }

        0.0
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
        self.tps_from_list(&self.transferred_msg_count_list, time)
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
        let found = Self::tps_from_locked_list(&self.get_times_found_list.lock(), time);
        let miss = Self::tps_from_locked_list(&self.get_times_miss_list.lock(), time);
        (found + miss).to_string()
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
        self.tps_from_list(&self.get_times_miss_list, time)
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
        self.tps_from_list(&self.get_times_found_list, time)
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
        self.tps_from_list(&self.put_times_list, time)
    }

    #[inline]
    pub fn get_put_message_distribute_time_string_info(&self, _total: u64) -> String {
        self.put_message_distribute_time_to_string()
    }

    #[inline]
    pub fn put_message_distribute_time_to_string(&self) -> String {
        let times = self.last_put_message_distribute_time.lock();
        let mut result = String::new();
        for (index, value) in times.iter().enumerate() {
            result.push_str(&format!("{}:{} ", PUT_MESSAGE_ENTIRE_TIME_MAX_DESC[index], value));
        }
        result
    }

    #[inline]
    pub fn get_put_message_size_total(&self) -> u64 {
        Self::sum_topic_values(&self.put_message_topic_size_total)
    }

    #[inline]
    pub fn get_put_message_times_total(&self) -> u64 {
        Self::sum_topic_values(&self.put_message_topic_times_total)
    }

    #[inline]
    pub fn get_format_runtime(&self) -> String {
        let time = (SystemClock::now() as u64).saturating_sub(self.message_store_boot_timestamp);
        let second = 1000;
        let minute = 60 * second;
        let hour = 60 * minute;
        let day = 24 * hour;

        let days = time / day;
        let hours = (time % day) / hour;
        let minutes = (time % hour) / minute;
        let seconds = (time % minute) / second;

        format!("[ {days} days, {hours} hours, {minutes} minutes, {seconds} seconds ]")
    }

    fn get_service_name(&self) -> String {
        if let Some(broker_identity) = &self.broker_identity {
            if broker_identity.is_in_broker_container {
                return format!("{}StoreStatsService", broker_identity.get_canonical_name());
            }
        }
        "StoreStatsService".to_string()
    }

    fn sampling(&self) {
        let _guard = self.sampling_lock.lock();
        let now = current_millis();

        Self::push_snapshot(
            &mut self.put_times_list.lock(),
            CallSnapshot::new(now, self.get_put_message_times_total()),
        );
        Self::push_snapshot(
            &mut self.get_times_found_list.lock(),
            CallSnapshot::new(now, self.get_message_times_total_found.load(Ordering::Relaxed) as u64),
        );
        Self::push_snapshot(
            &mut self.get_times_miss_list.lock(),
            CallSnapshot::new(now, self.get_message_times_total_miss.load(Ordering::Relaxed) as u64),
        );
        Self::push_snapshot(
            &mut self.transferred_msg_count_list.lock(),
            CallSnapshot::new(
                now,
                self.get_message_transferred_msg_count.load(Ordering::Relaxed) as u64,
            ),
        );
    }

    fn print_tps(&self) {
        let now = current_millis();
        let last_print_timestamp = self.last_print_timestamp.load(Ordering::Acquire);
        if now <= last_print_timestamp + PRINT_TPS_INTERVAL_SECS * 1000 {
            return;
        }

        if self
            .last_print_timestamp
            .compare_exchange(last_print_timestamp, now, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        info!(
            "[STORETPS] put_tps {} get_found_tps {} get_miss_tps {} get_transferred_tps {}",
            self.get_put_tps_time(PRINT_TPS_INTERVAL_SECS as usize),
            self.get_get_found_tps_time(PRINT_TPS_INTERVAL_SECS as usize),
            self.get_get_miss_tps_time(PRINT_TPS_INTERVAL_SECS as usize),
            self.get_get_transferred_tps_time(PRINT_TPS_INTERVAL_SECS as usize)
        );

        let times = self.reset_put_message_distribute_time();
        let total_put: u64 = times.iter().sum();
        let mut info = String::new();
        for (index, value) in times.iter().enumerate() {
            info.push_str(&format!("{}:{} ", PUT_MESSAGE_ENTIRE_TIME_MAX_DESC[index], value));
        }

        self.reset_put_message_time_buckets();
        let put_latency_99 = self.find_put_message_entire_time_px(0.99);
        let put_latency_999 = self.find_put_message_entire_time_px(0.999);
        info!(
            "[PAGECACHERT] TotalPut {}, PutMessageDistributeTime {}, putLatency99 {:.2}, putLatency999 {:.2}",
            total_put, info, put_latency_99, put_latency_999
        );
    }

    fn reset_put_message_time_buckets(&self) {
        let mut last_buckets = self.last_put_message_time_buckets.lock();
        for (index, bucket) in self.put_message_time_buckets.iter().enumerate() {
            last_buckets[index] = bucket.swap(0, Ordering::AcqRel);
        }
    }

    fn reset_put_message_distribute_time(&self) -> [u64; PUT_MESSAGE_DISTRIBUTE_BUCKETS] {
        let mut next = [0; PUT_MESSAGE_DISTRIBUTE_BUCKETS];
        for (index, bucket) in self.put_message_distribute_time.iter().enumerate() {
            next[index] = bucket.swap(0, Ordering::AcqRel);
        }
        *self.last_put_message_distribute_time.lock() = next;
        next
    }

    #[inline]
    fn inc_put_message_entire_time(&self, value: u64) {
        let index = PUT_MESSAGE_ENTIRE_TIME_BUCKET_BOUNDS.partition_point(|bound| *bound < value);
        if let Some(bucket) = self.put_message_time_buckets.get(index) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn inc_put_message_distribute_time(&self, value: u64) {
        let index = if value == 0 {
            0
        } else {
            PUT_MESSAGE_DISTRIBUTE_LIMITS.partition_point(|limit| value >= *limit)
        };
        self.put_message_distribute_time[index].fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn add_topic_value(&self, map: &DashMap<String, AtomicU64>, topic: &str, delta: u64) {
        if delta == 0 {
            return;
        }

        if let Some(value) = map.get(topic) {
            value.fetch_add(delta, Ordering::Relaxed);
            return;
        }

        map.entry(topic.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(delta, Ordering::Relaxed);
    }

    #[inline]
    fn sum_topic_values(map: &DashMap<String, AtomicU64>) -> u64 {
        map.iter().map(|entry| entry.value().load(Ordering::Relaxed)).sum()
    }

    fn tps_from_list(&self, list: &Mutex<VecDeque<CallSnapshot>>, time: usize) -> String {
        let _guard = self.sampling_lock.lock();
        Self::tps_from_locked_list(&list.lock(), time).to_string()
    }

    fn tps_from_locked_list(list: &VecDeque<CallSnapshot>, time: usize) -> f64 {
        let Some(last) = list.back() else {
            return 0.0;
        };
        if list.len() <= time {
            return 0.0;
        }

        let Some(last_before) = list.get(list.len() - (time + 1)) else {
            return 0.0;
        };
        CallSnapshot::get_tps(last_before, last)
    }

    fn push_snapshot(list: &mut VecDeque<CallSnapshot>, snapshot: CallSnapshot) {
        list.push_back(snapshot);
        if list.len() > MAX_SNAPSHOT_RECORDS {
            list.pop_front();
        }
    }
}

impl Drop for StoreStatsService {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(handle) = self.worker_handle.get_mut().take() {
            handle.abort();
        }
    }
}

impl fmt::Display for StoreStatsService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_times = self.get_put_message_times_total();
        let total_times_adj = if total_times == 0 { 1 } else { total_times };
        let put_message_size_total = self.get_put_message_size_total();

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
        writeln!(f, "\tputMessageSizeTotal: {put_message_size_total}")?;
        writeln!(
            f,
            "\tputMessageDistributeTime: {}",
            self.get_put_message_distribute_time_string_info(total_times_adj)
        )?;
        writeln!(
            f,
            "\tputMessageAverageSize: {:.2}",
            put_message_size_total as f64 / total_times_adj as f64
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

#[derive(Clone, Copy, Debug)]
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
    use std::thread;

    use super::*;
    use tokio::task;

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

    #[test]
    fn accumulates_put_topic_totals_concurrently() {
        let stats = Arc::new(StoreStatsService::new(None));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let stats = Arc::clone(&stats);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    stats.add_single_put_message_topic_times_total("topic-a", 2);
                    stats.add_single_put_message_topic_size_total("topic-a", 16);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(stats.get_put_message_times_total(), 16_000);
        assert_eq!(stats.get_put_message_size_total(), 128_000);
    }

    #[test]
    fn calculates_window_tps_without_allocating_snapshot_vec() {
        let stats = StoreStatsService::new(None);
        {
            let mut put_times = stats.put_times_list.lock();
            for second in 0..=10 {
                StoreStatsService::push_snapshot(&mut put_times, CallSnapshot::new(second * 1000, second * 100));
            }
        }

        assert_eq!(stats.get_put_tps_time(10), "100");
    }

    #[test]
    fn records_latency_distribution_and_percentiles_from_last_window() {
        let stats = StoreStatsService::new(None);
        for value in 1..=100 {
            stats.set_put_message_entire_time_max(value);
        }

        stats.reset_put_message_distribute_time();
        stats.reset_put_message_time_buckets();

        assert_eq!(stats.put_message_entire_time_max.load(Ordering::Relaxed), 100);
        assert!(stats.put_message_distribute_time_to_string().contains("[10~50ms]:40"));
        assert!(stats.find_put_message_entire_time_px(0.99) >= 90.0);
    }

    #[test]
    fn updates_get_latency_and_dispatch_max_with_fetch_max_semantics() {
        let stats = StoreStatsService::new(None);

        stats.set_get_message_entire_time_max(10);
        stats.set_get_message_entire_time_max(7);
        stats.set_dispatch_max_buffer(100);
        stats.set_dispatch_max_buffer(80);

        assert_eq!(stats.get_message_entire_time_max.load(Ordering::Relaxed), 10);
        assert_eq!(stats.dispatch_max_buffer.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn sampling_keeps_only_java_compatible_ten_minute_window() {
        let stats = StoreStatsService::new(None);

        for index in 0..(MAX_SNAPSHOT_RECORDS + 10) {
            stats.add_single_put_message_topic_times_total("topic-a", 1);
            stats.get_message_times_total_found().store(index, Ordering::Relaxed);
            stats.get_message_times_total_miss().store(index * 2, Ordering::Relaxed);
            stats
                .get_message_transferred_msg_count()
                .store(index * 3, Ordering::Relaxed);
            stats.sampling();
        }

        assert_eq!(stats.put_times_list.lock().len(), MAX_SNAPSHOT_RECORDS);
        assert_eq!(stats.get_times_found_list.lock().len(), MAX_SNAPSHOT_RECORDS);
        assert_eq!(stats.get_times_miss_list.lock().len(), MAX_SNAPSHOT_RECORDS);
        assert_eq!(stats.transferred_msg_count_list.lock().len(), MAX_SNAPSHOT_RECORDS);
        assert_eq!(stats.put_times_list.lock().front().unwrap().call_times_total, 11);
    }

    #[test]
    fn runtime_info_reports_java_compatible_fields_and_values() {
        let stats = StoreStatsService::new(None);
        stats.add_single_put_message_topic_times_total("topic-a", 4);
        stats.add_single_put_message_topic_size_total("topic-a", 100);
        stats.get_put_message_failed_times().store(2, Ordering::Relaxed);
        stats.set_put_message_entire_time_max(50);
        stats.set_get_message_entire_time_max(9);
        stats.set_dispatch_max_buffer(256);
        stats.reset_put_message_distribute_time();
        stats.reset_put_message_time_buckets();

        let runtime_info = stats.get_runtime_info();

        for key in [
            "bootTimestamp",
            "runtime",
            "putMessageEntireTimeMax",
            "putMessageTimesTotal",
            "putMessageFailedTimes",
            "putMessageSizeTotal",
            "putMessageDistributeTime",
            "putMessageAverageSize",
            "dispatchMaxBuffer",
            "getMessageEntireTimeMax",
            "putTps",
            "getFoundTps",
            "getMissTps",
            "getTotalTps",
            "getTransferredTps",
            "putLatency99",
            "putLatency999",
        ] {
            assert!(runtime_info.contains_key(key), "missing runtime info key {key}");
        }

        assert_eq!(runtime_info["putMessageTimesTotal"], "4");
        assert_eq!(runtime_info["putMessageFailedTimes"], "2");
        assert_eq!(runtime_info["putMessageSizeTotal"], "100");
        assert_eq!(runtime_info["putMessageAverageSize"], "25");
        assert_eq!(runtime_info["putMessageEntireTimeMax"], "50");
        assert_eq!(runtime_info["getMessageEntireTimeMax"], "9");
        assert_eq!(runtime_info["dispatchMaxBuffer"], "256");
        assert!(runtime_info["runtime"].starts_with("[ 0 days, 0 hours"));
        assert!(runtime_info["putMessageDistributeTime"].contains("[50~100ms]:1"));
    }

    #[test]
    fn print_tps_rotates_latency_snapshots_for_percentile_queries() {
        let stats = StoreStatsService::new(None);
        for value in 1..=100 {
            stats.set_put_message_entire_time_max(value);
        }
        stats.last_print_timestamp.store(0, Ordering::Release);

        stats.print_tps();

        assert!(stats.find_put_message_entire_time_px(0.99) >= 90.0);
        assert!(stats.put_message_distribute_time_to_string().contains("[10~50ms]:40"));
        assert_eq!(
            stats
                .put_message_time_buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .sum::<u64>(),
            0
        );
    }

    #[tokio::test]
    async fn start_shutdown_are_idempotent_and_restartable() {
        let stats = Arc::new(StoreStatsService::new(None));

        stats.start();
        stats.start();
        assert!(stats.worker_handle.lock().is_some());
        assert!(!stats.stopped.load(Ordering::Acquire));

        task::yield_now().await;
        stats.shutdown_gracefully().await;
        stats.shutdown_gracefully().await;
        assert!(stats.worker_handle.lock().is_none());
        assert!(stats.stopped.load(Ordering::Acquire));

        stats.start();
        assert!(stats.worker_handle.lock().is_some());
        stats.shutdown_gracefully().await;
    }
}
