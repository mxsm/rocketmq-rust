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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_common::common::broker::broker_config::BrokerIdentity;
use rocketmq_common::TimeUtils::get_current_millis;

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

lazy_static::lazy_static! {
    static ref PUT_MESSAGE_ENTIRE_TIME_BUCKETS: BTreeMap<i32, i32> = {
        let mut m = BTreeMap::new();
        m.insert(1, 20);
        m.insert(2, 15);
        m.insert(5, 10);
        m.insert(10, 10);
        m.insert(50, 6);
        m.insert(100, 5);
        m.insert(1000, 9);
        m
    };
}

type AtomicUsizeArray = Arc<Vec<AtomicUsize>>;

pub struct StoreStatsService {
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
    pub fn new(broker_identity: Option<BrokerIdentity>) -> Self {
        Self {
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
            last_put_message_distribute_time: Arc::new(
                (0..13).map(|_| AtomicUsize::new(0)).collect(),
            ),
            message_store_boot_timestamp: get_current_millis(),
            put_message_entire_time_max: Arc::new(AtomicUsize::new(0)),
            get_message_entire_time_max: Arc::new(AtomicUsize::new(0)),
            dispatch_max_buffer: Arc::new(AtomicUsize::new(0)),
            sampling_lock: Mutex::new(()),
            last_print_timestamp: get_current_millis(),
            broker_identity,
        }
    }

    fn reset_put_message_time_buckets(&self) {
        let mut buckets = BTreeMap::new();
        for (interval, times) in PUT_MESSAGE_ENTIRE_TIME_BUCKETS.iter() {
            buckets.insert(*interval as i64, AtomicUsize::new(*times as usize));
        }
        let mut last_buckets = BTreeMap::new();
        for (interval, times) in PUT_MESSAGE_ENTIRE_TIME_BUCKETS.iter() {
            last_buckets.insert(*interval as i64, AtomicUsize::new(*times as usize));
        }
    }

    fn reset_put_message_distribute_time(&self) {
        for i in 0..13 {
            self.put_message_distribute_time[i].store(0, Ordering::SeqCst);
            self.last_put_message_distribute_time[i].store(0, Ordering::SeqCst);
        }
    }

    // Add more methods as needed for functionality
}

impl StoreStatsService {
    pub fn get_get_message_times_total_found(&self) -> &AtomicUsize {
        &self.get_message_times_total_found
    }

    pub fn get_get_message_times_total_miss(&self) -> &AtomicUsize {
        &self.get_message_times_total_miss
    }

    pub fn get_message_transferred_msg_count(&self) -> &AtomicUsize {
        &self.get_message_transferred_msg_count
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
