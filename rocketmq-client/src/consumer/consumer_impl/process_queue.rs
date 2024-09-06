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
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::RwLock;

use once_cell::sync::Lazy;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::TimeUtils::get_current_millis;

static REBALANCE_LOCK_MAX_LIVE_TIME: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.lockMaxLiveTime")
        .unwrap_or_else(|_| "30000".into())
        .parse()
        .unwrap_or(30000)
});

static REBALANCE_LOCK_INTERVAL: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.rebalance.lockInterval")
        .unwrap_or_else(|_| "20000".into())
        .parse()
        .unwrap_or(20000)
});

static PULL_MAX_IDLE_TIME: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.pull.pullMaxIdleTime")
        .unwrap_or_else(|_| "120000".into())
        .parse()
        .unwrap_or(120000)
});

pub(crate) struct ProcessQueue {
    tree_map_lock: RwLock<()>,
    msg_tree_map: Arc<RwLock<std::collections::BTreeMap<i64, MessageExt>>>,
    msg_count: Arc<AtomicI64>,
    msg_size: Arc<AtomicI64>,
    consume_lock: RwLock<()>,
    consuming_msg_orderly_tree_map: Arc<RwLock<std::collections::BTreeMap<i64, MessageExt>>>,
    try_unlock_times: Arc<AtomicI64>,
    queue_offset_max: Arc<std::sync::Mutex<i64>>,
    dropped: Arc<std::sync::Mutex<bool>>,
    last_pull_timestamp: Arc<std::sync::Mutex<u64>>,
    last_consume_timestamp: Arc<std::sync::Mutex<u64>>,
    locked: Arc<std::sync::Mutex<bool>>,
    last_lock_timestamp: Arc<std::sync::Mutex<u64>>,
    consuming: Arc<std::sync::Mutex<bool>>,
    msg_acc_cnt: Arc<AtomicI64>,
}

impl ProcessQueue {
    pub(crate) fn new() -> Self {
        ProcessQueue {
            tree_map_lock: RwLock::new(()),
            msg_tree_map: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            msg_count: Arc::new(AtomicI64::new(0)),
            msg_size: Arc::new(AtomicI64::new(0)),
            consume_lock: RwLock::new(()),
            consuming_msg_orderly_tree_map: Arc::new(
                RwLock::new(std::collections::BTreeMap::new()),
            ),
            try_unlock_times: Arc::new(AtomicI64::new(0)),
            queue_offset_max: Arc::new(std::sync::Mutex::new(0)),
            dropped: Arc::new(std::sync::Mutex::new(false)),
            last_pull_timestamp: Arc::new(std::sync::Mutex::new(get_current_millis())),
            last_consume_timestamp: Arc::new(std::sync::Mutex::new(get_current_millis())),
            locked: Arc::new(std::sync::Mutex::new(false)),
            last_lock_timestamp: Arc::new(std::sync::Mutex::new(get_current_millis())),
            consuming: Arc::new(std::sync::Mutex::new(false)),
            msg_acc_cnt: Arc::new(AtomicI64::new(0)),
        }
    }
}
