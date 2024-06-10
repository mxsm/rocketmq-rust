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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tracing::error;
use tracing::info;

use crate::log_file::MessageStore;

pub struct BrokerStats<MS: MessageStore> {
    default_message_store: Arc<MS>,
    msg_put_total_yesterday_morning: AtomicU64,
    msg_put_total_today_morning: AtomicU64,
    msg_get_total_yesterday_morning: AtomicU64,
    msg_get_total_today_morning: AtomicU64,
}

impl<MS: MessageStore> BrokerStats<MS> {
    pub fn new(default_message_store: Arc<MS>) -> Self {
        BrokerStats {
            default_message_store,
            msg_put_total_yesterday_morning: AtomicU64::new(0),
            msg_put_total_today_morning: AtomicU64::new(0),
            msg_get_total_yesterday_morning: AtomicU64::new(0),
            msg_get_total_today_morning: AtomicU64::new(0),
        }
    }

    pub fn record(&self) {
        self.msg_put_total_yesterday_morning.store(
            self.msg_put_total_today_morning.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.msg_get_total_yesterday_morning.store(
            self.msg_get_total_today_morning.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        let broker_stats_manager = self.default_message_store.get_broker_stats_manager();
        match broker_stats_manager {
            Some(manager) => {
                self.msg_put_total_today_morning.store(
                    manager.get_broker_puts_num_without_system_topic(),
                    Ordering::Relaxed,
                );
                self.msg_get_total_today_morning.store(
                    manager.get_broker_gets_num_without_system_topic(),
                    Ordering::Relaxed,
                );

                info!(
                    "yesterday put message total: {}",
                    self.msg_put_total_today_morning.load(Ordering::Relaxed)
                        - self.msg_put_total_yesterday_morning.load(Ordering::Relaxed)
                );
                info!(
                    "yesterday get message total: {}",
                    self.msg_get_total_today_morning.load(Ordering::Relaxed)
                        - self.msg_get_total_yesterday_morning.load(Ordering::Relaxed)
                );
            }
            None => {
                error!("Failed to get BrokerStatsManager");
            }
        }
    }

    pub fn get_msg_put_total_yesterday_morning(&self) -> u64 {
        self.msg_put_total_yesterday_morning.load(Ordering::Relaxed)
    }

    pub fn get_msg_put_total_today_morning(&self) -> u64 {
        self.msg_put_total_today_morning.load(Ordering::Relaxed)
    }

    pub fn get_msg_get_total_yesterday_morning(&self) -> u64 {
        self.msg_get_total_yesterday_morning.load(Ordering::Relaxed)
    }

    pub fn get_msg_get_total_today_morning(&self) -> u64 {
        self.msg_get_total_today_morning.load(Ordering::Relaxed)
    }

    pub fn get_msg_put_total_today_now(&self) -> u64 {
        match self.default_message_store.get_broker_stats_manager() {
            Some(manager) => manager.get_broker_puts_num_without_system_topic(),
            None => {
                error!("Failed to get BrokerStatsManager");
                0
            }
        }
    }

    pub fn get_msg_get_total_today_now(&self) -> u64 {
        match self.default_message_store.get_broker_stats_manager() {
            Some(manager) => manager.get_broker_gets_num_without_system_topic(),
            None => {
                error!("Failed to get BrokerStatsManager");
                0
            }
        }
    }
}
