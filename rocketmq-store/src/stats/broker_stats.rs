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

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tracing::error;
use tracing::info;

use crate::base::message_store::MessageStore;
use crate::stats::broker_stats_manager::BrokerStatsManager;

pub struct BrokerStats<MS> {
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
    message_store_marker: PhantomData<fn() -> MS>,
    msg_put_total_yesterday_morning: AtomicU64,
    msg_put_total_today_morning: AtomicU64,
    msg_get_total_yesterday_morning: AtomicU64,
    msg_get_total_today_morning: AtomicU64,
}

impl<MS> BrokerStats<MS> {
    pub fn from_manager(broker_stats_manager: Option<Arc<BrokerStatsManager>>) -> Self {
        BrokerStats {
            broker_stats_manager,
            message_store_marker: PhantomData,
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

        match self.broker_stats_manager.as_ref() {
            Some(manager) => {
                self.msg_put_total_today_morning
                    .store(manager.get_broker_puts_num_without_system_topic(), Ordering::Relaxed);
                self.msg_get_total_today_morning
                    .store(manager.get_broker_gets_num_without_system_topic(), Ordering::Relaxed);

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
        match self.broker_stats_manager.as_ref() {
            Some(manager) => manager.get_broker_puts_num_without_system_topic(),
            None => {
                error!("Failed to get BrokerStatsManager");
                0
            }
        }
    }

    pub fn get_msg_get_total_today_now(&self) -> u64 {
        match self.broker_stats_manager.as_ref() {
            Some(manager) => manager.get_broker_gets_num_without_system_topic(),
            None => {
                error!("Failed to get BrokerStatsManager");
                0
            }
        }
    }
}

impl<MS: MessageStore> BrokerStats<MS> {
    /// Creates broker statistics from a message store without retaining the store handle.
    ///
    /// New composition roots should prefer [`Self::from_manager`] and inject the observer
    /// capability directly. This constructor remains as a compatibility bridge for callers
    /// that still own a dereferenceable message-store handle.
    pub fn new<S>(message_store: S) -> Self
    where
        S: Deref<Target = MS>,
    {
        Self::from_manager(message_store.get_broker_stats_manager().cloned())
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::broker::broker_config::BrokerConfig;

    use super::*;

    #[test]
    fn records_snapshots_from_injected_stats_manager() {
        let manager = Arc::new(BrokerStatsManager::new(Arc::new(BrokerConfig::default())));
        let stats = BrokerStats::<()>::from_manager(Some(Arc::clone(&manager)));

        manager.inc_broker_put_nums("UserTopic", 10);
        manager.inc_broker_get_nums("UserTopic", 7);
        stats.record();

        assert_eq!(stats.get_msg_put_total_yesterday_morning(), 0);
        assert_eq!(stats.get_msg_put_total_today_morning(), 10);
        assert_eq!(stats.get_msg_get_total_yesterday_morning(), 0);
        assert_eq!(stats.get_msg_get_total_today_morning(), 7);

        manager.inc_broker_put_nums("UserTopic", 4);
        manager.inc_broker_get_nums("UserTopic", 3);
        assert_eq!(stats.get_msg_put_total_today_now(), 14);
        assert_eq!(stats.get_msg_get_total_today_now(), 10);

        stats.record();
        assert_eq!(stats.get_msg_put_total_yesterday_morning(), 10);
        assert_eq!(stats.get_msg_put_total_today_morning(), 14);
        assert_eq!(stats.get_msg_get_total_yesterday_morning(), 7);
        assert_eq!(stats.get_msg_get_total_today_morning(), 10);
    }

    #[test]
    fn missing_stats_manager_preserves_zero_fallback() {
        let stats = BrokerStats::<()>::from_manager(None);

        stats.record();

        assert_eq!(stats.get_msg_put_total_today_now(), 0);
        assert_eq!(stats.get_msg_get_total_today_now(), 0);
        assert_eq!(stats.get_msg_put_total_today_morning(), 0);
        assert_eq!(stats.get_msg_get_total_today_morning(), 0);
    }

    #[test]
    fn broker_stats_does_not_retain_message_store_ownership() {
        let source = include_str!("broker_stats.rs");
        let forbidden = concat!("Arc", "Mut");
        let forbidden_store_field = concat!("default_message", "_store");

        assert!(!source.contains(forbidden));
        assert!(!source.contains(forbidden_store_field));
        assert!(source.contains("broker_stats_manager: Option<Arc<BrokerStatsManager>>"));
    }
}
