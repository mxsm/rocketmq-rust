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

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::statistics::state_getter::StateGetter;
use rocketmq_common::common::statistics::statistics_item::StatisticsItem;
use rocketmq_common::common::statistics::statistics_item_formatter::StatisticsItemFormatter;
use rocketmq_common::common::statistics::statistics_item_printer::StatisticsItemPrinter;
use rocketmq_common::common::statistics::statistics_item_scheduled_printer::StatisticsItemScheduledPrinter;
use rocketmq_common::common::statistics::statistics_item_state_getter::StatisticsItemStateGetter;
use rocketmq_common::common::statistics::statistics_kind_meta::StatisticsKindMeta;
use rocketmq_common::common::statistics::statistics_manager::StatisticsManager;
use rocketmq_common::common::stats::moment_stats_item_set::MomentStatsItemSet;
use rocketmq_common::common::stats::stats_item::StatsItem;
use rocketmq_common::common::stats::stats_item_set::StatsItemSet;
use rocketmq_common::common::stats::Stats;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_rust::schedule::simple_scheduler::ScheduledTaskManager;
use tokio::time::Duration;
use tracing::info;
use tracing::warn;

type TaskId = u64;

pub struct BrokerStatsManager {
    stats_table: Arc<DashMap<String, StatsItemSet>>,
    cluster_name: String,
    enable_queue_stat: bool,
    moment_stats_item_set_fall_size: Option<Arc<MomentStatsItemSet>>,
    moment_stats_item_set_fall_time: Option<Arc<MomentStatsItemSet>>,
    account_stat_manager: StatisticsManager,
    producer_state_getter: Option<Arc<dyn StateGetter>>,
    consumer_state_getter: Option<Arc<dyn StateGetter>>,
    broker_config: Option<Arc<BrokerConfig>>,
    scheduler: Option<Arc<ScheduledTaskManager>>,
    task_ids: Arc<Mutex<Vec<TaskId>>>,
}

impl BrokerStatsManager {
    pub const ACCOUNT_AUTH_FAILED: &'static str = "AUTH_FAILED";
    pub const ACCOUNT_AUTH_TYPE: &'static str = "AUTH_TYPE";
    pub const ACCOUNT_OWNER_PARENT: &'static str = "OWNER_PARENT";
    pub const ACCOUNT_OWNER_SELF: &'static str = "OWNER_SELF";
    pub const ACCOUNT_RCV: &'static str = "RCV";
    pub const ACCOUNT_REV_REJ: &'static str = "RCV_REJ";
    pub const ACCOUNT_SEND: &'static str = "SEND";
    pub const ACCOUNT_SEND_BACK: &'static str = "SEND_BACK";
    pub const ACCOUNT_SEND_BACK_TO_DLQ: &'static str = "SEND_BACK_TO_DLQ";
    pub const ACCOUNT_SEND_REJ: &'static str = "SEND_REJ";
    pub const ACCOUNT_STAT_INVERTAL: u64 = 60 * 1000;
    pub const BROKER_ACK_NUMS: &'static str = "BROKER_ACK_NUMS";
    pub const BROKER_CK_NUMS: &'static str = "BROKER_CK_NUMS";
    pub const BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC: &'static str = "BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC";
    pub const BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC: &'static str = "BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC";
    pub const CHANNEL_ACTIVITY: &'static str = "CHANNEL_ACTIVITY";
    pub const CHANNEL_ACTIVITY_CLOSE: &'static str = "CLOSE";
    pub const CHANNEL_ACTIVITY_CONNECT: &'static str = "CONNECT";
    pub const CHANNEL_ACTIVITY_EXCEPTION: &'static str = "EXCEPTION";
    pub const CHANNEL_ACTIVITY_IDLE: &'static str = "IDLE";
    pub const COMMERCIAL_MSG_NUM: &'static str = "COMMERCIAL_MSG_NUM";
    pub const COMMERCIAL_OWNER: &'static str = "Owner";
    // Consumer Register Time
    pub const CONSUMER_REGISTER_TIME: &'static str = "CONSUMER_REGISTER_TIME";
    pub const DLQ_PUT_NUMS: &'static str = "DLQ_PUT_NUMS";
    pub const FAILURE_MSG_NUM: &'static str = "FAILURE_MSG_NUM";
    pub const FAILURE_MSG_SIZE: &'static str = "FAILURE_MSG_SIZE";
    pub const FAILURE_REQ_NUM: &'static str = "FAILURE_REQ_NUM";
    pub const GROUP_ACK_NUMS: &'static str = "GROUP_ACK_NUMS";
    pub const GROUP_CK_NUMS: &'static str = "GROUP_CK_NUMS";
    #[deprecated]
    pub const GROUP_GET_FALL_SIZE: &'static str = "GROUP_GET_FALL_SIZE";
    #[deprecated]
    pub const GROUP_GET_FALL_TIME: &'static str = "GROUP_GET_FALL_TIME";
    // Pull Message Latency
    #[deprecated]
    pub const GROUP_GET_LATENCY: &'static str = "GROUP_GET_LATENCY";
    pub const INNER_RT: &'static str = "INNER_RT";
    pub const MSG_NUM: &'static str = "MSG_NUM";
    pub const MSG_SIZE: &'static str = "MSG_SIZE";
    // Producer Register Time
    pub const PRODUCER_REGISTER_TIME: &'static str = "PRODUCER_REGISTER_TIME";
    pub const RT: &'static str = "RT";
    pub const SNDBCK2DLQ_TIMES: &'static str = "SNDBCK2DLQ_TIMES";
    pub const SUCCESS_MSG_NUM: &'static str = "SUCCESS_MSG_NUM";
    pub const SUCCESS_MSG_SIZE: &'static str = "SUCCESS_MSG_SIZE";
    pub const SUCCESS_REQ_NUM: &'static str = "SUCCESS_REQ_NUM";
    pub const TOPIC_PUT_LATENCY: &'static str = "TOPIC_PUT_LATENCY";
}

impl BrokerStatsManager {
    #[inline]
    pub fn start(&self) {
        if let Some(scheduler) = &self.scheduler {
            self.start_sampling_tasks(scheduler);
            info!("BrokerStatsManager started with scheduled tasks");
        } else {
            warn!("ScheduledTaskManager not provided, sampling tasks not started");
        }
    }

    /// Start all periodic sampling tasks
    fn start_sampling_tasks(&self, scheduler: &Arc<ScheduledTaskManager>) {
        // Task 1: Sample every 10 seconds for minute-level statistics
        let stats_table = Arc::clone(&self.stats_table);
        let task_id = scheduler.add_fixed_rate_task(Duration::from_secs(10), Duration::from_secs(10), move |_cancel| {
            let stats_table = Arc::clone(&stats_table);
            async move {
                for entry in stats_table.iter() {
                    entry.value().sampling_in_minutes();
                }
                Ok(())
            }
        });
        self.task_ids.lock().push(task_id);

        // Task 2: Sample every minute (aligned to minute boundary)
        let stats_table = Arc::clone(&self.stats_table);
        let initial_delay = Self::compute_initial_delay_to_next_minute();
        let task_id = scheduler.add_fixed_rate_task(initial_delay, Duration::from_secs(60), move |_cancel| {
            let stats_table = Arc::clone(&stats_table);
            async move {
                info!("Executing minute-level sampling for all stats");
                for entry in stats_table.iter() {
                    entry.value().sampling_in_minutes();
                }
                Ok(())
            }
        });
        self.task_ids.lock().push(task_id);

        // Task 3: Clean up expired stats every 10 minutes
        let stats_table = Arc::clone(&self.stats_table);
        let task_id =
            scheduler.add_fixed_rate_task(Duration::from_secs(600), Duration::from_secs(600), move |_cancel| {
                let stats_table = Arc::clone(&stats_table);
                async move {
                    info!("Cleaning expired statistics items");
                    // TODO: Implement cleanup logic based on last access time
                    Ok(())
                }
            });
        self.task_ids.lock().push(task_id);

        info!(
            "Started {} scheduled tasks for BrokerStatsManager",
            self.task_ids.lock().len()
        );
    }

    /// Compute delay to next minute boundary
    fn compute_initial_delay_to_next_minute() -> Duration {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let next_minute = ((now / 60000) + 1) * 60000;
        let delay_ms = next_minute - now;

        Duration::from_millis(delay_ms)
    }

    #[inline]
    pub fn new(broker_config: Arc<BrokerConfig>) -> Self {
        Self::new_with_scheduler(broker_config, None)
    }

    #[inline]
    pub fn new_with_scheduler(broker_config: Arc<BrokerConfig>, scheduler: Option<Arc<ScheduledTaskManager>>) -> Self {
        let stats_table = Arc::new(DashMap::new());
        let enable_queue_stat = broker_config.enable_detail_stat;
        let cluster_name = broker_config.broker_identity.broker_cluster_name.to_string();
        let mut broker_stats_manager = BrokerStatsManager {
            stats_table,
            cluster_name,
            enable_queue_stat,
            moment_stats_item_set_fall_size: None,
            moment_stats_item_set_fall_time: None,
            account_stat_manager: Default::default(),
            producer_state_getter: None,
            consumer_state_getter: None,
            broker_config: Some(broker_config),
            scheduler,
            task_ids: Arc::new(Mutex::new(Vec::new())),
        };
        broker_stats_manager.init();
        broker_stats_manager
    }

    #[inline]
    pub fn new_with_name(broker_config: Arc<BrokerConfig>, cluster_name: String, enable_queue_stat: bool) -> Self {
        let stats_table = Arc::new(DashMap::new());
        let mut broker_stats_manager = BrokerStatsManager {
            stats_table,
            cluster_name,
            enable_queue_stat,
            moment_stats_item_set_fall_size: None,
            moment_stats_item_set_fall_time: None,
            account_stat_manager: Default::default(),
            producer_state_getter: None,
            consumer_state_getter: None,
            broker_config: Some(broker_config),
            scheduler: None,
            task_ids: Arc::new(Mutex::new(Vec::new())),
        };
        broker_stats_manager.init();
        broker_stats_manager
    }

    #[inline]
    pub fn init(&mut self) {
        self.moment_stats_item_set_fall_size = Some(Arc::new(MomentStatsItemSet::new(
            Stats::GROUP_GET_FALL_SIZE.to_string(),
        )));

        self.moment_stats_item_set_fall_time = Some(Arc::new(MomentStatsItemSet::new(
            Stats::GROUP_GET_FALL_TIME.to_string(),
        )));

        let enable_queue_stat = self.enable_queue_stat;

        if enable_queue_stat {
            self.stats_table.insert(
                Stats::QUEUE_PUT_NUMS.to_string(),
                StatsItemSet::new(Stats::QUEUE_PUT_NUMS.to_string()),
            );
            self.stats_table.insert(
                Stats::QUEUE_PUT_SIZE.to_string(),
                StatsItemSet::new(Stats::QUEUE_PUT_SIZE.to_string()),
            );
            self.stats_table.insert(
                Stats::QUEUE_GET_NUMS.to_string(),
                StatsItemSet::new(Stats::QUEUE_GET_NUMS.to_string()),
            );
            self.stats_table.insert(
                Stats::QUEUE_GET_SIZE.to_string(),
                StatsItemSet::new(Stats::QUEUE_GET_SIZE.to_string()),
            );
        }

        self.stats_table.insert(
            Stats::TOPIC_PUT_NUMS.to_string(),
            StatsItemSet::new(Stats::TOPIC_PUT_NUMS.to_string()),
        );
        self.stats_table.insert(
            Stats::TOPIC_PUT_SIZE.to_string(),
            StatsItemSet::new(Stats::TOPIC_PUT_SIZE.to_string()),
        );
        self.stats_table.insert(
            Stats::GROUP_GET_NUMS.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_NUMS.to_string()),
        );
        self.stats_table.insert(
            Stats::GROUP_GET_SIZE.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_SIZE.to_string()),
        );
        self.stats_table.insert(
            Self::GROUP_ACK_NUMS.to_string(),
            StatsItemSet::new(Self::GROUP_ACK_NUMS.to_string()),
        );
        self.stats_table.insert(
            Self::GROUP_CK_NUMS.to_string(),
            StatsItemSet::new(Self::GROUP_CK_NUMS.to_string()),
        );
        self.stats_table.insert(
            Stats::GROUP_GET_LATENCY.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_LATENCY.to_string()),
        );
        self.stats_table.insert(
            Self::TOPIC_PUT_LATENCY.to_string(),
            StatsItemSet::new(Self::TOPIC_PUT_LATENCY.to_string()),
        );
        self.stats_table.insert(
            Stats::SNDBCK_PUT_NUMS.to_string(),
            StatsItemSet::new(Stats::SNDBCK_PUT_NUMS.to_string()),
        );
        self.stats_table.insert(
            Self::DLQ_PUT_NUMS.to_string(),
            StatsItemSet::new(Self::DLQ_PUT_NUMS.to_string()),
        );
        self.stats_table.insert(
            Stats::BROKER_PUT_NUMS.to_string(),
            StatsItemSet::new(Stats::BROKER_PUT_NUMS.to_string()),
        );
        self.stats_table.insert(
            Stats::BROKER_GET_NUMS.to_string(),
            StatsItemSet::new(Stats::BROKER_GET_NUMS.to_string()),
        );
        self.stats_table.insert(
            Self::BROKER_ACK_NUMS.to_string(),
            StatsItemSet::new(Self::BROKER_ACK_NUMS.to_string()),
        );
        self.stats_table.insert(
            Self::BROKER_CK_NUMS.to_string(),
            StatsItemSet::new(Self::BROKER_CK_NUMS.to_string()),
        );
        self.stats_table.insert(
            Self::BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC.to_string(),
            StatsItemSet::new(Self::BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC.to_string()),
        );
        self.stats_table.insert(
            Self::BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC.to_string(),
            StatsItemSet::new(Self::BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC.to_string()),
        );
        self.stats_table.insert(
            Stats::GROUP_GET_FROM_DISK_NUMS.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_FROM_DISK_NUMS.to_string()),
        );
        self.stats_table.insert(
            Stats::GROUP_GET_FROM_DISK_SIZE.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_FROM_DISK_SIZE.to_string()),
        );
        self.stats_table.insert(
            Stats::BROKER_GET_FROM_DISK_NUMS.to_string(),
            StatsItemSet::new(Stats::BROKER_GET_FROM_DISK_NUMS.to_string()),
        );
        self.stats_table.insert(
            Stats::BROKER_GET_FROM_DISK_SIZE.to_string(),
            StatsItemSet::new(Stats::BROKER_GET_FROM_DISK_SIZE.to_string()),
        );
        self.stats_table.insert(
            Self::SNDBCK2DLQ_TIMES.to_string(),
            StatsItemSet::new(Self::SNDBCK2DLQ_TIMES.to_string()),
        );
        self.stats_table.insert(
            Stats::COMMERCIAL_SEND_TIMES.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_SEND_TIMES.to_string()),
        );
        self.stats_table.insert(
            Stats::COMMERCIAL_RCV_TIMES.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_RCV_TIMES.to_string()),
        );
        self.stats_table.insert(
            Stats::COMMERCIAL_SEND_SIZE.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_SEND_SIZE.to_string()),
        );
        self.stats_table.insert(
            Stats::COMMERCIAL_RCV_SIZE.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_RCV_SIZE.to_string()),
        );
        self.stats_table.insert(
            Stats::COMMERCIAL_RCV_EPOLLS.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_RCV_EPOLLS.to_string()),
        );
        self.stats_table.insert(
            Stats::COMMERCIAL_SNDBCK_TIMES.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_SNDBCK_TIMES.to_string()),
        );
        self.stats_table.insert(
            Stats::COMMERCIAL_PERM_FAILURES.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_PERM_FAILURES.to_string()),
        );
        self.stats_table.insert(
            Self::CONSUMER_REGISTER_TIME.to_string(),
            StatsItemSet::new(Self::CONSUMER_REGISTER_TIME.to_string()),
        );
        self.stats_table.insert(
            Self::PRODUCER_REGISTER_TIME.to_string(),
            StatsItemSet::new(Self::PRODUCER_REGISTER_TIME.to_string()),
        );
        self.stats_table.insert(
            Self::CHANNEL_ACTIVITY.to_string(),
            StatsItemSet::new(Self::CHANNEL_ACTIVITY.to_string()),
        );

        let formatter = StatisticsItemFormatter;

        self.account_stat_manager.set_brief_meta(vec![
            (Self::RT.to_string(), vec![vec![50, 50], vec![100, 10], vec![1000, 10]]),
            (
                Self::INNER_RT.to_string(),
                vec![vec![50, 50], vec![100, 10], vec![1000, 10]],
            ),
        ]);

        let item_names = vec![
            Self::MSG_NUM,
            Self::SUCCESS_MSG_NUM,
            Self::FAILURE_MSG_NUM,
            Self::COMMERCIAL_MSG_NUM,
            Self::SUCCESS_REQ_NUM,
            Self::FAILURE_REQ_NUM,
            Self::MSG_SIZE,
            Self::SUCCESS_MSG_SIZE,
            Self::FAILURE_MSG_SIZE,
            Self::RT,
            Self::INNER_RT,
        ];

        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                Self::ACCOUNT_SEND,
                item_names.clone(),
                &formatter,
                Self::ACCOUNT_STAT_INVERTAL,
                self.broker_config.as_ref().expect("Broker config must be initialized"),
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                Self::ACCOUNT_RCV,
                item_names.clone(),
                &formatter,
                Self::ACCOUNT_STAT_INVERTAL,
                self.broker_config.as_ref().expect("Broker config must be initialized"),
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                Self::ACCOUNT_SEND_BACK,
                item_names.clone(),
                &formatter,
                Self::ACCOUNT_STAT_INVERTAL,
                self.broker_config.as_ref().expect("Broker config must be initialized"),
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                Self::ACCOUNT_SEND_BACK_TO_DLQ,
                item_names.clone(),
                &formatter,
                Self::ACCOUNT_STAT_INVERTAL,
                self.broker_config.as_ref().expect("Broker config must be initialized"),
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                Self::ACCOUNT_SEND_REJ,
                item_names.clone(),
                &formatter,
                Self::ACCOUNT_STAT_INVERTAL,
                self.broker_config.as_ref().expect("Broker config must be initialized"),
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                Self::ACCOUNT_REV_REJ,
                item_names.clone(),
                &formatter,
                Self::ACCOUNT_STAT_INVERTAL,
                self.broker_config.as_ref().expect("Broker config must be initialized"),
            ));

        struct DefaultStatisticsItemStateGetter {
            producer_state_getter: Option<Arc<dyn StateGetter>>,
            consumer_state_getter: Option<Arc<dyn StateGetter>>,
        }
        impl StatisticsItemStateGetter for DefaultStatisticsItemStateGetter {
            #[inline]
            fn online(&self, item: &StatisticsItem) -> bool {
                let vec = split_account_stat_key(item.stat_object());
                if vec.is_empty() || vec.len() < 4 {
                    return false;
                }
                let instance_id = CheetahString::from_slice(vec[1]);
                let topic = CheetahString::from_slice(vec[2]);
                let group = CheetahString::from_slice(vec[3]);
                let kind = item.stat_kind();
                if BrokerStatsManager::ACCOUNT_SEND == kind || BrokerStatsManager::ACCOUNT_SEND_REJ == kind {
                    self.producer_state_getter
                        .as_ref()
                        .unwrap()
                        .online(&instance_id, &group, &topic);
                } else if BrokerStatsManager::ACCOUNT_RCV == kind
                    || BrokerStatsManager::ACCOUNT_SEND_BACK == kind
                    || BrokerStatsManager::ACCOUNT_SEND_BACK_TO_DLQ == kind
                    || BrokerStatsManager::ACCOUNT_REV_REJ == kind
                {
                    self.consumer_state_getter
                        .as_ref()
                        .unwrap()
                        .online(&instance_id, &group, &topic);
                }

                false
            }
        }

        self.account_stat_manager
            .set_statistics_item_state_getter(Arc::new(DefaultStatisticsItemStateGetter {
                producer_state_getter: self.producer_state_getter.clone(),
                consumer_state_getter: self.consumer_state_getter.clone(),
            }));
    }

    #[inline]
    pub fn set_producer_state_getter(&mut self, state_getter: Arc<dyn StateGetter>) {
        self.producer_state_getter = Some(state_getter);
    }

    #[inline]
    pub fn set_consumer_state_getter(&mut self, state_getter: Arc<dyn StateGetter>) {
        self.consumer_state_getter = Some(state_getter);
    }

    #[inline]
    pub fn get_stats_table(&self) -> Arc<DashMap<String, StatsItemSet>> {
        Arc::clone(&self.stats_table)
    }

    #[inline]
    pub fn get_cluster_name(&self) -> &str {
        &self.cluster_name
    }

    #[inline]
    pub fn get_enable_queue_stat(&self) -> bool {
        self.enable_queue_stat
    }

    #[inline]
    pub fn get_moment_stats_item_set_fall_size(&self) -> Option<Arc<MomentStatsItemSet>> {
        self.moment_stats_item_set_fall_size.clone()
    }

    #[inline]
    pub fn get_moment_stats_item_set_fall_time(&self) -> Option<Arc<MomentStatsItemSet>> {
        self.moment_stats_item_set_fall_time.clone()
    }

    #[inline]
    pub fn get_broker_puts_num_without_system_topic(&self) -> u64 {
        if let Some(stats) = self.stats_table.get(Self::BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC) {
            stats.get_stats_data_in_minute(&self.cluster_name).get_sum()
        } else {
            0
        }
    }

    #[inline]
    pub fn get_broker_gets_num_without_system_topic(&self) -> u64 {
        if let Some(stats) = self.stats_table.get(Self::BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC) {
            stats.get_stats_data_in_minute(&self.cluster_name).get_sum()
        } else {
            0
        }
    }

    #[inline]
    pub fn get_broker_put_nums(&self) -> u64 {
        if let Some(stats) = self.stats_table.get(Stats::BROKER_PUT_NUMS) {
            stats.get_stats_data_in_minute(&self.cluster_name).get_sum()
        } else {
            0
        }
    }

    #[inline]
    pub fn get_broker_get_nums(&self) -> u64 {
        if let Some(stats) = self.stats_table.get(Stats::BROKER_GET_NUMS) {
            stats.get_stats_data_in_minute(&self.cluster_name).get_sum()
        } else {
            0
        }
    }

    #[inline]
    pub fn get_stats_item(&self, stats_name: &str, stats_key: &str) -> Option<Arc<StatsItem>> {
        self.stats_table
            .get(stats_name)
            .and_then(|stats_set| stats_set.get_stats_item(stats_key))
    }

    #[inline]
    pub fn record_disk_fall_behind_size(&self, group: &str, topic: &str, queue_id: i32, fall_behind: i64) {
        if let Some(fall_size_set) = &self.moment_stats_item_set_fall_size {
            let stats_key = format!("{}@{}@{}", queue_id, topic, group);
            let item = fall_size_set.get_and_create_stats_item(stats_key);
            item.get_value()
                .store(fall_behind, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn inc_topic_put_nums(&self, topic: &str, num: i32, times: i32) {
        if let Some(stats) = self.stats_table.get(Stats::TOPIC_PUT_NUMS) {
            stats.add_value(topic, num, times);
        }
    }

    #[inline]
    pub fn inc_topic_put_size(&self, topic: &str, size: i32) {
        if let Some(stats) = self.stats_table.get(Stats::TOPIC_PUT_SIZE) {
            stats.add_value(topic, size, 1);
        }
    }

    #[inline]
    pub fn inc_group_get_nums(&self, group: &str, topic: &str, inc_value: i32) {
        let stats_key = build_stats_key(Some(topic), Some(group));
        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_NUMS) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_group_get_size(&self, group: &str, topic: &str, inc_value: i32) {
        let stats_key = build_stats_key(Some(topic), Some(group));
        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_SIZE) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_group_ck_nums(&self, group: &str, topic: &str, inc_value: i32) {
        let stats_key = build_stats_key(Some(topic), Some(group));
        if let Some(stats) = self.stats_table.get(Self::GROUP_CK_NUMS) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_group_ack_nums(&self, group: &str, topic: &str, inc_value: i32) {
        let stats_key = build_stats_key(Some(topic), Some(group));
        if let Some(stats) = self.stats_table.get(Self::GROUP_ACK_NUMS) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_group_get_from_disk_nums(&self, group: &str, topic: &str, inc_value: i32) {
        let stats_key = build_stats_key(Some(topic), Some(group));
        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_FROM_DISK_NUMS) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_group_get_from_disk_size(&self, group: &str, topic: &str, inc_value: i32) {
        let stats_key = build_stats_key(Some(topic), Some(group));
        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_FROM_DISK_SIZE) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_broker_get_nums(&self, topic: &str, inc_value: i32) {
        if let Some(stats) = self.stats_table.get(Stats::BROKER_GET_NUMS) {
            stats.add_value(&self.cluster_name, inc_value, 1);
        }

        if !TopicValidator::is_system_topic(topic) {
            if let Some(stats) = self.stats_table.get(Self::BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC) {
                stats.add_value(&self.cluster_name, inc_value, 1);
            }
        }
    }

    #[inline]
    pub fn inc_broker_put_nums(&self, topic: &str, inc_value: i32) {
        if let Some(stats) = self.stats_table.get(Stats::BROKER_PUT_NUMS) {
            stats.add_value(&self.cluster_name, inc_value, 1);
        }

        if !TopicValidator::is_system_topic(topic) {
            if let Some(stats) = self.stats_table.get(Self::BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC) {
                stats.add_value(&self.cluster_name, inc_value, 1);
            }
        }
    }

    #[inline]
    pub fn on_topic_deleted(&self, topic: &CheetahString) {
        let topic_str = topic.as_str();

        if let Some(stats) = self.stats_table.get(Stats::TOPIC_PUT_NUMS) {
            stats.del_value(topic_str);
        }
        if let Some(stats) = self.stats_table.get(Stats::TOPIC_PUT_SIZE) {
            stats.del_value(topic_str);
        }

        if self.enable_queue_stat {
            if let Some(stats) = self.stats_table.get(Stats::QUEUE_PUT_NUMS) {
                stats.del_value_by_prefix_key(topic_str, "@");
            }
            if let Some(stats) = self.stats_table.get(Stats::QUEUE_PUT_SIZE) {
                stats.del_value_by_prefix_key(topic_str, "@");
            }
            if let Some(stats) = self.stats_table.get(Stats::QUEUE_GET_NUMS) {
                stats.del_value_by_prefix_key(topic_str, "@");
            }
            if let Some(stats) = self.stats_table.get(Stats::QUEUE_GET_SIZE) {
                stats.del_value_by_prefix_key(topic_str, "@");
            }
        }

        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_NUMS) {
            stats.del_value_by_prefix_key(topic_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_SIZE) {
            stats.del_value_by_prefix_key(topic_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Self::GROUP_ACK_NUMS) {
            stats.del_value_by_prefix_key(topic_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Self::GROUP_CK_NUMS) {
            stats.del_value_by_prefix_key(topic_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Stats::SNDBCK_PUT_NUMS) {
            stats.del_value_by_prefix_key(topic_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_LATENCY) {
            stats.del_value_by_infix_key(topic_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Self::TOPIC_PUT_LATENCY) {
            stats.del_value_by_suffix_key(topic_str, "@");
        }

        if let Some(fall_size) = &self.moment_stats_item_set_fall_size {
            fall_size.del_value_by_infix_key(topic_str, "@");
        }
        if let Some(fall_time) = &self.moment_stats_item_set_fall_time {
            fall_time.del_value_by_infix_key(topic_str, "@");
        }

        info!("Deleted all stats for topic: {}", topic_str);
    }

    #[inline]
    pub fn on_group_deleted(&self, group: &CheetahString) {
        let group_str = group.as_str();

        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_NUMS) {
            stats.del_value_by_suffix_key(group_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_SIZE) {
            stats.del_value_by_suffix_key(group_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Self::GROUP_ACK_NUMS) {
            stats.del_value_by_suffix_key(group_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Self::GROUP_CK_NUMS) {
            stats.del_value_by_suffix_key(group_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Stats::SNDBCK_PUT_NUMS) {
            stats.del_value_by_suffix_key(group_str, "@");
        }
        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_LATENCY) {
            stats.del_value_by_suffix_key(group_str, "@");
        }

        if let Some(fall_size) = &self.moment_stats_item_set_fall_size {
            fall_size.del_value_by_suffix_key(group_str, "@");
        }
        if let Some(fall_time) = &self.moment_stats_item_set_fall_time {
            fall_time.del_value_by_suffix_key(group_str, "@");
        }

        info!("Deleted all stats for group: {}", group_str);
    }

    #[inline]
    pub fn inc_queue_put_nums(&self, topic: &str, queue_id: i32, num: i32, times: i32) {
        if self.enable_queue_stat {
            let stats_key = format!("{}@{}", topic, queue_id);
            if let Some(stats) = self.stats_table.get(Stats::QUEUE_PUT_NUMS) {
                stats.add_value(&stats_key, num, times);
            }
        }
    }

    #[inline]
    pub fn inc_queue_put_size(&self, topic: &str, queue_id: i32, size: i32) {
        if self.enable_queue_stat {
            let stats_key = format!("{}@{}", topic, queue_id);
            if let Some(stats) = self.stats_table.get(Stats::QUEUE_PUT_SIZE) {
                stats.add_value(&stats_key, size, 1);
            }
        }
    }

    #[inline]
    pub fn inc_queue_get_nums(&self, topic: &str, queue_id: i32, num: i32, times: i32) {
        if self.enable_queue_stat {
            let stats_key = format!("{}@{}", topic, queue_id);
            if let Some(stats) = self.stats_table.get(Stats::QUEUE_GET_NUMS) {
                stats.add_value(&stats_key, num, times);
            }
        }
    }

    #[inline]
    pub fn inc_queue_get_size(&self, topic: &str, queue_id: i32, size: i32) {
        if self.enable_queue_stat {
            let stats_key = format!("{}@{}", topic, queue_id);
            if let Some(stats) = self.stats_table.get(Stats::QUEUE_GET_SIZE) {
                stats.add_value(&stats_key, size, 1);
            }
        }
    }

    #[inline]
    pub fn inc_topic_put_latency(&self, topic: &str, queue_id: i32, inc_value: i32) {
        let stats_key = format!("{}@{}", queue_id, topic);
        if let Some(stats) = self.stats_table.get(Self::TOPIC_PUT_LATENCY) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_group_get_latency(&self, group: &str, topic: &str, queue_id: i32, inc_value: i32) {
        let stats_key = format!("{}@{}@{}", queue_id, topic, group);
        if let Some(stats) = self.stats_table.get(Stats::GROUP_GET_LATENCY) {
            stats.add_rt_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn record_disk_fall_behind_time(&self, group: &str, topic: &str, queue_id: i32, fall_behind: i64) {
        if let Some(fall_time_set) = &self.moment_stats_item_set_fall_time {
            let stats_key = format!("{}@{}@{}", queue_id, topic, group);
            let item = fall_time_set.get_and_create_stats_item(stats_key);
            item.get_value()
                .store(fall_behind, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn tps_group_get_nums(&self, group: &str, topic: &str) -> f64 {
        let stats_key = build_stats_key(Some(topic), Some(group));
        match self.stats_table.get(Stats::GROUP_GET_NUMS) {
            Some(stats) => stats.get_stats_data_in_minute(&stats_key).get_tps(),
            None => 0.0,
        }
    }

    #[inline]
    pub fn inc_broker_ack_nums(&self, inc_value: i32) {
        if let Some(stats) = self.stats_table.get(Self::BROKER_ACK_NUMS) {
            stats.add_value(&self.cluster_name, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_broker_get_from_disk_nums(&self, inc_value: i32) {
        if let Some(stats) = self.stats_table.get(Stats::BROKER_GET_FROM_DISK_NUMS) {
            stats.add_value(&self.cluster_name, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_broker_get_from_disk_size(&self, inc_value: i32) {
        if let Some(stats) = self.stats_table.get(Stats::BROKER_GET_FROM_DISK_SIZE) {
            stats.add_value(&self.cluster_name, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_send_back_nums(&self, group: &str, topic: &str) {
        let stats_key = build_stats_key(Some(topic), Some(group));
        if let Some(stats) = self.stats_table.get(Stats::SNDBCK_PUT_NUMS) {
            stats.add_value(&stats_key, 1, 1);
        }
    }

    #[inline]
    pub fn inc_dlq_stat_value(&self, key: &str, owner: &str, group: &str, topic: &str, msg_type: &str, inc_value: i32) {
        let stats_key = build_commercial_stats_key(owner, topic, group, msg_type);
        if let Some(stats) = self.stats_table.get(key) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_commercial_value(
        &self,
        key: &str,
        owner: &str,
        group: &str,
        topic: &str,
        msg_type: &str,
        inc_value: i32,
    ) {
        let stats_key = build_commercial_stats_key(owner, topic, group, msg_type);
        if let Some(stats) = self.stats_table.get(key) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_account_value(
        &self,
        key: &str,
        account_owner_parent: &str,
        account_owner_self: &str,
        instance_id: &str,
        topic: &str,
        group: &str,
        msg_type: &str,
        inc_value: i32,
    ) {
        let stats_key = build_account_stats_key(
            account_owner_parent,
            account_owner_self,
            instance_id,
            topic,
            group,
            msg_type,
        );
        if let Some(stats) = self.stats_table.get(key) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    #[inline]
    pub fn inc_account_value_with_flow_limit(
        &self,
        key: &str,
        account_owner_parent: &str,
        account_owner_self: &str,
        instance_id: &str,
        topic: &str,
        group: &str,
        msg_type: &str,
        flow_limit_threshold: &str,
        inc_value: i32,
    ) {
        let stats_key = build_account_stats_key_with_flowlimit(
            account_owner_parent,
            account_owner_self,
            instance_id,
            topic,
            group,
            msg_type,
            flow_limit_threshold,
        );
        if let Some(stats) = self.stats_table.get(key) {
            stats.add_value(&stats_key, inc_value, 1);
        }
    }

    pub fn shutdown(&self) {
        info!("Shutting down BrokerStatsManager...");

        if let Some(scheduler) = &self.scheduler {
            for task_id in self.task_ids.lock().drain(..) {
                scheduler.cancel_task(task_id);
                info!("Cancelled task {}", task_id);
            }
        }

        info!("BrokerStatsManager shutdown complete");
    }

    pub fn inc_consumer_register_time(&self, inc_value: i32) {
        if let Some(stats) = self.stats_table.get(Self::CONSUMER_REGISTER_TIME) {
            stats.add_value(&self.cluster_name, inc_value, 1);
        }
    }

    pub fn inc_producer_register_time(&self, inc_value: i32) {
        if let Some(stats) = self.stats_table.get(Self::PRODUCER_REGISTER_TIME) {
            stats.add_value(&self.cluster_name, inc_value, 1);
        }
    }

    pub fn inc_channel_idle_num(&self) {
        if let Some(stats) = self.stats_table.get(Self::CHANNEL_ACTIVITY) {
            stats.add_value(Self::CHANNEL_ACTIVITY_IDLE, 1, 1);
        }
    }

    pub fn inc_channel_exception_num(&self) {
        if let Some(stats) = self.stats_table.get(Self::CHANNEL_ACTIVITY) {
            stats.add_value(Self::CHANNEL_ACTIVITY_EXCEPTION, 1, 1);
        }
    }

    pub fn inc_channel_close_num(&self) {
        if let Some(stats) = self.stats_table.get(Self::CHANNEL_ACTIVITY) {
            stats.add_value(Self::CHANNEL_ACTIVITY_CLOSE, 1, 1);
        }
    }

    pub fn inc_channel_connect_num(&self) {
        if let Some(stats) = self.stats_table.get(Self::CHANNEL_ACTIVITY) {
            stats.add_value(Self::CHANNEL_ACTIVITY_CONNECT, 1, 1);
        }
    }
}

#[inline]
pub fn build_stats_key(topic: Option<&str>, group: Option<&str>) -> String {
    let mut str_builder = String::new();
    if let Some(t) = topic {
        str_builder.push_str(t);
    }
    str_builder.push('@');
    if let Some(g) = group {
        str_builder.push_str(g);
    }
    str_builder
}

#[inline]
pub fn create_statistics_kind_meta(
    name: &str,
    item_names: Vec<&str>,
    formatter: &StatisticsItemFormatter,
    interval: u64,
    broker_config: &Arc<BrokerConfig>,
) -> Arc<StatisticsKindMeta> {
    let printer = StatisticsItemPrinter::new(formatter);
    let scheduled_printer = StatisticsItemScheduledPrinter;
    let kind_meta = StatisticsKindMeta::new(
        name.to_string(),
        item_names.into_iter().map(String::from).collect(),
        scheduled_printer,
    );
    Arc::new(kind_meta)
}

#[inline]
pub fn build_commercial_stats_key(owner: &str, topic: &str, group: &str, type_: &str) -> String {
    format!("{owner}@{topic}@{group}@{type_}")
}

#[inline]
pub fn build_account_stats_key(
    account_owner_parent: &str,
    account_owner_self: &str,
    instance_id: &str,
    topic: &str,
    group: &str,
    msg_type: &str,
) -> String {
    format!("{account_owner_parent}@{account_owner_self}@{instance_id}@{topic}@{group}@{msg_type}")
}

#[inline]
pub fn build_account_stats_key_with_flowlimit(
    account_owner_parent: &str,
    account_owner_self: &str,
    instance_id: &str,
    topic: &str,
    group: &str,
    msg_type: &str,
    flow_limit_threshold: &str,
) -> String {
    format!(
        "{account_owner_parent}@{account_owner_self}@{instance_id}@{topic}@{group}@{msg_type}@{flow_limit_threshold}"
    )
}

#[inline]
pub fn build_account_stat_key(owner: &str, instance_id: &str, topic: &str, group: &str, msg_type: &str) -> String {
    format!("{owner}|{instance_id}|{topic}|{group}|{msg_type}")
}

#[inline]
pub fn build_account_stat_key_with_flowlimit(
    owner: &str,
    instance_id: &str,
    topic: &str,
    group: &str,
    msg_type: &str,
    flow_limit_threshold: &str,
) -> String {
    format!("{owner}|{instance_id}|{topic}|{group}|{msg_type}|{flow_limit_threshold}")
}

#[inline]
pub fn split_account_stat_key(account_stat_key: &str) -> Vec<&str> {
    account_stat_key.split('|').collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_commercial_stats_key_creates_correct_key() {
        let key = build_commercial_stats_key("owner1", "topic1", "group1", "type1");
        assert_eq!(key, "owner1@topic1@group1@type1");
    }

    #[test]
    fn build_account_stats_key_creates_correct_key() {
        let key = build_account_stats_key("parent1", "self1", "id1", "topic1", "group1", "type1");
        assert_eq!(key, "parent1@self1@id1@topic1@group1@type1");
    }

    #[test]
    fn build_account_stats_key_with_flowlimit_creates_correct_key() {
        let key =
            build_account_stats_key_with_flowlimit("parent1", "self1", "id1", "topic1", "group1", "type1", "limit1");
        assert_eq!(key, "parent1@self1@id1@topic1@group1@type1@limit1");
    }

    #[test]
    fn build_account_stat_key_creates_correct_key() {
        let key = build_account_stat_key("owner1", "id1", "topic1", "group1", "type1");
        assert_eq!(key, "owner1|id1|topic1|group1|type1");
    }

    #[test]
    fn build_account_stat_key_with_flowlimit_creates_correct_key() {
        let key = build_account_stat_key_with_flowlimit("owner1", "id1", "topic1", "group1", "type1", "limit1");
        assert_eq!(key, "owner1|id1|topic1|group1|type1|limit1");
    }

    #[test]
    fn split_account_stat_key_splits_correctly() {
        let parts = split_account_stat_key("part1|part2|part3|part4|part5");
        assert_eq!(parts, vec!["part1", "part2", "part3", "part4", "part5"]);
    }

    #[tokio::test]
    async fn test_broker_stats_manager_initialization() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        assert_eq!(manager.get_cluster_name(), "DefaultCluster");
        assert!(!manager.get_stats_table().is_empty());
    }

    #[tokio::test]
    async fn test_inc_topic_put_nums() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_topic_put_nums("TestTopic", 100, 1);
        manager.inc_topic_put_nums("TestTopic", 200, 1);

        if let Some(stats) = manager.stats_table.get(Stats::TOPIC_PUT_NUMS) {
            if let Some(item) = stats.get_stats_item("TestTopic") {
                assert_eq!(item.get_value(), 300);
            } else {
                panic!("TestTopic stats item not found");
            }
        } else {
            panic!("TOPIC_PUT_NUMS stats not found");
        };
    }

    #[tokio::test]
    async fn test_inc_group_get_nums() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_group_get_nums("TestGroup", "TestTopic", 50);
        manager.inc_group_get_nums("TestGroup", "TestTopic", 30);

        let stats_key = build_stats_key(Some("TestTopic"), Some("TestGroup"));
        if let Some(stats) = manager.stats_table.get(Stats::GROUP_GET_NUMS) {
            if let Some(item) = stats.get_stats_item(&stats_key) {
                assert_eq!(item.get_value(), 80);
            } else {
                panic!("Stats item not found");
            }
        } else {
            panic!("GROUP_GET_NUMS stats not found");
        };
    }

    #[tokio::test]
    async fn test_inc_broker_put_nums_excludes_system_topic() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_broker_put_nums("UserTopic", 100);
        manager.inc_broker_put_nums("SCHEDULE_TOPIC_XXXX", 50);

        if let Some(stats) = manager
            .stats_table
            .get(BrokerStatsManager::BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC)
        {
            if let Some(item) = stats.get_stats_item(&manager.cluster_name) {
                assert_eq!(item.get_value(), 100);
            }
        };
    }

    #[tokio::test]
    async fn test_inc_send_back_nums() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_send_back_nums("TestGroup", "TestTopic");
        manager.inc_send_back_nums("TestGroup", "TestTopic");

        let stats_key = build_stats_key(Some("TestTopic"), Some("TestGroup"));
        if let Some(stats) = manager.stats_table.get(Stats::SNDBCK_PUT_NUMS) {
            if let Some(item) = stats.get_stats_item(&stats_key) {
                assert_eq!(item.get_value(), 2);
            }
        };
    }

    #[tokio::test]
    async fn test_inc_commercial_value() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_commercial_value(
            BrokerStatsManager::COMMERCIAL_MSG_NUM,
            "owner1",
            "group1",
            "topic1",
            "SEND",
            1000,
        );

        let stats_key = build_commercial_stats_key("owner1", "topic1", "group1", "SEND");
        if let Some(stats) = manager.stats_table.get(BrokerStatsManager::COMMERCIAL_MSG_NUM) {
            let snapshot = stats.get_stats_data_in_minute(&stats_key);
            assert_eq!(snapshot.get_sum(), 1000);
        };
    }

    #[tokio::test]
    async fn test_inc_disk_stats() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_group_get_from_disk_nums("TestGroup", "TestTopic", 10);
        manager.inc_group_get_from_disk_size("TestGroup", "TestTopic", 1024);

        let stats_key = build_stats_key(Some("TestTopic"), Some("TestGroup"));

        if let Some(stats) = manager.stats_table.get(Stats::GROUP_GET_FROM_DISK_NUMS) {
            if let Some(item) = stats.get_stats_item(&stats_key) {
                assert_eq!(item.get_value(), 10);
            }
        }

        if let Some(stats) = manager.stats_table.get(Stats::GROUP_GET_FROM_DISK_SIZE) {
            if let Some(item) = stats.get_stats_item(&stats_key) {
                assert_eq!(item.get_value(), 1024);
            }
        };
    }

    #[tokio::test]
    async fn test_query_interfaces() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_broker_put_nums("UserTopic", 500);
        manager.inc_broker_get_nums("UserTopic", 300);

        if let Some(stats) = manager.stats_table.get(Stats::BROKER_PUT_NUMS) {
            if let Some(item) = stats.get_stats_item(&manager.cluster_name) {
                assert_eq!(item.get_value(), 500);
            }
        }

        if let Some(stats) = manager.stats_table.get(Stats::BROKER_GET_NUMS) {
            if let Some(item) = stats.get_stats_item(&manager.cluster_name) {
                assert_eq!(item.get_value(), 300);
            }
        };
    }

    #[tokio::test]
    async fn test_get_stats_item() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_topic_put_nums("TestTopic", 100, 1);

        let item = manager.get_stats_item(Stats::TOPIC_PUT_NUMS, "TestTopic");
        assert!(item.is_some());

        let item = manager.get_stats_item(Stats::TOPIC_PUT_NUMS, "NonExistentTopic");
        assert!(item.is_none());
    }

    #[tokio::test]
    async fn test_inc_producer_consumer_register_time() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_producer_register_time(100);
        manager.inc_consumer_register_time(200);

        if let Some(stats) = manager.stats_table.get(BrokerStatsManager::PRODUCER_REGISTER_TIME) {
            if let Some(item) = stats.get_stats_item(&manager.cluster_name) {
                assert_eq!(item.get_value(), 100);
            }
        }

        if let Some(stats) = manager.stats_table.get(BrokerStatsManager::CONSUMER_REGISTER_TIME) {
            if let Some(item) = stats.get_stats_item(&manager.cluster_name) {
                assert_eq!(item.get_value(), 200);
            }
        };
    }
    #[tokio::test]
    async fn test_concurrent_increments() {
        use std::thread;

        let broker_config = Arc::new(BrokerConfig::default());
        let manager = Arc::new(BrokerStatsManager::new(broker_config));

        let mut handles = vec![];

        for _i in 0..10 {
            let manager_clone = Arc::clone(&manager);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    manager_clone.inc_topic_put_nums("ConcurrentTopic", 1, 1);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        if let Some(stats) = manager.stats_table.get(Stats::TOPIC_PUT_NUMS) {
            if let Some(item) = stats.get_stats_item("ConcurrentTopic") {
                assert_eq!(item.get_value(), 1000);
            }
        };
    }

    #[tokio::test]
    async fn test_concurrent_different_topics() {
        use std::thread;

        let broker_config = Arc::new(BrokerConfig::default());
        let manager = Arc::new(BrokerStatsManager::new(broker_config));

        let mut handles = vec![];

        for i in 0..5 {
            let manager_clone = Arc::clone(&manager);
            let topic = format!("Topic{}", i);
            let handle = thread::spawn(move || {
                for _ in 0..50 {
                    manager_clone.inc_topic_put_nums(&topic, 2, 1);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..5 {
            let topic = format!("Topic{}", i);
            if let Some(stats) = manager.stats_table.get(Stats::TOPIC_PUT_NUMS) {
                if let Some(item) = stats.get_stats_item(&topic) {
                    assert_eq!(item.get_value(), 100);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_start_with_scheduler() {
        let broker_config = Arc::new(BrokerConfig::default());
        let scheduler = Arc::new(ScheduledTaskManager::new());
        let manager = BrokerStatsManager::new_with_scheduler(broker_config, Some(scheduler));

        manager.start();

        assert_eq!(manager.task_ids.lock().len(), 3);
    }

    #[tokio::test]
    async fn test_shutdown_cancels_tasks() {
        let broker_config = Arc::new(BrokerConfig::default());
        let scheduler = Arc::new(ScheduledTaskManager::new());
        let manager = BrokerStatsManager::new_with_scheduler(broker_config, Some(scheduler));

        manager.start();
        let task_count_before = manager.task_ids.lock().len();
        assert_eq!(task_count_before, 3);

        manager.shutdown();
        let task_count_after = manager.task_ids.lock().len();
        assert_eq!(task_count_after, 0);
    }

    #[tokio::test]
    async fn test_on_topic_deleted() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_topic_put_nums("DeletedTopic", 100, 1);
        manager.inc_topic_put_size("DeletedTopic", 1024);
        manager.inc_group_get_nums("TestGroup", "DeletedTopic", 50);

        assert!(manager.get_stats_item(Stats::TOPIC_PUT_NUMS, "DeletedTopic").is_some());

        manager.on_topic_deleted(&CheetahString::from("DeletedTopic"));

        assert!(manager.get_stats_item(Stats::TOPIC_PUT_NUMS, "DeletedTopic").is_none());
    }

    #[tokio::test]
    async fn test_on_group_deleted() {
        let broker_config = Arc::new(BrokerConfig::default());
        let manager = BrokerStatsManager::new(broker_config);

        manager.inc_group_get_nums("DeletedGroup", "TestTopic", 50);
        manager.inc_group_get_size("DeletedGroup", "TestTopic", 1024);

        let stats_key = build_stats_key(Some("TestTopic"), Some("DeletedGroup"));
        assert!(manager.get_stats_item(Stats::GROUP_GET_NUMS, &stats_key).is_some());

        manager.on_group_deleted(&CheetahString::from("DeletedGroup"));

        assert!(manager.get_stats_item(Stats::GROUP_GET_NUMS, &stats_key).is_none());
    }
}
