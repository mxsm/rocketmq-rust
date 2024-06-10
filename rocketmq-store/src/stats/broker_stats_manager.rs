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
use std::collections::HashMap;
use std::sync::Arc;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::statistics::state_getter::StateGetter;
use rocketmq_common::common::statistics::statistics_item_formatter::StatisticsItemFormatter;
use rocketmq_common::common::stats::moment_stats_item_set::MomentStatsItemSet;
use rocketmq_common::common::stats::stats_item_set::StatsItemSet;
use rocketmq_common::common::stats::Stats;

pub struct BrokerStatsManager {
    stats_table: Arc<parking_lot::RwLock<HashMap<String, StatsItemSet>>>,
    cluster_name: String,
    enable_queue_stat: bool,
    moment_stats_item_set_fall_size: Option<Arc<MomentStatsItemSet>>,
    moment_stats_item_set_fall_time: Option<Arc<MomentStatsItemSet>>,
    producer_state_getter: Option<Arc<Box<dyn StateGetter>>>,
    consumer_state_getter: Option<Arc<Box<dyn StateGetter>>>,
    broker_config: Option<Arc<BrokerConfig>>,
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
    pub const BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC: &'static str =
        "BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC";
    pub const BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC: &'static str =
        "BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC";
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
    pub fn new(broker_config: Arc<BrokerConfig>) -> Self {
        let stats_table = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let enable_queue_stat = broker_config.enable_detail_stat;
        let cluster_name = broker_config
            .broker_identity
            .broker_cluster_name
            .to_string();
        BrokerStatsManager {
            stats_table,
            cluster_name,
            enable_queue_stat,
            moment_stats_item_set_fall_size: None,
            moment_stats_item_set_fall_time: None,
            producer_state_getter: None,
            consumer_state_getter: None,
            broker_config: None,
        }
    }

    pub fn new_with_name(cluster_name: String, enable_queue_stat: bool) -> Self {
        let stats_table = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let moment_stats_item_set_fall_size =
            MomentStatsItemSet::new(Stats::GROUP_GET_FALL_SIZE.to_string());
        let moment_stats_item_set_fall_time =
            MomentStatsItemSet::new(Stats::GROUP_GET_FALL_TIME.to_string());
        BrokerStatsManager {
            stats_table,
            cluster_name,
            enable_queue_stat,
            moment_stats_item_set_fall_size: None,
            moment_stats_item_set_fall_time: None,
            producer_state_getter: None,
            consumer_state_getter: None,
            broker_config: None,
        }
    }

    pub fn init(&mut self) {
        self.moment_stats_item_set_fall_size = Some(Arc::new(MomentStatsItemSet::new(
            Stats::GROUP_GET_FALL_SIZE.to_string(),
        )));

        self.moment_stats_item_set_fall_time = Some(Arc::new(MomentStatsItemSet::new(
            Stats::GROUP_GET_FALL_TIME.to_string(),
        )));

        let enable_queue_stat = true; // replace with actual condition

        if enable_queue_stat {
            self.stats_table.write().insert(
                Stats::QUEUE_PUT_NUMS.to_string(),
                StatsItemSet::new(Stats::QUEUE_PUT_NUMS.to_string()),
            );
            self.stats_table.write().insert(
                Stats::QUEUE_PUT_SIZE.to_string(),
                StatsItemSet::new(Stats::QUEUE_PUT_SIZE.to_string()),
            );
            self.stats_table.write().insert(
                Stats::QUEUE_GET_NUMS.to_string(),
                StatsItemSet::new(Stats::QUEUE_GET_NUMS.to_string()),
            );
            self.stats_table.write().insert(
                Stats::QUEUE_GET_SIZE.to_string(),
                StatsItemSet::new(Stats::QUEUE_GET_SIZE.to_string()),
            );
        }

        self.stats_table.write().insert(
            Stats::TOPIC_PUT_NUMS.to_string(),
            StatsItemSet::new(Stats::TOPIC_PUT_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Stats::TOPIC_PUT_SIZE.to_string(),
            StatsItemSet::new(Stats::TOPIC_PUT_SIZE.to_string()),
        );
        self.stats_table.write().insert(
            Stats::GROUP_GET_NUMS.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Stats::GROUP_GET_SIZE.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_SIZE.to_string()),
        );
        self.stats_table.write().insert(
            Self::GROUP_ACK_NUMS.to_string(),
            StatsItemSet::new(Self::GROUP_ACK_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Self::GROUP_CK_NUMS.to_string(),
            StatsItemSet::new(Self::GROUP_CK_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Stats::GROUP_GET_LATENCY.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_LATENCY.to_string()),
        );
        self.stats_table.write().insert(
            Self::TOPIC_PUT_LATENCY.to_string(),
            StatsItemSet::new(Self::TOPIC_PUT_LATENCY.to_string()),
        );
        self.stats_table.write().insert(
            Stats::SNDBCK_PUT_NUMS.to_string(),
            StatsItemSet::new(Stats::SNDBCK_PUT_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Self::DLQ_PUT_NUMS.to_string(),
            StatsItemSet::new(Self::DLQ_PUT_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Stats::BROKER_PUT_NUMS.to_string(),
            StatsItemSet::new(Stats::BROKER_PUT_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Stats::BROKER_GET_NUMS.to_string(),
            StatsItemSet::new(Stats::BROKER_GET_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Self::BROKER_ACK_NUMS.to_string(),
            StatsItemSet::new(Self::BROKER_ACK_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Self::BROKER_CK_NUMS.to_string(),
            StatsItemSet::new(Self::BROKER_CK_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Self::BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC.to_string(),
            StatsItemSet::new(Self::BROKER_GET_NUMS_WITHOUT_SYSTEM_TOPIC.to_string()),
        );
        self.stats_table.write().insert(
            Self::BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC.to_string(),
            StatsItemSet::new(Self::BROKER_PUT_NUMS_WITHOUT_SYSTEM_TOPIC.to_string()),
        );
        self.stats_table.write().insert(
            Stats::GROUP_GET_FROM_DISK_NUMS.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_FROM_DISK_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Stats::GROUP_GET_FROM_DISK_SIZE.to_string(),
            StatsItemSet::new(Stats::GROUP_GET_FROM_DISK_SIZE.to_string()),
        );
        self.stats_table.write().insert(
            Stats::BROKER_GET_FROM_DISK_NUMS.to_string(),
            StatsItemSet::new(Stats::BROKER_GET_FROM_DISK_NUMS.to_string()),
        );
        self.stats_table.write().insert(
            Stats::BROKER_GET_FROM_DISK_SIZE.to_string(),
            StatsItemSet::new(Stats::BROKER_GET_FROM_DISK_SIZE.to_string()),
        );
        self.stats_table.write().insert(
            Self::SNDBCK2DLQ_TIMES.to_string(),
            StatsItemSet::new(Self::SNDBCK2DLQ_TIMES.to_string()),
        );
        self.stats_table.write().insert(
            Stats::COMMERCIAL_SEND_TIMES.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_SEND_TIMES.to_string()),
        );
        self.stats_table.write().insert(
            Stats::COMMERCIAL_RCV_TIMES.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_RCV_TIMES.to_string()),
        );
        self.stats_table.write().insert(
            Stats::COMMERCIAL_SEND_SIZE.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_SEND_SIZE.to_string()),
        );
        self.stats_table.write().insert(
            Stats::COMMERCIAL_RCV_SIZE.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_RCV_SIZE.to_string()),
        );
        self.stats_table.write().insert(
            Stats::COMMERCIAL_RCV_EPOLLS.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_RCV_EPOLLS.to_string()),
        );
        self.stats_table.write().insert(
            Stats::COMMERCIAL_SNDBCK_TIMES.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_SNDBCK_TIMES.to_string()),
        );
        self.stats_table.write().insert(
            Stats::COMMERCIAL_PERM_FAILURES.to_string(),
            StatsItemSet::new(Stats::COMMERCIAL_PERM_FAILURES.to_string()),
        );
        self.stats_table.write().insert(
            Self::CONSUMER_REGISTER_TIME.to_string(),
            StatsItemSet::new(Self::CONSUMER_REGISTER_TIME.to_string()),
        );
        self.stats_table.write().insert(
            Self::PRODUCER_REGISTER_TIME.to_string(),
            StatsItemSet::new(Self::PRODUCER_REGISTER_TIME.to_string()),
        );
        self.stats_table.write().insert(
            Self::CHANNEL_ACTIVITY.to_string(),
            StatsItemSet::new(Self::CHANNEL_ACTIVITY.to_string()),
        );

        let formatter = StatisticsItemFormatter;

        /*        self.account_stat_manager.set_brief_meta(vec![
            Pair::of("RT.to_string(), vec![[50, 50], [100, 10], [1000, 10]]),
            Pair::of("INNER_RT", vec![[10, 10], [100, 10], [1000, 10]]),
        ]);*/

        /*        let _item_names = vec![
            "MSG_NUM",
            "SUCCESS_MSG_NUM",
            "FAILURE_MSG_NUM",
            "COMMERCIAL_MSG_NUM",
            "SUCCESS_REQ_NUM",
            "FAILURE_REQ_NUM",
            "MSG_SIZE",
            "SUCCESS_MSG_SIZE",
            "FAILURE_MSG_SIZE",
            "RT",
            "INNER_RT",
        ];*/

        /*self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                "ACCOUNT_SEND",
                item_names.clone(),
                self.account_executor.clone(),
                formatter,
                "ACCOUNT_LOG",
                "ACCOUNT_STAT_INVERTAL",
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                "ACCOUNT_RCV",
                item_names.clone(),
                self.account_executor.clone(),
                formatter,
                "ACCOUNT_LOG",
                "ACCOUNT_STAT_INVERTAL",
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                "ACCOUNT_SEND_BACK",
                item_names.clone(),
                self.account_executor.clone(),
                formatter,
                "ACCOUNT_LOG",
                "ACCOUNT_STAT_INVERTAL",
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                "ACCOUNT_SEND_BACK_TO_DLQ",
                item_names.clone(),
                self.account_executor.clone(),
                formatter,
                "ACCOUNT_LOG",
                "ACCOUNT_STAT_INVERTAL",
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                "ACCOUNT_SEND_REJ",
                item_names.clone(),
                self.account_executor.clone(),
                formatter,
                "ACCOUNT_LOG",
                "ACCOUNT_STAT_INVERTAL",
            ));
        self.account_stat_manager
            .add_statistics_kind_meta(create_statistics_kind_meta(
                "ACCOUNT_REV_REJ",
                item_names.clone(),
                self.account_executor.clone(),
                formatter,
                "ACCOUNT_LOG",
                "ACCOUNT_STAT_INVERTAL",
            ));

        let state_getter = Box::new(StatisticsItemStateGetter);
        self.account_stat_manager
            .set_statistics_item_state_getter(state_getter);*/
    }

    pub fn get_stats_table(&self) -> Arc<parking_lot::RwLock<HashMap<String, StatsItemSet>>> {
        Arc::clone(&self.stats_table)
    }

    pub fn get_cluster_name(&self) -> &str {
        &self.cluster_name
    }

    pub fn get_enable_queue_stat(&self) -> bool {
        self.enable_queue_stat
    }

    pub fn get_moment_stats_item_set_fall_size(&self) -> Option<Arc<MomentStatsItemSet>> {
        self.moment_stats_item_set_fall_size.clone()
    }

    pub fn get_moment_stats_item_set_fall_time(&self) -> Option<Arc<MomentStatsItemSet>> {
        self.moment_stats_item_set_fall_time.clone()
    }

    pub fn get_broker_puts_num_without_system_topic(&self) -> u64 {
        0
    }

    pub fn get_broker_gets_num_without_system_topic(&self) -> u64 {
        0
    }
}

pub fn build_commercial_stats_key(owner: &str, topic: &str, group: &str, type_: &str) -> String {
    format!("{}@{}@{}@{}", owner, topic, group, type_)
}

pub fn build_account_stats_key(
    account_owner_parent: &str,
    account_owner_self: &str,
    instance_id: &str,
    topic: &str,
    group: &str,
    msg_type: &str,
) -> String {
    format!(
        "{}@{}@{}@{}@{}@{}",
        account_owner_parent, account_owner_self, instance_id, topic, group, msg_type
    )
}

pub fn build_account_stats_key_with_flowlimit(
    account_owner_parent: &str,
    account_owner_self: &str,
    instance_id: &str,
    topic: &str,
    group: &str,
    msg_type: &str,
    flowlimit_threshold: &str,
) -> String {
    format!(
        "{}@{}@{}@{}@{}@{}@{}",
        account_owner_parent,
        account_owner_self,
        instance_id,
        topic,
        group,
        msg_type,
        flowlimit_threshold
    )
}

pub fn build_account_stat_key(
    owner: &str,
    instance_id: &str,
    topic: &str,
    group: &str,
    msg_type: &str,
) -> String {
    format!("{}|{}|{}|{}|{}", owner, instance_id, topic, group, msg_type)
}

pub fn build_account_stat_key_with_flowlimit(
    owner: &str,
    instance_id: &str,
    topic: &str,
    group: &str,
    msg_type: &str,
    flowlimit_threshold: &str,
) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        owner, instance_id, topic, group, msg_type, flowlimit_threshold
    )
}

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
        let key = build_account_stats_key_with_flowlimit(
            "parent1", "self1", "id1", "topic1", "group1", "type1", "limit1",
        );
        assert_eq!(key, "parent1@self1@id1@topic1@group1@type1@limit1");
    }

    #[test]
    fn build_account_stat_key_creates_correct_key() {
        let key = build_account_stat_key("owner1", "id1", "topic1", "group1", "type1");
        assert_eq!(key, "owner1|id1|topic1|group1|type1");
    }

    #[test]
    fn build_account_stat_key_with_flowlimit_creates_correct_key() {
        let key = build_account_stat_key_with_flowlimit(
            "owner1", "id1", "topic1", "group1", "type1", "limit1",
        );
        assert_eq!(key, "owner1|id1|topic1|group1|type1|limit1");
    }

    #[test]
    fn split_account_stat_key_splits_correctly() {
        let parts = split_account_stat_key("part1|part2|part3|part4|part5");
        assert_eq!(parts, vec!["part1", "part2", "part3", "part4", "part5"]);
    }
}
