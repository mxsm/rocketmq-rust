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

use std::any::Any;
use std::collections::HashMap;

use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;

use crate::common::constant::PermName;
use crate::common::mix_all;
use crate::common::mix_all::NAMESRV_ADDR_PROPERTY;
use crate::common::server::config::ServerConfig;
use crate::common::topic::TopicValidator;

const DEFAULT_CLUSTER_NAME: &str = "DefaultCluster";

lazy_static! {
    pub static ref LOCAL_HOST_NAME: Option<String> = match hostname::get() {
        Ok(hostname) => {
            Some(hostname.to_string_lossy().to_string())
        }
        Err(_) => {
            None
        }
    };
    pub static ref NAMESRV_ADDR: Option<String> =
        std::env::var(NAMESRV_ADDR_PROPERTY).map_or(Some("127.0.0.1:9876".to_string()), Some);
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BrokerIdentity {
    pub broker_name: String,
    pub broker_cluster_name: String,
    pub broker_id: u64,
    pub is_broker_container: bool,
    pub is_in_broker_container: bool,
}

impl BrokerIdentity {
    pub fn new() -> Self {
        let broker_name = default_broker_name();
        let broker_cluster_name = String::from(DEFAULT_CLUSTER_NAME);
        let broker_id = mix_all::MASTER_ID;
        let is_broker_container = false;

        BrokerIdentity {
            broker_name,
            broker_cluster_name,
            broker_id,
            is_broker_container,
            is_in_broker_container: false,
        }
    }

    fn new_with_container(is_broker_container: bool) -> Self {
        let mut identity = BrokerIdentity::new();
        identity.is_broker_container = is_broker_container;
        identity
    }

    fn new_with_params(broker_cluster_name: String, broker_name: String, broker_id: u64) -> Self {
        BrokerIdentity {
            broker_name,
            broker_cluster_name,
            broker_id,
            is_broker_container: false,
            is_in_broker_container: false,
        }
    }

    fn new_with_container_params(
        broker_cluster_name: String,
        broker_name: String,
        broker_id: u64,
        is_in_broker_container: bool,
    ) -> Self {
        BrokerIdentity {
            broker_name,
            broker_cluster_name,
            broker_id,
            is_broker_container: true,
            is_in_broker_container,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfig {
    pub broker_identity: BrokerIdentity,

    pub topic_queue_config: TopicQueueConfig,

    pub timer_wheel_config: TimerWheelConfig,

    pub broker_server_config: ServerConfig,

    pub broker_ip1: String,
    pub broker_ip2: Option<String>,
    pub listen_port: u32,
    pub trace_topic_enable: bool,
    pub msg_trace_topic_name: String,
    pub enable_controller_mode: bool,
    pub broker_name: String,
    pub region_id: String,
    pub trace_on: bool,
    pub broker_permission: u32,
    pub async_send_enable: bool,
    pub store_path_root_dir: String,
    pub enable_split_registration: bool,
    pub split_registration_size: i32,
    pub register_broker_timeout_mills: i32,
    pub is_in_broker_container: bool,
    pub commercial_size_per_msg: i32,
    pub recover_concurrently: bool,
    pub duplication_enable: bool,
    pub start_accept_send_request_time_stamp: i64,
    pub auto_create_topic_enable: bool,
    pub enable_single_topic_register: bool,
    pub broker_topic_enable: bool,
    pub cluster_topic_enable: bool,
    pub revive_queue_num: u32,
    pub enable_slave_acting_master: bool,
    pub reject_transaction_message: bool,
    pub enable_detail_stat: bool,
    pub flush_consumer_offset_interval: u64,
    pub force_register: bool,
    pub register_name_server_period: u64,
    pub skip_pre_online: bool,
    pub namesrv_addr: Option<String>,
    pub fetch_name_srv_addr_by_dns_lookup: bool,
    pub lite_pull_message_enable: bool,
    pub auto_create_subscription_group: bool,
    pub channel_expired_timeout: u64,
    pub subscription_expired_timeout: u64,
    pub enable_property_filter: bool,
    pub filter_support_retry: bool,
    pub use_server_side_reset_offset: bool,
    pub slave_read_enable: bool,
    pub commercial_base_count: i32,
    pub reject_pull_consumer_enable: bool,
    pub consumer_offset_update_version_step: i64,
    pub enable_broadcast_offset_store: bool,
    pub transfer_msg_by_heap: bool,
    pub short_polling_time_mills: u64,
    pub long_polling_enable: bool,
    pub max_error_rate_of_bloom_filter: i32,
    pub expect_consumer_num_use_filter: i32,
    pub bit_map_length_consume_queue_ext: i32,
    pub validate_system_topic_when_update_topic: bool,
    pub enable_mixed_message_type: bool,
    pub auto_delete_unused_stats: bool,
    pub forward_timeout: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        let broker_identity = BrokerIdentity::new();
        let local_ip = local_ip_address::local_ip().unwrap();
        let broker_ip1 = local_ip.to_string();
        let broker_ip2 = Some(local_ip.to_string());
        let listen_port = 10911;

        BrokerConfig {
            broker_identity,
            topic_queue_config: TopicQueueConfig::default(),
            timer_wheel_config: TimerWheelConfig::default(),
            broker_server_config: Default::default(),
            broker_ip1,
            broker_ip2,
            listen_port,
            trace_topic_enable: false,
            msg_trace_topic_name: TopicValidator::RMQ_SYS_TRACE_TOPIC.to_string(),
            enable_controller_mode: false,
            broker_name: default_broker_name(),
            region_id: mix_all::DEFAULT_TRACE_REGION_ID.to_string(),
            trace_on: true,
            broker_permission: PermName::PERM_WRITE | PermName::PERM_READ,
            async_send_enable: false,
            store_path_root_dir: dirs::home_dir()
                .unwrap()
                .join("store")
                .to_string_lossy()
                .into_owned(),
            enable_split_registration: false,
            split_registration_size: 800,
            register_broker_timeout_mills: 24000,
            is_in_broker_container: false,
            commercial_size_per_msg: 4 * 1024,
            recover_concurrently: false,
            duplication_enable: false,
            start_accept_send_request_time_stamp: 0,
            auto_create_topic_enable: true,
            enable_single_topic_register: true,
            broker_topic_enable: true,
            cluster_topic_enable: true,
            revive_queue_num: 8,
            enable_slave_acting_master: false,
            reject_transaction_message: false,
            enable_detail_stat: true,
            flush_consumer_offset_interval: 1000 * 5,
            force_register: true,
            register_name_server_period: 1000 * 30,
            skip_pre_online: false,
            namesrv_addr: NAMESRV_ADDR.clone(),
            fetch_name_srv_addr_by_dns_lookup: false,
            lite_pull_message_enable: true,
            auto_create_subscription_group: true,
            channel_expired_timeout: 1000 * 120,
            subscription_expired_timeout: 1000 * 60 * 10,
            enable_property_filter: false,
            filter_support_retry: false,
            use_server_side_reset_offset: true,
            slave_read_enable: false,
            commercial_base_count: 1,
            reject_pull_consumer_enable: false,
            consumer_offset_update_version_step: 500,
            enable_broadcast_offset_store: true,
            transfer_msg_by_heap: true,
            short_polling_time_mills: 1000,
            long_polling_enable: true,
            max_error_rate_of_bloom_filter: 20,
            expect_consumer_num_use_filter: 32,
            bit_map_length_consume_queue_ext: 64,
            forward_timeout: 3 * 1000,
            validate_system_topic_when_update_topic: true,
            enable_mixed_message_type: false,
            auto_delete_unused_stats: false,
        }
    }
}

impl BrokerConfig {
    pub fn broker_name(&self) -> String {
        self.broker_name.clone()
    }

    pub fn broker_ip1(&self) -> String {
        self.broker_ip1.clone()
    }

    pub fn broker_ip2(&self) -> Option<String> {
        self.broker_ip2.clone()
    }

    pub fn listen_port(&self) -> u32 {
        self.listen_port
    }

    pub fn trace_topic_enable(&self) -> bool {
        self.trace_topic_enable
    }

    pub fn broker_server_config(&self) -> &ServerConfig {
        &self.broker_server_config
    }

    pub fn region_id(&self) -> &str {
        self.region_id.as_str()
    }

    pub fn broker_permission(&self) -> u32 {
        self.broker_permission
    }

    pub fn get_broker_addr(&self) -> String {
        format!("{}:{}", self.broker_ip1, self.listen_port)
    }

    pub fn get_start_accept_send_request_time_stamp(&self) -> i64 {
        self.start_accept_send_request_time_stamp
    }

    pub fn get_properties(&self) -> HashMap<String, String> {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert("brokerName".to_string(), self.broker_name.clone());
        properties.insert(
            "brokerClusterName".to_string(),
            self.broker_identity.broker_cluster_name.clone(),
        );
        properties.insert(
            "brokerId".to_string(),
            self.broker_identity.broker_id.to_string(),
        );
        properties.insert(
            "isBrokerContainer".to_string(),
            self.broker_identity.is_broker_container.to_string(),
        );
        properties.insert(
            "isInBrokerContainer".to_string(),
            self.broker_identity.is_in_broker_container.to_string(),
        );
        properties.insert(
            "defaultTopicQueueNums".to_string(),
            self.topic_queue_config.default_topic_queue_nums.to_string(),
        );
        properties.insert(
            "timerWheelEnable".to_string(),
            self.timer_wheel_config.timer_wheel_enable.to_string(),
        );
        properties.insert(
            "bindAddress".to_string(),
            self.broker_server_config.bind_address.clone().to_string(),
        );
        properties.insert("brokerIp1".to_string(), self.broker_ip1.clone().to_string());
        properties.insert(
            "brokerIp2".to_string(),
            self.broker_ip2.clone().unwrap_or_default(),
        );
        properties.insert("listenPort".to_string(), self.listen_port.to_string());
        properties.insert(
            "traceTopicEnable".to_string(),
            self.trace_topic_enable.to_string(),
        );
        properties.insert(
            "msgTraceTopicName".to_string(),
            self.msg_trace_topic_name.clone(),
        );
        properties.insert(
            "enableControllerMode".to_string(),
            self.enable_controller_mode.to_string(),
        );
        properties.insert("regionId".to_string(), self.region_id.clone());
        properties.insert("traceOn".to_string(), self.trace_on.to_string());
        properties.insert(
            "brokerPermission".to_string(),
            self.broker_permission.to_string(),
        );
        properties.insert(
            "asyncSendEnable".to_string(),
            self.async_send_enable.to_string(),
        );
        properties.insert(
            "storePathRootDir".to_string(),
            self.store_path_root_dir.clone(),
        );
        properties.insert(
            "enableSplitRegistration".to_string(),
            self.enable_split_registration.to_string(),
        );
        properties.insert(
            "splitRegistrationSize".to_string(),
            self.split_registration_size.to_string(),
        );
        properties.insert(
            "registerBrokerTimeoutMills".to_string(),
            self.register_broker_timeout_mills.to_string(),
        );
        properties.insert(
            "isInBrokerContainer".to_string(),
            self.is_in_broker_container.to_string(),
        );
        properties.insert(
            "commercialSizePerMsg".to_string(),
            self.commercial_size_per_msg.to_string(),
        );
        properties.insert(
            "recoverConcurrently".to_string(),
            self.recover_concurrently.to_string(),
        );
        properties.insert(
            "duplicationEnable".to_string(),
            self.duplication_enable.to_string(),
        );
        properties.insert(
            "startAcceptSendRequestTimeStamp".to_string(),
            self.start_accept_send_request_time_stamp.to_string(),
        );
        properties.insert(
            "autoCreateTopicEnable".to_string(),
            self.auto_create_topic_enable.to_string(),
        );
        properties.insert(
            "enableSingleTopicRegister".to_string(),
            self.enable_single_topic_register.to_string(),
        );
        properties.insert(
            "brokerTopicEnable".to_string(),
            self.broker_topic_enable.to_string(),
        );
        properties.insert(
            "clusterTopicEnable".to_string(),
            self.cluster_topic_enable.to_string(),
        );
        properties.insert(
            "reviveQueueNum".to_string(),
            self.revive_queue_num.to_string(),
        );
        properties.insert(
            "enableSlaveActingMaster".to_string(),
            self.enable_slave_acting_master.to_string(),
        );
        properties.insert(
            "rejectTransactionMessage".to_string(),
            self.reject_transaction_message.to_string(),
        );
        properties.insert(
            "enableDetailStat".to_string(),
            self.enable_detail_stat.to_string(),
        );
        properties.insert(
            "flushConsumerOffsetInterval".to_string(),
            self.flush_consumer_offset_interval.to_string(),
        );
        properties.insert("forceRegister".to_string(), self.force_register.to_string());
        properties.insert(
            "registerNameServerPeriod".to_string(),
            self.register_name_server_period.to_string(),
        );
        properties.insert(
            "skipPreOnline".to_string(),
            self.skip_pre_online.to_string(),
        );
        properties.insert(
            "namesrvAddr".to_string(),
            self.namesrv_addr.clone().unwrap_or_default(),
        );
        properties.insert(
            "fetchNameSrvAddrByDnsLookup".to_string(),
            self.fetch_name_srv_addr_by_dns_lookup.to_string(),
        );
        properties.insert(
            "litePullMessageEnable".to_string(),
            self.lite_pull_message_enable.to_string(),
        );
        properties.insert(
            "autoCreateSubscriptionGroup".to_string(),
            self.auto_create_subscription_group.to_string(),
        );
        properties.insert(
            "channelExpiredTimeout".to_string(),
            self.channel_expired_timeout.to_string(),
        );
        properties.insert(
            "subscriptionExpiredTimeout".to_string(),
            self.subscription_expired_timeout.to_string(),
        );
        properties.insert(
            "enablePropertyFilter".to_string(),
            self.enable_property_filter.to_string(),
        );
        properties.insert(
            "filterSupportRetry".to_string(),
            self.filter_support_retry.to_string(),
        );
        properties.insert(
            "useServerSideResetOffset".to_string(),
            self.use_server_side_reset_offset.to_string(),
        );
        properties.insert(
            "slaveReadEnable".to_string(),
            self.slave_read_enable.to_string(),
        );
        properties.insert(
            "commercialBaseCount".to_string(),
            self.commercial_base_count.to_string(),
        );
        properties.insert(
            "rejectPullConsumerEnable".to_string(),
            self.reject_pull_consumer_enable.to_string(),
        );
        properties.insert(
            "consumerOffsetUpdateVersionStep".to_string(),
            self.consumer_offset_update_version_step.to_string(),
        );
        properties.insert(
            "enableBroadcastOffsetStore".to_string(),
            self.enable_broadcast_offset_store.to_string(),
        );
        properties.insert(
            "transferMsgByHeap".to_string(),
            self.transfer_msg_by_heap.to_string(),
        );
        properties.insert(
            "shortPollingTimeMills".to_string(),
            self.short_polling_time_mills.to_string(),
        );
        properties.insert(
            "longPollingEnable".to_string(),
            self.long_polling_enable.to_string(),
        );
        properties.insert(
            "maxErrorRateOfBloomFilter".to_string(),
            self.max_error_rate_of_bloom_filter.to_string(),
        );
        properties.insert(
            "expectConsumerNumUseFilter".to_string(),
            self.expect_consumer_num_use_filter.to_string(),
        );
        properties.insert(
            "bitMapLengthConsumeQueueExt".to_string(),
            self.bit_map_length_consume_queue_ext.to_string(),
        );
        properties.insert(
            "validateSystemTopicWhenUpdateTopic".to_string(),
            self.validate_system_topic_when_update_topic.to_string(),
        );
        properties.insert(
            "enableMixedMessageType".to_string(),
            self.enable_mixed_message_type.to_string(),
        );
        properties.insert(
            "autoDeleteUnusedStats".to_string(),
            self.auto_delete_unused_stats.to_string(),
        );
        properties.insert(
            "forwardTimeout".to_string(),
            self.forward_timeout.to_string(),
        );
        properties
    }
}

pub fn default_broker_name() -> String {
    LOCAL_HOST_NAME
        .clone()
        .unwrap_or_else(|| "DEFAULT_BROKER".to_string())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TopicQueueConfig {
    pub default_topic_queue_nums: u32,
}

impl Default for TopicQueueConfig {
    fn default() -> Self {
        TopicQueueConfig {
            default_topic_queue_nums: 8,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TimerWheelConfig {
    pub timer_wheel_enable: bool,
}
