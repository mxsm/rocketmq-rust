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

use cheetah_string::CheetahString;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;

use crate::common::broker::broker_role::BrokerRole;
use crate::common::constant::PermName;
use crate::common::message::message_enum::MessageRequestMode;
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
    pub broker_name: CheetahString,
    pub broker_cluster_name: CheetahString,
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
            broker_name: CheetahString::from_string(broker_name),
            broker_cluster_name: CheetahString::from_string(broker_cluster_name),
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
            broker_name: CheetahString::from_string(broker_name),
            broker_cluster_name: CheetahString::from_string(broker_cluster_name),
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
            broker_name: CheetahString::from_string(broker_name),
            broker_cluster_name: CheetahString::from_string(broker_cluster_name),
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

    pub broker_ip1: CheetahString,
    pub broker_ip2: Option<CheetahString>,
    pub listen_port: u32,
    pub trace_topic_enable: bool,
    pub msg_trace_topic_name: CheetahString,
    pub enable_controller_mode: bool,
    pub broker_name: CheetahString,
    pub region_id: CheetahString,
    pub trace_on: bool,
    pub broker_permission: u32,
    pub async_send_enable: bool,
    pub store_path_root_dir: CheetahString,
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
    pub namesrv_addr: Option<CheetahString>,
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
    pub store_reply_message_enable: bool,
    pub lock_in_strict_mode: bool,
    pub transaction_timeout: u64,
    pub transaction_op_msg_max_size: i32,
    pub default_message_request_mode: MessageRequestMode,
    pub default_pop_share_queue_num: i32,
    pub load_balance_poll_name_server_interval: u64,
    pub server_load_balancer_enable: bool,
    pub enable_remote_escape: bool,
    pub enable_pop_log: bool,
    pub enable_retry_topic_v2: bool,
    // read message from pop retry topic v1, for the compatibility, will be removed in the future
    // version
    pub retrieve_message_from_pop_retry_topic_v1: bool,
    pub pop_from_retry_probability: i32,
    pub pop_response_return_actual_retry_topic: bool,
    pub init_pop_offset_by_check_msg_in_mem: bool,
    pub enable_pop_buffer_merge: bool,
    pub pop_ck_stay_buffer_time_out: u64,
    pub broker_role: BrokerRole,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        let broker_identity = BrokerIdentity::new();
        let local_ip = local_ip_address::local_ip().unwrap();
        let broker_ip1 = local_ip.to_string().into();
        let broker_ip2 = Some(local_ip.to_string().into());
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
            msg_trace_topic_name: CheetahString::from_static_str(
                TopicValidator::RMQ_SYS_TRACE_TOPIC,
            ),
            enable_controller_mode: false,
            broker_name: default_broker_name().into(),
            region_id: CheetahString::from_static_str(mix_all::DEFAULT_TRACE_REGION_ID),
            trace_on: true,
            broker_permission: PermName::PERM_WRITE | PermName::PERM_READ,
            async_send_enable: false,
            store_path_root_dir: dirs::home_dir()
                .unwrap()
                .join("store")
                .to_string_lossy()
                .into_owned()
                .into(),
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
            namesrv_addr: NAMESRV_ADDR.clone().map(|addr| addr.into()),
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
            store_reply_message_enable: true,
            lock_in_strict_mode: false,
            transaction_timeout: 6_000,
            transaction_op_msg_max_size: 4096,
            default_message_request_mode: MessageRequestMode::Pull,
            default_pop_share_queue_num: -1,
            load_balance_poll_name_server_interval: 30_000,
            server_load_balancer_enable: true,
            enable_remote_escape: false,
            enable_pop_log: false,
            enable_retry_topic_v2: false,
            retrieve_message_from_pop_retry_topic_v1: true,
            pop_from_retry_probability: 20,
            pop_response_return_actual_retry_topic: false,
            init_pop_offset_by_check_msg_in_mem: true,
            enable_pop_buffer_merge: false,
            pop_ck_stay_buffer_time_out: 3_000,
            broker_role: BrokerRole::AsyncMaster,
        }
    }
}

impl BrokerConfig {
    pub fn broker_name(&self) -> CheetahString {
        self.broker_name.clone()
    }

    pub fn broker_ip1(&self) -> CheetahString {
        self.broker_ip1.clone()
    }

    pub fn broker_ip2(&self) -> Option<CheetahString> {
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

    pub fn get_properties(&self) -> HashMap<CheetahString, CheetahString> {
        let mut properties = HashMap::new();
        properties.insert("brokerName".into(), self.broker_name.clone());
        properties.insert(
            "brokerClusterName".into(),
            self.broker_identity.broker_cluster_name.clone(),
        );
        properties.insert(
            "brokerId".into(),
            self.broker_identity.broker_id.to_string().into(),
        );
        properties.insert(
            "isBrokerContainer".into(),
            self.broker_identity.is_broker_container.to_string().into(),
        );
        properties.insert(
            "defaultTopicQueueNums".into(),
            self.topic_queue_config
                .default_topic_queue_nums
                .to_string()
                .into(),
        );
        properties.insert(
            "timerWheelEnable".into(),
            self.timer_wheel_config
                .timer_wheel_enable
                .to_string()
                .into(),
        );
        properties.insert(
            "bindAddress".into(),
            self.broker_server_config
                .bind_address
                .clone()
                .to_string()
                .into(),
        );
        properties.insert(
            "brokerIp1".into(),
            self.broker_ip1.clone().to_string().into(),
        );
        properties.insert(
            "brokerIp2".into(),
            self.broker_ip2.clone().unwrap_or_default(),
        );
        properties.insert("listenPort".into(), self.listen_port.to_string().into());
        properties.insert(
            "traceTopicEnable".into(),
            self.trace_topic_enable.to_string().into(),
        );
        properties.insert(
            "msgTraceTopicName".into(),
            self.msg_trace_topic_name.clone(),
        );
        properties.insert(
            "enableControllerMode".into(),
            self.enable_controller_mode.to_string().into(),
        );
        properties.insert("regionId".into(), self.region_id.clone());
        properties.insert("brokerName".into(), self.broker_name.clone());
        properties.insert("traceOn".into(), self.trace_on.to_string().into());
        properties.insert(
            "brokerPermission".into(),
            self.broker_permission.to_string().into(),
        );
        properties.insert(
            "asyncSendEnable".into(),
            self.async_send_enable.to_string().into(),
        );
        properties.insert("storePathRootDir".into(), self.store_path_root_dir.clone());
        properties.insert(
            "enableSplitRegistration".into(),
            self.enable_split_registration.to_string().into(),
        );
        properties.insert(
            "splitRegistrationSize".into(),
            self.split_registration_size.to_string().into(),
        );
        properties.insert(
            "registerBrokerTimeoutMills".into(),
            self.register_broker_timeout_mills.to_string().into(),
        );
        properties.insert(
            "isInBrokerContainer".into(),
            self.is_in_broker_container.to_string().into(),
        );
        properties.insert(
            "commercialSizePerMsg".into(),
            self.commercial_size_per_msg.to_string().into(),
        );
        properties.insert(
            "recoverConcurrently".into(),
            self.recover_concurrently.to_string().into(),
        );
        properties.insert(
            "duplicationEnable".into(),
            self.duplication_enable.to_string().into(),
        );
        properties.insert(
            "startAcceptSendRequestTimeStamp".into(),
            self.start_accept_send_request_time_stamp.to_string().into(),
        );
        properties.insert(
            "autoCreateTopicEnable".into(),
            self.auto_create_topic_enable.to_string().into(),
        );
        properties.insert(
            "enableSingleTopicRegister".into(),
            self.enable_single_topic_register.to_string().into(),
        );
        properties.insert(
            "brokerTopicEnable".into(),
            self.broker_topic_enable.to_string().into(),
        );
        properties.insert(
            "clusterTopicEnable".into(),
            self.cluster_topic_enable.to_string().into(),
        );
        properties.insert(
            "reviveQueueNum".into(),
            self.revive_queue_num.to_string().into(),
        );
        properties.insert(
            "enableSlaveActingMaster".into(),
            self.enable_slave_acting_master.to_string().into(),
        );
        properties.insert(
            "rejectTransactionMessage".into(),
            self.reject_transaction_message.to_string().into(),
        );
        properties.insert(
            "enableDetailStat".into(),
            self.enable_detail_stat.to_string().into(),
        );
        properties.insert(
            "flushConsumerOffsetInterval".into(),
            self.flush_consumer_offset_interval.to_string().into(),
        );
        properties.insert(
            "forceRegister".into(),
            self.force_register.to_string().into(),
        );
        properties.insert(
            "registerNameServerPeriod".into(),
            self.register_name_server_period.to_string().into(),
        );
        properties.insert(
            "skipPreOnline".into(),
            self.skip_pre_online.to_string().into(),
        );
        properties.insert(
            "namesrvAddr".into(),
            self.namesrv_addr.clone().unwrap_or_default(),
        );
        properties.insert(
            "fetchNameSrvAddrByDnsLookup".into(),
            self.fetch_name_srv_addr_by_dns_lookup.to_string().into(),
        );
        properties.insert(
            "litePullMessageEnable".into(),
            self.lite_pull_message_enable.to_string().into(),
        );
        properties.insert(
            "autoCreateSubscriptionGroup".into(),
            self.auto_create_subscription_group.to_string().into(),
        );
        properties.insert(
            "channelExpiredTimeout".into(),
            self.channel_expired_timeout.to_string().into(),
        );
        properties.insert(
            "subscriptionExpiredTimeout".into(),
            self.subscription_expired_timeout.to_string().into(),
        );
        properties.insert(
            "enablePropertyFilter".into(),
            self.enable_property_filter.to_string().into(),
        );
        properties.insert(
            "filterSupportRetry".into(),
            self.filter_support_retry.to_string().into(),
        );
        properties.insert(
            "useServerSideResetOffset".into(),
            self.use_server_side_reset_offset.to_string().into(),
        );
        properties.insert(
            "slaveReadEnable".into(),
            self.slave_read_enable.to_string().into(),
        );
        properties.insert(
            "commercialBaseCount".into(),
            self.commercial_base_count.to_string().into(),
        );
        properties.insert(
            "rejectPullConsumerEnable".into(),
            self.reject_pull_consumer_enable.to_string().into(),
        );
        properties.insert(
            "consumerOffsetUpdateVersionStep".into(),
            self.consumer_offset_update_version_step.to_string().into(),
        );
        properties.insert(
            "enableBroadcastOffsetStore".into(),
            self.enable_broadcast_offset_store.to_string().into(),
        );
        properties.insert(
            "transferMsgByHeap".into(),
            self.transfer_msg_by_heap.to_string().into(),
        );
        properties.insert(
            "shortPollingTimeMills".into(),
            self.short_polling_time_mills.to_string().into(),
        );
        properties.insert(
            "longPollingEnable".into(),
            self.long_polling_enable.to_string().into(),
        );
        properties.insert(
            "maxErrorRateOfBloomFilter".into(),
            self.max_error_rate_of_bloom_filter.to_string().into(),
        );
        properties.insert(
            "expectConsumerNumUseFilter".into(),
            self.expect_consumer_num_use_filter.to_string().into(),
        );
        properties.insert(
            "bitMapLengthConsumeQueueExt".into(),
            self.bit_map_length_consume_queue_ext.to_string().into(),
        );
        properties.insert(
            "validateSystemTopicWhenUpdateTopic".into(),
            self.validate_system_topic_when_update_topic
                .to_string()
                .into(),
        );
        properties.insert(
            "enableMixedMessageType".into(),
            self.enable_mixed_message_type.to_string().into(),
        );
        properties.insert(
            "autoDeleteUnusedStats".into(),
            self.auto_delete_unused_stats.to_string().into(),
        );
        properties.insert(
            "forwardTimeout".into(),
            self.forward_timeout.to_string().into(),
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
