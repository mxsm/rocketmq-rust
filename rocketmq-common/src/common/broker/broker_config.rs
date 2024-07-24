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
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Default,Serialize, Deserialize, Clone)]
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

    pub fn get_properties(&self) -> HashMap<String,Box<dyn Any>> {
        let mut properties:HashMap<String,Box<dyn Any>>   = HashMap::new();
        properties.insert("broker_identity.broker_name".to_string(), Box::new(self.broker_identity.broker_name.clone()) as Box<dyn Any>);
        properties.insert("broker_identity.brokerClusterName".to_string(), Box::new(self.broker_identity.broker_cluster_name.clone()) as Box<dyn Any>);
        properties.insert("broker_identity.brokerId".to_string(), Box::new(self.broker_identity.broker_id) as Box<dyn Any>);
        properties.insert("broker_identity.isBrokerContainer".to_string(), Box::new(self.broker_identity.is_broker_container ) as Box<dyn Any>);
        properties.insert("broker_identity.isInBrokerContainer".to_string(), Box::new(self.broker_identity.is_in_broker_container ) as Box<dyn Any>);
        properties.insert("topic_queue_config.default_topic_queue_nums".to_string(), Box::new(self.topic_queue_config.default_topic_queue_nums ) as Box<dyn Any>);
        properties.insert("timer_wheel_config.timer_wheel_enable".to_string(), Box::new(self.timer_wheel_config.timer_wheel_enable ) as Box<dyn Any>);
        properties.insert("broker_server_config.listen_port".to_string(), Box::new(self.broker_server_config.listen_port ) as Box<dyn Any>);
        properties.insert("broker_server_config.bind_address".to_string(), Box::new(self.broker_server_config.bind_address.clone() ) as Box<dyn Any>);
        properties.insert("broker_ip1".to_string(), Box::new(self.broker_ip1.clone() ) as Box<dyn Any>);
        properties.insert("broker_ip2".to_string(), Box::new(self.broker_ip2.clone() ) as Box<dyn Any>);
        properties.insert("listen_port".to_string(), Box::new(self.listen_port ) as Box<dyn Any>);
        properties.insert("trace_topic_enable".to_string(), Box::new(self.trace_topic_enable ) as Box<dyn Any>);
        properties.insert("msg_trace_topic_name".to_string(), Box::new(self.msg_trace_topic_name.clone() ) as Box<dyn Any>);
        properties.insert("enable_controller_mode".to_string(), Box::new(self.enable_controller_mode ) as Box<dyn Any>);
        properties.insert("broker_name".to_string(), Box::new(self.broker_name.clone() ) as Box<dyn Any>);
        properties.insert("region_id".to_string(), Box::new(self.region_id.clone() ) as Box<dyn Any>);
        properties.insert("trace_on".to_string(), Box::new(self.trace_on ) as Box<dyn Any>);
        properties.insert("broker_permission".to_string(), Box::new(self.broker_permission ) as Box<dyn Any>);
        properties.insert("async_send_enable".to_string(), Box::new(self.async_send_enable ) as Box<dyn Any>);
        properties.insert("store_path_root_dir".to_string(), Box::new(self.store_path_root_dir.clone() ) as Box<dyn Any>);
        properties.insert("enable_split_registration".to_string(), Box::new(self.enable_split_registration ) as Box<dyn Any>);
        properties.insert("split_registration_size".to_string(), Box::new(self.split_registration_size ) as Box<dyn Any>);
        properties.insert("register_broker_timeout_mills".to_string(), Box::new(self.register_broker_timeout_mills ) as Box<dyn Any>);
        properties.insert("is_in_broker_container".to_string(), Box::new(self.is_in_broker_container ) as Box<dyn Any>);
        properties.insert("commercial_size_per_msg".to_string(), Box::new(self.commercial_size_per_msg ) as Box<dyn Any>);
        properties.insert("recover_concurrently".to_string(), Box::new(self.recover_concurrently ) as Box<dyn Any>);
        properties.insert("duplication_enable".to_string(), Box::new(self.duplication_enable ) as Box<dyn Any>);
        properties.insert("start_accept_send_request_time_stamp".to_string(), Box::new(self.start_accept_send_request_time_stamp ) as Box<dyn Any>);
        properties.insert("auto_create_topic_enable".to_string(), Box::new(self.auto_create_topic_enable ) as Box<dyn Any>);
        properties.insert("enable_single_topic_register".to_string(), Box::new(self.enable_single_topic_register ) as Box<dyn Any>);
        properties.insert("broker_topic_enable".to_string(), Box::new(self.broker_topic_enable ) as Box<dyn Any>);
        properties.insert("cluster_topic_enable".to_string(), Box::new(self.cluster_topic_enable ) as Box<dyn Any>);
        properties.insert("revive_queue_num".to_string(), Box::new(self.revive_queue_num ) as Box<dyn Any>);
        properties.insert("enable_slave_acting_master".to_string(), Box::new(self.enable_slave_acting_master ) as Box<dyn Any>);
        properties.insert("reject_transaction_message".to_string(), Box::new(self.reject_transaction_message ) as Box<dyn Any>);
        properties.insert("enable_detail_stat".to_string(), Box::new(self.enable_detail_stat ) as Box<dyn Any>);
        properties.insert("flush_consumer_offset_interval".to_string(), Box::new(self.flush_consumer_offset_interval ) as Box<dyn Any>);
        properties.insert("force_register".to_string(), Box::new(self.force_register ) as Box<dyn Any>);
        properties.insert("register_name_server_period".to_string(), Box::new(self.register_name_server_period ) as Box<dyn Any>);
        properties.insert("skip_pre_online".to_string(), Box::new(self.skip_pre_online ) as Box<dyn Any>);
        properties.insert("namesrv_addr".to_string(), Box::new(self.namesrv_addr.clone() ) as Box<dyn Any>);
        properties.insert("fetch_name_srv_addr_by_dns_lookup".to_string(), Box::new(self.fetch_name_srv_addr_by_dns_lookup ) as Box<dyn Any>);
        properties.insert("lite_pull_message_enable".to_string(), Box::new(self.lite_pull_message_enable ) as Box<dyn Any>);
        properties.insert("auto_create_subscription_group".to_string(), Box::new(self.auto_create_subscription_group ) as Box<dyn Any>);
        properties.insert("channel_expired_timeout".to_string(), Box::new(self.channel_expired_timeout ) as Box<dyn Any>);
        properties.insert("subscription_expired_timeout".to_string(), Box::new(self.subscription_expired_timeout ) as Box<dyn Any>);
        properties.insert("enable_property_filter".to_string(), Box::new(self.enable_property_filter ) as Box<dyn Any>);
        properties.insert("filter_support_retry".to_string(), Box::new(self.filter_support_retry ) as Box<dyn Any>);
        properties.insert("use_server_side_reset_offset".to_string(), Box::new(self.use_server_side_reset_offset ) as Box<dyn Any>);
        properties.insert("slave_read_enable".to_string(), Box::new(self.slave_read_enable ) as Box<dyn Any>);
        properties.insert("commercial_base_count".to_string(), Box::new(self.commercial_base_count ) as Box<dyn Any>);
        properties.insert("reject_pull_consumer_enable".to_string(), Box::new(self.reject_pull_consumer_enable ) as Box<dyn Any>);
        properties.insert("consumer_offset_update_version_step".to_string(), Box::new(self.consumer_offset_update_version_step ) as Box<dyn Any>);
        properties.insert("enable_broadcast_offset_store".to_string(), Box::new(self.enable_broadcast_offset_store ) as Box<dyn Any>);
        properties.insert("transfer_msg_by_heap".to_string(), Box::new(self.transfer_msg_by_heap ) as Box<dyn Any>);
        properties.insert("short_polling_time_mills".to_string(), Box::new(self.short_polling_time_mills ) as Box<dyn Any>);
        properties.insert("long_polling_enable".to_string(), Box::new(self.long_polling_enable ) as Box<dyn Any>);
        properties.insert("max_error_rate_of_bloom_filter".to_string(), Box::new(self.max_error_rate_of_bloom_filter ) as Box<dyn Any>);
        properties.insert("expect_consumer_num_use_filter".to_string(), Box::new(self.expect_consumer_num_use_filter ) as Box<dyn Any>);
        properties.insert("bit_map_length_consume_queue_ext".to_string(), Box::new(self.bit_map_length_consume_queue_ext ) as Box<dyn Any>);
        properties.insert("validate_system_topic_when_update_topic".to_string(), Box::new(self.validate_system_topic_when_update_topic ) as Box<dyn Any>);
        properties.insert("enable_mixed_message_type".to_string(), Box::new(self.enable_mixed_message_type ) as Box<dyn Any>);
        properties.insert("auto_delete_unused_stats".to_string(), Box::new(self.auto_delete_unused_stats ) as Box<dyn Any>);
        properties.insert("forward_timeout".to_string(), Box::new(self.forward_timeout ) as Box<dyn Any>);
        properties
    }

}

pub fn default_broker_name() -> String {
    LOCAL_HOST_NAME
        .clone()
        .unwrap_or_else(|| "DEFAULT_BROKER".to_string())
}

pub struct Propertie<T> {
    pub key: String,
    pub value: T
}

impl<T: std::fmt::Debug> Propertie<T> {
    fn new(key: String, value: T) -> Propertie<T> {
        return Propertie {
            key, value
        };
    }
}


#[derive(Debug, Serialize,Deserialize, Clone)]
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

#[derive(Debug, Serialize,Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TimerWheelConfig {
    pub timer_wheel_enable: bool,
}
