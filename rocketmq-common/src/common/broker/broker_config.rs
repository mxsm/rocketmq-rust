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

use lazy_static::lazy_static;
use serde::Deserialize;

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

#[derive(Debug, Default, Deserialize)]
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

#[derive(Debug, Deserialize)]
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

    pub fn region_id(&self) -> String {
        self.region_id.clone()
    }

    pub fn broker_permission(&self) -> u32 {
        self.broker_permission
    }
}

pub fn default_broker_name() -> String {
    LOCAL_HOST_NAME
        .clone()
        .unwrap_or_else(|| "DEFAULT_BROKER".to_string())
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TimerWheelConfig {
    pub timer_wheel_enable: bool,
}
