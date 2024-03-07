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

use rocketmq_common::common::{
    broker::broker_config::{BrokerIdentity, TimerWheelConfig, TopicConfig, TopicQueueConfig},
    constant::PermName,
    mix_all,
    topic::TopicValidator,
};
use rocketmq_remoting::server::config::BrokerServerConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfig {
    pub broker_identity: BrokerIdentity,

    pub topic_config: TopicConfig,

    pub topic_queue_config: TopicQueueConfig,

    pub timer_wheel_config: TimerWheelConfig,

    pub broker_server_config: BrokerServerConfig,

    pub broker_ip1: String,
    pub broker_ip2: Option<String>,
    pub listen_port: u32,
    pub trace_topic_enable: bool,
    pub msg_trace_topic_name: String,
    pub enable_controller_mode: bool,
    pub broker_name: String,
    pub region_id: String,
    pub trace_on: bool,
    pub broker_permission: i8,
    pub async_send_enable: bool,
    pub store_path_root_dir: String,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        let broker_identity = BrokerIdentity::new();
        let broker_ip1 = String::from("127.0.0.1");
        let broker_ip2 = None;
        let listen_port = 10911;

        BrokerConfig {
            broker_identity,
            topic_config: TopicConfig::default(),
            topic_queue_config: TopicQueueConfig::default(),
            timer_wheel_config: TimerWheelConfig::default(),
            broker_server_config: Default::default(),
            broker_ip1,
            broker_ip2,
            listen_port,
            trace_topic_enable: false,
            msg_trace_topic_name: TopicValidator::RMQ_SYS_TRACE_TOPIC.to_string(),
            enable_controller_mode: false,
            broker_name: "".to_string(),
            region_id: mix_all::DEFAULT_TRACE_REGION_ID.to_string(),
            trace_on: true,
            broker_permission: PermName::PERM_WRITE | PermName::PERM_READ,
            async_send_enable: false,
            store_path_root_dir: dirs::home_dir()
                .unwrap()
                .join("store")
                .to_string_lossy()
                .into_owned(),
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

    pub fn broker_server_config(&self) -> &BrokerServerConfig {
        &self.broker_server_config
    }

    pub fn region_id(&self) -> String {
        self.region_id.clone()
    }

    pub fn broker_permission(&self) -> i8 {
        self.broker_permission
    }
}
