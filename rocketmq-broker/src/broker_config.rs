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
        }
    }
}
