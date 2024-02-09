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

use serde::Deserialize;

use crate::common::{mix_all, topic::TopicValidator};

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
    fn new() -> Self {
        let broker_name = default_broker_name();
        let broker_cluster_name = String::from("DefaultCluster");
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

fn default_broker_name() -> String {
    // Implement logic to obtain default broker name
    // For example, use local hostname
    // ...

    // Placeholder value for demonstration
    String::from("DefaultBrokerName")
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfig {
    pub broker_identity: BrokerIdentity,

    pub topic_config: TopicConfig,
    pub topic_queue_config: TopicQueueConfig,

    pub broker_ip1: String,
    pub broker_ip2: Option<String>,
    pub listen_port: u32,
    pub trace_topic_enable: bool,
    pub msg_trace_topic_name: String,
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
            topic_queue_config: Default::default(),
            broker_ip1,
            broker_ip2,
            listen_port,
            trace_topic_enable: false,
            msg_trace_topic_name: TopicValidator::RMQ_SYS_TRACE_TOPIC.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfig {
    pub auto_create_topic_enable: bool,
    pub cluster_topic_enable: bool,
    pub broker_topic_enable: bool,
}

impl Default for TopicConfig {
    fn default() -> Self {
        TopicConfig {
            auto_create_topic_enable: true,
            cluster_topic_enable: true,
            broker_topic_enable: true,
        }
    }
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
