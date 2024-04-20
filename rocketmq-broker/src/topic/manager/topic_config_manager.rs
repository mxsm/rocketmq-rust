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

use std::{collections::HashMap, sync::Arc};

use rocketmq_common::common::{
    config::TopicConfig, config_manager::ConfigManager, constant::PermName, mix_all,
    topic::TopicValidator,
};
use rocketmq_remoting::protocol::{
    body::topic_info_wrapper::{
        topic_config_wrapper::TopicConfigAndMappingSerializeWrapper, TopicConfigSerializeWrapper,
    },
    static_topic::topic_queue_info::TopicQueueMappingInfo,
    DataVersion,
};
use tracing::info;

use crate::{broker_config::BrokerConfig, broker_path_config_helper::get_topic_config_path};

#[derive(Default)]
pub(crate) struct TopicConfigManager {
    pub consumer_order_info_manager: String,
    pub topic_config_table: parking_lot::Mutex<HashMap<String, TopicConfig>>,
    pub data_version: parking_lot::Mutex<DataVersion>,
    pub broker_config: Arc<BrokerConfig>,
}

impl TopicConfigManager {
    pub fn new(broker_config: Arc<BrokerConfig>) -> Self {
        let mut manager = Self {
            consumer_order_info_manager: "".to_string(),
            topic_config_table: parking_lot::Mutex::new(HashMap::new()),
            data_version: parking_lot::Mutex::new(DataVersion::default()),
            broker_config,
        };
        manager.init();
        manager
    }

    fn init(&mut self) {
        //SELF_TEST_TOPIC
        {
            self.topic_config_table.lock().insert(
                TopicValidator::RMQ_SYS_SELF_TEST_TOPIC.to_string(),
                TopicConfig::new_with(TopicValidator::RMQ_SYS_SELF_TEST_TOPIC, 1, 1),
            );
        }

        //auto create topic setting
        {
            if self.broker_config.topic_config.auto_create_topic_enable {
                let default_topic_queue_nums = self
                    .broker_config
                    .topic_queue_config
                    .default_topic_queue_nums;
                self.topic_config_table.lock().insert(
                    TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC.to_string(),
                    TopicConfig::new_with_perm(
                        TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
                        default_topic_queue_nums,
                        default_topic_queue_nums,
                        (PermName::PERM_INHERIT | PermName::PERM_READ | PermName::PERM_WRITE)
                            as u32,
                    ),
                );
            }
        }
        {
            self.topic_config_table.lock().insert(
                TopicValidator::RMQ_SYS_BENCHMARK_TOPIC.to_string(),
                TopicConfig::new_with(TopicValidator::RMQ_SYS_BENCHMARK_TOPIC, 1024, 1024),
            );
        }
        {
            let topic = self
                .broker_config
                .broker_identity
                .broker_cluster_name
                .to_string();
            let mut config = TopicConfig::new(topic.to_string());
            let mut perm = PermName::PERM_INHERIT;
            if self.broker_config.topic_config.cluster_topic_enable {
                perm |= PermName::PERM_READ | PermName::PERM_WRITE;
            }
            config.perm = perm as u32;
            self.topic_config_table.lock().insert(topic, config);
        }

        {
            let topic = self.broker_config.broker_identity.broker_name.to_string();
            let mut config = TopicConfig::new_with(topic.to_string(), 1, 1);
            let mut perm = PermName::PERM_INHERIT;
            if self.broker_config.topic_config.broker_topic_enable {
                perm |= PermName::PERM_READ | PermName::PERM_WRITE;
            }
            config.perm = perm as u32;
            self.topic_config_table.lock().insert(topic, config);
        }

        {
            self.topic_config_table.lock().insert(
                TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT.to_string(),
                TopicConfig::new_with(TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT, 1, 1),
            );
        }

        {
            self.topic_config_table.lock().insert(
                TopicValidator::RMQ_SYS_SCHEDULE_TOPIC.to_string(),
                TopicConfig::new_with(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC, 18, 18),
            );
        }

        {
            if self.broker_config.trace_topic_enable {
                let topic = self.broker_config.msg_trace_topic_name.clone();
                self.topic_config_table
                    .lock()
                    .insert(topic.clone(), TopicConfig::new_with(topic, 1, 1));
            }
        }

        {
            let topic = format!(
                "{}_{}",
                self.broker_config.broker_identity.broker_name,
                mix_all::REPLY_TOPIC_POSTFIX
            );
            self.topic_config_table
                .lock()
                .insert(topic.clone(), TopicConfig::new_with(topic, 1, 1));
        }

        {
            // PopAckConstants.REVIVE_TOPIC
            let topic = format!(
                "rmq_sys_REVIVE_LOG_{}",
                self.broker_config.broker_identity.broker_cluster_name
            );
            self.topic_config_table
                .lock()
                .insert(topic.clone(), TopicConfig::new_with(topic, 1, 1));
        }

        {
            let topic = format!(
                "{}_{}",
                TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX,
                self.broker_config.broker_identity.broker_name,
            );
            self.topic_config_table.lock().insert(
                topic.clone(),
                TopicConfig::new_with_perm(topic, 1, 1, PermName::PERM_INHERIT as u32),
            );
        }

        {
            self.topic_config_table.lock().insert(
                TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC.to_string(),
                TopicConfig::new_with(TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC, 1, 1),
            );
        }

        {
            self.topic_config_table.lock().insert(
                TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC.to_string(),
                TopicConfig::new_with(TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC, 1, 1),
            );
        }
    }

    pub fn select_topic_config(&self, topic: &str) -> Option<TopicConfig> {
        self.topic_config_table.lock().get(topic).cloned()
    }

    pub fn build_serialize_wrapper(
        &self,
        topic_config_table: HashMap<String, TopicConfig>,
    ) -> TopicConfigAndMappingSerializeWrapper {
        self.build_serialize_wrapper_with_topic_queue_map(topic_config_table, HashMap::new())
    }

    pub fn build_serialize_wrapper_with_topic_queue_map(
        &self,
        topic_config_table: HashMap<String, TopicConfig>,
        topic_queue_mapping_info_map: HashMap<String, TopicQueueMappingInfo>,
    ) -> TopicConfigAndMappingSerializeWrapper {
        if self.broker_config.enable_split_registration {
            self.data_version.lock().next_version();
        }
        TopicConfigAndMappingSerializeWrapper {
            topic_config_table: Some(topic_config_table),
            topic_queue_mapping_info_map,
            ..TopicConfigAndMappingSerializeWrapper::default()
        }
    }
}

//Fully implemented will be removed
#[allow(unused_variables)]
impl ConfigManager for TopicConfigManager {
    fn decode0(&mut self, key: &[u8], body: &[u8]) {
        todo!()
    }

    fn stop(&mut self) -> bool {
        todo!()
    }

    fn config_file_path(&self) -> String {
        get_topic_config_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode(&mut self) -> String {
        todo!()
    }

    fn encode_pretty(&mut self, pretty_format: bool) -> String {
        todo!()
    }

    fn decode(&self, json_string: &str) {
        info!("decode topic config from json string:{}", json_string);
        if json_string.is_empty() {
            return;
        }
        let wrapper =
            serde_json::from_str::<TopicConfigSerializeWrapper>(json_string).unwrap_or_default();
        if let Some(value) = wrapper.data_version() {
            self.data_version.lock().assign_new_one(value);
        }
        if let Some(map) = wrapper.topic_config_table() {
            for (key, value) in map {
                self.topic_config_table
                    .lock()
                    .insert(key.clone(), value.clone());
            }
        }
    }
}
