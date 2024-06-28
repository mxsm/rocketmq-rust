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

use std::cell::SyncUnsafeCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::attribute::attribute_util::alter_current_attributes;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::TopicAttributes::ALL;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::message_store::default_message_store::DefaultMessageStore;
use tracing::info;
use tracing::warn;

use crate::broker_path_config_helper::get_topic_config_path;
use crate::broker_runtime::BrokerRuntimeInner;

pub(crate) struct TopicConfigManager {
    topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
    data_version: Arc<SyncUnsafeCell<DataVersion>>,
    broker_config: Arc<BrokerConfig>,
    message_store: Option<DefaultMessageStore>,
    topic_config_table_lock: Arc<parking_lot::ReentrantMutex<()>>,
    broker_runtime_inner: Arc<BrokerRuntimeInner>,
}

impl Clone for TopicConfigManager {
    fn clone(&self) -> Self {
        Self {
            topic_config_table: self.topic_config_table.clone(),
            data_version: self.data_version.clone(),
            broker_config: self.broker_config.clone(),
            message_store: self.message_store.clone(),
            topic_config_table_lock: self.topic_config_table_lock.clone(),
            broker_runtime_inner: self.broker_runtime_inner.clone(),
        }
    }
}

impl TopicConfigManager {
    const SCHEDULE_TOPIC_QUEUE_NUM: u32 = 18;

    pub fn new(
        broker_config: Arc<BrokerConfig>,
        broker_runtime_inner: Arc<BrokerRuntimeInner>,
    ) -> Self {
        let mut manager = Self {
            topic_config_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            data_version: Arc::new(SyncUnsafeCell::new(DataVersion::default())),
            broker_config,
            message_store: None,
            topic_config_table_lock: Default::default(),
            broker_runtime_inner,
        };
        manager.init();
        manager
    }

    fn init(&mut self) {
        //SELF_TEST_TOPIC
        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_SELF_TEST_TOPIC,
                1,
                1,
            ));
        }

        //auto create topic setting
        {
            if self.broker_config.auto_create_topic_enable {
                let default_topic_queue_nums = self
                    .broker_config
                    .topic_queue_config
                    .default_topic_queue_nums;
                self.put_topic_config(TopicConfig::with_perm(
                    TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
                    default_topic_queue_nums,
                    default_topic_queue_nums,
                    PermName::PERM_INHERIT | PermName::PERM_READ | PermName::PERM_WRITE,
                ));
            }
        }
        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_BENCHMARK_TOPIC,
                1024,
                1024,
            ));
        }
        {
            let topic = self
                .broker_config
                .broker_identity
                .broker_cluster_name
                .to_string();
            let mut config = TopicConfig::new(topic);
            let mut perm = PermName::PERM_INHERIT;
            if self.broker_config.cluster_topic_enable {
                perm |= PermName::PERM_READ | PermName::PERM_WRITE;
            }
            config.perm = perm;
            self.put_topic_config(config);
        }

        {
            let topic = self.broker_config.broker_identity.broker_name.to_string();
            let mut config = TopicConfig::new(topic);
            let mut perm = PermName::PERM_INHERIT;
            if self.broker_config.broker_topic_enable {
                perm |= PermName::PERM_READ | PermName::PERM_WRITE;
            }
            config.perm = perm;
            self.put_topic_config(config);
        }

        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT,
                1,
                1,
            ));
        }

        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_SCHEDULE_TOPIC,
                Self::SCHEDULE_TOPIC_QUEUE_NUM,
                Self::SCHEDULE_TOPIC_QUEUE_NUM,
            ));
        }

        {
            if self.broker_config.trace_topic_enable {
                let topic = self.broker_config.msg_trace_topic_name.clone();
                self.put_topic_config(TopicConfig::with_queues(topic, 1, 1));
            }
        }

        {
            let topic = format!(
                "{}_{}",
                self.broker_config.broker_identity.broker_name,
                mix_all::REPLY_TOPIC_POSTFIX
            );
            self.put_topic_config(TopicConfig::with_queues(topic, 1, 1));
        }

        {
            // PopAckConstants.REVIVE_TOPIC
            let topic = format!(
                "rmq_sys_REVIVE_LOG_{}",
                self.broker_config.broker_identity.broker_cluster_name
            );

            self.put_topic_config(TopicConfig::with_queues(
                topic,
                self.broker_config.revive_queue_num,
                self.broker_config.revive_queue_num,
            ));
        }

        {
            let topic = format!(
                "{}_{}",
                TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX,
                self.broker_config.broker_identity.broker_name,
            );
            self.put_topic_config(TopicConfig::with_perm(topic, 1, 1, PermName::PERM_INHERIT));
        }

        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC,
                1,
                1,
            ));
        }

        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC,
                1,
                1,
            ));
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
            self.data_version_mut().next_version();
        }
        TopicConfigAndMappingSerializeWrapper {
            topic_config_table: Some(topic_config_table),
            topic_queue_mapping_info_map,
            ..TopicConfigAndMappingSerializeWrapper::default()
        }
    }

    pub fn is_order_topic(&self, topic: &str) -> bool {
        match self.get_topic_config(topic) {
            None => false,
            Some(value) => value.order,
        }
    }

    fn get_topic_config(&self, topic_name: &str) -> Option<TopicConfig> {
        self.topic_config_table.lock().get(topic_name).cloned()
    }

    pub(crate) fn put_topic_config(&self, topic_config: TopicConfig) -> Option<TopicConfig> {
        self.topic_config_table.lock().insert(
            topic_config.topic_name.as_ref().unwrap().clone(),
            topic_config,
        )
    }

    pub fn create_topic_in_send_message_method(
        &mut self,
        topic: &str,
        default_topic: &str,
        remote_address: SocketAddr,
        client_default_topic_queue_nums: i32,
        topic_sys_flag: u32,
    ) -> Option<TopicConfig> {
        let (topic_config, create_new) = if let Some(_lock) = self
            .topic_config_table_lock
            .try_lock_for(Duration::from_secs(3))
        {
            if let Some(topic_config) = self.get_topic_config(topic) {
                return Some(topic_config);
            }

            if let Some(mut default_topic_config) = self.get_topic_config(default_topic) {
                if default_topic == TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC
                    && !self.broker_config.auto_create_topic_enable
                {
                    default_topic_config.perm = PermName::PERM_READ | PermName::PERM_WRITE;
                }

                if PermName::is_inherited(default_topic_config.perm) {
                    let mut topic_config = TopicConfig::new(topic);
                    let queue_nums = client_default_topic_queue_nums
                        .min(default_topic_config.write_queue_nums as i32)
                        .max(0);
                    topic_config.write_queue_nums = queue_nums as u32;
                    topic_config.read_queue_nums = queue_nums as u32;
                    topic_config.perm = default_topic_config.perm & !PermName::PERM_INHERIT;
                    topic_config.topic_sys_flag = topic_sys_flag;
                    topic_config.topic_filter_type = default_topic_config.topic_filter_type;
                    info!(
                        "Create new topic by default topic:[{}] config:[{:?}] producer:[{}]",
                        default_topic, topic_config, remote_address
                    );
                    self.put_topic_config(topic_config.clone());
                    self.data_version_mut().next_version_with(
                        self.message_store
                            .as_ref()
                            .unwrap()
                            .get_state_machine_version(),
                    );
                    self.persist();
                    (Some(topic_config), true)
                } else {
                    warn!(
                        "Create new topic failed, because the default topic[{}] has no perm [{}] \
                         producer:[{}]",
                        default_topic, default_topic_config.perm, remote_address
                    );
                    (None, false)
                }
            } else {
                (None, false)
            }
        } else {
            (None, false)
        };

        if create_new {
            self.register_broker_data(topic_config.as_ref().unwrap());
        }
        topic_config
    }

    pub fn create_topic_in_send_message_back_method(
        &mut self,
        topic: &str,
        client_default_topic_queue_nums: i32,
        perm: u32,
        is_order: bool,
        topic_sys_flag: u32,
    ) -> Option<TopicConfig> {
        if let Some(ref mut config) = self.get_topic_config(topic) {
            if is_order != config.order {
                config.order = is_order;
                self.update_topic_config(config);
            }
            return Some(config.clone());
        }

        let (topic_config, create_new) = if let Some(_lock) = self
            .topic_config_table_lock
            .try_lock_for(Duration::from_secs(3))
        {
            if let Some(config) = self.get_topic_config(topic) {
                return Some(config);
            }

            let mut config = TopicConfig::new(topic);
            config.read_queue_nums = client_default_topic_queue_nums as u32;
            config.write_queue_nums = client_default_topic_queue_nums as u32;
            config.perm = perm;
            config.topic_sys_flag = topic_sys_flag;
            config.order = is_order;

            self.put_topic_config(config.clone());
            self.data_version_mut().next_version_with(
                self.message_store
                    .as_ref()
                    .unwrap()
                    .get_state_machine_version(),
            );
            self.persist();
            (Some(config), true)
        } else {
            (None, false)
        };

        if create_new {
            self.register_broker_data(topic_config.as_ref().unwrap());
        }
        topic_config
    }

    fn register_broker_data(&mut self, topic_config: &TopicConfig) {
        let broker_config = self.broker_config.clone();
        let broker_runtime_inner = self.broker_runtime_inner.clone();
        let topic_config_clone = topic_config.clone();
        tokio::spawn(async move {
            if broker_config.enable_single_topic_register {
                broker_runtime_inner
                    .register_single_topic_all(topic_config_clone)
                    .await;
            } else {
                unimplemented!()
            }
        });
    }

    pub fn update_topic_config(&mut self, topic_config: &mut TopicConfig) {
        let new_attributes = Self::request(topic_config);
        let current_attributes = self.current(topic_config.topic_name.as_ref().unwrap().as_str());
        let create = self
            .topic_config_table
            .lock()
            .get(topic_config.topic_name.as_ref().unwrap().as_str())
            .is_none();
        let final_attributes =
            alter_current_attributes(create, ALL.clone(), new_attributes, current_attributes);
        topic_config.attributes = final_attributes;
        match self.put_topic_config(topic_config.clone()) {
            None => {
                info!("create new topic [{:?}]", topic_config)
            }
            Some(ref old) => {
                info!(
                    "update topic config, old:[{:?}] new:[{:?}]",
                    old, topic_config
                );
            }
        }

        self.data_version_mut().next_version_with(
            self.message_store
                .as_ref()
                .unwrap()
                .get_state_machine_version(),
        );
        self.persist_with_topic(
            topic_config.topic_name.as_ref().unwrap().as_str(),
            topic_config.clone(),
        );
    }

    fn request(topic_config: &TopicConfig) -> HashMap<String, String> {
        topic_config.attributes.clone()
    }

    fn current(&self, topic: &str) -> HashMap<String, String> {
        let topic_config = self.get_topic_config(topic);
        match topic_config {
            None => HashMap::new(),
            Some(config) => config.attributes.clone(),
        }
    }

    pub fn topic_config_table(&self) -> Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>> {
        self.topic_config_table.clone()
    }

    pub fn set_topic_config_table(
        &mut self,
        topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
    ) {
        self.topic_config_table = topic_config_table;
    }

    pub fn set_message_store(&mut self, message_store: Option<DefaultMessageStore>) {
        self.message_store = message_store;
    }

    pub fn create_topic_of_tran_check_max_time(
        &mut self,
        client_default_topic_queue_nums: i32,
        perm: u32,
    ) -> Option<TopicConfig> {
        if let Some(ref mut config) =
            self.get_topic_config(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC)
        {
            return Some(config.clone());
        }

        let (topic_config, create_new) = if let Some(_lock) = self
            .topic_config_table_lock
            .try_lock_for(Duration::from_secs(3))
        {
            if let Some(config) =
                self.get_topic_config(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC)
            {
                return Some(config);
            }

            let mut config = TopicConfig::new(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
            config.read_queue_nums = client_default_topic_queue_nums as u32;
            config.write_queue_nums = client_default_topic_queue_nums as u32;
            config.perm = perm;
            config.topic_sys_flag = 0;
            info!("create new topic {:?}", config);
            self.put_topic_config(config.clone());
            self.data_version_mut().next_version_with(
                self.message_store
                    .as_ref()
                    .unwrap()
                    .get_state_machine_version(),
            );
            self.persist();
            (Some(config), true)
        } else {
            (None, false)
        };

        if create_new {
            self.register_broker_data(topic_config.as_ref().unwrap());
        }
        topic_config
    }

    pub fn contains_topic(&self, topic: &str) -> bool {
        self.topic_config_table.lock().contains_key(topic)
    }

    fn data_version(&self) -> &DataVersion {
        unsafe { &*self.data_version.get() }
    }

    fn data_version_mut(&self) -> &mut DataVersion {
        unsafe { &mut *self.data_version.get() }
    }
}

impl ConfigManager for TopicConfigManager {
    fn config_file_path(&self) -> String {
        get_topic_config_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        let topic_config_table = self.topic_config_table.lock().clone();
        let version = self.data_version().clone();
        match pretty_format {
            true => TopicConfigSerializeWrapper::new(Some(topic_config_table), Some(version))
                .to_json_pretty(),
            false => {
                TopicConfigSerializeWrapper::new(Some(topic_config_table), Some(version)).to_json()
            }
        }
    }

    fn decode(&self, json_string: &str) {
        info!("decode topic config from json string:{}", json_string);
        if json_string.is_empty() {
            return;
        }
        let wrapper = SerdeJsonUtils::from_json_str::<TopicConfigSerializeWrapper>(json_string)
            .expect("Decode TopicConfigSerializeWrapper from json failed");
        if let Some(value) = wrapper.data_version() {
            self.data_version_mut().assign_new_one(value);
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
