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
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::mix_all::is_sys_consumer_group;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_store::log_file::MessageStore;
use serde::Deserialize;
use serde::Serialize;
use tracing::info;

use crate::broker_path_config_helper::get_subscription_group_path;

pub const CHARACTER_MAX_LENGTH: usize = 255;
pub const TOPIC_MAX_LENGTH: usize = 127;

pub(crate) struct SubscriptionGroupManager<MS> {
    pub(crate) broker_config: Arc<BrokerConfig>,
    subscription_group_wrapper: Arc<parking_lot::Mutex<SubscriptionGroupWrapper>>,
    pub(crate) message_store: Option<MS>,
}

impl<MS> SubscriptionGroupManager<MS> {
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        message_store: Option<MS>,
    ) -> SubscriptionGroupManager<MS> {
        Self {
            broker_config,
            subscription_group_wrapper: Arc::new(parking_lot::Mutex::new(
                SubscriptionGroupWrapper::default(),
            )),
            message_store,
        }
    }
}

impl<MS> ConfigManager for SubscriptionGroupManager<MS> {
    fn config_file_path(&self) -> String {
        get_subscription_group_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        match pretty_format {
            true => self
                .subscription_group_wrapper
                .lock()
                .clone()
                .to_json_pretty(),
            false => self.subscription_group_wrapper.lock().clone().to_json(),
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let wrapper =
            serde_json::from_str::<SubscriptionGroupWrapper>(json_string).unwrap_or_default();
        for (key, subscription_group_config) in wrapper.subscription_group_table.iter() {
            self.subscription_group_wrapper
                .lock()
                .subscription_group_table
                .insert(key.clone(), subscription_group_config.clone());
        }
        for (key, subscription_group_config) in wrapper.forbidden_table.iter() {
            self.subscription_group_wrapper
                .lock()
                .forbidden_table
                .insert(key.clone(), subscription_group_config.clone());
        }
        self.subscription_group_wrapper
            .lock()
            .data_version
            .assign_new_one(&wrapper.data_version)
    }
}

impl<MS> SubscriptionGroupManager<MS>
where
    MS: MessageStore,
{
    pub fn contains_subscription_group(&self, group: &str) -> bool {
        if group.is_empty() {
            return false;
        }
        self.subscription_group_wrapper
            .lock()
            .subscription_group_table
            .contains_key(group)
    }

    pub fn find_subscription_group_config(&self, group: &str) -> Option<SubscriptionGroupConfig> {
        let mut subscription_group_config = self.find_subscription_group_config_inner(group);
        if subscription_group_config.is_none()
            && (self.broker_config.auto_create_subscription_group || is_sys_consumer_group(group))
        {
            if group.len() > CHARACTER_MAX_LENGTH
                || TopicValidator::is_topic_or_group_illegal(group)
            {
                return None;
            }
            let mut subscription_group_config_new = SubscriptionGroupConfig::default();
            subscription_group_config_new.set_group_name(group.to_string());
            let pre_config = self
                .subscription_group_wrapper
                .lock()
                .subscription_group_table
                .insert(group.to_string(), subscription_group_config_new.clone());
            if pre_config.is_none() {
                info!(
                    "auto create a subscription group, {:?}",
                    subscription_group_config_new
                );
            }
            let state_machine_version = if let Some(ref store) = self.message_store {
                store.get_state_machine_version()
            } else {
                0
            };
            self.subscription_group_wrapper
                .lock()
                .data_version
                .next_version_with(state_machine_version);
            self.persist();
            subscription_group_config = Some(subscription_group_config_new);
        }
        subscription_group_config
    }

    pub fn find_subscription_group_config_inner(
        &self,
        group: &str,
    ) -> Option<SubscriptionGroupConfig> {
        self.subscription_group_wrapper
            .lock()
            .subscription_group_table
            .get(group)
            .cloned()
    }

    pub fn get_forbidden(&self, group: &str, topic: &str, forbidden_index: i32) -> bool {
        let topic_forbidden = self.get_forbidden_internal(group, topic);
        let bit_forbidden = 1 << forbidden_index;
        (topic_forbidden & bit_forbidden) == bit_forbidden
    }
    pub fn get_forbidden_internal(&self, group: &str, topic: &str) -> i32 {
        match self
            .subscription_group_wrapper
            .lock()
            .forbidden_table
            .get(group)
        {
            Some(topic_forbiddens) => match topic_forbiddens.get(topic) {
                Some(topic_forbidden) => {
                    if *topic_forbidden < 0 {
                        0
                    } else {
                        *topic_forbidden
                    }
                }
                None => 0,
            },
            None => 0,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionGroupWrapper {
    subscription_group_table: HashMap<String, SubscriptionGroupConfig>,
    forbidden_table: HashMap<String, HashMap<String, i32>>,
    data_version: DataVersion,
}

impl SubscriptionGroupWrapper {
    pub fn subscription_group_table(&self) -> &HashMap<String, SubscriptionGroupConfig> {
        &self.subscription_group_table
    }

    pub fn forbidden_table(&self) -> &HashMap<String, HashMap<String, i32>> {
        &self.forbidden_table
    }
}

impl RemotingSerializable for SubscriptionGroupWrapper {
    type Output = Self;
}
