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

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::attribute_util::AttributeUtil;
use rocketmq_common::common::attribute::subscription_group_attributes::SubscriptionGroupAttributes;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::mix_all::is_sys_consumer_group;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use serde::Deserialize;
use serde::Serialize;
use tracing::info;

use crate::broker_path_config_helper::get_subscription_group_path;
use crate::broker_runtime::BrokerRuntimeInner;

pub const CHARACTER_MAX_LENGTH: usize = 255;
pub const TOPIC_MAX_LENGTH: usize = 127;

pub(crate) struct SubscriptionGroupManager<MS: MessageStore> {
    pub(crate) subscription_group_wrapper: Arc<parking_lot::Mutex<SubscriptionGroupWrapperInner>>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    data_version: DataVersion,
}

impl<MS> SubscriptionGroupManager<MS>
where
    MS: MessageStore,
{
    pub fn new(
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> SubscriptionGroupManager<MS> {
        Self {
            subscription_group_wrapper: Arc::new(parking_lot::Mutex::new(
                SubscriptionGroupWrapperInner::default(),
            )),
            broker_runtime_inner,
            data_version: DataVersion::new(),
        }
    }

    pub fn subscription_group_wrapper(
        &self,
    ) -> &Arc<parking_lot::Mutex<SubscriptionGroupWrapperInner>> {
        &self.subscription_group_wrapper
    }
    pub(crate) fn update_subscription_group_config(
        &mut self,
        config: &mut SubscriptionGroupConfig,
    ) {
        self.update_subscription_group_config_without_persist(config);
        self.persist();
    }

    fn update_subscription_group_config_without_persist(
        &mut self,
        config: &mut SubscriptionGroupConfig,
    ) {
        let new_attributes = self.request(config);
        let current_attributes = self.current(config.group_name());
        let final_attributes = match AttributeUtil::alter_current_attributes(
            self.subscription_group_wrapper
                .as_ref()
                .lock()
                .subscription_group_table
                .contains_key(config.group_name()),
            SubscriptionGroupAttributes::all(),
            &current_attributes,
            &new_attributes,
        ) {
            Ok(final_attributes) => final_attributes,
            Err(_err) => {
                todo!("deal runtime exception")
            }
        };
        config.set_attributes(final_attributes);

        match self.put_subscription_group_config(config.clone()) {
            //todo avoid clone
            Some(old) => {
                info!(
                    "update subscription group config, old: {:?} new: {:?}",
                    old, config,
                );
            }
            None => {
                info!("create new subscription group, {:?}", config)
            }
        }

        self.update_data_version();
    }
    fn request(
        &self,
        subscription_group_config: &SubscriptionGroupConfig,
    ) -> HashMap<CheetahString, CheetahString> {
        subscription_group_config.attributes().clone()
    }

    fn current(&self, group_name: &str) -> HashMap<CheetahString, CheetahString> {
        match self
            .subscription_group_wrapper()
            .lock()
            .subscription_group_table()
            .get(group_name)
        {
            Some(subscription_group_config) => subscription_group_config.attributes().clone(),
            None => HashMap::new(),
        }
    }

    fn put_subscription_group_config(
        &self,
        subscription_group_config: SubscriptionGroupConfig,
    ) -> Option<SubscriptionGroupConfig> {
        self.subscription_group_wrapper
            .lock()
            .subscription_group_table
            .insert(
                subscription_group_config.group_name().into(),
                subscription_group_config,
            )
    }

    fn update_data_version(&mut self) {
        let state_machine_version = match self.broker_runtime_inner.message_store() {
            Some(message_store) => message_store.get_state_machine_version(),
            None => 0,
        };
        self.date_version_mut()
            .next_version_with(state_machine_version);
    }

    pub fn date_version(&self) -> &DataVersion {
        &self.data_version
    }
    pub fn date_version_mut(&mut self) -> &mut DataVersion {
        &mut self.data_version
    }

    pub fn get_subscription_group_table(
        &self,
    ) -> Arc<parking_lot::Mutex<SubscriptionGroupWrapperInner>> {
        self.subscription_group_wrapper.clone()
    }
}

impl<MS: MessageStore> ConfigManager for SubscriptionGroupManager<MS> {
    fn config_file_path(&self) -> String {
        get_subscription_group_path(
            self.broker_runtime_inner
                .broker_config()
                .store_path_root_dir
                .as_str(),
        )
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        match pretty_format {
            true => self
                .subscription_group_wrapper
                .lock()
                .clone()
                .serialize_json_pretty()
                .expect("encode subscription group pretty failed"),
            false => self
                .subscription_group_wrapper
                .lock()
                .clone()
                .serialize_json()
                .expect("encode subscription group failed"),
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let wrapper =
            serde_json::from_str::<SubscriptionGroupWrapperInner>(json_string).unwrap_or_default();
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
    pub fn contains_subscription_group(&self, group: &CheetahString) -> bool {
        if group.is_empty() {
            return false;
        }
        self.subscription_group_wrapper
            .lock()
            .subscription_group_table
            .contains_key(group)
    }

    pub fn find_subscription_group_config(
        &self,
        group: &CheetahString,
    ) -> Option<SubscriptionGroupConfig> {
        let mut subscription_group_config = self.find_subscription_group_config_inner(group);
        if subscription_group_config.is_none()
            && (self
                .broker_runtime_inner
                .broker_config()
                .auto_create_subscription_group
                || is_sys_consumer_group(group))
        {
            if group.len() > CHARACTER_MAX_LENGTH
                || TopicValidator::is_topic_or_group_illegal(group)
            {
                return None;
            }
            let mut subscription_group_config_new = SubscriptionGroupConfig::default();
            subscription_group_config_new.set_group_name(group.clone());
            let pre_config = self
                .subscription_group_wrapper
                .lock()
                .subscription_group_table
                .insert(group.clone(), subscription_group_config_new.clone());
            if pre_config.is_none() {
                info!(
                    "auto create a subscription group, {:?}",
                    subscription_group_config_new
                );
            }
            let state_machine_version =
                if let Some(ref store) = self.broker_runtime_inner.message_store() {
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

    fn find_subscription_group_config_inner(
        &self,
        group: &CheetahString,
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
pub(crate) struct SubscriptionGroupWrapperInner {
    //todo dashmap to concurrent safe
    subscription_group_table: HashMap<CheetahString, SubscriptionGroupConfig>,
    forbidden_table: HashMap<CheetahString, HashMap<CheetahString, i32>>,
    data_version: DataVersion,
}

impl SubscriptionGroupWrapperInner {
    pub fn subscription_group_table(&self) -> &HashMap<CheetahString, SubscriptionGroupConfig> {
        &self.subscription_group_table
    }

    pub fn subscription_group_table_mut(
        &mut self,
    ) -> &mut HashMap<CheetahString, SubscriptionGroupConfig> {
        &mut self.subscription_group_table
    }

    pub fn forbidden_table(&self) -> &HashMap<CheetahString, HashMap<CheetahString, i32>> {
        &self.forbidden_table
    }
}
