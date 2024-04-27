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

use rocketmq_common::common::{broker::broker_config::BrokerConfig, config_manager::ConfigManager};
use rocketmq_remoting::protocol::{
    subscription::subscription_group_config::SubscriptionGroupConfig, DataVersion,
};
use serde::{Deserialize, Serialize};

use crate::broker_path_config_helper::get_subscription_group_path;

#[derive(Default)]
pub(crate) struct SubscriptionGroupManager {
    pub(crate) broker_config: Arc<BrokerConfig>,
    subscription_group_wrapper: parking_lot::Mutex<SubscriptionGroupWrapper>,
}

//Fully implemented will be removed
#[allow(unused_variables)]
impl ConfigManager for SubscriptionGroupManager {
    fn decode0(&mut self, key: &[u8], body: &[u8]) {
        todo!()
    }

    fn stop(&mut self) -> bool {
        todo!()
    }

    fn config_file_path(&self) -> String {
        get_subscription_group_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode(&mut self) -> String {
        todo!()
    }

    fn encode_pretty(&mut self, pretty_format: bool) -> String {
        todo!()
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
