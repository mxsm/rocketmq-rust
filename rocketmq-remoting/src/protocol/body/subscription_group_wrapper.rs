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
use cheetah_string::CheetahString;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use crate::protocol::DataVersion;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionGroupWrapper {
    pub subscription_group_table: DashMap<CheetahString, SubscriptionGroupConfig>,

    pub data_version: DataVersion,
}

impl Default for SubscriptionGroupWrapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionGroupWrapper {
    pub fn new() -> Self {
        SubscriptionGroupWrapper {
            subscription_group_table: DashMap::with_capacity(1024),
            data_version: DataVersion::default(),
        }
    }

    pub fn get_subscription_group_table(&self) -> &DashMap<CheetahString, SubscriptionGroupConfig> {
        &self.subscription_group_table
    }

    pub fn set_subscription_group_table(
        &mut self,
        table: DashMap<CheetahString, SubscriptionGroupConfig>,
    ) {
        self.subscription_group_table = table;
    }

    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }

    pub fn set_data_version(&mut self, version: DataVersion) {
        self.data_version = version;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

    #[test]
    fn new_creates_wrapper_with_default_values() {
        let wrapper = SubscriptionGroupWrapper::new();

        assert_eq!(wrapper.subscription_group_table.len(), 0);
        assert!(wrapper.data_version.timestamp <= DataVersion::default().timestamp);
    }

    #[test]
    fn get_subscription_group_table_returns_reference() {
        let wrapper = SubscriptionGroupWrapper::new();
        wrapper
            .subscription_group_table
            .insert("test_group".into(), SubscriptionGroupConfig::default());

        let table = wrapper.get_subscription_group_table();
        assert_eq!(table.len(), 1);
        assert!(table.contains_key("test_group"));
    }
}
