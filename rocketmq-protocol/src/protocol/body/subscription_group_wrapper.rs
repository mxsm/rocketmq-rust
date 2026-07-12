// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use crate::protocol::DataVersion;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionGroupWrapper {
    pub subscription_group_table: HashMap<CheetahString, SubscriptionGroupConfig>,
    pub forbidden_table: HashMap<CheetahString, HashMap<CheetahString, i32>>,
    pub data_version: DataVersion,
}

impl SubscriptionGroupWrapper {
    pub fn get_subscription_group_table(&self) -> &HashMap<CheetahString, SubscriptionGroupConfig> {
        &self.subscription_group_table
    }

    pub fn get_subscription_group_table_mut(&mut self) -> &mut HashMap<CheetahString, SubscriptionGroupConfig> {
        &mut self.subscription_group_table
    }

    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }
}
