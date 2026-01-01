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
use rocketmq_common::common::config::TopicConfig;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::DataVersion;

pub mod topic_config_wrapper;
pub mod topic_queue_wrapper;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct TopicConfigSerializeWrapper {
    #[serde(rename = "topicConfigTable")]
    topic_config_table: Option<HashMap<CheetahString, TopicConfig>>,

    #[serde(rename = "dataVersion")]
    data_version: Option<DataVersion>,
}

impl TopicConfigSerializeWrapper {
    pub fn new(
        topic_config_table: Option<HashMap<CheetahString, TopicConfig>>,
        data_version: Option<DataVersion>,
    ) -> Self {
        Self {
            topic_config_table,
            data_version,
        }
    }
}

impl TopicConfigSerializeWrapper {
    pub fn topic_config_table(&self) -> Option<&HashMap<CheetahString, TopicConfig>> {
        match &self.topic_config_table {
            None => None,
            Some(value) => Some(value),
        }
    }

    pub fn data_version(&self) -> Option<&DataVersion> {
        match &self.data_version {
            None => None,
            Some(value) => Some(value),
        }
    }

    pub fn set_topic_config_table(&mut self, topic_config_table: Option<HashMap<CheetahString, TopicConfig>>) {
        self.topic_config_table = topic_config_table;
    }

    pub fn set_data_version(&mut self, data_version: Option<DataVersion>) {
        self.data_version = data_version;
    }

    pub fn take_topic_config_table(&mut self) -> Option<HashMap<CheetahString, TopicConfig>> {
        self.topic_config_table.take()
    }
}
