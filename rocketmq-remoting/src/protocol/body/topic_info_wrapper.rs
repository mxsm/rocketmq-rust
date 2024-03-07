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

use rocketmq_common::common::config::TopicConfig;
use serde::{Deserialize, Serialize};

use crate::protocol::{DataVersion, RemotingSerializable};

pub mod topic_config_wrapper;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct TopicConfigSerializeWrapper {
    #[serde(rename = "topicConfigTable")]
    topic_config_table: Option<HashMap<String, TopicConfig>>,

    #[serde(rename = "dataVersion")]
    data_version: Option<DataVersion>,
}

impl TopicConfigSerializeWrapper {
    pub fn topic_config_table(&self) -> Option<&HashMap<String, TopicConfig>> {
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

    pub fn set_topic_config_table(
        &mut self,
        topic_config_table: Option<HashMap<String, TopicConfig>>,
    ) {
        self.topic_config_table = topic_config_table;
    }
    pub fn set_data_version(&mut self, data_version: Option<DataVersion>) {
        self.data_version = data_version;
    }
}

impl RemotingSerializable for TopicConfigSerializeWrapper {
    type Output = Self;

    /*fn decode(bytes: &[u8]) -> Self::Output {
        serde_json::from_slice::<Self::Output>(bytes).unwrap()
    }

    fn encode(&self, _compress: bool) -> Vec<u8> {
        todo!()
    }*/
}
