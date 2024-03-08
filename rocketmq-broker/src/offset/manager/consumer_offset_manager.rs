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

use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_remoting::protocol::DataVersion;
use serde::{Deserialize, Serialize};

use crate::{broker_config::BrokerConfig, broker_path_config_helper::get_consumer_offset_path};

#[derive(Default)]
pub(crate) struct ConsumerOffsetManager {
    pub(crate) broker_config: Arc<BrokerConfig>,
    consumer_offset_wrapper: ConsumerOffsetWrapper,
}

//Fully implemented will be removed
#[allow(unused_variables)]
impl ConfigManager for ConsumerOffsetManager {
    fn decode0(&mut self, key: &[u8], body: &[u8]) {
        todo!()
    }

    fn stop(&mut self) -> bool {
        todo!()
    }

    fn config_file_path(&mut self) -> String {
        get_consumer_offset_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode(&mut self) -> String {
        todo!()
    }

    fn encode_pretty(&mut self, pretty_format: bool) -> String {
        todo!()
    }

    fn decode(&mut self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let wrapper =
            serde_json::from_str::<ConsumerOffsetWrapper>(json_string).unwrap_or_default();
        if !wrapper.offset_table.is_empty() {
            self.consumer_offset_wrapper
                .offset_table
                .clone_from(&wrapper.offset_table);
            self.consumer_offset_wrapper.data_version = wrapper.data_version.clone();
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ConsumerOffsetWrapper {
    data_version: DataVersion,
    offset_table: HashMap<String /* topic@group */, HashMap<i32, i64>>,
    reset_offset_table: HashMap<String, HashMap<i32, i64>>,
    pull_offset_table: HashMap<String /* topic@group */, HashMap<i32, i64>>,
}
