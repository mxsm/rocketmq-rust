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

use std::sync::Arc;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_store::store_path_config_helper::get_delay_offset_store_path;

#[derive(Default, Clone)]
pub struct ScheduleMessageService {
    pub(crate) broker_config: Arc<BrokerConfig>,
}

impl ScheduleMessageService {
    pub fn delay_level2queue_id(delay_level: i32) -> i32 {
        delay_level - 1
    }
}

impl ScheduleMessageService {
    pub fn get_max_delay_level(&self) -> i32 {
        0
    }
}

impl ConfigManager for ScheduleMessageService {
    fn decode0(&mut self, _key: &[u8], _body: &[u8]) {
        todo!()
    }

    fn stop(&mut self) -> bool {
        todo!()
    }

    fn config_file_path(&self) -> String {
        get_delay_offset_store_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode(&mut self) -> String {
        todo!()
    }

    fn encode_pretty(&self, _pretty_format: bool) -> String {
        todo!()
    }

    fn decode(&self, _json_string: &str) {}
}
