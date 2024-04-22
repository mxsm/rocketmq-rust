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

use crate::client::client_channel_info::ClientChannelInfo;

#[derive(Default)]
pub struct ProducerManager {
    group_channel_table:
        parking_lot::Mutex<HashMap<String /* group name */, HashMap<String, ClientChannelInfo>>>,
    client_channel_table: parking_lot::Mutex<HashMap<String, String /* client ip:port */>>,
}

impl ProducerManager {
    pub fn new() -> Self {
        Self {
            group_channel_table: parking_lot::Mutex::new(HashMap::new()),
            client_channel_table: parking_lot::Mutex::new(HashMap::new()),
        }
    }
}

impl ProducerManager {
    pub fn group_online(&self, group: String) -> bool {
        let binding = self.group_channel_table.lock();
        let channels = binding.get(&group);
        if channels.is_none() {
            return false;
        }
        channels.unwrap().len() > 0
    }

    pub fn unregister_producer(&self, _group: &String, _client_channel_info: &ClientChannelInfo) {}
}
