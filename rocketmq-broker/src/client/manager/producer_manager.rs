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

use rocketmq_common::TimeUtils::get_current_millis;
use tracing::info;

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
        !channels.unwrap().is_empty()
    }

    pub fn unregister_producer(&self, group: &str, client_channel_info: &ClientChannelInfo) {
        let mut mutex_guard = self.group_channel_table.lock();
        let channel_table = mutex_guard.get_mut(group);
        if let Some(ct) = channel_table {
            if !ct.is_empty() {
                let old = ct.remove(client_channel_info.socket_addr());
                if old.is_some() {
                    info!(
                        "unregister a producer[{}] from groupChannelTable {:?}",
                        group, client_channel_info
                    );
                }
            }
            if ct.is_empty() {
                let _ = mutex_guard.remove(group);
                info!(
                    "unregister a producer group[{}] from groupChannelTable",
                    group
                );
            }
        }
    }

    pub fn register_producer(&self, group: &String, client_channel_info: &ClientChannelInfo) {
        let mut group_channel_table = self.group_channel_table.lock();

        let channel_table = group_channel_table.entry(group.to_owned()).or_default();

        if let Some(client_channel_info_found) =
            channel_table.get_mut(client_channel_info.socket_addr())
        {
            client_channel_info_found.set_last_update_timestamp(get_current_millis() as i64);
            return;
        }

        channel_table.insert(
            client_channel_info.socket_addr().clone(),
            client_channel_info.clone(),
        );

        let mut client_channel_table = self.client_channel_table.lock();
        client_channel_table.insert(
            client_channel_info.client_id().clone(),
            client_channel_info.socket_addr().clone(),
        );
    }
}
