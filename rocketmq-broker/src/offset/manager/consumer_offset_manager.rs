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
use std::net::SocketAddr;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::message_store::default_message_store::DefaultMessageStore;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use crate::broker_path_config_helper::get_consumer_offset_path;

pub const TOPIC_GROUP_SEPARATOR: &str = "@";

#[derive(Default, Clone)]
pub(crate) struct ConsumerOffsetManager {
    pub(crate) broker_config: Arc<BrokerConfig>,
    consumer_offset_wrapper: Arc<parking_lot::Mutex<ConsumerOffsetWrapper>>,
    message_store: Option<Arc<DefaultMessageStore>>,
}

impl ConsumerOffsetManager {
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        message_store: Option<Arc<DefaultMessageStore>>,
    ) -> Self {
        ConsumerOffsetManager {
            broker_config,
            consumer_offset_wrapper: Arc::new(parking_lot::Mutex::new(ConsumerOffsetWrapper {
                data_version: DataVersion::default(),
                offset_table: HashMap::new(),
                reset_offset_table: HashMap::new(),
                pull_offset_table: HashMap::new(),
                version_change_counter: Arc::new(AtomicI64::new(0)),
            })),
            message_store,
        }
    }
    pub fn set_message_store(&mut self, message_store: Option<Arc<DefaultMessageStore>>) {
        self.message_store = message_store;
    }
}

impl ConsumerOffsetManager {
    pub fn commit_offset(
        &self,
        client_host: SocketAddr,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
    ) {
        let key = format!("{}{}{}", topic, TOPIC_GROUP_SEPARATOR, group);
        let mut offset_table = self.consumer_offset_wrapper.lock();
        let map = offset_table
            .offset_table
            .entry(key.to_string())
            .or_default();
        let store_offset = map.insert(queue_id, offset);
        if let Some(store_offset) = store_offset {
            if offset < store_offset {
                warn!(
                    "[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, \
                     queueId={}, requestOffset={}, storeOffset={}",
                    client_host, key, queue_id, offset, store_offset
                );
            }
        }
        let _ = offset_table
            .version_change_counter
            .fetch_add(1, Ordering::Release);
        if offset_table.version_change_counter.load(Ordering::Acquire)
            % self.broker_config.consumer_offset_update_version_step
            == 0
        {
            let state_machine_version = if let Some(ref message_store) = self.message_store {
                message_store.get_state_machine_version()
            } else {
                0
            };
            offset_table
                .data_version
                .next_version_with(state_machine_version);
        }
    }

    pub fn has_offset_reset(&self, group: &str, topic: &str, queue_id: i32) -> bool {
        let key = format!("{}{}{}", topic, TOPIC_GROUP_SEPARATOR, group);
        match self
            .consumer_offset_wrapper
            .lock()
            .reset_offset_table
            .get(key.as_str())
        {
            None => false,
            Some(inner) => inner.contains_key(&queue_id),
        }
    }

    pub fn query_offset(&self, group: &str, topic: &str, queue_id: i32) -> i64 {
        let key = format!("{}{}{}", topic, TOPIC_GROUP_SEPARATOR, group);
        if self.broker_config.use_server_side_reset_offset {
            if let Some(value) = self
                .consumer_offset_wrapper
                .lock()
                .reset_offset_table
                .get(key.as_str())
            {
                if value.contains_key(&queue_id) {
                    return value.get(&queue_id).cloned().unwrap_or(-1);
                }
            }
        }
        if let Some(value) = self
            .consumer_offset_wrapper
            .lock()
            .offset_table
            .get(key.as_str())
        {
            if value.contains_key(&queue_id) {
                return value.get(&queue_id).cloned().unwrap_or(-1);
            }
        }
        -1
    }
}

impl ConfigManager for ConsumerOffsetManager {
    fn config_file_path(&self) -> String {
        get_consumer_offset_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        let wrapper = &*self.consumer_offset_wrapper.lock();
        if pretty_format {
            wrapper.to_json_pretty()
        } else {
            wrapper.to_json()
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let wrapper =
            serde_json::from_str::<ConsumerOffsetWrapper>(json_string).unwrap_or_default();
        if !wrapper.offset_table.is_empty() {
            self.consumer_offset_wrapper
                .lock()
                .offset_table
                .clone_from(&wrapper.offset_table);
            self.consumer_offset_wrapper.lock().data_version = wrapper.data_version.clone();
        }
    }
}

#[allow(unused_variables)]
impl ConsumerOffsetManager {
    pub fn query_then_erase_reset_offset(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
    ) -> Option<i64> {
        let key = format!("{}{}{}", topic, TOPIC_GROUP_SEPARATOR, group);
        let mut mutex_guard = self.consumer_offset_wrapper.lock();
        let offset_table = mutex_guard.reset_offset_table.get_mut(key.as_str());
        match offset_table {
            None => None,
            Some(value) => value.remove(&queue_id),
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
    #[serde(skip)]
    version_change_counter: Arc<AtomicI64>,
}

impl RemotingSerializable for ConsumerOffsetWrapper {
    type Output = Self;
}
