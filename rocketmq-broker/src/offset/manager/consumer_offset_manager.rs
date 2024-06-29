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

use std::cell::SyncUnsafeCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::message_store::default_message_store::DefaultMessageStore;
use serde::de;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tracing::warn;

use crate::broker_path_config_helper::get_consumer_offset_path;

pub const TOPIC_GROUP_SEPARATOR: &str = "@";

#[derive(Default, Clone)]
pub(crate) struct ConsumerOffsetManager {
    pub(crate) broker_config: Arc<BrokerConfig>,
    consumer_offset_wrapper: ConsumerOffsetWrapper,
    message_store: Option<Arc<DefaultMessageStore>>,
}

impl ConsumerOffsetManager {
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        message_store: Option<Arc<DefaultMessageStore>>,
    ) -> Self {
        ConsumerOffsetManager {
            broker_config,
            consumer_offset_wrapper: ConsumerOffsetWrapper {
                data_version: Arc::new(SyncUnsafeCell::new(DataVersion::default())),
                offset_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
                reset_offset_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
                pull_offset_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
                version_change_counter: Arc::new(AtomicI64::new(0)),
            },
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

        let mut write_guard = self.consumer_offset_wrapper.offset_table.write();
        let map = write_guard.entry(key.to_string()).or_default();
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
        let _ = self
            .consumer_offset_wrapper
            .version_change_counter
            .fetch_add(1, Ordering::Release);
        if self
            .consumer_offset_wrapper
            .version_change_counter
            .load(Ordering::Acquire)
            % self.broker_config.consumer_offset_update_version_step
            == 0
        {
            let state_machine_version = if let Some(ref message_store) = self.message_store {
                message_store.get_state_machine_version()
            } else {
                0
            };
            unsafe { &mut *self.consumer_offset_wrapper.data_version.get() }
                .next_version_with(state_machine_version);
        }
    }

    pub fn has_offset_reset(&self, group: &str, topic: &str, queue_id: i32) -> bool {
        let key = format!("{}{}{}", topic, TOPIC_GROUP_SEPARATOR, group);
        match self
            .consumer_offset_wrapper
            .reset_offset_table
            .read()
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
                .reset_offset_table
                .read()
                .get(key.as_str())
            {
                if value.contains_key(&queue_id) {
                    return value.get(&queue_id).cloned().unwrap_or(-1);
                }
            }
        }
        if let Some(value) = self
            .consumer_offset_wrapper
            .offset_table
            .read()
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
        if pretty_format {
            self.consumer_offset_wrapper.to_json_pretty()
        } else {
            self.consumer_offset_wrapper.to_json()
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let wrapper = SerdeJsonUtils::from_json_str::<ConsumerOffsetWrapper>(json_string).unwrap();
        if !wrapper.offset_table.read().is_empty() {
            self.consumer_offset_wrapper
                .offset_table
                .write()
                .extend(wrapper.offset_table.read().clone());
            let data_version = unsafe { &mut *self.consumer_offset_wrapper.data_version.get() };
            *data_version = unsafe { &*wrapper.data_version.get() }.clone();
        }
    }
}

#[allow(unused_variables)]
impl ConsumerOffsetManager {
    pub fn commit_pull_offset(
        &self,
        _client_host: SocketAddr,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
    ) {
        let key = format!("{}{}{}", topic, TOPIC_GROUP_SEPARATOR, group);
        self.consumer_offset_wrapper
            .pull_offset_table
            .write()
            .entry(key)
            .or_default()
            .insert(queue_id, offset);
    }

    pub fn query_then_erase_reset_offset(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
    ) -> Option<i64> {
        let key = format!("{}{}{}", topic, TOPIC_GROUP_SEPARATOR, group);
        let mut write_guard = self.consumer_offset_wrapper.reset_offset_table.write();
        let offset_table = write_guard.get_mut(key.as_str());
        match offset_table {
            None => None,
            Some(value) => value.remove(&queue_id),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct ConsumerOffsetWrapper {
    data_version: Arc<SyncUnsafeCell<DataVersion>>,
    offset_table: Arc<parking_lot::RwLock<HashMap<String /* topic@group */, HashMap<i32, i64>>>>,
    reset_offset_table: Arc<parking_lot::RwLock<HashMap<String, HashMap<i32, i64>>>>,
    pull_offset_table:
        Arc<parking_lot::RwLock<HashMap<String /* topic@group */, HashMap<i32, i64>>>>,
    version_change_counter: Arc<AtomicI64>,
}

impl ConsumerOffsetWrapper {
    pub fn get_group_topic_map(&self) -> HashMap<String, HashSet<String>> {
        let mut ret_map: HashMap<String, HashSet<String>> = HashMap::with_capacity(128);

        for key in self.offset_table.read().keys() {
            let arr: Vec<&str> = key.split(TOPIC_GROUP_SEPARATOR).collect();
            if arr.len() == 2 {
                let topic = arr[0].to_string();
                let group = arr[1].to_string();

                ret_map
                    .entry(group)
                    .or_insert_with(|| HashSet::with_capacity(8))
                    .insert(topic);
            }
        }

        ret_map
    }
}

impl Serialize for ConsumerOffsetWrapper {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ConsumerOffsetWrapper", 5)?;
        state.serialize_field("dataVersion", unsafe { &*self.data_version.get() })?;
        state.serialize_field("offsetTable", &*self.offset_table.read())?;
        state.serialize_field("resetOffsetTable", &*self.reset_offset_table.read())?;
        state.serialize_field("pullOffsetTable", &*self.pull_offset_table.read())?;
        state.serialize_field("processedPullOffsetTable", &self.get_group_topic_map())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ConsumerOffsetWrapper {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Debug)]
        enum Field {
            DataVersion,
            OffsetTable,
            ResetOffsetTable,
            PullOffsetTable,
            Ignore,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Field, D::Error> {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str(
                            "`dataVersion`, `offsetTable`, `resetOffsetTable` or `pullOffsetTable`",
                        )
                    }

                    fn visit_str<E: de::Error>(self, value: &str) -> Result<Field, E> {
                        match value {
                            "dataVersion" => Ok(Field::DataVersion),
                            "offsetTable" => Ok(Field::OffsetTable),
                            "resetOffsetTable" => Ok(Field::ResetOffsetTable),
                            "pullOffsetTable" => Ok(Field::PullOffsetTable),
                            _ => Ok(Field::Ignore),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct ConsumerOffsetWrapperVisitor;

        impl<'de> Visitor<'de> for ConsumerOffsetWrapperVisitor {
            type Value = ConsumerOffsetWrapper;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ConsumerOffsetWrapper")
            }

            fn visit_map<V: MapAccess<'de>>(
                self,
                mut map: V,
            ) -> Result<ConsumerOffsetWrapper, V::Error> {
                let mut data_version = None;
                let mut offset_table = None;
                let mut reset_offset_table = None;
                let mut pull_offset_table = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::DataVersion => {
                            if data_version.is_some() {
                                return Err(de::Error::duplicate_field("dataVersion"));
                            }
                            data_version = Some(map.next_value()?);
                        }
                        Field::OffsetTable => {
                            if offset_table.is_some() {
                                return Err(de::Error::duplicate_field("offsetTable"));
                            }
                            offset_table = Some(map.next_value()?);
                        }
                        Field::ResetOffsetTable => {
                            if reset_offset_table.is_some() {
                                return Err(de::Error::duplicate_field("resetOffsetTable"));
                            }
                            reset_offset_table = Some(map.next_value()?);
                        }
                        Field::PullOffsetTable => {
                            if pull_offset_table.is_some() {
                                return Err(de::Error::duplicate_field("pullOffsetTable"));
                            }
                            pull_offset_table = Some(map.next_value()?);
                        }
                        Field::Ignore => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                let data_version = data_version.unwrap_or_default();
                let offset_table = offset_table.unwrap_or_default();
                let reset_offset_table = reset_offset_table.unwrap_or_default();
                let pull_offset_table = pull_offset_table.unwrap_or_default();

                Ok(ConsumerOffsetWrapper {
                    data_version: Arc::new(SyncUnsafeCell::new(data_version)),
                    offset_table: Arc::new(parking_lot::RwLock::new(offset_table)),
                    reset_offset_table: Arc::new(parking_lot::RwLock::new(reset_offset_table)),
                    pull_offset_table: Arc::new(parking_lot::RwLock::new(pull_offset_table)),
                    version_change_counter: Arc::new(AtomicI64::new(0)),
                })
            }
        }

        const FIELDS: &[&str] = &[
            "dataVersion",
            "offsetTable",
            "resetOffsetTable",
            "pullOffsetTable",
        ];
        deserializer.deserialize_struct(
            "ConsumerOffsetWrapper",
            FIELDS,
            ConsumerOffsetWrapperVisitor,
        )
    }
}
