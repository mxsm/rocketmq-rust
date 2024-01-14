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

use rocketmq_common::{common::namesrv::namesrv_config::NamesrvConfig, FileUtils};
use rocketmq_remoting::protocol::{body::kv_table::KVTable, RemotingSerializable};
use tracing::info;

use crate::kvconfig::KVConfigSerializeWrapper;

pub struct KVConfigManager {
    pub(crate) config_table:
        HashMap<String /* Namespace */, HashMap<String /* Key */, String /* Value */>>,

    pub(crate) namesrv_config: NamesrvConfig,
}

impl KVConfigManager {
    /// Creates a new `KVConfigManager` instance.
    ///
    /// # Arguments
    ///
    /// * `namesrv_config` - The configuration for the Namesrv.
    ///
    /// # Returns
    ///
    /// A new `KVConfigManager` instance.
    pub fn new(namesrv_config: NamesrvConfig) -> KVConfigManager {
        KVConfigManager {
            config_table: HashMap::new(),
            namesrv_config,
        }
    }

    /// Gets a reference to the configuration table.
    ///
    /// # Returns
    ///
    /// A reference to the configuration table.
    pub fn get_config_table(&self) -> &HashMap<String, HashMap<String, String>> {
        &self.config_table
    }

    /// Gets a reference to the Namesrv configuration.
    ///
    /// # Returns
    ///
    /// A reference to the Namesrv configuration.
    pub fn get_namesrv_config(&self) -> &NamesrvConfig {
        &self.namesrv_config
    }
}

impl KVConfigManager {
    /// Loads key-value configurations from a file.
    pub fn load(&mut self) {
        let result = FileUtils::file_to_string(self.namesrv_config.kv_config_path.as_str());
        if let Ok(content) = result {
            let wrapper = KVConfigSerializeWrapper::decode(content.as_bytes());
            if let Some(ref config_table) = wrapper.config_table {
                for (namespace, config) in config_table {
                    self.config_table.insert(namespace.clone(), config.clone());
                }
                info!("load KV config success");
            }
        }
    }

    /// Persists the current key-value configurations to a file.
    pub fn persist(&mut self) {
        let wrapper = KVConfigSerializeWrapper::new_with_config_table(self.config_table.clone());
        let content = serde_json::to_string(&wrapper).unwrap();

        let result = FileUtils::string_to_file(
            self.namesrv_config.kv_config_path.as_str(),
            content.as_str(),
        );
        if let Err(err) = result {
            info!("persist KV config failed: {}", err);
        }
    }

    /// Adds or updates a key-value configuration.
    pub fn put_kv_config(
        &mut self,
        namespace: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) {
        let namespace_inner = namespace.into();
        if self.config_table.get(namespace_inner.as_str()).is_none() {
            self.config_table
                .insert(namespace_inner.clone(), HashMap::new());
        }

        let key = key.into();
        let value = value.into();
        let pre_value = self
            .config_table
            .get_mut(namespace_inner.as_str())
            .unwrap()
            .insert(key.clone(), value.clone());
        match pre_value {
            None => {
                info!(
                    "putKVConfig create new config item, Namespace: {} Key: {} Value: {}",
                    namespace_inner, key, value
                )
            }
            Some(_) => {
                info!(
                    "putKVConfig update config item, Namespace: {} Key: {} Value: {}",
                    namespace_inner, key, value
                )
            }
        }
        self.persist();
    }

    /// Deletes a key-value configuration.
    pub fn delete_kv_config(&mut self, namespace: impl Into<String>, key: impl Into<String>) {
        let namespace_inner = namespace.into();
        if self.config_table.get(namespace_inner.as_str()).is_none() {
            return;
        }

        let key = key.into();
        let pre_value = self
            .config_table
            .get_mut(namespace_inner.as_str())
            .unwrap()
            .remove(key.as_str());
        match pre_value {
            None => {}
            Some(value) => {
                info!(
                    "deleteKVConfig delete a config item, Namespace: {} Key: {} Value: {}",
                    namespace_inner, key, value
                )
            }
        }
        self.persist();
    }

    /// Gets the key-value list for a specific namespace.
    pub fn get_kv_list_by_namespace(&mut self, namespace: impl Into<String>) -> Option<Vec<u8>> {
        let namespace_inner = namespace.into();
        match self.config_table.get(namespace_inner.as_str()) {
            None => None,
            Some(kv_table) => {
                let table = KVTable {
                    table: kv_table.clone(),
                };
                Some(table.encode())
            }
        }
    }

    // Gets the value for a specific key in a namespace.
    pub fn get_kvconfig(
        &self,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Option<String> {
        match self.config_table.get(namespace.into().as_str()) {
            None => None,
            Some(kv_table) => {
                if let Some(value) = kv_table.get(key.into().as_str()) {
                    return Some(value.clone());
                }
                None
            }
        }
    }
}
