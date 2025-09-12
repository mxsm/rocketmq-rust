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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::FileUtils;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::info;

use crate::bootstrap::NameServerRuntimeInner;
use crate::kvconfig::KVConfigSerializeWrapper;

pub struct KVConfigManager {
    pub(crate) config_table: Arc<
        dashmap::DashMap<
            CheetahString, /* Namespace */
            HashMap<CheetahString /* Key */, CheetahString /* Value */>,
        >,
    >,
    pub(crate) name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
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
    pub(crate) fn new(
        name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
    ) -> KVConfigManager {
        KVConfigManager {
            config_table: Arc::new(dashmap::DashMap::new()),
            name_server_runtime_inner,
        }
    }

    /// Gets a reference to the configuration table.
    ///
    /// # Returns
    ///
    /// A reference to the configuration table.
    pub fn get_config_table(
        &self,
    ) -> &dashmap::DashMap<CheetahString, HashMap<CheetahString, CheetahString>> {
        &self.config_table
    }

    /// Gets a reference to the Namesrv configuration.
    ///
    /// # Returns
    ///
    /// A reference to the Namesrv configuration.
    pub fn get_namesrv_config(&self) -> &NamesrvConfig {
        self.name_server_runtime_inner.name_server_config()
    }
}

impl KVConfigManager {
    /// Loads key-value configurations from a file.
    pub fn load(&mut self) -> RocketMQResult<()> {
        let result = FileUtils::file_to_string(
            self.name_server_runtime_inner
                .name_server_config()
                .kv_config_path
                .as_str(),
        );
        if let Ok(content) = result {
            if content.is_empty() {
                return Ok(());
            }
            let wrapper = KVConfigSerializeWrapper::decode(content.as_bytes())?;
            if let Some(config_table) = wrapper.config_table {
                for (key, value) in config_table {
                    self.config_table.insert(key, value);
                }
                info!("load KV config success");
            }
        }
        Ok(())
    }

    /// Updates the Namesrv configuration.
    pub fn update_namesrv_config(
        &mut self,
        updates: HashMap<CheetahString, CheetahString>,
    ) -> Result<(), String> {
        self.name_server_runtime_inner
            .name_server_config_mut()
            .update(updates)
    }

    /// Persists the current key-value configurations to a file.
    pub fn persist(&mut self) {
        let wrapper =
            KVConfigSerializeWrapper::new_with_config_table(self.config_table.as_ref().clone());
        let content = serde_json::to_string(&wrapper).unwrap();

        let result = FileUtils::string_to_file(
            content.as_str(),
            self.name_server_runtime_inner
                .name_server_config()
                .kv_config_path
                .as_str(),
        );
        if let Err(err) = result {
            error!("persist KV config failed: {}", err);
        }
    }

    /// Adds or updates a key-value configuration.
    pub fn put_kv_config(
        &mut self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) {
        {
            // use {} to drop namespace_entry
            let mut namespace_entry = self.config_table.entry(namespace.clone()).or_default();
            let pre_value = namespace_entry.insert(key.clone(), value.clone());
            match pre_value {
                None => {
                    info!(
                        "putKVConfig create new config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value
                    )
                }
                Some(_) => {
                    info!(
                        "putKVConfig update config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value
                    )
                }
            }
        }
        self.persist();
    }

    /// Deletes a key-value configuration.
    pub fn delete_kv_config(&mut self, namespace: &CheetahString, key: &CheetahString) {
        let pre_value = match self.config_table.get_mut(namespace) {
            None => return,
            Some(mut table) => table.remove(key),
        };
        match pre_value {
            None => {}
            Some(value) => {
                info!(
                    "deleteKVConfig delete a config item, Namespace: {} Key: {} Value: {}",
                    namespace, key, value
                )
            }
        }
        self.persist();
    }

    /// Gets the key-value list for a specific namespace.
    pub fn get_kv_list_by_namespace(&self, namespace: &CheetahString) -> Option<Vec<u8>> {
        self.config_table.get(namespace).map(|kv_table| {
            let table = KVTable {
                table: kv_table.clone(),
            };
            table.encode().expect("encode failed")
        })
    }

    // Gets the value for a specific key in a namespace.
    pub fn get_kvconfig(
        &self,
        namespace: &CheetahString,
        key: &CheetahString,
    ) -> Option<CheetahString> {
        match self.config_table.get(namespace) {
            None => None,
            Some(kv_table) => kv_table.get(key).cloned(),
        }
    }
}

/*#[cfg(test)]
mod tests {
    use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
    use rocketmq_rust::ArcMut;
    use crate::bootstrap::Builder;
    use super::*;

    fn create_kv_config_manager() -> KVConfigManager {
        Builder::new().build().name_server_runtime_inner.kvconfig_manager()
        KVConfigManager::new(namesrv_config)
    }

    #[test]
    fn new_kv_config_manager_initializes_empty_config_table() {
        let manager = create_kv_config_manager();
        assert!(manager.get_config_table().is_empty());
    }

    #[test]
    fn put_kv_config_creates_new_entry() {
        let mut manager = create_kv_config_manager();
        manager.put_kv_config("namespace".into(), "key".into(), "value".into());
        let config_table = manager.get_config_table();
        assert_eq!(config_table["namespace"]["key"], "value");
    }

    #[test]
    fn put_kv_config_updates_existing_entry() {
        let mut manager = create_kv_config_manager();
        manager.put_kv_config("namespace".into(), "key".into(), "value".into());
        manager.put_kv_config("namespace".into(), "key".into(), "new_value".into());
        let config_table = manager.get_config_table();
        assert_eq!(config_table["namespace"]["key"], "new_value");
    }

    #[test]
    fn delete_kv_config_removes_entry() {
        let mut manager = create_kv_config_manager();
        manager.put_kv_config("namespace".into(), "key".into(), "value".into());
        manager.delete_kv_config(&"namespace".into(), &"key".into());
        let config_table = manager.get_config_table();
        assert!(config_table["namespace"].get("key").is_none());
    }

    #[test]
    fn delete_kv_config_does_nothing_if_key_does_not_exist() {
        let mut manager = create_kv_config_manager();
        manager.put_kv_config("namespace".into(), "key".into(), "value".into());
        manager.delete_kv_config(&"namespace".into(), &"non_existent_key".into());
        let config_table = manager.get_config_table();
        assert_eq!(config_table["namespace"]["key"], "value");
    }

    #[test]
    fn get_kv_list_by_namespace_returns_encoded_list() {
        let mut manager = create_kv_config_manager();
        manager.put_kv_config("namespace".into(), "key".into(), "value".into());
        let kv_list = manager.get_kv_list_by_namespace(&"namespace".into());
        assert!(kv_list.is_some());
    }

    #[test]
    fn get_kv_list_by_namespace_returns_none_if_namespace_does_not_exist() {
        let manager = create_kv_config_manager();
        let kv_list = manager.get_kv_list_by_namespace(&"non_existent_namespace".into());
        assert!(kv_list.is_none());
    }

    #[test]
    fn get_kvconfig_returns_value_if_key_exists() {
        let mut manager = create_kv_config_manager();
        manager.put_kv_config("namespace".into(), "key".into(), "value".into());
        let value = manager.get_kvconfig(&"namespace".into(), &"key".into());
        assert_eq!(value, Some("value".into()));
    }

    #[test]
    fn get_kvconfig_returns_none_if_key_does_not_exist() {
        let manager = create_kv_config_manager();
        let value = manager.get_kvconfig(&"namespace".into(), &"non_existent_key".into());
        assert!(value.is_none());
    }
}
*/
