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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::FileUtils;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::bootstrap::NameServerRuntimeInner;
use crate::kvconfig::KVConfigSerializeWrapper;

/// Type alias for namespace in the KV configuration
type Namespace = CheetahString;
/// Type alias for key in the KV configuration
type Key = CheetahString;
/// Type alias for value in the KV configuration
type Value = CheetahString;
/// Type alias for the configuration map structure
type ConfigMap = HashMap<Key, Value>;

/// Threshold for triggering automatic persistence (number of pending changes)
const AUTO_PERSIST_THRESHOLD: usize = 100;
/// Minimum interval between automatic persistence operations
const MIN_PERSIST_INTERVAL: Duration = Duration::from_secs(1);

/// KV Configuration Manager
///
/// Manages key-value configurations with namespace support.
pub struct KVConfigManager {
    /// Configuration storage: namespace -> (key -> value)
    pub(crate) config_table: Arc<dashmap::DashMap<Namespace, ConfigMap>>,
    /// Runtime inner for accessing configuration
    pub(crate) name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
    /// Counter for pending changes since last persistence
    pending_changes: Arc<std::sync::atomic::AtomicUsize>,
    /// Last persistence timestamp
    last_persist_time: Arc<parking_lot::Mutex<Instant>>,
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
    pub(crate) fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
        Self {
            config_table: Arc::new(dashmap::DashMap::with_capacity(64)),
            name_server_runtime_inner,
            pending_changes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            last_persist_time: Arc::new(parking_lot::Mutex::new(Instant::now())),
        }
    }

    /// Gets a reference to the configuration table.
    ///
    /// # Returns
    ///
    /// A reference to the configuration table.
    #[inline]
    pub fn get_config_table(&self) -> &dashmap::DashMap<Namespace, ConfigMap> {
        &self.config_table
    }

    /// Gets a reference to the Namesrv configuration.
    ///
    /// # Returns
    ///
    /// A reference to the Namesrv configuration.
    #[inline]
    pub fn get_namesrv_config(&self) -> &NamesrvConfig {
        self.name_server_runtime_inner.name_server_config()
    }

    /// Gets the number of pending changes since last persistence
    #[inline]
    pub fn pending_changes(&self) -> usize {
        self.pending_changes.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl KVConfigManager {
    /// Loads key-value configurations from a file.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if loading succeeds or file doesn't exist
    /// - `Err(RocketMQError)` if deserialization fails
    pub fn load(&mut self) -> RocketMQResult<()> {
        let config_path = self
            .name_server_runtime_inner
            .name_server_config()
            .kv_config_path
            .as_str();

        let content = match FileUtils::file_to_string(config_path) {
            Ok(content) if !content.is_empty() => content,
            Ok(_) => {
                debug!("KV config file is empty, skipping load");
                return Ok(());
            }
            Err(_) => {
                debug!("KV config file not found, skipping load");
                return Ok(());
            }
        };

        let wrapper = KVConfigSerializeWrapper::decode(content.as_bytes()).map_err(|e| {
            error!("Failed to decode KV config: {}", e);
            e
        })?;

        if let Some(config_table) = wrapper.config_table {
            let entry_count = config_table.len();
            for (namespace, kv_map) in config_table {
                self.config_table.insert(namespace, kv_map);
            }
            info!("Loaded {} namespace(s) from KV config file", entry_count);
        }

        // Reset pending changes after successful load
        self.pending_changes.store(0, std::sync::atomic::Ordering::Relaxed);
        *self.last_persist_time.lock() = Instant::now();

        Ok(())
    }

    /// Updates the Namesrv configuration.
    ///
    /// # Arguments
    ///
    /// * `updates` - Map of configuration key-value pairs to update
    ///
    /// # Returns
    ///
    /// - `Ok(())` if update succeeds
    /// - `Err(String)` if update fails (legacy API)
    pub fn update_namesrv_config(&mut self, updates: HashMap<CheetahString, CheetahString>) -> Result<(), String> {
        let result = self.name_server_runtime_inner.name_server_config_mut().update(updates);

        if result.is_ok() {
            debug!("Namesrv configuration updated successfully");
        }

        result
    }

    /// Persists the current key-value configurations to a file.
    ///
    /// This method performs actual file I/O. For better performance,
    /// consider using `persist_if_needed()` which applies deferred persistence.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if persistence succeeds
    /// - `Err(RocketMQError)` if serialization or file write fails
    pub fn persist(&mut self) -> RocketMQResult<()> {
        let config_path = self
            .name_server_runtime_inner
            .name_server_config()
            .kv_config_path
            .as_str();

        // Create a snapshot to minimize lock time
        let snapshot: dashmap::DashMap<Namespace, ConfigMap> = self
            .config_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let wrapper = KVConfigSerializeWrapper::new_with_config_table(snapshot);

        let content = wrapper.serialize_json_pretty()?;

        FileUtils::string_to_file(content.as_str(), config_path).map_err(|e| {
            error!("Failed to persist KV config to {}: {}", config_path, e);
            RocketMQError::storage_write_failed(config_path, format!("Write failed: {}", e))
        })?;

        // Reset counters after successful persistence
        self.pending_changes.store(0, std::sync::atomic::Ordering::Relaxed);
        *self.last_persist_time.lock() = Instant::now();

        debug!("KV config persisted successfully to {}", config_path);
        Ok(())
    }

    /// Conditionally persists configuration based on pending changes and time threshold.
    ///
    /// This method implements deferred persistence to reduce I/O operations:
    /// - Persists if pending changes exceed AUTO_PERSIST_THRESHOLD
    /// - Persists if MIN_PERSIST_INTERVAL has elapsed since last persistence
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if persistence was performed
    /// - `Ok(false)` if persistence was skipped
    /// - `Err(RocketMQError)` if persistence fails
    pub fn persist_if_needed(&mut self) -> RocketMQResult<bool> {
        let pending = self.pending_changes.load(std::sync::atomic::Ordering::Relaxed);
        let elapsed = self.last_persist_time.lock().elapsed();

        if pending >= AUTO_PERSIST_THRESHOLD || elapsed >= MIN_PERSIST_INTERVAL {
            self.persist()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Forces immediate persistence regardless of thresholds.
    ///
    /// This should be called when shutting down or when immediate
    /// durability is required.
    #[inline]
    pub fn force_persist(&mut self) -> RocketMQResult<()> {
        self.persist()
    }

    /// Adds or updates a key-value configuration.
    ///
    /// Uses deferred persistence to reduce I/O operations.
    /// Call `persist_if_needed()` or `force_persist()` to save changes.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for the configuration
    /// * `key` - The configuration key
    /// * `value` - The configuration value
    pub fn put_kv_config(&mut self, namespace: Namespace, key: Key, value: Value) -> RocketMQResult<()> {
        let is_new = {
            let mut namespace_entry = self.config_table.entry(namespace.clone()).or_default();
            let pre_value = namespace_entry.insert(key.clone(), value.clone());
            pre_value.is_none()
        };

        // Increment pending changes counter
        self.pending_changes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if is_new {
            debug!(
                "Created new KV config: namespace={}, key={}, value={}",
                namespace, key, value
            );
        } else {
            debug!(
                "Updated KV config: namespace={}, key={}, value={}",
                namespace, key, value
            );
        }

        // Auto-persist if threshold reached
        self.persist_if_needed()?;
        Ok(())
    }

    /// Deletes a key-value configuration.
    ///
    /// Uses deferred persistence to reduce I/O operations.
    /// Call `persist_if_needed()` or `force_persist()` to save changes.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for the configuration
    /// * `key` - The configuration key to delete
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if the key was deleted
    /// - `Ok(false)` if the key didn't exist
    pub fn delete_kv_config(&mut self, namespace: &Namespace, key: &Key) -> RocketMQResult<bool> {
        let deleted_value = self
            .config_table
            .get_mut(namespace)
            .and_then(|mut table| table.remove(key));

        if let Some(value) = deleted_value {
            // Increment pending changes counter
            self.pending_changes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            debug!(
                "Deleted KV config: namespace={}, key={}, value={}",
                namespace, key, value
            );

            // Auto-persist if threshold reached
            self.persist_if_needed()?;
            Ok(true)
        } else {
            debug!("KV config not found for deletion: namespace={}, key={}", namespace, key);
            Ok(false)
        }
    }

    /// Batch update multiple key-value configurations in a namespace.
    ///
    /// This is more efficient than calling `put_kv_config` multiple times
    /// as it performs a single persistence check at the end.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for all configurations
    /// * `kv_pairs` - Map of key-value pairs to insert/update
    ///
    /// # Returns
    ///
    /// - `Ok(usize)` - Number of entries updated
    pub fn batch_put_kv_config(
        &mut self,
        namespace: Namespace,
        kv_pairs: HashMap<Key, Value>,
    ) -> RocketMQResult<usize> {
        if kv_pairs.is_empty() {
            return Ok(0);
        }

        let count = kv_pairs.len();
        {
            let mut namespace_entry = self.config_table.entry(namespace.clone()).or_default();
            for (key, value) in kv_pairs {
                namespace_entry.insert(key, value);
            }
        }

        // Increment pending changes counter
        self.pending_changes
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);

        debug!("Batch updated {} KV configs in namespace={}", count, namespace);

        // Auto-persist if threshold reached
        self.persist_if_needed()?;
        Ok(count)
    }

    /// Batch delete multiple key-value configurations in a namespace.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for the configurations
    /// * `keys` - Keys to delete
    ///
    /// # Returns
    ///
    /// - `Ok(usize)` - Number of entries deleted
    pub fn batch_delete_kv_config(&mut self, namespace: &Namespace, keys: &[Key]) -> RocketMQResult<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let mut deleted_count = 0;
        if let Some(mut namespace_entry) = self.config_table.get_mut(namespace) {
            for key in keys {
                if namespace_entry.remove(key).is_some() {
                    deleted_count += 1;
                }
            }
        }

        if deleted_count > 0 {
            // Increment pending changes counter
            self.pending_changes
                .fetch_add(deleted_count, std::sync::atomic::Ordering::Relaxed);

            debug!(
                "Batch deleted {} KV configs from namespace={}",
                deleted_count, namespace
            );

            // Auto-persist if threshold reached
            self.persist_if_needed()?;
        }

        Ok(deleted_count)
    }

    /// Deletes an entire namespace and all its key-value configurations.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to delete
    ///
    /// # Returns
    ///
    /// - `Ok(usize)` - Number of keys deleted
    pub fn delete_namespace(&mut self, namespace: &Namespace) -> RocketMQResult<usize> {
        if let Some((_, kv_map)) = self.config_table.remove(namespace) {
            let count = kv_map.len();

            // Increment pending changes counter
            self.pending_changes
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);

            info!("Deleted namespace={} with {} configs", namespace, count);

            // Auto-persist if threshold reached
            self.persist_if_needed()?;
            Ok(count)
        } else {
            debug!("Namespace not found for deletion: {}", namespace);
            Ok(0)
        }
    }

    /// Gets the key-value list for a specific namespace.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to retrieve
    ///
    /// # Returns
    ///
    /// - `Some(Vec<u8>)` - Encoded KV table if namespace exists
    /// - `None` - If namespace doesn't exist
    pub fn get_kv_list_by_namespace(&self, namespace: &Namespace) -> Option<Vec<u8>> {
        self.config_table.get(namespace).and_then(|kv_table| {
            let table = KVTable {
                table: kv_table.clone(),
            };
            match table.encode() {
                Ok(encoded) => Some(encoded),
                Err(e) => {
                    error!("Failed to encode KV table for namespace {}: {}", namespace, e);
                    None
                }
            }
        })
    }

    /// Gets the value for a specific key in a namespace.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace
    /// * `key` - The configuration key
    ///
    /// # Returns
    ///
    /// - `Some(Value)` - The value if found
    /// - `None` - If namespace or key doesn't exist
    #[inline]
    pub fn get_kvconfig(&self, namespace: &Namespace, key: &Key) -> Option<Value> {
        self.config_table
            .get(namespace)
            .and_then(|kv_table| kv_table.get(key).cloned())
    }

    /// Checks if a namespace exists.
    #[inline]
    pub fn namespace_exists(&self, namespace: &Namespace) -> bool {
        self.config_table.contains_key(namespace)
    }

    /// Gets all namespaces.
    ///
    /// # Returns
    ///
    /// A vector of all namespace names
    pub fn get_all_namespaces(&self) -> Vec<Namespace> {
        self.config_table.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Gets the number of key-value pairs in a namespace.
    ///
    /// # Returns
    ///
    /// - `Some(usize)` - Number of keys if namespace exists
    /// - `None` - If namespace doesn't exist
    pub fn get_namespace_size(&self, namespace: &Namespace) -> Option<usize> {
        self.config_table.get(namespace).map(|kv_map| kv_map.len())
    }

    /// Gets all key-value pairs in a namespace.
    ///
    /// # Returns
    ///
    /// - `Some(HashMap)` - Clone of the KV map if namespace exists
    /// - `None` - If namespace doesn't exist
    pub fn get_all_in_namespace(&self, namespace: &Namespace) -> Option<ConfigMap> {
        self.config_table.get(namespace).map(|kv_map| kv_map.clone())
    }

    /// Gets statistics about the configuration manager.
    ///
    /// # Returns
    ///
    /// A tuple of (namespace_count, total_kv_pairs, pending_changes)
    pub fn get_statistics(&self) -> (usize, usize, usize) {
        let namespace_count = self.config_table.len();
        let total_kv_pairs: usize = self.config_table.iter().map(|entry| entry.value().len()).sum();
        let pending = self.pending_changes.load(std::sync::atomic::Ordering::Relaxed);

        (namespace_count, total_kv_pairs, pending)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use cheetah_string::CheetahString;

    use super::*;

    /// Tests for the internal config_table (DashMap) operations
    /// These tests don't require full KVConfigManager setup

    #[test]
    fn test_config_table_basic_operations() {
        let config_table: Arc<dashmap::DashMap<Namespace, ConfigMap>> = Arc::new(dashmap::DashMap::with_capacity(64));

        // Test empty state
        assert_eq!(config_table.len(), 0);

        // Test insert
        let ns = CheetahString::from_static_str("test_namespace");
        let mut kv_map = HashMap::new();
        kv_map.insert(
            CheetahString::from_static_str("key1"),
            CheetahString::from_static_str("value1"),
        );
        config_table.insert(ns.clone(), kv_map);

        assert_eq!(config_table.len(), 1);
        assert!(config_table.contains_key(&ns));

        // Test get
        let retrieved = config_table.get(&ns);
        assert!(retrieved.is_some());
        let map = retrieved.unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("key1")),
            Some(&CheetahString::from_static_str("value1"))
        );
    }

    #[test]
    fn test_config_table_namespace_isolation() {
        let config_table: Arc<dashmap::DashMap<Namespace, ConfigMap>> = Arc::new(dashmap::DashMap::new());

        let ns1 = CheetahString::from_static_str("namespace1");
        let ns2 = CheetahString::from_static_str("namespace2");
        let key = CheetahString::from_static_str("same_key");

        // Add same key to different namespaces
        let mut map1 = HashMap::new();
        map1.insert(key.clone(), CheetahString::from_static_str("value1"));
        config_table.insert(ns1.clone(), map1);

        let mut map2 = HashMap::new();
        map2.insert(key.clone(), CheetahString::from_static_str("value2"));
        config_table.insert(ns2.clone(), map2);

        // Verify isolation
        let map1_retrieved = config_table.get(&ns1).unwrap();
        let map2_retrieved = config_table.get(&ns2).unwrap();

        assert_eq!(
            map1_retrieved.get(&key),
            Some(&CheetahString::from_static_str("value1"))
        );
        assert_eq!(
            map2_retrieved.get(&key),
            Some(&CheetahString::from_static_str("value2"))
        );
    }

    #[test]
    fn test_config_table_concurrent_access() {
        let config_table: Arc<dashmap::DashMap<Namespace, ConfigMap>> = Arc::new(dashmap::DashMap::new());

        let ns1 = CheetahString::from_static_str("namespace1");
        let ns2 = CheetahString::from_static_str("namespace2");

        // Add configs to different namespaces
        let mut map1 = HashMap::new();
        map1.insert(
            CheetahString::from_static_str("key1"),
            CheetahString::from_static_str("value1"),
        );
        config_table.insert(ns1.clone(), map1);

        let mut map2 = HashMap::new();
        map2.insert(
            CheetahString::from_static_str("key2"),
            CheetahString::from_static_str("value2"),
        );
        config_table.insert(ns2.clone(), map2);

        assert_eq!(config_table.len(), 2);
    }

    #[test]
    fn test_config_table_update() {
        let config_table: Arc<dashmap::DashMap<Namespace, ConfigMap>> = Arc::new(dashmap::DashMap::new());

        let ns = CheetahString::from_static_str("test_namespace");
        let key = CheetahString::from_static_str("test_key");

        // Insert initial value
        let mut map = HashMap::new();
        map.insert(key.clone(), CheetahString::from_static_str("value1"));
        config_table.insert(ns.clone(), map);

        // Update value
        {
            let mut entry = config_table.get_mut(&ns).unwrap();
            entry.insert(key.clone(), CheetahString::from_static_str("value2"));
        }

        // Verify update
        let entry = config_table.get(&ns).unwrap();
        assert_eq!(entry.get(&key), Some(&CheetahString::from_static_str("value2")));
    }

    #[test]
    fn test_config_table_remove() {
        let config_table: Arc<dashmap::DashMap<Namespace, ConfigMap>> = Arc::new(dashmap::DashMap::new());

        let ns = CheetahString::from_static_str("test_namespace");
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("key1"),
            CheetahString::from_static_str("value1"),
        );
        config_table.insert(ns.clone(), map);

        assert!(config_table.contains_key(&ns));

        // Remove namespace
        config_table.remove(&ns);

        assert!(!config_table.contains_key(&ns));
        assert_eq!(config_table.len(), 0);
    }

    #[test]
    fn test_config_table_iter() {
        let config_table: Arc<dashmap::DashMap<Namespace, ConfigMap>> = Arc::new(dashmap::DashMap::new());

        // Add multiple namespaces
        for i in 1..=3 {
            let ns = CheetahString::from_string(format!("namespace{}", i));
            let mut map = HashMap::new();
            map.insert(
                CheetahString::from_string(format!("key{}", i)),
                CheetahString::from_string(format!("value{}", i)),
            );
            config_table.insert(ns, map);
        }

        // Test iteration
        let count = config_table.iter().count();
        assert_eq!(count, 3);

        // Collect namespaces
        let namespaces: Vec<_> = config_table.iter().map(|entry| entry.key().clone()).collect();
        assert_eq!(namespaces.len(), 3);
    }

    #[test]
    fn test_pending_changes_atomic() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        let pending = Arc::new(AtomicUsize::new(0));

        // Test atomic operations
        pending.fetch_add(1, Ordering::Relaxed);
        assert_eq!(pending.load(Ordering::Relaxed), 1);

        pending.fetch_add(5, Ordering::Relaxed);
        assert_eq!(pending.load(Ordering::Relaxed), 6);

        pending.store(0, Ordering::Relaxed);
        assert_eq!(pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_constants() {
        // Verify constants are defined with expected values
        assert_eq!(AUTO_PERSIST_THRESHOLD, 100);
        assert_eq!(MIN_PERSIST_INTERVAL.as_secs(), 1);
    }
}
