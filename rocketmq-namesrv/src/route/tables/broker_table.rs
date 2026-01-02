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

//! Broker address table with concurrent access
//!
//! Manages broker name -> broker data mappings.

use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

use crate::route::types::BrokerName;

/// Broker address table: BrokerName -> BrokerData
///
/// This table maintains the mapping of broker names to their configurations
/// including cluster membership and broker addresses (master/slaves).
///
/// # Performance
/// - Read operations: O(1) average, lock-free
/// - Write operations: O(1) average, per-entry lock
///
/// # Example
/// ```no_run
/// use std::sync::Arc;
///
/// use rocketmq_namesrv::route::tables::BrokerAddrTable;
///
/// let table = BrokerAddrTable::new();
/// // Thread-safe operations without explicit locking
/// ```
#[derive(Clone)]
pub struct BrokerAddrTable {
    inner: DashMap<BrokerName, Arc<BrokerData>>,
}

impl BrokerAddrTable {
    /// Create a new broker address table
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Create with estimated capacity
    ///
    /// # Arguments
    /// * `capacity` - Expected number of brokers
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(capacity),
        }
    }

    /// Insert or update broker data
    ///
    /// # Arguments
    /// * `broker_name` - Broker name (zero-copy Arc<str>)
    /// * `broker_data` - Broker configuration including addresses
    ///
    /// # Returns
    /// Previous broker data if existed
    pub fn insert(&self, broker_name: BrokerName, broker_data: BrokerData) -> Option<Arc<BrokerData>> {
        self.inner.insert(broker_name, Arc::new(broker_data))
    }

    /// Get broker data by name
    ///
    /// # Arguments
    /// * `broker_name` - Broker name
    ///
    /// # Returns
    /// Cloned Arc to broker data if exists
    pub fn get(&self, broker_name: &str) -> Option<Arc<BrokerData>> {
        self.inner.get(broker_name).map(|entry| Arc::clone(entry.value()))
    }

    /// Remove broker by name
    ///
    /// # Arguments
    /// * `broker_name` - Broker name
    ///
    /// # Returns
    /// Removed broker data if existed
    pub fn remove(&self, broker_name: &str) -> Option<Arc<BrokerData>> {
        self.inner.remove(broker_name).map(|(_, v)| v)
    }

    /// Check if broker exists
    pub fn contains(&self, broker_name: &str) -> bool {
        self.inner.contains_key(broker_name)
    }

    /// Get all broker names
    ///
    /// # Returns
    /// Vector of broker names (CheetahString for zero-copy)
    pub fn get_all_broker_names(&self) -> Vec<BrokerName> {
        self.inner.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get all broker data entries
    ///
    /// # Returns
    /// Vector of (broker_name, broker_data) pairs
    pub fn get_all_brokers(&self) -> Vec<(BrokerName, Arc<BrokerData>)> {
        self.inner
            .iter()
            .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
            .collect()
    }

    /// Get brokers by cluster name
    ///
    /// # Arguments
    /// * `cluster_name` - Cluster name to filter by
    ///
    /// # Returns
    /// Vector of broker names in the cluster
    pub fn get_brokers_by_cluster(&self, cluster_name: &str) -> Vec<BrokerName> {
        self.inner
            .iter()
            .filter(|entry| entry.value().cluster() == cluster_name)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get number of brokers
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if table is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clear all data
    pub fn clear(&self) {
        self.inner.clear();
    }

    /// Update broker address for a specific broker ID
    ///
    /// # Arguments
    /// * `broker_name` - Broker name
    /// * `broker_id` - Broker ID (0 for master, >0 for slaves)
    /// * `address` - New broker address
    ///
    /// # Returns
    /// true if broker exists and address was updated
    pub fn update_broker_address(
        &self,
        broker_name: &str,
        broker_id: u64,
        address: impl Into<cheetah_string::CheetahString>,
    ) -> bool {
        if let Some(mut entry) = self.inner.get_mut(broker_name) {
            // Clone the BrokerData, update it, and replace
            let mut new_data = (**entry.value()).clone();
            new_data.broker_addrs_mut().insert(broker_id, address.into());
            *entry.value_mut() = Arc::new(new_data);
            true
        } else {
            false
        }
    }

    /// Remove broker address for a specific broker ID
    ///
    /// # Arguments
    /// * `broker_name` - Broker name
    /// * `broker_id` - Broker ID to remove
    ///
    /// # Returns
    /// true if address was removed, false if broker or ID not found
    pub fn remove_broker_address(&self, broker_name: &str, broker_id: u64) -> bool {
        if let Some(mut entry) = self.inner.get_mut(broker_name) {
            let mut new_data = (**entry.value()).clone();
            let removed = new_data.broker_addrs_mut().remove(&broker_id).is_some();
            if removed {
                *entry.value_mut() = Arc::new(new_data);
            }
            removed
        } else {
            false
        }
    }
}

impl Default for BrokerAddrTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    fn create_test_broker_data(cluster: &str, broker_name: &str) -> BrokerData {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, format!("{}:10911", broker_name).into());

        BrokerData::new(cluster.into(), broker_name.into(), broker_addrs, None)
    }

    #[test]
    fn test_insert_and_get() {
        let table = BrokerAddrTable::new();
        let broker_name: BrokerName = CheetahString::from_string("broker-a".to_string());
        let broker_data = create_test_broker_data("DefaultCluster", "broker-a");

        // Insert
        let old = table.insert(broker_name.clone(), broker_data);
        assert!(old.is_none());

        // Get
        let retrieved = table.get("broker-a").unwrap();
        assert_eq!(retrieved.cluster(), "DefaultCluster");
        assert_eq!(retrieved.broker_name().as_str(), "broker-a");
    }

    #[test]
    fn test_remove() {
        let table = BrokerAddrTable::new();
        let broker_name: BrokerName = CheetahString::from_string("broker-a".to_string());
        let broker_data = create_test_broker_data("DefaultCluster", "broker-a");

        table.insert(broker_name.clone(), broker_data);

        // Remove
        let removed = table.remove("broker-a");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().broker_name().as_str(), "broker-a");

        // Verify removed
        assert!(!table.contains("broker-a"));
    }

    #[test]
    fn test_get_brokers_by_cluster() {
        let table = BrokerAddrTable::new();

        // Insert brokers in different clusters
        table.insert(
            CheetahString::from_string("broker-a".to_string()),
            create_test_broker_data("ClusterA", "broker-a"),
        );
        table.insert(
            CheetahString::from_string("broker-b".to_string()),
            create_test_broker_data("ClusterA", "broker-b"),
        );
        table.insert(
            CheetahString::from_string("broker-c".to_string()),
            create_test_broker_data("ClusterB", "broker-c"),
        );

        let cluster_a_brokers = table.get_brokers_by_cluster("ClusterA");
        assert_eq!(cluster_a_brokers.len(), 2);

        let cluster_b_brokers = table.get_brokers_by_cluster("ClusterB");
        assert_eq!(cluster_b_brokers.len(), 1);
    }

    #[test]
    fn test_update_broker_address() {
        let table = BrokerAddrTable::new();
        let broker_name: BrokerName = CheetahString::from_string("broker-a".to_string());
        let broker_data = create_test_broker_data("DefaultCluster", "broker-a");

        table.insert(broker_name.clone(), broker_data);

        // Update address
        let updated = table.update_broker_address("broker-a", 1, "slave1:10911".to_string());
        assert!(updated);

        // Verify update
        let broker = table.get("broker-a").unwrap();
        assert_eq!(broker.broker_addrs().get(&1).unwrap().as_str(), "slave1:10911");
    }

    #[test]
    fn test_remove_broker_address() {
        let table = BrokerAddrTable::new();
        let broker_name: BrokerName = CheetahString::from_string("broker-a".to_string());
        let mut broker_data = create_test_broker_data("DefaultCluster", "broker-a");
        broker_data.broker_addrs_mut().insert(1, "slave1:10911".into());

        table.insert(broker_name.clone(), broker_data);

        // Remove slave address
        let removed = table.remove_broker_address("broker-a", 1);
        assert!(removed);

        // Verify removal
        let broker = table.get("broker-a").unwrap();
        assert!(!broker.broker_addrs().contains_key(&1));
        assert!(broker.broker_addrs().contains_key(&0)); // Master still exists
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let table = Arc::new(BrokerAddrTable::new());
        let mut handles = vec![];

        // Spawn multiple threads
        for i in 0..10 {
            let table_clone = table.clone();
            handles.push(thread::spawn(move || {
                let broker_name: BrokerName = CheetahString::from_string(format!("broker-{}", i));
                let broker_data = create_test_broker_data("TestCluster", &format!("broker-{}", i));
                table_clone.insert(broker_name, broker_data);
            }));
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify data
        assert_eq!(table.len(), 10);
        assert_eq!(table.get_brokers_by_cluster("TestCluster").len(), 10);
    }
}
