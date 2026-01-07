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

//! Cluster address table with concurrent access
//!
//! Manages cluster name -> broker names set mappings.

use dashmap::DashMap;
use dashmap::DashSet;

use crate::route::types::BrokerName;
use crate::route::types::ClusterName;

/// Cluster address table: ClusterName -> Set<BrokerName>
///
/// This table maintains the mapping of cluster names to the set of
/// broker names that belong to each cluster. Uses DashSet for
/// concurrent set operations.
///
/// # Performance
/// - Read operations: O(1) average, lock-free
/// - Write operations: O(1) average, per-entry lock
///
/// # Example
/// ```no_run
/// use std::sync::Arc;
///
/// use rocketmq_namesrv::route::tables::ClusterAddrTable;
///
/// let table = ClusterAddrTable::new();
/// // Thread-safe operations without explicit locking
/// ```
#[derive(Clone)]
pub struct ClusterAddrTable {
    inner: DashMap<ClusterName, DashSet<BrokerName>>,
}

impl ClusterAddrTable {
    /// Create a new cluster address table
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Create with estimated capacity
    ///
    /// # Arguments
    /// * `capacity` - Expected number of clusters
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(capacity),
        }
    }

    /// Add a broker to a cluster
    ///
    /// # Arguments
    /// * `cluster_name` - Cluster name (zero-copy Arc<str>)
    /// * `broker_name` - Broker name to add (zero-copy Arc<str>)
    ///
    /// # Returns
    /// true if broker was newly added, false if already existed
    pub fn add_broker(&self, cluster_name: ClusterName, broker_name: BrokerName) -> bool {
        self.inner.entry(cluster_name).or_default().insert(broker_name)
    }

    /// Remove a broker from a cluster
    ///
    /// # Arguments
    /// * `cluster_name` - Cluster name
    /// * `broker_name` - Broker name to remove
    ///
    /// # Returns
    /// true if broker was removed, false if didn't exist
    pub fn remove_broker(&self, cluster_name: &str, broker_name: &str) -> bool {
        self.inner
            .get(cluster_name)
            .map(|brokers| {
                // CheetahString implements Borrow<str>, so we can use &str directly
                brokers.remove(broker_name).is_some()
            })
            .unwrap_or(false)
    }

    /// Remove entire cluster
    ///
    /// # Arguments
    /// * `cluster_name` - Cluster name
    ///
    /// # Returns
    /// true if cluster existed and was removed
    pub fn remove_cluster(&self, cluster_name: &str) -> bool {
        self.inner.remove(cluster_name).is_some()
    }

    /// Get all broker names in a cluster
    ///
    /// # Arguments
    /// * `cluster_name` - Cluster name
    ///
    /// # Returns
    /// Vector of broker names (CheetahString for zero-copy)
    pub fn get_brokers(&self, cluster_name: &str) -> Vec<BrokerName> {
        self.inner
            .get(cluster_name)
            .map(|brokers| brokers.iter().map(|broker| broker.key().clone()).collect())
            .unwrap_or_default()
    }

    /// Check if cluster exists
    pub fn contains_cluster(&self, cluster_name: &str) -> bool {
        self.inner.contains_key(cluster_name)
    }

    /// Check if broker exists in cluster
    ///
    /// # Arguments
    /// * `cluster_name` - Cluster name
    /// * `broker_name` - Broker name
    pub fn contains_broker(&self, cluster_name: &str, broker_name: &str) -> bool {
        self.inner
            .get(cluster_name)
            .map(|brokers| brokers.iter().any(|b| b.key().as_str() == broker_name))
            .unwrap_or(false)
    }

    /// Get all cluster names
    ///
    /// # Returns
    /// Vector of cluster names (CheetahString for zero-copy)
    pub fn get_all_clusters(&self) -> Vec<ClusterName> {
        self.inner.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get all clusters with their brokers
    ///
    /// # Returns
    /// Vector of (cluster_name, broker_names) pairs
    pub fn get_all_cluster_brokers(&self) -> Vec<(ClusterName, Vec<BrokerName>)> {
        self.inner
            .iter()
            .map(|entry| {
                let cluster = entry.key().clone();
                let brokers = entry.value().iter().map(|b| b.key().clone()).collect();
                (cluster, brokers)
            })
            .collect()
    }

    /// Get number of clusters
    pub fn cluster_count(&self) -> usize {
        self.inner.len()
    }

    /// Get number of brokers in a cluster
    ///
    /// # Arguments
    /// * `cluster_name` - Cluster name
    pub fn broker_count_in_cluster(&self, cluster_name: &str) -> usize {
        self.inner.get(cluster_name).map(|brokers| brokers.len()).unwrap_or(0)
    }

    /// Get total number of brokers across all clusters
    pub fn total_broker_count(&self) -> usize {
        self.inner.iter().map(|entry| entry.value().len()).sum()
    }

    /// Clear all data
    pub fn clear(&self) {
        self.inner.clear();
    }

    /// Clean up empty clusters
    ///
    /// Removes clusters that have no brokers.
    /// Returns number of clusters removed.
    pub fn cleanup_empty_clusters(&self) -> usize {
        let mut removed = 0;
        self.inner.retain(|_, brokers| {
            let is_empty = brokers.is_empty();
            if is_empty {
                removed += 1;
            }
            !is_empty
        });
        removed
    }
}

impl Default for ClusterAddrTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn test_add_and_get_brokers() {
        let table = ClusterAddrTable::new();
        let cluster: ClusterName = CheetahString::from_string("DefaultCluster".to_string());
        let broker_a: BrokerName = CheetahString::from_string("broker-a".to_string());
        let broker_b: BrokerName = CheetahString::from_string("broker-b".to_string());

        // Add brokers
        assert!(table.add_broker(cluster.clone(), broker_a.clone()));
        assert!(table.add_broker(cluster.clone(), broker_b.clone()));

        // Try adding duplicate
        assert!(!table.add_broker(cluster.clone(), broker_a.clone()));

        // Get brokers
        let brokers = table.get_brokers("DefaultCluster");
        assert_eq!(brokers.len(), 2);
    }

    #[test]
    fn test_remove_broker() {
        let table = ClusterAddrTable::new();
        let cluster: ClusterName = CheetahString::from_string("DefaultCluster".to_string());
        let broker: BrokerName = CheetahString::from_string("broker-a".to_string());

        table.add_broker(cluster.clone(), broker.clone());

        // Remove
        assert!(table.remove_broker("DefaultCluster", "broker-a"));
        assert!(!table.contains_broker("DefaultCluster", "broker-a"));

        // Try removing again
        assert!(!table.remove_broker("DefaultCluster", "broker-a"));
    }

    #[test]
    fn test_remove_cluster() {
        let table = ClusterAddrTable::new();
        let cluster: ClusterName = CheetahString::from_string("DefaultCluster".to_string());

        table.add_broker(cluster.clone(), CheetahString::from_string("broker-a".to_string()));
        table.add_broker(cluster.clone(), CheetahString::from_string("broker-b".to_string()));

        // Remove cluster
        assert!(table.remove_cluster("DefaultCluster"));
        assert!(!table.contains_cluster("DefaultCluster"));
    }

    #[test]
    fn test_get_all_clusters() {
        let table = ClusterAddrTable::new();

        table.add_broker(
            CheetahString::from_string("ClusterA".to_string()),
            CheetahString::from_string("broker-a".to_string()),
        );
        table.add_broker(
            CheetahString::from_string("ClusterB".to_string()),
            CheetahString::from_string("broker-b".to_string()),
        );
        table.add_broker(
            CheetahString::from_string("ClusterC".to_string()),
            CheetahString::from_string("broker-c".to_string()),
        );

        let clusters = table.get_all_clusters();
        assert_eq!(clusters.len(), 3);
    }

    #[test]
    fn test_broker_counts() {
        let table = ClusterAddrTable::new();

        table.add_broker(
            CheetahString::from_string("ClusterA".to_string()),
            CheetahString::from_string("broker-a1".to_string()),
        );
        table.add_broker(
            CheetahString::from_string("ClusterA".to_string()),
            CheetahString::from_string("broker-a2".to_string()),
        );
        table.add_broker(
            CheetahString::from_string("ClusterB".to_string()),
            CheetahString::from_string("broker-b1".to_string()),
        );

        assert_eq!(table.cluster_count(), 2);
        assert_eq!(table.broker_count_in_cluster("ClusterA"), 2);
        assert_eq!(table.broker_count_in_cluster("ClusterB"), 1);
        assert_eq!(table.total_broker_count(), 3);
    }

    #[test]
    fn test_cleanup_empty_clusters() {
        let table = ClusterAddrTable::new();

        // Add and then remove all brokers
        table.add_broker(
            CheetahString::from_string("EmptyCluster".to_string()),
            CheetahString::from_string("broker-a".to_string()),
        );
        table.remove_broker("EmptyCluster", "broker-a");

        // Add a non-empty cluster
        table.add_broker(
            CheetahString::from_string("NonEmptyCluster".to_string()),
            CheetahString::from_string("broker-b".to_string()),
        );

        // Cleanup
        let removed = table.cleanup_empty_clusters();
        assert_eq!(removed, 1);
        assert!(!table.contains_cluster("EmptyCluster"));
        assert!(table.contains_cluster("NonEmptyCluster"));
    }

    #[test]
    fn test_get_all_cluster_brokers() {
        let table = ClusterAddrTable::new();

        table.add_broker(
            CheetahString::from_string("ClusterA".to_string()),
            CheetahString::from_string("broker-a1".to_string()),
        );
        table.add_broker(
            CheetahString::from_string("ClusterA".to_string()),
            CheetahString::from_string("broker-a2".to_string()),
        );
        table.add_broker(
            CheetahString::from_string("ClusterB".to_string()),
            CheetahString::from_string("broker-b1".to_string()),
        );

        let all_data = table.get_all_cluster_brokers();
        assert_eq!(all_data.len(), 2);

        // Verify ClusterA has 2 brokers
        let cluster_a_data = all_data.iter().find(|(name, _)| &**name == "ClusterA").unwrap();
        assert_eq!(cluster_a_data.1.len(), 2);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let table = Arc::new(ClusterAddrTable::new());
        let mut handles = vec![];

        // Spawn multiple threads
        for i in 0..10 {
            let table_clone = table.clone();
            handles.push(thread::spawn(move || {
                let cluster: ClusterName = CheetahString::from_string(format!("Cluster{}", i % 3));
                let broker: BrokerName = CheetahString::from_string(format!("broker-{}", i));
                table_clone.add_broker(cluster, broker);
            }));
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify data
        assert!(table.cluster_count() <= 3);
        assert_eq!(table.total_broker_count(), 10);
    }
}
