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

//! Filter server table with concurrent access
//!
//! Manages filter servers associated with broker addresses.

use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;

use crate::route_info::broker_addr_info::BrokerAddrInfo;

/// Filter server table: BrokerAddrInfo -> List<FilterServer>
///
/// This table maintains the mapping from broker addresses to their associated
/// filter servers. Filter servers are used for message filtering in RocketMQ.
///
/// # Performance
/// - Read operations: O(1) average, lock-free
/// - Write operations: O(1) average, per-entry lock
/// - Concurrent access: Lock-free reads, per-entry lock for writes
#[derive(Clone)]
pub struct FilterServerTable {
    inner: DashMap<Arc<BrokerAddrInfo>, Vec<CheetahString>>,
}

impl Default for FilterServerTable {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterServerTable {
    /// Create a new filter server table
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Create with estimated capacity
    ///
    /// # Arguments
    /// * `capacity` - Expected number of brokers with filter servers
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(capacity),
        }
    }

    /// Register or update filter servers for a broker
    ///
    /// # Arguments
    /// * `broker_addr_info` - Broker address info (zero-copy Arc)
    /// * `filter_servers` - List of filter server addresses
    ///
    /// # Returns
    /// Previous filter server list if existed
    pub fn register(
        &self,
        broker_addr_info: Arc<BrokerAddrInfo>,
        filter_servers: Vec<CheetahString>,
    ) -> Option<Vec<CheetahString>> {
        self.inner.insert(broker_addr_info, filter_servers)
    }

    /// Get filter servers for a broker
    ///
    /// # Arguments
    /// * `broker_addr_info` - Broker address info
    ///
    /// # Returns
    /// Filter server list if found
    pub fn get(&self, broker_addr_info: &BrokerAddrInfo) -> Option<Vec<CheetahString>> {
        self.inner.get(broker_addr_info).map(|v| v.clone())
    }

    /// Remove filter servers for a broker
    ///
    /// # Arguments
    /// * `broker_addr_info` - Broker address info
    ///
    /// # Returns
    /// Removed filter server list if existed
    pub fn remove(&self, broker_addr_info: &BrokerAddrInfo) -> Option<Vec<CheetahString>> {
        self.inner.remove(broker_addr_info).map(|(_, v)| v)
    }

    /// Check if broker has filter servers registered
    ///
    /// # Arguments
    /// * `broker_addr_info` - Broker address info
    pub fn contains(&self, broker_addr_info: &BrokerAddrInfo) -> bool {
        self.inner.contains_key(broker_addr_info)
    }

    /// Check if the table is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get the number of brokers with filter servers
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Clear all filter server entries
    pub fn clear(&self) {
        self.inner.clear();
    }

    /// Get all broker addresses with filter servers
    ///
    /// # Returns
    /// Iterator over broker address info references
    pub fn get_all_broker_addrs(&self) -> Vec<Arc<BrokerAddrInfo>> {
        self.inner.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get all filter server mappings
    ///
    /// # Returns
    /// Vector of (BrokerAddrInfo, FilterServerList) tuples
    pub fn get_all(&self) -> Vec<(Arc<BrokerAddrInfo>, Vec<CheetahString>)> {
        self.inner
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_server_table_basic_operations() {
        let table = FilterServerTable::new();

        let broker_info = Arc::new(BrokerAddrInfo::new("DefaultCluster", "192.168.1.100:10911"));
        let servers = vec![
            CheetahString::from_static_str("192.168.1.100:30000"),
            CheetahString::from_static_str("192.168.1.101:30000"),
        ];

        // Test register
        assert!(table.register(broker_info.clone(), servers.clone()).is_none());
        assert_eq!(table.len(), 1);

        // Test get
        let result = table.get(&broker_info);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), servers);

        // Test contains
        assert!(table.contains(&broker_info));

        // Test remove
        let removed = table.remove(&broker_info);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap(), servers);
        assert!(table.is_empty());
    }

    #[test]
    fn test_filter_server_table_update() {
        let table = FilterServerTable::new();

        let broker_info = Arc::new(BrokerAddrInfo::new("DefaultCluster", "192.168.1.100:10911"));
        let servers1 = vec![CheetahString::from_static_str("192.168.1.100:30000")];
        let servers2 = vec![
            CheetahString::from_static_str("192.168.1.100:30000"),
            CheetahString::from_static_str("192.168.1.101:30000"),
        ];

        table.register(broker_info.clone(), servers1.clone());
        let old = table.register(broker_info.clone(), servers2.clone());

        assert!(old.is_some());
        assert_eq!(old.unwrap(), servers1);
        assert_eq!(table.get(&broker_info).unwrap(), servers2);
    }

    #[test]
    fn test_filter_server_table_multiple_brokers() {
        let table = FilterServerTable::new();

        let broker1 = Arc::new(BrokerAddrInfo::new("Cluster1", "192.168.1.100:10911"));
        let broker2 = Arc::new(BrokerAddrInfo::new("Cluster2", "192.168.1.101:10911"));

        let servers1 = vec![CheetahString::from_static_str("192.168.1.100:30000")];
        let servers2 = vec![CheetahString::from_static_str("192.168.1.101:30000")];

        table.register(broker1.clone(), servers1.clone());
        table.register(broker2.clone(), servers2.clone());

        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&broker1).unwrap(), servers1);
        assert_eq!(table.get(&broker2).unwrap(), servers2);
    }

    #[test]
    fn test_filter_server_table_get_all() {
        let table = FilterServerTable::new();

        let broker1 = Arc::new(BrokerAddrInfo::new("Cluster1", "192.168.1.100:10911"));
        let broker2 = Arc::new(BrokerAddrInfo::new("Cluster2", "192.168.1.101:10911"));

        let servers1 = vec![CheetahString::from_static_str("192.168.1.100:30000")];
        let servers2 = vec![CheetahString::from_static_str("192.168.1.101:30000")];

        table.register(broker1.clone(), servers1);
        table.register(broker2.clone(), servers2);

        let all = table.get_all();
        assert_eq!(all.len(), 2);

        let addrs = table.get_all_broker_addrs();
        assert_eq!(addrs.len(), 2);
    }

    #[test]
    fn test_filter_server_table_clear() {
        let table = FilterServerTable::new();

        let broker_info = Arc::new(BrokerAddrInfo::new("DefaultCluster", "192.168.1.100:10911"));
        let servers = vec![CheetahString::from_static_str("192.168.1.100:30000")];

        table.register(broker_info, servers);
        assert_eq!(table.len(), 1);

        table.clear();
        assert!(table.is_empty());
    }

    #[test]
    fn test_filter_server_table_with_capacity() {
        let table = FilterServerTable::with_capacity(100);
        assert!(table.is_empty());
    }
}
