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
use std::time::SystemTime;

use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::config::ControllerConfig;
use crate::error::Result;

/// Replica role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaRole {
    /// Master replica
    Master,
    /// Slave replica
    Slave,
}

/// Broker replica information
///
/// Tracks the state of a broker replica including:
/// - Role (master/slave)
/// - Epoch (version number for master election)
/// - Sync state (whether replica is in-sync)
/// - Offset tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerReplicaInfo {
    /// Cluster name
    pub cluster_name: String,

    /// Broker name (identifies the broker set)
    pub broker_name: String,

    /// Broker ID (0 for master, >0 for slaves)
    pub broker_id: u64,

    /// Broker address
    pub broker_addr: String,

    /// Replica role
    pub role: ReplicaRole,

    /// Epoch number (incremented on master election)
    pub epoch: u64,

    /// Maximum offset
    pub max_offset: i64,

    /// Last sync timestamp
    pub last_sync_timestamp: u64,

    /// Whether this replica is in-sync
    pub in_sync: bool,
}

impl BrokerReplicaInfo {
    /// Create a new master replica
    pub fn new_master(
        cluster_name: String,
        broker_name: String,
        broker_id: u64,
        broker_addr: String,
        epoch: u64,
    ) -> Self {
        Self {
            cluster_name,
            broker_name,
            broker_id,
            broker_addr,
            role: ReplicaRole::Master,
            epoch,
            max_offset: 0,
            last_sync_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            in_sync: true,
        }
    }

    /// Create a new slave replica
    pub fn new_slave(cluster_name: String, broker_name: String, broker_id: u64, broker_addr: String) -> Self {
        Self {
            cluster_name,
            broker_name,
            broker_id,
            broker_addr,
            role: ReplicaRole::Slave,
            epoch: 0,
            max_offset: 0,
            last_sync_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            in_sync: false,
        }
    }

    /// Check if this is a master replica
    pub fn is_master(&self) -> bool {
        self.role == ReplicaRole::Master
    }

    /// Check if this replica is in-sync
    pub fn is_in_sync(&self) -> bool {
        self.in_sync
    }

    /// Get replica ID (broker_name:broker_id)
    pub fn replica_id(&self) -> String {
        format!("{}:{}", self.broker_name, self.broker_id)
    }
}

/// Sync state set (In-Sync Replicas)
///
/// Tracks the set of replicas that are considered in-sync with the master.
/// Similar to Kafka's ISR (In-Sync Replica) concept.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStateSet {
    /// Broker name (identifies the broker set)
    pub broker_name: String,

    /// Master broker ID
    pub master_broker_id: u64,

    /// Master address
    pub master_addr: String,

    /// Master epoch
    pub master_epoch: u64,

    /// Sync state set (list of in-sync broker IDs)
    pub sync_state_set: Vec<u64>,

    /// Last update timestamp
    pub last_update_timestamp: u64,
}

impl SyncStateSet {
    /// Create a new sync state set
    pub fn new(broker_name: String, master_broker_id: u64, master_addr: String) -> Self {
        Self {
            broker_name,
            master_broker_id,
            master_addr,
            master_epoch: 0,
            sync_state_set: vec![master_broker_id],
            last_update_timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Check if a broker is in the sync state set
    pub fn contains(&self, broker_id: u64) -> bool {
        self.sync_state_set.contains(&broker_id)
    }

    /// Add a broker to the sync state set
    pub fn add_broker(&mut self, broker_id: u64) {
        if !self.sync_state_set.contains(&broker_id) {
            self.sync_state_set.push(broker_id);
            self.last_update_timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }
    }

    /// Remove a broker from the sync state set
    pub fn remove_broker(&mut self, broker_id: u64) {
        self.sync_state_set.retain(|&id| id != broker_id);
        self.last_update_timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Get the size of the sync state set
    pub fn size(&self) -> usize {
        self.sync_state_set.len()
    }

    /// Check if a broker is the master
    pub fn is_master(&self, broker_id: u64) -> bool {
        self.master_broker_id == broker_id
    }
}

/// Replicas manager
///
/// Manages broker replicas and sync state sets across the cluster.
/// Provides functionality for:
/// - Replica registration and tracking
/// - ISR (In-Sync Replicas) management
/// - Master election and failover
pub struct ReplicasManager {
    /// Configuration
    config: Arc<ControllerConfig>,

    /// Replicas: broker_name -> (broker_id -> BrokerReplicaInfo)
    replicas: Arc<DashMap<String, HashMap<u64, BrokerReplicaInfo>>>,

    /// Sync state sets: broker_name -> SyncStateSet
    sync_state_sets: Arc<DashMap<String, SyncStateSet>>,
}

impl ReplicasManager {
    /// Create a new replicas manager
    pub fn new(config: Arc<ControllerConfig>) -> Self {
        Self {
            config,
            replicas: Arc::new(DashMap::new()),
            sync_state_sets: Arc::new(DashMap::new()),
        }
    }

    /// Start the replicas manager
    pub async fn start(&self) -> Result<()> {
        info!("Starting replicas manager");
        Ok(())
    }

    /// Shutdown the replicas manager
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down replicas manager");
        self.replicas.clear();
        self.sync_state_sets.clear();
        Ok(())
    }

    /// Register a replica
    pub async fn register_replica(&self, replica: BrokerReplicaInfo) -> Result<()> {
        let broker_name = replica.broker_name.clone();
        let broker_id = replica.broker_id;
        let is_master = replica.is_master();

        debug!(
            "Registering replica: {}:{} (role={:?})",
            broker_name, broker_id, replica.role
        );

        // Update replicas map
        self.replicas
            .entry(broker_name.clone())
            .or_default()
            .insert(broker_id, replica.clone());

        // If this is a master, initialize or update sync state set
        if is_master {
            let mut sync_state_set = SyncStateSet::new(broker_name.clone(), broker_id, replica.broker_addr.clone());
            sync_state_set.master_epoch = replica.epoch;
            self.sync_state_sets.insert(broker_name, sync_state_set);
        }

        Ok(())
    }

    /// Unregister a replica
    pub async fn unregister_replica(&self, broker_name: &str, broker_id: u64) -> Result<()> {
        debug!("Unregistering replica: {}:{}", broker_name, broker_id);

        // Remove from replicas map
        if let Some(mut replicas) = self.replicas.get_mut(broker_name) {
            replicas.remove(&broker_id);
            if replicas.is_empty() {
                drop(replicas);
                self.replicas.remove(broker_name);
            }
        }

        // Remove from sync state set
        if let Some(mut sync_state_set) = self.sync_state_sets.get_mut(broker_name) {
            sync_state_set.remove_broker(broker_id);
        }

        Ok(())
    }

    /// Get the master replica for a broker set
    pub async fn get_master(&self, broker_name: &str) -> Option<BrokerReplicaInfo> {
        self.replicas
            .get(broker_name)
            .and_then(|replicas| replicas.values().find(|replica| replica.is_master()).cloned())
    }

    /// Get all replicas for a broker set
    pub async fn get_replicas(&self, broker_name: &str) -> Vec<BrokerReplicaInfo> {
        self.replicas
            .get(broker_name)
            .map(|replicas| replicas.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Get a specific replica
    pub async fn get_replica(&self, broker_name: &str, broker_id: u64) -> Option<BrokerReplicaInfo> {
        self.replicas
            .get(broker_name)
            .and_then(|replicas| replicas.get(&broker_id).cloned())
    }

    /// Get sync state set for a broker set
    pub async fn get_sync_state_set(&self, broker_name: &str) -> Option<SyncStateSet> {
        self.sync_state_sets.get(broker_name).map(|s| s.clone())
    }

    /// Add a broker to the sync state set
    pub async fn add_to_sync_state_set(&self, broker_name: &str, broker_id: u64) -> Result<()> {
        if let Some(mut sync_state_set) = self.sync_state_sets.get_mut(broker_name) {
            sync_state_set.add_broker(broker_id);
            debug!("Added broker {} to sync state set for {}", broker_id, broker_name);
        }

        // Update replica in-sync status
        if let Some(mut replicas) = self.replicas.get_mut(broker_name) {
            if let Some(replica) = replicas.get_mut(&broker_id) {
                replica.in_sync = true;
            }
        }

        Ok(())
    }

    /// Remove a broker from the sync state set
    pub async fn remove_from_sync_state_set(&self, broker_name: &str, broker_id: u64) -> Result<()> {
        if let Some(mut sync_state_set) = self.sync_state_sets.get_mut(broker_name) {
            sync_state_set.remove_broker(broker_id);
            warn!("Removed broker {} from sync state set for {}", broker_id, broker_name);
        }

        // Update replica in-sync status
        if let Some(mut replicas) = self.replicas.get_mut(broker_name) {
            if let Some(replica) = replicas.get_mut(&broker_id) {
                replica.in_sync = false;
            }
        }

        Ok(())
    }

    /// Update sync state set
    pub async fn update_sync_state_set(&self, broker_name: &str, new_sync_state_set: Vec<u64>) -> Result<()> {
        if let Some(mut sync_state_set) = self.sync_state_sets.get_mut(broker_name) {
            sync_state_set.sync_state_set = new_sync_state_set.clone();
            sync_state_set.last_update_timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            debug!("Updated sync state set for {}: {:?}", broker_name, new_sync_state_set);
        }

        // Update replica in-sync status
        if let Some(mut replicas) = self.replicas.get_mut(broker_name) {
            for (broker_id, replica) in replicas.iter_mut() {
                replica.in_sync = new_sync_state_set.contains(broker_id);
            }
        }

        Ok(())
    }

    /// Elect a new master for a broker set
    ///
    /// This is typically called when the current master fails.
    /// A new master is elected from the in-sync replicas.
    pub async fn elect_master(&self, broker_name: &str) -> Result<Option<BrokerReplicaInfo>> {
        info!("Electing new master for broker set: {}", broker_name);

        // Get current sync state set
        let sync_state_set = match self.sync_state_sets.get(broker_name) {
            Some(s) => s.clone(),
            None => {
                warn!("No sync state set found for {}", broker_name);
                return Ok(None);
            }
        };

        // Find the first in-sync slave to promote
        let new_master_id = sync_state_set
            .sync_state_set
            .iter()
            .find(|&&id| id != sync_state_set.master_broker_id)
            .copied();

        let new_master_id = match new_master_id {
            Some(id) => id,
            None => {
                warn!("No in-sync slaves available for {}", broker_name);
                return Ok(None);
            }
        };

        // Promote the slave to master
        let mut new_master = None;
        if let Some(mut replicas) = self.replicas.get_mut(broker_name) {
            // Demote old master if it still exists
            if let Some(old_master) = replicas.get_mut(&sync_state_set.master_broker_id) {
                old_master.role = ReplicaRole::Slave;
            }

            // Promote new master
            if let Some(replica) = replicas.get_mut(&new_master_id) {
                replica.role = ReplicaRole::Master;
                replica.epoch += 1;
                new_master = Some(replica.clone());

                info!(
                    "Elected new master for {}: {} (epoch={})",
                    broker_name, new_master_id, replica.epoch
                );
            }
        }

        // Update sync state set
        if let (Some(master), Some(mut sync_state_set)) =
            (new_master.as_ref(), self.sync_state_sets.get_mut(broker_name))
        {
            sync_state_set.master_broker_id = new_master_id;
            sync_state_set.master_addr = master.broker_addr.clone();
            sync_state_set.master_epoch = master.epoch;
            sync_state_set.last_update_timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }

        Ok(new_master)
    }

    /// List all broker sets
    pub async fn list_broker_sets(&self) -> Vec<String> {
        self.replicas.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get statistics
    pub async fn get_stats(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        stats.insert("broker_sets".to_string(), self.replicas.len());
        stats.insert("sync_state_sets".to_string(), self.sync_state_sets.len());

        let total_replicas: usize = self.replicas.iter().map(|entry| entry.value().len()).sum();
        stats.insert("total_replicas".to_string(), total_replicas);

        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_replica_info() {
        let master = BrokerReplicaInfo::new_master(
            "test-cluster".to_string(),
            "broker-a".to_string(),
            0,
            "127.0.0.1:10911".to_string(),
            1,
        );

        assert!(master.is_master());
        assert!(master.is_in_sync());
        assert_eq!(master.replica_id(), "broker-a:0");
        assert_eq!(master.epoch, 1);

        let slave = BrokerReplicaInfo::new_slave(
            "test-cluster".to_string(),
            "broker-a".to_string(),
            1,
            "127.0.0.1:10912".to_string(),
        );

        assert!(!slave.is_master());
        assert!(!slave.is_in_sync());
        assert_eq!(slave.replica_id(), "broker-a:1");
        assert_eq!(slave.epoch, 0);
    }

    #[test]
    fn test_sync_state_set() {
        let mut sync_state = SyncStateSet::new("broker-a".to_string(), 0, "127.0.0.1:10911".to_string());

        assert_eq!(sync_state.size(), 1);
        assert!(sync_state.contains(0));
        assert!(sync_state.is_master(0));

        sync_state.add_broker(1);
        assert_eq!(sync_state.size(), 2);
        assert!(sync_state.contains(1));
        assert!(!sync_state.is_master(1));

        sync_state.remove_broker(1);
        assert_eq!(sync_state.size(), 1);
        assert!(!sync_state.contains(1));
    }

    #[tokio::test]
    async fn test_replicas_manager() {
        let config = Arc::new(ControllerConfig::new_node(1, "127.0.0.1:9876".parse().unwrap()));
        let manager = ReplicasManager::new(config);

        // Register master
        let master = BrokerReplicaInfo::new_master(
            "test-cluster".to_string(),
            "broker-a".to_string(),
            0,
            "127.0.0.1:10911".to_string(),
            1,
        );
        manager.register_replica(master.clone()).await.unwrap();

        // Register slave
        let slave = BrokerReplicaInfo::new_slave(
            "test-cluster".to_string(),
            "broker-a".to_string(),
            1,
            "127.0.0.1:10912".to_string(),
        );
        manager.register_replica(slave).await.unwrap();

        // Verify
        let replicas = manager.get_replicas("broker-a").await;
        assert_eq!(replicas.len(), 2);

        let master_replica = manager.get_master("broker-a").await;
        assert!(master_replica.is_some());
        assert_eq!(master_replica.unwrap().broker_id, 0);

        // Elect new master
        manager.add_to_sync_state_set("broker-a", 1).await.unwrap();
        let new_master = manager.elect_master("broker-a").await.unwrap();
        assert!(new_master.is_some());
        assert_eq!(new_master.unwrap().broker_id, 1);
    }
}
