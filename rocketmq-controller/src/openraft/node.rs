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

//! OpenRaft node management
//!
//! Provides high-level interface for managing Raft nodes.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use openraft::Config;
use tracing::info;

use crate::config::ControllerConfig;
use crate::error::ControllerError;
use crate::error::Result;
use crate::openraft::NetworkFactory;
use crate::openraft::Store;
use crate::typ::Node;
use crate::typ::NodeId;
use crate::typ::Raft;

/// OpenRaft node manager
///
/// Manages the lifecycle of an OpenRaft node including:
/// - Raft instance initialization
/// - Cluster membership management
/// - State queries and modifications
pub struct RaftNodeManager {
    /// Node ID
    node_id: NodeId,

    /// Raft instance
    raft: Arc<Raft>,

    /// Storage
    store: Arc<Store>,
}

impl RaftNodeManager {
    /// Create a new Raft node manager
    pub async fn new(config: Arc<ControllerConfig>) -> Result<Self> {
        let node_id = config.node_id;

        // Create storage
        let store = Arc::new(Store::new());

        // Create network factory
        let network = NetworkFactory::new();

        // Add peer addresses
        for peer in &config.raft_peers {
            network.add_peer(peer.id, peer.addr.to_string()).await;
        }

        // Configure OpenRaft
        let raft_config = Config {
            heartbeat_interval: config.heartbeat_interval_ms,
            election_timeout_min: config.election_timeout_ms,
            election_timeout_max: config.election_timeout_ms * 2,
            max_in_snapshot_log_to_keep: 1000,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
            ..Default::default()
        };

        let raft_config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| ControllerError::Internal(format!("Invalid Raft config: {}", e)))?,
        );

        // Create Raft instance
        let raft = openraft::Raft::new(
            node_id,
            raft_config,
            network,
            store.log_store.clone(),
            store.state_machine.clone(),
        )
        .await
        .map_err(|e| ControllerError::Internal(format!("Failed to create Raft: {}", e)))?;

        info!("Created OpenRaft node with ID: {}", node_id);

        Ok(Self {
            node_id,
            raft: Arc::new(raft),
            store,
        })
    }

    /// Initialize the cluster (called on the first node only)
    pub async fn initialize_cluster(&self, nodes: BTreeMap<NodeId, Node>) -> Result<()> {
        info!("Initializing Raft cluster with {} nodes", nodes.len());

        self.raft
            .initialize(nodes)
            .await
            .map_err(|e| ControllerError::Internal(format!("Failed to initialize cluster: {}", e)))?;

        info!("Raft cluster initialized successfully");
        Ok(())
    }

    /// Add a learner node to the cluster
    pub async fn add_learner(&self, node_id: NodeId, node: Node, blocking: bool) -> Result<()> {
        info!("Adding learner node {} to cluster", node_id);

        self.raft
            .add_learner(node_id, node, blocking)
            .await
            .map_err(|e| ControllerError::Internal(format!("Failed to add learner: {}", e)))?;

        info!("Learner node {} added successfully", node_id);
        Ok(())
    }

    /// Change cluster membership
    pub async fn change_membership(&self, members: BTreeSet<NodeId>, retain: bool) -> Result<()> {
        info!("Changing cluster membership: members={:?}, retain={}", members, retain);

        self.raft
            .change_membership(members, retain)
            .await
            .map_err(|e| ControllerError::Internal(format!("Failed to change membership: {}", e)))?;

        info!("Cluster membership changed successfully");
        Ok(())
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> Result<bool> {
        use openraft::async_runtime::WatchReceiver;
        let metrics = self.raft.metrics().borrow_watched().clone();
        Ok(metrics.current_leader == Some(self.node_id))
    }

    /// Get current leader ID
    pub async fn get_leader(&self) -> Result<Option<NodeId>> {
        use openraft::async_runtime::WatchReceiver;
        let metrics = self.raft.metrics().borrow_watched().clone();
        Ok(metrics.current_leader)
    }

    /// Submit a client write request
    pub async fn client_write(
        &self,
        request: crate::typ::ControllerRequest,
    ) -> Result<crate::typ::ClientWriteResponse> {
        self.raft
            .client_write(request)
            .await
            .map_err(|e| ControllerError::Internal(format!("Client write failed: {}", e)))
    }

    /// Get the Raft instance
    pub fn raft(&self) -> Arc<Raft> {
        self.raft.clone()
    }

    /// Get the storage
    pub fn store(&self) -> Arc<Store> {
        self.store.clone()
    }

    /// Shutdown the Raft node
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down Raft node {}", self.node_id);

        self.raft
            .shutdown()
            .await
            .map_err(|e| ControllerError::Internal(format!("Shutdown failed: {}", e)))?;

        info!("Raft node {} shut down successfully", self.node_id);
        Ok(())
    }
}
