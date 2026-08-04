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
use openraft::ReadPolicy;
use rocketmq_error::RocketMQResult;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use tracing::info;

use crate::config::ControllerConfigReader;
use crate::controller::membership::ConsensusMembership;
use crate::controller::membership::ConsensusMembershipPort;
use crate::controller::membership::MembershipChangeCoordinator;
use crate::controller::membership::MembershipChangeOutcome;
use crate::controller::membership::MembershipChangeRequest;
use crate::error::ControllerError;
use crate::error::Result;
use crate::openraft::GrpcRaftService;
use crate::openraft::NetworkFactory;
use crate::openraft::Store;
use crate::typ::Node;
use crate::typ::NodeId;
use crate::typ::Raft;
use crate::typ::RaftMetrics;

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

    /// Serializes and reconciles all administrative membership mutations.
    membership_changes: MembershipChangeCoordinator,
}

impl RaftNodeManager {
    /// Create a new Raft node manager
    pub async fn new(config: ControllerConfigReader, storage_io: BlockingExecutor) -> Result<Self> {
        let startup_config = config.snapshot();
        let node_id = startup_config.node_id;

        // Create storage
        let store = Arc::new(Store::open(config.clone(), storage_io).await?);

        // Create network factory
        let network = NetworkFactory::new();

        // Add peer addresses
        for peer in &startup_config.raft_peers {
            if peer.id == node_id {
                continue;
            }
            network.add_peer(peer.id, peer.addr.to_string()).await;
        }

        // Configure OpenRaft
        let raft_config = Config {
            heartbeat_interval: startup_config.heartbeat_interval_ms,
            election_timeout_min: startup_config.election_timeout_ms,
            election_timeout_max: startup_config.election_timeout_ms * 2,
            max_in_snapshot_log_to_keep: 1000,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
            allow_log_reversion: Some(true),
            ..Default::default()
        };

        let raft_config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| ControllerError::raft_source("validate Raft config", e))?,
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
        .map_err(|e| ControllerError::raft_source("create Raft node", e))?;

        info!("Created OpenRaft node with ID: {}", node_id);

        Ok(Self {
            node_id,
            raft: Arc::new(raft),
            store,
            membership_changes: MembershipChangeCoordinator::default(),
        })
    }

    /// Initializes a brand-new cluster during first-node bootstrap only.
    ///
    /// This is the sole bootstrap exception to the guarded membership mutation boundary. It must
    /// not be used for administrative membership changes after the initial cluster is formed;
    /// those changes must use [`Self::apply_membership_change`].
    ///
    /// # Errors
    ///
    /// Returns a typed Controller error if OpenRaft rejects cluster initialization.
    pub async fn initialize_cluster(&self, nodes: BTreeMap<NodeId, Node>) -> Result<()> {
        info!("Initializing Raft cluster with {} nodes", nodes.len());

        self.raft
            .initialize(nodes)
            .await
            .map_err(|e| ControllerError::raft_source("initialize Raft cluster", e))?;

        info!("Raft cluster initialized successfully");
        Ok(())
    }

    /// Add a learner node to the cluster
    pub(crate) async fn add_learner(&self, node_id: NodeId, node: Node, blocking: bool) -> Result<()> {
        info!("Adding learner node {} to cluster", node_id);

        self.raft
            .add_learner(node_id, node, blocking)
            .await
            .map_err(|e| ControllerError::raft_source(format!("add Raft learner {node_id}"), e))?;

        info!("Learner node {} added successfully", node_id);
        Ok(())
    }

    /// Change cluster membership
    pub(crate) async fn change_membership(&self, members: BTreeSet<NodeId>, retain: bool) -> Result<()> {
        info!("Changing cluster membership: members={:?}, retain={}", members, retain);

        self.raft
            .change_membership(members, retain)
            .await
            .map_err(|e| ControllerError::raft_source("change Raft membership", e))?;

        info!("Cluster membership changed successfully");
        Ok(())
    }

    /// Applies one authorized and version-fenced consensus membership step.
    ///
    /// This boundary temporarily reuses the release-maintenance
    /// [`rocketmq_security_api::MaintenanceCapability::ReleaseCheckpoint`] permission. A future
    /// versioned maintenance policy should split membership administration into a dedicated
    /// capability.
    ///
    /// # Errors
    ///
    /// Returns a typed error when request validation, authorization, optimistic fencing,
    /// consensus application, or verification fails.
    pub async fn apply_membership_change(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: MembershipChangeRequest,
    ) -> RocketMQResult<MembershipChangeOutcome> {
        self.membership_changes.apply(self, authorization, request).await
    }

    /// Returns the current OpenRaft-independent consensus membership projection.
    ///
    /// # Errors
    ///
    /// Returns a typed Controller error if the membership projection cannot be read.
    pub async fn consensus_membership(&self) -> Result<ConsensusMembership> {
        ConsensusMembershipPort::current_membership(self).await
    }

    /// Allow a specific follower/learner to reset replication progress once when log
    /// reversion is detected during bootstrap or recovery.
    pub async fn allow_next_revert(&self, node_id: NodeId, allow: bool) -> Result<()> {
        self.raft
            .trigger()
            .allow_next_revert(&node_id, allow)
            .await
            .map_err(|e| ControllerError::raft_source(format!("send allow-next-revert request for node {node_id}"), e))?
            .map_err(|e| {
                ControllerError::raft_source(format!("apply allow-next-revert request for node {node_id}"), e)
            })?;
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

    /// Check whether the node has applied at least one log entry.
    pub fn has_committed_log(&self) -> bool {
        use openraft::async_runtime::WatchReceiver;
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics.last_applied.is_some()
    }

    /// Checks durable applied state without waiting for the asynchronous metrics publisher.
    pub async fn has_persisted_committed_log(&self) -> bool {
        self.store.state_machine.has_persisted_applied_state().await
    }

    /// Submit a client write request
    pub async fn client_write(
        &self,
        request: crate::typ::ControllerRequest,
    ) -> Result<crate::typ::ClientWriteResponse> {
        self.raft
            .client_write(request)
            .await
            .map_err(|e| ControllerError::raft_source("client write", e))
    }

    /// Confirms leadership through ReadIndex and waits until the local state machine has applied
    /// every log entry required by the read barrier.
    pub async fn ensure_linearizable_read(&self) -> Result<Option<crate::typ::LogId>> {
        self.raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map_err(|error| ControllerError::raft_source("linearizable ReadIndex", error))
    }

    /// Returns the raw Raft handle for internal adapters only.
    pub(crate) fn raft(&self) -> Arc<Raft> {
        self.raft.clone()
    }

    /// Returns a read-only snapshot of current Raft metrics.
    pub fn raft_metrics(&self) -> RaftMetrics {
        use openraft::async_runtime::WatchReceiver;

        self.raft.metrics().borrow_watched().clone()
    }

    /// Creates the gRPC service adapter without exposing the raw Raft handle.
    pub fn grpc_service(&self) -> GrpcRaftService {
        GrpcRaftService::new(self.raft.clone())
    }

    /// Enables or disables Raft runtime ticks.
    pub fn set_runtime_tick_enabled(&self, enabled: bool) {
        self.raft.runtime_config().tick(enabled);
    }

    /// Enables or disables Raft runtime heartbeats.
    pub fn set_runtime_heartbeat_enabled(&self, enabled: bool) {
        self.raft.runtime_config().heartbeat(enabled);
    }

    /// Enables or disables Raft runtime elections.
    pub fn set_runtime_elect_enabled(&self, enabled: bool) {
        self.raft.runtime_config().elect(enabled);
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
            .map_err(|e| ControllerError::raft_source("shutdown Raft node", e))?;

        info!("Raft node {} shut down successfully", self.node_id);
        Ok(())
    }
}
