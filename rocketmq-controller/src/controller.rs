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

//! # Controller Abstraction
//!
//! This module defines the core `Controller` trait, which serves as the primary abstraction
//! for RocketMQ controller implementations. The controller is responsible for:
//!
//! - **Cluster Coordination**: Managing distributed consensus via Raft/DLedger
//! - **Broker Management**: Tracking broker registration, heartbeats, and lifecycle
//! - **Master Election**: Coordinating master election for broker replica groups
//! - **Metadata Management**: Maintaining and synchronizing cluster metadata
//! - **ISR Management**: Managing In-Sync Replica sets for high availability
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                     Controller Trait                         │
//! │  (Abstract interface for all controller implementations)     │
//! └──────────────────────┬───────────────────────────────────────┘
//!                        │
//!                        │ implements
//!                        │
//!          ┌─────────────▼──────────────┐
//!          │     RaftController         │
//!          │  (Wrapper/Dispatcher)      │
//!          └──────────────┬─────────────┘
//!                         │
//!          ┌──────────────┴──────────────┐
//!          │                             │
//!          │ delegates                   │ delegates
//!          │                             │
//! ┌────────▼──────────┐       ┌─────────▼──────────┐
//! │ OpenRaftController│       │  RaftRsController  │
//! │ (OpenRaft backend)│       │  (raft-rs backend) │
//! └───────────────────┘       └────────────────────┘
//!          │                             │
//!          └──────────────┬──────────────┘
//!                         │
//!          ┌──────────────▼──────────────┐
//!          │  ReplicasInfoManager        │
//!          │  BrokerHeartbeatManager     │
//!          │  MetadataStore              │
//!          └─────────────────────────────┘
//! ```
//!
//! ## Lifecycle
//!
//! Controllers follow a strict lifecycle:
//!
//! 1. **Creation**: Construct via implementation-specific `new()`
//! 2. **Startup**: Call `startup()` to initialize resources
//! 3. **Active**: Handle requests, participate in consensus
//! 4. **Leader Transition**: `start_scheduling()` when becoming leader
//! 5. **Follower Transition**: `stop_scheduling()` when losing leadership
//! 6. **Shutdown**: Call `shutdown()` for graceful cleanup
//!
//! ## Thread Safety
//!
//! All trait methods are designed for concurrent access. Implementations must ensure:
//! - Methods can be called from multiple threads simultaneously
//! - Internal state is protected via appropriate synchronization primitives
//! - Async methods are `Send + Sync` compatible
//!
//! ## Example Implementation Skeleton
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use async_trait::async_trait;
//!
//! pub struct RaftController {
//!     // ... fields
//! }
//!
//! #[async_trait]
//! impl Controller for RaftController {
//!     async fn startup(&self) -> Result<()> {
//!         // Initialize Raft node, network, etc.
//!         Ok(())
//!     }
//!
//!     async fn shutdown(&self) -> Result<()> {
//!         // Gracefully stop all components
//!         Ok(())
//!     }
//!
//!     fn is_leader(&self) -> bool {
//!         // Query Raft state
//!         self.raft_node.is_leader()
//!     }
//!
//!     // ... implement other methods
//! }
//! ```

pub mod broker_heartbeat_manager;
pub mod broker_housekeeping_service;
pub mod controller_manager;
pub mod open_raft_controller;
pub mod raft_controller;
pub mod raft_rs_controller;

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;

/// Core Controller trait defining the API for RocketMQ controller implementations
///
/// This trait abstracts the behavior of a distributed controller that manages broker
/// metadata, coordinates master elections, and maintains cluster consistency via
/// consensus protocols (Raft or DLedger).
///
/// # Design Goals
///
/// - **Protocol Agnostic**: Supports multiple consensus backends (Raft, DLedger)
/// - **Async-First**: All I/O-bound operations are async
/// - **Type-Safe**: Leverages Rust's type system for compile-time guarantees
/// - **Testable**: Easy to mock for unit tests via trait objects
///
/// # Implementation Requirements
///
/// Implementations must:
/// 1. Be `Send + Sync` to allow concurrent access
/// 2. Handle all errors via `Result<T, ControllerError>`
/// 3. Support graceful shutdown and cleanup
/// 4. Maintain idempotency for all state-mutating operations
///
/// # Thread Safety
///
/// While individual methods may be called concurrently, implementations should
/// serialize state-mutating operations internally (e.g., via Raft log).
#[allow(async_fn_in_trait)]
pub trait Controller: Send + Sync {
    // ==================== Lifecycle Management ====================

    /// Start the controller
    ///
    /// Initializes all controller components, starts consensus layer, and begins
    /// accepting requests. This method must be idempotent.
    ///
    /// # Lifecycle
    ///
    /// - Called once after construction
    /// - Must complete before any other methods are called
    /// - Should handle restart scenarios gracefully
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Consensus layer fails to initialize
    /// - Network binding fails
    /// - State recovery fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut controller = RaftController::new(config).await?;
    /// controller.startup().await?;
    /// ```
    async fn startup(&mut self) -> RocketMQResult<()>;

    /// Shutdown the controller gracefully
    ///
    /// Stops all background tasks, flushes pending writes, and releases resources.
    /// After this call, the controller should not be used.
    ///
    /// # Guarantees
    ///
    /// - All pending operations are completed or cancelled
    /// - State is persisted to durable storage
    /// - Network connections are closed cleanly
    ///
    /// # Errors
    ///
    /// Returns error if graceful shutdown fails. The controller may still be
    /// in an inconsistent state, requiring external intervention.
    async fn shutdown(&mut self) -> RocketMQResult<()>;

    /// Start scheduling controller events
    ///
    /// Called when this controller becomes the Raft/DLedger leader. Begins
    /// executing periodic tasks such as:
    /// - Broker health checks
    /// - Inactive broker cleanup
    /// - Metadata synchronization
    ///
    /// # Leadership Semantics
    ///
    /// - Only the leader should execute scheduled tasks
    /// - Followers should ignore scheduling
    /// - Called automatically on leader election
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Called by Raft state machine on leadership change
    /// if raft.is_leader() {
    ///     controller.start_scheduling().await?;
    /// }
    /// ```
    async fn start_scheduling(&self) -> RocketMQResult<()>;

    /// Stop scheduling controller events
    ///
    /// Called when this controller loses Raft/DLedger leadership. Stops all
    /// periodic tasks to prevent split-brain scenarios.
    ///
    /// # Idempotency
    ///
    /// Safe to call multiple times. Should be a no-op if not currently scheduling.
    async fn stop_scheduling(&self) -> RocketMQResult<()>;

    /// Check if this controller is the current leader
    ///
    /// # Returns
    ///
    /// - `true` if this node is the Raft/DLedger leader
    /// - `false` otherwise (follower, candidate, or observer)
    ///
    /// # Usage
    ///
    /// Used by:
    /// - Request handlers to reject writes from followers
    /// - Admin tools to find the leader node
    /// - Metrics collection
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if !controller.is_leader() {
    ///     return Err(ControllerError::NotLeader);
    /// }
    /// ```
    fn is_leader(&self) -> bool;

    // ==================== Broker Management ====================

    /// Register a new broker or update existing broker information
    ///
    /// Assigns a unique broker ID and records broker metadata in the cluster.
    /// This operation is replicated via Raft/DLedger for consistency.
    ///
    /// # Arguments
    ///
    /// * `request` - Registration request containing broker address, cluster name, etc.
    ///
    /// # Returns
    ///
    /// RemotingCommand with:
    /// - Response code (SUCCESS or error)
    /// - Assigned broker ID
    /// - Master broker address (if applicable)
    ///
    /// # Errors
    ///
    /// - `NotLeader` if called on follower
    /// - `InvalidRequest` if request validation fails
    /// - `ConsensusTimeout` if Raft proposal times out
    async fn register_broker(
        &self,
        request: &RegisterBrokerToControllerRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>>;

    /// Allocate the next available broker ID
    ///
    /// Used during broker initialization to obtain a unique cluster-wide ID.
    ///
    /// # Arguments
    ///
    /// * `request` - Request containing cluster name and broker name
    ///
    /// # Returns
    ///
    /// RemotingCommand with the newly allocated broker ID
    async fn get_next_broker_id(
        &self,
        request: &GetNextBrokerIdRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>>;

    /// Apply for a specific broker ID
    ///
    /// Used when a broker restarts and wants to reclaim its previous ID.
    ///
    /// # Arguments
    ///
    /// * `request` - Request containing desired broker ID
    ///
    /// # Returns
    ///
    /// RemotingCommand indicating success or rejection
    async fn apply_broker_id(&self, request: &ApplyBrokerIdRequestHeader) -> RocketMQResult<Option<RemotingCommand>>;

    /// Clean up broker data from controller
    ///
    /// Removes all metadata associated with a broker (e.g., after permanent decommission).
    ///
    /// # Arguments
    ///
    /// * `cluster_name` - The cluster name
    /// * `broker_name` - The broker name to clean
    ///
    /// # Returns
    ///
    /// RemotingCommand indicating success or error
    async fn clean_broker_data(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> RocketMQResult<Option<RemotingCommand>>;

    // ==================== Master Election & ISR Management ====================

    /// Trigger master election for a broker replica group
    ///
    /// Selects a new master broker from the in-sync replica set based on
    /// election policy (e.g., highest broker ID, lowest replication lag).
    ///
    /// # Arguments
    ///
    /// * `request` - Election request containing broker name and election policy
    ///
    /// # Returns
    ///
    /// RemotingCommand with:
    /// - Elected master broker ID
    /// - New master epoch
    /// - Updated sync state set
    ///
    /// # Consensus
    ///
    /// This operation is replicated via Raft/DLedger to ensure all controllers
    /// agree on the elected master.
    ///
    /// # Example Flow
    ///
    /// ```text
    /// 1. Broker detects master failure
    /// 2. Sends ELECT_MASTER request to controller
    /// 3. Controller proposes election via Raft
    /// 4. State machine selects new master
    /// 5. Response sent to broker with new master info
    /// ```
    async fn elect_master(&self, request: &ElectMasterRequestHeader) -> RocketMQResult<Option<RemotingCommand>>;

    /// Alter the In-Sync Replica set (ISR) for a broker
    ///
    /// Updates the set of brokers that are currently in-sync with the master.
    /// Typically called by the master broker to reflect replication lag.
    ///
    /// # Arguments
    ///
    /// * `request` - Request header with broker name and master epoch
    /// * `sync_state_set` - New ISR set and epoch
    ///
    /// # Returns
    ///
    /// RemotingCommand with:
    /// - Updated sync state set
    /// - New epoch
    ///
    /// # Validation
    ///
    /// - Master epoch must match current epoch (fencing)
    /// - All replicas in new ISR must exist and be alive
    /// - Master broker must remain in ISR
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Master detects slave caught up
    /// let new_isr = HashSet::from([0, 1, 2]); // broker IDs
    /// controller.alter_sync_state_set(header, SyncStateSet::new(new_isr, epoch)).await?;
    /// ```
    async fn alter_sync_state_set(
        &self,
        request: &AlterSyncStateSetRequestHeader,
        sync_state_set: SyncStateSet,
    ) -> RocketMQResult<Option<RemotingCommand>>;

    // ==================== Metadata Queries ====================

    /// Get replica information for a broker
    ///
    /// Returns detailed replica topology including master, in-sync replicas,
    /// and out-of-sync replicas.
    ///
    /// # Arguments
    ///
    /// * `request` - Request specifying broker name
    ///
    /// # Returns
    ///
    /// RemotingCommand with:
    /// - Master broker ID and address
    /// - Master epoch
    /// - In-sync replica list
    /// - Out-of-sync replica list
    /// - Sync state set epoch
    async fn get_replica_info(&self, request: &GetReplicaInfoRequestHeader) -> RocketMQResult<Option<RemotingCommand>>;

    /// Get controller metadata
    ///
    /// Returns information about the controller cluster itself, including:
    /// - Leader node address
    /// - Cluster topology
    /// - Raft/DLedger status
    ///
    /// # Returns
    ///
    /// RemotingCommand with controller metadata
    ///
    /// # Note
    ///
    /// This is a read-only operation that does not go through Raft consensus.
    async fn get_controller_metadata(&self) -> RocketMQResult<Option<RemotingCommand>>;

    /// Get sync state data for specified brokers
    ///
    /// Batch query for ISR information across multiple broker groups.
    /// Used by admin tools for cluster visualization.
    ///
    /// # Arguments
    ///
    /// * `broker_names` - List of broker names to query
    ///
    /// # Returns
    ///
    /// RemotingCommand with aggregated sync state data
    async fn get_sync_state_data(&self, broker_names: &[CheetahString]) -> RocketMQResult<Option<RemotingCommand>>;

    // ==================== Lifecycle Listeners ====================

    /// Register a listener for broker lifecycle events
    ///
    /// The listener will be notified when brokers become inactive, allowing
    /// external components to react (e.g., triggering failover).
    ///
    /// # Arguments
    ///
    /// * `listener` - Implementation of `BrokerLifecycleListener` trait
    ///
    /// # Thread Safety
    ///
    /// Listeners are called synchronously from controller threads. Implementations
    /// should avoid blocking operations.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// struct MyListener;
    ///
    /// impl BrokerLifecycleListener for MyListener {
    ///     fn on_broker_inactive(&self, cluster: &str, broker: &str, id: i64) {
    ///         log::warn!("Broker {}/{}/{} is now inactive", cluster, broker, id);
    ///     }
    /// }
    ///
    /// controller.register_broker_lifecycle_listener(Arc::new(MyListener));
    /// ```
    fn register_broker_lifecycle_listener(&self, listener: Arc<dyn BrokerLifecycleListener>);
}

// ==================== Mock Controller for Testing ====================

/// Mock controller implementation for unit tests
///
/// Provides a no-op implementation of all Controller methods. Useful for:
/// - Testing components that depend on Controller trait
/// - Integration tests that don't require full Raft cluster
/// - Performance benchmarking
///
/// # Example
///
/// ```rust,ignore
/// use rocketmq_controller::controller::MockController;
///
/// #[tokio::test]
/// async fn test_request_processor() {
///     let controller = Arc::new(MockController::new());
///     let processor = ControllerRequestProcessor::new(controller);
///     // ... test processor logic
/// }
/// ```
pub struct MockController;

impl MockController {
    /// Create a new mock controller
    pub fn new() -> Self {
        Self
    }
}

impl Default for MockController {
    fn default() -> Self {
        Self::new()
    }
}

impl Controller for MockController {
    async fn startup(&mut self) -> RocketMQResult<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> RocketMQResult<()> {
        Ok(())
    }

    async fn start_scheduling(&self) -> RocketMQResult<()> {
        Ok(())
    }

    async fn stop_scheduling(&self) -> RocketMQResult<()> {
        Ok(())
    }

    fn is_leader(&self) -> bool {
        true // Mock is always leader
    }

    async fn register_broker(
        &self,
        _request: &RegisterBrokerToControllerRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn get_next_broker_id(
        &self,
        _request: &GetNextBrokerIdRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn apply_broker_id(&self, _request: &ApplyBrokerIdRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn clean_broker_data(
        &self,
        _cluster_name: CheetahString,
        _broker_name: CheetahString,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn elect_master(&self, _request: &ElectMasterRequestHeader) -> RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn alter_sync_state_set(
        &self,
        _request: &AlterSyncStateSetRequestHeader,
        _sync_state_set: SyncStateSet,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn get_replica_info(
        &self,
        _request: &GetReplicaInfoRequestHeader,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn get_controller_metadata(&self) -> RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn get_sync_state_data(&self, _broker_names: &[CheetahString]) -> RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(RemotingCommand::create_response_command()))
    }

    fn register_broker_lifecycle_listener(&self, _listener: Arc<dyn BrokerLifecycleListener>) {
        // No-op
    }

    /*    fn get_runtime(&self) -> Arc<RocketMQRuntime> {
        unimplemented!("MockController does not provide runtime in tests")
    }*/
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_controller_lifecycle() {
        let mut controller = MockController::new();
        assert!(controller.startup().await.is_ok());
        assert!(controller.is_leader());
        assert!(controller.shutdown().await.is_ok());
    }

    #[tokio::test]
    async fn test_mock_controller_operations() {
        let controller = MockController::new();

        // Test register_broker
        let register_req = RegisterBrokerToControllerRequestHeader::default();
        assert!(controller.register_broker(&register_req).await.is_ok());

        // Test get_next_broker_id
        let next_id_req = GetNextBrokerIdRequestHeader::default();
        assert!(controller.get_next_broker_id(&next_id_req).await.is_ok());
    }
}
