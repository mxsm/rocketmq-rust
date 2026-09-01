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

use std::sync::atomic::AtomicU32;

use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
use tokio::sync::Notify;

use crate::ha::general_ha_client::GeneralHAClient;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::log_file::group_commit_request::GroupCommitRequest;
use rocketmq_store_api::StoreError;
pub(crate) use rocketmq_store_local::ha::replication::HAAckedReplicaSnapshot;

#[trait_variant::make(HAService: Send)]
pub trait RocketHAService: Sync {
    /// Starts the HA service.
    ///
    /// # Errors
    ///
    /// Returns a storage error when binding, runtime startup, or another operational HA
    /// initialization step fails.
    async fn start(&self) -> Result<(), StoreError>;

    /// Shutdown the HA service
    async fn shutdown(&self);

    /// Changes this node to master state.
    ///
    /// # Parameters
    /// * `master_epoch` - The new master epoch
    ///
    /// Returns `Ok(false)` when the requested epoch or role transition is rejected by the HA
    /// contract, and `Ok(true)` after the transition is applied.
    ///
    /// # Errors
    ///
    /// Returns a storage error only when the transition fails operationally.
    async fn change_to_master(&self, master_epoch: i32) -> Result<bool, StoreError>;

    /// Changes this node to master state when it was already a master.
    ///
    /// # Parameters
    /// * `master_epoch` - The new master epoch
    ///
    /// Returns `Ok(false)` when the requested epoch or transition is rejected by the HA
    /// contract, and `Ok(true)` after the transition is applied.
    ///
    /// # Errors
    ///
    /// Returns a storage error only when the transition fails operationally.
    async fn change_to_master_when_last_role_is_master(&self, master_epoch: i32) -> Result<bool, StoreError>;

    /// Changes this node to slave state.
    ///
    /// # Parameters
    /// * `new_master_addr` - Address of the new master
    /// * `new_master_epoch` - The new master epoch
    /// * `slave_id` - Optional ID for this slave
    ///
    /// Returns `Ok(false)` when the address, epoch, replica identifier, or requested transition
    /// is rejected by the HA contract, and `Ok(true)` after the transition is applied.
    ///
    /// # Errors
    ///
    /// Returns a storage error only when the transition fails operationally.
    async fn change_to_slave(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
        slave_id: Option<i64>,
    ) -> Result<bool, StoreError>;

    /// Changes this node to slave state when the master has not changed.
    ///
    /// # Parameters
    /// * `new_master_addr` - Address of the new master
    /// * `new_master_epoch` - The new master epoch
    ///
    /// Returns `Ok(false)` when the address, epoch, or requested transition is rejected by the HA
    /// contract, and `Ok(true)` after the transition is applied.
    ///
    /// # Errors
    ///
    /// Returns a storage error only when the transition fails operationally.
    async fn change_to_slave_when_master_not_change(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
    ) -> Result<bool, StoreError>;

    /// Update the master address
    ///
    /// # Parameters
    /// * `new_addr` - New master address
    async fn update_master_address(&self, new_addr: &str);

    /// Update the HA master address
    ///
    /// # Parameters
    /// * `new_addr` - New HA master address
    async fn update_ha_master_address(&self, new_addr: &str);

    /// Get the number of replicas whose commit logs are not far behind the master
    ///
    /// This includes the master itself. Returns syncStateSet size if this service
    /// is an AutoSwitchService.
    ///
    /// # Parameters
    /// * `master_put_where` - Current write position of the master
    ///
    /// # Returns
    /// Number of in-sync replicas
    fn in_sync_replicas_nums(&self, master_put_where: i64) -> i32;

    /// Get the connection count
    ///
    /// # Returns
    /// Atomic reference to connection count
    fn get_connection_count(&self) -> &AtomicU32;

    /// Put a group commit request to handle HA
    ///
    /// # Parameters
    /// * `request` - The commit request
    async fn put_request(&self, request: GroupCommitRequest);

    /// Wake pending group-transfer requests after local durability, membership, or authority
    /// progress changes.
    fn notify_transfer_progress(&self);

    /// Put a connection state notification request
    ///
    /// # Parameters
    /// * `request` - The connection state request
    async fn put_group_connection_state_request(&self, request: HAConnectionStateNotificationRequest);

    /// Snapshot replica acknowledgements without exposing connection owners.
    ///
    /// # Returns
    /// Owned replica acknowledgement snapshots.
    async fn snapshot_acked_replicas(&self) -> Vec<HAAckedReplicaSnapshot>;

    /// Look up the current state for one remote HA connection.
    ///
    /// # Returns
    /// The current state when the remote address is registered.
    async fn connection_state(&self, remote_addr: &str) -> Option<HAConnectionState>;

    /// Get the HA client instance.
    ///
    /// This function returns an optional reference to the `GeneralHAClient` instance,
    /// which represents the client used for high availability operations.
    ///
    /// # Returns
    /// A reference to an `Option<GeneralHAClient>` instance.
    fn get_ha_client(&self) -> Option<&GeneralHAClient>;

    /// Get the maximum offset across all slaves
    ///
    /// # Returns
    /// Atomic reference to the maximum slave offset
    fn get_push_to_slave_max_offset(&self) -> i64;

    /// Get HA runtime information
    ///
    /// # Parameters
    /// * `master_put_where` - Current write position of the master
    ///
    /// # Returns
    /// Runtime info about the HA service
    fn get_runtime_info(&self, master_put_where: i64) -> HARuntimeInfo;

    /// Get the wait/notify synchronization object
    ///
    /// # Returns
    /// Reference to the wait/notify object
    fn get_wait_notify_object(&self) -> &Notify;

    /// Check if the slave is keeping up with the master
    ///
    /// If the offset gap exceeds haSlaveFallBehindMax, then the slave is not OK.
    ///
    /// # Parameters
    /// * `master_put_where` - Current write position of the master
    ///
    /// # Returns
    /// Whether the slave is keeping up
    async fn is_slave_ok(&self, master_put_where: i64) -> bool;
}
