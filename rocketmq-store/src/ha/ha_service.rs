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

use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tokio::sync::Notify;

use crate::ha::general_ha_client::GeneralHAClient;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::store_error::HAResult;

#[trait_variant::make(HAService: Send)]
pub trait RocketHAService: Sync {
    //fn init<MS: MessageStore>(&mut self, message_store: ArcMut<MS>) -> HAResult<()>;

    /// Start the HA service
    ///
    /// # Returns
    /// Result indicating success or failure
    async fn start(&mut self) -> HAResult<()>;

    /// Shutdown the HA service
    async fn shutdown(&self);

    /// Change this node to master state
    ///
    /// # Parameters
    /// * `master_epoch` - The new master epoch
    ///
    /// # Returns
    /// Whether the change was successful
    async fn change_to_master(&self, master_epoch: i32) -> HAResult<bool>;

    /// Change this node to master state when it was already a master
    ///
    /// # Parameters
    /// * `master_epoch` - The new master epoch
    ///
    /// # Returns
    /// Whether the change was successful
    async fn change_to_master_when_last_role_is_master(&self, master_epoch: i32) -> HAResult<bool>;

    /// Change this node to slave state
    ///
    /// # Parameters
    /// * `new_master_addr` - Address of the new master
    /// * `new_master_epoch` - The new master epoch
    /// * `slave_id` - Optional ID for this slave
    ///
    /// # Returns
    /// Whether the change was successful
    async fn change_to_slave(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
        slave_id: Option<i64>,
    ) -> HAResult<bool>;

    /// Change this node to slave state when master has not changed
    ///
    /// # Parameters
    /// * `new_master_addr` - Address of the new master
    /// * `new_master_epoch` - The new master epoch
    ///
    /// # Returns
    /// Whether the change was successful
    async fn change_to_slave_when_master_not_change(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
    ) -> HAResult<bool>;

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

    /// Put a connection state notification request
    ///
    /// # Parameters
    /// * `request` - The connection state request
    async fn put_group_connection_state_request(&self, request: HAConnectionStateNotificationRequest);

    /// Get the list of HA connections
    ///
    /// # Returns
    /// List of HA connections
    async fn get_connection_list(&self) -> Vec<ArcMut<GeneralHAConnection>>;

    /// Get the HA client instance.
    ///
    /// This function returns an optional reference to the `GeneralHAClient` instance,
    /// which represents the client used for high availability operations.
    ///
    /// # Returns
    /// A reference to an `Option<GeneralHAClient>` instance.
    fn get_ha_client(&self) -> Option<&GeneralHAClient>;

    /// Get a mutable reference to the HA client instance.
    ///
    /// This function provides a mutable reference to the `GeneralHAClient` instance,
    /// allowing modifications to the high availability client used for operations.
    ///
    /// # Returns
    /// A mutable reference to an `Option<GeneralHAClient>` instance.
    fn get_ha_client_mut(&mut self) -> Option<&mut GeneralHAClient>;

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
