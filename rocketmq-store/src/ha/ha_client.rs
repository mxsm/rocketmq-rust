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

use crate::ha::ha_connection_state::HAConnectionState;

#[trait_variant::make(HAClient: Send)]
pub trait RocketmqHAClient: Sync {
    /// Start the HA client
    ///
    /// This initializes the client and begins connection attempts to the master.
    async fn start(&mut self);

    /// Shutdown the HA client
    ///
    /// This terminates all connections and stops background tasks.
    async fn shutdown(&self);

    /// Wakeup the HA client
    ///
    /// This forces the client to attempt reconnection if it's currently idle
    /// or disconnected.
    async fn wakeup(&self);

    /// Update the master address
    ///
    /// # Parameters
    /// * `new_address` - The new address to connect to
    async fn update_master_address(&self, new_address: &str);

    /// Update the HA master address
    ///
    /// # Parameters
    /// * `new_address` - The new HA address to connect to
    async fn update_ha_master_address(&self, new_address: &str);

    /// Get the current master address
    ///
    /// # Returns
    /// The master address as a string
    fn get_master_address(&self) -> String;

    /// Get the current HA master address
    ///
    /// # Returns
    /// The HA master address as a string
    fn get_ha_master_address(&self) -> String;

    /// Get the timestamp of the last read operation
    ///
    /// # Returns
    /// Timestamp in milliseconds since epoch
    fn get_last_read_timestamp(&self) -> i64;

    /// Get the timestamp of the last write operation
    ///
    /// # Returns
    /// Timestamp in milliseconds since epoch
    fn get_last_write_timestamp(&self) -> i64;

    /// Get the current state of the HA connection
    ///
    /// # Returns
    /// The current connection state
    fn get_current_state(&self) -> HAConnectionState;

    /// Change the current state of the HA connection (for testing)
    ///
    /// # Parameters
    /// * `ha_connection_state` - The new state to set
    fn change_current_state(&self, ha_connection_state: HAConnectionState);

    /// Disconnect from the master (for testing)
    async fn close_master(&self);

    /// Get the data transfer rate per second
    ///
    /// # Returns
    /// Number of bytes transferred per second
    fn get_transferred_byte_in_second(&self) -> i64;
}
