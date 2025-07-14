/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use rocketmq_rust::WeakArcMut;
use tokio::net::TcpStream;

use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::HAConnectionError;

#[trait_variant::make(HAConnection: Send)]
pub trait RocketmqHAConnection: Sync {
    /// Start the HA connection
    ///
    /// This initiates the connection threads and begins processing.
    async fn start(
        &mut self,
        conn: WeakArcMut<GeneralHAConnection>,
    ) -> Result<(), HAConnectionError>;

    /// Shutdown the HA connection gracefully
    ///
    /// This initiates a clean shutdown of the connection.
    async fn shutdown(&mut self);

    /// Close the HA connection immediately
    ///
    /// This forcibly closes the connection without waiting for pending operations.
    fn close(&self);

    /// Get the underlying TCP stream
    ///
    /// # Returns
    /// Reference to the TCP stream for this connection
    fn get_socket(&self) -> &TcpStream;

    /// Get the current state of the connection
    ///
    /// # Returns
    /// Current connection state
    async fn get_current_state(&self) -> HAConnectionState;

    /// Get the client address for this connection
    ///
    /// # Returns
    /// Socket address of the connected client
    fn get_client_address(&self) -> &str;

    /// Get the data transfer rate per second
    ///
    /// # Returns
    /// Number of bytes transferred per second
    fn get_transferred_byte_in_second(&self) -> i64;

    /// Get the current transfer offset to the slave
    ///
    /// # Returns
    /// Current offset being transferred
    fn get_transfer_from_where(&self) -> i64;

    /// Get the latest offset acknowledged by the slave
    ///
    /// # Returns
    /// The latest offset confirmed by the slave
    fn get_slave_ack_offset(&self) -> i64;
}
