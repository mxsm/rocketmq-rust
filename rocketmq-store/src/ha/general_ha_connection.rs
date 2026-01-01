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

use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::net::TcpStream;

use crate::ha::auto_switch::auto_switch_ha_connection::AutoSwitchHAConnection;
use crate::ha::default_ha_connection::DefaultHAConnection;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection::HAConnectionId;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::HAConnectionError;

#[derive(Default)]
pub struct GeneralHAConnection {
    default_ha_connection: Option<ArcMut<DefaultHAConnection>>,
    auto_switch_ha_connection: Option<ArcMut<AutoSwitchHAConnection>>,
}

impl GeneralHAConnection {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_default_ha_connection(default_ha_connection: DefaultHAConnection) -> Self {
        GeneralHAConnection {
            default_ha_connection: Some(ArcMut::new(default_ha_connection)),
            auto_switch_ha_connection: None,
        }
    }

    pub fn new_with_auto_switch_ha_connection(auto_switch_ha_connection: AutoSwitchHAConnection) -> Self {
        GeneralHAConnection {
            default_ha_connection: None,
            auto_switch_ha_connection: Some(ArcMut::new(auto_switch_ha_connection)),
        }
    }

    pub fn set_default_ha_connection(&mut self, connection: DefaultHAConnection) {
        self.default_ha_connection = Some(ArcMut::new(connection));
    }

    pub fn set_auto_switch_ha_connection(&mut self, connection: AutoSwitchHAConnection) {
        self.auto_switch_ha_connection = Some(ArcMut::new(connection));
    }
}

impl HAConnection for GeneralHAConnection {
    async fn start(&mut self, conn: WeakArcMut<GeneralHAConnection>) -> Result<(), HAConnectionError> {
        match (&mut self.default_ha_connection, &mut self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.start(conn).await,
            (_, Some(connection)) => connection.start(conn).await,
            (None, None) => Err(HAConnectionError::Connection("No HA connection set".to_string())),
        }
    }

    async fn shutdown(&mut self) {
        match (&mut self.default_ha_connection, &mut self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.shutdown().await,
            (_, Some(connection)) => connection.shutdown().await,
            (None, None) => {
                tracing::warn!("No HA connection to shutdown");
            }
        }
    }

    fn close(&self) {
        match (&self.default_ha_connection, &self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.close(),
            (_, Some(connection)) => connection.close(),
            (None, None) => {
                tracing::warn!("No HA connection to close");
            }
        }
    }

    fn get_socket(&self) -> &TcpStream {
        match (&self.default_ha_connection, &self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.get_socket(),
            (_, Some(connection)) => connection.get_socket(),
            (None, None) => {
                tracing::warn!("No HA connection to get socket from");
                panic!("No HA connection available");
            }
        }
    }

    async fn get_current_state(&self) -> HAConnectionState {
        match (&self.default_ha_connection, &self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.get_current_state().await,
            (_, Some(connection)) => connection.get_current_state().await,
            (None, None) => {
                tracing::warn!("No HA connection to get current state from");
                panic!("No HA connection available");
            }
        }
    }

    fn get_client_address(&self) -> &str {
        match (&self.default_ha_connection, &self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.get_client_address(),
            (_, Some(connection)) => connection.get_client_address(),
            (None, None) => {
                tracing::warn!("No HA connection to get client address from");
                panic!("No HA connection available");
            }
        }
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        match (&self.default_ha_connection, &self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.get_transferred_byte_in_second(),
            (_, Some(connection)) => connection.get_transferred_byte_in_second(),
            (None, None) => {
                tracing::warn!("No HA connection to get transferred bytes from");
                panic!("No HA connection available");
            }
        }
    }

    fn get_transfer_from_where(&self) -> i64 {
        match (&self.default_ha_connection, &self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.get_transfer_from_where(),
            (_, Some(connection)) => connection.get_transfer_from_where(),
            (None, None) => {
                tracing::warn!("No HA connection to get transfer offset from");
                panic!("No HA connection available");
            }
        }
    }

    fn get_slave_ack_offset(&self) -> i64 {
        match (&self.default_ha_connection, &self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.get_slave_ack_offset(),
            (_, Some(connection)) => connection.get_slave_ack_offset(),
            (None, None) => {
                tracing::warn!("No HA connection to get slave ack offset from");
                panic!("No HA connection available");
            }
        }
    }

    fn get_ha_connection_id(&self) -> &HAConnectionId {
        match (&self.default_ha_connection, &self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.get_ha_connection_id(),
            (_, Some(connection)) => connection.get_ha_connection_id(),
            (None, None) => {
                tracing::warn!("No HA connection to get ID from");
                panic!("No HA connection available");
            }
        }
    }

    fn remote_address(&self) -> String {
        match (&self.default_ha_connection, &self.auto_switch_ha_connection) {
            (Some(connection), _) => connection.remote_address(),
            (_, Some(connection)) => connection.remote_address(),
            (None, None) => {
                tracing::warn!("No HA connection to get remote address from");
                panic!("No HA connection available");
            }
        }
    }
}
