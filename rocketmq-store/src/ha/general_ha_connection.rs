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

use tokio::net::TcpStream;

use rocketmq_store_api::StoreError;

use crate::ha::auto_switch::auto_switch_ha_connection::AutoSwitchHAConnection;
use crate::ha::default_ha_connection::DefaultHAConnection;
use crate::ha::default_ha_connection::HAConnectionRuntimeHandle;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection::HAConnectionId;
use crate::ha::ha_connection_state::HAConnectionState;

pub enum GeneralHAConnection {
    Default(DefaultHAConnection),
    AutoSwitch(AutoSwitchHAConnection),
}

impl GeneralHAConnection {
    pub fn is_auto_switch(&self) -> bool {
        matches!(self, Self::AutoSwitch(_))
    }

    pub fn set_slave_broker_id(&self, slave_broker_id: Option<i64>) {
        match self {
            Self::Default(_) => {}
            Self::AutoSwitch(connection) => connection.set_slave_broker_id(slave_broker_id),
        }
    }

    pub fn slave_broker_id(&self) -> Option<i64> {
        match self {
            Self::Default(_) => None,
            Self::AutoSwitch(connection) => connection.slave_broker_id(),
        }
    }

    pub(crate) fn runtime_handle(&self) -> HAConnectionRuntimeHandle {
        match self {
            Self::Default(connection) => connection.runtime_handle(),
            Self::AutoSwitch(connection) => connection.runtime_handle(),
        }
    }
}

impl HAConnection for GeneralHAConnection {
    async fn start(&mut self) -> Result<(), StoreError> {
        match self {
            Self::Default(connection) => connection.start().await,
            Self::AutoSwitch(connection) => connection.start().await,
        }
    }

    async fn shutdown(&mut self) {
        match self {
            Self::Default(connection) => connection.shutdown().await,
            Self::AutoSwitch(connection) => connection.shutdown().await,
        }
    }

    fn close(&self) {
        match self {
            Self::Default(connection) => connection.close(),
            Self::AutoSwitch(connection) => connection.close(),
        }
    }

    fn get_socket(&self) -> &TcpStream {
        match self {
            Self::Default(connection) => connection.get_socket(),
            Self::AutoSwitch(connection) => connection.get_socket(),
        }
    }

    async fn get_current_state(&self) -> HAConnectionState {
        match self {
            Self::Default(connection) => connection.get_current_state().await,
            Self::AutoSwitch(connection) => connection.get_current_state().await,
        }
    }

    fn get_client_address(&self) -> &str {
        match self {
            Self::Default(connection) => connection.get_client_address(),
            Self::AutoSwitch(connection) => connection.get_client_address(),
        }
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        match self {
            Self::Default(connection) => connection.get_transferred_byte_in_second(),
            Self::AutoSwitch(connection) => connection.get_transferred_byte_in_second(),
        }
    }

    fn get_transfer_from_where(&self) -> i64 {
        match self {
            Self::Default(connection) => connection.get_transfer_from_where(),
            Self::AutoSwitch(connection) => connection.get_transfer_from_where(),
        }
    }

    fn get_slave_ack_offset(&self) -> i64 {
        match self {
            Self::Default(connection) => connection.get_slave_ack_offset(),
            Self::AutoSwitch(connection) => connection.get_slave_ack_offset(),
        }
    }

    fn get_ha_connection_id(&self) -> &HAConnectionId {
        match self {
            Self::Default(connection) => connection.get_ha_connection_id(),
            Self::AutoSwitch(connection) => connection.get_ha_connection_id(),
        }
    }

    fn remote_address(&self) -> String {
        match self {
            Self::Default(connection) => connection.remote_address(),
            Self::AutoSwitch(connection) => connection.remote_address(),
        }
    }
}
