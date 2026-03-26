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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use rocketmq_rust::WeakArcMut;
use tokio::net::TcpStream;

use crate::ha::default_ha_connection::DefaultHAConnection;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection::HAConnectionId;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::HAConnectionError;

pub struct AutoSwitchHAConnection {
    delegate: DefaultHAConnection,
    slave_broker_id: AtomicI64,
}

impl AutoSwitchHAConnection {
    pub fn new(delegate: DefaultHAConnection) -> Self {
        Self {
            delegate,
            slave_broker_id: AtomicI64::new(-1),
        }
    }

    pub fn set_slave_broker_id(&self, slave_broker_id: Option<i64>) {
        self.slave_broker_id
            .store(slave_broker_id.unwrap_or(-1), Ordering::SeqCst);
    }

    pub fn slave_broker_id(&self) -> Option<i64> {
        match self.slave_broker_id.load(Ordering::SeqCst) {
            id if id >= 0 => Some(id),
            _ => None,
        }
    }
}

impl HAConnection for AutoSwitchHAConnection {
    async fn start(&mut self, conn: WeakArcMut<GeneralHAConnection>) -> Result<(), HAConnectionError> {
        self.delegate.start(conn).await
    }

    async fn shutdown(&mut self) {
        self.delegate.shutdown().await;
    }

    fn close(&self) {
        self.delegate.close();
    }

    fn get_socket(&self) -> &TcpStream {
        self.delegate.get_socket()
    }

    async fn get_current_state(&self) -> HAConnectionState {
        self.delegate.get_current_state().await
    }

    fn get_client_address(&self) -> &str {
        self.delegate.get_client_address()
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        self.delegate.get_transferred_byte_in_second()
    }

    fn get_transfer_from_where(&self) -> i64 {
        self.delegate.get_transfer_from_where()
    }

    fn get_slave_ack_offset(&self) -> i64 {
        self.delegate.get_slave_ack_offset()
    }

    fn get_ha_connection_id(&self) -> &HAConnectionId {
        self.delegate.get_ha_connection_id()
    }

    fn remote_address(&self) -> String {
        self.delegate.remote_address()
    }
}
