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

use rocketmq_rust::WeakArcMut;
use tokio::net::TcpStream;

use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection::HAConnectionId;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::HAConnectionError;

pub struct AutoSwitchHAConnection;

impl HAConnection for AutoSwitchHAConnection {
    async fn start(&mut self, conn: WeakArcMut<GeneralHAConnection>) -> Result<(), HAConnectionError> {
        todo!()
    }

    async fn shutdown(&mut self) {
        todo!()
    }

    fn close(&self) {
        todo!()
    }

    fn get_socket(&self) -> &TcpStream {
        todo!()
    }

    async fn get_current_state(&self) -> HAConnectionState {
        todo!()
    }

    fn get_client_address(&self) -> &str {
        todo!()
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        todo!()
    }

    fn get_transfer_from_where(&self) -> i64 {
        todo!()
    }

    fn get_slave_ack_offset(&self) -> i64 {
        todo!()
    }

    fn get_ha_connection_id(&self) -> &HAConnectionId {
        todo!()
    }

    fn remote_address(&self) -> String {
        todo!()
    }
}
