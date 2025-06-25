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

use tokio::net::TcpStream;

use crate::ha::auto_switch::auto_switch_ha_connection::AutoSwitchHAConnection;
use crate::ha::default_ha_connection::DefaultHAConnection;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::HAConnectionError;

pub struct GeneralHAConnection {
    default_ha_connection: Option<DefaultHAConnection>,
    auto_switch_ha_connection: Option<AutoSwitchHAConnection>,
}

impl GeneralHAConnection {
    pub fn new() -> Self {
        GeneralHAConnection {
            default_ha_connection: None,
            auto_switch_ha_connection: None,
        }
    }

    pub fn new_with_default_ha_connection(default_ha_connection: DefaultHAConnection) -> Self {
        GeneralHAConnection {
            default_ha_connection: Some(default_ha_connection),
            auto_switch_ha_connection: None,
        }
    }

    pub fn new_with_auto_switch_ha_connection(
        auto_switch_ha_connection: AutoSwitchHAConnection,
    ) -> Self {
        GeneralHAConnection {
            default_ha_connection: None,
            auto_switch_ha_connection: Some(auto_switch_ha_connection),
        }
    }

    pub fn set_default_ha_connection(&mut self, connection: DefaultHAConnection) {
        self.default_ha_connection = Some(connection);
    }

    pub fn set_auto_switch_ha_connection(&mut self, connection: AutoSwitchHAConnection) {
        self.auto_switch_ha_connection = Some(connection);
    }
}

impl HAConnection for GeneralHAConnection {
    async fn start(&mut self) -> Result<(), HAConnectionError> {
        if let Some(ref mut connection) = self.default_ha_connection {
            connection.start().await
        } else if let Some(ref mut connection) = self.auto_switch_ha_connection {
            connection.start().await
        } else {
            Err(HAConnectionError::Connection(
                "No HA connection set".to_string(),
            ))
        }
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
}
