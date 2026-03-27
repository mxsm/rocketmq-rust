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

use crate::ha::auto_switch::auto_switch_ha_client::AutoSwitchHAClient;
use crate::ha::default_ha_client::DefaultHAClient;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state::HAConnectionState;

#[derive(Clone)]
pub enum GeneralHAClient {
    DefaultHaClient(ArcMut<DefaultHAClient>),
    AutoSwitchHaClient(ArcMut<AutoSwitchHAClient>),
}

impl GeneralHAClient {
    pub fn new_with_default_ha_client(default_ha_client: DefaultHAClient) -> Self {
        GeneralHAClient::DefaultHaClient(ArcMut::new(default_ha_client))
    }

    pub fn new_with_auto_switch_ha_client(auto_switch_ha_client: AutoSwitchHAClient) -> Self {
        GeneralHAClient::AutoSwitchHaClient(ArcMut::new(auto_switch_ha_client))
    }

    pub fn set_reported_broker_id(&self, broker_id: Option<i64>) {
        if let GeneralHAClient::DefaultHaClient(client) = self {
            client.set_reported_broker_id(broker_id);
        }
    }
}

impl HAClient for GeneralHAClient {
    async fn start(&mut self) {
        match self {
            GeneralHAClient::DefaultHaClient(client) => {
                client.start().await;
            }
            GeneralHAClient::AutoSwitchHaClient(client) => {
                client.start().await;
            }
        }
    }

    async fn shutdown(&self) {
        match self {
            GeneralHAClient::DefaultHaClient(client) => {
                client.shutdown().await;
            }
            GeneralHAClient::AutoSwitchHaClient(client) => {
                client.shutdown().await;
            }
        }
    }

    async fn wakeup(&self) {
        match self {
            GeneralHAClient::DefaultHaClient(client) => {
                client.wakeup().await;
            }
            GeneralHAClient::AutoSwitchHaClient(client) => {
                client.wakeup().await;
            }
        }
    }

    async fn update_master_address(&self, new_address: &str) {
        match self {
            GeneralHAClient::DefaultHaClient(client) => {
                client.update_master_address(new_address).await;
            }
            GeneralHAClient::AutoSwitchHaClient(client) => {
                client.update_master_address(new_address).await;
            }
        }
    }

    async fn update_ha_master_address(&self, new_address: &str) {
        match self {
            GeneralHAClient::DefaultHaClient(client) => {
                client.update_ha_master_address(new_address).await;
            }
            GeneralHAClient::AutoSwitchHaClient(client) => {
                client.update_ha_master_address(new_address).await;
            }
        }
    }

    fn get_master_address(&self) -> String {
        match self {
            GeneralHAClient::DefaultHaClient(client) => HAClient::get_master_address(client.as_ref()),
            GeneralHAClient::AutoSwitchHaClient(client) => client.get_master_address(),
        }
    }

    fn get_ha_master_address(&self) -> String {
        match self {
            GeneralHAClient::DefaultHaClient(client) => HAClient::get_ha_master_address(client.as_ref()),
            GeneralHAClient::AutoSwitchHaClient(client) => client.get_ha_master_address(),
        }
    }

    fn get_last_read_timestamp(&self) -> i64 {
        match self {
            GeneralHAClient::DefaultHaClient(client) => HAClient::get_last_read_timestamp(client.as_ref()),
            GeneralHAClient::AutoSwitchHaClient(client) => client.get_last_read_timestamp(),
        }
    }

    fn get_last_write_timestamp(&self) -> i64 {
        match self {
            GeneralHAClient::DefaultHaClient(client) => HAClient::get_last_write_timestamp(client.as_ref()),
            GeneralHAClient::AutoSwitchHaClient(client) => client.get_last_write_timestamp(),
        }
    }

    fn get_current_state(&self) -> HAConnectionState {
        match self {
            GeneralHAClient::DefaultHaClient(client) => HAClient::get_current_state(client.as_ref()),
            GeneralHAClient::AutoSwitchHaClient(client) => client.get_current_state(),
        }
    }

    fn change_current_state(&self, ha_connection_state: HAConnectionState) {
        match self {
            GeneralHAClient::DefaultHaClient(client) => client.change_current_state(ha_connection_state),
            GeneralHAClient::AutoSwitchHaClient(client) => client.change_current_state(ha_connection_state),
        }
    }

    async fn close_master(&self) {
        match self {
            GeneralHAClient::DefaultHaClient(client) => {
                client.close_master().await;
            }
            GeneralHAClient::AutoSwitchHaClient(client) => {
                client.close_master().await;
            }
        }
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        match self {
            GeneralHAClient::DefaultHaClient(client) => HAClient::get_transferred_byte_in_second(client.as_ref()),
            GeneralHAClient::AutoSwitchHaClient(client) => client.get_transferred_byte_in_second(),
        }
    }
}
