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

use crate::ha::default_ha_client::DefaultHAClient;
use crate::ha::default_ha_client::HAClientError;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::message_store::local_file_message_store::LocalFileMessageStore;

pub struct AutoSwitchHAClient {
    delegate: ArcMut<DefaultHAClient>,
}

impl AutoSwitchHAClient {
    pub fn new(message_store: ArcMut<LocalFileMessageStore>, broker_id: Option<i64>) -> Result<Self, HAClientError> {
        let client = DefaultHAClient::new(message_store)?;
        client.set_reported_broker_id(broker_id);
        Ok(Self {
            delegate: ArcMut::new(client),
        })
    }

    pub fn set_reported_broker_id(&self, broker_id: Option<i64>) {
        self.delegate.set_reported_broker_id(broker_id);
    }
}

impl HAClient for AutoSwitchHAClient {
    async fn start(&mut self) {
        self.delegate.start().await;
    }

    async fn shutdown(&self) {
        self.delegate.shutdown().await;
    }

    async fn wakeup(&self) {
        self.delegate.wakeup().await;
    }

    async fn update_master_address(&self, new_address: &str) {
        self.delegate.update_master_address(new_address).await;
    }

    async fn update_ha_master_address(&self, new_address: &str) {
        self.delegate.update_ha_master_address(new_address).await;
    }

    fn get_master_address(&self) -> String {
        HAClient::get_master_address(&*self.delegate)
    }

    fn get_ha_master_address(&self) -> String {
        HAClient::get_ha_master_address(&*self.delegate)
    }

    fn get_last_read_timestamp(&self) -> i64 {
        HAClient::get_last_read_timestamp(&*self.delegate)
    }

    fn get_last_write_timestamp(&self) -> i64 {
        HAClient::get_last_write_timestamp(&*self.delegate)
    }

    fn get_current_state(&self) -> HAConnectionState {
        HAClient::get_current_state(&*self.delegate)
    }

    fn change_current_state(&self, ha_connection_state: HAConnectionState) {
        self.delegate.change_current_state(ha_connection_state);
    }

    async fn close_master(&self) {
        self.delegate.close_master().await;
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        HAClient::get_transferred_byte_in_second(&*self.delegate)
    }
}
