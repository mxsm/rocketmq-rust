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
use rocketmq_rust::ArcMut;

use crate::ha::auto_switch::auto_switch_ha_client::AutoSwitchHAClient;
use crate::ha::default_ha_client::DefaultHAClient;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state::HAConnectionState;

#[derive(Clone)]
pub struct GeneralHAClient {
    default_ha_client: Option<ArcMut<DefaultHAClient>>,
    auto_switch_ha_client: Option<ArcMut<AutoSwitchHAClient>>,
}

impl Default for GeneralHAClient {
    fn default() -> Self {
        GeneralHAClient::new()
    }
}

impl GeneralHAClient {
    pub fn new() -> Self {
        GeneralHAClient {
            default_ha_client: None,
            auto_switch_ha_client: None,
        }
    }

    pub fn new_with_default_ha_client(default_ha_client: DefaultHAClient) -> Self {
        GeneralHAClient {
            default_ha_client: Some(ArcMut::new(default_ha_client)),
            auto_switch_ha_client: None,
        }
    }

    pub fn new_with_auto_switch_ha_client(auto_switch_ha_client: AutoSwitchHAClient) -> Self {
        GeneralHAClient {
            default_ha_client: None,
            auto_switch_ha_client: Some(ArcMut::new(auto_switch_ha_client)),
        }
    }

    pub fn set_default_ha_service(&mut self, service: ArcMut<DefaultHAClient>) {
        self.default_ha_client = Some(service);
    }

    pub fn set_auto_switch_ha_service(&mut self, service: AutoSwitchHAClient) {
        self.auto_switch_ha_client = Some(ArcMut::new(service));
    }

    // Additional methods to interact with the HA services can be added here
}

impl HAClient for GeneralHAClient {
    async fn start(&self) {
        if let Some(ref client) = self.default_ha_client {
            client.start().await;
        } else if let Some(ref client) = self.auto_switch_ha_client {
            client.start().await;
        } else {
            panic!("No HA service is set for GeneralHAClient");
        }
    }

    async fn shutdown(&self) {
        todo!()
    }

    async fn wakeup(&self) {
        todo!()
    }

    fn update_master_address(&self, new_address: &str) {
        if let Some(ref client) = self.default_ha_client {
            client.update_master_address(new_address);
        } else if let Some(ref client) = self.auto_switch_ha_client {
            client.update_master_address(new_address);
        } else {
            panic!("No HA service is set for GeneralHAClient");
        }
    }

    fn update_ha_master_address(&self, new_address: &str) {
        if let Some(ref client) = self.default_ha_client {
            client.update_ha_master_address(new_address);
        } else if let Some(ref client) = self.auto_switch_ha_client {
            client.update_ha_master_address(new_address);
        } else {
            panic!("No HA service is set for GeneralHAClient");
        }
    }

    fn get_master_address(&self) -> String {
        todo!()
    }

    fn get_ha_master_address(&self) -> String {
        todo!()
    }

    fn get_last_read_timestamp(&self) -> i64 {
        todo!()
    }

    fn get_last_write_timestamp(&self) -> i64 {
        todo!()
    }

    fn get_current_state(&self) -> HAConnectionState {
        todo!()
    }

    fn change_current_state(&self, ha_connection_state: HAConnectionState) {
        todo!()
    }

    async fn close_master(&self) {
        todo!()
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        todo!()
    }
}
