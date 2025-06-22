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

pub struct GeneralHAClient {
    default_ha_service: Option<ArcMut<DefaultHAClient>>,
    auto_switch_ha_service: Option<AutoSwitchHAClient>,
}

impl Default for GeneralHAClient {
    fn default() -> Self {
        GeneralHAClient::new()
    }
}

impl GeneralHAClient {
    pub fn new() -> Self {
        GeneralHAClient {
            default_ha_service: None,
            auto_switch_ha_service: None,
        }
    }

    pub fn set_default_ha_service(&mut self, service: ArcMut<DefaultHAClient>) {
        self.default_ha_service = Some(service);
    }

    pub fn set_auto_switch_ha_service(&mut self, service: AutoSwitchHAClient) {
        self.auto_switch_ha_service = Some(service);
    }

    // Additional methods to interact with the HA services can be added here
}
