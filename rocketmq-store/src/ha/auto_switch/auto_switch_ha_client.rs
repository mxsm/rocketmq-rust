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

use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state::HAConnectionState;

pub struct AutoSwitchHAClient;

impl HAClient for AutoSwitchHAClient {
    async fn start(&self) {
        todo!()
    }

    async fn shutdown(&self) {
        todo!()
    }

    async fn wakeup(&self) {
        todo!()
    }

    fn update_master_address(&self, new_address: &str) {
        todo!()
    }

    fn update_ha_master_address(&self, new_address: &str) {
        todo!()
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
