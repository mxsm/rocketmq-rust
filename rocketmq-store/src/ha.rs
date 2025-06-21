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

use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;

use crate::base::message_store::MessageStore;
use crate::ha::auto_switch::auto_switch_ha_service::AutoSwitchHAService;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAService;
use crate::ha::wait_notify_object::WaitNotifyObject;
use crate::log_file::flush_manager_impl::group_commit_request::GroupCommitRequest;
use crate::store_error::HAResult;

mod auto_switch;
mod default_ha_connection;
pub(crate) mod default_ha_service;
pub(crate) mod flow_monitor;
pub(crate) mod general_ha_service;
pub(crate) mod ha_client;
pub(crate) mod ha_connection;
pub(crate) mod ha_connection_state;
pub(crate) mod ha_connection_state_notification_request;
pub(crate) mod ha_service;
pub(crate) mod wait_notify_object;

pub struct HAServiceWrapper {
    default_ha_service: Option<DefaultHAService>,
    auto_switch_ha_service: Option<AutoSwitchHAService>,
}

impl Default for HAServiceWrapper {
    fn default() -> Self {
        Self::new()
    }
}

impl HAServiceWrapper {
    pub fn new() -> Self {
        Self {
            default_ha_service: None,
            auto_switch_ha_service: None,
        }
    }

    pub fn set_default_ha_service(&mut self, service: DefaultHAService) {
        self.default_ha_service = Some(service);
    }

    pub fn set_auto_switch_ha_service(&mut self, service: AutoSwitchHAService) {
        self.auto_switch_ha_service = Some(service);
    }

    pub fn get_default_ha_service(&self) -> Option<&DefaultHAService> {
        self.default_ha_service.as_ref()
    }

    pub fn get_auto_switch_ha_service(&self) -> Option<&AutoSwitchHAService> {
        self.auto_switch_ha_service.as_ref()
    }
}

impl HAService for HAServiceWrapper {
    fn init<MS: MessageStore>(&mut self, message_store: ArcMut<MS>) -> HAResult<()> {
        todo!()
    }

    fn start(&mut self) -> HAResult<()> {
        todo!()
    }

    fn shutdown(&self) {
        todo!()
    }

    async fn change_to_master(&self, master_epoch: i32) -> HAResult<bool> {
        todo!()
    }

    async fn change_to_master_when_last_role_is_master(&self, master_epoch: i32) -> HAResult<bool> {
        todo!()
    }

    async fn change_to_slave(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
        slave_id: Option<i64>,
    ) -> HAResult<bool> {
        todo!()
    }

    async fn change_to_slave_when_master_not_change(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
    ) -> HAResult<bool> {
        todo!()
    }

    fn update_master_address(&self, new_addr: &str) {
        todo!()
    }

    fn update_ha_master_address(&self, new_addr: &str) {
        todo!()
    }

    fn in_sync_replicas_nums(&self, master_put_where: i64) -> i32 {
        todo!()
    }

    fn get_connection_count(&self) -> &AtomicI32 {
        todo!()
    }

    fn put_request(&self, request: GroupCommitRequest) {
        todo!()
    }

    fn put_group_connection_state_request(&self, request: HAConnectionStateNotificationRequest) {
        todo!()
    }

    fn get_connection_list<CN: HAConnection>(&self) -> Vec<Arc<CN>> {
        todo!()
    }

    fn get_ha_client<CL: HAClient>(&self) -> Arc<CL> {
        todo!()
    }

    fn get_push_to_slave_max_offset(&self) -> &AtomicI64 {
        todo!()
    }

    fn get_runtime_info(&self, master_put_where: i64) -> HARuntimeInfo {
        todo!()
    }

    fn get_wait_notify_object(&self) -> Arc<WaitNotifyObject> {
        todo!()
    }

    fn is_slave_ok(&self, master_put_where: i64) -> bool {
        todo!()
    }
}
