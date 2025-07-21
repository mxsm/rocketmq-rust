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
use std::sync::Arc;

use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tracing::error;

use crate::ha::auto_switch::auto_switch_ha_service::AutoSwitchHAService;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAService;
use crate::ha::wait_notify_object::WaitNotifyObject;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::store_error::HAError;
use crate::store_error::HAResult;

#[derive(Clone, Default)]
pub struct GeneralHAService {
    default_ha_service: Option<ArcMut<DefaultHAService>>,
    auto_switch_ha_service: Option<ArcMut<AutoSwitchHAService>>,
}

impl GeneralHAService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_default_ha_service(default_ha_service: ArcMut<DefaultHAService>) -> Self {
        GeneralHAService {
            default_ha_service: Some(default_ha_service),
            auto_switch_ha_service: None,
        }
    }

    pub(crate) fn init(&mut self, message_store: ArcMut<LocalFileMessageStore>) -> HAResult<()> {
        if message_store
            .get_message_store_config()
            .enable_controller_mode
        {
            self.auto_switch_ha_service = Some(ArcMut::new(AutoSwitchHAService))
        } else {
            let mut default_ha_service = ArcMut::new(DefaultHAService::new(message_store));
            let default_ha_service_clone = default_ha_service.clone();
            DefaultHAService::init(&mut default_ha_service, default_ha_service_clone)?;
            self.default_ha_service = Some(default_ha_service);
        }
        Ok(())
    }

    #[inline]
    pub fn is_auto_switch_enabled(&self) -> bool {
        self.auto_switch_ha_service.is_some()
    }
}

impl HAService for GeneralHAService {
    async fn start(&mut self) -> HAResult<()> {
        if let Some(ref mut service) = self.default_ha_service {
            service.start().await?;
        } else if let Some(ref mut service) = self.auto_switch_ha_service {
            service.start().await?;
        } else {
            error!("No HA service initialized");
            return Err(HAError::Service("No HA service initialized".to_string()));
        }
        Ok(())
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
        if let Some(ref service) = self.default_ha_service {
            service.update_master_address(new_addr);
        } else if let Some(ref service) = self.auto_switch_ha_service {
            service.update_master_address(new_addr);
        } else {
            error!("No HA service initialized to update master address");
        }
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

    async fn put_request(&self, request: ArcMut<GroupCommitRequest>) {
        match (&self.default_ha_service, &self.auto_switch_ha_service) {
            (Some(default_ha_service), _) => {
                default_ha_service.put_request(request).await;
            }
            (_, Some(auto_switch_service)) => {
                auto_switch_service.put_request(request).await;
            }
            (None, None) => {
                error!("No HA service initialized to put request");
            }
        }
    }

    fn put_group_connection_state_request(&self, request: HAConnectionStateNotificationRequest) {
        todo!()
    }

    async fn get_connection_list(&self) -> Vec<ArcMut<GeneralHAConnection>> {
        match (&self.default_ha_service, &self.auto_switch_ha_service) {
            (Some(default_ha_service), _) => default_ha_service.get_connection_list().await,
            (_, Some(auto_switch_service)) => auto_switch_service.get_connection_list().await,
            (None, None) => {
                error!("No HA service initialized to get connection list");
                Vec::new()
            }
        }
    }

    fn get_ha_client(&self) -> &GeneralHAConnection {
        todo!()
    }

    fn get_ha_client_mut(&mut self) -> &mut GeneralHAConnection {
        todo!()
    }

    fn get_push_to_slave_max_offset(&self) -> i64 {
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
