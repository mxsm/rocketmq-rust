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

use std::sync::atomic::AtomicU32;

use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tokio::sync::Notify;

use crate::ha::auto_switch::auto_switch_ha_service::AutoSwitchHAService;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::general_ha_client::GeneralHAClient;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAService;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::store_error::HAResult;

#[derive(Clone)]
pub enum GeneralHAService {
    DefaultHAService(ArcMut<DefaultHAService>),
    AutoSwitchHAService(ArcMut<AutoSwitchHAService>),
}

impl GeneralHAService {
    pub fn new_with_default_ha_service(default_ha_service: ArcMut<DefaultHAService>) -> Self {
        GeneralHAService::DefaultHAService(default_ha_service)
    }

    pub fn new_with_auto_switch_ha_service(auto_switch_ha_service: ArcMut<AutoSwitchHAService>) -> Self {
        GeneralHAService::AutoSwitchHAService(auto_switch_ha_service)
    }

    pub(crate) fn init(&mut self) -> HAResult<()> {
        let ha_service = self.clone();
        match self {
            GeneralHAService::DefaultHAService(service) => DefaultHAService::init(service, ha_service),
            GeneralHAService::AutoSwitchHAService(service) => {
                unimplemented!("AutoSwitchHAService init is not implemented yet")
            }
        }
    }

    #[inline]
    pub fn is_auto_switch_enabled(&self) -> bool {
        matches!(self, GeneralHAService::AutoSwitchHAService(_))
    }
}

impl HAService for GeneralHAService {
    async fn start(&mut self) -> HAResult<()> {
        match self {
            GeneralHAService::DefaultHAService(service) => service.start().await,
            GeneralHAService::AutoSwitchHAService(service) => service.start().await,
        }
    }

    async fn shutdown(&self) {
        match self {
            GeneralHAService::DefaultHAService(service) => service.shutdown().await,
            GeneralHAService::AutoSwitchHAService(service) => service.shutdown().await,
        }
    }

    async fn change_to_master(&self, master_epoch: i32) -> HAResult<bool> {
        match self {
            GeneralHAService::DefaultHAService(service) => service.change_to_master(master_epoch).await,
            GeneralHAService::AutoSwitchHAService(service) => service.change_to_master(master_epoch).await,
        }
    }

    async fn change_to_master_when_last_role_is_master(&self, master_epoch: i32) -> HAResult<bool> {
        match self {
            GeneralHAService::DefaultHAService(service) => {
                service.change_to_master_when_last_role_is_master(master_epoch).await
            }
            GeneralHAService::AutoSwitchHAService(service) => {
                service.change_to_master_when_last_role_is_master(master_epoch).await
            }
        }
    }

    async fn change_to_slave(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
        slave_id: Option<i64>,
    ) -> HAResult<bool> {
        match self {
            GeneralHAService::DefaultHAService(service) => {
                service
                    .change_to_slave(new_master_addr, new_master_epoch, slave_id)
                    .await
            }
            GeneralHAService::AutoSwitchHAService(service) => {
                service
                    .change_to_slave(new_master_addr, new_master_epoch, slave_id)
                    .await
            }
        }
    }

    async fn change_to_slave_when_master_not_change(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
    ) -> HAResult<bool> {
        match self {
            GeneralHAService::DefaultHAService(service) => {
                service
                    .change_to_slave_when_master_not_change(new_master_addr, new_master_epoch)
                    .await
            }
            GeneralHAService::AutoSwitchHAService(service) => {
                service
                    .change_to_slave_when_master_not_change(new_master_addr, new_master_epoch)
                    .await
            }
        }
    }

    async fn update_master_address(&self, new_addr: &str) {
        match self {
            GeneralHAService::DefaultHAService(service) => service.update_master_address(new_addr).await,
            GeneralHAService::AutoSwitchHAService(service) => service.update_master_address(new_addr).await,
        }
    }

    async fn update_ha_master_address(&self, new_addr: &str) {
        match self {
            GeneralHAService::DefaultHAService(service) => service.update_ha_master_address(new_addr).await,
            GeneralHAService::AutoSwitchHAService(service) => service.update_ha_master_address(new_addr).await,
        }
    }

    fn in_sync_replicas_nums(&self, master_put_where: i64) -> i32 {
        match self {
            GeneralHAService::DefaultHAService(service) => service.in_sync_replicas_nums(master_put_where),
            GeneralHAService::AutoSwitchHAService(service) => service.in_sync_replicas_nums(master_put_where),
        }
    }

    fn get_connection_count(&self) -> &AtomicU32 {
        match self {
            GeneralHAService::DefaultHAService(service) => service.get_connection_count(),
            GeneralHAService::AutoSwitchHAService(service) => service.get_connection_count(),
        }
    }

    async fn put_request(&self, request: GroupCommitRequest) {
        match self {
            GeneralHAService::DefaultHAService(service) => service.put_request(request).await,
            GeneralHAService::AutoSwitchHAService(service) => service.put_request(request).await,
        }
    }

    async fn put_group_connection_state_request(&self, request: HAConnectionStateNotificationRequest) {
        match self {
            GeneralHAService::DefaultHAService(service) => service.put_group_connection_state_request(request).await,
            GeneralHAService::AutoSwitchHAService(service) => service.put_group_connection_state_request(request).await,
        }
    }

    async fn get_connection_list(&self) -> Vec<ArcMut<GeneralHAConnection>> {
        match self {
            GeneralHAService::DefaultHAService(service) => service.get_connection_list().await,
            GeneralHAService::AutoSwitchHAService(service) => service.get_connection_list().await,
        }
    }

    fn get_ha_client(&self) -> Option<&GeneralHAClient> {
        match self {
            GeneralHAService::DefaultHAService(service) => service.get_ha_client(),
            GeneralHAService::AutoSwitchHAService(service) => service.get_ha_client(),
        }
    }

    fn get_ha_client_mut(&mut self) -> Option<&mut GeneralHAClient> {
        match self {
            GeneralHAService::DefaultHAService(service) => service.get_ha_client_mut(),
            GeneralHAService::AutoSwitchHAService(service) => service.get_ha_client_mut(),
        }
    }

    fn get_push_to_slave_max_offset(&self) -> i64 {
        match self {
            GeneralHAService::DefaultHAService(service) => service.get_push_to_slave_max_offset(),
            GeneralHAService::AutoSwitchHAService(service) => service.get_push_to_slave_max_offset(),
        }
    }

    fn get_runtime_info(&self, master_put_where: i64) -> HARuntimeInfo {
        match self {
            GeneralHAService::DefaultHAService(service) => service.get_runtime_info(master_put_where),
            GeneralHAService::AutoSwitchHAService(service) => service.get_runtime_info(master_put_where),
        }
    }

    fn get_wait_notify_object(&self) -> &Notify {
        match self {
            GeneralHAService::DefaultHAService(service) => service.get_wait_notify_object(),
            GeneralHAService::AutoSwitchHAService(service) => service.get_wait_notify_object(),
        }
    }

    async fn is_slave_ok(&self, master_put_where: i64) -> bool {
        match self {
            GeneralHAService::DefaultHAService(service) => service.is_slave_ok(master_put_where).await,
            GeneralHAService::AutoSwitchHAService(service) => service.is_slave_ok(master_put_where).await,
        }
    }
}
