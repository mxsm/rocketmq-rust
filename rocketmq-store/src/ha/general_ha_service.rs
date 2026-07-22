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

use std::collections::HashSet;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;

use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use tokio::sync::Notify;

use crate::ha::auto_switch::auto_switch_ha_service::AutoSwitchHAService;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::general_ha_client::GeneralHAClient;
use crate::ha::group_transfer_service::GroupTransferRuntimeInfo;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAAckedReplicaSnapshot;
use crate::ha::ha_service::HAService;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::store_error::HAError;
use crate::store_error::HAResult;

#[derive(Clone, Default)]
pub(crate) struct GeneralHAServiceReference {
    target: Arc<OnceLock<GeneralHAServiceWeak>>,
}

enum GeneralHAServiceWeak {
    Default(Weak<DefaultHAService>),
    AutoSwitch(Weak<AutoSwitchHAService>),
}

impl GeneralHAServiceReference {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn bind(&self, service: &GeneralHAService) -> HAResult<()> {
        let target = match service {
            GeneralHAService::DefaultHAService(service) => GeneralHAServiceWeak::Default(Arc::downgrade(service)),
            GeneralHAService::AutoSwitchHAService(service) => GeneralHAServiceWeak::AutoSwitch(Arc::downgrade(service)),
        };
        self.target
            .set(target)
            .map_err(|_| HAError::Service("General HA service reference already bound".to_string()))
    }

    pub(crate) fn upgrade(&self) -> Option<GeneralHAService> {
        match self.target.get()? {
            GeneralHAServiceWeak::Default(service) => service.upgrade().map(GeneralHAService::DefaultHAService),
            GeneralHAServiceWeak::AutoSwitch(service) => service.upgrade().map(GeneralHAService::AutoSwitchHAService),
        }
    }
}

#[derive(Clone)]
pub enum GeneralHAService {
    DefaultHAService(Arc<DefaultHAService>),
    AutoSwitchHAService(Arc<AutoSwitchHAService>),
}

impl GeneralHAService {
    pub fn new_with_default_ha_service(default_ha_service: DefaultHAService) -> Self {
        GeneralHAService::DefaultHAService(Arc::new(default_ha_service))
    }

    pub fn new_with_auto_switch_ha_service(auto_switch_ha_service: AutoSwitchHAService) -> Self {
        GeneralHAService::AutoSwitchHAService(Arc::new(auto_switch_ha_service))
    }

    pub(crate) fn init(&mut self) -> HAResult<()> {
        let reference = GeneralHAServiceReference::new();
        match self {
            GeneralHAService::DefaultHAService(service) => Arc::get_mut(service)
                .ok_or_else(|| HAError::Service("Default HA root was shared before initialization".to_string()))?
                .init(reference.clone(), None)?,
            GeneralHAService::AutoSwitchHAService(service) => Arc::get_mut(service)
                .ok_or_else(|| HAError::Service("AutoSwitch HA root was shared before initialization".to_string()))?
                .init(reference.clone())?,
        }
        reference.bind(self)
    }

    pub(crate) fn default_service(&self) -> Option<&DefaultHAService> {
        match self {
            GeneralHAService::DefaultHAService(service) => Some(service),
            GeneralHAService::AutoSwitchHAService(_) => None,
        }
    }

    pub(crate) fn auto_switch_service(&self) -> Option<&AutoSwitchHAService> {
        match self {
            GeneralHAService::DefaultHAService(_) => None,
            GeneralHAService::AutoSwitchHAService(service) => Some(service),
        }
    }

    #[inline]
    pub fn is_auto_switch_enabled(&self) -> bool {
        matches!(self, GeneralHAService::AutoSwitchHAService(_))
    }

    pub fn local_sync_state_set_size(&self, master_put_where: i64) -> usize {
        match self {
            GeneralHAService::DefaultHAService(service) => {
                service.in_sync_replicas_nums(master_put_where).max(1) as usize
            }
            GeneralHAService::AutoSwitchHAService(service) => service.local_sync_state_set_size(master_put_where),
        }
    }

    pub fn compute_confirm_offset(&self, current_confirm_offset: i64, max_phy_offset: i64) -> i64 {
        match self {
            GeneralHAService::DefaultHAService(_) => current_confirm_offset,
            GeneralHAService::AutoSwitchHAService(service) => {
                service.compute_confirm_offset(current_confirm_offset, max_phy_offset)
            }
        }
    }

    pub fn sync_controller_sync_state_set(&self, local_broker_id: i64, sync_state_set: &HashSet<i64>) {
        if let GeneralHAService::AutoSwitchHAService(service) = self {
            service.sync_controller_sync_state_set(local_broker_id, sync_state_set);
        }
    }

    pub fn sync_state_set(&self) -> Option<HashSet<i64>> {
        match self {
            GeneralHAService::DefaultHAService(_) => None,
            GeneralHAService::AutoSwitchHAService(service) => Some(service.get_sync_state_set()),
        }
    }

    pub(crate) fn group_transfer_runtime_info(&self) -> GroupTransferRuntimeInfo {
        match self {
            GeneralHAService::DefaultHAService(service) => service.group_transfer_runtime_info(),
            GeneralHAService::AutoSwitchHAService(service) => service.group_transfer_runtime_info(),
        }
    }
}

impl HAService for GeneralHAService {
    async fn start(&self) -> HAResult<()> {
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

    async fn snapshot_acked_replicas(&self) -> Vec<HAAckedReplicaSnapshot> {
        match self {
            GeneralHAService::DefaultHAService(service) => service.snapshot_acked_replicas().await,
            GeneralHAService::AutoSwitchHAService(service) => service.snapshot_acked_replicas().await,
        }
    }

    async fn connection_state(&self, remote_addr: &str) -> Option<HAConnectionState> {
        match self {
            GeneralHAService::DefaultHAService(service) => service.connection_state(remote_addr).await,
            GeneralHAService::AutoSwitchHAService(service) => service.connection_state(remote_addr).await,
        }
    }

    fn get_ha_client(&self) -> Option<&GeneralHAClient> {
        match self {
            GeneralHAService::DefaultHAService(service) => service.get_ha_client(),
            GeneralHAService::AutoSwitchHAService(service) => service.get_ha_client(),
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
