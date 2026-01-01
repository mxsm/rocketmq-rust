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

use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::common::remoting_helper::RemotingHelper;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_rust::task::service_task::ServiceContext;
use rocketmq_rust::task::service_task::ServiceTask;
use rocketmq_rust::task::ServiceManager;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use rocketmq_store::ha::ha_service::HAService;
use tracing::error;
use tracing::info;

use crate::broker_runtime::BrokerRuntimeInner;

pub struct BrokerPreOnlineService<MS: MessageStore> {
    service_manager: ServiceManager<BrokerPreOnlineServiceInner<MS>>,
}

impl<MS: MessageStore> BrokerPreOnlineService<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        let inner = BrokerPreOnlineServiceInner {
            broker_runtime_inner,
            wait_broker_index: AtomicU32::new(0),
        };
        let service_manager = ServiceManager::new(inner);
        BrokerPreOnlineService { service_manager }
    }
}

struct BrokerPreOnlineServiceInner<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    wait_broker_index: AtomicU32,
}

impl<MS> ServiceTask for BrokerPreOnlineServiceInner<MS>
where
    MS: MessageStore,
{
    fn get_service_name(&self) -> String {
        "BrokerPreOnlineService".to_string()
    }

    async fn run(&self, context: &ServiceContext) {
        while !context.is_stopped() {
            if !self.broker_runtime_inner.is_isolated().load(Ordering::SeqCst) {
                info!(
                    "broker {} is online",
                    self.broker_runtime_inner
                        .broker_config()
                        .broker_identity
                        .get_canonical_name()
                );
                break;
            }
            match self.prepare_for_broker_online().await {
                Ok(is_success) => {
                    if is_success {
                        break;
                    } else {
                        let _ = context.wait_for_running(Duration::from_millis(1000)).await;
                    }
                }
                Err(e) => {
                    error!("prepare for broker online failed, retry later. error: {:?}", e);
                }
            }
        }
    }
}

impl<MS> BrokerPreOnlineServiceInner<MS>
where
    MS: MessageStore,
{
    async fn prepare_for_broker_online(&self) -> RocketMQResult<bool> {
        let broker_cluster_name = &self
            .broker_runtime_inner
            .broker_config()
            .broker_identity
            .broker_cluster_name;
        let broker_name = &self.broker_runtime_inner.broker_config().broker_identity.broker_name;
        let compatible_with_old_name_srv = self.broker_runtime_inner.broker_config().compatible_with_old_name_srv;
        let broker_member_group = match self
            .broker_runtime_inner
            .broker_outer_api()
            .sync_broker_member_group(broker_cluster_name, broker_name, compatible_with_old_name_srv)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                error!(
                    "syncBrokerMemberGroup from namesrv error, start service failed, will try later, {}",
                    e
                );
                return Ok(false);
            }
        };
        let broker_id = self.broker_runtime_inner.broker_config().broker_identity.broker_id;
        if let Some(broker_member_group) = broker_member_group {
            let min_broker_id = self.get_min_broker_id(&broker_member_group.broker_addrs);
            if !broker_member_group.broker_addrs.is_empty() {
                if broker_id == MASTER_ID {
                    return Ok(self.prepare_for_master_online(broker_member_group).await);
                } else if min_broker_id == MASTER_ID {
                    return Ok(self.prepare_for_slave_online(broker_member_group).await);
                } else {
                    info!("no master online, start service directly");
                    BrokerRuntimeInner::<MS>::start_service(
                        self.broker_runtime_inner.clone(),
                        min_broker_id,
                        broker_member_group.broker_addrs.get(&min_broker_id).cloned(),
                    )
                    .await;
                    return Ok(true);
                }
            }
        }
        info!("no other broker online, will start service directly");
        let broker_addr = self.broker_runtime_inner.get_broker_addr().clone();

        BrokerRuntimeInner::<MS>::start_service(self.broker_runtime_inner.clone(), broker_id, Some(broker_addr)).await;
        Ok(true)
    }

    fn get_min_broker_id(
        &self,
        broker_addr_map: &HashMap<u64 /* brokerId */, CheetahString /* broker address */>,
    ) -> u64 {
        let mut local_broker_addr_map = broker_addr_map.clone();
        local_broker_addr_map.remove(&self.broker_runtime_inner.broker_config().broker_identity.broker_id);
        if !local_broker_addr_map.is_empty() {
            *local_broker_addr_map.keys().min().unwrap()
        } else {
            self.broker_runtime_inner.broker_config().broker_identity.broker_id
        }
    }

    async fn prepare_for_master_online(&self, broker_member_group: BrokerMemberGroup) -> bool {
        let mut broker_id_list = broker_member_group.broker_addrs.keys().copied().collect::<Vec<u64>>();
        broker_id_list.sort();
        loop {
            let wait_index = self.wait_broker_index.load(Ordering::SeqCst);
            if (wait_index as usize) >= broker_id_list.len() {
                info!("master preOnline complete, start service");
                let broker_addr = self.broker_runtime_inner.get_broker_addr().clone();
                BrokerRuntimeInner::<MS>::start_service(
                    self.broker_runtime_inner.clone(),
                    MASTER_ID,
                    Some(broker_addr),
                )
                .await;

                return true;
            }
            let wait_broker_id = broker_id_list[wait_index as usize];
            let broker_addr_to_wait = broker_member_group.broker_addrs.get(&wait_broker_id).cloned();
            if broker_addr_to_wait.is_none() {
                self.wait_broker_index.fetch_add(1, Ordering::SeqCst);
                continue;
            }
            let broker_addr_to_wait = broker_addr_to_wait.unwrap();
            if let Err(e) = self
                .broker_runtime_inner
                .broker_outer_api()
                .send_broker_ha_info(
                    &broker_addr_to_wait,
                    &self.broker_runtime_inner.get_ha_server_addr(),
                    self.broker_runtime_inner
                        .message_store_unchecked()
                        .get_broker_init_max_offset(),
                    self.broker_runtime_inner.get_broker_addr(),
                )
                .await
            {
                error!(
                    "sendBrokerHaInfo to broker {} error, will retry later, {}",
                    broker_addr_to_wait, e
                );
                return false;
            }

            let ha_handshake_future = self.wait_for_ha_handshake_complete(broker_addr_to_wait.clone()).await;
            let is_success = self.future_wait_action(ha_handshake_future, &broker_member_group).await;
            if !is_success {
                return false;
            }
            if !self.sync_metadata_reverse(broker_addr_to_wait.clone()).await {
                error!(
                    "syncMetadataReverse to broker {} error, will retry later",
                    broker_addr_to_wait
                );
                return false;
            }
            self.wait_broker_index.fetch_add(1, Ordering::SeqCst);
        }
    }
    async fn sync_metadata_reverse(&self, _broker_addr: CheetahString) -> bool {
        unimplemented!("syncMetadataReverse unimplemented")
    }

    async fn wait_for_ha_handshake_complete(&self, broker_addr: CheetahString) -> bool {
        info!("wait for handshake completion with {}", broker_addr);

        let (request, tx) = HAConnectionStateNotificationRequest::new(
            rocketmq_store::ha::ha_connection_state::HAConnectionState::Transfer,
            &RemotingHelper::parse_host_from_address(Some(broker_addr.as_str())),
            true,
        );
        if let Some(ha_service) = self.broker_runtime_inner.message_store_unchecked().get_ha_service() {
            ha_service.put_group_connection_state_request(request).await;
            tx.await.unwrap_or(false)
        } else {
            error!("HAService is null, maybe broker config is wrong. For example, duplicationEnable is true");
            false
        }
    }
    async fn future_wait_action(&self, result: bool, broker_member_group: &BrokerMemberGroup) -> bool {
        match result {
            true => {
                if self.broker_runtime_inner.broker_config().broker_identity.broker_id != MASTER_ID {
                    info!("slave preOnline complete, start service");
                    let min_broker_id = self.get_min_broker_id(&broker_member_group.broker_addrs);
                    let broker_addr = broker_member_group.broker_addrs.get(&min_broker_id).cloned();
                    BrokerRuntimeInner::<MS>::start_service(
                        self.broker_runtime_inner.clone(),
                        min_broker_id,
                        broker_addr,
                    )
                    .await;
                }
                true
            }
            false => {
                error!("wait for handshake completion failed, HA connection lost");
                false
            }
        }
    }

    async fn prepare_for_slave_online(&self, broker_member_group: BrokerMemberGroup) -> bool {
        let master_broker_addr = broker_member_group.broker_addrs.get(&mix_all::MASTER_ID);
        let broker_sync_info = match self
            .broker_runtime_inner
            .broker_outer_api()
            .retrieve_broker_ha_info(master_broker_addr)
            .await
        {
            Ok(addr) => addr,
            Err(e) => {
                error!("retrieve master ha info exception, {}", e);
                return false;
            }
        };
        let message_store = self.broker_runtime_inner.message_store_unchecked();
        if message_store.get_master_flushed_offset() == 0
            && self
                .broker_runtime_inner
                .message_store_config()
                .sync_master_flush_offset_when_startup
        {
            info!(
                "Set master flush offset in slave to {}",
                broker_sync_info.master_flush_offset
            );
            message_store.set_master_flushed_offset(broker_sync_info.master_flush_offset);
        }

        match broker_sync_info.master_ha_address {
            None => {
                let min_broker_id = self.get_min_broker_id(&broker_member_group.broker_addrs);
                BrokerRuntimeInner::<MS>::start_service(
                    self.broker_runtime_inner.clone(),
                    min_broker_id,
                    broker_member_group.broker_addrs.get(&mix_all::MASTER_ID).cloned(),
                )
                .await;
            }
            Some(ref value) => {
                message_store.update_ha_master_address(value).await;
                message_store.update_master_address(&broker_sync_info.master_address.clone().unwrap());
            }
        }

        let ha_handshake_result = self
            .wait_for_ha_handshake_complete(broker_sync_info.master_ha_address.unwrap())
            .await;
        self.future_wait_action(ha_handshake_result, &broker_member_group).await
    }
}

impl<MS> BrokerPreOnlineService<MS>
where
    MS: MessageStore,
{
    pub async fn start(&mut self) {
        self.service_manager.start().await.unwrap();
    }

    pub async fn shutdown(&mut self) {
        self.service_manager.shutdown().await.unwrap();
    }
}
