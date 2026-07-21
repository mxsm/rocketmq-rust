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
use std::sync::Weak;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_runtime::task::service_task::ServiceContext;
use rocketmq_runtime::task::service_task::ServiceTask;
use rocketmq_runtime::task::ServiceManager;
use rocketmq_runtime::TaskGroup;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::BrokerPreOnlineContext;
use crate::schedule::delay_offset_serialize_wrapper::DelayOffsetSerializeWrapper;

pub struct BrokerPreOnlineService<MS: MessageStore> {
    service_manager: ServiceManager<BrokerPreOnlineServiceInner<MS>>,
}

impl<MS: MessageStore> BrokerPreOnlineService<MS> {
    pub(crate) fn new(context: BrokerPreOnlineContext<MS>, parent_task_group: Option<TaskGroup>) -> Self {
        let inner = BrokerPreOnlineServiceInner {
            context,
            wait_broker_index: AtomicU32::new(0),
        };
        let service_manager = match parent_task_group {
            Some(parent_task_group) => ServiceManager::new_with_task_group(inner, parent_task_group),
            None => ServiceManager::new(inner),
        };
        BrokerPreOnlineService { service_manager }
    }
}

struct BrokerPreOnlineServiceInner<MS: MessageStore> {
    context: BrokerPreOnlineContext<MS>,
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
            if !self.context.role_state.is_isolated() {
                info!(
                    broker = %self.context.policy.broker_name,
                    broker_id = self.context.role_state.local_broker_id(),
                    "broker is online"
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
        let broker_member_group = match self
            .context
            .broker_outer_api
            .sync_broker_member_group(
                &self.context.policy.broker_cluster_name,
                &self.context.policy.broker_name,
                self.context.policy.compatible_with_old_name_srv,
            )
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
        let broker_id = self.context.role_state.local_broker_id();
        if let Some(broker_member_group) = broker_member_group {
            let min_broker_id = self.get_min_broker_id(&broker_member_group.broker_addrs);
            if !broker_member_group.broker_addrs.is_empty() {
                if broker_id == MASTER_ID {
                    return Ok(self.prepare_for_master_online(broker_member_group).await);
                } else if min_broker_id == MASTER_ID {
                    return Ok(self.prepare_for_slave_online(broker_member_group).await);
                } else {
                    info!("no master online, start service directly");
                    return Ok(self
                        .context
                        .transition
                        .start_service(
                            min_broker_id,
                            broker_member_group.broker_addrs.get(&min_broker_id).cloned(),
                        )
                        .await);
                }
            }
        }
        info!("no other broker online, will start service directly");
        Ok(self
            .context
            .transition
            .start_service(broker_id, Some(self.context.policy.broker_addr.clone()))
            .await)
    }

    fn get_min_broker_id(
        &self,
        broker_addr_map: &HashMap<u64 /* brokerId */, CheetahString /* broker address */>,
    ) -> u64 {
        let mut local_broker_addr_map = broker_addr_map.clone();
        let local_broker_id = self.context.role_state.local_broker_id();
        local_broker_addr_map.remove(&local_broker_id);
        if !local_broker_addr_map.is_empty() {
            *local_broker_addr_map.keys().min().unwrap()
        } else {
            local_broker_id
        }
    }

    async fn prepare_for_master_online(&self, broker_member_group: BrokerMemberGroup) -> bool {
        let mut broker_id_list = broker_member_group.broker_addrs.keys().copied().collect::<Vec<u64>>();
        broker_id_list.sort();
        loop {
            let wait_index = self.wait_broker_index.load(Ordering::SeqCst);
            if (wait_index as usize) >= broker_id_list.len() {
                info!("master preOnline complete, start service");
                return self
                    .context
                    .transition
                    .start_service(MASTER_ID, Some(self.context.policy.broker_addr.clone()))
                    .await;
            }
            let wait_broker_id = broker_id_list[wait_index as usize];
            let broker_addr_to_wait = broker_member_group.broker_addrs.get(&wait_broker_id).cloned();
            if broker_addr_to_wait.is_none() {
                self.wait_broker_index.fetch_add(1, Ordering::SeqCst);
                continue;
            }
            let broker_addr_to_wait = broker_addr_to_wait.unwrap();
            if let Err(e) = self
                .context
                .broker_outer_api
                .send_broker_ha_info(
                    &broker_addr_to_wait,
                    &self.context.policy.ha_server_addr,
                    match self.context.store.broker_init_max_offset() {
                        Ok(offset) => offset,
                        Err(error) => {
                            error!(?error, "message store unavailable while sending broker HA info");
                            return false;
                        }
                    },
                    &self.context.policy.broker_addr,
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
    async fn sync_metadata_reverse(&self, broker_addr: CheetahString) -> bool {
        let mut success = true;

        match self
            .context
            .broker_outer_api
            .get_all_consumer_offset(&broker_addr)
            .await
        {
            Ok(Some(offset_wrapper)) => {
                if let Some(consumer_offset_manager) = self.context.consumer_offsets.upgrade() {
                    let local_version = consumer_offset_manager.data_version();
                    if should_sync_from_peer(&local_version, Some(offset_wrapper.data_version())) {
                        let data_version = offset_wrapper.data_version().clone();
                        consumer_offset_manager.merge_offsets_from_peer(offset_wrapper.offset_table(), data_version);
                        consumer_offset_manager.persist();
                        info!(
                            "{}'s consumer offset data version is newer or equal, merged into local broker",
                            broker_addr
                        );
                    }
                } else {
                    error!("ConsumerOffsetManager is unavailable during reverse metadata sync");
                    success = false;
                }
            }
            Ok(None) => {
                warn!("GetAllConsumerOffset return null, {}", broker_addr);
            }
            Err(e) => {
                error!("GetAllConsumerOffset reverse sync failed, {}: {:?}", broker_addr, e);
                success = false;
            }
        }

        match self.context.broker_outer_api.get_delay_offset(&broker_addr).await {
            Ok(Some(delay_offset)) => {
                match SerdeJsonUtils::from_json_str::<DelayOffsetSerializeWrapper>(delay_offset.as_str()) {
                    Ok(delay_offset_wrapper) => {
                        if let Some(schedule_message_service) = self.context.schedule.upgrade() {
                            let local_version = schedule_message_service.get_data_version();
                            if should_sync_from_peer(&local_version, delay_offset_wrapper.data_version()) {
                                if let Err(e) = schedule_message_service
                                    .sync_delay_offset_from_peer(delay_offset.as_str(), &delay_offset_wrapper)
                                    .await
                                {
                                    error!("Sync reverse delay offset error, {}: {:?}", broker_addr, e);
                                    success = false;
                                }
                            }
                        } else {
                            error!("ScheduleMessageService is unavailable during reverse metadata sync");
                            success = false;
                        }
                    }
                    Err(e) => {
                        error!("Decode reverse delay offset failed, {}: {:?}", broker_addr, e);
                        success = false;
                    }
                }
            }
            Ok(None) => {
                warn!("GetDelayOffset return null, {}", broker_addr);
            }
            Err(e) => {
                error!("GetDelayOffset reverse sync failed, {}: {:?}", broker_addr, e);
                success = false;
            }
        }

        if let Some(timer_message_store) = self.context.timer.as_ref().and_then(Weak::upgrade) {
            match self.context.broker_outer_api.get_timer_check_point(&broker_addr).await {
                Ok(Some(checkpoint_snapshot)) => {
                    let local_snapshot = timer_message_store.timer_checkpoint_snapshot();
                    if local_snapshot.as_ref().is_none_or(|snapshot| {
                        should_sync_from_peer(snapshot.data_version(), Some(checkpoint_snapshot.data_version()))
                    }) {
                        match timer_message_store.sync_checkpoint_from_master(&checkpoint_snapshot) {
                            Ok(true) => {}
                            Ok(false) => {
                                warn!("Local timer checkpoint is not initialized, {}", broker_addr);
                            }
                            Err(e) => {
                                error!("Persist reverse timer checkpoint error, {}: {:?}", broker_addr, e);
                                success = false;
                            }
                        }
                    }
                }
                Ok(None) => {
                    warn!("GetTimerCheckPoint return null, {}", broker_addr);
                }
                Err(e) => {
                    error!("GetTimerCheckPoint reverse sync failed, {}: {:?}", broker_addr, e);
                    success = false;
                }
            }
        }

        for plugin in &self.context.plugins {
            match plugin.upgrade() {
                Some(plugin) => {
                    if let Err(e) = plugin.sync_metadata_reverse(&broker_addr) {
                        error!("Plugin reverse metadata sync failed, {}: {:?}", broker_addr, e);
                        success = false;
                    }
                }
                None => {
                    error!("Broker plugin is unavailable during reverse metadata sync");
                    success = false;
                }
            }
        }

        success
    }

    async fn wait_for_ha_handshake_complete(&self, broker_addr: CheetahString) -> bool {
        info!("wait for handshake completion with {}", broker_addr);

        match self.context.store.wait_for_ha_transfer(&broker_addr).await {
            Ok(true) => true,
            Ok(false) => {
                error!("HAService is null, maybe broker config is wrong. For example, duplicationEnable is true");
                false
            }
            Err(error) => {
                error!(?error, "message store unavailable while waiting for HA handshake");
                false
            }
        }
    }
    async fn future_wait_action(&self, result: bool, broker_member_group: &BrokerMemberGroup) -> bool {
        match result {
            true => {
                if self.context.role_state.local_broker_id() != MASTER_ID {
                    info!("slave preOnline complete, start service");
                    let min_broker_id = self.get_min_broker_id(&broker_member_group.broker_addrs);
                    let broker_addr = broker_member_group.broker_addrs.get(&min_broker_id).cloned();
                    return self.context.transition.start_service(min_broker_id, broker_addr).await;
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
            .context
            .broker_outer_api
            .retrieve_broker_ha_info(master_broker_addr)
            .await
        {
            Ok(addr) => addr,
            Err(e) => {
                error!("retrieve master ha info exception, {}", e);
                return false;
            }
        };
        let master_flushed_offset = match self.context.store.master_flushed_offset() {
            Ok(offset) => offset,
            Err(error) => {
                error!(?error, "message store unavailable while preparing slave broker");
                return false;
            }
        };
        if master_flushed_offset == 0 && self.context.policy.sync_master_flush_offset_when_startup {
            info!(
                "Set master flush offset in slave to {}",
                broker_sync_info.master_flush_offset
            );
            if let Err(error) = self
                .context
                .store
                .set_master_flushed_offset(broker_sync_info.master_flush_offset)
            {
                error!(?error, "message store unavailable while setting master flush offset");
                return false;
            }
        }

        let Some(master_ha_address) = broker_sync_info.master_ha_address.clone() else {
            let min_broker_id = self.get_min_broker_id(&broker_member_group.broker_addrs);
            return self
                .context
                .transition
                .start_service(
                    min_broker_id,
                    broker_member_group.broker_addrs.get(&mix_all::MASTER_ID).cloned(),
                )
                .await;
        };
        let Some(master_address) = broker_sync_info.master_address.as_ref() else {
            error!(
                "retrieve master ha info missing master address, {}",
                broker_member_group.broker_name
            );
            return false;
        };
        if let Err(error) = self
            .context
            .store
            .update_master_addresses(&master_ha_address, master_address)
            .await
        {
            error!(?error, "message store unavailable while updating master addresses");
            return false;
        }

        let ha_handshake_result = self.wait_for_ha_handshake_complete(master_ha_address).await;
        self.future_wait_action(ha_handshake_result, &broker_member_group).await
    }
}

fn should_sync_from_peer(local_version: &DataVersion, remote_version: Option<&DataVersion>) -> bool {
    match remote_version {
        Some(remote_version) => local_version <= remote_version,
        None => true,
    }
}

impl<MS> BrokerPreOnlineService<MS>
where
    MS: MessageStore,
{
    pub async fn start(&self) -> rocketmq_error::RocketMQResult<()> {
        self.service_manager
            .start()
            .await
            .map_err(|source| rocketmq_error::RocketMQError::BrokerAsyncTaskFailed {
                task: "BrokerPreOnlineService",
                context: "failed to start runtime-owned service task".to_string(),
                source: Box::new(source),
            })
    }

    pub async fn shutdown(&self) -> rocketmq_error::RocketMQResult<()> {
        self.service_manager
            .shutdown()
            .await
            .map_err(|source| rocketmq_error::RocketMQError::BrokerAsyncTaskFailed {
                task: "BrokerPreOnlineService",
                context: "failed to shutdown runtime-owned service task".to_string(),
                source: Box::new(source),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use rocketmq_store::message_store::GenericMessageStore;

    use super::super::BrokerOnlineRoleState;
    use super::super::BrokerPreOnlineStoreCapability;
    use super::should_sync_from_peer;

    #[test]
    fn should_sync_from_peer_accepts_newer_version() {
        let mut local = rocketmq_remoting::protocol::data_version_facade::new_data_version();
        local.set_state_version(1);
        local.set_timestamp(10);
        local.set_counter(1);

        let mut remote = rocketmq_remoting::protocol::data_version_facade::new_data_version();
        remote.set_state_version(2);
        remote.set_timestamp(20);
        remote.set_counter(2);

        assert!(should_sync_from_peer(&local, Some(&remote)));
    }

    #[test]
    fn should_sync_from_peer_rejects_older_version() {
        let mut local = rocketmq_remoting::protocol::data_version_facade::new_data_version();
        local.set_state_version(2);
        local.set_timestamp(20);
        local.set_counter(2);

        let mut remote = rocketmq_remoting::protocol::data_version_facade::new_data_version();
        remote.set_state_version(1);
        remote.set_timestamp(10);
        remote.set_counter(1);

        assert!(!should_sync_from_peer(&local, Some(&remote)));
    }

    #[test]
    fn should_sync_from_peer_accepts_missing_version() {
        assert!(should_sync_from_peer(
            &rocketmq_remoting::protocol::data_version_facade::new_data_version(),
            None,
        ));
    }

    #[tokio::test]
    async fn online_role_state_publishes_live_role_and_minimum_broker() {
        let state = BrokerOnlineRoleState::new(1);
        state.set_local_broker_id(2);
        state.publish_min_broker(1, Some("127.0.0.1:10911".into())).await;

        assert_eq!(state.local_broker_id(), 2);
        assert_eq!(state.min_broker_id(), 1);
        assert_eq!(state.min_broker_addr().await.as_deref(), Some("127.0.0.1:10911"));
        assert!(!state.is_isolated());

        state.set_isolated(true);
        assert!(state.is_isolated());
    }

    #[tokio::test]
    async fn pre_online_store_capability_fails_closed_without_provider() {
        let capability = BrokerPreOnlineStoreCapability::<GenericMessageStore> {
            escape_bridge: Weak::new(),
        };

        assert!(capability.broker_init_max_offset().is_err());
        assert!(capability.master_flushed_offset().is_err());
        assert!(capability.set_master_flushed_offset(1).is_err());
        assert!(capability
            .wait_for_ha_transfer(&"127.0.0.1:10912".into())
            .await
            .is_err());
    }

    #[test]
    fn broker_pre_online_source_uses_only_explicit_capabilities() {
        let service_source = include_str!("service.rs");
        let capability_source = include_str!("../broker_pre_online_capability.rs");

        for source in [service_source, capability_source] {
            assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
            assert!(!source.contains(concat!("Arc", "Mut")));
            assert!(!source.contains(concat!("WeakArc", "Mut")));
        }
        assert!(service_source.contains("BrokerPreOnlineContext"));
        assert!(capability_source.contains("BrokerOnlineTransitionCapability"));
        assert!(capability_source.contains("BrokerPreOnlineStoreCapability"));
        assert!(service_source.contains("ServiceManager::new_with_task_group"));
    }
}
