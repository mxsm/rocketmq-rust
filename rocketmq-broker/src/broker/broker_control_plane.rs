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

use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;

use parking_lot::lock_api::MappedRwLockWriteGuard;
use parking_lot::Mutex;
use parking_lot::RawRwLock;
use parking_lot::RwLock;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_store::base::message_store::MessageStore;
use tokio::sync::Mutex as TokioMutex;

use crate::broker::broker_pre_online_capability::BrokerOnlineRoleState;
use crate::broker::broker_pre_online_capability::BrokerRegistrationCapability;
use crate::broker::broker_pre_online_capability::BrokerSpecialServiceCapability;
use crate::broker::broker_runtime_config_state::BrokerRuntimeConfigState;
use crate::controller::replicas_manager::BrokerReplicaRole;
use crate::controller::replicas_manager::ReplicasManager;
use crate::failover::escape_bridge_capability::EscapeBridgePolicyState;
use crate::failover::escape_bridge_capability::EscapeBridgeStoreCapability;
use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::processor::pop_message_processor::capability::PopPolicyState;
use crate::processor::pull_message_processor::capability::PullMessagePolicyState;
use crate::processor::send_message_processor::capability::SendMessagePolicyState;
use crate::slave::slave_synchronize::SlaveMasterAddress;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

mod bootstrap;

/// Shared controller state with a separate asynchronous operation gate.
///
/// The parking-lot mutex protects only short, synchronous ReplicasManager
/// decisions. Network, Store and lifecycle awaits must use `operation_gate`
/// for serialization and release the state guard before awaiting.
#[derive(Clone, Default)]
pub(crate) struct BrokerControllerState {
    replicas: Arc<Mutex<Option<ReplicasManager>>>,
    operation_gate: Arc<TokioMutex<()>>,
}

impl BrokerControllerState {
    pub(crate) fn install(&self, replicas_manager: ReplicasManager) {
        *self.replicas.lock() = Some(replicas_manager);
    }

    pub(crate) fn is_initialized(&self) -> bool {
        self.replicas.lock().is_some()
    }

    pub(crate) fn replicas_snapshot(&self) -> Option<ReplicasManager> {
        self.replicas.lock().clone()
    }

    pub(crate) fn with_replicas_mut<T>(&self, operation: impl FnOnce(&mut ReplicasManager) -> T) -> Option<T> {
        self.replicas.lock().as_mut().map(operation)
    }

    pub(crate) async fn lock_operation(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.operation_gate.lock().await
    }
}

/// Shared broker-member table used by background membership sync and Admin
/// lock/unlock requests.
#[derive(Clone)]
pub(crate) struct BrokerMembershipState {
    current: Arc<RwLock<BrokerMemberGroup>>,
    operation_gate: Arc<TokioMutex<()>>,
}

impl BrokerMembershipState {
    pub(crate) fn new(initial: BrokerMemberGroup) -> Self {
        Self {
            current: Arc::new(RwLock::new(initial)),
            operation_gate: Arc::new(TokioMutex::new(())),
        }
    }

    pub(crate) fn snapshot(&self) -> BrokerMemberGroup {
        self.current.read().clone()
    }

    pub(crate) fn write(&self) -> MappedRwLockWriteGuard<'_, RawRwLock, BrokerMemberGroup> {
        parking_lot::RwLockWriteGuard::map(self.current.write(), |current| current)
    }

    pub(crate) fn publish(&self, broker_member_group: BrokerMemberGroup) {
        *self.current.write() = broker_member_group;
    }

    async fn lock_operation(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.operation_gate.lock().await
    }
}

/// Explicit controller and membership control-plane capability.
///
/// This owner contains only the state and services needed for role transitions;
/// it never retains `BrokerRuntimeInner`.
pub(crate) struct BrokerControllerRuntime<MS: MessageStore> {
    controller: BrokerControllerState,
    membership: BrokerMembershipState,
    config: BrokerRuntimeConfigState,
    role_state: Arc<BrokerOnlineRoleState>,
    store: EscapeBridgeStoreCapability<MS>,
    special_services: BrokerSpecialServiceCapability<MS>,
    registration: BrokerRegistrationCapability,
    broker_addr: CheetahString,
    slave_master_addr: Arc<SlaveMasterAddress>,
    broker_outer_api: BrokerOuterAPI,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    pull_request_hold_service: Option<Weak<PullRequestHoldService<MS>>>,
    topic_config_manager: Weak<TopicConfigManager>,
    send_policy: SendMessagePolicyState,
    pull_policy: PullMessagePolicyState,
    pop_policy: PopPolicyState,
    escape_policy: EscapeBridgePolicyState,
    metadata_io: Option<MetadataIoActor>,
    blocking: Option<BlockingExecutor>,
}

impl<MS: MessageStore> BrokerControllerRuntime<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "the composition root lists the complete controller boundary"
    )]
    pub(crate) fn new(
        controller: BrokerControllerState,
        membership: BrokerMembershipState,
        config: BrokerRuntimeConfigState,
        role_state: Arc<BrokerOnlineRoleState>,
        store: EscapeBridgeStoreCapability<MS>,
        special_services: BrokerSpecialServiceCapability<MS>,
        registration: BrokerRegistrationCapability,
        broker_addr: CheetahString,
        slave_master_addr: Arc<SlaveMasterAddress>,
        broker_outer_api: BrokerOuterAPI,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
        pull_request_hold_service: Option<&Arc<PullRequestHoldService<MS>>>,
        topic_config_manager: &Arc<TopicConfigManager>,
        send_policy: SendMessagePolicyState,
        pull_policy: PullMessagePolicyState,
        pop_policy: PopPolicyState,
        escape_policy: EscapeBridgePolicyState,
        metadata_io: Option<MetadataIoActor>,
        blocking: Option<BlockingExecutor>,
    ) -> Self {
        Self {
            controller,
            membership,
            config,
            role_state,
            store,
            special_services,
            registration,
            broker_addr,
            slave_master_addr,
            broker_outer_api,
            shutdown,
            pull_request_hold_service: pull_request_hold_service.map(Arc::downgrade),
            topic_config_manager: Arc::downgrade(topic_config_manager),
            send_policy,
            pull_policy,
            pop_policy,
            escape_policy,
            metadata_io,
            blocking,
        }
    }

    pub(crate) fn controller_initialized(&self) -> bool {
        self.controller.is_initialized()
    }

    pub(crate) fn replicas_snapshot(&self) -> Option<ReplicasManager> {
        self.controller.replicas_snapshot()
    }

    pub(crate) async fn change_special_service_status(&self, should_start: bool) {
        let _ = self.special_services.change_status(should_start).await;
    }

    pub(crate) fn update_slave_master_addr(&self, address: Option<&CheetahString>) {
        self.slave_master_addr.store(address);
    }

    pub(crate) fn min_broker_id(&self) -> u64 {
        self.role_state.min_broker_id()
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "preserves the controller notification protocol"
    )]
    pub(crate) async fn apply_controller_role_change(
        &self,
        controller_leader_address: Option<CheetahString>,
        new_master_broker_id: Option<u64>,
        new_master_address: Option<CheetahString>,
        new_master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
        sync_state_set: std::collections::HashSet<i64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let _operation_guard = self.controller.lock_operation().await;
        let outcome = self
            .controller
            .with_replicas_mut(|replicas_manager| {
                replicas_manager.change_broker_role(
                    controller_leader_address,
                    new_master_broker_id,
                    new_master_address,
                    new_master_epoch,
                    sync_state_set_epoch,
                    Some(&sync_state_set),
                )
            })
            .ok_or_else(|| {
                rocketmq_error::RocketMQError::illegal_argument(
                    "controller mode role change received before replicas manager initialization",
                )
            })??;

        // Controller metadata can arrive before Store initialization. The legacy
        // root path treated that window as a no-op and still advanced control-plane
        // state, so keep the extracted boundary compatible.
        let _ = self
            .store
            .sync_controller_sync_state_set(outcome.local_broker_id as i64, &outcome.sync_state_set);
        let Some(role) = outcome.role else {
            return Ok(());
        };

        let previous_store_role = self.config.store_snapshot().broker_role;
        let target_store_role = outcome.target_broker_role().unwrap_or(previous_store_role);
        let generation = self.config.apply_role(outcome.local_broker_id, target_store_role);
        self.role_state.set_local_broker_id(outcome.local_broker_id);
        self.send_policy.update_broker_config(generation.broker());
        self.send_policy.update_message_store_config(generation.store());
        self.pull_policy.update_broker_config(generation.broker());
        self.pull_policy.update_store_config(generation.store());
        self.pop_policy.update_broker_config(generation.broker());
        self.pop_policy.update_store_config(generation.store());
        self.escape_policy.update_broker_config(generation.broker());
        self.escape_policy.update_message_store_config(generation.store());
        if let Some(topic_config_manager) = self.topic_config_manager.upgrade() {
            topic_config_manager.update_message_store_policy(generation.store());
        }

        match role {
            BrokerReplicaRole::Master => {
                self.store
                    .apply_controller_role(
                        previous_store_role,
                        BrokerReplicaRole::Master,
                        outcome.local_broker_id,
                        None,
                        outcome.master_epoch,
                    )
                    .await
                    .map_err(|error| {
                        rocketmq_error::RocketMQError::illegal_argument(format!(
                            "apply controller role change to message store failed: {error}"
                        ))
                    })?;
                self.special_services
                    .change_status(outcome.should_start_special_service)
                    .await?;
                self.slave_master_addr.store(None);
            }
            BrokerReplicaRole::Slave => {
                self.special_services
                    .change_status(outcome.should_start_special_service)
                    .await?;
                self.store
                    .apply_controller_role(
                        previous_store_role,
                        BrokerReplicaRole::Slave,
                        outcome.local_broker_id,
                        outcome.slave_master_address(),
                        outcome.master_epoch,
                    )
                    .await
                    .map_err(|error| {
                        rocketmq_error::RocketMQError::illegal_argument(format!(
                            "apply controller role change to message store failed: {error}"
                        ))
                    })?;
                if let Some(master_address) = outcome.slave_master_address() {
                    self.slave_master_addr.store(Some(master_address));
                    if outcome.should_sync_master_online() {
                        self.on_master_online(Some(master_address.clone()), None).await;
                    }
                }
            }
        }

        self.role_state.set_isolated(false);
        if outcome.should_register_to_namesrv {
            if let Err(error) = self.registration.register().await {
                tracing::warn!(?error, "failed to register broker after controller role transition");
            }
        }
        Ok(())
    }

    pub(crate) async fn update_min_broker(&self, min_broker_id: u64, min_broker_addr: Option<CheetahString>) {
        let config = self.config.broker_snapshot();
        if !config.enable_slave_acting_master || config.broker_identity.broker_id == MASTER_ID {
            return;
        }
        let _operation_guard = self.membership.lock_operation().await;
        let current_min = self.role_state.min_broker_id();
        if current_min == min_broker_id {
            return;
        }
        let offline_broker_addr = if min_broker_id > current_min {
            self.role_state.min_broker_addr().await
        } else {
            None
        };
        self.on_min_broker_change(min_broker_id, min_broker_addr, offline_broker_addr, None)
            .await;
    }

    pub(crate) async fn on_min_broker_change(
        &self,
        min_broker_id: u64,
        min_broker_addr: Option<CheetahString>,
        offline_broker_addr: Option<CheetahString>,
        master_ha_addr: Option<CheetahString>,
    ) {
        let (old_id, old_addr) = self
            .role_state
            .replace_min_broker(min_broker_id, min_broker_addr.clone())
            .await;
        tracing::info!(
            old_id,
            ?old_addr,
            min_broker_id,
            ?min_broker_addr,
            "minimum broker changed"
        );
        let _ = self
            .special_services
            .change_status(self.role_state.local_broker_id() == min_broker_id)
            .await;
        if offline_broker_addr.is_some() && offline_broker_addr.as_ref() == self.slave_master_addr.load().as_deref() {
            self.slave_master_addr.store(None);
            let _ = self.store.update_ha_master_address("").await;
        }
        if min_broker_id == MASTER_ID && min_broker_addr.is_some() {
            self.on_master_online(min_broker_addr, master_ha_addr).await;
        }
        if self.role_state.min_broker_id() == MASTER_ID {
            if let Some(pull_request_hold_service) = self.pull_request_hold_service.as_ref().and_then(Weak::upgrade) {
                pull_request_hold_service.notify_master_online();
            }
        }
    }

    async fn on_master_online(&self, master_addr: Option<CheetahString>, master_ha_addr: Option<CheetahString>) {
        let need_sync_master_flush_offset = self.store.master_flushed_offset().unwrap_or_default() == 0
            && self.config.store_snapshot().all_ack_in_sync_state_set;
        if master_ha_addr.is_none() || need_sync_master_flush_offset {
            match self
                .broker_outer_api
                .retrieve_broker_ha_info(master_addr.as_ref())
                .await
            {
                Ok(info) => {
                    if need_sync_master_flush_offset {
                        let _ = self.store.set_master_flushed_offset(info.master_flush_offset);
                    }
                    if master_ha_addr.is_none() {
                        if let Some(master_ha_address) = info.master_ha_address {
                            let _ = self.store.update_ha_master_address(master_ha_address.as_str()).await;
                        }
                        if let Some(master_address) = info.master_address {
                            let _ = self.store.update_master_address(&master_address).await;
                        }
                    }
                }
                Err(error) => tracing::error!(?error, "failed to retrieve broker HA info"),
            }
        }
        if let Some(master_ha_addr) = master_ha_addr {
            let _ = self.store.update_ha_master_address(master_ha_addr.as_str()).await;
        }
        let _ = self.store.wakeup_ha_client();
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::BrokerControllerState;
    use super::BrokerMembershipState;
    use crate::controller::replicas_manager::ReplicasManager;

    #[test]
    fn controller_state_owns_the_installed_replicas_manager() {
        let state = BrokerControllerState::default();
        assert!(!state.is_initialized());

        state.install(ReplicasManager::new(
            &BrokerConfig::default(),
            &MessageStoreConfig::default(),
            CheetahString::from_static_str("127.0.0.1:10911"),
        ));

        assert!(state.is_initialized());
        assert_eq!(
            state
                .replicas_snapshot()
                .expect("installed replicas manager should be available")
                .broker_address(),
            "127.0.0.1:10911"
        );
    }

    #[test]
    fn membership_clones_share_published_snapshots() {
        let state = BrokerMembershipState::new(BrokerMemberGroup::new("cluster".into(), "broker".into()));
        let observer = state.clone();
        let mut next = BrokerMemberGroup::new("cluster".into(), "broker".into());
        next.broker_addrs.insert(1, "127.0.0.1:10911".into());

        state.publish(next);

        assert_eq!(
            observer.snapshot().broker_addrs.get(&1).map(CheetahString::as_str),
            Some("127.0.0.1:10911")
        );
    }
}
