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
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use crate::config::broker_config::BrokerConfig;
use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_store::store_append_receipt;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::BrokerAdminStore;
use rocketmq_store::BrokerMasterAddressStore;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerReplicationStore;
use rocketmq_store::BrokerWriteStore;
use rocketmq_store::GetMessageResult;
use rocketmq_store::HAConnectionStateNotificationRequest;
use rocketmq_store::HAError;
use rocketmq_store::HAResult;
use rocketmq_store::HAService;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::MessageStoreHealthCapability;
use rocketmq_store::PutMessageResult;
use rocketmq_store::QueryMessageRequest;
use rocketmq_store::QueryMessageResult;
use rocketmq_store::SelectMappedBufferResult;
use rocketmq_store::StoreAppendReceipt;
use rocketmq_store::StoreError as BackendStoreError;
use rocketmq_store::StoreHealthSnapshot;
use rocketmq_store::StorePorts;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
use rocketmq_store_api::StoreHealth;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::WriteLeaseToken;

use crate::controller::replicas_manager::BrokerReplicaRole;
use crate::failover::escape_bridge::MessageStoreUnavailable;

/// Immutable failover policy generation published by the broker composition root.
#[derive(Clone, Debug)]
pub(crate) struct EscapeBridgePolicy {
    pub(crate) broker_name: CheetahString,
    pub(crate) broker_id: u64,
    pub(crate) enable_slave_acting_master: bool,
    pub(crate) enable_remote_escape: bool,
    pub(crate) broker_role: BrokerRole,
}

impl EscapeBridgePolicy {
    fn from_configs(broker_config: &BrokerConfig, message_store_config: &MessageStoreConfig) -> Self {
        Self {
            broker_name: broker_config.broker_identity.broker_name.clone(),
            broker_id: broker_config.broker_identity.broker_id,
            enable_slave_acting_master: broker_config.enable_slave_acting_master,
            enable_remote_escape: broker_config.enable_remote_escape,
            broker_role: message_store_config.broker_role,
        }
    }

    fn apply_broker_config(&mut self, broker_config: &BrokerConfig) {
        self.broker_name = broker_config.broker_identity.broker_name.clone();
        self.broker_id = broker_config.broker_identity.broker_id;
        self.enable_slave_acting_master = broker_config.enable_slave_acting_master;
        self.enable_remote_escape = broker_config.enable_remote_escape;
    }

    fn apply_message_store_config(&mut self, message_store_config: &MessageStoreConfig) {
        self.broker_role = message_store_config.broker_role;
    }
}

/// Atomically published failover policy generations.
#[derive(Clone)]
pub(crate) struct EscapeBridgePolicyState {
    current: Arc<ArcSwap<EscapeBridgePolicy>>,
}

impl EscapeBridgePolicyState {
    pub(crate) fn from_configs(broker_config: &BrokerConfig, message_store_config: &MessageStoreConfig) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(EscapeBridgePolicy::from_configs(
                broker_config,
                message_store_config,
            ))),
        }
    }

    pub(crate) fn snapshot(&self) -> Arc<EscapeBridgePolicy> {
        self.current.load_full()
    }

    pub(crate) fn update_broker_config(&self, broker_config: &BrokerConfig) {
        self.current.rcu(|current| {
            let mut replacement = current.as_ref().clone();
            replacement.apply_broker_config(broker_config);
            Arc::new(replacement)
        });
    }

    pub(crate) fn update_message_store_config(&self, message_store_config: &MessageStoreConfig) {
        self.current.rcu(|current| {
            let mut replacement = current.as_ref().clone();
            replacement.apply_message_store_config(message_store_config);
            Arc::new(replacement)
        });
    }
}

struct SharedAppendOutcome {
    result: PutMessageResult,
    appended_watermark: i64,
    durable_watermark: i64,
}

#[derive(Clone)]
struct SharedStoreAppendPort {
    store: Weak<StorePorts>,
}

impl SharedStoreAppendPort {
    fn new(store: &Arc<StorePorts>) -> Self {
        Self {
            store: Arc::downgrade(store),
        }
    }

    fn store(&self) -> Result<Arc<StorePorts>, MessageStoreUnavailable> {
        self.store.upgrade().ok_or(MessageStoreUnavailable)
    }

    async fn put_message(
        &self,
        message: MessageExtBrokerInner,
    ) -> Result<SharedAppendOutcome, MessageStoreUnavailable> {
        let store = self.store()?;
        let result = store.put_message_shared(message).await;
        Ok(SharedAppendOutcome {
            result,
            appended_watermark: store.get_max_phy_offset(),
            durable_watermark: store.get_flushed_where(),
        })
    }

    async fn put_messages(&self, batch: MessageExtBatch) -> Result<SharedAppendOutcome, MessageStoreUnavailable> {
        let store = self.store()?;
        let result = store.put_messages_shared(batch).await;
        Ok(SharedAppendOutcome {
            result,
            appended_watermark: store.get_max_phy_offset(),
            durable_watermark: store.get_flushed_where(),
        })
    }
}

/// Request-scoped read access to the Store boundary.
///
/// The lease upgrades the weak provider for one operation. Long-lived Broker
/// capabilities must retain only the weak provider, not this lease.
pub(crate) struct EscapeStoreReadLease<MS> {
    store: Arc<MS>,
}

impl<MS> Deref for EscapeStoreReadLease<MS> {
    type Target = MS;

    fn deref(&self) -> &Self::Target {
        self.store.as_ref()
    }
}

/// Late-bound Store operations required by failover and offset processing.
pub(crate) struct EscapeBridgeStoreCapability<MS> {
    current: Arc<RwLock<Option<Weak<MS>>>>,
    shared_append: Arc<RwLock<Option<SharedStoreAppendPort>>>,
}

impl<MS> Clone for EscapeBridgeStoreCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            current: Arc::clone(&self.current),
            shared_append: Arc::clone(&self.shared_append),
        }
    }
}

impl<MS> Default for EscapeBridgeStoreCapability<MS> {
    fn default() -> Self {
        Self {
            current: Arc::new(RwLock::new(None)),
            shared_append: Arc::new(RwLock::new(None)),
        }
    }
}

impl<MS: BrokerReadStore> EscapeBridgeStoreCapability<MS> {
    fn bind(&self, store: &Arc<MS>) {
        *self.current.write() = Some(Arc::downgrade(store));
    }

    pub(crate) fn detach(&self) {
        *self.current.write() = None;
        *self.shared_append.write() = None;
    }

    fn store(&self) -> Result<Arc<MS>, MessageStoreUnavailable> {
        self.current
            .read()
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or(MessageStoreUnavailable)
    }

    fn shared_append(&self) -> Result<SharedStoreAppendPort, MessageStoreUnavailable> {
        self.shared_append.read().clone().ok_or(MessageStoreUnavailable)
    }

    pub(crate) fn read_lease(&self) -> Result<EscapeStoreReadLease<MS>, MessageStoreUnavailable> {
        Ok(EscapeStoreReadLease { store: self.store()? })
    }

    pub(crate) fn with_store<R>(&self, operation: impl FnOnce(&MS) -> R) -> Result<R, MessageStoreUnavailable> {
        let store = self.store()?;
        Ok(operation(store.as_ref()))
    }

    pub(crate) fn health_snapshot(&self) -> Result<StoreHealthSnapshot, MessageStoreUnavailable> {
        self.with_store(|store| MessageStoreHealthCapability::new(store).health_snapshot())
    }

    pub(crate) async fn append_message(&self, message: MessageExtBrokerInner) -> Result<StoreAppendReceipt, StoreError>
    where
        MS: BrokerWriteStore,
    {
        let append = self
            .shared_append()
            .map_err(|_| StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Append))?;
        let outcome = append
            .put_message(message)
            .await
            .map_err(|_| StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Append))?;
        Ok(store_append_receipt(
            outcome.result,
            outcome.appended_watermark,
            outcome.durable_watermark,
        ))
    }

    pub(crate) async fn append_batch(&self, batch: MessageExtBatch) -> Result<StoreAppendReceipt, StoreError>
    where
        MS: BrokerWriteStore,
    {
        let append = self
            .shared_append()
            .map_err(|_| StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Append))?;
        let outcome = append
            .put_messages(batch)
            .await
            .map_err(|_| StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Append))?;
        Ok(store_append_receipt(
            outcome.result,
            outcome.appended_watermark,
            outcome.durable_watermark,
        ))
    }

    pub(crate) fn append_progress(&self) -> Result<(i64, i64), MessageStoreUnavailable> {
        self.with_store(|store| (store.get_max_phy_offset(), store.get_flushed_where()))
    }

    pub(crate) fn controller_heartbeat_offsets(&self) -> (Option<i64>, Option<i64>) {
        self.with_store(|store| (store.get_max_phy_offset(), store.get_confirm_offset()))
            .map(|(max_offset, confirm_offset)| (Some(max_offset), Some(confirm_offset)))
            .unwrap_or((None, None))
    }

    pub(crate) fn set_alive_replica_num_in_group(&self, alive_replica_num: i32) -> Result<(), MessageStoreUnavailable>
    where
        MS: BrokerReplicationStore,
    {
        self.with_store(|store| store.set_alive_replica_num_in_group(alive_replica_num))
    }

    pub(crate) fn set_master_flushed_offset(&self, offset: i64) -> Result<(), MessageStoreUnavailable>
    where
        MS: BrokerReplicationStore,
    {
        self.with_store(|store| store.set_master_flushed_offset(offset))
    }

    pub(crate) async fn submit_ha_transfer(
        &self,
        request: HAConnectionStateNotificationRequest,
    ) -> Result<bool, MessageStoreUnavailable>
    where
        MS: BrokerReplicationStore,
    {
        let store = self.store()?;
        let Some(ha_service) = store.get_ha_service() else {
            return Ok(false);
        };
        ha_service.put_group_connection_state_request(request).await;
        Ok(true)
    }

    pub(crate) async fn update_master_addresses(
        &self,
        master_ha_address: &CheetahString,
        master_address: &CheetahString,
    ) -> Result<(), MessageStoreUnavailable>
    where
        MS: BrokerMasterAddressStore,
    {
        let store = self.store()?;
        store
            .update_master_addresses(master_ha_address.as_str(), master_address)
            .await;
        Ok(())
    }

    pub(crate) fn sync_controller_sync_state_set(
        &self,
        local_broker_id: i64,
        sync_state_set: &HashSet<i64>,
    ) -> Result<(), MessageStoreUnavailable>
    where
        MS: BrokerReplicationStore,
    {
        self.with_store(|store| store.sync_controller_sync_state_set(local_broker_id, sync_state_set))
    }

    pub(crate) async fn apply_controller_role(
        &self,
        previous_store_role: BrokerRole,
        target_role: BrokerReplicaRole,
        controller_broker_id: u64,
        master_address: Option<&CheetahString>,
        master_epoch: i32,
    ) -> HAResult<()>
    where
        MS: BrokerReplicationStore,
    {
        let store = match self.store() {
            Ok(store) => store,
            Err(_) => return Ok(()),
        };
        store.fence_controller_writes();
        let Some(ha_service) = store.get_ha_service().cloned() else {
            return Ok(());
        };
        let result = match target_role {
            BrokerReplicaRole::Master => {
                if previous_store_role == BrokerRole::SyncMaster {
                    ha_service.change_to_master_when_last_role_is_master(master_epoch).await
                } else {
                    ha_service.change_to_master(master_epoch).await
                }
            }
            BrokerReplicaRole::Slave => {
                let master_address = master_address.ok_or_else(|| {
                    HAError::invalid_state("controller role change missing master address for store transition")
                })?;
                let current_master_address = ha_service.get_runtime_info(0).ha_client_runtime_info.master_addr;
                if previous_store_role == BrokerRole::Slave && current_master_address == master_address.as_str() {
                    ha_service
                        .change_to_slave_when_master_not_change(master_address.as_str(), master_epoch)
                        .await
                } else {
                    ha_service
                        .change_to_slave(master_address.as_str(), master_epoch, Some(controller_broker_id as i64))
                        .await
                }
            }
        };
        result?;
        store
            .sync_broker_role_with_term(
                match target_role {
                    BrokerReplicaRole::Master => BrokerRole::SyncMaster,
                    BrokerReplicaRole::Slave => BrokerRole::Slave,
                },
                u64::try_from(master_epoch).unwrap_or_default(),
            )
            .map_err(|error| HAError::invalid_state(format!("timer role fencing failed: {error}")))?;
        Ok(())
    }

    pub(crate) fn install_controller_write_lease(
        &self,
        token: WriteLeaseToken,
        valid_for: Duration,
    ) -> Result<bool, MessageStoreUnavailable>
    where
        MS: BrokerReplicationStore,
    {
        self.with_store(|store| store.install_controller_write_lease(token, valid_for))
    }

    pub(crate) fn fence_controller_writes(&self) -> Result<(), MessageStoreUnavailable>
    where
        MS: BrokerReplicationStore,
    {
        self.with_store(BrokerReplicationStore::fence_controller_writes)
    }

    pub(crate) fn master_flushed_offset(&self) -> Result<i64, MessageStoreUnavailable> {
        self.with_store(BrokerReadStore::get_master_flushed_offset)
    }

    pub(crate) async fn update_ha_master_address(&self, address: &str) -> Result<(), MessageStoreUnavailable>
    where
        MS: BrokerReplicationStore,
    {
        let store = self.store()?;
        store.update_ha_master_address(address).await;
        Ok(())
    }

    pub(crate) fn wakeup_ha_client(&self) -> Result<(), MessageStoreUnavailable>
    where
        MS: BrokerReplicationStore,
    {
        self.with_store(BrokerReplicationStore::wakeup_ha_client)
    }

    pub(crate) async fn query_message(
        &self,
        request: &QueryMessageRequest,
    ) -> Result<Option<QueryMessageResult>, MessageStoreUnavailable> {
        let store = self.store()?;
        Ok(store.query_message_with_options(request).await)
    }

    pub(crate) fn select_message(
        &self,
        offset: i64,
    ) -> Result<Option<SelectMappedBufferResult>, MessageStoreUnavailable> {
        self.with_store(|store| store.select_one_message_by_offset(offset))
    }

    pub(crate) async fn put_message(
        &self,
        message: MessageExtBrokerInner,
    ) -> Result<PutMessageResult, MessageStoreUnavailable>
    where
        MS: BrokerWriteStore,
    {
        Ok(self.shared_append()?.put_message(message).await?.result)
    }

    pub(crate) fn set_commitlog_read_mode(
        &self,
        read_ahead_mode: rocketmq_store::CommitLogReadMode,
    ) -> Result<(), BackendStoreError>
    where
        MS: BrokerAdminStore,
    {
        let store = self
            .store()
            .map_err(|_| BackendStoreError::new(StoreErrorKind::NotStarted, StoreOperation::Admin))?;
        store.set_commitlog_read_mode(read_ahead_mode)
    }

    pub(crate) fn delete_topics(&self, delete_topics: Vec<&CheetahString>) -> Result<i32, MessageStoreUnavailable>
    where
        MS: BrokerAdminStore,
    {
        let store = self.store()?;
        Ok(store.delete_topics(delete_topics))
    }

    pub(crate) fn min_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.with_store(|store| store.get_min_offset_in_queue(topic, queue_id))
    }

    pub(crate) fn max_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.with_store(|store| store.get_max_offset_in_queue(topic, queue_id))
    }

    pub(crate) fn now(&self) -> Result<u64, MessageStoreUnavailable> {
        self.with_store(BrokerReadStore::now)
    }

    pub(crate) async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        let store = self.store()?;
        Ok(store.get_message(group, topic, queue_id, offset, nums, None).await)
    }

    pub(crate) async fn get_message_with_filter(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        let store = self.store()?;
        Ok(store
            .get_message(group, topic, queue_id, offset, nums, message_filter)
            .await)
    }

    #[allow(clippy::too_many_arguments, reason = "preserves the Store pull read contract")]
    pub(crate) async fn get_message_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_msg_bytes: i32,
        message_filter: ArcMessageFilter,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        let store = self.store()?;
        Ok(store
            .get_message_with_size_limit(
                group,
                topic,
                queue_id,
                offset,
                max_msg_nums,
                max_msg_bytes,
                Some(message_filter),
            )
            .await)
    }

    pub(crate) fn look_message_by_offset(&self, offset: i64) -> Result<Option<MessageExt>, MessageStoreUnavailable> {
        self.with_store(|store| store.look_message_by_offset(offset))
    }

    pub(crate) fn state_machine_version(&self) -> Result<i64, MessageStoreUnavailable> {
        self.with_store(BrokerReadStore::get_state_machine_version)
    }

    pub(crate) fn update_logical_master_address(
        &self,
        master_addr: &CheetahString,
    ) -> Result<(), MessageStoreUnavailable>
    where
        MS: BrokerMasterAddressStore,
    {
        self.with_store(|store| store.update_logical_master_address(master_addr))
    }

    pub(crate) fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
    ) -> Result<bool, MessageStoreUnavailable> {
        self.with_store(|store| store.check_in_mem_by_consume_offset(topic, queue_id, 0, 1))
    }

    pub(crate) fn max_offset_in_queue(
        &self,
        topic: &CheetahString,
        queue_id: i32,
    ) -> Result<i64, MessageStoreUnavailable> {
        self.with_store(|store| store.get_max_offset_in_queue(topic, queue_id))
    }

    pub(crate) fn timer_lag(&self) -> Result<(i64, i64), MessageStoreUnavailable> {
        self.with_store(|store| {
            store
                .get_timer_message_store()
                .map(|timer_store| (timer_store.get_dequeue_behind(), timer_store.get_enqueue_behind()))
                .unwrap_or((0, 0))
        })
    }
}

impl EscapeBridgeStoreCapability<StorePorts> {
    pub(crate) fn bind_owned(&self, store: &Arc<StorePorts>) {
        self.bind(store);
        *self.shared_append.write() = Some(SharedStoreAppendPort::new(store));
    }
}

#[cfg(test)]
mod tests {
    use crate::config::broker_config::BrokerConfig;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::broker::broker_role::BrokerRole;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_store::StorePorts;

    use super::EscapeBridgePolicyState;
    use super::EscapeBridgeStoreCapability;
    use crate::controller::replicas_manager::BrokerReplicaRole;

    #[test]
    fn failover_policy_publishes_broker_and_store_updates() {
        let mut broker_config = BrokerConfig::default();
        broker_config.broker_identity.broker_name = CheetahString::from_static_str("broker-a");
        broker_config.broker_identity.broker_id = 1;
        let mut store_config = MessageStoreConfig::default();
        let state = EscapeBridgePolicyState::from_configs(&broker_config, &store_config);

        broker_config.broker_identity.broker_name = CheetahString::from_static_str("broker-b");
        broker_config.broker_identity.broker_id = 7;
        broker_config.enable_slave_acting_master = true;
        broker_config.enable_remote_escape = true;
        store_config.broker_role = BrokerRole::Slave;
        state.update_broker_config(&broker_config);
        state.update_message_store_config(&store_config);

        let snapshot = state.snapshot();
        assert_eq!(snapshot.broker_name.as_str(), "broker-b");
        assert_eq!(snapshot.broker_id, 7);
        assert!(snapshot.enable_slave_acting_master);
        assert!(snapshot.enable_remote_escape);
        assert_eq!(snapshot.broker_role, BrokerRole::Slave);
    }

    #[tokio::test]
    async fn controller_role_change_is_a_noop_before_store_binding() {
        let store = EscapeBridgeStoreCapability::<StorePorts>::default();

        assert!(store
            .apply_controller_role(BrokerRole::Slave, BrokerReplicaRole::Master, 0, None, 1)
            .await
            .is_ok());
    }

    #[test]
    fn controller_observations_fail_closed_before_store_binding() {
        let store = EscapeBridgeStoreCapability::<StorePorts>::default();

        assert_eq!(store.controller_heartbeat_offsets(), (None, None));
        assert!(store.set_alive_replica_num_in_group(1).is_err());
    }
}
