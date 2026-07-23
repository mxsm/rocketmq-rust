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
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Weak;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::base::query_message_result::QueryMessageResult;
use rocketmq_store::base::select_result::SelectMappedBufferResult;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::filter::ArcMessageFilter;
use rocketmq_store::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use rocketmq_store::ha::ha_service::HAService;
use rocketmq_store::message_store::OwnedMessageStore;
use rocketmq_store::store_api_adapter::legacy_append_receipt;
use rocketmq_store::store_api_adapter::LegacyAppendReceipt;
use rocketmq_store::store_api_adapter::LegacyMessageStoreHealthAdapter;
use rocketmq_store::store_api_adapter::LegacyStoreHealthSnapshot;
use rocketmq_store::store_error::HAError;
use rocketmq_store::store_error::HAResult;
use rocketmq_store::store_error::StoreError as LegacyStoreError;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
use rocketmq_store_api::StoreHealth;
use rocketmq_store_api::StoreOperation;

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

/// Temporary Store lifecycle owner used while the Store facade is migrated by R09-R16.
///
/// Broker runtime keeps the only long-lived strong owner. Other capabilities retain a weak
/// provider and upgrade it only for the duration of one operation.
pub(crate) struct LegacyEscapeStoreOwner<MS: MessageStore>(rocketmq_rust::ArcMut<MS>);

impl<MS: MessageStore> LegacyEscapeStoreOwner<MS> {
    pub(crate) fn new(store: MS) -> Self {
        Self(rocketmq_rust::ArcMut::new(store))
    }

    pub(crate) fn store(&self) -> &MS {
        self.0.as_ref()
    }

    pub(crate) fn write_lease(&self) -> LegacyEscapeStoreWriteLease<MS> {
        LegacyEscapeStoreWriteLease {
            owner: Self(self.0.clone()),
        }
    }

    #[cfg(test)]
    pub(crate) fn legacy_strong_count(&self) -> usize {
        self.0.strong_count()
    }

    fn cloned_store(&self) -> impl DerefMut<Target = MS> + Clone {
        self.0.clone()
    }
}

struct SharedAppendOutcome {
    result: PutMessageResult,
    appended_watermark: i64,
    durable_watermark: i64,
}

#[derive(Clone)]
struct SharedStoreAppendPort {
    owner: Weak<LegacyEscapeStoreOwner<OwnedMessageStore>>,
}

impl SharedStoreAppendPort {
    fn new(owner: &Arc<LegacyEscapeStoreOwner<OwnedMessageStore>>) -> Self {
        Self {
            owner: Arc::downgrade(owner),
        }
    }

    fn owner(&self) -> Result<Arc<LegacyEscapeStoreOwner<OwnedMessageStore>>, MessageStoreUnavailable> {
        self.owner.upgrade().ok_or(MessageStoreUnavailable)
    }

    async fn put_message(
        &self,
        message: MessageExtBrokerInner,
    ) -> Result<SharedAppendOutcome, MessageStoreUnavailable> {
        let owner = self.owner()?;
        let result = owner.store().put_message_shared(message).await;
        Ok(SharedAppendOutcome {
            result,
            appended_watermark: owner.store().get_max_phy_offset(),
            durable_watermark: owner.store().get_flushed_where(),
        })
    }

    async fn put_messages(&self, batch: MessageExtBatch) -> Result<SharedAppendOutcome, MessageStoreUnavailable> {
        let owner = self.owner()?;
        let result = owner.store().put_messages_shared(batch).await;
        Ok(SharedAppendOutcome {
            result,
            appended_watermark: owner.store().get_max_phy_offset(),
            durable_watermark: owner.store().get_flushed_where(),
        })
    }
}

/// Request-scoped read access to the legacy Store boundary.
///
/// The lease upgrades the weak provider for one operation without cloning the underlying
/// compatibility pointer. Long-lived Broker capabilities must not retain this lease.
pub(crate) struct LegacyEscapeStoreReadLease<MS: MessageStore> {
    owner: Arc<LegacyEscapeStoreOwner<MS>>,
}

impl<MS: MessageStore> Deref for LegacyEscapeStoreReadLease<MS> {
    type Target = MS;

    fn deref(&self) -> &Self::Target {
        self.owner.store()
    }
}

/// Request-scoped mutable access to the legacy Store boundary.
///
/// Mutable compatibility operations temporarily clone the legacy owner for the duration of one
/// Admin request. The lease must not be stored in a long-lived service or task.
pub(crate) struct LegacyEscapeStoreWriteLease<MS: MessageStore> {
    owner: LegacyEscapeStoreOwner<MS>,
}

impl<MS: MessageStore> Deref for LegacyEscapeStoreWriteLease<MS> {
    type Target = MS;

    fn deref(&self) -> &Self::Target {
        self.owner.store()
    }
}

impl<MS: MessageStore> DerefMut for LegacyEscapeStoreWriteLease<MS> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.owner.0.as_mut()
    }
}

/// Late-bound Store operations required by failover and offset processing.
pub(crate) struct EscapeBridgeStoreCapability<MS: MessageStore> {
    current: Arc<RwLock<Option<Weak<LegacyEscapeStoreOwner<MS>>>>>,
    shared_append: Arc<RwLock<Option<SharedStoreAppendPort>>>,
}

impl<MS: MessageStore> Clone for EscapeBridgeStoreCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            current: Arc::clone(&self.current),
            shared_append: Arc::clone(&self.shared_append),
        }
    }
}

impl<MS: MessageStore> Default for EscapeBridgeStoreCapability<MS> {
    fn default() -> Self {
        Self {
            current: Arc::new(RwLock::new(None)),
            shared_append: Arc::new(RwLock::new(None)),
        }
    }
}

impl<MS: MessageStore> EscapeBridgeStoreCapability<MS> {
    fn bind(&self, owner: &Arc<LegacyEscapeStoreOwner<MS>>) {
        *self.current.write() = Some(Arc::downgrade(owner));
    }

    fn owner(&self) -> Result<Arc<LegacyEscapeStoreOwner<MS>>, MessageStoreUnavailable> {
        self.current
            .read()
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or(MessageStoreUnavailable)
    }

    fn shared_append(&self) -> Result<SharedStoreAppendPort, MessageStoreUnavailable> {
        self.shared_append.read().clone().ok_or(MessageStoreUnavailable)
    }

    pub(crate) fn read_lease(&self) -> Result<LegacyEscapeStoreReadLease<MS>, MessageStoreUnavailable> {
        Ok(LegacyEscapeStoreReadLease { owner: self.owner()? })
    }

    pub(crate) fn with_store<R>(&self, operation: impl FnOnce(&MS) -> R) -> Result<R, MessageStoreUnavailable> {
        let owner = self.owner()?;
        Ok(operation(owner.store()))
    }

    pub(crate) fn health_snapshot(&self) -> Result<LegacyStoreHealthSnapshot, MessageStoreUnavailable> {
        self.with_store(|store| LegacyMessageStoreHealthAdapter::new(store).health_snapshot())
    }

    pub(crate) async fn append_message(
        &self,
        message: MessageExtBrokerInner,
    ) -> Result<LegacyAppendReceipt, StoreError> {
        let append = self
            .shared_append()
            .map_err(|_| StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Append))?;
        let outcome = append
            .put_message(message)
            .await
            .map_err(|_| StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Append))?;
        Ok(legacy_append_receipt(
            outcome.result,
            outcome.appended_watermark,
            outcome.durable_watermark,
        ))
    }

    pub(crate) async fn append_batch(&self, batch: MessageExtBatch) -> Result<LegacyAppendReceipt, StoreError> {
        let append = self
            .shared_append()
            .map_err(|_| StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Append))?;
        let outcome = append
            .put_messages(batch)
            .await
            .map_err(|_| StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Append))?;
        Ok(legacy_append_receipt(
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

    pub(crate) fn set_alive_replica_num_in_group(&self, alive_replica_num: i32) -> Result<(), MessageStoreUnavailable> {
        self.with_store(|store| store.set_alive_replica_num_in_group(alive_replica_num))
    }

    pub(crate) fn set_master_flushed_offset(&self, offset: i64) -> Result<(), MessageStoreUnavailable> {
        self.with_store(|store| store.set_master_flushed_offset(offset))
    }

    pub(crate) async fn submit_ha_transfer(
        &self,
        request: HAConnectionStateNotificationRequest,
    ) -> Result<bool, MessageStoreUnavailable> {
        let owner = self.owner()?;
        let Some(ha_service) = owner.store().get_ha_service() else {
            return Ok(false);
        };
        ha_service.put_group_connection_state_request(request).await;
        Ok(true)
    }

    pub(crate) async fn update_master_addresses(
        &self,
        master_ha_address: &CheetahString,
        master_address: &CheetahString,
    ) -> Result<(), MessageStoreUnavailable> {
        let owner = self.owner()?;
        let store = owner.store();
        store.update_ha_master_address(master_ha_address.as_str()).await;
        store.update_master_address(master_address);
        Ok(())
    }

    pub(crate) fn sync_controller_sync_state_set(
        &self,
        local_broker_id: i64,
        sync_state_set: &HashSet<i64>,
    ) -> Result<(), MessageStoreUnavailable> {
        self.with_store(|store| store.sync_controller_sync_state_set(local_broker_id, sync_state_set))
    }

    pub(crate) async fn apply_controller_role(
        &self,
        previous_store_role: BrokerRole,
        target_role: BrokerReplicaRole,
        controller_broker_id: u64,
        master_address: Option<&CheetahString>,
        master_epoch: i32,
    ) -> HAResult<()> {
        let owner = match self.owner() {
            Ok(owner) => owner,
            Err(_) => return Ok(()),
        };
        let mut store = owner.cloned_store();
        let Some(ha_service) = store.get_ha_service() else {
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
                    HAError::Service("controller role change missing master address for store transition".to_owned())
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
        store.sync_broker_role(match target_role {
            BrokerReplicaRole::Master => BrokerRole::SyncMaster,
            BrokerReplicaRole::Slave => BrokerRole::Slave,
        });
        Ok(())
    }

    pub(crate) fn master_flushed_offset(&self) -> Result<i64, MessageStoreUnavailable> {
        self.with_store(MessageStore::get_master_flushed_offset)
    }

    pub(crate) async fn update_ha_master_address(&self, address: &str) -> Result<(), MessageStoreUnavailable> {
        let owner = self.owner()?;
        owner.store().update_ha_master_address(address).await;
        Ok(())
    }

    pub(crate) fn wakeup_ha_client(&self) -> Result<(), MessageStoreUnavailable> {
        self.with_store(MessageStore::wakeup_ha_client)
    }

    pub(crate) async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<Option<QueryMessageResult>, MessageStoreUnavailable> {
        let owner = self.owner()?;
        Ok(owner
            .store()
            .query_message(topic, key, max_num, begin_timestamp, end_timestamp)
            .await)
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
    ) -> Result<PutMessageResult, MessageStoreUnavailable> {
        Ok(self.shared_append()?.put_message(message).await?.result)
    }

    pub(crate) fn set_commitlog_read_mode(&self, read_ahead_mode: i32) -> Result<(), LegacyStoreError> {
        let owner = self.owner().map_err(|_| LegacyStoreError::NotStarted)?;
        owner.write_lease().set_commitlog_read_mode(read_ahead_mode)
    }

    pub(crate) fn delete_topics(&self, delete_topics: Vec<&CheetahString>) -> Result<i32, MessageStoreUnavailable> {
        let owner = self.owner()?;
        Ok(owner.write_lease().delete_topics(delete_topics))
    }

    pub(crate) fn min_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.with_store(|store| store.get_min_offset_in_queue(topic, queue_id))
    }

    pub(crate) fn max_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.with_store(|store| store.get_max_offset_in_queue(topic, queue_id))
    }

    pub(crate) fn now(&self) -> Result<u64, MessageStoreUnavailable> {
        self.with_store(MessageStore::now)
    }

    pub(crate) async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        let owner = self.owner()?;
        Ok(owner
            .store()
            .get_message(group, topic, queue_id, offset, nums, None)
            .await)
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
        let owner = self.owner()?;
        Ok(owner
            .store()
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
        let owner = self.owner()?;
        let store = owner.store();
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
        self.with_store(MessageStore::get_state_machine_version)
    }

    pub(crate) async fn update_master_address(
        &self,
        master_addr: &CheetahString,
    ) -> Result<(), MessageStoreUnavailable> {
        let owner = self.owner()?;
        let store = owner.store();
        store.update_ha_master_address(master_addr.as_str()).await;
        store.update_master_address(master_addr);
        Ok(())
    }

    pub(crate) fn check_in_mem_by_consume_offset(
        &self,
        topic: &CheetahString,
        queue_id: i32,
    ) -> Result<bool, MessageStoreUnavailable> {
        self.with_store(|store| store.check_in_mem_by_consume_offset(topic, queue_id, 0, 1))
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

impl EscapeBridgeStoreCapability<OwnedMessageStore> {
    pub(crate) fn bind_owned(&self, owner: &Arc<LegacyEscapeStoreOwner<OwnedMessageStore>>) {
        self.bind(owner);
        *self.shared_append.write() = Some(SharedStoreAppendPort::new(owner));
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::broker::broker_role::BrokerRole;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::GenericMessageStore;

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
        let store = EscapeBridgeStoreCapability::<GenericMessageStore>::default();

        assert!(store
            .apply_controller_role(BrokerRole::Slave, BrokerReplicaRole::Master, 0, None, 1)
            .await
            .is_ok());
    }

    #[test]
    fn controller_observations_fail_closed_before_store_binding() {
        let store = EscapeBridgeStoreCapability::<GenericMessageStore>::default();

        assert_eq!(store.controller_heartbeat_offsets(), (None, None));
        assert!(store.set_alive_replica_num_in_group(1).is_err());
    }
}
