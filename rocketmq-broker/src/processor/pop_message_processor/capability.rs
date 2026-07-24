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

use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Weak;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_runtime::TaskGroup;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::filter::ArcMessageFilter;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;

use crate::broker_runtime::broker_task_group_or_current;
use crate::client::manager::consumer_manager::ConsumerManager;
use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetRequestCapability;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::processor::pop_inflight_message_counter::PopInflightMessageCounter;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_coordinator::TopicConfigCoordinator;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

#[derive(Clone, Debug)]
pub(crate) struct PopPolicy {
    pub(crate) broker_ip: CheetahString,
    pub(crate) broker_name: CheetahString,
    pub(crate) broker_cluster_name: CheetahString,
    pub(crate) broker_permission: u32,
    pub(crate) broker_role: BrokerRole,
    pub(crate) enable_retry_topic_v2: bool,
    pub(crate) revive_queue_num: u32,
    pub(crate) pop_from_retry_probability: i32,
    pub(crate) retrieve_message_from_pop_retry_topic_v1: bool,
    pub(crate) transfer_msg_by_heap: bool,
    pub(crate) enable_pop_log: bool,
    pub(crate) pop_response_return_actual_retry_topic: bool,
    pub(crate) enable_pop_message_threshold: bool,
    pub(crate) pop_inflight_message_threshold: i64,
    pub(crate) init_pop_offset_by_check_msg_in_mem: bool,
    pub(crate) pop_consumer_kv_service_init: bool,
    pub(crate) pop_consumer_kv_service_enable: bool,
    pub(crate) enable_pop_buffer_merge: bool,
    pub(crate) pop_ck_stay_buffer_time_out: u64,
    pub(crate) pop_ck_stay_buffer_time: u64,
    pub(crate) pop_ck_max_buffer_size: i64,
    pub(crate) pop_ck_offset_max_queue_size: u64,
    pub(crate) enable_slave_acting_master: bool,
    pub(crate) enable_pop_batch_ack: bool,
    pub(crate) revive_interval: u64,
    pub(crate) revive_max_slow: u64,
    pub(crate) revive_scan_time: u64,
    pub(crate) enable_skip_long_awaiting_ack: bool,
    pub(crate) skip_when_ck_re_put_reach_max_times: bool,
    pub(crate) revive_ack_wait_ms: u64,
    pub(crate) timer_wheel_enable: bool,
    pub(crate) store_path_root_dir: CheetahString,
    pub(crate) pop_rocksdb_block_cache_size: usize,
    pub(crate) pop_rocksdb_write_buffer_size: usize,
    pub(crate) store_host: SocketAddr,
}

impl PopPolicy {
    fn from_configs(broker: &BrokerConfig, store: &MessageStoreConfig, store_host: SocketAddr) -> Self {
        Self {
            broker_ip: broker.broker_ip1.clone(),
            broker_name: broker.broker_name().clone(),
            broker_cluster_name: broker.broker_identity.broker_cluster_name.clone(),
            broker_permission: broker.broker_permission,
            broker_role: store.broker_role,
            enable_retry_topic_v2: broker.enable_retry_topic_v2,
            revive_queue_num: broker.revive_queue_num,
            pop_from_retry_probability: broker.pop_from_retry_probability,
            retrieve_message_from_pop_retry_topic_v1: broker.retrieve_message_from_pop_retry_topic_v1,
            transfer_msg_by_heap: broker.transfer_msg_by_heap,
            enable_pop_log: broker.enable_pop_log,
            pop_response_return_actual_retry_topic: broker.pop_response_return_actual_retry_topic,
            enable_pop_message_threshold: broker.enable_pop_message_threshold,
            pop_inflight_message_threshold: broker.pop_inflight_message_threshold,
            init_pop_offset_by_check_msg_in_mem: broker.init_pop_offset_by_check_msg_in_mem,
            pop_consumer_kv_service_init: broker.pop_consumer_kv_service_init,
            pop_consumer_kv_service_enable: broker.pop_consumer_kv_service_enable,
            enable_pop_buffer_merge: broker.enable_pop_buffer_merge,
            pop_ck_stay_buffer_time_out: broker.pop_ck_stay_buffer_time_out,
            pop_ck_stay_buffer_time: broker.pop_ck_stay_buffer_time,
            pop_ck_max_buffer_size: broker.pop_ck_max_buffer_size,
            pop_ck_offset_max_queue_size: broker.pop_ck_offset_max_queue_size,
            enable_slave_acting_master: broker.enable_slave_acting_master,
            enable_pop_batch_ack: broker.enable_pop_batch_ack,
            revive_interval: broker.revive_interval,
            revive_max_slow: broker.revive_max_slow,
            revive_scan_time: broker.revive_scan_time,
            enable_skip_long_awaiting_ack: broker.enable_skip_long_awaiting_ack,
            skip_when_ck_re_put_reach_max_times: broker.skip_when_ck_re_put_reach_max_times,
            revive_ack_wait_ms: broker.revive_ack_wait_ms,
            timer_wheel_enable: store.timer_wheel_enable,
            store_path_root_dir: store.store_path_root_dir.clone(),
            pop_rocksdb_block_cache_size: store.pop_rocksdb_block_cache_size,
            pop_rocksdb_write_buffer_size: store.pop_rocksdb_write_buffer_size,
            store_host,
        }
    }

    fn apply_broker_config(&mut self, broker: &BrokerConfig) {
        let store_role = self.broker_role;
        let store_host = self.store_host;
        let store_path_root_dir = self.store_path_root_dir.clone();
        let block_cache_size = self.pop_rocksdb_block_cache_size;
        let write_buffer_size = self.pop_rocksdb_write_buffer_size;
        let timer_wheel_enable = self.timer_wheel_enable;
        *self = Self::from_configs(broker, &MessageStoreConfig::default(), store_host);
        self.broker_role = store_role;
        self.store_path_root_dir = store_path_root_dir;
        self.pop_rocksdb_block_cache_size = block_cache_size;
        self.pop_rocksdb_write_buffer_size = write_buffer_size;
        self.timer_wheel_enable = timer_wheel_enable;
    }

    fn apply_store_config(&mut self, store: &MessageStoreConfig) {
        self.broker_role = store.broker_role;
        self.timer_wheel_enable = store.timer_wheel_enable;
        self.store_path_root_dir = store.store_path_root_dir.clone();
        self.pop_rocksdb_block_cache_size = store.pop_rocksdb_block_cache_size;
        self.pop_rocksdb_write_buffer_size = store.pop_rocksdb_write_buffer_size;
    }
}

#[derive(Clone)]
pub(crate) struct PopPolicyState {
    current: Arc<ArcSwap<PopPolicy>>,
}

impl PopPolicyState {
    pub(crate) fn from_configs(broker: &BrokerConfig, store: &MessageStoreConfig, store_host: SocketAddr) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(PopPolicy::from_configs(
                broker, store, store_host,
            ))),
        }
    }

    pub(crate) fn snapshot(&self) -> Arc<PopPolicy> {
        self.current.load_full()
    }

    pub(crate) fn update_broker_config(&self, broker: &BrokerConfig) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.apply_broker_config(broker);
            Arc::new(next)
        });
    }

    pub(crate) fn update_store_config(&self, store: &MessageStoreConfig) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.apply_store_config(store);
            Arc::new(next)
        });
    }

    pub(crate) fn update_store_host(&self, store_host: SocketAddr) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.store_host = store_host;
            Arc::new(next)
        });
    }
}

pub(crate) struct PopConsumerCapability {
    manager: ConsumerManager,
}

impl Clone for PopConsumerCapability {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone_shared_state(),
        }
    }
}

impl PopConsumerCapability {
    pub(crate) fn new(manager: &ConsumerManager) -> Self {
        Self {
            manager: manager.clone_shared_state(),
        }
    }

    pub(crate) fn compensate_basic_consumer_info(
        &self,
        group: &CheetahString,
        consume_type: ConsumeType,
        message_model: MessageModel,
    ) {
        self.manager
            .compensate_basic_consumer_info(group, consume_type, message_model);
    }

    pub(crate) fn compensate_subscribe_data(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        subscription_data: &SubscriptionData,
    ) {
        self.manager.compensate_subscribe_data(group, topic, subscription_data);
    }
}

pub(crate) struct PopOrderCapability {
    manager: Weak<ConsumerOrderInfoManager>,
}

impl Clone for PopOrderCapability {
    fn clone(&self) -> Self {
        Self {
            manager: Weak::clone(&self.manager),
        }
    }
}

impl PopOrderCapability {
    pub(crate) fn new(manager: &Arc<ConsumerOrderInfoManager>) -> Self {
        Self {
            manager: Arc::downgrade(manager),
        }
    }

    pub(crate) fn check_block(
        &self,
        attempt_id: &CheetahString,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        invisible_time: u64,
    ) -> bool {
        self.manager
            .upgrade()
            .is_some_and(|manager| manager.check_block(attempt_id, topic, group, queue_id, invisible_time))
    }

    #[allow(clippy::too_many_arguments, reason = "preserves the POP order update contract")]
    pub(crate) fn update(
        &self,
        attempt_id: CheetahString,
        is_retry: bool,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        pop_time: u64,
        invisible_time: u64,
        offsets: Vec<u64>,
        order_info: &mut String,
    ) -> bool {
        self.manager.upgrade().is_some_and(|manager| {
            manager.update(
                attempt_id,
                is_retry,
                topic,
                group,
                queue_id,
                pop_time,
                invisible_time,
                offsets,
                order_info,
            )
        })
    }

    pub(crate) fn clear_block(&self, topic: &CheetahString, group: &CheetahString, queue_id: i32) {
        if let Some(manager) = self.manager.upgrade() {
            manager.clear_block(topic, group, queue_id);
        }
    }
}

pub(crate) struct PopStoreCapability<MS: MessageStore> {
    provider: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> Clone for PopStoreCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            provider: Weak::clone(&self.provider),
        }
    }
}

impl<MS: MessageStore> PopStoreCapability<MS> {
    pub(crate) fn new(provider: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            provider: Arc::downgrade(provider),
        }
    }

    fn provider(&self) -> Result<Arc<EscapeBridge<MS>>, MessageStoreUnavailable> {
        self.provider.upgrade().ok_or(MessageStoreUnavailable)
    }

    pub(crate) fn min_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.provider()?.get_min_offset_from_local_store(topic, queue_id)
    }

    pub(crate) fn max_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.provider()?.get_max_offset_from_local_store(topic, queue_id)
    }

    pub(crate) fn check_in_mem(&self, topic: &CheetahString, queue_id: i32) -> Result<bool, MessageStoreUnavailable> {
        self.provider()?.check_in_mem_by_consume_offset(topic, queue_id)
    }

    pub(crate) async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        self.provider()?
            .get_message_with_filter_from_local_store(group, topic, queue_id, offset, nums, message_filter)
            .await
    }

    pub(crate) async fn put_local(
        &self,
        message: rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner,
    ) -> Result<PutMessageResult, MessageStoreUnavailable> {
        self.provider()?.put_message_to_local_store(message).await
    }

    pub(crate) async fn put_specific(
        &self,
        message: rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner,
    ) -> Result<PutMessageResult, MessageStoreUnavailable> {
        Ok(self.provider()?.put_message_to_specific_queue(message).await)
    }

    pub(crate) fn state_machine_version(&self) -> Result<i64, MessageStoreUnavailable> {
        self.provider()?.local_store_state_machine_version()
    }

    pub(crate) fn timer_lag(&self) -> Result<(i64, i64), MessageStoreUnavailable> {
        self.provider()?.local_timer_lag()
    }

    pub(crate) async fn get_message_async(
        &self,
        topic: &CheetahString,
        offset: i64,
        queue_id: i32,
        broker_name: &CheetahString,
        de_compress_body: bool,
    ) -> (
        Option<rocketmq_common::common::message::message_ext::MessageExt>,
        String,
        bool,
    ) {
        let Ok(provider) = self.provider() else {
            return (None, "message store is unavailable".to_string(), false);
        };
        provider
            .get_message_async(topic, offset, queue_id, broker_name, de_compress_body)
            .await
    }
}

pub(crate) struct PopMessageProcessorContext<MS: MessageStore> {
    pub(crate) policy: PopPolicyState,
    pub(crate) topics: Arc<TopicConfigManager>,
    pub(crate) subscriptions: SubscriptionGroupConfigLookup,
    pub(crate) consumers: PopConsumerCapability,
    pub(crate) filters: Arc<ConsumerFilterManager>,
    pub(crate) offsets: ConsumerOffsetRequestCapability<MS>,
    pub(crate) order: PopOrderCapability,
    pub(crate) store: PopStoreCapability<MS>,
    pub(crate) stats: Arc<BrokerStatsManager>,
    pub(crate) inflight: PopInflightMessageCounter,
}

#[allow(
    clippy::too_many_arguments,
    reason = "composition root enumerates the POP request capability boundary"
)]
impl<MS: MessageStore> PopMessageProcessorContext<MS> {
    pub(crate) fn new(
        policy: PopPolicyState,
        topics: Arc<TopicConfigManager>,
        subscriptions: SubscriptionGroupConfigLookup,
        consumers: PopConsumerCapability,
        filters: Arc<ConsumerFilterManager>,
        offsets: ConsumerOffsetRequestCapability<MS>,
        order: PopOrderCapability,
        store: PopStoreCapability<MS>,
        stats: Arc<BrokerStatsManager>,
        inflight: PopInflightMessageCounter,
    ) -> Self {
        Self {
            policy,
            topics,
            subscriptions,
            consumers,
            filters,
            offsets,
            order,
            store,
            stats,
            inflight,
        }
    }
}

pub(crate) struct PopBufferMergeContext<MS: MessageStore> {
    pub(crate) policy: PopPolicyState,
    pub(crate) topics: Arc<TopicConfigManager>,
    pub(crate) subscriptions: SubscriptionGroupConfigLookup,
    pub(crate) offsets: ConsumerOffsetRequestCapability<MS>,
    pub(crate) store: PopStoreCapability<MS>,
    parent_task_group: Option<TaskGroup>,
}

impl<MS: MessageStore> PopBufferMergeContext<MS> {
    pub(crate) fn new(
        policy: PopPolicyState,
        topics: Arc<TopicConfigManager>,
        subscriptions: SubscriptionGroupConfigLookup,
        offsets: ConsumerOffsetRequestCapability<MS>,
        store: PopStoreCapability<MS>,
        parent_task_group: Option<TaskGroup>,
    ) -> Self {
        Self {
            policy,
            topics,
            subscriptions,
            offsets,
            store,
            parent_task_group,
        }
    }

    pub(crate) fn task_group(&self) -> Option<TaskGroup> {
        broker_task_group_or_current(
            self.parent_task_group.as_ref(),
            "rocketmq-broker.pop-buffer-merge",
            "failed to start PopBufferMergeService outside Tokio runtime",
        )
    }
}

pub(crate) struct PopReviveContext<MS: MessageStore> {
    pub(crate) policy: PopPolicyState,
    pub(crate) topics: Arc<TopicConfigManager>,
    pub(crate) topic_coordinator: Arc<TopicConfigCoordinator>,
    pub(crate) subscriptions: SubscriptionGroupConfigLookup,
    pub(crate) offsets: ConsumerOffsetRequestCapability<MS>,
    pub(crate) store: PopStoreCapability<MS>,
    pub(crate) stats: Arc<BrokerStatsManager>,
    pub(crate) inflight: PopInflightMessageCounter,
    pub(crate) should_start_time: Arc<AtomicU64>,
    parent_task_group: Option<TaskGroup>,
}

#[allow(
    clippy::too_many_arguments,
    reason = "composition root enumerates the POP revive capability boundary"
)]
impl<MS: MessageStore> PopReviveContext<MS> {
    pub(crate) fn new(
        policy: PopPolicyState,
        topics: Arc<TopicConfigManager>,
        topic_coordinator: Arc<TopicConfigCoordinator>,
        subscriptions: SubscriptionGroupConfigLookup,
        offsets: ConsumerOffsetRequestCapability<MS>,
        store: PopStoreCapability<MS>,
        stats: Arc<BrokerStatsManager>,
        inflight: PopInflightMessageCounter,
        should_start_time: Arc<AtomicU64>,
        parent_task_group: Option<TaskGroup>,
    ) -> Self {
        Self {
            policy,
            topics,
            topic_coordinator,
            subscriptions,
            offsets,
            store,
            stats,
            inflight,
            should_start_time,
            parent_task_group,
        }
    }

    pub(crate) fn task_group(&self, queue_id: i32) -> Option<TaskGroup> {
        broker_task_group_or_current(
            self.parent_task_group.as_ref(),
            format!("rocketmq-broker.pop-revive.{queue_id}"),
            "failed to start PopReviveService outside Tokio runtime",
        )
    }

    pub(crate) fn update_retry_topic(&self, topic_config: TopicConfig) {
        let Ok(version) = self.store.state_machine_version() else {
            return;
        };
        let _ = self.topics.update_topic_config(topic_config, version);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::broker::broker_role::BrokerRole;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::OwnedMessageStore;

    use super::PopPolicyState;
    use super::PopStoreCapability;

    #[test]
    fn pop_policy_state_publishes_broker_and_store_updates() {
        let mut broker = BrokerConfig::default();
        let mut store = MessageStoreConfig::default();
        let state = PopPolicyState::from_configs(&broker, &store, "127.0.0.1:10911".parse().unwrap());

        broker.enable_pop_log = true;
        broker.revive_interval = 321;
        store.timer_wheel_enable = false;
        store.broker_role = BrokerRole::Slave;
        state.update_broker_config(&broker);
        state.update_store_config(&store);

        let policy = state.snapshot();
        assert!(policy.enable_pop_log);
        assert_eq!(policy.revive_interval, 321);
        assert!(!policy.timer_wheel_enable);
        assert_eq!(policy.broker_role, BrokerRole::Slave);
    }

    #[test]
    fn capability_source_has_no_complete_runtime_or_shared_mutation_owner() {
        let source = include_str!("capability.rs");

        assert!(!source.contains(concat!("Broker", "RuntimeInner")));
        assert!(!source.contains(concat!("Arc", "Mut")));
        assert!(!source.contains(concat!("mut", "_from_ref")));
    }

    #[test]
    fn pop_sources_have_no_complete_runtime_or_shared_mutation_owner() {
        for source in [
            include_str!("../pop_message_processor.rs"),
            include_str!("../processor_service/pop_buffer_merge_service.rs"),
            include_str!("../processor_service/pop_revive_service.rs"),
        ] {
            assert!(!source.contains(concat!("Broker", "RuntimeInner")));
            assert!(!source.contains(concat!("Arc", "Mut")));
            assert!(!source.contains(concat!("mut", "_from_ref")));
        }

        let processor = include_str!("../pop_message_processor.rs");
        assert!(processor.contains("get_message_result: &mut GetMessageResult"));
    }

    #[test]
    fn pop_store_capability_fails_closed_without_provider() {
        let capability = PopStoreCapability::<OwnedMessageStore> { provider: Weak::new() };

        assert!(capability.max_offset(&"topic".into(), 0).is_err());
        assert!(capability.timer_lag().is_err());
    }
}
