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
use std::sync::OnceLock;
use std::sync::Weak;

use arc_swap::ArcSwapOption;
use cheetah_string::CheetahString;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_error::RocketMQResult;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::timer::timer_message_store::TimerMessageStore;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::load_balance::message_request_mode_manager::MessageRequestModeManager;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::schedule::delay_offset_serialize_wrapper::DelayOffsetSerializeWrapper;
use crate::schedule::schedule_message_service::ScheduleMessageService;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_coordinator::TopicConfigCoordinator;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

pub(crate) struct SlaveSynchronize<MS: MessageStore> {
    context: SlaveSynchronizeContext<MS>,
    master_addr: Arc<SlaveMasterAddress>,
}

pub(crate) struct SlaveSynchronizePolicy {
    broker_addr: CheetahString,
    timer_wheel_enable: bool,
}

impl SlaveSynchronizePolicy {
    pub(crate) fn from_config(broker_addr: CheetahString, message_store_config: &MessageStoreConfig) -> Self {
        Self {
            broker_addr,
            timer_wheel_enable: message_store_config.timer_wheel_enable,
        }
    }
}

#[derive(Clone, Default)]
struct SlaveMessageRequestModeCapability {
    manager: Arc<ArcSwapOption<MessageRequestModeManager>>,
}

impl SlaveMessageRequestModeCapability {
    fn bind(&self, manager: &MessageRequestModeManager) {
        self.manager.store(Some(Arc::new(manager.clone())));
    }

    fn manager(&self) -> Option<Arc<MessageRequestModeManager>> {
        self.manager.load_full()
    }

    fn clear(&self) {
        self.manager.store(None);
    }
}

struct SlaveConsumerOffsetCapability<MS: MessageStore> {
    manager: Arc<OnceLock<Weak<ConsumerOffsetManager<MS>>>>,
}

impl<MS: MessageStore> Default for SlaveConsumerOffsetCapability<MS> {
    fn default() -> Self {
        Self {
            manager: Arc::new(OnceLock::new()),
        }
    }
}

impl<MS: MessageStore> SlaveConsumerOffsetCapability<MS> {
    fn bind(&self, manager: &Arc<ConsumerOffsetManager<MS>>) {
        self.manager.get_or_init(|| Arc::downgrade(manager));
    }

    fn manager(&self) -> Option<Arc<ConsumerOffsetManager<MS>>> {
        self.manager.get()?.upgrade()
    }
}

#[derive(Clone, Default)]
struct SlaveSubscriptionGroupCapability {
    manager: Arc<ArcSwapOption<SubscriptionGroupManager>>,
}

impl SlaveSubscriptionGroupCapability {
    fn new(manager: &SubscriptionGroupManager) -> Self {
        let capability = Self::default();
        capability.manager.store(Some(Arc::new(manager.clone())));
        capability
    }

    fn manager(&self) -> Option<Arc<SubscriptionGroupManager>> {
        self.manager.load_full()
    }

    fn clear(&self) {
        self.manager.store(None);
    }
}

pub(crate) struct SlaveTimerStoreCapability<MS: MessageStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> SlaveTimerStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
    }

    fn is_available(&self) -> bool {
        self.escape_bridge.strong_count() > 0
    }

    fn timer_message_store(&self) -> Option<Arc<TimerMessageStore>> {
        self.escape_bridge
            .upgrade()?
            .try_with_message_store(|store| store.get_timer_message_store().cloned())
            .ok()
            .flatten()
    }
}

pub(crate) struct SlaveSynchronizeContext<MS: MessageStore> {
    policy: SlaveSynchronizePolicy,
    broker_outer_api: BrokerOuterAPI,
    topic_config_manager: Weak<TopicConfigManager>,
    topic_config_coordinator: Weak<TopicConfigCoordinator>,
    topic_queue_mapping_manager: Weak<TopicQueueMappingManager>,
    consumer_offset_manager: SlaveConsumerOffsetCapability<MS>,
    schedule_message_service: Weak<ScheduleMessageService<MS>>,
    subscription_group_manager: SlaveSubscriptionGroupCapability,
    timer_store: SlaveTimerStoreCapability<MS>,
    message_request_mode: SlaveMessageRequestModeCapability,
}

impl<MS: MessageStore> SlaveSynchronizeContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "the composition root wires explicit slave synchronization capabilities"
    )]
    pub(crate) fn new(
        policy: SlaveSynchronizePolicy,
        broker_outer_api: BrokerOuterAPI,
        topic_config_manager: Arc<TopicConfigManager>,
        topic_config_coordinator: Arc<TopicConfigCoordinator>,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        schedule_message_service: Arc<ScheduleMessageService<MS>>,
        subscription_group_manager: SubscriptionGroupManager,
        timer_store: SlaveTimerStoreCapability<MS>,
    ) -> Self {
        Self {
            policy,
            broker_outer_api,
            topic_config_manager: Arc::downgrade(&topic_config_manager),
            topic_config_coordinator: Arc::downgrade(&topic_config_coordinator),
            topic_queue_mapping_manager: Arc::downgrade(&topic_queue_mapping_manager),
            consumer_offset_manager: SlaveConsumerOffsetCapability::default(),
            schedule_message_service: Arc::downgrade(&schedule_message_service),
            subscription_group_manager: SlaveSubscriptionGroupCapability::new(&subscription_group_manager),
            timer_store,
            message_request_mode: SlaveMessageRequestModeCapability::default(),
        }
    }
}

#[derive(Default)]
pub(crate) struct SlaveMasterAddress {
    current: ArcSwapOption<CheetahString>,
}

impl SlaveMasterAddress {
    pub(crate) fn load(&self) -> Option<Arc<CheetahString>> {
        self.current.load_full()
    }

    pub(crate) fn store(&self, addr: Option<&CheetahString>) {
        let current = self.load();
        if current.as_deref() == addr {
            return;
        }
        info!("Update master address from {:?} to {:?}", current, addr);
        self.current.store(addr.cloned().map(Arc::new));
    }
}

impl<MS> SlaveSynchronize<MS>
where
    MS: MessageStore,
{
    pub(crate) fn new(context: SlaveSynchronizeContext<MS>) -> Self {
        Self {
            context,
            master_addr: Arc::new(SlaveMasterAddress::default()),
        }
    }

    pub(crate) fn bind_message_request_mode_manager(&self, manager: &MessageRequestModeManager) {
        self.context.message_request_mode.bind(manager);
    }

    pub(crate) fn bind_consumer_offset_manager(&self, manager: &Arc<ConsumerOffsetManager<MS>>) {
        self.context.consumer_offset_manager.bind(manager);
    }

    pub(crate) fn release_runtime_capabilities(&self) {
        self.context.message_request_mode.clear();
        self.context.subscription_group_manager.clear();
    }

    pub fn master_addr(&self) -> Option<Arc<CheetahString>> {
        self.master_addr.load()
    }

    pub(crate) fn master_addr_handle(&self) -> Arc<SlaveMasterAddress> {
        Arc::clone(&self.master_addr)
    }

    pub fn set_master_addr(&self, addr: Option<&CheetahString>) {
        self.master_addr.store(addr);
    }

    pub async fn sync_all(&self) {
        self.sync_topic_config().await;
        self.sync_consumer_offset().await;
        self.sync_delay_offset().await;
        self.sync_subscription_group_config().await;
        self.sync_message_request_mode().await;
        if self.context.policy.timer_wheel_enable {
            self.sync_timer_metrics().await;
        }
    }

    fn check_master_addr(&self) -> (bool, Option<CheetahString>) {
        let master_addr_bak = self.master_addr();
        match &master_addr_bak {
            None => {
                warn!("Master address is not set");
                (false, None)
            }
            Some(addr) if addr.as_str() == self.context.policy.broker_addr.as_str() => {
                warn!(
                    "Master address is the same as broker address: {}",
                    self.context.policy.broker_addr
                );
                (false, None)
            }
            Some(addr) => (true, Some(addr.as_ref().clone())),
        }
    }

    pub async fn sync_timer_check_point(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if !flag {
            return;
        }

        if let Some(master_addr) = master_addr {
            let Some(timer_message_store) = self.context.timer_store.timer_message_store() else {
                return;
            };
            if timer_message_store.is_should_running_dequeue() {
                return;
            }

            match self.context.broker_outer_api.get_timer_check_point(&master_addr).await {
                Ok(Some(checkpoint_snapshot)) => {
                    match timer_message_store.sync_checkpoint_from_master(&checkpoint_snapshot) {
                        Ok(true) => {
                            info!("Update slave timer checkpoint from master, {}", master_addr);
                        }
                        Ok(false) => {
                            warn!("Local timer checkpoint is not initialized, {}", master_addr);
                        }
                        Err(e) => {
                            error!("Persist synced timer checkpoint error, {}: {:?}", master_addr, e);
                        }
                    }
                }
                Ok(None) => {
                    warn!("GetTimerCheckPoint return null, {}", master_addr);
                }
                Err(e) => {
                    error!("SyncTimerCheckPoint Exception, {}: {:?}", master_addr, e);
                }
            }
        }
    }

    async fn sync_topic_config(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self.sync_topic_config_internal(&master_addr).await {
                    Ok(_) => {
                        info!("Update slave topic config from master, {}", master_addr);
                    }
                    Err(e) => {
                        error!("SyncTopicConfig Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_topic_config_internal(&self, master_addr: &CheetahString) -> RocketMQResult<()> {
        let topic_wrapper = self.context.broker_outer_api.get_all_topic_config(master_addr).await?;
        if topic_wrapper.is_none() {
            warn!("GetAllTopicConfig return null, {}", master_addr);
            return Ok(());
        }

        let topic_wrapper = topic_wrapper.unwrap();
        let Some(topic_config_manager) = self.context.topic_config_manager.upgrade() else {
            warn!(
                "Topic config manager is unavailable, skip synchronization from {}",
                master_addr
            );
            return Ok(());
        };
        if topic_config_manager.replace_topic_config_table_from_master(
            topic_wrapper.topic_config_serialize_wrapper.topic_config_table.clone(),
            topic_wrapper.topic_config_serialize_wrapper.data_version(),
        ) {
            let Some(topic_config_coordinator) = self.context.topic_config_coordinator.upgrade() else {
                warn!("Topic config coordinator is unavailable, skip persistence after synchronization");
                return Ok(());
            };
            topic_config_coordinator.persist_and_wait().await?;
        }

        // Sync topic queue mapping if present and data version differs
        let version = topic_wrapper.mapping_data_version;
        let Some(topic_queue_mapping_manager) = self.context.topic_queue_mapping_manager.upgrade() else {
            warn!(
                "Topic queue mapping manager is unavailable, skip synchronization from {}",
                master_addr
            );
            return Ok(());
        };
        if version != topic_queue_mapping_manager.data_version() {
            topic_queue_mapping_manager
                .data_version_clone()
                .lock()
                .assign_new_one(&version);

            topic_queue_mapping_manager.persist();
        }
        Ok(())
    }

    async fn sync_consumer_offset(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self
                    .context
                    .broker_outer_api
                    .get_all_consumer_offset(&master_addr)
                    .await
                {
                    Ok(offset_wrapper) => {
                        if let Some(offset_wrapper) = offset_wrapper {
                            let Some(consumer_offset_manager) = self.context.consumer_offset_manager.manager() else {
                                warn!(
                                    "Consumer offset manager is unavailable, skip synchronization from {}",
                                    master_addr
                                );
                                return;
                            };
                            let data_version = offset_wrapper.data_version().clone();
                            consumer_offset_manager
                                .merge_offsets_from_peer(offset_wrapper.offset_table(), data_version);
                            consumer_offset_manager.persist();
                            info!("Update slave consumer offset from master, {}", master_addr);
                        } else {
                            warn!("GetAllConsumerOffset return null, {}", master_addr);
                        }
                    }
                    Err(e) => {
                        error!("SyncConsumerOffset Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_delay_offset(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self.context.broker_outer_api.get_delay_offset(&master_addr).await {
                    Ok(offset) => {
                        if let Some(offset) = offset {
                            let snapshot = match SerdeJsonUtils::from_json_str::<DelayOffsetSerializeWrapper>(&offset) {
                                Ok(snapshot) => snapshot,
                                Err(error) => {
                                    error!("Decode delay offset failed, {}: {:?}", master_addr, error);
                                    return;
                                }
                            };
                            let Some(schedule_message_service) = self.context.schedule_message_service.upgrade() else {
                                warn!(
                                    "Schedule message service is unavailable, skip synchronization from {}",
                                    master_addr
                                );
                                return;
                            };
                            if let Err(e) = schedule_message_service
                                .sync_delay_offset_from_peer(offset.as_str(), &snapshot)
                                .await
                            {
                                error!("Sync delay offset from peer error: {:?}", e);
                            }
                        } else {
                            warn!("GetDelayOffset return null, {}", master_addr);
                        }
                    }
                    Err(e) => {
                        error!("SyncDelayOffset Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_subscription_group_config(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self
                    .context
                    .broker_outer_api
                    .get_all_subscription_group_config(&master_addr)
                    .await
                {
                    Ok(subscription_wrapper) => {
                        if let Some(subscription_wrapper) = subscription_wrapper {
                            let Some(subscription_group_manager) = self.context.subscription_group_manager.manager()
                            else {
                                warn!(
                                    "Subscription group manager is unavailable, skip synchronization from {}",
                                    master_addr
                                );
                                return;
                            };

                            // Compare data versions using read locks
                            let current_version = subscription_group_manager.data_version().read().clone();
                            if current_version != *subscription_wrapper.data_version() {
                                // Update data version
                                *subscription_group_manager.data_version().write() =
                                    subscription_wrapper.data_version().clone();

                                let new_subscription_table = subscription_wrapper.subscription_group_table;
                                let subscription_table = subscription_group_manager.subscription_group_table();

                                // Clear and update subscription table using DashMap
                                subscription_table.clear();
                                for (key, value) in new_subscription_table {
                                    subscription_table.insert(key, value);
                                }
                                subscription_group_manager.persist();
                            }
                            info!("Update slave subscription group config from master, {}", master_addr);
                        } else {
                            warn!("GetAllSubscriptionGroupConfig return null, {}", master_addr);
                        }
                    }
                    Err(e) => {
                        error!("SyncSubscriptionGroupConfig Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_message_request_mode(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self
                    .context
                    .broker_outer_api
                    .get_message_request_mode(&master_addr)
                    .await
                {
                    Ok(mode) => {
                        if let Some(mode) = mode {
                            let Some(message_request_mode_manager) = self.context.message_request_mode.manager() else {
                                warn!(
                                    "Message request mode manager is not bound, skip synchronization from {}",
                                    master_addr
                                );
                                return;
                            };
                            let message_request_mode_map = message_request_mode_manager.message_request_mode_map();
                            let mut message_request_mode_map = message_request_mode_map.lock();
                            message_request_mode_map.clear();
                            message_request_mode_map.extend(mode.into_inner());
                            drop(message_request_mode_map);
                            message_request_mode_manager.persist();
                        } else {
                            warn!("GetMessageRequestMode return null, {}", master_addr);
                        }
                    }
                    Err(e) => {
                        error!("SyncMessageRequestMode Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_timer_metrics(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if !flag {
            return;
        }

        if let Some(master_addr) = master_addr {
            let Some(timer_message_store) = self.context.timer_store.timer_message_store() else {
                return;
            };

            match self.context.broker_outer_api.get_timer_metrics(&master_addr).await {
                Ok(Some(metrics_wrapper)) => {
                    if timer_message_store.timer_metrics.data_version() != *metrics_wrapper.data_version() {
                        timer_message_store.timer_metrics.apply_wrapper(metrics_wrapper);
                        timer_message_store.timer_metrics.persist();
                    }
                    info!("Update slave timer metrics from master, {}", master_addr);
                }
                Ok(None) => {
                    warn!("GetTimerMetrics return null, {}", master_addr);
                }
                Err(e) => {
                    error!("SyncTimerMetrics Exception, {}: {:?}", master_addr, e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::GenericMessageStore;

    use super::SlaveMasterAddress;
    use super::SlaveMessageRequestModeCapability;
    use super::SlaveSynchronizePolicy;
    use super::SlaveTimerStoreCapability;
    use crate::broker_runtime::BrokerRuntime;
    use crate::load_balance::message_request_mode_manager::MessageRequestModeManager;
    use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;

    #[test]
    fn slave_master_address_publishes_immutable_generations() {
        let address = SlaveMasterAddress::default();
        let first = CheetahString::from_static_str("127.0.0.1:10911");
        let second = CheetahString::from_static_str("127.0.0.2:10911");

        address.store(Some(&first));
        let first_generation = address.load().expect("first generation");
        address.store(Some(&second));

        assert_eq!(first_generation.as_str(), first.as_str());
        assert_eq!(address.load().as_deref(), Some(&second));
        address.store(None);
        assert!(address.load().is_none());
    }

    #[test]
    fn slave_synchronize_policy_captures_only_required_startup_values() {
        let message_store_config = MessageStoreConfig {
            timer_wheel_enable: true,
            ..Default::default()
        };
        let broker_addr = CheetahString::from_static_str("192.0.2.10:10911");

        let policy = SlaveSynchronizePolicy::from_config(broker_addr.clone(), &message_store_config);

        assert_eq!(policy.broker_addr, broker_addr);
        assert!(policy.timer_wheel_enable);
    }

    #[test]
    fn slave_synchronize_source_uses_only_explicit_capabilities() {
        let source = include_str!("slave_synchronize.rs");

        assert!(!source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(!source.contains(concat!("broker_runtime", "_inner")));
        assert!(!source.contains(concat!("mut_", "from_ref")));
        assert!(source.contains("context: SlaveSynchronizeContext<MS>"));
        assert!(source.contains("consumer_offset_manager: SlaveConsumerOffsetCapability<MS>"));
        assert!(source.contains("timer_store: SlaveTimerStoreCapability<MS>"));
        assert!(source.contains("message_request_mode: SlaveMessageRequestModeCapability"));
        assert!(source.contains("subscription_group_manager: SlaveSubscriptionGroupCapability"));
        assert!(source.contains("schedule_message_service: Weak<ScheduleMessageService<MS>>"));
    }

    #[test]
    fn slave_synchronize_request_mode_capability_supports_late_binding() {
        let capability = SlaveMessageRequestModeCapability::default();
        assert!(capability.manager().is_none());

        let manager = MessageRequestModeManager::new(Arc::new(MessageStoreConfig::default()));
        capability.bind(&manager);

        assert!(capability.manager().is_some());
    }

    #[test]
    fn slave_synchronize_weak_providers_do_not_keep_owners_alive() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let offset_manager = Arc::new(ConsumerOffsetManager::<GenericMessageStore>::new(
            Arc::clone(&broker_config),
            Arc::clone(&message_store_config),
            None,
        ));
        let offset_capability = super::SlaveConsumerOffsetCapability::default();

        assert!(offset_capability.manager().is_none());
        offset_capability.bind(&offset_manager);
        assert!(offset_capability.manager().is_some());
        drop(offset_manager);
        assert!(offset_capability.manager().is_none());

        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let inner = runtime.inner_for_test();
        let escape_bridge = inner.escape_bridge();
        let capability = SlaveTimerStoreCapability::new(&escape_bridge);

        assert!(capability.is_available());
        drop(escape_bridge);
        drop(runtime);

        assert!(!capability.is_available());
        assert!(capability.timer_message_store().is_none());
    }
}
