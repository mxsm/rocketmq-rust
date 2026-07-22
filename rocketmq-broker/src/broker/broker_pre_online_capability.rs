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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::common::remoting_helper::RemotingHelper;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use rocketmq_store::timer::timer_message_store::TimerMessageStore;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;

use crate::failover::escape_bridge::EscapeBridge;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::plugin::broker_attached_plugin::BrokerAttachedPlugin;
use crate::processor::ack_message_processor::AckMessageProcessor;
use crate::schedule::schedule_message_service::ScheduleMessageService;
use crate::topic::manager::topic_config_coordinator::TopicConfigCoordinator;
use crate::topic::manager::topic_config_coordinator::TopicRegistrationAction;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;
use crate::transaction::transactional_message_check_service::TransactionalMessageCheckService;

pub(crate) struct BrokerOnlineRoleState {
    local_broker_id: AtomicU64,
    min_broker_id: AtomicU64,
    min_broker_addr: Mutex<Option<CheetahString>>,
    isolated: AtomicBool,
}

impl BrokerOnlineRoleState {
    pub(crate) fn new(local_broker_id: u64) -> Self {
        Self {
            local_broker_id: AtomicU64::new(local_broker_id),
            min_broker_id: AtomicU64::new(0),
            min_broker_addr: Mutex::new(None),
            isolated: AtomicBool::new(false),
        }
    }

    pub(crate) fn local_broker_id(&self) -> u64 {
        self.local_broker_id.load(Ordering::Acquire)
    }

    pub(crate) fn set_local_broker_id(&self, broker_id: u64) {
        self.local_broker_id.store(broker_id, Ordering::Release);
    }

    pub(crate) fn min_broker_id(&self) -> u64 {
        self.min_broker_id.load(Ordering::Acquire)
    }

    pub(crate) async fn min_broker_addr(&self) -> Option<CheetahString> {
        self.min_broker_addr.lock().await.clone()
    }

    pub(crate) async fn publish_min_broker(&self, broker_id: u64, broker_addr: Option<CheetahString>) {
        self.min_broker_id.store(broker_id, Ordering::Release);
        *self.min_broker_addr.lock().await = broker_addr;
    }

    pub(crate) async fn replace_min_broker(
        &self,
        broker_id: u64,
        broker_addr: Option<CheetahString>,
    ) -> (u64, Option<CheetahString>) {
        let previous_id = self.min_broker_id.swap(broker_id, Ordering::AcqRel);
        let mut address = self.min_broker_addr.lock().await;
        let previous_addr = std::mem::replace(&mut *address, broker_addr);
        (previous_id, previous_addr)
    }

    pub(crate) fn isolated_flag(&self) -> &AtomicBool {
        &self.isolated
    }

    pub(crate) fn is_isolated(&self) -> bool {
        self.isolated.load(Ordering::Acquire)
    }

    pub(crate) fn set_isolated(&self, isolated: bool) {
        self.isolated.store(isolated, Ordering::Release);
    }
}

#[derive(Clone)]
pub(crate) struct BrokerPreOnlinePolicy {
    broker_cluster_name: CheetahString,
    broker_name: CheetahString,
    broker_addr: CheetahString,
    ha_server_addr: CheetahString,
    compatible_with_old_name_srv: bool,
    sync_master_flush_offset_when_startup: bool,
    enable_split_registration: bool,
    split_registration_size: i32,
    broker_permission: u32,
    force_register: bool,
    register_broker_timeout_millis: u64,
    is_in_broker_container: bool,
}

impl BrokerPreOnlinePolicy {
    pub(crate) fn from_configs(
        broker_config: &BrokerConfig,
        message_store_config: &MessageStoreConfig,
        broker_addr: CheetahString,
        ha_server_addr: CheetahString,
    ) -> Self {
        Self {
            broker_cluster_name: broker_config.broker_identity.broker_cluster_name.clone(),
            broker_name: broker_config.broker_identity.broker_name.clone(),
            broker_addr,
            ha_server_addr,
            compatible_with_old_name_srv: broker_config.compatible_with_old_name_srv,
            sync_master_flush_offset_when_startup: message_store_config.sync_master_flush_offset_when_startup,
            enable_split_registration: broker_config.enable_split_registration,
            split_registration_size: broker_config.split_registration_size,
            broker_permission: broker_config.broker_permission,
            force_register: broker_config.force_register,
            register_broker_timeout_millis: broker_config.register_broker_timeout_mills as u64,
            is_in_broker_container: broker_config.is_in_broker_container,
        }
    }
}

pub(crate) struct BrokerPreOnlineStoreCapability<MS: MessageStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> BrokerPreOnlineStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
    }

    fn bridge(&self) -> RocketMQResult<Arc<EscapeBridge<MS>>> {
        self.escape_bridge
            .upgrade()
            .ok_or_else(|| pre_online_unavailable("message store"))
    }

    fn broker_init_max_offset(&self) -> RocketMQResult<i64> {
        self.bridge()?
            .pre_online_broker_init_max_offset()
            .map_err(|_| pre_online_unavailable("message store"))
    }

    fn master_flushed_offset(&self) -> RocketMQResult<i64> {
        self.bridge()?
            .pre_online_master_flushed_offset()
            .map_err(|_| pre_online_unavailable("message store"))
    }

    fn set_master_flushed_offset(&self, offset: i64) -> RocketMQResult<()> {
        self.bridge()?
            .pre_online_set_master_flushed_offset(offset)
            .map_err(|_| pre_online_unavailable("message store"))
    }

    async fn wait_for_ha_transfer(&self, broker_addr: &CheetahString) -> RocketMQResult<bool> {
        let (request, completion) = HAConnectionStateNotificationRequest::new(
            rocketmq_store::ha::ha_connection_state::HAConnectionState::Transfer,
            &RemotingHelper::parse_host_from_address(Some(broker_addr.as_str())),
            true,
        );
        let submitted = self
            .bridge()?
            .pre_online_submit_ha_transfer(request)
            .await
            .map_err(|_| pre_online_unavailable("HA service"))?;
        if !submitted {
            return Ok(false);
        }
        Ok(completion.await.unwrap_or(false))
    }

    async fn update_master_addresses(
        &self,
        master_ha_address: &CheetahString,
        master_address: &CheetahString,
    ) -> RocketMQResult<()> {
        self.bridge()?
            .pre_online_update_master_addresses(master_ha_address, master_address)
            .await
            .map_err(|_| pre_online_unavailable("message store"))
    }
}

impl<MS: MessageStore> Clone for BrokerPreOnlineStoreCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            escape_bridge: Weak::clone(&self.escape_bridge),
        }
    }
}

pub(crate) struct BrokerSpecialServiceCapability<MS: MessageStore> {
    schedule: Weak<ScheduleMessageService<MS>>,
    timer: Option<Weak<TimerMessageStore>>,
    transaction_check: Option<Weak<TransactionalMessageCheckService<MS>>>,
    ack: Option<Weak<AckMessageProcessor<MS>>>,
    plugins: Vec<Weak<dyn BrokerAttachedPlugin>>,
    schedule_started: Arc<AtomicBool>,
    transaction_check_started: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
}

impl<MS: MessageStore> BrokerSpecialServiceCapability<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "the composition root lists each role-sensitive service capability explicitly"
    )]
    pub(crate) fn new(
        schedule: &Arc<ScheduleMessageService<MS>>,
        timer: Option<&Arc<TimerMessageStore>>,
        transaction_check: Option<&Arc<TransactionalMessageCheckService<MS>>>,
        ack: Option<&Arc<AckMessageProcessor<MS>>>,
        plugins: &[Arc<dyn BrokerAttachedPlugin>],
        schedule_started: Arc<AtomicBool>,
        transaction_check_started: Arc<AtomicBool>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            schedule: Arc::downgrade(schedule),
            timer: timer.map(Arc::downgrade),
            transaction_check: transaction_check.map(Arc::downgrade),
            ack: ack.map(Arc::downgrade),
            plugins: plugins.iter().map(Arc::downgrade).collect(),
            schedule_started,
            transaction_check_started,
            shutdown,
        }
    }

    pub(crate) async fn change_status(&self, should_start: bool) -> RocketMQResult<()> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(pre_online_unavailable("broker lifecycle"));
        }

        if self.schedule_started.load(Ordering::Acquire) != should_start {
            let schedule = self
                .schedule
                .upgrade()
                .ok_or_else(|| pre_online_unavailable("schedule service"))?;
            if should_start {
                ScheduleMessageService::start(schedule).await?;
            } else {
                schedule.stop().await?;
            }
            if let Some(timer) = self.timer.as_ref().and_then(Weak::upgrade) {
                timer.sync_last_read_time_ms();
                timer.set_should_running_dequeue(should_start);
            }
            self.schedule_started.store(should_start, Ordering::Release);
        }

        for plugin in &self.plugins {
            plugin
                .upgrade()
                .ok_or_else(|| pre_online_unavailable("broker plugin"))?
                .status_changed(should_start);
        }

        if self.transaction_check_started.load(Ordering::Acquire) != should_start {
            if let Some(transaction_check) = self.transaction_check.as_ref() {
                let transaction_check = transaction_check
                    .upgrade()
                    .ok_or_else(|| pre_online_unavailable("transaction check service"))?;
                if should_start {
                    transaction_check.start().await?;
                } else {
                    transaction_check.shutdown_interrupt(true).await;
                }
            }
            self.transaction_check_started.store(should_start, Ordering::Release);
        }

        if let Some(ack) = self.ack.as_ref() {
            ack.upgrade()
                .ok_or_else(|| pre_online_unavailable("acknowledgment processor"))?
                .set_pop_revive_service_status(should_start);
        }
        Ok(())
    }
}

impl<MS: MessageStore> Clone for BrokerSpecialServiceCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            schedule: Weak::clone(&self.schedule),
            timer: self.timer.as_ref().map(Weak::clone),
            transaction_check: self.transaction_check.as_ref().map(Weak::clone),
            ack: self.ack.as_ref().map(Weak::clone),
            plugins: self.plugins.iter().map(Weak::clone).collect(),
            schedule_started: Arc::clone(&self.schedule_started),
            transaction_check_started: Arc::clone(&self.transaction_check_started),
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

pub(crate) struct BrokerRegistrationCapability {
    policy: BrokerPreOnlinePolicy,
    role_state: Arc<BrokerOnlineRoleState>,
    topic_config_manager: Weak<TopicConfigManager>,
    topic_config_coordinator: Weak<TopicConfigCoordinator>,
    topic_queue_mapping_manager: Weak<TopicQueueMappingManager>,
    broker_outer_api: BrokerOuterAPI,
    shutdown: Arc<AtomicBool>,
}

impl BrokerRegistrationCapability {
    pub(crate) fn new(
        policy: BrokerPreOnlinePolicy,
        role_state: Arc<BrokerOnlineRoleState>,
        topic_config_manager: &Arc<TopicConfigManager>,
        topic_config_coordinator: &Arc<TopicConfigCoordinator>,
        topic_queue_mapping_manager: &Arc<TopicQueueMappingManager>,
        broker_outer_api: BrokerOuterAPI,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            policy,
            role_state,
            topic_config_manager: Arc::downgrade(topic_config_manager),
            topic_config_coordinator: Arc::downgrade(topic_config_coordinator),
            topic_queue_mapping_manager: Arc::downgrade(topic_queue_mapping_manager),
            broker_outer_api,
            shutdown,
        }
    }

    pub(crate) async fn register(&self) -> RocketMQResult<()> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(pre_online_unavailable("broker lifecycle"));
        }
        let coordinator = self
            .topic_config_coordinator
            .upgrade()
            .ok_or_else(|| pre_online_unavailable("topic registration coordinator"))?;
        let topic_config_manager = self
            .topic_config_manager
            .upgrade()
            .ok_or_else(|| pre_online_unavailable("topic config manager"))?;
        let topic_queue_mapping_manager = self
            .topic_queue_mapping_manager
            .upgrade()
            .ok_or_else(|| pre_online_unavailable("topic queue mapping manager"))?;
        let capability = self.clone();
        let registration: TopicRegistrationAction = Box::new(move || {
            Box::pin(async move {
                capability
                    .register_snapshot(topic_config_manager, topic_queue_mapping_manager)
                    .await
            })
        });
        coordinator.persist_and_register_wait(registration).await
    }

    async fn register_snapshot(
        &self,
        topic_config_manager: Arc<TopicConfigManager>,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    ) -> RocketMQResult<()> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(pre_online_unavailable("broker lifecycle"));
        }
        let (raw_topic_config_table, split_data_version, final_data_version) = topic_config_manager
            .full_registration_snapshot(
                self.policy.enable_split_registration,
                self.policy.split_registration_size,
            );
        let mut topic_config_table = raw_topic_config_table
            .into_values()
            .map(|topic_config| {
                let topic_config = self.topic_config_for_registration(&topic_config);
                (
                    topic_config
                        .topic_name
                        .clone()
                        .expect("registered topic config requires topic_name"),
                    topic_config,
                )
            })
            .collect::<HashMap<_, _>>();

        if let Some(split_data_version) = split_data_version {
            let wrapper = topic_config_manager.build_serialize_wrapper(topic_config_table.clone(), split_data_version);
            self.register_wrapper(wrapper).await;
            topic_config_table.clear();
        }

        let topic_queue_mapping_info_map = topic_queue_mapping_manager
            .snapshot_topic_queue_mapping_table()
            .into_iter()
            .map(|(topic, detail)| (topic, TopicQueueMappingDetail::clone_as_mapping_info(&detail)))
            .collect();
        let wrapper = topic_config_manager.build_serialize_wrapper_with_topic_queue_map(
            topic_config_table,
            topic_queue_mapping_info_map,
            final_data_version,
        );
        let should_register = self.policy.enable_split_registration
            || self.policy.force_register
            || self
                .broker_outer_api
                .need_register(
                    self.policy.broker_cluster_name.clone(),
                    self.policy.broker_addr.clone(),
                    self.policy.broker_name.clone(),
                    self.role_state.local_broker_id(),
                    &wrapper,
                    self.policy.register_broker_timeout_millis,
                    self.policy.is_in_broker_container,
                )
                .await
                .iter()
                .any(|changed| *changed);
        if should_register {
            self.register_wrapper(wrapper).await;
        }
        Ok(())
    }

    fn topic_config_for_registration(&self, topic_config: &TopicConfig) -> TopicConfig {
        let mut topic_config = topic_config.clone();
        if !PermName::is_writeable(self.policy.broker_permission)
            || !PermName::is_readable(self.policy.broker_permission)
        {
            topic_config.perm &= self.policy.broker_permission;
        }
        topic_config
    }

    async fn register_wrapper(
        &self,
        wrapper: rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper,
    ) {
        self.broker_outer_api
            .register_broker_all(
                self.policy.broker_cluster_name.clone(),
                self.policy.broker_addr.clone(),
                self.policy.broker_name.clone(),
                self.role_state.local_broker_id(),
                self.policy.broker_addr.clone(),
                wrapper,
                vec![],
                false,
                10_000,
                false,
                false,
                None,
                Default::default(),
            )
            .await;
    }
}

impl Clone for BrokerRegistrationCapability {
    fn clone(&self) -> Self {
        Self {
            policy: self.policy.clone(),
            role_state: Arc::clone(&self.role_state),
            topic_config_manager: Weak::clone(&self.topic_config_manager),
            topic_config_coordinator: Weak::clone(&self.topic_config_coordinator),
            topic_queue_mapping_manager: Weak::clone(&self.topic_queue_mapping_manager),
            broker_outer_api: self.broker_outer_api.clone(),
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

pub(crate) struct BrokerOnlineTransitionCapability<MS: MessageStore> {
    policy: BrokerPreOnlinePolicy,
    role_state: Arc<BrokerOnlineRoleState>,
    special_services: BrokerSpecialServiceCapability<MS>,
    registration: BrokerRegistrationCapability,
    shutdown: Arc<AtomicBool>,
}

impl<MS: MessageStore> BrokerOnlineTransitionCapability<MS> {
    pub(crate) fn new(
        policy: BrokerPreOnlinePolicy,
        role_state: Arc<BrokerOnlineRoleState>,
        special_services: BrokerSpecialServiceCapability<MS>,
        registration: BrokerRegistrationCapability,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            policy,
            role_state,
            special_services,
            registration,
            shutdown,
        }
    }

    async fn start_service(&self, min_broker_id: u64, min_broker_addr: Option<CheetahString>) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        info!(
            broker = %self.policy.broker_name,
            local_broker_id = self.role_state.local_broker_id(),
            min_broker_id,
            ?min_broker_addr,
            "start broker online transition"
        );
        self.role_state.publish_min_broker(min_broker_id, min_broker_addr).await;
        let should_start = self.role_state.local_broker_id() == min_broker_id;
        if let Err(error) = self.special_services.change_status(should_start).await {
            error!(
                ?error,
                "failed to change role-sensitive services during broker online transition"
            );
            return false;
        }
        if let Err(error) = self.registration.register().await {
            error!(?error, "failed to register broker during online transition");
            return false;
        }
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        self.role_state.set_isolated(false);
        true
    }
}

pub(crate) struct BrokerPreOnlineContext<MS: MessageStore> {
    policy: BrokerPreOnlinePolicy,
    role_state: Arc<BrokerOnlineRoleState>,
    broker_outer_api: BrokerOuterAPI,
    store: BrokerPreOnlineStoreCapability<MS>,
    consumer_offsets: Weak<ConsumerOffsetManager<MS>>,
    schedule: Weak<ScheduleMessageService<MS>>,
    timer: Option<Weak<TimerMessageStore>>,
    plugins: Vec<Weak<dyn BrokerAttachedPlugin>>,
    transition: BrokerOnlineTransitionCapability<MS>,
}

impl<MS: MessageStore> BrokerPreOnlineContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "the composition root lists the complete broker pre-online capability boundary"
    )]
    pub(crate) fn new(
        policy: BrokerPreOnlinePolicy,
        role_state: Arc<BrokerOnlineRoleState>,
        broker_outer_api: BrokerOuterAPI,
        store: BrokerPreOnlineStoreCapability<MS>,
        consumer_offsets: &Arc<ConsumerOffsetManager<MS>>,
        schedule: &Arc<ScheduleMessageService<MS>>,
        timer: Option<&Arc<TimerMessageStore>>,
        plugins: &[Arc<dyn BrokerAttachedPlugin>],
        transition: BrokerOnlineTransitionCapability<MS>,
    ) -> Self {
        Self {
            policy,
            role_state,
            broker_outer_api,
            store,
            consumer_offsets: Arc::downgrade(consumer_offsets),
            schedule: Arc::downgrade(schedule),
            timer: timer.map(Arc::downgrade),
            plugins: plugins.iter().map(Arc::downgrade).collect(),
            transition,
        }
    }
}

fn pre_online_unavailable(component: &'static str) -> RocketMQError {
    RocketMQError::broker_operation_failed(
        "broker_pre_online",
        -1,
        format!("{component} is unavailable during broker pre-online"),
    )
}

pub(crate) mod service;
