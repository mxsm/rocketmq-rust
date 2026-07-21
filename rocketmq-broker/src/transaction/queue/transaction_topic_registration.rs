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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_store::base::message_store::MessageStore;
use tracing::debug;
use tracing::info;

use crate::broker_runtime::complete_topic_config_creation;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::slave::slave_synchronize::SlaveMasterAddress;
use crate::topic::manager::topic_config_coordinator::TopicConfigCoordinator;
use crate::topic::manager::topic_config_manager::TopicConfigCreation;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_config_manager::TopicConfigUpdate;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;
use crate::transaction::queue::transaction_message_store::TransactionMessageStore;

const TCMT_QUEUE_NUMS: i32 = 1;

pub(crate) struct TransactionTopicRegistration<MS: MessageStore> {
    broker_config: Arc<BrokerConfig>,
    topic_config_manager: Arc<TopicConfigManager>,
    topic_config_coordinator: Arc<TopicConfigCoordinator>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    broker_outer_api: BrokerOuterAPI,
    message_store: TransactionMessageStore<MS>,
    slave_master_addr: Option<Arc<SlaveMasterAddress>>,
    update_master_haserver_addr_periodically: bool,
    shutdown: Arc<AtomicBool>,
}

pub(crate) struct TransactionTopicRegistrationContext<MS: MessageStore> {
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) topic_config_manager: Arc<TopicConfigManager>,
    pub(crate) topic_config_coordinator: Arc<TopicConfigCoordinator>,
    pub(crate) topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    pub(crate) broker_outer_api: BrokerOuterAPI,
    pub(crate) message_store: TransactionMessageStore<MS>,
    pub(crate) slave_master_addr: Option<Arc<SlaveMasterAddress>>,
    pub(crate) update_master_haserver_addr_periodically: bool,
    pub(crate) shutdown: Arc<AtomicBool>,
}

impl<MS> TransactionTopicRegistration<MS>
where
    MS: MessageStore + Send + Sync + 'static,
{
    pub(crate) fn new(context: TransactionTopicRegistrationContext<MS>) -> Self {
        Self {
            broker_config: context.broker_config,
            topic_config_manager: context.topic_config_manager,
            topic_config_coordinator: context.topic_config_coordinator,
            topic_queue_mapping_manager: context.topic_queue_mapping_manager,
            broker_outer_api: context.broker_outer_api,
            message_store: context.message_store,
            slave_master_addr: context.slave_master_addr,
            update_master_haserver_addr_periodically: context.update_master_haserver_addr_periodically,
            shutdown: context.shutdown,
        }
    }

    pub(crate) async fn select_or_create_send_back_topic(
        self: &Arc<Self>,
        topic: &CheetahString,
    ) -> Option<Arc<TopicConfig>> {
        if let Some(topic_config) = self.topic_config_manager.select_topic_config(topic) {
            return Some(topic_config);
        }
        self.select_or_create_send_back_topic_with(topic, 1, false, 0).await
    }

    pub(crate) async fn select_or_create_send_back_topic_with(
        self: &Arc<Self>,
        topic: &CheetahString,
        queue_nums: i32,
        is_order: bool,
        topic_sys_flag: u32,
    ) -> Option<Arc<TopicConfig>> {
        let start_time = Instant::now();
        let state_machine_version = self.message_store.state_machine_version()?;
        let creation = self.topic_config_manager.create_topic_in_send_message_back_method(
            topic,
            queue_nums,
            PermName::PERM_WRITE | PermName::PERM_READ,
            is_order,
            topic_sys_flag,
            state_machine_version,
        )?;
        Some(self.complete_creation(creation, start_time).await)
    }

    pub(crate) async fn select_or_create_check_max_time_topic(self: &Arc<Self>) -> Option<Arc<TopicConfig>> {
        let start_time = Instant::now();
        let state_machine_version = self.message_store.state_machine_version()?;
        let creation = self.topic_config_manager.create_topic_of_tran_check_max_time(
            TCMT_QUEUE_NUMS,
            PermName::PERM_READ | PermName::PERM_WRITE,
            state_machine_version,
        )?;
        Some(self.complete_creation(creation, start_time).await)
    }

    async fn complete_creation(
        self: &Arc<Self>,
        creation: TopicConfigCreation,
        start_time: Instant,
    ) -> Arc<TopicConfig> {
        let registration = Arc::clone(self);
        complete_topic_config_creation(
            Arc::clone(&self.topic_config_coordinator),
            creation,
            start_time,
            self.broker_config.async_topic_create_persist_enable,
            move |update| {
                let registration = Arc::clone(&registration);
                async move { registration.register_update(update).await }
            },
        )
        .await
    }

    async fn register_update(&self, update: TopicConfigUpdate) {
        if self.broker_config.enable_single_topic_register {
            self.register_single_topic(update.topic_config).await;
        } else {
            self.register_incremental_topic(update).await;
        }
    }

    fn topic_config_for_registration(&self, topic_config: &TopicConfig) -> TopicConfig {
        let mut topic_config = topic_config.clone();
        if !PermName::is_writeable(self.broker_config.broker_permission)
            || !PermName::is_readable(self.broker_config.broker_permission)
        {
            topic_config.perm &= self.broker_config.broker_permission;
        }
        topic_config
    }

    async fn register_single_topic(&self, topic_config: Arc<TopicConfig>) {
        let Some(topic_name) = topic_config.topic_name.clone() else {
            return;
        };
        let (mut current_configs, _) = self
            .topic_config_manager
            .topic_registration_snapshot(std::slice::from_ref(&topic_name));
        let Some(current) = current_configs.pop() else {
            info!(topic = %topic_name, "Skip stale transaction topic registration after topic removal");
            return;
        };
        self.broker_outer_api
            .register_single_topic_all(
                self.broker_config.broker_identity.broker_name.clone(),
                self.topic_config_for_registration(current.as_ref()),
                3000,
            )
            .await;
    }

    async fn register_incremental_topic(&self, update: TopicConfigUpdate) {
        if self.shutdown.load(Ordering::Acquire) {
            info!("Skip transaction topic registration after broker shutdown");
            return;
        }
        let Some(topic_name) = update.topic_config.topic_name.clone() else {
            return;
        };
        let (topic_config_list, current_data_version) = self
            .topic_config_manager
            .topic_registration_snapshot(std::slice::from_ref(&topic_name));
        if current_data_version != update.data_version {
            debug!(
                requested = ?update.data_version,
                current = ?current_data_version,
                "Resample transaction topic registration after a newer metadata commit"
            );
        }
        let mut topic_config_table = HashMap::new();
        let mut topic_queue_mapping_info_map = HashMap::new();
        for topic_config in &topic_config_list {
            let register_topic_config = self.topic_config_for_registration(topic_config.as_ref());
            if let Some(topic_name) = register_topic_config.topic_name.clone() {
                topic_config_table.insert(topic_name.clone(), register_topic_config);
                if let Some(mapping) = self
                    .topic_queue_mapping_manager
                    .get_topic_queue_mapping(topic_name.as_str())
                {
                    topic_queue_mapping_info_map.insert(
                        topic_name,
                        TopicQueueMappingDetail::clone_as_mapping_info(mapping.as_ref()),
                    );
                }
            }
        }
        let wrapper = TopicConfigAndMappingSerializeWrapper {
            topic_config_serialize_wrapper: TopicConfigSerializeWrapper {
                data_version: current_data_version,
                topic_config_table,
            },
            topic_queue_mapping_info_map,
            ..Default::default()
        };
        let broker_addr = CheetahString::from_string(format!(
            "{}:{}",
            self.broker_config.broker_ip1, self.broker_config.broker_server_config.listen_port
        ));
        let results = self
            .broker_outer_api
            .register_broker_all(
                self.broker_config.broker_identity.broker_cluster_name.clone(),
                broker_addr.clone(),
                self.broker_config.broker_identity.broker_name.clone(),
                self.broker_config.broker_identity.broker_id,
                broker_addr,
                wrapper,
                vec![],
                false,
                self.broker_config.register_broker_timeout_mills as u64,
                self.broker_config.enable_slave_acting_master,
                self.broker_config.compressed_register,
                self.broker_config
                    .enable_slave_acting_master
                    .then_some(self.broker_config.broker_not_active_timeout_millis),
                Default::default(),
            )
            .await;
        self.handle_register_result(results).await;
    }

    async fn handle_register_result(
        &self,
        register_broker_result: Vec<rocketmq_remoting::protocol::namesrv::RegisterBrokerResult>,
    ) {
        let Some(result) = register_broker_result.into_iter().next() else {
            return;
        };
        if self.update_master_haserver_addr_periodically {
            self.message_store.update_master_address(&result.master_addr).await;
        }
        if let Some(master_addr) = &self.slave_master_addr {
            master_addr.store(Some(&result.master_addr));
        }
        if let Some(state_machine_version) = self.message_store.state_machine_version() {
            self.topic_config_manager
                .update_order_topic_config(&result.kv_table, state_machine_version);
        }
    }
}
