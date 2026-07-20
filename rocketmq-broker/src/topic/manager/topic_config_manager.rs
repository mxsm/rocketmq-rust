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
use std::net::SocketAddr;
#[cfg(feature = "rocksdb_store")]
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::attribute::attribute_util::AttributeUtil;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::common::TopicSysFlag;
use rocketmq_common::utils::file_utils;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::TopicAttributes::TopicAttributes;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::data_version_facade::DataVersionExt;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::timer::timer_message_store;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_path_config_helper::get_topic_config_path;
#[cfg(feature = "rocksdb_store")]
use crate::config::rocksdb_manager::RocksDbBrokerConfigManager;

pub(crate) struct TopicConfigManager {
    topic_config_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>>,
    topic_config_snapshot: ArcSwap<HashMap<CheetahString, Arc<TopicConfig>>>,
    metadata_transition: parking_lot::Mutex<DataVersion>,
    topic_registration_lock: tokio::sync::Mutex<()>,
    persist_lock: Arc<parking_lot::Mutex<()>>,
    async_topic_create_pending_count: Arc<AtomicU64>,
    async_topic_create_spawn_failure_count: Arc<AtomicU64>,
    config_file_path: String,
    real_time_persist_rocksdb_config: AtomicBool,
    #[cfg(feature = "rocksdb_store")]
    rocksdb_config_manager: Option<Arc<RocksDbBrokerConfigManager>>,
}

#[derive(Clone)]
pub(crate) struct TopicConfigUpdate {
    pub(crate) topic_config: Arc<TopicConfig>,
    pub(crate) data_version: DataVersion,
}

pub(crate) struct TopicConfigCreation {
    pub(crate) topic_config: Arc<TopicConfig>,
    pub(crate) update: Option<TopicConfigUpdate>,
    pub(crate) register: bool,
    pub(crate) created: bool,
}

pub(crate) struct TopicConfigAsyncPersistGuard {
    pending_count: Arc<AtomicU64>,
}

impl Drop for TopicConfigAsyncPersistGuard {
    fn drop(&mut self) {
        self.pending_count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl TopicConfigManager {
    #[inline]
    pub(crate) fn record_topic_create_latency(start_time: Instant) {
        if let Some(metrics) = crate::metrics::broker_metrics_manager::BrokerMetricsManager::try_global() {
            metrics.record_topic_create_time(start_time.elapsed().as_millis().min(u64::MAX as u128) as u64);
        }
    }

    const SCHEDULE_TOPIC_QUEUE_NUM: u32 = 18;

    pub fn new(broker_config: &BrokerConfig, message_store_config: &MessageStoreConfig, init: bool) -> Self {
        let mut manager = Self {
            topic_config_table: Arc::new(DashMap::with_capacity(1024)),
            topic_config_snapshot: ArcSwap::from_pointee(HashMap::new()),
            metadata_transition: parking_lot::Mutex::new(DataVersion::default()),
            topic_registration_lock: tokio::sync::Mutex::new(()),
            persist_lock: Arc::new(parking_lot::Mutex::new(())),
            async_topic_create_pending_count: Arc::new(AtomicU64::new(0)),
            async_topic_create_spawn_failure_count: Arc::new(AtomicU64::new(0)),
            config_file_path: get_topic_config_path(broker_config.store_path_root_dir.as_str()),
            real_time_persist_rocksdb_config: AtomicBool::new(message_store_config.real_time_persist_rocksdb_config),
            #[cfg(feature = "rocksdb_store")]
            rocksdb_config_manager: None,
        };
        if init {
            manager.init(broker_config, message_store_config);
        }
        manager
    }

    #[cfg(feature = "rocksdb_store")]
    pub(crate) fn new_with_rocksdb_config_manager(
        broker_config: &BrokerConfig,
        message_store_config: &MessageStoreConfig,
        init: bool,
        rocksdb_config_manager: Arc<RocksDbBrokerConfigManager>,
    ) -> Self {
        let mut manager = Self::new(broker_config, message_store_config, init);
        manager.rocksdb_config_manager = Some(rocksdb_config_manager);
        manager
    }

    #[cfg(feature = "rocksdb_store")]
    pub(crate) fn is_rocksdb_config_enabled(&self) -> bool {
        self.rocksdb_config_manager.is_some()
    }

    #[cfg(feature = "rocksdb_store")]
    pub(crate) fn rocksdb_config_path(&self) -> Option<&Path> {
        self.rocksdb_config_manager.as_ref().map(|manager| manager.path())
    }

    #[cfg(feature = "rocksdb_store")]
    pub(crate) fn export_to_json(&self) -> Result<(), rocketmq_error::RocketMQError> {
        let json = self.encode_pretty(true);
        if json.is_empty() {
            return Ok(());
        }
        file_utils::string_to_file(json.as_str(), self.config_file_path().as_str()).map_err(|error| {
            rocketmq_error::RocketMQError::storage_write_failed(
                "rocksdb-topic-config",
                format!("export topic config to json failed: {error}"),
            )
        })
    }

    fn init(&mut self, broker_config: &BrokerConfig, message_store_config: &MessageStoreConfig) {
        //SELF_TEST_TOPIC
        {
            self.put_topic_config(TopicConfig::with_queues(TopicValidator::RMQ_SYS_SELF_TEST_TOPIC, 1, 1));
        }

        //auto create topic setting
        {
            if broker_config.auto_create_topic_enable {
                let default_topic_queue_nums = broker_config.topic_queue_config.default_topic_queue_nums;
                self.put_topic_config(TopicConfig::with_perm(
                    TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
                    default_topic_queue_nums,
                    default_topic_queue_nums,
                    PermName::PERM_INHERIT | PermName::PERM_READ | PermName::PERM_WRITE,
                ));
            }
        }
        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_BENCHMARK_TOPIC,
                1024,
                1024,
            ));
        }
        {
            let topic = broker_config.broker_identity.broker_cluster_name.to_string();
            TopicValidator::add_system_topic(topic.as_str());
            let mut config = TopicConfig::new(topic);
            let mut perm = PermName::PERM_INHERIT;
            if broker_config.cluster_topic_enable {
                perm |= PermName::PERM_READ | PermName::PERM_WRITE;
            }
            config.perm = perm;
            self.put_topic_config(config);
        }

        {
            let topic = broker_config.broker_identity.broker_name.to_string();
            TopicValidator::add_system_topic(topic.as_str());
            let mut config = TopicConfig::new(topic);
            config.write_queue_nums = 1;
            config.read_queue_nums = 1;
            let mut perm = PermName::PERM_INHERIT;
            if broker_config.broker_topic_enable {
                perm |= PermName::PERM_READ | PermName::PERM_WRITE;
            }
            config.perm = perm;
            self.put_topic_config(config);
        }

        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT,
                1,
                1,
            ));
        }

        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_SCHEDULE_TOPIC,
                Self::SCHEDULE_TOPIC_QUEUE_NUM,
                Self::SCHEDULE_TOPIC_QUEUE_NUM,
            ));
        }

        {
            if broker_config.trace_topic_enable {
                let topic = broker_config.msg_trace_topic_name.clone();
                TopicValidator::add_system_topic(topic.as_str());
                self.put_topic_config(TopicConfig::with_queues(topic, 1, 1));
            }
        }

        {
            let topic = format!(
                "{}_{}",
                broker_config.broker_identity.broker_name,
                mix_all::REPLY_TOPIC_POSTFIX
            );
            TopicValidator::add_system_topic(topic.as_str());
            self.put_topic_config(TopicConfig::with_queues(topic, 1, 1));
        }

        {
            let topic =
                PopAckConstants::build_cluster_revive_topic(broker_config.broker_identity.broker_cluster_name.as_str());
            TopicValidator::add_system_topic(topic.as_str());
            self.put_topic_config(TopicConfig::with_queues(
                topic,
                broker_config.revive_queue_num,
                broker_config.revive_queue_num,
            ));
        }

        {
            let topic = format!(
                "{}_{}",
                TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX,
                broker_config.broker_identity.broker_name,
            );
            TopicValidator::add_system_topic(topic.as_str());
            self.put_topic_config(TopicConfig::with_perm(topic, 1, 1, PermName::PERM_INHERIT));
        }

        {
            self.put_topic_config(TopicConfig::with_queues(TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC, 1, 1));
        }

        {
            self.put_topic_config(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC,
                1,
                1,
            ));
        }

        {
            if message_store_config.timer_wheel_enable {
                TopicValidator::add_system_topic(timer_message_store::TIMER_TOPIC);
                self.put_topic_config(TopicConfig::with_queues(timer_message_store::TIMER_TOPIC, 1, 1));
            }
        }
    }

    #[inline]
    pub fn select_topic_config(&self, topic: &CheetahString) -> Option<Arc<TopicConfig>> {
        self.topic_config_snapshot.load().get(topic).cloned()
    }

    fn rebuild_topic_config_snapshot_locked(&self) {
        let snapshot = self
            .topic_config_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<HashMap<CheetahString, Arc<TopicConfig>>>();
        self.topic_config_snapshot.store(Arc::new(snapshot));
    }

    pub(crate) fn metadata_snapshot(&self) -> (HashMap<CheetahString, TopicConfig>, DataVersion) {
        let data_version = self.metadata_transition.lock();
        let topic_config_table = self
            .topic_config_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().as_ref().clone()))
            .collect();
        (topic_config_table, data_version.clone())
    }

    fn topic_metadata_snapshot(&self, topic_name: &str) -> Option<(Arc<TopicConfig>, DataVersion)> {
        let data_version = self.metadata_transition.lock();
        let topic_config = self
            .topic_config_table
            .get(topic_name)
            .map(|entry| entry.value().clone())?;
        Some((topic_config, data_version.clone()))
    }

    pub(crate) fn topic_registration_snapshot(
        &self,
        topic_names: &[CheetahString],
    ) -> (Vec<Arc<TopicConfig>>, DataVersion) {
        let data_version = self.metadata_transition.lock();
        let topic_configs = topic_names
            .iter()
            .filter_map(|topic_name| {
                self.topic_config_table
                    .get(topic_name)
                    .map(|entry| entry.value().clone())
            })
            .collect();
        (topic_configs, data_version.clone())
    }

    pub(crate) fn full_registration_snapshot(
        &self,
        split_registration: bool,
        split_registration_size: i32,
    ) -> (HashMap<CheetahString, TopicConfig>, Option<DataVersion>, DataVersion) {
        let mut data_version = self.metadata_transition.lock();
        let topic_config_table = self
            .topic_config_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().as_ref().clone()))
            .collect::<HashMap<_, _>>();
        let split_data_version = if split_registration && topic_config_table.len() as i32 >= split_registration_size {
            data_version.next_version();
            Some(data_version.clone())
        } else {
            None
        };
        if split_registration {
            data_version.next_version();
        }
        (topic_config_table, split_data_version, data_version.clone())
    }

    pub(crate) fn async_topic_create_pending_count(&self) -> u64 {
        self.async_topic_create_pending_count.load(Ordering::Acquire)
    }

    pub(crate) fn async_topic_create_spawn_failure_count(&self) -> u64 {
        self.async_topic_create_spawn_failure_count.load(Ordering::Acquire)
    }

    pub(crate) fn update_message_store_policy(&self, message_store_config: &MessageStoreConfig) {
        self.real_time_persist_rocksdb_config
            .store(message_store_config.real_time_persist_rocksdb_config, Ordering::Release);
    }

    pub(crate) fn close(&self) {
        #[cfg(feature = "rocksdb_store")]
        if let Some(rocksdb_config_manager) = &self.rocksdb_config_manager {
            rocksdb_config_manager.close();
        }
    }

    pub fn build_serialize_wrapper(
        &self,
        topic_config_table: HashMap<CheetahString, TopicConfig>,
        data_version: DataVersion,
    ) -> TopicConfigAndMappingSerializeWrapper {
        self.build_serialize_wrapper_with_topic_queue_map(topic_config_table, HashMap::new(), data_version)
    }

    pub fn build_serialize_wrapper_with_topic_queue_map(
        &self,
        topic_config_table: HashMap<CheetahString, TopicConfig>,
        topic_queue_mapping_info_map: HashMap<CheetahString, TopicQueueMappingInfo>,
        data_version: DataVersion,
    ) -> TopicConfigAndMappingSerializeWrapper {
        TopicConfigAndMappingSerializeWrapper {
            topic_config_serialize_wrapper: rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper {
                topic_config_table,
                data_version,
            },
            topic_queue_mapping_info_map,
            ..TopicConfigAndMappingSerializeWrapper::default()
        }
    }

    #[inline]
    pub fn is_order_topic(&self, topic: &str) -> bool {
        match self.get_topic_config(topic) {
            None => false,
            Some(value) => value.order,
        }
    }

    pub fn get_topic_config(&self, topic_name: &str) -> Option<Arc<TopicConfig>> {
        self.topic_config_table.get(topic_name).as_deref().cloned()
    }

    pub(crate) fn put_topic_config(&self, topic_config: TopicConfig) -> Option<Arc<TopicConfig>> {
        let Some(topic_name) = topic_config.topic_name.clone() else {
            error!("refuse to publish topic config without a topic name");
            return None;
        };
        let _transition = self.metadata_transition.lock();
        let old = self.topic_config_table.insert(topic_name, Arc::new(topic_config));
        self.rebuild_topic_config_snapshot_locked();
        old
    }

    pub fn create_topic_in_send_message_method(
        &self,
        topic: &CheetahString,
        default_topic: &CheetahString,
        remote_address: SocketAddr,
        client_default_topic_queue_nums: i32,
        topic_sys_flag: u32,
        state_machine_version: i64,
        auto_create_topic_enable: bool,
    ) -> Option<TopicConfigCreation> {
        if let Some(config) = self.get_topic_config(topic.as_str()) {
            return Some(TopicConfigCreation {
                topic_config: config,
                update: None,
                register: false,
                created: false,
            });
        }

        let update = {
            let mut data_version = self.metadata_transition.lock();
            if let Some(config) = self.topic_config_table.get(topic).map(|entry| entry.value().clone()) {
                return Some(TopicConfigCreation {
                    topic_config: config,
                    update: None,
                    register: false,
                    created: false,
                });
            }

            // Clone the default generation before writing the same DashMap so no shard guard is re-entered.
            let mut default_topic_config = match self
                .topic_config_table
                .get(default_topic)
                .map(|entry| entry.value().as_ref().clone())
            {
                Some(config) => config,
                None => {
                    warn!(
                        "Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                        default_topic, remote_address
                    );
                    return None;
                }
            };

            if default_topic == TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC && !auto_create_topic_enable {
                default_topic_config.perm = PermName::PERM_READ | PermName::PERM_WRITE;
            }

            if !PermName::is_inherited(default_topic_config.perm) {
                warn!(
                    "Create new topic failed, because the default topic[{}] has no perm [{}] producer:[{}]",
                    default_topic, default_topic_config.perm, remote_address
                );
                return None;
            }

            let mut topic_config = TopicConfig::new(topic);
            let queue_nums = client_default_topic_queue_nums
                .min(default_topic_config.write_queue_nums as i32)
                .max(0);
            topic_config.write_queue_nums = queue_nums as u32;
            topic_config.read_queue_nums = queue_nums as u32;
            topic_config.perm = default_topic_config.perm & !PermName::PERM_INHERIT;
            topic_config.topic_sys_flag = topic_sys_flag;
            topic_config.topic_filter_type = default_topic_config.topic_filter_type;

            info!(
                "Create new topic by default topic:[{}] config:[{:?}] producer:[{}]",
                default_topic, topic_config, remote_address
            );

            let topic_config = Arc::new(topic_config);
            self.topic_config_table.insert(topic.clone(), topic_config.clone());
            data_version.next_version_with(state_machine_version);
            self.rebuild_topic_config_snapshot_locked();
            TopicConfigUpdate {
                topic_config,
                data_version: data_version.clone(),
            }
        };

        Some(TopicConfigCreation {
            topic_config: update.topic_config.clone(),
            update: Some(update),
            register: true,
            created: true,
        })
    }

    pub fn create_topic_in_send_message_back_method(
        &self,
        topic: &CheetahString,
        client_default_topic_queue_nums: i32,
        perm: u32,
        is_order: bool,
        topic_sys_flag: u32,
        state_machine_version: i64,
    ) -> Option<TopicConfigCreation> {
        if let Some(config) = self.get_topic_config(topic) {
            if is_order != config.order {
                if let Some(update) = self.update_existing_topic(topic, state_machine_version, |replacement| {
                    replacement.order = is_order;
                }) {
                    return Some(TopicConfigCreation {
                        topic_config: update.topic_config.clone(),
                        update: Some(update),
                        register: false,
                        created: false,
                    });
                }
            } else {
                return Some(TopicConfigCreation {
                    topic_config: config,
                    update: None,
                    register: false,
                    created: false,
                });
            }
        }

        let update = {
            let mut data_version = self.metadata_transition.lock();
            if let Some(config) = self.topic_config_table.get(topic).map(|entry| entry.value().clone()) {
                return Some(TopicConfigCreation {
                    topic_config: config,
                    update: None,
                    register: false,
                    created: false,
                });
            }

            let mut config = TopicConfig::new(topic);
            config.read_queue_nums = client_default_topic_queue_nums as u32;
            config.write_queue_nums = client_default_topic_queue_nums as u32;
            config.perm = perm;
            config.topic_sys_flag = topic_sys_flag;
            config.order = is_order;

            let topic_config = Arc::new(config);
            self.topic_config_table.insert(topic.clone(), topic_config.clone());
            data_version.next_version_with(state_machine_version);
            self.rebuild_topic_config_snapshot_locked();
            TopicConfigUpdate {
                topic_config,
                data_version: data_version.clone(),
            }
        };

        Some(TopicConfigCreation {
            topic_config: update.topic_config.clone(),
            update: Some(update),
            register: true,
            created: true,
        })
    }

    pub(crate) fn begin_async_topic_create_persist(&self) -> TopicConfigAsyncPersistGuard {
        self.async_topic_create_pending_count.fetch_add(1, Ordering::AcqRel);
        TopicConfigAsyncPersistGuard {
            pending_count: Arc::clone(&self.async_topic_create_pending_count),
        }
    }

    pub(crate) fn record_async_topic_create_spawn_failure(&self) {
        self.async_topic_create_spawn_failure_count
            .fetch_add(1, Ordering::AcqRel);
    }

    pub fn update_topic_config_list(
        &self,
        topic_config_list: &mut [TopicConfig],
        state_machine_version: i64,
    ) -> (Vec<Arc<TopicConfig>>, DataVersion) {
        let (published, data_version) = {
            let mut data_version = self.metadata_transition.lock();
            let mut published = Vec::with_capacity(topic_config_list.len());
            for topic_config in topic_config_list {
                self.apply_topic_attributes_locked(topic_config);
                let topic_name = topic_config
                    .topic_name
                    .clone()
                    .expect("TopicConfigManager batch updates require topic_name");
                let generation = Arc::new(topic_config.clone());
                self.topic_config_table.insert(topic_name, generation.clone());
                data_version.next_version_with(state_machine_version);
                published.push(generation);
            }
            self.rebuild_topic_config_snapshot_locked();
            (published, data_version.clone())
        };
        self.persist();
        (published, data_version)
    }

    pub fn delete_topic_config(&self, topic: &CheetahString, state_machine_version: i64) {
        let old = {
            let mut data_version = self.metadata_transition.lock();
            let old = self.topic_config_table.remove(topic).map(|(_, value)| value);
            if old.is_some() {
                data_version.next_version_with(state_machine_version);
                self.rebuild_topic_config_snapshot_locked();
            }
            old
        };
        if let Some(old) = old {
            info!("delete topic config OK, topic: {:?}", old);
            #[cfg(feature = "rocksdb_store")]
            if let Some(rocksdb_config_manager) = &self.rocksdb_config_manager {
                if let Err(error) = rocksdb_config_manager.delete(topic.as_str()) {
                    error!(
                        "delete topic config from rocksdb failed, topic={}, error={}",
                        topic, error
                    );
                }
            }
            self.persist();
        } else {
            warn!("delete topic config failed, topic: {} not exists", topic);
        }
    }

    pub fn update_topic_config(&self, mut topic_config: TopicConfig, state_machine_version: i64) -> TopicConfigUpdate {
        let start_time = Instant::now();
        let topic_name = topic_config
            .topic_name
            .clone()
            .expect("TopicConfigManager updates require topic_name");
        let (update, old) = {
            let mut data_version = self.metadata_transition.lock();
            self.apply_topic_attributes_locked(&mut topic_config);
            let topic_config = Arc::new(topic_config);
            let old = self.topic_config_table.insert(topic_name.clone(), topic_config.clone());
            data_version.next_version_with(state_machine_version);
            self.rebuild_topic_config_snapshot_locked();
            (
                TopicConfigUpdate {
                    topic_config,
                    data_version: data_version.clone(),
                },
                old,
            )
        };
        match old {
            None => {
                info!("create new topic [{:?}]", update.topic_config)
            }
            Some(ref old) => {
                info!("update topic config, old:[{:?}] new:[{:?}]", old, update.topic_config);
            }
        }
        self.persist_topic_config(topic_name.as_str());
        if old.is_none() {
            Self::record_topic_create_latency(start_time);
        }
        update
    }

    fn apply_topic_attributes_locked(&self, topic_config: &mut TopicConfig) {
        let topic_name = topic_config
            .topic_name
            .as_ref()
            .expect("TopicConfigManager attribute updates require topic_name");
        let new_attributes = Self::request(topic_config);
        let current = self
            .topic_config_table
            .get(topic_name)
            .map(|config| config.value().clone());
        let create = current.is_none();
        let current_attributes = current.map_or_else(HashMap::new, |config| config.attributes.clone());
        match AttributeUtil::alter_current_attributes(
            create,
            TopicAttributes::all(),
            &current_attributes,
            &new_attributes,
        ) {
            Ok(final_attributes) => topic_config.attributes = final_attributes,
            Err(error) => {
                error!("Failed to alter current attributes: {:?}", error);
                topic_config.attributes = Default::default();
            }
        }
    }

    fn update_existing_topic(
        &self,
        topic: &str,
        state_machine_version: i64,
        update: impl FnOnce(&mut TopicConfig),
    ) -> Option<TopicConfigUpdate> {
        let mut data_version = self.metadata_transition.lock();
        let current = self
            .topic_config_table
            .get(topic)
            .map(|entry| entry.value().as_ref().clone())?;
        let mut replacement = current;
        update(&mut replacement);
        let topic_config = Arc::new(replacement);
        self.topic_config_table
            .insert(CheetahString::from_slice(topic), topic_config.clone());
        data_version.next_version_with(state_machine_version);
        self.rebuild_topic_config_snapshot_locked();
        Some(TopicConfigUpdate {
            topic_config,
            data_version: data_version.clone(),
        })
    }

    fn request(topic_config: &TopicConfig) -> HashMap<CheetahString, CheetahString> {
        topic_config.attributes.clone()
    }

    pub fn topic_config_table(&self) -> Arc<DashMap<CheetahString, Arc<TopicConfig>>> {
        self.topic_config_table.clone()
    }

    pub fn topic_config_table_hash_map(&self) -> HashMap<CheetahString, TopicConfig> {
        self.metadata_snapshot().0
    }

    pub fn create_topic_of_tran_check_max_time(
        &self,
        client_default_topic_queue_nums: i32,
        perm: u32,
        state_machine_version: i64,
    ) -> Option<TopicConfigCreation> {
        if let Some(config) = self
            .topic_config_table
            .get(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC)
        {
            return Some(TopicConfigCreation {
                topic_config: config.value().clone(),
                update: None,
                register: false,
                created: false,
            });
        }

        let update = {
            let mut data_version = self.metadata_transition.lock();
            let topic_key = CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
            if let Some(config) = self
                .topic_config_table
                .get(&topic_key)
                .map(|entry| entry.value().clone())
            {
                return Some(TopicConfigCreation {
                    topic_config: config,
                    update: None,
                    register: false,
                    created: false,
                });
            }

            let mut config = TopicConfig::new(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
            config.read_queue_nums = client_default_topic_queue_nums as u32;
            config.write_queue_nums = client_default_topic_queue_nums as u32;
            config.perm = perm;
            config.topic_sys_flag = 0;
            info!("create new topic {:?}", config);
            let topic_config = Arc::new(config);
            self.topic_config_table.insert(topic_key, topic_config.clone());
            data_version.next_version_with(state_machine_version);
            self.rebuild_topic_config_snapshot_locked();
            TopicConfigUpdate {
                topic_config,
                data_version: data_version.clone(),
            }
        };

        Some(TopicConfigCreation {
            topic_config: update.topic_config.clone(),
            update: Some(update),
            register: true,
            created: true,
        })
    }

    pub fn contains_topic(&self, topic: &CheetahString) -> bool {
        self.topic_config_table.contains_key(topic)
    }

    pub fn data_version(&self) -> DataVersion {
        self.metadata_transition.lock().clone()
    }

    fn assign_data_version(&self, data_version: &DataVersion) {
        self.metadata_transition.lock().assign_new_one(data_version);
    }

    pub(crate) async fn topic_registration_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.topic_registration_lock.lock().await
    }

    fn replace_topic_config_table(
        &self,
        topic_config_table: HashMap<CheetahString, TopicConfig>,
        data_version: &DataVersion,
    ) {
        let mut current_version = self.metadata_transition.lock();
        self.install_topic_config_table_locked(topic_config_table);
        current_version.assign_new_one(data_version);
        self.rebuild_topic_config_snapshot_locked();
    }

    fn install_topic_config_table_locked(&self, topic_config_table: HashMap<CheetahString, TopicConfig>) {
        self.topic_config_table.clear();
        for (topic, config) in topic_config_table {
            self.topic_config_table.insert(topic, Arc::new(config));
        }
    }

    pub(crate) fn replace_topic_config_table_from_master(
        &self,
        topic_config_table: HashMap<CheetahString, TopicConfig>,
        data_version: &DataVersion,
    ) -> bool {
        let mut current_version = self.metadata_transition.lock();
        if *current_version == *data_version {
            return false;
        }
        self.install_topic_config_table_locked(topic_config_table);
        current_version.assign_new_one(data_version);
        self.rebuild_topic_config_snapshot_locked();
        true
    }

    pub fn update_order_topic_config(&self, order_kv_table_from_ns: &KVTable, state_machine_version: i64) {
        if !order_kv_table_from_ns.table.is_empty() {
            let is_change = {
                let mut data_version = self.metadata_transition.lock();
                let mut is_change = false;
                for topic in order_kv_table_from_ns.table.keys() {
                    if let Some(current) = self
                        .topic_config_table
                        .get(topic)
                        .map(|entry| entry.value().as_ref().clone())
                    {
                        if !current.order {
                            let mut replacement = current;
                            replacement.order = true;
                            self.topic_config_table.insert(topic.clone(), Arc::new(replacement));
                            is_change = true;
                            info!("update order topic config, topic={}, order=true", topic);
                        }
                    }
                }
                if is_change {
                    data_version.next_version_with(state_machine_version);
                    self.rebuild_topic_config_snapshot_locked();
                }
                is_change
            };
            if is_change {
                self.persist();
            }
        }
    }

    pub fn create_topic_if_absent(
        &self,
        mut topic_config: TopicConfig,
        register: bool,
        state_machine_version: i64,
    ) -> Option<TopicConfigCreation> {
        if topic_config.topic_name.is_none() {
            error!("createTopicIfAbsent: TopicName cannot be None");
            return None;
        }

        let topic_name = topic_config.topic_name.as_ref().unwrap().clone();

        if let Some(config) = self.get_topic_config(topic_name.as_str()) {
            return Some(TopicConfigCreation {
                topic_config: config,
                update: None,
                register: false,
                created: false,
            });
        }

        let update = {
            let mut data_version = self.metadata_transition.lock();
            if let Some(config) = self
                .topic_config_table
                .get(&topic_name)
                .map(|entry| entry.value().clone())
            {
                return Some(TopicConfigCreation {
                    topic_config: config,
                    update: None,
                    register: false,
                    created: false,
                });
            }

            info!("Create new topic [{}] config:[{:?}]", topic_name, topic_config);
            topic_config.topic_name = Some(topic_name.clone());
            let topic_config = Arc::new(topic_config);
            self.topic_config_table.insert(topic_name, topic_config.clone());
            data_version.next_version_with(state_machine_version);
            self.rebuild_topic_config_snapshot_locked();
            TopicConfigUpdate {
                topic_config,
                data_version: data_version.clone(),
            }
        };

        Some(TopicConfigCreation {
            topic_config: update.topic_config.clone(),
            update: Some(update),
            register,
            created: true,
        })
    }

    pub fn update_topic_unit_flag(
        &self,
        topic: &str,
        unit: bool,
        state_machine_version: i64,
    ) -> Option<TopicConfigUpdate> {
        let update = self.update_existing_topic(topic, state_machine_version, |topic_config| {
            let old_topic_sys_flag = topic_config.topic_sys_flag;
            topic_config.topic_sys_flag = if unit {
                TopicSysFlag::set_unit_flag(old_topic_sys_flag)
            } else {
                TopicSysFlag::clear_unit_flag(old_topic_sys_flag)
            };
            info!(
                "update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag={}",
                old_topic_sys_flag, topic_config.topic_sys_flag
            );
        });
        if update.is_some() {
            self.persist();
        }
        update
    }

    pub fn update_topic_unit_sub_flag(
        &self,
        topic: &str,
        has_unit_sub: bool,
        state_machine_version: i64,
    ) -> Option<TopicConfigUpdate> {
        let update = self.update_existing_topic(topic, state_machine_version, |topic_config| {
            let old_topic_sys_flag = topic_config.topic_sys_flag;
            topic_config.topic_sys_flag = if has_unit_sub {
                TopicSysFlag::set_unit_sub_flag(old_topic_sys_flag)
            } else {
                TopicSysFlag::clear_unit_sub_flag(old_topic_sys_flag)
            };
            info!(
                "update topic sys flag. oldTopicSysFlag={}, newTopicSysFlag={}",
                old_topic_sys_flag, topic_config.topic_sys_flag
            );
        });
        if update.is_some() {
            self.persist();
        }
        update
    }

    pub fn load_data_version(&self) -> bool {
        let file_path = self.config_file_path();
        match file_utils::file_to_string(&file_path) {
            Ok(json_string) => {
                if let Ok(wrapper) = TopicConfigSerializeWrapper::decode_string(json_string) {
                    if let Some(data_version) = wrapper.data_version() {
                        self.assign_data_version(data_version);
                        info!(
                            "load topic metadata dataVersion success {}, {:?}",
                            file_path, data_version
                        );
                        return true;
                    }
                }
                false
            }
            Err(e) => {
                error!("load topic metadata dataVersion failed {}: {:?}", file_path, e);
                false
            }
        }
    }

    #[cfg(feature = "rocksdb_store")]
    fn load_from_rocksdb_or_migrate_from_file(&self) -> bool {
        let Some(rocksdb_config_manager) = &self.rocksdb_config_manager else {
            return false;
        };
        match rocksdb_config_manager.default_cf_is_empty() {
            Ok(false) => self.load_from_rocksdb(),
            Ok(true) => self.migrate_config_file_to_rocksdb(),
            Err(error) => {
                error!("check topic rocksdb config records failed: {}", error);
                false
            }
        }
    }

    #[cfg(feature = "rocksdb_store")]
    fn migrate_config_file_to_rocksdb(&self) -> bool {
        if !self.config_file_or_backup_exists() {
            return true;
        }
        if !self.load_from_config_file() {
            return false;
        }
        if let Err(error) = self.persist_all_topics_to_rocksdb() {
            error!("migrate topic config file to rocksdb failed: {}", error);
            return false;
        }
        true
    }

    #[cfg(feature = "rocksdb_store")]
    fn config_file_or_backup_exists(&self) -> bool {
        let file_name = self.config_file_path();
        Path::new(&file_name).exists() || Path::new(format!("{file_name}.bak").as_str()).exists()
    }

    #[cfg(feature = "rocksdb_store")]
    fn load_from_rocksdb(&self) -> bool {
        let Some(rocksdb_config_manager) = &self.rocksdb_config_manager else {
            return false;
        };

        let data_version = match rocksdb_config_manager.load_data_version() {
            Ok(Some(data_version)) => data_version,
            Ok(None) => DataVersion::default(),
            Err(error) => {
                error!("load topic rocksdb dataVersion failed: {}", error);
                return false;
            }
        };

        let records = match rocksdb_config_manager.load_data() {
            Ok(records) => records,
            Err(error) => {
                error!("load topic config from rocksdb failed: {}", error);
                return false;
            }
        };
        let mut topic_config_table = HashMap::with_capacity(records.len());
        for (key, body) in records {
            match self.decode_topic_config_record(&key, &body) {
                Ok((topic, config)) => {
                    topic_config_table.insert(topic, config);
                }
                Err(error) => {
                    error!("decode topic config from rocksdb failed: {}", error);
                    return false;
                }
            }
        }
        self.replace_topic_config_table(topic_config_table, &data_version);
        true
    }

    #[cfg(feature = "rocksdb_store")]
    fn decode_topic_config_record(
        &self,
        key: &[u8],
        body: &[u8],
    ) -> Result<(CheetahString, TopicConfig), rocketmq_error::RocketMQError> {
        let topic_name = String::from_utf8(key.to_vec()).map_err(|error| {
            rocketmq_error::RocketMQError::deserialization_failed(
                "rocksdb-topic-config",
                format!("topic key utf8 decode failed: {error}"),
            )
        })?;
        let mut topic_config = serde_json::from_slice::<TopicConfig>(body).map_err(|error| {
            rocketmq_error::RocketMQError::deserialization_failed(
                "rocksdb-topic-config",
                format!("topic config decode failed: {error}"),
            )
        })?;
        if topic_config.topic_name.is_none() {
            topic_config.topic_name = Some(CheetahString::from_string(topic_name.clone()));
        }
        Ok((CheetahString::from_string(topic_name), topic_config))
    }

    #[cfg(feature = "rocksdb_store")]
    fn persist_topic_to_rocksdb(&self, topic_name: &str) -> Result<(), rocketmq_error::RocketMQError> {
        let Some(rocksdb_config_manager) = &self.rocksdb_config_manager else {
            return Ok(());
        };
        let Some((topic_config, data_version)) = self.topic_metadata_snapshot(topic_name) else {
            rocksdb_config_manager.delete(topic_name)?;
            rocksdb_config_manager.set_kv_data_version(self.data_version())?;
            return self.flush_rocksdb_config_if_needed(rocksdb_config_manager);
        };
        let body = serde_json::to_string(topic_config.as_ref()).map_err(|error| {
            rocketmq_error::RocketMQError::storage_write_failed(
                "rocksdb-topic-config",
                format!("topic config encode failed: {error}"),
            )
        })?;
        rocksdb_config_manager.put_string(topic_name, &body)?;
        rocksdb_config_manager.set_kv_data_version(data_version)?;
        self.flush_rocksdb_config_if_needed(rocksdb_config_manager)
    }

    #[cfg(feature = "rocksdb_store")]
    fn persist_all_topics_to_rocksdb(&self) -> Result<(), rocketmq_error::RocketMQError> {
        let Some(rocksdb_config_manager) = &self.rocksdb_config_manager else {
            return Ok(());
        };
        let (topic_config_table, data_version) = self.metadata_snapshot();
        let records = topic_config_table
            .iter()
            .map(|(topic, config)| serde_json::to_vec(config).map(|body| (topic.as_bytes().to_vec(), body)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                rocketmq_error::RocketMQError::storage_write_failed(
                    "rocksdb-topic-config",
                    format!("topic config encode failed: {error}"),
                )
            })?;
        rocksdb_config_manager.batch_put_with_wal(&records)?;
        rocksdb_config_manager.set_kv_data_version(data_version)?;
        self.flush_rocksdb_config_if_needed(rocksdb_config_manager)
    }

    #[cfg(feature = "rocksdb_store")]
    fn flush_rocksdb_config_if_needed(
        &self,
        rocksdb_config_manager: &RocksDbBrokerConfigManager,
    ) -> Result<(), rocketmq_error::RocketMQError> {
        if self.real_time_persist_rocksdb_config.load(Ordering::Acquire) {
            rocksdb_config_manager.flush_wal()?;
        }
        Ok(())
    }

    fn persist_topic_config(&self, topic_name: &str) {
        let _persist = self.persist_lock.lock();
        #[cfg(not(feature = "rocksdb_store"))]
        let _ = topic_name;
        #[cfg(feature = "rocksdb_store")]
        if self.rocksdb_config_manager.is_some() {
            if let Err(error) = self.persist_topic_to_rocksdb(topic_name) {
                error!(
                    "persist topic config to rocksdb failed, topic={}, error={}",
                    topic_name, error
                );
            }
            return;
        }
        self.persist_unlocked();
    }

    fn persist_unlocked(&self) {
        #[cfg(feature = "rocksdb_store")]
        if self.rocksdb_config_manager.is_some() {
            if let Err(error) = self.persist_all_topics_to_rocksdb() {
                error!("persist topic configs to rocksdb failed: {}", error);
            }
            return;
        }

        let json = self.encode_pretty(true);
        if !json.is_empty() {
            let file_name = self.config_file_path();
            if file_utils::string_to_file(json.as_str(), file_name.as_str()).is_err() {
                error!("persist file {} exception", file_name);
            }
        }
    }

    fn load_from_config_file(&self) -> bool {
        let file_name = self.config_file_path();
        match file_utils::file_to_string(file_name.as_str()) {
            Ok(content) if content.is_empty() => self.load_from_backup_config_file(file_name.as_str()),
            Ok(content) => {
                self.decode(&content);
                info!("load Config file: {} -----OK", file_name);
                true
            }
            Err(_) => self.load_from_backup_config_file(file_name.as_str()),
        }
    }

    fn load_from_backup_config_file(&self, file_name: &str) -> bool {
        let backup_file_name = format!("{file_name}.bak");
        match file_utils::file_to_string(backup_file_name.as_str()) {
            Ok(content) if !content.is_empty() => {
                self.decode(&content);
                info!("load Config file: {} -----OK", backup_file_name);
                true
            }
            Ok(_) => true,
            Err(error) => {
                error!("load Config file: {} -----Failed: {:?}", backup_file_name, error);
                false
            }
        }
    }
}

impl ConfigManager for TopicConfigManager {
    fn load(&self) -> bool {
        #[cfg(feature = "rocksdb_store")]
        if self.rocksdb_config_manager.is_some() {
            return self.load_from_rocksdb_or_migrate_from_file();
        }
        self.load_from_config_file()
    }

    fn persist_with_topic(&mut self, topic_name: &str, _t: Box<dyn std::any::Any>) {
        self.persist_topic_config(topic_name)
    }

    fn persist(&self) {
        let _persist = self.persist_lock.lock();
        self.persist_unlocked();
    }

    fn stop(&mut self) -> bool {
        #[cfg(feature = "rocksdb_store")]
        if let Some(rocksdb_config_manager) = &self.rocksdb_config_manager {
            rocksdb_config_manager.close();
        }
        true
    }

    fn config_file_path(&self) -> String {
        self.config_file_path.clone()
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        let (topic_config_table, version) = self.metadata_snapshot();
        match pretty_format {
            true => TopicConfigSerializeWrapper::new(Some(topic_config_table), Some(version))
                .serialize_json_pretty()
                .expect("Encode TopicConfigSerializeWrapper to json failed"),
            false => TopicConfigSerializeWrapper::new(Some(topic_config_table), Some(version))
                .serialize_json()
                .expect("Encode TopicConfigSerializeWrapper to json failed"),
        }
    }

    fn decode(&self, json_string: &str) {
        info!("decode topic config from json string:{}", json_string);
        if json_string.is_empty() {
            return;
        }
        let mut wrapper = SerdeJsonUtils::from_json_str::<TopicConfigSerializeWrapper>(json_string)
            .expect("Decode TopicConfigSerializeWrapper from json failed");
        let data_version = wrapper.data_version().cloned().unwrap_or_default();
        if let Some(map) = wrapper.take_topic_config_table() {
            self.replace_topic_config_table(map, &data_version);
        } else {
            self.assign_data_version(&data_version);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Barrier;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::config_manager::ConfigManager;
    use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
    use rocketmq_remoting::protocol::DataVersion;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use tempfile::TempDir;

    use crate::topic::manager::topic_config_manager::TopicConfigManager;

    fn test_topic_config_manager() -> (TempDir, TopicConfigManager) {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let root = CheetahString::from_string(temp_dir.path().to_string_lossy().to_string());
        let broker_config = BrokerConfig {
            store_path_root_dir: root.clone(),
            ..BrokerConfig::default()
        };
        let message_store_config = MessageStoreConfig {
            store_path_root_dir: root,
            ..MessageStoreConfig::default()
        };
        let manager = TopicConfigManager::new(&broker_config, &message_store_config, false);
        (temp_dir, manager)
    }

    #[test]
    fn serialize_wrapper_preserves_owned_topic_queue_mapping_info() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let topic = CheetahString::from_static_str("OwnedMappingTopic");
        let mapping = TopicQueueMappingInfo::new(topic.clone(), 8, CheetahString::from_static_str("broker-a"), 7);
        let wrapper = manager.build_serialize_wrapper_with_topic_queue_map(
            HashMap::new(),
            HashMap::from([(topic.clone(), mapping.clone())]),
            DataVersion::default(),
        );

        assert_eq!(wrapper.topic_queue_mapping_info_map.get(&topic), Some(&mapping));
    }

    #[test]
    fn select_topic_config_reads_snapshot_after_put_and_delete() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let topic = CheetahString::from_static_str("SnapshotTopic");

        assert!(manager.select_topic_config(&topic).is_none());

        manager.put_topic_config(TopicConfig::with_queues(topic.clone(), 2, 3));
        let config = manager
            .select_topic_config(&topic)
            .expect("topic should be visible through snapshot");
        assert_eq!(config.read_queue_nums, 2);
        assert_eq!(config.write_queue_nums, 3);

        manager.delete_topic_config(&topic, 0);
        assert!(manager.select_topic_config(&topic).is_none());
    }

    #[test]
    fn select_topic_config_reads_snapshot_after_table_replacement() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let topic = CheetahString::from_static_str("ReplacementTopic");
        manager.replace_topic_config_table(
            HashMap::from([(topic.clone(), TopicConfig::with_queues(topic.clone(), 4, 5))]),
            &DataVersion::default(),
        );
        let config = manager
            .select_topic_config(&topic)
            .expect("replacement table should rebuild snapshot");
        assert_eq!(config.read_queue_nums, 4);
        assert_eq!(config.write_queue_nums, 5);

        manager.replace_topic_config_table(HashMap::new(), &DataVersion::default());
        assert!(manager.select_topic_config(&topic).is_none());
    }

    #[test]
    fn decode_rebuilds_topic_config_snapshot() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let topic = CheetahString::from_static_str("LoadedTopic");
        manager.put_topic_config(TopicConfig::with_queues(topic.clone(), 6, 7));
        let encoded = manager.encode_pretty(false);

        let (_restart_temp_dir, restarted_manager) = test_topic_config_manager();
        restarted_manager.decode(&encoded);

        let config = restarted_manager
            .select_topic_config(&topic)
            .expect("decoded topic should be visible through snapshot");
        assert_eq!(config.read_queue_nums, 6);
        assert_eq!(config.write_queue_nums, 7);
    }

    #[test]
    fn async_topic_create_runtime_counters_start_empty() {
        let (_temp_dir, manager) = test_topic_config_manager();

        assert_eq!(manager.async_topic_create_pending_count(), 0);
        assert_eq!(manager.async_topic_create_spawn_failure_count(), 0);
    }

    #[test]
    fn async_topic_create_pending_guard_releases_on_drop() {
        let (_temp_dir, manager) = test_topic_config_manager();

        let pending = manager.begin_async_topic_create_persist();
        assert_eq!(manager.async_topic_create_pending_count(), 1);
        drop(pending);
        assert_eq!(manager.async_topic_create_pending_count(), 0);
    }

    #[test]
    fn source_has_no_runtime_or_arc_mut_owner_back_reference() {
        let source = include_str!("topic_config_manager.rs");

        for forbidden in [
            ["Arc", "Mut"].concat(),
            ["Broker", "Runtime", "Inner"].concat(),
            ["broker", "_runtime", "_inner"].concat(),
            ["TopicConfigManager", "<"].concat(),
        ] {
            assert!(!source.contains(&forbidden), "forbidden owner carrier: {forbidden}");
        }
    }

    #[test]
    fn update_publishes_immutable_generation_with_matching_version() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let topic = CheetahString::from_static_str("GenerationTopic");

        let first = manager.update_topic_config(TopicConfig::with_queues(topic.clone(), 2, 3), 0);
        let first_reader = manager
            .select_topic_config(&topic)
            .expect("first generation should be visible");
        assert!(Arc::ptr_eq(&first.topic_config, &first_reader));

        let second = manager.update_topic_config(TopicConfig::with_queues(topic.clone(), 5, 7), 0);
        let second_reader = manager
            .select_topic_config(&topic)
            .expect("second generation should be visible");

        assert_eq!(first_reader.read_queue_nums, 2);
        assert_eq!(first_reader.write_queue_nums, 3);
        assert_eq!(second_reader.read_queue_nums, 5);
        assert_eq!(second_reader.write_queue_nums, 7);
        assert!(!Arc::ptr_eq(&first_reader, &second_reader));
        assert!(Arc::ptr_eq(&second.topic_config, &second_reader));
        assert_eq!(manager.data_version(), second.data_version);
        assert_eq!(second.data_version.counter(), 2);
    }

    #[test]
    fn concurrent_updates_publish_complete_snapshot_and_serialize_persistence() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let manager = Arc::new(manager);
        let barrier = Arc::new(Barrier::new(3));

        std::thread::scope(|scope| {
            for topic in ["ConcurrentTopicA", "ConcurrentTopicB"] {
                let manager = Arc::clone(&manager);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    manager.update_topic_config(TopicConfig::with_queues(topic, 1, 1), 0);
                });
            }
            barrier.wait();
        });

        assert!(manager
            .select_topic_config(&CheetahString::from_static_str("ConcurrentTopicA"))
            .is_some());
        assert!(manager
            .select_topic_config(&CheetahString::from_static_str("ConcurrentTopicB"))
            .is_some());
        assert_eq!(manager.data_version().counter(), 2);
    }

    #[test]
    fn master_replacement_removes_stale_topics_and_assigns_exact_version() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let stale_topic = CheetahString::from_static_str("StaleTopic");
        let current_topic = CheetahString::from_static_str("CurrentTopic");
        manager.put_topic_config(TopicConfig::with_queues(stale_topic.clone(), 1, 1));
        let remote_version = DataVersion::with_values(17, 23, 5);
        let remote_table = HashMap::from([(
            current_topic.clone(),
            TopicConfig::with_queues(current_topic.clone(), 4, 6),
        )]);

        assert!(manager.replace_topic_config_table_from_master(remote_table, &remote_version));
        assert!(manager.select_topic_config(&stale_topic).is_none());
        let current = manager
            .select_topic_config(&current_topic)
            .expect("remote generation should be visible");
        assert_eq!(current.read_queue_nums, 4);
        assert_eq!(current.write_queue_nums, 6);
        assert_eq!(manager.data_version(), remote_version);
        assert!(!manager.replace_topic_config_table_from_master(HashMap::new(), &remote_version));
        assert!(manager.select_topic_config(&current_topic).is_some());
    }

    #[test]
    fn split_registration_reserves_versions_with_the_captured_table() {
        let (_temp_dir, manager) = test_topic_config_manager();
        manager.put_topic_config(TopicConfig::with_queues("SplitTopic", 1, 1));

        let (table, split_version, final_version) = manager.full_registration_snapshot(true, 1);

        assert_eq!(table.len(), 1);
        assert_eq!(split_version.expect("split version should be reserved").counter(), 1);
        assert_eq!(final_version.counter(), 2);
        assert_eq!(manager.data_version(), final_version);
    }

    #[tokio::test]
    async fn stale_registration_trigger_resamples_current_generation_after_send_guard() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let topic = CheetahString::from_static_str("RegistrationTopic");
        let stale = manager.update_topic_config(TopicConfig::with_queues(topic.clone(), 1, 1), 0);
        let current = manager.update_topic_config(TopicConfig::with_queues(topic.clone(), 7, 9), 0);

        let _registration = manager.topic_registration_guard().await;
        let (configs, data_version) = manager.topic_registration_snapshot(std::slice::from_ref(&topic));

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].read_queue_nums, 7);
        assert_eq!(configs[0].write_queue_nums, 9);
        assert!(!Arc::ptr_eq(&configs[0], &stale.topic_config));
        assert!(Arc::ptr_eq(&configs[0], &current.topic_config));
        assert_eq!(data_version, current.data_version);
    }
}

#[cfg(all(test, feature = "rocksdb_store"))]
mod rocksdb_config_tests {
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::config_manager::ConfigManager;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use tempfile::TempDir;

    use crate::config::rocksdb_manager::RocksDbBrokerConfigManager;
    use crate::config::rocksdb_manager::RocksDbBrokerConfigManagerConfig;
    use crate::topic::manager::topic_config_manager::TopicConfigManager;

    #[tokio::test]
    async fn topic_config_manager_persists_single_topic_to_rocksdb_and_loads_after_restart() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let (broker_config, message_store_config) = test_configs(&temp_dir);
        let rocksdb_path = temp_dir.path().join("config").join("topics");
        let rocksdb_manager = Arc::new(
            RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(rocksdb_path.clone()))
                .expect("rocksdb config manager should open"),
        );
        let topic_manager = TopicConfigManager::new_with_rocksdb_config_manager(
            &broker_config,
            &message_store_config,
            false,
            rocksdb_manager,
        );
        topic_manager.update_topic_config(TopicConfig::with_queues("TopicA", 4, 5), 0);
        drop(topic_manager);

        let restarted_rocksdb_manager = Arc::new(
            RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(rocksdb_path))
                .expect("rocksdb config manager should reopen"),
        );
        let restarted_manager = TopicConfigManager::new_with_rocksdb_config_manager(
            &broker_config,
            &message_store_config,
            false,
            restarted_rocksdb_manager,
        );

        assert!(restarted_manager.load());
        let loaded = restarted_manager
            .get_topic_config("TopicA")
            .expect("topic config should load from rocksdb");
        assert_eq!(loaded.read_queue_nums, 4);
        assert_eq!(loaded.write_queue_nums, 5);
        assert_eq!(restarted_manager.data_version().counter(), 1);
    }

    #[tokio::test]
    async fn topic_config_manager_delete_removes_topic_from_rocksdb() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let (broker_config, message_store_config) = test_configs(&temp_dir);
        let rocksdb_path = temp_dir.path().join("config").join("topics-delete");
        let rocksdb_manager = Arc::new(
            RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(rocksdb_path.clone()))
                .expect("rocksdb config manager should open"),
        );
        let topic_manager = TopicConfigManager::new_with_rocksdb_config_manager(
            &broker_config,
            &message_store_config,
            false,
            rocksdb_manager,
        );
        let topic_name = CheetahString::from_static_str("TopicDelete");
        topic_manager.update_topic_config(TopicConfig::with_queues(topic_name.clone(), 1, 1), 0);
        topic_manager.delete_topic_config(&topic_name, 0);
        drop(topic_manager);

        let restarted_rocksdb_manager = Arc::new(
            RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(rocksdb_path))
                .expect("rocksdb config manager should reopen"),
        );
        let restarted_manager = TopicConfigManager::new_with_rocksdb_config_manager(
            &broker_config,
            &message_store_config,
            false,
            restarted_rocksdb_manager,
        );

        assert!(restarted_manager.load());
        assert!(restarted_manager.get_topic_config(topic_name.as_str()).is_none());
    }

    fn test_configs(temp_dir: &TempDir) -> (BrokerConfig, MessageStoreConfig) {
        let root = CheetahString::from_string(temp_dir.path().to_string_lossy().to_string());
        (
            BrokerConfig {
                store_path_root_dir: root.clone(),
                ..BrokerConfig::default()
            },
            MessageStoreConfig {
                store_path_root_dir: root,
                real_time_persist_rocksdb_config: true,
                ..MessageStoreConfig::default()
            },
        )
    }
}
