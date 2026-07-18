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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::attribute::attribute_util::AttributeUtil;
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
use rocketmq_runtime::TaskKind;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::timer::timer_message_store;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_path_config_helper::get_topic_config_path;
use crate::broker_runtime::BrokerRuntimeInner;
#[cfg(feature = "rocksdb_store")]
use crate::config::rocksdb_manager::RocksDbBrokerConfigManager;

pub(crate) struct TopicConfigManager<MS: MessageStore> {
    topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>,
    topic_config_snapshot: ArcSwap<HashMap<CheetahString, ArcMut<TopicConfig>>>,
    data_version: ArcMut<DataVersion>,
    persist_lock: Arc<parking_lot::Mutex<()>>,
    async_topic_create_pending_count: Arc<AtomicU64>,
    async_topic_create_spawn_failure_count: Arc<AtomicU64>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    #[cfg(feature = "rocksdb_store")]
    rocksdb_config_manager: Option<Arc<RocksDbBrokerConfigManager>>,
}

impl<MS: MessageStore> TopicConfigManager<MS> {
    #[inline]
    fn record_topic_create_latency(start_time: Instant) {
        if let Some(metrics) = crate::metrics::broker_metrics_manager::BrokerMetricsManager::try_global() {
            metrics.record_topic_create_time(start_time.elapsed().as_millis().min(u64::MAX as u128) as u64);
        }
    }

    const SCHEDULE_TOPIC_QUEUE_NUM: u32 = 18;

    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>, init: bool) -> Self {
        let mut manager = Self {
            topic_config_table: Arc::new(DashMap::with_capacity(1024)),
            topic_config_snapshot: ArcSwap::from_pointee(HashMap::new()),
            data_version: ArcMut::new(DataVersion::default()),
            persist_lock: Arc::new(parking_lot::Mutex::new(())),
            async_topic_create_pending_count: Arc::new(AtomicU64::new(0)),
            async_topic_create_spawn_failure_count: Arc::new(AtomicU64::new(0)),
            broker_runtime_inner,
            #[cfg(feature = "rocksdb_store")]
            rocksdb_config_manager: None,
        };
        if init {
            manager.init();
        }
        manager
    }

    #[cfg(feature = "rocksdb_store")]
    pub(crate) fn new_with_rocksdb_config_manager(
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
        init: bool,
        rocksdb_config_manager: Arc<RocksDbBrokerConfigManager>,
    ) -> Self {
        let mut manager = Self::new(broker_runtime_inner, init);
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

    fn init(&mut self) {
        //SELF_TEST_TOPIC
        {
            self.put_topic_config(ArcMut::new(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_SELF_TEST_TOPIC,
                1,
                1,
            )));
        }

        //auto create topic setting
        {
            if self.broker_runtime_inner.broker_config().auto_create_topic_enable {
                let default_topic_queue_nums = self
                    .broker_runtime_inner
                    .broker_config()
                    .topic_queue_config
                    .default_topic_queue_nums;
                self.put_topic_config(ArcMut::new(TopicConfig::with_perm(
                    TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
                    default_topic_queue_nums,
                    default_topic_queue_nums,
                    PermName::PERM_INHERIT | PermName::PERM_READ | PermName::PERM_WRITE,
                )));
            }
        }
        {
            self.put_topic_config(ArcMut::new(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_BENCHMARK_TOPIC,
                1024,
                1024,
            )));
        }
        {
            let topic = self
                .broker_runtime_inner
                .broker_config()
                .broker_identity
                .broker_cluster_name
                .to_string();
            TopicValidator::add_system_topic(topic.as_str());
            let mut config = TopicConfig::new(topic);
            let mut perm = PermName::PERM_INHERIT;
            if self.broker_runtime_inner.broker_config().cluster_topic_enable {
                perm |= PermName::PERM_READ | PermName::PERM_WRITE;
            }
            config.perm = perm;
            self.put_topic_config(ArcMut::new(config));
        }

        {
            let topic = self
                .broker_runtime_inner
                .broker_config()
                .broker_identity
                .broker_name
                .to_string();
            TopicValidator::add_system_topic(topic.as_str());
            let mut config = TopicConfig::new(topic);
            config.write_queue_nums = 1;
            config.read_queue_nums = 1;
            let mut perm = PermName::PERM_INHERIT;
            if self.broker_runtime_inner.broker_config().broker_topic_enable {
                perm |= PermName::PERM_READ | PermName::PERM_WRITE;
            }
            config.perm = perm;
            self.put_topic_config(ArcMut::new(config));
        }

        {
            self.put_topic_config(ArcMut::new(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_OFFSET_MOVED_EVENT,
                1,
                1,
            )));
        }

        {
            self.put_topic_config(ArcMut::new(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_SCHEDULE_TOPIC,
                Self::SCHEDULE_TOPIC_QUEUE_NUM,
                Self::SCHEDULE_TOPIC_QUEUE_NUM,
            )));
        }

        {
            if self.broker_runtime_inner.broker_config().trace_topic_enable {
                let topic = self.broker_runtime_inner.broker_config().msg_trace_topic_name.clone();
                TopicValidator::add_system_topic(topic.as_str());
                self.put_topic_config(ArcMut::new(TopicConfig::with_queues(topic, 1, 1)));
            }
        }

        {
            let topic = format!(
                "{}_{}",
                self.broker_runtime_inner.broker_config().broker_identity.broker_name,
                mix_all::REPLY_TOPIC_POSTFIX
            );
            TopicValidator::add_system_topic(topic.as_str());
            self.put_topic_config(ArcMut::new(TopicConfig::with_queues(topic, 1, 1)));
        }

        {
            let topic = PopAckConstants::build_cluster_revive_topic(
                self.broker_runtime_inner
                    .broker_config()
                    .broker_identity
                    .broker_cluster_name
                    .as_str(),
            );
            TopicValidator::add_system_topic(topic.as_str());
            self.put_topic_config(ArcMut::new(TopicConfig::with_queues(
                topic,
                self.broker_runtime_inner.broker_config().revive_queue_num,
                self.broker_runtime_inner.broker_config().revive_queue_num,
            )));
        }

        {
            let topic = format!(
                "{}_{}",
                TopicValidator::SYNC_BROKER_MEMBER_GROUP_PREFIX,
                self.broker_runtime_inner.broker_config().broker_identity.broker_name,
            );
            TopicValidator::add_system_topic(topic.as_str());
            self.put_topic_config(ArcMut::new(TopicConfig::with_perm(topic, 1, 1, PermName::PERM_INHERIT)));
        }

        {
            self.put_topic_config(ArcMut::new(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC,
                1,
                1,
            )));
        }

        {
            self.put_topic_config(ArcMut::new(TopicConfig::with_queues(
                TopicValidator::RMQ_SYS_TRANS_OP_HALF_TOPIC,
                1,
                1,
            )));
        }

        {
            if self.broker_runtime_inner.message_store_config().timer_wheel_enable {
                TopicValidator::add_system_topic(timer_message_store::TIMER_TOPIC);
                self.put_topic_config(ArcMut::new(TopicConfig::with_queues(
                    timer_message_store::TIMER_TOPIC,
                    1,
                    1,
                )));
            }
        }
    }

    #[inline]
    pub fn select_topic_config(&self, topic: &CheetahString) -> Option<ArcMut<TopicConfig>> {
        self.topic_config_snapshot.load().get(topic).cloned()
    }

    pub(crate) fn rebuild_topic_config_snapshot(&self) {
        let snapshot = self
            .topic_config_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<HashMap<CheetahString, ArcMut<TopicConfig>>>();
        self.topic_config_snapshot.store(Arc::new(snapshot));
    }

    pub(crate) fn async_topic_create_pending_count(&self) -> u64 {
        self.async_topic_create_pending_count.load(Ordering::Acquire)
    }

    pub(crate) fn async_topic_create_spawn_failure_count(&self) -> u64 {
        self.async_topic_create_spawn_failure_count.load(Ordering::Acquire)
    }

    pub fn build_serialize_wrapper(
        &self,
        topic_config_table: HashMap<CheetahString, TopicConfig>,
    ) -> TopicConfigAndMappingSerializeWrapper {
        self.build_serialize_wrapper_with_topic_queue_map(topic_config_table, DashMap::new())
    }

    pub fn build_serialize_wrapper_with_topic_queue_map(
        &self,
        topic_config_table: HashMap<CheetahString, TopicConfig>,
        topic_queue_mapping_info_map: DashMap<CheetahString, ArcMut<TopicQueueMappingInfo>>,
    ) -> TopicConfigAndMappingSerializeWrapper {
        if self.broker_runtime_inner.broker_config().enable_split_registration {
            self.data_version.mut_from_ref().next_version();
        }
        TopicConfigAndMappingSerializeWrapper {
            topic_config_serialize_wrapper: rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigSerializeWrapper {
                topic_config_table,
                data_version: self.data_version.as_ref().clone(),
            },
            topic_queue_mapping_info_map: topic_queue_mapping_info_map
                .iter()
                .map(|entry| (entry.key().clone(), (**entry.value()).clone()))
                .collect(),
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

    pub fn get_topic_config(&self, topic_name: &str) -> Option<ArcMut<TopicConfig>> {
        self.topic_config_table.get(topic_name).as_deref().cloned()
    }

    pub(crate) fn put_topic_config(&self, topic_config: ArcMut<TopicConfig>) -> Option<ArcMut<TopicConfig>> {
        let old = self
            .topic_config_table
            .insert(topic_config.topic_name.as_ref().unwrap().clone(), topic_config);
        self.rebuild_topic_config_snapshot();
        old
    }

    pub async fn create_topic_in_send_message_method(
        &mut self,
        topic: &CheetahString,
        default_topic: &CheetahString,
        remote_address: SocketAddr,
        client_default_topic_queue_nums: i32,
        topic_sys_flag: u32,
    ) -> Option<ArcMut<TopicConfig>> {
        let start_time = Instant::now();

        // Fast path: lock-free read
        if let Some(config) = self.topic_config_table.get(topic) {
            return Some(config.value().clone());
        }

        // Use DashMap entry API for atomic check-and-insert
        let (topic_config, create_new) = {
            let topic_key = topic.clone();
            let entry = self.topic_config_table.entry(topic_key);

            match entry {
                dashmap::mapref::entry::Entry::Occupied(e) => {
                    // Another thread created it
                    (Some(e.get().clone()), false)
                }
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    // We need to create it
                    let mut default_topic_config = match self.get_topic_config(default_topic) {
                        Some(config) => config,
                        None => {
                            warn!(
                                "Create new topic failed, because the default topic[{}] not exist. producer:[{}]",
                                default_topic, remote_address
                            );
                            return None;
                        }
                    };

                    if default_topic == TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC
                        && !self.broker_runtime_inner.broker_config().auto_create_topic_enable
                    {
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

                    let topic_config_arc = ArcMut::new(topic_config);
                    e.insert(topic_config_arc.clone());
                    (Some(topic_config_arc), true)
                }
            }
        }; // Entry is dropped here

        // Persist operation only needs lock to prevent concurrent file writes
        if create_new {
            self.rebuild_topic_config_snapshot();
            let config_for_register = topic_config.clone().unwrap();

            self.next_topic_config_data_version();
            self.persist_and_register_created_topic(config_for_register, true).await;
            Self::record_topic_create_latency(start_time);
        }
        topic_config
    }

    pub async fn create_topic_in_send_message_back_method(
        &mut self,
        topic: &CheetahString,
        client_default_topic_queue_nums: i32,
        perm: u32,
        is_order: bool,
        topic_sys_flag: u32,
    ) -> Option<ArcMut<TopicConfig>> {
        let start_time = Instant::now();

        // Fast path: check existing config
        if let Some(mut config) = self.get_topic_config(topic) {
            if is_order != config.order {
                config.order = is_order;
                self.update_topic_config(config.clone());
            }
            return Some(config);
        }

        // Use DashMap entry API for atomic check-and-insert
        let (topic_config, create_new) = {
            let entry = self.topic_config_table.entry(topic.clone());

            match entry {
                dashmap::mapref::entry::Entry::Occupied(e) => (Some(e.get().clone()), false),
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    let mut config = ArcMut::new(TopicConfig::new(topic));
                    config.read_queue_nums = client_default_topic_queue_nums as u32;
                    config.write_queue_nums = client_default_topic_queue_nums as u32;
                    config.perm = perm;
                    config.topic_sys_flag = topic_sys_flag;
                    config.order = is_order;

                    e.insert(config.clone());
                    (Some(config), true)
                }
            }
        }; // Entry is dropped here

        // Persist operation only needs lock
        if create_new {
            self.rebuild_topic_config_snapshot();
            let config_for_register = topic_config.clone().unwrap();

            self.next_topic_config_data_version();
            self.persist_and_register_created_topic(config_for_register, true).await;
            Self::record_topic_create_latency(start_time);
        }
        topic_config
    }

    async fn register_broker_data(&mut self, topic_config: ArcMut<TopicConfig>) {
        let broker_config = self.broker_runtime_inner.broker_config().clone();
        let broker_runtime_inner = self.broker_runtime_inner.clone();
        let data_version = self.data_version.as_ref().clone();

        if broker_config.enable_single_topic_register {
            broker_runtime_inner.register_single_topic_all(topic_config).await;
        } else {
            BrokerRuntimeInner::register_increment_broker_data(broker_runtime_inner, vec![topic_config], data_version)
                .await;
        }
    }

    fn next_topic_config_data_version(&mut self) {
        let state_machine_version = self
            .broker_runtime_inner
            .message_store()
            .map(|message_store| message_store.get_state_machine_version())
            .unwrap_or_default();
        self.data_version
            .mut_from_ref()
            .next_version_with(state_machine_version);
    }

    async fn persist_and_register_created_topic(&mut self, topic_config: ArcMut<TopicConfig>, register: bool) {
        if self
            .broker_runtime_inner
            .broker_config()
            .async_topic_create_persist_enable
            && self.enqueue_async_topic_create_persist(topic_config.clone(), register)
        {
            return;
        }

        self.persist_created_topic_sync(topic_config, register).await;
    }

    async fn persist_created_topic_sync(&mut self, topic_config: ArcMut<TopicConfig>, register: bool) {
        let _lock = self.persist_lock.lock();
        self.persist();
        drop(_lock);

        if register {
            self.register_broker_data(topic_config).await;
        }
    }

    fn enqueue_async_topic_create_persist(&self, topic_config: ArcMut<TopicConfig>, register: bool) -> bool {
        self.async_topic_create_pending_count.fetch_add(1, Ordering::AcqRel);

        let pending_count_for_task = self.async_topic_create_pending_count.clone();
        let pending_count_for_spawn_failure = self.async_topic_create_pending_count.clone();
        let spawn_failure_count = self.async_topic_create_spawn_failure_count.clone();
        let broker_runtime_inner = self.broker_runtime_inner.clone();
        let task = async move {
            let topic_name = topic_config
                .topic_name
                .as_ref()
                .map_or_else(|| "<unknown>".to_string(), ToString::to_string);
            {
                let topic_config_manager = broker_runtime_inner.topic_config_manager();
                let _lock = topic_config_manager.persist_lock.lock();
                topic_config_manager.persist();
            }

            if register {
                let broker_config = broker_runtime_inner.broker_config().clone();
                if broker_config.enable_single_topic_register {
                    broker_runtime_inner.register_single_topic_all(topic_config).await;
                } else {
                    let data_version = broker_runtime_inner.topic_config_manager().data_version_ref().clone();
                    BrokerRuntimeInner::register_increment_broker_data(
                        broker_runtime_inner.clone(),
                        vec![topic_config],
                        data_version,
                    )
                    .await;
                }
            }

            pending_count_for_task.fetch_sub(1, Ordering::AcqRel);
            info!("async topic create persist/register completed, topic={}", topic_name);
        };

        if let Some(task_group) = self.broker_runtime_inner.broker_task_group_or_current(
            "broker.topic-config.async-create",
            "async topic create persist requested without an active broker runtime; falling back to synchronous \
             persist",
        ) {
            match task_group.spawn("broker.topic-config.async-create.persist", TaskKind::Worker, task) {
                Ok(_) => true,
                Err(error) => {
                    pending_count_for_spawn_failure.fetch_sub(1, Ordering::AcqRel);
                    spawn_failure_count.fetch_add(1, Ordering::AcqRel);
                    warn!(?error, "failed to spawn async topic create persist task");
                    false
                }
            }
        } else {
            pending_count_for_spawn_failure.fetch_sub(1, Ordering::AcqRel);
            spawn_failure_count.fetch_add(1, Ordering::AcqRel);
            false
        }
    }

    pub fn update_topic_config_list(&mut self, topic_config_list: &mut [ArcMut<TopicConfig>]) {
        for topic_config in topic_config_list {
            //can optimize todo
            self.update_topic_config(topic_config.clone());
        }
    }

    #[inline]
    pub fn remove_topic_config(&self, topic: &str) -> Option<ArcMut<TopicConfig>> {
        match self.topic_config_table.remove(topic) {
            None => None,
            Some(value) => {
                self.rebuild_topic_config_snapshot();
                Some(value.1)
            }
        }
    }

    pub fn delete_topic_config(&self, topic: &CheetahString) {
        let old = self.remove_topic_config(topic);
        if let Some(old) = old {
            info!("delete topic config OK, topic: {:?}", old);
            let state_machine_version = if let Some(message_store) = self.broker_runtime_inner.message_store() {
                message_store.get_state_machine_version()
            } else {
                0
            };
            self.data_version
                .mut_from_ref()
                .next_version_with(state_machine_version);
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

    pub fn update_topic_config(&mut self, mut topic_config: ArcMut<TopicConfig>) {
        let start_time = Instant::now();
        let new_attributes = Self::request(topic_config.as_ref());
        let current_attributes = self.current(topic_config.topic_name.as_ref().unwrap().as_str());
        let create = self
            .topic_config_table
            .get(topic_config.topic_name.as_ref().unwrap().as_str())
            .is_none();

        let final_attributes_result = AttributeUtil::alter_current_attributes(
            create,
            TopicAttributes::all(),
            &current_attributes,
            &new_attributes,
        );
        match final_attributes_result {
            Ok(final_attributes) => {
                topic_config.attributes = final_attributes;
            }
            Err(e) => {
                error!("Failed to alter current attributes: {:?}", e);
                // Decide on an appropriate fallback action, e.g., using default attributes
                topic_config.attributes = Default::default();
            }
        }
        match self.put_topic_config(topic_config.clone()) {
            None => {
                info!("create new topic [{:?}]", topic_config)
            }
            Some(ref old) => {
                info!("update topic config, old:[{:?}] new:[{:?}]", old, topic_config);
            }
        }

        self.data_version.mut_from_ref().next_version_with(
            self.broker_runtime_inner
                .message_store()
                .unwrap()
                .get_state_machine_version(),
        );
        self.persist_with_topic(
            topic_config.topic_name.as_ref().unwrap().as_str(),
            Box::new(topic_config.clone()),
        );
        if create {
            Self::record_topic_create_latency(start_time);
        }
    }

    fn request(topic_config: &TopicConfig) -> HashMap<CheetahString, CheetahString> {
        topic_config.attributes.clone()
    }

    fn current(&self, topic: &str) -> HashMap<CheetahString, CheetahString> {
        let topic_config = self.get_topic_config(topic);
        match topic_config {
            None => HashMap::new(),
            Some(config) => config.attributes.clone(),
        }
    }

    pub fn topic_config_table(&self) -> Arc<DashMap<CheetahString, ArcMut<TopicConfig>>> {
        self.topic_config_table.clone()
    }

    pub fn topic_config_table_hash_map(&self) -> HashMap<CheetahString, TopicConfig> {
        self.topic_config_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().as_ref().clone()))
            .collect::<HashMap<CheetahString, TopicConfig>>()
    }

    pub fn set_topic_config_table(&mut self, topic_config_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>>) {
        self.topic_config_table = topic_config_table;
        self.rebuild_topic_config_snapshot();
    }

    pub async fn create_topic_of_tran_check_max_time(
        &mut self,
        client_default_topic_queue_nums: i32,
        perm: u32,
    ) -> Option<ArcMut<TopicConfig>> {
        // Fast path: lock-free read
        if let Some(config) = self
            .topic_config_table
            .get(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC)
        {
            return Some(config.value().clone());
        }

        // Use DashMap entry API for atomic check-and-insert
        let (topic_config, create_new) = {
            let topic_key = CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
            let entry = self.topic_config_table.entry(topic_key);

            match entry {
                dashmap::mapref::entry::Entry::Occupied(e) => (Some(e.get().clone()), false),
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    let mut config = ArcMut::new(TopicConfig::new(TopicValidator::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC));
                    config.read_queue_nums = client_default_topic_queue_nums as u32;
                    config.write_queue_nums = client_default_topic_queue_nums as u32;
                    config.perm = perm;
                    config.topic_sys_flag = 0;
                    info!("create new topic {:?}", config);
                    e.insert(config.clone());
                    (Some(config), true)
                }
            }
        }; // Entry is dropped here

        // Persist operation only needs lock
        if create_new {
            self.rebuild_topic_config_snapshot();
            let config_for_register = topic_config.clone().unwrap();

            self.next_topic_config_data_version();
            self.persist_and_register_created_topic(config_for_register, true).await;
        }
        topic_config
    }

    pub fn contains_topic(&self, topic: &CheetahString) -> bool {
        self.topic_config_table.contains_key(topic)
    }

    pub fn data_version(&self) -> ArcMut<DataVersion> {
        self.data_version.clone()
    }

    pub fn data_version_ref(&self) -> &DataVersion {
        self.data_version.as_ref()
    }

    pub fn data_version_ref_mut(&mut self) -> &mut DataVersion {
        self.data_version.as_mut()
    }

    #[inline]
    pub fn broker_runtime_inner(&self) -> &ArcMut<BrokerRuntimeInner<MS>> {
        &self.broker_runtime_inner
    }

    pub fn update_order_topic_config(&mut self, order_kv_table_from_ns: &KVTable) {
        if !order_kv_table_from_ns.table.is_empty() {
            let mut is_change = false;

            for topic in order_kv_table_from_ns.table.keys() {
                if let Some(mut topic_config) = self.get_topic_config(topic) {
                    if !topic_config.order {
                        topic_config.order = true;
                        self.put_topic_config(topic_config);
                        is_change = true;
                        info!("update order topic config, topic={}, order=true", topic);
                    }
                }
            }

            if is_change {
                self.data_version.mut_from_ref().next_version_with(
                    self.broker_runtime_inner
                        .message_store()
                        .unwrap()
                        .get_state_machine_version(),
                );
                self.persist();
            }
        }
    }

    pub async fn create_topic_if_absent(
        &mut self,
        mut topic_config: TopicConfig,
        register: bool,
    ) -> Option<ArcMut<TopicConfig>> {
        let start_time = Instant::now();
        if topic_config.topic_name.is_none() {
            error!("createTopicIfAbsent: TopicName cannot be None");
            return None;
        }

        let topic_name = topic_config.topic_name.as_ref().unwrap().clone();

        // Fast path: lock-free read
        if let Some(config) = self.topic_config_table.get(&topic_name) {
            return Some(config.value().clone());
        }

        // Use DashMap entry API for atomic check-and-insert
        let (result, create_new) = {
            let entry = self.topic_config_table.entry(topic_name.clone());

            match entry {
                dashmap::mapref::entry::Entry::Occupied(e) => (Some(e.get().clone()), false),
                dashmap::mapref::entry::Entry::Vacant(e) => {
                    info!("Create new topic [{}] config:[{:?}]", topic_name, topic_config);

                    // Ensure topic_name is set
                    topic_config.topic_name = Some(topic_name.clone());

                    let config = ArcMut::new(topic_config);
                    e.insert(config.clone());
                    (Some(config), true)
                }
            }
        }; // Entry is dropped here

        // Persist operation only needs lock
        if create_new {
            self.rebuild_topic_config_snapshot();
            let config_for_register = if register { result.clone() } else { None };

            self.next_topic_config_data_version();
            if let Some(config) = config_for_register {
                self.persist_and_register_created_topic(config, true).await;
            } else if let Some(config) = result.clone() {
                self.persist_and_register_created_topic(config, false).await;
            }
            Self::record_topic_create_latency(start_time);
        }

        result
    }

    pub async fn update_topic_unit_flag(&mut self, topic: &str, unit: bool) {
        if let Some(mut topic_config) = self.get_topic_config(topic) {
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

            self.put_topic_config(topic_config.clone());
            self.data_version.mut_from_ref().next_version_with(
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_state_machine_version(),
            );
            self.persist();
            self.register_broker_data(topic_config).await;
        }
    }

    pub async fn update_topic_unit_sub_flag(&mut self, topic: &str, has_unit_sub: bool) {
        if let Some(mut topic_config) = self.get_topic_config(topic) {
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

            self.put_topic_config(topic_config.clone());
            self.data_version.mut_from_ref().next_version_with(
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_state_machine_version(),
            );
            self.persist();
            self.register_broker_data(topic_config).await;
        }
    }

    pub fn load_data_version(&mut self) -> bool {
        let file_path = self.config_file_path();
        match file_utils::file_to_string(&file_path) {
            Ok(json_string) => {
                if let Ok(wrapper) = TopicConfigSerializeWrapper::decode_string(json_string) {
                    if let Some(data_version) = wrapper.data_version() {
                        self.data_version.mut_from_ref().assign_new_one(data_version);
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

        match rocksdb_config_manager.load_data_version() {
            Ok(Some(data_version)) => self.data_version.mut_from_ref().assign_new_one(&data_version),
            Ok(None) => {}
            Err(error) => {
                error!("load topic rocksdb dataVersion failed: {}", error);
                return false;
            }
        }

        let records = match rocksdb_config_manager.load_data() {
            Ok(records) => records,
            Err(error) => {
                error!("load topic config from rocksdb failed: {}", error);
                return false;
            }
        };
        for (key, body) in records {
            if let Err(error) = self.decode_topic_config_record(&key, &body) {
                error!("decode topic config from rocksdb failed: {}", error);
                return false;
            }
        }
        self.rebuild_topic_config_snapshot();
        true
    }

    #[cfg(feature = "rocksdb_store")]
    fn decode_topic_config_record(&self, key: &[u8], body: &[u8]) -> Result<(), rocketmq_error::RocketMQError> {
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
        self.topic_config_table
            .insert(CheetahString::from_string(topic_name), ArcMut::new(topic_config));
        Ok(())
    }

    #[cfg(feature = "rocksdb_store")]
    fn persist_topic_to_rocksdb(&self, topic_name: &str) -> Result<(), rocketmq_error::RocketMQError> {
        let Some(rocksdb_config_manager) = &self.rocksdb_config_manager else {
            return Ok(());
        };
        let Some(topic_config) = self.get_topic_config(topic_name) else {
            return Ok(());
        };
        let body = serde_json::to_string(topic_config.as_ref()).map_err(|error| {
            rocketmq_error::RocketMQError::storage_write_failed(
                "rocksdb-topic-config",
                format!("topic config encode failed: {error}"),
            )
        })?;
        rocksdb_config_manager.put_string(topic_name, &body)?;
        rocksdb_config_manager.set_kv_data_version(self.data_version.as_ref().clone())?;
        self.flush_rocksdb_config_if_needed(rocksdb_config_manager)
    }

    #[cfg(feature = "rocksdb_store")]
    fn persist_all_topics_to_rocksdb(&self) -> Result<(), rocketmq_error::RocketMQError> {
        let Some(rocksdb_config_manager) = &self.rocksdb_config_manager else {
            return Ok(());
        };
        let records = self
            .topic_config_table
            .iter()
            .map(|entry| serde_json::to_vec(entry.value().as_ref()).map(|body| (entry.key().as_bytes().to_vec(), body)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                rocketmq_error::RocketMQError::storage_write_failed(
                    "rocksdb-topic-config",
                    format!("topic config encode failed: {error}"),
                )
            })?;
        rocksdb_config_manager.batch_put_with_wal(&records)?;
        rocksdb_config_manager.set_kv_data_version(self.data_version.as_ref().clone())?;
        self.flush_rocksdb_config_if_needed(rocksdb_config_manager)
    }

    #[cfg(feature = "rocksdb_store")]
    fn flush_rocksdb_config_if_needed(
        &self,
        rocksdb_config_manager: &RocksDbBrokerConfigManager,
    ) -> Result<(), rocketmq_error::RocketMQError> {
        if self
            .broker_runtime_inner
            .message_store_config()
            .real_time_persist_rocksdb_config
        {
            rocksdb_config_manager.flush_wal()?;
        }
        Ok(())
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

impl<MS: MessageStore> ConfigManager for TopicConfigManager<MS> {
    fn load(&self) -> bool {
        #[cfg(feature = "rocksdb_store")]
        if self.rocksdb_config_manager.is_some() {
            return self.load_from_rocksdb_or_migrate_from_file();
        }
        self.load_from_config_file()
    }

    fn persist_with_topic(&mut self, topic_name: &str, _t: Box<dyn std::any::Any>) {
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
        self.persist()
    }

    fn persist(&self) {
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

    fn stop(&mut self) -> bool {
        #[cfg(feature = "rocksdb_store")]
        if let Some(rocksdb_config_manager) = &self.rocksdb_config_manager {
            rocksdb_config_manager.close();
        }
        true
    }

    fn config_file_path(&self) -> String {
        get_topic_config_path(self.broker_runtime_inner.broker_config().store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        let topic_config_table = self
            .topic_config_table
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().as_ref().clone()))
            .collect::<HashMap<CheetahString, TopicConfig>>();

        let version = self.data_version().as_ref().clone();
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
        if let Some(value) = wrapper.data_version() {
            self.data_version.mut_from_ref().assign_new_one(value);
        }
        if let Some(map) = wrapper.take_topic_config_table() {
            for (key, value) in map {
                self.topic_config_table.insert(key, ArcMut::new(value));
            }
            self.rebuild_topic_config_snapshot();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::config_manager::ConfigManager;
    use rocketmq_rust::ArcMut;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::GenericMessageStore;
    use tempfile::TempDir;

    use crate::broker_runtime::BrokerRuntime;
    use crate::topic::manager::topic_config_manager::TopicConfigManager;

    fn test_topic_config_manager() -> (TempDir, TopicConfigManager<GenericMessageStore>) {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let root = CheetahString::from_string(temp_dir.path().to_string_lossy().to_string());
        let mut runtime = BrokerRuntime::new(
            Arc::new(BrokerConfig {
                store_path_root_dir: root.clone(),
                ..BrokerConfig::default()
            }),
            Arc::new(MessageStoreConfig {
                store_path_root_dir: root,
                ..MessageStoreConfig::default()
            }),
        );
        let manager = TopicConfigManager::new(runtime.inner_for_test().clone(), false);
        (temp_dir, manager)
    }

    #[test]
    fn select_topic_config_reads_snapshot_after_put_and_delete() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let topic = CheetahString::from_static_str("SnapshotTopic");

        assert!(manager.select_topic_config(&topic).is_none());

        manager.put_topic_config(ArcMut::new(TopicConfig::with_queues(topic.clone(), 2, 3)));
        let config = manager
            .select_topic_config(&topic)
            .expect("topic should be visible through snapshot");
        assert_eq!(config.read_queue_nums, 2);
        assert_eq!(config.write_queue_nums, 3);

        manager.delete_topic_config(&topic);
        assert!(manager.select_topic_config(&topic).is_none());
    }

    #[test]
    fn select_topic_config_reads_snapshot_after_table_replacement() {
        let (_temp_dir, mut manager) = test_topic_config_manager();
        let topic = CheetahString::from_static_str("ReplacementTopic");
        let replacement = Arc::new(DashMap::new());
        replacement.insert(
            topic.clone(),
            ArcMut::new(TopicConfig::with_queues(topic.clone(), 4, 5)),
        );

        manager.set_topic_config_table(replacement);
        let config = manager
            .select_topic_config(&topic)
            .expect("replacement table should rebuild snapshot");
        assert_eq!(config.read_queue_nums, 4);
        assert_eq!(config.write_queue_nums, 5);

        manager.set_topic_config_table(Arc::new(DashMap::new()));
        assert!(manager.select_topic_config(&topic).is_none());
    }

    #[test]
    fn decode_rebuilds_topic_config_snapshot() {
        let (_temp_dir, manager) = test_topic_config_manager();
        let topic = CheetahString::from_static_str("LoadedTopic");
        manager.put_topic_config(ArcMut::new(TopicConfig::with_queues(topic.clone(), 6, 7)));
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
}

#[cfg(all(test, feature = "rocksdb_store"))]
mod rocksdb_config_tests {
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::config_manager::ConfigManager;
    use rocketmq_remoting::protocol::data_version_facade::DataVersionExt;
    use rocketmq_rust::ArcMut;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use tempfile::TempDir;

    use crate::broker_runtime::BrokerRuntime;
    use crate::config::rocksdb_manager::RocksDbBrokerConfigManager;
    use crate::config::rocksdb_manager::RocksDbBrokerConfigManagerConfig;
    use crate::topic::manager::topic_config_manager::TopicConfigManager;

    #[tokio::test]
    async fn topic_config_manager_persists_single_topic_to_rocksdb_and_loads_after_restart() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut runtime = test_runtime(&temp_dir);
        let inner = runtime.inner_for_test().clone();
        let rocksdb_path = temp_dir.path().join("config").join("topics");
        let rocksdb_manager = Arc::new(
            RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(rocksdb_path.clone()))
                .expect("rocksdb config manager should open"),
        );
        let mut topic_manager =
            TopicConfigManager::new_with_rocksdb_config_manager(inner.clone(), false, rocksdb_manager);
        let topic = ArcMut::new(TopicConfig::with_queues("TopicA", 4, 5));

        topic_manager.put_topic_config(topic.clone());
        topic_manager.data_version_ref_mut().next_version();
        topic_manager.persist_with_topic("TopicA", Box::new(topic));
        drop(topic_manager);

        let restarted_rocksdb_manager = Arc::new(
            RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(rocksdb_path))
                .expect("rocksdb config manager should reopen"),
        );
        let restarted_manager =
            TopicConfigManager::new_with_rocksdb_config_manager(inner, false, restarted_rocksdb_manager);

        assert!(restarted_manager.load());
        let loaded = restarted_manager
            .get_topic_config("TopicA")
            .expect("topic config should load from rocksdb");
        assert_eq!(loaded.read_queue_nums, 4);
        assert_eq!(loaded.write_queue_nums, 5);
        assert_eq!(restarted_manager.data_version_ref().counter(), 1);
    }

    #[tokio::test]
    async fn topic_config_manager_delete_removes_topic_from_rocksdb() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut runtime = test_runtime(&temp_dir);
        let inner = runtime.inner_for_test().clone();
        let rocksdb_path = temp_dir.path().join("config").join("topics-delete");
        let rocksdb_manager = Arc::new(
            RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(rocksdb_path.clone()))
                .expect("rocksdb config manager should open"),
        );
        let mut topic_manager =
            TopicConfigManager::new_with_rocksdb_config_manager(inner.clone(), false, rocksdb_manager);
        let topic_name = CheetahString::from_static_str("TopicDelete");
        let topic = ArcMut::new(TopicConfig::with_queues(topic_name.clone(), 1, 1));

        topic_manager.put_topic_config(topic.clone());
        topic_manager.data_version_ref_mut().next_version();
        topic_manager.persist_with_topic(topic_name.as_str(), Box::new(topic));
        topic_manager.delete_topic_config(&topic_name);
        drop(topic_manager);

        let restarted_rocksdb_manager = Arc::new(
            RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::topic(rocksdb_path))
                .expect("rocksdb config manager should reopen"),
        );
        let restarted_manager =
            TopicConfigManager::new_with_rocksdb_config_manager(inner, false, restarted_rocksdb_manager);

        assert!(restarted_manager.load());
        assert!(restarted_manager.get_topic_config(topic_name.as_str()).is_none());
    }

    fn test_runtime(temp_dir: &TempDir) -> BrokerRuntime {
        let root = CheetahString::from_string(temp_dir.path().to_string_lossy().to_string());
        BrokerRuntime::new(
            Arc::new(BrokerConfig {
                store_path_root_dir: root.clone(),
                ..BrokerConfig::default()
            }),
            Arc::new(MessageStoreConfig {
                store_path_root_dir: root,
                real_time_persist_rocksdb_config: true,
                ..MessageStoreConfig::default()
            }),
        )
    }
}
