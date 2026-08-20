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
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
#[cfg(feature = "rocksdb_store")]
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;

use crate::config::broker_config::BrokerConfig;
use crate::config::config_manager::ConfigManager;
use arc_swap::ArcSwap;
use cheetah_string::CheetahBuilder;
use cheetah_string::CheetahString;
use rocketmq_model::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_protocol::protocol::data_version_facade::DataVersionExt;
use rocketmq_protocol::protocol::DataVersion;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::common::file_utils;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::MessageStoreConfig;
use serde::de;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_path_config_helper::get_consumer_offset_path;
#[cfg(feature = "rocksdb_store")]
use crate::config::rocksdb_manager::RocksDbBrokerConfigManager;
use crate::failover::escape_bridge::EscapeBridge;

pub const TOPIC_GROUP_SEPARATOR: &str = "@";

fn build_topic_group_key(topic: &CheetahString, group: &CheetahString) -> CheetahString {
    let mut builder = CheetahBuilder::with_capacity(topic.len() + TOPIC_GROUP_SEPARATOR.len() + group.len());
    builder.push_str(topic);
    builder.push_str(TOPIC_GROUP_SEPARATOR);
    builder.push_str(group);
    builder.finish()
}

fn build_topic_group_lookup_key(topic: &str, group: &str) -> String {
    let mut key = String::with_capacity(topic.len() + TOPIC_GROUP_SEPARATOR.len() + group.len());
    key.push_str(topic);
    key.push_str(TOPIC_GROUP_SEPARATOR);
    key.push_str(group);
    key
}

fn split_topic_group_key(key: &CheetahString) -> Option<(&str, &str)> {
    let (topic, group) = key.split_once(TOPIC_GROUP_SEPARATOR)?;
    if topic.is_empty() || group.is_empty() || group.contains(TOPIC_GROUP_SEPARATOR) {
        return None;
    }
    Some((topic, group))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsumerLagObservation {
    pub(crate) topic: String,
    pub(crate) consumer_group: String,
    pub(crate) lag_messages: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConsumerLagAdjustment {
    pub(crate) pull_offset: i64,
    pub(crate) inflight_messages: i64,
}

pub(crate) type ConsumerLagAdjustments = HashMap<CheetahString, HashMap<i32, ConsumerLagAdjustment>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsumerLagTarget {
    pub(crate) topic_group: CheetahString,
    pub(crate) topic: CheetahString,
    pub(crate) consumer_group: CheetahString,
    pub(crate) queue_offsets: Vec<(i32, i64)>,
}

pub(crate) struct ConsumerOffsetManager<MS: BrokerReadStore> {
    broker_config: Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    consumer_offset_wrapper: ConsumerOffsetWrapper,
    message_store: ConsumerOffsetStoreCapability<MS>,
    #[cfg(feature = "rocksdb_store")]
    rocksdb_config_manager: Option<Arc<RocksDbBrokerConfigManager>>,
}

/// Late-bound, non-owning Store query provider for offset processing.
struct ConsumerOffsetStoreCapability<MS: BrokerReadStore> {
    provider: Arc<OnceLock<Weak<EscapeBridge<MS>>>>,
}

impl<MS: BrokerReadStore> Default for ConsumerOffsetStoreCapability<MS> {
    fn default() -> Self {
        Self {
            provider: Arc::new(OnceLock::new()),
        }
    }
}

impl<MS: BrokerReadStore> ConsumerOffsetStoreCapability<MS> {
    fn bind(&self, provider: &Arc<EscapeBridge<MS>>) {
        let _ = self.provider.set(Arc::downgrade(provider));
    }

    fn provider(&self) -> Option<Arc<EscapeBridge<MS>>> {
        self.provider.get().and_then(Weak::upgrade)
    }
}

/// Narrow live offset capability used by consumer offset request processing.
///
/// The capability shares the existing manager owner and exposes only request-time offset
/// operations plus the two Store reads required by the legacy query fallback.
pub(crate) struct ConsumerOffsetRequestCapability<MS: BrokerReadStore> {
    manager: Arc<ConsumerOffsetManager<MS>>,
}

/// Read-only consumer offset view that does not keep the manager or Store alive.
pub(crate) struct ConsumerOffsetQueryCapability<MS: BrokerReadStore> {
    manager: Weak<ConsumerOffsetManager<MS>>,
}

impl<MS: BrokerReadStore> Clone for ConsumerOffsetQueryCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            manager: Weak::clone(&self.manager),
        }
    }
}

impl<MS> ConsumerOffsetQueryCapability<MS>
where
    MS: BrokerReadStore,
{
    pub(crate) fn query_offset(&self, group: &CheetahString, topic: &CheetahString, queue_id: i32) -> i64 {
        self.manager
            .upgrade()
            .map(|manager| manager.query_offset(group, topic, queue_id))
            .unwrap_or(-1)
    }
}

impl<MS: BrokerReadStore> Clone for ConsumerOffsetRequestCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            manager: Arc::clone(&self.manager),
        }
    }
}

impl<MS> ConsumerOffsetRequestCapability<MS>
where
    MS: BrokerReadStore,
{
    pub(crate) fn commit_offset(
        &self,
        client_host: CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
    ) {
        self.manager.commit_offset(client_host, group, topic, queue_id, offset);
    }

    pub(crate) fn has_offset_reset(&self, group: &str, topic: &str, queue_id: i32) -> bool {
        self.manager.has_offset_reset(group, topic, queue_id)
    }

    pub(crate) fn query_offset(&self, group: &CheetahString, topic: &CheetahString, queue_id: i32) -> i64 {
        self.manager.query_offset(group, topic, queue_id)
    }

    pub(crate) fn query_then_erase_reset_offset(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
    ) -> Option<i64> {
        self.manager.query_then_erase_reset_offset(group, topic, queue_id)
    }

    pub(crate) fn min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        self.manager
            .message_store
            .provider()
            .and_then(|store| store.get_min_offset_from_local_store(topic, queue_id).ok())
            .unwrap_or(0)
    }

    pub(crate) fn check_in_mem_by_consume_offset(&self, topic: &CheetahString, queue_id: i32) -> bool {
        self.manager
            .message_store
            .provider()
            .and_then(|store| store.check_in_mem_by_consume_offset(topic, queue_id).ok())
            .unwrap_or(false)
    }
}

impl<MS> ConsumerOffsetManager<MS>
where
    MS: BrokerReadStore,
{
    pub(crate) fn request_capability(self: &Arc<Self>) -> ConsumerOffsetRequestCapability<MS> {
        ConsumerOffsetRequestCapability {
            manager: Arc::clone(self),
        }
    }

    pub(crate) fn query_capability(self: &Arc<Self>) -> ConsumerOffsetQueryCapability<MS> {
        ConsumerOffsetQueryCapability {
            manager: Arc::downgrade(self),
        }
    }

    pub fn new(broker_config: Arc<BrokerConfig>, message_store_config: Arc<MessageStoreConfig>) -> Self {
        ConsumerOffsetManager {
            broker_config,
            message_store_config,
            consumer_offset_wrapper: ConsumerOffsetWrapper {
                data_version: ArcSwap::from_pointee(DataVersion::default()),
                data_version_transition: parking_lot::Mutex::new(()),
                offset_table: parking_lot::RwLock::new(HashMap::new()),
                reset_offset_table: parking_lot::RwLock::new(HashMap::new()),
                pull_offset_table: parking_lot::RwLock::new(HashMap::new()),
                version_change_counter: AtomicI64::new(0),
            },
            message_store: ConsumerOffsetStoreCapability::default(),
            #[cfg(feature = "rocksdb_store")]
            rocksdb_config_manager: None,
        }
    }

    #[cfg(feature = "rocksdb_store")]
    pub fn new_with_rocksdb_config_manager(
        broker_config: Arc<BrokerConfig>,
        message_store_config: Arc<MessageStoreConfig>,
        rocksdb_config_manager: Arc<RocksDbBrokerConfigManager>,
    ) -> Self {
        let mut manager = Self::new(broker_config, message_store_config);
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
                "rocksdb-consumer-offset",
                format!("export consumer offset config to json failed: {error}"),
            )
        })
    }

    pub(crate) fn bind_message_store(&self, provider: &Arc<EscapeBridge<MS>>) {
        self.message_store.bind(provider);
    }

    pub fn commit_pull_offset(
        &self,
        _client_host: SocketAddr,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
    ) {
        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        let key = build_topic_group_key(topic, group);
        self.consumer_offset_wrapper
            .pull_offset_table
            .write()
            .entry(key)
            .or_default()
            .insert(queue_id, offset);
    }

    pub fn query_then_erase_reset_offset(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
    ) -> Option<i64> {
        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        let key = build_topic_group_lookup_key(topic, group);
        let mut write_guard = self.consumer_offset_wrapper.reset_offset_table.write();
        let offset_table = write_guard.get_mut(key.as_str());
        match offset_table {
            None => None,
            Some(value) => value.remove(&queue_id),
        }
    }

    pub fn assign_reset_offset(&self, topic: &CheetahString, group: &CheetahString, queue_id: i32, offset: i64) {
        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        let key = build_topic_group_key(topic, group);
        self.consumer_offset_wrapper
            .reset_offset_table
            .write()
            .entry(key)
            .or_default()
            .insert(queue_id, offset);
    }

    pub fn clean_offset_by_topic(&self, topic: &CheetahString) {
        self.remove_keys_matching(
            |key| matches!(split_topic_group_key(key), Some((key_topic, _)) if key_topic == topic.as_str()),
        );
    }

    pub fn clean_offset_by_group(&self, group: &CheetahString) {
        self.remove_keys_matching(
            |key| matches!(split_topic_group_key(key), Some((_, key_group)) if key_group == group.as_str()),
        );
    }

    pub fn clear_pull_offset(&self, group: &CheetahString, topic: &CheetahString) {
        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        let key = build_topic_group_key(topic, group);
        self.consumer_offset_wrapper.pull_offset_table.write().remove(&key);
    }

    pub fn clone_offset(&self, src_group: &CheetahString, dest_group: &CheetahString, topic: &CheetahString) {
        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        let src_key = build_topic_group_key(topic, src_group);
        let Some(offsets) = self.consumer_offset_wrapper.offset_table.read().get(&src_key).cloned() else {
            return;
        };

        let dest_key = build_topic_group_key(topic, dest_group);
        self.consumer_offset_wrapper
            .offset_table
            .write()
            .insert(dest_key, offsets);
    }

    pub fn which_group_by_topic(&self, topic: &str) -> HashSet<CheetahString> {
        let read_guard = self.consumer_offset_wrapper.offset_table.read();
        let mut groups = HashSet::new();
        for key in read_guard.keys() {
            if let Some((key_topic, group)) = split_topic_group_key(key) {
                if key_topic == topic {
                    groups.insert(CheetahString::from_slice(group));
                }
            }
        }
        groups
    }

    pub fn commit_offset(
        &self,
        client_host: CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
    ) {
        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        let key = build_topic_group_key(topic, group);

        {
            let mut write_guard = self.consumer_offset_wrapper.offset_table.write();
            let map = write_guard.entry(key.clone()).or_default();
            if let Some(&store_offset) = map.get(&queue_id) {
                if offset < store_offset {
                    warn!(
                        "[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, \
                         requestOffset={}, storeOffset={}",
                        client_host, key, queue_id, offset, store_offset
                    );
                    return;
                }
                if offset == store_offset {
                    return;
                }
            }
            map.insert(queue_id, offset);
        }

        let committed_updates = self
            .consumer_offset_wrapper
            .version_change_counter
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        let update_step = self.broker_config.consumer_offset_update_version_step;
        if update_step > 0 && committed_updates % update_step == 0 {
            let state_machine_version = self
                .message_store
                .provider()
                .and_then(|store| store.local_store_state_machine_version().ok())
                .unwrap_or(0);
            self.advance_data_version_with_locked(state_machine_version);
        }
    }

    pub fn has_offset_reset(&self, group: &str, topic: &str, queue_id: i32) -> bool {
        let key = build_topic_group_lookup_key(topic, group);
        match self.consumer_offset_wrapper.reset_offset_table.read().get(key.as_str()) {
            None => false,
            Some(inner) => inner.contains_key(&queue_id),
        }
    }

    pub fn query_offset(&self, group: &CheetahString, topic: &CheetahString, queue_id: i32) -> i64 {
        let key = build_topic_group_lookup_key(topic, group);
        if self.broker_config.use_server_side_reset_offset {
            if let Some(value) = self.consumer_offset_wrapper.reset_offset_table.read().get(key.as_str()) {
                return *value.get(&queue_id).unwrap_or(&-1);
            }
        }
        if let Some(value) = self.consumer_offset_wrapper.offset_table.read().get(key.as_str()) {
            return *value.get(&queue_id).unwrap_or(&-1);
        }
        -1
    }

    pub fn query_offsets(&self, group: &CheetahString, topic: &CheetahString) -> Option<HashMap<i32, i64>> {
        let key = build_topic_group_lookup_key(topic, group);
        self.consumer_offset_wrapper
            .offset_table
            .read()
            .get(key.as_str())
            .cloned()
    }

    pub fn query_min_offset_in_all_group(
        &self,
        topic: &CheetahString,
        filter_groups: Option<&CheetahString>,
    ) -> HashMap<i32, i64> {
        let excluded_groups = filter_groups
            .filter(|value| !value.is_empty())
            .map(|value| {
                value
                    .split_char(',')
                    .map(str::trim)
                    .filter(|group| !group.is_empty())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        let Some(message_store) = self.message_store.provider() else {
            return HashMap::new();
        };

        let mut queue_min_offset = HashMap::new();
        for (topic_group, offsets) in self.consumer_offset_wrapper.offset_table.read().iter() {
            let Some((key_topic, group)) = split_topic_group_key(topic_group) else {
                continue;
            };
            if key_topic != topic.as_str() {
                continue;
            }

            if excluded_groups.contains(group) {
                continue;
            }

            for (&queue_id, &offset) in offsets.iter() {
                let Ok(min_offset) = message_store.get_min_offset_from_local_store(topic, queue_id) else {
                    continue;
                };
                if offset < min_offset {
                    continue;
                }

                queue_min_offset
                    .entry(queue_id)
                    .and_modify(|current: &mut i64| *current = (*current).min(offset))
                    .or_insert(offset);
            }
        }

        queue_min_offset
    }

    pub fn which_topic_by_consumer(&self, group: &CheetahString) -> HashSet<CheetahString> {
        let read_guard = self.consumer_offset_wrapper.offset_table.read();
        let mut topics = HashSet::new();
        for key in read_guard.keys() {
            if let Some((topic, key_group)) = split_topic_group_key(key) {
                if key_group == group.as_str() {
                    topics.insert(CheetahString::from_slice(topic));
                }
            }
        }
        topics
    }

    pub fn offset_table_snapshot(&self) -> HashMap<CheetahString, HashMap<i32, i64>> {
        self.consumer_offset_wrapper.offset_table.read().clone()
    }

    pub fn offset_table_len(&self) -> usize {
        self.consumer_offset_wrapper.offset_table.read().len()
    }

    pub(crate) fn consumer_lag_targets(&self) -> Vec<ConsumerLagTarget> {
        self.consumer_lag_targets_with_limit(
            rocketmq_observability::ObservabilityConfig::default()
                .metrics
                .cardinality_limit,
        )
    }

    fn consumer_lag_targets_with_limit(&self, limit: usize) -> Vec<ConsumerLagTarget> {
        let mut targets = self
            .offset_table_snapshot()
            .into_iter()
            .filter_map(|(topic_group, offsets)| {
                let (topic, consumer_group) = split_topic_group_key(&topic_group)?;
                let mut queue_offsets = offsets.into_iter().collect::<Vec<_>>();
                queue_offsets.sort_unstable_by_key(|(queue_id, _)| *queue_id);
                Some(ConsumerLagTarget {
                    topic_group: topic_group.clone(),
                    topic: CheetahString::from_slice(topic),
                    consumer_group: CheetahString::from_slice(consumer_group),
                    queue_offsets,
                })
            })
            .collect::<Vec<_>>();
        targets.sort_unstable_by(|left, right| {
            (&left.topic, &left.consumer_group).cmp(&(&right.topic, &right.consumer_group))
        });
        targets.truncate(limit);
        targets
    }

    pub(crate) fn consumer_lag_snapshot_for_targets_with_adjustments(
        &self,
        targets: &[ConsumerLagTarget],
        adjustments: &ConsumerLagAdjustments,
    ) -> Vec<ConsumerLagObservation> {
        let Some(message_store) = self.message_store.provider() else {
            return Vec::new();
        };

        self.consumer_lag_snapshot_for_targets_with(targets, adjustments, |topic, queue_id| {
            message_store.get_max_offset_from_local_store(topic, queue_id).ok()
        })
    }

    fn consumer_lag_snapshot_with<F>(&self, limit: usize, mut max_offset: F) -> Vec<ConsumerLagObservation>
    where
        F: FnMut(&CheetahString, i32) -> Option<i64>,
    {
        self.consumer_lag_snapshot_for_targets_with(
            &self.consumer_lag_targets_with_limit(limit),
            &ConsumerLagAdjustments::new(),
            &mut max_offset,
        )
    }

    fn consumer_lag_snapshot_for_targets_with<F>(
        &self,
        targets: &[ConsumerLagTarget],
        adjustments: &ConsumerLagAdjustments,
        mut max_offset: F,
    ) -> Vec<ConsumerLagObservation>
    where
        F: FnMut(&CheetahString, i32) -> Option<i64>,
    {
        targets
            .iter()
            .filter_map(|target| {
                if target.queue_offsets.is_empty() {
                    return None;
                }

                let mut lag_messages = 0i64;
                for (queue_id, committed_offset) in &target.queue_offsets {
                    let adjustment = adjustments
                        .get(&target.topic_group)
                        .and_then(|queue_adjustments| queue_adjustments.get(queue_id));
                    let consumer_offset = adjustment.map_or(*committed_offset, |value| value.pull_offset);
                    let inflight_messages = adjustment.map_or(0, |value| value.inflight_messages);
                    if consumer_offset < 0 || inflight_messages < 0 {
                        return None;
                    }
                    let queue_max_offset = max_offset(&target.topic, *queue_id)?;
                    if queue_max_offset < 0 {
                        return None;
                    }
                    lag_messages = lag_messages
                        .saturating_add(queue_max_offset.saturating_sub(consumer_offset).max(0))
                        .saturating_add(inflight_messages);
                }

                Some(ConsumerLagObservation {
                    topic: target.topic.to_string(),
                    consumer_group: target.consumer_group.to_string(),
                    lag_messages,
                })
            })
            .collect()
    }

    pub fn data_version(&self) -> Arc<DataVersion> {
        self.consumer_offset_wrapper.data_version.load_full()
    }

    pub(crate) fn advance_data_version(&self) {
        self.advance_data_version_with(0);
    }

    fn advance_data_version_with(&self, state_machine_version: i64) {
        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        self.advance_data_version_with_locked(state_machine_version);
    }

    fn advance_data_version_with_locked(&self, state_machine_version: i64) {
        let mut data_version = self.consumer_offset_wrapper.data_version.load_full().as_ref().clone();
        data_version.next_version_with(state_machine_version);
        self.consumer_offset_wrapper.data_version.store(Arc::new(data_version));
    }

    pub(crate) fn merge_offsets_from_peer(
        &self,
        offset_table: HashMap<CheetahString, HashMap<i32, i64>>,
        data_version: DataVersion,
    ) {
        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        self.consumer_offset_wrapper.offset_table.write().extend(offset_table);
        self.consumer_offset_wrapper.data_version.store(Arc::new(data_version));
    }

    fn remove_keys_matching<F>(&self, predicate: F)
    where
        F: Fn(&CheetahString) -> bool,
    {
        let _keys_to_remove = {
            let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
            let keys_to_remove: Vec<CheetahString> = self
                .consumer_offset_wrapper
                .offset_table
                .read()
                .keys()
                .filter(|key| predicate(key))
                .cloned()
                .collect();

            if keys_to_remove.is_empty() {
                return;
            }

            let mut offset_table = self.consumer_offset_wrapper.offset_table.write();
            let mut reset_offset_table = self.consumer_offset_wrapper.reset_offset_table.write();
            let mut pull_offset_table = self.consumer_offset_wrapper.pull_offset_table.write();
            for key in &keys_to_remove {
                offset_table.remove(key);
                reset_offset_table.remove(key);
                pull_offset_table.remove(key);
            }
            keys_to_remove
        };

        #[cfg(feature = "rocksdb_store")]
        if let Err(error) = self.delete_offsets_from_rocksdb(&_keys_to_remove) {
            error!("delete consumer offsets from rocksdb failed: {}", error);
        }
    }

    #[cfg(feature = "rocksdb_store")]
    fn delete_offsets_from_rocksdb(&self, keys: &[CheetahString]) -> Result<(), rocketmq_error::RocketMQError> {
        let Some(rocksdb_config_manager) = &self.rocksdb_config_manager else {
            return Ok(());
        };
        for key in keys {
            rocksdb_config_manager.delete(key.as_str())?;
        }
        rocksdb_config_manager.set_kv_data_version(self.data_version().as_ref().clone())?;
        self.flush_rocksdb_config_if_needed(rocksdb_config_manager)
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
                error!("check consumer offset rocksdb config records failed: {}", error);
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
        if let Err(error) = self.persist_offsets_to_rocksdb() {
            error!("migrate consumer offset config file to rocksdb failed: {}", error);
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
            Ok(None) => self.data_version().as_ref().clone(),
            Err(error) => {
                error!("load consumer offset rocksdb dataVersion failed: {}", error);
                return false;
            }
        };

        let records = match rocksdb_config_manager.load_data() {
            Ok(records) => records,
            Err(error) => {
                error!("load consumer offset from rocksdb failed: {}", error);
                return false;
            }
        };

        let mut decoded_offsets = HashMap::with_capacity(records.len());
        for (key, body) in records {
            let topic_at_group = match String::from_utf8(key) {
                Ok(value) => CheetahString::from_string(value),
                Err(error) => {
                    error!("decode consumer offset key from rocksdb failed: {}", error);
                    return false;
                }
            };
            let wrapper = match serde_json::from_slice::<RocksDbOffsetSerializeWrapper>(&body) {
                Ok(wrapper) => wrapper,
                Err(error) => {
                    error!("decode consumer offset value from rocksdb failed: {}", error);
                    return false;
                }
            };
            decoded_offsets.insert(topic_at_group, wrapper.offset_table);
        }

        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        self.consumer_offset_wrapper
            .offset_table
            .write()
            .extend(decoded_offsets);
        self.consumer_offset_wrapper.data_version.store(Arc::new(data_version));
        true
    }

    #[cfg(feature = "rocksdb_store")]
    fn persist_offsets_to_rocksdb(&self) -> Result<(), rocketmq_error::RocketMQError> {
        let Some(rocksdb_config_manager) = &self.rocksdb_config_manager else {
            return Ok(());
        };
        let (data_version, offset_table) = {
            let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
            (
                self.consumer_offset_wrapper.data_version.load_full(),
                self.consumer_offset_wrapper.offset_table.read().clone(),
            )
        };
        let records = offset_table
            .iter()
            .map(|(key, offset_table)| {
                let wrapper = RocksDbOffsetSerializeWrapper {
                    offset_table: offset_table.clone(),
                };
                serde_json::to_vec(&wrapper).map(|body| (key.as_bytes().to_vec(), body))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                rocketmq_error::RocketMQError::storage_write_failed(
                    "rocksdb-consumer-offset",
                    format!("consumer offset encode failed: {error}"),
                )
            })?;
        rocksdb_config_manager.batch_put_with_wal(&records)?;
        rocksdb_config_manager.set_kv_data_version(data_version.as_ref().clone())?;
        self.flush_rocksdb_config_if_needed(rocksdb_config_manager)
    }

    #[cfg(feature = "rocksdb_store")]
    fn flush_rocksdb_config_if_needed(
        &self,
        rocksdb_config_manager: &RocksDbBrokerConfigManager,
    ) -> Result<(), rocketmq_error::RocketMQError> {
        if self.message_store_config.real_time_persist_rocksdb_config {
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

impl<MS> ConfigManager for ConsumerOffsetManager<MS>
where
    MS: BrokerReadStore,
{
    fn supports_metadata_io_actor(&self) -> bool {
        #[cfg(feature = "rocksdb_store")]
        {
            self.rocksdb_config_manager.is_none()
        }
        #[cfg(not(feature = "rocksdb_store"))]
        true
    }

    fn load(&self) -> bool {
        #[cfg(feature = "rocksdb_store")]
        if self.rocksdb_config_manager.is_some() {
            return self.load_from_rocksdb_or_migrate_from_file();
        }
        self.load_from_config_file()
    }

    fn persist(&self) -> rocketmq_error::RocketMQResult<()> {
        #[cfg(feature = "rocksdb_store")]
        if self.rocksdb_config_manager.is_some() {
            return self.persist_offsets_to_rocksdb();
        }

        let json = self.encode_pretty(true);
        if !json.is_empty() {
            let file_name = self.config_file_path();
            file_utils::string_to_file(json.as_str(), file_name.as_str()).map_err(crate::runtime_to_rocketmq_error)?;
        }
        Ok(())
    }

    fn stop(&mut self) -> bool {
        true
    }

    fn config_file_path(&self) -> String {
        get_consumer_offset_path(self.message_store_config.store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        if pretty_format {
            self.consumer_offset_wrapper
                .serialize_json_pretty()
                .expect("encode pretty failed")
        } else {
            self.consumer_offset_wrapper.serialize_json().expect("encode failed")
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let wrapper = SerdeJsonUtils::from_json_str::<ConsumerOffsetWrapper>(json_string).unwrap();
        let data_version = wrapper.data_version.load_full();
        let offset_table = wrapper.offset_table.read().clone();
        let reset_offset_table = wrapper.reset_offset_table.read().clone();
        let pull_offset_table = wrapper.pull_offset_table.read().clone();

        let _transition = self.consumer_offset_wrapper.data_version_transition.lock();
        self.consumer_offset_wrapper.offset_table.write().extend(offset_table);
        self.consumer_offset_wrapper
            .reset_offset_table
            .write()
            .extend(reset_offset_table);
        self.consumer_offset_wrapper
            .pull_offset_table
            .write()
            .extend(pull_offset_table);
        self.consumer_offset_wrapper.data_version.store(data_version);
    }
}

#[cfg(feature = "rocksdb_store")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RocksDbOffsetSerializeWrapper {
    #[serde(rename = "offsetTable")]
    offset_table: HashMap<i32, i64>,
}

struct ConsumerOffsetWrapper {
    data_version: ArcSwap<DataVersion>,
    data_version_transition: parking_lot::Mutex<()>,
    // Pop mode offset table
    offset_table: parking_lot::RwLock<HashMap<CheetahString /* topic@group */, HashMap<i32 /* queue id */, i64>>>,
    // Pop mode reset offset table
    reset_offset_table: parking_lot::RwLock<HashMap<CheetahString /* topic@group */, HashMap<i32 /* queue id */, i64>>>,
    //Pull mode offset table
    pull_offset_table: parking_lot::RwLock<HashMap<CheetahString /* topic@group */, HashMap<i32 /* queue id */, i64>>>,
    version_change_counter: AtomicI64,
}

impl Default for ConsumerOffsetWrapper {
    fn default() -> Self {
        Self {
            data_version: ArcSwap::from_pointee(DataVersion::default()),
            data_version_transition: parking_lot::Mutex::new(()),
            offset_table: parking_lot::RwLock::new(HashMap::new()),
            reset_offset_table: parking_lot::RwLock::new(HashMap::new()),
            pull_offset_table: parking_lot::RwLock::new(HashMap::new()),
            version_change_counter: AtomicI64::new(0),
        }
    }
}

impl ConsumerOffsetWrapper {
    pub fn get_group_topic_map(&self) -> HashMap<CheetahString, HashSet<CheetahString>> {
        let mut ret_map = HashMap::with_capacity(128);

        for key in self.offset_table.read().keys() {
            if let Some((topic, group)) = split_topic_group_key(key) {
                ret_map
                    .entry(CheetahString::from_slice(group))
                    .or_insert_with(|| HashSet::with_capacity(8))
                    .insert(CheetahString::from_slice(topic));
            }
        }

        ret_map
    }
}

impl Serialize for ConsumerOffsetWrapper {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let _transition = self.data_version_transition.lock();
        let mut state = serializer.serialize_struct("ConsumerOffsetWrapper", 5)?;
        let data_version = self.data_version.load_full();
        state.serialize_field("dataVersion", data_version.as_ref())?;
        state.serialize_field("offsetTable", &*self.offset_table.read())?;
        state.serialize_field("resetOffsetTable", &*self.reset_offset_table.read())?;
        state.serialize_field("pullOffsetTable", &*self.pull_offset_table.read())?;
        state.serialize_field("processedPullOffsetTable", &self.get_group_topic_map())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ConsumerOffsetWrapper {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Debug)]
        enum Field {
            DataVersion,
            OffsetTable,
            ResetOffsetTable,
            PullOffsetTable,
            Ignore,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Field, D::Error> {
                struct FieldVisitor;

                impl Visitor<'_> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("`dataVersion`, `offsetTable`, `resetOffsetTable` or `pullOffsetTable`")
                    }

                    fn visit_str<E: de::Error>(self, value: &str) -> Result<Field, E> {
                        match value {
                            "dataVersion" => Ok(Field::DataVersion),
                            "offsetTable" => Ok(Field::OffsetTable),
                            "resetOffsetTable" => Ok(Field::ResetOffsetTable),
                            "pullOffsetTable" => Ok(Field::PullOffsetTable),
                            _ => Ok(Field::Ignore),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct ConsumerOffsetWrapperVisitor;

        impl<'de> Visitor<'de> for ConsumerOffsetWrapperVisitor {
            type Value = ConsumerOffsetWrapper;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ConsumerOffsetWrapper")
            }

            fn visit_map<V: MapAccess<'de>>(self, mut map: V) -> Result<ConsumerOffsetWrapper, V::Error> {
                let mut data_version = None;
                let mut offset_table = None;
                let mut reset_offset_table = None;
                let mut pull_offset_table = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::DataVersion => {
                            if data_version.is_some() {
                                return Err(de::Error::duplicate_field("dataVersion"));
                            }
                            data_version = Some(map.next_value()?);
                        }
                        Field::OffsetTable => {
                            if offset_table.is_some() {
                                return Err(de::Error::duplicate_field("offsetTable"));
                            }
                            offset_table = Some(map.next_value()?);
                        }
                        Field::ResetOffsetTable => {
                            if reset_offset_table.is_some() {
                                return Err(de::Error::duplicate_field("resetOffsetTable"));
                            }
                            reset_offset_table = Some(map.next_value()?);
                        }
                        Field::PullOffsetTable => {
                            if pull_offset_table.is_some() {
                                return Err(de::Error::duplicate_field("pullOffsetTable"));
                            }
                            pull_offset_table = Some(map.next_value()?);
                        }
                        Field::Ignore => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                let data_version = data_version.unwrap_or_default();
                let offset_table = offset_table.unwrap_or_default();
                let reset_offset_table = reset_offset_table.unwrap_or_default();
                let pull_offset_table = pull_offset_table.unwrap_or_default();

                Ok(ConsumerOffsetWrapper {
                    data_version: ArcSwap::from_pointee(data_version),
                    data_version_transition: parking_lot::Mutex::new(()),
                    offset_table: parking_lot::RwLock::new(offset_table),
                    reset_offset_table: parking_lot::RwLock::new(reset_offset_table),
                    pull_offset_table: parking_lot::RwLock::new(pull_offset_table),
                    version_change_counter: AtomicI64::new(0),
                })
            }
        }

        const FIELDS: &[&str] = &["dataVersion", "offsetTable", "resetOffsetTable", "pullOffsetTable"];
        deserializer.deserialize_struct("ConsumerOffsetWrapper", FIELDS, ConsumerOffsetWrapperVisitor)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Barrier;

    use crate::config::broker_config::BrokerConfig;
    use crate::config::config_manager::ConfigManager;
    use cheetah_string::CheetahString;
    use rocketmq_protocol::protocol::DataVersion;
    use rocketmq_store::LocalFileMessageStore;
    use rocketmq_store::MessageStoreConfig;

    use super::build_topic_group_key;
    use super::build_topic_group_lookup_key;
    use super::split_topic_group_key;
    use super::ConsumerOffsetManager;

    fn new_manager() -> ConsumerOffsetManager<LocalFileMessageStore> {
        ConsumerOffsetManager::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        )
    }

    #[test]
    fn topic_group_key_helpers_keep_expected_storage_key() {
        let topic = CheetahString::from_static_str("topic-a");
        let group = CheetahString::from_static_str("group-a");

        let owned_key = build_topic_group_key(&topic, &group);
        let lookup_key = build_topic_group_lookup_key(topic.as_str(), group.as_str());

        assert_eq!(owned_key, "topic-a@group-a");
        assert_eq!(lookup_key, "topic-a@group-a");
        assert_eq!(split_topic_group_key(&owned_key), Some(("topic-a", "group-a")));
    }

    #[test]
    fn request_capability_shares_live_offset_state_without_store_fallback() {
        let manager = Arc::new(new_manager());
        let capability = manager.request_capability();
        let topic = CheetahString::from_static_str("topic-a");
        let group = CheetahString::from_static_str("group-a");

        capability.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 42);
        assert_eq!(manager.query_offset(&group, &topic, 0), 42);
        assert_eq!(capability.query_offset(&group, &topic, 0), 42);

        manager.assign_reset_offset(&topic, &group, 0, 7);
        assert!(capability.has_offset_reset(group.as_str(), topic.as_str(), 0));
        assert_eq!(capability.min_offset_in_queue(&topic, 0), 0);
        assert!(!capability.check_in_mem_by_consume_offset(&topic, 0));
    }

    #[test]
    fn query_capability_reads_live_offsets_without_keeping_manager_alive() {
        let manager = Arc::new(new_manager());
        let capability = manager.query_capability();
        let topic = CheetahString::from_static_str("topic-a");
        let group = CheetahString::from_static_str("group-a");

        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 42);
        assert_eq!(capability.query_offset(&group, &topic, 0), 42);

        drop(manager);
        assert_eq!(capability.query_offset(&group, &topic, 0), -1);
    }

    #[test]
    fn split_topic_group_key_rejects_malformed_keys() {
        for raw_key in ["topic-a", "@group-a", "topic-a@", "topic-a@group-a@extra"] {
            let key = CheetahString::from_static_str(raw_key);

            assert_eq!(split_topic_group_key(&key), None, "{raw_key} should be rejected");
        }
    }

    #[test]
    fn topic_group_scans_ignore_malformed_offset_keys() {
        let manager = new_manager();
        {
            let mut offsets = manager.consumer_offset_wrapper.offset_table.write();
            offsets.insert(
                CheetahString::from_static_str("topic-a@group-a"),
                HashMap::from([(0, 10)]),
            );
            offsets.insert(CheetahString::from_static_str("topic-a"), HashMap::from([(0, 20)]));
            offsets.insert(CheetahString::from_static_str("@group-a"), HashMap::from([(0, 30)]));
            offsets.insert(CheetahString::from_static_str("topic-a@"), HashMap::from([(0, 40)]));
            offsets.insert(
                CheetahString::from_static_str("topic-a@group-a@extra"),
                HashMap::from([(0, 50)]),
            );
        }

        let groups = manager.which_group_by_topic("topic-a");
        assert_eq!(groups.len(), 1);
        assert!(groups.contains(&CheetahString::from_static_str("group-a")));

        let topics = manager.which_topic_by_consumer(&CheetahString::from_static_str("group-a"));
        assert_eq!(topics.len(), 1);
        assert!(topics.contains(&CheetahString::from_static_str("topic-a")));

        let group_topic_map = manager.consumer_offset_wrapper.get_group_topic_map();
        assert_eq!(group_topic_map.len(), 1);
        assert_eq!(
            group_topic_map.get("group-a"),
            Some(&HashSet::from([CheetahString::from_static_str("topic-a")]))
        );

        manager.clean_offset_by_topic(&CheetahString::from_static_str("topic-a"));

        let offsets = manager.consumer_offset_wrapper.offset_table.read();
        assert!(!offsets.contains_key("topic-a@group-a"));
        assert!(offsets.contains_key("topic-a"));
        assert!(offsets.contains_key("@group-a"));
        assert!(offsets.contains_key("topic-a@"));
        assert!(offsets.contains_key("topic-a@group-a@extra"));
    }

    #[test]
    fn consumer_lag_snapshot_is_deterministic_bounded_and_fail_closed() {
        let manager = new_manager();
        {
            let mut offsets = manager.consumer_offset_wrapper.offset_table.write();
            offsets.insert(
                CheetahString::from_static_str("topic-b@group-b"),
                HashMap::from([(0, 4), (1, 8)]),
            );
            offsets.insert(
                CheetahString::from_static_str("topic-a@group-a"),
                HashMap::from([(0, 10), (1, 20)]),
            );
            offsets.insert(
                CheetahString::from_static_str("topic-c@group-c"),
                HashMap::from([(0, 1), (1, 2)]),
            );
        }

        let bounded = manager.consumer_lag_snapshot_with(2, |topic, queue_id| match (topic.as_str(), queue_id) {
            ("topic-a", 0) => Some(16),
            ("topic-a", 1) => Some(30),
            ("topic-b", 0) => Some(9),
            ("topic-b", 1) => Some(10),
            _ => None,
        });
        assert_eq!(
            bounded,
            vec![
                super::ConsumerLagObservation {
                    topic: "topic-a".to_owned(),
                    consumer_group: "group-a".to_owned(),
                    lag_messages: 16,
                },
                super::ConsumerLagObservation {
                    topic: "topic-b".to_owned(),
                    consumer_group: "group-b".to_owned(),
                    lag_messages: 7,
                },
            ]
        );

        let fail_closed = manager.consumer_lag_snapshot_with(3, |topic, queue_id| {
            (topic.as_str() != "topic-c" || queue_id == 0).then_some(10)
        });
        assert!(fail_closed.iter().all(|observation| observation.topic != "topic-c"));
    }

    #[test]
    fn consumer_lag_snapshot_applies_pop_pull_offset_and_inflight_messages() {
        let manager = new_manager();
        manager.consumer_offset_wrapper.offset_table.write().insert(
            CheetahString::from_static_str("topic-a@group-a"),
            HashMap::from([(0, 10)]),
        );
        let adjustments = super::ConsumerLagAdjustments::from([(
            CheetahString::from_static_str("topic-a@group-a"),
            HashMap::from([(
                0,
                super::ConsumerLagAdjustment {
                    pull_offset: 18,
                    inflight_messages: 3,
                },
            )]),
        )]);

        let observations = manager.consumer_lag_snapshot_for_targets_with(
            &manager.consumer_lag_targets_with_limit(1),
            &adjustments,
            |_topic, _queue_id| Some(20),
        );

        assert_eq!(
            observations,
            vec![super::ConsumerLagObservation {
                topic: "topic-a".to_owned(),
                consumer_group: "group-a".to_owned(),
                lag_messages: 5,
            }]
        );
    }

    #[test]
    fn clean_offset_by_group_removes_all_related_tables() {
        let manager = new_manager();
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");
        let other_group = CheetahString::from_static_str("group-b");

        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 10);
        manager.assign_reset_offset(&topic, &group, 0, 5);
        manager.commit_pull_offset(
            std::net::SocketAddr::from(([127, 0, 0, 1], 10911)),
            &group,
            &topic,
            0,
            12,
        );
        manager.commit_offset("127.0.0.1:10911".into(), &other_group, &topic, 0, 20);

        manager.clean_offset_by_group(&group);

        assert_eq!(manager.query_offset(&group, &topic, 0), -1);
        assert!(!manager.has_offset_reset(group.as_str(), topic.as_str(), 0));
        assert!(!manager
            .offset_table_snapshot()
            .contains_key(&CheetahString::from_static_str("topic-a@group-a")));
        assert!(manager
            .offset_table_snapshot()
            .contains_key(&CheetahString::from_static_str("topic-a@group-b")));
    }

    #[test]
    fn clear_pull_offset_removes_only_target_entry() {
        let manager = new_manager();
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");
        let other_topic = CheetahString::from_static_str("topic-b");

        manager.commit_pull_offset(
            std::net::SocketAddr::from(([127, 0, 0, 1], 10911)),
            &group,
            &topic,
            0,
            12,
        );
        manager.commit_pull_offset(
            std::net::SocketAddr::from(([127, 0, 0, 1], 10911)),
            &group,
            &other_topic,
            0,
            24,
        );

        manager.clear_pull_offset(&group, &topic);

        let pull_offsets = manager.consumer_offset_wrapper.pull_offset_table.read();
        assert!(!pull_offsets.contains_key(&CheetahString::from_static_str("topic-a@group-a")));
        assert!(pull_offsets.contains_key(&CheetahString::from_static_str("topic-b@group-a")));
    }

    #[test]
    fn clone_offset_copies_source_offsets_to_destination_group() {
        let manager = new_manager();
        let topic = CheetahString::from_static_str("topic-a");
        let src_group = CheetahString::from_static_str("group-src");
        let dest_group = CheetahString::from_static_str("group-dest");

        manager.commit_offset("127.0.0.1:10911".into(), &src_group, &topic, 0, 32);

        manager.clone_offset(&src_group, &dest_group, &topic);

        assert_eq!(manager.query_offset(&dest_group, &topic, 0), 32);
    }

    #[test]
    fn commit_offset_skips_duplicate_and_backward_updates() {
        let manager = new_manager();
        let topic = CheetahString::from_static_str("topic-a");
        let group = CheetahString::from_static_str("group-a");

        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 32);
        let after_first = manager
            .consumer_offset_wrapper
            .version_change_counter
            .load(Ordering::Acquire);

        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 32);
        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 31);

        assert_eq!(manager.query_offset(&group, &topic, 0), 32);
        assert_eq!(
            manager
                .consumer_offset_wrapper
                .version_change_counter
                .load(Ordering::Acquire),
            after_first
        );

        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 33);

        assert_eq!(manager.query_offset(&group, &topic, 0), 33);
        assert_eq!(
            manager
                .consumer_offset_wrapper
                .version_change_counter
                .load(Ordering::Acquire),
            after_first + 1
        );
    }

    #[test]
    fn data_version_snapshots_remain_immutable_after_publish() {
        let manager = new_manager();
        let initial = manager.data_version();

        manager.advance_data_version();
        let advanced = manager.data_version();

        assert_eq!(initial.counter(), 0);
        assert_eq!(advanced.counter(), 1);
        assert!(!Arc::ptr_eq(&initial, &advanced));
    }

    #[test]
    fn concurrent_commits_advance_data_version_once_per_threshold() {
        const COMMIT_COUNT: usize = 16;

        let mut manager = new_manager();
        Arc::get_mut(&mut manager.broker_config)
            .expect("test manager should own its broker config")
            .consumer_offset_update_version_step = 1;
        let manager = Arc::new(manager);
        let barrier = Arc::new(Barrier::new(COMMIT_COUNT));
        let mut workers = Vec::with_capacity(COMMIT_COUNT);

        for queue_id in 0..COMMIT_COUNT {
            let manager = manager.clone();
            let barrier = barrier.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                manager.commit_offset(
                    "127.0.0.1:10911".into(),
                    &CheetahString::from_static_str("group-a"),
                    &CheetahString::from_static_str("topic-a"),
                    queue_id as i32,
                    queue_id as i64,
                );
            }));
        }

        for worker in workers {
            worker.join().expect("consumer offset commit worker should finish");
        }

        assert_eq!(manager.data_version().counter(), COMMIT_COUNT as i64);
        assert_eq!(
            manager
                .consumer_offset_wrapper
                .version_change_counter
                .load(Ordering::Acquire),
            COMMIT_COUNT as i64
        );
        assert_eq!(manager.offset_table_snapshot()["topic-a@group-a"].len(), COMMIT_COUNT);
    }

    #[test]
    fn zero_version_update_step_does_not_panic_or_advance_version() {
        let mut manager = new_manager();
        Arc::get_mut(&mut manager.broker_config)
            .expect("test manager should own its broker config")
            .consumer_offset_update_version_step = 0;

        manager.commit_offset(
            "127.0.0.1:10911".into(),
            &CheetahString::from_static_str("group-a"),
            &CheetahString::from_static_str("topic-a"),
            0,
            1,
        );

        assert_eq!(manager.data_version().counter(), 0);
    }

    #[test]
    fn merge_offsets_from_peer_publishes_complete_version_generation() {
        let manager = new_manager();
        let local_group = CheetahString::from_static_str("group-local");
        let topic = CheetahString::from_static_str("topic-a");
        manager.commit_offset("127.0.0.1:10911".into(), &local_group, &topic, 0, 7);

        let mut remote_offsets = HashMap::new();
        remote_offsets.insert(
            CheetahString::from_static_str("topic-a@group-remote"),
            HashMap::from([(0, 19)]),
        );
        let remote_version = DataVersion::with_values(3, 4, 5);
        manager.merge_offsets_from_peer(remote_offsets, remote_version.clone());

        assert_eq!(manager.query_offset(&local_group, &topic, 0), 7);
        assert_eq!(
            manager.query_offset(&CheetahString::from_static_str("group-remote"), &topic, 0),
            19
        );
        assert_eq!(manager.data_version().as_ref(), &remote_version);
    }

    #[test]
    fn json_round_trip_preserves_version_reset_and_pull_tables_without_committed_offsets() {
        let manager = new_manager();
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");
        manager.assign_reset_offset(&topic, &group, 0, 5);
        manager.commit_pull_offset(
            std::net::SocketAddr::from(([127, 0, 0, 1], 10911)),
            &group,
            &topic,
            0,
            9,
        );
        manager.advance_data_version();

        let encoded = manager.encode();
        let restored = new_manager();
        restored.decode(encoded.as_str());

        assert_eq!(restored.data_version().counter(), 1);
        assert_eq!(restored.query_then_erase_reset_offset(&topic, &group, 0), Some(5));
        assert_eq!(
            restored
                .consumer_offset_wrapper
                .pull_offset_table
                .read()
                .get("topic-a@group-a")
                .and_then(|offsets| offsets.get(&0)),
            Some(&9)
        );
    }

    #[test]
    fn query_offsets_returns_full_queue_map() {
        let manager = new_manager();
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");

        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 12);
        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 1, 24);

        let offsets = manager.query_offsets(&group, &topic).expect("offset map should exist");
        assert_eq!(offsets.get(&0), Some(&12));
        assert_eq!(offsets.get(&1), Some(&24));
    }

    #[cfg(feature = "rocksdb_store")]
    #[test]
    fn consumer_offset_manager_persists_offsets_to_rocksdb_and_loads_after_restart() {
        use crate::config::rocksdb_manager::RocksDbBrokerConfigManager;
        use crate::config::rocksdb_manager::RocksDbBrokerConfigManagerConfig;

        let temp_dir = tempfile::TempDir::new().expect("temp dir should be created");
        let rocksdb_path = temp_dir.path().join("config").join("consumerOffsets");
        let manager: ConsumerOffsetManager<LocalFileMessageStore> =
            ConsumerOffsetManager::new_with_rocksdb_config_manager(
                Arc::new(BrokerConfig::default()),
                Arc::new(MessageStoreConfig {
                    store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
                    real_time_persist_rocksdb_config: true,
                    ..MessageStoreConfig::default()
                }),
                Arc::new(
                    RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::consumer_offset(
                        rocksdb_path.clone(),
                    ))
                    .expect("rocksdb config manager should open"),
                ),
            );
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");

        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 12);
        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 1, 24);
        manager.advance_data_version();
        manager.persist().unwrap();
        drop(manager);

        let restarted: ConsumerOffsetManager<LocalFileMessageStore> =
            ConsumerOffsetManager::new_with_rocksdb_config_manager(
                Arc::new(BrokerConfig::default()),
                Arc::new(MessageStoreConfig {
                    store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
                    real_time_persist_rocksdb_config: true,
                    ..MessageStoreConfig::default()
                }),
                Arc::new(
                    RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::consumer_offset(rocksdb_path))
                        .expect("rocksdb config manager should reopen"),
                ),
            );

        assert!(restarted.load());
        assert_eq!(restarted.query_offset(&group, &topic, 0), 12);
        assert_eq!(restarted.query_offset(&group, &topic, 1), 24);
        assert_eq!(restarted.data_version().counter(), 1);
    }

    #[cfg(feature = "rocksdb_store")]
    #[test]
    fn consumer_offset_manager_clean_group_deletes_offsets_from_rocksdb() {
        use crate::config::rocksdb_manager::RocksDbBrokerConfigManager;
        use crate::config::rocksdb_manager::RocksDbBrokerConfigManagerConfig;

        let temp_dir = tempfile::TempDir::new().expect("temp dir should be created");
        let rocksdb_path = temp_dir.path().join("config").join("consumerOffsets-delete");
        let manager: ConsumerOffsetManager<LocalFileMessageStore> =
            ConsumerOffsetManager::new_with_rocksdb_config_manager(
                Arc::new(BrokerConfig::default()),
                Arc::new(MessageStoreConfig {
                    store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
                    real_time_persist_rocksdb_config: true,
                    ..MessageStoreConfig::default()
                }),
                Arc::new(
                    RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::consumer_offset(
                        rocksdb_path.clone(),
                    ))
                    .expect("rocksdb config manager should open"),
                ),
            );
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");

        manager.commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 12);
        manager.advance_data_version();
        manager.persist().unwrap();
        manager.clean_offset_by_group(&group);
        drop(manager);

        let restarted: ConsumerOffsetManager<LocalFileMessageStore> =
            ConsumerOffsetManager::new_with_rocksdb_config_manager(
                Arc::new(BrokerConfig::default()),
                Arc::new(MessageStoreConfig {
                    store_path_root_dir: CheetahString::from_string(temp_dir.path().to_string_lossy().to_string()),
                    real_time_persist_rocksdb_config: true,
                    ..MessageStoreConfig::default()
                }),
                Arc::new(
                    RocksDbBrokerConfigManager::open(RocksDbBrokerConfigManagerConfig::consumer_offset(rocksdb_path))
                        .expect("rocksdb config manager should reopen"),
                ),
            );

        assert!(restarted.load());
        assert_eq!(restarted.query_offset(&group, &topic, 0), -1);
    }
}
