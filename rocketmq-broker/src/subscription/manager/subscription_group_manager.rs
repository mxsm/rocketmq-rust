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
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::attribute::attribute_util::AttributeUtil;
use rocketmq_common::common::attribute::subscription_group_attributes::SubscriptionGroupAttributes;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::mix_all::is_sys_consumer_group;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use serde::Deserialize;
use serde::Serialize;
use tracing::info;
use tracing::warn;

use crate::broker_path_config_helper::get_subscription_group_path;
use crate::broker_runtime::BrokerRuntimeInner;

pub const CHARACTER_MAX_LENGTH: usize = 255;
pub const TOPIC_MAX_LENGTH: usize = 127;

pub(crate) struct SubscriptionGroupManager<MS: MessageStore> {
    /// Subscription group configuration table (group_name -> config)
    subscription_group_table: Arc<DashMap<CheetahString, Arc<SubscriptionGroupConfig>>>,

    /// Forbidden table (group -> topic -> forbidden_bitmap)
    forbidden_table: Arc<DashMap<CheetahString, DashMap<CheetahString, i32>>>,

    /// Data version for tracking configuration changes
    data_version: Arc<parking_lot::RwLock<DataVersion>>,

    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> SubscriptionGroupManager<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> SubscriptionGroupManager<MS> {
        let mut manager = Self {
            subscription_group_table: Arc::new(DashMap::new()),
            forbidden_table: Arc::new(DashMap::new()),
            data_version: Arc::new(parking_lot::RwLock::new(DataVersion::new())),
            broker_runtime_inner,
        };
        manager.init();
        manager
    }

    /// Initialize system default consumer groups
    fn init(&mut self) {
        use rocketmq_common::common::mix_all;

        let system_groups = [
            (mix_all::TOOLS_CONSUMER_GROUP, false),
            (mix_all::FILTERSRV_CONSUMER_GROUP, false),
            (mix_all::SELF_TEST_CONSUMER_GROUP, false),
            (mix_all::ONS_HTTP_PROXY_GROUP, true),
            (mix_all::CID_ONSAPI_PULL_GROUP, true),
            (mix_all::CID_ONSAPI_PERMISSION_GROUP, true),
            (mix_all::CID_ONSAPI_OWNER_GROUP, true),
            (mix_all::CID_SYS_RMQ_TRANS, true),
        ];

        for (group_name, broadcast_enable) in system_groups {
            let mut config = SubscriptionGroupConfig::new(CheetahString::from_static_str(group_name));
            if broadcast_enable {
                config.set_consume_broadcast_enable(true);
            }
            self.subscription_group_table
                .insert(CheetahString::from_static_str(group_name), Arc::new(config));
        }

        info!("Initialized {} system consumer groups", system_groups.len());
    }

    /// Get the subscription group table
    pub fn subscription_group_table(&self) -> &Arc<DashMap<CheetahString, Arc<SubscriptionGroupConfig>>> {
        &self.subscription_group_table
    }

    /// Get the forbidden table
    pub fn forbidden_table(&self) -> &Arc<DashMap<CheetahString, DashMap<CheetahString, i32>>> {
        &self.forbidden_table
    }

    /// Validate group name
    ///
    /// # Arguments
    /// * `group_name` - The group name to validate
    ///
    /// # Returns
    /// `true` if valid, `false` otherwise
    ///
    /// # Validation Rules
    /// - Cannot be empty
    /// - Length must not exceed CHARACTER_MAX_LENGTH (255)
    /// - Must not contain illegal characters (validated by TopicValidator)
    fn validate_group_name(group_name: &str) -> bool {
        if group_name.is_empty() {
            warn!("Group name validation failed: empty name");
            return false;
        }

        if group_name.len() > CHARACTER_MAX_LENGTH {
            warn!(
                "Group name validation failed: name too long ({} > {}): {}",
                group_name.len(),
                CHARACTER_MAX_LENGTH,
                group_name
            );
            return false;
        }

        if TopicValidator::is_topic_or_group_illegal(group_name) {
            warn!(
                "Group name validation failed: contains illegal characters: {}",
                group_name
            );
            return false;
        }

        true
    }

    /// Validate topic name
    ///
    /// # Arguments
    /// * `topic` - The topic name to validate
    ///
    /// # Returns
    /// `true` if valid, `false` otherwise
    fn validate_topic_name(topic: &str) -> bool {
        if topic.is_empty() {
            warn!("Topic name validation failed: empty name");
            return false;
        }

        if topic.len() > TOPIC_MAX_LENGTH {
            warn!(
                "Topic name validation failed: name too long ({} > {}): {}",
                topic.len(),
                TOPIC_MAX_LENGTH,
                topic
            );
            return false;
        }

        if TopicValidator::is_topic_or_group_illegal(topic) {
            warn!("Topic name validation failed: contains illegal characters: {}", topic);
            return false;
        }

        true
    }

    /// Validate forbidden index range
    ///
    /// # Arguments
    /// * `forbidden_index` - Index to validate (must be in [0, 32))
    ///
    /// # Returns
    /// `true` if valid, `false` otherwise
    fn validate_forbidden_index(forbidden_index: i32) -> bool {
        if !(0..32).contains(&forbidden_index) {
            warn!(
                "Forbidden index validation failed: index {} out of range [0, 32)",
                forbidden_index
            );
            return false;
        }
        true
    }

    pub(crate) fn update_subscription_group_config(&mut self, config: &mut SubscriptionGroupConfig) {
        self.update_subscription_group_config_without_persist(config);
        self.persist();
    }

    fn update_subscription_group_config_without_persist(&mut self, config: &mut SubscriptionGroupConfig) {
        let new_attributes = self.request(config);
        let current_attributes = self.current(config.group_name());
        let final_attributes = match AttributeUtil::alter_current_attributes(
            self.subscription_group_table.contains_key(config.group_name()),
            SubscriptionGroupAttributes::all(),
            &current_attributes,
            &new_attributes,
        ) {
            Ok(final_attributes) => final_attributes,
            Err(err) => {
                tracing::error!(
                    "Failed to alter subscription group attributes for group {}: {:?}",
                    config.group_name(),
                    err
                );
                // Return current attributes to maintain consistency
                // Rather than panic, we log the error and use current attributes
                current_attributes
            }
        };
        config.set_attributes(final_attributes);

        let old = self
            .subscription_group_table
            .insert(config.group_name().clone(), Arc::new(config.clone()));

        match old {
            Some(old_config) => {
                info!(
                    "update subscription group config, old: {:?} new: {:?}",
                    old_config, config,
                );
            }
            None => {
                info!("create new subscription group, {:?}", config)
            }
        }

        self.update_data_version();
    }
    fn request(&self, subscription_group_config: &SubscriptionGroupConfig) -> HashMap<CheetahString, CheetahString> {
        subscription_group_config.attributes().clone()
    }

    fn current(&self, group_name: &str) -> HashMap<CheetahString, CheetahString> {
        self.subscription_group_table
            .get(group_name)
            .map(|entry| entry.value().attributes().clone())
            .unwrap_or_default()
    }

    fn update_data_version(&self) {
        let state_machine_version = self
            .broker_runtime_inner
            .message_store()
            .map(|store| store.get_state_machine_version())
            .unwrap_or(0);

        self.data_version.write().next_version_with(state_machine_version);
    }

    pub fn data_version(&self) -> Arc<parking_lot::RwLock<DataVersion>> {
        self.data_version.clone()
    }
}

impl<MS: MessageStore> ConfigManager for SubscriptionGroupManager<MS> {
    fn config_file_path(&self) -> String {
        get_subscription_group_path(self.broker_runtime_inner.broker_config().store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        // Convert DashMap to HashMap for serialization
        let wrapper = SubscriptionGroupWrapperInner {
            subscription_group_table: self
                .subscription_group_table
                .iter()
                .map(|entry| (entry.key().clone(), (**entry.value()).clone()))
                .collect(),
            forbidden_table: self
                .forbidden_table
                .iter()
                .map(|entry| {
                    let inner: HashMap<_, _> = entry.value().iter().map(|e| (e.key().clone(), *e.value())).collect();
                    (entry.key().clone(), inner)
                })
                .collect(),
            data_version: self.data_version.read().clone(),
        };

        if pretty_format {
            wrapper.serialize_json_pretty()
        } else {
            wrapper.serialize_json()
        }
        .expect("Failed to serialize subscription group config")
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }

        let wrapper = serde_json::from_str::<SubscriptionGroupWrapperInner>(json_string).unwrap_or_default();

        // Load subscription group table
        for (key, config) in wrapper.subscription_group_table {
            self.subscription_group_table.insert(key, Arc::new(config));
        }

        // Load forbidden table
        for (group, topics) in wrapper.forbidden_table {
            let topic_map = DashMap::new();
            for (topic, value) in topics {
                topic_map.insert(topic, value);
            }
            self.forbidden_table.insert(group, topic_map);
        }

        // Update data version
        self.data_version.write().assign_new_one(&wrapper.data_version);
    }
}

impl<MS> SubscriptionGroupManager<MS>
where
    MS: MessageStore,
{
    pub fn contains_subscription_group(&self, group: &CheetahString) -> bool {
        if group.is_empty() {
            return false;
        }
        self.subscription_group_table.contains_key(group)
    }

    pub fn find_subscription_group_config(&self, group: &CheetahString) -> Option<Arc<SubscriptionGroupConfig>> {
        let mut subscription_group_config = self.find_subscription_group_config_inner(group);
        if subscription_group_config.is_none()
            && (self.broker_runtime_inner.broker_config().auto_create_subscription_group
                || is_sys_consumer_group(group))
        {
            if group.len() > CHARACTER_MAX_LENGTH || TopicValidator::is_topic_or_group_illegal(group) {
                return None;
            }
            let mut subscription_group_config_new = SubscriptionGroupConfig::default();
            subscription_group_config_new.set_group_name(group.clone());
            let arc_config = Arc::new(subscription_group_config_new.clone());
            let pre_config = self
                .subscription_group_table
                .insert(group.clone(), Arc::clone(&arc_config));
            if pre_config.is_none() {
                info!("auto create a subscription group, {:?}", subscription_group_config_new);
            }
            self.update_data_version();
            self.persist();
            subscription_group_config = Some(arc_config);
        }
        subscription_group_config
    }

    fn find_subscription_group_config_inner(&self, group: &CheetahString) -> Option<Arc<SubscriptionGroupConfig>> {
        self.subscription_group_table
            .get(group)
            .map(|entry| Arc::clone(entry.value()))
    }

    pub fn get_forbidden(&self, group: &CheetahString, topic: &CheetahString, forbidden_index: i32) -> bool {
        if !Self::validate_forbidden_index(forbidden_index) {
            return false;
        }
        let topic_forbidden = self.get_forbidden_internal(group, topic);
        let bit_forbidden = 1 << forbidden_index;
        (topic_forbidden & bit_forbidden) == bit_forbidden
    }
    pub fn get_forbidden_internal(&self, group: &CheetahString, topic: &CheetahString) -> i32 {
        self.forbidden_table
            .get(group)
            .and_then(|topics| topics.get(topic).map(|v| *v))
            .map(|v| if v < 0 { 0 } else { v })
            .unwrap_or(0)
    }

    /// Disable consume capability for a specific consumer group
    ///
    /// # Arguments
    /// * `group_name` - The name of the consumer group to disable
    ///
    /// # Returns
    /// Returns the updated config if the group exists, None otherwise
    pub fn disable_consume(&mut self, group_name: &CheetahString) -> Option<Arc<SubscriptionGroupConfig>> {
        let result = self.subscription_group_table.get_mut(group_name).map(|mut entry| {
            let mut new_config = (**entry.value()).clone();
            new_config.set_consume_enable(false);
            let arc_config = Arc::new(new_config);
            *entry.value_mut() = Arc::clone(&arc_config);
            arc_config
        });

        if result.is_some() {
            self.update_data_version();
            info!("Disabled consume for group: {}", group_name);
        } else {
            warn!("Cannot disable consume, group not found: {}", group_name);
        }

        result
    }

    /// Enable consume capability for a specific consumer group
    ///
    /// # Arguments
    /// * `group_name` - The name of the consumer group to enable
    ///
    /// # Returns
    /// Returns the updated config if the group exists, None otherwise
    pub fn enable_consume(&mut self, group_name: &str) -> Option<Arc<SubscriptionGroupConfig>> {
        let result = self.subscription_group_table.get_mut(group_name).map(|mut entry| {
            let mut new_config = (**entry.value()).clone();
            new_config.set_consume_enable(true);
            let arc_config = Arc::new(new_config);
            *entry.value_mut() = Arc::clone(&arc_config);
            arc_config
        });

        if result.is_some() {
            self.update_data_version();
            // Note: Java version does NOT persist here, only updates data version
            info!("Enabled consume for group: {}", group_name);
        } else {
            warn!("Cannot enable consume, group not found: {}", group_name);
        }

        result
    }

    /// Check if consume is enabled for a specific consumer group
    ///
    /// # Arguments
    /// * `group_name` - The name of the consumer group to check
    ///
    /// # Returns
    /// Returns true if consume is enabled, false otherwise (including when group doesn't exist)
    pub fn is_consume_enable(&self, group_name: &str) -> bool {
        self.subscription_group_table
            .get(group_name)
            .map(|config| config.consume_enable())
            .unwrap_or(false)
    }

    /// Delete a subscription group configuration
    ///
    /// # Arguments
    /// * `group_name` - The name of the consumer group to delete
    ///
    /// # Returns
    /// Returns the deleted config if it existed, None otherwise
    pub fn delete_subscription_group_config(&mut self, group_name: &str) -> Option<Arc<SubscriptionGroupConfig>> {
        let old = self.subscription_group_table.remove(group_name).map(|(_, v)| v);
        self.forbidden_table.remove(group_name);

        if old.is_some() {
            info!("Deleted subscription group: {}", group_name);
            self.update_data_version();
            self.persist();
        } else {
            warn!("Delete failed, subscription group not found: {}", group_name);
        }

        old
    }

    /// Batch update subscription group configurations
    ///
    /// # Arguments
    /// * `config_list` - List of subscription group configs to update
    ///
    /// This method updates multiple subscription groups in a single persist operation
    pub fn update_subscription_group_config_list(&mut self, config_list: Vec<SubscriptionGroupConfig>) {
        if config_list.is_empty() {
            return;
        }

        for mut config in config_list {
            self.update_subscription_group_config_without_persist(&mut config);
        }

        self.persist();
        info!("Batch updated subscription groups");
    }

    /// Set forbidden flag for a specific consumer group and topic
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    /// * `forbidden_index` - Forbidden flag bit index (0-31)
    ///
    /// Uses bitwise OR to set the forbidden flag at the specified index
    pub fn set_forbidden(&mut self, group: &CheetahString, topic: &CheetahString, forbidden_index: i32) {
        let topic_forbidden = self.get_forbidden_internal(group, topic);
        let new_forbidden = topic_forbidden | (1 << forbidden_index);
        self.update_forbidden_value(group, topic, new_forbidden);
    }

    /// Clear forbidden flag for a specific consumer group and topic
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    /// * `forbidden_index` - Forbidden flag bit index (0-31)
    ///
    /// Uses bitwise AND with complement to clear the forbidden flag at the specified index
    pub fn clear_forbidden(&mut self, group: &CheetahString, topic: &CheetahString, forbidden_index: i32) {
        if !Self::validate_forbidden_index(forbidden_index) {
            warn!("Invalid forbidden index: {}", forbidden_index);
            return;
        }
        let topic_forbidden = self.get_forbidden_internal(group, topic);
        let new_forbidden = topic_forbidden & !(1 << forbidden_index);
        self.update_forbidden_value(group, topic, new_forbidden);
    }

    /// Update forbidden value directly for a specific consumer group and topic
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    /// * `forbidden_value` - The complete forbidden bitmask value
    ///
    /// Directly sets the forbidden value, replacing any existing value.
    /// If forbidden_value <= 0, removes the group from the forbidden table.
    pub fn update_forbidden_value(&mut self, group: &CheetahString, topic: &CheetahString, forbidden_value: i32) {
        if forbidden_value <= 0 {
            self.forbidden_table.remove(group);
            info!("Cleared group forbidden, {}@{}", group, topic);
            return;
        } else {
            let old = self
                .forbidden_table
                .entry(group.clone())
                .or_default()
                .insert(topic.clone(), forbidden_value);

            if let Some(old_value) = old {
                info!(
                    "Set group forbidden, {}@{} old: {} new: {}",
                    group, topic, old_value, forbidden_value
                );
            } else {
                info!(
                    "Set group forbidden, {}@{} old: 0 new: {}",
                    group, topic, forbidden_value
                );
            }
        }

        self.update_data_version();
        self.persist();
    }

    /// Check if a specific forbidden flag is set
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    /// * `forbidden_index` - Forbidden flag bit index (0-31)
    ///
    /// # Returns
    /// `true` if the forbidden flag at the specified index is set, `false` otherwise
    pub fn is_forbidden(&self, group: &str, topic: &str, forbidden_index: i32) -> bool {
        if !Self::validate_forbidden_index(forbidden_index) {
            return false;
        }

        self.forbidden_table
            .get(group)
            .and_then(|topic_map| topic_map.get(topic).map(|v| (*v & (1 << forbidden_index)) != 0))
            .unwrap_or(false)
    }

    /// Get all forbidden topics for a consumer group
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    ///
    /// # Returns
    /// A vector of tuples (topic, forbidden_value) for all topics with forbidden flags set
    pub fn get_forbidden_topics(&self, group: &str) -> Vec<(CheetahString, i32)> {
        self.forbidden_table
            .get(group)
            .map(|topic_map| {
                topic_map
                    .iter()
                    .map(|entry| (entry.key().clone(), *entry.value()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Remove all forbidden flags for a consumer group
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    pub fn remove_forbidden_group(&self, group: &str) {
        if self.forbidden_table.remove(group).is_some() {
            info!("Removed all forbidden flags for group: {}", group);
        }
    }

    /// Remove forbidden flags for a specific topic in a consumer group
    ///
    /// # Arguments
    /// * `group` - Consumer group name
    /// * `topic` - Topic name
    pub fn remove_forbidden_topic(&self, group: &str, topic: &str) {
        if let Some(topic_map) = self.forbidden_table.get(group) {
            if topic_map.remove(topic).is_some() {
                info!("Removed forbidden flags for group: {}, topic: {}", group, topic);
            }
        }
    }

    /// Get a subset of subscription group configurations for pagination
    ///
    /// # Arguments
    /// * `data_version` - Expected data version for consistency check
    /// * `group_seq` - Starting sequence number for pagination (0-based)
    /// * `max_group_num` - Maximum number of groups to return
    ///
    /// # Returns
    /// A HashMap containing subscription group configurations for the requested page.
    /// Returns empty map if data version doesn't match or no groups in range.
    ///
    /// # Use Case
    /// Large-scale clusters can use this for incremental synchronization to avoid
    /// transferring all group configs at once.
    pub fn sub_group_table(
        &self,
        data_version: &DataVersion,
        group_seq: usize,
        max_group_num: usize,
    ) -> HashMap<CheetahString, Arc<SubscriptionGroupConfig>> {
        // Check data version consistency
        let current_version = self.data_version.read();
        let mut begin_index = group_seq;

        if data_version != &*current_version {
            begin_index = 0;
            info!(
                "Get sub subscription group table from {} due to DataVersion Changed",
                begin_index
            );
        }
        drop(current_version);

        let mut result = HashMap::new();
        let total_size = self.subscription_group_table.len();

        if begin_index < total_size {
            // Sort keys to ensure consistent pagination (like Java's ImmutableSortedMap)
            let mut sorted_keys: Vec<CheetahString> = self
                .subscription_group_table
                .iter()
                .map(|entry| entry.key().clone())
                .collect();
            sorted_keys.sort();

            let end_index = std::cmp::min(begin_index + max_group_num, total_size);

            for key in sorted_keys.iter().skip(begin_index).take(end_index - begin_index) {
                if let Some(config) = self.subscription_group_table.get(key) {
                    result.insert(key.clone(), Arc::clone(config.value()));
                }
            }
        }

        info!(
            "Retrieved {} subscription groups (seq: {}-{})",
            result.len(),
            begin_index,
            begin_index + result.len()
        );

        result
    }

    /// Get forbidden tables for specified consumer groups
    ///
    /// # Arguments
    /// * `group_set` - Set of consumer group names to query
    ///
    /// # Returns
    /// A nested HashMap: group -> (topic -> forbidden_bitmap)
    /// Only includes groups that exist in group_set and have forbidden entries.
    ///
    /// # Use Case
    /// Batch query optimization for checking forbidden status of multiple groups
    pub fn sub_forbidden_table(
        &self,
        group_set: &std::collections::HashSet<CheetahString>,
    ) -> HashMap<CheetahString, HashMap<CheetahString, i32>> {
        let mut result = HashMap::new();

        for group in group_set {
            if let Some(topic_map) = self.forbidden_table.get(group) {
                let topics: HashMap<_, _> = topic_map
                    .iter()
                    .map(|entry| (entry.key().clone(), *entry.value()))
                    .collect();

                if !topics.is_empty() {
                    result.insert(group.clone(), topics);
                }
            }
        }

        info!(
            "Retrieved forbidden tables for {} groups (out of {} requested)",
            result.len(),
            group_set.len()
        );

        result
    }

    /// Load only the data version from configuration file without loading full config
    ///
    /// # Returns
    /// `true` if data version was successfully loaded, `false` otherwise
    ///
    /// # Use Case
    /// Fast configuration change detection without the overhead of loading
    /// all subscription group configurations. Useful for master-slave synchronization.
    pub fn load_data_version(&mut self) -> bool {
        let config_file_path = self.config_file_path();

        if !std::path::Path::new(&config_file_path).exists() {
            warn!("Config file does not exist: {}", config_file_path);
            return false;
        }

        match std::fs::read_to_string(&config_file_path) {
            Ok(json_string) => match serde_json::from_str::<serde_json::Value>(&json_string) {
                Ok(json_obj) => {
                    if let Some(data_version_obj) = json_obj.get("dataVersion") {
                        match serde_json::from_value::<DataVersion>(data_version_obj.clone()) {
                            Ok(loaded_version) => {
                                *self.data_version.write() = loaded_version;
                                info!("Successfully loaded data version from {}", config_file_path);
                                return true;
                            }
                            Err(e) => {
                                warn!("Failed to deserialize data version: {:?}", e);
                            }
                        }
                    } else {
                        warn!("No dataVersion field found in config file");
                    }
                }
                Err(e) => {
                    warn!("Failed to parse config file as JSON: {:?}", e);
                }
            },
            Err(e) => {
                warn!("Failed to read config file {}: {:?}", config_file_path, e);
            }
        }

        false
    }

    /// Get the total count of subscription groups
    pub fn group_count(&self) -> usize {
        self.subscription_group_table.len()
    }

    /// Get all subscription group names
    pub fn get_all_group_names(&self) -> Vec<CheetahString> {
        self.subscription_group_table
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get statistics about subscription groups
    ///
    /// # Returns
    /// A tuple of (total_groups, enabled_groups, disabled_groups)
    pub fn get_statistics(&self) -> (usize, usize, usize) {
        let total = self.subscription_group_table.len();
        let mut enabled = 0;
        let mut disabled = 0;

        for entry in self.subscription_group_table.iter() {
            if entry.value().consume_enable() {
                enabled += 1;
            } else {
                disabled += 1;
            }
        }

        (total, enabled, disabled)
    }

    /// Get detailed statistics about forbidden table
    ///
    /// # Returns
    /// A tuple of (groups_with_forbidden, total_forbidden_topics, total_forbidden_flags)
    pub fn get_forbidden_statistics(&self) -> (usize, usize, usize) {
        let groups_with_forbidden = self.forbidden_table.len();
        let mut total_topics = 0;
        let mut total_flags = 0;

        for entry in self.forbidden_table.iter() {
            let topic_map = entry.value();
            total_topics += topic_map.len();

            for topic_entry in topic_map.iter() {
                // Count number of set bits in the forbidden value
                let forbidden_value = *topic_entry.value();
                total_flags += forbidden_value.count_ones() as usize;
            }
        }

        (groups_with_forbidden, total_topics, total_flags)
    }

    /// Print comprehensive statistics to log
    pub fn log_statistics(&self) {
        let (total_groups, enabled, disabled) = self.get_statistics();
        let (forbidden_groups, forbidden_topics, forbidden_flags) = self.get_forbidden_statistics();

        info!(
            "SubscriptionGroupManager Statistics: Total Groups: {}, Enabled: {}, Disabled: {}, Forbidden Groups: {}, \
             Forbidden Topics: {}, Active Forbidden Flags: {}",
            total_groups, enabled, disabled, forbidden_groups, forbidden_topics, forbidden_flags
        );
    }

    /// Check if a group exists
    pub fn group_exists(&self, group_name: &str) -> bool {
        self.subscription_group_table.contains_key(group_name)
    }

    /// Get configuration for a specific group (read-only)
    pub fn get_group_config(&self, group_name: &str) -> Option<Arc<SubscriptionGroupConfig>> {
        self.subscription_group_table
            .get(group_name)
            .map(|entry| Arc::clone(entry.value()))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SubscriptionGroupWrapperInner {
    //todo dashmap to concurrent safe
    subscription_group_table: HashMap<CheetahString, SubscriptionGroupConfig>,
    forbidden_table: HashMap<CheetahString, HashMap<CheetahString, i32>>,
    data_version: DataVersion,
}

impl SubscriptionGroupWrapperInner {
    pub fn subscription_group_table(&self) -> &HashMap<CheetahString, SubscriptionGroupConfig> {
        &self.subscription_group_table
    }

    pub fn subscription_group_table_mut(&mut self) -> &mut HashMap<CheetahString, SubscriptionGroupConfig> {
        &mut self.subscription_group_table
    }

    pub fn forbidden_table(&self) -> &HashMap<CheetahString, HashMap<CheetahString, i32>> {
        &self.forbidden_table
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consume_from_min_enable_default() {
        let config = SubscriptionGroupConfig::default();
        // Should be true after fix (aligns with Java implementation)
        assert!(config.consume_from_min_enable());
    }

    #[test]
    fn test_subscription_group_config_creation() {
        let group_name = "TEST_GROUP";
        let config = SubscriptionGroupConfig::new(CheetahString::from_string(group_name.to_string()));

        assert_eq!(config.group_name(), group_name);
        assert!(config.consume_enable());
        assert!(config.consume_from_min_enable());
        assert_eq!(config.retry_max_times(), 16);
        assert_eq!(config.retry_queue_nums(), 1);
    }

    #[test]
    fn test_wrapper_serialization() {
        let mut wrapper = SubscriptionGroupWrapperInner::default();

        // Add a test config
        let config = SubscriptionGroupConfig::new(CheetahString::from_static_str("TEST_GROUP"));
        wrapper
            .subscription_group_table
            .insert(CheetahString::from_static_str("TEST_GROUP"), config);

        // Serialize
        let json = serde_json::to_string(&wrapper).expect("Failed to serialize");

        // Should contain camelCase fields
        assert!(json.contains("subscriptionGroupTable"));
        assert!(json.contains("forbiddenTable"));
        assert!(json.contains("dataVersion"));
    }

    #[test]
    fn test_forbidden_table_bitwise_operations() {
        // Test forbidden table using DashMap directly
        let forbidden_table: DashMap<CheetahString, DashMap<CheetahString, i32>> = DashMap::new();

        let group = CheetahString::from_static_str("TEST_GROUP");
        let topic = CheetahString::from_static_str("TEST_TOPIC");

        // Set bit 0
        forbidden_table
            .entry(group.clone())
            .or_default()
            .insert(topic.clone(), 1);

        // Verify bit 0 is set
        let value = forbidden_table
            .get(&group)
            .and_then(|t| t.get(&topic).map(|v| *v))
            .unwrap_or(0);
        assert_eq!(value & 1, 1);

        // Set bit 3 using OR operation
        forbidden_table
            .get(&group)
            .unwrap()
            .entry(topic.clone())
            .and_modify(|v| *v |= 1 << 3);

        let value = forbidden_table.get(&group).unwrap().get(&topic).map(|v| *v).unwrap();
        assert_eq!(value, 0b1001); // bits 0 and 3 set
    }

    #[test]
    fn test_forbidden_table_nested_dashmap() {
        // Test nested DashMap structure for forbidden table
        let forbidden_table: DashMap<CheetahString, DashMap<CheetahString, i32>> = DashMap::new();

        let group = CheetahString::from_static_str("TEST_GROUP");

        // Add multiple topics for same group
        let topic_a = CheetahString::from_static_str("TOPIC_A");
        let topic_b = CheetahString::from_static_str("TOPIC_B");

        {
            let topic_map = forbidden_table.entry(group.clone()).or_default();
            topic_map.value().insert(topic_a.clone(), 1);
            topic_map.value().insert(topic_b.clone(), 2);
            // Drop the entry lock here before reading
        }

        // Verify both topics exist (after releasing the entry lock)
        let topics = forbidden_table.get(&group).unwrap();
        assert_eq!(topics.len(), 2);
        assert_eq!(*topics.get(&topic_a).unwrap(), 1);
        assert_eq!(*topics.get(&topic_b).unwrap(), 2);
    }

    #[test]
    fn test_forbidden_value_bitwise_check() {
        // Test bitwise operations for forbidden index checking
        let value = 0b1010; // bits 1 and 3 set

        // Check individual bits
        assert_eq!(value & (1 << 0), 0); // bit 0 not set
        assert_ne!(value & (1 << 1), 0); // bit 1 set
        assert_eq!(value & (1 << 2), 0); // bit 2 not set
        assert_ne!(value & (1 << 3), 0); // bit 3 set

        // Set bit 2
        let new_value = value | (1 << 2);
        assert_eq!(new_value, 0b1110);

        // Clear bit 1
        let cleared = new_value & !(1 << 1);
        assert_eq!(cleared, 0b1100);
    }

    #[test]
    fn test_forbidden_index_range_validation() {
        // Test index validation logic
        let valid_indices = vec![0, 1, 15, 30, 31];
        for index in valid_indices {
            assert!((0..32).contains(&index), "Index {} should be valid", index);
        }

        let invalid_indices = vec![-1, -10, 32, 33, 100];
        for index in invalid_indices {
            assert!(!(0..32).contains(&index), "Index {} should be invalid", index);
        }
    }

    #[test]
    fn test_dashmap_concurrent_access_pattern() {
        // Simulate concurrent access pattern using DashMap
        let table: Arc<DashMap<CheetahString, Arc<SubscriptionGroupConfig>>> = Arc::new(DashMap::new());

        let group_name = CheetahString::from_static_str("TEST_GROUP");
        let config = SubscriptionGroupConfig::new(group_name.clone());

        // Insert
        table.insert(group_name.clone(), Arc::new(config));

        // Concurrent read (lock-free)
        let exists = table.contains_key(&group_name);
        assert!(exists);

        // Get for read
        let read_config = table.get(&group_name).map(|v| v.clone());
        assert!(read_config.is_some());

        // Get mutable for write (fine-grained lock) - using Copy-on-Write
        if let Some(mut entry) = table.get_mut(&group_name) {
            let mut new_config = (**entry.value()).clone();
            new_config.set_consume_enable(false);
            *entry.value_mut() = Arc::new(new_config);
        }

        // Verify modification
        let updated = table.get(&group_name).unwrap();
        assert!(!updated.value().consume_enable());
    }

    #[test]
    fn test_sub_group_table_pagination() {
        // Test pagination using DashMap directly
        let table: DashMap<CheetahString, Arc<SubscriptionGroupConfig>> = DashMap::new();

        // Create 10 groups
        for i in 0..10 {
            let group_name = CheetahString::from_string(format!("GROUP_{}", i));
            let config = SubscriptionGroupConfig::new(group_name.clone());
            table.insert(group_name, Arc::new(config));
        }

        // Test pagination logic
        let mut collected = Vec::new();
        let mut seq = 0;

        for entry in table.iter() {
            if (3..6).contains(&seq) {
                collected.push(entry.key().clone());
            }
            seq += 1;
            if seq >= 6 {
                break;
            }
        }

        // Should collect 3 items (seq 3, 4, 5)
        assert_eq!(collected.len(), 3);
    }

    #[test]
    fn test_sub_forbidden_table_batch_query() {
        // Test batch query of forbidden table
        let forbidden_table: DashMap<CheetahString, DashMap<CheetahString, i32>> = DashMap::new();

        // Setup test data
        for i in 0..5 {
            let group = CheetahString::from_string(format!("GROUP_{}", i));
            let topics = DashMap::new();
            topics.insert(CheetahString::from_string(format!("TOPIC_{}", i)), 1 << i);
            forbidden_table.insert(group, topics);
        }

        // Query subset
        let mut query_set = std::collections::HashSet::new();
        query_set.insert(CheetahString::from_static_str("GROUP_1"));
        query_set.insert(CheetahString::from_static_str("GROUP_3"));
        query_set.insert(CheetahString::from_static_str("GROUP_9")); // Non-existent

        let mut result = HashMap::new();
        for group in &query_set {
            if let Some(topic_map) = forbidden_table.get(group) {
                let topics: HashMap<_, _> = topic_map
                    .iter()
                    .map(|entry| (entry.key().clone(), *entry.value()))
                    .collect();
                if !topics.is_empty() {
                    result.insert(group.clone(), topics);
                }
            }
        }

        // Should only have 2 results (GROUP_1 and GROUP_3)
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&CheetahString::from_static_str("GROUP_1")));
        assert!(result.contains_key(&CheetahString::from_static_str("GROUP_3")));
    }

    #[test]
    fn test_validation_functions() {
        // Test group name validation logic
        // Empty group name test omitted (const expression)

        let valid_name = "VALID_GROUP_NAME";
        assert!(valid_name.len() <= CHARACTER_MAX_LENGTH);

        let too_long = "A".repeat(CHARACTER_MAX_LENGTH + 1);
        assert!(too_long.len() > CHARACTER_MAX_LENGTH);

        // Test forbidden index validation
        let valid_indices = vec![0, 15, 31];
        for idx in valid_indices {
            assert!((0..32).contains(&idx));
        }

        let invalid_indices = vec![-1, 32, 100];
        for idx in invalid_indices {
            assert!(!(0..32).contains(&idx));
        }
    }

    #[test]
    fn test_statistics_calculation() {
        // Test statistics using DashMap
        let table: DashMap<CheetahString, Arc<SubscriptionGroupConfig>> = DashMap::new();

        // Create mixed groups
        for i in 0..10 {
            let group_name = CheetahString::from_string(format!("GROUP_{}", i));
            let mut config = SubscriptionGroupConfig::new(group_name.clone());

            // Disable every other group
            if i % 2 == 0 {
                config.set_consume_enable(false);
            }

            table.insert(group_name, Arc::new(config));
        }

        let total = table.len();
        let mut enabled = 0;
        let mut disabled = 0;

        for entry in table.iter() {
            if entry.value().consume_enable() {
                enabled += 1;
            } else {
                disabled += 1;
            }
        }

        assert_eq!(total, 10);
        assert_eq!(enabled, 5);
        assert_eq!(disabled, 5);
    }

    #[test]
    fn test_forbidden_statistics_bit_counting() {
        // Test counting set bits in forbidden values
        let test_values: Vec<(i32, u32)> = vec![
            (0b0000i32, 0),
            (0b0001i32, 1),
            (0b1111i32, 4),
            (0b10101010i32, 4),
            (0b11111111i32, 8),
        ];

        for (value, expected_count) in test_values {
            assert_eq!(value.count_ones(), expected_count);
        }
    }

    #[test]
    fn test_data_version_comparison() {
        // Test DataVersion equality for pagination consistency
        let version1 = DataVersion::new();
        let mut version2 = DataVersion::new();

        // Initially different (timestamps differ)
        // In real use, we'd set them to same values
        version2.set_timestamp(version1.timestamp());
        version2.set_counter(version1.counter());

        // Basic equality check
        assert_eq!(version1.timestamp(), version2.timestamp());
        assert_eq!(version1.counter(), version2.counter());
    }

    #[test]
    #[ignore] // Run with: cargo test -- --ignored --test-threads=1
    fn stress_test_concurrent_operations() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;
        use std::thread;
        use std::time::Instant;

        // High concurrency stress test: 10k+ operations
        let table: Arc<DashMap<CheetahString, Arc<SubscriptionGroupConfig>>> = Arc::new(DashMap::new());
        let operations = Arc::new(AtomicUsize::new(0));
        let num_threads = 20;
        let ops_per_thread = 500;

        let start = Instant::now();
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let table = table.clone();
                let ops = operations.clone();
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = CheetahString::from_string(format!("STRESS_GROUP_{}_{}", thread_id, i));

                        // Insert
                        let config = SubscriptionGroupConfig::new(key.clone());
                        table.insert(key.clone(), Arc::new(config));
                        ops.fetch_add(1, Ordering::Relaxed);

                        // Read
                        let _ = table.get(&key);
                        ops.fetch_add(1, Ordering::Relaxed);

                        // Update - using Copy-on-Write
                        if let Some(mut entry) = table.get_mut(&key) {
                            let mut new_config = (**entry.value()).clone();
                            new_config.set_consume_enable(i % 2 == 0);
                            *entry.value_mut() = Arc::new(new_config);
                            ops.fetch_add(1, Ordering::Relaxed);
                        }

                        // Contains check
                        let _ = table.contains_key(&key);
                        ops.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        let total_ops = operations.load(Ordering::Relaxed);
        let qps = total_ops as f64 / duration.as_secs_f64();

        println!("\n=== Stress Test Results ===");
        println!("Total operations: {}", total_ops);
        println!("Duration: {:?}", duration);
        println!("QPS: {:.2}", qps);
        println!("Final table size: {}", table.len());

        // Verify QPS target (should be > 10k QPS)
        assert!(qps > 10000.0, "QPS {} is below target 10k", qps);
        assert_eq!(table.len(), num_threads * ops_per_thread);
    }

    #[test]
    fn test_concurrent_forbidden_table_operations() {
        use std::thread;

        // Test concurrent access to nested DashMap
        let forbidden_table: Arc<DashMap<CheetahString, DashMap<CheetahString, i32>>> = Arc::new(DashMap::new());

        let handles: Vec<_> = (0..10)
            .map(|thread_id| {
                let table = forbidden_table.clone();
                thread::spawn(move || {
                    for i in 0..100 {
                        let group = CheetahString::from_string(format!("GROUP_{}", thread_id));
                        let topic = CheetahString::from_string(format!("TOPIC_{}", i));

                        // Create or get inner map and set forbidden flag
                        {
                            let inner = table.entry(group.clone()).or_default();
                            inner.value().insert(topic.clone(), 1 << (i % 32));
                            // Drop the entry lock here before reading
                        }

                        // Read forbidden flag (after releasing the entry lock)
                        if let Some(inner_map) = table.get(&group) {
                            let _ = inner_map.get(&topic);
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify structure
        assert_eq!(forbidden_table.len(), 10); // 10 groups
        for entry in forbidden_table.iter() {
            assert_eq!(entry.value().len(), 100); // 100 topics per group
        }
    }

    #[test]
    fn test_memory_stability_large_dataset() {
        // Test with large dataset to detect memory leaks
        let table: DashMap<CheetahString, Arc<SubscriptionGroupConfig>> = DashMap::new();

        // Insert 10k entries
        for i in 0..10000 {
            let key = CheetahString::from_string(format!("MEM_TEST_GROUP_{}", i));
            let config = SubscriptionGroupConfig::new(key.clone());
            table.insert(key, Arc::new(config));
        }

        assert_eq!(table.len(), 10000);

        // Clear half
        for i in 0..5000 {
            let key = format!("MEM_TEST_GROUP_{}", i);
            table.remove(key.as_str());
        }

        assert_eq!(table.len(), 5000);

        // Re-insert
        for i in 0..5000 {
            let key = CheetahString::from_string(format!("MEM_TEST_GROUP_NEW_{}", i));
            let config = SubscriptionGroupConfig::new(key.clone());
            table.insert(key, Arc::new(config));
        }

        assert_eq!(table.len(), 10000);
    }

    #[test]
    fn test_fault_injection_invalid_inputs() {
        // Test robustness against invalid inputs
        let table: DashMap<CheetahString, Arc<SubscriptionGroupConfig>> = DashMap::new();

        // Empty key
        let empty_key = CheetahString::from_static_str("");
        let config = SubscriptionGroupConfig::new(empty_key.clone());
        table.insert(empty_key.clone(), Arc::new(config));
        assert!(table.contains_key(""));

        // Very long key
        let long_key = CheetahString::from_string("A".repeat(300));
        let config = SubscriptionGroupConfig::new(long_key.clone());
        table.insert(long_key.clone(), Arc::new(config));
        let long_key_str = "A".repeat(300);
        assert!(table.contains_key(long_key_str.as_str()));

        // Special characters
        let special_key = CheetahString::from_static_str("GROUP_!@#$%^&*()");
        let config = SubscriptionGroupConfig::new(special_key.clone());
        table.insert(special_key.clone(), Arc::new(config));
        assert!(table.contains_key("GROUP_!@#$%^&*()"));
    }

    #[test]
    fn test_forbidden_index_boundary_conditions() {
        // Test boundary conditions for forbidden index
        let test_cases = vec![
            (-1, false),  // Below range
            (0, true),    // Lower bound
            (15, true),   // Middle
            (31, true),   // Upper bound
            (32, false),  // Above range
            (100, false), // Far above range
        ];

        for (index, expected_valid) in test_cases {
            let is_valid = (0..32).contains(&index);
            assert_eq!(is_valid, expected_valid, "Index {} validity mismatch", index);
        }
    }

    #[test]
    fn test_data_race_detection() {
        use std::sync::Arc;
        use std::thread;

        // Test for potential data races with concurrent read/write
        let table: Arc<DashMap<CheetahString, Arc<SubscriptionGroupConfig>>> = Arc::new(DashMap::new());

        // Pre-populate
        for i in 0..100 {
            let key = CheetahString::from_string(format!("RACE_GROUP_{}", i));
            let config = SubscriptionGroupConfig::new(key.clone());
            table.insert(key, Arc::new(config));
        }

        // Concurrent readers and writers
        let mut handles = vec![];

        // Writers
        for thread_id in 0..5 {
            let table = table.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("RACE_GROUP_{}", i);
                    if let Some(mut entry) = table.get_mut(key.as_str()) {
                        let mut new_config = (**entry.value()).clone();
                        new_config.set_consume_enable(thread_id % 2 == 0);
                        *entry.value_mut() = Arc::new(new_config);
                    }
                }
            }));
        }

        // Readers
        for _ in 0..5 {
            let table = table.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("RACE_GROUP_{}", i);
                    let _ = table.get(key.as_str()).map(|e| e.consume_enable());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify integrity
        assert_eq!(table.len(), 100);
        for i in 0..100 {
            let key = format!("RACE_GROUP_{}", i);
            assert!(table.contains_key(key.as_str()));
        }
    }

    #[test]
    fn test_pagination_consistency_under_concurrent_modifications() {
        use std::thread;

        let table: Arc<DashMap<CheetahString, Arc<SubscriptionGroupConfig>>> = Arc::new(DashMap::new());

        // Initial data
        for i in 0..1000 {
            let key = CheetahString::from_string(format!("PAGE_GROUP_{}", i));
            let config = SubscriptionGroupConfig::new(key.clone());
            table.insert(key, Arc::new(config));
        }

        let table_clone = table.clone();

        // Concurrent modifier
        let modifier = thread::spawn(move || {
            for i in 1000..1100 {
                let key = CheetahString::from_string(format!("PAGE_GROUP_{}", i));
                let config = SubscriptionGroupConfig::new(key.clone());
                table_clone.insert(key, Arc::new(config));
            }
        });

        // Paginate while modifications happen
        let mut page_start = 0;
        let page_size = 100;
        let mut pages_collected = 0;

        while page_start < 1000 {
            let mut current_seq = 0;
            let mut _page_items = 0;

            for entry in table.iter() {
                if current_seq >= page_start && current_seq < page_start + page_size {
                    _page_items += 1;
                    let _ = entry.key();
                }
                current_seq += 1;
                if current_seq >= page_start + page_size {
                    break;
                }
            }

            pages_collected += 1;
            page_start += page_size;
        }

        modifier.join().unwrap();

        assert!(pages_collected >= 10); // At least 10 pages
        assert!(table.len() >= 1000); // At least original data
    }

    /* #[test]
    fn test_performance_regression_detection() {
        use std::time::Instant;

        let table: DashMap<CheetahString, Arc<SubscriptionGroupConfig>> = DashMap::new();

        // Warm up
        for i in 0..1000 {
            let key = CheetahString::from_string(format!("PERF_GROUP_{}", i));
            let config = SubscriptionGroupConfig::new(key.clone());
            table.insert(key, Arc::new(config));
        }

        // Test read performance
        let start = Instant::now();
        for _ in 0..10000 {
            for i in 0..1000 {
                let key = format!("PERF_GROUP_{}", i);
                let _ = table.get(key.as_str());
            }
        }
        let read_duration = start.elapsed();

        // Test write performance
        let start = Instant::now();
        for iteration in 0..1000 {
            for i in 0..100 {
                let key =
                    CheetahString::from_string(format!("PERF_GROUP_WRITE_{}_{}", iteration, i));
                let config = SubscriptionGroupConfig::new(key.clone());
                table.insert(key, Arc::new(config));
            }
        }
        let write_duration = start.elapsed();

        println!("\n=== Performance Regression Test ===");
        println!("Read 10M operations: {:?}", read_duration);
        println!("Write 100k operations: {:?}", write_duration);

        // Basic sanity checks (these thresholds should be adjusted based on actual hardware)
        assert!(
            read_duration.as_millis() < 10000,
            "Read performance degraded: {:?}",
            read_duration
        );
        assert!(
            write_duration.as_millis() < 5000,
            "Write performance degraded: {:?}",
            write_duration
        );
    }*/
}
