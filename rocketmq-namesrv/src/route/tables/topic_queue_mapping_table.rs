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

//! Topic queue mapping info table with concurrent access
//!
//! Manages topic queue mapping information for static topics.

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_remoting::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;

use crate::route::types::BrokerName;
use crate::route::types::TopicName;

/// Topic queue mapping info table: Topic -> (BrokerName -> TopicQueueMappingInfo)
///
/// This table maintains the mapping information for static topics, which allows
/// topics to be partitioned across multiple brokers with custom queue mappings.
///
/// # Performance
/// - Read operations: O(1) average for topic, O(1) for broker lookup
/// - Write operations: O(1) average, per-topic lock
/// - Concurrent access: Lock-free reads, per-topic lock for writes
///
/// # Example
/// ```no_run
/// use cheetah_string::CheetahString;
/// use rocketmq_namesrv::route::tables::TopicQueueMappingInfoTable;
/// use rocketmq_remoting::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;
///
/// let table = TopicQueueMappingInfoTable::new();
/// let topic = CheetahString::from_static_str("test-topic");
/// let broker = CheetahString::from_static_str("broker-a");
/// let mapping_info = TopicQueueMappingInfo::default();
/// table.register(topic, broker, mapping_info.into());
/// ```
#[derive(Clone)]
pub struct TopicQueueMappingInfoTable {
    // Outer map: Topic -> Inner map
    // Inner map: BrokerName -> TopicQueueMappingInfo
    inner: DashMap<TopicName, DashMap<BrokerName, Arc<TopicQueueMappingInfo>>>,
}

impl Default for TopicQueueMappingInfoTable {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicQueueMappingInfoTable {
    /// Create a new topic queue mapping info table
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Create with estimated capacity
    ///
    /// # Arguments
    /// * `capacity` - Expected number of topics with mappings
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(capacity),
        }
    }

    /// Register or update topic queue mapping info for a broker
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `broker_name` - Broker name
    /// * `mapping_info` - Topic queue mapping information (zero-copy Arc)
    ///
    /// # Returns
    /// Previous mapping info if existed
    pub fn register(
        &self,
        topic: TopicName,
        broker_name: BrokerName,
        mapping_info: Arc<TopicQueueMappingInfo>,
    ) -> Option<Arc<TopicQueueMappingInfo>> {
        self.inner.entry(topic).or_default().insert(broker_name, mapping_info)
    }

    /// Get topic queue mapping info for a specific broker
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `broker_name` - Broker name
    ///
    /// # Returns
    /// Mapping info if found
    pub fn get(&self, topic: &str, broker_name: &str) -> Option<Arc<TopicQueueMappingInfo>> {
        self.inner
            .get(topic)
            .and_then(|broker_map| broker_map.get(broker_name).map(|v| v.clone()))
    }

    /// Get all mapping info for a topic
    ///
    /// # Arguments
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// HashMap of BrokerName -> TopicQueueMappingInfo if found
    pub fn get_topic_mappings(&self, topic: &str) -> Option<HashMap<CheetahString, TopicQueueMappingInfo>> {
        self.inner.get(topic).map(|broker_map| {
            broker_map
                .iter()
                .map(|entry| (entry.key().clone(), (**entry.value()).clone()))
                .collect()
        })
    }

    /// Remove mapping info for a specific broker in a topic
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `broker_name` - Broker name
    ///
    /// # Returns
    /// Removed mapping info if existed
    pub fn remove_broker(&self, topic: &str, broker_name: &str) -> Option<Arc<TopicQueueMappingInfo>> {
        self.inner
            .get(topic)
            .and_then(|broker_map| broker_map.remove(broker_name).map(|(_, v)| v))
    }

    /// Remove all mapping info for a topic
    ///
    /// # Arguments
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Removed broker map if existed
    pub fn remove_topic(&self, topic: &str) -> Option<DashMap<BrokerName, Arc<TopicQueueMappingInfo>>> {
        self.inner.remove(topic).map(|(_, v)| v)
    }

    /// Check if topic has any mapping info
    ///
    /// # Arguments
    /// * `topic` - Topic name
    pub fn contains_topic(&self, topic: &str) -> bool {
        self.inner.contains_key(topic)
    }

    /// Check if topic has mapping info for a specific broker
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `broker_name` - Broker name
    pub fn contains(&self, topic: &str, broker_name: &str) -> bool {
        self.inner
            .get(topic)
            .is_some_and(|broker_map| broker_map.contains_key(broker_name))
    }

    /// Check if the table is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get the number of topics with mapping info
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Clear all mapping info
    pub fn clear(&self) {
        self.inner.clear();
    }

    /// Get all topics with mapping info
    ///
    /// # Returns
    /// Vector of topic names
    pub fn get_all_topics(&self) -> Vec<TopicName> {
        self.inner.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get all broker names for a topic
    ///
    /// # Arguments
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Vector of broker names if topic exists
    pub fn get_brokers_for_topic(&self, topic: &str) -> Option<Vec<BrokerName>> {
        self.inner
            .get(topic)
            .map(|broker_map| broker_map.iter().map(|entry| entry.key().clone()).collect())
    }

    /// Cleanup empty topics (topics with no broker mappings)
    ///
    /// # Returns
    /// Number of topics removed
    pub fn cleanup_empty_topics(&self) -> usize {
        let mut removed_count = 0;
        self.inner.retain(|_, broker_map| {
            if broker_map.is_empty() {
                removed_count += 1;
                false
            } else {
                true
            }
        });
        removed_count
    }

    /// Iterate over all topic mappings
    ///
    /// # Returns
    /// Vector of (TopicName, HashMap<BrokerName, TopicQueueMappingInfo>)
    pub fn iter_all(&self) -> Vec<(TopicName, HashMap<BrokerName, Arc<TopicQueueMappingInfo>>)> {
        self.inner
            .iter()
            .map(|entry| {
                let broker_map: HashMap<BrokerName, Arc<TopicQueueMappingInfo>> = entry
                    .value()
                    .iter()
                    .map(|broker_entry| (broker_entry.key().clone(), broker_entry.value().clone()))
                    .collect();
                (entry.key().clone(), broker_map)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_mapping_info() -> TopicQueueMappingInfo {
        TopicQueueMappingInfo {
            topic: Some(CheetahString::from_static_str("test-topic")),
            total_queues: 16,
            bname: Some(CheetahString::from_static_str("broker-a")),
            epoch: 1,
            dirty: false,
            curr_id_map: Some(HashMap::new()),
            scope: None,
        }
    }

    #[test]
    fn test_topic_queue_mapping_info_table_basic_operations() {
        let table = TopicQueueMappingInfoTable::new();

        let topic = CheetahString::from_static_str("test-topic");
        let broker = CheetahString::from_static_str("broker-a");
        let mapping_info = Arc::new(create_test_mapping_info());

        // Test register
        assert!(table
            .register(topic.clone(), broker.clone(), mapping_info.clone())
            .is_none());
        assert_eq!(table.len(), 1);

        // Test get
        let result = table.get(&topic, &broker);
        assert!(result.is_some());
        assert_eq!(result.unwrap().topic, Some(topic.clone()));

        // Test contains
        assert!(table.contains(&topic, &broker));
        assert!(table.contains_topic(&topic));

        // Test remove broker
        let removed = table.remove_broker(&topic, &broker);
        assert!(removed.is_some());
        assert!(!table.contains(&topic, &broker));
    }

    #[test]
    fn test_topic_queue_mapping_info_table_multiple_brokers() {
        let table = TopicQueueMappingInfoTable::new();

        let topic = CheetahString::from_static_str("test-topic");
        let broker1 = CheetahString::from_static_str("broker-a");
        let broker2 = CheetahString::from_static_str("broker-b");

        let mut info1 = create_test_mapping_info();
        info1.bname = Some(broker1.clone());
        let mut info2 = create_test_mapping_info();
        info2.bname = Some(broker2.clone());

        table.register(topic.clone(), broker1.clone(), Arc::new(info1));
        table.register(topic.clone(), broker2.clone(), Arc::new(info2));

        // Check both brokers exist for the topic
        assert!(table.contains(&topic, &broker1));
        assert!(table.contains(&topic, &broker2));

        let brokers = table.get_brokers_for_topic(&topic).unwrap();
        assert_eq!(brokers.len(), 2);
    }

    #[test]
    fn test_topic_queue_mapping_info_table_get_topic_mappings() {
        let table = TopicQueueMappingInfoTable::new();

        let topic = CheetahString::from_static_str("test-topic");
        let broker1 = CheetahString::from_static_str("broker-a");
        let broker2 = CheetahString::from_static_str("broker-b");

        let mut info1 = create_test_mapping_info();
        info1.bname = Some(broker1.clone());
        let mut info2 = create_test_mapping_info();
        info2.bname = Some(broker2.clone());

        table.register(topic.clone(), broker1.clone(), Arc::new(info1));
        table.register(topic.clone(), broker2.clone(), Arc::new(info2));

        let mappings = table.get_topic_mappings(&topic).unwrap();
        assert_eq!(mappings.len(), 2);
        assert!(mappings.contains_key(&broker1));
        assert!(mappings.contains_key(&broker2));
    }

    #[test]
    fn test_topic_queue_mapping_info_table_remove_topic() {
        let table = TopicQueueMappingInfoTable::new();

        let topic = CheetahString::from_static_str("test-topic");
        let broker = CheetahString::from_static_str("broker-a");
        let mapping_info = Arc::new(create_test_mapping_info());

        table.register(topic.clone(), broker, mapping_info);
        assert!(table.contains_topic(&topic));

        let removed = table.remove_topic(&topic);
        assert!(removed.is_some());
        assert!(!table.contains_topic(&topic));
        assert!(table.is_empty());
    }

    #[test]
    fn test_topic_queue_mapping_info_table_cleanup_empty_topics() {
        let table = TopicQueueMappingInfoTable::new();

        let topic1 = CheetahString::from_static_str("test-topic-1");
        let topic2 = CheetahString::from_static_str("test-topic-2");
        let broker = CheetahString::from_static_str("broker-a");
        let mapping_info = Arc::new(create_test_mapping_info());

        table.register(topic1.clone(), broker.clone(), mapping_info.clone());
        table.register(topic2.clone(), broker.clone(), mapping_info);

        // Remove all brokers from topic1
        table.remove_broker(&topic1, &broker);

        // Cleanup should remove topic1 (empty) but keep topic2
        let removed_count = table.cleanup_empty_topics();
        assert_eq!(removed_count, 1);
        assert!(!table.contains_topic(&topic1));
        assert!(table.contains_topic(&topic2));
    }

    #[test]
    fn test_topic_queue_mapping_info_table_get_all_topics() {
        let table = TopicQueueMappingInfoTable::new();

        let topic1 = CheetahString::from_static_str("test-topic-1");
        let topic2 = CheetahString::from_static_str("test-topic-2");
        let broker = CheetahString::from_static_str("broker-a");
        let mapping_info = Arc::new(create_test_mapping_info());

        table.register(topic1.clone(), broker.clone(), mapping_info.clone());
        table.register(topic2.clone(), broker, mapping_info);

        let topics = table.get_all_topics();
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&topic1) || topics.contains(&topic2));
    }

    #[test]
    fn test_topic_queue_mapping_info_table_clear() {
        let table = TopicQueueMappingInfoTable::new();

        let topic = CheetahString::from_static_str("test-topic");
        let broker = CheetahString::from_static_str("broker-a");
        let mapping_info = Arc::new(create_test_mapping_info());

        table.register(topic, broker, mapping_info);
        assert_eq!(table.len(), 1);

        table.clear();
        assert!(table.is_empty());
    }

    #[test]
    fn test_topic_queue_mapping_info_table_iter_all() {
        let table = TopicQueueMappingInfoTable::new();

        let topic = CheetahString::from_static_str("test-topic");
        let broker1 = CheetahString::from_static_str("broker-a");
        let broker2 = CheetahString::from_static_str("broker-b");

        let mut info1 = create_test_mapping_info();
        info1.bname = Some(broker1.clone());
        let mut info2 = create_test_mapping_info();
        info2.bname = Some(broker2.clone());

        table.register(topic.clone(), broker1, Arc::new(info1));
        table.register(topic.clone(), broker2, Arc::new(info2));

        let all_mappings = table.iter_all();
        assert_eq!(all_mappings.len(), 1);
        assert_eq!(all_mappings[0].1.len(), 2);
    }
}
