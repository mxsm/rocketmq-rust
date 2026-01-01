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

//! Topic queue table with concurrent access
//!
//! Manages topic -> broker -> queue data mappings using nested DashMap.

use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;

use crate::route::types::BrokerName;
use crate::route::types::TopicName;

/// Topic queue table: Topic -> (Broker -> QueueData)
///
/// This table maintains the mapping of topics to their queue configurations
/// across different brokers. Uses nested DashMap for fine-grained concurrency.
///
/// # Performance
/// - Read operations: O(1) average, lock-free
/// - Write operations: O(1) average, per-entry lock
/// - Memory: ~40 bytes overhead per entry (DashMap metadata)
///
/// # Example
/// ```no_run
/// use std::sync::Arc;
///
/// use rocketmq_namesrv::route::tables::TopicQueueTable;
///
/// let table = TopicQueueTable::new();
/// // Operations are thread-safe without explicit locking
/// ```
#[derive(Clone)]
pub struct TopicQueueTable {
    /// Outer map: Topic name -> Broker map
    /// Inner map: Broker name -> Queue data
    inner: DashMap<TopicName, DashMap<BrokerName, Arc<QueueData>>>,
}

impl TopicQueueTable {
    /// Create a new topic queue table
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Create with estimated capacity
    ///
    /// Pre-allocates space to avoid rehashing for better performance.
    ///
    /// # Arguments
    /// * `capacity` - Expected number of topics
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(capacity),
        }
    }

    /// Insert or update queue data for a topic-broker pair
    ///
    /// # Arguments
    /// * `topic` - Topic name (zero-copy Arc<str>)
    /// * `broker` - Broker name (zero-copy Arc<str>)
    /// * `queue_data` - Queue configuration
    ///
    /// # Returns
    /// Previous queue data if existed
    pub fn insert(&self, topic: TopicName, broker: BrokerName, queue_data: QueueData) -> Option<Arc<QueueData>> {
        self.inner
            .entry(topic)
            .or_default()
            .insert(broker, Arc::new(queue_data))
    }

    /// Get queue data for a specific topic-broker pair
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `broker` - Broker name
    ///
    /// # Returns
    /// Cloned Arc to queue data if exists
    pub fn get(&self, topic: &str, broker: &str) -> Option<Arc<QueueData>> {
        self.inner
            .get(topic)
            .and_then(|brokers| brokers.get(broker).map(|entry| Arc::clone(entry.value())))
    }

    /// Get all queue data for a topic
    ///
    /// # Arguments
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// Vector of (broker_name, queue_data) pairs
    pub fn get_topic_queues(&self, topic: &str) -> Vec<(BrokerName, Arc<QueueData>)> {
        self.inner
            .get(topic)
            .map(|brokers| {
                brokers
                    .iter()
                    .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the broker-to-queue mapping for a specific topic
    ///
    /// This method retrieves the `DashMap` containing the mapping of brokers to their
    /// respective `QueueData` for a given topic. The returned value is an `Option`
    /// that contains a reference to the inner `DashMap` if the topic exists.
    ///
    /// # Arguments
    /// * `topic` - A string slice representing the name of the topic to look up.
    ///
    /// # Returns
    /// * `Option<dashmap::mapref::one::Ref<'_, TopicName, DashMap<BrokerName, Arc<QueueData>>>>`
    ///   - `Some` if the topic exists, containing a reference to the `DashMap` of brokers and their
    ///     queue data.
    ///   - `None` if the topic does not exist.
    ///
    /// # Example
    /// ```rust,ignore
    /// let topic_map = table.get_topic_queues_map("example_topic");
    /// if let Some(broker_map) = topic_map {
    ///     // Access broker-specific queue data
    /// }
    pub fn get_topic_queues_map(
        &self,
        topic: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, TopicName, DashMap<BrokerName, Arc<QueueData>>>> {
        self.inner.get(topic)
    }

    /// Remove a broker from a topic
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `broker` - Broker name
    ///
    /// # Returns
    /// Removed queue data if existed
    pub fn remove_broker(&self, topic: &str, broker: &str) -> Option<Arc<QueueData>> {
        self.inner
            .get(topic)
            .and_then(|brokers| brokers.remove(broker).map(|(_, v)| v))
    }

    /// Remove entire topic
    ///
    /// # Arguments
    /// * `topic` - Topic name
    ///
    /// # Returns
    /// true if topic existed and was removed
    pub fn remove_topic(&self, topic: &str) -> bool {
        self.inner.remove(topic).is_some()
    }

    /// Check if topic exists
    pub fn contains_topic(&self, topic: &str) -> bool {
        self.inner.contains_key(topic)
    }

    /// Get all topic names
    ///
    /// # Returns
    /// Vector of topic names (CheetahString for zero-copy)
    pub fn get_all_topics(&self) -> Vec<TopicName> {
        self.inner.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get number of topics
    pub fn topic_count(&self) -> usize {
        self.inner.len()
    }

    /// Get total number of topic-broker pairs
    pub fn total_queue_count(&self) -> usize {
        self.inner.iter().map(|entry| entry.value().len()).sum()
    }

    /// Clear all data
    pub fn clear(&self) {
        self.inner.clear();
    }

    /// Clean up empty topic entries
    ///
    /// Removes topics that have no brokers registered.
    /// Returns number of topics removed.
    pub fn cleanup_empty_topics(&self) -> usize {
        let mut removed = 0;
        self.inner.retain(|_, brokers| {
            let is_empty = brokers.is_empty();
            if is_empty {
                removed += 1;
            }
            !is_empty
        });
        removed
    }

    /// Update queue data permission for a specific topic-broker (v1 compatibility)
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `broker_name` - Broker name
    /// * `perm` - New permission value
    ///
    /// # Returns
    /// true if update succeeded, false if topic-broker not found
    pub fn update_queue_data_perm(&self, topic: &str, broker_name: &str, perm: i32) -> bool {
        if let Some(brokers) = self.inner.get(topic) {
            if let Some(mut entry) = brokers.get_mut(broker_name) {
                // Create new QueueData with updated permission
                let old_data = entry.value();
                let new_data = QueueData::new(
                    old_data.broker_name().clone(),
                    old_data.read_queue_nums(),
                    old_data.write_queue_nums(),
                    perm as u32,
                    old_data.topic_sys_flag(),
                );
                *entry.value_mut() = Arc::new(new_data);
                return true;
            }
        }
        false
    }

    /// Iterate over all topics and their queue data (for permission updates)
    ///
    /// # Returns
    /// Iterator yielding (topic_name, vec of queue_data)
    pub fn iter_all_with_data(&self) -> Vec<(String, Vec<Arc<QueueData>>)> {
        self.inner
            .iter()
            .map(|entry| {
                let topic = entry.key().as_str().to_string();
                let queue_datas: Vec<Arc<QueueData>> = entry
                    .value()
                    .iter()
                    .map(|broker_entry| Arc::clone(broker_entry.value()))
                    .collect();
                (topic, queue_datas)
            })
            .collect()
    }
}

impl Default for TopicQueueTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    fn create_test_queue_data(read_count: u32, write_count: u32) -> QueueData {
        QueueData::new("test-broker".into(), read_count, write_count, 6, 0)
    }

    #[test]
    fn test_insert_and_get() {
        let table = TopicQueueTable::new();
        let topic: TopicName = CheetahString::from_string("TestTopic".to_string());
        let broker: BrokerName = CheetahString::from_string("broker-a".to_string());
        let queue_data = create_test_queue_data(8, 8);

        // Insert
        let old = table.insert(topic.clone(), broker.clone(), queue_data);
        assert!(old.is_none());

        // Get
        let retrieved = table.get("TestTopic", "broker-a").unwrap();
        assert_eq!(retrieved.read_queue_nums(), 8);
        assert_eq!(retrieved.write_queue_nums(), 8);
    }

    #[test]
    fn test_get_topic_queues() {
        let table = TopicQueueTable::new();
        let topic: TopicName = CheetahString::from_string("TestTopic".to_string());

        // Insert multiple brokers
        table.insert(
            topic.clone(),
            CheetahString::from_string("broker-a".to_string()),
            create_test_queue_data(8, 8),
        );
        table.insert(
            topic.clone(),
            CheetahString::from_string("broker-b".to_string()),
            create_test_queue_data(16, 16),
        );

        let queues = table.get_topic_queues("TestTopic");
        assert_eq!(queues.len(), 2);
    }

    #[test]
    fn test_remove_broker() {
        let table = TopicQueueTable::new();
        let topic: TopicName = CheetahString::from_string("TestTopic".to_string());

        table.insert(
            topic.clone(),
            CheetahString::from_string("broker-a".to_string()),
            create_test_queue_data(8, 8),
        );

        // Remove
        let removed = table.remove_broker("TestTopic", "broker-a");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().read_queue_nums(), 8);

        // Verify removed
        assert!(table.get("TestTopic", "broker-a").is_none());
    }

    #[test]
    fn test_cleanup_empty_topics() {
        let table = TopicQueueTable::new();

        // Insert and then remove broker, leaving empty topic
        let topic: TopicName = CheetahString::from_string("EmptyTopic".to_string());
        table.insert(
            topic.clone(),
            CheetahString::from_string("broker-a".to_string()),
            create_test_queue_data(8, 8),
        );
        table.remove_broker("EmptyTopic", "broker-a");

        // Cleanup
        let removed = table.cleanup_empty_topics();
        assert_eq!(removed, 1);
        assert!(!table.contains_topic("EmptyTopic"));
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let table = Arc::new(TopicQueueTable::new());
        let mut handles = vec![];

        // Spawn multiple threads
        for i in 0..10 {
            let table_clone = table.clone();
            handles.push(thread::spawn(move || {
                let topic: TopicName = CheetahString::from_string(format!("Topic{}", i % 3));
                let broker: BrokerName = CheetahString::from_string(format!("broker-{}", i));
                table_clone.insert(topic, broker, create_test_queue_data(8, 8));
            }));
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify data
        assert!(table.topic_count() <= 3); // At most 3 topics
        assert_eq!(table.total_queue_count(), 10); // 10 total entries
    }
}
