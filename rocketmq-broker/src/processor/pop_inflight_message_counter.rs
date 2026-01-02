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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use tracing::info;

// Type aliases for better readability
type QueueId = i32;
type InFlightCount = Arc<AtomicI64>;
type QueueCounterMap = Arc<DashMap<QueueId, InFlightCount>>;
type TopicGroupKey = CheetahString;

/// PopInflightMessageCounter tracks in-flight message counts for POP consumption model.
///
/// # Data Structure
/// - Uses three-layer nested mapping: topic@group -> queueId -> AtomicI64 counter
/// - Uses DashMap for lock-free concurrency
///
/// # Thread Safety
/// - All operations are thread-safe and use atomic operations
/// - Multiple POP requests/ACKs can be processed concurrently without data races
pub(crate) struct PopInflightMessageCounter {
    /// Timestamp threshold: messages popped before this time are ignored during decrement
    should_start_time: Arc<AtomicU64>,

    /// Main data structure: topic@group -> queueId -> counter
    topic_in_flight_message_num: Arc<DashMap<TopicGroupKey, QueueCounterMap>>,
}

impl PopInflightMessageCounter {
    const TOPIC_GROUP_SEPARATOR: &'static str = "@";

    /// Creates a new PopInflightMessageCounter.
    ///
    /// # Arguments
    /// * `should_start_time` - Timestamp threshold for filtering old POP operations
    pub fn new(should_start_time: Arc<AtomicU64>) -> Self {
        PopInflightMessageCounter {
            should_start_time,
            topic_in_flight_message_num: Arc::new(DashMap::with_capacity(512)),
        }
    }

    /// Increments the in-flight message count for a specific topic, group, and queue.
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `group` - Consumer group name
    /// * `queue_id` - Queue ID
    /// * `num` - Number of messages to increment (must be > 0)
    ///
    /// # Behavior
    /// - If num <= 0, no operation is performed
    /// - Creates nested maps if they don't exist
    /// - Uses atomic fetch_add for thread-safe increment
    pub fn increment_in_flight_message_num(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: QueueId,
        num: i64,
    ) {
        if num <= 0 {
            return;
        }

        let key = Self::build_key(topic, group);

        // Get or create queue map for this topic@group
        let queue_map = self
            .topic_in_flight_message_num
            .entry(key)
            .or_insert_with(|| Arc::new(DashMap::with_capacity(8)));

        // Get or create counter for this queue
        let counter = queue_map.entry(queue_id).or_insert_with(|| Arc::new(AtomicI64::new(0)));

        counter.fetch_add(num, Ordering::SeqCst);
    }

    /// Decrements the in-flight message count with timestamp validation.
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `group` - Consumer group name
    /// * `pop_time` - Timestamp when message was popped
    /// * `queue_id` - Queue ID
    /// * `delta` - Number of messages to decrement
    ///
    /// # Behavior
    /// - Validates pop_time >= should_start_time (ignores old operations)
    /// - Delegates to internal decrement method
    pub fn decrement_in_flight_message_num(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        pop_time: i64,
        queue_id: QueueId,
        delta: i64,
    ) {
        if pop_time < self.should_start_time.load(Ordering::SeqCst) as i64 {
            return;
        }
        self.decrement_in_flight_message_num_internal(topic, group, queue_id, delta);
    }

    /// Decrements the in-flight message count using a PopCheckPoint.
    ///
    /// # Arguments
    /// * `check_point` - PopCheckPoint containing message metadata
    ///
    /// # Behavior
    /// - Extracts topic, group, queueId from checkpoint
    /// - Validates checkpoint.pop_time >= should_start_time
    /// - Always decrements by 1 (represents single message ACK)
    pub fn decrement_in_flight_message_num_checkpoint(&self, check_point: &PopCheckPoint) {
        if check_point.pop_time < self.should_start_time.load(Ordering::SeqCst) as i64 {
            return;
        }
        self.decrement_in_flight_message_num_internal(&check_point.topic, &check_point.cid, check_point.queue_id, 1);
    }

    /// Internal method to decrement counter with automatic cleanup.
    ///
    /// # Behavior
    /// - Uses atomic fetch_sub for thread-safe decrement
    /// - Prevents negative counts (removes entry if count <= 0)
    /// - Auto-cleanup: removes empty queue maps and topic@group entries
    fn decrement_in_flight_message_num_internal(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: QueueId,
        delta: i64,
    ) {
        let key = Self::build_key(topic, group);

        // Step 1: Decrement and check if should remove
        let should_remove_queue = {
            if let Some(queue_map) = self.topic_in_flight_message_num.get(&key) {
                if let Some(counter_ref) = queue_map.get(&queue_id) {
                    let old_value = counter_ref.fetch_sub(delta, Ordering::SeqCst);
                    old_value - delta <= 0
                } else {
                    false
                }
            } else {
                return; // Key doesn't exist, nothing to do
            }
        };

        // Step 2: Remove queue entry if count <= 0 (no locks held)
        if should_remove_queue {
            let is_empty = {
                if let Some(queue_map) = self.topic_in_flight_message_num.get(&key) {
                    queue_map.remove(&queue_id);
                    queue_map.is_empty()
                } else {
                    false
                }
            };

            // Step 3: Remove topic@group entry if no queues remain
            if is_empty {
                self.topic_in_flight_message_num.remove(&key);
            }
        }
    }

    /// Clears all in-flight message counts for a specific consumer group.
    ///
    /// # Arguments
    /// * `group` - Consumer group name to clear
    ///
    /// # Behavior
    /// - Iterates through all topic@group keys
    /// - Removes entries where group matches (exact string comparison)
    /// - Logs each removed entry for audit
    pub fn clear_in_flight_message_num_by_group_name(&self, group: &CheetahString) {
        // Collect keys to avoid holding lock during iteration
        let keys: Vec<TopicGroupKey> = self
            .topic_in_flight_message_num
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some((topic, group_name)) = Self::split_key(&key) {
                if &group_name == group {
                    self.topic_in_flight_message_num.remove(&key);
                    info!(
                        "PopInflightMessageCounter#clearInFlightMessageNumByGroupName: clean by group, topic={}, \
                         group={}",
                        topic, group_name
                    );
                }
            }
        }
    }

    /// Clears all in-flight message counts for a specific topic.
    ///
    /// # Arguments
    /// * `topic` - Topic name to clear
    ///
    /// # Behavior
    /// - Iterates through all topic@group keys
    /// - Removes entries where topic matches (exact string comparison)
    /// - Logs each removed entry for audit
    pub fn clear_in_flight_message_num_by_topic_name(&self, topic: &CheetahString) {
        let keys: Vec<TopicGroupKey> = self
            .topic_in_flight_message_num
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys {
            if let Some((topic_name, group)) = Self::split_key(&key) {
                if topic_name.as_str() == topic.as_str() {
                    self.topic_in_flight_message_num.remove(&key);
                    info!(
                        "PopInflightMessageCounter#clearInFlightMessageNumByTopicName: clean by topic, topic={}, \
                         group={}",
                        topic_name, group
                    );
                }
            }
        }
    }

    /// Clears the in-flight message count for a specific queue.
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `group` - Consumer group name
    /// * `queue_id` - Queue ID to clear
    ///
    /// # Behavior
    /// - Removes counter for specific queue
    /// - Auto-cleanup: removes topic@group entry if no queues remain
    pub fn clear_in_flight_message_num(&self, topic: &CheetahString, group: &CheetahString, queue_id: QueueId) {
        let key = Self::build_key(topic, group);

        let should_remove_key = {
            if let Some(queue_map) = self.topic_in_flight_message_num.get(&key) {
                queue_map.remove(&queue_id);
                queue_map.is_empty()
            } else {
                false
            }
        };

        if should_remove_key {
            self.topic_in_flight_message_num.remove(&key);
        }
    }

    /// Gets the current in-flight message count for a specific queue.
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `group` - Consumer group name
    /// * `queue_id` - Queue ID
    ///
    /// # Returns
    /// Current in-flight count (always >= 0), or 0 if no entry exists
    ///
    /// # Behavior
    /// - Returns 0 if topic@group or queue_id doesn't exist
    /// - Prevents negative returns by using max(0, counter)
    pub fn get_group_pop_in_flight_message_num(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: QueueId,
    ) -> i64 {
        let key = Self::build_key(topic, group);

        if let Some(queue_map) = self.topic_in_flight_message_num.get(&key) {
            if let Some(counter) = queue_map.get(&queue_id) {
                // Prevent negative return (matches Java's Math.max(0, counter.get()))
                return counter.load(Ordering::SeqCst).max(0);
            }
        }

        0
    }

    /// Splits a topic@group key into its components.
    ///
    /// # Arguments
    /// * `key` - Combined key in format "topic@group"
    ///
    /// # Returns
    /// * `Some((topic, group))` if key is valid
    /// * `None` if key format is invalid
    fn split_key(key: &CheetahString) -> Option<(CheetahString, CheetahString)> {
        let parts: Vec<&str> = key.split(Self::TOPIC_GROUP_SEPARATOR).collect();
        if parts.len() == 2 {
            Some((parts[0].into(), parts[1].into()))
        } else {
            None
        }
    }

    /// Builds a combined topic@group key.
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `group` - Consumer group name
    ///
    /// # Returns
    /// Combined key in format "topic@group"
    fn build_key(topic: &CheetahString, group: &CheetahString) -> CheetahString {
        format!("{}{}{}", topic, Self::TOPIC_GROUP_SEPARATOR, group).into()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::thread;

    use cheetah_string::CheetahString;

    use super::*;

    fn setup_counter() -> PopInflightMessageCounter {
        PopInflightMessageCounter::new(Arc::new(AtomicU64::new(0)))
    }

    // ============ Basic Increment/Decrement Tests ============

    #[test]
    fn increment_in_flight_message_num_increments_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        counter.increment_in_flight_message_num(&topic, &group, 1, 5);

        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 5);
    }

    #[test]
    fn increment_with_zero_or_negative_num_does_nothing() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        counter.increment_in_flight_message_num(&topic, &group, 1, 0);
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 0);

        counter.increment_in_flight_message_num(&topic, &group, 1, -5);
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 0);
    }

    #[test]
    fn decrement_in_flight_message_num_decrements_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.decrement_in_flight_message_num(&topic, &group, 0, 1, 3);

        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 2);
    }

    #[test]
    fn decrement_to_zero_removes_entry() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.decrement_in_flight_message_num(&topic, &group, 0, 1, 5);

        // Entry should be removed, count returns 0
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 0);
    }

    #[test]
    fn decrement_below_zero_prevents_negative_count() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.decrement_in_flight_message_num(&topic, &group, 0, 1, 10);

        // Should not go negative (matches Java's Math.max(0, ...))
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 0);
    }

    // ============ Timestamp Validation Tests ============

    #[test]
    fn decrement_ignores_old_pop_time() {
        let counter = PopInflightMessageCounter::new(Arc::new(AtomicU64::new(1000)));
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        counter.increment_in_flight_message_num(&topic, &group, 1, 10);

        // pop_time < should_start_time, should be ignored
        counter.decrement_in_flight_message_num(&topic, &group, 500, 1, 5);

        assert_eq!(
            counter.get_group_pop_in_flight_message_num(&topic, &group, 1),
            10 // No change
        );

        // pop_time >= should_start_time, should work
        counter.decrement_in_flight_message_num(&topic, &group, 1000, 1, 5);

        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 5);
    }

    #[test]
    fn checkpoint_decrement_ignores_old_pop_time() {
        let counter = PopInflightMessageCounter::new(Arc::new(AtomicU64::new(1000)));
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        counter.increment_in_flight_message_num(&topic, &group, 1, 10);

        let old_checkpoint = PopCheckPoint {
            start_offset: 0,
            topic: topic.clone(),
            cid: group.clone(),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            queue_id: 1,
            pop_time: 500, // Old timestamp
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            re_put_times: None,
        };

        counter.decrement_in_flight_message_num_checkpoint(&old_checkpoint);

        assert_eq!(
            counter.get_group_pop_in_flight_message_num(&topic, &group, 1),
            10 // No change
        );
    }

    // ============ Clear Operations Tests ============

    #[test]
    fn clear_in_flight_message_num_by_group_name_clears_correctly() {
        let counter = setup_counter();
        let topic1 = CheetahString::from("topic1");
        let topic2 = CheetahString::from("topic2");
        let group = CheetahString::from("test_group");
        let other_group = CheetahString::from("other_group");

        counter.increment_in_flight_message_num(&topic1, &group, 1, 5);
        counter.increment_in_flight_message_num(&topic2, &group, 1, 3);
        counter.increment_in_flight_message_num(&topic1, &other_group, 1, 7);

        counter.clear_in_flight_message_num_by_group_name(&group);

        // test_group entries should be removed
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic1, &group, 1), 0);
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic2, &group, 1), 0);

        // other_group should remain
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic1, &other_group, 1), 7);
    }

    #[test]
    fn clear_in_flight_message_num_by_topic_name_clears_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let other_topic = CheetahString::from("other_topic");
        let group1 = CheetahString::from("group1");
        let group2 = CheetahString::from("group2");

        counter.increment_in_flight_message_num(&topic, &group1, 1, 5);
        counter.increment_in_flight_message_num(&topic, &group2, 1, 3);
        counter.increment_in_flight_message_num(&other_topic, &group1, 1, 7);

        counter.clear_in_flight_message_num_by_topic_name(&topic);

        // test_topic entries should be removed
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group1, 1), 0);
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group2, 1), 0);

        // other_topic should remain
        assert_eq!(counter.get_group_pop_in_flight_message_num(&other_topic, &group1, 1), 7);
    }

    #[test]
    fn clear_in_flight_message_num_clears_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.increment_in_flight_message_num(&topic, &group, 2, 3);

        counter.clear_in_flight_message_num(&topic, &group, 1);

        // Queue 1 should be cleared
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 0);

        // Queue 2 should remain
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 2), 3);
    }

    // ============ Checkpoint Tests ============

    #[test]
    fn decrement_in_flight_message_num_checkpoint_decrements_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        let checkpoint = PopCheckPoint {
            start_offset: 0,
            topic: topic.clone(),
            cid: group.clone(),
            revive_offset: 0,
            queue_offset_diff: vec![],
            broker_name: None,
            queue_id: 1,
            pop_time: 0,
            invisible_time: 0,
            bit_map: 0,
            num: 0,
            re_put_times: None,
        };

        counter.increment_in_flight_message_num(&topic, &group, 1, 5);
        counter.decrement_in_flight_message_num_checkpoint(&checkpoint);

        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 4);
    }

    // ============ Topic/Group Isolation Tests ============

    #[test]
    fn different_topics_are_isolated() {
        let counter = setup_counter();
        let topic1 = CheetahString::from("topic1");
        let topic2 = CheetahString::from("topic2");
        let group = CheetahString::from("group");

        counter.increment_in_flight_message_num(&topic1, &group, 1, 10);
        counter.increment_in_flight_message_num(&topic2, &group, 1, 20);

        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic1, &group, 1), 10);
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic2, &group, 1), 20);
    }

    #[test]
    fn different_groups_are_isolated() {
        let counter = setup_counter();
        let topic = CheetahString::from("topic");
        let group1 = CheetahString::from("group1");
        let group2 = CheetahString::from("group2");

        counter.increment_in_flight_message_num(&topic, &group1, 1, 10);
        counter.increment_in_flight_message_num(&topic, &group2, 1, 20);

        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group1, 1), 10);
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group2, 1), 20);
    }

    #[test]
    fn different_queues_are_isolated() {
        let counter = setup_counter();
        let topic = CheetahString::from("topic");
        let group = CheetahString::from("group");

        counter.increment_in_flight_message_num(&topic, &group, 1, 10);
        counter.increment_in_flight_message_num(&topic, &group, 2, 20);
        counter.increment_in_flight_message_num(&topic, &group, 3, 30);

        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 10);
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 2), 20);
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 3), 30);
    }

    // ============ Concurrent Operations Tests ============

    #[test]
    fn concurrent_increments_are_thread_safe() {
        let counter = Arc::new(setup_counter());
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        let mut handles = vec![];

        for _ in 0..10 {
            let counter_clone = Arc::clone(&counter);
            let topic_clone = topic.clone();
            let group_clone = group.clone();

            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    counter_clone.increment_in_flight_message_num(&topic_clone, &group_clone, 1, 1);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 10 threads * 100 increments = 1000
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 1000);
    }

    #[test]
    fn concurrent_increments_and_decrements_are_consistent() {
        let counter = Arc::new(setup_counter());
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        // Pre-populate
        counter.increment_in_flight_message_num(&topic, &group, 1, 1000);

        let mut handles = vec![];

        // 5 threads incrementing
        for _ in 0..5 {
            let counter_clone = Arc::clone(&counter);
            let topic_clone = topic.clone();
            let group_clone = group.clone();

            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    counter_clone.increment_in_flight_message_num(&topic_clone, &group_clone, 1, 1);
                }
            });

            handles.push(handle);
        }

        // 5 threads decrementing
        for _ in 0..5 {
            let counter_clone = Arc::clone(&counter);
            let topic_clone = topic.clone();
            let group_clone = group.clone();

            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    counter_clone.decrement_in_flight_message_num(&topic_clone, &group_clone, 0, 1, 1);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 1000 + (5*100) - (5*100) = 1000
        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 1), 1000);
    }

    // ============ Edge Cases Tests ============

    #[test]
    fn get_nonexistent_entry_returns_zero() {
        let counter = setup_counter();
        let topic = CheetahString::from("nonexistent_topic");
        let group = CheetahString::from("nonexistent_group");

        assert_eq!(counter.get_group_pop_in_flight_message_num(&topic, &group, 99), 0);
    }

    #[test]
    fn large_increment_values_work_correctly() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        counter.increment_in_flight_message_num(&topic, &group, 1, i64::MAX / 2);
        counter.increment_in_flight_message_num(&topic, &group, 1, 100);

        assert_eq!(
            counter.get_group_pop_in_flight_message_num(&topic, &group, 1),
            i64::MAX / 2 + 100
        );
    }

    #[test]
    fn multiple_queue_operations() {
        let counter = setup_counter();
        let topic = CheetahString::from("test_topic");
        let group = CheetahString::from("test_group");

        // Operate on multiple queues
        for queue_id in 0..10 {
            counter.increment_in_flight_message_num(&topic, &group, queue_id, queue_id as i64);
        }

        // Verify each queue
        for queue_id in 0..10 {
            assert_eq!(
                counter.get_group_pop_in_flight_message_num(&topic, &group, queue_id),
                queue_id as i64
            );
        }
    }

    #[test]
    fn key_building_and_splitting_work_correctly() {
        let topic = CheetahString::from("my_topic");
        let group = CheetahString::from("my_group");

        let key = PopInflightMessageCounter::build_key(&topic, &group);
        assert_eq!(key.as_str(), "my_topic@my_group");

        let (split_topic, split_group) = PopInflightMessageCounter::split_key(&key).unwrap();
        assert_eq!(split_topic, topic);
        assert_eq!(split_group, group);
    }

    #[test]
    fn split_key_handles_invalid_keys() {
        let invalid_key1 = CheetahString::from("no_separator");
        let invalid_key2 = CheetahString::from("too@many@separators");

        assert!(PopInflightMessageCounter::split_key(&invalid_key1).is_none());
        assert!(PopInflightMessageCounter::split_key(&invalid_key2).is_none());
    }
}
