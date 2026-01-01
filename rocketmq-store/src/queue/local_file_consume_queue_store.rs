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

#![allow(unused_variables)]

use std::any::Any;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use futures_util::future::join_all;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::info;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::message_store::MessageStore;
use crate::config::message_store_config::MessageStoreConfig;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::queue::batch_consume_queue::BatchConsumeQueue;
use crate::queue::consume_queue::ConsumeQueueTrait;
use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::single_consume_queue::ConsumeQueue;
use crate::queue::ArcConsumeQueue;
use crate::queue::ConsumeQueueTable;
use crate::queue::CqUnit;
use crate::store_path_config_helper::get_store_path_batch_consume_queue;
use crate::store_path_config_helper::get_store_path_consume_queue;

#[derive(Clone)]
pub struct ConsumeQueueStore {
    inner: ArcMut<Inner>,
}

struct Inner {
    pub(crate) message_store: Option<ArcMut<LocalFileMessageStore>>,
    pub(crate) message_store_config: Arc<MessageStoreConfig>,
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) queue_offset_operator: QueueOffsetOperator,
    pub(crate) consume_queue_table: Arc<ConsumeQueueTable>,
}

impl Inner {
    fn put_message_position_info_wrapper(&self, consume_queue: &mut dyn ConsumeQueueTrait, request: &DispatchRequest) {
        consume_queue.put_message_position_info_wrapper(request)
    }
}

impl ConsumeQueueStore {
    #[inline]
    pub fn new(message_store_config: Arc<MessageStoreConfig>, broker_config: Arc<BrokerConfig>) -> Self {
        Self {
            inner: ArcMut::new(Inner {
                message_store: None,
                message_store_config,
                broker_config,
                queue_offset_operator: Default::default(),
                consume_queue_table: Arc::new(Default::default()),
            }),
        }
    }

    pub fn set_message_store(&mut self, message_store: ArcMut<LocalFileMessageStore>) {
        self.inner.message_store = Some(message_store);
    }

    pub fn check_self(&self, consume_queue: &dyn ConsumeQueueTrait) {
        let life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        life_cycle.check_self();
    }
}

#[allow(unused_variables)]
impl ConsumeQueueStoreTrait for ConsumeQueueStore {
    fn start(&self) {
        //nothing to do
        info!("consume queue store start");
    }

    fn load(&mut self) -> bool {
        self.load_consume_queues(
            get_store_path_consume_queue(self.inner.message_store_config.store_path_root_dir.as_str()).as_str(),
            CQType::SimpleCQ,
        ) & self.load_consume_queues(
            get_store_path_batch_consume_queue(self.inner.message_store_config.store_path_root_dir.as_str()).as_str(),
            CQType::BatchCQ,
        )
    }

    fn load_after_destroy(&self) -> bool {
        true
    }

    async fn recover(&self) {
        let mut mutex = self.inner.consume_queue_table.lock().clone();
        for (_topic, consume_queue_table) in mutex.iter_mut() {
            for (_queue_id, consume_queue) in consume_queue_table.iter() {
                let queue_id = consume_queue.get_queue_id();
                let topic = consume_queue.get_topic();
                let mut file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
                file_queue_life_cycle.recover();
            }
        }
    }

    async fn recover_concurrently(&self) -> bool {
        // Count the total number of consume queues
        let mut count = 0;
        for maps in self.inner.consume_queue_table.lock().values() {
            count += maps.values().len();
        }

        // Create a vector to hold all futures
        let mut futures = Vec::with_capacity(count);

        // For each consume queue, create a future to recover it
        for maps in self.inner.consume_queue_table.lock().values() {
            for logic in maps.values() {
                let mut logic_clone = logic.clone();
                let future = async move {
                    let mut ret = true;
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| logic_clone.recover())) {
                        Ok(_) => {}
                        Err(_) => {
                            ret = false;
                            error!(
                                "Exception occurs while recover consume queue concurrently, topic={}, queueId={}",
                                logic_clone.get_topic(),
                                logic_clone.get_queue_id()
                            );
                        }
                    }
                    ret
                };
                futures.push(future);
            }
        }

        // Wait for all futures to complete
        let results = join_all(futures).await;

        // Check if any recovery failed
        for result in results {
            if !result {
                return false;
            }
        }

        true
    }

    fn shutdown(&self) -> bool {
        true
    }

    fn destroy(&self) {
        let mutex = self.inner.consume_queue_table.lock().clone();
        for consume_queue_table in mutex.values() {
            for consume_queue in consume_queue_table.values() {
                let queue_id = consume_queue.get_queue_id();
                let topic = consume_queue.get_topic();
                let mut file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
                file_queue_life_cycle.destroy();
            }
        }
    }

    fn destroy_queue(&self, consume_queue: &dyn ConsumeQueueTrait) {
        let mut file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.destroy();
    }

    fn flush(&self, consume_queue: &dyn ConsumeQueueTrait, flush_least_pages: i32) -> bool {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.flush(flush_least_pages)
    }

    async fn clean_expired(&self, min_commit_log_offset: i64) {
        // Collect queues to remove
        let mut queues_to_destroy = Vec::new();
        let mut topics_to_remove = Vec::new();

        {
            let mut consume_queue_table = self.inner.consume_queue_table.lock();
            let topics: Vec<CheetahString> = consume_queue_table.keys().cloned().collect();

            for topic in topics {
                // Skip system topics
                if TopicValidator::is_system_topic(&topic) {
                    continue;
                }

                if let Some(queue_table) = consume_queue_table.get_mut(&topic) {
                    let queue_ids: Vec<i32> = queue_table.keys().cloned().collect();
                    let mut queues_to_remove = Vec::new();

                    for queue_id in queue_ids {
                        if let Some(consume_queue) = queue_table.get(&queue_id) {
                            let max_cl_offset_in_queue = consume_queue.get_max_physic_offset();

                            if max_cl_offset_in_queue == -1 {
                                tracing::warn!(
                                    "maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} \
                                     minLogicOffset={}.",
                                    consume_queue.get_topic(),
                                    consume_queue.get_queue_id(),
                                    consume_queue.get_max_physic_offset(),
                                    consume_queue.get_min_logic_offset()
                                );
                            } else if max_cl_offset_in_queue < min_commit_log_offset {
                                tracing::info!(
                                    "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: \
                                     {} maxCLOffsetInConsumeQueue: {}",
                                    topic,
                                    queue_id,
                                    min_commit_log_offset,
                                    max_cl_offset_in_queue
                                );

                                queues_to_remove.push(queue_id);
                            }
                        }
                    }

                    // Remove expired queues and collect for destruction
                    for queue_id in queues_to_remove {
                        if let Some(consume_queue) = queue_table.remove(&queue_id) {
                            queues_to_destroy.push((topic.clone(), queue_id, consume_queue));
                        }
                    }

                    // If all queues removed, mark topic for removal
                    if queue_table.is_empty() {
                        topics_to_remove.push(topic.clone());
                    }
                }
            }

            // Remove empty topics
            for topic in &topics_to_remove {
                consume_queue_table.remove(topic);
                tracing::info!("cleanExpiredConsumerQueue: {},topic destroyed", topic);
            }
        }

        // Now destroy queues and remove from offset table (outside the lock)
        for (topic, queue_id, consume_queue) in queues_to_destroy {
            // Remove from topic queue table
            self.inner.queue_offset_operator.remove(&topic, queue_id);

            // Destroy the queue
            let consume_queue_ref = &**consume_queue.as_ref();
            self.destroy_queue(consume_queue_ref);
        }
    }

    fn check_self(&self) {
        let consume_queue_table = self.inner.consume_queue_table.lock().clone();
        for consume_queue_table in consume_queue_table.values() {
            for consume_queue in consume_queue_table.values() {
                let consume_queue = &**consume_queue.as_ref();
                ConsumeQueueStore::check_self(self, consume_queue);
            }
        }
    }

    fn delete_expired_file(&self, consume_queue: &dyn ConsumeQueueTrait, min_commit_log_pos: i64) -> i32 {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.delete_expired_file(min_commit_log_pos)
    }

    fn is_first_file_available(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.is_first_file_available()
    }

    fn is_first_file_exist(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.is_first_file_exist()
    }

    fn roll_next_file(&self, consume_queue: &dyn ConsumeQueueTrait, offset: i64) -> i64 {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.roll_next_file(offset)
    }

    fn truncate_dirty(&self, offset_to_truncate: i64) {
        let cloned = self.inner.consume_queue_table.lock().clone();
        for consume_queue_table in cloned.values() {
            for logic in consume_queue_table.values() {
                let topic = logic.get_topic();
                let queue_id = logic.get_queue_id();
                self.truncate_dirty_logic_files(topic, queue_id, offset_to_truncate);
            }
        }
    }

    fn put_message_position_info_wrapper_with_cq(
        &self,
        consume_queue: &mut dyn ConsumeQueueTrait,
        request: &DispatchRequest,
    ) {
        self.inner.put_message_position_info_wrapper(consume_queue, request);
    }

    fn put_message_position_info_wrapper(&self, request: &DispatchRequest) {
        let mut cq = self.find_or_create_consume_queue(request.topic.as_ref(), request.queue_id);
        self.put_message_position_info_wrapper_with_cq(cq.as_mut().as_mut(), request);
    }

    async fn range_query(&self, topic: &CheetahString, queue_id: i32, start_index: i64, num: i32) -> Vec<Bytes> {
        // This method is primarily for RocksDB mode
        // For LocalFile mode, use ConsumeQueue's iterateFrom method instead
        tracing::warn!("range_query is not supported in LocalFile mode, use ConsumeQueue iterateFrom instead");
        Vec::new()
    }

    async fn get(&self, topic: &CheetahString, queue_id: i32, start_index: i64) -> Bytes {
        // This method is primarily for RocksDB mode
        // For LocalFile mode, use ConsumeQueue's get method instead
        tracing::warn!("get is not supported in LocalFile mode, use ConsumeQueue get method instead");
        Bytes::new()
    }

    fn get_consume_queue_table(&self) -> Arc<ConsumeQueueTable> {
        self.inner.consume_queue_table.clone()
    }

    fn assign_queue_offset(&self, msg: &mut MessageExtBrokerInner) {
        let consume_queue = self.find_or_create_consume_queue(msg.get_topic(), msg.queue_id());
        consume_queue.assign_queue_offset(&self.inner.queue_offset_operator, msg);
    }

    fn increase_queue_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        let consume_queue = self.find_or_create_consume_queue(msg.get_topic(), msg.queue_id());
        consume_queue.increase_queue_offset(&self.inner.queue_offset_operator, msg, message_num);
    }

    fn increase_lmq_offset(&self, queue_key: &str, message_num: i16) {
        self.inner
            .queue_offset_operator
            .increase_queue_offset(CheetahString::from(queue_key), message_num);
    }

    fn get_lmq_queue_offset(&self, queue_key: &str) -> i64 {
        self.inner.queue_offset_operator.current_queue_offset(&queue_key.into())
    }

    fn recover_offset_table(&mut self, min_phy_offset: i64) {
        let mut cq_offset_table = HashMap::with_capacity(1024);
        let mut bcq_offset_table = HashMap::with_capacity(1024);
        for (topic, consume_queue_table) in self.inner.consume_queue_table.lock().iter_mut() {
            for (queue_id, consume_queue) in consume_queue_table.iter() {
                let key = CheetahString::from_string(format!(
                    "{}-{}",
                    consume_queue.get_topic(),
                    consume_queue.get_queue_id()
                ));
                let max_offset_in_queue = consume_queue.get_max_offset_in_queue();
                if consume_queue.get_cq_type() == CQType::SimpleCQ {
                    cq_offset_table.insert(key, max_offset_in_queue);
                } else {
                    bcq_offset_table.insert(key, max_offset_in_queue);
                }
                self.correct_min_offset(&***consume_queue, min_phy_offset)
            }
        }
        if self.inner.message_store_config.duplication_enable || self.inner.broker_config.enable_controller_mode {
            // In duplication/controller mode, use LMQ offset table
            self.inner
                .queue_offset_operator
                .set_lmq_topic_queue_table(cq_offset_table);
        } else {
            self.set_topic_queue_table(cq_offset_table);
            self.set_batch_topic_queue_table(bcq_offset_table);
        }
    }

    fn set_topic_queue_table(&mut self, topic_queue_table: HashMap<CheetahString, i64>) {
        self.inner
            .queue_offset_operator
            .set_topic_queue_table(topic_queue_table.clone());
        self.inner
            .queue_offset_operator
            .set_lmq_topic_queue_table(topic_queue_table);
    }

    fn remove_topic_queue_table(&mut self, topic: &CheetahString, queue_id: i32) {
        self.inner.queue_offset_operator.remove(topic, queue_id);
    }

    fn get_topic_queue_table(&self) -> HashMap<CheetahString, i64> {
        self.inner.queue_offset_operator.get_topic_queue_table()
    }

    fn get_max_phy_offset_in_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<i64> {
        let mut max_physic_offset = -1i64;
        for (topic, consume_queue_table) in self.inner.consume_queue_table.lock().iter() {
            for (queue_id, consume_queue) in consume_queue_table.iter() {
                let max_physic_offset_in_consume_queue = consume_queue.get_max_physic_offset();
                if max_physic_offset_in_consume_queue > max_physic_offset {
                    max_physic_offset = max_physic_offset_in_consume_queue;
                }
            }
        }
        Some(max_physic_offset)
    }

    fn get_max_offset(&self, topic: &CheetahString, queue_id: i32) -> Option<i64> {
        Some(
            self.inner
                .queue_offset_operator
                .current_queue_offset(&format!("{topic}-{queue_id}").into()),
        )
    }

    fn get_max_phy_offset_in_consume_queue_global(&self) -> i64 {
        let mut max_physic_offset = -1i64;
        for (topic, consume_queue_table) in self.inner.consume_queue_table.lock().iter() {
            for (queue_id, consume_queue) in consume_queue_table.iter() {
                let max_physic_offset_in_consume_queue = consume_queue.get_max_physic_offset();
                if max_physic_offset_in_consume_queue > max_physic_offset {
                    max_physic_offset = max_physic_offset_in_consume_queue;
                }
            }
        }
        max_physic_offset
    }

    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        let queue = self.find_or_create_consume_queue(topic, queue_id);
        queue.get_min_offset_in_queue()
    }

    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        let queue = self.find_or_create_consume_queue(topic, queue_id);
        queue.get_max_offset_in_queue()
    }

    fn get_offset_in_queue_by_time(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        let logic = self.find_or_create_consume_queue(topic, queue_id);
        let result_offset = logic.get_offset_in_queue_by_time_with_boundary(timestamp, boundary_type);
        result_offset
            .max(logic.get_min_offset_in_queue())
            .min(logic.get_max_offset_in_queue())
    }

    fn find_or_create_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> ArcConsumeQueue {
        // Fast path: check if queue already exists
        {
            let consume_queue_table = self.inner.consume_queue_table.lock();
            if let Some(topic_map) = consume_queue_table.get(topic) {
                if let Some(queue) = topic_map.get(&queue_id) {
                    return queue.clone();
                }
            }
        }

        // Slow path: create new consume queue without holding lock
        // Get topic config and determine queue type (lock-free)
        let message_store = self
            .inner
            .message_store
            .as_ref()
            .expect("MessageStore must be set before creating consume queues");
        let topic_config = message_store.get_topic_config(topic);
        let cq_type = QueueTypeUtils::get_cq_type_arc_mut(topic_config.as_ref());

        // Create the queue object (expensive operation, no lock held)
        let new_queue: ArcConsumeQueue = match cq_type {
            CQType::SimpleCQ | CQType::RocksDBCQ => {
                let consume_queue = ConsumeQueue::new(
                    topic.clone(),
                    queue_id,
                    CheetahString::from_string(get_store_path_consume_queue(
                        self.inner.message_store_config.store_path_root_dir.as_str(),
                    )),
                    self.inner.message_store_config.get_mapped_file_size_consume_queue(),
                    self.inner.message_store.clone().unwrap(),
                );
                ArcMut::new(Box::new(consume_queue))
            }
            CQType::BatchCQ => {
                let consume_queue = BatchConsumeQueue::new(
                    topic.clone(),
                    queue_id,
                    CheetahString::from_string(get_store_path_batch_consume_queue(
                        self.inner.message_store_config.store_path_root_dir.as_str(),
                    )),
                    self.inner.message_store_config.mapper_file_size_batch_consume_queue,
                    None,
                    self.inner.message_store_config.clone(),
                );
                ArcMut::new(Box::new(consume_queue))
            }
        };

        //Insert into table with triple-check pattern (prevents race conditions)
        let mut consume_queue_table = self.inner.consume_queue_table.lock();
        let topic_map = consume_queue_table.entry(topic.clone()).or_default();

        topic_map.entry(queue_id).or_insert(new_queue).clone()
    }

    fn find_consume_queue_map(&self, topic: &CheetahString) -> Option<HashMap<i32, ArcConsumeQueue>> {
        self.inner.consume_queue_table.lock().get(topic).cloned()
    }

    fn get_total_size(&self) -> i64 {
        let mut total_size = 0;
        for consume_queue_table in self.inner.consume_queue_table.lock().values() {
            for consume_queue in consume_queue_table.values() {
                total_size += consume_queue.get_total_size();
            }
        }
        total_size
    }

    fn get_store_time(&self, cq_unit: &CqUnit) -> i64 {
        match self.inner.message_store.as_ref() {
            Some(ms) => ms.get_commit_log().pickup_store_timestamp(cq_unit.pos, cq_unit.size),
            None => {
                error!("Message store is not set in ConsumeQueueStore");
                -1
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl ConsumeQueueStore {
    #[inline]
    pub fn correct_min_offset(&self, consume_queue: &dyn ConsumeQueueTrait, min_commit_log_offset: i64) {
        consume_queue.correct_min_offset(min_commit_log_offset)
    }

    #[inline]
    pub fn set_batch_topic_queue_table(&self, batch_topic_queue_table: HashMap<CheetahString, i64>) {
        self.inner
            .queue_offset_operator
            .set_batch_topic_queue_table(batch_topic_queue_table)
    }

    /// Load consume queues from the given directory path for a specific queue type
    ///
    /// # Arguments
    ///
    /// * `store_path` - Base path where queue directories are stored
    /// * `cq_type` - Type of consume queue to load (SimpleCQ or BatchCQ)
    ///
    /// # Returns
    ///
    /// Returns true if loading was successful, false otherwise
    fn load_consume_queues(&mut self, store_path: &str, cq_type: CQType) -> bool {
        let dir_logic = Path::new(store_path);

        // Check if directory exists
        if !dir_logic.exists() || !dir_logic.is_dir() {
            info!("Directory {} doesn't exist or is not a directory", store_path);
            return true; // Return true as this is not an error case
        }

        // Iterate through topic directories
        match fs::read_dir(dir_logic) {
            Ok(topic_entries) => {
                for topic_dir in topic_entries.flatten() {
                    let topic_path = topic_dir.path();
                    if !topic_path.is_dir() {
                        continue;
                    }

                    // Get topic name from directory name
                    let topic = match topic_path.file_name().and_then(|n| n.to_str()) {
                        Some(name) => CheetahString::from_string(name.to_string()),
                        None => continue,
                    };

                    // Iterate through queue ID directories
                    match fs::read_dir(topic_path) {
                        Ok(queue_id_entries) => {
                            for queue_id_dir in queue_id_entries.flatten() {
                                if !queue_id_dir.path().is_dir() {
                                    continue;
                                }

                                // Parse queue ID from directory name
                                let os_string = queue_id_dir.file_name();
                                let queue_id_op = os_string.to_str();
                                let queue_id_str = match queue_id_op {
                                    Some(name) => name,
                                    None => continue,
                                };

                                let queue_id = match i32::from_str(queue_id_str) {
                                    Ok(id) => id,
                                    Err(_) => {
                                        // Skip non-numeric directory names
                                        continue;
                                    }
                                };

                                // Verify queue type matches expected type
                                self.queue_type_should_be(&topic, cq_type);

                                // Create consume queue based on type
                                let logic =
                                    self.create_consume_queue_by_type(&topic, queue_id, cq_type, store_path.into());

                                // Store the queue in memory table
                                self.put_consume_queue(topic.clone(), queue_id, logic);

                                // Load the queue data
                                if !self.load_logic(&topic, queue_id) {
                                    return false;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to read queue ID directories for topic {}: {}", topic, e);
                            return false;
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to read topic directories: {e}");
                return false;
            }
        }

        info!("load {cq_type} all over, OK");
        true
    }

    #[inline]
    fn load_logic(&mut self, topic: &CheetahString, queue_id: i32) -> bool {
        let mut file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
        file_queue_life_cycle.load()
    }

    #[inline]
    fn put_consume_queue(
        &self,
        topic: CheetahString,
        queue_id: i32,
        consume_queue: ArcMut<Box<dyn ConsumeQueueTrait>>,
    ) {
        let mut consume_queue_table = self.inner.consume_queue_table.lock();
        let topic_table = consume_queue_table.entry(topic).or_default();
        topic_table.insert(queue_id, consume_queue);
    }

    #[inline]
    fn queue_type_should_be(&self, topic: &CheetahString, cq_type: CQType) {
        let topic_config = self.inner.message_store.as_ref().unwrap().get_topic_config(topic);
        let act = QueueTypeUtils::get_cq_type_arc_mut(topic_config.as_ref());
        if act != cq_type {
            panic!("The queue type of topic: {topic} should be {cq_type:?}, but is {act:?}",);
        }
    }

    #[inline]
    fn truncate_dirty_logic_files(&self, topic: &CheetahString, queue_id: i32, phy_offset: i64) {
        let mut file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
        file_queue_life_cycle.truncate_dirty_logic_files(phy_offset);
    }
    #[inline]
    fn create_consume_queue_by_type(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        cq_type: CQType,
        store_path: CheetahString,
    ) -> ArcMut<Box<dyn ConsumeQueueTrait>> {
        match cq_type {
            CQType::SimpleCQ => {
                let ms_ref = self.inner.message_store.as_ref().unwrap();
                let consume_queue = ConsumeQueue::new(
                    topic.clone(),
                    queue_id,
                    store_path,
                    self.inner.message_store_config.get_mapped_file_size_consume_queue(),
                    self.inner.message_store.clone().unwrap(),
                );
                ArcMut::new(Box::new(consume_queue))
            }
            CQType::BatchCQ => {
                let consume_queue = BatchConsumeQueue::new(
                    topic.clone(),
                    queue_id,
                    store_path,
                    self.inner.message_store_config.mapper_file_size_batch_consume_queue,
                    None,
                    self.inner.message_store_config.clone(),
                );
                ArcMut::new(Box::new(consume_queue))
            }
            _ => {
                error!("Unsupported consume queue type: {cq_type:?}");
                panic!("Unsupported consume queue type: {cq_type:?}");
            }
        }
    }

    #[inline]
    fn get_life_cycle(&self, topic: &CheetahString, queue_id: i32) -> ArcConsumeQueue {
        self.find_or_create_consume_queue(topic, queue_id)
    }
}
