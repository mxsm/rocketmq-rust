/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#![allow(unused_variables)]

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_single::MessageExtBrokerInner;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use tracing::info;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::config::message_store_config::MessageStoreConfig;
use crate::queue::batch_consume_queue::BatchConsumeQueue;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::single_consume_queue::ConsumeQueue;
use crate::queue::ConsumeQueueStoreTrait;
use crate::queue::ConsumeQueueTrait;
use crate::queue::CqUnit;
use crate::store::running_flags::RunningFlags;
use crate::store_path_config_helper::get_store_path_batch_consume_queue;
use crate::store_path_config_helper::get_store_path_consume_queue;

#[derive(Clone)]
pub struct ConsumeQueueStore {
    inner: Arc<ConsumeQueueStoreInner>,
    running_flags: Arc<RunningFlags>,
    store_checkpoint: Arc<StoreCheckpoint>,
    topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
}

type ConsumeQueueTable = parking_lot::Mutex<
    HashMap<String, HashMap<i32, Arc<parking_lot::Mutex<Box<dyn ConsumeQueueTrait>>>>>,
>;

struct ConsumeQueueStoreInner {
    // commit_log: Arc<Mutex<CommitLog>>,
    pub(crate) message_store_config: Arc<MessageStoreConfig>,
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) queue_offset_operator: QueueOffsetOperator,
    pub(crate) consume_queue_table: ConsumeQueueTable,
}

impl ConsumeQueueStoreInner {
    fn put_message_position_info_wrapper(
        &self,
        consume_queue: &mut dyn ConsumeQueueTrait,
        request: &DispatchRequest,
    ) {
        consume_queue.put_message_position_info_wrapper(request)
    }
}

impl ConsumeQueueStore {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        topic_config_table: Arc<parking_lot::Mutex<HashMap<String, TopicConfig>>>,
        running_flags: Arc<RunningFlags>,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        Self {
            inner: Arc::new(ConsumeQueueStoreInner {
                //commit_log,
                message_store_config,
                broker_config,
                queue_offset_operator: QueueOffsetOperator::new(),
                consume_queue_table: parking_lot::Mutex::new(HashMap::new()),
            }),
            running_flags,
            store_checkpoint,
            topic_config_table,
        }
    }
}

#[allow(unused_variables)]
impl ConsumeQueueStoreTrait for ConsumeQueueStore {
    fn start(&self) {
        todo!()
    }

    fn load(&mut self) -> bool {
        self.load_consume_queues(
            get_store_path_consume_queue(
                self.inner.message_store_config.store_path_root_dir.as_str(),
            ),
            CQType::SimpleCQ,
        ) & self.load_consume_queues(
            get_store_path_batch_consume_queue(
                self.inner.message_store_config.store_path_root_dir.as_str(),
            ),
            CQType::BatchCQ,
        )
    }

    fn load_after_destroy(&self) -> bool {
        true
    }

    fn recover(&mut self) {
        let mut mutex = self.inner.consume_queue_table.lock().clone();
        for (_topic, consume_queue_table) in mutex.iter_mut() {
            for (_queue_id, consume_queue) in consume_queue_table.iter_mut() {
                //consume_queue.lock().recover();
                let lock = consume_queue.lock();
                let queue_id = lock.get_queue_id();
                let topic = lock.get_topic();
                drop(lock);
                let file_queue_life_cycle = self.get_life_cycle(topic.as_str(), queue_id);
                file_queue_life_cycle.lock().recover();
            }
        }
    }

    fn recover_concurrently(&mut self) -> bool {
        todo!()
    }

    fn shutdown(&self) -> bool {
        todo!()
    }

    fn destroy(&self) {
        let mutex = self.inner.consume_queue_table.lock().clone();
        for consume_queue_table in mutex.values() {
            for consume_queue in consume_queue_table.values() {
                let lock = consume_queue.lock();
                let queue_id = lock.get_queue_id();
                let topic = lock.get_topic();
                drop(lock);
                let file_queue_life_cycle = self.get_life_cycle(topic.as_str(), queue_id);
                file_queue_life_cycle.lock().destroy();
            }
        }
    }

    fn destroy_consume_queue(&self, consume_queue: &dyn ConsumeQueueTrait) {
        todo!()
    }

    fn flush(&self, consume_queue: &dyn ConsumeQueueTrait, flush_least_pages: i32) -> bool {
        todo!()
    }

    fn clean_expired(&self, min_phy_offset: i64) {
        todo!()
    }

    fn check_self(&self) {
        println!("ConsumeQueueStore::check_self unimplemented");
    }

    fn delete_expired_file(
        &self,
        consume_queue: &dyn ConsumeQueueTrait,
        min_commit_log_pos: i64,
    ) -> i32 {
        todo!()
    }

    fn is_first_file_available(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool {
        todo!()
    }

    fn is_first_file_exist(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool {
        todo!()
    }

    fn roll_next_file(&self, consume_queue: &dyn ConsumeQueueTrait, offset: i64) -> i64 {
        todo!()
    }

    fn truncate_dirty(&self, offset_to_truncate: i64) {
        let cloned = self.inner.consume_queue_table.lock().clone();
        for consume_queue_table in cloned.values() {
            for logic in consume_queue_table.values() {
                let lock = logic.lock();
                let topic = lock.get_topic();
                let queue_id = lock.get_queue_id();
                drop(lock);
                self.truncate_dirty_logic_files(topic.as_str(), queue_id, offset_to_truncate);
            }
        }
    }

    fn put_message_position_info_wrapper(&self, request: &DispatchRequest) {
        let cq = self.find_or_create_consume_queue(request.topic.as_str(), request.queue_id);
        self.put_message_position_info_wrapper_with_cq(cq.lock().as_mut(), request);
        // println!("put_message_position_info_wrapper-----{}", request.topic)
    }

    fn put_message_position_info_wrapper_with_cq(
        &self,
        consume_queue: &mut dyn ConsumeQueueTrait,
        request: &DispatchRequest,
    ) {
        self.inner
            .put_message_position_info_wrapper(consume_queue, request);
    }

    fn range_query(
        &self,
        topic: &str,
        queue_id: i32,
        start_index: i64,
        num: i32,
    ) -> Option<Vec<Bytes>> {
        todo!()
    }

    fn get_signal(&self, topic: &str, queue_id: i32, start_index: i64) -> Option<Bytes> {
        todo!()
    }

    fn increase_queue_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        let consume_queue = self.find_or_create_consume_queue(msg.topic(), msg.queue_id());
        consume_queue.lock().increase_queue_offset(
            &self.inner.queue_offset_operator,
            msg,
            message_num,
        );
    }

    fn assign_queue_offset(&self, msg: &mut MessageExtBrokerInner) {
        let consume_queue = self.find_or_create_consume_queue(msg.topic(), msg.queue_id());
        consume_queue
            .lock()
            .assign_queue_offset(&self.inner.queue_offset_operator, msg);
    }

    fn increase_lmq_offset(&mut self, queue_key: &str, message_num: i16) {
        todo!()
    }

    fn get_lmq_queue_offset(&self, queue_key: &str) -> i64 {
        todo!()
    }

    fn recover_offset_table(&mut self, min_phy_offset: i64) {
        let mut cq_offset_table = HashMap::with_capacity(1024);
        let mut bcq_offset_table = HashMap::with_capacity(1024);
        for (topic, consume_queue_table) in self.inner.consume_queue_table.lock().iter_mut() {
            for (queue_id, consume_queue) in consume_queue_table.iter_mut() {
                let guard = consume_queue.lock();
                let key = format!("{}-{}", guard.get_topic(), guard.get_queue_id());
                let max_offset_in_queue = guard.get_max_offset_in_queue();
                if guard.get_cq_type() == CQType::SimpleCQ {
                    cq_offset_table.insert(key, max_offset_in_queue);
                } else {
                    bcq_offset_table.insert(key, max_offset_in_queue);
                }
                self.correct_min_offset(&**guard, min_phy_offset)
            }
        }
        if self.inner.message_store_config.duplication_enable
            || self.inner.broker_config.enable_controller_mode
        {
            unimplemented!()
        }
        self.set_topic_queue_table(cq_offset_table);
        self.set_batch_topic_queue_table(bcq_offset_table);
    }

    fn set_topic_queue_table(&mut self, topic_queue_table: HashMap<String, i64>) {
        self.inner
            .queue_offset_operator
            .set_topic_queue_table(topic_queue_table.clone());
        self.inner
            .queue_offset_operator
            .set_lmq_topic_queue_table(topic_queue_table);
    }

    fn remove_topic_queue_table(&mut self, topic: &str, queue_id: i32) {
        todo!()
    }

    fn get_topic_queue_table(&self) -> HashMap<String, i64> {
        todo!()
    }

    fn get_max_phy_offset_in_consume_queue_id(&self, topic: &str, queue_id: i32) -> i64 {
        todo!()
    }

    fn get_max_phy_offset_in_consume_queue(&self) -> i64 {
        let mut max_physic_offset = -1i64;
        for (topic, consume_queue_table) in self.inner.consume_queue_table.lock().iter() {
            for (queue_id, consume_queue) in consume_queue_table.iter() {
                let max_physic_offset_in_consume_queue =
                    consume_queue.lock().get_max_physic_offset();
                if max_physic_offset_in_consume_queue > max_physic_offset {
                    max_physic_offset = max_physic_offset_in_consume_queue;
                }
            }
        }
        max_physic_offset
    }

    fn get_max_offset(&self, topic: &str, queue_id: i32) -> Option<i64> {
        Some(
            self.inner
                .queue_offset_operator
                .current_queue_offset(format!("{}-{}", topic, queue_id).as_str()),
        )
    }

    fn find_or_create_consume_queue(
        &self,
        topic: &str,
        queue_id: i32,
    ) -> Arc<parking_lot::Mutex<Box<dyn ConsumeQueueTrait>>> {
        let mut consume_queue_table = self.inner.consume_queue_table.lock();

        let topic_map = consume_queue_table.entry(topic.to_string()).or_default();

        if let Some(value) = topic_map.get(&queue_id) {
            return value.clone();
        }

        let consume_queue = topic_map.entry(queue_id).or_insert_with(|| {
            let option = self
                .topic_config_table
                .lock()
                .get(&topic.to_string())
                .cloned();
            match QueueTypeUtils::get_cq_type(&option) {
                CQType::SimpleCQ => Arc::new(parking_lot::Mutex::new(Box::new(ConsumeQueue::new(
                    topic.to_string(),
                    queue_id,
                    get_store_path_consume_queue(
                        self.inner.message_store_config.store_path_root_dir.as_str(),
                    ),
                    self.inner
                        .message_store_config
                        .get_mapped_file_size_consume_queue(),
                    self.inner.message_store_config.clone(),
                    self.running_flags.clone(),
                    self.store_checkpoint.clone(),
                )))),
                CQType::BatchCQ => {
                    Arc::new(parking_lot::Mutex::new(Box::new(BatchConsumeQueue::new(
                        topic.to_string(),
                        queue_id,
                        get_store_path_batch_consume_queue(
                            self.inner.message_store_config.store_path_root_dir.as_str(),
                        ),
                        self.inner
                            .message_store_config
                            .mapper_file_size_batch_consume_queue,
                        None,
                        self.inner.message_store_config.clone(),
                    ))))
                }
                CQType::RocksDBCQ => {
                    unimplemented!()
                }
            }
        });
        consume_queue.clone()
    }

    fn find_consume_queue_map(
        &self,
        topic: &str,
    ) -> Option<HashMap<i32, Box<dyn ConsumeQueueTrait>>> {
        todo!()
    }

    fn get_total_size(&self) -> i64 {
        todo!()
    }

    fn get_store_time(&self, cq_unit: CqUnit) -> i64 {
        todo!()
    }

    fn get_min_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64 {
        self.find_or_create_consume_queue(topic, queue_id)
            .lock()
            .get_min_offset_in_queue()
    }

    fn get_max_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64 {
        todo!()
    }
}

impl ConsumeQueueStore {
    pub fn correct_min_offset(
        &self,
        consume_queue: &dyn ConsumeQueueTrait,
        min_commit_log_offset: i64,
    ) {
        consume_queue.correct_min_offset(min_commit_log_offset)
    }

    pub fn set_batch_topic_queue_table(&self, batch_topic_queue_table: HashMap<String, i64>) {
        self.inner
            .queue_offset_operator
            .set_batch_topic_queue_table(batch_topic_queue_table)
    }

    fn load_consume_queues(&mut self, store_path: String, cq_type: CQType) -> bool {
        let dir = Path::new(&store_path);
        if let Ok(ls) = fs::read_dir(dir) {
            let dirs: Vec<_> = ls
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect();
            for dir in dirs {
                let topic = dir.file_name().unwrap().to_str().unwrap().to_string();
                if let Ok(ls) = fs::read_dir(&dir) {
                    let file_queue_id_list: Vec<_> = ls
                        .filter_map(Result::ok)
                        .map(|entry| entry.path())
                        .collect();
                    for file_queue_id in file_queue_id_list {
                        let queue_id = file_queue_id
                            .file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .parse::<i32>()
                            .unwrap();
                        self.queue_type_should_be(&topic, cq_type);
                        let logic = self.create_consume_queue_by_type(
                            topic.as_str(),
                            queue_id,
                            cq_type,
                            store_path.clone(),
                        );
                        self.put_consume_queue(topic.clone(), queue_id, logic);
                        if !self.load_logic(topic.clone(), queue_id) {
                            return false;
                        }
                    }
                }
            }
        }
        info!("load {:?} all over, OK", cq_type);
        true
    }

    fn load_logic(&mut self, topic: String, queue_id: i32) -> bool {
        let file_queue_life_cycle = self.get_life_cycle(topic.as_str(), queue_id);
        let result = file_queue_life_cycle.lock().load();
        result
    }

    fn put_consume_queue(
        &self,
        topic: String,
        queue_id: i32,
        consume_queue: Box<dyn ConsumeQueueTrait>,
    ) {
        let mut consume_queue_table = self.inner.consume_queue_table.lock();
        let topic_table = consume_queue_table.entry(topic).or_default();
        topic_table.insert(queue_id, Arc::new(parking_lot::Mutex::new(consume_queue)));
    }

    fn queue_type_should_be(&self, topic: &str, cq_type: CQType) {
        let option = self.topic_config_table.lock().get(topic).cloned();
        let act = QueueTypeUtils::get_cq_type(&option);
        if act != cq_type {
            panic!(
                "The queue type of topic: {} should be {:?}, but is {:?}",
                topic, cq_type, act
            );
        }
    }

    /*fn truncate_dirty_logic_files(&self, consume_queue: &dyn ConsumeQueueTrait, phy_offset: i64) {
        let file_queue_life_cycle = self.get_life_cycle(
            consume_queue.get_topic().as_str(),
            consume_queue.get_queue_id(),
        );
        file_queue_life_cycle
            .lock()
            .truncate_dirty_logic_files(phy_offset);
    }*/

    fn truncate_dirty_logic_files(&self, topic: &str, queue_id: i32, phy_offset: i64) {
        let file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
        file_queue_life_cycle
            .lock()
            .truncate_dirty_logic_files(phy_offset);
    }
}

impl ConsumeQueueStore {
    fn create_consume_queue_by_type(
        &self,
        topic: &str,
        queue_id: i32,
        cq_type: CQType,
        store_path: String,
    ) -> Box<dyn ConsumeQueueTrait> {
        match cq_type {
            CQType::SimpleCQ => {
                let consume_queue = ConsumeQueue::new(
                    topic.to_string(),
                    queue_id,
                    store_path,
                    self.inner
                        .message_store_config
                        .get_mapped_file_size_consume_queue(),
                    self.inner.message_store_config.clone(),
                    self.running_flags.clone(),
                    self.store_checkpoint.clone(),
                );
                Box::new(consume_queue)
            }
            CQType::BatchCQ => {
                let consume_queue = BatchConsumeQueue::new(
                    topic.to_string(),
                    queue_id,
                    store_path,
                    self.inner
                        .message_store_config
                        .mapper_file_size_batch_consume_queue,
                    None,
                    self.inner.message_store_config.clone(),
                );
                Box::new(consume_queue)
            }
            _ => {
                unimplemented!()
            }
        }
    }

    fn get_life_cycle(
        &self,
        topic: &str,
        queue_id: i32,
    ) -> Arc<parking_lot::Mutex<Box<dyn ConsumeQueueTrait>>> {
        self.find_or_create_consume_queue(topic, queue_id)
    }
}
