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
use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_rust::ArcMut;
use tracing::info;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::config::message_store_config::MessageStoreConfig;
use crate::queue::batch_consume_queue::BatchConsumeQueue;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::single_consume_queue::ConsumeQueue;
use crate::queue::ArcConsumeQueue;
use crate::queue::ConsumeQueueStoreTrait;
use crate::queue::ConsumeQueueTable;
use crate::queue::ConsumeQueueTrait;
use crate::queue::CqUnit;
use crate::store::running_flags::RunningFlags;
use crate::store_path_config_helper::get_store_path_batch_consume_queue;
use crate::store_path_config_helper::get_store_path_consume_queue;

#[derive(Clone)]
pub struct ConsumeQueueStore {
    inner: Arc<Inner>,
    running_flags: Arc<RunningFlags>,
    store_checkpoint: Arc<StoreCheckpoint>,
    topic_config_table: Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
}

struct Inner {
    // commit_log: Arc<Mutex<CommitLog>>,
    pub(crate) message_store_config: Arc<MessageStoreConfig>,
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) queue_offset_operator: QueueOffsetOperator,
    pub(crate) consume_queue_table: Arc<ConsumeQueueTable>,
}

impl Inner {
    fn put_message_position_info_wrapper(
        &self,
        consume_queue: &mut dyn ConsumeQueueTrait,
        request: &DispatchRequest,
    ) {
        consume_queue.put_message_position_info_wrapper(request)
    }
}

impl ConsumeQueueStore {
    #[inline]
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        broker_config: Arc<BrokerConfig>,
        topic_config_table: Arc<parking_lot::Mutex<HashMap<CheetahString, TopicConfig>>>,
        running_flags: Arc<RunningFlags>,
        store_checkpoint: Arc<StoreCheckpoint>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                //commit_log,
                message_store_config,
                broker_config,
                queue_offset_operator: QueueOffsetOperator::new(),
                consume_queue_table: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            }),
            running_flags,
            store_checkpoint,
            topic_config_table,
        }
    }
}

#[allow(unused_variables)]
impl ConsumeQueueStoreTrait for ConsumeQueueStore {
    #[inline]
    fn start(&self) {
        todo!()
    }

    #[inline]
    fn load(&mut self) -> bool {
        self.load_consume_queues(
            CheetahString::from_string(get_store_path_consume_queue(
                self.inner.message_store_config.store_path_root_dir.as_str(),
            )),
            CQType::SimpleCQ,
        ) & self.load_consume_queues(
            CheetahString::from_string(get_store_path_batch_consume_queue(
                self.inner.message_store_config.store_path_root_dir.as_str(),
            )),
            CQType::BatchCQ,
        )
    }

    #[inline]
    fn load_after_destroy(&self) -> bool {
        true
    }

    #[inline]
    fn recover(&mut self) {
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

    #[inline]
    fn recover_concurrently(&mut self) -> bool {
        todo!()
    }

    #[inline]
    fn shutdown(&self) -> bool {
        todo!()
    }

    #[inline]
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

    #[inline]
    fn destroy_consume_queue(&self, consume_queue: &dyn ConsumeQueueTrait) {
        let mut file_queue_life_cycle =
            self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.destroy();
    }

    #[inline]
    fn flush(&self, consume_queue: &dyn ConsumeQueueTrait, flush_least_pages: i32) -> bool {
        todo!()
    }

    #[inline]
    fn clean_expired(&self, min_phy_offset: i64) {
        todo!()
    }

    #[inline]
    fn check_self(&self) {
        println!("ConsumeQueueStore::check_self unimplemented");
    }

    #[inline]
    fn delete_expired_file(
        &self,
        consume_queue: &dyn ConsumeQueueTrait,
        min_commit_log_pos: i64,
    ) -> i32 {
        todo!()
    }

    #[inline]
    fn is_first_file_available(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool {
        todo!()
    }

    #[inline]
    fn is_first_file_exist(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool {
        todo!()
    }

    #[inline]
    fn roll_next_file(&self, consume_queue: &dyn ConsumeQueueTrait, offset: i64) -> i64 {
        todo!()
    }

    #[inline]
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

    #[inline]
    fn put_message_position_info_wrapper(&self, request: &DispatchRequest) {
        let mut cq = self.find_or_create_consume_queue(request.topic.as_ref(), request.queue_id);
        self.put_message_position_info_wrapper_with_cq(&mut **cq.as_mut(), request);
        // println!("put_message_position_info_wrapper-----{}", request.topic)
    }

    #[inline]
    fn put_message_position_info_wrapper_with_cq(
        &self,
        consume_queue: &mut dyn ConsumeQueueTrait,
        request: &DispatchRequest,
    ) {
        self.inner
            .put_message_position_info_wrapper(consume_queue, request);
    }

    #[inline]
    fn range_query(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        start_index: i64,
        num: i32,
    ) -> Option<Vec<Bytes>> {
        todo!()
    }

    #[inline]
    fn get_signal(&self, topic: &CheetahString, queue_id: i32, start_index: i64) -> Option<Bytes> {
        todo!()
    }

    #[inline]
    fn increase_queue_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        let consume_queue = self.find_or_create_consume_queue(msg.get_topic(), msg.queue_id());
        consume_queue.increase_queue_offset(&self.inner.queue_offset_operator, msg, message_num);
    }

    #[inline]
    fn assign_queue_offset(&self, msg: &mut MessageExtBrokerInner) {
        let consume_queue = self.find_or_create_consume_queue(msg.get_topic(), msg.queue_id());
        consume_queue.assign_queue_offset(&self.inner.queue_offset_operator, msg);
    }

    #[inline]
    fn increase_lmq_offset(&mut self, queue_key: &CheetahString, message_num: i16) {
        todo!()
    }

    #[inline]
    fn get_lmq_queue_offset(&self, queue_key: &CheetahString) -> i64 {
        todo!()
    }

    #[inline]
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
        if self.inner.message_store_config.duplication_enable
            || self.inner.broker_config.enable_controller_mode
        {
            unimplemented!()
        }
        self.set_topic_queue_table(cq_offset_table);
        self.set_batch_topic_queue_table(bcq_offset_table);
    }

    #[inline]
    fn set_topic_queue_table(&mut self, topic_queue_table: HashMap<CheetahString, i64>) {
        self.inner
            .queue_offset_operator
            .set_topic_queue_table(topic_queue_table.clone());
        self.inner
            .queue_offset_operator
            .set_lmq_topic_queue_table(topic_queue_table);
    }

    #[inline]
    fn remove_topic_queue_table(&mut self, topic: &CheetahString, queue_id: i32) {
        self.inner.queue_offset_operator.remove(topic, queue_id);
    }

    #[inline]
    fn get_topic_queue_table(&self) -> HashMap<CheetahString, i64> {
        todo!()
    }

    #[inline]
    fn get_max_phy_offset_in_consume_queue_id(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        todo!()
    }

    #[inline]
    fn get_max_phy_offset_in_consume_queue(&self) -> i64 {
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

    #[inline]
    fn get_max_offset(&self, topic: &CheetahString, queue_id: i32) -> Option<i64> {
        Some(
            self.inner
                .queue_offset_operator
                .current_queue_offset(&format!("{}-{}", topic, queue_id).into()),
        )
    }

    #[inline]
    fn find_or_create_consume_queue(
        &self,
        topic: &CheetahString,
        queue_id: i32,
    ) -> ArcConsumeQueue {
        let mut consume_queue_table = self.inner.consume_queue_table.lock();

        let topic_map = consume_queue_table.entry(topic.clone()).or_default();

        if let Some(value) = topic_map.get(&queue_id) {
            return value.clone();
        }

        let consume_queue = topic_map.entry(queue_id).or_insert_with(|| {
            let option = self.topic_config_table.lock().get(topic).cloned();
            match QueueTypeUtils::get_cq_type(&option) {
                CQType::SimpleCQ => ArcMut::new(Box::new(ConsumeQueue::new(
                    topic.clone(),
                    queue_id,
                    CheetahString::from_string(get_store_path_consume_queue(
                        self.inner.message_store_config.store_path_root_dir.as_str(),
                    )),
                    self.inner
                        .message_store_config
                        .get_mapped_file_size_consume_queue(),
                    self.inner.message_store_config.clone(),
                    self.running_flags.clone(),
                    self.store_checkpoint.clone(),
                ))),
                CQType::BatchCQ => ArcMut::new(Box::new(BatchConsumeQueue::new(
                    topic.clone(),
                    queue_id,
                    CheetahString::from_string(get_store_path_batch_consume_queue(
                        self.inner.message_store_config.store_path_root_dir.as_str(),
                    )),
                    self.inner
                        .message_store_config
                        .mapper_file_size_batch_consume_queue,
                    None,
                    self.inner.message_store_config.clone(),
                ))),
                CQType::RocksDBCQ => {
                    unimplemented!()
                }
            }
        });
        consume_queue.clone()
    }

    #[inline]
    fn find_consume_queue_map(
        &self,
        topic: &CheetahString,
    ) -> Option<HashMap<i32, ArcConsumeQueue>> {
        todo!()
    }

    #[inline]
    fn get_total_size(&self) -> i64 {
        todo!()
    }

    #[inline]
    fn get_store_time(&self, cq_unit: CqUnit) -> i64 {
        todo!()
    }

    #[inline]
    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        let queue = self.find_or_create_consume_queue(topic, queue_id);
        queue.get_min_offset_in_queue()
    }

    #[inline]
    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        todo!()
    }

    #[inline]
    fn get_consume_queue_table(&self) -> Arc<ConsumeQueueTable> {
        self.inner.consume_queue_table.clone()
    }
}

impl ConsumeQueueStore {
    #[inline]
    pub fn correct_min_offset(
        &self,
        consume_queue: &dyn ConsumeQueueTrait,
        min_commit_log_offset: i64,
    ) {
        consume_queue.correct_min_offset(min_commit_log_offset)
    }

    #[inline]
    pub fn set_batch_topic_queue_table(
        &self,
        batch_topic_queue_table: HashMap<CheetahString, i64>,
    ) {
        self.inner
            .queue_offset_operator
            .set_batch_topic_queue_table(batch_topic_queue_table)
    }

    #[inline]
    fn load_consume_queues(&mut self, store_path: CheetahString, cq_type: CQType) -> bool {
        let dir = Path::new(store_path.as_str());
        if let Ok(ls) = fs::read_dir(dir) {
            let dirs: Vec<_> = ls
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect();
            for dir in dirs {
                let topic = CheetahString::from_string(
                    dir.file_name().unwrap().to_str().unwrap().to_string(),
                );
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
                            topic.as_ref(),
                            queue_id,
                            cq_type,
                            store_path.clone(),
                        );
                        self.put_consume_queue(topic.clone(), queue_id, logic);
                        if !self.load_logic(&topic, queue_id) {
                            return false;
                        }
                    }
                }
            }
        }
        info!("load {:?} all over, OK", cq_type);
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
        consume_queue: Box<dyn ConsumeQueueTrait>,
    ) {
        let mut consume_queue_table = self.inner.consume_queue_table.lock();
        let topic_table = consume_queue_table.entry(topic).or_default();
        topic_table.insert(queue_id, ArcMut::new(consume_queue));
    }

    #[inline]
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

    /*

    #[inline]
    fn truncate_dirty_logic_files(&self, consume_queue: &dyn ConsumeQueueTrait, phy_offset: i64) {
        let file_queue_life_cycle = self.get_life_cycle(
            consume_queue.get_topic().as_str(),
            consume_queue.get_queue_id(),
        );
        file_queue_life_cycle
            .lock()
            .truncate_dirty_logic_files(phy_offset);

    }*/

    #[inline]
    fn truncate_dirty_logic_files(&self, topic: &CheetahString, queue_id: i32, phy_offset: i64) {
        let mut file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
        file_queue_life_cycle.truncate_dirty_logic_files(phy_offset);
    }
}

impl ConsumeQueueStore {
    #[inline]
    fn create_consume_queue_by_type(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        cq_type: CQType,
        store_path: CheetahString,
    ) -> Box<dyn ConsumeQueueTrait> {
        match cq_type {
            CQType::SimpleCQ => {
                let consume_queue = ConsumeQueue::new(
                    topic.clone(),
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
                    topic.clone(),
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

    #[inline]
    fn get_life_cycle(&self, topic: &CheetahString, queue_id: i32) -> ArcConsumeQueue {
        self.find_or_create_consume_queue(topic, queue_id)
    }
}
