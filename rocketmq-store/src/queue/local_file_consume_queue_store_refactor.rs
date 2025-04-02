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
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_rust::ArcMut;
use tracing::info;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::message_store::MessageStore;
use crate::config::message_store_config::MessageStoreConfig;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::queue::batch_consume_queue::BatchConsumeQueue;
use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::single_consume_queue::ConsumeQueue;
use crate::queue::ArcConsumeQueue;
use crate::queue::ConsumeQueueTable;
use crate::queue::ConsumeQueueTrait;
use crate::queue::CqUnit;

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
    ) -> Self {
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
}

#[allow(unused_variables)]
impl ConsumeQueueStoreTrait for ConsumeQueueStore {
    async fn start(&self) {
        todo!()
    }

    fn load(&self) -> bool {
        todo!()
    }

    fn load_after_destroy(&self) -> bool {
        todo!()
    }

    async fn recover(&self) {
        todo!()
    }

    async fn recover_concurrently(&self) -> bool {
        todo!()
    }

    async fn shutdown(&self) -> bool {
        todo!()
    }

    async fn destroy(&self) {
        todo!()
    }

    async fn destroy_queue(
        &self,
        consume_queue: ArcMut<dyn crate::queue::consume_queue::ConsumeQueue>,
    ) {
        todo!()
    }

    fn flush(
        &self,
        consume_queue: ArcMut<dyn crate::queue::consume_queue::ConsumeQueue>,
        flush_least_pages: i32,
    ) -> bool {
        todo!()
    }

    async fn clean_expired(&self, min_phy_offset: i64) {
        todo!()
    }

    async fn check_self(&self) {
        todo!()
    }

    fn delete_expired_file(
        &self,
        consume_queue: ArcMut<dyn crate::queue::consume_queue::ConsumeQueue>,
        min_commit_log_pos: i64,
    ) -> i32 {
        todo!()
    }

    fn is_first_file_available(
        &self,
        consume_queue: ArcMut<dyn crate::queue::consume_queue::ConsumeQueue>,
    ) -> bool {
        todo!()
    }

    fn is_first_file_exist(
        &self,
        consume_queue: ArcMut<dyn crate::queue::consume_queue::ConsumeQueue>,
    ) -> bool {
        todo!()
    }

    fn roll_next_file(
        &self,
        consume_queue: ArcMut<dyn crate::queue::consume_queue::ConsumeQueue>,
        offset: i64,
    ) -> i64 {
        todo!()
    }

    async fn truncate_dirty(&self, offset_to_truncate: i64) {
        todo!()
    }

    fn put_message_position_info_wrapper(
        &self,
        consume_queue: ArcMut<dyn crate::queue::consume_queue::ConsumeQueue>,
        request: &DispatchRequest,
    ) {
        todo!()
    }

    fn put_message_position_info_wrapper_for_request(&self, request: &DispatchRequest) {
        todo!()
    }

    async fn range_query(
        &self,
        topic: &str,
        queue_id: i32,
        start_index: i64,
        num: i32,
    ) -> Vec<Bytes> {
        todo!()
    }

    async fn get(&self, topic: &str, queue_id: i32, start_index: i64) -> Bytes {
        todo!()
    }

    fn get_consume_queue_table(&self) -> Arc<ConsumeQueueTable> {
        todo!()
    }

    fn assign_queue_offset(&self, msg: &mut MessageExtBrokerInner) {
        todo!()
    }

    fn increase_queue_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        todo!()
    }

    fn increase_lmq_offset(&self, queue_key: &str, message_num: i16) {
        todo!()
    }

    fn get_lmq_queue_offset(&self, queue_key: &str) -> i64 {
        todo!()
    }

    async fn recover_offset_table(&self, min_phy_offset: i64) {
        todo!()
    }

    fn set_topic_queue_table(&mut self, topic_queue_table: HashMap<CheetahString, i64>) {
        todo!()
    }

    fn remove_topic_queue_table(&mut self, topic: &CheetahString, queue_id: i32) {
        todo!()
    }

    fn get_topic_queue_table(&self) -> HashMap<CheetahString, i64> {
        todo!()
    }

    fn get_max_phy_offset_in_consume_queue(&self, topic: &str, queue_id: i32) -> Option<i64> {
        todo!()
    }

    fn get_max_offset(&self, topic: &str, queue_id: i32) -> Option<i64> {
        todo!()
    }

    fn get_max_phy_offset_in_consume_queue_global(&self) -> i64 {
        todo!()
    }

    fn get_min_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64 {
        todo!()
    }

    fn get_max_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64 {
        todo!()
    }

    fn get_offset_in_queue_by_time(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        todo!()
    }

    fn find_or_create_consume_queue(
        &self,
        topic: &CheetahString,
        queue_id: i32,
    ) -> ArcMut<dyn crate::queue::consume_queue::ConsumeQueue> {
        todo!()
    }

    fn find_consume_queue_map(&self, topic: &str) -> Option<HashMap<i32, ArcConsumeQueue>> {
        todo!()
    }

    fn get_total_size(&self) -> i64 {
        todo!()
    }

    fn get_store_time(&self, cq_unit: &CqUnit) -> i64 {
        todo!()
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
    fn queue_type_should_be(&self, topic: &CheetahString, cq_type: CQType) {
        let topic_config = self
            .inner
            .message_store
            .as_ref()
            .unwrap()
            .get_topic_config(topic);
        let act = QueueTypeUtils::get_cq_type(&topic_config);
        if act != cq_type {
            panic!(
                "The queue type of topic: {} should be {:?}, but is {:?}",
                topic, cq_type, act
            );
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
    ) -> Box<dyn ConsumeQueueTrait> {
        match cq_type {
            CQType::SimpleCQ => {
                let ms_ref = self.inner.message_store.as_ref().unwrap();
                let consume_queue = ConsumeQueue::new(
                    topic.clone(),
                    queue_id,
                    store_path,
                    self.inner
                        .message_store_config
                        .get_mapped_file_size_consume_queue(),
                    self.inner.message_store_config.clone(),
                    ms_ref.get_running_flags_arc(),
                    ms_ref.get_store_checkpoint(),
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
        /* self.find_or_create_consume_queue(topic, queue_id) */
        unimplemented!("get_life_cycle")
    }
}
