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
use std::{collections::HashMap, sync::Arc};

use rocketmq_common::common::{
    attribute::cq_type::CQType, boundary_type::BoundaryType,
    message::message_single::MessageExtBrokerInner,
};
use tokio::sync::Mutex;

use crate::{
    base::{dispatch_request::DispatchRequest, swappable::Swappable},
    config::message_store_config::MessageStoreConfig,
    filter::MessageFilter,
    log_file::commit_log::CommitLog,
    queue::{
        consume_queue::{ConsumeQueueStoreTrait, ConsumeQueueTrait, CqUnit},
        file_queue::FileQueueLifeCycle,
        queue_offset_operator::QueueOffsetOperator,
    },
};

#[derive(Clone)]
pub struct LocalFileConsumeQueueStore<CQ> {
    inner: ConsumeQueueStoreInner<CQ>,
}

#[derive(Clone)]
struct ConsumeQueueStoreInner<CQ> {
    commit_log: Arc<Mutex<CommitLog>>,
    message_store_config: Arc<MessageStoreConfig>,
    queue_offset_operator: QueueOffsetOperator,
    consume_queue_table: HashMap<String, HashMap<i32, CQ>>,
}

impl<CQ> LocalFileConsumeQueueStore<CQ>
where
    CQ: ConsumeQueueTrait + Clone,
{
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        commit_log: Arc<Mutex<CommitLog>>,
    ) -> Self {
        Self {
            inner: ConsumeQueueStoreInner {
                commit_log,
                message_store_config,
                queue_offset_operator: QueueOffsetOperator {},
                consume_queue_table: HashMap::new(),
            },
        }
    }
}

#[allow(unused_variables)]
impl<CQ> ConsumeQueueStoreTrait for LocalFileConsumeQueueStore<CQ>
where
    CQ: ConsumeQueueTrait + Clone,
{
    fn start(&self) {
        todo!()
    }

    fn load(&mut self) -> bool {
        todo!()
    }

    fn load_after_destroy(&self) -> bool {
        todo!()
    }

    fn recover(&mut self) {
        for (_topic, consume_queue_table) in self.inner.consume_queue_table.iter_mut() {
            for (_queue_id, consume_queue) in consume_queue_table.iter_mut() {
                consume_queue.recover();
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
        todo!()
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
        todo!()
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
        todo!()
    }

    fn put_message_position_info_wrapper_single(&self, request: DispatchRequest) {
        todo!()
    }

    fn increase_queue_offset(&mut self, msg: MessageExtBrokerInner, message_num: i16) {
        todo!()
    }

    fn increase_lmq_offset(&mut self, queue_key: &str, message_num: i16) {
        todo!()
    }

    fn get_lmq_queue_offset(&self, queue_key: &str) -> i64 {
        todo!()
    }

    fn recover_offset_table(&mut self, min_phy_offset: i64) {
        todo!()
    }

    fn set_topic_queue_table(&mut self, topic_queue_table: HashMap<String, i64>) {
        todo!()
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
        -1
    }

    fn get_max_offset(&self, topic: &str, queue_id: i32) -> i64 {
        todo!()
    }

    fn find_or_create_consume_queue(
        &self,
        topic: &str,
        queue_id: i32,
    ) -> Box<dyn ConsumeQueueTrait> {
        todo!()
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
}

#[derive(Clone)]
pub struct LocalFileConsumeQueue {}

impl FileQueueLifeCycle for LocalFileConsumeQueue {
    fn load(&self) -> bool {
        todo!()
    }

    fn recover(&self) {
        println!("LocalFileConsumeQueue recover")
    }

    fn check_self(&self) {
        todo!()
    }

    fn flush(&self, flush_least_pages: i32) -> bool {
        todo!()
    }

    fn destroy(&self) {
        todo!()
    }

    fn truncate_dirty_logic_files(&self, max_commit_log_pos: i64) {
        todo!()
    }

    fn delete_expired_file(&self, min_commit_log_pos: i64) -> i32 {
        todo!()
    }

    fn roll_next_file(&self, next_begin_offset: i64) -> i64 {
        todo!()
    }

    fn is_first_file_available(&self) -> bool {
        todo!()
    }

    fn is_first_file_exist(&self) -> bool {
        todo!()
    }
}

impl Swappable for LocalFileConsumeQueue {
    fn swap_map(
        &self,
        reserve_num: i32,
        force_swap_interval_ms: i64,
        normal_swap_interval_ms: i64,
    ) {
        todo!()
    }

    fn clean_swapped_map(&self, _force_clean_swap_interval_ms: i64) {
        todo!()
    }
}

#[allow(unused_variables)]
impl ConsumeQueueTrait for LocalFileConsumeQueue {
    fn get_topic(&self) -> String {
        todo!()
    }

    fn get_queue_id(&self) -> i32 {
        todo!()
    }

    fn get(&self, index: i64) -> CqUnit {
        todo!()
    }

    fn get_earliest_unit(&self) -> CqUnit {
        todo!()
    }

    fn get_latest_unit(&self) -> CqUnit {
        todo!()
    }

    fn get_last_offset(&self) -> i64 {
        todo!()
    }

    fn get_min_offset_in_queue(&self) -> i64 {
        todo!()
    }

    fn get_max_offset_in_queue(&self) -> i64 {
        todo!()
    }

    fn get_message_total_in_queue(&self) -> i64 {
        todo!()
    }

    fn get_offset_in_queue_by_time(&self, timestamp: i64) -> i64 {
        todo!()
    }

    fn get_offset_in_queue_by_time_boundary(
        &self,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        todo!()
    }

    fn get_max_physic_offset(&self) -> i64 {
        todo!()
    }

    fn get_min_logic_offset(&self) -> i64 {
        todo!()
    }

    fn get_cq_type(&self) -> CQType {
        todo!()
    }

    fn get_total_size(&self) -> i64 {
        todo!()
    }

    fn get_unit_size(&self) -> i32 {
        todo!()
    }

    fn correct_min_offset(&self, min_commit_log_offset: i64) {
        todo!()
    }

    fn put_message_position_info_wrapper(&self, request: DispatchRequest) {
        todo!()
    }

    fn increase_queue_offset(
        &self,
        queue_offset_assigner: QueueOffsetOperator,
        msg: MessageExtBrokerInner,
        message_num: i16,
    ) {
        todo!()
    }

    fn estimate_message_count(&self, from: i64, to: i64, filter: &dyn MessageFilter) -> i64 {
        todo!()
    }
}
