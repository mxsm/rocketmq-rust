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

use rocketmq_common::common::{
    attribute::cq_type::CQType, boundary_type::BoundaryType,
    message::message_single::MessageExtBrokerInner,
};

use crate::{
    base::{dispatch_request::DispatchRequest, swappable::Swappable},
    filter::MessageFilter,
    queue::{
        queue_offset_operator::QueueOffsetOperator, ConsumeQueueTrait, CqUnit, FileQueueLifeCycle,
    },
};

pub struct BatchConsumeQueue {}

#[allow(unused_variables)]
impl FileQueueLifeCycle for BatchConsumeQueue {
    fn load(&self) -> bool {
        todo!()
    }

    fn recover(&self) {
        todo!()
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

impl Swappable for BatchConsumeQueue {
    fn swap_map(
        &self,
        reserve_num: i32,
        force_swap_interval_ms: i64,
        normal_swap_interval_ms: i64,
    ) {
        todo!()
    }

    fn clean_swapped_map(&self, force_clean_swap_interval_ms: i64) {
        todo!()
    }
}

impl ConsumeQueueTrait for BatchConsumeQueue {
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
