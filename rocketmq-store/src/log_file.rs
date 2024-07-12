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

use std::sync::Arc;

use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_single::MessageExtBrokerInner;
use rocketmq_common::TimeUtils::get_current_millis;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_result::PutMessageResult;
use crate::filter::MessageFilter;
use crate::hook::put_message_hook::BoxedPutMessageHook;
use crate::queue::ArcConsumeQueue;
use crate::stats::broker_stats_manager::BrokerStatsManager;
use crate::store::running_flags::RunningFlags;

pub(crate) mod cold_data_check_service;
pub mod commit_log;
pub mod flush_manager_impl;
pub mod mapped_file;

pub const MAX_PULL_MSG_SIZE: i32 = 128 * 1024 * 1024;

#[trait_variant::make(MessageStore: Send)]
pub trait RocketMQMessageStore: Clone + 'static {
    /// Load previously stored messages.
    ///
    /// Returns `true` if success; `false` otherwise.
    async fn load(&mut self) -> bool;

    /// Launch this message store.
    ///
    /// # Throws
    ///
    /// Throws an `Exception` if there is any error.
    fn start(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    fn shutdown(&mut self);

    fn set_confirm_offset(&mut self, phy_offset: i64);

    fn get_max_phy_offset(&self) -> i64;

    fn set_broker_init_max_offset(&mut self, broker_init_max_offset: i64);

    fn now(&self) -> u64 {
        get_current_millis()
    }

    fn get_state_machine_version(&self) -> i64;

    async fn put_message(&mut self, msg: MessageExtBrokerInner) -> PutMessageResult;

    async fn put_messages(&mut self, msg_batch: MessageExtBatch) -> PutMessageResult;

    fn truncate_files(&mut self, offset_to_truncate: i64) -> bool;

    fn is_os_page_cache_busy(&self) -> bool {
        false
    }

    fn get_running_flags(&self) -> &RunningFlags;

    fn is_shutdown(&self) -> bool;

    fn get_put_message_hook_list(&self) -> Arc<parking_lot::RwLock<Vec<BoxedPutMessageHook>>>;

    fn set_put_message_hook(&self, put_message_hook: BoxedPutMessageHook);

    fn get_broker_stats_manager(&self) -> Option<Arc<BrokerStatsManager>>;

    fn dispatch_behind_bytes(&self);

    fn get_min_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64;
    fn get_max_offset_in_queue(&self, topic: &str, queue_id: i32) -> i64;
    fn get_max_offset_in_queue_committed(&self, topic: &str, queue_id: i32, committed: bool)
        -> i64;

    async fn get_message(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<&dyn MessageFilter>,
    ) -> Option<GetMessageResult>;

    fn check_in_mem_by_consume_offset(
        &self,
        topic: &str,
        queue_id: i32,
        consume_offset: i64,
        batch_size: i32,
    ) -> bool;

    fn notify_message_arrive_if_necessary(&self, dispatch_request: &mut DispatchRequest);

    fn find_consume_queue(&self, topic: &str, queue_id: i32) -> Option<ArcConsumeQueue>;

    fn delete_topics(&self, delete_topics: Vec<String>);
}
