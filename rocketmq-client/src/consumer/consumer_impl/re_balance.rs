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
use std::collections::HashSet;

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;

use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::Result;

pub(crate) mod rebalance_impl;
pub(crate) mod rebalance_push_impl;
pub(crate) mod rebalance_service;

#[trait_variant::make(Rebalance: Send)]
pub trait RebalanceLocal {
    fn message_queue_changed(
        &self,
        topic: &str,
        mq_all: &HashSet<MessageQueue>,
        mq_divided: &HashSet<MessageQueue>,
    );
    fn remove_unnecessary_message_queue(
        &self,
        topic: &str,
        mq: MessageQueue,
        pq: ProcessQueue,
    ) -> bool;

    fn remove_unnecessary_pop_message_queue(&self, mq: MessageQueue, pq: ProcessQueue) -> bool;
    fn remove_unnecessary_pop_message_queue_pop(
        &self,
        _mq: MessageQueue,
        _pq: PopProcessQueue,
    ) -> bool {
        true
    }

    fn consume_type(&self) -> ConsumeType;
    fn remove_dirty_offset(&self, mq: MessageQueue);

    fn compute_pull_from_where_with_exception(&self, mq: MessageQueue) -> Result<i64>;

    fn get_consume_init_mode(&self) -> i32;
    fn dispatch_pull_request(&self, pull_request_list: Vec<PullRequest>, delay: i64);
    fn dispatch_pop_pull_request(&self, pull_request_list: Vec<PopRequest>, delay: i64);
    fn create_process_queue(&self) -> ProcessQueue;
    fn create_pop_process_queue(&self) -> PopProcessQueue;
    fn remove_process_queue(&self, mq: MessageQueue);
    fn unlock(&self, mq: MessageQueue, oneway: bool);
    fn lock_all(&self);
    fn unlock_all(&self, oneway: bool);
    async fn do_rebalance(&mut self, is_order: bool) -> bool;

    fn client_rebalance(&mut self, topic: &str) -> bool {
        true
    }
}
