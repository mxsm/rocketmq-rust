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

use std::collections::HashSet;

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;

use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;

pub(crate) mod rebalance_impl;
pub(crate) mod rebalance_push_impl;
pub(crate) mod rebalance_service;

#[trait_variant::make(Rebalance: Send)]
pub trait RebalanceLocal {
    /// Handles changes in the message queue.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic for which the message queue has changed.
    /// * `mq_all` - A set of all message queues.
    /// * `mq_divided` - A set of divided message queues.
    async fn message_queue_changed(
        &mut self,
        topic: &str,
        mq_all: &HashSet<MessageQueue>,
        mq_divided: &HashSet<MessageQueue>,
    );

    /// Removes an unnecessary message queue.
    ///
    /// # Arguments
    ///
    /// * `mq` - The message queue to be removed.
    /// * `pq` - The process queue associated with the message queue.
    ///
    /// # Returns
    ///
    /// A boolean indicating whether the message queue was removed.
    async fn remove_unnecessary_message_queue(&mut self, mq: &MessageQueue, pq: &ProcessQueue) -> bool;

    /// Removes an unnecessary pop message queue.
    ///
    /// # Arguments
    ///
    /// * `_mq` - The message queue to be removed.
    /// * `_pq` - The pop process queue associated with the message queue.
    ///
    /// # Returns
    ///
    /// A boolean indicating whether the pop message queue was removed.
    fn remove_unnecessary_pop_message_queue(&mut self, _mq: &MessageQueue, _pq: &PopProcessQueue) -> bool {
        true
    }

    /// Retrieves the consume type.
    ///
    /// # Returns
    ///
    /// The consume type.
    fn consume_type(&self) -> ConsumeType;

    /// Removes a dirty offset.
    ///
    /// # Arguments
    ///
    /// * `mq` - The message queue for which the offset should be removed.
    async fn remove_dirty_offset(&mut self, mq: &MessageQueue);

    /// Computes the pull offset with exception handling.
    ///
    /// # Arguments
    ///
    /// * `mq` - The message queue for which the pull offset should be computed.
    ///
    /// # Returns
    ///
    /// A result containing the pull offset or an error.
    async fn compute_pull_from_where_with_exception(
        &mut self,
        mq: &MessageQueue,
    ) -> rocketmq_error::RocketMQResult<i64>;

    /// Computes the pull offset.
    ///
    /// # Arguments
    ///
    /// * `mq` - The message queue for which the pull offset should be computed.
    ///
    /// # Returns
    ///
    /// The pull offset.
    async fn compute_pull_from_where(&mut self, mq: &MessageQueue) -> i64;

    /// Retrieves the consume initialization mode.
    ///
    /// # Returns
    ///
    /// The consume initialization mode.
    fn get_consume_init_mode(&self) -> i32;

    /// Dispatches pull requests.
    ///
    /// # Arguments
    ///
    /// * `pull_request_list` - A list of pull requests to be dispatched.
    /// * `delay` - The delay in milliseconds before dispatching the pull requests.
    async fn dispatch_pull_request(&self, pull_request_list: Vec<PullRequest>, delay: u64);

    /// Dispatches pop pull requests.
    ///
    /// # Arguments
    ///
    /// * `pop_request_list` - A list of pop pull requests to be dispatched.
    /// * `delay` - The delay in milliseconds before dispatching the pop pull requests.
    async fn dispatch_pop_pull_request(&self, pop_request_list: Vec<PopRequest>, delay: u64);

    /// Creates a process queue.
    ///
    /// # Returns
    ///
    /// A new process queue.
    fn create_process_queue(&self) -> ProcessQueue;

    /// Creates a pop process queue.
    ///
    /// # Returns
    ///
    /// A new pop process queue.
    fn create_pop_process_queue(&self) -> PopProcessQueue;

    /// Removes a process queue.
    ///
    /// # Arguments
    ///
    /// * `mq` - The message queue for which the process queue should be removed.
    async fn remove_process_queue(&mut self, mq: &MessageQueue);

    /// Unlocks a message queue.
    ///
    /// # Arguments
    ///
    /// * `mq` - The message queue to be unlocked.
    /// * `oneway` - A boolean indicating if the unlock should be one-way.
    async fn unlock(&mut self, mq: &MessageQueue, oneway: bool);

    /// Locks all message queues.
    fn lock_all(&self);

    /// Unlocks all message queues.
    ///
    /// # Arguments
    ///
    /// * `oneway` - A boolean indicating if the unlock should be one-way.
    fn unlock_all(&self, oneway: bool);

    /// Performs rebalancing.
    ///
    /// # Arguments
    ///
    /// * `is_order` - A boolean indicating if the rebalancing is ordered.
    ///
    /// # Returns
    ///
    /// A boolean indicating if the rebalancing was successful.
    async fn do_rebalance(&mut self, is_order: bool) -> bool;

    /// Performs client-side rebalancing.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic for which the rebalancing should be performed.
    ///
    /// # Returns
    ///
    /// A boolean indicating if the client-side rebalancing was successful.
    fn client_rebalance(&mut self, topic: &str) -> bool;

    /// Destroys the rebalancer.
    fn destroy(&mut self);
}
