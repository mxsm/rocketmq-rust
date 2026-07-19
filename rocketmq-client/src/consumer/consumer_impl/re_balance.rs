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
use rocketmq_common::utils::util_all;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;

use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::pull_request::PullRequest;

pub(crate) mod rebalance_impl;
pub(crate) mod rebalance_lite_pull_impl;
pub(crate) mod rebalance_push_impl;
pub(crate) mod rebalance_service;

pub(crate) fn parse_consume_timestamp_millis(
    consume_timestamp: Option<&str>,
    mq: &MessageQueue,
) -> rocketmq_error::RocketMQResult<u64> {
    let Some(consume_timestamp) = consume_timestamp else {
        return Err(crate::mq_client_err!(
            ResponseCode::SystemError as i32,
            format!("Consume timestamp is not configured for mq: {}", mq)
        ));
    };
    let timestamp = util_all::parse_date_to_millis(consume_timestamp, util_all::YYYYMMDDHHMMSS).ok_or_else(|| {
        crate::mq_client_err!(
            ResponseCode::SystemError as i32,
            format!("Failed to parse consume timestamp for mq: {}", mq)
        )
    })?;
    u64::try_from(timestamp).map_err(|_| {
        crate::mq_client_err!(
            ResponseCode::SystemError as i32,
            format!("Consume timestamp is before Unix epoch for mq: {}", mq)
        )
    })
}

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
        &self,
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
    async fn remove_unnecessary_message_queue(&self, mq: &MessageQueue, pq: &ProcessQueue) -> bool;

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
    fn remove_unnecessary_pop_message_queue(&self, _mq: &MessageQueue, _pq: &PopProcessQueue) -> bool {
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
    async fn remove_dirty_offset(&self, mq: &MessageQueue);

    /// Computes the pull offset with exception handling.
    ///
    /// # Arguments
    ///
    /// * `mq` - The message queue for which the pull offset should be computed.
    ///
    /// # Returns
    ///
    /// A result containing the pull offset or an error.
    async fn compute_pull_from_where_with_exception(&self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64>;

    /// Computes the pull offset.
    ///
    /// # Arguments
    ///
    /// * `mq` - The message queue for which the pull offset should be computed.
    ///
    /// # Returns
    ///
    /// The pull offset.
    async fn compute_pull_from_where(&self, mq: &MessageQueue) -> i64;

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
    async fn remove_process_queue(&self, mq: &MessageQueue);

    /// Unlocks a message queue.
    ///
    /// # Arguments
    ///
    /// * `mq` - The message queue to be unlocked.
    /// * `oneway` - A boolean indicating if the unlock should be one-way.
    async fn unlock(&self, mq: &MessageQueue, oneway: bool);

    /// Locks all message queues.
    async fn lock_all(&self);

    /// Unlocks all message queues.
    ///
    /// # Arguments
    ///
    /// * `oneway` - A boolean indicating if the unlock should be one-way.
    async fn unlock_all(&self, oneway: bool);

    /// Performs rebalancing.
    ///
    /// # Arguments
    ///
    /// * `is_order` - A boolean indicating if the rebalancing is ordered.
    ///
    /// # Returns
    ///
    /// A boolean indicating if the rebalancing was successful.
    async fn do_rebalance(&self, is_order: bool) -> bool;

    /// Performs client-side rebalancing.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic for which the rebalancing should be performed.
    ///
    /// # Returns
    ///
    /// A boolean indicating if the client-side rebalancing was successful.
    fn client_rebalance(&self, topic: &str) -> bool;

    /// Destroys the rebalancer.
    fn destroy(&self);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_consume_timestamp_uses_java_millis_in_local_timezone() {
        let mq = MessageQueue::from_parts("topic-a", "broker-a", 0);

        let timestamp =
            parse_consume_timestamp_millis(Some("20250102030405"), &mq).expect("Java consume timestamp should parse");

        assert_eq!(
            timestamp,
            util_all::parse_date_to_millis("20250102030405", util_all::YYYYMMDDHHMMSS)
                .expect("Java consume timestamp should parse") as u64
        );
        assert!(timestamp > 1_000_000_000_000);
    }

    #[test]
    fn parse_consume_timestamp_rejects_missing_or_invalid_values() {
        let mq = MessageQueue::from_parts("topic-a", "broker-a", 0);

        assert!(parse_consume_timestamp_millis(None, &mq).is_err());
        assert!(parse_consume_timestamp_millis(Some("2025-01-02 03:04:05"), &mq).is_err());
    }
}
