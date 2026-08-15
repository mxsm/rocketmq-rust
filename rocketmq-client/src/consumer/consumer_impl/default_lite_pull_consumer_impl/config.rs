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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::common::util_all;

use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;

/// Configuration specific to lite pull consumer.
#[derive(Clone)]
pub struct LitePullConsumerConfig {
    /// Consumer group name.
    pub consumer_group: CheetahString,
    /// Message model (clustering or broadcasting).
    pub message_model: MessageModel,
    /// Where to start consuming from when no offset exists.
    pub consume_from_where: ConsumeFromWhere,
    /// Timestamp to consume from (when consume_from_where is CONSUME_FROM_TIMESTAMP).
    pub consume_timestamp: Option<CheetahString>,
    /// Strategy for allocating message queues among consumers.
    pub allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy + Send + Sync>,
    /// Whether the subscription group runs in unit mode.
    pub unit_mode: bool,
    /// Whether this instance provides only the manual Classic Pull compatibility surface.
    ///
    /// Manual mode participates in consumer registration and queue allocation but never starts
    /// LitePull background pull tasks.
    pub classic_pull_manual_mode: bool,
    /// Number of messages to pull in a single request.
    pub pull_batch_size: i32,
    /// Number of concurrent pull threads.
    pub pull_thread_nums: usize,
    /// Whether broker selection is controlled by the user-configured default broker ID.
    pub connect_broker_by_user: bool,
    /// Broker ID used when `connect_broker_by_user` is enabled.
    pub default_broker_id: u64,
    /// Maximum number of messages cached per queue.
    pub pull_threshold_for_queue: i64,
    /// Maximum size in MiB of messages cached per queue.
    pub pull_threshold_size_for_queue: i32,
    /// Maximum total number of cached messages across all queues.
    pub pull_threshold_for_all: i64,
    /// Maximum offset span allowed in a process queue.
    pub consume_max_span: i64,
    /// Delay in milliseconds when pull encounters an exception.
    pub pull_time_delay_millis_when_exception: u64,
    /// Delay in milliseconds when cache flow control is triggered.
    pub pull_time_delay_millis_when_cache_flow_control: u64,
    /// Delay in milliseconds when broker flow control is triggered.
    pub pull_time_delay_millis_when_broker_flow_control: u64,
    /// Maximum time in milliseconds that the broker may suspend a long-poll pull request.
    pub broker_suspend_max_time_millis: u64,
    /// Consumer-side timeout in milliseconds for suspended long-poll pull requests.
    pub consumer_timeout_millis_when_suspend: u64,
    /// Timeout in milliseconds for a lite pull RPC when a non-blocking pull timeout is used.
    pub consumer_pull_timeout_millis: u64,
    /// Default timeout for poll operations in milliseconds.
    pub poll_timeout_millis: u64,
    /// Whether to automatically commit offsets.
    pub auto_commit: bool,
    /// Interval in milliseconds between automatic offset commits.
    pub auto_commit_interval_millis: u64,
    /// Interval in milliseconds for checking topic metadata changes.
    pub topic_metadata_check_interval_millis: u64,
    /// Message request mode (pull or pop).
    pub message_request_mode: MessageRequestMode,
}

pub(crate) fn default_lite_pull_consume_timestamp() -> CheetahString {
    let thirty_minutes_ago = current_millis().saturating_sub(1000 * 60 * 30);
    CheetahString::from_string(util_all::time_millis_to_human_string3(thirty_minutes_ago as i64))
}

#[allow(deprecated)]
pub(crate) fn validate_lite_pull_consume_from_where(consume_from_where: ConsumeFromWhere) -> RocketMQResult<()> {
    match consume_from_where {
        ConsumeFromWhere::ConsumeFromFirstOffset
        | ConsumeFromWhere::ConsumeFromLastOffset
        | ConsumeFromWhere::ConsumeFromTimestamp => Ok(()),
        ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst
        | ConsumeFromWhere::ConsumeFromMinOffset
        | ConsumeFromWhere::ConsumeFromMaxOffset => Err(crate::mq_client_err!("Invalid ConsumeFromWhere Value")),
    }
}

impl Default for LitePullConsumerConfig {
    fn default() -> Self {
        Self {
            consumer_group: CheetahString::from_static_str("DEFAULT_CONSUMER"),
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            consume_timestamp: Some(default_lite_pull_consume_timestamp()),
            allocate_message_queue_strategy: Arc::new(
                crate::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely,
            ),
            unit_mode: false,
            classic_pull_manual_mode: false,
            pull_batch_size: 10,
            pull_thread_nums: 20,
            connect_broker_by_user: false,
            default_broker_id: mix_all::MASTER_ID,
            pull_threshold_for_queue: 1000,
            pull_threshold_size_for_queue: 100,
            pull_threshold_for_all: 10000,
            consume_max_span: 2000,
            pull_time_delay_millis_when_exception: 1000,
            pull_time_delay_millis_when_cache_flow_control: 50,
            pull_time_delay_millis_when_broker_flow_control: 20,
            broker_suspend_max_time_millis: 20_000,
            consumer_timeout_millis_when_suspend: 30_000,
            consumer_pull_timeout_millis: 10_000,
            poll_timeout_millis: 5000,
            auto_commit: true,
            auto_commit_interval_millis: 5000,
            topic_metadata_check_interval_millis: 30000,
            message_request_mode: MessageRequestMode::Pull,
        }
    }
}

impl LitePullConsumerConfig {
    /// Converts LitePullConsumerConfig to ConsumerConfig for rebalance.
    pub(super) fn to_consumer_config(&self) -> ConsumerConfig {
        ConsumerConfig {
            consumer_group: self.consumer_group.clone(),
            topic: CheetahString::from_static_str(""),
            sub_expression: CheetahString::from_static_str("*"),
            message_model: self.message_model,
            consume_from_where: self.consume_from_where,
            consume_timestamp: self.consume_timestamp.clone(),
            allocate_message_queue_strategy: Some(self.allocate_message_queue_strategy.clone()),
            subscription: Arc::new(HashMap::new()),
            message_listener: None,
            message_queue_listener: None,
            offset_store: None,
            consume_thread_min: 20,
            consume_thread_max: 20,
            adjust_thread_pool_nums_threshold: 100000,
            consume_concurrently_max_span: 2000,
            pull_threshold_for_queue: self.pull_threshold_for_queue as u32,
            pop_threshold_for_queue: 1024,
            pull_threshold_size_for_queue: self.pull_threshold_size_for_queue as u32,
            pull_threshold_for_topic: -1,
            pull_threshold_size_for_topic: -1,
            pull_interval: 0,
            consume_message_batch_max_size: 1,
            pull_batch_size: self.pull_batch_size as u32,
            pull_batch_size_in_bytes: 0,
            post_subscription_when_pull: false,
            unit_mode: self.unit_mode,
            max_reconsume_times: -1,
            suspend_current_queue_time_millis: 1000,
            consume_timeout: 15,
            pop_invisible_time: 60000,
            pop_batch_nums: 32,
            await_termination_millis_when_shutdown: 0,
            trace_dispatcher: None,
            client_rebalance: false,
            rpc_hook: None,
        }
    }
}
