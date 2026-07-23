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

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::FAQUrl;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::error_response;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::peek_message_request_header::PeekMessageRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::error;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetQueryCapability;
use crate::processor::processor_service::pop_buffer_merge_service::PopBufferMergeService;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

#[derive(Clone)]
pub(crate) struct PeekMessagePolicy {
    broker_permission: u32,
    broker_ip1: CheetahString,
    revive_queue_num: u32,
    transfer_msg_by_heap: bool,
    enable_retry_topic_v2: bool,
}

impl PeekMessagePolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig) -> Self {
        Self {
            broker_permission: broker_config.broker_permission,
            broker_ip1: broker_config.broker_ip1().clone(),
            revive_queue_num: broker_config.revive_queue_num,
            transfer_msg_by_heap: broker_config.transfer_msg_by_heap,
            enable_retry_topic_v2: broker_config.enable_retry_topic_v2,
        }
    }
}

pub(crate) struct PeekMessageStoreCapability<MS: MessageStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> PeekMessageStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
    }

    fn now(&self) -> u64 {
        self.escape_bridge
            .upgrade()
            .and_then(|bridge| bridge.local_store_now().ok())
            .unwrap_or(0)
    }

    fn min_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.escape_bridge
            .upgrade()
            .ok_or(MessageStoreUnavailable)?
            .get_min_offset_from_local_store(topic, queue_id)
    }

    fn max_offset(&self, topic: &CheetahString, queue_id: i32) -> Result<i64, MessageStoreUnavailable> {
        self.escape_bridge
            .upgrade()
            .ok_or(MessageStoreUnavailable)?
            .get_max_offset_from_local_store(topic, queue_id)
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
    ) -> Result<Option<GetMessageResult>, MessageStoreUnavailable> {
        self.escape_bridge
            .upgrade()
            .ok_or(MessageStoreUnavailable)?
            .get_message_from_local_store(group, topic, queue_id, offset, max_msg_nums)
            .await
    }
}

pub(crate) struct PeekPopOffsetCapability<MS: MessageStore> {
    merge_service: Weak<PopBufferMergeService<MS>>,
}

impl<MS: MessageStore> PeekPopOffsetCapability<MS> {
    pub(crate) fn new(merge_service: &Arc<PopBufferMergeService<MS>>) -> Self {
        Self {
            merge_service: Arc::downgrade(merge_service),
        }
    }

    async fn latest_offset(&self, topic: &CheetahString, group: &CheetahString, queue_id: i32) -> i64 {
        let Some(service) = self.merge_service.upgrade() else {
            return -1;
        };
        let key = CheetahString::from_string(KeyBuilder::build_polling_key(topic.as_str(), group.as_str(), queue_id));
        service.get_latest_offset(&key).await
    }
}

pub(crate) struct PeekMessageProcessorContext<MS: MessageStore> {
    policy: PeekMessagePolicy,
    topic_config_manager: Arc<TopicConfigManager>,
    subscription_group_lookup: SubscriptionGroupConfigLookup,
    consumer_offset_query: ConsumerOffsetQueryCapability<MS>,
    broker_stats_manager: Arc<BrokerStatsManager>,
    message_store: PeekMessageStoreCapability<MS>,
    pop_offset: PeekPopOffsetCapability<MS>,
}

impl<MS: MessageStore> PeekMessageProcessorContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor lists the complete narrow Peek capability boundary"
    )]
    pub(crate) fn new(
        policy: PeekMessagePolicy,
        topic_config_manager: Arc<TopicConfigManager>,
        subscription_group_lookup: SubscriptionGroupConfigLookup,
        consumer_offset_query: ConsumerOffsetQueryCapability<MS>,
        broker_stats_manager: Arc<BrokerStatsManager>,
        message_store: PeekMessageStoreCapability<MS>,
        pop_offset: PeekPopOffsetCapability<MS>,
    ) -> Self {
        Self {
            policy,
            topic_config_manager,
            subscription_group_lookup,
            consumer_offset_query,
            broker_stats_manager,
            message_store,
            pop_offset,
        }
    }
}

/// Handles peek message requests from clients.
pub struct PeekMessageProcessor<MS: MessageStore> {
    context: PeekMessageProcessorContext<MS>,
    random_counter: AtomicU32,
}

impl<MS: MessageStore> PeekMessageProcessor<MS> {
    pub(crate) fn new(context: PeekMessageProcessorContext<MS>) -> Self {
        Self {
            context,
            random_counter: AtomicU32::new(0),
        }
    }
}

impl<MS: MessageStore> RequestProcessor for PeekMessageProcessor<MS> {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_internal(channel, ctx, request, true).await
    }
}

impl<MS: MessageStore> PeekMessageProcessor<MS> {
    pub async fn process_request_shared(
        &self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_internal(channel, ctx, request, true).await
    }

    async fn process_request_internal(
        &self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
        _broker_allow_suspend: bool,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let begin_time_mills = self.context.message_store.now();

        let mut response = RemotingCommand::create_response_command_with_header(PopMessageResponseHeader::default())
            .set_opaque(request.opaque());

        // Decode request header
        let request_header = match request.decode_command_custom_header::<PeekMessageRequestHeader>() {
            Ok(header) => header,
            Err(e) => {
                error!(
                    "Failed to decode PeekMessageRequestHeader: {:?}, channel: {}",
                    e,
                    channel.remote_address()
                );
                let remark = format!("decode request header failed: {:?}", e);
                let error = RocketMQError::request_header_error(remark.clone());
                return Ok(Some(error_response::command_from_error_with_remark_and_opaque(
                    &error,
                    remark,
                    request.opaque(),
                )));
            }
        };

        if !PermName::is_readable(self.context.policy.broker_permission) {
            let response = response.set_code(ResponseCode::NoPermission).set_remark(format!(
                "the broker[{}] peeking message is forbidden",
                self.context.policy.broker_ip1
            ));
            return Ok(Some(response));
        }

        let topic_config = self
            .context
            .topic_config_manager
            .select_topic_config(&request_header.topic);

        if topic_config.is_none() {
            error!(
                "The topic {} not exist, consumer: {}",
                request_header.topic,
                channel.remote_address()
            );
            let response = response.set_code(ResponseCode::TopicNotExist).set_remark(format!(
                "topic[{}] not exist, apply first please! {}",
                request_header.topic,
                FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
            ));
            return Ok(Some(response));
        }

        let topic_config = topic_config.unwrap();

        if !PermName::is_readable(topic_config.perm) {
            let response = response.set_code(ResponseCode::NoPermission).set_remark(format!(
                "the topic[{}] peeking message is forbidden",
                request_header.topic
            ));
            return Ok(Some(response));
        }

        if request_header.queue_id >= topic_config.read_queue_nums as i32 {
            let error_info = format!(
                "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                request_header.queue_id,
                request_header.topic,
                topic_config.read_queue_nums,
                channel.remote_address()
            );
            warn!("{}", error_info);
            let response = response.set_code(ResponseCode::SystemError).set_remark(error_info);
            return Ok(Some(response));
        }

        let subscription_group_config = self
            .context
            .subscription_group_lookup
            .find_subscription_group_config(&request_header.consumer_group);

        if subscription_group_config.is_none() {
            let response = response
                .set_code(ResponseCode::SubscriptionGroupNotExist)
                .set_remark(format!(
                    "subscription group [{}] does not exist, {}",
                    request_header.consumer_group,
                    FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                ));
            return Ok(Some(response));
        }

        let subscription_group_config = subscription_group_config.unwrap();

        if !subscription_group_config.consume_enable() {
            let response = response.set_code(ResponseCode::NoPermission).set_remark(format!(
                "subscription group no permission, {}",
                request_header.consumer_group
            ));
            return Ok(Some(response));
        }

        // Random seed for queue selection
        let random_q = self.random_counter.fetch_add(1, Ordering::Relaxed) % 100;
        let revive_qid = (random_q % self.context.policy.revive_queue_num) as i32;

        let mut get_message_result = GetMessageResult::new_result_size(request_header.max_msg_nums as usize);
        let mut rest_num: i64 = 0;
        let pop_time = self.context.message_store.now() as i64;

        let need_retry = random_q.is_multiple_of(5);

        if need_retry {
            rest_num = self
                .peek_retry_topic(
                    &request_header,
                    &mut get_message_result,
                    random_q,
                    revive_qid,
                    &channel,
                    pop_time,
                )
                .await;
        }

        if request_header.queue_id < 0 {
            for i in 0..topic_config.read_queue_nums {
                let queue_id = ((random_q + i) % topic_config.read_queue_nums) as i32;
                rest_num = self
                    .peek_msg_from_queue(
                        false,
                        &mut get_message_result,
                        &request_header,
                        queue_id,
                        rest_num,
                        revive_qid,
                        &channel,
                        pop_time,
                    )
                    .await;
            }
        } else {
            rest_num = self
                .peek_msg_from_queue(
                    false,
                    &mut get_message_result,
                    &request_header,
                    request_header.queue_id,
                    rest_num,
                    revive_qid,
                    &channel,
                    pop_time,
                )
                .await;
        }

        if !need_retry && get_message_result.message_mapped_list().len() < request_header.max_msg_nums as usize {
            rest_num = self
                .peek_retry_topic(
                    &request_header,
                    &mut get_message_result,
                    random_q,
                    revive_qid,
                    &channel,
                    pop_time,
                )
                .await;
        }

        if !get_message_result.message_mapped_list().is_empty() {
            response = response.set_code(ResponseCode::Success);
        } else {
            response = response.set_code(ResponseCode::PullNotFound);
        }

        if let Some(response_header) = response.read_custom_header_mut::<PopMessageResponseHeader>() {
            response_header.rest_num = rest_num as u64;
        }

        response = response.set_remark(format!(
            "{:?}",
            get_message_result
                .status()
                .unwrap_or(GetMessageStatus::NoMessageInQueue)
        ));

        match response.code_ref() {
            &code if code == ResponseCode::Success as i32 => {
                // Record statistics
                self.context.broker_stats_manager.inc_group_get_nums(
                    &request_header.consumer_group,
                    &request_header.topic,
                    get_message_result.message_count(),
                );
                self.context.broker_stats_manager.inc_group_get_size(
                    &request_header.consumer_group,
                    &request_header.topic,
                    get_message_result.buffer_total_size(),
                );
                self.context
                    .broker_stats_manager
                    .inc_broker_get_nums(&request_header.topic, get_message_result.message_count());

                // Transfer messages to response body
                if self.context.policy.transfer_msg_by_heap {
                    // Transfer by heap
                    let body = self.read_get_message_result(
                        &get_message_result,
                        &request_header.consumer_group,
                        &request_header.topic,
                        request_header.queue_id,
                    );

                    self.context.broker_stats_manager.inc_group_get_latency(
                        &request_header.consumer_group,
                        &request_header.topic,
                        request_header.queue_id,
                        self.context.message_store.now().saturating_sub(begin_time_mills) as i32,
                    );

                    if let Some(body_bytes) = body {
                        response = response.set_body(body_bytes.to_vec());
                    }
                } else {
                    // Transfer by heap allocation (zero-copy not available in this context)
                    let body = self.read_get_message_result(
                        &get_message_result,
                        &request_header.consumer_group,
                        &request_header.topic,
                        request_header.queue_id,
                    );
                    if let Some(body_bytes) = body {
                        response = response.set_body(body_bytes.to_vec());
                    }
                }
            }
            _ => {
                // No action needed for other response codes
            }
        }

        Ok(Some(response))
    }

    async fn peek_retry_topic(
        &self,
        request_header: &PeekMessageRequestHeader,
        get_message_result: &mut GetMessageResult,
        random_q: u32,
        revive_qid: i32,
        channel: &Channel,
        pop_time: i64,
    ) -> i64 {
        let retry_topic = CheetahString::from_string(KeyBuilder::build_pop_retry_topic(
            request_header.topic.as_str(),
            request_header.consumer_group.as_str(),
            self.context.policy.enable_retry_topic_v2,
        ));

        let retry_topic_config = self.context.topic_config_manager.select_topic_config(&retry_topic);

        let mut rest_num = 0i64;

        if let Some(retry_config) = retry_topic_config {
            for i in 0..retry_config.read_queue_nums {
                let queue_id = ((random_q + i) % retry_config.read_queue_nums) as i32;
                rest_num = self
                    .peek_msg_from_queue(
                        true,
                        get_message_result,
                        request_header,
                        queue_id,
                        rest_num,
                        revive_qid,
                        channel,
                        pop_time,
                    )
                    .await;
            }
        }

        rest_num
    }

    async fn peek_msg_from_queue(
        &self,
        is_retry: bool,
        get_message_result: &mut GetMessageResult,
        request_header: &PeekMessageRequestHeader,
        queue_id: i32,
        mut rest_num: i64,
        _revive_qid: i32,
        _channel: &Channel,
        _pop_time: i64,
    ) -> i64 {
        // Determine topic (retry or normal)
        let topic = if is_retry {
            CheetahString::from_string(KeyBuilder::build_pop_retry_topic(
                request_header.topic.as_str(),
                request_header.consumer_group.as_str(),
                self.context.policy.enable_retry_topic_v2,
            ))
        } else {
            request_header.topic.clone()
        };

        // Get starting offset
        let offset = self
            .get_pop_offset(&topic, &request_header.consumer_group, queue_id)
            .await;

        let Ok(max_offset) = self.context.message_store.max_offset(&topic, queue_id) else {
            return rest_num;
        };

        // Calculate rest num (remaining messages in queue)
        rest_num += max_offset - offset;

        // Check if we already have enough messages
        if get_message_result.message_mapped_list().len() >= request_header.max_msg_nums as usize {
            return rest_num;
        }

        // Calculate how many messages to fetch
        let max_to_fetch = request_header.max_msg_nums - get_message_result.message_mapped_list().len() as i32;

        // Get messages from store
        let mut offset_to_use = offset;
        let Ok(get_message_tmp_result) = self
            .context
            .message_store
            .get_message(
                &request_header.consumer_group,
                &topic,
                queue_id,
                offset_to_use,
                max_to_fetch,
            )
            .await
        else {
            return rest_num;
        };

        if let Some(mut tmp_result) = get_message_tmp_result {
            // Handle offset correction if needed
            if matches!(
                tmp_result.status(),
                Some(GetMessageStatus::OffsetTooSmall) | Some(GetMessageStatus::OffsetOverflowBadly)
            ) {
                offset_to_use = tmp_result.next_begin_offset();
                tmp_result = self
                    .context
                    .message_store
                    .get_message(
                        &request_header.consumer_group,
                        &topic,
                        queue_id,
                        offset_to_use,
                        max_to_fetch,
                    )
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(tmp_result);
            }

            // Add messages to result
            for mapped_buffer in tmp_result.message_mapped_vec() {
                get_message_result.add_message_inner(mapped_buffer);
            }
        }

        rest_num
    }

    async fn get_pop_offset(&self, topic: &CheetahString, consumer_group: &CheetahString, queue_id: i32) -> i64 {
        // Get consumer offset
        let mut offset = self
            .context
            .consumer_offset_query
            .query_offset(consumer_group, topic, queue_id);

        // If no consumer offset, use min offset
        if offset < 0 {
            if let Ok(min_offset) = self.context.message_store.min_offset(topic, queue_id) {
                offset = min_offset;
            }
        }

        // Get pop buffer offset
        let buffer_offset = self
            .context
            .pop_offset
            .latest_offset(topic, consumer_group, queue_id)
            .await;

        if buffer_offset < 0 {
            offset
        } else {
            offset.max(buffer_offset)
        }
    }

    fn read_get_message_result(
        &self,
        get_message_result: &GetMessageResult,
        _group: &str,
        _topic: &str,
        _queue_id: i32,
    ) -> Option<Bytes> {
        if get_message_result.buffer_total_size() <= 0 || get_message_result.message_mapped_list().is_empty() {
            return None;
        }

        let mut bytes_mut = BytesMut::with_capacity(get_message_result.buffer_total_size() as usize);

        for msg in get_message_result.message_mapped_list() {
            bytes_mut.extend_from_slice(msg.get_buffer());
        }

        Some(bytes_mut.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_store::message_store::OwnedMessageStore;

    #[test]
    fn peek_policy_captures_only_required_startup_values() {
        let broker_config = BrokerConfig {
            broker_permission: 3,
            broker_ip1: CheetahString::from_static_str("192.0.2.10"),
            revive_queue_num: 12,
            transfer_msg_by_heap: false,
            enable_retry_topic_v2: true,
            ..Default::default()
        };

        let policy = PeekMessagePolicy::from_config(&broker_config);

        assert_eq!(policy.broker_permission, 3);
        assert_eq!(policy.broker_ip1, "192.0.2.10");
        assert_eq!(policy.revive_queue_num, 12);
        assert!(!policy.transfer_msg_by_heap);
        assert!(policy.enable_retry_topic_v2);
    }

    #[tokio::test]
    async fn peek_store_capability_fails_closed_after_provider_shutdown() {
        let capability = PeekMessageStoreCapability::<OwnedMessageStore> {
            escape_bridge: Weak::new(),
        };
        let topic = CheetahString::from_static_str("topic-a");
        let group = CheetahString::from_static_str("group-a");

        assert_eq!(capability.now(), 0);
        assert!(capability.min_offset(&topic, 0).is_err());
        assert!(capability.max_offset(&topic, 0).is_err());
        assert!(capability.get_message(&group, &topic, 0, 0, 1).await.is_err());
    }

    #[tokio::test]
    async fn peek_pop_offset_capability_fails_closed_after_provider_shutdown() {
        let capability = PeekPopOffsetCapability::<OwnedMessageStore> {
            merge_service: Weak::new(),
        };

        assert_eq!(
            capability
                .latest_offset(
                    &CheetahString::from_static_str("topic-a"),
                    &CheetahString::from_static_str("group-a"),
                    0,
                )
                .await,
            -1
        );
    }

    #[test]
    fn peek_processor_source_uses_only_explicit_capabilities() {
        let source = include_str!("peek_message_processor.rs");

        assert!(!source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(source.contains("PeekMessageProcessorContext<MS>"));
        assert!(source.contains("Weak<EscapeBridge<MS>>"));
        assert!(source.contains("Weak<PopBufferMergeService<MS>>"));
        assert!(source.contains("ConsumerOffsetQueryCapability<MS>"));
    }
}
