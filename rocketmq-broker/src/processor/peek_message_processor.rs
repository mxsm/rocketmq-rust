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

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::FAQUrl;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::peek_message_request_header::PeekMessageRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

/// Handles peek message requests from clients.
pub struct PeekMessageProcessor<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    random_counter: AtomicU32,
}

impl<MS: MessageStore> PeekMessageProcessor<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
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
    async fn process_request_internal(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
        _broker_allow_suspend: bool,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let begin_time_mills = self
            .broker_runtime_inner
            .message_store()
            .map(|store| store.now())
            .unwrap_or(0);

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
                return Ok(Some(
                    response
                        .set_code(ResponseCode::RequestCodeNotSupported)
                        .set_remark(format!("decode request header failed: {:?}", e)),
                ));
            }
        };

        if !PermName::is_readable(self.broker_runtime_inner.broker_config().broker_permission) {
            let response = response.set_code(ResponseCode::NoPermission).set_remark(format!(
                "the broker[{}] peeking message is forbidden",
                self.broker_runtime_inner.broker_config().broker_ip1()
            ));
            return Ok(Some(response));
        }

        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
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
            .broker_runtime_inner
            .subscription_group_manager()
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
        let revive_qid = (random_q % self.broker_runtime_inner.broker_config().revive_queue_num) as i32;

        let mut get_message_result = GetMessageResult::new_result_size(request_header.max_msg_nums as usize);
        let mut rest_num: i64 = 0;
        let pop_time = self
            .broker_runtime_inner
            .message_store()
            .map(|store| store.now())
            .unwrap_or(0) as i64;

        let need_retry = random_q % 5 == 0;

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
                self.broker_runtime_inner.broker_stats_manager().inc_group_get_nums(
                    &request_header.consumer_group,
                    &request_header.topic,
                    get_message_result.message_count(),
                );
                self.broker_runtime_inner.broker_stats_manager().inc_group_get_size(
                    &request_header.consumer_group,
                    &request_header.topic,
                    get_message_result.buffer_total_size(),
                );
                self.broker_runtime_inner
                    .broker_stats_manager()
                    .inc_broker_get_nums(&request_header.topic, get_message_result.message_count());

                // Transfer messages to response body
                if self.broker_runtime_inner.broker_config().transfer_msg_by_heap {
                    // Transfer by heap
                    let body = self.read_get_message_result(
                        &get_message_result,
                        &request_header.consumer_group,
                        &request_header.topic,
                        request_header.queue_id,
                    );

                    self.broker_runtime_inner.broker_stats_manager().inc_group_get_latency(
                        &request_header.consumer_group,
                        &request_header.topic,
                        request_header.queue_id,
                        (self
                            .broker_runtime_inner
                            .message_store()
                            .map(|store| store.now())
                            .unwrap_or(0)
                            - begin_time_mills) as i32,
                    );

                    if let Some(body_bytes) = body {
                        response = response.set_body(body_bytes.to_vec());
                    }
                } else {
                    // Transfer by zero-copy (not implemented in this Rust version yet)
                    // For now, fallback to heap transfer
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
            self.broker_runtime_inner.broker_config().enable_retry_topic_v2,
        ));

        let retry_topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(&retry_topic);

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
                self.broker_runtime_inner.broker_config().enable_retry_topic_v2,
            ))
        } else {
            request_header.topic.clone()
        };

        // Get starting offset
        let offset = self
            .get_pop_offset(&topic, &request_header.consumer_group, queue_id)
            .await;

        // Get message store
        let Some(message_store) = self.broker_runtime_inner.message_store() else {
            return rest_num;
        };

        // Calculate rest num (remaining messages in queue)
        let max_offset = message_store.get_max_offset_in_queue(&topic, queue_id);
        rest_num += max_offset - offset;

        // Check if we already have enough messages
        if get_message_result.message_mapped_list().len() >= request_header.max_msg_nums as usize {
            return rest_num;
        }

        // Calculate how many messages to fetch
        let max_to_fetch = request_header.max_msg_nums - get_message_result.message_mapped_list().len() as i32;

        // Get messages from store
        let mut offset_to_use = offset;
        let get_message_tmp_result = message_store
            .get_message(
                &request_header.consumer_group,
                &topic,
                queue_id,
                offset_to_use,
                max_to_fetch,
                None,
            )
            .await;

        if let Some(mut tmp_result) = get_message_tmp_result {
            // Handle offset correction if needed
            if matches!(
                tmp_result.status(),
                Some(GetMessageStatus::OffsetTooSmall) | Some(GetMessageStatus::OffsetOverflowBadly)
            ) {
                offset_to_use = tmp_result.next_begin_offset();
                tmp_result = message_store
                    .get_message(
                        &request_header.consumer_group,
                        &topic,
                        queue_id,
                        offset_to_use,
                        max_to_fetch,
                        None,
                    )
                    .await
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
        let mut offset =
            self.broker_runtime_inner
                .consumer_offset_manager()
                .query_offset(consumer_group, topic, queue_id);

        // If no consumer offset, use min offset
        if offset < 0 {
            if let Some(message_store) = self.broker_runtime_inner.message_store() {
                offset = message_store.get_min_offset_in_queue(topic, queue_id);
            }
        }

        // Get pop buffer offset
        let buffer_offset = if let Some(processor) = self.broker_runtime_inner.pop_message_processor() {
            let service = processor.pop_buffer_merge_service();
            let key = CheetahString::from_string(KeyBuilder::build_polling_key(
                topic.as_str(),
                consumer_group.as_str(),
                queue_id,
            ));
            service.get_latest_offset(&key).await
        } else {
            -1
        };

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
            if let Some(mapped_file) = &msg.mapped_file {
                let data = &mapped_file.get_mapped_file()
                    [msg.start_offset as usize..(msg.start_offset + msg.size as u64) as usize];
                bytes_mut.extend_from_slice(data);
            }
        }

        Some(bytes_mut.freeze())
    }
}
