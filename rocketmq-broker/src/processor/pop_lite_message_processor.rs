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

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_remoting::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

pub(crate) struct PopLiteMessageProcessor<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> PopLiteMessageProcessor<MS> {
    pub(crate) fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }

    fn pre_check(&self, request_header: &PopLiteMessageRequestHeader) -> Option<(ResponseCode, CheetahString)> {
        if request_header.client_id.is_empty() {
            return Some((
                ResponseCode::InvalidParameter,
                CheetahString::from_static_str("clientId is blank."),
            ));
        }
        if request_header.consumer_group.is_empty() {
            return Some((
                ResponseCode::InvalidParameter,
                CheetahString::from_static_str("consumerGroup is blank."),
            ));
        }
        if request_header.topic.is_empty() {
            return Some((
                ResponseCode::InvalidParameter,
                CheetahString::from_static_str("topic is blank."),
            ));
        }
        if request_header.is_timeout_too_much() {
            return Some((
                ResponseCode::PollingTimeout,
                CheetahString::from_string(format!(
                    "the broker[{}] pop lite message is timeout too much",
                    self.broker_runtime_inner.broker_config().broker_ip1
                )),
            ));
        }
        if !PermName::is_readable(self.broker_runtime_inner.broker_config().broker_permission) {
            return Some((
                ResponseCode::NoPermission,
                CheetahString::from_string(format!(
                    "the broker[{}] pop lite message is forbidden",
                    self.broker_runtime_inner.broker_config().broker_ip1
                )),
            ));
        }
        if request_header.max_msg_num <= 0 || request_header.max_msg_num > 32 {
            return Some((
                ResponseCode::InvalidParameter,
                CheetahString::from_string(format!(
                    "the broker[{}] pop lite message's num is invalid",
                    self.broker_runtime_inner.broker_config().broker_ip1
                )),
            ));
        }

        let Some(topic_config) = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(&request_header.topic)
        else {
            return Some((
                ResponseCode::TopicNotExist,
                CheetahString::from_string(format!("topic [{}] not exist.", request_header.topic)),
            ));
        };
        if !PermName::is_readable(topic_config.perm) {
            return Some((
                ResponseCode::NoPermission,
                CheetahString::from_string(format!(
                    "the topic [{}] pop lite message is forbidden",
                    request_header.topic
                )),
            ));
        }

        let Some(group_config) = self
            .broker_runtime_inner
            .subscription_group_manager()
            .find_subscription_group_config(&request_header.consumer_group)
        else {
            return Some((
                ResponseCode::SubscriptionGroupNotExist,
                CheetahString::from_string(format!(
                    "subscription group [{}] not exist.",
                    request_header.consumer_group
                )),
            ));
        };
        if !group_config.consume_enable() {
            return Some((
                ResponseCode::NoPermission,
                CheetahString::from_string(format!(
                    "subscription group no permission, {}",
                    request_header.consumer_group
                )),
            ));
        }
        if group_config
            .lite_bind_topic()
            .is_none_or(|bind_topic| bind_topic != &request_header.topic)
        {
            return Some((
                ResponseCode::InvalidParameter,
                CheetahString::from_string(format!(
                    "subscription bind topic not match, {}",
                    request_header.consumer_group
                )),
            ));
        }

        None
    }

    async fn pop_from_events(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        pending_events: Vec<CheetahString>,
    ) -> (Option<Bytes>, HashSet<CheetahString>, i32) {
        let Some(message_store) = self.broker_runtime_inner.message_store() else {
            return (None, HashSet::new(), 0);
        };

        let mut remaining = request_header.max_msg_num;
        let mut body = BytesMut::new();
        let mut fetched_count = 0;
        let mut requeue_events = HashSet::new();
        let mut event_iter = pending_events.into_iter();

        while remaining > 0 {
            let Some(lmq_name) = event_iter.next() else {
                break;
            };
            let consume_offset = self
                .broker_runtime_inner
                .consumer_offset_manager()
                .query_offset(&request_header.consumer_group, &lmq_name, 0)
                .max(0);
            let Some(get_message_result) = message_store
                .get_message(
                    &request_header.consumer_group,
                    &lmq_name,
                    0,
                    consume_offset,
                    remaining,
                    None,
                )
                .await
            else {
                continue;
            };

            if get_message_result.status() != Some(GetMessageStatus::Found) || get_message_result.message_count() <= 0 {
                continue;
            }

            body.extend_from_slice(&self.read_get_message_result(&get_message_result));
            fetched_count += get_message_result.message_count();
            remaining -= get_message_result.message_count();

            let next_offset = get_message_result.next_begin_offset();
            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                CheetahString::from_static_str("PopLiteMessageProcessor"),
                &request_header.consumer_group,
                &lmq_name,
                0,
                next_offset,
            );

            let broker_offset = self
                .broker_runtime_inner
                .lite_lifecycle_manager()
                .get_max_offset_in_queue(self.broker_runtime_inner.message_store(), &lmq_name);
            if next_offset < broker_offset {
                requeue_events.insert(lmq_name);
            }
        }

        requeue_events.extend(event_iter);
        let body = if body.is_empty() { None } else { Some(body.freeze()) };
        (body, requeue_events, fetched_count)
    }

    fn read_get_message_result(
        &self,
        get_message_result: &rocketmq_store::base::get_message_result::GetMessageResult,
    ) -> Bytes {
        let mut bytes_mut = BytesMut::with_capacity(get_message_result.buffer_total_size() as usize);
        for mapped in get_message_result.message_mapped_list() {
            if let Some(bytes) = mapped.bytes.as_ref() {
                bytes_mut.extend_from_slice(bytes);
            } else {
                bytes_mut.extend_from_slice(mapped.get_buffer());
            }
        }
        bytes_mut.freeze()
    }

    fn response_with_code(
        &self,
        request: &RemotingCommand,
        code: ResponseCode,
        remark: impl Into<CheetahString>,
    ) -> RemotingCommand {
        RemotingCommand::create_response_command_with_code_remark(code, remark).set_opaque(request.opaque())
    }
}

impl<MS: MessageStore> RequestProcessor for PopLiteMessageProcessor<MS> {
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<PopLiteMessageRequestHeader>()?;
        if let Some((code, remark)) = self.pre_check(&request_header) {
            return Ok(Some(self.response_with_code(request, code, remark)));
        }

        let dispatcher = self.broker_runtime_inner.lite_event_dispatcher();
        dispatcher.touch_client(&request_header.client_id);
        let pending_events = dispatcher.take_pending_events(&request_header.client_id);
        let (body, requeue_events, fetched_count) = self.pop_from_events(&request_header, pending_events).await;
        if !requeue_events.is_empty() {
            dispatcher.do_full_dispatch(
                &request_header.client_id,
                &request_header.consumer_group,
                &requeue_events,
            );
        }

        let response_header = PopLiteMessageResponseHeader {
            pop_time: current_millis() as i64,
            invisible_time: request_header.invisible_time,
            revive_qid: POP_ORDER_REVIVE_QUEUE,
            start_offset_info: None,
            msg_offset_info: None,
            order_count_info: (fetched_count > 0)
                .then(|| CheetahString::from_string(vec!["0"; fetched_count as usize].join(";"))),
        };
        let mut response =
            RemotingCommand::create_response_command_with_header(response_header).set_opaque(request.opaque());

        match body {
            Some(body) => {
                response.set_code_ref(ResponseCode::Success);
                response.set_remark_mut("FOUND");
                response.set_body_mut_ref(body);
            }
            None => {
                response.set_code_ref(ResponseCode::PollingTimeout);
                response.set_remark_mut("NO_MESSAGE_IN_QUEUE");
            }
        }

        Ok(Some(response))
    }
}
