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
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_remoting::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::lite::memory_consumer_order_info_manager::MemoryConsumerOrderInfoManager;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingService;
use crate::long_polling::polling_result::PollingResult;
use crate::processor::pop_message_processor::QueueLockManager;

pub(crate) struct PopLiteMessageProcessor<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    pop_lite_long_polling_service: ArcMut<PopLiteLongPollingService<MS, PopLiteMessageProcessor<MS>>>,
    consumer_order_info_manager: MemoryConsumerOrderInfoManager,
    queue_lock_manager: QueueLockManager,
}

enum PopLmqResult {
    Fetched {
        body: Bytes,
        next_offset: i64,
        fetched_count: i32,
        order_count_info: String,
    },
    Requeue,
    Skip,
}

impl<MS: MessageStore> PopLiteMessageProcessor<MS> {
    pub(crate) fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            pop_lite_long_polling_service: ArcMut::new(PopLiteLongPollingService::new(broker_runtime_inner.clone())),
            consumer_order_info_manager: MemoryConsumerOrderInfoManager::default(),
            queue_lock_manager: QueueLockManager::new(),
            broker_runtime_inner,
        }
    }

    pub(crate) fn new_arc_mut(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> ArcMut<Self> {
        let mut processor = ArcMut::new(Self::new(broker_runtime_inner.clone()));
        let wakeup_sender = processor.pop_lite_long_polling_service.wakeup_sender();
        broker_runtime_inner
            .lite_event_dispatcher()
            .set_wakeup_sender(wakeup_sender);

        let cloned = processor.clone();
        processor.pop_lite_long_polling_service.set_processor(cloned);
        processor
    }

    pub(crate) fn start(&mut self) {
        PopLiteLongPollingService::start(self.pop_lite_long_polling_service.clone());
        self.queue_lock_manager.start();
    }

    pub(crate) fn shutdown(&mut self) {
        self.pop_lite_long_polling_service.shutdown();
        self.queue_lock_manager.shutdown();
    }

    pub(crate) fn pop_lite_long_polling_service(
        &self,
    ) -> &ArcMut<PopLiteLongPollingService<MS, PopLiteMessageProcessor<MS>>> {
        &self.pop_lite_long_polling_service
    }

    pub(crate) fn order_info_count(&self) -> i32 {
        self.consumer_order_info_manager.order_info_count() as i32
    }

    pub(crate) fn clear_order_info(&self, topic: &CheetahString, group: &CheetahString) {
        self.consumer_order_info_manager.clear_block(topic, group, 0);
    }

    fn lite_dispatch_policy(&self, group: &CheetahString) -> (usize, u64) {
        let broker_config = self.broker_runtime_inner.broker_config();
        let max_event_count = self
            .broker_runtime_inner
            .subscription_group_manager()
            .find_subscription_group_config(group)
            .map(|config| config.max_client_event_count())
            .filter(|count| *count > 0)
            .unwrap_or(broker_config.max_client_event_count)
            .max(1) as usize;
        (max_event_count, broker_config.lite_event_full_dispatch_delay_time)
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
        if topic_config.get_topic_message_type() != TopicMessageType::Lite {
            return Some((
                ResponseCode::InvalidParameter,
                CheetahString::from_string(format!("the topic [{}] message type not match", request_header.topic)),
            ));
        }
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
    ) -> (Option<Bytes>, HashSet<CheetahString>, i32, Option<CheetahString>) {
        let Some(message_store) = self.broker_runtime_inner.message_store() else {
            return (None, HashSet::new(), 0, None);
        };

        let mut remaining = request_header.max_msg_num;
        let mut body = BytesMut::new();
        let mut fetched_count = 0;
        let mut requeue_events = HashSet::new();
        let mut order_count_infos = Vec::new();
        let mut event_iter = pending_events.into_iter();
        let attempt_id = request_header.attempt_id.clone().unwrap_or_default();

        while remaining > 0 {
            let Some(lmq_name) = event_iter.next() else {
                break;
            };
            let lock_key = CheetahString::from_string(QueueLockManager::build_lock_key(
                &lmq_name,
                &request_header.consumer_group,
                0,
            ));
            if !self.queue_lock_manager.try_lock_with_key(lock_key.clone()).await {
                requeue_events.insert(lmq_name);
                continue;
            }

            let result = self
                .pop_from_lmq(message_store.clone(), request_header, &attempt_id, &lmq_name, remaining)
                .await;
            self.queue_lock_manager.unlock_with_key(lock_key).await;

            match result {
                PopLmqResult::Fetched {
                    body: chunk,
                    next_offset,
                    fetched_count: local_count,
                    order_count_info,
                } => {
                    self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                        CheetahString::from_static_str("PopLiteMessageProcessor"),
                        &request_header.consumer_group,
                        &lmq_name,
                        0,
                        next_offset,
                    );
                    body.extend_from_slice(&chunk);
                    fetched_count += local_count;
                    remaining -= local_count;
                    if !order_count_info.is_empty() {
                        order_count_infos.push(order_count_info);
                    }

                    let broker_offset = self
                        .broker_runtime_inner
                        .lite_lifecycle_manager()
                        .get_max_offset_in_queue(self.broker_runtime_inner.message_store(), &lmq_name);
                    if next_offset < broker_offset {
                        requeue_events.insert(lmq_name);
                    }
                }
                PopLmqResult::Requeue => {
                    requeue_events.insert(lmq_name);
                }
                PopLmqResult::Skip => {}
            }
        }

        requeue_events.extend(event_iter);
        let body = if body.is_empty() { None } else { Some(body.freeze()) };
        let order_count_info =
            (!order_count_infos.is_empty()).then(|| CheetahString::from_string(order_count_infos.join(";")));
        (body, requeue_events, fetched_count, order_count_info)
    }

    async fn pop_from_lmq(
        &self,
        message_store: ArcMut<MS>,
        request_header: &PopLiteMessageRequestHeader,
        attempt_id: &CheetahString,
        lmq_name: &CheetahString,
        remaining: i32,
    ) -> PopLmqResult {
        if self.consumer_order_info_manager.check_block(
            attempt_id,
            lmq_name,
            &request_header.consumer_group,
            0,
            request_header.invisible_time as u64,
        ) {
            return PopLmqResult::Requeue;
        }

        let consume_offset = self.get_pop_offset(&request_header.consumer_group, lmq_name);
        let Some(get_message_result) = self
            .get_message(
                &message_store,
                &request_header.consumer_group,
                lmq_name,
                consume_offset,
                remaining,
            )
            .await
        else {
            return PopLmqResult::Skip;
        };

        if get_message_result.status() != Some(GetMessageStatus::Found) || get_message_result.message_count() <= 0 {
            return PopLmqResult::Skip;
        }

        let fetched_count = get_message_result.message_count();
        let mut order_count_info = String::new();
        self.consumer_order_info_manager.update(
            attempt_id.clone(),
            lmq_name,
            &request_header.consumer_group,
            0,
            current_millis(),
            request_header.invisible_time as u64,
            get_message_result.message_queue_offset().clone(),
            &mut order_count_info,
        );

        PopLmqResult::Fetched {
            body: self.read_get_message_result(&get_message_result),
            next_offset: get_message_result.next_begin_offset(),
            fetched_count,
            order_count_info: Self::transform_order_count_info(&order_count_info, fetched_count as usize),
        }
    }

    fn get_pop_offset(&self, group: &CheetahString, lmq_name: &CheetahString) -> i64 {
        let mut offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_offset(group, lmq_name, 0)
            .max(0);
        if let Some(reset_offset) = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_then_erase_reset_offset(lmq_name, group, 0)
        {
            self.consumer_order_info_manager.clear_block(lmq_name, group, 0);
            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                CheetahString::from_static_str("ResetOffset"),
                group,
                lmq_name,
                0,
                reset_offset,
            );
            offset = reset_offset;
        }
        offset
    }

    async fn get_message(
        &self,
        message_store: &ArcMut<MS>,
        group: &CheetahString,
        lmq_name: &CheetahString,
        offset: i64,
        batch_size: i32,
    ) -> Option<GetMessageResult> {
        let result = message_store
            .get_message(group, lmq_name, 0, offset, batch_size, None)
            .await?;
        if matches!(
            result.status(),
            Some(
                GetMessageStatus::OffsetTooSmall
                    | GetMessageStatus::OffsetOverflowBadly
                    | GetMessageStatus::OffsetFoundNull
                    | GetMessageStatus::NoMatchedMessage
                    | GetMessageStatus::MessageWasRemoving
                    | GetMessageStatus::NoMatchedLogicQueue
            )
        ) && result.next_begin_offset() >= 0
        {
            let correct_offset = result.next_begin_offset();
            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                CheetahString::from_static_str("CorrectOffset"),
                group,
                lmq_name,
                0,
                correct_offset,
            );
            return message_store
                .get_message(group, lmq_name, 0, correct_offset, batch_size, None)
                .await;
        }
        Some(result)
    }

    fn read_get_message_result(&self, get_message_result: &GetMessageResult) -> Bytes {
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

    fn transform_order_count_info(order_count_info: &str, msg_count: usize) -> String {
        if order_count_info.is_empty() {
            return vec!["0"; msg_count].join(";");
        }

        let infos: Vec<&str> = order_count_info.split(';').collect();
        if infos.len() > 1 {
            return infos[..infos.len() - 1].join(";");
        }

        let split: Vec<&str> = order_count_info.split(MessageConst::KEY_SEPARATOR).collect();
        if split.len() == 3 {
            return vec![split[2]; msg_count].join(";");
        }
        vec!["0"; msg_count].join(";")
    }
}

impl<MS: MessageStore> RequestProcessor for PopLiteMessageProcessor<MS> {
    async fn process_request(
        &mut self,
        _channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<PopLiteMessageRequestHeader>()?;
        if let Some((code, remark)) = self.pre_check(&request_header) {
            return Ok(Some(self.response_with_code(request, code, remark)));
        }

        let dispatcher = self.broker_runtime_inner.lite_event_dispatcher();
        dispatcher.touch_client(&request_header.client_id);
        let pending_events = dispatcher.take_pending_events(&request_header.client_id);
        let (body, requeue_events, fetched_count, order_count_info) =
            self.pop_from_events(&request_header, pending_events).await;
        if !requeue_events.is_empty() {
            let (max_event_count, dispatch_delay_millis) = self.lite_dispatch_policy(&request_header.consumer_group);
            dispatcher.do_full_dispatch_with_limit(
                &request_header.client_id,
                &request_header.consumer_group,
                &requeue_events,
                max_event_count,
                dispatch_delay_millis,
            );
        }

        let response_header = PopLiteMessageResponseHeader {
            pop_time: current_millis() as i64,
            invisible_time: request_header.invisible_time,
            revive_qid: POP_ORDER_REVIVE_QUEUE,
            start_offset_info: None,
            msg_offset_info: None,
            order_count_info: (fetched_count > 0).then_some(order_count_info).flatten(),
        };
        let mut response =
            RemotingCommand::create_response_command_with_header(response_header).set_opaque(request.opaque());

        match body {
            Some(body) => {
                response.set_code_ref(ResponseCode::Success);
                response.set_remark_mut("FOUND");
                response.set_body_mut_ref(body);
            }
            None => match self.pop_lite_long_polling_service.polling(
                ctx,
                request,
                &request_header.client_id,
                request_header.born_time,
                request_header.poll_time,
            ) {
                PollingResult::PollingSuc => {
                    if !dispatcher.pending_events(&request_header.client_id).is_empty() {
                        self.pop_lite_long_polling_service
                            .wake_up_client(&request_header.client_id);
                    }
                    return Ok(None);
                }
                PollingResult::PollingFull => {
                    response.set_code_ref(ResponseCode::PollingFull);
                    response.set_remark_mut("POP_LITE_POLLING_FULL");
                }
                _ => {
                    response.set_code_ref(ResponseCode::PollingTimeout);
                    response.set_remark_mut("NO_MESSAGE_IN_QUEUE");
                }
            },
        }

        Ok(Some(response))
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;

    use super::*;

    #[test]
    fn transform_order_count_info_drops_queue_level_suffix_when_offset_entries_exist() {
        let result =
            PopLiteMessageProcessor::<LocalFileMessageStore>::transform_order_count_info("0 qo0%100 1;0 0 1", 1);

        assert_eq!(result, "0 qo0%100 1");
    }
}
