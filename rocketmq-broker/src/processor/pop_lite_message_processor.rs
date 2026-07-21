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
use std::sync::Arc;
use std::sync::Weak;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
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
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use tokio::sync::Mutex as AsyncMutex;

use crate::failover::escape_bridge::EscapeBridge;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::lite::lite_lifecycle_manager::LiteLifecycleManager;
use crate::lite::memory_consumer_order_info_manager::MemoryConsumerOrderInfoManager;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingRequestProcessor;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingService;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingServiceContext;
use crate::long_polling::polling_result::PollingResult;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::processor::pop_message_processor::QueueLockManager;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

#[derive(Clone)]
pub(crate) struct PopLiteMessagePolicy {
    broker_ip1: CheetahString,
    broker_permission: u32,
    max_client_event_count: i32,
    lite_event_full_dispatch_delay_time: u64,
}

impl PopLiteMessagePolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig) -> Self {
        Self {
            broker_ip1: broker_config.broker_ip1.clone(),
            broker_permission: broker_config.broker_permission,
            max_client_event_count: broker_config.max_client_event_count,
            lite_event_full_dispatch_delay_time: broker_config.lite_event_full_dispatch_delay_time,
        }
    }
}

pub(crate) struct PopLiteOffsetCapability<MS: MessageStore> {
    manager: Weak<ConsumerOffsetManager<MS>>,
}

impl<MS: MessageStore> PopLiteOffsetCapability<MS> {
    pub(crate) fn new(manager: &Arc<ConsumerOffsetManager<MS>>) -> Self {
        Self {
            manager: Arc::downgrade(manager),
        }
    }

    pub(crate) fn query_offset(&self, group: &CheetahString, topic: &CheetahString) -> i64 {
        self.manager
            .upgrade()
            .map(|manager| manager.query_offset(group, topic, 0))
            .unwrap_or(-1)
    }

    fn query_then_erase_reset_offset(&self, topic: &CheetahString, group: &CheetahString) -> Option<i64> {
        self.manager
            .upgrade()
            .and_then(|manager| manager.query_then_erase_reset_offset(topic, group, 0))
    }

    fn commit_offset(&self, client_host: &'static str, group: &CheetahString, topic: &CheetahString, offset: i64) {
        if let Some(manager) = self.manager.upgrade() {
            manager.commit_offset(CheetahString::from_static_str(client_host), group, topic, 0, offset);
        }
    }

    pub(crate) fn assign_reset_offset(&self, topic: &CheetahString, group: &CheetahString, offset: i64) -> bool {
        let Some(manager) = self.manager.upgrade() else {
            return false;
        };
        manager.assign_reset_offset(topic, group, 0, offset);
        true
    }
}

pub(crate) struct PopLiteMessageStoreCapability<MS: MessageStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> PopLiteMessageStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
    }

    fn is_available(&self) -> bool {
        self.escape_bridge.strong_count() > 0
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        offset: i64,
        batch_size: i32,
    ) -> Option<GetMessageResult> {
        self.escape_bridge
            .upgrade()?
            .get_message_from_local_store(group, topic, 0, offset, batch_size)
            .await
            .ok()
            .flatten()
    }

    pub(crate) fn max_offset(&self, lmq_name: &CheetahString) -> i64 {
        self.escape_bridge
            .upgrade()
            .and_then(|bridge| {
                bridge
                    .try_with_message_store(|store| LiteLifecycleManager.get_max_offset_in_queue(Some(store), lmq_name))
                    .ok()
            })
            .unwrap_or(0)
    }
}

pub(crate) struct PopLiteMessageProcessorContext<MS: MessageStore> {
    policy: PopLiteMessagePolicy,
    topic_config_manager: Arc<TopicConfigManager>,
    subscription_group_lookup: SubscriptionGroupConfigLookup,
    consumer_offset: PopLiteOffsetCapability<MS>,
    message_store: PopLiteMessageStoreCapability<MS>,
    lite_event_dispatcher: LiteEventDispatcher,
    queue_lock_manager: QueueLockManager,
    long_polling: PopLiteLongPollingServiceContext,
}

impl<MS: MessageStore> PopLiteMessageProcessorContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "composition root lists each POP Lite capability explicitly"
    )]
    pub(crate) fn new(
        policy: PopLiteMessagePolicy,
        topic_config_manager: Arc<TopicConfigManager>,
        subscription_group_lookup: SubscriptionGroupConfigLookup,
        consumer_offset: PopLiteOffsetCapability<MS>,
        message_store: PopLiteMessageStoreCapability<MS>,
        lite_event_dispatcher: LiteEventDispatcher,
        queue_lock_manager: QueueLockManager,
        long_polling: PopLiteLongPollingServiceContext,
    ) -> Self {
        Self {
            policy,
            topic_config_manager,
            subscription_group_lookup,
            consumer_offset,
            message_store,
            lite_event_dispatcher,
            queue_lock_manager,
            long_polling,
        }
    }
}

pub(crate) struct PopLiteMessageProcessor<MS: MessageStore> {
    context: PopLiteMessageProcessorContext<MS>,
    pop_lite_long_polling_service: Arc<PopLiteLongPollingService<PopLiteMessageProcessor<MS>>>,
    consumer_order_info_manager: MemoryConsumerOrderInfoManager,
    lifecycle: AsyncMutex<()>,
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
    pub(crate) fn new(context: PopLiteMessageProcessorContext<MS>) -> Arc<Self> {
        let long_polling_context = context.long_polling.clone();
        Arc::new_cyclic(move |processor| Self {
            pop_lite_long_polling_service: Arc::new(PopLiteLongPollingService::new(
                long_polling_context,
                processor.clone(),
            )),
            consumer_order_info_manager: MemoryConsumerOrderInfoManager::default(),
            context,
            lifecycle: AsyncMutex::new(()),
        })
    }

    pub(crate) async fn start(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        PopLiteLongPollingService::start(&self.pop_lite_long_polling_service).await;
        self.context.queue_lock_manager.start();
    }

    pub(crate) async fn shutdown(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        self.pop_lite_long_polling_service.shutdown().await;
        self.context.queue_lock_manager.shutdown().await;
    }

    pub(crate) fn pop_lite_long_polling_service(&self) -> &Arc<PopLiteLongPollingService<PopLiteMessageProcessor<MS>>> {
        &self.pop_lite_long_polling_service
    }

    pub(crate) fn order_info_count(&self) -> i32 {
        self.consumer_order_info_manager.order_info_count() as i32
    }

    pub(crate) fn clear_order_info(&self, topic: &CheetahString, group: &CheetahString) {
        self.consumer_order_info_manager.clear_block(topic, group, 0);
    }

    fn lite_dispatch_policy(&self, group: &CheetahString) -> (usize, u64) {
        let max_event_count = self
            .context
            .subscription_group_lookup
            .find_subscription_group_config(group)
            .map(|config| config.max_client_event_count())
            .filter(|count| *count > 0)
            .unwrap_or(self.context.policy.max_client_event_count)
            .max(1) as usize;
        (max_event_count, self.context.policy.lite_event_full_dispatch_delay_time)
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
        if request_header.is_timeout_too_much_at(rocketmq_common::TimeUtils::current_millis() as i64) {
            return Some((
                ResponseCode::PollingTimeout,
                CheetahString::from_string(format!(
                    "the broker[{}] pop lite message is timeout too much",
                    self.context.policy.broker_ip1
                )),
            ));
        }
        if !PermName::is_readable(self.context.policy.broker_permission) {
            return Some((
                ResponseCode::NoPermission,
                CheetahString::from_string(format!(
                    "the broker[{}] pop lite message is forbidden",
                    self.context.policy.broker_ip1
                )),
            ));
        }
        if request_header.max_msg_num <= 0 || request_header.max_msg_num > 32 {
            return Some((
                ResponseCode::InvalidParameter,
                CheetahString::from_string(format!(
                    "the broker[{}] pop lite message's num is invalid",
                    self.context.policy.broker_ip1
                )),
            ));
        }

        let Some(topic_config) = self
            .context
            .topic_config_manager
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
            .context
            .subscription_group_lookup
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
        if !self.context.message_store.is_available() {
            return (None, HashSet::new(), 0, None);
        }

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
            if !self
                .context
                .queue_lock_manager
                .try_lock_with_key(lock_key.clone())
                .await
            {
                requeue_events.insert(lmq_name);
                continue;
            }

            let result = self
                .pop_from_lmq(request_header, &attempt_id, &lmq_name, remaining)
                .await;
            self.context.queue_lock_manager.unlock_with_key(lock_key).await;

            match result {
                PopLmqResult::Fetched {
                    body: chunk,
                    next_offset,
                    fetched_count: local_count,
                    order_count_info,
                } => {
                    self.context.consumer_offset.commit_offset(
                        "PopLiteMessageProcessor",
                        &request_header.consumer_group,
                        &lmq_name,
                        next_offset,
                    );
                    body.extend_from_slice(&chunk);
                    fetched_count += local_count;
                    remaining -= local_count;
                    if !order_count_info.is_empty() {
                        order_count_infos.push(order_count_info);
                    }

                    let broker_offset = self.context.message_store.max_offset(&lmq_name);
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
            .get_message(&request_header.consumer_group, lmq_name, consume_offset, remaining)
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
        let mut offset = self.context.consumer_offset.query_offset(group, lmq_name).max(0);
        if let Some(reset_offset) = self
            .context
            .consumer_offset
            .query_then_erase_reset_offset(lmq_name, group)
        {
            self.consumer_order_info_manager.clear_block(lmq_name, group, 0);
            self.context
                .consumer_offset
                .commit_offset("ResetOffset", group, lmq_name, reset_offset);
            offset = reset_offset;
        }
        offset
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        lmq_name: &CheetahString,
        offset: i64,
        batch_size: i32,
    ) -> Option<GetMessageResult> {
        let result = self
            .context
            .message_store
            .get_message(group, lmq_name, offset, batch_size)
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
            self.context
                .consumer_offset
                .commit_offset("CorrectOffset", group, lmq_name, correct_offset);
            return self
                .context
                .message_store
                .get_message(group, lmq_name, correct_offset, batch_size)
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

impl<MS: MessageStore> PopLiteMessageProcessor<MS> {
    pub(crate) async fn process_request_shared(
        &self,
        _channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<PopLiteMessageRequestHeader>()?;
        if let Some((code, remark)) = self.pre_check(&request_header) {
            return Ok(Some(self.response_with_code(request, code, remark)));
        }

        let dispatcher = &self.context.lite_event_dispatcher;
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

impl<MS: MessageStore> RequestProcessor for PopLiteMessageProcessor<MS> {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_shared(channel, ctx, request).await
    }
}

impl<MS: MessageStore> PopLiteLongPollingRequestProcessor for PopLiteMessageProcessor<MS> {
    async fn process_request_when_wakeup(
        &self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        mut request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_shared(channel, ctx, &mut request).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Weak;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::GenericMessageStore;

    use super::PopLiteMessagePolicy;
    use super::PopLiteMessageProcessor;
    use super::PopLiteMessageProcessorContext;
    use super::PopLiteMessageStoreCapability;
    use super::PopLiteOffsetCapability;
    use crate::broker_runtime::BrokerRuntime;
    use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingPolicy;
    use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingService;
    use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingServiceContext;
    use crate::processor::pop_message_processor::QueueLockManager;

    fn pop_lite_processor_for_test(runtime: &mut BrokerRuntime) -> Arc<PopLiteMessageProcessor<GenericMessageStore>> {
        let inner = runtime.inner_for_test();
        let topic_config_manager = inner.topic_config_manager_handle();
        let subscription_group_lookup = inner.subscription_group_manager().config_lookup();
        let lite_event_dispatcher = inner.lite_event_dispatcher().clone();
        let parent_task_group = inner.broker_service_task_group();
        let queue_lock_manager = parent_task_group
            .clone()
            .map(QueueLockManager::new_with_parent_task_group)
            .unwrap_or_else(QueueLockManager::new);
        let long_polling = PopLiteLongPollingServiceContext::new(
            PopLiteLongPollingPolicy::from_config(inner.broker_config()),
            lite_event_dispatcher.clone(),
            parent_task_group,
        );
        let consumer_offset_manager = inner.consumer_offset_manager_handle();

        PopLiteMessageProcessor::new(PopLiteMessageProcessorContext::new(
            PopLiteMessagePolicy::from_config(inner.broker_config()),
            topic_config_manager,
            subscription_group_lookup,
            PopLiteOffsetCapability::new(&consumer_offset_manager),
            PopLiteMessageStoreCapability {
                escape_bridge: Weak::new(),
            },
            lite_event_dispatcher,
            queue_lock_manager,
            long_polling,
        ))
    }

    #[test]
    fn transform_order_count_info_drops_queue_level_suffix_when_offset_entries_exist() {
        let result = PopLiteMessageProcessor::<GenericMessageStore>::transform_order_count_info("0 qo0%100 1;0 0 1", 1);

        assert_eq!(result, "0 qo0%100 1");
    }

    #[test]
    fn pop_lite_message_policy_captures_only_required_startup_values() {
        let broker_config = BrokerConfig {
            broker_ip1: CheetahString::from_static_str("192.0.2.10"),
            broker_permission: 4,
            max_client_event_count: 17,
            lite_event_full_dispatch_delay_time: 29,
            ..Default::default()
        };

        let policy = PopLiteMessagePolicy::from_config(&broker_config);

        assert_eq!(policy.broker_ip1, "192.0.2.10");
        assert_eq!(policy.broker_permission, 4);
        assert_eq!(policy.max_client_event_count, 17);
        assert_eq!(policy.lite_event_full_dispatch_delay_time, 29);
    }

    #[test]
    fn pop_lite_message_processor_source_uses_only_explicit_capabilities() {
        let source = include_str!("pop_lite_message_processor.rs");

        assert!(!source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(!source.contains(concat!("broker_runtime", "_inner")));
        assert!(source.contains("context: PopLiteMessageProcessorContext<MS>"));
        assert!(source.contains("consumer_offset: PopLiteOffsetCapability<MS>"));
        assert!(source.contains("message_store: PopLiteMessageStoreCapability<MS>"));
    }

    #[test]
    fn pop_lite_message_providers_do_not_keep_runtime_or_store_alive() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let inner = runtime.inner_for_test();
        let offset_manager = inner.consumer_offset_manager_handle();
        let offset = PopLiteOffsetCapability::new(&offset_manager);
        let escape_bridge = inner.escape_bridge();
        let store = PopLiteMessageStoreCapability::new(&escape_bridge);
        let group = CheetahString::from_static_str("group");
        let topic = CheetahString::from_static_str("topic");

        assert!(store.is_available());
        drop(offset_manager);
        drop(escape_bridge);
        drop(runtime);

        assert!(!store.is_available());
        assert_eq!(offset.query_offset(&group, &topic), -1);
        assert_eq!(offset.query_then_erase_reset_offset(&topic, &group), None);
        offset.commit_offset("provider-shutdown-test", &group, &topic, 1);
    }

    #[tokio::test]
    async fn pop_lite_long_polling_service_uses_weak_processor_back_reference() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}

        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let processor = pop_lite_processor_for_test(&mut runtime);
        let processor_weak = Arc::downgrade(&processor);
        let service = processor.pop_lite_long_polling_service.clone();

        assert_send_sync(&processor);
        drop(processor);

        assert!(processor_weak.upgrade().is_none());
        assert_eq!(Arc::strong_count(&service), 1);
    }

    #[tokio::test]
    async fn pop_lite_long_polling_service_start_shutdown_and_restart_are_serialized() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let processor = pop_lite_processor_for_test(&mut runtime);
        let service = processor.pop_lite_long_polling_service.clone();

        PopLiteLongPollingService::start(&service).await;
        let first_task_group = service
            .task_group_for_test()
            .expect("long-polling task group should be installed");
        PopLiteLongPollingService::start(&service).await;
        assert_eq!(
            service
                .task_group_for_test()
                .expect("duplicate start should retain the task group")
                .task_count(),
            1
        );
        assert_eq!(first_task_group.task_count(), 1);
        assert!(service.is_running());

        service.shutdown().await;
        assert!(!service.is_running());
        assert!(service.task_group_for_test().is_none());

        PopLiteLongPollingService::start(&service).await;
        let restarted_task_group = service
            .task_group_for_test()
            .expect("restart should install a task group");
        assert_eq!(restarted_task_group.task_count(), 1);
        assert!(!restarted_task_group.cancellation_token().is_cancelled());
        service.shutdown().await;
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn pop_lite_long_polling_service_uses_broker_parent_task_group() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let runtime_context = RuntimeContext::from_current("pop-lite-long-polling-parent-test");
        let broker_service = runtime_context.service_context("broker-service");
        let mut runtime =
            BrokerRuntime::new_with_service_context(broker_config, message_store_config, broker_service.clone());
        let parent_id = runtime
            .inner_for_test()
            .broker_service_task_group()
            .expect("broker service task group should exist")
            .id();
        let processor = pop_lite_processor_for_test(&mut runtime);
        let service = processor.pop_lite_long_polling_service.clone();

        PopLiteLongPollingService::start(&service).await;
        let task_group = service
            .task_group_for_test()
            .expect("POP Lite long-polling task group should be installed");

        assert_eq!(task_group.parent_id(), Some(parent_id));
        service.shutdown().await;
        let report = broker_service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn active_pop_lite_long_polling_scan_does_not_keep_owner_alive() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let processor = pop_lite_processor_for_test(&mut runtime);
        let service = processor.pop_lite_long_polling_service.clone();
        let processor_weak = Arc::downgrade(&processor);
        let service_weak = Arc::downgrade(&service);

        PopLiteLongPollingService::start(&service).await;
        drop(processor);
        drop(service);

        tokio::time::timeout(Duration::from_secs(1), async {
            while service_weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scan task should release the service owner");
        assert!(processor_weak.upgrade().is_none());
    }
}
