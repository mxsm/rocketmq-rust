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
use std::sync::OnceLock;
use std::sync::Weak;

use crate::config::broker_config::BrokerConfig;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::message::MessageConst;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_store::GetMessageResult;
use rocketmq_store::GetMessageStatus;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::RequestProcessor;
use tokio::sync::Mutex as AsyncMutex;

use crate::failover::escape_bridge::EscapeBridge;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::lite::lite_lifecycle_manager::LiteLifecycleManager;
use crate::lite::memory_consumer_order_info_manager::LiteOrderVisibilityUpdate;
use crate::lite::memory_consumer_order_info_manager::MemoryConsumerOrderInfoManager;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingRequestProcessor;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingService;
use crate::long_polling::long_polling_service::pop_lite_long_polling_service::PopLiteLongPollingServiceContext;
use crate::long_polling::polling_result::PollingResult;
use crate::long_polling::pop_lite_deferred::service::PopLiteDeferredService;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::processor::pop_message_processor::QueueLockManager;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub(crate) mod core;
pub(crate) mod response;
mod resume;
mod v2;

#[derive(Clone)]
pub(crate) struct PopLiteMessagePolicy {
    broker_ip1: CheetahString,
    broker_permission: u32,
    max_client_event_count: i32,
    lite_event_full_dispatch_delay_time: u64,
    lite_event_full_dispatch_delay_time_for_wildcard_group: u64,
}

impl PopLiteMessagePolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig) -> Self {
        Self {
            broker_ip1: broker_config.broker_ip1.clone(),
            broker_permission: broker_config.broker_permission,
            max_client_event_count: broker_config.max_client_event_count,
            lite_event_full_dispatch_delay_time: broker_config.lite_event_full_dispatch_delay_time,
            lite_event_full_dispatch_delay_time_for_wildcard_group: broker_config
                .lite_event_full_dispatch_delay_time_for_wildcard_group,
        }
    }
}

pub(crate) struct PopLiteOffsetCapability<MS: BrokerReadWriteStore> {
    manager: Weak<ConsumerOffsetManager<MS>>,
}

impl<MS: BrokerReadWriteStore> PopLiteOffsetCapability<MS> {
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

    fn correct_offset_if_current(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        expected_current: i64,
        corrected_offset: i64,
    ) -> bool {
        self.manager.upgrade().is_some_and(|manager| {
            manager.correct_offset_if_current(group, topic, 0, expected_current, corrected_offset)
        })
    }

    fn apply_store_offset_correction(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        expected_current: i64,
        corrected_offset: i64,
    ) -> Option<i64> {
        let manager = self.manager.upgrade()?;
        if expected_current == -1 {
            manager.commit_offset(
                CheetahString::from_static_str("PopLiteInitialOffset"),
                group,
                topic,
                0,
                corrected_offset,
            );
            let effective = manager.query_offset(group, topic, 0);
            return (effective >= 0).then_some(effective);
        }
        manager
            .correct_offset_if_current(group, topic, 0, expected_current, corrected_offset)
            .then_some(corrected_offset)
    }

    pub(crate) fn assign_reset_offset(&self, topic: &CheetahString, group: &CheetahString, offset: i64) -> bool {
        let Some(manager) = self.manager.upgrade() else {
            return false;
        };
        manager.assign_reset_offset(topic, group, 0, offset);
        true
    }
}

pub(crate) struct PopLiteMessageStoreCapability<MS: BrokerReadWriteStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
    #[cfg(test)]
    store_await_hook: parking_lot::Mutex<Option<PopLiteStoreAwaitHook>>,
}

#[cfg(test)]
#[derive(Clone)]
struct PopLiteStoreAwaitHook {
    entered: Arc<tokio::sync::Barrier>,
    release: Arc<tokio::sync::Barrier>,
}

impl<MS: BrokerReadWriteStore> PopLiteMessageStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
            #[cfg(test)]
            store_await_hook: parking_lot::Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn set_store_await_hook(&self, entered: Arc<tokio::sync::Barrier>, release: Arc<tokio::sync::Barrier>) {
        *self.store_await_hook.lock() = Some(PopLiteStoreAwaitHook { entered, release });
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
        #[cfg(test)]
        let store_await_hook = self.store_await_hook.lock().clone();
        #[cfg(test)]
        if let Some(hook) = store_await_hook {
            hook.entered.wait().await;
            hook.release.wait().await;
        }
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

pub(crate) struct PopLiteMessageProcessorContext<MS: BrokerReadWriteStore> {
    command_factory: RemotingCommandFactory,
    policy: PopLiteMessagePolicy,
    topic_config_manager: Arc<TopicConfigManager>,
    subscription_group_lookup: SubscriptionGroupConfigLookup,
    consumer_offset: PopLiteOffsetCapability<MS>,
    message_store: PopLiteMessageStoreCapability<MS>,
    lite_event_dispatcher: LiteEventDispatcher,
    queue_lock_manager: QueueLockManager,
    long_polling: PopLiteLongPollingServiceContext,
}

impl<MS: BrokerReadWriteStore> PopLiteMessageProcessorContext<MS> {
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
            command_factory: application_remoting_command_factory(),
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

    pub(crate) fn with_command_factory(mut self, command_factory: RemotingCommandFactory) -> Self {
        self.command_factory = command_factory;
        self
    }
}

pub(crate) struct PopLiteMessageProcessor<MS: BrokerReadWriteStore> {
    context: PopLiteMessageProcessorContext<MS>,
    pop_lite_long_polling_service: Arc<PopLiteLongPollingService<PopLiteMessageProcessor<MS>>>,
    pop_lite_deferred_service: OnceLock<Arc<PopLiteDeferredService>>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PopLiteVisibilityUpdate {
    Updated,
    LockBusy,
    Missing,
    Stale,
}

impl<MS: BrokerReadWriteStore> PopLiteMessageProcessor<MS> {
    pub(crate) fn new(context: PopLiteMessageProcessorContext<MS>) -> Arc<Self> {
        let long_polling_context = context.long_polling.clone();
        Arc::new_cyclic(move |processor| Self {
            pop_lite_long_polling_service: Arc::new(PopLiteLongPollingService::new(
                long_polling_context,
                processor.clone(),
            )),
            pop_lite_deferred_service: OnceLock::new(),
            consumer_order_info_manager: MemoryConsumerOrderInfoManager::default(),
            context,
            lifecycle: AsyncMutex::new(()),
        })
    }

    /// Installs the Broker-owned BRK-05 deferred POP Lite service.
    ///
    /// BRK-06 owns the service lifecycle and installs it once during Broker composition. The
    /// service must share this processor's `LiteEventDispatcher` reservation domain.
    pub(crate) fn install_pop_lite_deferred_service(
        &self,
        service: Arc<PopLiteDeferredService>,
    ) -> Result<(), Arc<PopLiteDeferredService>> {
        self.pop_lite_deferred_service.set(service)
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

    pub(crate) fn max_offset(&self, lmq_name: &CheetahString) -> i64 {
        self.context.message_store.max_offset(lmq_name)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the fields are the ordered POP fencing coordinates carried by ChangeInvisibleTime"
    )]
    pub(crate) async fn change_order_visibility(
        &self,
        lmq_name: &CheetahString,
        group: &CheetahString,
        queue_offset: u64,
        pop_time: u64,
        next_visible_time: u64,
        suspend: bool,
    ) -> PopLiteVisibilityUpdate {
        if !self.context.queue_lock_manager.try_lock(lmq_name, group, 0).await {
            return PopLiteVisibilityUpdate::LockBusy;
        }
        let result = self.consumer_order_info_manager.change_visibility(
            lmq_name,
            group,
            0,
            queue_offset,
            pop_time,
            next_visible_time,
            suspend,
        );
        self.context.queue_lock_manager.unlock(lmq_name, group, 0).await;
        match result {
            LiteOrderVisibilityUpdate::Updated => PopLiteVisibilityUpdate::Updated,
            LiteOrderVisibilityUpdate::Missing => PopLiteVisibilityUpdate::Missing,
            LiteOrderVisibilityUpdate::Stale => PopLiteVisibilityUpdate::Stale,
        }
    }

    fn lite_dispatch_policy(&self, group: &CheetahString) -> (usize, u64) {
        let group_config = self
            .context
            .subscription_group_lookup
            .find_subscription_group_config(group);
        let max_event_count = group_config
            .as_ref()
            .map(|config| config.max_client_event_count())
            .filter(|count| *count > 0)
            .unwrap_or(self.context.policy.max_client_event_count)
            .max(1) as usize;
        let delay = if group_config.is_some_and(|config| config.lite_sub_wildcard()) {
            self.context
                .policy
                .lite_event_full_dispatch_delay_time_for_wildcard_group
        } else {
            self.context.policy.lite_event_full_dispatch_delay_time
        };
        (max_event_count, delay)
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
        if request_header.is_timeout_too_much_at(rocketmq_runtime::common::time_utils::current_millis() as i64) {
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
            let Some(queue_lock) = self.context.queue_lock_manager.try_acquire_with_key(lock_key).await else {
                requeue_events.insert(lmq_name);
                continue;
            };

            let result = self
                .pop_from_lmq(request_header, &attempt_id, &lmq_name, remaining)
                .await;
            drop(queue_lock);

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

        let (consume_offset, expected_current_offset) = self.get_pop_offset(&request_header.consumer_group, lmq_name);
        let Some(get_message_result) = self
            .get_message(
                &request_header.consumer_group,
                lmq_name,
                consume_offset,
                expected_current_offset,
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

    fn get_pop_offset(&self, group: &CheetahString, lmq_name: &CheetahString) -> (i64, i64) {
        let reset_offset = self
            .context
            .consumer_offset
            .query_then_erase_reset_offset(lmq_name, group);
        let expected_current = self.context.consumer_offset.query_offset(group, lmq_name);
        if let Some(reset_offset) = reset_offset {
            self.consumer_order_info_manager.clear_block(lmq_name, group, 0);
            if reset_offset >= expected_current {
                self.context
                    .consumer_offset
                    .commit_offset("ResetOffset", group, lmq_name, reset_offset);
            } else {
                self.context
                    .consumer_offset
                    .correct_offset_if_current(group, lmq_name, expected_current, reset_offset);
            }
            let effective_offset = self.context.consumer_offset.query_offset(group, lmq_name);
            return (effective_offset.max(0), effective_offset);
        }
        (expected_current.max(0), expected_current)
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        lmq_name: &CheetahString,
        offset: i64,
        expected_current_offset: i64,
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
            if let Some(effective_offset) = self.context.consumer_offset.apply_store_offset_correction(
                group,
                lmq_name,
                expected_current_offset,
                correct_offset,
            ) {
                return self
                    .context
                    .message_store
                    .get_message(group, lmq_name, effective_offset, batch_size)
                    .await;
            }
        }
        Some(result)
    }

    fn read_get_message_result(&self, get_message_result: &GetMessageResult) -> Bytes {
        let mut bytes_mut = BytesMut::with_capacity(get_message_result.buffer_total_size() as usize);
        for mapped in get_message_result.message_mapped_list() {
            if let Some(bytes) = mapped.get_bytes_ref() {
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
        self.context
            .command_factory
            .create_response_command_with_code_remark(code, remark)
            .set_opaque(request.opaque())
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

impl<MS: BrokerReadWriteStore> PopLiteMessageProcessor<MS> {
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
        let result = match dispatcher.reserve_pending_events(&request_header.client_id) {
            Some(reservation) => self.execute_pop_lite_batch(&request_header, reservation.commit()).await,
            None => self.execute_pop_lite_without_events(&request_header).await,
        };
        let response_kind = if result.body.is_some() {
            response::PopLiteResponseKind::Found
        } else {
            match self.pop_lite_long_polling_service.polling(
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
                PollingResult::PollingFull => response::PopLiteResponseKind::PollingFull,
                _ => response::PopLiteResponseKind::PollingTimeout,
            }
        };
        let response = self.compose_pop_lite_command(request.opaque(), &request_header, result, response_kind);

        Ok(Some(response))
    }
}

impl<MS: BrokerReadWriteStore> RequestProcessor for PopLiteMessageProcessor<MS> {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_shared(channel, ctx, request).await
    }
}

impl<MS: BrokerReadWriteStore> PopLiteLongPollingRequestProcessor for PopLiteMessageProcessor<MS> {
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
mod tests;
