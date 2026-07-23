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
use std::sync::Weak;

use cheetah_string::CheetahString;
use rand::RngExt;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_remoting::protocol::header::notification_response_header::NotificationResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_store::base::message_store::MessageStore;
use tokio::sync::Mutex as AsyncMutex;
use tracing::error;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingRequestProcessor;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingService;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingServiceContext;
use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetQueryCapability;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::processor::processor_service::pop_buffer_merge_service::PopBufferMergeService;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

#[derive(Clone)]
pub(crate) struct NotificationPolicy {
    broker_permission: u32,
    broker_ip1: CheetahString,
    enable_retry_topic_v2: bool,
    retrieve_message_from_pop_retry_topic_v1: bool,
}

impl NotificationPolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig) -> Self {
        Self {
            broker_permission: broker_config.broker_permission,
            broker_ip1: broker_config.broker_ip1().clone(),
            enable_retry_topic_v2: broker_config.enable_retry_topic_v2,
            retrieve_message_from_pop_retry_topic_v1: broker_config.retrieve_message_from_pop_retry_topic_v1,
        }
    }
}

pub(crate) struct NotificationStoreCapability<MS: MessageStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> NotificationStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
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
}

pub(crate) struct NotificationPopOffsetCapability<MS: MessageStore> {
    merge_service: Weak<PopBufferMergeService<MS>>,
}

impl<MS: MessageStore> NotificationPopOffsetCapability<MS> {
    pub(crate) fn new(merge_service: &Arc<PopBufferMergeService<MS>>) -> Self {
        Self {
            merge_service: Arc::downgrade(merge_service),
        }
    }

    async fn latest_offset(&self, topic: &CheetahString, group: &CheetahString, queue_id: i32) -> i64 {
        let Some(service) = self.merge_service.upgrade() else {
            return -1;
        };
        service.get_latest_offset_full(topic, group, queue_id).await
    }
}

pub(crate) struct NotificationProcessorContext<MS: MessageStore> {
    policy: NotificationPolicy,
    topic_config_manager: Arc<TopicConfigManager>,
    subscription_group_lookup: SubscriptionGroupConfigLookup,
    consumer_order_info_manager: Arc<ConsumerOrderInfoManager>,
    consumer_offset_query: ConsumerOffsetQueryCapability<MS>,
    message_store: NotificationStoreCapability<MS>,
    pop_offset: NotificationPopOffsetCapability<MS>,
    long_polling: PopLongPollingServiceContext,
}

impl<MS: MessageStore> NotificationProcessorContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor lists the complete narrow Notification capability boundary"
    )]
    pub(crate) fn new(
        policy: NotificationPolicy,
        topic_config_manager: Arc<TopicConfigManager>,
        subscription_group_lookup: SubscriptionGroupConfigLookup,
        consumer_order_info_manager: Arc<ConsumerOrderInfoManager>,
        consumer_offset_query: ConsumerOffsetQueryCapability<MS>,
        message_store: NotificationStoreCapability<MS>,
        pop_offset: NotificationPopOffsetCapability<MS>,
        long_polling: PopLongPollingServiceContext,
    ) -> Self {
        Self {
            policy,
            topic_config_manager,
            subscription_group_lookup,
            consumer_order_info_manager,
            consumer_offset_query,
            message_store,
            pop_offset,
            long_polling,
        }
    }
}

pub struct NotificationProcessor<MS: MessageStore> {
    context: NotificationProcessorContext<MS>,
    pop_long_polling_service: Arc<PopLongPollingService<NotificationProcessor<MS>>>,
    lifecycle: AsyncMutex<()>,
}

impl<MS: MessageStore> NotificationProcessor<MS> {
    pub const BORN_TIME: &'static str = "bornTime";
    pub(crate) fn new(context: NotificationProcessorContext<MS>) -> Arc<Self> {
        Arc::new_cyclic(move |processor| Self {
            pop_long_polling_service: Arc::new(PopLongPollingService::new(
                context.long_polling.clone(),
                true,
                processor.clone(),
            )),
            context,
            lifecycle: AsyncMutex::new(()),
        })
    }

    pub async fn start(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        PopLongPollingService::start(&self.pop_long_polling_service).await
    }

    pub async fn shutdown(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        self.pop_long_polling_service.shutdown().await;
    }

    pub fn notify_message_arriving_simple(&self, topic: &CheetahString, queue_id: i32) {
        self.pop_long_polling_service
            .notify_message_arriving_with_retry_topic(topic, queue_id);
    }

    pub fn notify_message_arriving(
        &self,
        topic: CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        self.pop_long_polling_service
            .notify_message_arriving_with_retry_topic_full(
                topic,
                queue_id,
                tags_code,
                msg_store_time,
                filter_bit_map,
                properties,
            );
    }

    async fn has_msg_from_topic_name(
        &self,
        topic_name: &CheetahString,
        random_q: i32,
        request_header: &NotificationRequestHeader,
    ) -> bool {
        let topic_config = self.context.topic_config_manager.select_topic_config(topic_name);
        self.has_msg_from_topic(topic_config.as_deref(), random_q, request_header)
            .await
    }

    async fn has_msg_from_topic(
        &self,
        topic_config: Option<&TopicConfig>,
        random_q: i32,
        request_header: &NotificationRequestHeader,
    ) -> bool {
        if let Some(tc) = topic_config {
            let topic_name = match tc.topic_name.as_ref() {
                Some(name) => name,
                None => return false,
            };
            for i in 0..tc.read_queue_nums {
                let queue_id = ((random_q as u32) + i) % tc.read_queue_nums;
                if self
                    .has_msg_from_queue(topic_name, request_header, queue_id as i32)
                    .await
                {
                    return true;
                }
            }
        }
        false
    }

    async fn has_msg_from_queue(
        &self,
        target_topic: &CheetahString,
        request_header: &NotificationRequestHeader,
        queue_id: i32,
    ) -> bool {
        // For order mode, check if blocked. If attempt_id is missing, skip block check.
        if request_header.order {
            if let Some(attempt_id) = request_header.attempt_id.as_ref() {
                if self.context.consumer_order_info_manager.check_block(
                    attempt_id,
                    &request_header.topic,
                    &request_header.consumer_group,
                    queue_id,
                    0,
                ) {
                    return false;
                }
            }
        }

        let offset = self
            .get_pop_offset(target_topic, &request_header.consumer_group, queue_id)
            .await;
        let Ok(max_offset) = self.context.message_store.max_offset(target_topic, queue_id) else {
            return false;
        };
        let rest_num = max_offset - offset;
        rest_num > 0
    }

    async fn get_pop_offset(&self, topic: &CheetahString, cid: &CheetahString, queue_id: i32) -> i64 {
        let mut offset = self.context.consumer_offset_query.query_offset(cid, topic, queue_id);
        if offset < 0 {
            if let Ok(min_offset) = self.context.message_store.min_offset(topic, queue_id) {
                offset = min_offset;
            }
        }
        let buffer_offset = self.context.pop_offset.latest_offset(topic, cid, queue_id).await;
        if buffer_offset < 0 {
            offset
        } else {
            buffer_offset.max(offset)
        }
    }
}

impl<MS> NotificationProcessor<MS>
where
    MS: MessageStore,
{
    pub(crate) async fn process_request_shared(
        &self,
        _channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let now = current_millis();
        request.add_ext_field_if_not_exist(NotificationProcessor::<MS>::BORN_TIME, now.to_string());
        if request
            .ext_fields()
            .and_then(|fields| fields.get(NotificationProcessor::<MS>::BORN_TIME))
            .map(|v| v == "0")
            .unwrap_or(false)
        {
            request.add_ext_field(NotificationProcessor::<MS>::BORN_TIME, now.to_string());
        }
        let channel = ctx.channel();

        let mut response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header::<NotificationRequestHeader>()?;

        response.set_opaque_mut(request.opaque());

        if !PermName::is_readable(self.context.policy.broker_permission) {
            response.set_code_ref(ResponseCode::NoPermission);
            response.set_remark_mut(format!(
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
            response.set_code_ref(ResponseCode::TopicNotExist);
            response.set_remark_mut(format!(
                "topic[{}] not exist, apply first please! {}",
                request_header.topic,
                FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
            ));
            return Ok(Some(response));
        }
        let topic_config = topic_config.unwrap();

        if !PermName::is_readable(topic_config.perm) {
            response.set_code_ref(ResponseCode::NoPermission);
            response.set_remark_mut(format!(
                "the topic[{}] peeking message is forbidden",
                request_header.topic
            ));
            return Ok(Some(response));
        }

        if request_header.queue_id >= topic_config.get_read_queue_nums() as i32 {
            let error_info = format!(
                "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{:?}]",
                request_header.queue_id,
                request_header.topic,
                topic_config.get_read_queue_nums(),
                channel.remote_address()
            );
            warn!("{}", error_info);
            response.set_code_ref(ResponseCode::SystemError);
            response.set_remark_mut(&error_info);
            return Ok(Some(response));
        }

        let subscription_group_config = match self
            .context
            .subscription_group_lookup
            .find_subscription_group_config(&request_header.consumer_group)
        {
            Some(config) => config,
            None => {
                response.set_code_ref(ResponseCode::SubscriptionGroupNotExist);
                response.set_remark_mut(format!(
                    "subscription group [{}] does not exist, {}",
                    request_header.consumer_group,
                    FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                ));
                return Ok(Some(response));
            }
        };

        if !subscription_group_config.consume_enable() {
            response.set_code_ref(ResponseCode::NoPermission);
            response.set_remark_mut(format!(
                "subscription group no permission, {}",
                request_header.consumer_group
            ));
            return Ok(Some(response));
        }

        let random_q: i32 = rand::rng().random_range(0..100);
        let mut has_msg = false;
        let need_retry = random_q % 5 == 0;

        if need_retry {
            let retry_topic = KeyBuilder::build_pop_retry_topic(
                request_header.topic.as_str(),
                request_header.consumer_group.as_str(),
                self.context.policy.enable_retry_topic_v2,
            )
            .into();
            has_msg = self
                .has_msg_from_topic_name(&retry_topic, random_q, &request_header)
                .await;
            if !has_msg
                && self.context.policy.enable_retry_topic_v2
                && self.context.policy.retrieve_message_from_pop_retry_topic_v1
            {
                let retry_topic_v1 = KeyBuilder::build_pop_retry_topic_v1(
                    request_header.topic.as_str(),
                    request_header.consumer_group.as_str(),
                )
                .into();
                has_msg = self
                    .has_msg_from_topic_name(&retry_topic_v1, random_q, &request_header)
                    .await;
            }
        }
        if !has_msg {
            if request_header.queue_id < 0 {
                has_msg = self
                    .has_msg_from_topic(Some(&topic_config), random_q, &request_header)
                    .await;
            } else if let Some(topic_name) = topic_config.topic_name.as_ref() {
                let queue_id = request_header.queue_id;
                has_msg = self.has_msg_from_queue(topic_name, &request_header, queue_id).await;
            }
            // if it doesn't have message, fetch retry again
            if !need_retry && !has_msg {
                let retry_topic = KeyBuilder::build_pop_retry_topic(
                    request_header.topic.as_str(),
                    request_header.consumer_group.as_str(),
                    self.context.policy.enable_retry_topic_v2,
                )
                .into();
                has_msg = self
                    .has_msg_from_topic_name(&retry_topic, random_q, &request_header)
                    .await;
                if !has_msg
                    && self.context.policy.enable_retry_topic_v2
                    && self.context.policy.retrieve_message_from_pop_retry_topic_v1
                {
                    let retry_topic_v1 = KeyBuilder::build_pop_retry_topic_v1(
                        request_header.topic.as_str(),
                        request_header.consumer_group.as_str(),
                    )
                    .into();
                    has_msg = self
                        .has_msg_from_topic_name(&retry_topic_v1, random_q, &request_header)
                        .await;
                }
            }
        }

        let mut polling_full = false;
        if !has_msg {
            match self.pop_long_polling_service.polling_(
                ctx,
                request,
                PollingHeader::new_from_notification_request_header(&request_header),
            ) {
                PollingResult::PollingSuc => return Ok(None),
                PollingResult::PollingFull => polling_full = true,
                _ => {}
            }
        }

        response.set_code_ref(ResponseCode::Success);
        response.set_command_custom_header_ref(NotificationResponseHeader { has_msg, polling_full });

        Ok(Some(response))
    }
}

impl<MS> PopLongPollingRequestProcessor for NotificationProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request_when_wakeup(
        &self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        mut request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_shared(channel, ctx, &mut request).await
    }
}

impl<MS> RequestProcessor for NotificationProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_shared(channel, ctx, request).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::GenericMessageStore;

    use crate::broker_runtime::BrokerMessageStore;
    use crate::broker_runtime::BrokerRuntime;
    use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingPolicy;

    fn notification_processor_for_test(runtime: &mut BrokerRuntime) -> Arc<NotificationProcessor<BrokerMessageStore>> {
        let inner = runtime.inner_for_test();
        let policy = NotificationPolicy::from_config(&inner.broker_config());
        let long_polling_policy = PopLongPollingPolicy::from_config(&inner.broker_config());
        let topic_config_manager = inner.topic_config_manager_handle();
        let subscription_group_lookup = inner.subscription_group_manager().config_lookup();
        let long_polling = PopLongPollingServiceContext::new(
            long_polling_policy,
            Arc::clone(&topic_config_manager),
            subscription_group_lookup.clone(),
            inner.broker_service_task_group(),
        );
        NotificationProcessor::new(NotificationProcessorContext::new(
            policy,
            topic_config_manager,
            subscription_group_lookup,
            inner.consumer_order_info_manager_handle(),
            inner.consumer_offset_manager_handle().query_capability(),
            NotificationStoreCapability {
                escape_bridge: Weak::new(),
            },
            NotificationPopOffsetCapability {
                merge_service: Weak::new(),
            },
            long_polling,
        ))
    }

    #[tokio::test]
    async fn notification_long_polling_service_uses_weak_processor_back_reference() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}

        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let processor = notification_processor_for_test(&mut runtime);
        let processor_weak = Arc::downgrade(&processor);
        let service = processor.pop_long_polling_service.clone();

        assert_send_sync(&processor);
        drop(processor);

        assert!(processor_weak.upgrade().is_none());
        assert_eq!(Arc::strong_count(&service), 1);
    }

    #[tokio::test]
    async fn notification_long_polling_service_uses_broker_parent_task_group() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let runtime_context = RuntimeContext::from_current("notification-long-polling-parent-test");
        let broker_service = runtime_context.service_context("broker-service");
        let mut runtime =
            BrokerRuntime::new_with_service_context(broker_config, message_store_config, broker_service.clone());
        let parent_id = runtime
            .inner_for_test()
            .broker_service_task_group()
            .expect("broker service task group should exist")
            .id();
        let processor = notification_processor_for_test(&mut runtime);

        PopLongPollingService::start(&processor.pop_long_polling_service).await;
        let task_group = processor
            .pop_long_polling_service
            .task_group_for_test()
            .expect("notification long-polling task group should be installed");

        assert_eq!(task_group.parent_id(), Some(parent_id));
        processor.shutdown().await;
        let report = broker_service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[test]
    fn notification_policy_captures_only_required_startup_values() {
        let broker_config = BrokerConfig {
            broker_permission: 3,
            broker_ip1: CheetahString::from_static_str("192.0.2.11"),
            enable_retry_topic_v2: true,
            retrieve_message_from_pop_retry_topic_v1: true,
            ..Default::default()
        };

        let policy = NotificationPolicy::from_config(&broker_config);

        assert_eq!(policy.broker_permission, 3);
        assert_eq!(policy.broker_ip1, "192.0.2.11");
        assert!(policy.enable_retry_topic_v2);
        assert!(policy.retrieve_message_from_pop_retry_topic_v1);
    }

    #[tokio::test]
    async fn notification_store_and_pop_capabilities_fail_closed_after_provider_shutdown() {
        let store = NotificationStoreCapability::<GenericMessageStore> {
            escape_bridge: Weak::new(),
        };
        let pop = NotificationPopOffsetCapability::<GenericMessageStore> {
            merge_service: Weak::new(),
        };
        let topic = CheetahString::from_static_str("topic-a");
        let group = CheetahString::from_static_str("group-a");

        assert!(store.min_offset(&topic, 0).is_err());
        assert!(store.max_offset(&topic, 0).is_err());
        assert_eq!(pop.latest_offset(&topic, &group, 0).await, -1);
    }

    #[test]
    fn notification_and_long_polling_sources_use_only_explicit_capabilities() {
        let notification_source = include_str!("notification_processor.rs");
        let long_polling_source = include_str!("../long_polling/long_polling_service/pop_long_polling_service.rs");

        assert!(!notification_source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!notification_source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(notification_source.contains("NotificationProcessorContext<MS>"));
        assert!(notification_source.contains("Weak<EscapeBridge<MS>>"));
        assert!(notification_source.contains("Weak<PopBufferMergeService<MS>>"));
        assert!(!long_polling_source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!long_polling_source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(long_polling_source.contains("parent_task_group: Option<TaskGroup>"));
        assert!(long_polling_source.contains("PopLongPollingServiceContext"));
    }
}
