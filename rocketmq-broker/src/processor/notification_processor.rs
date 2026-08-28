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

mod core;
mod response;
mod resume;
mod v2;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::filter::expression_type::ExpressionType;
#[cfg(any(test, feature = "test-support"))]
use rocketmq_model::common::hasher::string_hasher::JavaStringHasher;
use rocketmq_protocol::protocol::filter::filter_api::FilterAPI;
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::RequestProcessor;
use tokio::sync::Mutex as AsyncMutex;

use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::filter::expression_message_filter::ExpressionMessageFilter;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingRequestProcessor;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingService;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingServiceContext;
use crate::long_polling::notification_deferred::service::NotificationDeferredService;
use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetQueryCapability;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::processor::pop_message_processor::capability::PopPolicyState;
use crate::processor::processor_service::pop_buffer_merge_service::PopBufferMergeService;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

#[derive(Clone)]
pub(crate) struct NotificationPolicy {
    broker_permission: u32,
    broker_ip1: CheetahString,
    use_message_filter_for_notification: bool,
    max_message_filter_num_for_notification: i32,
}

impl NotificationPolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig) -> Self {
        Self {
            broker_permission: broker_config.broker_permission,
            broker_ip1: broker_config.broker_ip1().clone(),
            use_message_filter_for_notification: broker_config.use_message_filter_for_notification,
            max_message_filter_num_for_notification: broker_config.max_message_filter_num_for_notification,
        }
    }
}

#[derive(Clone)]
struct NotificationFilterContract {
    subscription_data: SubscriptionData,
    message_filter: ArcMessageFilter,
}

fn build_notification_filter_contract(
    enabled: bool,
    filters: &Arc<ConsumerFilterManager>,
    request_header: &NotificationRequestHeader,
) -> Result<Option<NotificationFilterContract>, ()> {
    let Some(expression) = request_header.exp.as_ref().filter(|expression| !expression.is_empty()) else {
        return Ok(None);
    };
    let Some(expression_type) = request_header
        .exp_type
        .as_ref()
        .filter(|expression_type| !expression_type.is_empty())
    else {
        return Ok(None);
    };
    if !enabled {
        return Ok(None);
    }

    let subscription_data =
        FilterAPI::build(&request_header.topic, expression, Some(expression_type.clone())).map_err(|_| ())?;
    if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str()))
        && subscription_data.sub_string.as_str() != SubscriptionData::SUB_ALL
        && subscription_data.code_set.is_empty()
    {
        return Err(());
    }
    let consumer_filter_data = if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
        None
    } else {
        Some(
            filters
                .resolve(
                    request_header.topic.clone(),
                    request_header.consumer_group.clone(),
                    Some(expression.clone()),
                    Some(expression_type.clone()),
                    current_millis(),
                )
                .ok_or(())?,
        )
    };
    let message_filter: ArcMessageFilter = Arc::new(ExpressionMessageFilter::new(
        Some(subscription_data.clone()),
        consumer_filter_data,
        Arc::clone(filters),
    ));

    Ok(Some(NotificationFilterContract {
        subscription_data,
        message_filter,
    }))
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug)]
pub struct NotificationFilterProbeMessage {
    pub tag: Option<CheetahString>,
    pub properties: HashMap<CheetahString, CheetahString>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NotificationFilterProbe {
    pub has_message: bool,
    pub optimistic: bool,
    pub scanned_messages: usize,
}

#[cfg(any(test, feature = "test-support"))]
pub fn run_notification_filter_probe(
    expression_type: &str,
    expression: &str,
    messages: &[NotificationFilterProbeMessage],
    max_scan: usize,
) -> Result<NotificationFilterProbe, CheetahString> {
    if max_scan == 0 {
        return Err(CheetahString::from_static_str("max scan must be greater than zero"));
    }
    let filters = Arc::new(ConsumerFilterManager::new(
        Arc::new(BrokerConfig::default()),
        Arc::new(rocketmq_store::MessageStoreConfig::default()),
    ));
    let header = NotificationRequestHeader {
        consumer_group: CheetahString::from_static_str("notification-probe-group"),
        topic: CheetahString::from_static_str("notification-probe-topic"),
        queue_id: 0,
        born_time: 0,
        order: false,
        attempt_id: None,
        exp_type: Some(CheetahString::from(expression_type)),
        exp: Some(CheetahString::from(expression)),
        is_lite_consumer: false,
        client_id: None,
        poll_time: 0,
        topic_request_header: None,
    };
    let contract = build_notification_filter_contract(true, &filters, &header)
        .map_err(|()| CheetahString::from_static_str("invalid notification expression"))?
        .ok_or_else(|| CheetahString::from_static_str("notification filter was not created"))?;

    if messages.len() > max_scan {
        return Ok(NotificationFilterProbe {
            has_message: true,
            optimistic: true,
            scanned_messages: 0,
        });
    }
    if messages.is_empty() {
        return Ok(NotificationFilterProbe {
            has_message: false,
            optimistic: false,
            scanned_messages: 0,
        });
    }

    for (index, message) in messages.iter().enumerate() {
        let tags_code = message
            .tag
            .as_ref()
            .map(|tag| JavaStringHasher::hash_str(tag.as_str()) as i64);
        if contract.message_filter.is_matched_by_consume_queue(tags_code, None)
            && contract
                .message_filter
                .is_matched_by_commit_log(None, Some(&message.properties))
        {
            return Ok(NotificationFilterProbe {
                has_message: true,
                optimistic: false,
                scanned_messages: index + 1,
            });
        }
    }

    Ok(NotificationFilterProbe {
        has_message: false,
        optimistic: false,
        scanned_messages: messages.len(),
    })
}

pub(crate) struct NotificationStoreCapability<MS: BrokerReadWriteStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: BrokerReadWriteStore> NotificationStoreCapability<MS> {
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

    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: ArcMessageFilter,
    ) -> Result<Option<rocketmq_store::GetMessageResult>, MessageStoreUnavailable> {
        self.escape_bridge
            .upgrade()
            .ok_or(MessageStoreUnavailable)?
            .get_message_with_filter_from_local_store(
                group,
                topic,
                queue_id,
                offset,
                max_msg_nums,
                Some(message_filter),
            )
            .await
    }
}

pub(crate) struct NotificationPopOffsetCapability<MS: BrokerReadWriteStore> {
    merge_service: Weak<PopBufferMergeService<MS>>,
}

impl<MS: BrokerReadWriteStore> NotificationPopOffsetCapability<MS> {
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

pub(crate) struct NotificationProcessorContext<MS: BrokerReadWriteStore> {
    command_factory: RemotingCommandFactory,
    policy: NotificationPolicy,
    retry_policies: PopPolicyState,
    topic_config_manager: Arc<TopicConfigManager>,
    subscription_group_lookup: SubscriptionGroupConfigLookup,
    consumer_filter_manager: Arc<ConsumerFilterManager>,
    consumer_order_info_manager: Arc<ConsumerOrderInfoManager>,
    consumer_offset_query: ConsumerOffsetQueryCapability<MS>,
    message_store: NotificationStoreCapability<MS>,
    pop_offset: NotificationPopOffsetCapability<MS>,
    long_polling: PopLongPollingServiceContext,
}

impl<MS: BrokerReadWriteStore> NotificationProcessorContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor lists the complete narrow Notification capability boundary"
    )]
    pub(crate) fn new(
        policy: NotificationPolicy,
        retry_policies: PopPolicyState,
        topic_config_manager: Arc<TopicConfigManager>,
        subscription_group_lookup: SubscriptionGroupConfigLookup,
        consumer_filter_manager: Arc<ConsumerFilterManager>,
        consumer_order_info_manager: Arc<ConsumerOrderInfoManager>,
        consumer_offset_query: ConsumerOffsetQueryCapability<MS>,
        message_store: NotificationStoreCapability<MS>,
        pop_offset: NotificationPopOffsetCapability<MS>,
        long_polling: PopLongPollingServiceContext,
    ) -> Self {
        Self {
            command_factory: application_remoting_command_factory(),
            policy,
            retry_policies,
            topic_config_manager,
            subscription_group_lookup,
            consumer_filter_manager,
            consumer_order_info_manager,
            consumer_offset_query,
            message_store,
            pop_offset,
            long_polling,
        }
    }

    pub(crate) fn with_command_factory(mut self, command_factory: RemotingCommandFactory) -> Self {
        self.command_factory = command_factory;
        self
    }
}

pub struct NotificationProcessor<MS: BrokerReadWriteStore> {
    context: NotificationProcessorContext<MS>,
    pop_long_polling_service: Arc<PopLongPollingService<NotificationProcessor<MS>>>,
    notification_deferred_service: OnceLock<Arc<NotificationDeferredService>>,
    lifecycle: AsyncMutex<()>,
}

impl<MS: BrokerReadWriteStore> NotificationProcessor<MS> {
    pub const BORN_TIME: &'static str = "bornTime";
    pub(crate) fn new(context: NotificationProcessorContext<MS>) -> Arc<Self> {
        Arc::new_cyclic(move |processor| Self {
            pop_long_polling_service: Arc::new(PopLongPollingService::new(
                context.long_polling.clone(),
                true,
                processor.clone(),
            )),
            notification_deferred_service: OnceLock::new(),
            context,
            lifecycle: AsyncMutex::new(()),
        })
    }

    /// Installs the Broker-owned BRK-05 deferred Notification service.
    ///
    /// BRK-06 owns the service lifecycle and installs it once during Broker composition. Until
    /// then, V2 requests that need suspension fail closed with `SERVICE_NOT_AVAILABLE`.
    pub(crate) fn install_notification_deferred_service(
        &self,
        service: Arc<NotificationDeferredService>,
    ) -> Result<(), Arc<NotificationDeferredService>> {
        self.notification_deferred_service.set(service)
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
        filter_contract: Option<&NotificationFilterContract>,
    ) -> bool {
        let topic_config = self.context.topic_config_manager.select_topic_config(topic_name);
        self.has_msg_from_topic(topic_config.as_deref(), random_q, request_header, filter_contract)
            .await
    }

    async fn has_msg_from_topic(
        &self,
        topic_config: Option<&TopicConfig>,
        random_q: i32,
        request_header: &NotificationRequestHeader,
        filter_contract: Option<&NotificationFilterContract>,
    ) -> bool {
        if let Some(tc) = topic_config {
            let topic_name = match tc.topic_name.as_ref() {
                Some(name) => name,
                None => return false,
            };
            for i in 0..tc.read_queue_nums {
                let queue_id = ((random_q as u32) + i) % tc.read_queue_nums;
                if self
                    .has_msg_from_queue(topic_name, request_header, queue_id as i32, filter_contract)
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
        filter_contract: Option<&NotificationFilterContract>,
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
        if rest_num <= 0 {
            return false;
        }
        let Some(filter_contract) = filter_contract else {
            return true;
        };
        if rest_num > i64::from(self.context.policy.max_message_filter_num_for_notification) {
            return true;
        }

        self.context
            .message_store
            .get_message(
                &request_header.consumer_group,
                target_topic,
                queue_id,
                offset,
                self.context.policy.max_message_filter_num_for_notification,
                Arc::clone(&filter_contract.message_filter),
            )
            .await
            .ok()
            .flatten()
            .is_some_and(|result| result.message_count() > 0)
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
    MS: BrokerReadWriteStore,
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

        let request_header = request.decode_command_custom_header::<NotificationRequestHeader>()?;
        let effective_peer = channel.remote_address();
        let core = self
            .execute_notification_core(&request_header, effective_peer, request.opaque(), None)
            .await;
        let core = match core {
            core::NotificationCoreOutcome::Reply(response) => return Ok(Some(response)),
            core::NotificationCoreOutcome::Ready(core) => core,
        };

        let mut polling_full = false;
        if !core.has_msg {
            match self.pop_long_polling_service.polling(
                ctx,
                request,
                PollingHeader::new_from_notification_request_header(&request_header),
                core.filter_contract
                    .as_ref()
                    .map(|contract| contract.subscription_data.clone()),
                core.filter_contract.map(|contract| contract.message_filter),
            ) {
                PollingResult::PollingSuc => return Ok(None),
                PollingResult::PollingFull => polling_full = true,
                _ => {}
            }
        }

        Ok(Some(response::compose_notification_response(
            &self.context.command_factory,
            core.has_msg,
            polling_full,
            request.opaque(),
        )))
    }
}

impl<MS> PopLongPollingRequestProcessor for NotificationProcessor<MS>
where
    MS: BrokerReadWriteStore,
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
    MS: BrokerReadWriteStore,
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
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_store::StorePorts;

    use crate::broker_runtime::BrokerMessageStore;
    use crate::broker_runtime::BrokerRuntime;
    use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingPolicy;

    pub(super) fn notification_processor_for_test(
        runtime: &mut BrokerRuntime,
    ) -> Arc<NotificationProcessor<BrokerMessageStore>> {
        let inner = runtime.runtime_state_mut();
        let policy = NotificationPolicy::from_config(&inner.broker_config());
        let long_polling_policy = PopLongPollingPolicy::from_config(&inner.broker_config());
        let topic_config_manager = inner.topic_config_manager_handle();
        let subscription_group_lookup = inner.subscription_group_manager().config_lookup();
        let consumer_filter_manager = Arc::new(inner.consumer_filter_manager().clone());
        let long_polling = PopLongPollingServiceContext::new(
            long_polling_policy,
            Arc::clone(&topic_config_manager),
            subscription_group_lookup.clone(),
            inner.broker_service_context(),
        );
        NotificationProcessor::new(NotificationProcessorContext::new(
            policy,
            inner.pop_policy_state(),
            topic_config_manager,
            subscription_group_lookup,
            consumer_filter_manager,
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
            .runtime_state_mut()
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
            use_message_filter_for_notification: false,
            max_message_filter_num_for_notification: 17,
            ..Default::default()
        };

        let policy = NotificationPolicy::from_config(&broker_config);

        assert_eq!(policy.broker_permission, 3);
        assert_eq!(policy.broker_ip1, "192.0.2.11");
        assert!(!policy.use_message_filter_for_notification);
        assert_eq!(policy.max_message_filter_num_for_notification, 17);
    }

    #[tokio::test]
    async fn notification_store_and_pop_capabilities_fail_closed_after_provider_shutdown() {
        let store = NotificationStoreCapability::<StorePorts> {
            escape_bridge: Weak::new(),
        };
        let pop = NotificationPopOffsetCapability::<StorePorts> {
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
        assert!(long_polling_source.contains("service_context: Option<ChildServiceContext>"));
        assert!(long_polling_source.contains("PopLongPollingServiceContext"));
    }
}
