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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::entity::ClientGroup;
use rocketmq_common::common::lite::get_lite_topic;
use rocketmq_common::common::lite::to_lmq_name;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::error_response;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_remoting::protocol::admin::topic_offset::TopicOffset;
use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
use rocketmq_remoting::protocol::header::trigger_lite_dispatch_request_header::TriggerLiteDispatchRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::queue::consume_queue_store::ConsumeQueueStoreTrait;
use rocketmq_store::queue::local_file_consume_queue_store::ConsumeQueueStore;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::lite::lite_consumer_lag_calculator::LiteConsumerLagCalculator;
use crate::lite::lite_consumer_lag_calculator::LiteConsumerLagDataSource;
use crate::lite::lite_consumer_lag_calculator::LiteOffsetTable;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;
use crate::lite::lite_lifecycle_manager::LiteLifecycleManager;
use crate::lite::lite_sharding::LiteShardingView;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::processor::pop_lite_message_processor::PopLiteMessageProcessor;
use crate::subscription::lite_subscription_registry::LiteSubscriptionRecord;
use crate::subscription::lite_subscription_registry::LiteSubscriptionRegistry;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub(crate) struct LiteManagerPolicy {
    store_type: CheetahString,
    max_lmq_num: i32,
    broker_name: CheetahString,
    max_client_event_count: i32,
    dispatch_delay_millis: u64,
}

impl LiteManagerPolicy {
    pub(crate) fn from_configs(broker_config: &BrokerConfig, message_store_config: &MessageStoreConfig) -> Self {
        Self {
            store_type: CheetahString::from_slice(message_store_config.store_type.get_store_type()),
            max_lmq_num: message_store_config.max_lmq_consume_queue_num as i32,
            broker_name: broker_config.broker_name().clone(),
            max_client_event_count: broker_config.max_client_event_count,
            dispatch_delay_millis: broker_config.lite_event_full_dispatch_delay_time,
        }
    }
}

pub(crate) struct LiteManagerOffsetCapability<MS: MessageStore> {
    manager: Weak<ConsumerOffsetManager<MS>>,
}

impl<MS: MessageStore> LiteManagerOffsetCapability<MS> {
    pub(crate) fn new(manager: &Arc<ConsumerOffsetManager<MS>>) -> Self {
        Self {
            manager: Arc::downgrade(manager),
        }
    }

    fn query_offset(&self, group: &CheetahString, topic: &CheetahString) -> i64 {
        self.manager
            .upgrade()
            .map(|manager| manager.query_offset(group, topic, 0))
            .unwrap_or(-1)
    }

    fn offset_table_snapshot(&self) -> LiteOffsetTable {
        self.manager
            .upgrade()
            .map(|manager| manager.offset_table_snapshot())
            .unwrap_or_default()
    }

    fn offset_table_len(&self) -> usize {
        self.manager
            .upgrade()
            .map(|manager| manager.offset_table_len())
            .unwrap_or(0)
    }
}

pub(crate) struct LiteManagerStoreCapability<MS: MessageStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> LiteManagerStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
    }

    fn is_available(&self) -> bool {
        self.escape_bridge.strong_count() > 0
    }

    fn with_store<R>(&self, operation: impl FnOnce(&MS) -> R) -> Option<R> {
        self.escape_bridge.upgrade()?.try_with_message_store(operation).ok()
    }

    fn queue_store_stats(&self) -> (i32, i32) {
        self.with_store(|store| {
            let Some(queue_store) = store.get_queue_store().downcast_ref::<ConsumeQueueStore>() else {
                return (0, 0);
            };

            let consume_queue_table = queue_store.get_consume_queue_table();
            let cq_table_size = consume_queue_table
                .lock()
                .values()
                .map(|queue_map| queue_map.len() as i32)
                .sum();
            (queue_store.get_lmq_num(), cq_table_size)
        })
        .unwrap_or_default()
    }

    fn topic_offset(&self, lmq_name: &CheetahString) -> Option<TopicOffset> {
        self.with_store(|store| {
            let min_offset = store.get_min_offset_in_queue(lmq_name, 0);
            let max_offset = store.get_max_offset_in_queue(lmq_name, 0);
            let last_update_timestamp = if max_offset > 0 {
                store.get_message_store_timestamp(lmq_name, 0, max_offset - 1)
            } else {
                -1
            };

            let mut topic_offset = TopicOffset::new();
            topic_offset.set_min_offset(min_offset);
            topic_offset.set_max_offset(max_offset);
            topic_offset.set_last_update_timestamp(last_update_timestamp);
            topic_offset
        })
    }

    fn max_offset(&self, lifecycle_manager: &LiteLifecycleManager, lmq_name: &CheetahString) -> i64 {
        self.with_store(|store| lifecycle_manager.get_max_offset_in_queue(Some(store), lmq_name))
            .unwrap_or(0)
    }

    fn is_lmq_exist(&self, lifecycle_manager: &LiteLifecycleManager, lmq_name: &CheetahString) -> bool {
        self.with_store(|store| lifecycle_manager.is_lmq_exist(Some(store), lmq_name))
            .unwrap_or(false)
    }

    fn message_store_timestamp(&self, lmq_name: &CheetahString, offset: i64) -> i64 {
        self.with_store(|store| store.get_message_store_timestamp(lmq_name, 0, offset))
            .unwrap_or(0)
    }
}

pub(crate) struct LiteManagerContext<MS: MessageStore> {
    policy: LiteManagerPolicy,
    topic_config_manager: Arc<TopicConfigManager>,
    subscription_group_manager: SubscriptionGroupManager,
    lite_subscription_registry: LiteSubscriptionRegistry,
    lite_event_dispatcher: LiteEventDispatcher,
    lite_lifecycle_manager: LiteLifecycleManager,
    sharding: LiteShardingView,
    consumer_offset: LiteManagerOffsetCapability<MS>,
    message_store: LiteManagerStoreCapability<MS>,
    pop_lite_message_processor: Weak<PopLiteMessageProcessor<MS>>,
}

impl<MS: MessageStore> LiteManagerContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "composition root wires explicit Lite manager capabilities"
    )]
    pub(crate) fn new(
        policy: LiteManagerPolicy,
        topic_config_manager: Arc<TopicConfigManager>,
        subscription_group_manager: SubscriptionGroupManager,
        lite_subscription_registry: LiteSubscriptionRegistry,
        lite_event_dispatcher: LiteEventDispatcher,
        lite_lifecycle_manager: LiteLifecycleManager,
        sharding: LiteShardingView,
        consumer_offset: LiteManagerOffsetCapability<MS>,
        message_store: LiteManagerStoreCapability<MS>,
        pop_lite_message_processor: Weak<PopLiteMessageProcessor<MS>>,
    ) -> Self {
        Self {
            policy,
            topic_config_manager,
            subscription_group_manager,
            lite_subscription_registry,
            lite_event_dispatcher,
            lite_lifecycle_manager,
            sharding,
            consumer_offset,
            message_store,
            pop_lite_message_processor,
        }
    }

    fn order_info_count(&self) -> i32 {
        self.pop_lite_message_processor
            .upgrade()
            .map_or(0, |processor| processor.order_info_count())
    }
}

impl<MS: MessageStore> LiteConsumerLagDataSource for LiteManagerContext<MS> {
    fn offset_table_snapshot(&self) -> LiteOffsetTable {
        self.consumer_offset.offset_table_snapshot()
    }

    fn max_offset(&self, lmq_name: &CheetahString) -> i64 {
        self.message_store.max_offset(&self.lite_lifecycle_manager, lmq_name)
    }

    fn message_store_timestamp(&self, lmq_name: &CheetahString, offset: i64) -> i64 {
        self.message_store.message_store_timestamp(lmq_name, offset)
    }
}

pub(crate) struct LiteManagerProcessor<MS: MessageStore> {
    context: LiteManagerContext<MS>,
}

impl<MS: MessageStore> LiteManagerProcessor<MS> {
    pub(crate) fn new(context: LiteManagerContext<MS>) -> Self {
        Self { context }
    }
}

impl<MS: MessageStore> LiteManagerProcessor<MS> {
    pub(crate) async fn process_request_shared(
        &self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match RequestCode::from(request.code()) {
            RequestCode::GetBrokerLiteInfo => self.get_broker_lite_info(request),
            RequestCode::GetParentTopicInfo => self.get_parent_topic_info(request),
            RequestCode::GetLiteTopicInfo => self.get_lite_topic_info(request),
            RequestCode::GetLiteClientInfo => self.get_lite_client_info(request),
            RequestCode::GetLiteGroupInfo => self.get_lite_group_info(request),
            RequestCode::TriggerLiteDispatch => self.trigger_lite_dispatch(request),
            request_code => {
                warn!("LiteManagerProcessor received unknown request code: {:?}", request_code);
                Ok(Some(error_response::request_code_not_supported_with_remark_and_opaque(
                    request.code(),
                    format!("LiteManagerProcessor request code {} not supported", request.code()),
                    request.opaque(),
                )))
            }
        }
    }
}

impl<MS: MessageStore> RequestProcessor for LiteManagerProcessor<MS> {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_shared(channel, ctx, request).await
    }
}

impl<MS: MessageStore> LiteManagerProcessor<MS> {
    fn get_broker_lite_info(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let subscriptions = self.context.lite_subscription_registry.all_subscriptions();
        let topic_meta = self.build_lite_topic_meta();
        let group_meta = self.build_lite_group_meta();
        let unique_lmq_count = self.unique_lmq_count(&subscriptions);
        let (store_lmq_num, cq_table_size) = self.queue_store_stats();

        let mut body = GetBrokerLiteInfoResponseBody::new();
        body.set_store_type(self.context.policy.store_type.clone());
        body.set_max_lmq_num(self.context.policy.max_lmq_num);
        body.set_current_lmq_num(store_lmq_num.max(unique_lmq_count));
        body.set_lite_subscription_count(self.context.lite_subscription_registry.active_subscription_num() as i32);
        body.set_order_info_count(self.context.order_info_count());
        body.set_cq_table_size(cq_table_size);
        body.set_offset_table_size(self.context.consumer_offset.offset_table_len() as i32);
        body.set_event_map_size(self.context.lite_event_dispatcher.event_map_size() as i32);
        body.set_topic_meta(topic_meta);
        body.set_group_meta(group_meta);

        Ok(Some(self.response_with_body(request, &body)?))
    }

    fn get_parent_topic_info(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetParentTopicInfoRequestHeader>()?;
        let topic_config = match self.validate_lite_parent_topic(&request_header.topic) {
            Ok(topic_config) => topic_config,
            Err((code, remark)) => return Ok(Some(self.response_with_code(request, code, remark))),
        };

        let subscriptions = self
            .context
            .lite_subscription_registry
            .all_subscriptions()
            .into_iter()
            .filter(|subscription| subscription.topic == request_header.topic)
            .collect::<Vec<_>>();
        let groups = self.lite_bound_groups(&request_header.topic);
        let lite_topic_count = self
            .context
            .lite_lifecycle_manager
            .get_lite_topic_count(&subscriptions, &request_header.topic);

        let mut body = GetParentTopicInfoResponseBody::new();
        body.set_topic(request_header.topic);
        body.set_ttl(topic_config.get_lite_topic_expiration());
        body.set_groups(groups);
        body.set_lmq_num(lite_topic_count);
        body.set_lite_topic_count(lite_topic_count);

        Ok(Some(self.response_with_body(request, &body)?))
    }

    fn get_lite_topic_info(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetLiteTopicInfoRequestHeader>()?;
        if let Err((code, remark)) = self.validate_lite_parent_topic(&request_header.parent_topic) {
            return Ok(Some(self.response_with_code(request, code, remark)));
        }

        let Some(lmq_name) = to_lmq_name(request_header.parent_topic.as_str(), request_header.lite_topic.as_str())
        else {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::InvalidParameter,
                "parentTopic or liteTopic is blank.",
            )));
        };
        let lmq_name = CheetahString::from_string(lmq_name);
        let subscribers = self
            .context
            .lite_subscription_registry
            .all_subscriptions()
            .into_iter()
            .filter(|subscription| {
                subscription.topic == request_header.parent_topic && subscription.lite_topic_set.contains(&lmq_name)
            })
            .map(|subscription| ClientGroup::from_parts(subscription.client_id, subscription.group))
            .collect::<HashSet<_>>();
        if subscribers.is_empty() {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::QueryNotFound,
                format!(
                    "Lite topic [{}] under [{}] has no subscribers.",
                    request_header.lite_topic, request_header.parent_topic
                ),
            )));
        }

        let parent_topic = request_header.parent_topic;
        let lite_topic = request_header.lite_topic;
        let mut body = GetLiteTopicInfoResponseBody::new();
        body.with_parent_topic(parent_topic.clone())
            .with_lite_topic(lite_topic)
            .with_subscriber(subscribers)
            .with_sharding_to_broker(
                self.context.sharding.sharding_by_lmq_name(&parent_topic, &lmq_name) == self.context.policy.broker_name,
            );
        if let Some(topic_offset) = self.topic_offset_for_lmq(&lmq_name) {
            body.with_topic_offset(topic_offset);
        }

        Ok(Some(self.response_with_body(request, &body)?))
    }

    fn get_lite_client_info(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetLiteClientInfoRequestHeader>()?;
        let Some(parent_topic) = request_header.parent_topic.filter(|topic| !topic.is_empty()) else {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::InvalidParameter,
                "parentTopic is blank.",
            )));
        };
        let Some(group) = request_header.group.filter(|group| !group.is_empty()) else {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::InvalidParameter,
                "group is blank.",
            )));
        };
        let Some(client_id) = request_header.client_id.filter(|client_id| !client_id.is_empty()) else {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::InvalidParameter,
                "clientId is blank.",
            )));
        };
        if let Err((code, remark)) = self.validate_lite_parent_topic(&parent_topic) {
            return Ok(Some(self.response_with_code(request, code, remark)));
        }
        if let Err((code, remark)) = self.validate_consumer_group(&group, &parent_topic) {
            return Ok(Some(self.response_with_code(request, code, remark)));
        }

        let Some(subscription) =
            self.context
                .lite_subscription_registry
                .lite_subscription(&client_id, &group, &parent_topic)
        else {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::QueryNotFound,
                format!(
                    "Client [{}] has no lite subscription on [{}]-[{}].",
                    client_id, group, parent_topic
                ),
            )));
        };

        let lite_topic_set = self.decode_lite_topic_set(subscription.lite_topic_set(), request_header.max_count);
        let last_access_time = self
            .context
            .lite_event_dispatcher
            .get_client_last_access_time(&client_id)
            .max(subscription.update_time().max(0) as u64);
        let mut body = GetLiteClientInfoResponseBody::new();
        body.with_parent_topic(parent_topic)
            .with_group(group)
            .with_client_id(client_id)
            .with_last_access_time(last_access_time)
            .with_last_consume_time(0)
            .with_lite_topic_count(lite_topic_set.len() as u32)
            .with_lite_topic_set(lite_topic_set);

        Ok(Some(self.response_with_body(request, &body)?))
    }

    fn get_lite_group_info(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetLiteGroupInfoRequestHeader>()?;
        let group = request_header.group;
        let lite_topic = request_header.lite_topic;
        let top_k = request_header.top_k;
        let bind_topic = match self.validate_lite_group(&group) {
            Ok(bind_topic) => bind_topic,
            Err((code, remark)) => return Ok(Some(self.response_with_code(request, code, remark))),
        };

        let mut body = GetLiteGroupInfoResponseBody::new();
        body.with_group(group.clone())
            .with_parent_topic(bind_topic.clone())
            .with_lite_topic(lite_topic.clone());

        if lite_topic.is_empty() {
            let (lag_count_top_k, total_lag_count) =
                LiteConsumerLagCalculator::get_lag_count_top_k(&self.context, &group, top_k);
            let (lag_timestamp_top_k, earliest_unconsumed_timestamp) =
                LiteConsumerLagCalculator::get_lag_timestamp_top_k(&self.context, &group, &bind_topic, top_k);

            body.with_total_lag_count(total_lag_count)
                .with_earliest_unconsumed_timestamp(earliest_unconsumed_timestamp)
                .with_lag_count_top_k(lag_count_top_k)
                .with_lag_timestamp_top_k(lag_timestamp_top_k);
        } else {
            let Some(lmq_name) = to_lmq_name(bind_topic.as_str(), lite_topic.as_str()) else {
                return Ok(Some(self.response_with_code(
                    request,
                    ResponseCode::InvalidParameter,
                    "liteTopic is blank.",
                )));
            };
            let lmq_name = CheetahString::from_string(lmq_name);
            let broker_offset = self.lmq_broker_offset(&lmq_name);
            if broker_offset > 0 {
                let commit_offset = self.context.consumer_offset.query_offset(&group, &lmq_name);
                if commit_offset >= 0 {
                    let mut offset_wrapper = OffsetWrapper::new();
                    offset_wrapper.set_broker_offset(broker_offset);
                    offset_wrapper.set_consumer_offset(commit_offset);
                    offset_wrapper.set_last_timestamp(self.last_consumed_timestamp(&lmq_name, commit_offset));

                    body.with_total_lag_count((broker_offset - commit_offset).max(0))
                        .with_earliest_unconsumed_timestamp(
                            self.earliest_unconsumed_timestamp(&lmq_name, commit_offset),
                        )
                        .with_lite_topic_offset_wrapper(offset_wrapper);
                }
            } else {
                body.with_total_lag_count(-1).with_earliest_unconsumed_timestamp(-1);
            }
        }

        Ok(Some(self.response_with_body(request, &body)?))
    }

    fn trigger_lite_dispatch(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let TriggerLiteDispatchRequestHeader { group, client_id } =
            request.decode_command_custom_header::<TriggerLiteDispatchRequestHeader>()?;
        let bind_topic = match self.validate_lite_group(&group) {
            Ok(bind_topic) => bind_topic,
            Err((code, remark)) => return Ok(Some(self.response_with_code(request, code, remark))),
        };

        let dispatcher = &self.context.lite_event_dispatcher;
        let (max_event_count, dispatch_delay_millis) = self.lite_dispatch_policy(&group);
        if let Some(client_id) = client_id.filter(|client_id| !client_id.is_empty()) {
            let lmq_names = self.dispatchable_lmq_for_client(&client_id, &group, &bind_topic);
            if !lmq_names.is_empty() {
                dispatcher.do_full_dispatch_with_limit(
                    &client_id,
                    &group,
                    &lmq_names,
                    max_event_count,
                    dispatch_delay_millis,
                );
            }
        } else {
            let dispatch_map = self.dispatchable_lmq_by_group(&group, &bind_topic);
            dispatcher.do_full_dispatch_by_group_with_limit(
                &group,
                &dispatch_map,
                max_event_count,
                dispatch_delay_millis,
            );
        }

        Ok(Some(
            RemotingCommand::create_response_command().set_opaque(request.opaque()),
        ))
    }

    fn unique_lmq_count(&self, subscriptions: &[LiteSubscriptionRecord]) -> i32 {
        subscriptions
            .iter()
            .fold(
                HashMap::<CheetahString, HashSet<CheetahString>>::new(),
                |mut acc, subscription| {
                    acc.entry(subscription.topic.clone())
                        .or_default()
                        .extend(subscription.lite_topic_set.iter().cloned());
                    acc
                },
            )
            .into_values()
            .map(|lite_topics| lite_topics.len() as i32)
            .sum()
    }

    fn build_lite_topic_meta(&self) -> HashMap<CheetahString, i32> {
        self.context
            .topic_config_manager
            .topic_config_table()
            .iter()
            .filter_map(|entry| {
                (entry.value().get_topic_message_type() == TopicMessageType::Lite)
                    .then(|| (entry.key().clone(), entry.value().get_lite_topic_expiration()))
            })
            .collect()
    }

    fn build_lite_group_meta(&self) -> HashMap<CheetahString, HashSet<CheetahString>> {
        self.context
            .subscription_group_manager
            .subscription_group_table()
            .iter()
            .filter_map(|entry| {
                entry
                    .value()
                    .lite_bind_topic()
                    .map(|bind_topic| (bind_topic.clone(), entry.key().clone()))
            })
            .fold(
                HashMap::<CheetahString, HashSet<CheetahString>>::new(),
                |mut acc, (topic, group)| {
                    acc.entry(topic).or_default().insert(group);
                    acc
                },
            )
    }

    fn lite_bound_groups(&self, parent_topic: &CheetahString) -> HashSet<CheetahString> {
        self.context
            .subscription_group_manager
            .subscription_group_table()
            .iter()
            .filter_map(|entry| (entry.value().lite_bind_topic() == Some(parent_topic)).then(|| entry.key().clone()))
            .collect()
    }

    fn dispatchable_lmq_for_client(
        &self,
        client_id: &CheetahString,
        group: &CheetahString,
        parent_topic: &CheetahString,
    ) -> HashSet<CheetahString> {
        self.context
            .lite_subscription_registry
            .all_subscriptions()
            .into_iter()
            .filter(|subscription| {
                subscription.client_id == *client_id
                    && subscription.group == *group
                    && subscription.topic == *parent_topic
            })
            .flat_map(|subscription| subscription.lite_topic_set.into_iter())
            .filter(|lmq_name| self.has_dispatchable_messages(group, lmq_name))
            .collect()
    }

    fn dispatchable_lmq_by_group(
        &self,
        group: &CheetahString,
        parent_topic: &CheetahString,
    ) -> HashMap<CheetahString, HashSet<CheetahString>> {
        self.context
            .lite_subscription_registry
            .all_subscriptions()
            .into_iter()
            .filter(|subscription| subscription.group == *group && subscription.topic == *parent_topic)
            .fold(HashMap::new(), |mut acc, subscription| {
                let lmq_names = subscription
                    .lite_topic_set
                    .into_iter()
                    .filter(|lmq_name| self.has_dispatchable_messages(group, lmq_name))
                    .collect::<HashSet<_>>();
                if !lmq_names.is_empty() {
                    acc.entry(subscription.client_id).or_default().extend(lmq_names);
                }
                acc
            })
    }

    fn has_dispatchable_messages(&self, group: &CheetahString, lmq_name: &CheetahString) -> bool {
        if !self
            .context
            .message_store
            .is_lmq_exist(&self.context.lite_lifecycle_manager, lmq_name)
        {
            return false;
        }

        let broker_offset = self
            .context
            .message_store
            .max_offset(&self.context.lite_lifecycle_manager, lmq_name);
        broker_offset > 0 && self.context.consumer_offset.query_offset(group, lmq_name) < broker_offset
    }

    fn queue_store_stats(&self) -> (i32, i32) {
        self.context.message_store.queue_store_stats()
    }

    fn topic_offset_for_lmq(&self, lmq_name: &CheetahString) -> Option<TopicOffset> {
        self.context.message_store.topic_offset(lmq_name)
    }

    fn lmq_broker_offset(&self, lmq_name: &CheetahString) -> i64 {
        self.context
            .message_store
            .max_offset(&self.context.lite_lifecycle_manager, lmq_name)
    }

    fn earliest_unconsumed_timestamp(&self, lmq_name: &CheetahString, commit_offset: i64) -> i64 {
        if commit_offset < 0 {
            return 0;
        }

        self.context
            .message_store
            .message_store_timestamp(lmq_name, commit_offset)
            .max(0)
    }

    fn last_consumed_timestamp(&self, lmq_name: &CheetahString, commit_offset: i64) -> i64 {
        if commit_offset <= 0 {
            return 0;
        }

        self.context
            .message_store
            .message_store_timestamp(lmq_name, commit_offset - 1)
            .max(0)
    }

    fn decode_lite_topic_set(&self, lmq_name_set: &HashSet<CheetahString>, max_count: i32) -> HashSet<CheetahString> {
        let mut lite_topics = lmq_name_set
            .iter()
            .filter_map(|lmq_name| get_lite_topic(lmq_name.as_str()))
            .collect::<BTreeSet<_>>();
        if max_count > 0 && lite_topics.len() > max_count as usize {
            lite_topics = lite_topics.into_iter().take(max_count as usize).collect();
        }
        lite_topics.into_iter().map(CheetahString::from_string).collect()
    }

    fn lite_dispatch_policy(&self, group: &CheetahString) -> (usize, u64) {
        let max_event_count = self
            .context
            .subscription_group_manager
            .find_subscription_group_config(group)
            .map(|config| config.max_client_event_count())
            .filter(|count| *count > 0)
            .unwrap_or(self.context.policy.max_client_event_count)
            .max(1) as usize;
        (max_event_count, self.context.policy.dispatch_delay_millis)
    }

    fn validate_consumer_group(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
    ) -> Result<
        std::sync::Arc<rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig>,
        (ResponseCode, CheetahString),
    > {
        let group_config = self
            .context
            .subscription_group_manager
            .subscription_group_table()
            .get(group)
            .map(|entry| std::sync::Arc::clone(entry.value()))
            .ok_or_else(|| {
                (
                    ResponseCode::SubscriptionGroupNotExist,
                    CheetahString::from_string(format!("Group [{}] not exist.", group)),
                )
            })?;

        if !group_config.consume_enable() {
            return Err((
                ResponseCode::IllegalOperation,
                CheetahString::from_static_str("Consumer group is not allowed to consume."),
            ));
        }

        match group_config.lite_bind_topic() {
            Some(bind_topic) if bind_topic == topic => Ok(group_config),
            _ => Err((
                ResponseCode::InvalidParameter,
                CheetahString::from_string(format!("Subscription [{}]-[{}] not match.", group, topic)),
            )),
        }
    }

    fn validate_lite_parent_topic(
        &self,
        topic: &CheetahString,
    ) -> Result<Arc<TopicConfig>, (ResponseCode, CheetahString)> {
        let topic_config = self
            .context
            .topic_config_manager
            .select_topic_config(topic)
            .ok_or_else(|| {
                (
                    ResponseCode::TopicNotExist,
                    CheetahString::from_string(format!("Topic [{}] not exist.", topic)),
                )
            })?;

        if topic_config.get_topic_message_type() != TopicMessageType::Lite {
            return Err((
                ResponseCode::InvalidParameter,
                CheetahString::from_string(format!("Topic [{}] type not match.", topic)),
            ));
        }

        Ok(topic_config)
    }

    fn validate_lite_group(&self, group: &CheetahString) -> Result<CheetahString, (ResponseCode, CheetahString)> {
        let group_config = self
            .context
            .subscription_group_manager
            .subscription_group_table()
            .get(group)
            .map(|entry| std::sync::Arc::clone(entry.value()))
            .ok_or_else(|| {
                (
                    ResponseCode::SubscriptionGroupNotExist,
                    CheetahString::from_string(format!("Group [{}] not exist.", group)),
                )
            })?;

        match group_config.lite_bind_topic() {
            Some(bind_topic) if !bind_topic.is_empty() => Ok(bind_topic.clone()),
            _ => Err((
                ResponseCode::InvalidParameter,
                CheetahString::from_string(format!("Group [{}] is not a LITE group.", group)),
            )),
        }
    }

    fn response_with_body<T: RemotingSerializable>(
        &self,
        request: &RemotingCommand,
        body: &T,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        Ok(
            RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                .set_body(body.encode()?)
                .set_opaque(request.opaque()),
        )
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::LiteManagerOffsetCapability;
    use super::LiteManagerPolicy;
    use super::LiteManagerStoreCapability;
    use crate::broker_runtime::BrokerRuntime;
    use crate::lite::lite_lifecycle_manager::LiteLifecycleManager;

    #[test]
    fn lite_manager_policy_captures_only_required_startup_values() {
        let broker_config = BrokerConfig {
            max_client_event_count: 17,
            lite_event_full_dispatch_delay_time: 29,
            ..Default::default()
        };
        let message_store_config = MessageStoreConfig {
            max_lmq_consume_queue_num: 23,
            ..Default::default()
        };

        let policy = LiteManagerPolicy::from_configs(&broker_config, &message_store_config);

        assert_eq!(policy.store_type, message_store_config.store_type.get_store_type());
        assert_eq!(policy.max_lmq_num, 23);
        assert_eq!(policy.broker_name, *broker_config.broker_name());
        assert_eq!(policy.max_client_event_count, 17);
        assert_eq!(policy.dispatch_delay_millis, 29);
    }

    #[test]
    fn lite_manager_source_uses_only_explicit_capabilities() {
        let source = include_str!("lite_manager_processor.rs");
        let sharding_source = include_str!("../lite/lite_sharding.rs");
        let lag_source = include_str!("../lite/lite_consumer_lag_calculator.rs");

        assert!(!source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(!source.contains(concat!("broker_runtime", "_inner")));
        assert!(source.contains("context: LiteManagerContext<MS>"));
        assert!(source.contains("consumer_offset: LiteManagerOffsetCapability<MS>"));
        assert!(source.contains("message_store: LiteManagerStoreCapability<MS>"));
        assert!(!sharding_source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(!lag_source.contains(concat!("BrokerRuntime", "Inner")));
    }

    #[test]
    fn lite_manager_weak_providers_do_not_keep_runtime_or_store_alive() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let inner = runtime.inner_for_test();
        let offset_manager = inner.consumer_offset_manager_handle();
        let offset = LiteManagerOffsetCapability::new(&offset_manager);
        let escape_bridge = inner.escape_bridge();
        let store = LiteManagerStoreCapability::new(&escape_bridge);
        let group = CheetahString::from_static_str("group");
        let topic = CheetahString::from_static_str("topic");

        assert!(store.is_available());
        drop(offset_manager);
        drop(escape_bridge);
        drop(runtime);

        assert!(!store.is_available());
        assert_eq!(offset.query_offset(&group, &topic), -1);
        assert!(offset.offset_table_snapshot().is_empty());
        assert_eq!(offset.offset_table_len(), 0);
        assert_eq!(store.queue_store_stats(), (0, 0));
        assert!(store.topic_offset(&topic).is_none());
        assert_eq!(store.max_offset(&LiteLifecycleManager, &topic), 0);
        assert!(!store.is_lmq_exist(&LiteLifecycleManager, &topic));
        assert_eq!(store.message_store_timestamp(&topic, 0), 0);
    }
}
