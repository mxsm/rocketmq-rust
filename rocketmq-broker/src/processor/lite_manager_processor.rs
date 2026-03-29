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

use cheetah_string::CheetahString;
use rocketmq_common::common::entity::ClientGroup;
use rocketmq_common::common::lite::get_lite_topic;
use rocketmq_common::common::lite::to_lmq_name;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::admin::topic_offset::TopicOffset;
use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::queue::consume_queue_store::ConsumeQueueStoreTrait;
use rocketmq_store::queue::local_file_consume_queue_store::ConsumeQueueStore;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::subscription::lite_subscription_registry::LiteSubscriptionRecord;

pub(crate) struct LiteManagerProcessor<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> LiteManagerProcessor<MS> {
    pub(crate) fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

impl<MS: MessageStore> RequestProcessor for LiteManagerProcessor<MS> {
    async fn process_request(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match RequestCode::from(request.code()) {
            RequestCode::GetBrokerLiteInfo => self.get_broker_lite_info(request),
            RequestCode::GetParentTopicInfo => self.get_parent_topic_info(request),
            RequestCode::GetLiteTopicInfo => self.get_lite_topic_info(request),
            RequestCode::GetLiteClientInfo => self.get_lite_client_info(request),
            request_code => {
                warn!("LiteManagerProcessor received unknown request code: {:?}", request_code);
                Ok(Some(self.response_with_code(
                    request,
                    ResponseCode::RequestCodeNotSupported,
                    format!("LiteManagerProcessor request code {} not supported", request.code()),
                )))
            }
        }
    }
}

impl<MS: MessageStore> LiteManagerProcessor<MS> {
    fn get_broker_lite_info(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let subscriptions = self
            .broker_runtime_inner
            .lite_subscription_registry()
            .all_subscriptions();
        let topic_meta = self.build_lite_topic_meta(&subscriptions);
        let group_meta = self.build_lite_group_meta(&subscriptions);
        let unique_lmq_count = topic_meta.values().copied().sum::<i32>();
        let (store_lmq_num, cq_table_size) = self.queue_store_stats();

        let mut body = GetBrokerLiteInfoResponseBody::new();
        body.set_store_type(CheetahString::from_static_str(
            self.broker_runtime_inner
                .message_store_config()
                .store_type
                .get_store_type(),
        ));
        body.set_max_lmq_num(
            self.broker_runtime_inner
                .message_store_config()
                .max_lmq_consume_queue_num as i32,
        );
        body.set_current_lmq_num(store_lmq_num.max(unique_lmq_count));
        body.set_lite_subscription_count(
            self.broker_runtime_inner
                .lite_subscription_registry()
                .active_subscription_num() as i32,
        );
        body.set_order_info_count(0);
        body.set_cq_table_size(cq_table_size);
        body.set_offset_table_size(
            self.broker_runtime_inner
                .consumer_offset_manager()
                .offset_table()
                .read()
                .len() as i32,
        );
        body.set_event_map_size(0);
        body.set_topic_meta(topic_meta);
        body.set_group_meta(group_meta);

        Ok(Some(self.response_with_body(request, &body)?))
    }

    fn get_parent_topic_info(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetParentTopicInfoRequestHeader>()?;
        if !self
            .broker_runtime_inner
            .topic_config_manager()
            .contains_topic(&request_header.topic)
        {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::TopicNotExist,
                format!("Topic [{}] not exist.", request_header.topic),
            )));
        }

        let subscriptions = self
            .broker_runtime_inner
            .lite_subscription_registry()
            .all_subscriptions()
            .into_iter()
            .filter(|subscription| subscription.topic == request_header.topic)
            .collect::<Vec<_>>();
        if subscriptions.is_empty() {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::QueryNotFound,
                format!("Topic [{}] has no lite subscriptions.", request_header.topic),
            )));
        }

        let groups = subscriptions
            .iter()
            .map(|subscription| subscription.group.clone())
            .collect::<HashSet<_>>();
        let lite_topic_set = subscriptions
            .iter()
            .flat_map(|subscription| subscription.lite_topic_set.iter())
            .filter_map(|lmq_name| get_lite_topic(lmq_name.as_str()).map(CheetahString::from_string))
            .collect::<HashSet<_>>();

        let mut body = GetParentTopicInfoResponseBody::new();
        body.set_topic(request_header.topic);
        body.set_ttl(0);
        body.set_groups(groups);
        body.set_lmq_num(lite_topic_set.len() as i32);
        body.set_lite_topic_count(lite_topic_set.len() as i32);

        Ok(Some(self.response_with_body(request, &body)?))
    }

    fn get_lite_topic_info(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetLiteTopicInfoRequestHeader>()?;
        if !self
            .broker_runtime_inner
            .topic_config_manager()
            .contains_topic(&request_header.parent_topic)
        {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::TopicNotExist,
                format!("Topic [{}] not exist.", request_header.parent_topic),
            )));
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
            .broker_runtime_inner
            .lite_subscription_registry()
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

        let mut body = GetLiteTopicInfoResponseBody::new();
        body.with_parent_topic(request_header.parent_topic)
            .with_lite_topic(request_header.lite_topic)
            .with_subscriber(subscribers)
            .with_sharding_to_broker(false);
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
        if let Err((code, remark)) = self.validate_consumer_group(&group, &parent_topic) {
            return Ok(Some(self.response_with_code(request, code, remark)));
        }

        let Some(subscription) = self
            .broker_runtime_inner
            .lite_subscription_registry()
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
        let mut body = GetLiteClientInfoResponseBody::new();
        body.with_parent_topic(parent_topic)
            .with_group(group)
            .with_client_id(client_id)
            .with_last_access_time(subscription.update_time().max(0) as u64)
            .with_last_consume_time(0)
            .with_lite_topic_count(lite_topic_set.len() as u32)
            .with_lite_topic_set(lite_topic_set);

        Ok(Some(self.response_with_body(request, &body)?))
    }

    fn build_lite_topic_meta(&self, subscriptions: &[LiteSubscriptionRecord]) -> HashMap<CheetahString, i32> {
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
            .into_iter()
            .map(|(topic, lite_topics)| (topic, lite_topics.len() as i32))
            .collect()
    }

    fn build_lite_group_meta(
        &self,
        subscriptions: &[LiteSubscriptionRecord],
    ) -> HashMap<CheetahString, HashSet<CheetahString>> {
        subscriptions.iter().fold(
            HashMap::<CheetahString, HashSet<CheetahString>>::new(),
            |mut acc, subscription| {
                acc.entry(subscription.topic.clone())
                    .or_default()
                    .insert(subscription.group.clone());
                acc
            },
        )
    }

    fn queue_store_stats(&self) -> (i32, i32) {
        let Some(message_store) = self.broker_runtime_inner.message_store() else {
            return (0, 0);
        };
        let Some(queue_store) = message_store.get_queue_store().downcast_ref::<ConsumeQueueStore>() else {
            return (0, 0);
        };

        let consume_queue_table = queue_store.get_consume_queue_table();
        let cq_table_size = consume_queue_table
            .lock()
            .values()
            .map(|queue_map| queue_map.len() as i32)
            .sum();
        (queue_store.get_lmq_num(), cq_table_size)
    }

    fn topic_offset_for_lmq(&self, lmq_name: &CheetahString) -> Option<TopicOffset> {
        let message_store = self.broker_runtime_inner.message_store()?;
        let min_offset = message_store.get_min_offset_in_queue(lmq_name, 0);
        let max_offset = message_store.get_max_offset_in_queue(lmq_name, 0);
        let last_update_timestamp = if max_offset > 0 {
            message_store.get_message_store_timestamp(lmq_name, 0, max_offset - 1)
        } else {
            -1
        };

        let mut topic_offset = TopicOffset::new();
        topic_offset.set_min_offset(min_offset);
        topic_offset.set_max_offset(max_offset);
        topic_offset.set_last_update_timestamp(last_update_timestamp);
        Some(topic_offset)
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

    fn validate_consumer_group(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
    ) -> Result<
        std::sync::Arc<rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig>,
        (ResponseCode, CheetahString),
    > {
        let group_config = self
            .broker_runtime_inner
            .subscription_group_manager()
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
