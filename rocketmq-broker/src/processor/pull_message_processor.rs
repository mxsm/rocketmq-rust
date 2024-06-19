/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::Arc;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::forbidden_type::ForbiddenType;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::request_source::RequestSource;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use rocketmq_store::log_file::MessageStore;
use tracing::error;

use crate::client::manager::consumer_manager::ConsumerManager;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::mqtrace::consume_message_hook::ConsumeMessageHook;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

#[derive(Clone)]
pub struct PullMessageProcessor<MS> {
    consume_message_hook_vec: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
    pull_message_result_handler: Arc<dyn PullMessageResultHandler>,
    broker_config: Arc<BrokerConfig>,
    subscription_group_manager: Arc<SubscriptionGroupManager<MS>>,
    topic_config_manager: Arc<TopicConfigManager>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    consumer_manager: Arc<ConsumerManager>,
    consumer_filter_manager: Arc<ConsumerFilterManager>,
}

impl<MS> Default for PullMessageProcessor<MS> {
    fn default() -> Self {
        todo!()
    }
}

#[allow(unused_variables)]
impl<MS> PullMessageProcessor<MS>
where
    MS: MessageStore,
{
    pub async fn process_request(
        &mut self,
        ctx: ConnectionHandlerContext<'_>,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        self.process_request_inner(request_code, ctx, request, true, true)
            .await
    }

    async fn process_request_inner(
        &mut self,
        request_code: RequestCode,
        ctx: ConnectionHandlerContext<'_>,
        request: RemotingCommand,
        _broker_allow_suspend: bool,
        _broker_allow_flow_ctr_suspend: bool,
    ) -> Option<RemotingCommand> {
        let _begin_time_mills = get_current_millis();
        let mut response = RemotingCommand::create_response_command();
        response.set_opaque_mut(request.opaque());
        let request_header = request
            .decode_command_custom_header_fast::<PullMessageRequestHeader>()
            .unwrap();
        let mut response_header = PullMessageResponseHeader::default();
        if !PermName::is_readable(self.broker_config.broker_permission) {
            response_header.forbidden_type = Some(ForbiddenType::BROKER_FORBIDDEN);
            return Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(Some(format!(
                        "the broker[{}] pulling message is forbidden",
                        self.broker_config.broker_ip1
                    ))),
            );
        }
        if RequestCode::LitePullMessage == request_code
            && !self.broker_config.lite_pull_message_enable
        {
            response_header.forbidden_type = Some(ForbiddenType::BROKER_FORBIDDEN);
            return Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(Some(format!(
                        "the broker[{}] pulling message is forbidden",
                        self.broker_config.broker_ip1
                    ))),
            );
        }
        let subscription_group_config = self
            .subscription_group_manager
            .find_subscription_group_config(request_header.consumer_group.as_str());

        if subscription_group_config.is_none() {
            return Some(
                response
                    .set_code(ResponseCode::SubscriptionGroupNotExist)
                    .set_remark(Some(format!(
                        "subscription group [{}] does not exist, {}",
                        request_header.consumer_group,
                        FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                    ))),
            );
        }

        if !subscription_group_config.as_ref().unwrap().consume_enable() {
            response_header.forbidden_type = Some(ForbiddenType::GROUP_FORBIDDEN);
            return Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark(Some(format!(
                        "subscription group no permission, {}",
                        request_header.consumer_group,
                    ))),
            );
        }
        let topic_config = self
            .topic_config_manager
            .select_topic_config(request_header.topic.as_str());
        if topic_config.is_none() {
            error!(
                "the topic {} not exist, consumer: {}",
                request_header.topic,
                ctx.as_ref().remoting_address()
            );
            return Some(
                response
                    .set_code(ResponseCode::TopicNotExist)
                    .set_remark(Some(format!(
                        "topic[{}] not exist, apply first please! {}",
                        request_header.topic,
                        FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
                    ))),
            );
        }
        if !PermName::is_readable(topic_config.as_ref().unwrap().perm) {
            response_header.forbidden_type = Some(ForbiddenType::TOPIC_FORBIDDEN);
            return Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark(Some(format!(
                        "the topic[{}] pulling message is forbidden",
                        request_header.topic,
                    ))),
            );
        }
        let topic_queue_mapping_context = self
            .topic_queue_mapping_manager
            .build_topic_queue_mapping_context(&request_header, false);
        if let Some(resp) =
            self.rewrite_request_for_static_topic(&request_header, &topic_queue_mapping_context)
        {
            return Some(resp);
        }
        if request_header.queue_id.is_none()
            || request_header.queue_id.unwrap() < 0
            || request_header.queue_id.unwrap()
                > topic_config.as_ref().unwrap().read_queue_nums as i32
        {
            return Some(
                response
                    .set_code(RemotingSysResponseCode::SystemError)
                    .set_remark(Some(format!(
                        "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] \
                         consumer:[{}]",
                        request_header.queue_id.unwrap(),
                        request_header.topic,
                        topic_config.as_ref().unwrap().read_queue_nums,
                        ctx.as_ref().remoting_address()
                    ))),
            );
        }
        match RequestSource::parse_integer(request_header.request_source) {
            RequestSource::ProxyForBroadcast => {}
            RequestSource::ProxyForStream => {}
            _ => {}
        }
        let has_subscription_flag =
            PullSysFlag::has_subscription_flag(request_header.sys_flag as u32);
        let (subscription_data, consumer_filter_data) = if has_subscription_flag {
            let subscription_data = FilterAPI::build(
                request_header.topic.as_str(),
                request_header
                    .subscription
                    .as_ref()
                    .map_or_else(|| "", |value| value.as_str()),
                request_header.expression_type.clone(),
            );
            if subscription_data.is_err() {
                return Some(
                    response
                        .set_code(ResponseCode::SubscriptionParseFailed)
                        .set_remark(Some(String::from(
                            "parse the consumer's subscription failed",
                        ))),
                );
            }
            let subscription_data = subscription_data.unwrap();
            self.consumer_manager.compensate_subscribe_data(
                request_header.consumer_group.as_str(),
                request_header.topic.as_str(),
                &subscription_data,
            );
            let consumer_filter_data =
                if !ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
                    let consumer_filter_data = ConsumerFilterManager::build(
                        request_header.topic.as_str(),
                        request_header.consumer_group.as_str(),
                        request_header
                            .subscription
                            .as_ref()
                            .map_or_else(|| "", |value| value.as_str()),
                        request_header
                            .expression_type
                            .as_ref()
                            .map_or_else(|| "", |value| value.as_str()),
                        request_header.sub_version as u64,
                    );
                    if consumer_filter_data.is_none() {
                        return Some(
                            response
                                .set_code(ResponseCode::SubscriptionParseFailed)
                                .set_remark(Some(String::from(
                                    "parse the consumer's subscription failed",
                                ))),
                        );
                    }
                    consumer_filter_data
                } else {
                    None
                };
            (Some(subscription_data), consumer_filter_data)
        } else {
            unimplemented!()
        };
        Some(response)
    }

    fn rewrite_request_for_static_topic(
        &mut self,
        _request_header: &PullMessageRequestHeader,
        _mapping_context: &TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        unimplemented!()
    }
}
