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
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::forbidden_type::ForbiddenType;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::request_source::RequestSource;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::rpc::rpc_client_utils::RpcClientUtils;
use rocketmq_remoting::rpc::rpc_response::RpcResponse;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::filter::MessageFilter;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::log_file::MAX_PULL_MSG_SIZE;
use tracing::error;
use tracing::warn;

use crate::client::consumer_group_info::ConsumerGroupInfo;
use crate::client::manager::consumer_manager::ConsumerManager;
use crate::filter::expression_for_retry_message_filter::ExpressionForRetryMessageFilter;
use crate::filter::expression_message_filter::ExpressionMessageFilter;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::offset::manager::broadcast_offset_manager::BroadcastOffsetManager;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

#[derive(Clone)]
pub struct PullMessageProcessor<MS> {
    pull_message_result_handler: Arc<dyn PullMessageResultHandler>,
    broker_config: Arc<BrokerConfig>,
    subscription_group_manager: Arc<SubscriptionGroupManager<MS>>,
    topic_config_manager: Arc<TopicConfigManager>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    consumer_manager: Arc<ConsumerManager>,
    consumer_filter_manager: Arc<ConsumerFilterManager>,
    consumer_offset_manager: Arc<ConsumerOffsetManager>,
    broadcast_offset_manager: Arc<BroadcastOffsetManager>,
    message_store: MS,
}

impl<MS> PullMessageProcessor<MS> {
    pub fn new(
        pull_message_result_handler: Arc<dyn PullMessageResultHandler>,
        broker_config: Arc<BrokerConfig>,
        subscription_group_manager: Arc<SubscriptionGroupManager<MS>>,
        topic_config_manager: Arc<TopicConfigManager>,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        consumer_manager: Arc<ConsumerManager>,
        consumer_filter_manager: Arc<ConsumerFilterManager>,
        consumer_offset_manager: Arc<ConsumerOffsetManager>,
        broadcast_offset_manager: Arc<BroadcastOffsetManager>,
        message_store: MS,
    ) -> Self {
        Self {
            pull_message_result_handler,
            broker_config,
            subscription_group_manager,
            topic_config_manager,
            topic_queue_mapping_manager,
            consumer_manager,
            consumer_filter_manager,
            consumer_offset_manager,
            broadcast_offset_manager,
            message_store,
        }
    }

    pub fn rewrite_request_for_static_topic(
        request_header: &mut PullMessageRequestHeader,
        mapping_context: &mut TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        mapping_context.mapping_detail.as_ref()?;
        let mapping_detail = mapping_context.mapping_detail.as_ref().unwrap();
        let topic = mapping_context.topic.as_str();
        let global_id = mapping_context.global_id;
        if !mapping_context.is_leader() {
            return Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NotLeaderForQueue,
                format!(
                    "{}-{} cannot find mapping item in request process of current broker {}",
                    topic,
                    global_id.unwrap_or_default(),
                    mapping_detail
                        .topic_queue_mapping_info
                        .bname
                        .clone()
                        .unwrap_or_default()
                ),
            ));
        }

        let global_offset = request_header.queue_offset;
        let mapping_item = TopicQueueMappingUtils::find_logic_queue_mapping_item(
            &mapping_context.mapping_item_list,
            global_offset,
            true,
        )?;
        mapping_context.current_item = Some(mapping_item.clone());

        if global_offset < mapping_item.logic_offset {
            // Handle offset moved...
        }

        let bname = &mapping_item.bname;
        let phy_queue_id = mapping_item.queue_id;
        let phy_queue_offset = mapping_item.compute_physical_queue_offset(global_offset);
        request_header.queue_id = Some(phy_queue_id);
        request_header.queue_offset = phy_queue_offset;
        if mapping_item.check_if_end_offset_decided()
        /* && request_header.max_msg_nums.is_some() */
        {
            request_header.max_msg_nums = std::cmp::min(
                (mapping_item.end_offset - mapping_item.start_offset) as i32,
                request_header.max_msg_nums,
            );
        }

        if &mapping_detail.topic_queue_mapping_info.bname == bname {
            return None;
        }

        let mut sys_flag = request_header.sys_flag;
        let topic_request = request_header.topic_request.as_mut().unwrap();
        topic_request.lo = Some(false);
        topic_request
            .rpc
            .as_mut()
            .unwrap()
            .broker_name
            .clone_from(bname);
        sys_flag = PullSysFlag::clear_suspend_flag(sys_flag as u32) as i32;
        sys_flag = PullSysFlag::clear_commit_offset_flag(sys_flag as u32) as i32;
        request_header.sys_flag = sys_flag;
        /* let rpc_request = RpcRequest::new(RequestCode::PullMessage, request_header.clone(), None);
        let rpc_response = broker_controller
            .broker_outer_api
            .rpc_client
            .invoke(rpc_request, broker_controller.broker_config.forward_timeout)?;
        if rpc_response.exception.is_some() {
            return Err(rpc_response.exception.unwrap());
        }*/

        let rpc_response = RpcResponse::default();
        let response_header = rpc_response.get_header_mut::<PullMessageResponseHeader>();
        let rewrite_result = rewrite_response_for_static_topic(
            request_header,
            response_header.unwrap(),
            mapping_context,
            ResponseCode::from(rpc_response.code),
        );
        if rewrite_result.is_some() {
            return rewrite_result;
        }
        Some(RpcClientUtils::create_command_for_rpc_response(
            rpc_response,
        ))
    }
}

pub fn rewrite_response_for_static_topic(
    request_header: &PullMessageRequestHeader,
    response_header: &mut PullMessageResponseHeader,
    mapping_context: &mut TopicQueueMappingContext,
    code: ResponseCode,
) -> Option<RemotingCommand> {
    mapping_context.mapping_detail.as_ref()?;
    let mapping_detail = mapping_context.mapping_detail.as_ref().unwrap();
    let leader_item = mapping_context.leader_item.as_ref().unwrap();
    let current_item = mapping_context.current_item.as_ref().unwrap();
    let mapping_items = &mut mapping_context.mapping_item_list;
    let earlist_item =
        TopicQueueMappingUtils::find_logic_queue_mapping_item(mapping_items, 0, true).unwrap();

    assert!(current_item.logic_offset >= 0);

    let request_offset = request_header.queue_offset;
    let mut next_begin_offset = response_header.next_begin_offset;
    let mut min_offset = response_header.min_offset;
    let mut max_offset = response_header.max_offset;
    let mut response_code = code;

    if code != ResponseCode::Success {
        let mut is_revised = false;
        if leader_item.gen == current_item.gen {
            if request_offset > max_offset.unwrap() {
                if code == ResponseCode::PullOffsetMoved {
                    response_code = ResponseCode::PullOffsetMoved;
                    next_begin_offset = max_offset;
                } else {
                    response_code = code;
                }
            } else if request_offset < min_offset.unwrap() {
                next_begin_offset = min_offset;
                response_code = ResponseCode::PullRetryImmediately;
            } else {
                response_code = code;
            }
        }

        if earlist_item.gen == current_item.gen {
            if request_offset < min_offset.unwrap() {
                /*if code == ResponseCode::PullOffsetMoved {
                    response_code = ResponseCode::PullOffsetMoved;
                    next_begin_offset = min_offset;
                } else {
                    response_code = ResponseCode::PullOffsetMoved;
                    next_begin_offset = min_offset;
                }*/
                response_code = ResponseCode::PullOffsetMoved;
                next_begin_offset = min_offset;
            } else if request_offset >= max_offset.unwrap() {
                if let Some(next_item) =
                    TopicQueueMappingUtils::find_next(mapping_items, Some(current_item), true)
                {
                    is_revised = true;
                    next_begin_offset = Some(next_item.start_offset);
                    min_offset = Some(next_item.start_offset);
                    max_offset = min_offset;
                    response_code = ResponseCode::PullRetryImmediately;
                } else {
                    response_code = ResponseCode::PullNotFound;
                }
            } else {
                response_code = code;
            }
        }

        if !is_revised
            && leader_item.gen != current_item.gen
            && earlist_item.gen != current_item.gen
        {
            if request_offset < min_offset? {
                next_begin_offset = min_offset;
                response_code = ResponseCode::PullRetryImmediately;
            } else if request_offset >= max_offset? {
                if let Some(next_item) =
                    TopicQueueMappingUtils::find_next(mapping_items, Some(current_item), true)
                {
                    next_begin_offset = Some(next_item.start_offset);
                    min_offset = Some(next_item.start_offset);
                    max_offset = min_offset;
                    response_code = ResponseCode::PullRetryImmediately;
                } else {
                    response_code = ResponseCode::PullNotFound;
                }
            } else {
                response_code = code;
            }
        }
    }

    if current_item.check_if_end_offset_decided()
        && next_begin_offset.unwrap() >= current_item.end_offset
    {
        next_begin_offset = Some(current_item.end_offset);
    }

    response_header.next_begin_offset =
        Some(current_item.compute_static_queue_offset_strictly(next_begin_offset.unwrap()));
    response_header.min_offset =
        Some(current_item.compute_static_queue_offset_strictly(
            min_offset.unwrap().max(current_item.start_offset),
        ));
    response_header.max_offset = Some(
        current_item
            .compute_static_queue_offset_strictly(max_offset.unwrap())
            .max(TopicQueueMappingDetail::compute_max_offset_from_mapping(
                mapping_detail,
                mapping_context.global_id,
            )),
    );
    response_header.offset_delta = Some(current_item.compute_offset_delta());

    if code != ResponseCode::Success {
        Some(
            RemotingCommand::create_response_command_with_header(response_header.clone())
                .set_code(response_code),
        )
    } else {
        None
    }
}

#[allow(unused_variables)]
impl<MS> PullMessageProcessor<MS>
where
    MS: MessageStore,
{
    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        self.process_request_inner(request_code, channel, ctx, request, true, true)
            .await
    }

    async fn process_request_inner(
        &mut self,
        request_code: RequestCode,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
        broker_allow_suspend: bool,
        broker_allow_flow_ctr_suspend: bool,
    ) -> Option<RemotingCommand> {
        let begin_time_mills = get_current_millis();
        let mut response = RemotingCommand::create_response_command();
        response.set_opaque_mut(request.opaque());
        let mut request_header = request
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
                channel.remote_address()
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
        let mut topic_queue_mapping_context = self
            .topic_queue_mapping_manager
            .build_topic_queue_mapping_context(&request_header, false);
        if let Some(resp) = Self::rewrite_request_for_static_topic(
            &mut request_header,
            &mut topic_queue_mapping_context,
        ) {
            return Some(resp);
        }
        if request_header.queue_id.is_none()
            || request_header.queue_id.unwrap() < 0
            || request_header.queue_id.unwrap()
                >= topic_config.as_ref().unwrap().read_queue_nums as i32
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
                        channel.remote_address()
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
            let consumer_group_info = self
                .consumer_manager
                .get_consumer_group_info(request_header.consumer_group.as_str());
            if consumer_group_info.is_none() {
                warn!(
                    "the consumer's group info not exist, group: {}",
                    request_header.consumer_group.as_str()
                );
                return Some(
                    response
                        .set_code(ResponseCode::SubscriptionNotExist)
                        .set_remark(Some(format!(
                            "the consumer's group info not exist {}",
                            FAQUrl::suggest_todo(FAQUrl::SAME_GROUP_DIFFERENT_TOPIC),
                        ))),
                );
            }
            let sgc_ref = subscription_group_config.as_ref().unwrap();
            if sgc_ref.consume_broadcast_enable()
                && consumer_group_info.as_ref().unwrap().get_message_model()
                    == MessageModel::Broadcasting
            {
                response_header.forbidden_type =
                    Some(ForbiddenType::BROADCASTING_DISABLE_FORBIDDEN);
                return Some(
                    response
                        .set_code(ResponseCode::NoPermission)
                        .set_command_custom_header(response_header)
                        .set_remark(Some(format!(
                            " the consumer group[{}] can not consume by broadcast way",
                            request_header.consumer_group.as_str(),
                        ))),
                );
            }

            let read_forbidden = self.subscription_group_manager.get_forbidden(
                sgc_ref.group_name(),
                request_header.topic.as_str(),
                PermName::INDEX_PERM_READ as i32,
            );
            if read_forbidden {
                response_header.forbidden_type = Some(ForbiddenType::SUBSCRIPTION_FORBIDDEN);
                return Some(
                    response
                        .set_code(ResponseCode::NoPermission)
                        .set_command_custom_header(response_header)
                        .set_remark(Some(format!(
                            "the consumer group[{}] is forbidden for topic[{}]",
                            request_header.consumer_group.as_str(),
                            request_header.topic
                        ))),
                );
            }
            let subscription_data = consumer_group_info
                .as_ref()
                .unwrap()
                .find_subscription_data(request_header.topic.as_str());
            if subscription_data.is_none() {
                warn!(
                    "the consumer's subscription not exist, group: {}, topic:{}",
                    request_header.consumer_group, request_header.topic
                );
                return Some(
                    response
                        .set_code(ResponseCode::SubscriptionNotExist)
                        .set_remark(Some(format!(
                            "the consumer's subscription not exist {}",
                            FAQUrl::suggest_todo(FAQUrl::SAME_GROUP_DIFFERENT_TOPIC),
                        ))),
                );
            }

            if subscription_data.as_ref().unwrap().sub_version < request_header.sub_version {
                warn!(
                    "The broker's subscription is not latest, group: {} {}",
                    request_header.consumer_group,
                    subscription_data.as_ref().unwrap().sub_string
                );
                return Some(
                    response
                        .set_code(ResponseCode::SubscriptionNotExist)
                        .set_remark(Some("the consumer's subscription not latest".to_string())),
                );
            }

            let consumer_filter_data = if !ExpressionType::is_tag_type(Some(
                subscription_data.as_ref().unwrap().expression_type.as_str(),
            )) {
                let consumer_filter_data = self.consumer_filter_manager.get_consumer_filter_data(
                    request_header.topic.as_str(),
                    request_header.consumer_group.as_str(),
                );
                if consumer_filter_data.is_none() {
                    return Some(
                        response
                            .set_code(ResponseCode::FilterDataNotExist)
                            .set_remark(Some(String::from(
                                "The broker's consumer filter data is not exist!Your expression \
                                 may be wrong!",
                            ))),
                    );
                }
                if consumer_filter_data.as_ref().unwrap().client_version()
                    < request_header.sub_version as u64
                {
                    warn!(
                        "The broker's consumer filter data is not latest, group: {}, topic: {}, \
                         serverV: {}, clientV: {}",
                        request_header.consumer_group,
                        request_header.topic,
                        consumer_filter_data.as_ref().unwrap().client_version(),
                        request_header.sub_version,
                    );
                    return Some(
                        response
                            .set_code(ResponseCode::FilterDataNotExist)
                            .set_remark(Some(String::from(
                                "the consumer's consumer filter data not latest",
                            ))),
                    );
                }
                consumer_filter_data
            } else {
                None
            };
            (subscription_data, consumer_filter_data)
        };

        let subscription_data = subscription_data.unwrap();
        if !ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str()))
            && !self.broker_config.enable_property_filter
        {
            return Some(
                response
                    .set_code(RemotingSysResponseCode::SystemError)
                    .set_remark(Some(format!(
                        "The broker does not support consumer to filter message by {}",
                        subscription_data.expression_type
                    ))),
            );
        }

        let message_filter: Box<dyn MessageFilter> = if self.broker_config.filter_support_retry {
            Box::new(ExpressionForRetryMessageFilter)
        } else {
            Box::new(ExpressionMessageFilter)
        };

        //ColdDataFlow not implement
        let use_reset_offset_feature = self.broker_config.use_server_side_reset_offset;
        let topic = request_header.topic.as_str();
        let group = request_header.consumer_group.as_str();
        let queue_id = request_header.queue_id.unwrap();
        let reset_offset = self
            .consumer_offset_manager
            .query_then_erase_reset_offset(topic, group, queue_id);
        let get_message_result = if use_reset_offset_feature && reset_offset.is_some() {
            let mut get_message_result = GetMessageResult::new();
            get_message_result.set_status(Some(GetMessageStatus::OffsetReset));
            get_message_result.set_next_begin_offset(reset_offset.unwrap());
            get_message_result
                .set_min_offset(self.message_store.get_min_offset_in_queue(topic, queue_id));
            get_message_result
                .set_max_offset(self.message_store.get_max_offset_in_queue(topic, queue_id));
            get_message_result.set_suggest_pulling_from_slave(false);
            Some(get_message_result)
        } else {
            let broadcast_init_offset = self.query_broadcast_pull_init_offset(
                topic,
                group,
                queue_id,
                &request_header,
                &channel,
            );
            if broadcast_init_offset >= 0 {
                let mut get_message_result = GetMessageResult::new();
                get_message_result.set_status(Some(GetMessageStatus::OffsetReset));
                get_message_result.set_next_begin_offset(broadcast_init_offset);
                Some(get_message_result)
            } else {
                self.message_store
                    .get_message(
                        group,
                        topic,
                        queue_id,
                        request_header.queue_offset,
                        request_header.max_msg_nums,
                        MAX_PULL_MSG_SIZE,
                        Some(message_filter.as_ref()),
                    )
                    .await
            }
        };
        if let Some(get_message_result) = get_message_result {
            return self.pull_message_result_handler.handle(
                get_message_result,
                request,
                request_header,
                channel,
                ctx,
                subscription_data,
                subscription_group_config.unwrap(),
                broker_allow_suspend,
                message_filter,
                response,
                topic_queue_mapping_context,
                begin_time_mills,
            );
        }
        None
    }

    /*    fn rewrite_request_for_static_topic(
        &mut self,
        _request_header: &PullMessageRequestHeader,
        _mapping_context: &TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        unimplemented!()
    }*/

    fn query_broadcast_pull_init_offset(
        &mut self,
        topic: &str,
        group: &str,
        queue_id: i32,
        request_header: &PullMessageRequestHeader,
        channel: &Channel,
    ) -> i64 {
        if !self.broker_config.enable_broadcast_offset_store {
            return -1;
        }
        let consumer_group_info = self.consumer_manager.get_consumer_group_info(group);
        let proxy_pull_broadcast = RequestSource::ProxyForBroadcast.get_value()
            == request_header.request_source.unwrap_or(-2);

        if is_broadcast(proxy_pull_broadcast, consumer_group_info.as_ref()) {
            let client_id = if proxy_pull_broadcast {
                request_header.proxy_forward_client_id.as_ref().cloned()
            } else {
                match consumer_group_info
                    .as_ref()
                    .unwrap()
                    .find_channel_by_channel(channel)
                {
                    None => {
                        return -1;
                    }
                    Some(value) => Some(value.client_id().clone()),
                }
            };
            return self.broadcast_offset_manager.query_init_offset(
                topic,
                group,
                queue_id,
                client_id.as_ref().unwrap().as_str(),
                request_header.queue_offset,
                proxy_pull_broadcast,
            );
        }

        -1
    }

    pub fn execute_request_when_wakeup(&self, channel: Channel, request: RemotingCommand) {}
}
pub(crate) fn is_broadcast(
    proxy_pull_broadcast: bool,
    consumer_group_info: Option<&ConsumerGroupInfo>,
) -> bool {
    match consumer_group_info {
        Some(info) => {
            proxy_pull_broadcast
                || (info.get_message_model() == MessageModel::Broadcasting
                    && info.get_consume_type() == ConsumeType::ConsumePassively)
        }
        None => proxy_pull_broadcast,
    }
}
