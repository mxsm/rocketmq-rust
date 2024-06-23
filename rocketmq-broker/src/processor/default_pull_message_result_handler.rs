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
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::NamespaceUtil;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::filter::MessageFilter;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::stats::stats_type::StatsType;
use tracing::debug;
use tracing::info;

use crate::mqtrace::consume_message_context::ConsumeMessageContext;
use crate::mqtrace::consume_message_hook::ConsumeMessageHook;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub struct DefaultPullMessageResultHandler {
    topic_config_manager: Arc<TopicConfigManager>,
    broker_config: Arc<BrokerConfig>,
    consume_message_hook_list: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
}

impl DefaultPullMessageResultHandler {
    pub fn new(
        topic_config_manager: Arc<TopicConfigManager>,
        broker_config: Arc<BrokerConfig>,
        consume_message_hook_list: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
    ) -> Self {
        Self {
            topic_config_manager,
            broker_config,
            consume_message_hook_list,
        }
    }
}

#[allow(unused_variables)]
impl PullMessageResultHandler for DefaultPullMessageResultHandler {
    fn handle(
        &self,
        get_message_result: GetMessageResult,
        request: RemotingCommand,
        request_header: PullMessageRequestHeader,
        channel: Channel,
        subscription_data: SubscriptionData,
        subscription_group_config: SubscriptionGroupConfig,
        broker_allow_suspend: bool,
        message_filter: Box<dyn MessageFilter>,
        mut response: RemotingCommand,
        mapping_context: TopicQueueMappingContext,
        begin_time_mills: u64,
    ) -> Option<RemotingCommand> {
        let client_address = channel.remote_address().to_string();
        let topic_config = self
            .topic_config_manager
            .select_topic_config(request_header.topic.as_str());
        Self::compose_response_header(
            &self.broker_config,
            &request_header,
            &get_message_result,
            topic_config.as_ref().unwrap().topic_sys_flag as i32,
            &subscription_group_config,
            &mut response,
            client_address.as_str(),
        );
        /*        self.execute_consume_message_hook_before(
            &request,
            &request_header,
            &get_message_result,
            broker_allow_suspend,
            From::from(response.code()),
        );*/

        None
    }
}

impl DefaultPullMessageResultHandler {
    fn execute_consume_message_hook_before(
        &self,
        request: &RemotingCommand,
        request_header: &PullMessageRequestHeader,
        get_message_result: &GetMessageResult,
        broker_allow_suspend: bool,
        response_code: ResponseCode,
    ) {
        if self.has_consume_message_hook() {
            let ext_fields = request.get_ext_fields().unwrap();
            let owner = ext_fields
                .get(BrokerStatsManager::COMMERCIAL_OWNER)
                .cloned();
            let auth_type = ext_fields
                .get(BrokerStatsManager::ACCOUNT_AUTH_TYPE)
                .cloned();
            let owner_parent = ext_fields
                .get(BrokerStatsManager::ACCOUNT_OWNER_PARENT)
                .cloned();
            let owner_self = ext_fields
                .get(BrokerStatsManager::ACCOUNT_OWNER_SELF)
                .cloned();

            let mut context = ConsumeMessageContext::default();
            context
                .consumer_group
                .clone_from(&request_header.consumer_group);
            context.topic.clone_from(&request_header.topic);
            context.queue_id = request_header.queue_id;
            context.account_auth_type = auth_type;
            context.account_owner_parent = owner_parent;
            context.account_owner_self = owner_self;
            context.namespace = NamespaceUtil::get_namespace_from_resource(&request_header.topic);

            match response_code {
                ResponseCode::Success => {
                    let commercial_base_count = self.broker_config.commercial_base_count;
                    let inc_value =
                        get_message_result.msg_count4_commercial() * commercial_base_count;

                    context.commercial_rcv_stats = StatsType::RcvSuccess;
                    context.commercial_rcv_times = inc_value;
                    context.commercial_rcv_size = get_message_result.buffer_total_size();
                    context.commercial_owner.clone_from(&owner);

                    context.rcv_stat = StatsType::RcvSuccess;
                    context.rcv_msg_num = get_message_result.message_count();
                    context.rcv_msg_size = get_message_result.buffer_total_size();
                    context.commercial_rcv_msg_num = get_message_result.msg_count4_commercial();
                }
                ResponseCode::PullNotFound => {
                    if !broker_allow_suspend {
                        context.commercial_rcv_stats = StatsType::RcvEpolls;
                        context.commercial_rcv_times = 1;
                        context.commercial_owner.clone_from(&owner);

                        context.rcv_stat = StatsType::RcvEpolls;
                        context.rcv_msg_num = 0;
                        context.rcv_msg_size = 0;
                        context.commercial_rcv_msg_num = 0;
                    }
                }
                ResponseCode::PullRetryImmediately | ResponseCode::PullOffsetMoved => {
                    context.commercial_rcv_stats = StatsType::RcvEpolls;
                    context.commercial_rcv_times = 1;
                    context.commercial_owner.clone_from(&owner);

                    context.rcv_stat = StatsType::RcvEpolls;
                    context.rcv_msg_num = 0;
                    context.rcv_msg_size = 0;
                    context.commercial_rcv_msg_num = 0;
                }
                _ => {}
            }

            for hook in self.consume_message_hook_list.iter() {
                hook.consume_message_before(&mut context);
            }
        }
    }

    pub fn has_consume_message_hook(&self) -> bool {
        !self.consume_message_hook_list.is_empty()
    }
}

impl DefaultPullMessageResultHandler {
    fn compose_response_header(
        broker_config: &Arc<BrokerConfig>,
        request_header: &PullMessageRequestHeader,
        get_message_result: &GetMessageResult,
        topic_sys_flag: i32,
        subscription_group_config: &SubscriptionGroupConfig,
        response: &mut RemotingCommand,
        client_address: &str,
    ) {
        let mut response_header = PullMessageResponseHeader::default();
        response.set_remark_ref(Some(format!("{:?}", get_message_result.status())));
        response_header.next_begin_offset = Some(get_message_result.next_begin_offset());
        response_header.min_offset = Some(get_message_result.min_offset());
        response_header.max_offset = Some(get_message_result.max_offset());
        response_header.topic_sys_flag = Some(topic_sys_flag);
        response_header.group_sys_flag = Some(subscription_group_config.group_sys_flag());

        match get_message_result.status().unwrap() {
            GetMessageStatus::Found => {
                response.set_code_ref(RemotingSysResponseCode::Success);
            }
            GetMessageStatus::MessageWasRemoving | GetMessageStatus::NoMatchedMessage => {
                response.set_code_ref(ResponseCode::PullRetryImmediately);
            }
            GetMessageStatus::NoMatchedLogicQueue | GetMessageStatus::NoMessageInQueue => {
                if request_header.queue_offset != 0 {
                    response.set_code_ref(ResponseCode::PullOffsetMoved);
                    info!(
                        "The broker stores no queue data, fix the request offset {} to {}, Topic: \
                         {} QueueId: {} Consumer Group: {}",
                        request_header.queue_offset,
                        get_message_result.next_begin_offset(),
                        request_header.topic,
                        request_header.queue_id.unwrap_or(0),
                        request_header.consumer_group
                    );
                } else {
                    response.set_code_ref(ResponseCode::PullNotFound);
                }
            }
            GetMessageStatus::OffsetFoundNull | GetMessageStatus::OffsetOverflowOne => {
                response.set_code_ref(ResponseCode::PullNotFound);
            }
            GetMessageStatus::OffsetOverflowBadly => {
                response.set_code_ref(ResponseCode::PullOffsetMoved);
                info!(
                    "The request offset: {} over flow badly, fix to {}, broker max offset: {}, \
                     consumer: {}",
                    request_header.queue_offset,
                    get_message_result.next_begin_offset(),
                    get_message_result.max_offset(),
                    client_address
                );
            }
            GetMessageStatus::OffsetReset => {
                response.set_code_ref(ResponseCode::PullOffsetMoved);
                info!(
                    "The queue under pulling was previously reset to start from {}",
                    get_message_result.next_begin_offset()
                );
            }
            GetMessageStatus::OffsetTooSmall => {
                response.set_code_ref(ResponseCode::PullOffsetMoved);
                info!(
                    "The request offset too small. group={}, topic={}, requestOffset={}, \
                     brokerMinOffset={}, clientIp={}",
                    request_header.consumer_group,
                    request_header.topic,
                    request_header.queue_offset,
                    get_message_result.min_offset(),
                    client_address
                );
            }
        }

        if broker_config.slave_read_enable && !broker_config.is_in_broker_container {
            if get_message_result.suggest_pulling_from_slave() {
                response_header.suggest_which_broker_id =
                    Some(subscription_group_config.which_broker_when_consume_slowly());
            } else {
                response_header.suggest_which_broker_id =
                    Some(subscription_group_config.broker_id());
            }
        } else {
            response_header.suggest_which_broker_id = Some(MASTER_ID);
        }

        if broker_config.broker_identity.broker_id != MASTER_ID
            && !get_message_result.suggest_pulling_from_slave()
        {
            debug!(
                "slave redirect pullRequest to master, topic: {}, queueId: {}, consumer group: \
                 {}, next: {}, min: {}, max: {}",
                request_header.topic,
                request_header.queue_id.unwrap_or(0),
                request_header.consumer_group,
                response_header.next_begin_offset.unwrap(),
                response_header.min_offset.unwrap(),
                response_header.max_offset.unwrap()
            );
            response_header.suggest_which_broker_id = Some(MASTER_ID);
            if get_message_result.status() != Some(GetMessageStatus::Found) {
                response.set_code_ref(ResponseCode::PullRetryImmediately);
            }
        }
        response.set_command_custom_header_ref(response_header)
    }
}
