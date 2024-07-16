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
use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::request_source::RequestSource;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::filter::MessageFilter;
use rocketmq_store::message_store::default_message_store::DefaultMessageStore;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::stats::stats_type::StatsType;
use tracing::debug;
use tracing::info;

use crate::client::manager::consumer_manager::ConsumerManager;
use crate::long_polling::long_polling_service::pull_request_hold_service::PullRequestHoldService;
use crate::long_polling::pull_request::PullRequest;
use crate::mqtrace::consume_message_context::ConsumeMessageContext;
use crate::mqtrace::consume_message_hook::ConsumeMessageHook;
use crate::offset::manager::broadcast_offset_manager::BroadcastOffsetManager;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::processor::pull_message_processor::is_broadcast;
use crate::processor::pull_message_processor::rewrite_response_for_static_topic;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub struct DefaultPullMessageResultHandler {
    topic_config_manager: Arc<TopicConfigManager>,
    consumer_offset_manager: Arc<ConsumerOffsetManager>,
    consumer_manager: Arc<ConsumerManager>,
    broadcast_offset_manager: Arc<BroadcastOffsetManager>,
    broker_stats_manager: Arc<BrokerStatsManager>,
    broker_config: Arc<BrokerConfig>,
    consume_message_hook_list: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
    pull_request_hold_service: Option<Arc<PullRequestHoldService<DefaultMessageStore>>>,
}

impl DefaultPullMessageResultHandler {
    pub fn new(
        topic_config_manager: Arc<TopicConfigManager>,
        consumer_offset_manager: Arc<ConsumerOffsetManager>,
        consumer_manager: Arc<ConsumerManager>,
        broadcast_offset_manager: Arc<BroadcastOffsetManager>,
        broker_stats_manager: Arc<BrokerStatsManager>,
        broker_config: Arc<BrokerConfig>,
        consume_message_hook_list: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
    ) -> Self {
        Self {
            topic_config_manager,
            consumer_offset_manager,
            consumer_manager,
            broadcast_offset_manager,
            broker_stats_manager,
            broker_config,
            consume_message_hook_list,
            pull_request_hold_service: None,
        }
    }

    pub fn set_pull_request_hold_service(
        &mut self,
        pull_request_hold_service: Option<Arc<PullRequestHoldService<DefaultMessageStore>>>,
    ) {
        self.pull_request_hold_service = pull_request_hold_service;
    }
}

impl PullMessageResultHandler for DefaultPullMessageResultHandler {
    fn handle(
        &self,
        get_message_result: GetMessageResult,
        request: RemotingCommand,
        request_header: PullMessageRequestHeader,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        subscription_data: SubscriptionData,
        subscription_group_config: SubscriptionGroupConfig,
        broker_allow_suspend: bool,
        message_filter: Box<dyn MessageFilter>,
        mut response: RemotingCommand,
        mut mapping_context: TopicQueueMappingContext,
        _begin_time_mills: u64,
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
        let code = From::from(response.code());
        self.execute_consume_message_hook_before(
            &request,
            &request_header,
            &get_message_result,
            broker_allow_suspend,
            code,
        );
        let response_header = response.read_custom_header_mut::<PullMessageResponseHeader>();
        let rewrite_result = rewrite_response_for_static_topic(
            &request_header,
            response_header.unwrap(),
            &mut mapping_context,
            code,
        );
        if rewrite_result.is_some() {
            return rewrite_result;
        }
        self.update_broadcast_pulled_offset(
            request_header.topic.as_str(),
            request_header.consumer_group.as_str(),
            request_header.queue_id.unwrap(),
            &request_header,
            &channel,
            Some(&response),
            get_message_result.next_begin_offset(),
        );
        self.try_commit_offset(
            broker_allow_suspend,
            &request_header,
            get_message_result.next_begin_offset(),
            channel.remote_address(),
        );

        match code {
            ResponseCode::Success => {
                self.broker_stats_manager.inc_group_get_nums(
                    request_header.consumer_group.as_str(),
                    request_header.topic.as_str(),
                    get_message_result.message_count(),
                );
                self.broker_stats_manager.inc_group_get_size(
                    request_header.consumer_group.as_str(),
                    request_header.topic.as_str(),
                    get_message_result.buffer_total_size(),
                );
                self.broker_stats_manager.inc_broker_get_nums(
                    request_header.topic.as_str(),
                    get_message_result.message_count(),
                );

                ctx.upgrade()?;

                if self.broker_config.transfer_msg_by_heap {
                    let body = self.read_get_message_result(
                        &get_message_result,
                        request_header.consumer_group.as_str(),
                        request_header.topic.as_str(),
                        request_header.queue_id.unwrap(),
                    );
                    Some(response.set_body(body))
                } else {
                    None
                }
            }
            ResponseCode::PullNotFound => {
                let has_suspend_flag =
                    PullSysFlag::has_suspend_flag(request_header.sys_flag as u32);
                let suspend_timeout_millis_long = if has_suspend_flag {
                    request_header.suspend_timeout_millis
                } else {
                    0
                };
                if broker_allow_suspend && has_suspend_flag {
                    let mut polling_time_mills = suspend_timeout_millis_long;
                    if !self.broker_config.long_polling_enable {
                        polling_time_mills = self.broker_config.short_polling_time_mills;
                    }
                    let topic = request_header.topic.as_str();
                    let queue_id = request_header.queue_id.unwrap();
                    let offset = request_header.queue_offset;

                    let pull_request = PullRequest::new(
                        request,
                        channel,
                        ctx,
                        polling_time_mills,
                        get_current_millis(),
                        offset,
                        subscription_data,
                        Arc::new(message_filter),
                    );
                    self.pull_request_hold_service
                        .as_ref()
                        .unwrap()
                        .suspend_pull_request(topic, queue_id, pull_request);
                }
                None
            }
            ResponseCode::PullOffsetMoved => Some(response),
            ResponseCode::PullRetryImmediately => Some(response),
            _ => None,
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl DefaultPullMessageResultHandler {
    fn read_get_message_result(
        &self,
        get_message_result: &GetMessageResult,
        _group: &str,
        _topic: &str,
        _queue_id: i32,
    ) -> Option<Bytes> {
        let mut bytes_mut =
            BytesMut::with_capacity(get_message_result.buffer_total_size() as usize);
        for msg in get_message_result.message_mapped_list() {
            let data = &msg.mapped_file.as_ref().unwrap().get_mapped_file()
                [msg.start_offset as usize..(msg.start_offset + msg.size as u64) as usize];
            bytes_mut.extend_from_slice(data);
        }
        Some(bytes_mut.freeze())
    }

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

    fn try_commit_offset(
        &self,
        broker_allow_suspend: bool,
        request_header: &PullMessageRequestHeader,
        next_offset: i64,
        client_address: SocketAddr,
    ) {
        self.consumer_offset_manager.commit_pull_offset(
            client_address,
            request_header.consumer_group.as_str(),
            request_header.topic.as_str(),
            request_header.queue_id.unwrap(),
            next_offset,
        );

        let mut store_offset_enable = broker_allow_suspend;
        let has_commit_offset_flag =
            PullSysFlag::has_commit_offset_flag(request_header.sys_flag as u32);
        store_offset_enable = store_offset_enable && has_commit_offset_flag;
        if store_offset_enable {
            self.consumer_offset_manager.commit_offset(
                client_address,
                request_header.consumer_group.as_str(),
                request_header.topic.as_str(),
                request_header.queue_id.unwrap(),
                request_header.commit_offset,
            );
        }
    }

    fn update_broadcast_pulled_offset(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        request_header: &PullMessageRequestHeader,
        channel: &Channel,
        response: Option<&RemotingCommand>,
        next_begin_offset: i64,
    ) {
        if response.is_none() || !self.broker_config.enable_broadcast_offset_store {
            return;
        }
        let proxy_pull_broadcast =
            request_header.request_source == Some(RequestSource::ProxyForBroadcast.get_value());
        let consumer_group_info = self.consumer_manager.get_consumer_group_info(group);

        if is_broadcast(proxy_pull_broadcast, consumer_group_info.as_ref()) {
            let mut offset = request_header.queue_offset;
            if let Some(response) = response {
                if ResponseCode::from(response.code()) == ResponseCode::PullOffsetMoved {
                    offset = next_begin_offset;
                }
            }

            let client_id = if proxy_pull_broadcast {
                request_header
                    .proxy_forward_client_id
                    .clone()
                    .unwrap_or_default()
            } else if let Some(ref consumer_group_info) = consumer_group_info {
                if let Some(ref client_channel_info) =
                    consumer_group_info.find_channel_by_channel(channel)
                {
                    client_channel_info.client_id().clone()
                } else {
                    return;
                }
            } else {
                return;
            };
            self.broadcast_offset_manager.update_offset(
                topic,
                group,
                queue_id,
                offset,
                client_id.as_str(),
                proxy_pull_broadcast,
            );
        }
    }
}
