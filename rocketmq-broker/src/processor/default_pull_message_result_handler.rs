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

use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::MessageDecoder;
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
use rocketmq_remoting::protocol::topic::OffsetMovedEvent;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::filter::MessageFilter;
use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::stats::stats_type::StatsType;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::long_polling::pull_request::PullRequest;
use crate::mqtrace::consume_message_context::ConsumeMessageContext;
use crate::mqtrace::consume_message_hook::ConsumeMessageHook;
use crate::processor::pull_message_processor::is_broadcast;
use crate::processor::pull_message_processor::rewrite_response_for_static_topic;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;

pub struct DefaultPullMessageResultHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    consume_message_hook_list: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
}

impl<MS: MessageStore> DefaultPullMessageResultHandler<MS> {
    pub fn new(
        consume_message_hook_list: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Self {
        Self {
            broker_runtime_inner,
            consume_message_hook_list,
        }
    }

    pub fn set_pull_request_hold_service(
        &mut self,
        //pull_request_hold_service: Option<ArcMut<PullRequestHoldService<DefaultMessageStore>>>,
        _inner: ArcMut<BrokerRuntimeInner<LocalFileMessageStore>>,
    ) {
        //self.pull_request_hold_service = pull_request_hold_service;
    }
}

impl<MS: MessageStore> PullMessageResultHandler for DefaultPullMessageResultHandler<MS> {
    async fn handle(
        &self,
        mut get_message_result: GetMessageResult,
        request: &mut RemotingCommand,
        request_header: PullMessageRequestHeader,
        mut channel: Channel,
        ctx: ConnectionHandlerContext,
        subscription_data: SubscriptionData,
        subscription_group_config: &SubscriptionGroupConfig,
        broker_allow_suspend: bool,
        message_filter: Arc<Box<dyn MessageFilter>>,
        mut response: RemotingCommand,
        mut mapping_context: TopicQueueMappingContext,
        _begin_time_mills: u64,
    ) -> Option<RemotingCommand> {
        let client_address = channel.remote_address().to_string();
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(request_header.topic.as_ref());
        let topic_sys_flag = topic_config.as_ref().map(|tc| tc.topic_sys_flag as i32).unwrap_or(0);
        Self::compose_response_header(
            &self.broker_runtime_inner, //need optimization
            &request_header,
            &get_message_result,
            topic_sys_flag,
            subscription_group_config,
            &mut response,
            client_address.as_str(),
        );
        let code = From::from(response.code());
        self.execute_consume_message_hook_before(
            request,
            &request_header,
            &get_message_result,
            broker_allow_suspend,
            code,
        );
        {
            let response_header = response.read_custom_header_mut::<PullMessageResponseHeader>().unwrap();
            let rewrite_result =
                rewrite_response_for_static_topic(&request_header, response_header, &mut mapping_context, code);
            if rewrite_result.is_some() {
                return rewrite_result;
            }
        }
        self.update_broadcast_pulled_offset(
            request_header.topic.as_ref(),
            request_header.consumer_group.as_ref(),
            request_header.queue_id,
            &request_header,
            &channel,
            Some(&mut response),
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
                let broker_stats = self.broker_runtime_inner.broker_stats_manager();
                broker_stats.inc_group_get_nums(
                    request_header.consumer_group.as_str(),
                    request_header.topic.as_str(),
                    get_message_result.message_count(),
                );
                broker_stats.inc_group_get_size(
                    request_header.consumer_group.as_str(),
                    request_header.topic.as_str(),
                    get_message_result.buffer_total_size(),
                );
                broker_stats.inc_broker_get_nums(request_header.topic.as_str(), get_message_result.message_count());

                // Record BrokerMetrics for non-retry/dlq topics
                if let Some(metrics) = crate::metrics::broker_metrics_manager::BrokerMetricsManager::try_global() {
                    let topic = request_header.topic.as_str();
                    let consumer_group = request_header.consumer_group.as_str();
                    let is_retry = topic.starts_with("%RETRY%") || topic.starts_with("%DLQ%");
                    if !is_retry {
                        metrics.inc_messages_out_total(
                            topic,
                            consumer_group,
                            get_message_result.message_count() as u64,
                            false,
                        );
                        metrics.inc_throughput_out_total(
                            topic,
                            consumer_group,
                            get_message_result.buffer_total_size() as u64,
                            false,
                        );
                    }
                }

                if self.broker_runtime_inner.broker_config().transfer_msg_by_heap {
                    let body = self.read_get_message_result(
                        &get_message_result,
                        request_header.consumer_group.as_str(),
                        request_header.topic.as_str(),
                        request_header.queue_id,
                    );
                    // Record group get latency
                    let latency = (get_current_millis() - _begin_time_mills) as i32;
                    self.broker_runtime_inner.broker_stats_manager().inc_group_get_latency(
                        request_header.consumer_group.as_str(),
                        request_header.topic.as_str(),
                        request_header.queue_id,
                        latency,
                    );
                    if let Some(body) = body {
                        response.set_body_mut_ref(body);
                    }
                    Some(response)
                } else {
                    //zero copy transfer
                    if let Some(header_bytes) =
                        response.encode_header_with_body_length(get_message_result.buffer_total_size() as usize)
                    {
                        let _ = channel.connection_mut().send_bytes(header_bytes).await;
                    }
                    for select_result in get_message_result.message_mapped_list_mut() {
                        if let Some(message) = select_result.bytes.take() {
                            let _ = channel.connection_mut().send_bytes(message).await;
                        }
                    }

                    None
                }
            }
            ResponseCode::PullNotFound => {
                let has_suspend_flag = PullSysFlag::has_suspend_flag(request_header.sys_flag as u32);
                let suspend_timeout_millis_long = if has_suspend_flag {
                    request_header.suspend_timeout_millis
                } else {
                    0
                };
                if broker_allow_suspend && has_suspend_flag {
                    let mut polling_time_mills = suspend_timeout_millis_long;
                    if !self.broker_runtime_inner.broker_config().long_polling_enable {
                        polling_time_mills = self.broker_runtime_inner.broker_config().short_polling_time_mills;
                    }
                    let topic = request_header.topic.as_str();
                    let queue_id = request_header.queue_id;
                    let offset = request_header.queue_offset;

                    let pull_request = PullRequest::new(
                        request.clone(),
                        channel,
                        ctx,
                        polling_time_mills,
                        get_current_millis(),
                        offset,
                        subscription_data,
                        message_filter,
                    );
                    self.broker_runtime_inner
                        .pull_request_hold_service()
                        .as_ref()
                        .unwrap()
                        .suspend_pull_request(topic, queue_id, pull_request);
                    return None;
                }
                Some(response)
            }
            ResponseCode::PullRetryImmediately => Some(response),
            ResponseCode::PullOffsetMoved => {
                if self.broker_runtime_inner.message_store_config().broker_role != BrokerRole::Slave
                    || self.broker_runtime_inner.message_store_config().offset_check_in_slave
                {
                    let response_header = response.read_custom_header_mut::<PullMessageResponseHeader>().unwrap();
                    let mut mq = MessageQueue::new();
                    mq.set_topic(request_header.topic.clone());
                    mq.set_broker_name(self.broker_runtime_inner.broker_config().broker_name().clone());
                    mq.set_queue_id(request_header.queue_id);

                    let offset_moved_event = OffsetMovedEvent {
                        consumer_group: request_header.consumer_group.to_string(),
                        message_queue: mq,
                        offset_request: request_header.queue_offset,
                        offset_new: get_message_result.next_begin_offset(),
                    };
                    warn!(
                        "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, \
                         suggestBrokerId={}",
                        request_header.topic,
                        request_header.consumer_group,
                        offset_moved_event.offset_request,
                        offset_moved_event.offset_new,
                        response_header.suggest_which_broker_id
                    );
                } else {
                    let response_header = response.read_custom_header_mut::<PullMessageResponseHeader>().unwrap();
                    response_header.suggest_which_broker_id = subscription_group_config.broker_id();
                    response.set_code_ref(ResponseCode::PullRetryImmediately);
                    warn!(
                        "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, \
                         suggestBrokerId={}",
                        request_header.topic,
                        request_header.consumer_group,
                        request_header.queue_offset,
                        subscription_group_config.broker_id()
                    );
                }
                Some(response)
            }
            _ => {
                warn!("[BUG] impossible result code of get message: {}", response.code());
                Some(response)
            }
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<MS: MessageStore> DefaultPullMessageResultHandler<MS> {
    /// Read message result and return (body bytes, last store timestamp)
    fn read_get_message_result(
        &self,
        get_message_result: &GetMessageResult,
        group: &str,
        topic: &str,
        queue_id: i32,
    ) -> Option<Bytes> {
        let mut bytes_mut = BytesMut::with_capacity(get_message_result.buffer_total_size() as usize);
        let mut store_timestamp: i64 = 0;

        for msg in get_message_result.message_mapped_list() {
            if let Some(mapped_file) = msg.mapped_file.as_ref() {
                let data = &mapped_file.get_mapped_file()
                    [msg.start_offset as usize..(msg.start_offset + msg.size as u64) as usize];
                bytes_mut.extend_from_slice(data);

                // Parse storeTimestamp from the last message
                // The position depends on whether bornHost is IPv4 or IPv6
                if data.len() > MessageDecoder::SYSFLAG_POSITION + 4 {
                    let sys_flag = i32::from_be_bytes([
                        data[MessageDecoder::SYSFLAG_POSITION],
                        data[MessageDecoder::SYSFLAG_POSITION + 1],
                        data[MessageDecoder::SYSFLAG_POSITION + 2],
                        data[MessageDecoder::SYSFLAG_POSITION + 3],
                    ]);

                    // bornHost: IPv4 = 8 bytes, IPv6 = 20 bytes
                    let bornhost_length = if (sys_flag & MessageSysFlag::BORNHOST_V6_FLAG) == 0 {
                        8
                    } else {
                        20
                    };

                    // storeTimestamp position = 4(TOTALSIZE) + 4(MAGICCODE) + 4(BODYCRC)
                    //                         + 4(QUEUEID) + 4(FLAG) + 8(QUEUEOFFSET)
                    //                         + 8(PHYSICALOFFSET) + 4(SYSFLAG) + 8(BORNTIMESTAMP)
                    //                         + bornhost_length
                    let store_timestamp_pos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhost_length;

                    if data.len() > store_timestamp_pos + 8 {
                        store_timestamp = i64::from_be_bytes([
                            data[store_timestamp_pos],
                            data[store_timestamp_pos + 1],
                            data[store_timestamp_pos + 2],
                            data[store_timestamp_pos + 3],
                            data[store_timestamp_pos + 4],
                            data[store_timestamp_pos + 5],
                            data[store_timestamp_pos + 6],
                            data[store_timestamp_pos + 7],
                        ]);
                    }
                }
            }
        }

        // Record disk fall behind time
        if store_timestamp > 0 {
            let fall_behind_time = get_current_millis() as i64 - store_timestamp;
            self.broker_runtime_inner
                .broker_stats_manager()
                .record_disk_fall_behind_time(group, topic, queue_id, fall_behind_time);
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
            let owner = ext_fields.get(BrokerStatsManager::COMMERCIAL_OWNER);
            let auth_type = ext_fields.get(BrokerStatsManager::ACCOUNT_AUTH_TYPE);
            let owner_parent = ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_PARENT);
            let owner_self = ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_SELF);

            let namespace =
                CheetahString::from_string(NamespaceUtil::get_namespace_from_resource(&request_header.topic));

            let mut context = ConsumeMessageContext {
                consumer_group: &request_header.consumer_group,
                topic: &request_header.topic,
                queue_id: Some(request_header.queue_id),
                client_host: None,
                store_host: None,
                message_ids: None,
                body_length: 0,
                success: false,
                status: None,
                topic_config: None,
                account_auth_type: auth_type,
                account_owner_parent: owner_parent,
                account_owner_self: owner_self,
                rcv_msg_num: 0,
                rcv_msg_size: 0,
                rcv_stat: StatsType::RcvSuccess,
                commercial_rcv_msg_num: 0,
                commercial_owner: None,
                commercial_rcv_stats: StatsType::RcvSuccess,
                commercial_rcv_times: 0,
                commercial_rcv_size: 0,
                namespace: &namespace,
            };

            match response_code {
                ResponseCode::Success => {
                    let commercial_base_count = self.broker_runtime_inner.broker_config().commercial_base_count;
                    let inc_value = get_message_result.msg_count4_commercial() * commercial_base_count;

                    context.commercial_rcv_stats = StatsType::RcvSuccess;
                    context.commercial_rcv_times = inc_value;
                    context.commercial_rcv_size = get_message_result.buffer_total_size();
                    context.commercial_owner = owner;

                    context.rcv_stat = StatsType::RcvSuccess;
                    context.rcv_msg_num = get_message_result.message_count();
                    context.rcv_msg_size = get_message_result.buffer_total_size();
                    context.commercial_rcv_msg_num = get_message_result.msg_count4_commercial();
                }
                ResponseCode::PullNotFound => {
                    if !broker_allow_suspend {
                        context.commercial_rcv_stats = StatsType::RcvEpolls;
                        context.commercial_rcv_times = 1;
                        context.commercial_owner = owner;

                        context.rcv_stat = StatsType::RcvEpolls;
                        context.rcv_msg_num = 0;
                        context.rcv_msg_size = 0;
                        context.commercial_rcv_msg_num = 0;
                    }
                }
                ResponseCode::PullRetryImmediately | ResponseCode::PullOffsetMoved => {
                    context.commercial_rcv_stats = StatsType::RcvEpolls;
                    context.commercial_rcv_times = 1;
                    context.commercial_owner = owner;

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

impl<MS: MessageStore> DefaultPullMessageResultHandler<MS> {
    fn compose_response_header(
        //broker_config: &BrokerConfig,
        broker_runtime_inner: &ArcMut<BrokerRuntimeInner<MS>>,
        request_header: &PullMessageRequestHeader,
        get_message_result: &GetMessageResult,
        topic_sys_flag: i32,
        subscription_group_config: &SubscriptionGroupConfig,
        response: &mut RemotingCommand,
        client_address: &str,
    ) {
        let mut response_header = PullMessageResponseHeader::default();
        response.set_remark_mut(format!("{:?}", get_message_result.status()));
        response_header.next_begin_offset = get_message_result.next_begin_offset();
        response_header.min_offset = get_message_result.min_offset();
        response_header.max_offset = get_message_result.max_offset();
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
                        "The broker stores no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} \
                         Consumer Group: {}",
                        request_header.queue_offset,
                        get_message_result.next_begin_offset(),
                        request_header.topic,
                        request_header.queue_id,
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
                    "The request offset: {} over flow badly, fix to {}, broker max offset: {}, consumer: {}",
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
                    "The request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, \
                     clientIp={}",
                    request_header.consumer_group,
                    request_header.topic,
                    request_header.queue_offset,
                    get_message_result.min_offset(),
                    client_address
                );
            }
        }

        let broker_config = broker_runtime_inner.broker_config();
        if broker_config.slave_read_enable && !broker_config.is_in_broker_container {
            if get_message_result.suggest_pulling_from_slave() {
                response_header.suggest_which_broker_id = subscription_group_config.which_broker_when_consume_slowly();
            } else {
                response_header.suggest_which_broker_id = subscription_group_config.broker_id();
            }
        } else {
            response_header.suggest_which_broker_id = MASTER_ID;
        }

        if broker_config.broker_identity.broker_id != MASTER_ID
            && !get_message_result.suggest_pulling_from_slave()
            && broker_runtime_inner.get_min_broker_id_in_group() == MASTER_ID
        {
            debug!(
                "slave redirect pullRequest to master, topic: {}, queueId: {}, consumer group: {}, next: {}, min: {}, \
                 max: {}",
                request_header.topic,
                request_header.queue_id,
                request_header.consumer_group,
                response_header.next_begin_offset,
                response_header.min_offset,
                response_header.max_offset
            );
            response_header.suggest_which_broker_id = MASTER_ID;
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
        self.broker_runtime_inner.consumer_offset_manager().commit_pull_offset(
            client_address,
            request_header.consumer_group.as_ref(),
            request_header.topic.as_ref(),
            request_header.queue_id,
            next_offset,
        );

        let mut store_offset_enable = broker_allow_suspend;
        let has_commit_offset_flag = PullSysFlag::has_commit_offset_flag(request_header.sys_flag as u32);
        store_offset_enable = store_offset_enable && has_commit_offset_flag;
        if store_offset_enable {
            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                client_address.to_string().into(),
                request_header.consumer_group.as_ref(),
                request_header.topic.as_ref(),
                request_header.queue_id,
                request_header.commit_offset,
            );
        }
    }

    fn update_broadcast_pulled_offset(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        request_header: &PullMessageRequestHeader,
        channel: &Channel,
        response: Option<&mut RemotingCommand>,
        next_begin_offset: i64,
    ) {
        if response.is_none() || !self.broker_runtime_inner.broker_config().enable_broadcast_offset_store {
            return;
        }
        let proxy_pull_broadcast = request_header.request_source == Some(RequestSource::ProxyForBroadcast.get_value());
        let consumer_group_info = self
            .broker_runtime_inner
            .consumer_manager()
            .get_consumer_group_info(group);

        if is_broadcast(proxy_pull_broadcast, consumer_group_info.as_ref()) {
            let mut offset = request_header.queue_offset;
            if let Some(response) = response {
                if ResponseCode::from(response.code()) == ResponseCode::PullOffsetMoved {
                    offset = next_begin_offset;
                }
            }

            let client_id = if proxy_pull_broadcast {
                request_header.proxy_forward_client_id.clone().unwrap_or_default()
            } else if let Some(ref consumer_group_info) = consumer_group_info {
                if let Some(ref client_channel_info) = consumer_group_info.find_channel_by_channel(channel) {
                    client_channel_info.client_id().clone()
                } else {
                    return;
                }
            } else {
                return;
            };
            self.broker_runtime_inner.broadcast_offset_manager().update_offset(
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
