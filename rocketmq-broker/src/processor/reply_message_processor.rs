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
use std::net::SocketAddr;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::header::reply_message_request_header::ReplyMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::stats::stats_type::StatsType;
use tracing::warn;

use crate::client::manager::producer_manager::ProducerManager;
use crate::client::rebalance::rebalance_lock_manager::RebalanceLockManager;
use crate::mqtrace::send_message_context::SendMessageContext;
use crate::processor::send_message_processor::Inner;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;
use crate::transaction::transactional_message_service::TransactionalMessageService;

pub struct ReplyMessageProcessor<MS, TS> {
    inner: Inner<MS, TS>,
    store_host: SocketAddr,
}

impl<MS, TS> ReplyMessageProcessor<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    pub fn new(
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        subscription_group_manager: Arc<SubscriptionGroupManager<MS>>,
        topic_config_manager: TopicConfigManager,
        broker_config: Arc<BrokerConfig>,
        message_store: ArcMut<MS>,
        rebalance_lock_manager: Arc<RebalanceLockManager>,
        broker_stats_manager: Arc<BrokerStatsManager>,
        producer_manager: Option<Arc<ProducerManager>>,
        transactional_message_service: ArcMut<TS>,
    ) -> Self {
        let store_host = format!("{}:{}", broker_config.broker_ip1, broker_config.listen_port)
            .parse::<SocketAddr>()
            .unwrap();
        Self {
            inner: Inner {
                broker_config,
                topic_config_manager,
                send_message_hook_vec: ArcMut::new(Vec::new()),
                topic_queue_mapping_manager,
                subscription_group_manager,
                message_store,
                transactional_message_service,
                rebalance_lock_manager,
                broker_stats_manager,
                producer_manager,
                broker_to_client: Default::default(),
            },
            store_host,
        }
    }
}
impl<MS, TS> ReplyMessageProcessor<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_header = parse_request_header(&request);
        let mut request_header = request_header?;
        let mut mqtrace_context =
            self.inner
                .build_msg_context(&channel, &ctx, &mut request_header, &request);
        self.inner
            .execute_send_message_hook_before(&mqtrace_context);

        let mut response = self
            .process_reply_message_request(
                &ctx,
                &channel,
                request,
                &mut mqtrace_context,
                request_header,
            )
            .await;

        self.inner
            .execute_send_message_hook_after(Some(&mut response), &mut mqtrace_context);
        Some(response)
    }

    async fn process_reply_message_request(
        &mut self,
        ctx: &ConnectionHandlerContext,
        channel: &Channel,
        request: RemotingCommand,
        send_message_context: &mut SendMessageContext,
        request_header: SendMessageRequestHeader,
    ) -> RemotingCommand {
        let mut response = RemotingCommand::create_response_command().set_opaque(request.opaque());

        response
            .add_ext_field(
                MessageConst::PROPERTY_MSG_REGION,
                self.inner.broker_config.region_id.as_str(),
            )
            .add_ext_field(
                MessageConst::PROPERTY_TRACE_SWITCH,
                self.inner.broker_config.trace_on.to_string(),
            );
        let start_timstamp = self
            .inner
            .broker_config
            .start_accept_send_request_time_stamp as u64;
        if get_current_millis() < start_timstamp {
            return response
                .set_code(ResponseCode::SystemError)
                .set_remark(format!(
                    "broker unable to service, until, {}",
                    start_timstamp
                ));
        }
        response.set_code_mut(-1);
        self.inner
            .msg_check(channel, ctx, &request, &request_header, &mut response);
        if response.code() != -1 {
            return response;
        }
        let mut queue_id_int = request_header.queue_id.unwrap_or(0);
        let topic_config = self
            .inner
            .topic_config_manager
            .select_topic_config(request_header.topic())
            .unwrap();
        if queue_id_int < 0 {
            queue_id_int = self.inner.random_queue_id(topic_config.write_queue_nums) as i32;
        }

        let mut msg_inner = MessageExtBrokerInner::default();
        msg_inner.set_topic(request_header.topic().to_owned());
        msg_inner.message_ext_inner.queue_id = queue_id_int;
        if let Some(body) = request.body() {
            msg_inner.set_body(body.clone());
        }
        msg_inner.set_flag(request_header.flag);
        MessageAccessor::set_properties(
            &mut msg_inner,
            MessageDecoder::string_to_message_properties(request_header.properties.as_ref()),
        );
        msg_inner.properties_string = request_header.properties.clone().unwrap_or_default();
        msg_inner.message_ext_inner.born_timestamp = request_header.born_timestamp;
        msg_inner.message_ext_inner.born_host = channel.remote_address();
        msg_inner.message_ext_inner.store_host = self.store_host;
        msg_inner.message_ext_inner.reconsume_times = request_header.reconsume_times.unwrap_or(0);

        let mut push_reply_result = self
            .push_reply_message(channel, ctx, &request_header, &msg_inner)
            .await;
        let mut response_header = SendMessageResponseHeader::default();
        Self::handle_push_reply_result(
            &mut push_reply_result,
            &mut response,
            &mut response_header,
            queue_id_int,
        );

        if self.inner.broker_config.store_reply_message_enable {
            let put_message_result = self.inner.message_store.put_message(msg_inner).await;
            self.handle_put_message_result(
                put_message_result,
                &request,
                &mut response_header,
                send_message_context,
                queue_id_int,
                TopicMessageType::Normal,
                request_header.topic(),
            );
        }
        response.set_command_custom_header(response_header)
    }

    fn handle_put_message_result(
        &mut self,
        put_message_result: PutMessageResult,
        request: &RemotingCommand,
        response_header: &mut SendMessageResponseHeader,
        send_message_context: &mut SendMessageContext,
        queue_id_int: i32,
        _message_type: TopicMessageType,
        topic: &str,
    ) {
        let put_ok = match put_message_result.put_message_status() {
            PutMessageStatus::PutOk
            | PutMessageStatus::FlushDiskTimeout
            | PutMessageStatus::FlushSlaveTimeout
            | PutMessageStatus::SlaveNotAvailable => true,
            PutMessageStatus::ServiceNotAvailable => {
                warn!(
                    "service not available now. It may be caused by one of the following reasons: \
                     the broker's disk is full, messages are put to the slave, message store has \
                     been shut down, etc."
                );
                false
            }
            PutMessageStatus::CreateMappedFileFailed => {
                warn!("create mapped file failed, remoting_server is busy or broken.");
                false
            }
            PutMessageStatus::MessageIllegal => {
                /* warn!(
                "the message is illegal, maybe msg body or properties length not matched. msg body length limit {}B.",
                self.inner..getMaxMessageSize())*/
                false
            }
            PutMessageStatus::PropertiesSizeExceeded => {
                warn!("the message is illegal, maybe msg properties length limit 32KB.");
                false
            }
            PutMessageStatus::OsPageCacheBusy => {
                warn!("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                false
            }
            PutMessageStatus::UnknownError => {
                warn!("UNKNOWN_ERROR");
                false
            }
            _ => {
                warn!("UNKNOWN_ERROR DEFAULT");
                false
            }
        };
        let owner = request
            .get_ext_fields()
            .unwrap()
            .get(BrokerStatsManager::COMMERCIAL_OWNER)
            .cloned();
        let commercial_size_per_msg = self.inner.broker_config.commercial_size_per_msg;
        if put_ok {
            self.inner.broker_stats_manager.inc_topic_put_nums(
                topic,
                put_message_result.append_message_result().unwrap().msg_num,
                1,
            );
            self.inner.broker_stats_manager.inc_topic_put_size(
                topic,
                put_message_result
                    .append_message_result()
                    .unwrap()
                    .wrote_bytes,
            );
            self.inner.broker_stats_manager.inc_broker_put_nums(
                topic,
                put_message_result.append_message_result().unwrap().msg_num,
            );
            response_header.set_msg_id(
                put_message_result
                    .append_message_result()
                    .unwrap()
                    .msg_id
                    .clone()
                    .unwrap_or_default(),
            );
            response_header.set_queue_id(queue_id_int);
            response_header.set_queue_offset(
                put_message_result
                    .append_message_result()
                    .unwrap()
                    .logics_offset,
            );
            if self.inner.has_send_message_hook() {
                let msg_id = response_header.msg_id().clone();
                let queue_id = Some(response_header.queue_id());
                let queue_offset = Some(response_header.queue_offset());
                send_message_context.msg_id = msg_id;
                send_message_context.queue_id = queue_id;
                send_message_context.queue_offset = queue_offset;
                let commercial_base_count = self.inner.broker_config.commercial_base_count;
                let wrote_size = put_message_result
                    .append_message_result()
                    .unwrap()
                    .wrote_bytes;
                let commercial_msg_num =
                    (wrote_size as f64 / commercial_size_per_msg as f64).ceil() as i32;
                let inc_value = commercial_msg_num * commercial_base_count;
                send_message_context.commercial_send_stats = StatsType::SendSuccess;
                send_message_context.commercial_send_times = inc_value;
                send_message_context.commercial_send_size = wrote_size;
                send_message_context.commercial_owner = owner.unwrap_or_default();
            }
        } else if self.inner.has_send_message_hook() {
            let wrote_size = request.get_body().map_or(0, |body| body.len());
            let inc_value = (wrote_size as f64 / commercial_size_per_msg as f64).ceil() as i32;
            send_message_context.commercial_send_stats = StatsType::SendFailure;
            send_message_context.commercial_send_times = inc_value;
            send_message_context.commercial_send_size = wrote_size as i32;
            send_message_context.commercial_owner = owner.unwrap_or_default();
        }
    }

    fn handle_push_reply_result(
        push_reply_result: &mut PushReplyResult,
        response: &mut RemotingCommand,
        response_header: &mut SendMessageResponseHeader,
        queue_id_int: i32,
    ) {
        if !push_reply_result.0 {
            response.set_code_mut(ResponseCode::SystemError);
            response.set_remark_mut(push_reply_result.1.clone());
        } else {
            response.set_code_mut(ResponseCode::Success);
            //response.set_remark_mut(None);
            response_header.set_msg_id("0");
            response_header.set_queue_id(queue_id_int);
            response_header.set_queue_offset(0);
        }
    }

    async fn push_reply_message<M: MessageTrait>(
        &mut self,
        channel: &Channel,
        _ctx: &ConnectionHandlerContext,
        request_header: &SendMessageRequestHeader,
        msg: &M,
    ) -> PushReplyResult {
        let reply_message_request_header = ReplyMessageRequestHeader {
            born_host: CheetahString::from_string(channel.remote_address().to_string()),
            store_host: CheetahString::from_string(self.store_host.to_string()),
            store_timestamp: get_current_millis() as i64,
            producer_group: request_header.producer_group.clone(),
            topic: request_header.topic.clone(),
            default_topic: request_header.default_topic.clone(),
            default_topic_queue_nums: request_header.default_topic_queue_nums,
            queue_id: request_header.queue_id.unwrap_or(0),
            sys_flag: request_header.sys_flag,
            born_timestamp: request_header.born_timestamp,
            flag: request_header.flag,
            properties: request_header.properties.clone(),
            reconsume_times: request_header.reconsume_times,
            unit_mode: request_header.unit_mode,
            ..Default::default()
        };
        let mut command = RemotingCommand::create_request_command(
            RequestCode::PushReplyMessageToClient,
            reply_message_request_header,
        );
        if let Some(body) = msg.get_body().cloned() {
            command.set_body_mut_ref(body);
        }
        let sender_id = msg.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
        ));
        let mut push_reply_result = PushReplyResult(false, "".to_string());
        if let Some(sender_id) = sender_id {
            let channel = self
                .inner
                .producer_manager
                .as_ref()
                .unwrap()
                .find_channel(sender_id.as_str());
            if let Some(mut channel) = channel {
                let push_response = self
                    .inner
                    .broker_to_client
                    .call_client(&mut channel, command, 3000)
                    .await;
                match push_response {
                    Ok(response) => {
                        if response.code() == ResponseCode::Success as i32 {
                            push_reply_result.0 = true;
                        } else {
                            push_reply_result.1 = format!(
                                "push reply message to client failed, response code: {},{}",
                                response.code(),
                                sender_id
                            );
                        }
                    }
                    Err(error) => {
                        push_reply_result.1 = format!(
                            "push reply message to client failed, error: {},{}",
                            error, sender_id
                        );
                    }
                };
            } else {
                warn!("can not find channel by sender_id: {}", sender_id);
                push_reply_result.1 = format!("can not find channel by sender_id: {}", sender_id);
            }
        } else {
            warn!(
                " {}is null, can not reply message",
                MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT
            );
            push_reply_result.1 = format!(
                "{} is null, can not reply message",
                MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT
            );
        }
        push_reply_result
    }
}

fn parse_request_header(request: &RemotingCommand) -> Option<SendMessageRequestHeader> {
    let request_code = RequestCode::from(request.code());
    let mut request_header_v2 = None;
    if RequestCode::SendReplyMessageV2 == request_code
        || RequestCode::SendReplyMessage == request_code
    {
        request_header_v2 = request.decode_command_custom_header::<SendMessageRequestHeaderV2>();
    }

    match request_header_v2 {
        Some(header) => {
            Some(SendMessageRequestHeaderV2::create_send_message_request_header_v1(&header))
        }
        None => request.decode_command_custom_header::<SendMessageRequestHeader>(),
    }
}

#[derive(Debug, Clone)]
struct PushReplyResult(bool, String);
