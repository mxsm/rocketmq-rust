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

use log::warn;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::ArcRefCellWrapper;
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
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::message_store::default_message_store::DefaultMessageStore;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;

use crate::client::manager::producer_manager::ProducerManager;
use crate::client::rebalance::rebalance_lock_manager::RebalanceLockManager;
use crate::mqtrace::send_message_context::SendMessageContext;
use crate::processor::send_message_processor::Inner;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

#[derive(Clone)]
pub struct ReplyMessageProcessor<MS = DefaultMessageStore> {
    inner: Inner<MS>,
    store_host: SocketAddr,
}

impl<MS: MessageStore> ReplyMessageProcessor<MS> {
    pub fn new(
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        subscription_group_manager: Arc<SubscriptionGroupManager<MS>>,
        topic_config_manager: TopicConfigManager,
        broker_config: Arc<BrokerConfig>,
        message_store: &MS,
        rebalance_lock_manager: Arc<RebalanceLockManager>,
        broker_stats_manager: Arc<BrokerStatsManager>,
        producer_manager: Option<Arc<ProducerManager>>,
    ) -> Self {
        let store_host = format!("{}:{}", broker_config.broker_ip1, broker_config.listen_port)
            .parse::<SocketAddr>()
            .unwrap();
        Self {
            inner: Inner {
                broker_config,
                topic_config_manager,
                send_message_hook_vec: ArcRefCellWrapper::new(Vec::new()),
                topic_queue_mapping_manager,
                subscription_group_manager,
                message_store: message_store.clone(),
                rebalance_lock_manager,
                broker_stats_manager,
                producer_manager,
            },
            store_host,
        }
    }
}
impl<M> ReplyMessageProcessor<M> {
    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_header = parse_request_header(&request);
        if request_header.is_none() {
            return None;
        }
        let mut request_header = request_header?;
        let mqtrace_context =
            self.inner
                .build_msg_context(&channel, &ctx, &mut request_header, &request);
        self.inner
            .execute_send_message_hook_before(&mqtrace_context);
        //self.inner.execute_send_message_hook_after()
        unimplemented!()
    }

    async fn process_reply_message_request(
        &mut self,
        ctx: &ConnectionHandlerContext,
        channel: &Channel,
        request: RemotingCommand,
        send_message_context: &SendMessageContext,
        request_header: SendMessageRequestHeader,
    ) -> RemotingCommand {
        let mut response = RemotingCommand::create_response_command_with_header(
            SendMessageResponseHeader::default(),
        )
        .set_opaque(request.opaque());

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
                .set_remark(Some(format!(
                    "broker unable to service, until, {}",
                    start_timstamp
                )));
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

        let mut message_ext = MessageExtBrokerInner::default();
        message_ext.set_topic(request_header.topic());
        message_ext.message_ext_inner.queue_id = queue_id_int;
        if let Some(body) = request.body() {
            message_ext.set_body(body.clone());
        }
        message_ext.set_flag(request_header.flag);
        MessageAccessor::set_properties(
            &mut message_ext,
            MessageDecoder::string_to_message_properties(request_header.properties.as_ref()),
        );
        message_ext.properties_string = request_header.properties.unwrap_or("".to_string());
        message_ext.message_ext_inner.born_timestamp = request_header.born_timestamp;
        message_ext.message_ext_inner.born_host = channel.remote_address();
        message_ext.message_ext_inner.store_host = self.store_host;
        message_ext.message_ext_inner.reconsume_times = request_header.reconsume_times.unwrap_or(0);

        /*if self.inner.broker_config.store_reply_message_enable {
            self.inner.message_store.put_message(message_ext.clone());
        }*/
        unimplemented!("process_reply_message_request")
    }

    /* fn push_reply_message<M: MessageTrait>(
        &mut self,
        ctx: &ConnectionHandlerContext,
        channel: &Channel,
        msg: &M,
    ) -> PushReplyResult {
        let reply_message_request_header = ReplyMessageRequestHeader {
            ..Default::default()
        };
        let command = RemotingCommand::create_request_command(
            RequestCode::PushReplyMessageToClient,
            reply_message_request_header,
        )
        .set_body(msg.get_body().cloned());
        let sender_id = msg.get_property(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT);
        let mut push_reply_result = PushReplyResult(false, "".to_string());
        if let Some(sender_id) = sender_id {
            self.inner.producer_manager.as_ref().unwrap().fin
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
    }*/
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
