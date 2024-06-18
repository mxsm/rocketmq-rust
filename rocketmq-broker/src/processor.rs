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

use rand::Rng;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::common::TopicSysFlag::build_sys_flag;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode::SystemError;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::NamespaceUtil;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::info;
use tracing::warn;

use self::client_manage_processor::ClientManageProcessor;
use crate::mqtrace::send_message_context::SendMessageContext;
use crate::mqtrace::send_message_hook::SendMessageHook;
use crate::processor::ack_message_processor::AckMessageProcessor;
use crate::processor::admin_broker_processor::AdminBrokerProcessor;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeProcessor;
use crate::processor::consumer_manage_processor::ConsumerManageProcessor;
use crate::processor::end_transaction_processor::EndTransactionProcessor;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::peek_message_processor::PeekMessageProcessor;
use crate::processor::polling_info_processor::PollingInfoProcessor;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::PullMessageProcessor;
use crate::processor::query_assignment_processor::QueryAssignmentProcessor;
use crate::processor::query_message_processor::QueryMessageProcessor;
use crate::processor::reply_message_processor::ReplyMessageProcessor;
use crate::processor::send_message_processor::SendMessageProcessor;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub(crate) mod ack_message_processor;
pub(crate) mod admin_broker_processor;
pub(crate) mod change_invisible_time_processor;
pub(crate) mod client_manage_processor;
pub(crate) mod consumer_manage_processor;
pub(crate) mod end_transaction_processor;
pub(crate) mod notification_processor;
pub(crate) mod peek_message_processor;
pub(crate) mod polling_info_processor;
pub(crate) mod pop_message_processor;
pub(crate) mod pull_message_processor;
pub(crate) mod pull_message_result_handler;
pub(crate) mod query_assignment_processor;
pub(crate) mod query_message_processor;
pub(crate) mod reply_message_processor;
pub(crate) mod send_message_processor;

pub struct BrokerRequestProcessor<MS>
where
    MS: Clone,
{
    pub(crate) send_message_processor: SendMessageProcessor<MS>,
    pub(crate) pull_message_processor: PullMessageProcessor,
    pub(crate) peek_message_processor: PeekMessageProcessor,
    pub(crate) pop_message_processor: PopMessageProcessor,
    pub(crate) ack_message_processor: AckMessageProcessor,
    pub(crate) change_invisible_time_processor: ChangeInvisibleTimeProcessor,
    pub(crate) notification_processor: NotificationProcessor,
    pub(crate) polling_info_processor: PollingInfoProcessor,
    pub(crate) reply_message_processor: ReplyMessageProcessor,
    pub(crate) query_message_processor: QueryMessageProcessor,
    pub(crate) client_manage_processor: ClientManageProcessor,
    pub(crate) consumer_manage_processor: ConsumerManageProcessor,
    pub(crate) query_assignment_processor: QueryAssignmentProcessor,
    pub(crate) end_transaction_processor: EndTransactionProcessor,
    pub(crate) admin_broker_processor: AdminBrokerProcessor,
}
impl<MS: Clone> Clone for BrokerRequestProcessor<MS> {
    fn clone(&self) -> Self {
        Self {
            send_message_processor: self.send_message_processor.clone(),
            pull_message_processor: self.pull_message_processor.clone(),
            peek_message_processor: self.peek_message_processor.clone(),
            pop_message_processor: self.pop_message_processor.clone(),
            ack_message_processor: self.ack_message_processor.clone(),
            change_invisible_time_processor: self.change_invisible_time_processor.clone(),
            notification_processor: self.notification_processor.clone(),
            polling_info_processor: self.polling_info_processor.clone(),
            reply_message_processor: self.reply_message_processor.clone(),
            admin_broker_processor: self.admin_broker_processor.clone(),
            client_manage_processor: self.client_manage_processor.clone(),
            consumer_manage_processor: self.consumer_manage_processor.clone(),
            query_assignment_processor: self.query_assignment_processor.clone(),
            query_message_processor: self.query_message_processor.clone(),
            end_transaction_processor: self.end_transaction_processor.clone(),
        }
    }
}

impl<MS: MessageStore + Send + Sync + 'static> RequestProcessor for BrokerRequestProcessor<MS> {
    async fn process_request(
        &mut self,
        ctx: ConnectionHandlerContext<'_>,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_code = RequestCode::from(request.code());
        info!("process_request: {:?}", request_code);
        match request_code {
            RequestCode::SendMessage
            | RequestCode::SendMessageV2
            | RequestCode::SendBatchMessage
            | RequestCode::ConsumerSendMsgBack => {
                self.send_message_processor
                    .process_request(ctx, request_code, request)
                    .await
            }
            RequestCode::HeartBeat
            | RequestCode::UnregisterClient
            | RequestCode::CheckClientConfig => {
                self.client_manage_processor
                    .process_request(ctx, request_code, request)
                    .await
            }
            RequestCode::PullMessage | RequestCode::LitePullMessage => {
                self.pull_message_processor
                    .process_request(ctx, request_code, request)
                    .await
            }

            _ => self.admin_broker_processor.process_request(ctx, request),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SendMessageProcessorInner {
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) topic_config_manager: TopicConfigManager,
    pub(crate) send_message_hook_vec: Arc<parking_lot::RwLock<Vec<Box<dyn SendMessageHook>>>>,
}

impl SendMessageProcessorInner {
    pub(crate) fn execute_send_message_hook_before(&self, context: &SendMessageContext) {
        for hook in self.send_message_hook_vec.read().iter() {
            hook.send_message_before(context);
        }
    }

    pub(crate) fn execute_send_message_hook_after(
        &self,
        response: Option<&mut RemotingCommand>,
        context: &mut SendMessageContext,
    ) {
        for hook in self.send_message_hook_vec.read().iter() {
            if let Some(ref response) = response {
                if let Some(ref header) =
                    response.decode_command_custom_header::<SendMessageResponseHeader>()
                {
                    context.msg_id = header.msg_id().to_string();
                    context.queue_id = Some(header.queue_id());
                    context.queue_offset = Some(header.queue_offset());
                    context.code = response.code();
                    context.error_msg = response.remark().unwrap_or(&"".to_string()).to_string();
                }
            }

            hook.send_message_after(context);
        }
    }

    pub(crate) fn consumer_send_msg_back(
        &self,
        _ctx: &ConnectionHandlerContext,
        _request: &RemotingCommand,
    ) -> Option<RemotingCommand> {
        todo!()
    }

    pub(crate) fn build_msg_context(
        &self,
        ctx: &ConnectionHandlerContext,
        request_header: &mut SendMessageRequestHeader,
        request: &RemotingCommand,
    ) -> SendMessageContext {
        let namespace = NamespaceUtil::get_namespace_from_resource(&request_header.topic);

        let mut send_message_context = SendMessageContext {
            namespace,
            producer_group: request_header.producer_group.clone(),
            ..Default::default()
        };
        send_message_context.topic(request_header.topic.clone());
        send_message_context.body_length(
            request
                .body()
                .as_ref()
                .map_or_else(|| 0, |b| b.len() as i32),
        );
        send_message_context.msg_props(request_header.properties.clone().unwrap());
        send_message_context.born_host(ctx.remoting_address().to_string());
        send_message_context.broker_addr(self.broker_config.broker_server_config().bind_address());
        send_message_context.queue_id(request_header.queue_id);
        send_message_context.broker_region_id(self.broker_config.region_id());
        send_message_context.born_time_stamp(request_header.born_timestamp);
        send_message_context.request_time_stamp(TimeUtils::get_current_millis() as i64);

        if let Some(owner) = request
            .ext_fields()
            .unwrap()
            .get(BrokerStatsManager::COMMERCIAL_OWNER)
        {
            send_message_context.commercial_owner(owner.clone());
        }

        let mut properties =
            MessageDecoder::string_to_message_properties(request_header.properties.as_ref());
        properties.insert(
            MessageConst::PROPERTY_MSG_REGION.to_string(),
            self.broker_config.region_id(),
        );
        properties.insert(
            MessageConst::PROPERTY_TRACE_SWITCH.to_string(),
            self.broker_config.trace_on.to_string(),
        );
        request_header.properties = Some(MessageDecoder::message_properties_to_string(&properties));

        if let Some(unique_key) =
            properties.get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
        {
            send_message_context.msg_unique_key.clone_from(unique_key);
        } else {
            send_message_context.msg_unique_key = "".to_string();
        }

        if properties.contains_key(MessageConst::PROPERTY_SHARDING_KEY) {
            send_message_context.msg_type = MessageType::OrderMsg;
        } else {
            send_message_context.msg_type = MessageType::NormalMsg;
        }
        send_message_context
    }

    pub(crate) fn msg_check(
        &mut self,
        ctx: &ConnectionHandlerContext<'_>,
        _request: &RemotingCommand,
        request_header: &SendMessageRequestHeader,
        response: &mut RemotingCommand,
    ) {
        //check broker permission
        if !PermName::is_writeable(self.broker_config.broker_permission())
            && self
                .topic_config_manager
                .is_order_topic(request_header.topic.as_str())
        {
            response.with_code(ResponseCode::NoPermission);
            response.with_remark(Some(format!(
                "the broker[{}] sending message is forbidden",
                self.broker_config.broker_ip1.clone()
            )));
            return;
        }

        //check Topic
        let result = TopicValidator::validate_topic(request_header.topic.as_str());
        if !result.valid() {
            response.with_code(SystemError);
            response.with_remark(Some(result.remark().to_string()));
            return;
        }

        if TopicValidator::is_not_allowed_send_topic(request_header.topic.as_str()) {
            response.with_code(ResponseCode::NoPermission);
            response.with_remark(Some(format!(
                "Sending message to topic[{}] is forbidden.",
                request_header.topic.as_str()
            )));
            return;
        }
        let mut topic_config = self
            .topic_config_manager
            .select_topic_config(request_header.topic.as_str());
        if topic_config.is_none() {
            let mut topic_sys_flag = 0;
            if request_header.unit_mode.unwrap_or(false) {
                if request_header.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
                    topic_sys_flag = build_sys_flag(false, true);
                } else {
                    topic_sys_flag = build_sys_flag(true, false);
                }
            }
            warn!(
                "the topic {} not exist, producer: {}",
                request_header.topic(),
                ctx.remoting_address(),
            );
            topic_config = self
                .topic_config_manager
                .create_topic_in_send_message_method(
                    request_header.topic.as_str(),
                    request_header.default_topic.as_str(),
                    ctx.remoting_address(),
                    request_header.default_topic_queue_nums,
                    topic_sys_flag,
                );

            if topic_config.is_none() && request_header.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX)
            {
                topic_config = self
                    .topic_config_manager
                    .create_topic_in_send_message_back_method(
                        request_header.topic.as_str(),
                        1,
                        PermName::PERM_WRITE | PermName::PERM_READ,
                        false,
                        topic_sys_flag,
                    );
            }

            if topic_config.is_none() {
                response.with_code(ResponseCode::TopicNotExist);
                response.with_remark(Some(format!(
                    "topic[{}] not exist, apply first please!",
                    request_header.topic.as_str()
                )));
            }
        }
    }

    pub(crate) fn random_queue_id(&self, write_queue_nums: u32) -> u32 {
        rand::thread_rng().gen_range(0..=99999999) % write_queue_nums
    }
}
