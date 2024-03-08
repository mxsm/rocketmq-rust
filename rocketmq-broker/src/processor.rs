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
use rocketmq_common::{
    common::{
        constant::PermName,
        message::{message_enum::MessageType, MessageConst},
        topic::TopicValidator,
    },
    MessageDecoder, TimeUtils,
};
use rocketmq_remoting::{
    code::response_code::{RemotingSysResponseCode::SystemError, ResponseCode},
    protocol::{
        header::message_operation_header::send_message_request_header::SendMessageRequestHeader,
        remoting_command::RemotingCommand, NamespaceUtil,
    },
    runtime::server::ConnectionHandlerContext,
};
use rocketmq_store::status::manager::broker_stats_manager::BrokerStatsManager;

use crate::{broker_config::BrokerConfig, mqtrace::send_message_context::SendMessageContext};

pub(crate) mod ack_message_processor;
pub(crate) mod admin_broker_processor;
pub(crate) mod change_invisible_time_processor;
pub(crate) mod notification_processor;
pub(crate) mod peek_message_processor;
pub(crate) mod polling_info_processor;
pub(crate) mod pop_message_processor;
pub(crate) mod pull_message_processor;
pub(crate) mod reply_message_processor;
pub(crate) mod send_message_processor;

#[derive(Default)]
pub(crate) struct SendMessageProcessorInner {
    pub(crate) broker_config: Arc<BrokerConfig>,
}

impl SendMessageProcessorInner {
    pub(crate) fn consumer_send_msg_back(
        &mut self,
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
            MessageDecoder::string_to_message_properties(request_header.properties.as_deref());
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
        _ctx: &ConnectionHandlerContext,
        _request: &RemotingCommand,
        request_header: &SendMessageRequestHeader,
        response: &mut RemotingCommand,
    ) {
        //check broker permission
        if !PermName::is_writeable(self.broker_config.broker_permission()) {
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
        }
        //todo the next
    }

    pub(crate) fn random_queue_id(&self, write_queue_nums: u32) -> u32 {
        rand::thread_rng().gen_range(0..=99999999) % write_queue_nums
    }
}
