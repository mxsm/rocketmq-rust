use std::sync::Arc;

use rocketmq_common::common::message::MessageConst;
use rocketmq_remoting::protocol::{
    header::message_operation_header::send_message_request_header::SendMessageRequestHeader,
    static_topic::topic_queue_mapping_context::TopicQueueMappingContext,
};
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
use rocketmq_remoting::{
    code::request_code::RequestCode,
    protocol::{
        header::message_operation_header::send_message_request_header::parse_request_header,
        remoting_command::RemotingCommand,
    },
    runtime::{processor::RequestProcessor, server::ConnectionHandlerContext},
};

use crate::{
    broker_config::BrokerConfig, mqtrace::send_message_context::SendMessageContext,
    processor::SendMessageProcessorInner,
    topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager,
};

#[derive(Default)]
pub struct SendMessageProcessor {
    inner: SendMessageProcessorInner,
    topic_queue_mapping_manager: Arc<parking_lot::RwLock<TopicQueueMappingManager>>,
    broker_config: Arc<parking_lot::RwLock<BrokerConfig>>,
}

impl RequestProcessor for SendMessageProcessor {
    fn process_request(
        &mut self,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> RemotingCommand {
        let request_code = RequestCode::from(request.code());
        let response = match request_code {
            RequestCode::ConsumerSendMsgBack => self.inner.consumer_send_msg_back(&ctx, &request),
            _ => {
                let mut request_header = parse_request_header(&request).unwrap();
                let mapping_context = self
                    .topic_queue_mapping_manager
                    .write()
                    .build_topic_queue_mapping_context(&request_header, true);
                let rewrite_result = self
                    .topic_queue_mapping_manager
                    .read()
                    .rewrite_request_for_static_topic(&request_header, &mapping_context);
                if let Some(rewrite_result) = rewrite_result {
                    return rewrite_result;
                }

                let send_message_context =
                    self.inner
                        .build_msg_context(&ctx, &mut request_header, &request);
                if request_header.batch.is_none() || !request_header.batch.unwrap() {
                    self.send_message(
                        &ctx,
                        &request,
                        send_message_context,
                        request_header,
                        mapping_context,
                        |_, _| {},
                    )
                } else {
                    self.send_batch_message(
                        &ctx,
                        &request,
                        send_message_context,
                        request_header,
                        mapping_context,
                        |_, _| {},
                    )
                }
            }
        };
        response
    }
}

#[allow(unused_variables)]
impl SendMessageProcessor {
    pub fn new(
        topic_queue_mapping_manager: Arc<parking_lot::RwLock<TopicQueueMappingManager>>,
    ) -> Self {
        Self {
            inner: SendMessageProcessorInner::default(),
            topic_queue_mapping_manager,
            broker_config: Arc::new(Default::default()),
        }
    }

    fn send_batch_message<F>(
        &self,
        ctx: &ConnectionHandlerContext,
        request: &RemotingCommand,
        send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        mapping_context: TopicQueueMappingContext,
        send_message_callback: F,
    ) -> RemotingCommand
    where
        F: FnOnce(&SendMessageContext, &RemotingCommand),
    {
        RemotingCommand::create_response_command()
    }

    fn send_message<F>(
        &self,
        ctx: &ConnectionHandlerContext,
        request: &RemotingCommand,
        send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        mapping_context: TopicQueueMappingContext,
        send_message_callback: F,
    ) -> RemotingCommand
    where
        F: FnOnce(&SendMessageContext, &RemotingCommand),
    {
        RemotingCommand::create_response_command()
    }

    fn pre_send(
        &mut self,
        ctx: &ConnectionHandlerContext,
        request: &RemotingCommand,
        request_header: &SendMessageRequestHeader,
    ) -> RemotingCommand {
        let mut response = RemotingCommand::create_response_command();
        response.with_opaque(request.opaque());
        response.add_ext_field(
            MessageConst::PROPERTY_MSG_REGION,
            self.broker_config.read().region_id(),
        );
        response.add_ext_field(
            MessageConst::PROPERTY_TRACE_SWITCH,
            self.broker_config.read().trace_on.to_string(),
        );

        //todo java code to implement
        /*        if (this.brokerController.getMessageStore().now() < startTimestamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimestamp)));
            return response;
        }*/
        self.inner
            .msg_check(ctx, request, request_header, &mut response);
        response
    }
}
