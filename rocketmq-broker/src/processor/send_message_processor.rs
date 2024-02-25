use std::sync::Arc;

use rocketmq_common::common::{
    attribute::topic_message_type::TopicMessageType,
    message::{
        message_single::{MessageExt, MessageExtBrokerInner},
        MessageConst,
    },
};
use rocketmq_remoting::protocol::{
    header::message_operation_header::{
        send_message_request_header::SendMessageRequestHeader,
        send_message_response_header::SendMessageResponseHeader, TopicRequestHeaderTrait,
    },
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
use rocketmq_store::{
    base::message_result::PutMessageResult, log_file::MessageStore,
    message_store::local_file_store::LocalFileMessageStore,
};

use crate::{
    broker_config::BrokerConfig,
    mqtrace::send_message_context::SendMessageContext,
    processor::SendMessageProcessorInner,
    topic::manager::{
        topic_config_manager::TopicConfigManager,
        topic_queue_mapping_manager::TopicQueueMappingManager,
    },
};

pub struct SendMessageProcessor {
    inner: SendMessageProcessorInner,
    topic_queue_mapping_manager: Arc<parking_lot::RwLock<TopicQueueMappingManager>>,
    topic_config_manager: Arc<parking_lot::RwLock<TopicConfigManager>>,
    broker_config: Arc<parking_lot::RwLock<BrokerConfig>>,
    message_store: Arc<parking_lot::RwLock<Box<dyn MessageStore + Sync + Send + 'static>>>,
}

impl Default for SendMessageProcessor {
    fn default() -> Self {
        Self {
            inner: SendMessageProcessorInner::default(),
            topic_queue_mapping_manager: Arc::new(parking_lot::RwLock::new(
                TopicQueueMappingManager::default(),
            )),
            topic_config_manager: Arc::new(parking_lot::RwLock::new(TopicConfigManager::default())),
            broker_config: Arc::new(parking_lot::RwLock::new(BrokerConfig::default())),
            message_store: Arc::new(parking_lot::RwLock::new(
                Box::<LocalFileMessageStore>::default(),
            )),
        }
    }
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
        response.unwrap()
    }
}

#[allow(unused_variables)]
impl SendMessageProcessor {
    pub fn new(
        topic_queue_mapping_manager: Arc<parking_lot::RwLock<TopicQueueMappingManager>>,
        message_store: Arc<parking_lot::RwLock<Box<dyn MessageStore + Sync + Send + 'static>>>,
    ) -> Self {
        Self {
            inner: SendMessageProcessorInner::default(),
            topic_queue_mapping_manager,
            topic_config_manager: Arc::new(Default::default()),
            broker_config: Arc::new(Default::default()),
            message_store,
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
    ) -> Option<RemotingCommand>
    where
        F: FnOnce(&SendMessageContext, &RemotingCommand),
    {
        Some(RemotingCommand::create_response_command())
    }

    fn send_message<F>(
        &mut self,
        ctx: &ConnectionHandlerContext,
        request: &RemotingCommand,
        send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        mapping_context: TopicQueueMappingContext,
        send_message_callback: F,
    ) -> Option<RemotingCommand>
    where
        F: FnOnce(&SendMessageContext, &RemotingCommand),
    {
        let response = self.pre_send(ctx.as_ref(), request.as_ref(), &request_header);
        if response.code() != -1 {
            return Some(response);
        }
        let response_header = SendMessageResponseHeader::default();
        let topic_config = self
            .topic_config_manager
            .read()
            .select_topic_config(request_header.topic().as_str())
            .unwrap();
        let mut _queue_id = request_header.queue_id;
        if _queue_id < 0 {
            _queue_id = self.inner.random_queue_id(topic_config.write_queue_nums) as i32;
        }

        if self.broker_config.read().async_send_enable {
            None
        } else {
            let result = self
                .message_store
                .read()
                .put_message(MessageExtBrokerInner::default());
            Some(response)
        }
    }

    fn handle_put_message_result(
        put_message_result: PutMessageResult,
        response: RemotingCommand,
        request: RemotingCommand,
        msg: MessageExt,
        response_header: SendMessageResponseHeader,
        send_message_context: SendMessageContext,
        ctx: ConnectionHandlerContext,
        queue_id_int: i32,
        begin_time_millis: i64,
        mapping_context: TopicQueueMappingContext,
        message_type: TopicMessageType,
    ) -> RemotingCommand {
        // Your implementation here
        todo!()
    }
    pub fn pre_send(
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
