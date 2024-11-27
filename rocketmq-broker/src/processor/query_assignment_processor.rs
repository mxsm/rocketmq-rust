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
use crate::load_balance::message_request_mode_manager::MessageRequestModeManager;
 use cheetah_string::CheetahString;
 use rocketmq_client_rust::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
 use rocketmq_client_rust::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely;
 use rocketmq_client_rust::consumer::rebalance_strategy::allocate_message_queue_averagely_by_circle::AllocateMessageQueueAveragelyByCircle;
 use rocketmq_common::common::config_manager::ConfigManager;
 use rocketmq_remoting::code::request_code::RequestCode;
 use rocketmq_remoting::net::channel::Channel;
 use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
 use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
 use rocketmq_store::config::message_store_config::MessageStoreConfig;
 use std::collections::HashMap;
 use std::sync::Arc;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
use rocketmq_remoting::protocol::RemotingDeserializable;

pub struct QueryAssignmentProcessor {
    message_request_mode_manager: MessageRequestModeManager,
    load_strategy: HashMap<CheetahString, Arc<dyn AllocateMessageQueueStrategy>>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl QueryAssignmentProcessor {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> Self {
        let allocate_message_queue_averagely: Arc<dyn AllocateMessageQueueStrategy> =
            Arc::new(AllocateMessageQueueAveragely);
        let allocate_message_queue_averagely_by_circle: Arc<dyn AllocateMessageQueueStrategy> =
            Arc::new(AllocateMessageQueueAveragelyByCircle);
        let mut load_strategy = HashMap::new();
        load_strategy.insert(
            CheetahString::from_static_str(allocate_message_queue_averagely.get_name()),
            allocate_message_queue_averagely,
        );
        load_strategy.insert(
            CheetahString::from_static_str(allocate_message_queue_averagely_by_circle.get_name()),
            allocate_message_queue_averagely_by_circle,
        );
        let manager = MessageRequestModeManager::new(message_store_config.clone());
        let _ = manager.load();
        Self {
            message_request_mode_manager: manager,
            load_strategy,
            message_store_config,
        }
    }
}

impl QueryAssignmentProcessor {
    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        match request_code {
            RequestCode::QueryAssignment => self.query_assignment(channel, ctx, request).await,
            RequestCode::SetMessageRequestMode => {
                self.set_message_request_mode(channel, ctx, request).await
            }
            _ => None,
        }
    }

    async fn query_assignment(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        unimplemented!()
    }

    async fn set_message_request_mode(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_body =
            SetMessageRequestModeRequestBody::decode(request.get_body().expect("empty body"))
                .expect("decode SetMessageRequestModeRequestBody failed");
        if request_body.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            return Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::NoPermission)
                    .set_remark(CheetahString::from_static_str(
                        "retry topic is not allowed to set mode",
                    )),
            );
        }
        self.message_request_mode_manager.set_message_request_mode(
            request_body.topic.clone(),
            request_body.consumer_group.clone(),
            request_body,
        );
        self.message_request_mode_manager.persist();
        Some(RemotingCommand::create_response_command_with_code(
            ResponseCode::Success,
        ))
    }
}
