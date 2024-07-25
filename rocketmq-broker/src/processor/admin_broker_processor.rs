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
mod broker_config_request_handler;
mod topic_request_handler;
use std::sync::Arc;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use rocketmq_store::message_store::default_message_store::DefaultMessageStore;
use tracing::warn;

use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::processor::admin_broker_processor::broker_config_request_handler::BrokerConfigRequestHandler;
use crate::processor::admin_broker_processor::topic_request_handler::TopicRequestHandler;
use crate::processor::pop_inflight_message_counter::PopInflightMessageCounter;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

#[derive(Clone)]
pub struct AdminBrokerProcessor {
    topic_request_handler: TopicRequestHandler,
    broker_config_request_handler: BrokerConfigRequestHandler,
}

impl AdminBrokerProcessor {
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        topic_config_manager: TopicConfigManager,
        consumer_offset_manager: ConsumerOffsetManager,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        default_message_store: DefaultMessageStore,
    ) -> Self {
        let inner = Inner {
            broker_config,
            topic_config_manager,
            consumer_offset_manager,
            topic_queue_mapping_manager,
            default_message_store,
            pop_inflight_message_counter: Arc::new(PopInflightMessageCounter),
        };
        let topic_request_handler = TopicRequestHandler::new(inner.clone());
        let broker_config_request_handler = BrokerConfigRequestHandler::new(inner.clone());
        AdminBrokerProcessor {
            topic_request_handler,
            broker_config_request_handler,
        }
    }
}

impl AdminBrokerProcessor {
    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        match request_code {
            RequestCode::UpdateAndCreateTopic => {
                self.topic_request_handler
                    .update_and_create_topic(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::UpdateAndCreateTopicList => {
                self.topic_request_handler
                    .update_and_create_topic_list(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::DeleteTopicInBroker => {
                self.topic_request_handler
                    .delete_topic(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetAllTopicConfig => {
                self.topic_request_handler
                    .get_all_topic_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::UpdateBrokerConfig => {
                self.broker_config_request_handler
                    .update_broker_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetBrokerConfig => {
                self.broker_config_request_handler
                    .get_broker_config(channel, ctx, request_code, request)
                    .await
            }
            _ => Some(get_unknown_cmd_response(request_code)),
        }
    }
}

fn get_unknown_cmd_response(request_code: RequestCode) -> RemotingCommand {
    warn!(
        "request type {:?}-{} not supported",
        request_code,
        request_code.to_i32()
    );
    RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::RequestCodeNotSupported,
        format!(" request type {} not supported", request_code.to_i32()),
    )
}

#[derive(Clone)]
struct Inner {
    broker_config: Arc<BrokerConfig>,
    topic_config_manager: TopicConfigManager,
    consumer_offset_manager: ConsumerOffsetManager,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    default_message_store: DefaultMessageStore,
    pop_inflight_message_counter: Arc<PopInflightMessageCounter>,
}
