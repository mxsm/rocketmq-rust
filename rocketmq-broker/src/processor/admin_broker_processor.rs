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

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::message_store::default_message_store::DefaultMessageStore;
use rocketmq_store::stats::broker_stats::BrokerStats;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::warn;

use crate::client::manager::consumer_manager::ConsumerManager;
use crate::client::rebalance::rebalance_lock_manager::RebalanceLockManager;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::processor::admin_broker_processor::batch_mq_handler::BatchMqHandler;
use crate::processor::admin_broker_processor::broker_config_request_handler::BrokerConfigRequestHandler;
use crate::processor::admin_broker_processor::consumer_request_handler::ConsumerRequestHandler;
use crate::processor::admin_broker_processor::offset_request_handler::OffsetRequestHandler;
use crate::processor::admin_broker_processor::topic_request_handler::TopicRequestHandler;
use crate::processor::pop_inflight_message_counter::PopInflightMessageCounter;
use crate::schedule::schedule_message_service::ScheduleMessageService;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

mod batch_mq_handler;
mod broker_config_request_handler;
mod consumer_request_handler;
mod offset_request_handler;
mod topic_request_handler;

pub struct AdminBrokerProcessor {
    topic_request_handler: TopicRequestHandler,
    broker_config_request_handler: BrokerConfigRequestHandler,
    consumer_request_handler: ConsumerRequestHandler,
    offset_request_handler: OffsetRequestHandler,
    batch_mq_handler: BatchMqHandler,
}

impl AdminBrokerProcessor {
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        server_config: Arc<ServerConfig>,
        message_store_config: Arc<MessageStoreConfig>,
        topic_config_manager: TopicConfigManager,
        consumer_offset_manager: ConsumerOffsetManager,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        default_message_store: ArcMut<DefaultMessageStore>,
        schedule_message_service: ScheduleMessageService,
        broker_stats: Option<Arc<BrokerStats<DefaultMessageStore>>>,
        consume_manager: Arc<ConsumerManager>,
        broker_out_api: Arc<BrokerOuterAPI>,
        broker_stats_manager: Arc<BrokerStatsManager>,
        rebalance_lock_manager: Arc<RebalanceLockManager>,
        broker_member_group: Arc<BrokerMemberGroup>,
    ) -> Self {
        let inner = Inner {
            broker_config,
            server_config,
            message_store_config,
            topic_config_manager,
            consumer_offset_manager,
            topic_queue_mapping_manager,
            default_message_store,
            pop_inflight_message_counter: Arc::new(PopInflightMessageCounter),
            schedule_message_service,
            broker_stats,
            consume_manager,
            broker_out_api,
            broker_stats_manager,
            rebalance_lock_manager,
            broker_member_group,
        };
        let topic_request_handler = TopicRequestHandler::new(inner.clone());
        let broker_config_request_handler = BrokerConfigRequestHandler::new(inner.clone());
        let consumer_request_handler = ConsumerRequestHandler::new(inner.clone());
        let offset_request_handler = OffsetRequestHandler::new(inner.clone());
        let batch_mq_handler = BatchMqHandler::new(inner.clone());
        AdminBrokerProcessor {
            topic_request_handler,
            broker_config_request_handler,
            consumer_request_handler,
            offset_request_handler,
            batch_mq_handler,
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
            RequestCode::GetSystemTopicListFromBroker => {
                self.topic_request_handler
                    .get_system_topic_list_from_broker(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetTopicStatsInfo => {
                self.topic_request_handler
                    .get_topic_stats_info(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetConsumerConnectionList => {
                self.consumer_request_handler
                    .get_consumer_connection_list(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetConsumeStats => {
                self.consumer_request_handler
                    .get_consume_stats(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetAllConsumerOffset => {
                self.consumer_request_handler
                    .get_all_consumer_offset(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetTopicConfig => {
                self.topic_request_handler
                    .get_topic_config(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetBrokerRuntimeInfo => {
                self.broker_config_request_handler
                    .get_broker_runtime_info(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::QueryTopicConsumeByWho => {
                self.topic_request_handler
                    .query_topic_consume_by_who(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::QueryTopicsByConsumer => {
                self.topic_request_handler
                    .query_topics_by_consumer(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetMaxOffset => {
                self.offset_request_handler
                    .get_max_offset(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetMinOffset => {
                self.offset_request_handler
                    .get_min_offset(channel, ctx, request_code, request)
                    .await
            }

            RequestCode::LockBatchMq => {
                self.batch_mq_handler
                    .lock_natch_mq(channel, ctx, request_code, request)
                    .await
            }

            RequestCode::UnlockBatchMq => {
                self.batch_mq_handler
                    .unlock_batch_mq(channel, ctx, request_code, request)
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
    server_config: Arc<ServerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    topic_config_manager: TopicConfigManager,
    consumer_offset_manager: ConsumerOffsetManager,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    default_message_store: ArcMut<DefaultMessageStore>,
    pop_inflight_message_counter: Arc<PopInflightMessageCounter>,
    schedule_message_service: ScheduleMessageService,
    broker_stats: Option<Arc<BrokerStats<DefaultMessageStore>>>,
    consume_manager: Arc<ConsumerManager>,
    broker_out_api: Arc<BrokerOuterAPI>,
    broker_stats_manager: Arc<BrokerStatsManager>,
    rebalance_lock_manager: Arc<RebalanceLockManager>,
    broker_member_group: Arc<BrokerMemberGroup>,
}
