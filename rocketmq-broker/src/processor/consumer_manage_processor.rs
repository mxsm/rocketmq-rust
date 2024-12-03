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
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::get_consumer_listby_group_response_body::GetConsumerListByGroupResponseBody;
use rocketmq_remoting::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::log_file::MessageStore;
use tracing::info;
use tracing::warn;

use crate::client::manager::consumer_manager::ConsumerManager;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

pub struct ConsumerManageProcessor<MS> {
    broker_config: Arc<BrokerConfig>,
    consumer_manager: Arc<ConsumerManager>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    consumer_offset_manager: Arc<ConsumerOffsetManager>,
    subscription_group_manager: Arc<SubscriptionGroupManager<MS>>,
    topic_config_manager: Arc<TopicConfigManager>,
    message_store: ArcMut<MS>,
}

impl<MS> ConsumerManageProcessor<MS>
where
    MS: MessageStore,
{
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        consumer_manager: Arc<ConsumerManager>,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        subscription_group_manager: Arc<SubscriptionGroupManager<MS>>,
        consumer_offset_manager: Arc<ConsumerOffsetManager>,
        topic_config_manager: Arc<TopicConfigManager>,
        message_store: ArcMut<MS>,
    ) -> Self {
        Self {
            broker_config,
            consumer_manager,
            topic_queue_mapping_manager,
            consumer_offset_manager,
            subscription_group_manager,
            topic_config_manager,
            message_store,
        }
    }
}

#[allow(unused_variables)]
impl<MS> ConsumerManageProcessor<MS>
where
    MS: MessageStore,
{
    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        match request_code {
            RequestCode::GetConsumerListByGroup => {
                self.get_consumer_list_by_group(channel, ctx, request).await
            }
            RequestCode::UpdateConsumerOffset => {
                self.update_consumer_offset(channel, ctx, request).await
            }
            RequestCode::QueryConsumerOffset => {
                self.query_consumer_offset(channel, ctx, request).await
            }
            _ => None,
        }
    }

    pub async fn get_consumer_list_by_group(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<GetConsumerListByGroupRequestHeader>()
            .unwrap();
        let consumer_group_info = self
            .consumer_manager
            .get_consumer_group_info(request_header.consumer_group.as_ref());

        match consumer_group_info {
            None => {
                warn!(
                    "getConsumerGroupInfo failed, {} {}",
                    request_header.consumer_group,
                    channel.remote_address()
                );
            }
            Some(info) => {
                let client_ids = info.get_all_client_ids();
                if !client_ids.is_empty() {
                    let body = GetConsumerListByGroupResponseBody {
                        consumer_id_list: client_ids,
                    };
                    return Some(
                        response
                            .set_body(
                                body.encode()
                                    .expect("GetConsumerListByGroupResponseBody encode error"),
                            )
                            .set_code(ResponseCode::Success),
                    );
                } else {
                    warn!(
                        "getAllClientId failed, {} {}",
                        request_header.consumer_group,
                        channel.remote_address()
                    )
                }
            }
        }
        Some(
            response
                .set_remark(format!(
                    "no consumer for this group, {}",
                    request_header.consumer_group
                ))
                .set_code(ResponseCode::SystemError),
        )
    }

    async fn update_consumer_offset(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut request_header = request
            .decode_command_custom_header::<UpdateConsumerOffsetRequestHeader>()
            .unwrap();
        let mut mapping_context = self
            .topic_queue_mapping_manager
            .build_topic_queue_mapping_context(&request_header, false);

        let rewrite_result = self.rewrite_request_for_static_topic_for_consume_offset(
            &mut request_header,
            &mut mapping_context,
        );
        if let Some(result) = rewrite_result {
            return Some(result);
        }
        let topic = request_header.topic.as_ref();
        let group = request_header.consumer_group.as_ref();
        let queue_id = request_header.queue_id;
        let offset = request_header.commit_offset;
        let response = RemotingCommand::create_response_command();
        if !self
            .subscription_group_manager
            .contains_subscription_group(group)
        {
            return Some(
                response
                    .set_code(ResponseCode::SubscriptionGroupNotExist)
                    .set_remark(format!("subscription group not exist, {}", group)),
            );
        }

        if !self.topic_config_manager.contains_topic(topic) {
            return Some(
                response
                    .set_code(ResponseCode::TopicNotExist)
                    .set_remark(format!("topic not exist, {}", topic)),
            );
        }

        // if queue_id.is_none() {
        //     return Some(
        //         response
        //             .set_code(ResponseCode::SystemError)
        //             .set_remark(format!("QueueId is null, topic is {}", topic)),
        //     );
        // }
        // if offset.is_none() {
        //     return Some(
        //         response
        //             .set_code(ResponseCode::SystemError)
        //             .set_remark(format!("Offset is null, topic is {}", topic)),
        //     );
        // }
        if self.broker_config.use_server_side_reset_offset
            && self
                .consumer_offset_manager
                .has_offset_reset(topic, group, queue_id)
        {
            info!(
                "Update consumer offset is rejected because of previous offset-reset. \
                 Group={},Topic={}, QueueId={}, Offset={}",
                topic, group, queue_id, offset
            );
            return Some(response.set_remark("Offset has been previously reset"));
        }
        self.consumer_offset_manager.commit_offset(
            channel.remote_address(),
            group,
            topic,
            queue_id,
            offset,
        );
        Some(response)
    }

    async fn query_consumer_offset(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut request_header = request
            .decode_command_custom_header::<QueryConsumerOffsetRequestHeader>()
            .unwrap();
        let mut mapping_context = self
            .topic_queue_mapping_manager
            .build_topic_queue_mapping_context(&request_header, false);
        if let Some(result) =
            self.rewrite_request_for_static_topic(&mut request_header, &mut mapping_context)
        {
            return Some(result);
        }
        let offset = self.consumer_offset_manager.query_offset(
            request_header.consumer_group.as_ref(),
            request_header.topic.as_ref(),
            request_header.queue_id,
        );
        let mut response = RemotingCommand::create_response_command();
        let mut response_header = QueryConsumerOffsetResponseHeader::default();
        if offset >= 0 {
            response_header.offset = Some(offset);
        } else {
            let min_offset = self
                .message_store
                .get_min_offset_in_queue(request_header.topic.as_ref(), request_header.queue_id);
            if let Some(value) = request_header.set_zero_if_not_found {
                if !value {
                    response = response
                        .set_code(ResponseCode::QueryNotFound)
                        .set_remark("Not found, do not set to zero, maybe this group boot first");
                }
            } else if min_offset <= 0
                && self.message_store.check_in_mem_by_consume_offset(
                    request_header.topic.as_ref(),
                    request_header.queue_id,
                    0,
                    1,
                )
            {
                response_header.offset = Some(0);
            } else {
                response = response
                    .set_code(ResponseCode::QueryNotFound)
                    .set_remark("Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first");
            }
        }
        if let Some(result) = self.rewrite_response_for_static_topic(
            &request_header,
            &mut response_header,
            &mapping_context,
            response.code(),
        ) {
            return Some(result);
        }
        Some(response.set_command_custom_header(response_header))
    }

    fn rewrite_request_for_static_topic_for_consume_offset(
        &mut self,
        request_header: &mut UpdateConsumerOffsetRequestHeader,
        mapping_context: &mut TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        None
    }

    fn rewrite_request_for_static_topic(
        &mut self,
        request_header: &mut QueryConsumerOffsetRequestHeader,
        mapping_context: &mut TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        let mapping_detail = mapping_context.mapping_detail.as_ref()?;
        if !mapping_context.is_leader() {
            return Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NotLeaderForQueue,
                format!(
                    "{}-{} does not exit in request process of current broker {}",
                    request_header.topic,
                    request_header.queue_id,
                    mapping_detail
                        .topic_queue_mapping_info
                        .bname
                        .as_ref()
                        .cloned()
                        .unwrap_or_default()
                ),
            ));
        }
        let mapping_item_list = &mapping_context.mapping_item_list;
        if mapping_item_list.len() == 1 && mapping_item_list[0].logic_offset == 0 {
            mapping_context.current_item = Some(mapping_item_list[0].clone());
            request_header.queue_id = mapping_context.leader_item.as_ref()?.queue_id;
            return None;
        }
        let mut offset = -1;
        for mapping_item in mapping_item_list.iter().rev() {
            mapping_context.current_item = Some(mapping_item.clone());
            if mapping_item.bname == mapping_detail.topic_queue_mapping_info.bname {
                offset = self.consumer_offset_manager.query_offset(
                    request_header.consumer_group.as_ref(),
                    request_header.topic.as_ref(),
                    mapping_item.queue_id,
                );
                if offset >= 0 {
                    break;
                }
            } else {
                request_header.set_broker_name(mapping_item.bname.clone().unwrap_or_default());
                request_header.queue_id = mapping_item.queue_id;
                request_header.set_lo(Some(false));
                request_header.set_zero_if_not_found = Some(true);

                /*let rpc_request = RpcRequest;
                let rpc_response = self
                    .broker_outer_api
                    .get_rpc_client()
                    .await
                    .invoke(rpc_request, self.broker_config.forward_timeout)
                    .await?;

                if let Some(exception) = rpc_response.exception {
                    return Err(exception);
                }

                if rpc_response.code == ResponseCode::SUCCESS {
                    offset = rpc_response.header.unwrap().offset;
                    break;
                } else if rpc_response.code == ResponseCode::QUERY_NOT_FOUND {
                    continue;
                } else {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Unknown response code",
                    )));
                }*/
            }
        }
        let mut response = RemotingCommand::create_response_command();
        let mut response_header = QueryConsumerOffsetResponseHeader { offset: None };
        if offset >= 0 {
            response_header.offset = Some(offset);
            response = response.set_code(ResponseCode::Success);
        } else {
            response = response
                .set_code(ResponseCode::QueryNotFound)
                .set_remark("Not found, maybe this group consumer boot first");
        }
        let rewrite_response_result = self.rewrite_response_for_static_topic(
            request_header,
            &mut response_header,
            mapping_context,
            response.code(),
        );
        if rewrite_response_result.is_some() {
            return rewrite_response_result;
        }
        Some(response.set_command_custom_header(response_header))
    }

    fn rewrite_response_for_static_topic(
        &mut self,
        request_header: &QueryConsumerOffsetRequestHeader,
        response_header: &mut QueryConsumerOffsetResponseHeader,
        mapping_context: &TopicQueueMappingContext,
        code: i32,
    ) -> Option<RemotingCommand> {
        mapping_context.mapping_detail.as_ref()?;
        if ResponseCode::from(code) != ResponseCode::Success {
            return None;
        }
        response_header.offset = Some(
            mapping_context
                .current_item
                .as_ref()
                .unwrap()
                .compute_static_queue_offset_strictly(response_header.offset.unwrap_or(0)),
        );
        None
    }
}
