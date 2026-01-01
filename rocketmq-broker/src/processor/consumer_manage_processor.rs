// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::rpc::rpc_client::RpcClient;
use rocketmq_remoting::rpc::rpc_request::RpcRequest;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

pub struct ConsumerManageProcessor<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> RequestProcessor for ConsumerManageProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("ConsumerManageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::GetConsumerListByGroup
            | RequestCode::UpdateConsumerOffset
            | RequestCode::QueryConsumerOffset => self.process_request_inner(channel, ctx, request_code, request).await,
            _ => {
                warn!(
                    "ConsumerManageProcessor received unknown request code: {:?}",
                    request_code
                );
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("ConsumerManageProcessor request code {} not supported", request.code()),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }
}

impl<MS> ConsumerManageProcessor<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

#[allow(unused_variables)]
impl<MS> ConsumerManageProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request_inner(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request_code {
            RequestCode::GetConsumerListByGroup => self.get_consumer_list_by_group(channel, ctx, request).await,
            RequestCode::UpdateConsumerOffset => self.update_consumer_offset(channel, ctx, request).await,
            RequestCode::QueryConsumerOffset => self.query_consumer_offset(channel, ctx, request).await,
            _ => Ok(None),
        }
    }

    pub async fn get_consumer_list_by_group(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header::<GetConsumerListByGroupRequestHeader>()?;
        let consumer_group_info = self
            .broker_runtime_inner
            .consumer_manager()
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
                    return Ok(Some(
                        response
                            .set_body(body.encode().expect("GetConsumerListByGroupResponseBody encode error"))
                            .set_code(ResponseCode::Success),
                    ));
                } else {
                    warn!(
                        "getAllClientId failed, {} {}",
                        request_header.consumer_group,
                        channel.remote_address()
                    )
                }
            }
        }
        Ok(Some(
            response
                .set_remark(format!("no consumer for this group, {}", request_header.consumer_group))
                .set_code(ResponseCode::SystemError),
        ))
    }

    async fn update_consumer_offset(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut request_header = request.decode_command_custom_header::<UpdateConsumerOffsetRequestHeader>()?;
        let mut mapping_context = self
            .broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&request_header, false);

        let rewrite_result = self
            .rewrite_request_for_static_topic_for_consume_offset(&mut request_header, &mut mapping_context)
            .await;
        if let Some(result) = rewrite_result {
            return Ok(Some(result));
        }
        let topic = request_header.topic.as_ref();
        let group = request_header.consumer_group.as_ref();
        let queue_id = request_header.queue_id;
        let offset = request_header.commit_offset;
        let response = RemotingCommand::create_response_command();
        if !self
            .broker_runtime_inner
            .subscription_group_manager()
            .contains_subscription_group(group)
        {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SubscriptionGroupNotExist)
                    .set_remark(format!("subscription group not exist, {group}")),
            ));
        }

        if !self.broker_runtime_inner.topic_config_manager().contains_topic(topic) {
            return Ok(Some(
                response
                    .set_code(ResponseCode::TopicNotExist)
                    .set_remark(format!("topic not exist, {topic}")),
            ));
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
        if self.broker_runtime_inner.broker_config().use_server_side_reset_offset
            && self
                .broker_runtime_inner
                .consumer_offset_manager()
                .has_offset_reset(topic, group, queue_id)
        {
            info!(
                "Update consumer offset is rejected because of previous offset-reset. Group={},Topic={}, QueueId={}, \
                 Offset={}",
                group, topic, queue_id, offset
            );
            return Ok(Some(
                response
                    .set_code(ResponseCode::Success)
                    .set_remark("Offset has been previously reset"),
            ));
        }
        self.broker_runtime_inner.consumer_offset_manager().commit_offset(
            channel.remote_address().to_string().into(),
            group,
            topic,
            queue_id,
            offset,
        );
        Ok(Some(response.set_code(ResponseCode::Success)))
    }

    async fn query_consumer_offset(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut request_header = request.decode_command_custom_header::<QueryConsumerOffsetRequestHeader>()?;
        let mut mapping_context = self
            .broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&request_header, false);
        if let Some(result) = self
            .rewrite_request_for_static_topic(&mut request_header, &mut mapping_context)
            .await
        {
            return Ok(Some(result));
        }
        let offset = self.broker_runtime_inner.consumer_offset_manager().query_offset(
            request_header.consumer_group.as_ref(),
            request_header.topic.as_ref(),
            request_header.queue_id,
        );
        let mut response = RemotingCommand::create_response_command();
        let mut response_header = QueryConsumerOffsetResponseHeader::default();
        if offset >= 0 {
            response_header.offset = Some(offset);
            response = response.set_code(ResponseCode::Success);
        } else {
            let min_offset = self
                .broker_runtime_inner
                .message_store()
                .map(|ms| ms.get_min_offset_in_queue(request_header.topic.as_ref(), request_header.queue_id))
                .unwrap_or(0);
            if let Some(value) = request_header.set_zero_if_not_found {
                if !value {
                    response = response
                        .set_code(ResponseCode::QueryNotFound)
                        .set_remark("Not found, do not set to zero, maybe this group boot first");
                }
            } else if min_offset <= 0
                && self
                    .broker_runtime_inner
                    .message_store()
                    .map(|ms| {
                        ms.check_in_mem_by_consume_offset(request_header.topic.as_ref(), request_header.queue_id, 0, 1)
                    })
                    .unwrap_or(false)
            {
                response_header.offset = Some(0);
                response = response.set_code(ResponseCode::Success);
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
            return Ok(Some(result));
        }
        Ok(Some(response.set_command_custom_header(response_header)))
    }

    /// Rewrite request for static topic when updating consumer offset.
    /// This handles the case where the consumer offset needs to be committed to a different broker
    /// based on the static topic queue mapping.
    async fn rewrite_request_for_static_topic_for_consume_offset(
        &mut self,
        request_header: &mut UpdateConsumerOffsetRequestHeader,
        mapping_context: &mut TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        let mapping_detail = mapping_context.mapping_detail.as_ref()?;

        // Check if current broker is the leader for this queue
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

        let global_offset = request_header.commit_offset;

        // Find the mapping item for this offset
        let mapping_item = TopicQueueMappingUtils::find_logic_queue_mapping_item(
            &mapping_context.mapping_item_list,
            global_offset,
            true,
        )?;

        // Update request header with the physical queue info
        request_header.queue_id = mapping_item.queue_id;
        request_header.set_lo(Some(false));
        request_header.set_broker_name(mapping_item.bname.clone().unwrap_or_default());
        request_header.commit_offset = mapping_item.compute_physical_queue_offset(global_offset);

        // If this broker is the target, let it go through normal processing
        if mapping_detail.topic_queue_mapping_info.bname == mapping_item.bname {
            return None;
        }

        // For non-local broker, forward the request via RPC
        let rpc_request = RpcRequest::new(RequestCode::UpdateConsumerOffset.to_i32(), request_header.clone(), None);
        let rpc_response = self
            .broker_runtime_inner
            .broker_outer_api()
            .rpc_client()
            .invoke(rpc_request, self.broker_runtime_inner.broker_config().forward_timeout)
            .await;

        match rpc_response {
            Ok(response) => {
                if let Some(exception) = response.exception {
                    return Some(RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SystemError,
                        format!("RPC exception: {exception}"),
                    ));
                }
                if ResponseCode::from(response.code) == ResponseCode::Success {
                    Some(RemotingCommand::create_response_command_with_code(
                        ResponseCode::Success,
                    ))
                } else {
                    Some(RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::from(response.code),
                        format!("RPC to broker {:?} returned code {}", mapping_item.bname, response.code),
                    ))
                }
            }
            Err(e) => Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!("RPC forwarding to broker {:?} failed: {e}", mapping_item.bname),
            )),
        }
    }

    async fn rewrite_request_for_static_topic(
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
        let mut offset = -1i64;
        // Clone mapping_item_list to avoid borrow issues
        let mapping_item_list_clone: Vec<_> = mapping_context.mapping_item_list.to_vec();
        let current_broker_name = mapping_detail.topic_queue_mapping_info.bname.clone();

        for mapping_item in mapping_item_list_clone.iter().rev() {
            mapping_context.current_item = Some(mapping_item.clone());
            if mapping_item.bname == current_broker_name {
                offset = self.broker_runtime_inner.consumer_offset_manager().query_offset(
                    request_header.consumer_group.as_ref(),
                    request_header.topic.as_ref(),
                    mapping_item.queue_id,
                );
                if offset >= 0 {
                    break;
                }
            } else {
                // RPC call to remote broker
                let mut query_header = request_header.clone();
                query_header.set_broker_name(mapping_item.bname.clone().unwrap_or_default());
                query_header.queue_id = mapping_item.queue_id;
                query_header.set_lo(Some(false));
                query_header.set_zero_if_not_found = Some(false);

                let rpc_request = RpcRequest::new(RequestCode::QueryConsumerOffset.to_i32(), query_header, None);
                let rpc_response = self
                    .broker_runtime_inner
                    .broker_outer_api()
                    .rpc_client()
                    .invoke(rpc_request, self.broker_runtime_inner.broker_config().forward_timeout)
                    .await;

                match rpc_response {
                    Ok(response) => {
                        if let Some(exception) = response.exception {
                            warn!(
                                "QueryConsumerOffset RPC exception for broker {:?}: {}",
                                mapping_item.bname, exception
                            );
                            return Some(RemotingCommand::create_response_command_with_code_remark(
                                ResponseCode::SystemError,
                                format!("RPC exception: {exception}"),
                            ));
                        }

                        if ResponseCode::from(response.code) == ResponseCode::Success {
                            if let Some(header) = response.get_header::<QueryConsumerOffsetResponseHeader>() {
                                offset = header.offset.unwrap_or(-1);
                                if offset >= 0 {
                                    break;
                                }
                            }
                        } else if ResponseCode::from(response.code) == ResponseCode::QueryNotFound {
                            // Continue to next mapping item
                            continue;
                        } else {
                            warn!(
                                "QueryConsumerOffset RPC to broker {:?} returned unexpected code: {}",
                                mapping_item.bname, response.code
                            );
                            return Some(RemotingCommand::create_response_command_with_code_remark(
                                ResponseCode::SystemError,
                                format!("RPC to broker {:?} returned code {}", mapping_item.bname, response.code),
                            ));
                        }
                    }
                    Err(e) => {
                        warn!(
                            "QueryConsumerOffset RPC to broker {:?} failed: {}",
                            mapping_item.bname, e
                        );
                        return Some(RemotingCommand::create_response_command_with_code_remark(
                            ResponseCode::SystemError,
                            format!("RPC forwarding to broker {:?} failed: {e}", mapping_item.bname),
                        ));
                    }
                }
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
        if let Some(current_item) = mapping_context.current_item.as_ref() {
            response_header.offset =
                Some(current_item.compute_static_queue_offset_strictly(response_header.offset.unwrap_or(0)));
        }
        None
    }
}
