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
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::rpc::rpc_request::RpcRequest;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::log_file::MessageStore;

use crate::processor::admin_broker_processor::Inner;

#[derive(Clone)]
pub(super) struct OffsetRequestHandler {
    inner: Inner,
}

impl OffsetRequestHandler {
    pub fn new(inner: Inner) -> Self {
        Self { inner }
    }
}

impl OffsetRequestHandler {
    pub async fn get_max_offset(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut request_header =
            request.decode_command_custom_header::<GetMaxOffsetRequestHeader>()?;
        let mapping_context = self
            .inner
            .topic_queue_mapping_manager
            .build_topic_queue_mapping_context(&request_header, false);

        let rewrite_result =
            self.rewrite_request_for_static_topic(&mut request_header, mapping_context);
        if rewrite_result.is_some() {
            return rewrite_result;
        }

        let offset = self
            .inner
            .default_message_store
            .get_max_offset_in_queue(request_header.topic.as_str(), request_header.queue_id);
        let response_header = GetMaxOffsetResponseHeader { offset };
        Some(RemotingCommand::create_response_command_with_header(
            response_header,
        ))
    }

    pub async fn get_min_offset(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<GetMaxOffsetRequestHeader>()?;
        unimplemented!("getMinOffset")
    }

    fn rewrite_request_for_static_topic(
        &mut self,
        request_header: &mut GetMaxOffsetRequestHeader,
        mapping_context: TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        if mapping_context.mapping_detail.is_none() {
            return None;
        }
        let mapping_detail = mapping_context.mapping_detail.as_ref()?;
        if !mapping_context.is_leader() {
            return Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::NotLeaderForQueue)
                    .set_remark(Some(format!(
                        "{}-{:?} does not exit in request process of current broker {:?}",
                        mapping_context.topic,
                        mapping_context.global_id,
                        mapping_detail.topic_queue_mapping_info.bname
                    ))),
            );
        }

        let max_item = TopicQueueMappingUtils::find_logic_queue_mapping_item(
            &mapping_context.mapping_item_list,
            i64::MAX,
            true,
        )?;
        request_header.with_broker_name(max_item.bname.clone()?);
        request_header.with_lo(Some(false));
        request_header.queue_id = max_item.queue_id;
        let max_physical_offset = if max_item.bname == mapping_detail.topic_queue_mapping_info.bname
        {
            self.inner
                .default_message_store
                .get_max_offset_in_queue(mapping_context.topic.as_str(), max_item.queue_id)
        } else {
            /*let rpcRequest = RpcRequest {
                code: 0,
                header: request_header.clone(),
                body: None,
            };*/
            unimplemented!("rewriteRequestForStaticTopic")
        };
        Some(RemotingCommand::create_response_command_with_header(
            GetMaxOffsetResponseHeader {
                offset: max_item.compute_static_queue_offset_strictly(max_physical_offset),
            },
        ))
    }
}
