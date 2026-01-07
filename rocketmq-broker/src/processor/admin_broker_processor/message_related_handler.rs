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
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_remoting::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::rpc::rpc_client::RpcClient;
use rocketmq_remoting::rpc::rpc_request::RpcRequest;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;

pub(super) struct MessageRelatedHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> MessageRelatedHandler<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

impl<MS> MessageRelatedHandler<MS>
where
    MS: MessageStore,
{
    pub async fn search_offset_by_timestamp(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let search_offset_request_header = request.decode_command_custom_header::<SearchOffsetRequestHeader>()?;
        let mapping_context = self
            .broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&search_offset_request_header, false);
        let rewrite_result = self
            .rewrite_request_for_static_topic(&search_offset_request_header, mapping_context)
            .await?;
        if rewrite_result.is_some() {
            return Ok(rewrite_result);
        }
        let response = RemotingCommand::create_response_command();
        let message_store = match self.broker_runtime_inner.message_store() {
            Some(store) => store,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is none"),
                ));
            }
        };
        let offset = message_store.get_offset_in_queue_by_time_with_boundary(
            &search_offset_request_header.topic,
            search_offset_request_header.queue_id,
            search_offset_request_header.timestamp,
            search_offset_request_header.boundary_type,
        );
        let response_header = SearchOffsetResponseHeader { offset };

        Ok(Some(response.set_command_custom_header(response_header)))
    }

    async fn rewrite_request_for_static_topic(
        &mut self,
        request_header: &SearchOffsetRequestHeader,
        mapping_context: TopicQueueMappingContext,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if mapping_context.mapping_detail.is_none() {
            return Ok(None);
        }

        let mapping_detail = mapping_context.mapping_detail.as_ref().unwrap();
        let mapping_items = &mapping_context.mapping_item_list;

        if !mapping_context.is_leader() {
            return Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::NotLeaderForQueue).set_remark(
                    format!(
                        "{}-{:?} does not exist in request process of current broker {:?}",
                        mapping_context.topic, mapping_context.global_id, mapping_detail.topic_queue_mapping_info.bname
                    ),
                ),
            ));
        }

        // TO DO should make sure the timestampOfOffset is equal or bigger than the searched
        // timestamp
        let timestamp = request_header.timestamp;
        let mut offset: i64 = -1;

        for item in mapping_items.iter() {
            // Check if logic offset is decided (logic_offset >= 0)
            if item.logic_offset < 0 {
                continue;
            }

            if mapping_detail.topic_queue_mapping_info.bname == item.bname {
                // Local broker - query directly from message store
                if let Some(message_store) = self.broker_runtime_inner.message_store() {
                    let local_offset = message_store.get_offset_in_queue_by_time_with_boundary(
                        &mapping_context.topic,
                        item.queue_id,
                        timestamp,
                        request_header.boundary_type,
                    );
                    if local_offset > 0 {
                        offset = item.compute_static_queue_offset_strictly(local_offset);
                        break;
                    }
                }
            } else {
                // Remote broker - make RPC call
                let mut remote_request_header = SearchOffsetRequestHeader {
                    topic: mapping_context.topic.clone(),
                    queue_id: item.queue_id,
                    timestamp,
                    boundary_type: request_header.boundary_type,
                    topic_request_header: request_header.topic_request_header.clone(),
                };
                remote_request_header.set_lo(Some(false));
                remote_request_header.set_broker_name(item.bname.clone().unwrap_or_default());

                let rpc_request = RpcRequest::new(
                    RequestCode::SearchOffsetByTimestamp.to_i32(),
                    remote_request_header,
                    None,
                );

                let rpc_response = self
                    .broker_runtime_inner
                    .broker_outer_api()
                    .rpc_client()
                    .invoke(rpc_request, self.broker_runtime_inner.broker_config().forward_timeout)
                    .await;

                match rpc_response {
                    Err(e) => {
                        return Ok(Some(
                            RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
                                .set_remark(format!("{e}")),
                        ));
                    }
                    Ok(response) => {
                        match response.get_header::<SearchOffsetResponseHeader>() {
                            None => {
                                continue;
                            }
                            Some(offset_response_header) => {
                                let remote_offset = offset_response_header.offset;
                                // Check if offset is valid
                                if remote_offset < 0 {
                                    continue;
                                }
                                // Check if end offset is decided and offset exceeds it
                                if item.check_if_end_offset_decided() && remote_offset >= item.end_offset {
                                    continue;
                                }
                                offset = item.compute_static_queue_offset_strictly(remote_offset);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(Some(RemotingCommand::create_response_command_with_header(
            SearchOffsetResponseHeader { offset },
        )))
    }
}
