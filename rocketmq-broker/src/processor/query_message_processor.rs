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

use rocketmq_common::common::mix_all::UNIQUE_MSG_QUERY_FLAG;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_remoting::protocol::header::query_message_response_header::QueryMessageResponseHeader;
use rocketmq_remoting::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::MessageStore;

#[derive(Default)]
pub struct QueryMessageProcessor<MS> {
    message_store_config: Arc<MessageStoreConfig>,
    message_store: ArcMut<MS>,
}

impl<MS> QueryMessageProcessor<MS> {
    pub fn new(message_store_config: Arc<MessageStoreConfig>, message_store: ArcMut<MS>) -> Self {
        Self {
            message_store_config,
            message_store,
        }
    }
}

impl<MS> QueryMessageProcessor<MS>
where
    MS: MessageStore + Send + Sync + 'static,
{
    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        match request_code {
            RequestCode::QueryMessage => self.query_message(channel, ctx, request).await,
            RequestCode::ViewMessageById => self.view_message_by_id(channel, ctx, request).await,
            _ => None,
        }
    }

    async fn query_message(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command_with_header(
            QueryMessageResponseHeader::default(),
        );
        let mut request_header = request
            .decode_command_custom_header::<QueryMessageRequestHeader>()
            .unwrap();
        response.set_opaque_mut(request.opaque());
        let is_unique_key = request.ext_fields().unwrap().get(UNIQUE_MSG_QUERY_FLAG);
        if is_unique_key.is_some() && is_unique_key.unwrap() == "true" {
            request_header.max_num = self.message_store_config.default_query_max_num as i32;
        }
        let query_message_result = self
            .message_store
            .query_message(
                request_header.topic.as_ref(),
                request_header.key.as_ref(),
                request_header.max_num,
                request_header.begin_timestamp,
                request_header.end_timestamp,
            )
            .await?;

        let response_header = response
            .read_custom_header_mut::<QueryMessageResponseHeader>()
            .unwrap();
        response_header.index_last_update_phyoffset =
            query_message_result.index_last_update_phyoffset;
        response_header.index_last_update_timestamp =
            query_message_result.index_last_update_timestamp;

        if query_message_result.buffer_total_size > 0 {
            let message_data = query_message_result.get_message_data();
            if let Some(body) = message_data {
                response.set_body_mut_ref(body);
            }
            return Some(response);
        }
        Some(
            response
                .set_code(ResponseCode::QueryNotFound)
                .set_remark("can not find message, maybe time range not correct"),
        )
    }

    async fn view_message_by_id(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<ViewMessageRequestHeader>()
            .unwrap();
        let select_mapped_buffer_result = self
            .message_store
            .select_one_message_by_offset(request_header.offset)
            .await;
        if let Some(result) = select_mapped_buffer_result {
            let message_data = result.get_bytes();
            if let Some(body) = message_data {
                response.set_body_mut_ref(body)
            }
            return Some(response);
        }
        Some(
            response
                .set_code(ResponseCode::SystemError)
                .set_remark(format!(
                    "can not find message by offset: {}",
                    request_header.offset
                )),
        )
    }
}
