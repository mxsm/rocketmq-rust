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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all::UNIQUE_MSG_QUERY_FLAG;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_remoting::protocol::header::query_message_response_header::QueryMessageResponseHeader;
use rocketmq_remoting::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

pub struct QueryMessageProcessor<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

fn query_index_type(request_header: &QueryMessageRequestHeader, is_unique_key: bool) -> Option<&str> {
    request_header
        .index_type
        .as_deref()
        .filter(|idx_type| {
            matches!(
                *idx_type,
                MessageConst::INDEX_UNIQUE_TYPE | MessageConst::INDEX_TAG_TYPE
            )
        })
        .or_else(|| is_unique_key.then_some(MessageConst::INDEX_UNIQUE_TYPE))
}

impl<MS> RequestProcessor for QueryMessageProcessor<MS>
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
        info!("QueryMessageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::QueryMessage | RequestCode::ViewMessageById => {
                self.process_request_inner(channel, ctx, request_code, request).await
            }
            _ => {
                warn!(
                    "QueryMessageProcessor received unknown request code: {:?}",
                    request_code
                );
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("QueryMessageProcessor request code {} not supported", request.code()),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }
}

impl<MS: MessageStore> QueryMessageProcessor<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

impl<MS> QueryMessageProcessor<MS>
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
            RequestCode::QueryMessage => self.query_message(channel, ctx, request).await,
            RequestCode::ViewMessageById => self.view_message_by_id(channel, ctx, request).await,
            _ => Ok(None),
        }
    }

    async fn query_message(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command_with_header(QueryMessageResponseHeader::default());
        let mut request_header = request.decode_command_custom_header::<QueryMessageRequestHeader>()?;
        response.set_opaque_mut(request.opaque());
        let Some(ext_fields) = request.ext_fields() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("ext fields is none"),
            ));
        };
        let is_unique_key = ext_fields
            .get(UNIQUE_MSG_QUERY_FLAG)
            .is_some_and(|value| value == "true");
        let query_index_type = query_index_type(&request_header, is_unique_key)
            .map(|idx_type| CheetahString::from_string(idx_type.to_string()));
        if is_unique_key
            || query_index_type
                .as_deref()
                .is_some_and(|idx_type| idx_type == MessageConst::INDEX_UNIQUE_TYPE)
        {
            request_header.max_num = self.broker_runtime_inner.message_store_config().default_query_max_num as i32;
        }
        let typed_query_key = query_index_type
            .as_deref()
            .map(|idx_type| CheetahString::from_string(format!("{}#{}", idx_type, request_header.key.as_str())));
        let query_key = typed_query_key.as_ref().unwrap_or(&request_header.key);
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

        let Some(query_message_result) = message_store
            .query_message(
                request_header.topic.as_ref(),
                query_key,
                request_header.max_num,
                request_header.begin_timestamp,
                request_header.end_timestamp,
            )
            .await
        else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::QueryNotFound)
                    .set_remark("query message failed, no result returned"),
            ));
        };

        let response_header = response.read_custom_header_mut::<QueryMessageResponseHeader>().unwrap();
        response_header.index_last_update_phyoffset = query_message_result.index_last_update_phyoffset;
        response_header.index_last_update_timestamp = query_message_result.index_last_update_timestamp;

        if query_message_result.buffer_total_size > 0 {
            let message_data = query_message_result.get_message_data();
            if let Some(body) = message_data {
                response.set_body_mut_ref(body);
            }
            return Ok(Some(response));
        }
        Ok(Some(
            response
                .set_code(ResponseCode::QueryNotFound)
                .set_remark("can not find message, maybe time range not correct"),
        ))
    }

    async fn view_message_by_id(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header::<ViewMessageRequestHeader>()?;

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

        let select_mapped_buffer_result = message_store.select_one_message_by_offset(request_header.offset);
        if let Some(result) = select_mapped_buffer_result {
            let message_data = result.get_bytes();
            if let Some(body) = message_data {
                response.set_body_mut_ref(body)
            }
            return Ok(Some(response));
        }
        Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
            "can not find message by offset: {}",
            request_header.offset
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(index_type: Option<&'static str>) -> QueryMessageRequestHeader {
        QueryMessageRequestHeader {
            topic: CheetahString::from_static_str("TopicA"),
            key: CheetahString::from_static_str("KeyA"),
            max_num: 32,
            begin_timestamp: 0,
            end_timestamp: i64::MAX,
            index_type: index_type.map(CheetahString::from_static_str),
            last_key: None,
            topic_request_header: None,
        }
    }

    #[test]
    fn query_index_type_uses_java_index_type_when_present() {
        assert_eq!(
            query_index_type(&header(Some(MessageConst::INDEX_UNIQUE_TYPE)), false),
            Some(MessageConst::INDEX_UNIQUE_TYPE)
        );
        assert_eq!(
            query_index_type(&header(Some(MessageConst::INDEX_TAG_TYPE)), false),
            Some(MessageConst::INDEX_TAG_TYPE)
        );
    }

    #[test]
    fn query_index_type_maps_legacy_unique_flag_to_unique_index() {
        assert_eq!(
            query_index_type(&header(None), true),
            Some(MessageConst::INDEX_UNIQUE_TYPE)
        );
    }

    #[test]
    fn query_index_type_ignores_normal_key_index_type_for_store_key_compatibility() {
        assert_eq!(
            query_index_type(&header(Some(MessageConst::INDEX_KEY_TYPE)), false),
            None
        );
    }
}
