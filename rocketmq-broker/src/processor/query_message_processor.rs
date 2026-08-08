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

use std::future::Future;
use std::sync::Arc;
use std::sync::Weak;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::mix_all::UNIQUE_MSG_QUERY_FLAG;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::query_message_request_header::QueryMessageRequestHeader;
use rocketmq_protocol::protocol::header::query_message_response_header::QueryMessageResponseHeader;
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::QueryMessageResult;
use rocketmq_store::SelectMappedBufferResult;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
use rocketmq_store_api::StoreOperation;
use rocketmq_transport::request_code_not_supported_with_remark_and_opaque;
use rocketmq_transport::Channel;
use rocketmq_transport::ConnectionHandlerContext;
use rocketmq_transport::RequestProcessor;
use tracing::info;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;

pub(crate) struct QueryMessageStoreCapability<MS: BrokerReadStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: BrokerReadStore> QueryMessageStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
    }

    fn provider(&self) -> Result<Arc<EscapeBridge<MS>>, StoreError> {
        self.escape_bridge.upgrade().ok_or_else(query_store_unavailable)
    }
}

fn query_store_unavailable() -> StoreError {
    StoreError::new(StoreErrorKind::NotStarted, StoreOperation::Read)
}

/// Narrow storage operations required by [`QueryMessageProcessor`].
pub trait QueryMessageStore: Send + Sync {
    fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> impl Future<Output = Result<Option<QueryMessageResult>, StoreError>> + Send;

    fn select_message_by_offset(&self, offset: i64) -> Result<Option<SelectMappedBufferResult>, StoreError>;
}

impl<MS: BrokerReadStore> QueryMessageStore for QueryMessageStoreCapability<MS> {
    async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Result<Option<QueryMessageResult>, StoreError> {
        self.provider()?
            .query_message_from_store(topic, key, max_num, begin_timestamp, end_timestamp)
            .await
            .map_err(|MessageStoreUnavailable| query_store_unavailable())
    }

    fn select_message_by_offset(&self, offset: i64) -> Result<Option<SelectMappedBufferResult>, StoreError> {
        self.provider()?
            .select_message_from_store(offset)
            .map_err(|MessageStoreUnavailable| query_store_unavailable())
    }
}

impl<MS: BrokerReadStore> Clone for QueryMessageStoreCapability<MS> {
    fn clone(&self) -> Self {
        Self {
            escape_bridge: Weak::clone(&self.escape_bridge),
        }
    }
}

pub struct QueryMessageProcessor<S: QueryMessageStore> {
    default_query_max_num: i32,
    query_store: S,
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

fn unsafe_index_query_remark(query_message_result: &QueryMessageResult) -> Option<String> {
    (!query_message_result.index_query_safe).then(|| {
        format!(
            "index query is unsafe because index safe offset {} is behind confirm offset {}; background Index rebuild \
             may still be in progress",
            query_message_result.index_safe_phyoffset, query_message_result.index_confirm_phyoffset
        )
    })
}

impl<S> RequestProcessor for QueryMessageProcessor<S>
where
    S: QueryMessageStore + Clone,
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
                let response = request_code_not_supported_with_remark_and_opaque(
                    request.code(),
                    format!("QueryMessageProcessor request code {} not supported", request.code()),
                    request.opaque(),
                );
                Ok(Some(response))
            }
        }
    }
}

impl<S: QueryMessageStore> QueryMessageProcessor<S> {
    pub fn new(default_query_max_num: usize, query_store: S) -> Self {
        Self {
            default_query_max_num: default_query_max_num as i32,
            query_store,
        }
    }
}

impl<S: QueryMessageStore + Clone> Clone for QueryMessageProcessor<S> {
    fn clone(&self) -> Self {
        Self {
            default_query_max_num: self.default_query_max_num,
            query_store: self.query_store.clone(),
        }
    }
}

impl<S> QueryMessageProcessor<S>
where
    S: QueryMessageStore + Clone,
{
    pub async fn process_request_shared(
        &self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut processor = self.clone();
        processor.process_request(channel, ctx, request).await
    }

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
        let query_index_type = query_index_type(&request_header, is_unique_key).map(CheetahString::from_slice);
        if is_unique_key
            || query_index_type
                .as_deref()
                .is_some_and(|idx_type| idx_type == MessageConst::INDEX_UNIQUE_TYPE)
        {
            request_header.max_num = self.default_query_max_num;
        }
        let typed_query_key = query_index_type
            .as_deref()
            .map(|idx_type| CheetahString::from_string(format!("{}#{}", idx_type, request_header.key.as_str())));
        let query_key = typed_query_key.as_ref().unwrap_or(&request_header.key);
        let query_message_result = self
            .query_store
            .query_message(
                request_header.topic.as_ref(),
                query_key,
                request_header.max_num,
                request_header.begin_timestamp,
                request_header.end_timestamp,
            )
            .await;
        let query_message_result = match query_message_result {
            Ok(Some(query_message_result)) => query_message_result,
            Ok(None) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::QueryNotFound)
                        .set_remark("query message failed, no result returned"),
                ));
            }
            Err(_) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is none"),
                ));
            }
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
        if let Some(remark) = unsafe_index_query_remark(&query_message_result) {
            return Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(remark)));
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

        let select_mapped_buffer_result = match self.query_store.select_message_by_offset(request_header.offset) {
            Ok(result) => result,
            Err(_) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is none"),
                ));
            }
        };
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
    use rocketmq_store::QueryMessageResult;
    use rocketmq_store::StorePorts;

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

    #[test]
    fn unsafe_index_query_remark_is_absent_for_safe_result() {
        let result = QueryMessageResult::default();

        assert!(unsafe_index_query_remark(&result).is_none());
    }

    #[test]
    fn unsafe_index_query_remark_includes_safe_and_confirm_offsets() {
        let mut result = QueryMessageResult::default();
        result.set_index_query_safety(false, 128, 256);

        let remark = unsafe_index_query_remark(&result).expect("unsafe remark");

        assert!(remark.contains("index safe offset 128"));
        assert!(remark.contains("confirm offset 256"));
        assert!(remark.contains("background Index rebuild"));
    }

    #[test]
    fn query_processor_source_uses_only_the_query_store_capability() {
        let source = include_str!("query_message_processor.rs");

        assert!(!source.contains(concat!("ArcMut<Broker", "RuntimeInner")));
        assert!(!source.contains(concat!("broker_runtime", "_inner")));
        assert!(source.contains("QueryMessageStoreCapability"));
        assert!(source.contains(concat!("Weak<Escape", "Bridge")));
    }

    #[tokio::test]
    async fn query_store_capability_fails_closed_after_provider_shutdown() {
        let capability = QueryMessageStoreCapability::<StorePorts> {
            escape_bridge: Weak::new(),
        };
        let topic = CheetahString::from_static_str("TopicA");
        let key = CheetahString::from_static_str("KeyA");

        let Err(query_error) = capability.query_message(&topic, &key, 32, 0, i64::MAX).await else {
            panic!("closed provider must reject query");
        };
        assert_eq!(StoreErrorKind::NotStarted, query_error.kind());
        let Err(select_error) = capability.select_message_by_offset(0) else {
            panic!("closed provider must reject select");
        };
        assert_eq!(StoreErrorKind::NotStarted, select_error.kind());
    }
}
