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
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::QueryMessageRequest;
use rocketmq_store::QueryMessageResult;
use rocketmq_store::SelectMappedBufferResult;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
use rocketmq_store_api::StoreOperation;
use rocketmq_transport::api::v1::request_code_not_supported_with_factory_remark_and_opaque;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::RequestProcessor;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestProcessorV2;
use tracing::info;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::processor::response_plan::BrokerResponseParts;

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
        request: &QueryMessageRequest,
    ) -> impl Future<Output = Result<Option<QueryMessageResult>, StoreError>> + Send;

    fn select_message_by_offset(&self, offset: i64) -> Result<Option<SelectMappedBufferResult>, StoreError>;
}

impl<MS: BrokerReadStore> QueryMessageStore for QueryMessageStoreCapability<MS> {
    async fn query_message(&self, request: &QueryMessageRequest) -> Result<Option<QueryMessageResult>, StoreError> {
        self.provider()?
            .query_message_from_store(request)
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
    command_factory: RemotingCommandFactory,
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
                MessageConst::INDEX_KEY_TYPE | MessageConst::INDEX_UNIQUE_TYPE | MessageConst::INDEX_TAG_TYPE
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

struct QueryResponseParts {
    head: RemotingCommand,
    body: Option<bytes::Bytes>,
}

impl QueryResponseParts {
    fn command(head: RemotingCommand) -> Self {
        Self { head, body: None }
    }

    fn bytes(head: RemotingCommand, body: bytes::Bytes) -> Self {
        Self { head, body: Some(body) }
    }

    fn into_legacy_command(mut self) -> RemotingCommand {
        if let Some(body) = self.body {
            self.head.set_body_mut_ref(body);
        }
        self.head
    }

    fn into_legacy_command_with_opaque(mut self, opaque: i32) -> RemotingCommand {
        self.head.set_opaque_mut(opaque);
        self.into_legacy_command()
    }

    fn into_handler_outcome(self) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let parts = match self.body {
            Some(body) => BrokerResponseParts::bytes(self.head, body)?,
            None => BrokerResponseParts::command(self.head)?,
        };
        parts.into_handler_outcome()
    }
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
                let response = request_code_not_supported_with_factory_remark_and_opaque(
                    &self.command_factory,
                    request.code(),
                    format!("QueryMessageProcessor request code {} not supported", request.code()),
                    request.opaque(),
                );
                Ok(Some(response))
            }
        }
    }
}

impl<S> RequestProcessorV2 for QueryMessageProcessor<S>
where
    S: QueryMessageStore + Clone + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_v2_command(request.command_mut()).await
    }
}

impl<S: QueryMessageStore> QueryMessageProcessor<S> {
    pub fn new(default_query_max_num: usize, query_store: S) -> Self {
        Self::new_with_factory(
            default_query_max_num,
            query_store,
            application_remoting_command_factory(),
        )
    }

    pub(crate) fn new_with_factory(
        default_query_max_num: usize,
        query_store: S,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        Self {
            command_factory,
            default_query_max_num: default_query_max_num as i32,
            query_store,
        }
    }
}

impl<S: QueryMessageStore + Clone> Clone for QueryMessageProcessor<S> {
    fn clone(&self) -> Self {
        Self {
            command_factory: self.command_factory,
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

    async fn process_v2_command(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let request_code = RequestCode::from(request.code());
        info!("QueryMessageProcessor V2 received request code: {:?}", request_code);
        let parts = match request_code {
            RequestCode::QueryMessage => self.query_message_parts(request).await?,
            RequestCode::ViewMessageById => self.view_message_by_id_parts(request).await?,
            _ => {
                warn!(
                    "QueryMessageProcessor V2 received unknown request code: {:?}",
                    request_code
                );
                QueryResponseParts::command(request_code_not_supported_with_factory_remark_and_opaque(
                    &self.command_factory,
                    request.code(),
                    format!("QueryMessageProcessor request code {} not supported", request.code()),
                    0,
                ))
            }
        };
        parts.into_handler_outcome()
    }

    async fn query_message(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let opaque = request.opaque();
        Ok(Some(
            self.query_message_parts(request)
                .await?
                .into_legacy_command_with_opaque(opaque),
        ))
    }

    async fn query_message_parts(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<QueryResponseParts> {
        let mut response = self
            .command_factory
            .create_success_response_command_with_header(QueryMessageResponseHeader::default());
        let mut request_header = request.decode_command_custom_header::<QueryMessageRequestHeader>()?;
        let Some(ext_fields) = request.ext_fields() else {
            return Ok(QueryResponseParts::command(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("ext fields is none"),
            ));
        };
        let is_unique_key = ext_fields
            .get(UNIQUE_MSG_QUERY_FLAG)
            .is_some_and(|value| value == "true");
        if request_header.index_type.as_deref().is_some_and(|index_type| {
            !matches!(
                index_type,
                MessageConst::INDEX_KEY_TYPE | MessageConst::INDEX_UNIQUE_TYPE | MessageConst::INDEX_TAG_TYPE
            )
        }) {
            return Ok(QueryResponseParts::command(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("indexType must be K, U, or T"),
            ));
        }
        let query_index_type = query_index_type(&request_header, is_unique_key).map(CheetahString::from_slice);
        if is_unique_key
            || query_index_type
                .as_deref()
                .is_some_and(|idx_type| idx_type == MessageConst::INDEX_UNIQUE_TYPE)
        {
            request_header.max_num = self.default_query_max_num;
        }
        let store_request = QueryMessageRequest {
            topic: request_header.topic.clone(),
            key: request_header.key.clone(),
            index_type: query_index_type,
            max_num: request_header.max_num,
            begin: request_header.begin_timestamp,
            end: request_header.end_timestamp,
            last_key: request_header.last_key.clone(),
        };
        let query_message_result = self.query_store.query_message(&store_request).await;
        let query_message_result = match query_message_result {
            Ok(Some(query_message_result)) => query_message_result,
            Ok(None) => {
                return Ok(QueryResponseParts::command(
                    response
                        .set_code(ResponseCode::QueryNotFound)
                        .set_remark("query message failed, no result returned"),
                ));
            }
            Err(_) => {
                return Ok(QueryResponseParts::command(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is none"),
                ));
            }
        };

        let Some(response_header) = response.read_custom_header_mut::<QueryMessageResponseHeader>() else {
            return Err(rocketmq_error::RocketMQError::invariant_violated(
                "query response lost its required response header",
            ));
        };
        response_header.index_last_update_phyoffset = query_message_result.index_last_update_phyoffset;
        response_header.index_last_update_timestamp = query_message_result.index_last_update_timestamp;

        if query_message_result.buffer_total_size > 0 {
            if let Some(body) = query_message_result.get_message_data() {
                return Ok(QueryResponseParts::bytes(response, body));
            }
            return Ok(QueryResponseParts::command(response));
        }
        if let Some(remark) = unsafe_index_query_remark(&query_message_result) {
            return Ok(QueryResponseParts::command(
                response.set_code(ResponseCode::SystemError).set_remark(remark),
            ));
        }
        Ok(QueryResponseParts::command(
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
        Ok(Some(
            self.view_message_by_id_parts(request).await?.into_legacy_command(),
        ))
    }

    async fn view_message_by_id_parts(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<QueryResponseParts> {
        let response = self.command_factory.create_success_response_command();
        let request_header = request.decode_command_custom_header::<ViewMessageRequestHeader>()?;

        let select_mapped_buffer_result = match self.query_store.select_message_by_offset(request_header.offset) {
            Ok(result) => result,
            Err(_) => {
                return Ok(QueryResponseParts::command(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is none"),
                ));
            }
        };
        if let Some(result) = select_mapped_buffer_result {
            return Ok(QueryResponseParts::bytes(response, result.into_owner_bytes()));
        }
        Ok(QueryResponseParts::command(
            response
                .set_code(ResponseCode::SystemError)
                .set_remark(format!("can not find message by offset: {}", request_header.offset)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use bytes::Bytes;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_store::QueryMessageResult;
    use rocketmq_store::StorePorts;
    use rocketmq_transport::api::v1::ServerConfig;
    use rocketmq_transport::api::v2::ResponseBodyKind;
    use rocketmq_transport::api::v2::TransportServerV2;
    use rocketmq_transport::test_support::Connection;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    #[derive(Clone, Default)]
    struct TestQueryStore {
        query_body: Option<Bytes>,
        view_body: Option<Bytes>,
    }

    impl QueryMessageStore for TestQueryStore {
        async fn query_message(
            &self,
            _request: &QueryMessageRequest,
        ) -> Result<Option<QueryMessageResult>, StoreError> {
            let Some(body) = self.query_body.clone() else {
                return Ok(None);
            };
            let selected = SelectMappedBufferResult::from_bytes(0, body).expect("test body length is representable");
            let mut result = QueryMessageResult {
                index_last_update_phyoffset: 17,
                index_last_update_timestamp: 23,
                ..QueryMessageResult::default()
            };
            result.add_message(selected);
            Ok(Some(result))
        }

        fn select_message_by_offset(&self, _offset: i64) -> Result<Option<SelectMappedBufferResult>, StoreError> {
            Ok(self
                .view_body
                .clone()
                .and_then(|body| SelectMappedBufferResult::from_bytes(0, body)))
        }
    }

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
    fn query_index_type_preserves_normal_key_index_type_for_rocksdb_cursor_queries() {
        assert_eq!(
            query_index_type(&header(Some(MessageConst::INDEX_KEY_TYPE)), false),
            Some(MessageConst::INDEX_KEY_TYPE)
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

        let request = QueryMessageRequest::legacy(&topic, &key, 32, 0, i64::MAX);
        let Err(query_error) = capability.query_message(&request).await else {
            panic!("closed provider must reject query");
        };
        assert_eq!(StoreErrorKind::NotStarted, query_error.kind());
        let Err(select_error) = capability.select_message_by_offset(0) else {
            panic!("closed provider must reject select");
        };
        assert_eq!(StoreErrorKind::NotStarted, select_error.kind());
    }

    #[tokio::test]
    async fn v2_query_success_returns_one_owned_bytes_plan() {
        let body = Bytes::from_static(b"query-body");
        let mut processor = QueryMessageProcessor::new(
            64,
            TestQueryStore {
                query_body: Some(body.clone()),
                view_body: None,
            },
        );
        let mut request = RemotingCommand::create_request_command(RequestCode::QueryMessage, header(None));
        request.make_custom_header_to_net();

        let outcome = processor
            .process_v2_command(&mut request)
            .await
            .expect("V2 query response plan");
        let HandlerOutcome::Reply(plan) = outcome else {
            panic!("query success must return an inline reply");
        };

        assert_eq!(ResponseCode::Success as i32, plan.response_code());
        assert_eq!(ResponseBodyKind::Bytes, plan.body_kind());
        assert_eq!(body.len(), plan.body_len());
        assert_eq!(1, plan.body_part_count());
    }

    #[tokio::test]
    async fn v2_query_real_dispatcher_rebinds_the_original_opaque() {
        const ORIGINAL_OPAQUE: i32 = 8_701;

        let owner = RuntimeOwner::new(RuntimeConfig::server_default("broker-query-v2-dispatch-test"))
            .expect("query V2 test runtime owner");
        let server_context = owner.root_context().component("query-v2-server");
        let runner_context = owner.root_context().component("query-v2-runner");
        let processor = QueryMessageProcessor::new(
            64,
            TestQueryStore {
                query_body: Some(Bytes::from_static(b"query-dispatch-body")),
                view_body: None,
            },
        );
        let server = TransportServerV2::new(
            Arc::new(ServerConfig {
                bind_address: "127.0.0.1".to_owned(),
                listen_port: 0,
                ..ServerConfig::default()
            }),
            server_context,
            processor,
        );
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (startup_sender, startup_receiver) = oneshot::channel();
        let (result_sender, result_receiver) = oneshot::channel();
        runner_context
            .spawn_service("query-v2-server", async move {
                let result = server
                    .try_run_with_shutdown_report_and_startup(
                        async move {
                            let _ = shutdown_receiver.await;
                        },
                        startup_sender,
                    )
                    .await;
                let _ = result_sender.send(result);
            })
            .expect("spawn query V2 server");

        let address = startup_receiver
            .await
            .expect("query V2 startup channel")
            .expect("query V2 server startup");
        let mut client = Connection::new(TcpStream::connect(address).await.expect("connect query V2 client"));
        let mut request = RemotingCommand::create_request_command(RequestCode::QueryMessage, header(None))
            .set_opaque(ORIGINAL_OPAQUE);
        request.make_custom_header_to_net();
        client.send_command(request).await.expect("send query V2 request");

        let response = tokio::time::timeout(Duration::from_secs(1), client.receive_command())
            .await
            .expect("query V2 response deadline")
            .expect("query V2 connection remains open")
            .expect("query V2 response frame");
        assert_eq!(ORIGINAL_OPAQUE, response.opaque());
        assert_eq!(ResponseCode::Success as i32, response.code());
        assert_eq!(Some(&Bytes::from_static(b"query-dispatch-body")), response.body());

        client.shutdown().await.expect("shutdown query V2 client");
        let _ = shutdown_sender.send(());
        let report = tokio::time::timeout(Duration::from_secs(2), result_receiver)
            .await
            .expect("query V2 shutdown deadline")
            .expect("query V2 shutdown result channel")
            .expect("query V2 shutdown report");
        assert!(report.is_healthy(), "{}", report.to_json());
        let task_report = owner.shutdown_tasks().await;
        assert!(task_report.is_healthy(), "{}", task_report.to_json());
        let final_report = owner.shutdown_background();
        assert!(final_report.is_healthy(), "{}", final_report.to_json());
    }

    #[tokio::test]
    async fn v2_view_consumes_the_selected_owner_into_one_bytes_plan() {
        let body = Bytes::from_static(b"view-body");
        let mut processor = QueryMessageProcessor::new(
            64,
            TestQueryStore {
                query_body: None,
                view_body: Some(body.clone()),
            },
        );
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ViewMessageById,
            ViewMessageRequestHeader { topic: None, offset: 7 },
        );
        request.make_custom_header_to_net();

        let outcome = processor
            .process_v2_command(&mut request)
            .await
            .expect("V2 view response plan");
        let HandlerOutcome::Reply(plan) = outcome else {
            panic!("view success must return an inline reply");
        };

        assert_eq!(ResponseCode::Success as i32, plan.response_code());
        assert_eq!(ResponseBodyKind::Bytes, plan.body_kind());
        assert_eq!(body.len(), plan.body_len());
    }

    #[tokio::test]
    async fn v2_query_not_found_returns_an_empty_reply_plan() {
        let mut processor = QueryMessageProcessor::new(64, TestQueryStore::default());
        let mut request = RemotingCommand::create_request_command(RequestCode::QueryMessage, header(None));
        request.make_custom_header_to_net();

        let outcome = processor
            .process_v2_command(&mut request)
            .await
            .expect("V2 not-found response plan");
        let HandlerOutcome::Reply(plan) = outcome else {
            panic!("query not found must return an inline reply");
        };

        assert_eq!(ResponseCode::QueryNotFound as i32, plan.response_code());
        assert_eq!(ResponseBodyKind::Empty, plan.body_kind());
        assert_eq!(0, plan.body_len());
    }

    #[tokio::test]
    async fn legacy_query_projection_keeps_opaque_and_body_behavior() {
        let body = Bytes::from_static(b"legacy-query-body");
        let mut processor = QueryMessageProcessor::new(
            64,
            TestQueryStore {
                query_body: Some(body.clone()),
                view_body: None,
            },
        );
        let mut request =
            RemotingCommand::create_request_command(RequestCode::QueryMessage, header(None)).set_opaque(-73);
        request.make_custom_header_to_net();

        let response = processor
            .query_message_parts(&mut request)
            .await
            .expect("legacy query response parts")
            .into_legacy_command_with_opaque(request.opaque());

        assert_eq!(-73, response.opaque());
        assert_eq!(Some(body.as_ref()), response.body().map(Bytes::as_ref));
    }

    #[test]
    fn query_processor_is_a_send_v2_leaf() {
        fn assert_v2_processor<T: RequestProcessorV2 + Clone + Send + Sync + 'static>() {}

        assert_v2_processor::<QueryMessageProcessor<TestQueryStore>>();
    }
}
