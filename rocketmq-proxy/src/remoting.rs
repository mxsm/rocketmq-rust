//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::prelude::RemotingDeserializable;
use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_remoting::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_remoting::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::parse_request_header;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_remoting::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_remoting::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::remoting_server::rocketmq_tokio_server;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use tokio::net::TcpListener;

use crate::config::ProxyConfig;
use crate::context::ProxyContext;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::processor::ConsumerFilterExpression;
use crate::processor::GetOffsetRequest;
use crate::processor::MessageQueueTarget;
use crate::processor::MessagingProcessor;
use crate::processor::PullMessageRequest;
use crate::processor::QueryAssignmentRequest;
use crate::processor::QueryOffsetPolicy;
use crate::processor::QueryOffsetRequest;
use crate::processor::QueryRouteRequest;
use crate::processor::SendMessageEntry;
use crate::processor::SendMessageRequest;
use crate::service::ResourceIdentity;
use crate::status::ProxyPayloadStatus;

pub struct ProxyRemotingRequestProcessor<P> {
    dispatcher: Arc<ProxyRemotingDispatcher<P>>,
}

impl<P> Clone for ProxyRemotingRequestProcessor<P> {
    fn clone(&self) -> Self {
        Self {
            dispatcher: Arc::clone(&self.dispatcher),
        }
    }
}

impl<P> ProxyRemotingRequestProcessor<P>
where
    P: MessagingProcessor + 'static,
{
    pub fn new(processor: Arc<P>) -> Self {
        Self {
            dispatcher: Arc::new(ProxyRemotingDispatcher::new(processor)),
        }
    }
}

impl<P> RequestProcessor for ProxyRemotingRequestProcessor<P>
where
    P: MessagingProcessor + 'static,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let context = ProxyContext::from_remoting_request(remoting_rpc_name(request.code()), &channel, request);
        Ok(Some(self.dispatcher.dispatch(&context, request).await))
    }

    fn reject_request(&self, _code: i32) -> RejectRequestResponse {
        (false, None)
    }
}

pub async fn serve<P, F>(config: Arc<ProxyConfig>, processor: Arc<P>, shutdown: F) -> ProxyResult<()>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let addr = config.remoting.socket_addr()?;
    let listener = TcpListener::bind(addr).await.map_err(|error| ProxyError::Transport {
        message: format!("proxy remoting server failed to bind {addr}: {error}"),
    })?;
    rocketmq_tokio_server::run(
        listener,
        shutdown,
        ProxyRemotingRequestProcessor::new(processor),
        None,
        Vec::new(),
        None,
    )
    .await;
    Ok(())
}

pub struct ProxyRemotingDispatcher<P> {
    processor: Arc<P>,
}

impl<P> ProxyRemotingDispatcher<P>
where
    P: MessagingProcessor + 'static,
{
    pub fn new(processor: Arc<P>) -> Self {
        Self { processor }
    }

    pub async fn dispatch(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        match RequestCode::from(request.code()) {
            RequestCode::GetRouteinfoByTopic => self.dispatch_query_route(context, request).await,
            RequestCode::QueryAssignment => self.dispatch_query_assignment(context, request).await,
            RequestCode::SendMessage | RequestCode::SendMessageV2 => self.dispatch_send_message(context, request).await,
            RequestCode::SendBatchMessage => unsupported_response(
                request.opaque(),
                "proxy remoting ingress does not support SendBatchMessage yet",
            ),
            RequestCode::PullMessage => self.dispatch_pull_message(context, request).await,
            RequestCode::UpdateConsumerOffset => self.dispatch_update_consumer_offset(context, request).await,
            RequestCode::QueryConsumerOffset => self.dispatch_query_consumer_offset(context, request).await,
            RequestCode::GetMaxOffset => self.dispatch_get_max_offset(context, request).await,
            RequestCode::GetMinOffset => self.dispatch_get_min_offset(context, request).await,
            RequestCode::SearchOffsetByTimestamp => self.dispatch_search_offset(context, request).await,
            _ => unsupported_response(
                request.opaque(),
                format!(
                    "proxy remoting ingress does not support request code {}",
                    request.code()
                ),
            ),
        }
    }

    async fn dispatch_query_route(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetRouteInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode queryRoute header", error),
        };
        let plan = match self
            .processor
            .query_route(
                context,
                QueryRouteRequest {
                    topic: ResourceIdentity::new(String::new(), header.topic.to_string()),
                    endpoints: Vec::new(),
                },
            )
            .await
        {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };

        let body = match plan.route.encode() {
            Ok(body) => body,
            Err(error) => return transport_error_response(request.opaque(), "encode topic route response", error),
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(body)
            .set_opaque(request.opaque())
    }

    async fn dispatch_query_assignment(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let body = match request.body() {
            Some(body) => body,
            None => {
                return RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    "queryAssignment request body is missing",
                )
                .set_opaque(request.opaque())
            }
        };
        let request_body = match QueryAssignmentRequestBody::decode(body.as_ref()) {
            Ok(body) => body,
            Err(error) => {
                return transport_error_response(request.opaque(), "decode queryAssignment request body", error)
            }
        };
        let plan = match self
            .processor
            .query_assignment(
                context,
                QueryAssignmentRequest {
                    topic: ResourceIdentity::new(String::new(), request_body.topic.to_string()),
                    group: ResourceIdentity::new(String::new(), request_body.consumer_group.to_string()),
                    endpoints: Vec::new(),
                },
            )
            .await
        {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };

        let response_body = QueryAssignmentResponseBody {
            message_queue_assignments: plan.assignments.unwrap_or_default().into_iter().collect(),
        };
        let encoded = match response_body.encode() {
            Ok(body) => body,
            Err(error) => return transport_error_response(request.opaque(), "encode queryAssignment response", error),
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }

    async fn dispatch_send_message(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let request_code = RequestCode::from(request.code());
        let header = match parse_request_header(request, request_code) {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode sendMessage header", error),
        };
        if header.is_batch() {
            return unsupported_response(
                request.opaque(),
                "proxy remoting ingress does not support batch send yet",
            );
        }

        let send_request = match build_send_message_request(request, &header) {
            Ok(send_request) => send_request,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };
        let fallback_queue_id = send_request
            .messages
            .first()
            .and_then(|entry| entry.queue_id)
            .unwrap_or_default();
        let plan = match self.processor.send_message(context, send_request).await {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };
        let Some(entry) = plan.entries.into_iter().next() else {
            return transport_error_response(
                request.opaque(),
                "build sendMessage response",
                "processor returned no send results",
            );
        };
        if let Some(send_result) = entry.send_result.as_ref() {
            let response_header = build_send_message_response_header(send_result, fallback_queue_id);
            return response_with_header(
                request.opaque(),
                send_result_to_response_code(send_result),
                response_header,
                None,
                None,
            );
        }

        response_with_code(
            request.opaque(),
            send_payload_status_to_response_code(&entry.status),
            entry.status.message().to_owned(),
        )
    }

    async fn dispatch_pull_message(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<PullMessageRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode pullMessage header", error),
        };
        let pull_request = build_pull_message_request(&header);
        let plan = match self.processor.pull_message(context, pull_request).await {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };

        let body = if plan.messages.is_empty() {
            None
        } else {
            match encode_message_ext_batch(plan.messages.as_slice()) {
                Ok(body) => Some(body),
                Err(error) => return proxy_error_response(request.opaque(), error),
            }
        };
        let response_header = PullMessageResponseHeader {
            suggest_which_broker_id: 0,
            next_begin_offset: plan.next_offset,
            min_offset: plan.min_offset,
            max_offset: plan.max_offset,
            offset_delta: None,
            topic_sys_flag: None,
            group_sys_flag: None,
            forbidden_type: None,
        };
        response_with_header(
            request.opaque(),
            pull_payload_status_to_response_code(&plan.status),
            response_header,
            (!plan.status.is_ok()).then(|| plan.status.message().to_owned()),
            body,
        )
    }

    async fn dispatch_update_consumer_offset(
        &self,
        context: &ProxyContext,
        request: &RemotingCommand,
    ) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<UpdateConsumerOffsetRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return decode_error_response(request.opaque(), "decode updateConsumerOffset header", error);
            }
        };
        let plan = match self
            .processor
            .update_offset(context, build_update_offset_request(&header))
            .await
        {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };
        response_with_header(
            request.opaque(),
            offset_payload_status_to_response_code(&plan.status, ResponseCode::QueryNotFound),
            UpdateConsumerOffsetResponseHeader::default(),
            (!plan.status.is_ok()).then(|| plan.status.message().to_owned()),
            None,
        )
    }

    async fn dispatch_query_consumer_offset(
        &self,
        context: &ProxyContext,
        request: &RemotingCommand,
    ) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<QueryConsumerOffsetRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return decode_error_response(request.opaque(), "decode queryConsumerOffset header", error);
            }
        };
        let plan = match self
            .processor
            .get_offset(context, build_get_offset_request(&header))
            .await
        {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };
        response_with_header(
            request.opaque(),
            offset_payload_status_to_response_code(&plan.status, ResponseCode::QueryNotFound),
            QueryConsumerOffsetResponseHeader {
                offset: plan.status.is_ok().then_some(plan.offset),
            },
            (!plan.status.is_ok()).then(|| plan.status.message().to_owned()),
            None,
        )
    }

    async fn dispatch_get_max_offset(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetMaxOffsetRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode getMaxOffset header", error),
        };
        let plan = match self
            .processor
            .query_offset(
                context,
                build_query_offset_request(
                    topic_identity(&header),
                    header.queue_id,
                    header.broker_name().map(ToString::to_string),
                    crate::processor::QueryOffsetPolicy::End,
                    None,
                ),
            )
            .await
        {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };
        response_with_header(
            request.opaque(),
            offset_payload_status_to_response_code(&plan.status, ResponseCode::QueryNotFound),
            GetMaxOffsetResponseHeader {
                offset: if plan.status.is_ok() { plan.offset } else { 0 },
            },
            (!plan.status.is_ok()).then(|| plan.status.message().to_owned()),
            None,
        )
    }

    async fn dispatch_get_min_offset(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetMinOffsetRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode getMinOffset header", error),
        };
        let plan = match self
            .processor
            .query_offset(
                context,
                build_query_offset_request(
                    topic_identity(&header),
                    header.queue_id,
                    header.broker_name().map(ToString::to_string),
                    crate::processor::QueryOffsetPolicy::Beginning,
                    None,
                ),
            )
            .await
        {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };
        response_with_header(
            request.opaque(),
            offset_payload_status_to_response_code(&plan.status, ResponseCode::QueryNotFound),
            GetMinOffsetResponseHeader {
                offset: if plan.status.is_ok() { plan.offset } else { 0 },
            },
            (!plan.status.is_ok()).then(|| plan.status.message().to_owned()),
            None,
        )
    }

    async fn dispatch_search_offset(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<SearchOffsetRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode searchOffset header", error),
        };
        let plan = match self
            .processor
            .query_offset(
                context,
                build_query_offset_request(
                    topic_identity(&header),
                    header.queue_id,
                    header.broker_name().map(ToString::to_string),
                    crate::processor::QueryOffsetPolicy::Timestamp,
                    Some(header.timestamp),
                ),
            )
            .await
        {
            Ok(plan) => plan,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };
        response_with_header(
            request.opaque(),
            offset_payload_status_to_response_code(&plan.status, ResponseCode::QueryNotFound),
            SearchOffsetResponseHeader {
                offset: if plan.status.is_ok() { plan.offset } else { 0 },
            },
            (!plan.status.is_ok()).then(|| plan.status.message().to_owned()),
            None,
        )
    }
}

fn remoting_rpc_name(code: i32) -> &'static str {
    match RequestCode::from(code) {
        RequestCode::GetRouteinfoByTopic => "RemotingGetRouteinfoByTopic",
        RequestCode::QueryAssignment => "RemotingQueryAssignment",
        RequestCode::SendMessage => "RemotingSendMessage",
        RequestCode::SendMessageV2 => "RemotingSendMessageV2",
        RequestCode::PullMessage => "RemotingPullMessage",
        RequestCode::UpdateConsumerOffset => "RemotingUpdateConsumerOffset",
        RequestCode::QueryConsumerOffset => "RemotingQueryConsumerOffset",
        RequestCode::GetMaxOffset => "RemotingGetMaxOffset",
        RequestCode::GetMinOffset => "RemotingGetMinOffset",
        RequestCode::SearchOffsetByTimestamp => "RemotingSearchOffsetByTimestamp",
        _ => "RemotingRequest",
    }
}

fn build_send_message_request(
    request: &RemotingCommand,
    header: &SendMessageRequestHeader,
) -> crate::error::ProxyResult<SendMessageRequest> {
    let topic = topic_identity(header);
    let body = request.body().cloned().ok_or_else(|| {
        RocketMQError::request_body_invalid(
            "sendMessage",
            format!("sendMessage request body is missing for topic '{}'", header.topic),
        )
    })?;
    let mut message = Message::builder()
        .topic(topic.to_string())
        .body(body)
        .flag_bits(header.flag)
        .build_unchecked();
    let properties = message_decoder::string_to_message_properties(header.properties.as_ref());
    message.set_properties(properties.clone());
    attach_transaction_producer_group(&mut message, header.producer_group.as_str());

    let client_message_id = properties
        .get(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ))
        .map(ToString::to_string)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("remoting-{}", request.opaque()));

    Ok(SendMessageRequest {
        messages: vec![SendMessageEntry {
            topic,
            client_message_id,
            message,
            queue_id: (header.queue_id >= 0).then_some(header.queue_id),
        }],
        timeout: None,
    })
}

fn build_pull_message_request(header: &PullMessageRequestHeader) -> PullMessageRequest {
    let namespace = header.namespace().unwrap_or_default().to_owned();
    PullMessageRequest {
        group: ResourceIdentity::new(namespace.clone(), header.consumer_group.to_string()),
        target: MessageQueueTarget {
            topic: ResourceIdentity::new(namespace, header.topic.to_string()),
            queue_id: header.queue_id,
            broker_name: header.broker_name().map(ToString::to_string),
            broker_addr: None,
        },
        offset: header.queue_offset,
        batch_size: header.max_msg_nums.max(1) as u32,
        filter_expression: ConsumerFilterExpression {
            expression_type: header
                .expression_type
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| ExpressionType::TAG.to_owned()),
            expression: header
                .subscription
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "*".to_owned()),
        },
        long_polling_timeout: std::time::Duration::from_millis(header.suspend_timeout_millis),
    }
}

fn build_update_offset_request(header: &UpdateConsumerOffsetRequestHeader) -> crate::processor::UpdateOffsetRequest {
    let namespace = header.namespace().unwrap_or_default().to_owned();
    crate::processor::UpdateOffsetRequest {
        group: ResourceIdentity::new(namespace.clone(), header.consumer_group.to_string()),
        target: MessageQueueTarget {
            topic: ResourceIdentity::new(namespace, header.topic.to_string()),
            queue_id: header.queue_id,
            broker_name: header.broker_name().map(ToString::to_string),
            broker_addr: None,
        },
        offset: header.commit_offset,
    }
}

fn build_get_offset_request(header: &QueryConsumerOffsetRequestHeader) -> GetOffsetRequest {
    let namespace = header.namespace().unwrap_or_default().to_owned();
    GetOffsetRequest {
        group: ResourceIdentity::new(namespace.clone(), header.consumer_group.to_string()),
        target: MessageQueueTarget {
            topic: ResourceIdentity::new(namespace, header.topic.to_string()),
            queue_id: header.queue_id,
            broker_name: header.broker_name().map(ToString::to_string),
            broker_addr: None,
        },
    }
}

fn build_query_offset_request(
    topic: ResourceIdentity,
    queue_id: i32,
    broker_name: Option<String>,
    policy: QueryOffsetPolicy,
    timestamp_ms: Option<i64>,
) -> QueryOffsetRequest {
    QueryOffsetRequest {
        target: MessageQueueTarget {
            topic,
            queue_id,
            broker_name,
            broker_addr: None,
        },
        policy,
        timestamp_ms,
    }
}

fn topic_identity<T: TopicRequestHeaderTrait>(header: &T) -> ResourceIdentity {
    ResourceIdentity::new(header.namespace().unwrap_or_default(), header.topic().to_string())
}

fn build_send_message_response_header(
    send_result: &rocketmq_client_rust::producer::send_result::SendResult,
    fallback_queue_id: i32,
) -> SendMessageResponseHeader {
    let queue_id = send_result
        .message_queue
        .as_ref()
        .map(|queue| queue.queue_id())
        .unwrap_or(fallback_queue_id);
    SendMessageResponseHeader::new(
        send_result.msg_id.clone().unwrap_or_default(),
        queue_id,
        send_result.queue_offset as i64,
        send_result.transaction_id.as_deref().map(CheetahString::from),
        None,
        send_result.recall_handle().map(CheetahString::from),
    )
}

fn encode_message_ext_batch(messages: &[MessageExt]) -> crate::error::ProxyResult<Bytes> {
    let mut encoded = BytesMut::new();
    for message in messages {
        let body = message_decoder::encode(message, false)?;
        encoded.extend_from_slice(body.as_ref());
    }
    Ok(encoded.freeze())
}

fn attach_transaction_producer_group(message: &mut Message, producer_group: &str) {
    if !is_transaction_prepared(message) {
        return;
    }

    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP),
        CheetahString::from(producer_group),
    );
}

fn is_transaction_prepared(message: &Message) -> bool {
    message
        .property_ref(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_PREPARED,
        ))
        .and_then(|value| value.parse().ok())
        .unwrap_or(false)
}

fn send_result_to_response_code(send_result: &rocketmq_client_rust::producer::send_result::SendResult) -> ResponseCode {
    match send_result.send_status {
        rocketmq_client_rust::producer::send_status::SendStatus::SendOk => ResponseCode::Success,
        rocketmq_client_rust::producer::send_status::SendStatus::FlushDiskTimeout => ResponseCode::FlushDiskTimeout,
        rocketmq_client_rust::producer::send_status::SendStatus::FlushSlaveTimeout => ResponseCode::FlushSlaveTimeout,
        rocketmq_client_rust::producer::send_status::SendStatus::SlaveNotAvailable => ResponseCode::SlaveNotAvailable,
    }
}

fn send_payload_status_to_response_code(status: &ProxyPayloadStatus) -> ResponseCode {
    match payload_code(status) {
        crate::proto::v2::Code::Ok => ResponseCode::Success,
        crate::proto::v2::Code::MasterPersistenceTimeout => ResponseCode::FlushDiskTimeout,
        crate::proto::v2::Code::SlavePersistenceTimeout => ResponseCode::FlushSlaveTimeout,
        crate::proto::v2::Code::HaNotAvailable => ResponseCode::SlaveNotAvailable,
        crate::proto::v2::Code::TopicNotFound => ResponseCode::TopicNotExist,
        crate::proto::v2::Code::ConsumerGroupNotFound => ResponseCode::SubscriptionGroupNotExist,
        crate::proto::v2::Code::Forbidden | crate::proto::v2::Code::Unauthorized => ResponseCode::NoPermission,
        crate::proto::v2::Code::TooManyRequests => ResponseCode::SystemBusy,
        crate::proto::v2::Code::BadRequest
        | crate::proto::v2::Code::IllegalMessageId
        | crate::proto::v2::Code::IllegalMessageKey
        | crate::proto::v2::Code::IllegalMessageTag
        | crate::proto::v2::Code::IllegalMessageGroup
        | crate::proto::v2::Code::IllegalDeliveryTime
        | crate::proto::v2::Code::MessageBodyTooLarge
        | crate::proto::v2::Code::MessagePropertyConflictWithType => ResponseCode::MessageIllegal,
        crate::proto::v2::Code::NotImplemented
        | crate::proto::v2::Code::Unsupported
        | crate::proto::v2::Code::VersionUnsupported => ResponseCode::RequestCodeNotSupported,
        _ => ResponseCode::SystemError,
    }
}

fn pull_payload_status_to_response_code(status: &ProxyPayloadStatus) -> ResponseCode {
    match payload_code(status) {
        crate::proto::v2::Code::Ok => ResponseCode::Success,
        crate::proto::v2::Code::MessageNotFound => ResponseCode::PullNotFound,
        crate::proto::v2::Code::IllegalOffset => ResponseCode::PullOffsetMoved,
        crate::proto::v2::Code::TopicNotFound => ResponseCode::TopicNotExist,
        crate::proto::v2::Code::ConsumerGroupNotFound => ResponseCode::SubscriptionGroupNotExist,
        crate::proto::v2::Code::Forbidden | crate::proto::v2::Code::Unauthorized => ResponseCode::NoPermission,
        crate::proto::v2::Code::TooManyRequests => ResponseCode::SystemBusy,
        crate::proto::v2::Code::BadRequest | crate::proto::v2::Code::IllegalFilterExpression => {
            ResponseCode::InvalidParameter
        }
        _ => ResponseCode::SystemError,
    }
}

fn offset_payload_status_to_response_code(status: &ProxyPayloadStatus, not_found: ResponseCode) -> ResponseCode {
    match payload_code(status) {
        crate::proto::v2::Code::Ok => ResponseCode::Success,
        crate::proto::v2::Code::OffsetNotFound | crate::proto::v2::Code::MessageNotFound => not_found,
        crate::proto::v2::Code::TopicNotFound => ResponseCode::TopicNotExist,
        crate::proto::v2::Code::ConsumerGroupNotFound => ResponseCode::SubscriptionGroupNotExist,
        crate::proto::v2::Code::Forbidden | crate::proto::v2::Code::Unauthorized => ResponseCode::NoPermission,
        crate::proto::v2::Code::TooManyRequests => ResponseCode::SystemBusy,
        crate::proto::v2::Code::BadRequest | crate::proto::v2::Code::IllegalOffset => ResponseCode::InvalidParameter,
        crate::proto::v2::Code::NotImplemented
        | crate::proto::v2::Code::Unsupported
        | crate::proto::v2::Code::VersionUnsupported => ResponseCode::RequestCodeNotSupported,
        _ => ResponseCode::SystemError,
    }
}

fn payload_code(status: &ProxyPayloadStatus) -> crate::proto::v2::Code {
    crate::proto::v2::Code::try_from(status.code()).unwrap_or(crate::proto::v2::Code::InternalError)
}

fn response_with_header<H>(
    opaque: i32,
    code: ResponseCode,
    header: H,
    remark: Option<String>,
    body: Option<Bytes>,
) -> RemotingCommand
where
    H: CommandCustomHeader + Send + Sync + 'static,
{
    let mut response = RemotingCommand::create_response_command_with_header(header).set_code(code);
    if let Some(remark) = remark {
        response = response.set_remark(remark);
    }
    if let Some(body) = body {
        response = response.set_body(body);
    }
    response.make_custom_header_to_net();
    response.set_opaque(opaque)
}

fn response_with_code(opaque: i32, code: ResponseCode, remark: impl Into<String>) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(code, remark.into()).set_opaque(opaque)
}

fn unsupported_response(opaque: i32, remark: impl Into<String>) -> RemotingCommand {
    response_with_code(opaque, ResponseCode::RequestCodeNotSupported, remark)
}

fn decode_error_response(opaque: i32, operation: &'static str, error: RocketMQError) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::SystemError,
        format!("{operation} failed: {error}"),
    )
    .set_opaque(opaque)
}

fn transport_error_response(opaque: i32, operation: &'static str, error: impl std::fmt::Display) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::SystemError,
        format!("{operation} failed: {error}"),
    )
    .set_opaque(opaque)
}

fn proxy_error_response(opaque: i32, error: ProxyError) -> RemotingCommand {
    let code = match &error {
        ProxyError::RocketMQ(RocketMQError::TopicNotExist { .. })
        | ProxyError::RocketMQ(RocketMQError::RouteNotFound { .. }) => ResponseCode::TopicNotExist,
        ProxyError::RocketMQ(RocketMQError::SubscriptionGroupNotExist { .. }) => {
            ResponseCode::SubscriptionGroupNotExist
        }
        ProxyError::RocketMQ(RocketMQError::BrokerPermissionDenied { .. })
        | ProxyError::RocketMQ(RocketMQError::TopicSendingForbidden { .. }) => ResponseCode::NoPermission,
        ProxyError::TooManyRequests { .. } => ResponseCode::SystemBusy,
        ProxyError::IllegalOffset { .. } => ResponseCode::PullOffsetMoved,
        ProxyError::IllegalFilterExpression { .. } => ResponseCode::SubscriptionParseFailed,
        ProxyError::NotImplemented { .. } => ResponseCode::RequestCodeNotSupported,
        ProxyError::IllegalMessageId { .. }
        | ProxyError::IllegalMessageGroup { .. }
        | ProxyError::IllegalDeliveryTime { .. }
        | ProxyError::MessagePropertyConflictWithType { .. } => ResponseCode::MessageIllegal,
        _ => ResponseCode::SystemError,
    };
    RemotingCommand::create_response_command_with_code_remark(code, error.to_string()).set_opaque(opaque)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_client_rust::producer::send_result::SendResult;
    use rocketmq_client_rust::producer::send_status::SendStatus;
    use rocketmq_common::common::boundary_type::BoundaryType;
    use rocketmq_common::common::message::message_enum::MessageRequestMode;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_common::MessageDecoder;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::prelude::RemotingDeserializable;
    use rocketmq_remoting::prelude::RemotingSerializable;
    use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
    use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
    use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
    use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
    use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
    use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
    use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
    use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
    use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
    use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
    use rocketmq_remoting::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
    use rocketmq_remoting::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
    use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

    use super::ProxyRemotingDispatcher;
    use crate::context::ProxyContext;
    use crate::processor::AckMessageRequest;
    use crate::processor::AckMessageResultEntry;
    use crate::processor::ChangeInvisibleDurationPlan;
    use crate::processor::ChangeInvisibleDurationRequest;
    use crate::processor::DefaultMessagingProcessor;
    use crate::processor::ForwardMessageToDeadLetterQueuePlan;
    use crate::processor::ForwardMessageToDeadLetterQueueRequest;
    use crate::processor::GetOffsetPlan;
    use crate::processor::GetOffsetRequest;
    use crate::processor::PullMessagePlan;
    use crate::processor::PullMessageRequest;
    use crate::processor::QueryOffsetPlan;
    use crate::processor::QueryOffsetPolicy;
    use crate::processor::QueryOffsetRequest;
    use crate::processor::RecallMessagePlan;
    use crate::processor::RecallMessageRequest;
    use crate::processor::ReceiveMessagePlan;
    use crate::processor::ReceiveMessageRequest;
    use crate::processor::SendMessageRequest;
    use crate::processor::SendMessageResultEntry;
    use crate::processor::UpdateOffsetPlan;
    use crate::processor::UpdateOffsetRequest;
    use crate::service::AssignmentService;
    use crate::service::ConsumerService;
    use crate::service::DefaultAssignmentService;
    use crate::service::DefaultTransactionService;
    use crate::service::LocalServiceManager;
    use crate::service::MessageService;
    use crate::service::ResourceIdentity;
    use crate::service::StaticMetadataService;
    use crate::service::StaticRouteService;
    use crate::status::ProxyStatusMapper;

    fn test_context() -> ProxyContext {
        ProxyContext::for_internal_client("Remoting", "remoting-client")
    }

    fn test_dispatcher() -> ProxyRemotingDispatcher<DefaultMessagingProcessor> {
        let processor = Arc::new(DefaultMessagingProcessor::new(Arc::new(
            LocalServiceManager::with_services(
                Arc::new(StaticRouteService::default()),
                Arc::new(StaticMetadataService::default()),
                Arc::new(DefaultAssignmentService),
                Arc::new(TestMessageService),
                Arc::new(TestConsumerService),
                Arc::new(DefaultTransactionService),
            ),
        )));
        ProxyRemotingDispatcher::new(processor)
    }

    #[tokio::test]
    async fn dispatch_query_route_returns_success_response() {
        let route_service = StaticRouteService::default();
        route_service.insert(
            ResourceIdentity::new(String::new(), "TopicA"),
            TopicRouteData {
                queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 4, 4, 6, 0)],
                broker_datas: vec![BrokerData::new(
                    CheetahString::from("cluster-a"),
                    CheetahString::from("broker-a"),
                    HashMap::from([(0_u64, CheetahString::from("127.0.0.1:10911"))]),
                    None,
                )],
                ..Default::default()
            },
        );
        let processor = Arc::new(DefaultMessagingProcessor::new(Arc::new(
            LocalServiceManager::with_services(
                Arc::new(route_service),
                Arc::new(StaticMetadataService::default()),
                Arc::new(DefaultAssignmentService),
                Arc::new(TestMessageService),
                Arc::new(TestConsumerService),
                Arc::new(DefaultTransactionService),
            ),
        )));
        let dispatcher = ProxyRemotingDispatcher::new(processor);
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new("TopicA", Some(true)),
        );
        request.make_custom_header_to_net();

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(response.body().is_some());
    }

    #[tokio::test]
    async fn dispatch_query_assignment_returns_supported_response() {
        let route_service = StaticRouteService::default();
        route_service.insert(
            ResourceIdentity::new(String::new(), "TopicA"),
            TopicRouteData::default(),
        );
        let assignment_service = Arc::new(TestAssignmentService);
        let processor = Arc::new(DefaultMessagingProcessor::new(Arc::new(
            LocalServiceManager::with_services(
                Arc::new(route_service),
                Arc::new(StaticMetadataService::default()),
                assignment_service,
                Arc::new(TestMessageService),
                Arc::new(TestConsumerService),
                Arc::new(DefaultTransactionService),
            ),
        )));
        let dispatcher = ProxyRemotingDispatcher::new(processor);
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from("TopicA"),
            consumer_group: CheetahString::from("GroupA"),
            client_id: CheetahString::from("client-a"),
            strategy_name: CheetahString::from("AVG"),
            message_model: rocketmq_remoting::protocol::heartbeat::message_model::MessageModel::Clustering,
        }
        .encode()
        .expect("assignment request body should encode");
        let request = RemotingCommand::new_request(RequestCode::QueryAssignment, body);

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let decoded = QueryAssignmentResponseBody::decode(
            response
                .body()
                .expect("queryAssignment response body should exist")
                .as_ref(),
        )
        .expect("queryAssignment response should decode");
        assert_eq!(decoded.message_queue_assignments.len(), 1);
    }

    #[tokio::test]
    async fn dispatch_send_message_returns_send_response_header() {
        let dispatcher = test_dispatcher();
        let properties = MessageDecoder::message_properties_to_string(&HashMap::from([(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from("client-msg-id"),
        )]));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::SendMessage,
            SendMessageRequestHeader {
                producer_group: CheetahString::from("ProducerA"),
                topic: CheetahString::from("TopicA"),
                default_topic: CheetahString::from("TBW102"),
                default_topic_queue_nums: 4,
                queue_id: 1,
                sys_flag: 0,
                born_timestamp: 1,
                flag: 0,
                properties: Some(properties),
                reconsume_times: None,
                unit_mode: Some(false),
                batch: Some(false),
                max_reconsume_times: None,
                topic_request_header: None,
            },
        )
        .set_body(Bytes::from_static(b"hello"));
        request.make_custom_header_to_net();

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let header = response
            .decode_command_custom_header::<SendMessageResponseHeader>()
            .expect("sendMessage response header should decode");
        assert_eq!(header.msg_id(), "client-msg-id");
        assert_eq!(header.queue_id(), 1);
    }

    #[tokio::test]
    async fn dispatch_pull_message_returns_header_and_body() {
        let dispatcher = test_dispatcher();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::PullMessage,
            PullMessageRequestHeader {
                consumer_group: CheetahString::from("GroupA"),
                topic: CheetahString::from("TopicA"),
                queue_id: 2,
                queue_offset: 7,
                max_msg_nums: 16,
                sys_flag: 0,
                commit_offset: 0,
                suspend_timeout_millis: 500,
                sub_version: 0,
                subscription: Some(CheetahString::from("*")),
                expression_type: Some(CheetahString::from("TAG")),
                max_msg_bytes: None,
                request_source: None,
                proxy_forward_client_id: None,
                topic_request: None,
            },
        );
        request.make_custom_header_to_net();

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let header = response
            .decode_command_custom_header::<PullMessageResponseHeader>()
            .expect("pullMessage response header should decode");
        assert_eq!(header.next_begin_offset, 8);
        assert_eq!(header.min_offset, 0);
        assert_eq!(header.max_offset, 1024);

        let messages = MessageDecoder::decodes_batch(
            &mut response.body().cloned().expect("pull response body should exist"),
            true,
            true,
        );
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].topic().as_str(), "TopicA");
        assert_eq!(messages[0].queue_offset(), 7);
        assert_eq!(messages[0].body().as_deref(), Some(&b"hello"[..]));
    }

    #[tokio::test]
    async fn dispatch_update_and_query_consumer_offset_return_expected_headers() {
        let dispatcher = test_dispatcher();

        let mut update_request = RemotingCommand::create_request_command(
            RequestCode::UpdateConsumerOffset,
            UpdateConsumerOffsetRequestHeader {
                consumer_group: CheetahString::from("GroupA"),
                topic: CheetahString::from("TopicA"),
                queue_id: 0,
                commit_offset: 88,
                topic_request_header: None,
            },
        );
        update_request.make_custom_header_to_net();
        let update_response = dispatcher.dispatch(&test_context(), &update_request).await;
        assert_eq!(ResponseCode::from(update_response.code()), ResponseCode::Success);

        let mut query_request = RemotingCommand::create_request_command(
            RequestCode::QueryConsumerOffset,
            QueryConsumerOffsetRequestHeader::new("GroupA", "TopicA", 0),
        );
        query_request.make_custom_header_to_net();
        let query_response = dispatcher.dispatch(&test_context(), &query_request).await;
        assert_eq!(ResponseCode::from(query_response.code()), ResponseCode::Success);
        let query_header = query_response
            .decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()
            .expect("queryConsumerOffset response header should decode");
        assert_eq!(query_header.offset, Some(123));
    }

    #[tokio::test]
    async fn dispatch_max_and_search_offset_return_expected_headers() {
        let dispatcher = test_dispatcher();

        let mut max_request = RemotingCommand::create_request_command(
            RequestCode::GetMaxOffset,
            GetMaxOffsetRequestHeader {
                topic: CheetahString::from("TopicA"),
                queue_id: 1,
                committed: true,
                topic_request_header: None,
            },
        );
        max_request.make_custom_header_to_net();
        let max_response = dispatcher.dispatch(&test_context(), &max_request).await;
        assert_eq!(ResponseCode::from(max_response.code()), ResponseCode::Success);
        let max_header = max_response
            .decode_command_custom_header::<GetMaxOffsetResponseHeader>()
            .expect("getMaxOffset response header should decode");
        assert_eq!(max_header.offset, 2048);

        let mut search_request = RemotingCommand::create_request_command(
            RequestCode::SearchOffsetByTimestamp,
            SearchOffsetRequestHeader {
                topic: CheetahString::from("TopicA"),
                queue_id: 1,
                timestamp: 99,
                boundary_type: BoundaryType::Lower,
                topic_request_header: None,
            },
        );
        search_request.make_custom_header_to_net();
        let search_response = dispatcher.dispatch(&test_context(), &search_request).await;
        assert_eq!(ResponseCode::from(search_response.code()), ResponseCode::Success);
        let search_header = search_response
            .decode_command_custom_header::<SearchOffsetResponseHeader>()
            .expect("searchOffset response header should decode");
        assert_eq!(search_header.offset, 99);
    }

    struct TestAssignmentService;

    #[async_trait]
    impl AssignmentService for TestAssignmentService {
        async fn query_assignment(
            &self,
            _context: &ProxyContext,
            _topic: &ResourceIdentity,
            _group: &ResourceIdentity,
            _endpoints: &[crate::context::ResolvedEndpoint],
        ) -> crate::error::ProxyResult<Option<Vec<MessageQueueAssignment>>> {
            Ok(Some(vec![MessageQueueAssignment {
                message_queue: Some(MessageQueue::from_parts("TopicA", CheetahString::from("broker-a"), 0)),
                mode: MessageRequestMode::Pull,
                attachments: None,
            }]))
        }
    }

    struct TestMessageService;

    #[async_trait]
    impl MessageService for TestMessageService {
        async fn send_message(
            &self,
            _context: &ProxyContext,
            request: &SendMessageRequest,
        ) -> crate::error::ProxyResult<Vec<SendMessageResultEntry>> {
            Ok(request
                .messages
                .iter()
                .map(|entry| {
                    let queue_id = entry.queue_id.unwrap_or_default();
                    let send_result = SendResult::new(
                        SendStatus::SendOk,
                        Some(CheetahString::from(entry.client_message_id.as_str())),
                        None,
                        Some(MessageQueue::from_parts(
                            entry.topic.to_string(),
                            CheetahString::from("broker-a"),
                            queue_id,
                        )),
                        7,
                    );
                    SendMessageResultEntry {
                        status: ProxyStatusMapper::ok_payload(),
                        send_result: Some(send_result),
                    }
                })
                .collect())
        }

        async fn recall_message(
            &self,
            _context: &ProxyContext,
            request: &RecallMessageRequest,
        ) -> crate::error::ProxyResult<RecallMessagePlan> {
            Ok(RecallMessagePlan {
                status: ProxyStatusMapper::ok_payload(),
                message_id: request.recall_handle.clone(),
            })
        }
    }

    struct TestConsumerService;

    #[async_trait]
    impl ConsumerService for TestConsumerService {
        async fn receive_message(
            &self,
            _context: &ProxyContext,
            _request: &ReceiveMessageRequest,
        ) -> crate::error::ProxyResult<ReceiveMessagePlan> {
            Err(crate::error::ProxyError::not_implemented("test receive"))
        }

        async fn pull_message(
            &self,
            _context: &ProxyContext,
            request: &PullMessageRequest,
        ) -> crate::error::ProxyResult<PullMessagePlan> {
            let mut message = MessageExt::default();
            message.set_topic(CheetahString::from(request.target.topic.to_string()));
            message.set_body(Bytes::from_static(b"hello"));
            message.set_msg_id(CheetahString::from("pull-msg-id"));
            message.set_queue_id(request.target.queue_id);
            message.set_queue_offset(request.offset);
            Ok(PullMessagePlan {
                status: ProxyStatusMapper::ok_payload(),
                next_offset: request.offset + 1,
                min_offset: 0,
                max_offset: 1024,
                messages: vec![message],
            })
        }

        async fn ack_message(
            &self,
            _context: &ProxyContext,
            _request: &AckMessageRequest,
        ) -> crate::error::ProxyResult<Vec<AckMessageResultEntry>> {
            Err(crate::error::ProxyError::not_implemented("test ack"))
        }

        async fn forward_message_to_dead_letter_queue(
            &self,
            _context: &ProxyContext,
            _request: &ForwardMessageToDeadLetterQueueRequest,
        ) -> crate::error::ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
            Err(crate::error::ProxyError::not_implemented("test dlq"))
        }

        async fn change_invisible_duration(
            &self,
            _context: &ProxyContext,
            _request: &ChangeInvisibleDurationRequest,
        ) -> crate::error::ProxyResult<ChangeInvisibleDurationPlan> {
            Err(crate::error::ProxyError::not_implemented("test change invisible"))
        }

        async fn update_offset(
            &self,
            _context: &ProxyContext,
            _request: &UpdateOffsetRequest,
        ) -> crate::error::ProxyResult<UpdateOffsetPlan> {
            Ok(UpdateOffsetPlan {
                status: ProxyStatusMapper::ok_payload(),
            })
        }

        async fn get_offset(
            &self,
            _context: &ProxyContext,
            _request: &GetOffsetRequest,
        ) -> crate::error::ProxyResult<GetOffsetPlan> {
            Ok(GetOffsetPlan {
                status: ProxyStatusMapper::ok_payload(),
                offset: 123,
            })
        }

        async fn query_offset(
            &self,
            _context: &ProxyContext,
            request: &QueryOffsetRequest,
        ) -> crate::error::ProxyResult<QueryOffsetPlan> {
            let offset = match request.policy {
                QueryOffsetPolicy::Beginning => 0,
                QueryOffsetPolicy::End => 2048,
                QueryOffsetPolicy::Timestamp => request.timestamp_ms.unwrap_or_default(),
            };
            Ok(QueryOffsetPlan {
                status: ProxyStatusMapper::ok_payload(),
                offset,
            })
        }
    }
}
