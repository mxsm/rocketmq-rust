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

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::entity::ClientGroup;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all::IS_SUB_CHANGE;
use rocketmq_common::common::mix_all::IS_SUPPORT_HEART_BEAT_V2;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::prelude::RemotingDeserializable;
use rocketmq_remoting::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_remoting::protocol::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody;
use rocketmq_remoting::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_remoting::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_remoting::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_remoting::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_remoting::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
use rocketmq_remoting::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
use rocketmq_remoting::protocol::header::lock_batch_mq_request_header::LockBatchMqRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::parse_request_header;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_remoting::protocol::header::notify_unsubscribe_lite_request_header::NotifyUnsubscribeLiteRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_remoting::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_remoting::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_remoting::protocol::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
use rocketmq_remoting::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetResponseHeader;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::remoting_server::rocketmq_tokio_server;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use tokio::net::TcpListener;

use crate::auth::is_auth_error;
use crate::auth::ProxyAuthRuntime;
use crate::cluster::initialize_client_instance;
use crate::config::ProxyConfig;
use crate::config::ProxyMode;
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
use crate::session::ClientSessionRegistry;
use crate::status::ProxyPayloadStatus;

#[async_trait]
pub trait ProxyRemotingBackend: Send + Sync {
    async fn process(&self, request: RemotingCommand) -> ProxyResult<RemotingCommand>;
}

pub struct ProxyRemotingRequestProcessor<P> {
    dispatcher: Arc<ProxyRemotingDispatcher<P>>,
    auth_runtime: Option<ProxyAuthRuntime>,
}

impl<P> Clone for ProxyRemotingRequestProcessor<P> {
    fn clone(&self) -> Self {
        Self {
            dispatcher: Arc::clone(&self.dispatcher),
            auth_runtime: self.auth_runtime.clone(),
        }
    }
}

impl<P> ProxyRemotingRequestProcessor<P>
where
    P: MessagingProcessor + 'static,
{
    pub fn new(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        auth_runtime: Option<ProxyAuthRuntime>,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    ) -> Self {
        Self {
            dispatcher: Arc::new(ProxyRemotingDispatcher::new(
                config,
                processor,
                sessions,
                remoting_backend,
            )),
            auth_runtime,
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
        let mut context = ProxyContext::from_remoting_request(remoting_rpc_name(request.code()), &channel, request);
        if let Some(auth_runtime) = &self.auth_runtime {
            let source_ip = channel.remote_address().to_string();
            match auth_runtime
                .authenticate_remoting(request, Some(channel.channel_id()), Some(source_ip.as_str()))
                .await
            {
                Ok(Some(principal)) => context.set_authenticated_principal(principal),
                Ok(None) => {}
                Err(error) => return Ok(Some(auth_error_response(request.opaque(), error))),
            }
            if let Err(error) = auth_runtime.authorize_remoting(&channel, request).await {
                return Ok(Some(auth_error_response(request.opaque(), error)));
            }
        }
        self.dispatcher.bind_remoting_channel_if_heartbeat(&channel, request);
        Ok(Some(self.dispatcher.dispatch(&context, request).await))
    }

    fn reject_request(&self, _code: i32) -> RejectRequestResponse {
        (false, None)
    }
}

pub async fn serve<P, F>(
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    auth_runtime: Option<ProxyAuthRuntime>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    shutdown: F,
) -> ProxyResult<()>
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
        ProxyRemotingRequestProcessor::new(config, processor, sessions, auth_runtime, remoting_backend),
        None,
        Vec::new(),
        None,
    )
    .await;
    Ok(())
}

pub struct ProxyRemotingDispatcher<P> {
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
}

impl<P> ProxyRemotingDispatcher<P>
where
    P: MessagingProcessor + 'static,
{
    pub fn new(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    ) -> Self {
        Self {
            config,
            processor,
            sessions,
            remoting_backend,
        }
    }

    pub async fn dispatch(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        match RequestCode::from(request.code()) {
            RequestCode::GetRouteinfoByTopic => self.dispatch_query_route(context, request).await,
            RequestCode::QueryAssignment => self.dispatch_query_assignment(context, request).await,
            RequestCode::SendMessage | RequestCode::SendMessageV2 | RequestCode::SendBatchMessage => {
                self.dispatch_send_message(context, request).await
            }
            RequestCode::HeartBeat => self.dispatch_heartbeat(context, request).await,
            RequestCode::UnregisterClient => self.dispatch_unregister_client(request).await,
            RequestCode::GetConsumerListByGroup => self.dispatch_get_consumer_list_by_group(request).await,
            RequestCode::NotifyConsumerIdsChanged => self.dispatch_notify_consumer_ids_changed(request).await,
            RequestCode::NotifyUnsubscribeLite => self.dispatch_notify_unsubscribe_lite(request).await,
            RequestCode::LockBatchMq => self.dispatch_lock_batch_mq(request).await,
            RequestCode::UnlockBatchMq => self.dispatch_unlock_batch_mq(request).await,
            RequestCode::CheckClientConfig => self.dispatch_check_client_config(request).await,
            RequestCode::PullMessage | RequestCode::LitePullMessage => {
                self.dispatch_pull_message(context, request).await
            }
            RequestCode::UpdateConsumerOffset => self.dispatch_update_consumer_offset(context, request).await,
            RequestCode::QueryConsumerOffset => self.dispatch_query_consumer_offset(context, request).await,
            RequestCode::GetMaxOffset => self.dispatch_get_max_offset(context, request).await,
            RequestCode::GetMinOffset => self.dispatch_get_min_offset(context, request).await,
            RequestCode::SearchOffsetByTimestamp => self.dispatch_search_offset(context, request).await,
            RequestCode::GetBrokerLiteInfo => self.dispatch_get_broker_lite_info(request).await,
            RequestCode::GetParentTopicInfo => self.dispatch_get_parent_topic_info(request).await,
            RequestCode::GetLiteTopicInfo => self.dispatch_get_lite_topic_info(request).await,
            RequestCode::GetLiteGroupInfo => self.dispatch_get_lite_group_info(request).await,
            _ => unsupported_response(
                request.opaque(),
                format!(
                    "proxy remoting ingress does not support request code {}",
                    request.code()
                ),
            ),
        }
    }

    fn bind_remoting_channel_if_heartbeat(&self, channel: &Channel, request: &RemotingCommand) {
        if RequestCode::from(request.code()) != RequestCode::HeartBeat {
            return;
        }
        let Some(body) = request.body() else {
            return;
        };
        let Ok(heartbeat) = SerdeJsonUtils::from_json_bytes::<HeartbeatData>(body.as_ref()) else {
            return;
        };
        if !heartbeat.client_id.is_empty() {
            self.sessions
                .bind_remoting_channel(heartbeat.client_id.as_str(), channel.clone());
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

    async fn dispatch_heartbeat(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        if let Err(error) = request.decode_command_custom_header::<HeartbeatRequestHeader>() {
            return decode_error_response(request.opaque(), "decode heartbeat header", error);
        }

        let Some(body) = request.body() else {
            return transport_error_response(
                request.opaque(),
                "decode heartbeat body",
                "heartbeat request body is missing",
            );
        };
        let heartbeat = match SerdeJsonUtils::from_json_bytes::<HeartbeatData>(body.as_ref()) {
            Ok(heartbeat) => heartbeat,
            Err(error) => return transport_error_response(request.opaque(), "decode heartbeat body", error),
        };

        let changed = self.sessions.update_membership_from_remoting_heartbeat(
            context,
            heartbeat.client_id.as_str(),
            heartbeat
                .producer_data_set
                .iter()
                .map(|producer| producer.group_name.to_string())
                .collect(),
            heartbeat
                .consumer_data_set
                .iter()
                .map(|consumer| consumer.group_name.to_string())
                .collect(),
        );

        let mut response = RemotingCommand::create_response_command().set_opaque(request.opaque());
        response.add_ext_field(IS_SUPPORT_HEART_BEAT_V2, true.to_string());
        response.add_ext_field(IS_SUB_CHANGE, changed.to_string());
        response
    }

    async fn dispatch_unregister_client(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<UnregisterClientRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode unregisterClient header", error),
        };
        self.sessions.unregister_client_groups(
            header.client_id.as_str(),
            header.producer_group.as_deref(),
            header.consumer_group.as_deref(),
        );
        RemotingCommand::create_response_command().set_opaque(request.opaque())
    }

    async fn dispatch_get_consumer_list_by_group(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetConsumerListByGroupRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return decode_error_response(request.opaque(), "decode getConsumerListByGroup header", error);
            }
        };
        let client_ids = self.sessions.consumer_client_ids(header.consumer_group.as_str());
        if client_ids.is_empty() {
            return response_with_code(
                request.opaque(),
                ResponseCode::SystemError,
                format!("no consumer for this group, {}", header.consumer_group),
            );
        }

        let body = match (GetConsumerListByGroupResponseBody {
            consumer_id_list: client_ids.into_iter().map(CheetahString::from).collect(),
        })
        .encode()
        {
            Ok(body) => body,
            Err(error) => {
                return transport_error_response(request.opaque(), "encode getConsumerListByGroup response", error);
            }
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(body)
            .set_opaque(request.opaque())
    }

    async fn dispatch_notify_consumer_ids_changed(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<NotifyConsumerIdsChangedRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return decode_error_response(request.opaque(), "decode notifyConsumerIdsChanged header", error);
            }
        };
        let client_ids = self.sessions.consumer_client_ids(header.consumer_group.as_str());
        if client_ids.is_empty() {
            return response_with_code(
                request.opaque(),
                ResponseCode::ConsumerNotOnline,
                format!("no consumer for this group, {}", header.consumer_group),
            );
        }
        let mut forwarded = 0usize;
        for client_id in client_ids {
            let Some(mut client_channel) = self.sessions.remoting_channel(client_id.as_str()) else {
                continue;
            };
            if let Err(error) = client_channel
                .channel_inner_mut()
                .send_oneway(request.clone(), 10)
                .await
            {
                return response_with_code(
                    request.opaque(),
                    ResponseCode::SystemError,
                    format!(
                        "forward notifyConsumerIdsChanged failed for group {}, clientId {}: {}",
                        header.consumer_group, client_id, error
                    ),
                );
            }
            forwarded += 1;
        }
        if forwarded == 0 {
            return response_with_code(
                request.opaque(),
                ResponseCode::ConsumerNotOnline,
                format!(
                    "no remoting channel for consumer group {}, clients are online",
                    header.consumer_group
                ),
            );
        }
        RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque())
    }

    async fn dispatch_notify_unsubscribe_lite(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<NotifyUnsubscribeLiteRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return decode_error_response(request.opaque(), "decode notifyUnsubscribeLite header", error);
            }
        };
        if !self
            .sessions
            .consumer_client_ids(header.consumer_group.as_str())
            .iter()
            .any(|client_id| client_id.as_str() == header.client_id.as_str())
        {
            return response_with_code(
                request.opaque(),
                ResponseCode::ConsumerNotOnline,
                format!(
                    "no matching lite consumer for group {}, clientId {}",
                    header.consumer_group, header.client_id
                ),
            );
        }
        let Some(mut client_channel) = self.sessions.remoting_channel(header.client_id.as_str()) else {
            return response_with_code(
                request.opaque(),
                ResponseCode::ConsumerNotOnline,
                format!(
                    "no remoting channel for lite consumer group {}, clientId {}",
                    header.consumer_group, header.client_id
                ),
            );
        };
        if let Err(error) = client_channel
            .channel_inner_mut()
            .send_oneway(request.clone(), 100)
            .await
        {
            return response_with_code(
                request.opaque(),
                ResponseCode::SystemError,
                format!(
                    "forward notifyUnsubscribeLite failed for group {}, clientId {}: {}",
                    header.consumer_group, header.client_id, error
                ),
            );
        }
        RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque())
    }

    async fn dispatch_lock_batch_mq(&self, request: &RemotingCommand) -> RemotingCommand {
        if let Some(response) = self.dispatch_via_local_backend(request).await {
            return response;
        }
        if let Err(error) = request.decode_command_custom_header::<LockBatchMqRequestHeader>() {
            return decode_error_response(request.opaque(), "decode lockBatchMQ header", error);
        }
        let Some(body) = request.body() else {
            return transport_error_response(
                request.opaque(),
                "decode lockBatchMQ body",
                "lockBatchMQ request body is missing",
            );
        };
        let request_body = match LockBatchRequestBody::decode(body.as_ref()) {
            Ok(body) => body,
            Err(error) => return transport_error_response(request.opaque(), "decode lockBatchMQ body", error),
        };
        let response_body = match self.lock_batch_mq_cluster(request_body).await {
            Ok(response_body) => response_body,
            Err(error) => return proxy_error_response(request.opaque(), error),
        };
        let body = match response_body.encode() {
            Ok(body) => body,
            Err(error) => return transport_error_response(request.opaque(), "encode lockBatchMQ response", error),
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(body)
            .set_opaque(request.opaque())
    }

    async fn dispatch_unlock_batch_mq(&self, request: &RemotingCommand) -> RemotingCommand {
        if let Some(response) = self.dispatch_via_local_backend(request).await {
            return response;
        }
        if let Err(error) = request.decode_command_custom_header::<UnlockBatchMqRequestHeader>() {
            return decode_error_response(request.opaque(), "decode unlockBatchMQ header", error);
        }
        let Some(body) = request.body() else {
            return transport_error_response(
                request.opaque(),
                "decode unlockBatchMQ body",
                "unlockBatchMQ request body is missing",
            );
        };
        let request_body = match UnlockBatchRequestBody::decode(body.as_ref()) {
            Ok(body) => body,
            Err(error) => return transport_error_response(request.opaque(), "decode unlockBatchMQ body", error),
        };
        if let Err(error) = self.unlock_batch_mq_cluster(request_body).await {
            return proxy_error_response(request.opaque(), error);
        }
        RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque())
    }

    async fn dispatch_check_client_config(&self, request: &RemotingCommand) -> RemotingCommand {
        RemotingCommand::create_response_command_with_code(ResponseCode::Success).set_opaque(request.opaque())
    }

    async fn dispatch_send_message(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let request_code = RequestCode::from(request.code());
        let header = match parse_request_header(request, request_code) {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode sendMessage header", error),
        };

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

    async fn dispatch_get_broker_lite_info(&self, request: &RemotingCommand) -> RemotingCommand {
        let body = self.build_broker_lite_info_response();
        let encoded = match body.encode() {
            Ok(body) => body,
            Err(error) => {
                return transport_error_response(request.opaque(), "encode getBrokerLiteInfo response", error)
            }
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }

    async fn dispatch_get_parent_topic_info(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetParentTopicInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode getParentTopicInfo header", error),
        };
        let namespace = header
            .rpc
            .as_ref()
            .and_then(|rpc| rpc.namespace.as_ref())
            .map(ToString::to_string)
            .unwrap_or_default();
        let topic = ResourceIdentity::new(namespace, header.topic.to_string());
        let Some(body) = self.build_parent_topic_info_response(&topic) else {
            return response_with_code(
                request.opaque(),
                ResponseCode::QueryNotFound,
                format!("parent topic '{}' has no lite subscriptions", topic),
            );
        };
        let encoded = match body.encode() {
            Ok(body) => body,
            Err(error) => {
                return transport_error_response(request.opaque(), "encode getParentTopicInfo response", error)
            }
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }

    async fn dispatch_get_lite_topic_info(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetLiteTopicInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode getLiteTopicInfo header", error),
        };
        let topic = ResourceIdentity::new(String::new(), header.parent_topic.to_string());
        let Some(body) = self.build_lite_topic_info_response(&topic, header.lite_topic.as_str()) else {
            return response_with_code(
                request.opaque(),
                ResponseCode::QueryNotFound,
                format!(
                    "lite topic '{}' under '{}' has no subscribers",
                    header.lite_topic, header.parent_topic
                ),
            );
        };
        let encoded = match body.encode() {
            Ok(body) => body,
            Err(error) => return transport_error_response(request.opaque(), "encode getLiteTopicInfo response", error),
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }

    async fn dispatch_get_lite_group_info(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetLiteGroupInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => return decode_error_response(request.opaque(), "decode getLiteGroupInfo header", error),
        };
        let namespace = header
            .rpc
            .as_ref()
            .and_then(|rpc| rpc.namespace.as_ref())
            .map(ToString::to_string)
            .unwrap_or_default();
        let group = ResourceIdentity::new(namespace, header.group.to_string());
        let Some(body) = self.build_lite_group_info_response(&group, header.lite_topic.as_str()) else {
            return response_with_code(
                request.opaque(),
                ResponseCode::QueryNotFound,
                format!(
                    "group '{}' has no lite subscription for '{}'",
                    header.group, header.lite_topic
                ),
            );
        };
        let encoded = match body.encode() {
            Ok(body) => body,
            Err(error) => return transport_error_response(request.opaque(), "encode getLiteGroupInfo response", error),
        };
        RemotingCommand::create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }

    async fn dispatch_via_local_backend(&self, request: &RemotingCommand) -> Option<RemotingCommand> {
        if !matches!(self.config.mode, ProxyMode::Local) {
            return None;
        }
        let backend = self.remoting_backend.as_ref()?;
        Some(match backend.process(request.clone()).await {
            Ok(response) => response.set_opaque(request.opaque()),
            Err(error) => proxy_error_response(request.opaque(), error),
        })
    }

    async fn lock_batch_mq_cluster(&self, request_body: LockBatchRequestBody) -> ProxyResult<LockBatchResponseBody> {
        ensure_cluster_mode(&self.config)?;
        let mut response_body = LockBatchResponseBody::default();
        if request_body.mq_set.is_empty() {
            return Ok(response_body);
        }
        let (broker_name, topic_name) = single_broker_and_topic(&request_body.mq_set)?;
        let mut client = initialize_client_instance(self.config.cluster.clone()).await?;
        let broker_addr = resolve_remoting_broker_addr(
            &mut client,
            broker_name.as_str(),
            topic_name.as_str(),
            request_body.only_this_broker,
        )
        .await?;
        let locked = client
            .get_mq_client_api_impl()
            .lock_batch_mq(
                broker_addr.as_str(),
                request_body,
                self.config.cluster.mq_client_api_timeout_ms,
            )
            .await?;
        response_body.lock_ok_mq_set = locked;
        Ok(response_body)
    }

    async fn unlock_batch_mq_cluster(&self, request_body: UnlockBatchRequestBody) -> ProxyResult<()> {
        ensure_cluster_mode(&self.config)?;
        if request_body.mq_set.is_empty() {
            return Ok(());
        }
        let (broker_name, topic_name) = single_broker_and_topic(&request_body.mq_set)?;
        let mut client = initialize_client_instance(self.config.cluster.clone()).await?;
        let broker_addr = resolve_remoting_broker_addr(
            &mut client,
            broker_name.as_str(),
            topic_name.as_str(),
            request_body.only_this_broker,
        )
        .await?;
        client
            .get_mq_client_api_impl()
            .unlock_batch_mq(
                &broker_addr,
                request_body,
                self.config.cluster.mq_client_api_timeout_ms,
                false,
            )
            .await
            .map_err(Into::into)
    }

    fn build_broker_lite_info_response(&self) -> GetBrokerLiteInfoResponseBody {
        let mut body = GetBrokerLiteInfoResponseBody::new();
        body.set_store_type(CheetahString::from("proxy"));
        let all_lite_subscriptions = self.sessions.all_lite_subscriptions();
        let topic_meta = build_lite_topic_meta(all_lite_subscriptions.as_slice());
        let group_meta = build_lite_group_meta(all_lite_subscriptions.as_slice());
        let unique_lmq_count = topic_meta.values().copied().sum::<i32>();
        body.set_current_lmq_num(unique_lmq_count);
        body.set_lite_subscription_count(self.sessions.lite_subscription_count() as i32);
        body.set_topic_meta(topic_meta);
        body.set_group_meta(group_meta);
        body
    }

    fn build_parent_topic_info_response(&self, topic: &ResourceIdentity) -> Option<GetParentTopicInfoResponseBody> {
        let subscriptions = self.sessions.lite_subscriptions_for_topic(topic);
        if subscriptions.is_empty() {
            return None;
        }

        let mut body = GetParentTopicInfoResponseBody::new();
        body.set_topic(CheetahString::from(topic.to_string()));
        body.set_groups(
            subscriptions
                .iter()
                .map(|subscription| CheetahString::from(subscription.group.to_string()))
                .collect(),
        );
        let unique_lite_topics = unique_lite_topics(subscriptions.as_slice());
        body.set_lmq_num(unique_lite_topics.len() as i32);
        body.set_lite_topic_count(unique_lite_topics.len() as i32);
        Some(body)
    }

    fn build_lite_topic_info_response(
        &self,
        topic: &ResourceIdentity,
        lite_topic: &str,
    ) -> Option<GetLiteTopicInfoResponseBody> {
        let subscriptions = self.sessions.lite_subscriptions_for_topic_and_lite(topic, lite_topic);
        if subscriptions.is_empty() {
            return None;
        }

        let mut body = GetLiteTopicInfoResponseBody::new();
        body.with_parent_topic(CheetahString::from(topic.to_string()))
            .with_lite_topic(CheetahString::from(lite_topic))
            .with_subscriber(
                subscriptions
                    .iter()
                    .map(|subscription| {
                        ClientGroup::from_parts(
                            CheetahString::from(subscription.client_id.clone()),
                            CheetahString::from(subscription.group.to_string()),
                        )
                    })
                    .collect(),
            )
            .with_sharding_to_broker(false);
        Some(body)
    }

    fn build_lite_group_info_response(
        &self,
        group: &ResourceIdentity,
        lite_topic: &str,
    ) -> Option<GetLiteGroupInfoResponseBody> {
        let subscriptions = self.sessions.lite_subscriptions_for_group_and_lite(group, lite_topic);
        let first = subscriptions.first()?;

        let mut body = GetLiteGroupInfoResponseBody::new();
        body.with_group(CheetahString::from(group.to_string()))
            .with_parent_topic(CheetahString::from(first.topic.to_string()))
            .with_lite_topic(CheetahString::from(lite_topic))
            .with_total_lag_count(0)
            .with_earliest_unconsumed_timestamp(0);
        Some(body)
    }
}

fn remoting_rpc_name(code: i32) -> &'static str {
    match RequestCode::from(code) {
        RequestCode::GetRouteinfoByTopic => "RemotingGetRouteinfoByTopic",
        RequestCode::QueryAssignment => "RemotingQueryAssignment",
        RequestCode::SendMessage => "RemotingSendMessage",
        RequestCode::SendMessageV2 => "RemotingSendMessageV2",
        RequestCode::SendBatchMessage => "RemotingSendBatchMessage",
        RequestCode::HeartBeat => "RemotingHeartBeat",
        RequestCode::UnregisterClient => "RemotingUnregisterClient",
        RequestCode::GetConsumerListByGroup => "RemotingGetConsumerListByGroup",
        RequestCode::NotifyConsumerIdsChanged => "RemotingNotifyConsumerIdsChanged",
        RequestCode::NotifyUnsubscribeLite => "RemotingNotifyUnsubscribeLite",
        RequestCode::LockBatchMq => "RemotingLockBatchMq",
        RequestCode::UnlockBatchMq => "RemotingUnlockBatchMq",
        RequestCode::CheckClientConfig => "RemotingCheckClientConfig",
        RequestCode::PullMessage => "RemotingPullMessage",
        RequestCode::LitePullMessage => "RemotingLitePullMessage",
        RequestCode::UpdateConsumerOffset => "RemotingUpdateConsumerOffset",
        RequestCode::QueryConsumerOffset => "RemotingQueryConsumerOffset",
        RequestCode::GetMaxOffset => "RemotingGetMaxOffset",
        RequestCode::GetMinOffset => "RemotingGetMinOffset",
        RequestCode::SearchOffsetByTimestamp => "RemotingSearchOffsetByTimestamp",
        RequestCode::GetBrokerLiteInfo => "RemotingGetBrokerLiteInfo",
        RequestCode::GetParentTopicInfo => "RemotingGetParentTopicInfo",
        RequestCode::GetLiteTopicInfo => "RemotingGetLiteTopicInfo",
        RequestCode::GetLiteGroupInfo => "RemotingGetLiteGroupInfo",
        _ => "RemotingRequest",
    }
}

fn ensure_cluster_mode(config: &ProxyConfig) -> ProxyResult<()> {
    if matches!(config.mode, ProxyMode::Cluster) {
        Ok(())
    } else {
        Err(ProxyError::not_implemented(
            "remoting request is only supported through the broker-backed local passthrough or cluster backend",
        ))
    }
}

fn single_broker_and_topic(mq_set: &HashSet<MessageQueue>) -> ProxyResult<(CheetahString, CheetahString)> {
    let mut broker_name: Option<CheetahString> = None;
    let mut topic_name: Option<CheetahString> = None;
    for mq in mq_set {
        if let Some(existing) = broker_name.as_ref() {
            if existing != mq.broker_name() {
                return Err(ProxyError::invalid_metadata(
                    "lock/unlock batch request must target a single broker",
                ));
            }
        } else {
            broker_name = Some(CheetahString::from(mq.broker_name()));
        }
        topic_name.get_or_insert_with(|| CheetahString::from(mq.topic()));
    }
    Ok((
        broker_name.ok_or_else(|| ProxyError::invalid_metadata("message queue set must not be empty"))?,
        topic_name.ok_or_else(|| ProxyError::invalid_metadata("message queue set must not be empty"))?,
    ))
}

async fn resolve_remoting_broker_addr(
    client: &mut rocketmq_client_rust::factory::mq_client_instance::MQClientInstance,
    broker_name: &str,
    topic: &str,
    only_this_broker: bool,
) -> ProxyResult<CheetahString> {
    let mut result = client
        .find_broker_address_in_subscribe(&CheetahString::from(broker_name), 0, only_this_broker)
        .await;
    if result.is_none() {
        client
            .update_topic_route_info_from_name_server_topic(&CheetahString::from(topic))
            .await;
        result = client
            .find_broker_address_in_subscribe(&CheetahString::from(broker_name), 0, only_this_broker)
            .await;
    }
    result
        .map(|found| found.broker_addr)
        .ok_or_else(|| RocketMQError::BrokerNotFound {
            name: broker_name.to_owned(),
        })
        .map_err(Into::into)
}

fn build_lite_topic_meta(subscriptions: &[crate::session::LiteSubscriptionSnapshot]) -> HashMap<CheetahString, i32> {
    subscriptions
        .iter()
        .fold(HashMap::<String, HashSet<String>>::new(), |mut acc, subscription| {
            acc.entry(subscription.topic.to_string())
                .or_default()
                .extend(subscription.lite_topic_set.iter().cloned());
            acc
        })
        .into_iter()
        .map(|(topic, lite_topics)| (CheetahString::from(topic), lite_topics.len() as i32))
        .collect()
}

fn build_lite_group_meta(
    subscriptions: &[crate::session::LiteSubscriptionSnapshot],
) -> HashMap<CheetahString, HashSet<CheetahString>> {
    subscriptions
        .iter()
        .fold(HashMap::<String, HashSet<String>>::new(), |mut acc, subscription| {
            acc.entry(subscription.topic.to_string())
                .or_default()
                .insert(subscription.group.to_string());
            acc
        })
        .into_iter()
        .map(|(topic, groups)| {
            (
                CheetahString::from(topic),
                groups.into_iter().map(CheetahString::from).collect(),
            )
        })
        .collect()
}

fn unique_lite_topics(subscriptions: &[crate::session::LiteSubscriptionSnapshot]) -> HashSet<String> {
    subscriptions
        .iter()
        .flat_map(|subscription| subscription.lite_topic_set.iter().cloned())
        .collect()
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
    let queue_id = (header.queue_id >= 0).then_some(header.queue_id);

    if header.is_batch() {
        let mut batch_body = body;
        let messages = message_decoder::decode_messages(&mut batch_body);
        if messages.is_empty() {
            return Err(RocketMQError::request_body_invalid(
                "sendBatchMessage",
                format!("sendBatchMessage request body is empty for topic '{}'", header.topic),
            )
            .into());
        }

        let entries = messages
            .into_iter()
            .enumerate()
            .map(|(index, mut message)| {
                message.set_topic(CheetahString::from(topic.to_string()));
                attach_transaction_producer_group(&mut message, header.producer_group.as_str());
                let client_message_id = message
                    .property_ref(&CheetahString::from_static_str(
                        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                    ))
                    .map(ToString::to_string)
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| format!("remoting-batch-{}-{index}", request.opaque()));

                SendMessageEntry {
                    topic: topic.clone(),
                    client_message_id,
                    message,
                    queue_id,
                }
            })
            .collect();

        return Ok(SendMessageRequest {
            messages: entries,
            timeout: None,
        });
    }

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
            queue_id,
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
        ProxyError::RocketMQ(error) if is_auth_error(error) => ResponseCode::NoPermission,
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

fn auth_error_response(opaque: i32, error: ProxyError) -> RemotingCommand {
    proxy_error_response(opaque, error)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_auth::authentication::acl_signer;
    use rocketmq_auth::authentication::enums::subject_type::SubjectType;
    use rocketmq_auth::authentication::enums::user_status::UserStatus;
    use rocketmq_auth::authentication::enums::user_type::UserType;
    use rocketmq_auth::authentication::model::user::User;
    use rocketmq_auth::authorization::enums::decision::Decision;
    use rocketmq_auth::authorization::model::acl::Acl;
    use rocketmq_auth::authorization::model::policy::Policy;
    use rocketmq_auth::authorization::model::resource::Resource;
    use rocketmq_client_rust::producer::send_result::SendResult;
    use rocketmq_client_rust::producer::send_status::SendStatus;
    use rocketmq_common::common::action::Action;
    use rocketmq_common::common::boundary_type::BoundaryType;
    use rocketmq_common::common::entity::ClientGroup;
    use rocketmq_common::common::message::message_decoder;
    use rocketmq_common::common::message::message_enum::MessageRequestMode;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
    use rocketmq_common::common::message::message_single::Message;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_common::MessageDecoder;
    use rocketmq_error::RocketMQError;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::local::LocalRequestHarness;
    use rocketmq_remoting::prelude::RemotingDeserializable;
    use rocketmq_remoting::prelude::RemotingSerializable;
    use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
    use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
    use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
    use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
    use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_remoting::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
    use rocketmq_remoting::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
    use rocketmq_remoting::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
    use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
    use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
    use rocketmq_remoting::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
    use rocketmq_remoting::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
    use rocketmq_remoting::protocol::header::lock_batch_mq_request_header::LockBatchMqRequestHeader;
    use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
    use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
    use rocketmq_remoting::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
    use rocketmq_remoting::protocol::header::notify_unsubscribe_lite_request_header::NotifyUnsubscribeLiteRequestHeader;
    use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
    use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
    use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
    use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
    use rocketmq_remoting::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
    use rocketmq_remoting::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
    use rocketmq_remoting::protocol::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
    use rocketmq_remoting::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
    use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
    use rocketmq_remoting::protocol::heartbeat::consumer_data::ConsumerData;
    use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
    use rocketmq_remoting::protocol::heartbeat::producer_data::ProducerData;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_remoting::runtime::processor::RequestProcessor;
    use tokio::time::timeout;

    use super::ProxyRemotingBackend;
    use super::ProxyRemotingDispatcher;
    use super::ProxyRemotingRequestProcessor;
    use crate::auth::ProxyAuthRuntime;
    use crate::config::ProxyAuthConfig;
    use crate::config::ProxyConfig;
    use crate::config::ProxyMode;
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
    use crate::session::build_lite_subscription_sync_request;
    use crate::session::ClientSessionRegistry;
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
        ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            ClientSessionRegistry::default(),
            None,
        )
    }

    #[derive(Default)]
    struct TestRemotingBackend {
        seen_codes: Mutex<Vec<i32>>,
    }

    #[async_trait]
    impl ProxyRemotingBackend for TestRemotingBackend {
        async fn process(&self, request: RemotingCommand) -> crate::error::ProxyResult<RemotingCommand> {
            self.seen_codes
                .lock()
                .expect("backend mutex poisoned")
                .push(request.code());
            Ok(RemotingCommand::create_response_command_with_code(
                ResponseCode::Success,
            ))
        }
    }

    async fn test_auth_runtime(authentication_enabled: bool, authorization_enabled: bool) -> ProxyAuthRuntime {
        ProxyAuthRuntime::from_proxy_config(&ProxyAuthConfig {
            authentication_enabled,
            authorization_enabled,
            auth_config_path: format!("target/proxy-remoting-auth-tests-{}", uuid::Uuid::new_v4()),
            ..ProxyAuthConfig::default()
        })
        .await
        .expect("auth runtime should initialize")
        .expect("auth runtime should be enabled")
    }

    async fn seed_normal_user(auth_runtime: &ProxyAuthRuntime, username: &str, password: &str) {
        let mut user = User::of_with_type(username, password, UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        auth_runtime.create_user(user).await.expect("user should be created");
    }

    async fn allow_topic_action(auth_runtime: &ProxyAuthRuntime, username: &str, topic: &str, action: Action) {
        auth_runtime
            .create_acl(Acl::of(
                username,
                SubjectType::User,
                Policy::of(vec![Resource::of_topic(topic)], vec![action], None, Decision::Allow),
            ))
            .await
            .expect("acl should be created");
    }

    fn sign_remoting_command(mut command: RemotingCommand, username: &str, secret: &str) -> RemotingCommand {
        command.ensure_ext_fields_initialized();
        command.add_ext_field("AccessKey", username);
        let mut fields = command
            .ext_fields()
            .cloned()
            .expect("ext fields should be initialized")
            .into_iter()
            .filter(|(key, _)| key.as_str() != "Signature")
            .collect::<Vec<_>>();
        fields.sort_by(|left, right| left.0.cmp(&right.0));
        let mut content = Vec::new();
        for (key, value) in fields {
            content.extend_from_slice(key.as_bytes());
            content.extend_from_slice(value.as_bytes());
        }
        if let Some(body) = command.body() {
            content.extend_from_slice(body);
        }
        let signature = acl_signer::cal_signature(content.as_slice(), secret).expect("signature should be generated");
        command.add_ext_field("Signature", signature);
        command
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
        let dispatcher = ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            ClientSessionRegistry::default(),
            None,
        );
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
        let dispatcher = ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            ClientSessionRegistry::default(),
            None,
        );
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
    async fn dispatch_send_batch_message_returns_first_batch_response_header() {
        let dispatcher = test_dispatcher();
        let properties = MessageDecoder::message_properties_to_string(&HashMap::from([(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from("batch-msg-id-1"),
        )]));
        let mut first = Message::builder()
            .topic("TopicA")
            .body_slice(b"hello-1")
            .build_unchecked();
        first.set_properties(message_decoder::string_to_message_properties(Some(&properties)));
        let second = Message::builder()
            .topic("TopicA")
            .body_slice(b"hello-2")
            .build_unchecked();
        let batch =
            rocketmq_common::common::message::message_batch::MessageBatch::generate_from_vec(vec![first, second])
                .expect("message batch should build");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::SendBatchMessage,
            SendMessageRequestHeader {
                producer_group: CheetahString::from("ProducerA"),
                topic: CheetahString::from("TopicA"),
                default_topic: CheetahString::from("TBW102"),
                default_topic_queue_nums: 4,
                queue_id: 2,
                sys_flag: 0,
                born_timestamp: 1,
                flag: 0,
                properties: None,
                reconsume_times: None,
                unit_mode: Some(false),
                batch: Some(true),
                max_reconsume_times: None,
                topic_request_header: None,
            },
        )
        .set_body(batch.encode());
        request.make_custom_header_to_net();

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let header = response
            .decode_command_custom_header::<SendMessageResponseHeader>()
            .expect("sendBatchMessage response header should decode");
        assert_eq!(header.msg_id(), "batch-msg-id-1");
        assert_eq!(header.queue_id(), 2);
    }

    #[tokio::test]
    async fn dispatch_heartbeat_and_get_consumer_list_tracks_membership() {
        let sessions = ClientSessionRegistry::default();
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
        let dispatcher = ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            sessions.clone(),
            None,
        );

        let heartbeat = HeartbeatData {
            client_id: CheetahString::from("client-a"),
            producer_data_set: HashSet::from([ProducerData {
                group_name: CheetahString::from("ProducerA"),
            }]),
            consumer_data_set: HashSet::from([ConsumerData {
                group_name: CheetahString::from("GroupA"),
                ..ConsumerData::default()
            }]),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };
        let mut heartbeat_request =
            RemotingCommand::create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
                .set_body(heartbeat.encode().expect("heartbeat should encode"));
        heartbeat_request.make_custom_header_to_net();

        let heartbeat_response = dispatcher.dispatch(&test_context(), &heartbeat_request).await;
        assert_eq!(ResponseCode::from(heartbeat_response.code()), ResponseCode::Success);
        assert_eq!(sessions.consumer_client_ids("GroupA"), vec!["client-a".to_owned()]);

        let mut get_request = RemotingCommand::create_request_command(
            RequestCode::GetConsumerListByGroup,
            GetConsumerListByGroupRequestHeader {
                consumer_group: CheetahString::from("GroupA"),
                rpc: None,
            },
        );
        get_request.make_custom_header_to_net();

        let response = dispatcher.dispatch(&test_context(), &get_request).await;
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let decoded = rocketmq_remoting::protocol::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody::decode(
            response.body().expect("consumer list body should exist").as_ref(),
        )
        .expect("consumer list body should decode");
        assert_eq!(decoded.consumer_id_list, vec![CheetahString::from("client-a")]);
    }

    #[tokio::test]
    async fn process_request_heartbeat_binds_remoting_channel() {
        let sessions = ClientSessionRegistry::default();
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
        let mut request_processor = ProxyRemotingRequestProcessor::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            sessions.clone(),
            None,
            None,
        );
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let heartbeat = HeartbeatData {
            client_id: CheetahString::from_static_str("client-a"),
            producer_data_set: HashSet::new(),
            consumer_data_set: HashSet::from([ConsumerData {
                group_name: CheetahString::from_static_str("GroupA"),
                ..ConsumerData::default()
            }]),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };
        let mut heartbeat_request =
            RemotingCommand::create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
                .set_body(heartbeat.encode().expect("heartbeat should encode"));
        heartbeat_request.make_custom_header_to_net();

        let response = request_processor
            .process_request(harness.channel(), harness.context(), &mut heartbeat_request)
            .await
            .expect("heartbeat should process")
            .expect("heartbeat should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(sessions.consumer_client_ids("GroupA"), vec!["client-a".to_owned()]);
        assert!(sessions.remoting_channel("client-a").is_some());
    }

    #[tokio::test]
    async fn dispatch_unregister_client_removes_membership() {
        let sessions = ClientSessionRegistry::default();
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
        let dispatcher = ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            sessions.clone(),
            None,
        );

        let heartbeat = HeartbeatData {
            client_id: CheetahString::from("client-a"),
            producer_data_set: HashSet::new(),
            consumer_data_set: HashSet::from([ConsumerData {
                group_name: CheetahString::from("GroupA"),
                ..ConsumerData::default()
            }]),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };
        let heartbeat_request =
            RemotingCommand::create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
                .set_body(heartbeat.encode().expect("heartbeat should encode"));
        let mut heartbeat_request = heartbeat_request;
        heartbeat_request.make_custom_header_to_net();
        let _ = dispatcher.dispatch(&test_context(), &heartbeat_request).await;
        assert_eq!(sessions.consumer_client_ids("GroupA"), vec!["client-a".to_owned()]);

        let mut unregister_request = RemotingCommand::create_request_command(
            RequestCode::UnregisterClient,
            UnregisterClientRequestHeader {
                client_id: CheetahString::from("client-a"),
                producer_group: None,
                consumer_group: Some(CheetahString::from("GroupA")),
                rpc_request_header: None,
            },
        );
        unregister_request.make_custom_header_to_net();
        let response = dispatcher.dispatch(&test_context(), &unregister_request).await;
        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(sessions.consumer_client_ids("GroupA").is_empty());
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

    #[tokio::test]
    async fn dispatch_notify_consumer_ids_changed_requires_online_consumers() {
        let dispatcher = test_dispatcher();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::NotifyConsumerIdsChanged,
            NotifyConsumerIdsChangedRequestHeader {
                consumer_group: CheetahString::from("GroupA"),
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::ConsumerNotOnline);
    }

    #[tokio::test]
    async fn dispatch_notify_consumer_ids_changed_forwards_to_remoting_consumers() {
        let sessions = ClientSessionRegistry::default();
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
        let dispatcher = ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            sessions.clone(),
            None,
        );

        let heartbeat = HeartbeatData {
            client_id: CheetahString::from_static_str("client-a"),
            producer_data_set: HashSet::new(),
            consumer_data_set: HashSet::from([ConsumerData {
                group_name: CheetahString::from_static_str("GroupA"),
                ..ConsumerData::default()
            }]),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };
        let mut heartbeat_request =
            RemotingCommand::create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
                .set_body(heartbeat.encode().expect("heartbeat should encode"));
        heartbeat_request.make_custom_header_to_net();
        let heartbeat_response = dispatcher.dispatch(&test_context(), &heartbeat_request).await;
        assert_eq!(ResponseCode::from(heartbeat_response.code()), ResponseCode::Success);

        let mut missing_channel_request = RemotingCommand::create_request_command(
            RequestCode::NotifyConsumerIdsChanged,
            NotifyConsumerIdsChangedRequestHeader {
                consumer_group: CheetahString::from_static_str("GroupA"),
                rpc_request_header: None,
            },
        );
        missing_channel_request.make_custom_header_to_net();
        let missing_channel_response = dispatcher.dispatch(&test_context(), &missing_channel_request).await;
        assert_eq!(
            ResponseCode::from(missing_channel_response.code()),
            ResponseCode::ConsumerNotOnline
        );

        let mut harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        sessions.bind_remoting_channel("client-a", harness.channel());

        let mut request = RemotingCommand::create_request_command(
            RequestCode::NotifyConsumerIdsChanged,
            NotifyConsumerIdsChangedRequestHeader {
                consumer_group: CheetahString::from_static_str("GroupA"),
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let forwarded = timeout(std::time::Duration::from_secs(3), harness.receive_response())
            .await
            .expect("forwarded notify consumer ids changed should arrive before timeout")
            .expect("forwarded notify consumer ids changed should decode")
            .expect("forwarded notify consumer ids changed should be present");
        assert_eq!(
            RequestCode::from(forwarded.code()),
            RequestCode::NotifyConsumerIdsChanged
        );
        assert!(forwarded.is_oneway_rpc());
        let forwarded_header = forwarded
            .decode_command_custom_header::<NotifyConsumerIdsChangedRequestHeader>()
            .expect("forwarded notify header should decode");
        assert_eq!(forwarded_header.consumer_group, "GroupA");
    }

    #[tokio::test]
    async fn dispatch_notify_unsubscribe_lite_validates_matching_session() {
        let sessions = ClientSessionRegistry::default();
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
        let dispatcher = ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            sessions.clone(),
            None,
        );

        let mut missing_request = RemotingCommand::create_request_command(
            RequestCode::NotifyUnsubscribeLite,
            NotifyUnsubscribeLiteRequestHeader {
                lite_topic: CheetahString::from_static_str("LiteTopic"),
                consumer_group: CheetahString::from_static_str("GroupA"),
                client_id: CheetahString::from_static_str("client-a"),
                rpc_request_header: None,
            },
        );
        missing_request.make_custom_header_to_net();
        let missing_response = dispatcher.dispatch(&test_context(), &missing_request).await;
        assert_eq!(
            ResponseCode::from(missing_response.code()),
            ResponseCode::ConsumerNotOnline
        );

        let heartbeat = HeartbeatData {
            client_id: CheetahString::from_static_str("client-a"),
            producer_data_set: HashSet::new(),
            consumer_data_set: HashSet::from([ConsumerData {
                group_name: CheetahString::from_static_str("GroupA"),
                ..ConsumerData::default()
            }]),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
        };
        let mut heartbeat_request =
            RemotingCommand::create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
                .set_body(heartbeat.encode().expect("heartbeat should encode"));
        heartbeat_request.make_custom_header_to_net();
        let heartbeat_response = dispatcher.dispatch(&test_context(), &heartbeat_request).await;
        assert_eq!(ResponseCode::from(heartbeat_response.code()), ResponseCode::Success);
        let mut harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        sessions.bind_remoting_channel("client-a", harness.channel());

        let mut request = RemotingCommand::create_request_command(
            RequestCode::NotifyUnsubscribeLite,
            NotifyUnsubscribeLiteRequestHeader {
                lite_topic: CheetahString::from_static_str("LiteTopic"),
                consumer_group: CheetahString::from_static_str("GroupA"),
                client_id: CheetahString::from_static_str("client-a"),
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let forwarded = timeout(std::time::Duration::from_secs(3), harness.receive_response())
            .await
            .expect("forwarded notify unsubscribe lite should arrive before timeout")
            .expect("forwarded notify unsubscribe lite should decode")
            .expect("forwarded notify unsubscribe lite should be present");
        assert_eq!(RequestCode::from(forwarded.code()), RequestCode::NotifyUnsubscribeLite);
        assert!(forwarded.is_oneway_rpc());
        let forwarded_header = forwarded
            .decode_command_custom_header::<NotifyUnsubscribeLiteRequestHeader>()
            .expect("forwarded notify header should decode");
        assert_eq!(forwarded_header.consumer_group, "GroupA");
        assert_eq!(forwarded_header.client_id, "client-a");
    }

    #[tokio::test]
    async fn dispatch_lock_and_unlock_batch_mq_use_local_backend_passthrough() {
        let backend = Arc::new(TestRemotingBackend::default());
        let dispatcher = ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            Arc::new(DefaultMessagingProcessor::new(Arc::new(
                LocalServiceManager::with_services(
                    Arc::new(StaticRouteService::default()),
                    Arc::new(StaticMetadataService::default()),
                    Arc::new(DefaultAssignmentService),
                    Arc::new(TestMessageService),
                    Arc::new(TestConsumerService),
                    Arc::new(DefaultTransactionService),
                ),
            ))),
            ClientSessionRegistry::default(),
            Some(backend.clone()),
        );

        let mq_set = HashSet::from([MessageQueue::from_parts("TopicA", "broker-a", 0)]);

        let mut lock_request =
            RemotingCommand::create_request_command(RequestCode::LockBatchMq, LockBatchMqRequestHeader::default())
                .set_body(
                    LockBatchRequestBody {
                        consumer_group: Some(CheetahString::from("GroupA")),
                        client_id: Some(CheetahString::from("client-a")),
                        only_this_broker: true,
                        mq_set: mq_set.clone(),
                    }
                    .encode()
                    .expect("lock batch body should encode"),
                );
        lock_request.make_custom_header_to_net();
        let lock_response = dispatcher.dispatch(&test_context(), &lock_request).await;
        assert_eq!(ResponseCode::from(lock_response.code()), ResponseCode::Success);

        let mut unlock_request =
            RemotingCommand::create_request_command(RequestCode::UnlockBatchMq, UnlockBatchMqRequestHeader::default())
                .set_body(
                    UnlockBatchRequestBody {
                        consumer_group: Some(CheetahString::from("GroupA")),
                        client_id: Some(CheetahString::from("client-a")),
                        only_this_broker: true,
                        mq_set,
                    }
                    .encode()
                    .expect("unlock batch body should encode"),
                );
        unlock_request.make_custom_header_to_net();
        let unlock_response = dispatcher.dispatch(&test_context(), &unlock_request).await;
        assert_eq!(ResponseCode::from(unlock_response.code()), ResponseCode::Success);

        assert_eq!(
            *backend.seen_codes.lock().expect("backend mutex poisoned"),
            vec![RequestCode::LockBatchMq.to_i32(), RequestCode::UnlockBatchMq.to_i32()]
        );
    }

    #[tokio::test]
    async fn dispatch_lite_info_requests_reflect_session_registry() {
        let registry = ClientSessionRegistry::default();
        let context = test_context();
        registry.upsert_from_context(&context);
        let request = crate::proto::v2::SyncLiteSubscriptionRequest {
            topic: Some(crate::proto::v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            group: Some(crate::proto::v2::Resource {
                resource_namespace: String::new(),
                name: "GroupA".to_owned(),
            }),
            lite_topic_set: vec!["lite-a".to_owned(), "lite-b".to_owned()],
            action: crate::proto::v2::LiteSubscriptionAction::CompleteAdd as i32,
            version: Some(1),
            offset_option: None,
        };
        registry
            .sync_lite_subscription(
                "remoting-client",
                build_lite_subscription_sync_request(&request).expect("lite request should decode"),
                None,
            )
            .expect("lite subscription should be stored");
        let dispatcher = ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            Arc::new(DefaultMessagingProcessor::new(Arc::new(
                LocalServiceManager::with_services(
                    Arc::new(StaticRouteService::default()),
                    Arc::new(StaticMetadataService::default()),
                    Arc::new(DefaultAssignmentService),
                    Arc::new(TestMessageService),
                    Arc::new(TestConsumerService),
                    Arc::new(DefaultTransactionService),
                ),
            ))),
            registry,
            None,
        );

        let mut parent_request = RemotingCommand::create_request_command(
            RequestCode::GetParentTopicInfo,
            GetParentTopicInfoRequestHeader {
                topic: CheetahString::from("TopicA"),
                rpc: None,
            },
        );
        parent_request.make_custom_header_to_net();
        let parent_response = dispatcher.dispatch(&context, &parent_request).await;
        assert_eq!(ResponseCode::from(parent_response.code()), ResponseCode::Success);

        let mut lite_topic_request = RemotingCommand::create_request_command(
            RequestCode::GetLiteTopicInfo,
            GetLiteTopicInfoRequestHeader {
                parent_topic: CheetahString::from("TopicA"),
                lite_topic: CheetahString::from("lite-a"),
            },
        );
        lite_topic_request.make_custom_header_to_net();
        let lite_topic_response = dispatcher.dispatch(&context, &lite_topic_request).await;
        let lite_topic_body =
            rocketmq_remoting::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody::decode(
                lite_topic_response
                    .body()
                    .expect("lite topic body should exist")
                    .as_ref(),
            )
            .expect("lite topic body should decode");
        assert_eq!(lite_topic_body.parent_topic().as_str(), "TopicA");
        assert_eq!(lite_topic_body.lite_topic().as_str(), "lite-a");
        assert!(lite_topic_body.subscriber().contains(&ClientGroup::from_parts(
            CheetahString::from("remoting-client"),
            CheetahString::from("GroupA"),
        )));

        let mut lite_group_request = RemotingCommand::create_request_command(
            RequestCode::GetLiteGroupInfo,
            GetLiteGroupInfoRequestHeader {
                group: CheetahString::from("GroupA"),
                lite_topic: CheetahString::from("lite-a"),
                top_k: 10,
                rpc: None,
            },
        );
        lite_group_request.make_custom_header_to_net();
        let lite_group_response = dispatcher.dispatch(&context, &lite_group_request).await;
        let lite_group_body =
            rocketmq_remoting::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody::decode(
                lite_group_response
                    .body()
                    .expect("lite group body should exist")
                    .as_ref(),
            )
            .expect("lite group body should decode");
        assert_eq!(lite_group_body.group().as_str(), "GroupA");
        assert_eq!(lite_group_body.parent_topic().as_str(), "TopicA");
        assert_eq!(lite_group_body.lite_topic().as_str(), "lite-a");
    }

    #[tokio::test]
    async fn remoting_auth_runtime_accepts_signed_route_request_and_enforces_acl() {
        let auth_runtime = test_auth_runtime(true, true).await;
        seed_normal_user(&auth_runtime, "alice", "secret").await;
        allow_topic_action(&auth_runtime, "alice", "TopicA", Action::Get).await;

        let mut command = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new("TopicA", None),
        );
        command.make_custom_header_to_net();
        let command = sign_remoting_command(command, "alice", "secret");

        let principal = auth_runtime
            .authenticate_remoting(&command, Some("channel-a"), Some("127.0.0.1"))
            .await
            .expect("authentication should succeed");
        assert_eq!(
            principal.as_ref().expect("principal should be returned").username(),
            "alice"
        );
        auth_runtime
            .authorize_remoting(&(), &command)
            .await
            .expect("authorization should succeed");

        let denied_runtime = test_auth_runtime(true, true).await;
        seed_normal_user(&denied_runtime, "bob", "secret").await;
        let denied_command = sign_remoting_command(command.clone(), "bob", "secret");
        denied_runtime
            .authenticate_remoting(&denied_command, Some("channel-b"), Some("127.0.0.1"))
            .await
            .expect("authentication should still succeed");
        let error = denied_runtime
            .authorize_remoting(&(), &denied_command)
            .await
            .expect_err("authorization should fail without matching acl");
        assert!(matches!(
            error,
            crate::error::ProxyError::RocketMQ(RocketMQError::BrokerPermissionDenied { .. })
        ));
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
