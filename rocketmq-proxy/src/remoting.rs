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
use std::time::Duration;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_auth::RemotingAuthContext;
use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RemotingResponseCode;
use rocketmq_error::RocketMQError;
use rocketmq_error::CORE_INTERNAL_FAILURE;
use rocketmq_error::CORE_SERIALIZATION_FAILED;
use rocketmq_error::PROTOCOL_BODY_INVALID;
use rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED;
use rocketmq_error::PROXY_DRAIN_UNAVAILABLE;
use rocketmq_error::PROXY_REMOTING_REQUEST_INVALID;
use rocketmq_error::PROXY_UPSTREAM_REQUEST_FAILED;
use rocketmq_model::common::entity::ClientGroup;
use rocketmq_model::common::filter::expression_type::ExpressionType;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all::IS_SUB_CHANGE;
use rocketmq_model::common::mix_all::IS_SUPPORT_HEART_BEAT_V2;
use rocketmq_model::result::SendResult;
use rocketmq_model::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::body::connection::Connection;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_protocol::protocol::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody;
use rocketmq_protocol::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_protocol::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_protocol::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainOperationRequestBody;
use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainPendingBody;
use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody;
use rocketmq_protocol::protocol::body::proxy_drain::PROXY_DRAIN_SCHEMA_VERSION;
use rocketmq_protocol::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_protocol::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_protocol::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
use rocketmq_protocol::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::parse_request_header;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_protocol::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_protocol::protocol::header::notify_unsubscribe_lite_request_header::NotifyUnsubscribeLiteRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_protocol::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_protocol::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetResponseHeader;
use rocketmq_protocol::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_proxy_core::identity::ResourceIdentity;
pub use rocketmq_proxy_core::remoting::ProxyRemotingBackend;
use rocketmq_proxy_core::remoting::RemotingIngressDispatcher;
use rocketmq_proxy_core::remoting::RemotingIngressRoute;
use rocketmq_proxy_core::remoting::RemotingStatusMapper;
use rocketmq_proxy_core::ProxyDrainController;
use rocketmq_proxy_core::ProxyDrainError;
use rocketmq_proxy_core::ProxyDrainPhase;
use rocketmq_proxy_core::ProxyDrainSnapshot;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownReport;
use rocketmq_transport::api::error_response;
use rocketmq_transport::api::EmbeddedDispatchOutcome;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RejectRequestDecision;
use rocketmq_transport::api::RemotingErrorTarget;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RemotingResponse;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ServerConfig;
use rocketmq_transport::api::ServerPushCommand;
use rocketmq_transport::api::ServerPushOutcome;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionRegistry;
use rocketmq_transport::api::TransportContractViolation;
use rocketmq_transport::api::TransportServer;
use rocketmq_transport::api::TransportTelemetry;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::warn;

use crate::auth::is_auth_error;
use crate::auth::ProxyAuthRuntime;
use crate::config::ProxyConfig;
use crate::context::ProxyContext;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::message::message_ext_from_core;
use crate::message::message_to_core;
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
use crate::session::ClientSessionBindInstruction;
use crate::session::ClientSessionRegistry;
use crate::session::ProxySessionBinder;

pub struct ProxyRequestProcessor<P> {
    dispatcher: Arc<ProxyRemotingDispatcher<P>>,
    auth_runtime: Option<ProxyAuthRuntime>,
    drain: ProxyDrainController,
    session_binder: ProxySessionBinder,
}

impl<P> Clone for ProxyRequestProcessor<P> {
    fn clone(&self) -> Self {
        Self {
            dispatcher: Arc::clone(&self.dispatcher),
            auth_runtime: self.auth_runtime.clone(),
            drain: self.drain.clone(),
            session_binder: self.session_binder.clone(),
        }
    }
}

impl<P> ProxyRequestProcessor<P>
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
        Self::new_with_remoting_command_factory(
            config,
            processor,
            sessions,
            auth_runtime,
            remoting_backend,
            application_remoting_command_factory(),
        )
    }

    pub fn new_with_remoting_command_factory(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        auth_runtime: Option<ProxyAuthRuntime>,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        Self::new_with_drain_controller_and_remoting_command_factory(
            config,
            processor,
            sessions,
            auth_runtime,
            remoting_backend,
            ProxyDrainController::default(),
            command_factory,
        )
    }

    pub fn new_with_drain_controller(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        auth_runtime: Option<ProxyAuthRuntime>,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        drain: ProxyDrainController,
    ) -> Self {
        Self::new_with_drain_controller_and_remoting_command_factory(
            config,
            processor,
            sessions,
            auth_runtime,
            remoting_backend,
            drain,
            application_remoting_command_factory(),
        )
    }

    pub fn new_with_drain_controller_and_remoting_command_factory(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        auth_runtime: Option<ProxyAuthRuntime>,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        drain: ProxyDrainController,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        let session_binder = ProxySessionBinder::new(sessions.clone());
        Self::new_with_session_binder(
            config,
            processor,
            sessions,
            auth_runtime,
            remoting_backend,
            drain,
            command_factory,
            session_binder,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "assembles the request boundary with its independent session binder"
    )]
    fn new_with_session_binder(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        auth_runtime: Option<ProxyAuthRuntime>,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        drain: ProxyDrainController,
        command_factory: RemotingCommandFactory,
        session_binder: ProxySessionBinder,
    ) -> Self {
        Self {
            dispatcher: Arc::new(
                ProxyRemotingDispatcher::new_with_drain_controller_and_remoting_command_factory(
                    config,
                    processor,
                    sessions,
                    remoting_backend,
                    drain.clone(),
                    command_factory,
                )
                .with_session_binder(session_binder.clone()),
            ),
            auth_runtime,
            drain,
            session_binder,
        }
    }
}

impl<P> RequestProcessor for ProxyRequestProcessor<P>
where
    P: MessagingProcessor + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let original_code = request.original_identity().original_code();
        let mut auth_command = request.command().clone();
        auth_command.set_code_ref(original_code);
        let mut context = ProxyContext::from_remoting_request(RemotingIngressRoute::rpc_name(original_code), request);
        let drain_management = is_drain_management_request(original_code);
        if drain_management && self.auth_runtime.is_none() {
            return remoting_response(authentication_required_response(
                &self.dispatcher.command_factory,
                request.original_identity().original_opaque(),
            ));
        }

        if let Some(auth_runtime) = &self.auth_runtime {
            let auth_context = match RemotingAuthContext::from_request(request) {
                Ok(auth_context) => auth_context,
                Err(error) => {
                    return remoting_response(proxy_operation_error_response(
                        &self.dispatcher.command_factory,
                        request.original_identity().original_opaque(),
                        "parse remoting authentication context",
                        ProxyError::from(error),
                    ));
                }
            };
            match auth_runtime.authenticate_remoting(&auth_command, &auth_context).await {
                Ok(Some(principal)) => context.set_authenticated_principal(principal),
                Ok(None) => {}
                Err(error) => {
                    return remoting_response(proxy_operation_error_response(
                        &self.dispatcher.command_factory,
                        request.original_identity().original_opaque(),
                        "authenticate remoting request",
                        error,
                    ));
                }
            }
            if drain_management && context.authenticated_principal().is_none() {
                return remoting_response(authentication_required_response(
                    &self.dispatcher.command_factory,
                    request.original_identity().original_opaque(),
                ));
            }
            if let Err(error) = auth_runtime.authorize_remoting(&auth_context, &auth_command).await {
                return remoting_response(proxy_operation_error_response(
                    &self.dispatcher.command_factory,
                    request.original_identity().original_opaque(),
                    "authorize remoting request",
                    error,
                ));
            }
        }

        let _drain_admission = if drain_management {
            None
        } else {
            match self.drain.try_admit() {
                Ok(admission) => Some(admission),
                Err(_) => {
                    return remoting_response(
                        self.dispatcher
                            .command_factory
                            .create_response_command_with_code(ResponseCode::ServiceNotAvailable)
                            .set_remark("Proxy is draining and does not accept new requests")
                            .set_opaque(request.original_identity().original_opaque()),
                    );
                }
            }
        };

        if RequestCode::from(original_code) == RequestCode::HeartBeat {
            if let Err(error) = auth_command.decode_command_custom_header::<HeartbeatRequestHeader>() {
                return remoting_response(request_invalid_response(
                    &self.dispatcher.command_factory,
                    request.original_identity().original_opaque(),
                    "decode heartbeat header",
                    error,
                ));
            }
            let Some(body) = auth_command.body() else {
                let error = owner_error(&PROXY_REMOTING_REQUEST_INVALID, "decode heartbeat body");
                return remoting_response(descriptor_error_response(
                    &self.dispatcher.command_factory,
                    request.original_identity().original_opaque(),
                    &error,
                ));
            };
            let heartbeat = match SerdeJsonUtils::from_json_bytes::<HeartbeatData>(body.as_ref()) {
                Ok(heartbeat) => heartbeat,
                Err(error) => {
                    return remoting_response(request_invalid_response(
                        &self.dispatcher.command_factory,
                        request.original_identity().original_opaque(),
                        "decode heartbeat body",
                        error,
                    ))
                }
            };
            if heartbeat.client_id.is_empty() {
                return remoting_response(response_with_code(
                    &self.dispatcher.command_factory,
                    request.original_identity().original_opaque(),
                    ResponseCode::SystemError,
                    "heartbeat clientId is missing",
                ));
            }
            let instruction = ClientSessionBindInstruction::new(
                heartbeat.client_id.as_str(),
                request.session().id(),
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
            let Some(commit) = self.session_binder.commit_heartbeat(&context, instruction) else {
                return remoting_response(response_with_code(
                    &self.dispatcher.command_factory,
                    request.original_identity().original_opaque(),
                    ResponseCode::SystemError,
                    "heartbeat session closed before capability binding",
                ));
            };
            if let Some(retired) = commit.retired {
                match retired.retire().await {
                    crate::session::RetiredSessionRetirement::Graceful => debug!(
                        retirement_outcome = "graceful",
                        "retired superseded Proxy remoting client session"
                    ),
                    crate::session::RetiredSessionRetirement::Forced => warn!(
                        retirement_outcome = "forced",
                        "forced retirement of superseded Proxy remoting client session"
                    ),
                    crate::session::RetiredSessionRetirement::AlreadyDisconnected => debug!(
                        retirement_outcome = "already_disconnected",
                        "superseded Proxy remoting client session disconnected before retirement fallback"
                    ),
                }
            }
            let mut response = self
                .dispatcher
                .command_factory
                .create_success_response_command()
                .set_opaque(request.original_identity().original_opaque());
            response.add_ext_field(IS_SUPPORT_HEART_BEAT_V2, true.to_string());
            response.add_ext_field(IS_SUB_CHANGE, commit.membership_changed.to_string());
            return remoting_response(response);
        }

        self.dispatcher.dispatch_request(&context, request).await
    }

    fn reject_request(&self, _code: i32) -> RejectRequestDecision {
        RejectRequestDecision::Proceed
    }
}

fn protocol_no_response_contract_error(error: TransportContractViolation) -> RocketMQError {
    match error {
        TransportContractViolation::ProtocolNoResponseOneWayRequest => {
            RocketMQError::illegal_argument("protocol no-response is unavailable for one-way requests")
        }
        TransportContractViolation::ProtocolNoResponseUnsupported { .. } => {
            RocketMQError::illegal_argument("protocol no-response reason is unsupported")
        }
        _ => RocketMQError::illegal_argument("protocol no-response contract is invalid"),
    }
}

fn remoting_response(response: RemotingCommand) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    RemotingResponse::from_command(response)
        .map(HandlerOutcome::Reply)
        .map_err(|error| RocketMQError::response_process_failed("proxy_remoting_response", error.to_string()))
}

#[doc(hidden)]
pub async fn serve_with_service_context<P, F>(
    service_context: ChildServiceContext,
    telemetry: TransportTelemetry,
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    auth_runtime: Option<ProxyAuthRuntime>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    shutdown: F,
) -> ProxyResult<Option<ShutdownReport>>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    serve_with_service_context_and_remoting_command_factory(
        service_context,
        telemetry,
        config,
        processor,
        sessions,
        auth_runtime,
        remoting_backend,
        application_remoting_command_factory(),
        shutdown,
    )
    .await
}

#[doc(hidden)]
#[allow(
    clippy::too_many_arguments,
    reason = "preserves the established transport startup boundary"
)]
pub async fn serve_with_service_context_and_remoting_command_factory<P, F>(
    service_context: ChildServiceContext,
    telemetry: TransportTelemetry,
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    auth_runtime: Option<ProxyAuthRuntime>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    command_factory: RemotingCommandFactory,
    shutdown: F,
) -> ProxyResult<Option<ShutdownReport>>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    serve_with_context(
        service_context,
        telemetry,
        config,
        processor,
        sessions,
        auth_runtime,
        remoting_backend,
        ProxyDrainController::default(),
        command_factory,
        shutdown,
        None,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_with_service_context_and_ready<P, F, R>(
    service_context: ChildServiceContext,
    telemetry: TransportTelemetry,
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    auth_runtime: Option<ProxyAuthRuntime>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    shutdown: F,
    ready: R,
) -> ProxyResult<Option<ShutdownReport>>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
    R: FnOnce() -> ProxyResult<()> + Send + 'static,
{
    serve_with_service_context_and_ready_and_remoting_command_factory(
        service_context,
        telemetry,
        config,
        processor,
        sessions,
        auth_runtime,
        remoting_backend,
        application_remoting_command_factory(),
        shutdown,
        ready,
    )
    .await
}

#[doc(hidden)]
#[allow(
    clippy::too_many_arguments,
    reason = "preserves the established transport startup boundary"
)]
pub async fn serve_with_service_context_and_ready_and_remoting_command_factory<P, F, R>(
    service_context: ChildServiceContext,
    telemetry: TransportTelemetry,
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    auth_runtime: Option<ProxyAuthRuntime>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    command_factory: RemotingCommandFactory,
    shutdown: F,
    ready: R,
) -> ProxyResult<Option<ShutdownReport>>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
    R: FnOnce() -> ProxyResult<()> + Send + 'static,
{
    serve_with_context(
        service_context,
        telemetry,
        config,
        processor,
        sessions,
        auth_runtime,
        remoting_backend,
        ProxyDrainController::default(),
        command_factory,
        shutdown,
        Some(Box::new(ready)),
    )
    .await
}

#[doc(hidden)]
#[allow(dead_code, reason = "retains the established internal startup compatibility path")]
#[allow(
    clippy::too_many_arguments,
    reason = "preserves the established transport startup boundary"
)]
pub async fn serve_with_service_context_and_ready_and_drain<P, F, R>(
    service_context: ChildServiceContext,
    telemetry: TransportTelemetry,
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    auth_runtime: Option<ProxyAuthRuntime>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    drain: ProxyDrainController,
    shutdown: F,
    ready: R,
) -> ProxyResult<Option<ShutdownReport>>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
    R: FnOnce() -> ProxyResult<()> + Send + 'static,
{
    serve_with_service_context_and_ready_and_drain_and_remoting_command_factory(
        service_context,
        telemetry,
        config,
        processor,
        sessions,
        auth_runtime,
        remoting_backend,
        drain,
        application_remoting_command_factory(),
        shutdown,
        ready,
    )
    .await
}

#[doc(hidden)]
#[allow(
    clippy::too_many_arguments,
    reason = "preserves the established transport startup boundary"
)]
pub async fn serve_with_service_context_and_ready_and_drain_and_remoting_command_factory<P, F, R>(
    service_context: ChildServiceContext,
    telemetry: TransportTelemetry,
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    auth_runtime: Option<ProxyAuthRuntime>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    drain: ProxyDrainController,
    command_factory: RemotingCommandFactory,
    shutdown: F,
    ready: R,
) -> ProxyResult<Option<ShutdownReport>>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
    R: FnOnce() -> ProxyResult<()> + Send + 'static,
{
    serve_with_context(
        service_context,
        telemetry,
        config,
        processor,
        sessions,
        auth_runtime,
        remoting_backend,
        drain,
        command_factory,
        shutdown,
        Some(Box::new(ready)),
    )
    .await
}

async fn serve_with_context<P, F>(
    service_context: ChildServiceContext,
    telemetry: TransportTelemetry,
    config: Arc<ProxyConfig>,
    processor: Arc<P>,
    sessions: ClientSessionRegistry,
    auth_runtime: Option<ProxyAuthRuntime>,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    drain: ProxyDrainController,
    command_factory: RemotingCommandFactory,
    shutdown: F,
    mut ready: Option<Box<dyn FnOnce() -> ProxyResult<()> + Send>>,
) -> ProxyResult<Option<ShutdownReport>>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let addr = config.remoting.socket_addr()?;
    let listener = TcpListener::bind(addr).await.map_err(|error| ProxyError::Transport {
        message: format!("proxy remoting server failed to bind {addr}: {error}"),
    })?;
    let proxy_protocol = config.remoting.proxy_protocol.clone();
    let session_binder = Arc::new(ProxySessionBinder::new(sessions.clone()));
    let transport_sessions = Arc::new(SessionRegistry::with_lifecycle_listener(session_binder.clone()));
    if !session_binder.attach(&transport_sessions) {
        return Err(ProxyError::Transport {
            message: "proxy remoting session binder was already attached".to_owned(),
        });
    }
    let request_processor = ProxyRequestProcessor::new_with_session_binder(
        config,
        processor,
        sessions,
        auth_runtime,
        remoting_backend,
        drain,
        command_factory,
        session_binder.as_ref().clone(),
    );
    let server = TransportServer::new(Arc::new(ServerConfig::default()), service_context, request_processor)
        .with_telemetry(telemetry)
        .try_with_proxy_protocol(proxy_protocol)?
        .with_session_registry(transport_sessions);
    let (startup_tx, mut startup_rx) = oneshot::channel();
    let readiness_cancellation = CancellationToken::new();
    let server_cancellation = readiness_cancellation.clone();
    let server_future = server.try_serve_bound_listener_until_with_startup(
        listener,
        None,
        async move {
            tokio::select! {
                _ = shutdown => {}
                _ = server_cancellation.cancelled() => {}
            }
        },
        startup_tx,
    );
    tokio::pin!(server_future);
    let startup = tokio::select! {
        biased;
        startup = &mut startup_rx => startup.map_err(|error| ProxyError::Transport {
            message: format!("proxy remoting startup acknowledgement was dropped: {error}"),
        })?,
        result = &mut server_future => {
            let report = result.map_err(|error| ProxyError::Transport {
                message: format!("proxy remoting server failed before readiness: {error}"),
            })?;
            let startup = startup_rx.await.map_err(|error| ProxyError::Transport {
                message: format!("proxy remoting startup acknowledgement was dropped: {error}"),
            })?;
            startup.map_err(|error| ProxyError::Transport {
                message: format!("proxy remoting server failed before readiness: {error}"),
            })?;
            run_ready_transition(&mut ready)?;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "Proxy remoting server task shutdown report is unhealthy"
                );
            }
            return Ok(Some(report));
        }
    };
    startup.map_err(|error| ProxyError::Transport {
        message: format!("proxy remoting server failed before readiness: {error}"),
    })?;
    if let Err(error) = run_ready_transition(&mut ready) {
        readiness_cancellation.cancel();
        match server_future.await {
            Ok(report) if !report.is_healthy() => warn!(
                report = %report.to_json(),
                "Proxy remoting server cleanup after readiness failure is unhealthy"
            ),
            Err(server_error) => warn!(
                %server_error,
                "Proxy remoting server cleanup after readiness failure returned an error"
            ),
            _ => {}
        }
        return Err(error);
    }
    let report = server_future.await.map_err(|error| ProxyError::Transport {
        message: format!("proxy remoting server failed: {error}"),
    })?;
    if !report.is_healthy() {
        warn!(
            report = %report.to_json(),
            "Proxy remoting server task shutdown report is unhealthy"
        );
    }
    Ok(Some(report))
}

fn run_ready_transition(ready: &mut Option<Box<dyn FnOnce() -> ProxyResult<()> + Send>>) -> ProxyResult<()> {
    let Some(ready) = ready.take() else {
        return Ok(());
    };
    ready()
}

pub struct ProxyRemotingDispatcher<P> {
    processor: Arc<P>,
    validate_message_type: bool,
    sessions: ClientSessionRegistry,
    remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
    drain: ProxyDrainController,
    command_factory: RemotingCommandFactory,
    session_binder: Option<ProxySessionBinder>,
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
        Self::new_with_remoting_command_factory(
            config,
            processor,
            sessions,
            remoting_backend,
            application_remoting_command_factory(),
        )
    }

    pub fn new_with_remoting_command_factory(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        Self::new_with_drain_controller_and_remoting_command_factory(
            config,
            processor,
            sessions,
            remoting_backend,
            ProxyDrainController::default(),
            command_factory,
        )
    }

    pub fn new_with_drain_controller(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        drain: ProxyDrainController,
    ) -> Self {
        Self::new_with_drain_controller_and_remoting_command_factory(
            config,
            processor,
            sessions,
            remoting_backend,
            drain,
            application_remoting_command_factory(),
        )
    }

    pub fn new_with_drain_controller_and_remoting_command_factory(
        config: Arc<ProxyConfig>,
        processor: Arc<P>,
        sessions: ClientSessionRegistry,
        remoting_backend: Option<Arc<dyn ProxyRemotingBackend>>,
        drain: ProxyDrainController,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        Self {
            processor,
            validate_message_type: config.settings.validate_message_type,
            sessions,
            remoting_backend,
            drain,
            command_factory,
            session_binder: None,
        }
    }

    fn with_session_binder(mut self, session_binder: ProxySessionBinder) -> Self {
        self.session_binder = Some(session_binder);
        self
    }

    pub async fn dispatch(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        match RemotingIngressDispatcher::route(request) {
            RemotingIngressRoute::QueryRoute => self.dispatch_query_route(context, request).await,
            RemotingIngressRoute::QueryAssignment => self.dispatch_query_assignment(context, request).await,
            RemotingIngressRoute::SendMessage => self.dispatch_send_message(context, request).await,
            RemotingIngressRoute::Heartbeat => response_with_code(
                &self.command_factory,
                request.opaque(),
                ResponseCode::SystemError,
                "heartbeat requires the atomic session-binding boundary",
            ),
            RemotingIngressRoute::UnregisterClient => self.dispatch_unregister_client(request, None).await,
            RemotingIngressRoute::GetConsumerListByGroup => self.dispatch_get_consumer_list_by_group(request).await,
            RemotingIngressRoute::GetConsumerConnectionList => self.dispatch_get_consumer_connection_list(request),
            RemotingIngressRoute::NotifyConsumerIdsChanged => self.dispatch_notify_consumer_ids_changed(request).await,
            RemotingIngressRoute::NotifyUnsubscribeLite => self.dispatch_notify_unsubscribe_lite(request).await,
            RemotingIngressRoute::LockBatchMessageQueue | RemotingIngressRoute::UnlockBatchMessageQueue => {
                error_response(
                    PublicErrorView::descriptor_only(&PROTOCOL_REQUEST_UNSUPPORTED),
                    RemotingErrorTarget::Reply {
                        factory: &self.command_factory,
                        opaque: request.opaque(),
                    },
                )
            }
            RemotingIngressRoute::CheckClientConfig => self.dispatch_check_client_config(request).await,
            RemotingIngressRoute::PullMessage => self.dispatch_pull_message(context, request).await,
            RemotingIngressRoute::UpdateConsumerOffset => self.dispatch_update_consumer_offset(context, request).await,
            RemotingIngressRoute::QueryConsumerOffset => self.dispatch_query_consumer_offset(context, request).await,
            RemotingIngressRoute::GetMaxOffset => self.dispatch_get_max_offset(context, request).await,
            RemotingIngressRoute::GetMinOffset => self.dispatch_get_min_offset(context, request).await,
            RemotingIngressRoute::SearchOffsetByTimestamp => self.dispatch_search_offset(context, request).await,
            RemotingIngressRoute::GetBrokerLiteInfo => self.dispatch_get_broker_lite_info(request).await,
            RemotingIngressRoute::GetParentTopicInfo => self.dispatch_get_parent_topic_info(request).await,
            RemotingIngressRoute::GetLiteTopicInfo => self.dispatch_get_lite_topic_info(request).await,
            RemotingIngressRoute::GetLiteGroupInfo => self.dispatch_get_lite_group_info(request).await,
            RemotingIngressRoute::GetProxyDrainState => self.dispatch_get_proxy_drain_state(request),
            RemotingIngressRoute::BeginProxyDrain => self.dispatch_begin_proxy_drain(request),
            RemotingIngressRoute::CancelProxyDrain => self.dispatch_cancel_proxy_drain(request),
            RemotingIngressRoute::ForwardBackend
            | RemotingIngressRoute::AuthAdminUnsupported
            | RemotingIngressRoute::Unsupported => error_response(
                PublicErrorView::descriptor_only(&PROTOCOL_REQUEST_UNSUPPORTED),
                RemotingErrorTarget::Reply {
                    factory: &self.command_factory,
                    opaque: request.opaque(),
                },
            ),
        }
    }

    async fn dispatch_request(
        &self,
        context: &ProxyContext,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let route =
            rocketmq_proxy_core::remoting::classify_remoting_request(request.original_identity().original_code());
        if matches!(route, RemotingIngressRoute::UnregisterClient) {
            let mut dispatch_command = request.command().clone();
            dispatch_command.set_code_ref(request.original_identity().original_code());
            dispatch_command.set_opaque_mut(request.original_identity().original_opaque());
            return remoting_response(
                self.dispatch_unregister_client(&dispatch_command, Some(request.session().id()))
                    .await,
            );
        }
        if matches!(
            route,
            RemotingIngressRoute::ForwardBackend
                | RemotingIngressRoute::LockBatchMessageQueue
                | RemotingIngressRoute::UnlockBatchMessageQueue
        ) {
            let Some(backend) = self.remoting_backend.as_ref() else {
                return remoting_response(error_response(
                    PublicErrorView::descriptor_only(&PROTOCOL_REQUEST_UNSUPPORTED),
                    RemotingErrorTarget::Reply {
                        factory: &self.command_factory,
                        opaque: request.original_identity().original_opaque(),
                    },
                ));
            };
            let mut backend_command = request.command().clone();
            backend_command.set_code_ref(request.original_identity().original_code());
            backend_command.set_opaque_mut(request.original_identity().original_opaque());
            return match backend.process(backend_command).await {
                Ok(EmbeddedDispatchOutcome::Reply(plan)) => Ok(HandlerOutcome::Reply(plan)),
                Ok(EmbeddedDispatchOutcome::OneWay { .. }) => Ok(HandlerOutcome::Reply(
                    RemotingResponse::empty_response(ResponseCode::Success as i32),
                )),
                Ok(EmbeddedDispatchOutcome::NoReply { reason, .. }) => request
                    .protocol_no_response(reason)
                    .map(HandlerOutcome::NoReply)
                    .map_err(protocol_no_response_contract_error),
                Ok(EmbeddedDispatchOutcome::Deferred { .. }) => Err(RocketMQError::invariant_violated(
                    "terminal Proxy backend route returned an unresolved deferred outcome",
                )),
                Err(error) => remoting_response(proxy_operation_error_response(
                    &self.command_factory,
                    request.original_identity().original_opaque(),
                    "dispatch remoting backend request",
                    error,
                )),
                Ok(_) => Err(RocketMQError::invariant_violated(
                    "Proxy backend returned an unsupported embedded dispatch outcome",
                )),
            };
        }

        let mut dispatch_command = request.command().clone();
        dispatch_command.set_code_ref(request.original_identity().original_code());
        dispatch_command.set_opaque_mut(request.original_identity().original_opaque());
        remoting_response(self.dispatch(context, &dispatch_command).await)
    }

    fn dispatch_get_consumer_connection_list(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetConsumerConnectionListRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode getConsumerConnectionList header",
                    error,
                );
            }
        };
        let sessions = self.sessions.consumer_sessions(header.consumer_group.as_str());
        if sessions.is_empty() {
            return response_with_code(
                &self.command_factory,
                request.opaque(),
                ResponseCode::ConsumerNotOnline,
                format!("the consumer group[{}] not online", header.consumer_group),
            );
        }

        let mut body = ConsumerConnection::new();
        for session in sessions {
            let mut connection = Connection::new();
            connection.set_client_id(CheetahString::from(session.client_id));
            connection.set_client_addr(CheetahString::from(session.remote_addr.unwrap_or_default()));
            connection.set_language(remoting_language(session.language.as_deref()));
            connection.set_version(
                session
                    .client_version
                    .as_deref()
                    .and_then(|version| version.parse::<i32>().ok())
                    .unwrap_or_default(),
            );
            body.insert_connection(connection);
        }
        match body.encode() {
            Ok(encoded) => self
                .command_factory
                .create_response_command_with_code(ResponseCode::Success)
                .set_body(encoded)
                .set_opaque(request.opaque()),
            Err(error) => serialization_error_response(
                &self.command_factory,
                request.opaque(),
                "encode getConsumerConnectionList response",
                error,
            ),
        }
    }

    fn dispatch_get_proxy_drain_state(&self, request: &RemotingCommand) -> RemotingCommand {
        proxy_drain_success_response(
            &self.command_factory,
            request.opaque(),
            self.drain.snapshot(&self.sessions),
        )
    }

    fn dispatch_begin_proxy_drain(&self, request: &RemotingCommand) -> RemotingCommand {
        let operation = match decode_proxy_drain_operation(request) {
            Ok(operation) => operation,
            Err(error) => return proxy_drain_request_error_response(&self.command_factory, request.opaque(), error),
        };
        match self.drain.begin(operation.operation_id.as_str()) {
            Ok(()) => proxy_drain_success_response(
                &self.command_factory,
                request.opaque(),
                self.drain.snapshot(&self.sessions),
            ),
            Err(error) => proxy_drain_error_response(&self.command_factory, request.opaque(), error),
        }
    }

    fn dispatch_cancel_proxy_drain(&self, request: &RemotingCommand) -> RemotingCommand {
        let operation = match decode_proxy_drain_operation(request) {
            Ok(operation) => operation,
            Err(error) => return proxy_drain_request_error_response(&self.command_factory, request.opaque(), error),
        };
        match self.drain.cancel(operation.operation_id.as_str()) {
            Ok(()) => proxy_drain_success_response(
                &self.command_factory,
                request.opaque(),
                self.drain.snapshot(&self.sessions),
            ),
            Err(error) => proxy_drain_error_response(&self.command_factory, request.opaque(), error),
        }
    }

    async fn dispatch_query_route(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetRouteInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode queryRoute header",
                    error,
                )
            }
        };
        let plan = match self
            .processor
            .query_route(
                &context.without_principal(),
                QueryRouteRequest {
                    topic: ResourceIdentity::new(String::new(), header.topic.to_string()),
                    endpoints: Vec::new(),
                },
            )
            .await
        {
            Ok(plan) => plan,
            Err(error) => {
                return proxy_operation_error_response(&self.command_factory, request.opaque(), "query route", error);
            }
        };

        let body = match plan.route.encode() {
            Ok(body) => body,
            Err(error) => {
                return serialization_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "encode topic route response",
                    error,
                )
            }
        };
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_body(body)
            .set_opaque(request.opaque())
    }

    async fn dispatch_query_assignment(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let body = match request.body() {
            Some(body) => body,
            None => {
                return self
                    .command_factory
                    .create_response_command_with_code_remark(
                        ResponseCode::SystemError,
                        "queryAssignment request body is missing",
                    )
                    .set_opaque(request.opaque())
            }
        };
        let request_body = match QueryAssignmentRequestBody::decode(body.as_ref()) {
            Ok(body) => body,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode queryAssignment request body",
                    error,
                )
            }
        };
        let plan = match self
            .processor
            .query_assignment(
                &context.without_principal(),
                QueryAssignmentRequest {
                    topic: ResourceIdentity::new(String::new(), request_body.topic.to_string()),
                    group: ResourceIdentity::new(String::new(), request_body.consumer_group.to_string()),
                    endpoints: Vec::new(),
                },
            )
            .await
        {
            Ok(plan) => plan,
            Err(error) => {
                return proxy_operation_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "query assignment",
                    error,
                );
            }
        };

        let response_body = QueryAssignmentResponseBody {
            message_queue_assignments: plan.assignments.unwrap_or_default().into_iter().collect(),
        };
        let encoded = match response_body.encode() {
            Ok(body) => body,
            Err(error) => {
                return serialization_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "encode queryAssignment response",
                    error,
                )
            }
        };
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }

    async fn dispatch_unregister_client(
        &self,
        request: &RemotingCommand,
        caller_session_id: Option<SessionId>,
    ) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<UnregisterClientRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode unregisterClient header",
                    error,
                )
            }
        };
        if let Some(session_binder) = &self.session_binder {
            let Some(caller_session_id) = caller_session_id else {
                return response_with_code(
                    &self.command_factory,
                    request.opaque(),
                    ResponseCode::SystemError,
                    "unregisterClient requires a typed session identity",
                );
            };
            if !session_binder.unregister_client_groups_for_session(
                header.client_id.as_str(),
                caller_session_id,
                header.producer_group.as_deref(),
                header.consumer_group.as_deref(),
            ) {
                return response_with_code(
                    &self.command_factory,
                    request.opaque(),
                    ResponseCode::SystemError,
                    "unregisterClient session does not own the client binding",
                );
            }
        } else {
            self.sessions.unregister_client_groups(
                header.client_id.as_str(),
                header.producer_group.as_deref(),
                header.consumer_group.as_deref(),
            );
        }
        self.command_factory
            .create_success_response_command()
            .set_opaque(request.opaque())
    }

    async fn dispatch_get_consumer_list_by_group(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetConsumerListByGroupRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode getConsumerListByGroup header",
                    error,
                );
            }
        };
        let client_ids = self.sessions.consumer_client_ids(header.consumer_group.as_str());
        if client_ids.is_empty() {
            return response_with_code(
                &self.command_factory,
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
                return serialization_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "encode getConsumerListByGroup response",
                    error,
                );
            }
        };
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_body(body)
            .set_opaque(request.opaque())
    }

    async fn dispatch_notify_consumer_ids_changed(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<NotifyConsumerIdsChangedRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode notifyConsumerIdsChanged header",
                    error,
                );
            }
        };
        let bindings = self.session_binder.as_ref().map_or_else(
            || {
                self.sessions
                    .consumer_client_ids(header.consumer_group.as_str())
                    .into_iter()
                    .filter_map(|client_id| {
                        self.sessions
                            .remoting_channel(client_id.as_str())
                            .map(|capability| (client_id, capability))
                    })
                    .collect()
            },
            |session_binder| session_binder.consumer_bindings(header.consumer_group.as_str()),
        );
        if bindings.is_empty() {
            return response_with_code(
                &self.command_factory,
                request.opaque(),
                ResponseCode::ConsumerNotOnline,
                format!("no consumer for this group, {}", header.consumer_group),
            );
        }
        let mut forwarded = 0usize;
        for (_client_id, capability) in bindings {
            let push = ServerPushCommand::NotifyConsumerIdsChanged {
                header: NotifyConsumerIdsChangedRequestHeader {
                    consumer_group: header.consumer_group.clone(),
                    rpc_request_header: header.rpc_request_header.clone(),
                },
                opaque: Some(request.opaque()),
            };
            match capability.send(push, Duration::from_millis(10)).await {
                Ok(ServerPushOutcome::Sent(_)) => {}
                Ok(_) => {
                    return response_with_code(
                        &self.command_factory,
                        request.opaque(),
                        ResponseCode::SystemError,
                        "server push rejected",
                    );
                }
                Err(error) => {
                    return upstream_failure_response(
                        &self.command_factory,
                        request.opaque(),
                        "notify consumer ids changed",
                        error,
                    );
                }
            }
            forwarded += 1;
        }
        if forwarded == 0 {
            return response_with_code(
                &self.command_factory,
                request.opaque(),
                ResponseCode::ConsumerNotOnline,
                format!(
                    "no remoting channel for consumer group {}, clients are online",
                    header.consumer_group
                ),
            );
        }
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_opaque(request.opaque())
    }

    async fn dispatch_notify_unsubscribe_lite(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<NotifyUnsubscribeLiteRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode notifyUnsubscribeLite header",
                    error,
                );
            }
        };
        let capability = self.session_binder.as_ref().map_or_else(
            || {
                self.sessions
                    .consumer_client_ids(header.consumer_group.as_str())
                    .iter()
                    .any(|client_id| client_id.as_str() == header.client_id.as_str())
                    .then(|| self.sessions.remoting_channel(header.client_id.as_str()))
                    .flatten()
            },
            |session_binder| session_binder.consumer_binding(header.client_id.as_str(), header.consumer_group.as_str()),
        );
        let Some(capability) = capability else {
            return response_with_code(
                &self.command_factory,
                request.opaque(),
                ResponseCode::ConsumerNotOnline,
                format!(
                    "no matching remoting lite consumer for group {}, clientId {}",
                    header.consumer_group, header.client_id
                ),
            );
        };
        let push = ServerPushCommand::NotifyUnsubscribeLite {
            header,
            opaque: Some(request.opaque()),
        };
        match capability.send(push, Duration::from_millis(100)).await {
            Ok(ServerPushOutcome::Sent(_)) => {}
            Ok(_) => {
                return response_with_code(
                    &self.command_factory,
                    request.opaque(),
                    ResponseCode::SystemError,
                    "server push rejected",
                );
            }
            Err(error) => {
                return upstream_failure_response(
                    &self.command_factory,
                    request.opaque(),
                    "notify unsubscribe lite",
                    error,
                );
            }
        }
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_opaque(request.opaque())
    }

    async fn dispatch_check_client_config(&self, request: &RemotingCommand) -> RemotingCommand {
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_opaque(request.opaque())
    }

    async fn dispatch_send_message(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let request_code = RequestCode::from(request.code());
        let header = match parse_request_header(request, request_code) {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode sendMessage header",
                    error,
                )
            }
        };

        let mut send_request = match build_send_message_request(request, &header) {
            Ok(send_request) => send_request,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "build sendMessage request",
                    error,
                );
            }
        };
        send_request.validate_message_type = self.validate_message_type;
        let fallback_queue_id = send_request
            .messages
            .first()
            .and_then(|entry| entry.queue_id)
            .unwrap_or_default();
        let plan = match self
            .processor
            .send_message(&context.without_principal(), send_request)
            .await
        {
            Ok(plan) => plan,
            Err(error) => {
                return proxy_operation_error_response(&self.command_factory, request.opaque(), "send message", error);
            }
        };
        let Some(entry) = plan.entries.into_iter().next() else {
            let error = owner_error(&CORE_INTERNAL_FAILURE, "build sendMessage response");
            return descriptor_error_response(&self.command_factory, request.opaque(), &error);
        };
        if let Some(send_result) = entry.send_result.as_ref() {
            let response_header = build_send_message_response_header(send_result, fallback_queue_id);
            return response_with_header(
                &self.command_factory,
                request.opaque(),
                RemotingStatusMapper::from_send_result(send_result),
                response_header,
                None,
                None,
            );
        }

        response_with_code(
            &self.command_factory,
            request.opaque(),
            RemotingStatusMapper::from_send_payload(&entry.status),
            entry.status.message().to_owned(),
        )
    }

    async fn dispatch_pull_message(&self, context: &ProxyContext, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<PullMessageRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode pullMessage header",
                    error,
                )
            }
        };
        let pull_request = build_pull_message_request(&header);
        let plan = match self
            .processor
            .pull_message(&context.without_principal(), pull_request)
            .await
        {
            Ok(plan) => plan,
            Err(error) => {
                return proxy_operation_error_response(&self.command_factory, request.opaque(), "pull message", error);
            }
        };

        let body = if plan.messages.is_empty() {
            None
        } else {
            let messages = plan.messages.iter().map(message_ext_from_core).collect::<Vec<_>>();
            match encode_message_ext_batch(messages.as_slice()) {
                Ok(body) => Some(body),
                Err(error) => {
                    return serialization_error_response(
                        &self.command_factory,
                        request.opaque(),
                        "encode pullMessage response body",
                        error,
                    );
                }
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
            &self.command_factory,
            request.opaque(),
            RemotingStatusMapper::from_pull_payload(&plan.status),
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
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode updateConsumerOffset header",
                    error,
                );
            }
        };
        let plan = match self
            .processor
            .update_offset(&context.without_principal(), build_update_offset_request(&header))
            .await
        {
            Ok(plan) => plan,
            Err(error) => {
                return proxy_operation_error_response(&self.command_factory, request.opaque(), "update offset", error);
            }
        };
        response_with_header(
            &self.command_factory,
            request.opaque(),
            RemotingStatusMapper::from_offset_payload(&plan.status, ResponseCode::QueryNotFound),
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
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode queryConsumerOffset header",
                    error,
                );
            }
        };
        let plan = match self
            .processor
            .get_offset(&context.without_principal(), build_get_offset_request(&header))
            .await
        {
            Ok(plan) => plan,
            Err(error) => {
                return proxy_operation_error_response(&self.command_factory, request.opaque(), "get offset", error);
            }
        };
        response_with_header(
            &self.command_factory,
            request.opaque(),
            RemotingStatusMapper::from_offset_payload(&plan.status, ResponseCode::QueryNotFound),
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
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode getMaxOffset header",
                    error,
                )
            }
        };
        let plan = match self
            .processor
            .query_offset(
                &context.without_principal(),
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
            Err(error) => {
                return proxy_operation_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "get maximum offset",
                    error,
                );
            }
        };
        response_with_header(
            &self.command_factory,
            request.opaque(),
            RemotingStatusMapper::from_offset_payload(&plan.status, ResponseCode::QueryNotFound),
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
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode getMinOffset header",
                    error,
                )
            }
        };
        let plan = match self
            .processor
            .query_offset(
                &context.without_principal(),
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
            Err(error) => {
                return proxy_operation_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "get minimum offset",
                    error,
                );
            }
        };
        response_with_header(
            &self.command_factory,
            request.opaque(),
            RemotingStatusMapper::from_offset_payload(&plan.status, ResponseCode::QueryNotFound),
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
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode searchOffset header",
                    error,
                )
            }
        };
        let plan = match self
            .processor
            .query_offset(
                &context.without_principal(),
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
            Err(error) => {
                return proxy_operation_error_response(&self.command_factory, request.opaque(), "search offset", error);
            }
        };
        response_with_header(
            &self.command_factory,
            request.opaque(),
            RemotingStatusMapper::from_offset_payload(&plan.status, ResponseCode::QueryNotFound),
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
                return serialization_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "encode getBrokerLiteInfo response",
                    error,
                )
            }
        };
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }

    async fn dispatch_get_parent_topic_info(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetParentTopicInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode getParentTopicInfo header",
                    error,
                )
            }
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
                &self.command_factory,
                request.opaque(),
                ResponseCode::QueryNotFound,
                format!("parent topic '{}' has no lite subscriptions", topic),
            );
        };
        let encoded = match body.encode() {
            Ok(body) => body,
            Err(error) => {
                return serialization_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "encode getParentTopicInfo response",
                    error,
                )
            }
        };
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }

    async fn dispatch_get_lite_topic_info(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetLiteTopicInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode getLiteTopicInfo header",
                    error,
                )
            }
        };
        let topic = ResourceIdentity::new(String::new(), header.parent_topic.to_string());
        let Some(body) = self.build_lite_topic_info_response(&topic, header.lite_topic.as_str()) else {
            return response_with_code(
                &self.command_factory,
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
            Err(error) => {
                return serialization_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "encode getLiteTopicInfo response",
                    error,
                )
            }
        };
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
    }

    async fn dispatch_get_lite_group_info(&self, request: &RemotingCommand) -> RemotingCommand {
        let header = match request.decode_command_custom_header::<GetLiteGroupInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                return request_invalid_response(
                    &self.command_factory,
                    request.opaque(),
                    "decode getLiteGroupInfo header",
                    error,
                )
            }
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
                &self.command_factory,
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
            Err(error) => {
                return serialization_error_response(
                    &self.command_factory,
                    request.opaque(),
                    "encode getLiteGroupInfo response",
                    error,
                )
            }
        };
        self.command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_body(encoded)
            .set_opaque(request.opaque())
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

fn is_drain_management_request(code: i32) -> bool {
    matches!(
        RequestCode::from(code),
        RequestCode::GetProxyDrainState | RequestCode::BeginProxyDrain | RequestCode::CancelProxyDrain
    )
}

fn authentication_required_response(command_factory: &RemotingCommandFactory, opaque: i32) -> RemotingCommand {
    command_factory
        .create_response_command_with_code(ResponseCode::NoPermission)
        .set_remark("Proxy drain management requires an authenticated principal")
        .set_opaque(opaque)
}

enum ProxyDrainRequestError {
    MissingBody,
    InvalidBody(RocketMQError),
    UnsupportedSchema,
}

fn decode_proxy_drain_operation(
    request: &RemotingCommand,
) -> Result<ProxyDrainOperationRequestBody, ProxyDrainRequestError> {
    let Some(body) = request.body() else {
        return Err(ProxyDrainRequestError::MissingBody);
    };
    let operation =
        ProxyDrainOperationRequestBody::decode(body.as_ref()).map_err(ProxyDrainRequestError::InvalidBody)?;
    if operation.schema_version != PROXY_DRAIN_SCHEMA_VERSION {
        return Err(ProxyDrainRequestError::UnsupportedSchema);
    }
    Ok(operation)
}

fn proxy_drain_request_error_response(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    error: ProxyDrainRequestError,
) -> RemotingCommand {
    let error = match error {
        ProxyDrainRequestError::MissingBody | ProxyDrainRequestError::UnsupportedSchema => {
            owner_error(&PROTOCOL_BODY_INVALID, "validate proxy drain request body")
        }
        ProxyDrainRequestError::InvalidBody(source) => {
            owner_error_with_source(&PROTOCOL_BODY_INVALID, "decode proxy drain request body", source)
        }
    };
    descriptor_error_response(command_factory, opaque, &error)
}

fn proxy_drain_success_response(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    snapshot: ProxyDrainSnapshot,
) -> RemotingCommand {
    let phase = match snapshot.phase {
        ProxyDrainPhase::Accepting => "accepting",
        ProxyDrainPhase::Draining => "draining",
        ProxyDrainPhase::Drained => "drained",
    };
    let body = ProxyDrainStateResponseBody {
        schema_version: snapshot.schema_version,
        phase: phase.to_owned(),
        operation_id: snapshot.operation_id,
        admission_open: snapshot.admission_open,
        routing_open: snapshot.routing_open,
        readiness_published: snapshot.readiness_published,
        zero_pending: snapshot.zero_pending,
        pending: ProxyDrainPendingBody {
            active_connections: snapshot.pending.active_connections,
            sessions: snapshot.pending.sessions,
            receipt_handles: snapshot.pending.receipt_handles,
            prepared_transactions: snapshot.pending.prepared_transactions,
            telemetry_links: snapshot.pending.telemetry_links,
            remoting_channels: snapshot.pending.remoting_channels,
            telemetry_commands: snapshot.pending.telemetry_commands,
            rpc_in_flight: snapshot.pending.rpc_in_flight,
        },
    };
    match body.encode() {
        Ok(body) => command_factory
            .create_response_command_with_code(ResponseCode::Success)
            .set_body(body)
            .set_opaque(opaque),
        Err(source) => {
            let error = owner_error_with_source(&CORE_SERIALIZATION_FAILED, "encode proxy drain state", source);
            descriptor_error_response(command_factory, opaque, &error)
        }
    }
}

fn proxy_drain_error_response(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    error: ProxyDrainError,
) -> RemotingCommand {
    match error {
        source @ (ProxyDrainError::LifecycleUnavailable | ProxyDrainError::ReadinessTransition { .. }) => {
            let error = owner_error_with_source(&PROXY_DRAIN_UNAVAILABLE, "change proxy drain state", source);
            descriptor_error_response(command_factory, opaque, &error)
        }
        ProxyDrainError::AdmissionClosed => response_with_code(
            command_factory,
            opaque,
            ResponseCode::InvalidParameter,
            "Proxy drain admission is closed",
        ),
        ProxyDrainError::CounterOverflow => response_with_code(
            command_factory,
            opaque,
            ResponseCode::InvalidParameter,
            "Proxy drain counter overflow",
        ),
        ProxyDrainError::InvalidOperationId => response_with_code(
            command_factory,
            opaque,
            ResponseCode::InvalidParameter,
            "Proxy drain operation id is invalid",
        ),
        ProxyDrainError::OperationConflict { .. } => response_with_code(
            command_factory,
            opaque,
            ResponseCode::InvalidParameter,
            "Proxy drain operation conflicts with an active operation",
        ),
        ProxyDrainError::OperationMismatch => response_with_code(
            command_factory,
            opaque,
            ResponseCode::InvalidParameter,
            "Proxy drain operation does not match the active operation",
        ),
    }
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

fn remoting_language(language: Option<&str>) -> LanguageCode {
    match language.map(str::to_ascii_uppercase).as_deref() {
        Some("JAVA") => LanguageCode::JAVA,
        Some("CPP") => LanguageCode::CPP,
        Some("DOTNET") => LanguageCode::DOTNET,
        Some("PYTHON") => LanguageCode::PYTHON,
        Some("DELPHI") => LanguageCode::DELPHI,
        Some("ERLANG") => LanguageCode::ERLANG,
        Some("RUBY") => LanguageCode::RUBY,
        Some("HTTP") => LanguageCode::HTTP,
        Some("GO") => LanguageCode::GO,
        Some("PHP") => LanguageCode::PHP,
        Some("RUST") => LanguageCode::RUST,
        Some("NODE_JS") => LanguageCode::NODE_JS,
        _ => LanguageCode::OTHER,
    }
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
        let messages = MessageDecoder::decode_messages(&mut batch_body);
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
                    message: message_to_core(&message),
                    queue_id,
                }
            })
            .collect();

        return Ok(SendMessageRequest {
            messages: entries,
            timeout: None,
            validate_message_type: true,
        });
    }

    let mut message = Message::builder()
        .topic(topic.to_string())
        .body(body)
        .flag_bits(header.flag)
        .build_unchecked();
    let properties = MessageDecoder::string_to_message_properties(header.properties.as_ref());
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
            message: message_to_core(&message),
            queue_id,
        }],
        timeout: None,
        validate_message_type: true,
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

fn build_send_message_response_header(send_result: &SendResult, fallback_queue_id: i32) -> SendMessageResponseHeader {
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
        let body = MessageDecoder::encode(message, false)?;
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

fn response_with_header<H>(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    code: ResponseCode,
    header: H,
    remark: Option<String>,
    body: Option<Bytes>,
) -> RemotingCommand
where
    H: CommandCustomHeader + Send + Sync + 'static,
{
    let mut response = command_factory.create_response_command_with_code_and_header(code, header);
    if let Some(remark) = remark {
        response = response.set_remark(remark);
    }
    if let Some(body) = body {
        response = response.set_body(body);
    }
    response.make_custom_header_to_net();
    response.set_opaque(opaque)
}

fn response_with_code(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    code: ResponseCode,
    remark: impl Into<String>,
) -> RemotingCommand {
    command_factory
        .create_response_command_with_code_remark(code, remark.into())
        .set_opaque(opaque)
}

fn owner_error(descriptor: &'static ErrorDescriptor, operation: &'static str) -> Error {
    Error::new(descriptor).with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation))
}

fn owner_error_with_source(
    descriptor: &'static ErrorDescriptor,
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> Error {
    Error::caused_by(descriptor, source).with_context(
        ErrorContext::new()
            .with_text(fields::OPERATION_DIAGNOSTIC, operation)
            .with_secret_presence(fields::SOURCE_PRESENT),
    )
}

fn descriptor_error_response(command_factory: &RemotingCommandFactory, opaque: i32, error: &Error) -> RemotingCommand {
    let view = error
        .public_view()
        .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
    error_response(
        view,
        RemotingErrorTarget::Reply {
            factory: command_factory,
            opaque,
        },
    )
}

fn request_invalid_response(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> RemotingCommand {
    let error = owner_error_with_source(&PROXY_REMOTING_REQUEST_INVALID, operation, source);
    descriptor_error_response(command_factory, opaque, &error)
}

fn serialization_error_response(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> RemotingCommand {
    let error = owner_error_with_source(&CORE_SERIALIZATION_FAILED, operation, source);
    descriptor_error_response(command_factory, opaque, &error)
}

fn upstream_failure_response(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    operation: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> RemotingCommand {
    let error = owner_error_with_source(&PROXY_UPSTREAM_REQUEST_FAILED, operation, source);
    descriptor_error_response(command_factory, opaque, &error)
}

fn proxy_operation_error_response(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    operation: &'static str,
    error: ProxyError,
) -> RemotingCommand {
    match error {
        ProxyError::BrokerResponse(error) => descriptor_error_response(command_factory, opaque, &error),
        ProxyError::RocketMQ(error @ RocketMQError::TopicNotExist { .. })
        | ProxyError::RocketMQ(error @ RocketMQError::RouteNotFound { .. })
        | ProxyError::RocketMQ(error @ RocketMQError::SubscriptionGroupNotExist { .. })
        | ProxyError::RocketMQ(error @ RocketMQError::BrokerPermissionDenied { .. })
        | ProxyError::RocketMQ(error @ RocketMQError::TopicSendingForbidden { .. }) => {
            let context = error.context();
            let view = PublicErrorView::try_new(error.descriptor(), &context)
                .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
            error_response(
                view,
                RemotingErrorTarget::Reply {
                    factory: command_factory,
                    opaque,
                },
            )
        }
        ProxyError::RocketMQ(error) if is_auth_error(&error) => {
            let context = error.context();
            let view = PublicErrorView::try_new(error.descriptor(), &context)
                .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
            error_response(
                view,
                RemotingErrorTarget::Reply {
                    factory: command_factory,
                    opaque,
                },
            )
        }
        ProxyError::RocketMQ(error)
            if error.descriptor().projection().remoting().code == RemotingResponseCode::SystemError =>
        {
            let context = error.context();
            let view = PublicErrorView::try_new(error.descriptor(), &context)
                .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
            error_response(
                view,
                RemotingErrorTarget::Reply {
                    factory: command_factory,
                    opaque,
                },
            )
        }
        ProxyError::TooManyRequests { .. } => response_with_code(
            command_factory,
            opaque,
            ResponseCode::SystemBusy,
            "Proxy request capacity is exhausted",
        ),
        ProxyError::IllegalOffset { .. } => response_with_code(
            command_factory,
            opaque,
            ResponseCode::PullOffsetMoved,
            "Proxy request offset is invalid",
        ),
        ProxyError::IllegalFilterExpression { .. } => response_with_code(
            command_factory,
            opaque,
            ResponseCode::SubscriptionParseFailed,
            "Proxy filter expression is invalid",
        ),
        ProxyError::NotImplemented { .. } => response_with_code(
            command_factory,
            opaque,
            ResponseCode::RequestCodeNotSupported,
            "Proxy operation is unsupported",
        ),
        ProxyError::IllegalMessageId { .. }
        | ProxyError::IllegalMessageGroup { .. }
        | ProxyError::IllegalDeliveryTime { .. }
        | ProxyError::MessagePropertyConflictWithType { .. } => response_with_code(
            command_factory,
            opaque,
            ResponseCode::MessageIllegal,
            "Proxy message is invalid",
        ),
        source => upstream_failure_response(command_factory, opaque, operation, source),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::error::Error as StdError;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_auth::cal_signature;
    use rocketmq_auth::Acl;
    use rocketmq_auth::Policy;
    use rocketmq_auth::PolicyDecision;
    use rocketmq_auth::PolicyResource;
    use rocketmq_auth::RemotingAuthContext;
    use rocketmq_auth::SubjectType;
    use rocketmq_auth::User;
    use rocketmq_auth::UserStatus;
    use rocketmq_auth::UserType;
    use rocketmq_error::RocketMQError;
    use rocketmq_model::common::boundary_type::BoundaryType;
    use rocketmq_model::common::entity::ClientGroup;
    use rocketmq_model::common::message::message_enum::MessageRequestMode;
    use rocketmq_model::common::message::message_queue::MessageQueue;
    use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
    use rocketmq_model::common::message::message_single::Message;
    use rocketmq_model::common::message::MessageConst;
    use rocketmq_model::result::SendResult;
    use rocketmq_model::result::SendStatus;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
    use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
    use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainOperationRequestBody;
    use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody;
    use rocketmq_protocol::protocol::body::proxy_drain::PROXY_DRAIN_SCHEMA_VERSION;
    use rocketmq_protocol::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
    use rocketmq_protocol::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
    use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
    use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
    use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_protocol::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
    use rocketmq_protocol::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
    use rocketmq_protocol::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
    use rocketmq_protocol::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
    use rocketmq_protocol::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
    use rocketmq_protocol::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
    use rocketmq_protocol::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
    use rocketmq_protocol::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
    use rocketmq_protocol::protocol::header::lock_batch_mq_request_header::LockBatchMqRequestHeader;
    use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
    use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
    use rocketmq_protocol::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
    use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
    use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
    use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
    use rocketmq_protocol::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
    use rocketmq_protocol::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
    use rocketmq_protocol::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
    use rocketmq_protocol::protocol::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
    use rocketmq_protocol::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
    use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
    use rocketmq_protocol::protocol::heartbeat::consumer_data::ConsumerData;
    use rocketmq_protocol::protocol::heartbeat::heartbeat_data::HeartbeatData;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;
    use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
    use rocketmq_protocol::protocol::RemotingDeserializable;
    use rocketmq_protocol::protocol::RemotingSerializable;
    use rocketmq_protocol::protocol::SerializeType;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::ServiceLifecycle;
    use rocketmq_runtime::ServiceLifecycleConfig;
    use rocketmq_security_api::Action;
    use rocketmq_security_api::AuthenticatedRequestContext;
    use rocketmq_security_api::Decision;
    use rocketmq_security_api::Principal;
    use rocketmq_security_api::RequestPolicy;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::api::AuthorizedCommandDispatcher;
    use rocketmq_transport::api::EmbeddedDispatchOutcome;
    use rocketmq_transport::api::HandlerOutcome;
    use rocketmq_transport::api::RejectRequestDecision;
    use rocketmq_transport::api::RemotingRequest;
    use rocketmq_transport::api::RemotingResponse;
    use rocketmq_transport::api::RequestProcessor;
    use rocketmq_transport::api::TransportSecurity;
    use rocketmq_transport::test_support::EmbeddedRequestHarness;

    use super::response_with_header;
    use super::ProxyDrainPhase;
    use super::ProxyError;
    use super::ProxyRemotingBackend;
    use super::ProxyRemotingDispatcher;
    use super::ProxyRequestProcessor;
    use super::TransportTelemetry;
    use crate::auth::ProxyAuthRuntime;
    use crate::config::ProxyAuthConfig;
    use crate::config::ProxyConfig;
    use crate::config::ProxyMode;
    use crate::config::RemotingConfig;
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
    use crate::service::StaticMetadataService;
    use crate::service::StaticRouteService;
    use crate::session::build_lite_subscription_sync_request;
    use crate::session::ClientSessionRegistry;
    use crate::status::ProxyStatusMapper;
    use rocketmq_proxy_core::identity::ResourceIdentity;
    use rocketmq_proxy_core::ProxyContext as CoreProxyContext;
    use rocketmq_proxy_core::ProxyDrainController;
    use rocketmq_proxy_core::ProxyMessage;
    use rocketmq_proxy_core::ProxyMessageExt;

    fn test_context() -> ProxyContext {
        ProxyContext::for_internal_client("Remoting", "remoting-client")
    }

    #[test]
    fn shared_transport_error_preserves_proxy_system_error_response_code() {
        let error = RocketMQError::Shared(Arc::new(rocketmq_error::Error::new(
            &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
        )));
        let response = super::proxy_operation_error_response(
            &super::application_remoting_command_factory(),
            17,
            "dispatch upstream request",
            ProxyError::RocketMQ(error),
        );

        assert_eq!(response.code(), 1);
        assert_eq!(response.opaque(), 17);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("Proxy upstream request failed")
        );
    }

    #[test]
    fn safe_canonical_r1_cause_is_projected_without_blanket_proxy_wrapper() {
        let response = super::proxy_operation_error_response(
            &super::application_remoting_command_factory(),
            19,
            "dispatch upstream request",
            ProxyError::RocketMQ(RocketMQError::Shared(Arc::new(rocketmq_error::Error::new(
                &rocketmq_error::CORE_INTERNAL_FAILURE,
            )))),
        );

        assert_eq!(response.code(), 1);
        assert_eq!(response.remark().map(CheetahString::as_str), Some("Internal error"));
    }

    #[test]
    fn upstream_owner_retains_same_shared_cause_and_physical_leaf() {
        let physical = Arc::new(rocketmq_error::Error::caused_by(
            &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
            std::io::Error::other("password=plain-text"),
        ));
        let owner = super::owner_error_with_source(
            &rocketmq_error::PROXY_UPSTREAM_REQUEST_FAILED,
            "dispatch upstream request",
            ProxyError::RocketMQ(RocketMQError::Shared(Arc::clone(&physical))),
        );

        assert_eq!(owner.descriptor(), &rocketmq_error::PROXY_UPSTREAM_REQUEST_FAILED);
        let stored = owner
            .source()
            .and_then(|source| source.downcast_ref::<ProxyError>())
            .expect("typed Proxy cause");
        let ProxyError::RocketMQ(RocketMQError::Shared(retained)) = stored else {
            panic!("expected retained shared transport cause");
        };
        assert!(Arc::ptr_eq(retained, &physical));
        assert!(retained
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
    }

    #[test]
    fn request_invalid_owner_retains_typed_r29_cause_while_emitting_r1() {
        let owner = super::owner_error_with_source(
            &rocketmq_error::PROXY_REMOTING_REQUEST_INVALID,
            "build sendMessage request",
            ProxyError::RocketMQ(RocketMQError::request_body_invalid("sendMessage", "secret body detail")),
        );
        let response = super::descriptor_error_response(&super::application_remoting_command_factory(), 29, &owner);

        assert_eq!(response.code(), 1);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("Proxy remoting request is invalid")
        );
        let stored = owner
            .source()
            .and_then(|source| source.downcast_ref::<ProxyError>())
            .expect("typed Proxy cause");
        let ProxyError::RocketMQ(cause) = stored else {
            panic!("expected typed RocketMQ request cause");
        };
        assert_eq!(cause.descriptor().projection().remoting().code.as_i32(), 29);
    }

    #[test]
    fn proxy_drain_unavailable_is_r14_with_fixed_message() {
        let response = super::proxy_drain_error_response(
            &super::application_remoting_command_factory(),
            14,
            rocketmq_proxy_core::ProxyDrainError::ReadinessTransition {
                message: "password=plain-text".to_owned(),
            },
        );

        assert_eq!(response.code(), 14);
        assert_eq!(response.opaque(), 14);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("Proxy drain service is unavailable")
        );
        assert!(!response.remark().expect("fixed remark").contains("plain-text"));
    }

    #[test]
    fn proxy_drain_invalid_body_is_r29_with_fixed_message() {
        let response = super::proxy_drain_request_error_response(
            &super::application_remoting_command_factory(),
            29,
            super::ProxyDrainRequestError::InvalidBody(RocketMQError::request_body_invalid(
                "beginProxyDrain",
                "password=plain-text",
            )),
        );

        assert_eq!(response.code(), 29);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("Request body is invalid")
        );
        assert!(!response.remark().expect("fixed remark").contains("plain-text"));
    }

    #[test]
    fn serialization_failure_is_r1_with_fixed_message() {
        let response = super::serialization_error_response(
            &super::application_remoting_command_factory(),
            1,
            "encode remoting response",
            std::io::Error::other("C:\\secret\\response.json"),
        );

        assert_eq!(response.code(), 1);
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("Serialization failed")
        );
        assert!(!response.remark().expect("fixed remark").contains("secret"));
    }

    #[test]
    fn local_proxy_business_mappings_keep_frozen_codes_and_safe_messages() {
        let cases = [
            (
                ProxyError::TooManyRequests {
                    resource: "secret queue",
                },
                2,
                "Proxy request capacity is exhausted",
            ),
            (
                ProxyError::NotImplemented {
                    feature: "secret feature",
                },
                3,
                "Proxy operation is unsupported",
            ),
            (
                ProxyError::IllegalMessageId {
                    message: "secret id".to_owned(),
                },
                13,
                "Proxy message is invalid",
            ),
            (
                ProxyError::IllegalOffset {
                    message: "secret offset".to_owned(),
                },
                21,
                "Proxy request offset is invalid",
            ),
            (
                ProxyError::IllegalFilterExpression {
                    message: "secret filter".to_owned(),
                },
                23,
                "Proxy filter expression is invalid",
            ),
        ];

        for (error, code, message) in cases {
            let response = super::proxy_operation_error_response(
                &super::application_remoting_command_factory(),
                7,
                "execute Proxy operation",
                error,
            );
            assert_eq!(response.code(), code);
            assert_eq!(response.remark().map(CheetahString::as_str), Some(message));
            assert!(!response.remark().expect("fixed remark").contains("secret"));
        }
    }

    #[test]
    fn canonical_proxy_mappings_keep_frozen_codes_and_catalog_messages() {
        let cases = [
            (
                RocketMQError::TopicNotExist {
                    topic: "secret topic".to_owned(),
                },
                17,
                "Topic does not exist",
            ),
            (
                RocketMQError::SubscriptionGroupNotExist {
                    group: "secret group".to_owned(),
                },
                26,
                "Subscription group does not exist",
            ),
            (
                RocketMQError::BrokerPermissionDenied {
                    operation: "secret operation".to_owned(),
                },
                16,
                "Permission was denied",
            ),
        ];

        for (error, code, message) in cases {
            let response = super::proxy_operation_error_response(
                &super::application_remoting_command_factory(),
                8,
                "execute Proxy operation",
                ProxyError::RocketMQ(error),
            );
            assert_eq!(response.code(), code);
            assert_eq!(response.remark().map(CheetahString::as_str), Some(message));
            assert!(!response.remark().expect("fixed remark").contains("secret"));
        }
    }

    #[test]
    fn normalized_broker_failures_keep_proxy_r1_and_fixed_catalog_messages() {
        let cases = [
            (ResponseCode::NoPermission, "Broker denied the Proxy operation"),
            (ResponseCode::TopicNotExist, "Broker topic was not found"),
            (
                ResponseCode::SubscriptionGroupNotExist,
                "Broker consumer group was not found",
            ),
            (ResponseCode::UserNotExist, "Broker resource was not found"),
            (ResponseCode::PolicyNotExist, "Broker resource was not found"),
            (ResponseCode::QueryNotFound, "Broker offset was not found"),
            (ResponseCode::PullOffsetMoved, "Broker rejected an invalid offset"),
            (
                ResponseCode::RequestCodeNotSupported,
                "Broker does not support the Proxy request",
            ),
            (ResponseCode::SystemError, "Broker response failed"),
        ];

        for (origin, expected_message) in cases {
            let error = RocketMQError::broker_operation_failed(
                "broker ingress",
                origin.to_i32(),
                "password=plain-text\r\nlocal-path=C:\\private\\data",
            );
            let response = super::proxy_operation_error_response(
                &super::application_remoting_command_factory(),
                73,
                "execute Proxy operation",
                ProxyError::from(error),
            );

            assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
            assert_eq!(response.opaque(), 73);
            assert_eq!(response.remark().map(CheetahString::as_str), Some(expected_message));
            assert!(response.is_response_type());
            assert!(response.body().is_none());
            assert!(response.ext_fields().is_none_or(HashMap::is_empty));
        }
    }

    fn test_dispatcher() -> ProxyRemotingDispatcher<DefaultMessagingProcessor> {
        test_dispatcher_with_factory(super::application_remoting_command_factory())
    }

    fn test_dispatcher_with_factory(
        command_factory: RemotingCommandFactory,
    ) -> ProxyRemotingDispatcher<DefaultMessagingProcessor> {
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
        ProxyRemotingDispatcher::new_with_remoting_command_factory(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            ClientSessionRegistry::default(),
            None,
            command_factory,
        )
    }

    fn test_remoting_config() -> Arc<ProxyConfig> {
        Arc::new(ProxyConfig {
            mode: ProxyMode::Local,
            remoting: RemotingConfig {
                enabled: true,
                listen_addr: "127.0.0.1:0".to_owned(),
                ..RemotingConfig::default()
            },
            ..ProxyConfig::default()
        })
    }

    #[tokio::test]
    async fn remoting_ready_callback_runs_before_immediate_shutdown_completes() {
        let runtime = RuntimeContext::from_current("proxy-remoting-ready-immediate-shutdown-test");
        let service = runtime.service_context("proxy-remoting-ready-immediate-shutdown");
        let ready_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ready_observer = ready_called.clone();
        let processor = Arc::clone(&test_dispatcher().processor);

        let report = super::serve_with_service_context_and_ready(
            service.clone(),
            TransportTelemetry::noop(),
            test_remoting_config(),
            processor,
            ClientSessionRegistry::default(),
            None,
            None,
            std::future::ready(()),
            move || {
                ready_observer.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
        )
        .await
        .expect("immediate shutdown should still publish Proxy readiness")
        .expect("remoting server should return a shutdown report");

        assert!(ready_called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(report.is_healthy(), "{}", report.to_json());
        let parent_report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(parent_report.is_healthy(), "{}", parent_report.to_json());
    }

    #[tokio::test]
    async fn remoting_ready_error_waits_for_owned_server_cleanup() {
        let runtime = RuntimeContext::from_current("proxy-remoting-ready-error-cleanup-test");
        let service = runtime.service_context("proxy-remoting-ready-error-cleanup");
        let processor = Arc::clone(&test_dispatcher().processor);

        let error = super::serve_with_service_context_and_ready(
            service.clone(),
            TransportTelemetry::noop(),
            test_remoting_config(),
            processor,
            ClientSessionRegistry::default(),
            None,
            None,
            std::future::pending(),
            || {
                Err(crate::error::ProxyError::Transport {
                    message: "test readiness rejection".to_owned(),
                })
            },
        )
        .await
        .expect_err("readiness error must propagate after server cleanup");

        assert!(error.to_string().contains("test readiness rejection"));
        let parent_report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(parent_report.is_healthy(), "{}", parent_report.to_json());
    }

    #[tokio::test]
    async fn dispatcher_responses_keep_independent_owner_defaults() {
        let json_dispatcher = test_dispatcher_with_factory(RemotingCommandFactory::new(RemotingCommandDefaults::new(
            650,
            SerializeType::JSON,
        )));
        let binary_dispatcher = test_dispatcher_with_factory(RemotingCommandFactory::new(
            RemotingCommandDefaults::new(651, SerializeType::ROCKETMQ),
        ));
        let success_request = RemotingCommand::create_remoting_command(RequestCode::CheckClientConfig);
        let unsupported_request = RemotingCommand::create_remoting_command(i32::MAX);

        for (dispatcher, expected_version, expected_serialize_type) in [
            (&json_dispatcher, 650, SerializeType::JSON),
            (&binary_dispatcher, 651, SerializeType::ROCKETMQ),
        ] {
            let success = dispatcher.dispatch(&test_context(), &success_request).await;
            let unsupported = dispatcher.dispatch(&test_context(), &unsupported_request).await;

            assert_eq!(success.version(), expected_version);
            assert_eq!(success.serialize_type(), expected_serialize_type);
            assert_eq!(unsupported.version(), expected_version);
            assert_eq!(unsupported.serialize_type(), expected_serialize_type);
            assert_eq!(
                ResponseCode::from(unsupported.code()),
                ResponseCode::RequestCodeNotSupported
            );
            assert_eq!(
                unsupported.remark().map(CheetahString::as_str),
                Some("Protocol request is unsupported")
            );
        }
    }

    #[derive(Default)]
    struct TestRemotingBackend {
        seen_requests: Mutex<Vec<RemotingCommand>>,
    }

    impl ProxyRemotingBackend for TestRemotingBackend {
        fn process(
            &self,
            request: RemotingCommand,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'_, EmbeddedDispatchOutcome> {
            Box::pin(async move {
                self.seen_requests.lock().expect("backend mutex poisoned").push(request);
                RemotingResponse::command(RemotingCommand::create_response_command_with_code(
                    ResponseCode::Success,
                ))
                .map(EmbeddedDispatchOutcome::Reply)
                .map_err(|error| ProxyError::Transport {
                    message: format!("test remoting response: {error}"),
                })
            })
        }
    }

    struct AllowEmbeddedProxyPolicy;

    impl RequestPolicy for AllowEmbeddedProxyPolicy {
        fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
            Decision::Allow
        }
    }

    #[derive(Clone)]
    struct MutatingRequestFixture<P> {
        inner: P,
    }

    impl<P> RequestProcessor for MutatingRequestFixture<P>
    where
        P: RequestProcessor + Clone + Sync,
    {
        async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
            request.command_mut().set_code_ref(RequestCode::BeginProxyDrain);
            request.command_mut().set_opaque_mut(-9_852);
            self.inner.process(request).await
        }

        fn reject_request(&self, code: i32) -> RejectRequestDecision {
            self.inner.reject_request(code)
        }
    }

    #[tokio::test]
    async fn embedded_routes_and_bindings_use_original_identity_without_a_channel() {
        let backend = Arc::new(TestRemotingBackend::default());
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
        let drain = ProxyDrainController::default();
        let proxy = ProxyRequestProcessor::new_with_drain_controller(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            ClientSessionRegistry::default(),
            None,
            Some(backend.clone()),
            drain.clone(),
        );
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            MutatingRequestFixture { inner: proxy },
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedProxyPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let runtime = RuntimeContext::from_current("proxy-embedded-original-identity-test");
        let service = runtime.service_context("proxy-embedded-original-identity");
        let harness =
            EmbeddedRequestHarness::new(dispatcher, service.task_group().clone(), Principal::new("broker-proxy"));

        let response = harness
            .dispatch(
                None,
                RemotingCommand::create_remoting_command(RequestCode::ConsumerSendMsgBack).set_opaque(9_701),
            )
            .await
            .expect("embedded Proxy reply");
        let EmbeddedDispatchOutcome::Reply(plan) = response else {
            panic!("forward backend must produce a reply")
        };
        assert_eq!(plan.response_code(), ResponseCode::Success.to_i32());

        let local_response = harness
            .dispatch(
                None,
                RemotingCommand::new_request(
                    RequestCode::CheckClientConfig,
                    ProxyDrainOperationRequestBody {
                        schema_version: PROXY_DRAIN_SCHEMA_VERSION.to_owned(),
                        operation_id: "must-not-start".to_owned(),
                    }
                    .encode()
                    .expect("decoy drain body should encode"),
                )
                .set_opaque(9_703),
            )
            .await
            .expect("embedded non-backend reply");
        let EmbeddedDispatchOutcome::Reply(plan) = local_response else {
            panic!("original CheckClientConfig route must produce a reply")
        };
        assert_eq!(plan.response_code(), ResponseCode::Success.to_i32());
        assert_eq!(drain.phase(), ProxyDrainPhase::Accepting);

        let mut one_way = RemotingCommand::create_remoting_command(RequestCode::ConsumerSendMsgBack).set_opaque(9_702);
        one_way.mark_oneway_rpc_ref();
        let one_way_outcome = harness
            .dispatch(None, one_way)
            .await
            .expect("embedded one-way compatibility mapping");
        assert!(matches!(one_way_outcome, EmbeddedDispatchOutcome::OneWay { .. }));

        {
            let seen = backend.seen_requests.lock().expect("backend mutex poisoned");
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[0].code(), RequestCode::ConsumerSendMsgBack.to_i32());
            assert_eq!(seen[0].opaque(), 9_701);
            assert_eq!(seen[1].code(), RequestCode::ConsumerSendMsgBack.to_i32());
            assert_eq!(seen[1].opaque(), 9_702);
        }

        let shutdown = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(shutdown.is_healthy(), "{}", shutdown.to_json());
    }

    async fn test_auth_runtime(authentication_enabled: bool, authorization_enabled: bool) -> ProxyAuthRuntime {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("proxy-remoting-auth-test");
        ProxyAuthRuntime::from_proxy_config(
            &ProxyAuthConfig {
                authentication_enabled,
                authorization_enabled,
                auth_config_path: format!("target/proxy-remoting-auth-tests-{}", uuid::Uuid::new_v4()),
                ..ProxyAuthConfig::default()
            },
            &runtime.service_context("proxy-remoting-auth"),
        )
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
                Policy::of(
                    vec![PolicyResource::of_topic(topic)],
                    vec![action],
                    None,
                    PolicyDecision::Allow,
                ),
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
        for (_, value) in fields {
            content.extend_from_slice(value.as_bytes());
        }
        if let Some(body) = command.body() {
            content.extend_from_slice(body);
        }
        let signature = cal_signature(content.as_slice(), secret).expect("signature should be generated");
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
    async fn dispatch_auth_admin_request_codes_are_explicitly_unsupported() {
        let dispatcher = test_dispatcher();
        for request_code in [
            RequestCode::AuthCreateUser,
            RequestCode::AuthUpdateUser,
            RequestCode::AuthDeleteUser,
            RequestCode::AuthGetUser,
            RequestCode::AuthListUsers,
            RequestCode::AuthCreateAcl,
            RequestCode::AuthUpdateAcl,
            RequestCode::AuthDeleteAcl,
            RequestCode::AuthGetAcl,
            RequestCode::AuthListAcl,
        ] {
            let request = RemotingCommand::create_remoting_command(request_code);

            let response = dispatcher.dispatch(&test_context(), &request).await;

            assert_eq!(
                ResponseCode::from(response.code()),
                ResponseCode::RequestCodeNotSupported
            );
            assert_eq!(response.code(), 3);
            assert_eq!(
                response.remark().map(CheetahString::as_str),
                Some("Protocol request is unsupported")
            );
        }
    }

    #[tokio::test]
    async fn drain_management_requires_authentication_at_request_boundary() {
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
        let request_processor = ProxyRequestProcessor::new(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            sessions,
            None,
            None,
        );
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            request_processor,
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedProxyPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let runtime = RuntimeContext::from_current("proxy-drain-auth-test");
        let service = runtime.service_context("proxy-drain-auth");
        let harness =
            EmbeddedRequestHarness::new(dispatcher, service.task_group().clone(), Principal::new("proxy-admin"));

        let outcome = harness
            .dispatch(
                None,
                RemotingCommand::create_remoting_command(RequestCode::GetProxyDrainState),
            )
            .await
            .expect("request should be rejected with a remoting response");
        let EmbeddedDispatchOutcome::Reply(plan) = outcome else {
            panic!("drain authentication rejection must be a reply")
        };
        assert_eq!(plan.response_code(), ResponseCode::NoPermission.to_i32());

        let shutdown = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(shutdown.is_healthy(), "{}", shutdown.to_json());
    }

    #[tokio::test]
    async fn drain_dispatcher_stops_and_restores_readiness_with_exact_state() {
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
        let drain = ProxyDrainController::default();
        let lifecycle = ServiceLifecycle::new(ServiceLifecycleConfig {
            service_name: Arc::from("proxy-drain-dispatch-test"),
            probe_bind_addr: None,
            shutdown_timeout: std::time::Duration::from_secs(45),
            liveness_stale_after: std::time::Duration::from_secs(30),
        });
        lifecycle.mark_ready().expect("lifecycle should become ready");
        drain
            .attach_lifecycle(lifecycle.clone())
            .expect("drain should attach lifecycle");
        let dispatcher = ProxyRemotingDispatcher::new_with_drain_controller(
            Arc::new(ProxyConfig {
                mode: ProxyMode::Local,
                ..ProxyConfig::default()
            }),
            processor,
            sessions,
            None,
            drain,
        );
        let operation = ProxyDrainOperationRequestBody {
            schema_version: PROXY_DRAIN_SCHEMA_VERSION.to_owned(),
            operation_id: "restart-1".to_owned(),
        };
        let begin = RemotingCommand::new_request(
            RequestCode::BeginProxyDrain,
            operation.encode().expect("drain operation should encode"),
        );

        let begin_response = dispatcher.dispatch(&test_context(), &begin).await;
        assert_eq!(ResponseCode::from(begin_response.code()), ResponseCode::Success);
        let begun = ProxyDrainStateResponseBody::decode(
            begin_response
                .body()
                .expect("begin response should contain state")
                .as_ref(),
        )
        .expect("begin state should decode");
        assert_eq!(begun.phase, "drained");
        assert!(begun.zero_pending);
        assert!(!begun.admission_open);
        assert!(!begun.routing_open);
        assert!(!begun.readiness_published);
        assert!(!lifecycle.is_ready());

        let cancel = RemotingCommand::new_request(
            RequestCode::CancelProxyDrain,
            operation.encode().expect("drain operation should encode"),
        );
        let cancel_response = dispatcher.dispatch(&test_context(), &cancel).await;
        assert_eq!(ResponseCode::from(cancel_response.code()), ResponseCode::Success);
        let cancelled = ProxyDrainStateResponseBody::decode(
            cancel_response
                .body()
                .expect("cancel response should contain state")
                .as_ref(),
        )
        .expect("cancel state should decode");
        assert_eq!(cancelled.phase, "accepting");
        assert!(cancelled.admission_open);
        assert!(cancelled.routing_open);
        assert!(cancelled.readiness_published);
        assert!(lifecycle.is_ready());
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
            message_model: rocketmq_protocol::protocol::heartbeat::message_model::MessageModel::Clustering,
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
        first.set_properties(MessageDecoder::string_to_message_properties(Some(&properties)));
        let second = Message::builder()
            .topic("TopicA")
            .body_slice(b"hello-2")
            .build_unchecked();
        let batch =
            rocketmq_model::common::message::message_batch::MessageBatch::generate_from_vec(vec![first, second])
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
        .set_body(MessageDecoder::encode_messages(&batch.messages));
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
    async fn consumer_queries_read_committed_membership() {
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

        assert!(sessions.update_membership_from_remoting_heartbeat(
            &test_context(),
            "client-a",
            ["ProducerA".to_owned()].into(),
            ["GroupA".to_owned()].into(),
        ));
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
        let decoded = rocketmq_protocol::protocol::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody::decode(
            response.body().expect("consumer list body should exist").as_ref(),
        )
        .expect("consumer list body should decode");
        assert_eq!(decoded.consumer_id_list, vec![CheetahString::from("client-a")]);

        let mut connection_request = RemotingCommand::create_request_command(
            RequestCode::GetConsumerConnectionList,
            GetConsumerConnectionListRequestHeader {
                consumer_group: CheetahString::from("GroupA"),
                rpc_request_header: None,
            },
        );
        connection_request.make_custom_header_to_net();
        let connection_response = dispatcher.dispatch(&test_context(), &connection_request).await;
        assert_eq!(ResponseCode::from(connection_response.code()), ResponseCode::Success);
        let connections = ConsumerConnection::decode(
            connection_response
                .body()
                .expect("consumer connection body should exist")
                .as_ref(),
        )
        .expect("consumer connection body should decode");
        assert_eq!(connections.get_connection_set().len(), 1);
        assert_eq!(
            connections
                .get_connection_set()
                .iter()
                .next()
                .expect("consumer connection should exist")
                .get_client_id(),
            "client-a"
        );
    }

    #[tokio::test]
    async fn dispatcher_heartbeat_route_cannot_mutate_membership() {
        let sessions = ClientSessionRegistry::default();
        let dispatcher = ProxyRemotingDispatcher::new(
            Arc::new(ProxyConfig::default()),
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
        let request =
            RemotingCommand::create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
                .set_body(heartbeat.encode().expect("heartbeat should encode"));

        let response = dispatcher.dispatch(&test_context(), &request).await;

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert!(sessions.consumer_client_ids("GroupA").is_empty());
        assert_eq!(sessions.remoting_channel_count(), 0);
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

        assert!(sessions.update_membership_from_remoting_heartbeat(
            &test_context(),
            "client-a",
            std::collections::BTreeSet::new(),
            ["GroupA".to_owned()].into(),
        ));
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
        assert_eq!(response.opaque(), unregister_request.opaque());
        assert!(sessions.consumer_client_ids("GroupA").is_empty());
    }

    #[test]
    fn response_with_header_preserves_explicit_contract() {
        let response = response_with_header(
            &super::application_remoting_command_factory(),
            42,
            ResponseCode::QueryNotFound,
            QueryConsumerOffsetResponseHeader { offset: Some(7) },
            Some("offset unavailable".to_owned()),
            Some(Bytes::from_static(b"body")),
        );

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::QueryNotFound);
        assert!(response.is_response_type());
        assert_eq!(response.opaque(), 42);
        assert_eq!(response.remark().map(CheetahString::as_str), Some("offset unavailable"));
        assert_eq!(response.body().map(Bytes::as_ref), Some(b"body".as_slice()));
        let header = response
            .decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()
            .expect("query offset response header");
        assert_eq!(header.offset, Some(7));
    }

    #[tokio::test]
    async fn dispatch_pull_message_returns_header_and_body() {
        let dispatcher = test_dispatcher();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::PullMessage,
            PullMessageRequestHeader {
                consumer_group: CheetahString::from("GroupA"),
                topic: CheetahString::from("TopicA"),
                lite_topic: None,
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
                lite_topic: None,
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
    async fn legacy_command_dispatch_rejects_lock_and_unlock_backend_passthrough() {
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
        assert_eq!(
            ResponseCode::from(lock_response.code()),
            ResponseCode::RequestCodeNotSupported
        );

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
        assert_eq!(
            ResponseCode::from(unlock_response.code()),
            ResponseCode::RequestCodeNotSupported
        );

        assert!(backend.seen_requests.lock().expect("backend mutex poisoned").is_empty());
    }

    #[tokio::test]
    async fn command_dispatch_rejects_forward_routes_without_materializing_responses() {
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
        let codes = [
            RequestCode::ConsumerSendMsgBack,
            RequestCode::EndTransaction,
            RequestCode::RecallMessage,
            RequestCode::PopMessage,
            RequestCode::AckMessage,
            RequestCode::ChangeMessageInvisibleTime,
        ];

        for (index, code) in codes.into_iter().enumerate() {
            let opaque = 700 + index as i32;
            let request = RemotingCommand::create_remoting_command(code)
                .set_opaque(opaque)
                .set_version(501)
                .set_serialize_type(SerializeType::ROCKETMQ)
                .set_body(Bytes::from_static(b"forward-body"))
                .set_ext_fields(HashMap::from([
                    (
                        CheetahString::from_static_str("bname"),
                        CheetahString::from_static_str("broker-a"),
                    ),
                    (
                        CheetahString::from_static_str("custom"),
                        CheetahString::from_static_str("preserved"),
                    ),
                ]));
            let response = dispatcher.dispatch(&test_context(), &request).await;
            assert_eq!(
                ResponseCode::from(response.code()),
                ResponseCode::RequestCodeNotSupported
            );
            assert_eq!(response.opaque(), opaque);
            assert_eq!(response.code(), 3);
            assert_eq!(
                response.remark().map(CheetahString::as_str),
                Some("Protocol request is unsupported")
            );
        }

        assert!(backend.seen_requests.lock().expect("backend mutex poisoned").is_empty());
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
            rocketmq_protocol::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody::decode(
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
            rocketmq_protocol::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody::decode(
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
        let auth_context = RemotingAuthContext::network("127.0.0.1", "channel-a");

        let principal = auth_runtime
            .authenticate_remoting(&command, &auth_context)
            .await
            .expect("authentication should succeed");
        assert_eq!(
            principal.as_ref().expect("principal should be returned").username(),
            "alice"
        );
        auth_runtime
            .authorize_remoting(&auth_context, &command)
            .await
            .expect("authorization should succeed");

        let denied_runtime = test_auth_runtime(true, true).await;
        seed_normal_user(&denied_runtime, "bob", "secret").await;
        let denied_command = sign_remoting_command(command.clone(), "bob", "secret");
        let denied_auth_context = RemotingAuthContext::network("127.0.0.1", "channel-b");
        denied_runtime
            .authenticate_remoting(&denied_command, &denied_auth_context)
            .await
            .expect("authentication should still succeed");
        let error = denied_runtime
            .authorize_remoting(&denied_auth_context, &denied_command)
            .await
            .expect_err("authorization should fail without matching acl");
        assert!(matches!(
            error,
            crate::error::ProxyError::RocketMQ(RocketMQError::BrokerPermissionDenied { .. })
        ));
    }

    struct TestAssignmentService;

    impl AssignmentService for TestAssignmentService {
        fn query_assignment<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            _topic: &'a ResourceIdentity,
            _group: &'a ResourceIdentity,
            _endpoints: &'a [crate::context::ResolvedEndpoint],
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, Option<Vec<MessageQueueAssignment>>> {
            Box::pin(async {
                Ok(Some(vec![MessageQueueAssignment {
                    message_queue: Some(MessageQueue::from_parts("TopicA", CheetahString::from("broker-a"), 0)),
                    mode: MessageRequestMode::Pull,
                    attachments: None,
                }]))
            })
        }
    }

    struct TestMessageService;

    impl MessageService for TestMessageService {
        fn send_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a SendMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, Vec<SendMessageResultEntry>> {
            Box::pin(async move {
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
            })
        }

        fn recall_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a RecallMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, RecallMessagePlan> {
            Box::pin(async move {
                Ok(RecallMessagePlan {
                    status: ProxyStatusMapper::ok_payload(),
                    message_id: request.recall_handle.clone(),
                })
            })
        }
    }

    struct TestConsumerService;

    impl ConsumerService for TestConsumerService {
        fn receive_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            _request: &'a ReceiveMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, ReceiveMessagePlan> {
            Box::pin(async { Err(crate::error::ProxyError::not_implemented("test receive")) })
        }

        fn pull_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a PullMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, PullMessagePlan> {
            Box::pin(async move {
                let message = ProxyMessageExt {
                    message: ProxyMessage::new(request.target.topic.to_string(), b"hello".to_vec()),
                    msg_id: "pull-msg-id".to_owned(),
                    queue_id: request.target.queue_id,
                    queue_offset: request.offset,
                    ..ProxyMessageExt::default()
                };
                Ok(PullMessagePlan {
                    status: ProxyStatusMapper::ok_payload(),
                    next_offset: request.offset + 1,
                    min_offset: 0,
                    max_offset: 1024,
                    messages: vec![message],
                })
            })
        }

        fn ack_message<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            _request: &'a AckMessageRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, Vec<AckMessageResultEntry>> {
            Box::pin(async { Err(crate::error::ProxyError::not_implemented("test ack")) })
        }

        fn forward_message_to_dead_letter_queue<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            _request: &'a ForwardMessageToDeadLetterQueueRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, ForwardMessageToDeadLetterQueuePlan> {
            Box::pin(async { Err(crate::error::ProxyError::not_implemented("test dlq")) })
        }

        fn change_invisible_duration<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            _request: &'a ChangeInvisibleDurationRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, ChangeInvisibleDurationPlan> {
            Box::pin(async { Err(crate::error::ProxyError::not_implemented("test change invisible")) })
        }

        fn update_offset<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            _request: &'a UpdateOffsetRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, UpdateOffsetPlan> {
            Box::pin(async {
                Ok(UpdateOffsetPlan {
                    status: ProxyStatusMapper::ok_payload(),
                })
            })
        }

        fn get_offset<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            _request: &'a GetOffsetRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, GetOffsetPlan> {
            Box::pin(async {
                Ok(GetOffsetPlan {
                    status: ProxyStatusMapper::ok_payload(),
                    offset: 123,
                })
            })
        }

        fn query_offset<'a>(
            &'a self,
            _context: &'a CoreProxyContext,
            request: &'a QueryOffsetRequest,
        ) -> rocketmq_proxy_core::ProxyServiceFuture<'a, QueryOffsetPlan> {
            Box::pin(async move {
                let offset = match request.policy {
                    QueryOffsetPolicy::Beginning => 0,
                    QueryOffsetPolicy::End => 2048,
                    QueryOffsetPolicy::Timestamp => request.timestamp_ms.unwrap_or_default(),
                };
                Ok(QueryOffsetPlan {
                    status: ProxyStatusMapper::ok_payload(),
                    offset,
                })
            })
        }
    }
}
