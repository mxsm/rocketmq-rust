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
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use cheetah_string::CheetahString;
use rocketmq_broker::ProxyBrokerFacade;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_client_rust::producer::send_status::SendStatus;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::prelude::RemotingDeserializable;
use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
use rocketmq_remoting::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_remoting::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::TopicRequestHeader as OperationTopicRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_remoting::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_remoting::protocol::header::recall_message_response_header::RecallMessageResponseHeader;
use rocketmq_remoting::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_remoting::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::config::LocalConfig;
use crate::context::ProxyContext;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::processor::AckMessageRequest;
use crate::processor::AckMessageResultEntry;
use crate::processor::ChangeInvisibleDurationPlan;
use crate::processor::ChangeInvisibleDurationRequest;
use crate::processor::EndTransactionPlan;
use crate::processor::EndTransactionRequest;
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
use crate::processor::ReceivedMessage;
use crate::processor::SendMessageEntry;
use crate::processor::SendMessageRequest;
use crate::processor::SendMessageResultEntry;
use crate::processor::TransactionResolution;
use crate::processor::TransactionSource;
use crate::processor::UpdateOffsetPlan;
use crate::processor::UpdateOffsetRequest;
use crate::remoting::ProxyRemotingBackend;
use crate::service::AssignmentService;
use crate::service::ConsumerService;
use crate::service::LocalServiceManager;
use crate::service::MessageService;
use crate::service::MetadataService;
use crate::service::ProxyTopicMessageType;
use crate::service::ResourceIdentity;
use crate::service::RouteService;
use crate::service::SubscriptionGroupMetadata;
use crate::service::TransactionService;
use crate::status::ProxyStatusMapper;

#[derive(Clone)]
pub struct LocalBrokerFacadeClient {
    sender: mpsc::UnboundedSender<LocalBrokerCommand>,
    broker_name: String,
}

enum LocalBrokerCommand {
    QueryRoute {
        topic: ResourceIdentity,
        reply: oneshot::Sender<ProxyResult<TopicRouteData>>,
    },
    QueryTopicMessageType {
        topic: ResourceIdentity,
        reply: oneshot::Sender<ProxyResult<ProxyTopicMessageType>>,
    },
    QuerySubscriptionGroup {
        group: ResourceIdentity,
        reply: oneshot::Sender<ProxyResult<Option<SubscriptionGroupMetadata>>>,
    },
    QueryAssignment {
        topic: ResourceIdentity,
        group: ResourceIdentity,
        client_id: String,
        strategy_name: String,
        reply: oneshot::Sender<ProxyResult<Option<Vec<MessageQueueAssignment>>>>,
    },
    SendMessage {
        request: SendMessageRequest,
        client_id: Option<String>,
        request_id: String,
        reply: oneshot::Sender<ProxyResult<Vec<SendMessageResultEntry>>>,
    },
    RecallMessage {
        request: RecallMessageRequest,
        client_id: Option<String>,
        request_id: String,
        reply: oneshot::Sender<ProxyResult<RecallMessagePlan>>,
    },
    EndTransaction {
        request: EndTransactionRequest,
        client_id: Option<String>,
        request_id: String,
        reply: oneshot::Sender<ProxyResult<EndTransactionPlan>>,
    },
    ProcessRemoting {
        request: RemotingCommand,
        reply: oneshot::Sender<ProxyResult<RemotingCommand>>,
    },
}

impl LocalBrokerFacadeClient {
    pub fn new(config: LocalConfig) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let broker_name = config.broker_name.clone();
        let thread_name = format!(
            "rocketmq-proxy-local-{}",
            sanitize_thread_component(&config.broker_name)
        );
        thread::Builder::new()
            .name(thread_name)
            .spawn(move || run_local_broker_worker(config, receiver))
            .expect("failed to spawn proxy local broker worker");
        Self { sender, broker_name }
    }

    pub fn broker_name(&self) -> &str {
        self.broker_name.as_str()
    }

    pub async fn query_route(&self, topic: ResourceIdentity) -> ProxyResult<TopicRouteData> {
        self.execute(|reply| LocalBrokerCommand::QueryRoute { topic, reply })
            .await
    }

    pub async fn query_topic_message_type(&self, topic: ResourceIdentity) -> ProxyResult<ProxyTopicMessageType> {
        self.execute(|reply| LocalBrokerCommand::QueryTopicMessageType { topic, reply })
            .await
    }

    pub async fn query_subscription_group(
        &self,
        _topic: ResourceIdentity,
        group: ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        self.execute(|reply| LocalBrokerCommand::QuerySubscriptionGroup { group, reply })
            .await
    }

    pub async fn query_assignment(
        &self,
        topic: ResourceIdentity,
        group: ResourceIdentity,
        client_id: String,
        strategy_name: String,
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        self.execute(|reply| LocalBrokerCommand::QueryAssignment {
            topic,
            group,
            client_id,
            strategy_name,
            reply,
        })
        .await
    }

    pub async fn process_remoting(&self, request: RemotingCommand) -> ProxyResult<RemotingCommand> {
        self.execute(|reply| LocalBrokerCommand::ProcessRemoting { request, reply })
            .await
    }

    pub async fn send_message(
        &self,
        request: SendMessageRequest,
        client_id: Option<String>,
        request_id: String,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        self.execute(|reply| LocalBrokerCommand::SendMessage {
            request,
            client_id,
            request_id,
            reply,
        })
        .await
    }

    pub async fn recall_message(
        &self,
        request: RecallMessageRequest,
        client_id: Option<String>,
        request_id: String,
    ) -> ProxyResult<RecallMessagePlan> {
        self.execute(|reply| LocalBrokerCommand::RecallMessage {
            request,
            client_id,
            request_id,
            reply,
        })
        .await
    }

    pub async fn end_transaction(
        &self,
        request: EndTransactionRequest,
        client_id: Option<String>,
        request_id: String,
    ) -> ProxyResult<EndTransactionPlan> {
        self.execute(|reply| LocalBrokerCommand::EndTransaction {
            request,
            client_id,
            request_id,
            reply,
        })
        .await
    }

    async fn execute<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<ProxyResult<T>>) -> LocalBrokerCommand,
    ) -> ProxyResult<T> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(build(reply_tx))
            .map_err(|error| ProxyError::Transport {
                message: format!("local broker worker is not available: {error}"),
            })?;
        reply_rx.await.map_err(|error| ProxyError::Transport {
            message: format!("local broker worker dropped response channel: {error}"),
        })?
    }
}

#[derive(Clone)]
pub struct LocalRemotingBackend {
    client: LocalBrokerFacadeClient,
}

impl LocalRemotingBackend {
    pub fn new(client: LocalBrokerFacadeClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ProxyRemotingBackend for LocalRemotingBackend {
    async fn process(&self, request: RemotingCommand) -> ProxyResult<RemotingCommand> {
        self.client.process_remoting(request).await
    }
}

#[derive(Clone)]
pub struct LocalRouteService {
    client: LocalBrokerFacadeClient,
}

impl LocalRouteService {
    pub fn new(client: LocalBrokerFacadeClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RouteService for LocalRouteService {
    async fn query_route(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<TopicRouteData> {
        self.client.query_route(topic.clone()).await
    }
}

#[derive(Clone)]
pub struct LocalMetadataService {
    client: LocalBrokerFacadeClient,
}

impl LocalMetadataService {
    pub fn new(client: LocalBrokerFacadeClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MetadataService for LocalMetadataService {
    async fn topic_message_type(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
    ) -> ProxyResult<ProxyTopicMessageType> {
        self.client.query_topic_message_type(topic.clone()).await
    }

    async fn subscription_group(
        &self,
        _context: &ProxyContext,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        self.client.query_subscription_group(topic.clone(), group.clone()).await
    }
}

#[derive(Clone)]
pub struct LocalAssignmentService {
    client: LocalBrokerFacadeClient,
    strategy_name: String,
}

impl LocalAssignmentService {
    pub fn new(client: LocalBrokerFacadeClient, strategy_name: impl Into<String>) -> Self {
        Self {
            client,
            strategy_name: strategy_name.into(),
        }
    }
}

#[async_trait]
impl AssignmentService for LocalAssignmentService {
    async fn query_assignment(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
        _endpoints: &[ResolvedEndpoint],
    ) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
        self.client
            .query_assignment(
                topic.clone(),
                group.clone(),
                context.require_client_id()?.to_owned(),
                self.strategy_name.clone(),
            )
            .await
    }
}

#[derive(Clone)]
pub struct LocalMessageService {
    client: LocalBrokerFacadeClient,
}

impl LocalMessageService {
    pub fn new(client: LocalBrokerFacadeClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MessageService for LocalMessageService {
    async fn send_message(
        &self,
        context: &ProxyContext,
        request: &SendMessageRequest,
    ) -> ProxyResult<Vec<SendMessageResultEntry>> {
        self.client
            .send_message(
                request.clone(),
                context.client_id().map(ToOwned::to_owned),
                context.request_id().to_owned(),
            )
            .await
    }

    async fn recall_message(
        &self,
        context: &ProxyContext,
        request: &RecallMessageRequest,
    ) -> ProxyResult<RecallMessagePlan> {
        self.client
            .recall_message(
                request.clone(),
                context.client_id().map(ToOwned::to_owned),
                context.request_id().to_owned(),
            )
            .await
    }
}

#[derive(Clone)]
pub struct LocalTransactionService {
    client: LocalBrokerFacadeClient,
}

impl LocalTransactionService {
    pub fn new(client: LocalBrokerFacadeClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl TransactionService for LocalTransactionService {
    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: &EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan> {
        self.client
            .end_transaction(
                request.clone(),
                context.client_id().map(ToOwned::to_owned),
                context.request_id().to_owned(),
            )
            .await
    }
}

#[derive(Clone)]
pub struct LocalConsumerService {
    client: LocalBrokerFacadeClient,
}

impl LocalConsumerService {
    pub fn new(client: LocalBrokerFacadeClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ConsumerService for LocalConsumerService {
    async fn receive_message(
        &self,
        _context: &ProxyContext,
        request: &ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan> {
        receive_message_via_broker(&self.client, request).await
    }

    async fn pull_message(
        &self,
        _context: &ProxyContext,
        request: &PullMessageRequest,
    ) -> ProxyResult<PullMessagePlan> {
        pull_message_via_broker(&self.client, request).await
    }

    async fn ack_message(
        &self,
        _context: &ProxyContext,
        request: &AckMessageRequest,
    ) -> ProxyResult<Vec<AckMessageResultEntry>> {
        ack_message_via_broker(&self.client, request).await
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        _context: &ProxyContext,
        request: &ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
        forward_message_to_dead_letter_queue_via_broker(&self.client, request).await
    }

    async fn change_invisible_duration(
        &self,
        _context: &ProxyContext,
        request: &ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        change_invisible_duration_via_broker(&self.client, request).await
    }

    async fn update_offset(
        &self,
        _context: &ProxyContext,
        request: &UpdateOffsetRequest,
    ) -> ProxyResult<UpdateOffsetPlan> {
        update_offset_via_broker(&self.client, request).await
    }

    async fn get_offset(&self, _context: &ProxyContext, request: &GetOffsetRequest) -> ProxyResult<GetOffsetPlan> {
        get_offset_via_broker(&self.client, request).await
    }

    async fn query_offset(
        &self,
        _context: &ProxyContext,
        request: &QueryOffsetRequest,
    ) -> ProxyResult<QueryOffsetPlan> {
        query_offset_via_broker(&self.client, request).await
    }
}

pub fn local_components_from_config(
    config: LocalConfig,
    strategy_name: impl Into<String>,
) -> (LocalServiceManager, LocalBrokerFacadeClient) {
    let client = LocalBrokerFacadeClient::new(config);
    let backend_client = client.clone();
    let manager = LocalServiceManager::with_services(
        std::sync::Arc::new(LocalRouteService::new(client.clone())),
        std::sync::Arc::new(LocalMetadataService::new(client.clone())),
        std::sync::Arc::new(LocalAssignmentService::new(client.clone(), strategy_name)),
        std::sync::Arc::new(LocalMessageService::new(client.clone())),
        std::sync::Arc::new(LocalConsumerService::new(client.clone())),
        std::sync::Arc::new(LocalTransactionService::new(client)),
    );
    (manager, backend_client)
}

pub fn local_service_manager_from_config(config: LocalConfig, strategy_name: impl Into<String>) -> LocalServiceManager {
    local_components_from_config(config, strategy_name).0
}

fn run_local_broker_worker(config: LocalConfig, mut receiver: mpsc::UnboundedReceiver<LocalBrokerCommand>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build proxy local broker runtime");
    runtime.block_on(async move {
        let mut facade = ProxyBrokerFacade::new(build_broker_config(&config), build_message_store_config(&config));
        let startup_error = if !facade.initialize().await {
            Some("embedded broker initialization failed".to_owned())
        } else {
            facade.start().await;
            None
        };

        while let Some(command) = receiver.recv().await {
            match command {
                LocalBrokerCommand::QueryRoute { topic, reply } => {
                    let _ = reply.send(startup_error.as_ref().map_or_else(
                        || facade.query_route(topic.name()).map_err(Into::into),
                        |message| {
                            Err(ProxyError::Transport {
                                message: message.clone(),
                            })
                        },
                    ));
                }
                LocalBrokerCommand::QueryTopicMessageType { topic, reply } => {
                    let _ = reply.send(startup_error.as_ref().map_or_else(
                        || {
                            facade
                                .query_topic_message_type(topic.name())
                                .map(convert_topic_message_type)
                                .map_err(Into::into)
                        },
                        |message| {
                            Err(ProxyError::Transport {
                                message: message.clone(),
                            })
                        },
                    ));
                }
                LocalBrokerCommand::QuerySubscriptionGroup { group, reply } => {
                    let _ = reply.send(startup_error.as_ref().map_or_else(
                        || {
                            facade
                                .query_subscription_group(group.name())
                                .map(|config| config.map(convert_subscription_group))
                                .map_err(Into::into)
                        },
                        |message| {
                            Err(ProxyError::Transport {
                                message: message.clone(),
                            })
                        },
                    ));
                }
                LocalBrokerCommand::QueryAssignment {
                    topic,
                    group,
                    client_id,
                    strategy_name,
                    reply,
                } => {
                    let result = if let Some(message) = startup_error.as_ref() {
                        Err(ProxyError::Transport {
                            message: message.clone(),
                        })
                    } else {
                        query_assignment(&facade, topic, group, client_id, strategy_name).await
                    };
                    let _ = reply.send(result);
                }
                LocalBrokerCommand::SendMessage {
                    request,
                    client_id,
                    request_id,
                    reply,
                } => {
                    let result = if let Some(message) = startup_error.as_ref() {
                        Err(ProxyError::Transport {
                            message: message.clone(),
                        })
                    } else {
                        send_message(&facade, request, client_id, request_id).await
                    };
                    let _ = reply.send(result);
                }
                LocalBrokerCommand::RecallMessage {
                    request,
                    client_id,
                    request_id,
                    reply,
                } => {
                    let result = if let Some(message) = startup_error.as_ref() {
                        Err(ProxyError::Transport {
                            message: message.clone(),
                        })
                    } else {
                        recall_message(&facade, request, client_id, request_id).await
                    };
                    let _ = reply.send(result);
                }
                LocalBrokerCommand::EndTransaction {
                    request,
                    client_id,
                    request_id,
                    reply,
                } => {
                    let result = if let Some(message) = startup_error.as_ref() {
                        Err(ProxyError::Transport {
                            message: message.clone(),
                        })
                    } else {
                        end_transaction(&facade, request, client_id, request_id).await
                    };
                    let _ = reply.send(result);
                }
                LocalBrokerCommand::ProcessRemoting { request, reply } => {
                    let result = if let Some(message) = startup_error.as_ref() {
                        Err(ProxyError::Transport {
                            message: message.clone(),
                        })
                    } else {
                        facade.process_request(request).await.map_err(Into::into)
                    };
                    let _ = reply.send(result);
                }
            }
        }

        facade.shutdown().await;
    });
}

async fn query_assignment(
    facade: &ProxyBrokerFacade,
    topic: ResourceIdentity,
    group: ResourceIdentity,
    client_id: String,
    strategy_name: String,
) -> ProxyResult<Option<Vec<MessageQueueAssignment>>> {
    let request_body = QueryAssignmentRequestBody {
        topic: CheetahString::from(topic.to_string()),
        consumer_group: CheetahString::from(group.to_string()),
        client_id: CheetahString::from(client_id),
        strategy_name: CheetahString::from(strategy_name),
        message_model: MessageModel::Clustering,
    };
    let request = RemotingCommand::new_request(
        RequestCode::QueryAssignment,
        request_body.encode().map_err(|error| ProxyError::Transport {
            message: format!("failed to encode local assignment request: {error}"),
        })?,
    );
    let response = facade.process_request(request).await?;
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(RocketMQError::Internal(format!(
            "embedded broker query assignment failed: {}",
            response.remark().cloned().unwrap_or_default()
        ))
        .into());
    }

    let Some(body) = response.body() else {
        return Ok(None);
    };
    let decoded = QueryAssignmentResponseBody::decode(body.as_ref()).map_err(|error| ProxyError::Transport {
        message: format!("failed to decode local assignment response: {error}"),
    })?;
    Ok(Some(decoded.message_queue_assignments.into_iter().collect()))
}

async fn send_message(
    facade: &ProxyBrokerFacade,
    request: SendMessageRequest,
    client_id: Option<String>,
    request_id: String,
) -> ProxyResult<Vec<SendMessageResultEntry>> {
    let producer_group = build_local_proxy_producer_group(client_id.as_deref(), request_id.as_str());
    let broker_name = facade.broker_config().broker_identity.broker_name.clone();
    let mut results = Vec::with_capacity(request.messages.len());
    for entry in request.messages {
        results.push(send_message_entry(facade, &broker_name, producer_group.as_str(), entry).await);
    }
    Ok(results)
}

async fn send_message_entry(
    facade: &ProxyBrokerFacade,
    broker_name: &CheetahString,
    producer_group: &str,
    entry: SendMessageEntry,
) -> SendMessageResultEntry {
    match send_message_entry_inner(facade, broker_name, producer_group, entry).await {
        Ok(send_result) => SendMessageResultEntry {
            status: ProxyStatusMapper::from_send_result_payload(&send_result),
            send_result: Some(send_result),
        },
        Err(error) => SendMessageResultEntry {
            status: ProxyStatusMapper::from_error_payload(&error),
            send_result: None,
        },
    }
}

async fn send_message_entry_inner(
    facade: &ProxyBrokerFacade,
    broker_name: &CheetahString,
    producer_group: &str,
    mut entry: SendMessageEntry,
) -> ProxyResult<SendResult> {
    attach_transaction_producer_group(&mut entry.message, producer_group);
    let request = build_send_message_request(broker_name, producer_group, &entry)?;
    let response = facade.process_request(request).await?;
    build_send_result(entry.topic, broker_name, response)
}

async fn recall_message(
    facade: &ProxyBrokerFacade,
    request: RecallMessageRequest,
    client_id: Option<String>,
    request_id: String,
) -> ProxyResult<RecallMessagePlan> {
    let producer_group = build_local_proxy_producer_group(client_id.as_deref(), request_id.as_str());
    let header = RecallMessageRequestHeader::new(
        request.topic.to_string(),
        request.recall_handle.as_str(),
        Some(producer_group.as_str()),
    );
    let mut command = RemotingCommand::create_request_command(RequestCode::RecallMessage, header);
    command.make_custom_header_to_net();
    let response = facade.process_request(command).await?;
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(broker_operation_error("recallMessage", &response).into());
    }

    let header = response
        .decode_command_custom_header::<RecallMessageResponseHeader>()
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to decode local recall response header: {error}"),
        })?;
    Ok(RecallMessagePlan {
        status: ProxyStatusMapper::ok_payload(),
        message_id: header.msg_id().to_string(),
    })
}

async fn end_transaction(
    facade: &ProxyBrokerFacade,
    request: EndTransactionRequest,
    client_id: Option<String>,
    request_id: String,
) -> ProxyResult<EndTransactionPlan> {
    let producer_group = request
        .producer_group
        .clone()
        .unwrap_or_else(|| build_local_proxy_producer_group(client_id.as_deref(), request_id.as_str()));
    let transaction_state_table_offset = request
        .transaction_state_table_offset
        .ok_or_else(|| ProxyError::invalid_transaction_id("missing transactional message offset for endTransaction"))?;
    let commit_log_message_id = request
        .commit_log_message_id
        .as_deref()
        .unwrap_or(request.message_id.as_str());
    let broker_message_id =
        MessageDecoder::decode_message_id(&CheetahString::from(commit_log_message_id)).map_err(|error| {
            ProxyError::invalid_transaction_id(format!("failed to decode transactional message id: {error}"))
        })?;

    let header = EndTransactionRequestHeader {
        topic: CheetahString::from(request.topic.to_string()),
        producer_group: CheetahString::from(producer_group),
        tran_state_table_offset: transaction_state_table_offset,
        commit_log_offset: broker_message_id.offset as u64,
        commit_or_rollback: transaction_resolution_flag(request.resolution),
        from_transaction_check: matches!(request.source, TransactionSource::ServerCheck),
        msg_id: CheetahString::from(request.message_id.as_str()),
        transaction_id: Some(CheetahString::from(request.transaction_id.as_str())),
        rpc_request_header: RpcRequestHeader::default(),
    };
    let mut command = RemotingCommand::create_request_command(RequestCode::EndTransaction, header)
        .set_remark(CheetahString::from(request.trace_context.as_deref().unwrap_or("")));
    command.make_custom_header_to_net();
    let response = facade.process_request(command).await?;
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(broker_operation_error("endTransaction", &response).into());
    }

    Ok(EndTransactionPlan {
        status: ProxyStatusMapper::ok_payload(),
    })
}

async fn receive_message_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &ReceiveMessageRequest,
) -> ProxyResult<ReceiveMessagePlan> {
    let header = build_pop_request_header(client.broker_name(), request);
    let response = client
        .process_remoting(RemotingCommand::create_request_command(RequestCode::PopMessage, header))
        .await?;
    process_pop_response(
        response,
        client.broker_name(),
        request.target.topic.to_string().as_str(),
        request.target.fifo,
    )
}

async fn pull_message_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &PullMessageRequest,
) -> ProxyResult<PullMessagePlan> {
    let header = build_pull_request_header(client.broker_name(), request);
    let response = client
        .process_remoting(RemotingCommand::create_request_command(
            RequestCode::PullMessage,
            header,
        ))
        .await?;
    process_pull_response(response)
}

async fn ack_message_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &AckMessageRequest,
) -> ProxyResult<Vec<AckMessageResultEntry>> {
    let mut results = Vec::with_capacity(request.entries.len());
    for entry in &request.entries {
        let status = match ack_message_entry_via_broker(client, request, entry).await {
            Ok(status) => status,
            Err(error) => ProxyStatusMapper::from_error_payload(&error),
        };
        results.push(AckMessageResultEntry {
            message_id: entry.message_id.clone(),
            receipt_handle: entry.receipt_handle.clone(),
            status,
        });
    }
    Ok(results)
}

async fn ack_message_entry_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &AckMessageRequest,
    entry: &crate::processor::AckMessageEntry,
) -> ProxyResult<crate::status::ProxyPayloadStatus> {
    let topic_name = entry.lite_topic.clone().unwrap_or_else(|| request.topic.to_string());
    let group_name = request.group.to_string();
    let parsed = parse_receipt_handle(entry.receipt_handle.as_str(), topic_name.as_str(), group_name.as_str())?;
    let header = AckMessageRequestHeader {
        consumer_group: CheetahString::from(group_name),
        topic: parsed.topic.clone(),
        queue_id: parsed.queue_id,
        extra_info: parsed.raw.clone(),
        offset: parsed.queue_offset,
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(parsed.broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let response = client
        .process_remoting(RemotingCommand::create_request_command(RequestCode::AckMessage, header))
        .await?;

    Ok(if ResponseCode::from(response.code()) == ResponseCode::Success {
        ProxyStatusMapper::ok_payload()
    } else {
        ProxyStatusMapper::from_payload_code(
            crate::proto::v2::Code::InvalidReceiptHandle,
            "receipt handle has expired or message no longer exists",
        )
    })
}

async fn forward_message_to_dead_letter_queue_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &ForwardMessageToDeadLetterQueueRequest,
) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
    let topic_name = request.lite_topic.clone().unwrap_or_else(|| request.topic.to_string());
    let parsed = parse_receipt_handle(
        request.receipt_handle.as_str(),
        topic_name.as_str(),
        request.group.to_string().as_str(),
    )?;
    let broker_message_id = decode_broker_message_id(request.message_id.as_str())?;
    let header = ConsumerSendMsgBackRequestHeader {
        offset: broker_message_id.offset,
        group: CheetahString::from(request.group.to_string()),
        delay_level: -1,
        origin_msg_id: Some(CheetahString::from(request.message_id.as_str())),
        origin_topic: Some(parsed.topic),
        unit_mode: false,
        max_reconsume_times: Some(request.max_delivery_attempts),
        rpc_request_header: Some(RpcRequestHeader {
            broker_name: Some(parsed.broker_name),
            ..Default::default()
        }),
    };
    let response = client
        .process_remoting(RemotingCommand::create_request_command(
            RequestCode::ConsumerSendMsgBack,
            header,
        ))
        .await?;
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(broker_operation_error("forwardMessageToDeadLetterQueue", &response).into());
    }

    Ok(ForwardMessageToDeadLetterQueuePlan {
        status: ProxyStatusMapper::ok_payload(),
    })
}

async fn change_invisible_duration_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &ChangeInvisibleDurationRequest,
) -> ProxyResult<ChangeInvisibleDurationPlan> {
    let topic_name = request.lite_topic.clone().unwrap_or_else(|| request.topic.to_string());
    let parsed = parse_receipt_handle(
        request.receipt_handle.as_str(),
        topic_name.as_str(),
        request.group.to_string().as_str(),
    )?;
    let header = ChangeInvisibleTimeRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: parsed.topic.clone(),
        queue_id: parsed.queue_id,
        extra_info: parsed.raw.clone(),
        offset: parsed.queue_offset,
        invisible_time: request.invisible_duration.as_millis().clamp(1, i64::MAX as u128) as i64,
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(parsed.broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let response = client
        .process_remoting(RemotingCommand::create_request_command(
            RequestCode::ChangeMessageInvisibleTime,
            header,
        ))
        .await?;
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Ok(ChangeInvisibleDurationPlan {
            status: ProxyStatusMapper::from_payload_code(
                crate::proto::v2::Code::InvalidReceiptHandle,
                "receipt handle has expired or message no longer exists",
            ),
            receipt_handle: String::new(),
        });
    }

    let response_header = response
        .decode_command_custom_header::<ChangeInvisibleTimeResponseHeader>()
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to decode local changeInvisibleDuration response header: {error}"),
        })?;
    Ok(ChangeInvisibleDurationPlan {
        status: ProxyStatusMapper::ok_payload(),
        receipt_handle: format!(
            "{}{}{}",
            ExtraInfoUtil::build_extra_info(
                parsed.queue_offset,
                response_header.pop_time as i64,
                response_header.invisible_time,
                response_header.revive_qid as i32,
                parsed.topic.as_str(),
                &parsed.broker_name,
                parsed.queue_id,
            ),
            MessageConst::KEY_SEPARATOR,
            parsed.queue_offset
        ),
    })
}

async fn update_offset_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &UpdateOffsetRequest,
) -> ProxyResult<UpdateOffsetPlan> {
    let header = build_update_offset_request_header(client.broker_name(), request);
    let response = client
        .process_remoting(RemotingCommand::create_request_command(
            RequestCode::UpdateConsumerOffset,
            header,
        ))
        .await?;
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(broker_operation_error("updateOffset", &response).into());
    }

    Ok(UpdateOffsetPlan {
        status: ProxyStatusMapper::ok_payload(),
    })
}

async fn get_offset_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &GetOffsetRequest,
) -> ProxyResult<GetOffsetPlan> {
    let header = build_query_consumer_offset_request_header(client.broker_name(), request);
    let response = client
        .process_remoting(RemotingCommand::create_request_command(
            RequestCode::QueryConsumerOffset,
            header,
        ))
        .await?;
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(broker_operation_error("getOffset", &response).into());
    }

    let response_header = response
        .decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to decode local getOffset response header: {error}"),
        })?;
    Ok(GetOffsetPlan {
        status: ProxyStatusMapper::ok_payload(),
        offset: response_header.offset.unwrap_or_default(),
    })
}

async fn query_offset_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &QueryOffsetRequest,
) -> ProxyResult<QueryOffsetPlan> {
    let broker_name = CheetahString::from(client.broker_name());
    let (request_code, command) = match request.policy {
        QueryOffsetPolicy::Beginning => {
            let header = GetMinOffsetRequestHeader {
                topic: CheetahString::from(request.target.topic.to_string()),
                queue_id: request.target.queue_id,
                topic_request_header: Some(TopicRequestHeader {
                    rpc_request_header: Some(RpcRequestHeader {
                        broker_name: Some(broker_name.clone()),
                        ..Default::default()
                    }),
                    lo: None,
                }),
            };
            (
                RequestCode::GetMinOffset,
                RemotingCommand::create_request_command(RequestCode::GetMinOffset, header),
            )
        }
        QueryOffsetPolicy::End => {
            let header = GetMaxOffsetRequestHeader {
                topic: CheetahString::from(request.target.topic.to_string()),
                queue_id: request.target.queue_id,
                committed: false,
                topic_request_header: Some(TopicRequestHeader {
                    rpc_request_header: Some(RpcRequestHeader {
                        broker_name: Some(broker_name.clone()),
                        ..Default::default()
                    }),
                    lo: None,
                }),
            };
            (
                RequestCode::GetMaxOffset,
                RemotingCommand::create_request_command(RequestCode::GetMaxOffset, header),
            )
        }
        QueryOffsetPolicy::Timestamp => {
            let header = SearchOffsetRequestHeader {
                topic: CheetahString::from(request.target.topic.to_string()),
                queue_id: request.target.queue_id,
                timestamp: request
                    .timestamp_ms
                    .ok_or_else(|| ProxyError::illegal_offset("timestamp policy requires timestamp to be present"))?,
                boundary_type: BoundaryType::Lower,
                topic_request_header: Some(TopicRequestHeader {
                    rpc_request_header: Some(RpcRequestHeader {
                        broker_name: Some(broker_name),
                        ..Default::default()
                    }),
                    lo: None,
                }),
            };
            (
                RequestCode::SearchOffsetByTimestamp,
                RemotingCommand::create_request_command(RequestCode::SearchOffsetByTimestamp, header),
            )
        }
    };
    let response = client.process_remoting(command).await?;
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(broker_operation_error("queryOffset", &response).into());
    }

    let offset = match request_code {
        RequestCode::GetMinOffset => response
            .decode_command_custom_header::<GetMinOffsetResponseHeader>()
            .map(|header| header.offset)
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to decode local min offset response header: {error}"),
            })?,
        RequestCode::GetMaxOffset => response
            .decode_command_custom_header::<GetMaxOffsetResponseHeader>()
            .map(|header| header.offset)
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to decode local max offset response header: {error}"),
            })?,
        RequestCode::SearchOffsetByTimestamp => response
            .decode_command_custom_header::<SearchOffsetResponseHeader>()
            .map(|header| header.offset)
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to decode local search offset response header: {error}"),
            })?,
        _ => unreachable!("query offset uses only min/max/search request codes"),
    };

    Ok(QueryOffsetPlan {
        status: ProxyStatusMapper::ok_payload(),
        offset,
    })
}

fn build_send_message_request(
    broker_name: &CheetahString,
    producer_group: &str,
    entry: &SendMessageEntry,
) -> ProxyResult<RemotingCommand> {
    let header = SendMessageRequestHeader {
        producer_group: CheetahString::from(producer_group),
        topic: CheetahString::from(entry.topic.to_string()),
        default_topic: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
        default_topic_queue_nums: 8,
        queue_id: entry.queue_id.unwrap_or(-1),
        sys_flag: send_message_sys_flag(&entry.message),
        born_timestamp: current_millis() as i64,
        flag: entry.message.flag(),
        properties: Some(MessageDecoder::message_properties_to_string(
            entry.message.properties().as_map(),
        )),
        reconsume_times: None,
        unit_mode: Some(false),
        batch: Some(false),
        max_reconsume_times: None,
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let body = entry.message.body().ok_or_else(|| {
        RocketMQError::request_body_invalid(
            "sendMessage",
            format!("message body is missing for topic '{}'", entry.topic),
        )
    })?;
    let mut command = RemotingCommand::create_request_command(RequestCode::SendMessage, header).set_body(body);
    command.make_custom_header_to_net();
    Ok(command)
}

fn build_pop_request_header(broker_name: &str, request: &ReceiveMessageRequest) -> PopMessageRequestHeader {
    PopMessageRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: CheetahString::from(request.target.topic.to_string()),
        queue_id: request.target.queue_id,
        max_msg_nums: request.batch_size,
        invisible_time: request.invisible_duration.as_millis().clamp(1, u128::from(u64::MAX)) as u64,
        poll_time: request.long_polling_timeout.as_millis().clamp(0, u128::from(u64::MAX)) as u64,
        born_time: current_millis(),
        init_mode: 0,
        exp_type: Some(CheetahString::from(request.filter_expression.expression_type.as_str())),
        exp: Some(CheetahString::from(request.filter_expression.expression.as_str())),
        order: Some(request.target.fifo),
        attempt_id: request.attempt_id.as_deref().map(CheetahString::from),
        topic_request_header: Some(OperationTopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from(broker_name)),
                ..Default::default()
            }),
        }),
    }
}

fn build_pull_request_header(broker_name: &str, request: &PullMessageRequest) -> PullMessageRequestHeader {
    PullMessageRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: CheetahString::from(request.target.topic.to_string()),
        queue_id: request.target.queue_id,
        queue_offset: request.offset,
        max_msg_nums: request.batch_size.min(i32::MAX as u32) as i32,
        sys_flag: PullSysFlag::build_sys_flag(
            false,
            !request.long_polling_timeout.is_zero(),
            true,
            request.filter_expression.expression_type != ExpressionType::TAG,
        ) as i32,
        commit_offset: 0,
        suspend_timeout_millis: request.long_polling_timeout.as_millis().clamp(0, u128::from(u64::MAX)) as u64,
        sub_version: 0,
        subscription: Some(CheetahString::from(request.filter_expression.expression.as_str())),
        expression_type: Some(CheetahString::from(request.filter_expression.expression_type.as_str())),
        max_msg_bytes: None,
        request_source: None,
        proxy_forward_client_id: None,
        topic_request: Some(OperationTopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from(broker_name)),
                ..Default::default()
            }),
        }),
    }
}

fn build_update_offset_request_header(
    broker_name: &str,
    request: &UpdateOffsetRequest,
) -> UpdateConsumerOffsetRequestHeader {
    UpdateConsumerOffsetRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: CheetahString::from(request.target.topic.to_string()),
        queue_id: request.target.queue_id,
        commit_offset: request.offset,
        topic_request_header: Some(OperationTopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from(broker_name)),
                ..Default::default()
            }),
        }),
    }
}

fn build_query_consumer_offset_request_header(
    broker_name: &str,
    request: &GetOffsetRequest,
) -> QueryConsumerOffsetRequestHeader {
    QueryConsumerOffsetRequestHeader {
        consumer_group: CheetahString::from(request.group.to_string()),
        topic: CheetahString::from(request.target.topic.to_string()),
        queue_id: request.target.queue_id,
        set_zero_if_not_found: Some(false),
        topic_request_header: Some(OperationTopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from(broker_name)),
                ..Default::default()
            }),
        }),
    }
}

fn build_send_result(
    topic: ResourceIdentity,
    broker_name: &CheetahString,
    response: RemotingCommand,
) -> ProxyResult<SendResult> {
    let response_code = ResponseCode::from(response.code());
    let header = response
        .decode_command_custom_header::<SendMessageResponseHeader>()
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to decode local send response header: {error}"),
        })?;
    let send_status = match response_code {
        ResponseCode::Success => SendStatus::SendOk,
        ResponseCode::FlushDiskTimeout => SendStatus::FlushDiskTimeout,
        ResponseCode::FlushSlaveTimeout => SendStatus::FlushSlaveTimeout,
        ResponseCode::SlaveNotAvailable => SendStatus::SlaveNotAvailable,
        _ => return Err(broker_operation_error("sendMessage", &response).into()),
    };

    let mut result = SendResult::new(
        send_status,
        Some(header.msg_id().clone()),
        None,
        Some(MessageQueue::from_parts(
            topic.to_string(),
            broker_name.clone(),
            header.queue_id(),
        )),
        header.queue_offset().max(0) as u64,
    );
    if let Some(transaction_id) = header.transaction_id() {
        result.set_transaction_id(transaction_id.to_owned());
    }
    if let Some(recall_handle) = header.recall_handle() {
        result.set_recall_handle(recall_handle.to_owned());
    }
    Ok(result)
}

fn process_pop_response(
    mut response: RemotingCommand,
    broker_name: &str,
    topic: &str,
    is_order: bool,
) -> ProxyResult<ReceiveMessagePlan> {
    match ResponseCode::from(response.code()) {
        ResponseCode::Success => {
            let response_header = response
                .decode_command_custom_header::<PopMessageResponseHeader>()
                .map_err(|error| ProxyError::Transport {
                    message: format!("failed to decode local pop response header: {error}"),
                })?;
            let delivery_timestamp_ms = (response_header.pop_time > 0).then_some(response_header.pop_time as i64);
            let mut messages = response
                .get_body_mut()
                .map(|body| MessageDecoder::decodes_batch(body, true, true))
                .unwrap_or_default();
            attach_pop_receipt_handles(
                &mut messages,
                topic,
                &CheetahString::from(broker_name),
                &response_header,
                is_order,
            )?;
            let invisible_duration = Duration::from_millis(response_header.invisible_time.max(1));
            let status = if messages.is_empty() {
                ProxyStatusMapper::from_payload_code(crate::proto::v2::Code::MessageNotFound, "no message available")
            } else {
                ProxyStatusMapper::ok_payload()
            };
            Ok(ReceiveMessagePlan {
                status,
                delivery_timestamp_ms,
                messages: messages
                    .into_iter()
                    .map(|message| ReceivedMessage {
                        message,
                        invisible_duration,
                    })
                    .collect(),
            })
        }
        ResponseCode::PollingFull => Ok(ReceiveMessagePlan {
            status: ProxyStatusMapper::from_payload_code(
                crate::proto::v2::Code::TooManyRequests,
                "broker polling queue is full",
            ),
            delivery_timestamp_ms: None,
            messages: Vec::new(),
        }),
        ResponseCode::PollingTimeout | ResponseCode::PullNotFound => Ok(ReceiveMessagePlan {
            status: ProxyStatusMapper::from_payload_code(
                crate::proto::v2::Code::MessageNotFound,
                "no message available",
            ),
            delivery_timestamp_ms: None,
            messages: Vec::new(),
        }),
        _ => Err(broker_operation_error("receiveMessage", &response).into()),
    }
}

fn process_pull_response(mut response: RemotingCommand) -> ProxyResult<PullMessagePlan> {
    let response_header = response
        .decode_command_custom_header::<PullMessageResponseHeader>()
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to decode local pull response header: {error}"),
        })?;
    let next_offset = response_header.next_begin_offset;
    let min_offset = response_header.min_offset;
    let max_offset = response_header.max_offset;
    match ResponseCode::from(response.code()) {
        ResponseCode::Success => Ok(PullMessagePlan {
            status: ProxyStatusMapper::ok_payload(),
            next_offset,
            min_offset,
            max_offset,
            messages: response
                .get_body_mut()
                .map(|body| MessageDecoder::decodes_batch(body, true, true))
                .unwrap_or_default(),
        }),
        ResponseCode::PullNotFound | ResponseCode::PullRetryImmediately => Ok(PullMessagePlan {
            status: ProxyStatusMapper::from_payload_code(
                crate::proto::v2::Code::MessageNotFound,
                "no message available",
            ),
            next_offset,
            min_offset,
            max_offset,
            messages: Vec::new(),
        }),
        ResponseCode::PullOffsetMoved => Ok(PullMessagePlan {
            status: ProxyStatusMapper::from_payload_code(
                crate::proto::v2::Code::IllegalOffset,
                "pull offset is illegal",
            ),
            next_offset,
            min_offset,
            max_offset,
            messages: Vec::new(),
        }),
        _ => Err(broker_operation_error("pullMessage", &response).into()),
    }
}

fn attach_pop_receipt_handles(
    messages: &mut Vec<MessageExt>,
    topic: &str,
    broker_name: &CheetahString,
    response_header: &PopMessageResponseHeader,
    is_order: bool,
) -> ProxyResult<()> {
    let start_offset_info = ExtraInfoUtil::parse_start_offset_info(
        response_header
            .start_offset_info
            .as_ref()
            .unwrap_or(&CheetahString::from_slice("")),
    )?;
    let order_count_info = ExtraInfoUtil::parse_order_count_info(
        response_header
            .order_count_info
            .as_ref()
            .unwrap_or(&CheetahString::from_slice("")),
    )?;
    let sort_map = build_queue_offset_sorted_map(topic, messages.as_slice())?;
    let mut cached_extra = HashMap::with_capacity(5);
    for message in messages {
        if start_offset_info.is_empty() {
            let key = CheetahString::from_string(format!("{}{}", message.topic(), message.queue_id() as i64));
            if !cached_extra.contains_key(&key) {
                let extra_info = ExtraInfoUtil::build_extra_info(
                    message.queue_offset(),
                    response_header.pop_time as i64,
                    response_header.invisible_time as i64,
                    response_header.revive_qid as i32,
                    message.topic(),
                    broker_name,
                    message.queue_id(),
                );
                cached_extra.insert(key.clone(), CheetahString::from_string(extra_info));
            }
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                CheetahString::from_string(format!(
                    "{}{}{}",
                    cached_extra.get(&key).cloned().unwrap_or_default(),
                    MessageConst::KEY_SEPARATOR,
                    message.queue_offset()
                )),
            );
        } else if message
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
            .is_none()
        {
            let dispatch = message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                ))
                .unwrap_or_default();
            let (queue_offset_key, queue_id_key) = if mix_all::is_lmq(Some(topic)) && !dispatch.is_empty() {
                let queues: Vec<&str> = dispatch.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                let data = message
                    .property(&CheetahString::from_static_str(
                        MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                    ))
                    .unwrap_or_default();
                let queue_offsets: Vec<&str> = data.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                let offset = queue_offsets[queues.iter().position(|&queue| queue == topic).unwrap()]
                    .parse::<i64>()
                    .unwrap_or_default();
                let queue_id_key = ExtraInfoUtil::get_start_offset_info_map_key(topic, mix_all::LMQ_QUEUE_ID as i64);
                let queue_offset_key =
                    ExtraInfoUtil::get_queue_offset_map_key(topic, mix_all::LMQ_QUEUE_ID as i64, offset);
                let index = sort_map
                    .get(&queue_id_key)
                    .and_then(|offsets| offsets.iter().position(|queue_offset| *queue_offset == offset as u64))
                    .unwrap_or_default();
                let msg_queue_offset = sort_map
                    .get(&queue_offset_key)
                    .and_then(|offsets| offsets.get(index))
                    .copied()
                    .unwrap_or_default();
                let extra_info = ExtraInfoUtil::build_extra_info(
                    message.queue_offset(),
                    response_header.pop_time as i64,
                    response_header.invisible_time as i64,
                    response_header.revive_qid as i32,
                    message.topic(),
                    broker_name,
                    msg_queue_offset as i32,
                );
                message.put_property(
                    CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                    CheetahString::from_string(extra_info),
                );
                (queue_offset_key, queue_id_key)
            } else {
                let queue_id_key =
                    ExtraInfoUtil::get_start_offset_info_map_key(message.topic(), message.queue_id() as i64);
                let queue_offset_key = ExtraInfoUtil::get_queue_offset_map_key(
                    message.topic(),
                    message.queue_id() as i64,
                    message.queue_offset(),
                );
                let index = sort_map
                    .get(&queue_id_key)
                    .and_then(|offsets| {
                        offsets
                            .iter()
                            .position(|queue_offset| *queue_offset == message.queue_offset() as u64)
                    })
                    .unwrap_or_default();
                let msg_queue_offset = sort_map
                    .get(&queue_offset_key)
                    .and_then(|offsets| offsets.get(index))
                    .copied()
                    .unwrap_or_default();
                let extra_info = ExtraInfoUtil::build_extra_info(
                    message.queue_offset(),
                    response_header.pop_time as i64,
                    response_header.invisible_time as i64,
                    response_header.revive_qid as i32,
                    message.topic(),
                    broker_name,
                    msg_queue_offset as i32,
                );
                message.put_property(
                    CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                    CheetahString::from_string(extra_info),
                );
                (queue_offset_key, queue_id_key)
            };
            if is_order && !order_count_info.is_empty() {
                let count = order_count_info
                    .get(&queue_offset_key)
                    .or_else(|| order_count_info.get(&queue_id_key));
                if let Some(count) = count {
                    message.set_reconsume_times(*count);
                }
            }
        }
        message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME),
            CheetahString::from(response_header.pop_time.to_string()),
        );
        message.broker_name = broker_name.clone();
        message.set_topic(CheetahString::from(topic));
    }
    Ok(())
}

fn build_queue_offset_sorted_map(topic: &str, messages: &[MessageExt]) -> ProxyResult<HashMap<String, Vec<u64>>> {
    let mut sort_map = HashMap::with_capacity(16);
    for message in messages {
        let dispatch = message
            .property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
            ))
            .unwrap_or_default();
        if mix_all::is_lmq(Some(topic)) && message.reconsume_times() == 0 && !dispatch.is_empty() {
            let queues: Vec<&str> = dispatch.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
            let data = message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                ))
                .unwrap_or_default();
            let queue_offsets: Vec<&str> = data.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
            let key = ExtraInfoUtil::get_start_offset_info_map_key(topic, mix_all::LMQ_QUEUE_ID as i64);
            sort_map.entry(key).or_insert_with(|| Vec::with_capacity(4)).push(
                queue_offsets[queues.iter().position(|&queue| queue == topic).unwrap()]
                    .parse()
                    .unwrap_or_default(),
            );
            continue;
        }
        let key = ExtraInfoUtil::get_start_offset_info_map_key_with_pop_ck(
            message.topic(),
            message
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
                .as_ref()
                .map(|value| value.as_str()),
            message.queue_id() as i64,
        )
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to build local pop queue offset key: {error}"),
        })?;
        sort_map
            .entry(key)
            .or_insert_with(|| Vec::with_capacity(4))
            .push(message.queue_offset() as u64);
    }
    Ok(sort_map)
}

#[derive(Debug, Clone)]
struct ParsedReceiptHandle {
    raw: CheetahString,
    broker_name: CheetahString,
    topic: CheetahString,
    queue_id: i32,
    queue_offset: i64,
}

fn parse_receipt_handle(receipt_handle: &str, topic: &str, consumer_group: &str) -> ProxyResult<ParsedReceiptHandle> {
    let trimmed = receipt_handle.trim();
    if trimmed.is_empty() {
        return Err(ProxyError::invalid_receipt_handle("receipt handle must not be empty"));
    }

    let parts = ExtraInfoUtil::split(trimmed);
    let broker_name = ExtraInfoUtil::get_broker_name(parts.as_slice())
        .map(CheetahString::from_string)
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;
    let queue_id = ExtraInfoUtil::get_queue_id(parts.as_slice())
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;
    let queue_offset = ExtraInfoUtil::get_queue_offset(parts.as_slice())
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;
    let real_topic = ExtraInfoUtil::get_real_topic(parts.as_slice(), topic, consumer_group)
        .map(CheetahString::from_string)
        .map_err(|error| ProxyError::invalid_receipt_handle(error.to_string()))?;

    Ok(ParsedReceiptHandle {
        raw: CheetahString::from(trimmed),
        broker_name,
        topic: real_topic,
        queue_id,
        queue_offset,
    })
}

fn decode_broker_message_id(message_id: &str) -> ProxyResult<rocketmq_common::common::message::message_id::MessageId> {
    MessageDecoder::decode_message_id(&CheetahString::from(message_id))
        .map_err(|error| ProxyError::illegal_message_id(format!("failed to decode broker message id: {error}")))
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

fn send_message_sys_flag(message: &Message) -> i32 {
    if is_transaction_prepared(message) {
        MessageSysFlag::TRANSACTION_PREPARED_TYPE
    } else {
        MessageSysFlag::TRANSACTION_NOT_TYPE
    }
}

fn build_local_proxy_producer_group(client_id: Option<&str>, request_id: &str) -> String {
    let identity = sanitize_thread_component(client_id.unwrap_or(request_id));
    format!("PROXY_SEND-{identity}")
}

fn transaction_resolution_flag(resolution: TransactionResolution) -> i32 {
    match resolution {
        TransactionResolution::Commit => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
        TransactionResolution::Rollback => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
    }
}

fn broker_operation_error(operation: &'static str, response: &RemotingCommand) -> RocketMQError {
    RocketMQError::BrokerOperationFailed {
        operation,
        code: response.code(),
        message: response.remark().map(ToString::to_string).unwrap_or_default(),
        broker_addr: None,
    }
}

fn build_broker_config(config: &LocalConfig) -> BrokerConfig {
    let mut broker_config = BrokerConfig::default();
    broker_config.broker_identity.broker_cluster_name = CheetahString::from(config.broker_cluster_name.as_str());
    broker_config.broker_identity.broker_name = CheetahString::from(config.broker_name.as_str());
    broker_config.broker_ip1 = CheetahString::from(config.broker_ip.as_str());
    broker_config.broker_ip2 = Some(CheetahString::from(config.broker_ip.as_str()));
    broker_config.listen_port = u32::from(config.broker_listen_port);
    broker_config.broker_server_config.listen_port = u32::from(config.broker_listen_port);
    broker_config.store_path_root_dir = CheetahString::from(config.store_root_dir.as_str());
    broker_config.namesrv_addr = None;
    broker_config.force_register = false;
    broker_config.skip_pre_online = true;
    broker_config.transfer_msg_by_heap = true;
    broker_config
}

fn build_message_store_config(config: &LocalConfig) -> MessageStoreConfig {
    MessageStoreConfig {
        store_path_root_dir: CheetahString::from(config.store_root_dir.as_str()),
        ..Default::default()
    }
}

fn convert_topic_message_type(message_type: TopicMessageType) -> ProxyTopicMessageType {
    match message_type {
        TopicMessageType::Unspecified => ProxyTopicMessageType::Unspecified,
        TopicMessageType::Normal => ProxyTopicMessageType::Normal,
        TopicMessageType::Fifo => ProxyTopicMessageType::Fifo,
        TopicMessageType::Delay => ProxyTopicMessageType::Delay,
        TopicMessageType::Transaction => ProxyTopicMessageType::Transaction,
        TopicMessageType::Mixed => ProxyTopicMessageType::Mixed,
        TopicMessageType::Lite => ProxyTopicMessageType::Lite,
    }
}

fn convert_subscription_group(config: Arc<SubscriptionGroupConfig>) -> SubscriptionGroupMetadata {
    SubscriptionGroupMetadata {
        consume_message_orderly: config.consume_message_orderly(),
        lite_bind_topic: None,
    }
}

fn sanitize_thread_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .collect();
    if sanitized.is_empty() {
        "local".to_owned()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;

    use super::convert_topic_message_type;
    use crate::service::ProxyTopicMessageType;

    #[test]
    fn topic_message_type_conversion_maps_lite_topics() {
        assert_eq!(
            convert_topic_message_type(TopicMessageType::Lite),
            ProxyTopicMessageType::Lite
        );
    }
}
