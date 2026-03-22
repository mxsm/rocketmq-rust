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

use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use cheetah_string::CheetahString;
use rocketmq_broker::ProxyBrokerFacade;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_client_rust::producer::send_status::SendStatus;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::prelude::RemotingDeserializable;
use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_remoting::protocol::header::recall_message_response_header::RecallMessageResponseHeader;
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
use crate::processor::EndTransactionPlan;
use crate::processor::EndTransactionRequest;
use crate::processor::RecallMessagePlan;
use crate::processor::RecallMessageRequest;
use crate::processor::SendMessageEntry;
use crate::processor::SendMessageRequest;
use crate::processor::SendMessageResultEntry;
use crate::processor::TransactionResolution;
use crate::processor::TransactionSource;
use crate::service::AssignmentService;
use crate::service::DefaultConsumerService;
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
        let thread_name = format!(
            "rocketmq-proxy-local-{}",
            sanitize_thread_component(&config.broker_name)
        );
        thread::Builder::new()
            .name(thread_name)
            .spawn(move || run_local_broker_worker(config, receiver))
            .expect("failed to spawn proxy local broker worker");
        Self { sender }
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

pub fn local_service_manager_from_config(config: LocalConfig, strategy_name: impl Into<String>) -> LocalServiceManager {
    let client = LocalBrokerFacadeClient::new(config);
    LocalServiceManager::with_services(
        std::sync::Arc::new(LocalRouteService::new(client.clone())),
        std::sync::Arc::new(LocalMetadataService::new(client.clone())),
        std::sync::Arc::new(LocalAssignmentService::new(client.clone(), strategy_name)),
        std::sync::Arc::new(LocalMessageService::new(client.clone())),
        std::sync::Arc::new(DefaultConsumerService),
        std::sync::Arc::new(LocalTransactionService::new(client)),
    )
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
