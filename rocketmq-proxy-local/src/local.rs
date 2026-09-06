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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_broker::proxy_facade::BrokerConfig;
use rocketmq_broker::ProxyBrokerFacade;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_model::common::filter::expression_type::ExpressionType;
use rocketmq_model::common::message::message_batch::MessageBatch;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_id::MessageId;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;
use rocketmq_observability::TelemetryHandle;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::body::batch_ack_builder::build_batch_ack_requests;
use rocketmq_protocol::protocol::body::batch_ack_builder::BatchAckInput;
use rocketmq_protocol::protocol::body::lite_subscription_ctl_request_body::LiteSubscriptionCtlRequestBody;
use rocketmq_protocol::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_protocol::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_protocol::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_protocol::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_protocol::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
use rocketmq_protocol::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
use rocketmq_protocol::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_protocol::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_protocol::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::TopicRequestHeader as OperationTopicRequestHeader;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_protocol::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_protocol::protocol::header::recall_message_response_header::RecallMessageResponseHeader;
use rocketmq_protocol::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_proxy_core::status::ProxyStatusMapper;
use rocketmq_proxy_core::AckMessageRequest;
use rocketmq_proxy_core::AckMessageResultEntry;
use rocketmq_proxy_core::AssignmentService;
use rocketmq_proxy_core::ChangeInvisibleDurationPlan;
use rocketmq_proxy_core::ChangeInvisibleDurationRequest;
use rocketmq_proxy_core::ConsumerService;
use rocketmq_proxy_core::EndTransactionPlan;
use rocketmq_proxy_core::EndTransactionRequest;
use rocketmq_proxy_core::ForwardMessageToDeadLetterQueuePlan;
use rocketmq_proxy_core::ForwardMessageToDeadLetterQueueRequest;
use rocketmq_proxy_core::GetOffsetPlan;
use rocketmq_proxy_core::GetOffsetRequest;
use rocketmq_proxy_core::LiteSubscriptionSyncRequest;
use rocketmq_proxy_core::MessageService;
use rocketmq_proxy_core::MetadataService;
use rocketmq_proxy_core::ProxyContext;
use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyMessage;
use rocketmq_proxy_core::ProxyRemotingBackend;
use rocketmq_proxy_core::ProxyResult;
use rocketmq_proxy_core::ProxyServiceFuture;
use rocketmq_proxy_core::ProxyTopicMessageType;
use rocketmq_proxy_core::PullMessagePlan;
use rocketmq_proxy_core::PullMessageRequest;
use rocketmq_proxy_core::QueryOffsetPlan;
use rocketmq_proxy_core::QueryOffsetPolicy;
use rocketmq_proxy_core::QueryOffsetRequest;
use rocketmq_proxy_core::RecallMessagePlan;
use rocketmq_proxy_core::RecallMessageRequest;
use rocketmq_proxy_core::ReceiveMessagePlan;
use rocketmq_proxy_core::ReceiveMessageRequest;
use rocketmq_proxy_core::ReceivedMessage;
use rocketmq_proxy_core::ResolvedEndpoint;
use rocketmq_proxy_core::ResourceIdentity;
use rocketmq_proxy_core::RouteService;
use rocketmq_proxy_core::SendMessageEntry;
use rocketmq_proxy_core::SendMessageRequest;
use rocketmq_proxy_core::SendMessageResultEntry;
use rocketmq_proxy_core::SubscriptionGroupMetadata;
use rocketmq_proxy_core::TransactionResolution;
use rocketmq_proxy_core::TransactionService;
use rocketmq_proxy_core::TransactionSource;
use rocketmq_proxy_core::UpdateOffsetPlan;
use rocketmq_proxy_core::UpdateOffsetRequest;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::api::EmbeddedDispatchOutcome;
use rocketmq_transport::api::EmbeddedResponse;
use rocketmq_transport::api::EmbeddedResponseBody;
use rocketmq_transport::api::RemotingDeserializable;
use rocketmq_transport::api::RpcRequestHeader;
use rocketmq_transport::api::TopicRequestHeader;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::config::LocalConfig;
use crate::execution::run_local_execution;
use crate::execution::LocalCommandHandler;
use crate::execution::LocalExecutionPolicy;
use crate::message::message_ext_to_core;
use crate::message::message_from_core;
use crate::message::message_properties_from_core;
use crate::service::LocalServiceManager;

const LOCAL_LONG_POLL_TIMEOUT_MARGIN: Duration = Duration::from_millis(500);
const LOCAL_REMOTING_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct LocalBrokerFacadeClient {
    sender: mpsc::Sender<QueuedLocalBrokerCommand>,
    count_budget: Arc<Semaphore>,
    byte_budget: Arc<Semaphore>,
    rejected: Arc<AtomicU64>,
    broker_name: String,
}

pub(crate) struct QueuedLocalBrokerCommand {
    pub(crate) command: LocalBrokerCommand,
    pub(crate) enqueued_at: Instant,
    pub(crate) deadline_at: Option<Instant>,
    pub(crate) timeout_budget: Option<Duration>,
    pub(crate) _count_permit: OwnedSemaphorePermit,
    pub(crate) _byte_permit: OwnedSemaphorePermit,
}

pub(crate) enum LocalBrokerCommand {
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
        timeout: Duration,
        reply: oneshot::Sender<ProxyResult<EmbeddedDispatchOutcome>>,
    },
}

impl QueuedLocalBrokerCommand {
    pub(crate) fn is_expired(&self, now: Instant, max_queue_age: Duration) -> bool {
        now.saturating_duration_since(self.enqueued_at) >= max_queue_age
    }

    pub(crate) fn deadline_expired(&self, now: Instant) -> bool {
        self.deadline_at.is_some_and(|deadline| now >= deadline)
    }

    pub(crate) fn apply_remaining_deadline(&mut self, now: Instant) {
        let Some(deadline_at) = self.deadline_at else {
            return;
        };
        if let LocalBrokerCommand::ProcessRemoting { timeout, .. } = &mut self.command {
            *timeout = deadline_at.saturating_duration_since(now);
        }
    }
}

impl LocalBrokerCommand {
    fn timeout(&self) -> Option<Duration> {
        match self {
            Self::ProcessRemoting { timeout, .. } => Some(*timeout),
            _ => None,
        }
    }

    fn estimated_bytes(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        match self {
            Self::QueryRoute { topic, .. } | Self::QueryTopicMessageType { topic, .. } => base
                .saturating_add(topic.namespace().len())
                .saturating_add(topic.name().len()),
            Self::QuerySubscriptionGroup { group, .. } => base
                .saturating_add(group.namespace().len())
                .saturating_add(group.name().len()),
            Self::QueryAssignment {
                topic,
                group,
                client_id,
                strategy_name,
                ..
            } => base
                .saturating_add(topic.namespace().len())
                .saturating_add(topic.name().len())
                .saturating_add(group.namespace().len())
                .saturating_add(group.name().len())
                .saturating_add(client_id.len())
                .saturating_add(strategy_name.len()),
            Self::SendMessage {
                request,
                client_id,
                request_id,
                ..
            } => request.messages.iter().fold(
                base.saturating_add(request_id.len())
                    .saturating_add(client_id.as_ref().map_or(0, String::len)),
                |bytes, entry| {
                    let message = &entry.message;
                    let property_bytes = message
                        .properties()
                        .iter()
                        .map(|(key, value)| key.len().saturating_add(value.len()))
                        .sum::<usize>();
                    bytes
                        .saturating_add(entry.topic.namespace().len())
                        .saturating_add(entry.topic.name().len())
                        .saturating_add(entry.client_message_id.len())
                        .saturating_add(message.topic().len())
                        .saturating_add(message.body().map_or(0, <[u8]>::len))
                        .saturating_add(message.transaction_id().map_or(0, str::len))
                        .saturating_add(property_bytes)
                },
            ),
            Self::RecallMessage {
                request,
                client_id,
                request_id,
                ..
            } => base
                .saturating_add(request.topic.namespace().len())
                .saturating_add(request.topic.name().len())
                .saturating_add(request.recall_handle.len())
                .saturating_add(client_id.as_ref().map_or(0, String::len))
                .saturating_add(request_id.len()),
            Self::EndTransaction {
                request,
                client_id,
                request_id,
                ..
            } => base
                .saturating_add(request.topic.namespace().len())
                .saturating_add(request.topic.name().len())
                .saturating_add(request.message_id.len())
                .saturating_add(request.transaction_id.len())
                .saturating_add(request.trace_context.as_ref().map_or(0, String::len))
                .saturating_add(request.producer_group.as_ref().map_or(0, String::len))
                .saturating_add(request.commit_log_message_id.as_ref().map_or(0, String::len))
                .saturating_add(client_id.as_ref().map_or(0, String::len))
                .saturating_add(request_id.len()),
            Self::ProcessRemoting { request, .. } => base.saturating_add(request.body().map_or(0, bytes::Bytes::len)),
        }
        .max(1)
    }

    pub(crate) fn reject_overload(self) {
        self.reject_with(local_queue_overloaded());
    }

    pub(crate) fn reject_unavailable(self) {
        self.reject_with_transport("local broker command execution is unavailable".to_owned());
    }

    pub(crate) fn reject_with_transport(self, message: String) {
        self.reject_with(ProxyError::Transport { message });
    }

    pub(crate) fn reject_timeout(self, timeout: Duration) {
        self.reject_with(
            RocketMQError::Timeout {
                operation: "local broker command queue",
                timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            }
            .into(),
        );
    }

    fn reject_with(self, error: ProxyError) {
        match self {
            Self::QueryRoute { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::QueryTopicMessageType { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::QuerySubscriptionGroup { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::QueryAssignment { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::SendMessage { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::RecallMessage { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::EndTransaction { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Self::ProcessRemoting { reply, .. } => {
                let _ = reply.send(Err(error));
            }
        }
    }
}

fn local_queue_overloaded() -> ProxyError {
    ProxyError::too_many_requests("local broker command queue")
}

fn validate_local_queue_config(config: &LocalConfig) -> ProxyResult<()> {
    if config.command_queue_capacity == 0 {
        return Err(ProxyError::Transport {
            message: "local command queue capacity must be greater than zero".to_owned(),
        });
    }
    if config.command_queue_max_bytes == 0 || config.command_queue_max_bytes > u32::MAX as usize {
        return Err(ProxyError::Transport {
            message: "local command queue byte budget must be in 1..=u32::MAX".to_owned(),
        });
    }
    if config.command_queue_max_age().is_zero() {
        return Err(ProxyError::Transport {
            message: "local command queue maximum age must be greater than zero".to_owned(),
        });
    }
    if config.control_reserve == 0 || config.io_max_inflight <= config.control_reserve {
        return Err(ProxyError::Transport {
            message: "local io_max_inflight must be greater than a nonzero control_reserve".to_owned(),
        });
    }
    if config.long_poll_max_inflight == 0 {
        return Err(ProxyError::Transport {
            message: "local long_poll_max_inflight must be greater than zero".to_owned(),
        });
    }
    if config.execution_lane_idle_timeout().is_zero() {
        return Err(ProxyError::Transport {
            message: "local execution lane idle timeout must be greater than zero".to_owned(),
        });
    }
    Ok(())
}

impl LocalBrokerFacadeClient {
    pub fn new(
        config: LocalConfig,
        service_context: &ChildServiceContext,
        telemetry_handle: TelemetryHandle,
    ) -> ProxyResult<Self> {
        validate_local_queue_config(&config)?;
        let broker_config = build_broker_config(&config);
        let (sender, receiver) = mpsc::channel(config.command_queue_capacity);
        let count_budget = Arc::new(Semaphore::new(config.command_queue_capacity));
        let byte_budget = Arc::new(Semaphore::new(config.command_queue_max_bytes));
        let rejected = Arc::new(AtomicU64::new(0));
        let capacity_items = config.command_queue_capacity;
        let capacity_bytes = config.command_queue_max_bytes;
        let metric_count_budget = Arc::clone(&count_budget);
        let metric_byte_budget = Arc::clone(&byte_budget);
        let metric_rejected = Arc::clone(&rejected);
        rocketmq_observability::metrics::resource::ResourceStabilityMetrics::from_handle(
            &telemetry_handle,
            rocketmq_observability::PROXY_METER_SCOPE,
        )
        .register_queue("proxy-local", "commands", "aggregate", move || {
            rocketmq_observability::metrics::resource::ResourceQueueSnapshot {
                items: capacity_items.saturating_sub(metric_count_budget.available_permits()) as u64,
                bytes: capacity_bytes.saturating_sub(metric_byte_budget.available_permits()) as u64,
                capacity_items: capacity_items as u64,
                capacity_bytes: capacity_bytes as u64,
                active: 0,
                rejected_total: metric_rejected.load(Ordering::Relaxed),
                ..rocketmq_observability::metrics::resource::ResourceQueueSnapshot::default()
            }
        });
        let max_queue_age = config.command_queue_max_age();
        let broker_name = config.broker_name.clone();
        let worker_context = service_context.component("command-worker");
        let broker_context = worker_context.component("embedded-broker-store");
        let facade = ProxyBrokerFacade::try_new_from_broker_config(broker_config, broker_context, telemetry_handle)
            .map_err(|error| {
                ProxyError::RocketMQ(RocketMQError::ConfigInvalidValue {
                    key: "proxy.local.embeddedBroker",
                    value: config.broker_name.clone(),
                    reason: error.to_string(),
                })
            })?;
        let shutdown_context = service_context.clone();
        let cancellation = worker_context.task_group().cancellation_token();
        let lane_context = worker_context.component("command-lanes");
        worker_context
            .spawn_service("proxy.local.worker", async move {
                run_local_broker_worker(
                    config,
                    facade,
                    receiver,
                    cancellation,
                    shutdown_context,
                    lane_context,
                    max_queue_age,
                )
                .await;
            })
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to spawn proxy local worker: {error}"),
            })?;
        Ok(Self {
            sender,
            count_budget,
            byte_budget,
            rejected,
            broker_name,
        })
    }

    pub fn broker_name(&self) -> &str {
        self.broker_name.as_str()
    }

    pub fn transaction_producer_group(&self, context: &ProxyContext) -> String {
        build_local_proxy_producer_group(context.client_id(), context.request_id())
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

    /// Processes one local remoting command through the Broker dispatcher.
    ///
    /// # Errors
    ///
    /// Returns an error when the local Broker worker is unavailable, rejects
    /// the command, or does not complete within the default timeout.
    pub async fn process_remoting(&self, request: RemotingCommand) -> ProxyResult<EmbeddedDispatchOutcome> {
        self.process_remoting_with_timeout(request, LOCAL_REMOTING_RESPONSE_TIMEOUT)
            .await
    }

    /// Processes one local remoting command through the Broker dispatcher
    /// with an explicit terminal-response timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when the local Broker worker is unavailable, rejects
    /// the command, or does not complete within `timeout`.
    pub async fn process_remoting_with_timeout(
        &self,
        mut request: RemotingCommand,
        timeout: Duration,
    ) -> ProxyResult<EmbeddedDispatchOutcome> {
        request.make_custom_header_to_net();
        self.execute(|reply| LocalBrokerCommand::ProcessRemoting {
            request,
            timeout,
            reply,
        })
        .await
    }

    async fn process_embedded_response(&self, request: RemotingCommand) -> ProxyResult<EmbeddedResponse> {
        self.process_embedded_response_with_timeout(request, LOCAL_REMOTING_RESPONSE_TIMEOUT)
            .await
    }

    async fn process_embedded_response_with_timeout(
        &self,
        request: RemotingCommand,
        timeout: Duration,
    ) -> ProxyResult<EmbeddedResponse> {
        embedded_response(self.process_remoting_with_timeout(request, timeout).await?)
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
        let reply_rx = self.enqueue(build)?;
        reply_rx.await.map_err(|error| ProxyError::Transport {
            message: format!("local broker worker dropped response channel: {error}"),
        })?
    }

    fn enqueue<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<ProxyResult<T>>) -> LocalBrokerCommand,
    ) -> ProxyResult<oneshot::Receiver<ProxyResult<T>>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = build(reply_tx);
        let estimated_bytes = command.estimated_bytes();
        let count_permit = Arc::clone(&self.count_budget)
            .try_acquire_owned()
            .map_err(|_| self.queue_overloaded())?;
        let byte_permits = u32::try_from(estimated_bytes).map_err(|_| self.queue_overloaded())?;
        let byte_permit = Arc::clone(&self.byte_budget)
            .try_acquire_many_owned(byte_permits)
            .map_err(|_| self.queue_overloaded())?;
        let timeout_budget = command.timeout();
        let enqueued_at = Instant::now();
        let deadline_at = timeout_budget.map(|timeout| enqueued_at.checked_add(timeout).unwrap_or(enqueued_at));
        let queued = QueuedLocalBrokerCommand {
            command,
            enqueued_at,
            deadline_at,
            timeout_budget,
            _count_permit: count_permit,
            _byte_permit: byte_permit,
        };
        match self.sender.try_send(queued) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => return Err(self.queue_overloaded()),
            Err(TrySendError::Closed(_)) => {
                return Err(ProxyError::Transport {
                    message: "local broker worker is not available".to_owned(),
                });
            }
        }
        Ok(reply_rx)
    }

    fn queue_overloaded(&self) -> ProxyError {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        local_queue_overloaded()
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

impl ProxyRemotingBackend for LocalRemotingBackend {
    fn process(&self, request: RemotingCommand) -> ProxyServiceFuture<'_, EmbeddedDispatchOutcome> {
        Box::pin(async move { self.client.process_remoting(request).await })
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

impl RouteService for LocalRouteService {
    fn query_route<'a>(
        &'a self,
        _context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        _endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, TopicRouteData> {
        Box::pin(async move { self.client.query_route(topic.clone()).await })
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

impl MetadataService for LocalMetadataService {
    fn topic_message_type<'a>(
        &'a self,
        _context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, ProxyTopicMessageType> {
        Box::pin(async move { self.client.query_topic_message_type(topic.clone()).await })
    }

    fn subscription_group<'a>(
        &'a self,
        _context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        group: &'a ResourceIdentity,
    ) -> ProxyServiceFuture<'a, Option<SubscriptionGroupMetadata>> {
        Box::pin(async move { self.client.query_subscription_group(topic.clone(), group.clone()).await })
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

impl AssignmentService for LocalAssignmentService {
    fn query_assignment<'a>(
        &'a self,
        context: &'a ProxyContext,
        topic: &'a ResourceIdentity,
        group: &'a ResourceIdentity,
        _endpoints: &'a [ResolvedEndpoint],
    ) -> ProxyServiceFuture<'a, Option<Vec<MessageQueueAssignment>>> {
        Box::pin(async move {
            self.client
                .query_assignment(
                    topic.clone(),
                    group.clone(),
                    context.require_client_id()?.to_owned(),
                    self.strategy_name.clone(),
                )
                .await
        })
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

impl MessageService for LocalMessageService {
    fn send_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a SendMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<SendMessageResultEntry>> {
        Box::pin(async move {
            self.client
                .send_message(
                    request.clone(),
                    context.client_id().map(ToOwned::to_owned),
                    context.request_id().to_owned(),
                )
                .await
        })
    }

    fn recall_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a RecallMessageRequest,
    ) -> ProxyServiceFuture<'a, RecallMessagePlan> {
        Box::pin(async move {
            self.client
                .recall_message(
                    request.clone(),
                    context.client_id().map(ToOwned::to_owned),
                    context.request_id().to_owned(),
                )
                .await
        })
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

impl TransactionService for LocalTransactionService {
    fn transaction_producer_group(&self, context: &ProxyContext) -> Option<String> {
        Some(self.client.transaction_producer_group(context))
    }

    fn end_transaction<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a EndTransactionRequest,
    ) -> ProxyServiceFuture<'a, EndTransactionPlan> {
        Box::pin(async move {
            self.client
                .end_transaction(
                    request.clone(),
                    context.client_id().map(ToOwned::to_owned),
                    context.request_id().to_owned(),
                )
                .await
        })
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

impl ConsumerService for LocalConsumerService {
    fn sync_lite_subscription<'a>(
        &'a self,
        _context: &'a ProxyContext,
        client_id: &'a str,
        request: &'a LiteSubscriptionSyncRequest,
    ) -> ProxyServiceFuture<'a, ()> {
        Box::pin(async move { sync_lite_subscription_via_broker(&self.client, client_id, request).await })
    }

    fn receive_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a ReceiveMessageRequest,
    ) -> ProxyServiceFuture<'a, ReceiveMessagePlan> {
        Box::pin(async move { receive_message_via_broker(&self.client, request, context.deadline()).await })
    }

    fn pull_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: &'a PullMessageRequest,
    ) -> ProxyServiceFuture<'a, PullMessagePlan> {
        Box::pin(async move { pull_message_via_broker(&self.client, request, context.deadline()).await })
    }

    fn ack_message<'a>(
        &'a self,
        _context: &'a ProxyContext,
        request: &'a AckMessageRequest,
    ) -> ProxyServiceFuture<'a, Vec<AckMessageResultEntry>> {
        Box::pin(async move { ack_message_via_broker(&self.client, request).await })
    }

    fn forward_message_to_dead_letter_queue<'a>(
        &'a self,
        _context: &'a ProxyContext,
        request: &'a ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyServiceFuture<'a, ForwardMessageToDeadLetterQueuePlan> {
        Box::pin(async move { forward_message_to_dead_letter_queue_via_broker(&self.client, request).await })
    }

    fn change_invisible_duration<'a>(
        &'a self,
        _context: &'a ProxyContext,
        request: &'a ChangeInvisibleDurationRequest,
    ) -> ProxyServiceFuture<'a, ChangeInvisibleDurationPlan> {
        Box::pin(async move { change_invisible_duration_via_broker(&self.client, request).await })
    }

    fn update_offset<'a>(
        &'a self,
        _context: &'a ProxyContext,
        request: &'a UpdateOffsetRequest,
    ) -> ProxyServiceFuture<'a, UpdateOffsetPlan> {
        Box::pin(async move { update_offset_via_broker(&self.client, request).await })
    }

    fn get_offset<'a>(
        &'a self,
        _context: &'a ProxyContext,
        request: &'a GetOffsetRequest,
    ) -> ProxyServiceFuture<'a, GetOffsetPlan> {
        Box::pin(async move { get_offset_via_broker(&self.client, request).await })
    }

    fn query_offset<'a>(
        &'a self,
        _context: &'a ProxyContext,
        request: &'a QueryOffsetRequest,
    ) -> ProxyServiceFuture<'a, QueryOffsetPlan> {
        Box::pin(async move { query_offset_via_broker(&self.client, request).await })
    }
}

pub fn local_components_from_config_with_service_context(
    config: LocalConfig,
    strategy_name: impl Into<String>,
    service_context: &ChildServiceContext,
    telemetry_handle: TelemetryHandle,
) -> ProxyResult<(LocalServiceManager, LocalBrokerFacadeClient)> {
    Ok(local_components(
        LocalBrokerFacadeClient::new(config, service_context, telemetry_handle)?,
        strategy_name,
    ))
}

fn local_components(
    client: LocalBrokerFacadeClient,
    strategy_name: impl Into<String>,
) -> (LocalServiceManager, LocalBrokerFacadeClient) {
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

pub fn local_service_manager_from_config(
    config: LocalConfig,
    strategy_name: impl Into<String>,
    service_context: &ChildServiceContext,
    telemetry_handle: TelemetryHandle,
) -> ProxyResult<LocalServiceManager> {
    Ok(local_components_from_config_with_service_context(config, strategy_name, service_context, telemetry_handle)?.0)
}

async fn run_local_broker_worker(
    config: LocalConfig,
    mut facade: ProxyBrokerFacade,
    mut receiver: mpsc::Receiver<QueuedLocalBrokerCommand>,
    cancellation: CancellationToken,
    shutdown_context: ChildServiceContext,
    lane_context: ChildServiceContext,
    max_queue_age: Duration,
) {
    let initialization = tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            let deadline = shutdown_deadline(&shutdown_context, &config);
            drain_local_commands(
                &facade,
                Some("embedded broker stopped before initialization completed"),
                &mut receiver,
                max_queue_age,
                deadline,
            )
            .await;
            shutdown_local_broker(&mut facade, deadline).await;
            return;
        }
        initialized = facade.initialize() => initialized,
    };
    let startup_error = if let Err(error) = initialization {
        Some(format!("embedded broker initialization failed: {error}"))
    } else {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                let deadline = shutdown_deadline(&shutdown_context, &config);
                drain_local_commands(
                    &facade,
                    Some("embedded broker stopped before startup completed"),
                    &mut receiver,
                    max_queue_age,
                    deadline,
                )
                .await;
                shutdown_local_broker(&mut facade, deadline).await;
                return;
            }
            result = facade.start() => {
                if let Err(error) = result {
                    Some(format!("embedded broker startup failed: {error}"))
                } else {
                    None
                }
            }
        }
    };

    let policy = LocalExecutionPolicy::from_config(&config);
    let shutdown_timeout = config.shutdown_timeout();
    let facade = Arc::new(facade);
    let handler = Arc::new(BrokerLocalCommandHandler {
        facade: facade.clone(),
        startup_error: startup_error.map(Arc::<str>::from),
    });
    run_local_execution(
        policy,
        receiver,
        cancellation,
        shutdown_context.clone(),
        lane_context,
        handler.clone(),
        shutdown_timeout,
    )
    .await;
    drop(handler);

    match Arc::try_unwrap(facade) {
        Ok(mut facade) => {
            shutdown_local_broker(&mut facade, shutdown_deadline(&shutdown_context, &config)).await;
        }
        Err(_) => {
            tracing::error!("proxy local command lanes retained the embedded Broker facade during shutdown");
        }
    }
}

struct BrokerLocalCommandHandler {
    facade: Arc<ProxyBrokerFacade>,
    startup_error: Option<Arc<str>>,
}

impl LocalCommandHandler for BrokerLocalCommandHandler {
    async fn handle(&self, command: LocalBrokerCommand) {
        handle_local_broker_command(&self.facade, self.startup_error.as_deref(), command).await;
    }
}

fn shutdown_deadline(shutdown_context: &ChildServiceContext, config: &LocalConfig) -> ShutdownDeadline {
    shutdown_context
        .task_group()
        .shutdown_deadline()
        .unwrap_or_else(|| ShutdownDeadline::after(config.shutdown_timeout()))
}

async fn drain_local_commands(
    facade: &ProxyBrokerFacade,
    startup_error: Option<&str>,
    receiver: &mut mpsc::Receiver<QueuedLocalBrokerCommand>,
    max_queue_age: Duration,
    deadline: ShutdownDeadline,
) {
    receiver.close();
    while !deadline.is_expired() {
        let Some(queued) = receiver.recv().await else {
            break;
        };
        if queued.is_expired(Instant::now(), max_queue_age) {
            queued.command.reject_overload();
            continue;
        }
        let QueuedLocalBrokerCommand {
            command,
            enqueued_at: _,
            deadline_at: _,
            timeout_budget: _,
            _count_permit,
            _byte_permit,
        } = queued;
        if tokio::time::timeout(
            deadline.remaining(),
            handle_local_broker_command(facade, startup_error, command),
        )
        .await
        .is_err()
        {
            break;
        }
    }
}

async fn handle_local_broker_command(
    facade: &ProxyBrokerFacade,
    startup_error: Option<&str>,
    command: LocalBrokerCommand,
) {
    match command {
        LocalBrokerCommand::QueryRoute { topic, reply } => {
            let _ = reply.send(startup_error.map_or_else(
                || facade.query_route(topic.name()).map_err(Into::into),
                |message| {
                    Err(ProxyError::Transport {
                        message: message.to_owned(),
                    })
                },
            ));
        }
        LocalBrokerCommand::QueryTopicMessageType { topic, reply } => {
            let _ = reply.send(startup_error.map_or_else(
                || {
                    facade
                        .query_topic_message_type(topic.name())
                        .map(convert_topic_message_type)
                        .map_err(Into::into)
                },
                |message| {
                    Err(ProxyError::Transport {
                        message: message.to_owned(),
                    })
                },
            ));
        }
        LocalBrokerCommand::QuerySubscriptionGroup { group, reply } => {
            let _ = reply.send(startup_error.map_or_else(
                || {
                    facade
                        .query_subscription_group(group.name())
                        .map(|config| config.map(convert_subscription_group))
                        .map_err(Into::into)
                },
                |message| {
                    Err(ProxyError::Transport {
                        message: message.to_owned(),
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
            let result = if let Some(message) = startup_error {
                Err(ProxyError::Transport {
                    message: message.to_owned(),
                })
            } else {
                query_assignment(facade, topic, group, client_id, strategy_name).await
            };
            let _ = reply.send(result);
        }
        LocalBrokerCommand::SendMessage {
            request,
            client_id,
            request_id,
            reply,
        } => {
            let result = if let Some(message) = startup_error {
                Err(ProxyError::Transport {
                    message: message.to_owned(),
                })
            } else {
                send_message(facade, request, client_id, request_id).await
            };
            let _ = reply.send(result);
        }
        LocalBrokerCommand::RecallMessage {
            request,
            client_id,
            request_id,
            reply,
        } => {
            let result = if let Some(message) = startup_error {
                Err(ProxyError::Transport {
                    message: message.to_owned(),
                })
            } else {
                recall_message(facade, request, client_id, request_id).await
            };
            let _ = reply.send(result);
        }
        LocalBrokerCommand::EndTransaction {
            request,
            client_id,
            request_id,
            reply,
        } => {
            let result = if let Some(message) = startup_error {
                Err(ProxyError::Transport {
                    message: message.to_owned(),
                })
            } else {
                end_transaction(facade, request, client_id, request_id).await
            };
            let _ = reply.send(result);
        }
        LocalBrokerCommand::ProcessRemoting {
            request,
            timeout,
            reply,
        } => {
            let result = if let Some(message) = startup_error {
                Err(ProxyError::Transport {
                    message: message.to_owned(),
                })
            } else {
                facade.process_request(request, timeout).await.map_err(Into::into)
            };
            let _ = reply.send(result);
        }
    }
}

async fn shutdown_local_broker(facade: &mut ProxyBrokerFacade, deadline: ShutdownDeadline) {
    if tokio::time::timeout(deadline.remaining(), facade.shutdown())
        .await
        .is_err()
    {
        tracing::warn!("proxy local embedded Broker shutdown exceeded the shared deadline");
    }
}

fn embedded_response(outcome: EmbeddedDispatchOutcome) -> ProxyResult<EmbeddedResponse> {
    match outcome {
        EmbeddedDispatchOutcome::Reply(response) => Ok(response.into_embedded_response()),
        EmbeddedDispatchOutcome::OneWay { .. } => Err(ProxyError::Transport {
            message: "local Broker returned one-way completion where a response was required".to_owned(),
        }),
        EmbeddedDispatchOutcome::Deferred { .. } => Err(ProxyError::Transport {
            message: "local Broker returned an unresolved deferred completion".to_owned(),
        }),
        EmbeddedDispatchOutcome::NoReply { .. } => Err(ProxyError::Transport {
            message: "local Broker suppressed a required response".to_owned(),
        }),
        _ => Err(ProxyError::Transport {
            message: "local Broker returned an unsupported embedded completion".to_owned(),
        }),
    }
}

async fn facade_embedded_response(
    facade: &ProxyBrokerFacade,
    request: RemotingCommand,
    timeout: Duration,
) -> ProxyResult<EmbeddedResponse> {
    embedded_response(facade.process_request(request, timeout).await?)
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
    let response = facade_embedded_response(facade, request, LOCAL_REMOTING_RESPONSE_TIMEOUT).await?;
    if ResponseCode::from(response.response_code()) != ResponseCode::Success {
        return Err(broker_operation_error("queryAssignment", &response));
    }

    let Some(body) = embedded_contiguous_body(response.body())? else {
        return Ok(None);
    };
    let decoded = QueryAssignmentResponseBody::decode(body).map_err(|error| ProxyError::Transport {
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
    let entries = request.messages;
    if compatible_batch_entries(&entries) {
        return Ok(send_compatible_batch(facade, &broker_name, producer_group.as_str(), entries).await);
    }
    let mut results = Vec::with_capacity(entries.len());
    for entry in entries {
        results.push(send_message_entry(facade, &broker_name, producer_group.as_str(), entry).await);
    }
    Ok(results)
}

async fn sync_lite_subscription_via_broker(
    client: &LocalBrokerFacadeClient,
    client_id: &str,
    request: &LiteSubscriptionSyncRequest,
) -> ProxyResult<()> {
    let mut body = LiteSubscriptionCtlRequestBody::new();
    body.set_subscription_set(vec![request.broker_dto(client_id)?]);
    let command = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
        .set_body(body.encode()?);
    let response = client.process_embedded_response(command).await?;
    if ResponseCode::from(response.response_code()) != ResponseCode::Success {
        return Err(broker_operation_error("syncLiteSubscription", &response));
    }
    Ok(())
}

fn compatible_batch_entries(entries: &[SendMessageEntry]) -> bool {
    let Some(first) = entries.first() else {
        return false;
    };
    entries.len() > 1
        && entries.iter().all(|entry| {
            entry.topic == first.topic
                && entry.queue_id == first.queue_id
                && !entry.message.properties().keys().any(|key| {
                    matches!(
                        key.as_str(),
                        MessageConst::PROPERTY_SHARDING_KEY
                            | MessageConst::PROPERTY_TIMER_DELIVER_MS
                            | MessageConst::PROPERTY_TRANSACTION_PREPARED
                            | MessageConst::PROPERTY_LITE_TOPIC
                            | MessageConst::PROPERTY_PRIORITY
                    )
                })
        })
}

async fn send_compatible_batch(
    facade: &ProxyBrokerFacade,
    broker_name: &CheetahString,
    producer_group: &str,
    entries: Vec<SendMessageEntry>,
) -> Vec<SendMessageResultEntry> {
    let result = async {
        let request = build_send_batch_message_request(broker_name, producer_group, &entries)?;
        let response = facade_embedded_response(facade, request, LOCAL_REMOTING_RESPONSE_TIMEOUT).await?;
        build_send_result(entries[0].topic.clone(), broker_name, response)
    }
    .await;
    match result {
        Ok(result) => split_batch_send_result(result, &entries),
        Err(error) => entries
            .iter()
            .map(|_| SendMessageResultEntry {
                status: ProxyStatusMapper::from_error_payload(&error),
                send_result: None,
            })
            .collect(),
    }
}

fn split_batch_send_result(result: SendResult, entries: &[SendMessageEntry]) -> Vec<SendMessageResultEntry> {
    let broker_ids = result
        .msg_id
        .as_ref()
        .map(|value| value.split_char(',').map(str::to_owned).collect::<Vec<_>>())
        .filter(|ids| ids.len() == entries.len());
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let mut item = result.clone();
            item.queue_offset = result.queue_offset.saturating_add(index as u64);
            item.msg_id = Some(CheetahString::from(
                broker_ids
                    .as_ref()
                    .and_then(|ids| ids.get(index))
                    .map(String::as_str)
                    .unwrap_or(entry.client_message_id.as_str()),
            ));
            SendMessageResultEntry {
                status: ProxyStatusMapper::from_send_result_payload(&item),
                send_result: Some(item),
            }
        })
        .collect()
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
    let response = facade_embedded_response(facade, request, LOCAL_REMOTING_RESPONSE_TIMEOUT).await?;
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
    let response = facade_embedded_response(facade, command, LOCAL_REMOTING_RESPONSE_TIMEOUT).await?;
    if ResponseCode::from(response.response_code()) != ResponseCode::Success {
        return Err(broker_operation_error("recallMessage", &response));
    }

    let header = response
        .head()
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
    let transaction_state_table_offset = i64::try_from(transaction_state_table_offset)
        .map_err(|_| ProxyError::invalid_transaction_id("transaction state table offset exceeds Java long range"))?;
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
        commit_log_offset: broker_message_id.offset,
        commit_or_rollback: transaction_resolution_flag(request.resolution),
        from_transaction_check: matches!(request.source, TransactionSource::ServerCheck),
        msg_id: CheetahString::from(request.message_id.as_str()),
        transaction_id: Some(CheetahString::from(request.transaction_id.as_str())),
        rpc_request_header: RpcRequestHeader::default(),
    };
    let mut command = RemotingCommand::create_request_command(RequestCode::EndTransaction, header)
        .set_remark(CheetahString::from(request.trace_context.as_deref().unwrap_or("")));
    command.make_custom_header_to_net();
    let response = facade_embedded_response(facade, command, LOCAL_REMOTING_RESPONSE_TIMEOUT).await?;
    if ResponseCode::from(response.response_code()) != ResponseCode::Success {
        return Err(broker_operation_error("endTransaction", &response));
    }

    Ok(EndTransactionPlan {
        status: ProxyStatusMapper::ok_payload(),
    })
}

async fn receive_message_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &ReceiveMessageRequest,
    caller_deadline: Option<Duration>,
) -> ProxyResult<ReceiveMessagePlan> {
    let header = build_pop_request_header(client.broker_name(), request);
    let response = client
        .process_embedded_response_with_timeout(
            RemotingCommand::create_request_command(RequestCode::PopMessage, header),
            local_long_poll_timeout(request.long_polling_timeout, caller_deadline),
        )
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
    caller_deadline: Option<Duration>,
) -> ProxyResult<PullMessagePlan> {
    let header = build_pull_request_header(client.broker_name(), request);
    let response = client
        .process_embedded_response_with_timeout(
            RemotingCommand::create_request_command(RequestCode::PullMessage, header),
            local_long_poll_timeout(request.long_polling_timeout, caller_deadline),
        )
        .await?;
    process_pull_response(response)
}

fn local_long_poll_timeout(long_polling_timeout: Duration, caller_deadline: Option<Duration>) -> Duration {
    let broker_timeout = long_polling_timeout.saturating_add(LOCAL_LONG_POLL_TIMEOUT_MARGIN);
    caller_deadline.map_or(broker_timeout, |deadline| deadline.min(broker_timeout))
}

async fn ack_message_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &AckMessageRequest,
) -> ProxyResult<Vec<AckMessageResultEntry>> {
    let group_name = request.group.to_string();
    let topics = request
        .entries
        .iter()
        .map(|entry| entry.lite_topic.clone().unwrap_or_else(|| request.topic.to_string()))
        .collect::<Vec<_>>();
    let batch_inputs = request
        .entries
        .iter()
        .zip(&topics)
        .enumerate()
        .filter(|(_, (entry, _))| entry.lite_topic.is_none())
        .map(|(entry_index, (entry, topic))| BatchAckInput {
            entry_index,
            consumer_group: group_name.as_str(),
            topic,
            receipt_handle: entry.receipt_handle.as_str(),
        })
        .collect::<Vec<_>>();
    let built = build_batch_ack_requests(&batch_inputs);
    let mut statuses = std::iter::repeat_with(|| None)
        .take(request.entries.len())
        .collect::<Vec<Option<rocketmq_proxy_core::ProxyPayloadStatus>>>();

    let mut fallback_indexes = request
        .entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| entry.lite_topic.is_some().then_some(index))
        .chain(built.failures.into_iter().map(|failure| failure.entry_index))
        .collect::<Vec<_>>();

    for batch_request in built.requests {
        let command = RemotingCommand::new_request(RequestCode::BatchAckMessage, batch_request.body.encode()?);
        match client.process_embedded_response(command).await {
            Ok(response) => {
                let status = if ResponseCode::from(response.response_code()) == ResponseCode::Success {
                    ProxyStatusMapper::ok_payload()
                } else {
                    invalid_receipt_handle_status()
                };
                for index in batch_request.entry_indexes {
                    statuses[index] = Some(status.clone());
                }
            }
            Err(error) => {
                tracing::warn!(
                    broker = %batch_request.broker_name,
                    entries = batch_request.entry_indexes.len(),
                    error = %error,
                    "local BatchAck failed; falling back to single ACK"
                );
                fallback_indexes.extend(batch_request.entry_indexes);
            }
        }
    }

    fallback_indexes.sort_unstable();
    fallback_indexes.dedup();
    for index in fallback_indexes {
        let status = match ack_message_entry_via_broker(client, request, &request.entries[index]).await {
            Ok(status) => status,
            Err(error) => ProxyStatusMapper::from_error_payload(&error),
        };
        statuses[index] = Some(status);
    }

    Ok(request
        .entries
        .iter()
        .enumerate()
        .map(|(index, entry)| AckMessageResultEntry {
            message_id: entry.message_id.clone(),
            receipt_handle: entry.receipt_handle.clone(),
            status: statuses[index].take().unwrap_or_else(|| {
                ProxyStatusMapper::from_payload_code(
                    rocketmq_proxy_core::proto::v2::Code::InternalError,
                    "ACK result was not produced",
                )
            }),
        })
        .collect())
}

fn invalid_receipt_handle_status() -> rocketmq_proxy_core::ProxyPayloadStatus {
    ProxyStatusMapper::from_payload_code(
        rocketmq_proxy_core::proto::v2::Code::InvalidReceiptHandle,
        "receipt handle has expired or message no longer exists",
    )
}

async fn ack_message_entry_via_broker(
    client: &LocalBrokerFacadeClient,
    request: &AckMessageRequest,
    entry: &rocketmq_proxy_core::AckMessageEntry,
) -> ProxyResult<rocketmq_proxy_core::ProxyPayloadStatus> {
    let topic_name = entry.lite_topic.clone().unwrap_or_else(|| request.topic.to_string());
    let group_name = request.group.to_string();
    let parsed = parse_receipt_handle(entry.receipt_handle.as_str(), topic_name.as_str(), group_name.as_str())?;
    let header = AckMessageRequestHeader {
        consumer_group: CheetahString::from(group_name),
        topic: parsed.topic.clone(),
        queue_id: parsed.queue_id,
        extra_info: parsed.raw.clone(),
        offset: parsed.queue_offset,
        lite_topic: entry
            .lite_topic
            .as_ref()
            .map(|topic| CheetahString::from(topic.as_str())),
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(parsed.broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let response = client
        .process_embedded_response(RemotingCommand::create_request_command(RequestCode::AckMessage, header))
        .await?;

    Ok(
        if ResponseCode::from(response.response_code()) == ResponseCode::Success {
            ProxyStatusMapper::ok_payload()
        } else {
            invalid_receipt_handle_status()
        },
    )
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
        .process_embedded_response(RemotingCommand::create_request_command(
            RequestCode::ConsumerSendMsgBack,
            header,
        ))
        .await?;
    if ResponseCode::from(response.response_code()) != ResponseCode::Success {
        return Err(broker_operation_error("forwardMessageToDeadLetterQueue", &response));
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
        lite_topic: request
            .lite_topic
            .as_ref()
            .map(|topic| CheetahString::from(topic.as_str())),
        suspend: request.suspend.unwrap_or(false),
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(parsed.broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let response = client
        .process_embedded_response(RemotingCommand::create_request_command(
            RequestCode::ChangeMessageInvisibleTime,
            header,
        ))
        .await?;
    if ResponseCode::from(response.response_code()) != ResponseCode::Success {
        return Ok(ChangeInvisibleDurationPlan {
            status: ProxyStatusMapper::from_payload_code(
                rocketmq_proxy_core::proto::v2::Code::InvalidReceiptHandle,
                "receipt handle has expired or message no longer exists",
            ),
            receipt_handle: String::new(),
        });
    }

    let response_header = response
        .head()
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
        .process_embedded_response(RemotingCommand::create_request_command(
            RequestCode::UpdateConsumerOffset,
            header,
        ))
        .await?;
    if ResponseCode::from(response.response_code()) != ResponseCode::Success {
        return Err(broker_operation_error("updateOffset", &response));
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
        .process_embedded_response(RemotingCommand::create_request_command(
            RequestCode::QueryConsumerOffset,
            header,
        ))
        .await?;
    if ResponseCode::from(response.response_code()) != ResponseCode::Success {
        return Err(broker_operation_error("getOffset", &response));
    }

    let response_header = response
        .head()
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
                lite_topic: None,
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
    let response = client.process_embedded_response(command).await?;
    if ResponseCode::from(response.response_code()) != ResponseCode::Success {
        return Err(broker_operation_error("queryOffset", &response));
    }

    let offset = match request_code {
        RequestCode::GetMinOffset => response
            .head()
            .decode_command_custom_header::<GetMinOffsetResponseHeader>()
            .map(|header| header.offset)
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to decode local min offset response header: {error}"),
            })?,
        RequestCode::GetMaxOffset => response
            .head()
            .decode_command_custom_header::<GetMaxOffsetResponseHeader>()
            .map(|header| header.offset)
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to decode local max offset response header: {error}"),
            })?,
        RequestCode::SearchOffsetByTimestamp => response
            .head()
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
            &message_properties_from_core(&entry.message),
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
    let mut command = RemotingCommand::create_request_command(RequestCode::SendMessage, header)
        .set_body(bytes::Bytes::copy_from_slice(body));
    command.make_custom_header_to_net();
    Ok(command)
}

fn build_send_batch_message_request(
    broker_name: &CheetahString,
    producer_group: &str,
    entries: &[SendMessageEntry],
) -> ProxyResult<RemotingCommand> {
    let first = entries
        .first()
        .ok_or_else(|| RocketMQError::request_body_invalid("sendMessage", "batch must contain at least one message"))?;
    let messages = entries
        .iter()
        .map(|entry| message_from_core(&entry.message))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = MessageBatch::generate_from_messages(messages)?;
    let body = MessageDecoder::encode_messages(&batch.messages);
    let header = SendMessageRequestHeader {
        producer_group: CheetahString::from(producer_group),
        topic: CheetahString::from(first.topic.to_string()),
        default_topic: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
        default_topic_queue_nums: 8,
        queue_id: first.queue_id.unwrap_or(-1),
        sys_flag: MessageSysFlag::TRANSACTION_NOT_TYPE,
        born_timestamp: current_millis() as i64,
        flag: 0,
        properties: None,
        reconsume_times: None,
        unit_mode: Some(false),
        batch: Some(true),
        max_reconsume_times: None,
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
            lo: None,
        }),
    };
    let mut command = RemotingCommand::create_request_command(RequestCode::SendBatchMessage, header).set_body(body);
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
        lite_topic: None,
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
    response: EmbeddedResponse,
) -> ProxyResult<SendResult> {
    let response_code = ResponseCode::from(response.response_code());
    let send_status = match response_code {
        ResponseCode::Success => SendStatus::SendOk,
        ResponseCode::FlushDiskTimeout => SendStatus::FlushDiskTimeout,
        ResponseCode::FlushSlaveTimeout => SendStatus::FlushSlaveTimeout,
        ResponseCode::SlaveNotAvailable => SendStatus::SlaveNotAvailable,
        _ => return Err(broker_operation_error("sendMessage", &response)),
    };
    let header = response
        .head()
        .decode_command_custom_header::<SendMessageResponseHeader>()
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to decode local send response header: {error}"),
        })?;

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
    response: EmbeddedResponse,
    broker_name: &str,
    topic: &str,
    is_order: bool,
) -> ProxyResult<ReceiveMessagePlan> {
    match ResponseCode::from(response.response_code()) {
        ResponseCode::Success => {
            let response_header = response
                .head()
                .decode_command_custom_header::<PopMessageResponseHeader>()
                .map_err(|error| ProxyError::Transport {
                    message: format!("failed to decode local pop response header: {error}"),
                })?;
            let delivery_timestamp_ms = (response_header.pop_time > 0).then_some(response_header.pop_time as i64);
            let (_, body) = response.into_parts();
            let mut messages = decode_embedded_messages(body)?;
            attach_pop_receipt_handles(
                &mut messages,
                topic,
                &CheetahString::from(broker_name),
                &response_header,
                is_order,
            )?;
            let invisible_duration = Duration::from_millis(response_header.invisible_time.max(1));
            let status = if messages.is_empty() {
                ProxyStatusMapper::from_payload_code(
                    rocketmq_proxy_core::proto::v2::Code::MessageNotFound,
                    "no message available",
                )
            } else {
                ProxyStatusMapper::ok_payload()
            };
            Ok(ReceiveMessagePlan {
                status,
                delivery_timestamp_ms,
                messages: messages
                    .into_iter()
                    .map(|message| ReceivedMessage {
                        message: message_ext_to_core(&message),
                        invisible_duration,
                    })
                    .collect(),
            })
        }
        ResponseCode::PollingFull => Ok(ReceiveMessagePlan {
            status: ProxyStatusMapper::from_payload_code(
                rocketmq_proxy_core::proto::v2::Code::TooManyRequests,
                "broker polling queue is full",
            ),
            delivery_timestamp_ms: None,
            messages: Vec::new(),
        }),
        ResponseCode::PollingTimeout | ResponseCode::PullNotFound => Ok(ReceiveMessagePlan {
            status: ProxyStatusMapper::from_payload_code(
                rocketmq_proxy_core::proto::v2::Code::MessageNotFound,
                "no message available",
            ),
            delivery_timestamp_ms: None,
            messages: Vec::new(),
        }),
        _ => Err(broker_operation_error("receiveMessage", &response)),
    }
}

fn process_pull_response(response: EmbeddedResponse) -> ProxyResult<PullMessagePlan> {
    enum PullResponseOutcome {
        Found,
        NotFound,
        OffsetMoved,
    }

    let outcome = match ResponseCode::from(response.response_code()) {
        ResponseCode::Success => PullResponseOutcome::Found,
        ResponseCode::PullNotFound | ResponseCode::PullRetryImmediately => PullResponseOutcome::NotFound,
        ResponseCode::PullOffsetMoved => PullResponseOutcome::OffsetMoved,
        _ => return Err(broker_operation_error("pullMessage", &response)),
    };
    let response_header = response
        .head()
        .decode_command_custom_header::<PullMessageResponseHeader>()
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to decode local pull response header: {error}"),
        })?;
    let next_offset = response_header.next_begin_offset;
    let min_offset = response_header.min_offset;
    let max_offset = response_header.max_offset;
    match outcome {
        PullResponseOutcome::Found => {
            let (_, body) = response.into_parts();
            Ok(PullMessagePlan {
                status: ProxyStatusMapper::ok_payload(),
                next_offset,
                min_offset,
                max_offset,
                messages: decode_embedded_messages(body)?
                    .into_iter()
                    .map(|message| message_ext_to_core(&message))
                    .collect(),
            })
        }
        PullResponseOutcome::NotFound => Ok(PullMessagePlan {
            status: ProxyStatusMapper::from_payload_code(
                rocketmq_proxy_core::proto::v2::Code::MessageNotFound,
                "no message available",
            ),
            next_offset,
            min_offset,
            max_offset,
            messages: Vec::new(),
        }),
        PullResponseOutcome::OffsetMoved => Ok(PullMessagePlan {
            status: ProxyStatusMapper::from_payload_code(
                rocketmq_proxy_core::proto::v2::Code::IllegalOffset,
                "pull offset is illegal",
            ),
            next_offset,
            min_offset,
            max_offset,
            messages: Vec::new(),
        }),
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
                let queues: Vec<&str> = dispatch.split_str(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                let data = message
                    .property(&CheetahString::from_static_str(
                        MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                    ))
                    .unwrap_or_default();
                let queue_offsets: Vec<&str> = data.split_str(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
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
            let queues: Vec<&str> = dispatch.split_str(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
            let data = message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                ))
                .unwrap_or_default();
            let queue_offsets: Vec<&str> = data.split_str(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
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

fn decode_broker_message_id(message_id: &str) -> ProxyResult<MessageId> {
    MessageDecoder::decode_message_id(&CheetahString::from(message_id))
        .map_err(|error| ProxyError::illegal_message_id(format!("failed to decode broker message id: {error}")))
}

fn attach_transaction_producer_group(message: &mut ProxyMessage, producer_group: &str) {
    if !is_transaction_prepared(message) {
        return;
    }

    message.put_property(MessageConst::PROPERTY_PRODUCER_GROUP, producer_group);
}

fn is_transaction_prepared(message: &ProxyMessage) -> bool {
    message
        .property(MessageConst::PROPERTY_TRANSACTION_PREPARED)
        .and_then(|value| value.parse().ok())
        .unwrap_or(false)
}

fn send_message_sys_flag(message: &ProxyMessage) -> i32 {
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

trait BrokerResponseMetadata {
    fn response_code(&self) -> i32;
    fn response_remark(&self) -> Option<&str>;
}

impl BrokerResponseMetadata for RemotingCommand {
    fn response_code(&self) -> i32 {
        self.code()
    }

    fn response_remark(&self) -> Option<&str> {
        self.remark().map(CheetahString::as_str)
    }
}

impl BrokerResponseMetadata for EmbeddedResponse {
    fn response_code(&self) -> i32 {
        self.response_code()
    }

    fn response_remark(&self) -> Option<&str> {
        self.head().remark().map(CheetahString::as_str)
    }
}

fn broker_operation_error(operation: &'static str, response: &impl BrokerResponseMetadata) -> ProxyError {
    ProxyError::from(RocketMQError::BrokerOperationFailed {
        operation,
        code: response.response_code(),
        message: response.response_remark().map(ToOwned::to_owned).unwrap_or_default(),
        broker_addr: None,
    })
}

fn embedded_contiguous_body(body: &EmbeddedResponseBody) -> ProxyResult<Option<&[u8]>> {
    match body {
        EmbeddedResponseBody::Empty => Ok(None),
        EmbeddedResponseBody::Bytes(body) => Ok(Some(body.as_ref())),
        EmbeddedResponseBody::Segments(segments) if segments.len() == 1 => Ok(Some(segments[0].as_ref())),
        EmbeddedResponseBody::Segments(_) => Err(ProxyError::Transport {
            message: "local Broker returned segmented body for a contiguous metadata response".to_owned(),
        }),
        EmbeddedResponseBody::FileRegions(_) => Err(ProxyError::Transport {
            message: "local Broker returned file regions for a metadata response".to_owned(),
        }),
    }
}

fn decode_embedded_messages(body: EmbeddedResponseBody) -> ProxyResult<Vec<MessageExt>> {
    match body {
        EmbeddedResponseBody::Empty => Ok(Vec::new()),
        EmbeddedResponseBody::Bytes(mut body) => Ok(MessageDecoder::decodes_batch(&mut body, true, true)),
        EmbeddedResponseBody::Segments(segments) => {
            let mut messages = Vec::new();
            for mut segment in segments {
                messages.extend(MessageDecoder::decodes_batch(&mut segment, true, true));
            }
            Ok(messages)
        }
        EmbeddedResponseBody::FileRegions(_) => Err(ProxyError::Transport {
            message: "local Proxy cannot decode message payload from file regions".to_owned(),
        }),
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

fn convert_topic_message_type(message_type: TopicMessageType) -> ProxyTopicMessageType {
    match message_type {
        TopicMessageType::Unspecified => ProxyTopicMessageType::Unspecified,
        TopicMessageType::Normal => ProxyTopicMessageType::Normal,
        TopicMessageType::Fifo => ProxyTopicMessageType::Fifo,
        TopicMessageType::Delay => ProxyTopicMessageType::Delay,
        TopicMessageType::Transaction => ProxyTopicMessageType::Transaction,
        TopicMessageType::Mixed => ProxyTopicMessageType::Mixed,
        TopicMessageType::Lite => ProxyTopicMessageType::Lite,
        TopicMessageType::Priority => ProxyTopicMessageType::Priority,
    }
}

fn convert_subscription_group(config: Arc<SubscriptionGroupConfig>) -> SubscriptionGroupMetadata {
    SubscriptionGroupMetadata::from(config.as_ref())
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
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQError;
    use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
    use rocketmq_model::common::message::MessageConst;
    use rocketmq_model::result::SendResult;
    use rocketmq_model::result::SendStatus;
    use rocketmq_observability::TelemetryHandle;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::header::extra_info_util::ExtraInfoUtil;
    use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
    use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
    use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_proxy_core::ConsumerFilterExpression;
    use rocketmq_proxy_core::MessageQueueTarget;
    use rocketmq_proxy_core::ProxyError;
    use rocketmq_proxy_core::ProxyMessage;
    use rocketmq_proxy_core::ProxyRemotingBackend;
    use rocketmq_proxy_core::ProxyTopicMessageType;
    use rocketmq_proxy_core::PullMessageRequest;
    use rocketmq_proxy_core::ReceiveMessageRequest;
    use rocketmq_proxy_core::ReceiveTarget;
    use rocketmq_proxy_core::ResourceIdentity;
    use rocketmq_proxy_core::SendMessageEntry;
    use rocketmq_proxy_core::TransactionResolution;
    use rocketmq_runtime::ShutdownDeadline;
    use rocketmq_transport::api::EmbeddedDispatchOutcome;
    use rocketmq_transport::api::EmbeddedResponse;
    use rocketmq_transport::api::RemotingResponse;

    use super::broker_operation_error;
    use super::build_local_proxy_producer_group;
    use super::build_pop_request_header;
    use super::build_pull_request_header;
    use super::build_send_batch_message_request;
    use super::build_send_message_request;
    use super::build_send_result;
    use super::compatible_batch_entries;
    use super::convert_topic_message_type;
    use super::local_long_poll_timeout;
    use super::parse_receipt_handle;
    use super::process_pop_response;
    use super::process_pull_response;
    use super::split_batch_send_result;
    use super::transaction_resolution_flag;
    use super::validate_local_queue_config;
    use super::LocalBrokerFacadeClient;
    use super::LocalRemotingBackend;
    use crate::LocalConfig;

    fn batch_entry(id: &str) -> SendMessageEntry {
        SendMessageEntry {
            topic: ResourceIdentity::new("", "TopicA"),
            client_message_id: id.to_owned(),
            message: ProxyMessage::new("TopicA", id.as_bytes().to_vec()),
            queue_id: None,
        }
    }

    #[test]
    fn compatible_entries_build_one_local_batch_request() {
        let entries = vec![batch_entry("a"), batch_entry("b")];
        assert!(compatible_batch_entries(&entries));

        let command = build_send_batch_message_request(&"broker-a".into(), "group-a", &entries).expect("batch command");
        assert_eq!(RequestCode::from(command.code()), RequestCode::SendBatchMessage);
        let header = command
            .decode_command_custom_header::<SendMessageRequestHeader>()
            .expect("batch header");
        assert_eq!(header.batch, Some(true));
        assert!(command.body().is_some_and(|body| !body.is_empty()));

        let mut fifo = entries;
        fifo[0]
            .message
            .put_property(MessageConst::PROPERTY_SHARDING_KEY, "group");
        assert!(!compatible_batch_entries(&fifo));
    }

    #[test]
    fn local_batch_result_preserves_per_entry_ids_and_offsets() {
        let entries = vec![batch_entry("client-a"), batch_entry("client-b")];
        let result = SendResult::new(
            SendStatus::SendOk,
            Some(CheetahString::from("broker-a,broker-b")),
            None,
            None,
            8,
        );

        let split = split_batch_send_result(result, &entries);

        assert_eq!(split.len(), 2);
        assert_eq!(
            split[0].send_result.as_ref().expect("first").msg_id.as_deref(),
            Some("broker-a")
        );
        assert_eq!(split[1].send_result.as_ref().expect("second").queue_offset, 9);
    }

    fn available_local_broker_port() -> u16 {
        loop {
            let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve an ephemeral broker port");
            let port = listener.local_addr().expect("read ephemeral broker port").port();
            if port > 2 && port != 10_912 && port.checked_sub(2) != Some(10_912) {
                return port;
            }
        }
    }

    fn embedded_response(mut command: RemotingCommand) -> EmbeddedResponse {
        command
            .try_make_custom_header_to_net()
            .expect("materialize Broker response header");
        RemotingResponse::from_command(command)
            .expect("build embedded Broker response")
            .into_embedded_response()
    }

    #[test]
    fn topic_message_type_conversion_maps_lite_topics() {
        assert_eq!(
            convert_topic_message_type(TopicMessageType::Lite),
            ProxyTopicMessageType::Lite
        );
        assert_eq!(
            convert_topic_message_type(TopicMessageType::Priority),
            ProxyTopicMessageType::Priority
        );
    }

    #[test]
    fn broker_operation_error_is_normalized_once_and_redacts_remote_remark() {
        const PRIVATE_REMARK: &str = "token=secret\r\nC:\\private\\broker.log";
        let response =
            RemotingCommand::create_response_command_with_code_remark(ResponseCode::SystemError, PRIVATE_REMARK);

        let error = broker_operation_error("queryAssignment", &response);

        let ProxyError::BrokerResponse(error) = error else {
            panic!("expected normalized BrokerResponse error");
        };
        assert_eq!(error.descriptor(), &rocketmq_error::PROXY_BROKER_RESPONSE_FAILED);
        let public = error.public_view().expect("valid public Broker response view");
        assert_eq!(public.message(), "Broker response failed");
        assert_eq!(public.fields().count(), 0);
        assert!(!public.message().contains(PRIVATE_REMARK));
        assert!(!error.to_string().contains(PRIVATE_REMARK));

        let source = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<RocketMQError>())
            .expect("retain typed BrokerOperationFailed source");
        match source {
            RocketMQError::BrokerOperationFailed {
                operation,
                code,
                message,
                broker_addr,
            } => {
                assert_eq!(*operation, "queryAssignment");
                assert_eq!(ResponseCode::from(*code), ResponseCode::SystemError);
                assert_eq!(message, PRIVATE_REMARK);
                assert!(broker_addr.is_none());
            }
            other => panic!("expected BrokerOperationFailed, got {other:?}"),
        }
    }

    #[test]
    fn headerless_broker_failures_normalize_before_business_header_decoding() {
        let send_response = embedded_response(RemotingCommand::create_response_command_with_code_remark(
            ResponseCode::TopicNotExist,
            "private send failure",
        ));
        let send_error = build_send_result(
            ResourceIdentity::new("", "TopicA"),
            &CheetahString::from("broker-a"),
            send_response,
        )
        .expect_err("failure response must not require a send header");
        let ProxyError::BrokerResponse(send_error) = send_error else {
            panic!("send failure must use the normalized Broker response");
        };
        assert_eq!(send_error.descriptor(), &rocketmq_error::PROXY_BROKER_TOPIC_NOT_FOUND);

        let pull_response = embedded_response(RemotingCommand::create_response_command_with_code_remark(
            ResponseCode::NoPermission,
            "private pull failure",
        ));
        let pull_error =
            process_pull_response(pull_response).expect_err("failure response must not require a pull header");
        let ProxyError::BrokerResponse(pull_error) = pull_error else {
            panic!("pull failure must use the normalized Broker response");
        };
        assert_eq!(pull_error.descriptor(), &rocketmq_error::PROXY_BROKER_PERMISSION_DENIED);
    }

    #[test]
    fn local_send_business_response_codes_remain_send_outcomes() {
        for (response_code, expected_status) in [
            (ResponseCode::Success, SendStatus::SendOk),
            (ResponseCode::FlushDiskTimeout, SendStatus::FlushDiskTimeout),
            (ResponseCode::FlushSlaveTimeout, SendStatus::FlushSlaveTimeout),
            (ResponseCode::SlaveNotAvailable, SendStatus::SlaveNotAvailable),
        ] {
            let response = embedded_response(RemotingCommand::create_response_command_with_code_and_header(
                response_code,
                SendMessageResponseHeader::new(CheetahString::from("message-id"), 2, 7, None, None, None),
            ));

            let result = build_send_result(
                ResourceIdentity::new("", "TopicA"),
                &CheetahString::from("broker-a"),
                response,
            )
            .expect("business send response remains a normal outcome");

            assert_eq!(result.send_status, expected_status);
        }
    }

    #[test]
    fn local_receive_business_response_codes_remain_receive_outcomes() {
        for (response_code, expected_code) in [
            (
                ResponseCode::PullNotFound,
                rocketmq_proxy_core::proto::v2::Code::MessageNotFound,
            ),
            (
                ResponseCode::PollingFull,
                rocketmq_proxy_core::proto::v2::Code::TooManyRequests,
            ),
            (
                ResponseCode::PollingTimeout,
                rocketmq_proxy_core::proto::v2::Code::MessageNotFound,
            ),
        ] {
            let response = embedded_response(RemotingCommand::create_response_command_with_code(response_code));

            let result = process_pop_response(response, "broker-a", "TopicA", false)
                .expect("business receive response remains a normal outcome");

            assert_eq!(result.status.code(), expected_code as i32);
        }
    }

    #[test]
    fn local_pull_business_response_codes_remain_pull_outcomes() {
        for (response_code, expected_code) in [
            (
                ResponseCode::PullNotFound,
                rocketmq_proxy_core::proto::v2::Code::MessageNotFound,
            ),
            (
                ResponseCode::PullRetryImmediately,
                rocketmq_proxy_core::proto::v2::Code::MessageNotFound,
            ),
            (
                ResponseCode::PullOffsetMoved,
                rocketmq_proxy_core::proto::v2::Code::IllegalOffset,
            ),
        ] {
            let response = embedded_response(RemotingCommand::create_response_command_with_code_and_header(
                response_code,
                PullMessageResponseHeader {
                    suggest_which_broker_id: 0,
                    next_begin_offset: 7,
                    min_offset: 1,
                    max_offset: 11,
                    ..Default::default()
                },
            ));

            let result = process_pull_response(response).expect("business pull response remains a normal outcome");

            assert_eq!(result.status.code(), expected_code as i32);
            assert_eq!(result.next_offset, 7);
            assert!(result.messages.is_empty());
        }
    }

    #[test]
    fn local_send_request_preserves_core_message_contract() {
        let mut message = ProxyMessage::default();
        message.set_topic("TopicA");
        message.set_body(Some(vec![1, 2, 3]));
        message.set_flag(7);
        message.put_property("key", "value");
        let entry = SendMessageEntry {
            topic: ResourceIdentity::new("", "TopicA"),
            client_message_id: "client-message-id".to_owned(),
            message,
            queue_id: Some(2),
        };

        let command = build_send_message_request(&CheetahString::from("broker-a"), "producer-a", &entry)
            .expect("build local send request");
        let header = command
            .decode_command_custom_header::<SendMessageRequestHeader>()
            .expect("decode local send header");

        assert_eq!(header.topic.as_str(), "TopicA");
        assert_eq!(header.producer_group.as_str(), "producer-a");
        assert_eq!(header.queue_id, 2);
        assert_eq!(command.body().map(|body| body.as_ref()), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn local_pop_and_pull_requests_preserve_consumer_contracts() {
        let receive = ReceiveMessageRequest {
            group: ResourceIdentity::new("", "group-a"),
            target: ReceiveTarget {
                topic: ResourceIdentity::new("", "TopicA"),
                queue_id: 3,
                broker_name: Some("broker-a".to_owned()),
                broker_addr: None,
                fifo: true,
            },
            filter_expression: ConsumerFilterExpression {
                expression_type: "TAG".to_owned(),
                expression: "green".to_owned(),
            },
            batch_size: 16,
            invisible_duration: Duration::from_secs(30),
            auto_renew: false,
            long_polling_timeout: Duration::from_secs(2),
            attempt_id: Some("attempt-a".to_owned()),
        };
        let pop = build_pop_request_header("broker-a", &receive);
        assert_eq!(pop.consumer_group.as_str(), "group-a");
        assert_eq!(pop.topic.as_str(), "TopicA");
        assert_eq!(pop.queue_id, 3);
        assert_eq!(pop.max_msg_nums, 16);
        assert_eq!(pop.order, Some(true));

        let pull = PullMessageRequest {
            group: receive.group.clone(),
            target: MessageQueueTarget {
                topic: receive.target.topic.clone(),
                queue_id: receive.target.queue_id,
                broker_name: receive.target.broker_name.clone(),
                broker_addr: None,
            },
            offset: 41,
            batch_size: 8,
            filter_expression: receive.filter_expression,
            long_polling_timeout: Duration::from_secs(1),
        };
        let pull = build_pull_request_header("broker-a", &pull);
        assert_eq!(pull.consumer_group.as_str(), "group-a");
        assert_eq!(pull.topic.as_str(), "TopicA");
        assert_eq!(pull.queue_id, 3);
        assert_eq!(pull.queue_offset, 41);
        assert_eq!(pull.max_msg_nums, 8);
    }

    #[test]
    fn local_ack_receipt_and_transaction_contracts_are_adapter_owned() {
        let broker_name = CheetahString::from("broker-a");
        let handle = ExtraInfoUtil::build_extra_info_with_offset(9, 10, 30_000, 0, "TopicA", &broker_name, 4, 9);
        let parsed = parse_receipt_handle(&handle, "TopicA", "group-a").expect("parse local receipt handle");
        assert_eq!(parsed.broker_name.as_str(), "broker-a");
        assert_eq!(parsed.topic.as_str(), "TopicA");
        assert_eq!(parsed.queue_offset, 9);

        assert_eq!(
            build_local_proxy_producer_group(Some("client@a"), "request-a"),
            "PROXY_SEND-clienta"
        );
        assert_ne!(
            transaction_resolution_flag(TransactionResolution::Commit),
            transaction_resolution_flag(TransactionResolution::Rollback)
        );
    }

    #[test]
    fn local_long_poll_timeout_adds_margin_without_extending_the_caller_deadline() {
        assert_eq!(
            local_long_poll_timeout(Duration::from_secs(15), None),
            Duration::from_millis(15_500)
        );
        assert_eq!(
            local_long_poll_timeout(Duration::from_secs(15), Some(Duration::from_secs(10))),
            Duration::from_secs(10)
        );
        assert_eq!(
            local_long_poll_timeout(Duration::from_secs(15), Some(Duration::from_secs(20))),
            Duration::from_millis(15_500)
        );
    }

    #[test]
    fn local_execution_config_rejects_invalid_concurrency_limits() {
        let cases = [
            LocalConfig {
                control_reserve: 0,
                ..LocalConfig::default()
            },
            LocalConfig {
                io_max_inflight: 2,
                control_reserve: 2,
                ..LocalConfig::default()
            },
            LocalConfig {
                long_poll_max_inflight: 0,
                ..LocalConfig::default()
            },
            LocalConfig {
                execution_lane_idle_timeout_millis: 0,
                ..LocalConfig::default()
            },
        ];

        for config in cases {
            assert!(matches!(
                validate_local_queue_config(&config),
                Err(ProxyError::Transport { .. })
            ));
        }
    }

    #[tokio::test]
    async fn local_command_queue_is_bounded_by_count_and_bytes() {
        let runtime =
            rocketmq_runtime::RuntimeContext::try_from_current("proxy-local-queue-test").expect("test runtime context");
        let service = runtime.service_context("proxy-local-queue-test.service");
        let store = tempfile::tempdir().expect("create local Broker store directory");
        let config = LocalConfig {
            command_queue_capacity: 7,
            command_queue_max_bytes: 1,
            broker_listen_port: available_local_broker_port(),
            store_root_dir: store.path().to_string_lossy().into_owned(),
            ..LocalConfig::default()
        };
        let client = LocalBrokerFacadeClient::new(config, &service, TelemetryHandle::noop())
            .expect("managed local client builds");
        assert_eq!(client.sender.max_capacity(), 7);

        let error = client
            .query_route(ResourceIdentity::new("", "TopicA"))
            .await
            .expect_err("request exceeding the queue byte budget must be rejected");
        assert!(matches!(
            error,
            ProxyError::TooManyRequests {
                resource: "local broker command queue"
            }
        ));

        let report = service.task_group().shutdown(Duration::from_secs(5)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[test]
    fn local_command_queue_rejects_count_overload_immediately() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let client = LocalBrokerFacadeClient {
            sender,
            count_budget: Arc::new(tokio::sync::Semaphore::new(1)),
            byte_budget: Arc::new(tokio::sync::Semaphore::new(4_096)),
            rejected: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            broker_name: "broker-a".to_owned(),
        };
        let _first_reply = client
            .enqueue(|reply| super::LocalBrokerCommand::QueryRoute {
                topic: ResourceIdentity::new("", "TopicA"),
                reply,
            })
            .expect("first command should consume the count slot");
        let error = client
            .enqueue(|reply| super::LocalBrokerCommand::QueryRoute {
                topic: ResourceIdentity::new("", "TopicB"),
                reply,
            })
            .expect_err("second command must be rejected without waiting for capacity");

        assert!(matches!(
            error,
            ProxyError::TooManyRequests {
                resource: "local broker command queue"
            }
        ));
    }

    #[tokio::test]
    async fn raw_command_backend_enqueues_only_one_dispatch() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let client = LocalBrokerFacadeClient {
            sender,
            count_budget: Arc::new(tokio::sync::Semaphore::new(1)),
            byte_budget: Arc::new(tokio::sync::Semaphore::new(4_096)),
            rejected: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            broker_name: "broker-a".to_owned(),
        };
        let backend = LocalRemotingBackend::new(client);
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerConfig).set_opaque(9_852);

        let call = tokio::spawn(async move { backend.process(request).await });
        let queued = receiver
            .recv()
            .await
            .expect("raw-command backend must enqueue local work");
        match queued.command {
            super::LocalBrokerCommand::ProcessRemoting {
                request,
                timeout,
                reply,
            } => {
                assert_eq!(RequestCode::from(request.code()), RequestCode::GetBrokerConfig);
                assert_eq!(request.opaque(), 9_852);
                assert_eq!(timeout, Duration::from_secs(3));
                let response = RemotingResponse::command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque())
                        .mark_response_type(),
                )
                .map(EmbeddedDispatchOutcome::Reply)
                .expect("valid embedded response");
                assert!(
                    reply.send(Ok(response)).is_ok(),
                    "backend call must own the reply receiver"
                );
            }
            _ => panic!("raw-command backend enqueued an unrelated command"),
        }

        let response = call.await.expect("backend task joins").expect("backend succeeds");
        let EmbeddedDispatchOutcome::Reply(response) = response else {
            panic!("backend must return a reply plan")
        };
        assert_eq!(ResponseCode::from(response.response_code()), ResponseCode::Success);
    }

    #[tokio::test]
    async fn client_api_preserves_timeout_and_identity() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let client = LocalBrokerFacadeClient {
            sender,
            count_budget: Arc::new(tokio::sync::Semaphore::new(1)),
            byte_budget: Arc::new(tokio::sync::Semaphore::new(4_096)),
            rejected: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            broker_name: "broker-a".to_owned(),
        };
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerConfig).set_opaque(9_857);

        let call = tokio::spawn(async move {
            client
                .process_remoting_with_timeout(request, Duration::from_millis(275))
                .await
        });
        let queued = receiver.recv().await.expect("client API must enqueue local work");
        match queued.command {
            super::LocalBrokerCommand::ProcessRemoting {
                request,
                timeout,
                reply,
            } => {
                assert_eq!(RequestCode::from(request.code()), RequestCode::GetBrokerConfig);
                assert_eq!(request.opaque(), 9_857);
                assert_eq!(timeout, Duration::from_millis(275));
                let response = RemotingResponse::command(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque())
                        .mark_response_type(),
                )
                .map(EmbeddedDispatchOutcome::Reply)
                .expect("valid embedded response");
                assert!(
                    reply.send(Ok(response)).is_ok(),
                    "client call must own the reply receiver"
                );
            }
            _ => panic!("client API enqueued an unrelated command"),
        }

        let response = call.await.expect("client task joins").expect("client call succeeds");
        let EmbeddedDispatchOutcome::Reply(response) = response else {
            panic!("client must return a reply plan")
        };
        assert_eq!(ResponseCode::from(response.response_code()), ResponseCode::Success);
    }

    #[test]
    fn local_command_queue_rejects_expired_entries() {
        let (reply, mut receiver) = tokio::sync::oneshot::channel();
        let byte_budget = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = byte_budget
            .try_acquire_owned()
            .expect("test byte permit should be available");
        let count_budget = Arc::new(tokio::sync::Semaphore::new(1));
        let count_permit = count_budget
            .try_acquire_owned()
            .expect("test count permit should be available");
        let enqueued_at = Instant::now();
        let queued = super::QueuedLocalBrokerCommand {
            command: super::LocalBrokerCommand::QueryRoute {
                topic: ResourceIdentity::new("", "TopicA"),
                reply,
            },
            enqueued_at,
            deadline_at: None,
            timeout_budget: None,
            _count_permit: count_permit,
            _byte_permit: permit,
        };

        assert!(queued.is_expired(enqueued_at + Duration::from_millis(11), Duration::from_millis(10)));
        queued.command.reject_overload();
        assert!(matches!(
            receiver
                .try_recv()
                .expect("expired command must receive an overload reply"),
            Err(ProxyError::TooManyRequests {
                resource: "local broker command queue"
            })
        ));
    }

    #[tokio::test]
    async fn embedded_broker_configuration_is_validated_before_worker_spawn() {
        let runtime = rocketmq_runtime::RuntimeContext::try_from_current("proxy-local-config-test")
            .expect("test runtime context");
        let service = runtime.service_context("proxy-local-config-test.service");
        let store = tempfile::tempdir().expect("create local Broker store directory");
        let config = LocalConfig {
            broker_ip: "invalid host!".to_owned(),
            broker_listen_port: available_local_broker_port(),
            store_root_dir: store.path().to_string_lossy().into_owned(),
            ..LocalConfig::default()
        };

        match LocalBrokerFacadeClient::new(config, &service, TelemetryHandle::noop()) {
            Err(ProxyError::RocketMQ(RocketMQError::ConfigInvalidValue { key, reason, .. })) => {
                assert_eq!(key, "proxy.local.embeddedBroker");
                assert!(reason.contains("broker.brokerIp1"), "{reason}");
            }
            Err(error) => panic!("unexpected error: {error}"),
            Ok(_) => panic!("invalid embedded broker configuration must be rejected"),
        }
        assert_eq!(service.task_group().component_count(), 0);
    }

    #[tokio::test]
    async fn embedded_local_worker_stops_with_its_task_group() {
        let runtime =
            rocketmq_runtime::RuntimeContext::try_from_current("proxy-local-test").expect("test runtime context");
        let service = runtime.service_context("proxy-local-test.service");
        let store = tempfile::tempdir().expect("create local Broker store directory");
        let config = LocalConfig {
            broker_listen_port: available_local_broker_port(),
            store_root_dir: store.path().to_string_lossy().into_owned(),
            ..LocalConfig::default()
        };
        let client = LocalBrokerFacadeClient::new(config, &service, TelemetryHandle::noop())
            .expect("managed local client builds");
        assert_eq!(service.task_group().task_count(), 0);
        assert_eq!(service.task_group().component_count(), 1);

        drop(client);
        let deadline = ShutdownDeadline::after(Duration::from_secs(5));
        let report = service.task_group().shutdown_until(deadline).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(report.to_json().contains("command-lanes"), "{}", report.to_json());
        assert_eq!(service.task_group().task_count(), 0);
    }
}
