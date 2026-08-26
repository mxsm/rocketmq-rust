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

use std::collections::HashMap;
use std::sync::Arc;

use rocketmq_auth::AuthRuntime;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_store::BrokerStorePort;
use rocketmq_transport::api::v1::command_from_error_with_factory_and_opaque;
use rocketmq_transport::api::v1::command_from_error_with_factory_remark_and_opaque;
use rocketmq_transport::api::v1::internal_error_with_factory_and_opaque;
use rocketmq_transport::api::v1::request_code_not_supported_with_factory;
use rocketmq_transport::api::v1::request_code_not_supported_with_factory_and_opaque;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::RejectRequestResponse;
use rocketmq_transport::api::v1::RequestOrdering;
use rocketmq_transport::api::v1::RequestProcessor;
use tracing::warn;

use self::client_manage_processor::ClientManageProcessor;
use crate::latency::broker_fast_failure::BrokerFastFailure;
use crate::latency::broker_fast_failure::FastFailureQueueKind;
use crate::latency::broker_fast_failure::FastFailureTask;
use crate::processor::ack_message_processor::AckMessageProcessor;
use crate::processor::admin_broker_processor::AdminBrokerProcessor;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeProcessor;
use crate::processor::consumer_manage_processor::ConsumerManageProcessor;
use crate::processor::end_transaction_processor::EndTransactionProcessor;
use crate::processor::lite_manager_processor::LiteManagerProcessor;
use crate::processor::lite_subscription_ctl_processor::LiteSubscriptionCtlProcessor;
use crate::processor::maintenance_request_processor::MaintenanceRequestProcessor;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::peek_message_processor::PeekMessageProcessor;
use crate::processor::polling_info_processor::PollingInfoProcessor;
use crate::processor::pop_lite_message_processor::PopLiteMessageProcessor;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::PullMessageProcessor;
use crate::processor::query_assignment_processor::QueryAssignmentProcessor;
use crate::processor::query_message_processor::QueryMessageProcessor;
use crate::processor::query_message_processor::QueryMessageStoreCapability;
use crate::processor::recall_message_processor::RecallMessageProcessor;
use crate::processor::reply_message_processor::ReplyMessageProcessor;
use crate::processor::send_message_processor::SendMessageProcessor;
use crate::transaction::transactional_message_service::TransactionalMessageService;

pub(crate) mod ack_message_processor;
pub(crate) mod admin_broker_processor;
pub(crate) mod change_invisible_time_processor;
pub(crate) mod client_manage_processor;
pub(crate) mod consumer_manage_processor;
pub(crate) mod default_pull_message_result_handler;
pub(crate) mod end_transaction_processor;
pub(crate) mod lite_manager_processor;
pub(crate) mod lite_subscription_ctl_processor;
pub(crate) mod maintenance_request_processor;
pub(crate) mod notification_processor;
pub(crate) mod peek_message_processor;
pub(crate) mod polling_info_processor;
pub(crate) mod pop_inflight_message_counter;
pub(crate) mod pop_lite_message_processor;
pub(crate) mod pop_message_processor;
pub(crate) mod processor_service;
pub(crate) mod pull_message_processor;
pub(crate) mod pull_message_result_handler;
pub(crate) mod query_assignment_processor;
pub(crate) mod query_message_processor;
pub(crate) mod recall_message_processor;
pub(crate) mod reply_message_processor;
mod request_ordering;
pub(crate) mod send_message_processor;

pub enum BrokerProcessorType<MS: BrokerStorePort, TS> {
    Send(Arc<SendMessageProcessor<MS, TS>>),
    Pull(Arc<PullMessageProcessor<MS>>),
    Peek(Arc<PeekMessageProcessor<MS>>),
    Pop(Arc<PopMessageProcessor<MS>>),
    PopLite(Arc<PopLiteMessageProcessor<MS>>),
    Ack(Arc<AckMessageProcessor<MS>>),
    ChangeInvisible(Arc<ChangeInvisibleTimeProcessor<MS>>),
    Notification(Arc<NotificationProcessor<MS>>),
    PollingInfo(Arc<PollingInfoProcessor>),
    Reply(Arc<ReplyMessageProcessor<MS, TS>>),
    Recall(Arc<RecallMessageProcessor<MS>>),
    QueryMessage(Arc<QueryMessageProcessor<QueryMessageStoreCapability<MS>>>),
    ClientManage(Arc<ClientManageProcessor<MS>>),
    ConsumerManage(Arc<ConsumerManageProcessor<MS>>),
    QueryAssignment(Arc<QueryAssignmentProcessor>),
    LiteManager(Arc<LiteManagerProcessor<MS>>),
    LiteSubscriptionCtl(Arc<LiteSubscriptionCtlProcessor<MS>>),
    EndTransaction(Arc<EndTransactionProcessor<TS, MS>>),
    Maintenance(Arc<MaintenanceRequestProcessor>),
    AdminBroker(Arc<AdminBrokerProcessor<MS>>),
}

impl<MS, TS> Clone for BrokerProcessorType<MS, TS>
where
    MS: BrokerStorePort,
{
    fn clone(&self) -> Self {
        match self {
            Self::Send(processor) => Self::Send(processor.clone()),
            Self::Pull(processor) => Self::Pull(processor.clone()),
            Self::Peek(processor) => Self::Peek(processor.clone()),
            Self::Pop(processor) => Self::Pop(processor.clone()),
            Self::PopLite(processor) => Self::PopLite(processor.clone()),
            Self::Ack(processor) => Self::Ack(processor.clone()),
            Self::ChangeInvisible(processor) => Self::ChangeInvisible(processor.clone()),
            Self::Notification(processor) => Self::Notification(processor.clone()),
            Self::PollingInfo(processor) => Self::PollingInfo(processor.clone()),
            Self::Reply(processor) => Self::Reply(processor.clone()),
            Self::Recall(processor) => Self::Recall(processor.clone()),
            Self::QueryMessage(processor) => Self::QueryMessage(processor.clone()),
            Self::ClientManage(processor) => Self::ClientManage(processor.clone()),
            Self::ConsumerManage(processor) => Self::ConsumerManage(processor.clone()),
            Self::QueryAssignment(processor) => Self::QueryAssignment(processor.clone()),
            Self::LiteManager(processor) => Self::LiteManager(processor.clone()),
            Self::LiteSubscriptionCtl(processor) => Self::LiteSubscriptionCtl(processor.clone()),
            Self::EndTransaction(processor) => Self::EndTransaction(processor.clone()),
            Self::Maintenance(processor) => Self::Maintenance(processor.clone()),
            Self::AdminBroker(processor) => Self::AdminBroker(processor.clone()),
        }
    }
}

#[cfg(test)]
impl<MS, TS> BrokerProcessorType<MS, TS>
where
    MS: BrokerStorePort,
{
    pub(crate) fn variant_name_for_test(&self) -> &'static str {
        match self {
            BrokerProcessorType::Send(_) => "Send",
            BrokerProcessorType::Pull(_) => "Pull",
            BrokerProcessorType::Peek(_) => "Peek",
            BrokerProcessorType::Pop(_) => "Pop",
            BrokerProcessorType::PopLite(_) => "PopLite",
            BrokerProcessorType::Ack(_) => "Ack",
            BrokerProcessorType::ChangeInvisible(_) => "ChangeInvisible",
            BrokerProcessorType::Notification(_) => "Notification",
            BrokerProcessorType::PollingInfo(_) => "PollingInfo",
            BrokerProcessorType::Reply(_) => "Reply",
            BrokerProcessorType::Recall(_) => "Recall",
            BrokerProcessorType::QueryMessage(_) => "QueryMessage",
            BrokerProcessorType::ClientManage(_) => "ClientManage",
            BrokerProcessorType::ConsumerManage(_) => "ConsumerManage",
            BrokerProcessorType::QueryAssignment(_) => "QueryAssignment",
            BrokerProcessorType::LiteManager(_) => "LiteManager",
            BrokerProcessorType::LiteSubscriptionCtl(_) => "LiteSubscriptionCtl",
            BrokerProcessorType::EndTransaction(_) => "EndTransaction",
            BrokerProcessorType::Maintenance(_) => "Maintenance",
            BrokerProcessorType::AdminBroker(_) => "AdminBroker",
        }
    }
}

impl<MS, TS> RequestProcessor for BrokerProcessorType<MS, TS>
where
    MS: BrokerStorePort,
    TS: TransactionalMessageService,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match self {
            BrokerProcessorType::Send(processor) => processor.process_request_shared(channel, ctx, request).await,
            BrokerProcessorType::Pull(processor) => processor.process_request_shared(channel, ctx, request).await,
            BrokerProcessorType::Peek(processor) => processor.process_request_shared(channel, ctx, request).await,
            BrokerProcessorType::Pop(processor) => processor.process_request_shared(channel, ctx, request).await,
            BrokerProcessorType::PopLite(processor) => processor.process_request_shared(channel, ctx, request).await,
            BrokerProcessorType::Ack(processor) => processor.process_request_shared(channel, ctx, request).await,
            BrokerProcessorType::ChangeInvisible(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::Notification(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::PollingInfo(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::Reply(processor) => processor.process_request_shared(channel, ctx, request).await,
            BrokerProcessorType::Recall(processor) => processor.process_request_shared(channel, ctx, request).await,
            BrokerProcessorType::QueryMessage(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::ClientManage(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::ConsumerManage(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::QueryAssignment(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::LiteManager(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::LiteSubscriptionCtl(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::EndTransaction(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::Maintenance(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
            BrokerProcessorType::AdminBroker(processor) => {
                processor.process_request_shared(channel, ctx, request).await
            }
        }
    }

    fn reject_request(&self, code: i32) -> RejectRequestResponse {
        match self {
            BrokerProcessorType::Send(processor) => processor.reject_request(code),
            BrokerProcessorType::Pull(processor) => processor.reject_request_shared(),
            BrokerProcessorType::Peek(processor) => processor.reject_request(code),
            BrokerProcessorType::Pop(processor) => processor.reject_request(code),
            BrokerProcessorType::PopLite(processor) => processor.reject_request(code),
            BrokerProcessorType::Ack(processor) => processor.reject_request(code),
            BrokerProcessorType::ChangeInvisible(processor) => processor.reject_request(code),
            BrokerProcessorType::Notification(processor) => processor.reject_request(code),
            BrokerProcessorType::PollingInfo(processor) => processor.reject_request(code),
            BrokerProcessorType::Reply(processor) => processor.reject_request(code),
            BrokerProcessorType::Recall(processor) => processor.reject_request(code),
            BrokerProcessorType::QueryMessage(processor) => processor.reject_request(code),
            BrokerProcessorType::ClientManage(processor) => processor.reject_request(code),
            BrokerProcessorType::ConsumerManage(processor) => processor.reject_request(code),
            BrokerProcessorType::QueryAssignment(processor) => processor.reject_request(code),
            BrokerProcessorType::LiteManager(processor) => processor.reject_request(code),
            BrokerProcessorType::LiteSubscriptionCtl(processor) => processor.reject_request(code),
            BrokerProcessorType::EndTransaction(processor) => processor.reject_request(code),
            BrokerProcessorType::Maintenance(_) => (false, None),
            BrokerProcessorType::AdminBroker(_) => (false, None),
        }
    }

    fn request_ordering(&self, request: &RemotingCommand) -> RequestOrdering {
        request_ordering::broker_request_ordering(request)
    }
}

pub(crate) type RequestCodeType = i32;

pub struct BrokerRequestProcessor<MS: BrokerStorePort, TS> {
    command_factory: RemotingCommandFactory,
    process_table: Arc<HashMap<RequestCodeType, BrokerProcessorType<MS, TS>>>,
    default_request_processor: Option<Arc<BrokerProcessorType<MS, TS>>>,
    auth_runtime: Option<Arc<AuthRuntime>>,
    broker_fast_failure: Option<BrokerFastFailure>,
    request_task_group: Option<TaskGroup>,
}

impl<MS, TS> BrokerRequestProcessor<MS, TS>
where
    MS: BrokerStorePort,
    TS: TransactionalMessageService,
{
    pub fn new() -> Self {
        Self::new_with_factory(application_remoting_command_factory())
    }

    pub fn new_with_factory(command_factory: RemotingCommandFactory) -> Self {
        Self {
            command_factory,
            process_table: Arc::new(HashMap::new()),
            default_request_processor: None,
            auth_runtime: None,
            broker_fast_failure: None,
            request_task_group: None,
        }
    }

    pub fn register_processor(&mut self, request_code: RequestCodeType, processor: BrokerProcessorType<MS, TS>) {
        Arc::make_mut(&mut self.process_table).insert(request_code, processor);
    }

    pub fn register_default_processor(&mut self, processor: BrokerProcessorType<MS, TS>) {
        self.default_request_processor = Some(Arc::new(processor));
    }

    pub fn set_auth_runtime(&mut self, auth_runtime: Arc<AuthRuntime>) {
        self.auth_runtime = Some(auth_runtime);
    }

    pub fn set_broker_fast_failure(&mut self, broker_fast_failure: BrokerFastFailure) {
        self.broker_fast_failure = Some(broker_fast_failure);
    }

    pub fn set_request_task_group(&mut self, request_task_group: TaskGroup) {
        self.request_task_group = Some(request_task_group);
    }
}

#[cfg(test)]
impl<MS, TS> BrokerRequestProcessor<MS, TS>
where
    MS: BrokerStorePort,
{
    pub(crate) fn dispatch_processor_variant_for_test(&self, request_code: RequestCode) -> Option<&'static str> {
        self.process_table
            .get(&request_code.to_i32())
            .map(BrokerProcessorType::variant_name_for_test)
            .or_else(|| {
                self.default_request_processor
                    .as_ref()
                    .map(|processor| processor.as_ref().variant_name_for_test())
            })
    }
}

impl<MS: BrokerStorePort, TS> Clone for BrokerRequestProcessor<MS, TS> {
    fn clone(&self) -> Self {
        Self {
            command_factory: self.command_factory,
            process_table: self.process_table.clone(),
            default_request_processor: self.default_request_processor.clone(),
            auth_runtime: self.auth_runtime.clone(),
            broker_fast_failure: self.broker_fast_failure.clone(),
            request_task_group: self.request_task_group.clone(),
        }
    }
}

impl<MS, TS> RequestProcessor for BrokerRequestProcessor<MS, TS>
where
    MS: BrokerStorePort + Send + Sync + 'static,
    TS: TransactionalMessageService + Send + Sync + 'static,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        let privileged_maintenance = is_privileged_maintenance_request(request_code);
        if privileged_maintenance
            && !matches!(
                self.process_table.get(request.code_ref()),
                Some(BrokerProcessorType::Maintenance(_))
            )
        {
            let error = rocketmq_error::RocketMQError::authentication_failed(
                "Broker maintenance API is disabled or unavailable",
            );
            return Ok(Some(command_from_error_with_factory_remark_and_opaque(
                &self.command_factory,
                &error,
                error.to_string(),
                request.opaque(),
            )));
        }
        if !privileged_maintenance {
            if let Some(auth_runtime) = &self.auth_runtime {
                if let Err(error) = auth_runtime.check_remoting(&ctx, request).await {
                    let response = command_from_error_with_factory_remark_and_opaque(
                        &self.command_factory,
                        &error,
                        error.to_string(),
                        request.opaque(),
                    );
                    return Ok(Some(response));
                }
            }
        }

        let request_code = *request.code_ref();
        let opaque = request.opaque();

        let result = match self.process_table.get(&request_code).cloned() {
            Some(processor) => {
                self.process_with_optional_fast_failure(
                    fast_failure_queue_kind(request_code, false),
                    processor,
                    channel,
                    ctx,
                    request,
                )
                .await
            }
            None => match self.default_request_processor.as_ref() {
                Some(default_processor) => {
                    self.process_with_optional_fast_failure(
                        fast_failure_queue_kind(request_code, true),
                        default_processor.as_ref().clone(),
                        channel,
                        ctx,
                        request,
                    )
                    .await
                }
                None => {
                    let response = request_code_not_supported_with_factory_and_opaque(
                        &self.command_factory,
                        request.code(),
                        request.opaque(),
                    );
                    Ok(Some(response))
                }
            },
        };

        map_request_header_error(&self.command_factory, result, opaque)
    }

    fn reject_request(&self, code: i32) -> RejectRequestResponse {
        match self.process_table.get(&code) {
            Some(processor) => processor.reject_request(code),
            None => {
                if let Some(default_processor) = &self.default_request_processor {
                    default_processor.reject_request(code)
                } else {
                    (
                        true,
                        Some(request_code_not_supported_with_factory(&self.command_factory, code)),
                    )
                }
            }
        }
    }

    fn request_ordering(&self, request: &RemotingCommand) -> RequestOrdering {
        request_ordering::broker_request_ordering(request)
    }
}

const fn is_privileged_maintenance_request(request_code: RequestCode) -> bool {
    matches!(
        request_code,
        RequestCode::MaintenanceGetCapabilities
            | RequestCode::MaintenanceCreateStoreCheckpoint
            | RequestCode::MaintenanceVerifyCheckpoint
            | RequestCode::MaintenanceRestoreVerify
    )
}

impl<MS, TS> BrokerRequestProcessor<MS, TS>
where
    MS: BrokerStorePort + Send + Sync + 'static,
    TS: TransactionalMessageService + Send + Sync + 'static,
{
    async fn process_with_optional_fast_failure(
        &self,
        queue_kind: Option<FastFailureQueueKind>,
        mut processor: BrokerProcessorType<MS, TS>,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let Some(queue_kind) = queue_kind else {
            return processor.process_request(channel, ctx, request).await;
        };
        let Some(broker_fast_failure) = &self.broker_fast_failure else {
            return processor.process_request(channel, ctx, request).await;
        };
        if !broker_fast_failure.is_enabled() {
            return processor.process_request(channel, ctx, request).await;
        }

        let opaque = request.opaque();
        let retained_bytes = estimate_fast_failure_retained_bytes(request);
        let (task, response_rx) = match broker_fast_failure.try_enqueue(queue_kind, opaque, retained_bytes) {
            Ok(admitted) => admitted,
            Err(response) => return Ok(Some(response)),
        };
        let queued_request = request.clone();
        let broker_fast_failure = broker_fast_failure.clone();
        let detach_response = should_detach_fast_failure_response(
            queue_kind,
            broker_fast_failure.send_request_executor_detached_enabled(),
            self.request_task_group.is_some(),
        );

        if detach_response {
            let Some(task_group) = &self.request_task_group else {
                return Ok(Some(system_error_response(
                    &self.command_factory,
                    opaque,
                    "detached send request executor has no task group",
                )));
            };
            let detached_task = Self::run_detached_fast_failure_request(
                queue_kind,
                broker_fast_failure.clone(),
                task.clone(),
                processor,
                channel,
                ctx,
                queued_request,
                opaque,
                response_rx,
            );
            if let Err(error) =
                task_group.spawn("broker.request.fast-failure.detached", TaskKind::Worker, detached_task)
            {
                warn!(?error, "failed to spawn detached fast failure request task");
                broker_fast_failure.cancel(
                    queue_kind,
                    &task,
                    system_error_response(
                        &self.command_factory,
                        opaque,
                        "detached fast failure request task spawn failed",
                    ),
                );
                return Ok(Some(system_error_response(
                    &self.command_factory,
                    opaque,
                    "detached fast failure request task spawn failed",
                )));
            }
            return Ok(None);
        }

        let request_task = Self::run_fast_failure_request(
            queue_kind,
            broker_fast_failure.clone(),
            task.clone(),
            processor,
            channel,
            ctx,
            queued_request,
            opaque,
        );
        if let Some(task_group) = &self.request_task_group {
            if let Err(error) = task_group.spawn("broker.request.fast-failure", TaskKind::Worker, request_task) {
                warn!(?error, "failed to spawn fast failure request task");
                broker_fast_failure.cancel(
                    queue_kind,
                    &task,
                    system_error_response(&self.command_factory, opaque, "fast failure request task spawn failed"),
                );
            }
        } else {
            request_task.await;
        }

        match response_rx.await {
            Ok(response) => Ok(response),
            Err(_error) => Ok(Some(system_error_response(
                &self.command_factory,
                opaque,
                "fast failure response channel closed before request completed",
            ))),
        }
    }

    async fn run_detached_fast_failure_request(
        queue_kind: FastFailureQueueKind,
        broker_fast_failure: BrokerFastFailure,
        task: Arc<FastFailureTask>,
        processor: BrokerProcessorType<MS, TS>,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        queued_request: RemotingCommand,
        opaque: i32,
        response_rx: tokio::sync::oneshot::Receiver<Option<RemotingCommand>>,
    ) {
        let command_factory = broker_fast_failure.command_factory();
        let request_task = Self::run_fast_failure_request(
            queue_kind,
            broker_fast_failure,
            task,
            processor,
            channel,
            ctx.clone(),
            queued_request,
            opaque,
        );
        tokio::pin!(request_task);
        tokio::pin!(response_rx);

        let response_result = tokio::select! {
            _ = &mut request_task => (&mut response_rx).await,
            response_result = &mut response_rx => response_result,
        };
        Self::write_detached_fast_failure_response(&command_factory, response_result, ctx, opaque).await;
    }

    async fn write_detached_fast_failure_response(
        command_factory: &RemotingCommandFactory,
        response_result: Result<Option<RemotingCommand>, tokio::sync::oneshot::error::RecvError>,
        ctx: ConnectionHandlerContext,
        opaque: i32,
    ) {
        match response_result {
            Ok(Some(response)) => {
                if let Err(error) = ctx.try_write_response(response.set_opaque(opaque)).await {
                    warn!(
                        kind = error.kind().as_str(),
                        progress = error.write_progress().map_or("none", |progress| progress.as_str()),
                        retryable = error.retryable(),
                        "detached fast failure response write failed; not retrying"
                    );
                }
            }
            Ok(None) => {}
            Err(_error) => {
                if let Err(error) = ctx
                    .try_write_response(system_error_response(
                        command_factory,
                        opaque,
                        "fast failure response channel closed before detached request completed",
                    ))
                    .await
                {
                    warn!(
                        kind = error.kind().as_str(),
                        progress = error.write_progress().map_or("none", |progress| progress.as_str()),
                        retryable = error.retryable(),
                        "detached fast failure response write failed; not retrying"
                    );
                }
            }
        }
    }

    async fn run_fast_failure_request(
        queue_kind: FastFailureQueueKind,
        broker_fast_failure: BrokerFastFailure,
        task: Arc<FastFailureTask>,
        mut processor: BrokerProcessorType<MS, TS>,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        mut queued_request: RemotingCommand,
        opaque: i32,
    ) {
        let Some(_permit) = broker_fast_failure.acquire_permit(queue_kind).await else {
            warn!("fast failure queue permit acquisition failed: queue={queue_kind:?}");
            if broker_fast_failure.try_mark_running(queue_kind, &task) {
                broker_fast_failure.complete(
                    queue_kind,
                    &task,
                    Some(system_error_response(
                        &broker_fast_failure.command_factory(),
                        opaque,
                        "fast failure queue permit acquisition failed",
                    )),
                );
            }
            return;
        };

        if !broker_fast_failure.try_mark_running(queue_kind, &task) {
            return;
        }

        let response = match processor.process_request(channel, ctx, &mut queued_request).await {
            Ok(response) => response,
            Err(error) => Some(command_from_error_with_factory_and_opaque(
                &broker_fast_failure.command_factory(),
                &error,
                opaque,
            )),
        };
        broker_fast_failure.complete(queue_kind, &task, response);
    }
}

fn estimate_fast_failure_retained_bytes(request: &RemotingCommand) -> usize {
    let mut retained_bytes = std::mem::size_of::<RemotingCommand>();
    retained_bytes = retained_bytes.saturating_add(request.body().map_or(0, bytes::Bytes::len));
    retained_bytes = retained_bytes.saturating_add(request.remark().map_or(0, |remark| remark.len()));
    if let Some(ext_fields) = request.ext_fields() {
        retained_bytes = retained_bytes.saturating_add(
            ext_fields
                .iter()
                .map(|(key, value)| {
                    std::mem::size_of_val(key)
                        .saturating_add(std::mem::size_of_val(value))
                        .saturating_add(key.len())
                        .saturating_add(value.len())
                })
                .fold(0usize, usize::saturating_add),
        );
    }
    retained_bytes.max(1)
}

fn should_detach_fast_failure_response(
    queue_kind: FastFailureQueueKind,
    send_request_executor_detached_enabled: bool,
    has_request_task_group: bool,
) -> bool {
    queue_kind == FastFailureQueueKind::Send && send_request_executor_detached_enabled && has_request_task_group
}

fn fast_failure_queue_kind(request_code: i32, default_processor: bool) -> Option<FastFailureQueueKind> {
    if default_processor {
        return Some(FastFailureQueueKind::AdminBroker);
    }

    match RequestCode::from(request_code) {
        RequestCode::SendMessage
        | RequestCode::SendMessageV2
        | RequestCode::SendBatchMessage
        | RequestCode::ConsumerSendMsgBack => Some(FastFailureQueueKind::Send),
        RequestCode::PullMessage => Some(FastFailureQueueKind::Pull),
        RequestCode::LitePullMessage => Some(FastFailureQueueKind::LitePull),
        RequestCode::HeartBeat => Some(FastFailureQueueKind::Heartbeat),
        RequestCode::EndTransaction => Some(FastFailureQueueKind::Transaction),
        RequestCode::AckMessage | RequestCode::BatchAckMessage => Some(FastFailureQueueKind::Ack),
        _ => None,
    }
}

fn system_error_response(
    command_factory: &RemotingCommandFactory,
    opaque: i32,
    remark: impl Into<String>,
) -> RemotingCommand {
    internal_error_with_factory_and_opaque(command_factory, opaque, remark)
}

fn map_request_header_error(
    command_factory: &RemotingCommandFactory,
    result: rocketmq_error::RocketMQResult<Option<RemotingCommand>>,
    opaque: i32,
) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
    match result {
        Err(error) if error.kind() == rocketmq_error::ErrorKind::RequestHeaderError => Ok(Some(
            command_from_error_with_factory_and_opaque(command_factory, &error, opaque),
        )),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_auth::AuthConfig;
    use rocketmq_auth::AuthRuntimeBuilder;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
    use rocketmq_protocol::protocol::SerializeType;
    use rocketmq_store::LocalFileMessageStore;
    use rocketmq_transport::test_support::LocalRequestHarness;

    use crate::transaction::queue::default_transactional_message_service::DefaultTransactionalMessageService;

    type TestBrokerRequestProcessor =
        BrokerRequestProcessor<LocalFileMessageStore, DefaultTransactionalMessageService<LocalFileMessageStore>>;

    #[test]
    fn request_header_error_mapping_is_stable_redacted_and_preserves_opaque() {
        let source = rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
            format: "header",
            message: "secret-extension-value".to_owned(),
        });
        let error = rocketmq_error::RocketMQError::request_header_source("decode test request header", source);

        let response = map_request_header_error(&application_remoting_command_factory(), Err(error), 47)
            .expect("request-header error should become a response")
            .expect("request-header error should return a response command");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        assert_eq!(response.opaque(), 47);
        assert_eq!(
            response.remark().map(|remark| remark.as_str()),
            Some("Request header is invalid")
        );
        assert!(!response.remark().is_some_and(|remark| remark.contains("secret")));
    }

    #[tokio::test]
    async fn broker_request_processors_keep_per_instance_wire_defaults() {
        let json_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(672, SerializeType::JSON));
        let binary_factory = RemotingCommandFactory::new(RemotingCommandDefaults::new(673, SerializeType::ROCKETMQ));
        let mut json_processor = TestBrokerRequestProcessor::new_with_factory(json_factory);
        let mut binary_processor = TestBrokerRequestProcessor::new_with_factory(binary_factory);
        let harness = LocalRequestHarness::new(crate::test_task_group("broker-factory-owner"))
            .await
            .expect("local remoting harness should start");

        let mut json_request = RemotingCommand::create_remoting_command(99_901).set_opaque(41);
        let json_response = json_processor
            .process_request(harness.channel(), harness.context(), &mut json_request)
            .await
            .expect("JSON owner should build a response")
            .expect("unsupported request should return a response");
        let mut binary_request = RemotingCommand::create_remoting_command(99_902).set_opaque(42);
        let binary_response = binary_processor
            .process_request(harness.channel(), harness.context(), &mut binary_request)
            .await
            .expect("ROCKETMQ owner should build a response")
            .expect("unsupported request should return a response");

        assert_eq!(json_response.version(), 672);
        assert_eq!(json_response.serialize_type(), SerializeType::JSON);
        assert_eq!(json_response.opaque(), 41);
        assert_eq!(binary_response.version(), 673);
        assert_eq!(binary_response.serialize_type(), SerializeType::ROCKETMQ);
        assert_eq!(binary_response.opaque(), 42);
    }

    #[test]
    fn fast_failure_queue_kind_maps_java_fast_failure_families() {
        assert_eq!(
            fast_failure_queue_kind(RequestCode::SendMessage as i32, false),
            Some(FastFailureQueueKind::Send)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::SendMessageV2 as i32, false),
            Some(FastFailureQueueKind::Send)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::SendBatchMessage as i32, false),
            Some(FastFailureQueueKind::Send)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::ConsumerSendMsgBack as i32, false),
            Some(FastFailureQueueKind::Send)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::PullMessage as i32, false),
            Some(FastFailureQueueKind::Pull)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::LitePullMessage as i32, false),
            Some(FastFailureQueueKind::LitePull)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::HeartBeat as i32, false),
            Some(FastFailureQueueKind::Heartbeat)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::EndTransaction as i32, false),
            Some(FastFailureQueueKind::Transaction)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::AckMessage as i32, false),
            Some(FastFailureQueueKind::Ack)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::BatchAckMessage as i32, false),
            Some(FastFailureQueueKind::Ack)
        );
    }

    #[test]
    fn fast_failure_retained_bytes_include_body_remark_and_extension_fields() {
        let base = RemotingCommand::create_remoting_command(RequestCode::SendMessage).set_opaque(1);
        let base_bytes = estimate_fast_failure_retained_bytes(&base);
        let request = RemotingCommand::new_request(RequestCode::SendMessage, bytes::Bytes::from(vec![1; 1_024]))
            .set_opaque(2)
            .set_remark("busy-budget-test")
            .set_ext_fields(std::collections::HashMap::from([("topic".into(), "orders".into())]));

        let retained_bytes = estimate_fast_failure_retained_bytes(&request);

        assert!(retained_bytes >= base_bytes + 1_024 + "busy-budget-test".len() + "topic".len() + "orders".len());
    }

    #[test]
    fn fast_failure_queue_kind_maps_default_processor_to_admin_queue() {
        assert_eq!(
            fast_failure_queue_kind(RequestCode::UpdateBrokerConfig as i32, true),
            Some(FastFailureQueueKind::AdminBroker)
        );
        assert_eq!(
            fast_failure_queue_kind(RequestCode::UpdateBrokerConfig as i32, false),
            None
        );
    }

    #[test]
    fn detach_fast_failure_response_only_applies_to_send_with_task_group() {
        assert!(should_detach_fast_failure_response(
            FastFailureQueueKind::Send,
            true,
            true
        ));
        assert!(!should_detach_fast_failure_response(
            FastFailureQueueKind::Send,
            false,
            true
        ));
        assert!(!should_detach_fast_failure_response(
            FastFailureQueueKind::Send,
            true,
            false
        ));
        assert!(!should_detach_fast_failure_response(
            FastFailureQueueKind::Pull,
            true,
            true
        ));
    }

    #[test]
    fn broker_request_processor_delegates_session_ordering_policy() {
        let processor = TestBrokerRequestProcessor::new();
        let send = RemotingCommand::create_remoting_command(RequestCode::SendMessage);
        let pull = RemotingCommand::create_remoting_command(RequestCode::PullMessage);

        assert!(matches!(processor.request_ordering(&send), RequestOrdering::Ordered(_)));
        assert_eq!(processor.request_ordering(&pull), RequestOrdering::Concurrent);
    }

    #[test]
    fn transaction_processor_roots_and_registry_use_standard_arc() {
        let processor_source = include_str!("processor.rs");
        let runtime_source = [
            include_str!("broker_runtime.rs"),
            include_str!("broker_runtime/request_pipeline.rs"),
        ]
        .join("\n");

        assert!(processor_source.contains("process_table: Arc<HashMap"));
        assert!(processor_source.contains("default_request_processor: Option<Arc<"));
        assert!(!processor_source.contains(concat!("Send(ArcMut<", "SendMessageProcessor")));
        assert!(!processor_source.contains(concat!("Reply(ArcMut<", "ReplyMessageProcessor")));
        assert!(!processor_source.contains(concat!("EndTransaction(ArcMut<", "EndTransactionProcessor")));
        assert!(!runtime_source.contains(concat!("ArcMut::new(", "send_message_processor")));
        assert!(!runtime_source.contains(concat!("ArcMut::new(", "reply_message_processor")));
        assert!(!runtime_source.contains(concat!("EndTransaction(ArcMut::new(", "EndTransactionProcessor")));
    }

    #[test]
    fn core_processor_roots_use_standard_arc() {
        let processor_source = include_str!("processor.rs");
        let runtime_source = [
            include_str!("broker_runtime.rs"),
            include_str!("broker_runtime/request_pipeline.rs"),
        ]
        .join("\n");
        let query_assignment_source = include_str!("processor/query_assignment_processor.rs");

        for (variant, processor) in [
            ("Ack", "AckMessageProcessor"),
            ("ChangeInvisible", "ChangeInvisibleTimeProcessor"),
            ("Peek", "PeekMessageProcessor"),
            ("PollingInfo", "PollingInfoProcessor"),
            ("Recall", "RecallMessageProcessor"),
            ("QueryMessage", "QueryMessageProcessor"),
            ("ClientManage", "ClientManageProcessor"),
            ("ConsumerManage", "ConsumerManageProcessor"),
            ("QueryAssignment", "QueryAssignmentProcessor"),
            ("AdminBroker", "AdminBrokerProcessor"),
        ] {
            let legacy_variant = format!("{variant}({}<", concat!("Arc", "Mut"));
            assert!(
                !processor_source.contains(&legacy_variant),
                "{processor} root regressed"
            );
            let legacy_constructor = format!("{}::new({processor}::new", concat!("Arc", "Mut"));
            assert!(
                !runtime_source.contains(&legacy_constructor),
                "{processor} startup root regressed"
            );
        }

        assert!(!runtime_source.contains(concat!("ack_message_processor: Option<ArcMut<", "AckMessageProcessor")));
        assert!(!runtime_source.contains(concat!(
            "query_assignment_processor: Option<ArcMut<",
            "QueryAssignmentProcessor"
        )));
        assert!(!runtime_source.contains(concat!("ArcMut::new(", "consumer_manage_processor")));
        assert!(!runtime_source.contains("query_assignment_processor_mut("));
        assert!(!runtime_source.contains("query_assignment_processor_unchecked_mut("));
        assert!(!query_assignment_source.contains(concat!("Arc", "Mut")));
        assert!(!query_assignment_source.contains("BrokerRuntimeState"));
    }

    #[tokio::test]
    async fn broker_request_processor_checks_auth_before_dispatch() {
        let auth_runtime = AuthRuntimeBuilder::new(
            AuthConfig {
                authentication_enabled: true,
                ..AuthConfig::default()
            },
            crate::test_service_context("auth-runtime"),
        )
        .build()
        .await
        .expect("auth runtime should initialize");
        let mut processor = TestBrokerRequestProcessor::new();
        processor.set_auth_runtime(Arc::new(auth_runtime));

        let mut request = RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32()).set_opaque(7);
        let harness = LocalRequestHarness::new(crate::test_task_group("local-harness"))
            .await
            .expect("local remoting harness should start");

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("broker processor should return auth response")
            .expect("auth failure should be encoded as a response command");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        assert_eq!(response.opaque(), 7);
        assert!(
            response
                .remark()
                .is_some_and(|remark| remark.as_str().contains("username cannot be null")),
            "missing AccessKey should be reported as an authentication failure"
        );
    }

    #[tokio::test]
    async fn unregistered_maintenance_request_fails_closed_before_admin_dispatch() {
        let mut processor = TestBrokerRequestProcessor::new();
        let mut request =
            RemotingCommand::create_remoting_command(RequestCode::MaintenanceCreateStoreCheckpoint).set_opaque(19);
        let harness = LocalRequestHarness::new(crate::test_task_group("maintenance-fail-closed"))
            .await
            .expect("local remoting harness should start");

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("Broker should encode maintenance denial")
            .expect("maintenance denial should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        assert_eq!(response.opaque(), 19);
        assert!(response.remark().is_some_and(|remark| remark.contains("disabled")));
    }
}
