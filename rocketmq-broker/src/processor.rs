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
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::warn;

use self::client_manage_processor::ClientManageProcessor;
use crate::latency::broker_fast_failure::BrokerFastFailure;
use crate::latency::broker_fast_failure::FastFailureQueueKind;
use crate::processor::ack_message_processor::AckMessageProcessor;
use crate::processor::admin_broker_processor::AdminBrokerProcessor;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeProcessor;
use crate::processor::consumer_manage_processor::ConsumerManageProcessor;
use crate::processor::end_transaction_processor::EndTransactionProcessor;
use crate::processor::lite_manager_processor::LiteManagerProcessor;
use crate::processor::lite_subscription_ctl_processor::LiteSubscriptionCtlProcessor;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::peek_message_processor::PeekMessageProcessor;
use crate::processor::polling_info_processor::PollingInfoProcessor;
use crate::processor::pop_lite_message_processor::PopLiteMessageProcessor;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::PullMessageProcessor;
use crate::processor::query_assignment_processor::QueryAssignmentProcessor;
use crate::processor::query_message_processor::QueryMessageProcessor;
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
pub(crate) mod send_message_processor;

pub enum BrokerProcessorType<MS: MessageStore, TS> {
    Send(ArcMut<SendMessageProcessor<MS, TS>>),
    Pull(ArcMut<PullMessageProcessor<MS>>),
    Peek(ArcMut<PeekMessageProcessor<MS>>),
    Pop(ArcMut<PopMessageProcessor<MS>>),
    PopLite(ArcMut<PopLiteMessageProcessor<MS>>),
    Ack(ArcMut<AckMessageProcessor<MS>>),
    ChangeInvisible(ArcMut<ChangeInvisibleTimeProcessor<MS>>),
    Notification(ArcMut<NotificationProcessor<MS>>),
    PollingInfo(ArcMut<PollingInfoProcessor<MS>>),
    Reply(ArcMut<ReplyMessageProcessor<MS, TS>>),
    Recall(ArcMut<RecallMessageProcessor<MS>>),
    QueryMessage(ArcMut<QueryMessageProcessor<MS>>),
    ClientManage(ArcMut<ClientManageProcessor<MS>>),
    ConsumerManage(ArcMut<ConsumerManageProcessor<MS>>),
    QueryAssignment(ArcMut<QueryAssignmentProcessor<MS>>),
    LiteManager(ArcMut<LiteManagerProcessor<MS>>),
    LiteSubscriptionCtl(ArcMut<LiteSubscriptionCtlProcessor<MS>>),
    EndTransaction(ArcMut<EndTransactionProcessor<TS, MS>>),
    AdminBroker(ArcMut<AdminBrokerProcessor<MS>>),
}

impl<MS, TS> Clone for BrokerProcessorType<MS, TS>
where
    MS: MessageStore,
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
            Self::AdminBroker(processor) => Self::AdminBroker(processor.clone()),
        }
    }
}

#[cfg(test)]
impl<MS, TS> BrokerProcessorType<MS, TS>
where
    MS: MessageStore,
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
            BrokerProcessorType::AdminBroker(_) => "AdminBroker",
        }
    }
}

impl<MS, TS> RequestProcessor for BrokerProcessorType<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match self {
            BrokerProcessorType::Send(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::Pull(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::Peek(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::Pop(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::PopLite(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::Ack(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::ChangeInvisible(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::Notification(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::PollingInfo(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::Reply(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::Recall(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::QueryMessage(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::ClientManage(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::ConsumerManage(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::QueryAssignment(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::LiteManager(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::LiteSubscriptionCtl(processor) => {
                processor.process_request(channel, ctx, request).await
            }
            BrokerProcessorType::EndTransaction(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::AdminBroker(processor) => processor.process_request(channel, ctx, request).await,
        }
    }

    fn reject_request(&self, code: i32) -> RejectRequestResponse {
        match self {
            BrokerProcessorType::Send(processor) => processor.reject_request(code),
            BrokerProcessorType::Pull(processor) => processor.reject_request(code),
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
            BrokerProcessorType::AdminBroker(processor) => processor.reject_request(code),
        }
    }
}

pub(crate) type RequestCodeType = i32;

pub struct BrokerRequestProcessor<MS: MessageStore, TS> {
    process_table: ArcMut<HashMap<RequestCodeType, BrokerProcessorType<MS, TS>>>,
    default_request_processor: Option<ArcMut<BrokerProcessorType<MS, TS>>>,
    auth_runtime: Option<Arc<AuthRuntime>>,
    broker_fast_failure: Option<BrokerFastFailure>,
}

impl<MS, TS> BrokerRequestProcessor<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    pub fn new() -> Self {
        Self {
            process_table: ArcMut::new(HashMap::new()),
            default_request_processor: None,
            auth_runtime: None,
            broker_fast_failure: None,
        }
    }

    pub fn register_processor(&mut self, request_code: RequestCodeType, processor: BrokerProcessorType<MS, TS>) {
        self.process_table.insert(request_code, processor);
    }

    pub fn register_default_processor(&mut self, processor: BrokerProcessorType<MS, TS>) {
        self.default_request_processor = Some(ArcMut::new(processor));
    }

    pub fn set_auth_runtime(&mut self, auth_runtime: Arc<AuthRuntime>) {
        self.auth_runtime = Some(auth_runtime);
    }

    pub fn set_broker_fast_failure(&mut self, broker_fast_failure: BrokerFastFailure) {
        self.broker_fast_failure = Some(broker_fast_failure);
    }
}

#[cfg(test)]
impl<MS, TS> BrokerRequestProcessor<MS, TS>
where
    MS: MessageStore,
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

impl<MS: MessageStore, TS> Clone for BrokerRequestProcessor<MS, TS> {
    fn clone(&self) -> Self {
        Self {
            process_table: self.process_table.clone(),
            default_request_processor: self.default_request_processor.clone(),
            auth_runtime: self.auth_runtime.clone(),
            broker_fast_failure: self.broker_fast_failure.clone(),
        }
    }
}

impl<MS, TS> RequestProcessor for BrokerRequestProcessor<MS, TS>
where
    MS: MessageStore + Send + Sync + 'static,
    TS: TransactionalMessageService + Send + Sync + 'static,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if let Some(auth_runtime) = &self.auth_runtime {
            if let Err(error) = auth_runtime.check_remoting(&ctx, request).await {
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::NoPermission,
                    error.to_string(),
                )
                .set_opaque(request.opaque());
                return Ok(Some(response));
            }
        }

        let request_code = *request.code_ref();

        match self.process_table.get(&request_code).cloned() {
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
                    let response_command = RemotingCommand::create_response_command_with_code_remark(
                        rocketmq_remoting::code::response_code::ResponseCode::RequestCodeNotSupported,
                        format!("The request code {} is not supported.", request.code_ref()),
                    );
                    Ok(Some(response_command.set_opaque(request.opaque())))
                }
            },
        }
    }

    fn reject_request(&self, code: i32) -> RejectRequestResponse {
        match self.process_table.get(&code) {
            Some(processor) => processor.reject_request(code),
            None => {
                if let Some(default_processor) = &self.default_request_processor {
                    default_processor.reject_request(code)
                } else {
                    let response_command = RemotingCommand::create_response_command_with_code_remark(
                        rocketmq_remoting::code::response_code::ResponseCode::RequestCodeNotSupported,
                        format!("The request code {code} is not supported."),
                    );
                    (true, Some(response_command))
                }
            }
        }
    }
}

impl<MS, TS> BrokerRequestProcessor<MS, TS>
where
    MS: MessageStore + Send + Sync + 'static,
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
        let mut queued_request = request.clone();
        let (task, response_rx) = broker_fast_failure.enqueue(queue_kind, opaque);
        let broker_fast_failure = broker_fast_failure.clone();

        tokio::spawn(async move {
            let Some(_permit) = broker_fast_failure.acquire_permit(queue_kind).await else {
                warn!("fast failure queue permit acquisition failed: queue={queue_kind:?}");
                if broker_fast_failure.try_mark_running(queue_kind, &task) {
                    broker_fast_failure.complete(
                        queue_kind,
                        &task,
                        Some(system_error_response(
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
                Err(error) => Some(system_error_response(opaque, error.to_string())),
            };
            broker_fast_failure.complete(queue_kind, &task, response);
        });

        match response_rx.await {
            Ok(response) => Ok(response),
            Err(_error) => Ok(Some(system_error_response(
                opaque,
                "fast failure response channel closed before request completed",
            ))),
        }
    }
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

fn system_error_response(opaque: i32, remark: impl Into<String>) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(ResponseCode::SystemError, remark.into())
        .set_opaque(opaque)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
