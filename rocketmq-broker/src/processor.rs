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

use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

use self::client_manage_processor::ClientManageProcessor;
use crate::processor::ack_message_processor::AckMessageProcessor;
use crate::processor::admin_broker_processor::AdminBrokerProcessor;
use crate::processor::change_invisible_time_processor::ChangeInvisibleTimeProcessor;
use crate::processor::consumer_manage_processor::ConsumerManageProcessor;
use crate::processor::end_transaction_processor::EndTransactionProcessor;
use crate::processor::notification_processor::NotificationProcessor;
use crate::processor::peek_message_processor::PeekMessageProcessor;
use crate::processor::polling_info_processor::PollingInfoProcessor;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pull_message_processor::PullMessageProcessor;
use crate::processor::query_assignment_processor::QueryAssignmentProcessor;
use crate::processor::query_message_processor::QueryMessageProcessor;
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
pub(crate) mod notification_processor;
pub(crate) mod peek_message_processor;
pub(crate) mod polling_info_processor;
pub(crate) mod pop_inflight_message_counter;
pub(crate) mod pop_message_processor;
pub(crate) mod processor_service;
pub(crate) mod pull_message_processor;
pub(crate) mod pull_message_result_handler;
pub(crate) mod query_assignment_processor;
pub(crate) mod query_message_processor;
pub(crate) mod reply_message_processor;
pub(crate) mod send_message_processor;

pub enum BrokerProcessorType<MS: MessageStore, TS> {
    Send(ArcMut<SendMessageProcessor<MS, TS>>),
    Pull(ArcMut<PullMessageProcessor<MS>>),
    Peek(ArcMut<PeekMessageProcessor<MS>>),
    Pop(ArcMut<PopMessageProcessor<MS>>),
    Ack(ArcMut<AckMessageProcessor<MS>>),
    ChangeInvisible(ArcMut<ChangeInvisibleTimeProcessor<MS>>),
    Notification(ArcMut<NotificationProcessor<MS>>),
    PollingInfo(ArcMut<PollingInfoProcessor<MS>>),
    Reply(ArcMut<ReplyMessageProcessor<MS, TS>>),
    QueryMessage(ArcMut<QueryMessageProcessor<MS>>),
    ClientManage(ArcMut<ClientManageProcessor<MS>>),
    ConsumerManage(ArcMut<ConsumerManageProcessor<MS>>),
    QueryAssignment(ArcMut<QueryAssignmentProcessor<MS>>),
    EndTransaction(ArcMut<EndTransactionProcessor<TS, MS>>),
    AdminBroker(ArcMut<AdminBrokerProcessor<MS>>),
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
            BrokerProcessorType::Ack(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::ChangeInvisible(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::Notification(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::PollingInfo(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::Reply(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::QueryMessage(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::ClientManage(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::ConsumerManage(processor) => processor.process_request(channel, ctx, request).await,
            BrokerProcessorType::QueryAssignment(processor) => processor.process_request(channel, ctx, request).await,
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
            BrokerProcessorType::Ack(processor) => processor.reject_request(code),
            BrokerProcessorType::ChangeInvisible(processor) => processor.reject_request(code),
            BrokerProcessorType::Notification(processor) => processor.reject_request(code),
            BrokerProcessorType::PollingInfo(processor) => processor.reject_request(code),
            BrokerProcessorType::Reply(processor) => processor.reject_request(code),
            BrokerProcessorType::QueryMessage(processor) => processor.reject_request(code),
            BrokerProcessorType::ClientManage(processor) => processor.reject_request(code),
            BrokerProcessorType::ConsumerManage(processor) => processor.reject_request(code),
            BrokerProcessorType::QueryAssignment(processor) => processor.reject_request(code),
            BrokerProcessorType::EndTransaction(processor) => processor.reject_request(code),
            BrokerProcessorType::AdminBroker(processor) => processor.reject_request(code),
        }
    }
}

pub(crate) type RequestCodeType = i32;

pub struct BrokerRequestProcessor<MS: MessageStore, TS> {
    process_table: ArcMut<HashMap<RequestCodeType, BrokerProcessorType<MS, TS>>>,
    default_request_processor: Option<ArcMut<BrokerProcessorType<MS, TS>>>,
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
        }
    }

    pub fn register_processor(&mut self, request_code: RequestCodeType, processor: BrokerProcessorType<MS, TS>) {
        self.process_table.insert(request_code, processor);
    }

    pub fn register_default_processor(&mut self, processor: BrokerProcessorType<MS, TS>) {
        self.default_request_processor = Some(ArcMut::new(processor));
    }
}

impl<MS: MessageStore, TS> Clone for BrokerRequestProcessor<MS, TS> {
    fn clone(&self) -> Self {
        Self {
            process_table: self.process_table.clone(),
            default_request_processor: self.default_request_processor.clone(),
        }
    }
}

impl<MS, TS> RequestProcessor for BrokerRequestProcessor<MS, TS>
where
    MS: MessageStore + Send + Sync + 'static,
    TS: TransactionalMessageService,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match self.process_table.get_mut(request.code_ref()) {
            Some(processor) => processor.process_request(channel, ctx, request).await,
            None => match self.default_request_processor.as_mut() {
                Some(default_processor) => default_processor.process_request(channel, ctx, request).await,
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
