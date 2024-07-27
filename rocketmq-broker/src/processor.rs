/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_remoting::runtime::server::ConnectionHandlerContext;
use rocketmq_remoting::Result;
use rocketmq_store::log_file::MessageStore;
use tracing::info;

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
pub(crate) mod pull_message_processor;
pub(crate) mod pull_message_result_handler;
pub(crate) mod query_assignment_processor;
pub(crate) mod query_message_processor;
pub(crate) mod reply_message_processor;
pub(crate) mod send_message_processor;

pub struct BrokerRequestProcessor<MS>
where
    MS: Clone,
{
    pub(crate) send_message_processor: SendMessageProcessor<MS>,
    pub(crate) pull_message_processor: PullMessageProcessor<MS>,
    pub(crate) peek_message_processor: PeekMessageProcessor,
    pub(crate) pop_message_processor: PopMessageProcessor,
    pub(crate) ack_message_processor: AckMessageProcessor,
    pub(crate) change_invisible_time_processor: ChangeInvisibleTimeProcessor,
    pub(crate) notification_processor: NotificationProcessor,
    pub(crate) polling_info_processor: PollingInfoProcessor,
    pub(crate) reply_message_processor: ReplyMessageProcessor,
    pub(crate) query_message_processor: QueryMessageProcessor<MS>,
    pub(crate) client_manage_processor: ClientManageProcessor<MS>,
    pub(crate) consumer_manage_processor: ConsumerManageProcessor<MS>,
    pub(crate) query_assignment_processor: QueryAssignmentProcessor,
    pub(crate) end_transaction_processor: EndTransactionProcessor,
    pub(crate) admin_broker_processor: AdminBrokerProcessor,
}
impl<MS: Clone> Clone for BrokerRequestProcessor<MS> {
    fn clone(&self) -> Self {
        Self {
            send_message_processor: self.send_message_processor.clone(),
            pull_message_processor: self.pull_message_processor.clone(),
            peek_message_processor: self.peek_message_processor.clone(),
            pop_message_processor: self.pop_message_processor.clone(),
            ack_message_processor: self.ack_message_processor.clone(),
            change_invisible_time_processor: self.change_invisible_time_processor.clone(),
            notification_processor: self.notification_processor.clone(),
            polling_info_processor: self.polling_info_processor.clone(),
            reply_message_processor: self.reply_message_processor.clone(),
            admin_broker_processor: self.admin_broker_processor.clone(),
            client_manage_processor: self.client_manage_processor.clone(),
            consumer_manage_processor: self.consumer_manage_processor.clone(),
            query_assignment_processor: self.query_assignment_processor.clone(),
            query_message_processor: self.query_message_processor.clone(),
            end_transaction_processor: self.end_transaction_processor.clone(),
        }
    }
}

impl<MS: MessageStore + Send + Sync + 'static> RequestProcessor for BrokerRequestProcessor<MS> {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Result<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("process_request: {:?}", request_code);
        let result = match request_code {
            RequestCode::SendMessage
            | RequestCode::SendMessageV2
            | RequestCode::SendBatchMessage
            | RequestCode::ConsumerSendMsgBack => {
                self.send_message_processor
                    .process_request(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::HeartBeat
            | RequestCode::UnregisterClient
            | RequestCode::CheckClientConfig => {
                self.client_manage_processor
                    .process_request(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::PullMessage | RequestCode::LitePullMessage => {
                self.pull_message_processor
                    .process_request(channel, ctx, request_code, request)
                    .await
            }
            RequestCode::GetConsumerListByGroup
            | RequestCode::UpdateConsumerOffset
            | RequestCode::QueryConsumerOffset => {
                self.consumer_manage_processor
                    .process_request(channel, ctx, request_code, request)
                    .await
            }

            RequestCode::QueryMessage | RequestCode::ViewMessageById => {
                self.query_message_processor
                    .process_request(channel, ctx, request_code, request)
                    .await
            }

            _ => {
                self.admin_broker_processor
                    .process_request(channel, ctx, request_code, request)
                    .await
            }
        };
        Ok(result)
    }
}
