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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::context::ProxyContext;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyResult;
use crate::service::ProxyTopicMessageType;
use crate::service::ResourceIdentity;
use crate::service::ServiceManager;
use crate::service::SubscriptionGroupMetadata;
use crate::status::ProxyPayloadStatus;

#[derive(Debug, Clone)]
pub struct QueryRouteRequest {
    pub topic: ResourceIdentity,
    pub endpoints: Vec<ResolvedEndpoint>,
}

#[derive(Debug, Clone)]
pub struct QueryRoutePlan {
    pub route: TopicRouteData,
    pub topic_message_type: ProxyTopicMessageType,
}

#[derive(Debug, Clone)]
pub struct QueryAssignmentRequest {
    pub topic: ResourceIdentity,
    pub group: ResourceIdentity,
    pub endpoints: Vec<ResolvedEndpoint>,
}

#[derive(Debug, Clone)]
pub struct QueryAssignmentPlan {
    pub route: TopicRouteData,
    pub assignments: Option<Vec<MessageQueueAssignment>>,
    pub subscription_group: Option<SubscriptionGroupMetadata>,
}

#[derive(Debug, Clone)]
pub struct SendMessageRequest {
    pub messages: Vec<SendMessageEntry>,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct SendMessageEntry {
    pub topic: ResourceIdentity,
    pub client_message_id: String,
    pub message: Message,
    pub queue_id: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct SendMessagePlan {
    pub entries: Vec<SendMessageResultEntry>,
}

#[derive(Debug, Clone)]
pub struct SendMessageResultEntry {
    pub status: ProxyPayloadStatus,
    pub send_result: Option<SendResult>,
}

#[derive(Debug, Clone)]
pub struct ConsumerFilterExpression {
    pub expression_type: String,
    pub expression: String,
}

#[derive(Debug, Clone)]
pub struct ReceiveTarget {
    pub topic: ResourceIdentity,
    pub queue_id: i32,
    pub broker_name: Option<String>,
    pub broker_addr: Option<String>,
    pub fifo: bool,
}

#[derive(Debug, Clone)]
pub struct ReceiveMessageRequest {
    pub group: ResourceIdentity,
    pub target: ReceiveTarget,
    pub filter_expression: ConsumerFilterExpression,
    pub batch_size: u32,
    pub invisible_duration: Duration,
    pub auto_renew: bool,
    pub long_polling_timeout: Duration,
    pub attempt_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub message: MessageExt,
    pub invisible_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct ReceiveMessagePlan {
    pub status: ProxyPayloadStatus,
    pub delivery_timestamp_ms: Option<i64>,
    pub messages: Vec<ReceivedMessage>,
}

#[derive(Debug, Clone)]
pub struct AckMessageEntry {
    pub message_id: String,
    pub receipt_handle: String,
    pub lite_topic: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AckMessageRequest {
    pub group: ResourceIdentity,
    pub topic: ResourceIdentity,
    pub entries: Vec<AckMessageEntry>,
}

#[derive(Debug, Clone)]
pub struct AckMessageResultEntry {
    pub message_id: String,
    pub receipt_handle: String,
    pub status: ProxyPayloadStatus,
}

#[derive(Debug, Clone)]
pub struct AckMessagePlan {
    pub entries: Vec<AckMessageResultEntry>,
}

#[derive(Debug, Clone)]
pub struct ChangeInvisibleDurationRequest {
    pub group: ResourceIdentity,
    pub topic: ResourceIdentity,
    pub receipt_handle: String,
    pub invisible_duration: Duration,
    pub message_id: String,
    pub lite_topic: Option<String>,
    pub suspend: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ChangeInvisibleDurationPlan {
    pub status: ProxyPayloadStatus,
    pub receipt_handle: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionResolution {
    Commit,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionSource {
    Client,
    ServerCheck,
}

#[derive(Debug, Clone)]
pub struct EndTransactionRequest {
    pub topic: ResourceIdentity,
    pub message_id: String,
    pub transaction_id: String,
    pub resolution: TransactionResolution,
    pub source: TransactionSource,
    pub trace_context: Option<String>,
    pub producer_group: Option<String>,
    pub transaction_state_table_offset: Option<u64>,
    pub commit_log_message_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EndTransactionPlan {
    pub status: ProxyPayloadStatus,
}

#[async_trait]
pub trait MessagingProcessor: Send + Sync {
    async fn query_route(&self, context: &ProxyContext, request: QueryRouteRequest) -> ProxyResult<QueryRoutePlan>;

    async fn query_assignment(
        &self,
        context: &ProxyContext,
        request: QueryAssignmentRequest,
    ) -> ProxyResult<QueryAssignmentPlan>;

    async fn send_message(&self, context: &ProxyContext, request: SendMessageRequest) -> ProxyResult<SendMessagePlan>;

    async fn receive_message(
        &self,
        context: &ProxyContext,
        request: ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan>;

    async fn ack_message(&self, context: &ProxyContext, request: AckMessageRequest) -> ProxyResult<AckMessagePlan>;

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan>;

    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan>;
}

#[derive(Clone)]
pub struct DefaultMessagingProcessor {
    service_manager: Arc<dyn ServiceManager>,
}

impl DefaultMessagingProcessor {
    pub fn new(service_manager: Arc<dyn ServiceManager>) -> Self {
        Self { service_manager }
    }

    pub fn service_manager(&self) -> &Arc<dyn ServiceManager> {
        &self.service_manager
    }
}

#[async_trait]
impl MessagingProcessor for DefaultMessagingProcessor {
    async fn query_route(&self, context: &ProxyContext, request: QueryRouteRequest) -> ProxyResult<QueryRoutePlan> {
        let route_service = self.service_manager.route_service();
        let metadata_service = self.service_manager.metadata_service();

        let route = route_service
            .query_route(context, &request.topic, &request.endpoints)
            .await?;
        let topic_message_type = metadata_service.topic_message_type(context, &request.topic).await?;

        Ok(QueryRoutePlan {
            route,
            topic_message_type,
        })
    }

    async fn query_assignment(
        &self,
        context: &ProxyContext,
        request: QueryAssignmentRequest,
    ) -> ProxyResult<QueryAssignmentPlan> {
        let route_service = self.service_manager.route_service();
        let assignment_service = self.service_manager.assignment_service();
        let metadata_service = self.service_manager.metadata_service();

        let route = route_service
            .query_route(context, &request.topic, &request.endpoints)
            .await?;
        let assignments = assignment_service
            .query_assignment(context, &request.topic, &request.group, &request.endpoints)
            .await?;
        let subscription_group = metadata_service
            .subscription_group(context, &request.topic, &request.group)
            .await?;

        Ok(QueryAssignmentPlan {
            route,
            assignments,
            subscription_group,
        })
    }

    async fn send_message(&self, context: &ProxyContext, request: SendMessageRequest) -> ProxyResult<SendMessagePlan> {
        let message_service = self.service_manager.message_service();
        let entries = message_service.send_message(context, &request).await?;

        Ok(SendMessagePlan { entries })
    }

    async fn receive_message(
        &self,
        context: &ProxyContext,
        request: ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan> {
        let consumer_service = self.service_manager.consumer_service();
        consumer_service.receive_message(context, &request).await
    }

    async fn ack_message(&self, context: &ProxyContext, request: AckMessageRequest) -> ProxyResult<AckMessagePlan> {
        let consumer_service = self.service_manager.consumer_service();
        let entries = consumer_service.ack_message(context, &request).await?;
        Ok(AckMessagePlan { entries })
    }

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        let consumer_service = self.service_manager.consumer_service();
        consumer_service.change_invisible_duration(context, &request).await
    }

    async fn end_transaction(
        &self,
        context: &ProxyContext,
        request: EndTransactionRequest,
    ) -> ProxyResult<EndTransactionPlan> {
        let transaction_service = self.service_manager.transaction_service();
        transaction_service.end_transaction(context, &request).await
    }
}
