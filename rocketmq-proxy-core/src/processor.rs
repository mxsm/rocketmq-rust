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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_model::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::result::SendResult;
use rocketmq_model::topic::TopicMessageType;
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

use crate::context::ProxyContext;
use crate::context::ResolvedEndpoint;
use crate::contracts::ProxyServiceFuture;
use crate::contracts::ProxyTopicMessageType;
use crate::contracts::ServiceManager;
use crate::contracts::SubscriptionGroupMetadata;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::message::ProxyMessage;
use crate::message::ProxyMessageExt;
use crate::session::LiteSubscriptionSyncRequest;
use crate::status::ProxyPayloadStatus;
use crate::ResourceIdentity;

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
    pub validate_message_type: bool,
}

#[derive(Debug, Clone)]
pub struct SendMessageEntry {
    pub topic: ResourceIdentity,
    pub client_message_id: String,
    pub message: ProxyMessage,
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
pub struct RecallMessageRequest {
    pub topic: ResourceIdentity,
    pub recall_handle: String,
}

#[derive(Debug, Clone)]
pub struct RecallMessagePlan {
    pub status: ProxyPayloadStatus,
    pub message_id: String,
}

#[derive(Debug, Clone)]
pub struct ConsumerFilterExpression {
    pub expression_type: String,
    pub expression: String,
}

#[derive(Debug, Clone)]
pub struct MessageQueueTarget {
    pub topic: ResourceIdentity,
    pub queue_id: i32,
    pub broker_name: Option<String>,
    pub broker_addr: Option<String>,
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
    pub message: ProxyMessageExt,
    pub invisible_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct ReceiveMessagePlan {
    pub status: ProxyPayloadStatus,
    pub delivery_timestamp_ms: Option<i64>,
    pub messages: Vec<ReceivedMessage>,
}

#[derive(Debug, Clone)]
pub struct PullMessageRequest {
    pub group: ResourceIdentity,
    pub target: MessageQueueTarget,
    pub offset: i64,
    pub batch_size: u32,
    pub filter_expression: ConsumerFilterExpression,
    pub long_polling_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct PullMessagePlan {
    pub status: ProxyPayloadStatus,
    pub next_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
    pub messages: Vec<ProxyMessageExt>,
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
pub struct ForwardMessageToDeadLetterQueueRequest {
    pub group: ResourceIdentity,
    pub topic: ResourceIdentity,
    pub receipt_handle: String,
    pub message_id: String,
    pub delivery_attempt: i32,
    pub max_delivery_attempts: i32,
    pub lite_topic: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ForwardMessageToDeadLetterQueuePlan {
    pub status: ProxyPayloadStatus,
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

#[derive(Debug, Clone)]
pub struct UpdateOffsetRequest {
    pub group: ResourceIdentity,
    pub target: MessageQueueTarget,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct UpdateOffsetPlan {
    pub status: ProxyPayloadStatus,
}

#[derive(Debug, Clone)]
pub struct GetOffsetRequest {
    pub group: ResourceIdentity,
    pub target: MessageQueueTarget,
}

#[derive(Debug, Clone)]
pub struct GetOffsetPlan {
    pub status: ProxyPayloadStatus,
    pub offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOffsetPolicy {
    Beginning,
    End,
    Timestamp,
}

#[derive(Debug, Clone)]
pub struct QueryOffsetRequest {
    pub target: MessageQueueTarget,
    pub policy: QueryOffsetPolicy,
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct QueryOffsetPlan {
    pub status: ProxyPayloadStatus,
    pub offset: i64,
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

pub trait MessagingProcessor: Send + Sync {
    /// Returns the opaque producer group selected by the active transaction
    /// adapter, if transactional sends are supported.
    fn transaction_producer_group(&self, _context: &ProxyContext) -> Option<String> {
        None
    }

    fn sync_lite_subscription(
        &self,
        _context: &ProxyContext,
        _client_id: &str,
        _request: LiteSubscriptionSyncRequest,
    ) -> impl Future<Output = ProxyResult<()>> + Send {
        async { Ok(()) }
    }

    fn subscription_group_metadata(
        &self,
        _context: &ProxyContext,
        _topic: &ResourceIdentity,
        _group: &ResourceIdentity,
    ) -> impl Future<Output = ProxyResult<Option<SubscriptionGroupMetadata>>> + Send {
        async { Ok(None) }
    }

    fn query_route(
        &self,
        context: &ProxyContext,
        request: QueryRouteRequest,
    ) -> impl Future<Output = ProxyResult<QueryRoutePlan>> + Send;

    fn query_assignment(
        &self,
        context: &ProxyContext,
        request: QueryAssignmentRequest,
    ) -> impl Future<Output = ProxyResult<QueryAssignmentPlan>> + Send;

    fn send_message(
        &self,
        context: &ProxyContext,
        request: SendMessageRequest,
    ) -> impl Future<Output = ProxyResult<SendMessagePlan>> + Send;

    fn recall_message(
        &self,
        context: &ProxyContext,
        request: RecallMessageRequest,
    ) -> impl Future<Output = ProxyResult<RecallMessagePlan>> + Send;

    fn receive_message(
        &self,
        context: &ProxyContext,
        request: ReceiveMessageRequest,
    ) -> impl Future<Output = ProxyResult<ReceiveMessagePlan>> + Send;

    fn pull_message(
        &self,
        context: &ProxyContext,
        request: PullMessageRequest,
    ) -> impl Future<Output = ProxyResult<PullMessagePlan>> + Send;

    fn ack_message(
        &self,
        context: &ProxyContext,
        request: AckMessageRequest,
    ) -> impl Future<Output = ProxyResult<AckMessagePlan>> + Send;

    fn forward_message_to_dead_letter_queue(
        &self,
        context: &ProxyContext,
        request: ForwardMessageToDeadLetterQueueRequest,
    ) -> impl Future<Output = ProxyResult<ForwardMessageToDeadLetterQueuePlan>> + Send;

    fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: ChangeInvisibleDurationRequest,
    ) -> impl Future<Output = ProxyResult<ChangeInvisibleDurationPlan>> + Send;

    fn update_offset(
        &self,
        context: &ProxyContext,
        request: UpdateOffsetRequest,
    ) -> impl Future<Output = ProxyResult<UpdateOffsetPlan>> + Send;

    fn get_offset(
        &self,
        context: &ProxyContext,
        request: GetOffsetRequest,
    ) -> impl Future<Output = ProxyResult<GetOffsetPlan>> + Send;

    fn query_offset(
        &self,
        context: &ProxyContext,
        request: QueryOffsetRequest,
    ) -> impl Future<Output = ProxyResult<QueryOffsetPlan>> + Send;

    fn end_transaction(
        &self,
        context: &ProxyContext,
        request: EndTransactionRequest,
    ) -> impl Future<Output = ProxyResult<EndTransactionPlan>> + Send;
}

/// Object-safe boundary for dynamically selected Proxy processor plugins.
///
/// Static gRPC and remoting paths use [`MessagingProcessor`] directly and do
/// not pay for boxed futures. Only callers that explicitly select a dynamic
/// plugin use this adapter.
pub trait MessagingProcessorPlugin: Send + Sync {
    fn transaction_producer_group(&self, context: &ProxyContext) -> Option<String>;

    fn sync_lite_subscription<'a>(
        &'a self,
        context: &'a ProxyContext,
        client_id: &'a str,
        request: LiteSubscriptionSyncRequest,
    ) -> ProxyServiceFuture<'a, ()>;

    fn query_route<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: QueryRouteRequest,
    ) -> ProxyServiceFuture<'a, QueryRoutePlan>;

    fn query_assignment<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: QueryAssignmentRequest,
    ) -> ProxyServiceFuture<'a, QueryAssignmentPlan>;

    fn send_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: SendMessageRequest,
    ) -> ProxyServiceFuture<'a, SendMessagePlan>;

    fn recall_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: RecallMessageRequest,
    ) -> ProxyServiceFuture<'a, RecallMessagePlan>;

    fn receive_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: ReceiveMessageRequest,
    ) -> ProxyServiceFuture<'a, ReceiveMessagePlan>;

    fn pull_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: PullMessageRequest,
    ) -> ProxyServiceFuture<'a, PullMessagePlan>;

    fn ack_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: AckMessageRequest,
    ) -> ProxyServiceFuture<'a, AckMessagePlan>;

    fn forward_message_to_dead_letter_queue<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyServiceFuture<'a, ForwardMessageToDeadLetterQueuePlan>;

    fn change_invisible_duration<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: ChangeInvisibleDurationRequest,
    ) -> ProxyServiceFuture<'a, ChangeInvisibleDurationPlan>;

    fn update_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: UpdateOffsetRequest,
    ) -> ProxyServiceFuture<'a, UpdateOffsetPlan>;

    fn get_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: GetOffsetRequest,
    ) -> ProxyServiceFuture<'a, GetOffsetPlan>;

    fn query_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: QueryOffsetRequest,
    ) -> ProxyServiceFuture<'a, QueryOffsetPlan>;

    fn end_transaction<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: EndTransactionRequest,
    ) -> ProxyServiceFuture<'a, EndTransactionPlan>;
}

impl<T> MessagingProcessorPlugin for T
where
    T: MessagingProcessor + 'static,
{
    fn transaction_producer_group(&self, context: &ProxyContext) -> Option<String> {
        MessagingProcessor::transaction_producer_group(self, context)
    }

    fn sync_lite_subscription<'a>(
        &'a self,
        context: &'a ProxyContext,
        client_id: &'a str,
        request: LiteSubscriptionSyncRequest,
    ) -> ProxyServiceFuture<'a, ()> {
        Box::pin(MessagingProcessor::sync_lite_subscription(
            self, context, client_id, request,
        ))
    }

    fn query_route<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: QueryRouteRequest,
    ) -> ProxyServiceFuture<'a, QueryRoutePlan> {
        Box::pin(MessagingProcessor::query_route(self, context, request))
    }

    fn query_assignment<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: QueryAssignmentRequest,
    ) -> ProxyServiceFuture<'a, QueryAssignmentPlan> {
        Box::pin(MessagingProcessor::query_assignment(self, context, request))
    }

    fn send_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: SendMessageRequest,
    ) -> ProxyServiceFuture<'a, SendMessagePlan> {
        Box::pin(MessagingProcessor::send_message(self, context, request))
    }

    fn recall_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: RecallMessageRequest,
    ) -> ProxyServiceFuture<'a, RecallMessagePlan> {
        Box::pin(MessagingProcessor::recall_message(self, context, request))
    }

    fn receive_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: ReceiveMessageRequest,
    ) -> ProxyServiceFuture<'a, ReceiveMessagePlan> {
        Box::pin(MessagingProcessor::receive_message(self, context, request))
    }

    fn pull_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: PullMessageRequest,
    ) -> ProxyServiceFuture<'a, PullMessagePlan> {
        Box::pin(MessagingProcessor::pull_message(self, context, request))
    }

    fn ack_message<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: AckMessageRequest,
    ) -> ProxyServiceFuture<'a, AckMessagePlan> {
        Box::pin(MessagingProcessor::ack_message(self, context, request))
    }

    fn forward_message_to_dead_letter_queue<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyServiceFuture<'a, ForwardMessageToDeadLetterQueuePlan> {
        Box::pin(MessagingProcessor::forward_message_to_dead_letter_queue(
            self, context, request,
        ))
    }

    fn change_invisible_duration<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: ChangeInvisibleDurationRequest,
    ) -> ProxyServiceFuture<'a, ChangeInvisibleDurationPlan> {
        Box::pin(MessagingProcessor::change_invisible_duration(self, context, request))
    }

    fn update_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: UpdateOffsetRequest,
    ) -> ProxyServiceFuture<'a, UpdateOffsetPlan> {
        Box::pin(MessagingProcessor::update_offset(self, context, request))
    }

    fn get_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: GetOffsetRequest,
    ) -> ProxyServiceFuture<'a, GetOffsetPlan> {
        Box::pin(MessagingProcessor::get_offset(self, context, request))
    }

    fn query_offset<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: QueryOffsetRequest,
    ) -> ProxyServiceFuture<'a, QueryOffsetPlan> {
        Box::pin(MessagingProcessor::query_offset(self, context, request))
    }

    fn end_transaction<'a>(
        &'a self,
        context: &'a ProxyContext,
        request: EndTransactionRequest,
    ) -> ProxyServiceFuture<'a, EndTransactionPlan> {
        Box::pin(MessagingProcessor::end_transaction(self, context, request))
    }
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

impl MessagingProcessor for DefaultMessagingProcessor {
    fn transaction_producer_group(&self, context: &ProxyContext) -> Option<String> {
        self.service_manager
            .transaction_service()
            .transaction_producer_group(context)
    }

    async fn sync_lite_subscription(
        &self,
        context: &ProxyContext,
        client_id: &str,
        request: LiteSubscriptionSyncRequest,
    ) -> ProxyResult<()> {
        self.service_manager
            .consumer_service()
            .sync_lite_subscription(context, client_id, &request)
            .await
    }

    async fn subscription_group_metadata(
        &self,
        context: &ProxyContext,
        topic: &ResourceIdentity,
        group: &ResourceIdentity,
    ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
        self.service_manager
            .metadata_service()
            .subscription_group(context, topic, group)
            .await
    }

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
        if request.validate_message_type {
            let metadata_service = self.service_manager.metadata_service();
            for entry in &request.messages {
                if should_validate_message_type(entry) {
                    let expected = metadata_service.topic_message_type(context, &entry.topic).await?;
                    let actual = proxy_message_type(&entry.message);
                    validate_message_type(expected, actual)?;
                }
            }
        }

        let message_service = self.service_manager.message_service();
        let entries = message_service.send_message(context, &request).await?;

        Ok(SendMessagePlan { entries })
    }

    async fn recall_message(
        &self,
        context: &ProxyContext,
        request: RecallMessageRequest,
    ) -> ProxyResult<RecallMessagePlan> {
        let message_service = self.service_manager.message_service();
        message_service.recall_message(context, &request).await
    }

    async fn receive_message(
        &self,
        context: &ProxyContext,
        request: ReceiveMessageRequest,
    ) -> ProxyResult<ReceiveMessagePlan> {
        let consumer_service = self.service_manager.consumer_service();
        consumer_service.receive_message(context, &request).await
    }

    async fn pull_message(&self, context: &ProxyContext, request: PullMessageRequest) -> ProxyResult<PullMessagePlan> {
        let consumer_service = self.service_manager.consumer_service();
        consumer_service.pull_message(context, &request).await
    }

    async fn ack_message(&self, context: &ProxyContext, request: AckMessageRequest) -> ProxyResult<AckMessagePlan> {
        let consumer_service = self.service_manager.consumer_service();
        let entries = consumer_service.ack_message(context, &request).await?;
        Ok(AckMessagePlan { entries })
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        context: &ProxyContext,
        request: ForwardMessageToDeadLetterQueueRequest,
    ) -> ProxyResult<ForwardMessageToDeadLetterQueuePlan> {
        let consumer_service = self.service_manager.consumer_service();
        consumer_service
            .forward_message_to_dead_letter_queue(context, &request)
            .await
    }

    async fn change_invisible_duration(
        &self,
        context: &ProxyContext,
        request: ChangeInvisibleDurationRequest,
    ) -> ProxyResult<ChangeInvisibleDurationPlan> {
        let consumer_service = self.service_manager.consumer_service();
        consumer_service.change_invisible_duration(context, &request).await
    }

    async fn update_offset(
        &self,
        context: &ProxyContext,
        request: UpdateOffsetRequest,
    ) -> ProxyResult<UpdateOffsetPlan> {
        let consumer_service = self.service_manager.consumer_service();
        consumer_service.update_offset(context, &request).await
    }

    async fn get_offset(&self, context: &ProxyContext, request: GetOffsetRequest) -> ProxyResult<GetOffsetPlan> {
        let consumer_service = self.service_manager.consumer_service();
        consumer_service.get_offset(context, &request).await
    }

    async fn query_offset(&self, context: &ProxyContext, request: QueryOffsetRequest) -> ProxyResult<QueryOffsetPlan> {
        let consumer_service = self.service_manager.consumer_service();
        consumer_service.query_offset(context, &request).await
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

fn should_validate_message_type(entry: &SendMessageEntry) -> bool {
    let topic = entry.topic.name();
    !topic.starts_with(RETRY_GROUP_TOPIC_PREFIX)
        && !topic.starts_with(DLQ_GROUP_TOPIC_PREFIX)
        && entry.message.property(MessageConst::PROPERTY_TRANSFER_FLAG).is_none()
}

fn proxy_message_type(message: &ProxyMessage) -> ProxyTopicMessageType {
    match TopicMessageType::parse_from_message_property(message.properties()) {
        TopicMessageType::Unspecified => ProxyTopicMessageType::Unspecified,
        TopicMessageType::Normal => ProxyTopicMessageType::Normal,
        TopicMessageType::Fifo => ProxyTopicMessageType::Fifo,
        TopicMessageType::Delay => ProxyTopicMessageType::Delay,
        TopicMessageType::Transaction => ProxyTopicMessageType::Transaction,
        TopicMessageType::Priority => ProxyTopicMessageType::Priority,
        TopicMessageType::Lite => ProxyTopicMessageType::Lite,
        TopicMessageType::Mixed => ProxyTopicMessageType::Mixed,
    }
}

fn validate_message_type(expected: ProxyTopicMessageType, actual: ProxyTopicMessageType) -> ProxyResult<()> {
    if actual == ProxyTopicMessageType::Unspecified || (expected != ProxyTopicMessageType::Mixed && actual != expected)
    {
        return Err(ProxyError::message_property_conflict(format!(
            "TopicMessageType validate failed, the expected type is {expected:?}, but actual type is {actual:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::should_validate_message_type;
    use super::validate_message_type;
    use super::MessagingProcessorPlugin;
    use super::ProxyMessage;
    use super::ProxyTopicMessageType;
    use super::ResourceIdentity;
    use super::SendMessageEntry;

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ValidationCorpus {
        validation_cases: Vec<ValidationCase>,
    }

    #[derive(Deserialize)]
    struct ValidationCase {
        expected: String,
        actual: String,
        accepted: bool,
    }

    #[test]
    fn dynamic_processor_plugin_boundary_remains_object_safe() {
        fn accepts_plugin(_: Option<&dyn MessagingProcessorPlugin>) {}

        accepts_plugin(None);
    }

    #[test]
    fn message_type_validation_matches_the_java_55_corpus() {
        let corpus: ValidationCorpus =
            serde_json::from_str(include_str!("../../scripts/fixtures/v1-message-type-corpus.json"))
                .expect("valid v1 message type corpus");

        for case in corpus.validation_cases {
            assert_eq!(
                validate_message_type(message_type(&case.expected), message_type(&case.actual)).is_ok(),
                case.accepted,
                "expected {}, actual {}",
                case.expected,
                case.actual
            );
        }
    }

    #[test]
    fn retry_dlq_and_transfer_messages_bypass_topic_type_validation() {
        for (topic, transfer, expected) in [
            ("TopicA", false, true),
            ("%RETRY%GroupA", false, false),
            ("%DLQ%GroupA", false, false),
            ("TopicA", true, false),
        ] {
            let mut message = ProxyMessage::new(topic, b"body".as_slice());
            if transfer {
                message.put_property(
                    rocketmq_model::common::message::MessageConst::PROPERTY_TRANSFER_FLAG,
                    "true",
                );
            }
            let entry = SendMessageEntry {
                topic: ResourceIdentity::new("", topic),
                client_message_id: "message-id".to_owned(),
                message,
                queue_id: None,
            };
            assert_eq!(should_validate_message_type(&entry), expected, "{topic}");
        }
    }

    fn message_type(value: &str) -> ProxyTopicMessageType {
        match value {
            "UNSPECIFIED" => ProxyTopicMessageType::Unspecified,
            "NORMAL" => ProxyTopicMessageType::Normal,
            "FIFO" => ProxyTopicMessageType::Fifo,
            "DELAY" => ProxyTopicMessageType::Delay,
            "TRANSACTION" => ProxyTopicMessageType::Transaction,
            "PRIORITY" => ProxyTopicMessageType::Priority,
            "LITE" => ProxyTopicMessageType::Lite,
            "MIXED" => ProxyTopicMessageType::Mixed,
            other => panic!("unknown message type {other}"),
        }
    }
}
